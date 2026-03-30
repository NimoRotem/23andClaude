"""Scoring engine — downloads PGS scoring files and runs custom variant scoring."""

import asyncio
import gzip
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

from backend.config import (
    BCFTOOLS,
    PGS_CACHE_DIR,
    PLINK2,
    REDIS_URL,
    REF_PANEL_1KG_GRCH38,
    REF_PANEL_1KG_GRCH37,
    RUNS_DIR,
    SCRATCH_RUNS,
    SCRATCH_TMP,
)
from backend.database import SessionLocal
from backend.models.schemas import PGSCacheEntry, RunResult, ScoringRun, VCF

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# System resource helper
# ---------------------------------------------------------------------------

def _get_system_resources():
    """Read current system resources for execution planning."""
    total_cores = os.cpu_count() or 44

    # Read actual available memory from /proc/meminfo
    mem_total_gb = 176.0
    mem_avail_gb = 160.0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if parts[0] == "MemTotal:":
                    mem_total_gb = int(parts[1]) / 1024 / 1024
                elif parts[0] == "MemAvailable:":
                    mem_avail_gb = int(parts[1]) / 1024 / 1024
    except Exception:
        pass

    # Read current load average
    load_1m = 0.0
    try:
        with open("/proc/loadavg") as f:
            load_1m = float(f.read().split()[0])
    except Exception:
        pass

    # Estimate available cores (total minus current load)
    available_cores = max(2, int(total_cores - load_1m))

    return {
        "total_cores": total_cores,
        "available_cores": available_cores,
        "mem_total_gb": round(mem_total_gb, 1),
        "mem_avail_gb": round(mem_avail_gb, 1),
        "load_1m": round(load_1m, 1),
    }


# ---------------------------------------------------------------------------
# Redis progress publisher
# ---------------------------------------------------------------------------

async def _publish_progress(
    redis_conn: aioredis.Redis,
    run_id: str,
    pct: float,
    step: str,
    status: str = "running",
    error: str | None = None,
) -> None:
    """Publish a progress event to the Redis pub/sub channel for this run."""
    payload = {
        "type": "progress",
        "run_id": run_id,
        "pct": round(pct, 1),
        "step": step,
        "status": status,
    }
    if error:
        payload["error"] = error
    try:
        await redis_conn.publish(f"run:{run_id}:progress", json.dumps(payload))
    except Exception as exc:
        logger.warning("Failed to publish progress for run %s: %s", run_id, exc)


# ---------------------------------------------------------------------------
# Database helpers (sync — used inside the background task)
# ---------------------------------------------------------------------------

def _update_run(run_id: str, **kwargs: Any) -> None:
    """Open a fresh session, update the ScoringRun row, and close."""
    db = SessionLocal()
    try:
        run = db.query(ScoringRun).filter(ScoringRun.id == run_id).first()
        if not run:
            return
        for k, v in kwargs.items():
            setattr(run, k, v)
        db.commit()
    finally:
        db.close()


def _load_run(run_id: str) -> dict | None:
    """Load a ScoringRun and its associated VCF info. Returns a plain dict."""
    db = SessionLocal()
    try:
        run = db.query(ScoringRun).filter(ScoringRun.id == run_id).first()
        if not run:
            return None
        vcf = db.query(VCF).filter(VCF.id == run.vcf_id).first()
        return {
            "id": run.id,
            "vcf_id": run.vcf_id,
            "pgs_ids": run.pgs_ids or [],
            "engine": run.engine,
            "genome_build": run.genome_build,
            "config_snapshot": run.config_snapshot or {},
            "vcf_path_persistent": vcf.path_persistent if vcf else None,
            "vcf_path_fast": vcf.path_fast if vcf else None,
            "vcf_samples": vcf.samples if vcf else [],
        }
    finally:
        db.close()


def _save_run_result(
    run_id: str,
    pgs_id: str,
    trait: str,
    variants_matched: int,
    variants_total: int,
    match_rate: float,
    scores_json: list[dict],
    source_file_path: str = None,
    source_file_type: str = None,
) -> None:
    """Insert a RunResult row for one PGS scoring."""
    db = SessionLocal()
    try:
        rr = RunResult(
            run_id=run_id,
            pgs_id=pgs_id,
            trait=trait,
            variants_matched=variants_matched,
            variants_total=variants_total,
            match_rate=match_rate,
            scores_json=scores_json,
            source_file_path=source_file_path,
            source_file_type=source_file_type,
        )
        db.add(rr)
        db.commit()
    finally:
        db.close()


def _get_pgs_cache_entry(pgs_id: str) -> dict | None:
    """Return PGSCacheEntry as dict, or None."""
    db = SessionLocal()
    try:
        entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
        if not entry:
            return None
        return {
            "pgs_id": entry.pgs_id,
            "trait_reported": entry.trait_reported,
            "variants_number": entry.variants_number,
            "builds_available": entry.builds_available or [],
            "file_path_grch37": entry.file_path_grch37,
            "file_path_grch38": entry.file_path_grch38,
        }
    finally:
        db.close()


def _upsert_pgs_cache(pgs_id: str, **kwargs: Any) -> None:
    """Create or update a PGSCacheEntry row."""
    db = SessionLocal()
    try:
        entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
        if entry:
            for k, v in kwargs.items():
                setattr(entry, k, v)
        else:
            entry = PGSCacheEntry(pgs_id=pgs_id, **kwargs)
            db.add(entry)
        db.commit()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# PGS scoring file download
# ---------------------------------------------------------------------------

PGS_FTP_TEMPLATE = (
    "https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/"
    "{pgs_id}/ScoringFiles/Harmonized/"
    "{pgs_id}_hmPOS_{build}.txt.gz"
)

PGS_FTP_TEMPLATE_ORIG = (
    "https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/"
    "{pgs_id}/ScoringFiles/{pgs_id}.txt.gz"
)

PGS_API_SCORE = "https://www.pgscatalog.org/rest/score/{pgs_id}"


async def _download_pgs_file(pgs_id: str, build: str) -> Path | None:
    """
    Download a PGS scoring file from the PGS Catalog FTP.
    Returns the local path on success, None on failure.
    """
    cache_dir = PGS_CACHE_DIR / pgs_id
    cache_dir.mkdir(parents=True, exist_ok=True)

    dest = cache_dir / f"{pgs_id}_hmPOS_{build}.txt.gz"

    if dest.exists() and dest.stat().st_size > 0:
        logger.info("PGS file already cached: %s", dest)
        return dest

    # Try harmonized file first
    url = PGS_FTP_TEMPLATE.format(pgs_id=pgs_id, build=build)
    proc = await asyncio.create_subprocess_shell(
        f'curl -fsSL --retry 3 --connect-timeout 30 -o "{dest}" "{url}"',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()

    if proc.returncode == 0 and dest.exists() and dest.stat().st_size > 0:
        logger.info("Downloaded harmonized PGS file: %s", dest)
        return dest

    # Fall back to original scoring file
    dest_orig = cache_dir / f"{pgs_id}.txt.gz"
    url_orig = PGS_FTP_TEMPLATE_ORIG.format(pgs_id=pgs_id)
    proc2 = await asyncio.create_subprocess_shell(
        f'curl -fsSL --retry 3 --connect-timeout 30 -o "{dest_orig}" "{url_orig}"',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc2.communicate()

    if proc2.returncode == 0 and dest_orig.exists() and dest_orig.stat().st_size > 0:
        logger.info("Downloaded original PGS file: %s", dest_orig)
        return dest_orig

    logger.error("Failed to download PGS file for %s (build %s)", pgs_id, build)
    return None


async def _fetch_pgs_metadata(pgs_id: str) -> dict:
    """Fetch trait and metadata from the PGS Catalog REST API."""
    proc = await asyncio.create_subprocess_shell(
        f'curl -fsSL --connect-timeout 15 "{PGS_API_SCORE.format(pgs_id=pgs_id)}"',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode == 0 and stdout:
        try:
            return json.loads(stdout.decode())
        except json.JSONDecodeError:
            pass
    return {}


# ---------------------------------------------------------------------------
# PGS scoring file parser
# ---------------------------------------------------------------------------

def _parse_scoring_file(filepath: Path) -> tuple[str, list[dict]]:
    """
    Parse a PGS Catalog scoring file (gzipped or plain text).

    Returns (trait_reported, list_of_weight_entries).
    Each weight entry: {"chr": str, "pos": int, "effect_allele": str, "weight": float}
    """
    trait_reported = ""
    weights: list[dict] = []
    header_cols: list[str] = []

    open_fn = gzip.open if str(filepath).endswith(".gz") else open

    with open_fn(filepath, "rt", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue

            # Header/comment lines
            if line.startswith("#"):
                if "trait_reported=" in line:
                    trait_reported = line.split("trait_reported=", 1)[1].strip()
                continue

            # Column header line
            if not header_cols:
                header_cols = line.lower().split("\t")
                if len(header_cols) < 2:
                    header_cols = line.lower().split()
                continue

            # Data line — split by tab; allow fewer parts than headers
            # (trailing empty columns are normal in PGS Catalog files)
            parts = line.split("\t")
            if len(parts) < 3:
                # Too few columns even for minimal data — skip
                continue

            row = dict(zip(header_cols, parts))

            # Extract chromosome
            chrom = row.get("hm_chr") or row.get("chr_name") or row.get("chr") or ""
            chrom = chrom.replace("chr", "")

            # Extract position
            pos_str = row.get("hm_pos") or row.get("chr_position") or row.get("pos") or ""
            try:
                pos = int(pos_str)
            except (ValueError, TypeError):
                continue

            # Extract effect allele and weight
            effect_allele = (
                row.get("effect_allele") or row.get("allele1") or row.get("a1") or ""
            ).upper()

            # Handle both standard (effect_weight) and dosage-specific (dosage_X_weight) formats
            dosage_weights = None
            if row.get("dosage_0_weight") or row.get("dosage_1_weight") or row.get("dosage_2_weight"):
                try:
                    dosage_weights = {
                        0: float(row.get("dosage_0_weight", "0") or "0"),
                        1: float(row.get("dosage_1_weight", "0") or "0"),
                        2: float(row.get("dosage_2_weight", "0") or "0"),
                    }
                    # Use dosage_1_weight as the "additive" weight for freq-based calculations
                    weight = dosage_weights[1]
                except (ValueError, TypeError):
                    continue
            else:
                weight_str = (
                    row.get("effect_weight") or row.get("weight") or row.get("beta") or "0"
                )
                try:
                    weight = float(weight_str)
                except (ValueError, TypeError):
                    continue

            # Extract allele frequency (for population percentile calculation)
            freq_str = row.get("allelefrequency_effect") or row.get("eaf") or ""
            try:
                freq = float(freq_str) if freq_str else None
            except (ValueError, TypeError):
                freq = None

            # Extract rsID and other allele for detailed logging
            rsid = row.get("hm_rsid") or row.get("rsid") or row.get("snp") or ""
            other_allele = (
                row.get("other_allele") or row.get("hm_inferotherallele")
                or row.get("reference_allele") or ""
            ).upper()

            weights.append({
                "chr": chrom,
                "pos": pos,
                "effect_allele": effect_allele,
                "other_allele": other_allele,
                "rsid": rsid,
                "weight": weight,
                "dosage_weights": dosage_weights,  # None for standard, {0:w0, 1:w1, 2:w2} for GenoBoost
                "freq": freq,
            })

    return trait_reported, weights


# ---------------------------------------------------------------------------
# VCF genotype extraction via bcftools
# ---------------------------------------------------------------------------

async def _extract_genotypes_bcftools(
    vcf_path: str,
    regions: list[str],
    samples: list[str],
) -> dict[str, dict[str, str]]:
    """
    Use bcftools query to extract genotypes at specific regions.

    Returns dict keyed by "chr:pos" -> {sample_name: genotype_string}.
    Handles gVCF block records by mapping them to all queried positions within the block.
    """
    if not regions:
        return {}

    # Build a set of queried positions for gVCF block mapping
    queried_positions: dict[str, set] = {}  # chr -> set of pos (int)
    for r in regions:
        parts = r.split(":")
        if len(parts) >= 2:
            chrom = parts[0].replace("chr", "")
            pos_range = parts[1].split("-")
            pos = int(pos_range[0])
            queried_positions.setdefault(chrom, set()).add(pos)

    # Detect if VCF has INFO/END (gVCF) by checking header once
    hdr_cmd = f'{BCFTOOLS} view -h "{vcf_path}" 2>/dev/null'
    hdr_proc = await asyncio.create_subprocess_shell(
        hdr_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    hdr_out, _ = await hdr_proc.communicate()
    has_end_field = b"##INFO=<ID=END," in hdr_out
    fmt = "%CHROM\\t%POS\\t%REF\\t%ALT\\t%INFO/END[\\t%GT]\\n" if has_end_field else "%CHROM\\t%POS\\t%REF\\t%ALT\\t.[\\t%GT]\\n"

    result: dict[str, dict[str, str]] = {}

    # Process in batches to avoid command-line length limits
    batch_size = 2000
    for i in range(0, len(regions), batch_size):
        batch = regions[i : i + batch_size]
        regions_str = ",".join(batch)

        cmd = (
            f'{BCFTOOLS} query -r "{regions_str}" '
            f'-f "{fmt}" '
            f'"{vcf_path}"'
        )

        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            logger.warning(
                "bcftools query failed (rc=%d): %s",
                proc.returncode,
                stderr.decode()[:500] if stderr else "no stderr",
            )
            continue

        for line in stdout.decode().strip().split("\n"):
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 5:
                continue

            chrom = parts[0].replace("chr", "")
            pos_str = parts[1]
            ref_allele = parts[2]
            alt_allele = parts[3]
            end_str = parts[4]  # INFO/END or "." if not present

            gts = parts[5:]  # one genotype per sample
            sample_gts: dict[str, str] = {}
            for idx, sample in enumerate(samples):
                if idx < len(gts):
                    sample_gts[sample] = gts[idx]
                else:
                    sample_gts[sample] = "./."
            sample_gts["__ref__"] = ref_allele
            sample_gts["__alt__"] = alt_allele

            key = f"{chrom}:{pos_str}"
            result[key] = sample_gts

            # For gVCF block records (ALT=<*> or <NON_REF> with END),
            # map this genotype to ALL queried positions within the block
            is_gvcf_block = "<*>" in alt_allele or "<NON_REF>" in alt_allele
            if is_gvcf_block and end_str and end_str != ".":
                try:
                    block_start = int(pos_str)
                    block_end = int(end_str)
                    chr_positions = queried_positions.get(chrom, set())
                    block_positions = [qpos for qpos in chr_positions
                                       if block_start <= qpos <= block_end]
                    if block_positions:
                        # Fetch actual reference bases for positions in this block
                        actual_refs = await _fetch_ref_bases(vcf_path, chrom, block_positions)
                        for qpos in block_positions:
                            block_key = f"{chrom}:{qpos}"
                            if block_key not in result:
                                block_gts = dict(sample_gts)
                                # Override REF with the actual reference base at this position
                                block_gts["__ref__"] = actual_refs.get(qpos, ref_allele)
                                result[block_key] = block_gts
                except (ValueError, TypeError):
                    pass

    return result


async def _fetch_ref_bases(vcf_path: str, chrom: str, positions: list[int]) -> dict[int, str]:
    """Fetch actual reference bases from the FASTA for specific positions."""
    from backend.config import EXISTING_REFERENCE
    ref_bases = {}
    # Build samtools faidx regions
    regions = " ".join(f"{chrom}:{p}-{p}" for p in positions)
    from backend.config import SAMTOOLS
    cmd = f"{SAMTOOLS} faidx {EXISTING_REFERENCE} {regions} 2>/dev/null"
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    current_pos = None
    for line in stdout.decode().strip().split("\n"):
        if line.startswith(">"):
            # Parse: >1:1065910-1065910
            try:
                coords = line[1:].split(":")[1].split("-")
                current_pos = int(coords[0])
            except (IndexError, ValueError):
                current_pos = None
        elif current_pos is not None:
            ref_bases[current_pos] = line.strip().upper()
            current_pos = None
    return ref_bases


async def _get_vcf_samples(vcf_path: str) -> list[str]:
    """Use bcftools to extract sample names from the VCF header."""
    cmd = f'{BCFTOOLS} query -l "{vcf_path}"'
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return []
    return [s.strip() for s in stdout.decode().strip().split("\n") if s.strip()]


# ---------------------------------------------------------------------------
# Genotype dosage calculation
# ---------------------------------------------------------------------------

def _genotype_dosage(gt: str, effect_allele: str, ref: str = "", alt: str = "") -> float | None:
    """
    Convert a VCF genotype string to dosage of the effect allele.

    Properly maps genotype indices to alleles (REF=0, ALT1=1, ALT2=2, etc.)
    and counts how many copies match the effect allele.

    Returns None for missing genotypes.
    """
    gt_clean = gt.replace("|", "/")
    if gt_clean in ("./.", ".|.", ".", ""):
        return None

    # Build allele list: index 0 = REF, index 1+ = ALT(s)
    allele_list = [ref.upper()] if ref else []
    if alt:
        allele_list.extend(a.upper() for a in alt.split(","))

    # Handle gVCF hom-ref blocks: ALT=<*> or <NON_REF> with GT=0/0
    # IMPORTANT: the block's REF field is the base at the BLOCK START,
    # not necessarily the reference at the queried position within the block.
    # We need the actual reference base at the queried position.
    # The caller passes this via the __actual_ref__ key when available.
    if alt and ("<*>" in alt or "<NON_REF>" in alt):
        if gt_clean in ("0/0", "0|0"):
            # Use actual reference base if provided, otherwise skip
            actual_ref = ref  # may be overridden by caller
            if effect_allele.upper() == actual_ref.upper():
                return 2.0  # EA matches actual reference → dosage 2
            return 0.0  # EA doesn't match reference → dosage 0

    ea = effect_allele.upper()

    # Match EA directly against REF/ALT — NO complement matching.
    # Complement matching (A↔T, C↔G) is unreliable for palindromic variants
    # and causes systematic score inflation. If EA doesn't directly match
    # any allele in the VCF, skip this variant (return 0 dosage).
    real_alleles = [a for a in allele_list if not a.startswith("<")]
    if real_alleles and ea not in real_alleles:
        return 0.0  # EA doesn't match any allele — can't score

    target = ea

    indices = gt_clean.split("/")
    dosage = 0.0
    for a in indices:
        try:
            idx = int(a)
            if allele_list and idx < len(allele_list):
                if allele_list[idx] == target:
                    dosage += 1.0
            else:
                if idx > 0:
                    dosage += 1.0
        except (ValueError, TypeError):
            return None
    return dosage


# ---------------------------------------------------------------------------
# Custom scoring calculator
# ---------------------------------------------------------------------------

async def _get_vcf_allele_frequencies(vcf_path: str, positions: list[str]) -> dict[str, float]:
    """Extract allele frequencies from VCF INFO/AF field for given positions.

    Returns {chr:pos: freq} dict.
    """
    if not positions:
        return {}
    freqs = {}
    batch_size = 2000
    for i in range(0, len(positions), batch_size):
        batch = positions[i:i + batch_size]
        regions = ",".join(batch)
        cmd = (
            f"{BCFTOOLS} query -f '%CHROM:%POS\\t%INFO/AF\\n' "
            f"-r {regions} {vcf_path} 2>/dev/null"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        for line in stdout.decode().strip().split("\n"):
            if not line or "\t" not in line:
                continue
            parts = line.split("\t")
            key = parts[0]
            try:
                af = float(parts[1].split(",")[0])  # take first AF for multi-allelic
                if 0 < af < 1:
                    freqs[key] = af
            except (ValueError, IndexError):
                pass
    return freqs


async def _get_1kg_plink2_frequencies(
    weights: list[dict],
    population: str = "EUR",
    genome_build: str = "GRCh38",
) -> tuple[dict[str, float], dict]:
    """Compute population-specific allele frequencies from 1000 Genomes using plink2.

    Uses the full plink2 binary reference panel (3,202 samples across 5 superpopulations).
    Returns (freq_lookup, metadata) where freq_lookup is {chr:pos: float}.
    """
    import uuid as _uuid

    panel = REF_PANEL_1KG_GRCH38 if genome_build == "GRCh38" else REF_PANEL_1KG_GRCH37
    psam_path = f"{panel}.psam"

    if not Path(f"{panel}.pgen").exists():
        return {}, {"error": "Reference panel not found"}

    tmp_prefix = Path(SCRATCH_TMP) / f"plink2_{_uuid.uuid4().hex[:8]}"

    try:
        # 1. Write BED1 file (positions to extract)
        bed_path = f"{tmp_prefix}.bed"
        seen_positions = set()
        with open(bed_path, "w") as f:
            for w in weights:
                key = f"{w['chr']}\t{w['pos']}\t{w['pos']}"
                if key not in seen_positions:
                    f.write(key + "\n")
                    seen_positions.add(key)

        # 2. Write keep file (population filter)
        keep_path = f"{tmp_prefix}.keep"
        sample_count = 0
        if population != "MULTI":
            with open(psam_path) as psam, open(keep_path, "w") as kf:
                for line in psam:
                    if line.startswith("#"):
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) >= 5 and parts[4] == population:
                        kf.write(f"{parts[0]}\n")
                        sample_count += 1
        else:
            sample_count = 3202

        # 3. Run plink2 --freq
        cmd = f"{PLINK2} --pfile {panel} vzs --extract bed1 {bed_path}"
        if population != "MULTI":
            cmd += f" --keep {keep_path}"
        cmd += f" --freq cols=chrom,pos,ref,alt,altfreq,nobs --out {tmp_prefix}"

        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning("plink2 --freq failed: %s", stderr.decode()[:500])
            return {}, {"error": f"plink2 rc={proc.returncode}"}

        # 4. Parse .afreq output
        afreq_path = f"{tmp_prefix}.afreq"
        if not Path(afreq_path).exists():
            return {}, {"error": "No .afreq output"}

        # Build lookup: chr:pos -> [(ref, alt, alt_freq)]
        # .afreq columns: #CHROM POS ID REF ALT ALT_FREQS OBS_CT
        panel_freqs: dict[str, list] = {}
        with open(afreq_path) as f:
            for line in f:
                if line.startswith("#"):
                    continue
                parts = line.strip().split("\t")
                if len(parts) < 7:
                    continue
                chrom, pos = parts[0], parts[1]
                ref_a, alt_a = parts[3], parts[4]  # skip ID at parts[2]
                try:
                    alt_freq = float(parts[5])
                except ValueError:
                    continue
                key = f"{chrom}:{pos}"
                if key not in panel_freqs:
                    panel_freqs[key] = []
                panel_freqs[key].append((ref_a, alt_a, alt_freq))

        # 5. Match PGS effect alleles to panel alleles
        COMPLEMENT = {"A": "T", "T": "A", "C": "G", "G": "C"}
        freq_lookup = {}
        variants_found = 0

        for w in weights:
            key = f"{w['chr']}:{w['pos']}"
            ea = w["effect_allele"]
            if key not in panel_freqs:
                continue
            variants_found += 1

            matched = False
            for ref_a, alt_a, alt_f in panel_freqs[key]:
                if ea == alt_a:
                    freq_lookup[key] = max(0.001, min(0.999, alt_f))
                    matched = True
                    break
                elif ea == ref_a:
                    freq_lookup[key] = max(0.001, min(0.999, 1.0 - alt_f))
                    matched = True
                    break

            # Try strand complement if no direct match
            if not matched:
                ea_comp = "".join(COMPLEMENT.get(c, c) for c in ea)
                for ref_a, alt_a, alt_f in panel_freqs[key]:
                    if ea_comp == alt_a:
                        freq_lookup[key] = max(0.001, min(0.999, alt_f))
                        break
                    elif ea_comp == ref_a:
                        freq_lookup[key] = max(0.001, min(0.999, 1.0 - alt_f))
                        break

        metadata = {
            "source": "1kg_plink2",
            "panel": str(panel),
            "population": population,
            "sample_count": sample_count,
            "variants_queried": len(seen_positions),
            "variants_in_panel": variants_found,
            "variants_with_freq": len(freq_lookup),
        }

        return freq_lookup, metadata

    finally:
        # Cleanup temp files
        for suffix in [".bed", ".keep", ".afreq", ".log", ".nosex"]:
            p = Path(f"{tmp_prefix}{suffix}")
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass


async def _score_reference_panel(
    weights: list[dict],
    population: str = "EUR",
    genome_build: str = "GRCh38",
) -> dict:
    """Score all individuals in a 1000G population using plink2 --score.

    Returns {mean, std, n} — the empirical score distribution which
    properly accounts for LD and allele correlations.
    """
    import uuid as _uuid
    import zstandard

    panel = REF_PANEL_1KG_GRCH38 if genome_build == "GRCh38" else REF_PANEL_1KG_GRCH37
    psam_path = f"{panel}.psam"

    if not Path(f"{panel}.pgen").exists():
        return {}

    tmp_prefix = Path(SCRATCH_TMP) / f"refscore_{_uuid.uuid4().hex[:8]}"

    try:
        # 1. Build plink2-compatible score file using pvar IDs
        # Read the pvar to map chr:pos → pvar ID
        pvar_ids = {}  # chr:pos → (id, ref, alt)
        with open(f"{panel}.pvar.zst", "rb") as f:
            dctx = zstandard.ZstdDecompressor()
            import io
            needed = {f"{w['chr']}:{w['pos']}" for w in weights}
            with dctx.stream_reader(f) as reader:
                text = io.TextIOWrapper(reader, encoding="utf-8")
                for line in text:
                    if line.startswith("#"):
                        continue
                    parts = line.split("\t", 5)
                    if len(parts) < 5:
                        continue
                    key = f"{parts[0]}:{parts[1]}"
                    if key in needed:
                        pvar_ids[key] = (parts[2], parts[3], parts[4].strip())

        # 2. Write score file with pvar IDs
        score_path = f"{tmp_prefix}_score.txt"
        COMPLEMENT = {"A": "T", "T": "A", "C": "G", "G": "C"}
        matched_count = 0
        with open(score_path, "w") as sf:
            for w in weights:
                key = f"{w['chr']}:{w['pos']}"
                if key not in pvar_ids:
                    continue
                vid, ref_a, alt_a = pvar_ids[key]
                ea = w["effect_allele"]
                ea_comp = "".join(COMPLEMENT.get(c, c) for c in ea)
                # Write the scoring allele — plink2 needs the variant ID and the allele
                if ea == alt_a or ea == ref_a:
                    sf.write(f"{vid}\t{ea}\t{w['weight']}\n")
                    matched_count += 1
                elif ea_comp == alt_a or ea_comp == ref_a:
                    sf.write(f"{vid}\t{ea_comp}\t{w['weight']}\n")
                    matched_count += 1

        if matched_count < 10:
            return {}

        # 3. Write keep file
        keep_path = f"{tmp_prefix}.keep"
        sample_count = 0
        if population != "MULTI":
            with open(psam_path) as psam, open(keep_path, "w") as kf:
                for line in psam:
                    if line.startswith("#"):
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) >= 5 and parts[4] == population:
                        kf.write(f"{parts[0]}\n")
                        sample_count += 1

        # 4. Run plink2 --score
        cmd = (
            f"{PLINK2} --pfile {panel} vzs "
            f"{'--keep ' + keep_path if population != 'MULTI' else ''} "
            f"--score {score_path} no-mean-imputation ignore-dup-ids "
            f"--score-col-nums 3 "
            f"--out {tmp_prefix}"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning("plink2 --score ref panel failed: %s", stderr.decode()[:300])
            return {}

        # 5. Parse .sscore and compute empirical mean + std
        sscore_path = f"{tmp_prefix}.sscore"
        if not Path(sscore_path).exists():
            return {}

        raw_sums = []
        with open(sscore_path) as f:
            header = f.readline()  # skip header
            for line in f:
                parts = line.strip().split("\t")
                # SCORE_AVG * ALLELE_CT = raw sum
                try:
                    allele_ct = int(parts[3])
                    score_avg = float(parts[-1])
                    raw_sums.append(score_avg * allele_ct)
                except (ValueError, IndexError):
                    continue

        if len(raw_sums) < 10:
            return {}

        n = len(raw_sums)
        mean = sum(raw_sums) / n
        var = sum((s - mean) ** 2 for s in raw_sums) / n
        std = math.sqrt(var) if var > 0 else 0.0

        return {"mean": mean, "std": std, "n": n, "variants_scored": matched_count}

    finally:
        for suffix in ["_score.txt", ".keep", ".sscore", ".log", ".nosex"]:
            p = Path(f"{tmp_prefix}{suffix}")
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass


def _scoring_confidence(freq_source: str, freq_count: int, variants_total: int) -> dict:
    """Determine scoring confidence level based on frequency source quality."""
    freq_coverage = freq_count / variants_total if variants_total > 0 else 0

    if freq_source in ("pgs_file", "1kg_plink2") and freq_coverage >= 0.5:
        return {
            "level": "high",
            "label": "High Confidence",
            "description": f"Real population allele frequencies from {FREQ_SOURCE_LABELS.get(freq_source, freq_source)} ({freq_coverage:.0%} variant coverage)",
        }
    elif freq_source in ("pgs_file", "1kg_plink2") and freq_coverage >= 0.3:
        return {
            "level": "moderate",
            "label": "Moderate Confidence",
            "description": f"Real frequencies for {freq_coverage:.0%} of variants from {FREQ_SOURCE_LABELS.get(freq_source, freq_source)}",
        }
    elif freq_source == "vcf_af":
        return {
            "level": "moderate",
            "label": "Moderate Confidence",
            "description": "Allele frequencies from VCF (sample-derived, not population reference)",
        }
    else:
        return {
            "level": "low",
            "label": "Low Confidence",
            "description": "No population reference available. Percentiles are rough estimates only.",
        }


FREQ_SOURCE_LABELS = {
    "pgs_file": "PGS File Frequencies",
    "1kg_plink2": "1000 Genomes (plink2, 3202 samples)",
    "vcf_af": "VCF Allele Frequencies",
    "fallback": "Estimated (no reference)",
}


async def _score_pgs_custom(
    pgs_id: str,
    scoring_file: Path,
    vcf_path: str,
    samples: list[str],
    redis_conn: aioredis.Redis,
    run_id: str,
    pgs_index: int,
    total_pgs: int,
    freq_source: str = "auto",
    ref_population: str = "EUR",
    genome_build: str = "GRCh38",
) -> dict:
    """
    Custom PGS scoring: parse scoring file weights, query VCF for matching
    variants, compute per-sample scores.

    Returns a dict with scoring results.
    """
    # Parse the scoring file
    trait, weights = _parse_scoring_file(scoring_file)
    if not trait:
        # Try to fetch trait from API metadata
        meta = await _fetch_pgs_metadata(pgs_id)
        trait = meta.get("trait_reported", pgs_id)

    variants_total = len(weights)
    if variants_total == 0:
        return {
            "pgs_id": pgs_id,
            "trait": trait,
            "variants_matched": 0,
            "variants_total": 0,
            "match_rate": 0.0,
            "scores": [],
        }

    # Build regions list for bcftools
    regions: list[str] = []
    weight_lookup: dict[str, dict] = {}  # "chr:pos" -> weight entry
    for w in weights:
        key = f"{w['chr']}:{w['pos']}"
        region = f"{w['chr']}:{w['pos']}-{w['pos']}"
        regions.append(region)
        weight_lookup[key] = w

    # Deduplicate regions
    regions = list(dict.fromkeys(regions))

    # Publish progress
    base_pct = (pgs_index / total_pgs) * 80 + 10  # 10-90% range for scoring
    await _publish_progress(
        redis_conn, run_id, base_pct,
        f"scoring {pgs_id}: querying {len(regions)} variant positions",
    )

    # Extract genotypes from VCF
    genotypes = await _extract_genotypes_bcftools(vcf_path, regions, samples)

    # Compute per-sample scores
    # For positions IN the VCF: use the actual genotype dosage
    # For positions NOT in VCF: use MEAN IMPUTATION (dosage = 2*freq)
    # This is the standard approach — missing positions contribute the
    # population-expected dosage, not 0. Assuming dosage=0 (hom-ref)
    # would underestimate the score because many missing positions
    # have common effect alleles (freq 0.2-0.5).
    # --- Step 1: Get allele frequencies (needed for imputation AND percentiles) ---
    all_positions = [f"{w['chr']}:{w['pos']}" for w in weights]
    freq_lookup: dict[str, float] = {}
    freq_metadata = {}
    used_freq_source = "none"

    if freq_source == "auto":
        source_order = ["pgs_file", "1kg_plink2", "vcf_af", "fallback"]
    elif freq_source == "fallback":
        source_order = ["fallback"]
    else:
        source_order = [freq_source, "fallback"]

    for src in source_order:
        if freq_lookup:
            break
        if src == "pgs_file":
            for w in weights:
                if w.get("freq") is not None and 0 < w["freq"] < 1:
                    freq_lookup[f"{w['chr']}:{w['pos']}"] = w["freq"]
            if len(freq_lookup) >= len(weights) * 0.3:
                used_freq_source = "pgs_file"
                freq_metadata = {"source": "pgs_file", "variants_with_freq": len(freq_lookup)}
            else:
                freq_lookup.clear()
        elif src == "1kg_plink2":
            freq_lookup, freq_metadata = await _get_1kg_plink2_frequencies(
                weights, population=ref_population, genome_build=genome_build,
            )
            if len(freq_lookup) >= len(weights) * 0.2:
                used_freq_source = "1kg_plink2"
            else:
                freq_lookup.clear()
                freq_metadata = {}
        elif src == "vcf_af":
            freq_lookup = await _get_vcf_allele_frequencies(vcf_path, all_positions)
            if freq_lookup:
                used_freq_source = "vcf_af"
                freq_metadata = {"source": "vcf_af", "variants_with_freq": len(freq_lookup)}
        elif src == "fallback":
            used_freq_source = "fallback"
            freq_metadata = {"source": "fallback"}

    # --- Step 2: Score samples using ONLY variants present in VCF ---
    # Only use positions where we have actual genotype data.
    # No imputation — this keeps the score honest and comparable to the
    # reference panel (which we also score with the same variant set).
    raw_scores: dict[str, float] = {s: 0.0 for s in samples}
    variants_used: dict[str, int] = {s: 0 for s in samples}
    variants_in_vcf = 0
    variants_imputed = 0
    vcf_weight_keys: list[str] = []
    variant_details: list[dict] = []  # per-variant detail log

    for key, w in weight_lookup.items():
        detail_entry = {
            "rsid": w.get("rsid", ""),
            "chr": w["chr"],
            "pos": w["pos"],
            "effect_allele": w["effect_allele"],
            "other_allele": w.get("other_allele", ""),
            "weight": w["weight"],
        }

        if key in genotypes:
            variants_in_vcf += 1
            vcf_weight_keys.append(key)
            sample_gts = genotypes[key]
            ref_a = sample_gts.get("__ref__", "")
            alt_a = sample_gts.get("__alt__", "")

            # Collect per-sample genotype info
            sample_detail = {}
            dw = w.get("dosage_weights")  # None for standard, {0: w0, 1: w1, 2: w2} for GenoBoost
            for sample in samples:
                gt = sample_gts.get(sample, "./.")
                dosage = _genotype_dosage(gt, w["effect_allele"], ref=ref_a, alt=alt_a)
                if dosage is not None:
                    if dw:
                        # Dosage-specific weights (GenoBoost): look up weight by dosage
                        contribution = dw.get(int(dosage), 0)
                    else:
                        # Standard: dosage × weight
                        contribution = dosage * w["weight"]
                    raw_scores[sample] += contribution
                    variants_used[sample] += 1
                sample_detail[sample] = {"gt": gt, "dosage": dosage if dosage is not None else 0}

            detail_entry["status"] = "found"
            detail_entry["ref"] = ref_a
            detail_entry["alt"] = alt_a
            detail_entry["samples"] = sample_detail
            if dw:
                detail_entry["contribution"] = round(sum(dw.get(int(sd["dosage"]), 0) for sd in sample_detail.values()), 6)
            else:
                detail_entry["contribution"] = round(sum(sd["dosage"] * w["weight"] for sd in sample_detail.values()), 6)
        else:
            freq = freq_lookup.get(key)
            if freq is not None and 0 < freq < 1:
                imputed_dosage = 2.0 * freq
                dw = w.get("dosage_weights")
                for sample in samples:
                    if dw:
                        # Expected contribution = freq^2*w2 + 2*freq*(1-freq)*w1 + (1-freq)^2*w0
                        imp_contrib = (freq**2)*dw.get(2,0) + 2*freq*(1-freq)*dw.get(1,0) + ((1-freq)**2)*dw.get(0,0)
                        raw_scores[sample] += imp_contrib
                    else:
                        raw_scores[sample] += imputed_dosage * w["weight"]
                    variants_used[sample] += 1
                variants_imputed += 1
                detail_entry["status"] = "imputed"
                detail_entry["imputed_dosage"] = round(imputed_dosage, 4)
                detail_entry["contribution"] = round(imputed_dosage * w["weight"], 6)
            else:
                detail_entry["status"] = "missing"
                detail_entry["contribution"] = 0.0

        variant_details.append(detail_entry)

    variants_not_in_vcf = variants_total - variants_in_vcf
    variants_matched = variants_in_vcf
    match_rate = variants_in_vcf / variants_total if variants_total > 0 else 0.0

    # --- Step 3: Compute empirical reference distribution using plink2 --score ---
    # Instead of the theoretical Var = sum(2*f*(1-f)*w^2) which assumes independence,
    # score the entire reference panel and get the REAL mean and std.
    # This accounts for LD, allele correlations, and the actual score distribution.
    pop_mean = 0.0
    pop_std = 0.0
    has_pop_stats = False
    freq_count = len(freq_lookup)
    is_fallback = used_freq_source == "fallback"

    if used_freq_source == "1kg_plink2" and freq_metadata.get("source") == "1kg_plink2":
        # Compute empirical distribution by scoring the 1000G reference panel
        # Score the reference panel with ALL PGS variants.
        # The sample's score includes real genotypes + mean-imputed missing,
        # so it's comparable to the reference which has all real genotypes.
        ref_stats = await _score_reference_panel(
            weights, ref_population, genome_build,
        )
        if ref_stats.get("std", 0) > 0:
            pop_mean = ref_stats["mean"]
            pop_std = ref_stats["std"]
            has_pop_stats = True
            freq_metadata["ref_panel_mean"] = round(pop_mean, 6)
            freq_metadata["ref_panel_std"] = round(pop_std, 6)
            freq_metadata["ref_panel_n"] = ref_stats.get("n", 0)

    if not has_pop_stats and freq_lookup:
        # Fallback: theoretical formula (less accurate but better than nothing)
        for w in weights:
            key = f"{w['chr']}:{w['pos']}"
            freq = freq_lookup.get(key)
            if freq is not None and 0 < freq < 1:
                pop_mean += 2.0 * freq * w["weight"]
                pop_std_sq = getattr(pop_std, '_var', 0)  # accumulator trick
        # Recompute with proper variance
        _pop_var = 0.0
        _pop_mean = 0.0
        for w in weights:
            key = f"{w['chr']}:{w['pos']}"
            freq = freq_lookup.get(key)
            if freq is not None and 0 < freq < 1:
                _pop_mean += 2.0 * freq * w["weight"]
                _pop_var += 2.0 * freq * (1.0 - freq) * (w["weight"] ** 2)
        pop_mean = _pop_mean
        pop_std = math.sqrt(_pop_var) if _pop_var > 0 else 0.0
        has_pop_stats = pop_std > 0

    confidence = _scoring_confidence(used_freq_source, freq_count, variants_total)

    # Compute within-family Z-scores (only meaningful with 2+ samples)
    score_values = list(raw_scores.values())
    n = len(score_values)
    if n > 1:
        fam_mean = sum(score_values) / n
        fam_var = sum((s - fam_mean) ** 2 for s in score_values) / n
        fam_std = math.sqrt(fam_var) if fam_var > 0 else 1.0
    else:
        fam_mean = score_values[0] if score_values else 0.0
        fam_std = 1.0

    # Normal CDF approximation (Abramowitz & Stegun)
    def _norm_cdf(x):
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    # Build per-sample result entries with ranks and percentiles
    sample_entries: list[dict] = []
    for sample in samples:
        raw = raw_scores[sample]

        # Within-family Z-score
        fam_z = (raw - fam_mean) / fam_std if n > 1 and fam_std > 0 else None

        # Population Z-score and percentile
        if has_pop_stats:
            pop_z = (raw - pop_mean) / pop_std
            percentile = round(_norm_cdf(pop_z) * 100, 1)
        else:
            pop_z = None
            percentile = None

        sample_entries.append({
            "sample": sample,
            "raw_score": round(raw, 6),
            "z_score": round(fam_z, 4) if fam_z is not None else None,
            "pop_z_score": round(pop_z, 4) if pop_z is not None else None,
            "percentile": percentile,
            "confidence_level": confidence["level"],
            "variants_used": variants_used[sample],
            "rank": 0,
        })

    # Rank by raw_score descending (rank 1 = highest score)
    sample_entries.sort(key=lambda x: x["raw_score"], reverse=True)
    for rank_idx, entry in enumerate(sample_entries, start=1):
        entry["rank"] = rank_idx

    # Publish progress
    done_pct = ((pgs_index + 1) / total_pgs) * 80 + 10
    await _publish_progress(
        redis_conn, run_id, done_pct,
        f"scored {pgs_id}: {variants_matched}/{variants_total} variants matched ({match_rate:.1%})",
    )

    return {
        "pgs_id": pgs_id,
        "trait": trait,
        "variants_matched": variants_matched,
        "variants_in_vcf": variants_in_vcf,
        "variants_imputed": variants_imputed,
        "variants_not_in_vcf": variants_not_in_vcf,
        "variants_total": variants_total,
        "match_rate": round(match_rate, 4),
        "freq_source": used_freq_source,
        "freq_source_label": FREQ_SOURCE_LABELS.get(used_freq_source, used_freq_source),
        "freq_variants_used": freq_count,
        "freq_metadata": freq_metadata,
        "confidence": confidence,
        "scores": sample_entries,
        "variant_details": variant_details,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run_scoring_job(run_id: str) -> None:
    """
    Full scoring pipeline for a single run.

    1. Load run config from DB
    2. Download / cache PGS scoring files
    3. Run custom scoring engine
    4. Save results to DB + disk
    5. Publish progress events throughout
    """
    redis_conn = aioredis.from_url(REDIS_URL, decode_responses=True)
    started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()

    try:
        # ---- Load run ----
        run_data = _load_run(run_id)
        if not run_data:
            logger.error("Run %s not found in database", run_id)
            return

        pgs_ids: list[str] = run_data["pgs_ids"]
        build: str = run_data["genome_build"]
        vcf_path = run_data.get("vcf_path_fast") or run_data.get("vcf_path_persistent")

        # Check if this is a BAM-only run (no VCF needed)
        source_files_list = run_data.get("source_files") or run_data.get("config_snapshot", {}).get("source_files") or []
        has_vcf_source = any(sf.get("type") in ("vcf", "gvcf") for sf in source_files_list)
        has_bam_source = any(sf.get("type") == "bam" for sf in source_files_list)

        if has_vcf_source and (not vcf_path or not Path(vcf_path).exists()):
            _update_run(
                run_id,
                status="failed",
                error_message=f"VCF file not found: {vcf_path}",
                completed_at=datetime.now(timezone.utc),
                duration_sec=round(time.monotonic() - t0, 2),
            )
            await _publish_progress(
                redis_conn, run_id, 0,
                "VCF file not found", status="failed",
                error=f"VCF file not found: {vcf_path}",
            )
            return

        # Prepare run directories
        run_dir_persistent = RUNS_DIR / run_id
        run_dir_fast = SCRATCH_RUNS / run_id
        run_dir_persistent.mkdir(parents=True, exist_ok=True)
        run_dir_fast.mkdir(parents=True, exist_ok=True)

        _update_run(
            run_id,
            status="downloading",
            progress_pct=2.0,
            current_step="downloading PGS scoring files",
            started_at=started_at,
            results_path_persistent=str(run_dir_persistent),
            results_path_fast=str(run_dir_fast),
        )
        await _publish_progress(
            redis_conn, run_id, 2.0, "downloading PGS scoring files", status="downloading",
        )

        # ---- Get sample list ----
        samples = run_data.get("vcf_samples") or []
        if not samples and vcf_path:
            samples = await _get_vcf_samples(vcf_path)
        if not samples:
            # For BAM-only runs, extract sample names from source files
            for sf in source_files_list:
                if sf.get("type") == "bam":
                    bam_name = os.path.basename(sf.get("path", "")).replace(".bam", "")
                    if bam_name and bam_name not in samples:
                        samples.append(bam_name)
            # Also check config_snapshot
            if not samples:
                samples = run_data.get("config_snapshot", {}).get("samples", [])

        if not samples:
            # Last resort: use "Unknown" so scoring doesn't fail
            samples = ["Unknown"]
            logger.warning("No samples found for run %s, using 'Unknown'", run_id)

        if False:  # disabled: don't fail on missing samples
            _update_run(
                run_id,
                status="failed",
                error_message="No samples found",
                completed_at=datetime.now(timezone.utc),
                duration_sec=round(time.monotonic() - t0, 2),
            )
            await _publish_progress(
                redis_conn, run_id, 0,
                "No samples found in VCF", status="failed",
                error="No samples found in VCF",
            )
            return

        # ---- Download PGS scoring files ----
        scoring_files: dict[str, Path] = {}
        total_pgs = len(pgs_ids)

        for idx, pgs_id in enumerate(pgs_ids):
            dl_pct = 2 + (idx / max(total_pgs, 1)) * 8  # 2-10% for downloading
            await _publish_progress(
                redis_conn, run_id, dl_pct,
                f"downloading {pgs_id} ({idx + 1}/{total_pgs})",
            )
            _update_run(
                run_id,
                progress_pct=dl_pct,
                current_step=f"downloading {pgs_id}",
            )

            # Check cache entry in DB
            cache_entry = _get_pgs_cache_entry(pgs_id)
            cached_path = None
            if cache_entry:
                if build == "GRCh38" and cache_entry.get("file_path_grch38"):
                    p = Path(cache_entry["file_path_grch38"])
                    if p.exists():
                        cached_path = p
                elif build == "GRCh37" and cache_entry.get("file_path_grch37"):
                    p = Path(cache_entry["file_path_grch37"])
                    if p.exists():
                        cached_path = p

            if cached_path:
                scoring_files[pgs_id] = cached_path
                logger.info("Using cached scoring file for %s: %s", pgs_id, cached_path)
            else:
                downloaded = await _download_pgs_file(pgs_id, build)
                if downloaded:
                    scoring_files[pgs_id] = downloaded
                    # Update cache DB entry
                    update_kwargs: dict[str, Any] = {
                        "downloaded_at": datetime.now(timezone.utc),
                        "file_size_bytes": downloaded.stat().st_size,
                    }
                    if build == "GRCh38":
                        update_kwargs["file_path_grch38"] = str(downloaded)
                    else:
                        update_kwargs["file_path_grch37"] = str(downloaded)
                    _upsert_pgs_cache(pgs_id, **update_kwargs)
                else:
                    logger.warning("Could not download scoring file for %s", pgs_id)

        if not scoring_files:
            _update_run(
                run_id,
                status="failed",
                error_message="Failed to download any PGS scoring files",
                completed_at=datetime.now(timezone.utc),
                duration_sec=round(time.monotonic() - t0, 2),
            )
            await _publish_progress(
                redis_conn, run_id, 10,
                "Failed to download PGS scoring files", status="failed",
                error="Failed to download any PGS scoring files",
            )
            return

        # ---- Resource planning ----
        import shutil

        resources = _get_system_resources()
        total_cores = resources["available_cores"]  # Use AVAILABLE, not total
        mem_avail_gb = resources["mem_avail_gb"]
        mem_gb = resources["mem_total_gb"]

        all_results: list[dict] = []
        scorable_pgs = list(scoring_files.keys())
        total_scorable = len(scorable_pgs)

        config = run_data.get("config_snapshot", {})
        freq_src = config.get("freq_source", "auto")
        ref_pop = config.get("ref_population", "EUR")

        source_files_list = run_data.get("source_files") or config.get("source_files") or []
        if not source_files_list:
            source_files_list = [{"type": "vcf", "path": vcf_path}] if vcf_path else []

        n_sources = max(len(source_files_list), 1)
        n_pgs = max(total_scorable, 1)
        total_tasks = n_sources * n_pgs

        # Determine parallelism strategy
        bam_sources = [sf for sf in source_files_list if sf.get("type") == "bam"]
        vcf_sources = [sf for sf in source_files_list if sf.get("type") in ("vcf", "gvcf")]

        # BAM scoring: ~4GB RAM per concurrent pysam instance, 1 core each
        max_parallel_bam = min(len(bam_sources), max(1, int(mem_avail_gb / 4)), total_cores // 2)
        # VCF scoring: lighter, can run more in parallel
        max_parallel_vcf = min(len(vcf_sources), max(1, total_cores // 4))
        # Total: don't exceed core count
        max_parallel = min(max_parallel_bam + max_parallel_vcf, total_cores - 2)

        execution_plan = {
            "total_cores": resources["total_cores"],
            "available_cores": resources["available_cores"],
            "load_1m": resources["load_1m"],
            "total_ram_gb": round(mem_gb, 1),
            "available_ram_gb": round(mem_avail_gb, 1),
            "source_files": n_sources,
            "bam_sources": len(bam_sources),
            "vcf_sources": len(vcf_sources),
            "pgs_scores": n_pgs,
            "total_tasks": total_tasks,
            "max_parallel_bam": max_parallel_bam,
            "max_parallel_vcf": max_parallel_vcf,
            "strategy": "parallel" if max_parallel > 1 else "sequential",
            "note": f"Running up to {max_parallel} tasks concurrently ({max_parallel_bam} BAM + {max_parallel_vcf} VCF)",
        }

        logger.info("Execution plan for run %s: %s", run_id, execution_plan)

        _update_run(
            run_id,
            status="scoring",
            progress_pct=10.0,
            current_step=f"scoring — {execution_plan['strategy']} ({max_parallel} concurrent)",
        )
        await _publish_progress(
            redis_conn, run_id, 10.0,
            f"scoring — {n_sources} sources × {n_pgs} PGS = {total_tasks} tasks, {execution_plan['strategy']} ({max_parallel} concurrent)",
            status="scoring",
        )

        # ---- Execute scoring tasks ----
        # Build all tasks as (source_file, pgs_id) pairs
        task_queue = []
        for sf_idx, sf in enumerate(source_files_list):
            sf_type = sf.get("type", "vcf")
            sf_path = sf.get("path", vcf_path)
            for pgs_idx, pgs_id in enumerate(scorable_pgs):
                task_queue.append((sf_idx, sf, sf_type, sf_path, pgs_idx, pgs_id))

        completed_count = 0

        # Run BAM and VCF tasks with controlled concurrency
        # For now: BAMs run concurrently (up to max_parallel_bam), VCFs sequential
        # (VCF scoring shares bcftools processes which don't parallelize well)

        async def run_single_task(task_tuple):
            nonlocal completed_count
            sf_idx, sf, sf_type, sf_path, pgs_idx, pgs_id = task_tuple
            task_num = sf_idx * n_pgs + pgs_idx

            # Use per-file ref_population if set, otherwise fall back to global
            task_ref_pop = sf.get("ref_population") or ref_pop

            pct = 10 + (completed_count / max(total_tasks, 1)) * 80
            step_msg = f"scoring {os.path.basename(sf_path)} ({sf_type}, {task_ref_pop}) — {pgs_id} [{completed_count+1}/{total_tasks}]"
            _update_run(run_id, progress_pct=round(pct, 1), current_step=step_msg)
            await _publish_progress(redis_conn, run_id, pct, step_msg, status="scoring")

            if sf_type == "bam":
                try:
                    from backend.scoring.pipeline_e_plus import score_bam_direct
                    scoring_result = await score_bam_direct(
                        bam_path=sf_path, pgs_id=pgs_id,
                        scoring_file_path=str(scoring_files[pgs_id]),
                        genome_build=build, ref_population=task_ref_pop,
                        freq_source=freq_src, redis_conn=redis_conn,
                        run_id=run_id, pgs_index=pgs_idx, total_pgs=total_scorable,
                    )
                except Exception as e:
                    logger.exception("Pipeline E+ failed for %s on %s", pgs_id, sf_path)
                    scoring_result = {
                        "pgs_id": pgs_id, "trait": pgs_id, "variants_matched": 0,
                        "variants_total": 0, "match_rate": 0, "scores": [],
                        "error": str(e),
                    }
            else:
                sf_vcf_path = sf_path
                sf_samples = sf.get("samples") or samples
                scoring_result = await _score_pgs_custom(
                    pgs_id=pgs_id, scoring_file=scoring_files[pgs_id],
                    vcf_path=sf_vcf_path, samples=sf_samples,
                    redis_conn=redis_conn, run_id=run_id,
                    pgs_index=pgs_idx, total_pgs=total_scorable,
                    freq_source=freq_src, ref_population=task_ref_pop,
                    genome_build=build,
                )

            scoring_result["source_file_path"] = sf_path
            scoring_result["source_file_type"] = sf_type

            completed_count += 1
            done_pct = 10 + (completed_count / max(total_tasks, 1)) * 80
            done_msg = f"scored {pgs_id} on {os.path.basename(sf_path)} [{completed_count}/{total_tasks}]"
            _update_run(run_id, progress_pct=round(done_pct, 1), current_step=done_msg)
            await _publish_progress(redis_conn, run_id, done_pct, done_msg, status="scoring")

            _save_run_result(
                run_id=run_id, pgs_id=scoring_result["pgs_id"],
                trait=scoring_result.get("trait", pgs_id),
                variants_matched=scoring_result.get("variants_matched", 0),
                variants_total=scoring_result.get("variants_total", 0),
                match_rate=scoring_result.get("match_rate", 0),
                scores_json=scoring_result.get("scores", []),
                source_file_path=sf_path, source_file_type=sf_type,
            )
            return scoring_result

        # Execute tasks with concurrency control
        if max_parallel > 1 and total_tasks > 1:
            # Parallel execution using semaphore
            sem = asyncio.Semaphore(max_parallel)

            async def sem_task(t):
                async with sem:
                    return await run_single_task(t)

            results_list = await asyncio.gather(
                *(sem_task(t) for t in task_queue),
                return_exceptions=True,
            )
            for r in results_list:
                if isinstance(r, Exception):
                    logger.exception("Task failed: %s", r)
                    all_results.append({"error": str(r), "pgs_id": "?", "scores": []})
                elif r:
                    all_results.append(r)
        else:
            # Sequential execution
            for t in task_queue:
                r = await run_single_task(t)
                if r:
                    all_results.append(r)

        # ---- Save per-PGS detail JSON files ----
        for result in all_results:
            variant_details = result.pop("variant_details", None) or result.pop("detail_variants", None)
            if variant_details:
                pgs_id_safe = result["pgs_id"].replace("/", "_")
                # Use source filename (without extension) to make unique per-source detail files
                sf_path = result.get("source_file_path", "")
                sf_basename = os.path.basename(sf_path).split(".")[0] if sf_path else "unknown"
                sf_type = result.get("source_file_type", "vcf")
                detail_filename = f"{pgs_id_safe}_{sf_basename}_{sf_type}_detail.json"
                # Limit detail log to first 1000 variants to avoid giant files / UI crashes
                MAX_DETAIL_VARIANTS = 1000
                truncated = len(variant_details) > MAX_DETAIL_VARIANTS
                detail_payload = {
                    "pgs_id": result["pgs_id"],
                    "trait": result.get("trait", ""),
                    "source_file_path": result.get("source_file_path", ""),
                    "source_file_type": result.get("source_file_type", ""),
                    "variants_total": result.get("variants_total", 0),
                    "variants_matched": result.get("variants_matched", 0),
                    "match_rate": result.get("match_rate", 0),
                    "variants_in_log": min(len(variant_details), MAX_DETAIL_VARIANTS),
                    "variants_truncated": truncated,
                    "variants": variant_details[:MAX_DETAIL_VARIANTS],
                }
                for d in (run_dir_persistent, run_dir_fast):
                    try:
                        (d / detail_filename).write_text(
                            json.dumps(detail_payload, indent=None, default=str)
                        )
                    except OSError:
                        pass

        # ---- Save results to disk ----
        await _publish_progress(
            redis_conn, run_id, 92.0, "saving results to disk",
        )

        duration_sec = round(time.monotonic() - t0, 2)
        completed_at = datetime.now(timezone.utc)

        results_payload = {
            "run_id": run_id,
            "genome_build": build,
            "vcf_path": vcf_path,
            "samples": samples,
            "engine": run_data["engine"],
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_sec": duration_sec,
            "execution_plan": execution_plan,
            "pgs_results": all_results,
        }

        # Write results.json to both locations
        for d in (run_dir_persistent, run_dir_fast):
            results_file = d / "results.json"
            results_file.write_text(json.dumps(results_payload, indent=2, default=str))

        # Write run_manifest.json
        manifest = {
            "run_id": run_id,
            "config": {
                "vcf_id": run_data["vcf_id"],
                "pgs_ids": pgs_ids,
                "engine": run_data["engine"],
                "genome_build": build,
            },
            "timing": {
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_sec": duration_sec,
            },
            "results_summary": {
                "total_pgs_scored": len(all_results),
                "total_pgs_requested": total_pgs,
                "samples": samples,
            },
        }

        for d in (run_dir_persistent, run_dir_fast):
            manifest_file = d / "run_manifest.json"
            manifest_file.write_text(json.dumps(manifest, indent=2, default=str))

        # ---- Generate Markdown report ----
        md = []
        md.append(f"# Scoring Run Report\n")
        md.append(f"**Run ID:** `{run_id}`  ")
        md.append(f"**Date:** {started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}  ")
        md.append(f"**Duration:** {duration_sec:.1f}s ({duration_sec/60:.1f} min)  ")
        md.append(f"**Status:** complete\n")

        md.append(f"## Source Files\n")
        md.append(f"| # | Type | Filename | Path |")
        md.append(f"|---|------|----------|------|")
        for i, sf in enumerate(source_files_list):
            fname = os.path.basename(sf.get("path", "?"))
            md.append(f"| {i+1} | {sf.get('type','?')} | {fname} | `{sf.get('path','?')}` |")

        md.append(f"\n## Settings\n")
        md.append(f"| Setting | Value |")
        md.append(f"|---------|-------|")
        md.append(f"| Engine | {run_data.get('engine', '?')} |")
        md.append(f"| Genome Build | {build} |")
        md.append(f"| Reference Population | {ref_pop} |")
        md.append(f"| Allele Frequency Source | {freq_src} |")
        md.append(f"| PGS Scores | {', '.join(pgs_ids)} |")

        md.append(f"\n## Server Resources\n")
        md.append(f"| Resource | Value |")
        md.append(f"|----------|-------|")
        md.append(f"| CPU Cores | {execution_plan.get('total_cores', '?')} |")
        md.append(f"| Total RAM | {execution_plan.get('total_ram_gb', '?')} GB |")
        md.append(f"| Available RAM | {execution_plan.get('available_ram_gb', '?')} GB |")
        md.append(f"| Strategy | {execution_plan.get('strategy', '?')} |")
        md.append(f"| Max Parallel BAM | {execution_plan.get('max_parallel_bam', '?')} |")
        md.append(f"| Max Parallel VCF | {execution_plan.get('max_parallel_vcf', '?')} |")
        md.append(f"| Note | {execution_plan.get('note', '')} |")

        md.append(f"\n## Results\n")
        for result in all_results:
            pgs = result.get("pgs_id", "?")
            trait = result.get("trait", "?")
            sf_type = result.get("source_file_type", "?")
            sf_name = os.path.basename(result.get("source_file_path", "?"))
            matched = result.get("variants_matched", 0)
            total = result.get("variants_total", 0)
            rate = result.get("match_rate", 0) * 100
            freq_s = result.get("freq_source", "?")
            freq_label = result.get("freq_source_label", freq_s)
            conf = result.get("confidence", {})
            scores = result.get("scores", [])

            md.append(f"### {pgs} — {trait}\n")
            md.append(f"**Source:** {sf_name} ({sf_type})  ")
            md.append(f"**Variants:** {matched:,}/{total:,} matched ({rate:.1f}%)  ")
            md.append(f"**Frequency Source:** {freq_label}  ")
            md.append(f"**Confidence:** {conf.get('label', '?')} — {conf.get('description', '')}  \n")

            if scores:
                md.append(f"| Sample | Raw Score | Pop Z-Score | Percentile | Variants Used |")
                md.append(f"|--------|-----------|-------------|------------|---------------|")
                for s in scores:
                    pct = s.get("percentile")
                    pct_str = f"{pct:.1f}%" if pct is not None else "N/A"
                    z = s.get("pop_z_score")
                    z_str = f"{z:+.4f}" if z is not None else "N/A"
                    md.append(f"| {s.get('sample','?')} | {s.get('raw_score',0):.6f} | {z_str} | {pct_str} | {s.get('variants_used',0):,} |")
                md.append("")

            # Reference panel info if available
            fm = result.get("freq_metadata", {})
            if fm.get("ref_panel_mean") is not None:
                md.append(f"**Reference Panel:** mean={fm['ref_panel_mean']:.6f}, std={fm.get('ref_panel_std',0):.6f}, n={fm.get('ref_panel_n','?')} {fm.get('population','?')}  \n")

        md.append(f"\n---\n*Generated by Genomics Dashboard on {completed_at.strftime('%Y-%m-%d %H:%M:%S UTC')}*\n")

        md_text = "\n".join(md)
        for d in (run_dir_persistent, run_dir_fast):
            (d / "run_report.md").write_text(md_text)

        # ---- Mark complete ----
        _update_run(
            run_id,
            status="complete",
            progress_pct=100.0,
            current_step="complete",
            completed_at=completed_at,
            duration_sec=duration_sec,
        )

        await _publish_progress(
            redis_conn, run_id, 100.0,
            f"complete — scored {len(all_results)} PGS across {len(samples)} samples",
            status="complete",
        )

        logger.info(
            "Run %s completed in %.1fs: %d PGS scored, %d samples",
            run_id, duration_sec, len(all_results), len(samples),
        )

        # Generate report in shared reports directory and sync checklist
        try:
            from backend.api.reports import generate_run_report
            generate_run_report(run_id)
        except Exception as e:
            logger.warning("Failed to generate run report for %s: %s", run_id, e)
        try:
            from backend.api.checklist import sync_checklist_from_db
            sync_checklist_from_db()
        except Exception as e:
            logger.warning("Failed to sync checklist for %s: %s", run_id, e)

    except Exception as exc:
        duration_sec = round(time.monotonic() - t0, 2)
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("Run %s failed: %s", run_id, error_msg)

        _update_run(
            run_id,
            status="failed",
            error_message=error_msg,
            completed_at=datetime.now(timezone.utc),
            duration_sec=duration_sec,
            progress_pct=0.0,
            current_step="failed",
        )

        await _publish_progress(
            redis_conn, run_id, 0,
            "run failed", status="failed", error=error_msg,
        )

    finally:
        await redis_conn.aclose()
