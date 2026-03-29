"""Shared scoring utilities — extracted from engine.py for reuse across pipelines."""

import asyncio
import gzip
import json
import logging
import math
import os
import time
import uuid

from pathlib import Path

from backend.config import (
    BCFTOOLS,
    PLINK2,
    REF_PANEL_1KG_GRCH38,
    REF_PANEL_1KG_GRCH37,
    SCRATCH_TMP,
    REDIS_URL,
)

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Frequency source labels
# ---------------------------------------------------------------------------

FREQ_SOURCE_LABELS = {
    "pgs_file": "PGS File Frequencies",
    "1kg_plink2": "1000 Genomes (plink2, 3202 samples)",
    "vcf_af": "VCF Allele Frequencies",
    "fallback": "Estimated (no reference)",
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
# Normal CDF
# ---------------------------------------------------------------------------

def _norm_cdf(x: float) -> float:
    """Normal CDF approximation (Abramowitz & Stegun via math.erf)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


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

            # Extract rsID and other allele
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
                "dosage_weights": dosage_weights,
                "freq": freq,
            })

    return trait_reported, weights


# ---------------------------------------------------------------------------
# Scoring confidence
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# 1000 Genomes plink2 allele frequencies
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Reference panel scoring (empirical distribution from 1000G)
# ---------------------------------------------------------------------------

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
        # Read the pvar to map chr:pos -> pvar ID
        pvar_ids = {}  # chr:pos -> (id, ref, alt)
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
