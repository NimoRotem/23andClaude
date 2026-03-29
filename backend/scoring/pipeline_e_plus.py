"""Pipeline E+ — BAM-direct PGS scoring using pysam pileup."""

import asyncio
import functools
import json
import logging
import math
import multiprocessing
import os
import time

from pathlib import Path

import pysam

from backend.scoring.shared import (
    FREQ_SOURCE_LABELS,
    _get_1kg_plink2_frequencies,
    _norm_cdf,
    _parse_scoring_file,
    _publish_progress,
    _score_reference_panel,
    _scoring_confidence,
)

logger = logging.getLogger(__name__)

# Palindromic allele pairs — these are ambiguous on strand and must be excluded
_PALINDROMIC_PAIRS = frozenset({("A", "T"), ("T", "A"), ("C", "G"), ("G", "C")})

# Minimum read depth to call a genotype from pileup
_MIN_DEPTH = 10

# Dosage thresholds for effect-allele fraction
_HOM_REF_THRESHOLD = 0.15   # below this → dosage 0
_HOM_ALT_THRESHOLD = 0.85   # above this → dosage 2


# ---------------------------------------------------------------------------
# Synchronous pileup scoring (runs in thread executor)
# ---------------------------------------------------------------------------

def _score_chunk(args: tuple) -> tuple[float, int, int, list[dict]]:
    """Score a chunk of SNPs against a BAM file. Designed to run in a worker process."""
    bam_path, snps, sample_name = args
    raw_score = 0.0
    called = 0
    low_depth = 0
    detail_variants: list[dict] = []

    bam = pysam.AlignmentFile(bam_path, "rb")
    bam_references = set(bam.references)
    try:
        for snp in snps:
            chrom_raw = snp["chr"]
            pos = snp["pos"]
            ea = snp["effect_allele"]
            oa = snp["other_allele"]
            weight = snp["weight"]

            if chrom_raw.startswith("chr"):
                chrom_options = [chrom_raw, chrom_raw[3:]]
            else:
                chrom_options = [chrom_raw, f"chr{chrom_raw}"]

            chrom = None
            for c in chrom_options:
                if c in bam_references:
                    chrom = c
                    break

            if chrom is None:
                detail_variants.append({
                    "rsid": snp.get("rsid", ""),
                    "chr": chrom_raw, "pos": pos,
                    "effect_allele": ea, "other_allele": oa,
                    "weight": weight, "contribution": 0.0,
                    "status": "missing",
                    "samples": {},
                })
                continue

            effect_count = 0
            other_count = 0
            total_reads = 0

            try:
                for pileup_col in bam.pileup(
                    chrom, pos - 1, pos,
                    min_base_quality=20, min_mapping_quality=20, truncate=True,
                ):
                    if pileup_col.reference_pos != pos - 1:
                        continue
                    for read in pileup_col.pileups:
                        if read.is_del or read.is_refskip:
                            continue
                        base = read.alignment.query_sequence[read.query_position].upper()
                        total_reads += 1
                        if base == ea:
                            effect_count += 1
                        elif base == oa:
                            other_count += 1
            except (ValueError, OSError):
                detail_variants.append({
                    "rsid": snp.get("rsid", ""),
                    "chr": chrom_raw, "pos": pos,
                    "effect_allele": ea, "other_allele": oa,
                    "weight": weight, "contribution": 0.0,
                    "status": "missing",
                    "samples": {},
                })
                continue

            if total_reads < _MIN_DEPTH:
                low_depth += 1
                detail_variants.append({
                    "rsid": snp.get("rsid", ""),
                    "chr": chrom_raw, "pos": pos,
                    "effect_allele": ea, "other_allele": oa,
                    "weight": weight, "depth": total_reads,
                    "contribution": 0.0,
                    "status": "missing",
                    "samples": {sample_name: {"gt": f"depth={total_reads}", "dosage": 0}},
                })
                continue

            effect_fraction = effect_count / total_reads if total_reads > 0 else 0.0
            if effect_fraction < _HOM_REF_THRESHOLD:
                dosage = 0.0
            elif effect_fraction > _HOM_ALT_THRESHOLD:
                dosage = 2.0
            else:
                dosage = 1.0

            dw = snp.get("dosage_weights")
            if dw:
                contribution = dw.get(int(dosage), 0)
            else:
                contribution = dosage * weight
            raw_score += contribution
            called += 1

            gt_str = "0/0" if dosage == 0 else ("0/1" if dosage == 1 else "1/1")

            detail_variants.append({
                "rsid": snp.get("rsid", ""),
                "chr": chrom_raw, "pos": pos,
                "effect_allele": ea, "other_allele": oa,
                "weight": weight,
                "depth": total_reads, "effect_reads": effect_count,
                "other_reads": other_count,
                "dosage": dosage, "contribution": round(contribution, 6),
                "status": "found",
                "samples": {sample_name: {"gt": gt_str, "dosage": dosage}},
            })
    finally:
        bam.close()

    return raw_score, called, low_depth, detail_variants


# Number of workers for parallel BAM pileup
_N_WORKERS = min(20, max(1, os.cpu_count() or 1))


def _score_sync(
    bam_path: str,
    snps: list[dict],
    sample_name: str = "Unknown",
) -> tuple[float, int, int, list[dict]]:
    """
    Synchronous BAM pileup scoring with multiprocessing.

    Splits SNPs into chunks and scores them in parallel across multiple
    processes, each with its own pysam file handle. This avoids GIL
    contention and scales with available CPU cores.
    """
    n_snps = len(snps)
    if n_snps == 0:
        return 0.0, 0, 0, []

    n_workers = min(_N_WORKERS, max(1, n_snps // 500))
    chunk_size = math.ceil(n_snps / n_workers)
    chunks = [snps[i:i + chunk_size] for i in range(0, n_snps, chunk_size)]

    logger.info(
        "BAM pileup: %d SNPs across %d workers (%d SNPs/chunk) on %s",
        n_snps, len(chunks), chunk_size, os.path.basename(bam_path),
    )

    args_list = [(bam_path, chunk, sample_name) for chunk in chunks]

    with multiprocessing.Pool(processes=len(chunks)) as pool:
        results = pool.map(_score_chunk, args_list)

    # Merge results from all chunks
    total_score = 0.0
    total_called = 0
    total_low_depth = 0
    all_detail: list[dict] = []
    for score, called, low_depth, details in results:
        total_score += score
        total_called += called
        total_low_depth += low_depth
        all_detail.extend(details)

    return total_score, total_called, total_low_depth, all_detail


# ---------------------------------------------------------------------------
# Main async entry point
# ---------------------------------------------------------------------------

async def score_bam_direct(
    bam_path: str,
    pgs_id: str,
    scoring_file_path: str,
    genome_build: str = "GRCh38",
    ref_population: str = "EUR",
    freq_source: str = "auto",
    redis_conn=None,
    run_id: str = "",
    pgs_index: int = 0,
    total_pgs: int = 1,
) -> dict:
    """
    Score a BAM file directly against a PGS scoring file using pysam pileup.

    This is Pipeline E+ — it bypasses variant calling entirely and reads
    allele counts directly from aligned reads at each PGS variant position.

    Palindromic SNPs (A/T, C/G pairs) are excluded to avoid strand ambiguity.

    Returns a result dict compatible with the engine.py _score_pgs_custom() format.
    """
    t0 = time.monotonic()

    # --- Step 1: Parse scoring file ---
    filepath = Path(scoring_file_path)
    trait, weights = _parse_scoring_file(filepath)
    if not trait:
        trait = pgs_id

    variants_total = len(weights)
    if variants_total == 0:
        return {
            "pgs_id": pgs_id,
            "trait": trait,
            "variants_matched": 0,
            "variants_in_vcf": 0,
            "variants_total": 0,
            "variants_palindromic_excluded": 0,
            "match_rate": 0.0,
            "freq_source": "none",
            "freq_source_label": None,
            "freq_variants_used": 0,
            "freq_metadata": {},
            "confidence": _scoring_confidence("none", 0, 0),
            "source_type": "bam",
            "source_path": bam_path,
            "scores": [],
        }

    # --- Step 2: Filter to SNPs and exclude palindromic ---
    snps: list[dict] = []
    n_palindromic = 0
    n_non_snp = 0

    for w in weights:
        ea = w["effect_allele"].upper()

        # We need "other_allele" to check palindromic.  PGS files may have
        # it under various column names. _parse_scoring_file doesn't extract
        # it, so we look at the raw file data.  For pipeline E+, we need to
        # re-parse to pick up the other allele.
        # However, the weights list only has ea — we'll re-scan the file once
        # to build a (chr, pos) -> other_allele lookup.
        pass  # placeholder; see below

    # Re-parse to get other_allele (not in the standard weights list)
    other_allele_lookup = _extract_other_alleles(filepath)

    for w in weights:
        ea = w["effect_allele"].upper()
        key = (w["chr"], w["pos"])
        oa = other_allele_lookup.get(key, "")

        # Skip non-SNPs (indels, multi-char alleles)
        if len(ea) != 1 or len(oa) != 1:
            n_non_snp += 1
            continue

        # Skip palindromic
        if (ea, oa) in _PALINDROMIC_PAIRS:
            n_palindromic += 1
            continue

        snps.append({
            "chr": w["chr"],
            "pos": w["pos"],
            "rsid": w.get("rsid", ""),
            "effect_allele": ea,
            "other_allele": oa,
            "weight": w["weight"],
            "dosage_weights": w.get("dosage_weights"),  # {0: w0, 1: w1, 2: w2} for GenoBoost
            "freq": w.get("freq"),
        })

    total_snps = len(weights)  # original total in scoring file

    if redis_conn and run_id:
        base_pct = (pgs_index / total_pgs) * 80 + 10
        await _publish_progress(
            redis_conn, run_id, base_pct,
            f"scoring {pgs_id}: pileup on {len(snps)} SNPs from BAM",
        )

    # --- Step 3: Extract sample name from BAM header ---
    sample_name = _get_bam_sample_name(bam_path)

    # --- Step 4: Run synchronous pileup in thread executor ---
    loop = asyncio.get_event_loop()
    raw_score, called, low_depth, detail_variants = await loop.run_in_executor(
        None,
        functools.partial(_score_sync, bam_path, snps, sample_name),
    )

    # --- Step 5: Get allele frequencies ---
    freq_lookup: dict[str, float] = {}
    freq_metadata: dict = {}
    used_freq_source = "none"
    freq_count = 0

    if freq_source == "auto":
        source_order = ["pgs_file", "1kg_plink2", "fallback"]
    elif freq_source == "fallback":
        source_order = ["fallback"]
    else:
        source_order = [freq_source, "fallback"]

    for src in source_order:
        if freq_lookup:
            break
        if src == "pgs_file":
            for w in snps:
                if w.get("freq") is not None and 0 < w["freq"] < 1:
                    freq_lookup[f"{w['chr']}:{w['pos']}"] = w["freq"]
            if len(freq_lookup) >= len(snps) * 0.3:
                used_freq_source = "pgs_file"
                freq_metadata = {"source": "pgs_file", "variants_with_freq": len(freq_lookup)}
            else:
                freq_lookup.clear()
        elif src == "1kg_plink2":
            freq_lookup, freq_metadata = await _get_1kg_plink2_frequencies(
                snps, population=ref_population, genome_build=genome_build,
            )
            if len(freq_lookup) >= len(snps) * 0.2:
                used_freq_source = "1kg_plink2"
            else:
                freq_lookup.clear()
                freq_metadata = {}
        elif src == "fallback":
            used_freq_source = "fallback"
            freq_metadata = {"source": "fallback"}

    freq_count = len(freq_lookup)

    # --- Step 6: Get reference panel distribution ---
    pop_mean = 0.0
    pop_std = 0.0
    has_pop_stats = False

    if used_freq_source == "1kg_plink2" and freq_metadata.get("source") == "1kg_plink2":
        ref_stats = await _score_reference_panel(
            snps, ref_population, genome_build,
        )
        if ref_stats.get("std", 0) > 0:
            pop_mean = ref_stats["mean"]
            pop_std = ref_stats["std"]
            has_pop_stats = True
            freq_metadata["ref_panel_mean"] = round(pop_mean, 6)
            freq_metadata["ref_panel_std"] = round(pop_std, 6)
            freq_metadata["ref_panel_n"] = ref_stats.get("n", 0)

    if not has_pop_stats and freq_lookup:
        # Fallback: theoretical formula
        _pop_var = 0.0
        _pop_mean = 0.0
        for w in snps:
            key = f"{w['chr']}:{w['pos']}"
            freq = freq_lookup.get(key)
            if freq is not None and 0 < freq < 1:
                _pop_mean += 2.0 * freq * w["weight"]
                _pop_var += 2.0 * freq * (1.0 - freq) * (w["weight"] ** 2)
        pop_mean = _pop_mean
        pop_std = math.sqrt(_pop_var) if _pop_var > 0 else 0.0
        has_pop_stats = pop_std > 0

    # --- Step 7: Compute Z-score and percentile ---
    if has_pop_stats and pop_std > 0:
        pop_z = (raw_score - pop_mean) / pop_std
        percentile = round(_norm_cdf(pop_z) * 100, 1)
    else:
        pop_z = None
        percentile = None

    confidence = _scoring_confidence(used_freq_source, freq_count, total_snps)

    match_rate = called / total_snps if total_snps > 0 else 0.0

    # --- Step 8: Publish progress ---
    if redis_conn and run_id:
        done_pct = ((pgs_index + 1) / total_pgs) * 80 + 10
        await _publish_progress(
            redis_conn, run_id, done_pct,
            f"scored {pgs_id}: {called}/{total_snps} variants called from BAM ({match_rate:.1%})",
        )

    duration = round(time.monotonic() - t0, 2)

    # --- Build result ---
    sample_entry = {
        "sample": sample_name,
        "raw_score": round(raw_score, 6),
        "z_score": None,  # no within-family for single BAM
        "pop_z_score": round(pop_z, 4) if pop_z is not None else None,
        "percentile": percentile,
        "confidence_level": confidence["level"],
        "variants_used": called,
        "variants_low_depth": low_depth,
        "rank": 1,
    }

    result = {
        "pgs_id": pgs_id,
        "trait": trait,
        "variants_matched": called,
        "variants_in_vcf": called,  # for BAM, "in_vcf" means "called from BAM"
        "variants_total": total_snps,
        "variants_palindromic_excluded": n_palindromic,
        "match_rate": round(match_rate, 4),
        "freq_source": used_freq_source,
        "freq_source_label": FREQ_SOURCE_LABELS.get(used_freq_source),
        "freq_variants_used": freq_count,
        "freq_metadata": freq_metadata,
        "confidence": confidence,
        "source_type": "bam",
        "source_path": bam_path,
        "duration_sec": duration,
        "scores": [sample_entry],
        "detail_variants": detail_variants,
    }

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_other_alleles(filepath: Path) -> dict[tuple[str, int], str]:
    """
    Re-parse a PGS scoring file to extract the other/reference allele.

    Returns {(chr, pos): other_allele} dict.
    """
    import gzip

    lookup: dict[tuple[str, int], str] = {}
    header_cols: list[str] = []

    open_fn = gzip.open if str(filepath).endswith(".gz") else open

    with open_fn(filepath, "rt", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if not header_cols:
                header_cols = line.lower().split("\t")
                if len(header_cols) < 2:
                    header_cols = line.lower().split()
                continue

            parts = line.split("\t")
            if len(parts) < 3:
                continue

            row = dict(zip(header_cols, parts))

            chrom = row.get("hm_chr") or row.get("chr_name") or row.get("chr") or ""
            chrom = chrom.replace("chr", "")

            pos_str = row.get("hm_pos") or row.get("chr_position") or row.get("pos") or ""
            try:
                pos = int(pos_str)
            except (ValueError, TypeError):
                continue

            # Other allele: try several column names (headers are lowercased)
            other_allele = (
                row.get("other_allele")
                or row.get("hm_inferotherallele")  # harmonized files (lowercased)
                or row.get("reference_allele")
                or row.get("allele2")
                or row.get("a2")
                or row.get("ref")
                or ""
            ).upper()

            if other_allele:
                lookup[(chrom, pos)] = other_allele

    return lookup


def _get_bam_sample_name(bam_path: str) -> str:
    """Extract sample name from BAM @RG header. Falls back to filename."""
    try:
        bam = pysam.AlignmentFile(bam_path, "rb")
        try:
            header = bam.header
            rg = header.get("RG", [])
            if rg and isinstance(rg, list) and len(rg) > 0:
                sm = rg[0].get("SM", "")
                if sm:
                    return sm
        finally:
            bam.close()
    except Exception:
        pass

    # Fallback: use the BAM filename stem
    return Path(bam_path).stem
