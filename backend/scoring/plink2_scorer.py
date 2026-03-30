"""
PGS scoring via plink2 native --score command.

Replaces the variant-by-variant Python iteration in engine.py.
Performance: ~5 seconds per 1M-variant PGS vs ~30 minutes in Python.
"""

import subprocess
import os
import json
import logging
import re
import tempfile
from typing import Optional

import numpy as np

from ..config import (
    PLINK2,
    REF_PANEL_STATS_DIR,
    REF_PANEL_1KG_GRCH38,
    REF_PANEL_1KG_GRCH37,
    POP_SAMPLE_DIR,
)

logger = logging.getLogger(__name__)

REF_PANEL_PREFIX = {
    "GRCh38": REF_PANEL_1KG_GRCH38,
    "GRCh37": REF_PANEL_1KG_GRCH37,
}


def prepare_plink2_scoring_file(pgs_harmonized_path: str, output_path: str) -> dict:
    """
    Convert a PGS Catalog harmonized scoring file to plink2 --score format.

    PGS Catalog format: chr_name, chr_position, effect_allele, other_allele, effect_weight, ...
    plink2 --score format: variant_id, allele, weight

    Returns dict with metadata (variant count, build, trait, etc.)
    """
    import gzip

    metadata = {}
    data_lines = []
    col_names = None

    opener = gzip.open if pgs_harmonized_path.endswith('.gz') else open
    with opener(pgs_harmonized_path, 'rt') as f:
        for line in f:
            if line.startswith('#'):
                if '=' in line:
                    key, _, val = line.lstrip('#').strip().partition('=')
                    metadata[key.strip()] = val.strip()
                continue

            parts = line.strip().split('\t')
            if col_names is None:
                col_names = parts
                continue

            # Find column indices
            try:
                # Prefer harmonized GRCh38 columns over original coordinates
                chr_idx = col_names.index('hm_chr') if 'hm_chr' in col_names else col_names.index('chr_name')
                pos_idx = col_names.index('hm_pos') if 'hm_pos' in col_names else col_names.index('chr_position')
                ea_idx = col_names.index('effect_allele')
                weight_idx = col_names.index('effect_weight')
            except ValueError:
                continue

            chrom = parts[chr_idx] if chr_idx < len(parts) else ''
            pos = parts[pos_idx] if pos_idx < len(parts) else ''
            ea = parts[ea_idx] if ea_idx < len(parts) else ''
            weight = parts[weight_idx] if weight_idx < len(parts) else ''

            if not chrom or not pos or chrom == 'NA' or pos == 'NA':
                continue

            # Normalize chromosome naming
            if not chrom.startswith('chr'):
                chrom = f"chr{chrom}"

            var_id = f"{chrom}:{pos}"
            data_lines.append(f"{var_id}\t{ea}\t{weight}")

    # Deduplicate: keep first entry for each (ID, allele) pair
    seen = set()
    unique_lines = []
    for line in data_lines:
        parts = line.split("\t")
        key = (parts[0], parts[1])  # (variant_id, allele)
        if key not in seen:
            seen.add(key)
            unique_lines.append(line)

    # Write plink2-compatible scoring file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write("ID\tA1\tWEIGHT\n")
        for line in unique_lines:
            f.write(line + "\n")
    data_lines = unique_lines

    metadata['variant_count'] = len(data_lines)
    return metadata


def score_sample_plink2(
    sample_pfile_prefix: str,
    scoring_file_path: str,
    output_prefix: str,
    pgs_id: str,
) -> dict:
    """
    Score a single sample against a single PGS using plink2 --score.

    Runs in ~3-10 seconds for a 1M-variant PGS.

    Args:
        sample_pfile_prefix: Path prefix to sample's .pgen/.pvar/.psam files
        scoring_file_path: Path to plink2-format scoring file
        output_prefix: Output path prefix for results
        pgs_id: PGS identifier for logging

    Returns:
        dict with raw_score, matched_variants, total_variants
    """
    os.makedirs(os.path.dirname(output_prefix), exist_ok=True)

    # Detect if pvar is zstd-compressed
    vzs_flag = "vzs" if os.path.exists(f"{sample_pfile_prefix}.pvar.zst") else ""
    pfile_args = [sample_pfile_prefix]
    if vzs_flag:
        pfile_args.append(vzs_flag)

    # Use multiple threads for plink2 scoring
    from backend.config import CPU_COUNT
    threads = min(CPU_COUNT, 16)  # Cap at 16 to leave headroom

    cmd = [
        PLINK2,
        "--pfile", *pfile_args,
        "--score", scoring_file_path,
        "header-read",
        "1",    # Variant ID column
        "2",    # Allele column
        "3",    # Score column
        "cols=+scoresums",
        "no-mean-imputation",
        "list-variants",
        "--threads", str(threads),
        "--allow-extra-chr",
        "--out", output_prefix,
    ]

    logger.info(f"Scoring {pgs_id}: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"plink2 scoring failed for {pgs_id}: {result.stderr}")
        raise RuntimeError(f"plink2 scoring failed: {result.stderr}")

    # Parse the .sscore output file
    sscore_path = f"{output_prefix}.sscore"
    score_data = parse_sscore(sscore_path)

    # Parse match stats from log
    matched = 0
    total = 0
    skipped = 0
    log_text = result.stderr + result.stdout
    # Also read the log file if available
    log_file = f"{output_prefix}.log"
    if os.path.exists(log_file):
        with open(log_file) as lf:
            log_text += lf.read()
    for line in log_text.split('\n'):
        # "--score: 1722 variants processed."
        m = re.search(r'--score:\s*(\d+)\s*variant', line)
        if m:
            matched = int(m.group(1))
        # "1567 entries in ... were skipped due to missing variant IDs"
        m = re.search(r'(\d+)\s*entr.*skipped due to missing variant', line)
        if m:
            skipped += int(m.group(1))
        # "1 was skipped due to a mismatching allele code"
        m = re.search(r'(\d+)\s*(?:was|were) skipped due to.*mismatching allele', line)
        if m:
            skipped += int(m.group(1))
    total = matched + skipped

    score_data['pgs_id'] = pgs_id
    score_data['matched_variants'] = matched
    score_data['total_variants'] = total
    score_data['match_rate'] = matched / total if total > 0 else 0
    # Path to file listing matched variant IDs (for consistent ref panel scoring)
    vars_file = f"{output_prefix}.sscore.vars"
    score_data['matched_variants_file'] = vars_file if os.path.exists(vars_file) else None

    return score_data


def parse_sscore(sscore_path: str) -> dict:
    """Parse plink2 .sscore output file."""
    if not os.path.exists(sscore_path):
        raise FileNotFoundError(f"Score file not found: {sscore_path}")

    with open(sscore_path) as f:
        header = f.readline().strip().split('\t')
        values = f.readline().strip().split('\t')

    data = dict(zip(header, values))

    # The score column name varies based on scoring file column name.
    # plink2 names it {colname}_AVG and {colname}_SUM.
    raw_score = None
    for key in data:
        if ('SCORE' in key.upper() or 'WEIGHT' in key.upper()) and 'SUM' in key.upper():
            raw_score = float(data[key])
            break
    if raw_score is None:
        for key in data:
            if 'SCORE' in key.upper() or 'WEIGHT' in key.upper():
                if key.upper() not in ('#IID', 'IID', 'ALLELE_CT', 'MISSING_CT', 'NAMED_ALLELE_DOSAGE_SUM'):
                    raw_score = float(data[key])

    return {
        'raw_score': raw_score,
        'sample_id': data.get('#IID', data.get('IID', 'unknown')),
        'allele_count': int(data.get('ALLELE_CT', 0)),
        'missing_count': int(data.get('MISSING_CT', 0)),
    }


def get_ref_panel_stats(
    pgs_id: str,
    scoring_file_path: str,
    population: str,
    genome_build: str = "GRCh38",
    extract_variants_file: str = None,
) -> dict:
    """
    Get precomputed reference panel statistics (mean, std) for a PGS.

    If not cached, compute them by scoring the 1000G reference panel
    and save for reuse.
    """
    # Include variant count in cache key when filtering to sample's variant set
    if extract_variants_file and os.path.exists(extract_variants_file):
        with open(extract_variants_file) as ef:
            n_extract = sum(1 for _ in ef)
        cache_key = f"{pgs_id}_{population}_{genome_build}_n{n_extract}"
    else:
        cache_key = f"{pgs_id}_{population}_{genome_build}"
    cache_dir = str(REF_PANEL_STATS_DIR)
    cache_path = os.path.join(cache_dir, f"{cache_key}.json")

    # Check cache
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            stats = json.load(f)
        logger.info(f"Loaded cached ref panel stats for {cache_key}")
        return stats

    # Compute: score the entire 1000G panel
    logger.info(f"Computing ref panel stats for {cache_key} (will cache for future use)")

    ref_prefix = REF_PANEL_PREFIX.get(genome_build)
    if not ref_prefix:
        raise ValueError(f"Unknown genome build: {genome_build}")

    pop_sample_dir = str(POP_SAMPLE_DIR)
    pop_keep_file = os.path.join(pop_sample_dir, f"{population}.txt") if population != "ALL" else None

    with tempfile.TemporaryDirectory() as tmpdir:
        out_prefix = os.path.join(tmpdir, "ref_score")

        cmd = [
            PLINK2,
            "--pfile", ref_prefix, "vzs",
            "--set-all-var-ids", "chr@:#",
            "--rm-dup", "force-first",
            "--score", scoring_file_path,
            "header-read", "1", "2", "3",
            "cols=+scoresums",
            "no-mean-imputation",
            "--allow-extra-chr",
            "--out", out_prefix,
        ]

        if pop_keep_file and os.path.exists(pop_keep_file):
            cmd.extend(["--keep", pop_keep_file])

        if extract_variants_file and os.path.exists(extract_variants_file):
            cmd.extend(["--extract", extract_variants_file])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Ref panel scoring failed: {result.stderr}")

        # Parse all scores from .sscore
        sscore_path = f"{out_prefix}.sscore"
        scores = []
        with open(sscore_path) as f:
            header = f.readline().strip().split('\t')
            score_col = None
            for i, h in enumerate(header):
                if ('SCORE' in h.upper() or 'WEIGHT' in h.upper()) and h.upper() not in ('NAMED_ALLELE_DOSAGE_SUM',):
                    score_col = i
                    if 'SUM' in h.upper():
                        break

            for line in f:
                vals = line.strip().split('\t')
                if score_col is not None:
                    try:
                        scores.append(float(vals[score_col]))
                    except (ValueError, IndexError):
                        continue

    scores_arr = np.array(scores)

    stats = {
        "pgs_id": pgs_id,
        "population": population,
        "genome_build": genome_build,
        "mean": float(np.mean(scores_arr)),
        "std": float(np.std(scores_arr)),
        "median": float(np.median(scores_arr)),
        "n_samples": len(scores),
        "min": float(np.min(scores_arr)),
        "max": float(np.max(scores_arr)),
    }

    # Cache to disk
    os.makedirs(cache_dir, exist_ok=True)
    with open(cache_path, 'w') as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Cached ref panel stats: {cache_key} (n={len(scores)}, mean={stats['mean']:.6f}, std={stats['std']:.6f})")
    return stats


def compute_percentile(raw_score: float, ref_stats: dict) -> dict:
    """
    Compute Z-score and percentile from raw score and reference stats.
    """
    from scipy import stats as scipy_stats

    mean = ref_stats['mean']
    std = ref_stats['std']

    if std == 0 or std < 1e-12:
        return {
            'z_score': 0.0,
            'percentile': 50.0,
            'raw_score': raw_score,
            'ref_mean': mean,
            'ref_std': std,
        }

    z_score = (raw_score - mean) / std
    percentile = float(scipy_stats.norm.cdf(z_score) * 100)

    return {
        'z_score': round(z_score, 4),
        'percentile': round(percentile, 2),
        'raw_score': raw_score,
        'ref_mean': mean,
        'ref_std': std,
        'ref_n_samples': ref_stats['n_samples'],
        'ref_population': ref_stats['population'],
    }
