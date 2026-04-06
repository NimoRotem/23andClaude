"""
PGS scoring via plink2 native --score command.

Replaces the variant-by-variant Python iteration in engine.py.
Performance: ~5 seconds per 1M-variant PGS vs ~30 minutes in Python.

Scoring runs per-chromosome in parallel for maximum CPU utilisation.
Hom-ref correction fixes accuracy for WGS gVCF inputs where reference
blocks are stripped during pgen conversion.
"""

import shutil
import subprocess
import os
import json
import logging
import re
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import numpy as np

from ..config import (
    CPU_COUNT,
    PLINK2,
    REF_PANEL_STATS_DIR,
    REF_PANEL_1KG_GRCH38,
    REF_PANEL_1KG_GRCH37,
    POP_SAMPLE_DIR,
    SCRATCH_TMP,
)

logger = logging.getLogger(__name__)

AUTOSOMES = [str(i) for i in range(1, 23)]

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


# ---------------------------------------------------------------------------
# Per-chromosome splitting and parallel scoring
# ---------------------------------------------------------------------------

def _split_scoring_by_chr(scoring_file_path: str, tmp_dir: str) -> dict[str, str]:
    """Split a plink2 scoring file into per-chromosome files.

    Returns dict mapping chromosome number (str) → temp scoring file path.
    """
    chr_lines: dict[str, list[str]] = defaultdict(list)

    with open(scoring_file_path) as f:
        header = f.readline()
        for line in f:
            var_id = line.split('\t', 1)[0]  # chr1:12345
            chrom = var_id.split(':')[0].replace('chr', '')
            chr_lines[chrom].append(line)

    chr_files = {}
    for chrom, lines in chr_lines.items():
        if chrom not in AUTOSOMES or not lines:
            continue
        chr_path = os.path.join(tmp_dir, f"chr{chrom}.tsv")
        with open(chr_path, 'w') as f:
            f.write(header)
            f.writelines(lines)
        chr_files[chrom] = chr_path

    return chr_files


def _score_one_chromosome(
    sample_pfile_prefix: str,
    scoring_file_path: str,
    output_prefix: str,
    chrom: str,
    threads: int,
) -> dict:
    """Score a single chromosome with plink2 --score."""
    vzs_flag = "vzs" if os.path.exists(f"{sample_pfile_prefix}.pvar.zst") else ""
    pfile_args = [sample_pfile_prefix]
    if vzs_flag:
        pfile_args.append(vzs_flag)

    cmd = [
        PLINK2,
        "--pfile", *pfile_args,
        "--chr", chrom,
        "--score", scoring_file_path,
        "header-read", "1", "2", "3",
        "cols=+scoresums",
        "no-mean-imputation",
        "list-variants",
        "--threads", str(threads),
        "--allow-extra-chr",
        "--out", output_prefix,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        # Chromosome may have no data — only error if it's a real failure
        stderr = result.stderr or ""
        if "Error" in stderr and "0 variants remaining" not in stderr:
            logger.warning(f"plink2 scoring chr{chrom} issue: {stderr[:300]}")
        return {
            'raw_score': 0.0, 'matched': 0, 'skipped': 0,
            'allele_count': 0, 'missing_count': 0,
            'sample_id': 'unknown', 'vars_file': None,
        }

    # Parse .sscore
    sscore_path = f"{output_prefix}.sscore"
    if not os.path.exists(sscore_path):
        return {
            'raw_score': 0.0, 'matched': 0, 'skipped': 0,
            'allele_count': 0, 'missing_count': 0,
            'sample_id': 'unknown', 'vars_file': None,
        }
    score_data = parse_sscore(sscore_path)

    # Parse match stats from log
    matched = 0
    skipped = 0
    log_text = result.stderr + result.stdout
    log_file = f"{output_prefix}.log"
    if os.path.exists(log_file):
        with open(log_file) as lf:
            log_text += lf.read()
    for line in log_text.split('\n'):
        m = re.search(r'--score:\s*(\d+)\s*variant', line)
        if m:
            matched = int(m.group(1))
        m = re.search(r'(\d+)\s*entr.*skipped due to missing variant', line)
        if m:
            skipped += int(m.group(1))
        m = re.search(r'(\d+)\s*(?:was|were) skipped due to.*mismatching allele', line)
        if m:
            skipped += int(m.group(1))

    vars_file = f"{output_prefix}.sscore.vars"
    return {
        'raw_score': score_data['raw_score'] or 0.0,
        'matched': matched,
        'skipped': skipped,
        'allele_count': score_data.get('allele_count', 0),
        'missing_count': score_data.get('missing_count', 0),
        'sample_id': score_data.get('sample_id', 'unknown'),
        'vars_file': vars_file if os.path.exists(vars_file) else None,
    }


# ---------------------------------------------------------------------------
# Hom-ref correction for WGS gVCF inputs
# ---------------------------------------------------------------------------

def _compute_homref_correction(
    scoring_file_path: str,
    matched_variants_file: str,
    ref_fasta_path: str,
) -> dict:
    """Compute score correction for hom-ref positions missing from the pgen.

    When a WGS gVCF is converted to pgen, hom-ref positions (reference blocks)
    are stripped.  plink2 with no-mean-imputation treats these as dosage 0.
    But when the PGS effect_allele IS the reference allele, the correct dosage
    is 2 (two copies of REF).

    Returns:
        {correction: float, homref_matches: int, homref_corrected: int}
    """
    try:
        import pysam
    except ImportError:
        logger.warning("pysam not available — skipping hom-ref correction")
        return {'correction': 0.0, 'homref_matches': 0, 'homref_corrected': 0}

    # Read matched variant IDs (the ones plink2 found in the pgen)
    matched_ids: set[str] = set()
    if matched_variants_file and os.path.exists(matched_variants_file):
        with open(matched_variants_file) as f:
            matched_ids = {line.strip() for line in f if line.strip()}

    correction = 0.0
    homref_matches = 0
    homref_corrected = 0

    try:
        ref_fasta = pysam.FastaFile(ref_fasta_path)
    except Exception as exc:
        logger.warning(f"Cannot open reference FASTA {ref_fasta_path}: {exc}")
        return {'correction': 0.0, 'homref_matches': 0, 'homref_corrected': 0}

    try:
        with open(scoring_file_path) as f:
            f.readline()  # skip header
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) < 3:
                    continue
                var_id, effect_allele, weight_str = parts[0], parts[1], parts[2]

                if var_id in matched_ids:
                    continue  # Already scored by plink2

                # This position is missing from the pgen → hom-ref in WGS
                homref_matches += 1

                # Only correct SNPs (single-base effect alleles)
                if len(effect_allele) != 1:
                    continue

                chr_pos = var_id.split(':')
                if len(chr_pos) != 2:
                    continue
                chrom = chr_pos[0]
                try:
                    pos = int(chr_pos[1])
                except ValueError:
                    continue

                try:
                    ref_base = ref_fasta.fetch(chrom, pos - 1, pos).upper()
                except Exception:
                    continue

                if effect_allele.upper() == ref_base:
                    # Effect allele is the REF allele → hom-ref = dosage 2
                    # plink2 gave dosage 0 → add 2×weight
                    try:
                        weight = float(weight_str)
                    except ValueError:
                        continue
                    correction += 2.0 * weight
                    homref_corrected += 1
                # else: effect allele is ALT → hom-ref = dosage 0 (correct)
    finally:
        ref_fasta.close()

    logger.info(
        f"Hom-ref correction: {homref_matches} hom-ref positions, "
        f"{homref_corrected} corrected (effect_allele=REF), "
        f"score adjustment={correction:+.6f}"
    )
    return {
        'correction': correction,
        'homref_matches': homref_matches,
        'homref_corrected': homref_corrected,
    }


# ---------------------------------------------------------------------------
# Main scoring entry point — per-chromosome parallel + hom-ref correction
# ---------------------------------------------------------------------------

def score_sample_plink2(
    sample_pfile_prefix: str,
    scoring_file_path: str,
    output_prefix: str,
    pgs_id: str,
    ref_fasta_path: str = None,
) -> dict:
    """
    Score a single sample against a single PGS using plink2 --score.

    Runs each autosome in parallel for maximum CPU utilisation.
    When ref_fasta_path is provided, applies hom-ref correction so that
    WGS gVCF samples get accurate scores even for positions absent from
    the pgen (reference blocks stripped during conversion).

    Args:
        sample_pfile_prefix: Path prefix to sample's .pgen/.pvar/.psam files
        scoring_file_path: Path to plink2-format scoring file
        output_prefix: Output path prefix for results
        pgs_id: PGS identifier for logging
        ref_fasta_path: Path to reference FASTA (enables hom-ref correction)

    Returns:
        dict with raw_score, matched_variants, total_variants, match_rate
    """
    import time as _time
    t0 = _time.monotonic()

    os.makedirs(os.path.dirname(output_prefix), exist_ok=True)

    # Create temp directory for per-chromosome files
    scratch = str(SCRATCH_TMP)
    os.makedirs(scratch, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(prefix=f"pgs_{pgs_id}_", dir=scratch)

    try:
        # 1. Split scoring file by chromosome
        chr_files = _split_scoring_by_chr(scoring_file_path, tmp_dir)
        n_chr = len(chr_files)

        if n_chr == 0:
            logger.warning(f"No autosomal variants in scoring file for {pgs_id}")
            return {
                'raw_score': 0.0, 'sample_id': 'unknown',
                'allele_count': 0, 'missing_count': 0,
                'pgs_id': pgs_id, 'matched_variants': 0,
                'total_variants': 0, 'match_rate': 0.0,
                'matched_variants_file': None,
            }

        # 2. Score each chromosome in parallel
        max_workers = min(n_chr, max(2, CPU_COUNT // 2))
        threads_per_chr = max(1, CPU_COUNT // max_workers)

        logger.info(
            f"Scoring {pgs_id}: {n_chr} chromosomes, "
            f"{max_workers} parallel workers, {threads_per_chr} threads each"
        )

        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for chrom, chr_score_file in chr_files.items():
                chr_out = os.path.join(tmp_dir, f"score_chr{chrom}")
                futures[chrom] = pool.submit(
                    _score_one_chromosome,
                    sample_pfile_prefix, chr_score_file,
                    chr_out, chrom, threads_per_chr,
                )

        # 3. Aggregate per-chromosome results
        total_score = 0.0
        total_matched = 0
        total_skipped = 0
        total_allele_ct = 0
        total_missing_ct = 0
        sample_id = 'unknown'
        combined_vars: list[str] = []

        for chrom in sorted(futures, key=lambda c: int(c)):
            res = futures[chrom].result()
            total_score += res['raw_score']
            total_matched += res['matched']
            total_skipped += res['skipped']
            total_allele_ct += res.get('allele_count', 0)
            total_missing_ct += res.get('missing_count', 0)
            if res.get('sample_id', 'unknown') != 'unknown':
                sample_id = res['sample_id']
            if res.get('vars_file') and os.path.exists(res['vars_file']):
                with open(res['vars_file']) as vf:
                    combined_vars.extend(line.strip() for line in vf if line.strip())

        # Write combined matched-variants file
        combined_vars_file = f"{output_prefix}.sscore.vars"
        with open(combined_vars_file, 'w') as f:
            for vid in combined_vars:
                f.write(vid + '\n')

        total_variants = total_matched + total_skipped
        plink2_match_rate = total_matched / total_variants if total_variants > 0 else 0

        logger.info(
            f"plink2 scoring {pgs_id}: {total_matched}/{total_variants} matched "
            f"({plink2_match_rate:.1%}), raw_score={total_score:.6f}"
        )

        # 4. Hom-ref correction (for WGS gVCF inputs)
        homref_info = None
        if ref_fasta_path and os.path.exists(ref_fasta_path + ".fai"):
            homref_info = _compute_homref_correction(
                scoring_file_path, combined_vars_file, ref_fasta_path,
            )
            total_score += homref_info['correction']
            # Hom-ref positions are now accounted for — update match counts
            total_matched += homref_info['homref_matches']
            total_skipped = max(0, total_skipped - homref_info['homref_matches'])

        elapsed = _time.monotonic() - t0
        final_match_rate = total_matched / total_variants if total_variants > 0 else 0

        logger.info(
            f"Scored {pgs_id} in {elapsed:.1f}s: corrected_score={total_score:.6f}, "
            f"match_rate={final_match_rate:.1%} "
            f"({n_chr} chr parallel)"
        )

        result = {
            'raw_score': total_score,
            'sample_id': sample_id,
            'allele_count': total_allele_ct,
            'missing_count': total_missing_ct,
            'pgs_id': pgs_id,
            'matched_variants': total_matched,
            'total_variants': total_variants,
            'match_rate': final_match_rate,
            'matched_variants_file': combined_vars_file,
        }

        if homref_info:
            result['homref_correction'] = homref_info['correction']
            result['homref_corrected_count'] = homref_info['homref_corrected']
            result['plink2_match_rate'] = plink2_match_rate

        return result

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


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
