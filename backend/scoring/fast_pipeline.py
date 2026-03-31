"""
Fast PGS scoring pipeline using plink2-native operations.

Replaces the variant-by-variant Python scoring in engine.py for gVCF inputs.
The BAM-direct pipeline (pipeline_e_plus.py) is preserved as a fallback.

Typical performance:
  - gVCF to pgen conversion: ~60 seconds per sample (one-time)
  - PGS scoring: ~3-10 seconds per PGS per sample
  - Reference panel stats: ~30 seconds per PGS (cached after first run)
  - Total for 6 samples x 49 PGS: ~25 minutes (vs ~12 hours in Python engine)
"""

import asyncio
import os
import time
import logging
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from .plink2_convert import gvcf_to_pgen, check_pgen_exists
from .plink2_scorer import (
    prepare_plink2_scoring_file,
    score_sample_plink2,
    get_ref_panel_stats,
    compute_percentile,
)
from ..config import PGEN_CACHE_DIR, PLINK2_SCORING_DIR

logger = logging.getLogger(__name__)

# Thread pool for parallel scoring — use more workers for concurrent PGS
from backend.config import CPU_COUNT
executor = ThreadPoolExecutor(max_workers=min(CPU_COUNT, 16))


def _read_scoring_file_variants(scoring_file_path: str) -> list[dict]:
    """Read PGS variant IDs and weights from a plink2-format scoring file.
    
    Returns list of dicts with id, allele, weight for building detail logs.
    """
    variants = []
    try:
        with open(scoring_file_path) as f:
            header = f.readline()  # skip header
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    var_id = parts[0]  # chr:pos format
                    allele = parts[1]
                    weight = parts[2]
                    # Parse chr:pos
                    chr_pos = var_id.split(':')
                    chrom = chr_pos[0] if len(chr_pos) >= 1 else ''
                    pos = chr_pos[1] if len(chr_pos) >= 2 else ''
                    variants.append({
                        'id': var_id,
                        'chr': chrom,
                        'pos': pos,
                        'effect_allele': allele,
                        'weight': float(weight) if weight else 0,
                    })
    except Exception as e:
        logger.warning(f"Could not read scoring file variants: {e}")
    return variants


def _build_variant_detail(
    scoring_file_path: str,
    matched_variants_file: Optional[str],
    pgs_id: str,
    sample_id: str,
    source_path: str,
    source_type: str,
) -> dict:
    """Build a variant detail dict for the detail JSON file.
    
    Cross-references the scoring file with the matched variants list
    from plink2 to determine found/missing status per variant.
    """
    all_variants = _read_scoring_file_variants(scoring_file_path)
    
    # Read matched variant IDs from plink2's list-variants output
    matched_ids = set()
    if matched_variants_file and os.path.exists(matched_variants_file):
        try:
            with open(matched_variants_file) as f:
                for line in f:
                    vid = line.strip()
                    if vid:
                        matched_ids.add(vid)
        except Exception:
            pass
    
    detail_variants = []
    for v in all_variants:
        status = 'found' if v['id'] in matched_ids else 'missing'
        detail_variants.append({
            'rsid': None,  # plink2 uses chr:pos, not rsIDs
            'chr': v['chr'],
            'pos': v['pos'],
            'effect_allele': v['effect_allele'],
            'other_allele': None,
            'weight': v['weight'],
            'status': status,
            'samples': {
                sample_id: {
                    'gt': '.' if status == 'missing' else '?',
                    'dosage': None if status == 'missing' else None,
                }
            },
        })
    
    MAX_DETAIL = 1000
    truncated = len(detail_variants) > MAX_DETAIL
    
    return {
        'pgs_id': pgs_id,
        'source_file_path': source_path,
        'source_file_type': source_type,
        'variants_total': len(all_variants),
        'variants_matched': len(matched_ids),
        'match_rate': len(matched_ids) / len(all_variants) if all_variants else 0,
        'variants_in_log': min(len(detail_variants), MAX_DETAIL),
        'variants_truncated': truncated,
        'variants': detail_variants[:MAX_DETAIL],
    }


async def run_fast_scoring(
    source_files: list[dict],
    pgs_ids: list[str],
    pgs_cache_dir: str,
    progress_callback=None,
) -> list[dict]:
    """
    Main entry point for fast PGS scoring.

    Args:
        source_files: List of dicts with keys:
            - path: path to gVCF file
            - sample_name: sample identifier
            - population: reference population (EUR, EAS, etc.)
            - type: "gvcf" (only gVCF supported in fast pipeline)
        pgs_ids: List of PGS IDs to score
        pgs_cache_dir: Directory containing downloaded PGS scoring files
        progress_callback: async callable(step, total, message, pgs_progress_list)
            for real-time progress updates. pgs_progress_list is a list of
            {id, pgs_id, progress} dicts tracking per-PGS completion.

    Returns:
        List of result dicts, one per (sample, pgs) combination.
        Each result includes a 'variant_detail' key with detail data.
    """
    total_tasks = len(source_files) * len(pgs_ids)
    completed = 0
    results = []

    pgen_dir = str(PGEN_CACHE_DIR)
    scoring_dir = str(PLINK2_SCORING_DIR)

    # Track per-PGS progress for UI
    pgs_progress = {pid: 0 for pid in pgs_ids}
    n_sources = max(len(source_files), 1)

    async def report(msg, pgs_id=None):
        nonlocal completed
        completed += 1
        # Update per-PGS progress
        if pgs_id and pgs_id in pgs_progress:
            pgs_progress[pgs_id] += 1
        if progress_callback:
            pgs_list = [
                {'id': pid, 'pgs_id': pid, 'progress': round(pgs_progress[pid] / n_sources * 100)}
                for pid in pgs_ids
            ]
            await progress_callback(completed, total_tasks, msg, pgs_list)

    # Phase 1: Ensure all gVCFs are converted to pgen format
    for sf in source_files:
        if sf.get('type') != 'gvcf':
            logger.warning(f"Fast pipeline only supports gVCF. Skipping {sf['path']}")
            continue

        sample = sf['sample_name']
        pgen_prefix = os.path.join(pgen_dir, sample, sample)

        if not check_pgen_exists(pgen_prefix):
            if progress_callback:
                await progress_callback(0, total_tasks,
                    f"Converting {sample} gVCF to pgen format...", [])

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                executor,
                gvcf_to_pgen,
                sf['path'],
                pgen_prefix,
            )
            logger.info(f"Converted {sample} gVCF to pgen")
        else:
            logger.info(f"Using cached pgen for {sample}")

        sf['pgen_prefix'] = pgen_prefix

    # Phase 2: Prepare plink2-format scoring files for each PGS
    plink2_scoring_files = {}
    for pgs_id in pgs_ids:
        p2_score_path = os.path.join(scoring_dir, f"{pgs_id}.tsv")

        if not os.path.exists(p2_score_path):
            # Find the harmonized scoring file in cache
            harmonized = find_harmonized_file(pgs_id, pgs_cache_dir)
            if not harmonized:
                logger.error(f"No harmonized scoring file found for {pgs_id}")
                continue

            os.makedirs(scoring_dir, exist_ok=True)
            meta = prepare_plink2_scoring_file(harmonized, p2_score_path)
            logger.info(f"Prepared plink2 scoring file for {pgs_id}: {meta['variant_count']} variants")

        plink2_scoring_files[pgs_id] = p2_score_path

    if progress_callback:
        await progress_callback(0, total_tasks,
            f"Scoring {len(source_files)} samples x {len(plink2_scoring_files)} PGS...", [])

    # Phase 3: Score all (sample, PGS) combinations
    # Each task reports progress on completion for real-time updates
    loop = asyncio.get_event_loop()

    async def _score_one(sf, pgs_id):
        """Score a single (sample, PGS) pair and report progress."""
        sample = sf['sample_name']
        population = sf.get('population', 'EUR')
        out_prefix = os.path.join(pgen_dir, sample, f"score_{pgs_id}")

        t0 = time.monotonic()

        score_result = await loop.run_in_executor(
            executor,
            score_sample_plink2,
            sf['pgen_prefix'],
            plink2_scoring_files[pgs_id],
            out_prefix,
            pgs_id,
        )

        matched_vars_file = score_result.get('matched_variants_file')
        ref_stats = await loop.run_in_executor(
            executor,
            get_ref_panel_stats,
            pgs_id,
            plink2_scoring_files[pgs_id],
            population,
            "GRCh38",
            matched_vars_file,
        )

        percentile_data = compute_percentile(score_result['raw_score'], ref_stats)
        elapsed = time.monotonic() - t0

        # Build variant detail
        variant_detail = _build_variant_detail(
            scoring_file_path=plink2_scoring_files[pgs_id],
            matched_variants_file=matched_vars_file,
            pgs_id=pgs_id,
            sample_id=sample,
            source_path=sf['path'],
            source_type='gvcf',
        )

        # Report progress for this task
        await report(
            f"Scored {sample} x {pgs_id}: {percentile_data['percentile']:.1f}% ({elapsed:.1f}s)",
            pgs_id=pgs_id,
        )

        return sf, pgs_id, score_result, percentile_data, variant_detail

    # Build tasks for all combinations
    tasks = []
    for sf in source_files:
        if 'pgen_prefix' not in sf:
            continue
        for pgs_id in pgs_ids:
            if pgs_id not in plink2_scoring_files:
                continue
            tasks.append(_score_one(sf, pgs_id))

    # Run all scoring tasks concurrently (thread pool handles actual parallelism)
    gather_results = await asyncio.gather(*tasks, return_exceptions=True)

    for item in gather_results:
        if isinstance(item, Exception):
            logger.error(f"Scoring task failed: {item}")
            continue

        sf, pgs_id, score_result, percentile_data, variant_detail = item
        sample = sf['sample_name']

        try:
            result = {
                'sample_name': sample,
                'source_path': sf['path'],
                'source_type': 'gvcf',
                'pgs_id': pgs_id,
                'pipeline': 'plink2_native',
                'variant_detail': variant_detail,
                **score_result,
                **percentile_data,
            }
            results.append(result)
        except Exception as e:
            logger.error(f"Result processing failed for {sample} x {pgs_id}: {e}")

    return results


def find_harmonized_file(pgs_id: str, cache_dir: str) -> Optional[str]:
    """Find the harmonized scoring file for a PGS ID in the cache directory."""
    # Check in subdirectory first (PGS_CACHE_DIR/{pgs_id}/)
    subdir = os.path.join(cache_dir, pgs_id)
    search_dirs = [subdir, cache_dir] if os.path.isdir(subdir) else [cache_dir]

    for d in search_dirs:
        if not os.path.isdir(d):
            continue

        # Try common naming patterns
        patterns = [
            f"{pgs_id}_hmPOS_GRCh38.txt.gz",
            f"{pgs_id}_hmPOS_GRCh38.txt",
            f"{pgs_id}.txt.gz",
            f"{pgs_id}.txt",
        ]
        for pattern in patterns:
            path = os.path.join(d, pattern)
            if os.path.exists(path):
                return path

        # Search for any file starting with the PGS ID
        for fname in os.listdir(d):
            if fname.startswith(pgs_id) and ('hmPOS' in fname or fname.endswith('.txt.gz')):
                return os.path.join(d, fname)

    return None
