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
        progress_callback: async callable(step, total, message) for progress updates

    Returns:
        List of result dicts, one per (sample, pgs) combination
    """
    total_tasks = len(source_files) * len(pgs_ids)
    completed = 0
    results = []

    pgen_dir = str(PGEN_CACHE_DIR)
    scoring_dir = str(PLINK2_SCORING_DIR)

    async def report(msg):
        nonlocal completed
        completed += 1
        if progress_callback:
            await progress_callback(completed, total_tasks, msg)

    # Phase 1: Ensure all gVCFs are converted to pgen format
    for sf in source_files:
        if sf.get('type') != 'gvcf':
            logger.warning(f"Fast pipeline only supports gVCF. Skipping {sf['path']}")
            continue

        sample = sf['sample_name']
        pgen_prefix = os.path.join(pgen_dir, sample, sample)

        if not check_pgen_exists(pgen_prefix):
            if progress_callback:
                await progress_callback(0, total_tasks, f"Converting {sample} gVCF to pgen format...")

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

    # Phase 3: Score all (sample, PGS) combinations — in parallel
    loop = asyncio.get_event_loop()

    async def _score_one(sf, pgs_id):
        """Score a single (sample, PGS) pair."""
        sample = sf['sample_name']
        population = sf.get('population', 'EUR')
        out_prefix = os.path.join(pgen_dir, sample, f"score_{pgs_id}")

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
        return sf, pgs_id, score_result, percentile_data

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

        sf, pgs_id, score_result, percentile_data = item
        sample = sf['sample_name']

        try:
            result = {
                'sample_name': sample,
                'source_path': sf['path'],
                'source_type': 'gvcf',
                'pgs_id': pgs_id,
                'pipeline': 'plink2_native',
                **score_result,
                **percentile_data,
            }
            results.append(result)
            logger.info(f"Scored {sample} x {pgs_id}: percentile={percentile_data['percentile']:.1f}%")
        except Exception as e:
            logger.error(f"Result processing failed for {sample} x {pgs_id}: {e}")

    # Report progress after all tasks complete
    for i, r in enumerate(results):
        completed += 1
        if progress_callback:
            await progress_callback(completed, total_tasks,
                f"Scored {r['sample_name']} x {r['pgs_id']}: percentile={r.get('percentile', 0):.1f}%")

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
