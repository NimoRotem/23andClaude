"""
One-time script to precompute reference panel statistics for all cached PGS.
Run this once after migration. Takes ~30 minutes for 49 PGS x 5 populations.
After this, all scoring runs skip the reference panel computation step.

Usage:
    conda activate genomics
    cd "$(dirname "$(dirname "$(readlink -f "$0")")")"
    python scripts/precompute_ref_stats.py
"""

import os
import sys
import time

import os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.scoring.plink2_scorer import (
    get_ref_panel_stats,
    prepare_plink2_scoring_file,
)
from backend.scoring.fast_pipeline import find_harmonized_file
from backend.config import PGS_CACHE_DIR, PLINK2_SCORING_DIR

POPULATIONS = ["EUR", "EAS", "AFR", "SAS", "AMR", "ALL"]

os.makedirs(str(PLINK2_SCORING_DIR), exist_ok=True)

# Find all cached PGS files
pgs_cache = str(PGS_CACHE_DIR)
pgs_files = {}

if os.path.isdir(pgs_cache):
    for item in os.listdir(pgs_cache):
        item_path = os.path.join(pgs_cache, item)
        if item.startswith("PGS") and os.path.isdir(item_path):
            # Subdirectory per PGS ID
            pgs_id = item
            harmonized = find_harmonized_file(pgs_id, pgs_cache)
            if harmonized:
                pgs_files[pgs_id] = harmonized
        elif item.startswith("PGS") and ("hmPOS" in item or item.endswith(".txt.gz") or item.endswith(".txt")):
            # Flat file
            pgs_id = item.split("_")[0].split(".")[0]
            pgs_files[pgs_id] = os.path.join(pgs_cache, item)

print(f"Found {len(pgs_files)} PGS files to process")

scoring_dir = str(PLINK2_SCORING_DIR)
total_start = time.time()

for i, (pgs_id, harmonized_path) in enumerate(sorted(pgs_files.items())):
    print(f"\n[{i+1}/{len(pgs_files)}] Processing {pgs_id}...")

    # Prepare plink2 scoring file
    p2_path = os.path.join(scoring_dir, f"{pgs_id}.tsv")
    if not os.path.exists(p2_path):
        try:
            meta = prepare_plink2_scoring_file(harmonized_path, p2_path)
            print(f"  Prepared scoring file: {meta.get('variant_count', '?')} variants")
        except Exception as e:
            print(f"  FAILED to prepare scoring file: {e}")
            continue
    else:
        print(f"  Scoring file already exists")

    # Compute stats for each population
    for pop in POPULATIONS:
        t0 = time.time()
        try:
            stats = get_ref_panel_stats(pgs_id, p2_path, pop, "GRCh38")
            elapsed = time.time() - t0
            print(f"  {pop}: mean={stats['mean']:.6f}, std={stats['std']:.6f}, "
                  f"n={stats['n_samples']}, {elapsed:.1f}s")
        except Exception as e:
            print(f"  {pop}: FAILED - {e}")

total_elapsed = time.time() - total_start
print(f"\nDone in {total_elapsed/60:.1f} minutes. All reference panel stats cached.")
