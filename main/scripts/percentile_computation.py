#!/usr/bin/env python3
"""Precompute reference score distributions for ancestry-matched percentiles.

For each trait that has been scored with PRS-CSx, score all 1KG reference samples
and build per-superpopulation distributions. These are stored as numpy arrays
for fast percentile lookups at query time.

Usage:
    cd <repo-root>
    python scripts/percentile_computation.py --trait CAD --pops EUR,EAS
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.config import DATA_DIR

ANCESTRY_DIR = Path(DATA_DIR) / "ancestry"
REFERENCE_DIR = Path(DATA_DIR) / "reference" / "1kg"
REF_SCORES_DIR = ANCESTRY_DIR / "reference_score_distributions"
PANEL_FILE = REFERENCE_DIR / "integrated_call_samples_v3.20130502.ALL.panel"

SUPERPOPS = ["EUR", "EAS", "AFR", "SAS", "AMR"]


def build_reference_distributions(trait: str, out_dir: str, pops: list):
    """Score 1KG reference panel and build per-population distributions."""

    # Load 1KG panel for population labels
    panel = pd.read_csv(PANEL_FILE, sep="\t")
    sample_to_pop = dict(zip(panel["sample"], panel["super_pop"]))

    for pop in pops:
        sscore_file = Path(out_dir) / f"{trait}_{pop}_scores.sscore"
        if not sscore_file.exists():
            print(f"  {pop}: sscore file not found, skipping")
            continue

        df = pd.read_csv(sscore_file, sep="\t")
        id_col = "IID" if "IID" in df.columns else df.columns[1]
        score_cols = [c for c in df.columns if "SCORE" in c.upper()]
        if not score_cols:
            print(f"  {pop}: no score column found")
            continue
        score_col = score_cols[0]

        # Group scores by superpopulation
        pop_scores = {}
        for _, row in df.iterrows():
            sample_id = str(row[id_col])
            if sample_id in sample_to_pop:
                spop = sample_to_pop[sample_id]
                pop_scores.setdefault(spop, []).append(float(row[score_col]))

        # Save distributions
        for spop, scores in pop_scores.items():
            out_dir_pop = REF_SCORES_DIR / spop
            out_dir_pop.mkdir(parents=True, exist_ok=True)
            arr = np.array(scores)
            np.save(str(out_dir_pop / f"{trait}.npy"), arr)
            print(f"  {spop}: {len(scores)} samples, mean={arr.mean():.6f}, std={arr.std():.6f}")

    # Also build an "ALL" distribution
    all_scores = []
    for pop in pops:
        sscore_file = Path(out_dir) / f"{trait}_{pop}_scores.sscore"
        if sscore_file.exists():
            df = pd.read_csv(sscore_file, sep="\t")
            score_cols = [c for c in df.columns if "SCORE" in c.upper()]
            if score_cols:
                all_scores.extend(df[score_cols[0]].dropna().tolist())

    if all_scores:
        out_dir_all = REF_SCORES_DIR / "ALL"
        out_dir_all.mkdir(parents=True, exist_ok=True)
        arr = np.array(all_scores)
        np.save(str(out_dir_all / f"{trait}.npy"), arr)
        print(f"  ALL: {len(all_scores)} samples")


def compute_percentile(score: float, primary_ancestry: str, trait: str) -> dict:
    """Compute percentile for a given score against pre-computed distributions."""
    # Try ancestry-matched distribution first
    for pop in [primary_ancestry, "EUR", "ALL"]:
        ref_file = REF_SCORES_DIR / pop / f"{trait}.npy"
        if ref_file.exists():
            ref = np.load(str(ref_file))
            pct = (np.sum(ref < score) / len(ref)) * 100
            return {
                "percentile": round(pct, 1),
                "reference_population": pop if pop == primary_ancestry else f"{pop} (fallback)",
                "reference_n": len(ref),
            }

    return {"percentile": None, "reference_population": None, "reference_n": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build reference score distributions")
    parser.add_argument("--trait", required=True)
    parser.add_argument("--out-dir", required=True, help="PRS-CSx output directory")
    parser.add_argument("--pops", required=True, help="Comma-separated populations")
    args = parser.parse_args()

    print(f"\nBuilding reference distributions for {args.trait}")
    print(f"Populations: {args.pops}")

    REF_SCORES_DIR.mkdir(parents=True, exist_ok=True)
    build_reference_distributions(args.trait, args.out_dir, args.pops.split(","))
    print("\nDone!")
