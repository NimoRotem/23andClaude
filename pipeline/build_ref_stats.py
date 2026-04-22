"""Build per-population reference distributions for PGS scores.

Usage:
    from pipeline.build_ref_stats import build_ref_stats
    stats = build_ref_stats("PGS000005", "EUR")

For each PGS × population, runs plink2 --score against the 1000G reference
panel with a population-specific keep-file, then computes distribution stats.
"""
import json
import logging
import os
import random
import statistics
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np

from .config import (
    PLINK2, REF_PANEL, REF_PANEL_PSAM, REF_STATS_DIR,
    PLINK_REF_THREADS, PLINK_MEMORY_MB,
    POPULATIONS, MIX_SEED, MIX_PER_POP_N,
    PGS_CACHE, ref_stats_path, ref_scores_npy_path,
)
from . import db

logger = logging.getLogger("pgs-pipeline")


@dataclass
class RefStats:
    """Result of building reference stats for a PGS × population."""
    pgs_id: str
    population: str
    genome_build: str = "GRCh38"
    success: bool = False
    n_samples: int = 0
    mean: float = 0.0
    std: float = 0.0
    quantiles: Dict = field(default_factory=dict)
    matched_variants: int = 0
    total_variants: int = 0
    stats_file: Optional[str] = None
    scores_npy_file: Optional[str] = None
    error: Optional[str] = None


def build_ref_stats(pgs_id: str, population: str,
                    genome_build: str = "GRCh38",
                    force: bool = False) -> RefStats:
    """Build reference distribution stats for a PGS × population. Idempotent.

    Steps:
      1. Load scoring_refpanel.tsv from ingested PGS cache
      2. Build population keep-file
      3. Run plink2 --score --keep against 1000G ref panel
      4. Parse .sscore, compute stats
      5. Save JSON + .npy to /data/ref_stats/{PGS_ID}/
      6. Insert into SQLite
    """
    result = RefStats(pgs_id=pgs_id, population=population, genome_build=genome_build)

    # MID is a placeholder — no panel available
    if population == "MID":
        result.error = "No reference panel available for MID (placeholder)"
        logger.info(f"{pgs_id}/{population}: skipped (no panel)")
        return result

    # Check if already built (idempotent)
    json_path = ref_stats_path(pgs_id, population, genome_build)
    npy_path = ref_scores_npy_path(pgs_id, population, genome_build)
    if not force and os.path.exists(json_path) and os.path.exists(npy_path):
        logger.info(f"{pgs_id}/{population}: already built, skipping")
        try:
            with open(json_path) as f:
                existing = json.load(f)
            result.success = True
            result.n_samples = existing.get("n_samples", 0)
            result.mean = existing.get("mean", 0)
            result.std = existing.get("std", 0)
            result.stats_file = json_path
            result.scores_npy_file = npy_path
            return result
        except (json.JSONDecodeError, KeyError):
            pass

    # Step 1: Load scoring file
    refpanel_scoring = os.path.join(PGS_CACHE, pgs_id, "scoring_refpanel.tsv")
    if not os.path.exists(refpanel_scoring):
        result.error = f"scoring_refpanel.tsv not found — run ingest_pgs({pgs_id}) first"
        logger.error(f"{pgs_id}/{population}: {result.error}")
        return result

    # Count total variants from scoring file
    with open(refpanel_scoring) as f:
        total_lines = sum(1 for _ in f) - 1  # subtract header
    result.total_variants = total_lines

    with tempfile.TemporaryDirectory(prefix=f"refstats_{pgs_id}_{population}_") as tmpdir:
        # Step 2: Build keep-file
        keep_file = _build_keep_file(population, tmpdir)
        if keep_file is None and population != "ALL":
            result.error = f"Could not build keep-file for {population}"
            logger.error(f"{pgs_id}/{population}: {result.error}")
            return result

        # Step 3: Run plink2 --score
        out_prefix = os.path.join(tmpdir, "ref_score")
        cmd = [
            PLINK2,
            "--pfile", REF_PANEL, "vzs",
            "--score", refpanel_scoring, "header-read", "1", "2", "3",
            "cols=+scoresums",
            "no-mean-imputation",
            "list-variants",
            "--threads", str(PLINK_REF_THREADS),
            "--memory", str(PLINK_MEMORY_MB),
            "--out", out_prefix,
        ]
        if keep_file:
            cmd.extend(["--keep", keep_file])

        logger.info(f"{pgs_id}/{population}: running plink2 --score")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            result.error = f"plink2 --score failed: {proc.stderr[:300]}"
            logger.error(f"{pgs_id}/{population}: {result.error}")
            return result

        # Count matched variants
        vars_file = out_prefix + ".sscore.vars"
        if os.path.exists(vars_file):
            with open(vars_file) as f:
                result.matched_variants = sum(1 for _ in f)

        # Step 4: Parse .sscore
        sscore_path = out_prefix + ".sscore"
        if not os.path.exists(sscore_path):
            result.error = "plink2 produced no .sscore output"
            return result

        avgs, sums = _parse_sscore_multi(sscore_path)

        if not avgs:
            result.error = "No samples scored"
            return result

        # Compute stats over AVG scores (consistent with how runners.py uses raw_score)
        scores = np.array(avgs)
        result.n_samples = len(scores)
        result.mean = float(np.mean(scores))
        result.std = float(np.std(scores, ddof=1)) if len(scores) > 1 else 0.0

        quantile_pcts = [1, 5, 10, 25, 50, 75, 90, 95, 99]
        result.quantiles = {
            str(q): float(np.percentile(scores, q)) for q in quantile_pcts
        }

        # Also compute SUM stats for scale reconciliation
        sum_scores = np.array(sums) if sums else scores
        sum_mean = float(np.mean(sum_scores))
        sum_std = float(np.std(sum_scores, ddof=1)) if len(sum_scores) > 1 else 0.0

        # Step 5: Save outputs
        os.makedirs(os.path.dirname(json_path), exist_ok=True)

        stats_dict = {
            "pgs_id": pgs_id,
            "population": population,
            "genome_build": genome_build,
            "mean": result.mean,
            "std": result.std,
            "median": float(np.median(scores)),
            "n_samples": result.n_samples,
            "min": float(np.min(scores)),
            "max": float(np.max(scores)),
            "quantiles": result.quantiles,
            "matched_variants": result.matched_variants,
            "total_variants": result.total_variants,
            "score_sum_mean": sum_mean,
            "score_sum_std": sum_std,
        }

        with open(json_path, 'w') as f:
            json.dump(stats_dict, f, indent=2)
        np.save(npy_path, scores)

        result.stats_file = json_path
        result.scores_npy_file = npy_path

        # Step 6: Insert into SQLite
        try:
            db.upsert_ref_stats(
                pgs_id=pgs_id, population=population, genome_build=genome_build,
                n_samples=result.n_samples, mean=result.mean, std=result.std,
                quantiles=result.quantiles,
                match_rate_mean=result.matched_variants / max(result.total_variants, 1),
                stats_file_path=json_path, scores_npy_path=npy_path,
            )
        except Exception as e:
            logger.warning(f"{pgs_id}/{population}: DB insert failed: {e}")

        result.success = True
        logger.info(f"{pgs_id}/{population}: stats built — n={result.n_samples}, "
                    f"mean={result.mean:.6f}, std={result.std:.6f}, "
                    f"matched={result.matched_variants}/{result.total_variants}")
        return result


def _build_keep_file(population: str, tmpdir: str) -> Optional[str]:
    """Build a plink2 --keep file for the given population.

    For MIX: subsample MIX_PER_POP_N from each of the 5 real populations.
    For ALL: return None (no filtering).
    """
    if population == "ALL":
        return None

    pop_config = POPULATIONS.get(population)
    if not pop_config:
        return None

    keep_path = os.path.join(tmpdir, f"keep_{population}.txt")

    if population == "MIX":
        # Balanced subsample from all 5 real populations
        rng = random.Random(MIX_SEED)
        all_samples = []

        for pop_code in ["EUR", "EAS", "AFR", "SAS", "AMR"]:
            pop_file = POPULATIONS[pop_code]["sample_file"]
            if not pop_file or not os.path.exists(pop_file):
                logger.warning(f"MIX: sample file missing for {pop_code}")
                continue

            samples = []
            with open(pop_file) as f:
                header = f.readline()  # skip header
                for line in f:
                    iid = line.strip().split('\t')[0] if '\t' in line else line.strip()
                    if iid:
                        samples.append(iid)

            # Subsample (or take all if fewer than MIX_PER_POP_N)
            n_take = min(MIX_PER_POP_N, len(samples))
            selected = rng.sample(samples, n_take)
            all_samples.extend(selected)

        with open(keep_path, 'w') as f:
            f.write("#IID\n")
            for iid in all_samples:
                f.write(f"{iid}\n")

        logger.info(f"MIX keep-file: {len(all_samples)} samples from 5 pops")
        return keep_path

    # Standard single-population
    sample_file = pop_config.get("sample_file")
    if not sample_file or not os.path.exists(sample_file):
        logger.warning(f"{population}: sample file not found: {sample_file}")
        return None

    # The pop sample files already have the right format for --keep
    # Just need to ensure they have #IID header
    samples = []
    with open(sample_file) as f:
        first_line = f.readline()
        if first_line.startswith('#') or first_line.strip().upper().startswith('IID'):
            pass  # header
        else:
            iid = first_line.strip().split('\t')[0] if '\t' in first_line else first_line.strip()
            if iid:
                samples.append(iid)
        for line in f:
            iid = line.strip().split('\t')[0] if '\t' in line else line.strip()
            if iid:
                samples.append(iid)

    with open(keep_path, 'w') as f:
        f.write("#IID\n")
        for iid in samples:
            f.write(f"{iid}\n")

    return keep_path


def _parse_sscore_multi(sscore_path: str):
    """Parse a multi-sample plink2 .sscore file. Returns (avgs_list, sums_list)."""
    avgs = []
    sums = []

    with open(sscore_path) as f:
        header = f.readline().lstrip('#').strip().split('\t')

        avg_idx = None
        sum_idx = None
        for i, h in enumerate(header):
            if 'AVG' in h and 'MISSING' not in h:
                avg_idx = i
            elif 'SUM' in h and 'DOSAGE' not in h and 'MISSING' not in h:
                sum_idx = i

        if avg_idx is None and sum_idx is None:
            logger.error(f"sscore has no score columns: {header}")
            return [], []

        for line in f:
            parts = line.strip().split('\t')
            try:
                if avg_idx is not None and avg_idx < len(parts):
                    avgs.append(float(parts[avg_idx]))
                if sum_idx is not None and sum_idx < len(parts):
                    sums.append(float(parts[sum_idx]))
            except ValueError:
                continue

    return avgs, sums
