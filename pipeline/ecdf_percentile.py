"""Phase 2.1 — ECDF-primary percentiles.

Per REMEDIATION_PLAN §2.1:

  - Persist `sum_scores.npy` per (PGS, population[, sex]) as primary artifact.
  - Primary percentile: rank(score) / (n+1) with linear interpolation on
    the sorted reference array.
  - z-score moves to secondary field, reported but not used in confidence gates.
  - Uncertainty band: 95% bootstrap CI from 1,000 bootstraps over the
    reference distribution. Surface reference n in report.
  - For PGS with n_ref < 200 in the relevant population, cap displayed
    precision to deciles.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

import numpy as np


log = logging.getLogger("pgs-pipeline.ecdf_percentile")


@dataclass
class EcdfPercentileResult:
    sample_id: str
    target_score: float
    n_reference: int
    ecdf_percentile: float           # 0..100 (primary)
    z_score: float                   # secondary — not used in confidence
    phi_z_percentile: float          # diagnostic only
    ci95_low: Optional[float] = None
    ci95_high: Optional[float] = None
    precision_deciles_only: bool = False
    method: str = "ecdf_linear_interp"


def ecdf_percentile(
    target: float, reference: Sequence[float],
) -> tuple[float, int]:
    """Linear-interpolation ECDF percentile. Returns (percentile_0_100, n)."""
    arr = np.asarray(reference, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    n = arr.size
    if n == 0:
        return 50.0, 0
    arr.sort()
    # Linear interpolation on the rank-vs-score curve. With ranks 1..n
    # and inserting `target` to find its rank between two consecutive
    # values, we use `searchsorted` for the bracket then interpolate.
    idx = float(np.searchsorted(arr, target, side="left"))
    if target <= arr[0]:
        rank = 1.0 if target == arr[0] else 0.0
    elif target >= arr[-1]:
        rank = float(n) if target == arr[-1] else float(n + 1)
    else:
        i = int(idx)
        a, b = arr[i - 1], arr[i]
        # rank_a = i, rank_b = i + 1 in 1-based ranks
        if b == a:
            rank = float(i)
        else:
            rank = i + (target - a) / (b - a)
    # rank ∈ [0, n+1]. Convert to a [0, 100] percentile via rank/(n+1).
    return 100.0 * rank / (n + 1), n


def bootstrap_ci_ecdf(
    target: float, reference: Sequence[float],
    *,
    n_bootstrap: int = 1000,
    rng_seed: int = 42,
) -> tuple[float, float]:
    """Bootstrap 95% CI on the ECDF percentile of `target`."""
    arr = np.asarray(reference, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    n = arr.size
    if n < 30:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(rng_seed)
    percs = np.empty(n_bootstrap, dtype=np.float64)
    for i in range(n_bootstrap):
        sample = rng.choice(arr, size=n, replace=True)
        p, _ = ecdf_percentile(target, sample)
        percs[i] = p
    lo = float(np.percentile(percs, 2.5))
    hi = float(np.percentile(percs, 97.5))
    return lo, hi


def _z_to_phi(z: float) -> float:
    return 100.0 * 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def ecdf_pipeline(
    sample_id: str,
    target_score: float,
    reference_scores: Sequence[float],
    *,
    deciles_only_threshold: int = 200,
    bootstrap: bool = True,
    n_bootstrap: int = 1000,
) -> EcdfPercentileResult:
    """End-to-end Phase 2.1 percentile compute. ECDF is primary; z/Φ(z)
    is diagnostic only and is NOT used in confidence gates."""
    arr = np.asarray(reference_scores, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    ecdf, n = ecdf_percentile(target_score, arr)
    if n >= 2:
        mu = float(arr.mean())
        sd = float(arr.std(ddof=1))
        z = (target_score - mu) / sd if sd > 0 else 0.0
    else:
        z = 0.0
    phi = _z_to_phi(z)
    ci_lo, ci_hi = (None, None)
    if bootstrap and n >= 30:
        ci_lo, ci_hi = bootstrap_ci_ecdf(
            target_score, arr, n_bootstrap=n_bootstrap,
        )
    precision_deciles_only = n < deciles_only_threshold
    if precision_deciles_only:
        ecdf = round(ecdf / 10) * 10
        if ci_lo is not None:
            ci_lo = round(ci_lo / 10) * 10
            ci_hi = round(ci_hi / 10) * 10
    return EcdfPercentileResult(
        sample_id=sample_id, target_score=target_score, n_reference=n,
        ecdf_percentile=float(ecdf), z_score=z, phi_z_percentile=phi,
        ci95_low=ci_lo, ci95_high=ci_hi,
        precision_deciles_only=precision_deciles_only,
    )


def load_sum_scores_npy(path: str | Path) -> np.ndarray:
    return np.load(path)
