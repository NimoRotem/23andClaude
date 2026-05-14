"""Item 4 — Continuous PC-regression percentile normalization.

Per the PGS Catalog method (pgsc_calc `normalization_method=mean mean+var
empirical`, Nature Genetics 2024). Replaces the discrete EUR/EAS/AFR/SAS/AMR
bucket lookup with a continuous fit:

    raw_score ≈ β₀ + β₁·PC1 + β₂·PC2 + … + β_k·PCk      (mean fit)
    log var   ≈ γ₀ + γ₁·PC1 + γ₂·PC2 + …                  (variance fit)

For a new user:
    residual_mean   = β₀ + Σ βᵢ·user_PCᵢ
    residual_var    = exp(γ₀ + Σ γᵢ·user_PCᵢ)              (mean+var)
    z = (raw_score − residual_mean) / sqrt(residual_var)
    percentile = Φ(z) × 100                                  (parametric)

The empirical variant ranks the user's residual against the panel's residuals
via ECDF (the most defensible for non-normal score distributions).

Per-PGS coefficients live alongside the existing ref-stats JSON:
    /data/pgs2/ref_panel_stats/_pcnorm/<PGS>/coeffs.json

Coefficients are fit by `scripts/fit_pc_normalization.py` after the bulk
ref-stats rebuild completes; this module is read-side only.

The gate (pipeline.result_gate.apply_gate) consumes this module via
`pc_adjusted_percentile()` — when coefficients exist for a PGS, the
percentile is computed continuously instead of via discrete μ/σ lookup.
The discrete path is the fallback for PGSes without coefficients yet.
"""
from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("pgs-pipeline.pc_normalization")

PCNORM_DIR = "/data/pgs2/ref_panel_stats/_pcnorm"

# Number of PCs used in regression. 5 is the pgsc_calc default
# (n_normalization=4 + intercept). 8–10 recommended for admixed.
DEFAULT_N_PCS = 5


@dataclass
class PcNormCoeffs:
    """Per-PGS regression coefficients."""
    pgs_id: str
    n_pcs: int
    # Mean fit: raw_score ~ β₀ + Σ βᵢ·PCᵢ
    intercept: float
    beta: List[float]              # length == n_pcs
    # Variance fit (optional, may be None for mean-only): log(var) ~ γ₀ + Σ γᵢ·PCᵢ
    var_intercept: Optional[float] = None
    var_beta: Optional[List[float]] = None
    # Residual variance (constant for mean-only fit)
    residual_std: Optional[float] = None
    # Empirical residual percentile fallback — sorted panel residuals
    # written separately as <pop>_residuals.npy alongside scores.
    n_panel_samples: int = 0
    variant_ids_sha256: str = ""
    scoring_method: str = "plink2-nomi"
    imputation_policy: str = "no-mean-imputation"
    generated_at: str = ""
    panel: str = "GRCh38_1000G_ALL"


def coeffs_path(pgs_id: str) -> str:
    return os.path.join(PCNORM_DIR, pgs_id, "coeffs.json")


def residuals_path(pgs_id: str) -> str:
    """Sorted panel residuals (mean-adjusted scores) for ECDF percentile."""
    return os.path.join(PCNORM_DIR, pgs_id, "residuals_sorted.npy")


def load_coeffs(pgs_id: str) -> Optional[PcNormCoeffs]:
    p = coeffs_path(pgs_id)
    if not os.path.exists(p):
        return None
    try:
        d = json.load(open(p))
    except (OSError, json.JSONDecodeError):
        return None
    return PcNormCoeffs(
        pgs_id=d["pgs_id"],
        n_pcs=int(d["n_pcs"]),
        intercept=float(d["intercept"]),
        beta=list(d["beta"]),
        var_intercept=d.get("var_intercept"),
        var_beta=d.get("var_beta"),
        residual_std=d.get("residual_std"),
        n_panel_samples=int(d.get("n_panel_samples", 0)),
        variant_ids_sha256=d.get("variant_ids_sha256", ""),
        scoring_method=d.get("scoring_method", "plink2-nomi"),
        imputation_policy=d.get("imputation_policy", "no-mean-imputation"),
        generated_at=d.get("generated_at", ""),
        panel=d.get("panel", "GRCh38_1000G_ALL"),
    )


def save_coeffs(c: PcNormCoeffs) -> None:
    p = coeffs_path(c.pgs_id)
    Path(os.path.dirname(p)).mkdir(parents=True, exist_ok=True)
    payload = {
        "pgs_id": c.pgs_id,
        "n_pcs": c.n_pcs,
        "intercept": c.intercept,
        "beta": list(c.beta),
        "var_intercept": c.var_intercept,
        "var_beta": list(c.var_beta) if c.var_beta is not None else None,
        "residual_std": c.residual_std,
        "n_panel_samples": c.n_panel_samples,
        "variant_ids_sha256": c.variant_ids_sha256,
        "scoring_method": c.scoring_method,
        "imputation_policy": c.imputation_policy,
        "generated_at": c.generated_at,
        "panel": c.panel,
    }
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, p)


def predict_mean(c: PcNormCoeffs, user_pcs: Sequence[float]) -> float:
    if len(user_pcs) < c.n_pcs:
        raise ValueError(f"need {c.n_pcs} PCs, got {len(user_pcs)}")
    return float(c.intercept + sum(c.beta[i] * float(user_pcs[i])
                                   for i in range(c.n_pcs)))


def predict_var(c: PcNormCoeffs, user_pcs: Sequence[float]) -> float:
    """Variance under the heteroskedastic fit (mean+var). Falls back to
    residual_std² when no variance fit is available."""
    if c.var_beta is not None and c.var_intercept is not None:
        log_var = float(c.var_intercept + sum(c.var_beta[i] * float(user_pcs[i])
                                              for i in range(c.n_pcs)))
        return math.exp(log_var)
    if c.residual_std is not None and c.residual_std > 0:
        return float(c.residual_std) ** 2
    raise ValueError("no variance estimate in coeffs")


def pc_adjusted_percentile(
    pgs_id: str,
    raw_score: float,
    user_pcs: Sequence[float],
    method: str = "mean+var",
) -> Optional[Dict[str, Any]]:
    """Return percentile + diagnostics, or None if no coeffs available.

    method ∈ {"mean", "mean+var", "empirical"}
        - mean:     z = (raw - predict_mean) / residual_std (homoskedastic)
        - mean+var: z = (raw - predict_mean) / sqrt(predict_var) (default; pgsc_calc)
        - empirical: ECDF on (raw - predict_mean) against panel residuals

    Returns dict:
        {
          "method": "pc_normalized_<method>",
          "predicted_mean": ...,
          "predicted_var": ...,
          "residual": raw_score - predicted_mean,
          "z_score": ...,
          "percentile": ...,
          "n_panel_samples": int,
          "coeffs_path": str,
        }
    """
    c = load_coeffs(pgs_id)
    if c is None:
        return None
    try:
        pred_mean = predict_mean(c, user_pcs)
    except ValueError as e:
        logger.warning(f"{pgs_id}: predict_mean failed: {e}")
        return None
    residual = float(raw_score) - pred_mean

    if method == "mean":
        if not c.residual_std or c.residual_std <= 0:
            return None
        z = residual / c.residual_std
        pred_var = c.residual_std ** 2
    elif method == "mean+var":
        try:
            pred_var = predict_var(c, user_pcs)
        except ValueError:
            return None
        if pred_var <= 0:
            return None
        z = residual / math.sqrt(pred_var)
    elif method == "empirical":
        import numpy as np
        try:
            arr = np.load(residuals_path(pgs_id))
        except FileNotFoundError:
            return None
        rank = float(np.searchsorted(arr, residual, side="right"))
        n = arr.size
        pct = 100.0 * rank / max(n + 1, 1)
        return {
            "method": "pc_normalized_empirical",
            "predicted_mean": pred_mean,
            "predicted_var": None,
            "residual": residual,
            "z_score": None,
            "percentile": round(float(min(max(pct, 0.5), 99.5)), 1),
            "n_panel_samples": int(n),
            "coeffs_path": coeffs_path(pgs_id),
        }
    else:
        return None

    # Clamp extremes the same way the discrete path does
    if abs(z) > 6:
        return {
            "method": f"pc_normalized_{method}",
            "predicted_mean": pred_mean,
            "predicted_var": pred_var,
            "residual": residual,
            "z_score": round(z, 3),
            "percentile": None,
            "reason": "z_score_extreme",
            "n_panel_samples": c.n_panel_samples,
            "coeffs_path": coeffs_path(pgs_id),
        }
    p = 0.5 * (1 + math.erf(z / math.sqrt(2))) * 100
    p = min(max(p, 0.5), 99.5)
    return {
        "method": f"pc_normalized_{method}",
        "predicted_mean": pred_mean,
        "predicted_var": pred_var,
        "residual": residual,
        "z_score": round(z, 3),
        "percentile": round(float(p), 1),
        "n_panel_samples": c.n_panel_samples,
        "coeffs_path": coeffs_path(pgs_id),
    }


def has_pc_norm(pgs_id: str) -> bool:
    return os.path.exists(coeffs_path(pgs_id))
