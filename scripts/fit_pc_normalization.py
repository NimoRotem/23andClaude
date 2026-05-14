#!/usr/bin/env python3
"""Item 4 fitter — compute PC-regression coefficients for each blessed PGS.

For each (PGS) with a complete set of per-pop `_scores.npy` files in
/data/pgs2/ref_panel_stats/_scores/, fit:
    raw_score ≈ β₀ + Σ βᵢ·PCᵢ          (mean)
    log var   ≈ γ₀ + Σ γᵢ·PCᵢ          (variance, heteroskedastic)
…using all panel samples (concatenated across pops). Output:
    /data/pgs2/ref_panel_stats/_pcnorm/<PGS>/coeffs.json
    /data/pgs2/ref_panel_stats/_pcnorm/<PGS>/residuals_sorted.npy  (for empirical)

Panel sample PCs are read from /data/pgs_cache/pca_1000g/ref.eigenvec.

Usage:
    python3 scripts/fit_pc_normalization.py --all
    python3 scripts/fit_pc_normalization.py --pgs PGS002746
    python3 scripts/fit_pc_normalization.py --pgs PGS002746 --n-pcs 8
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

import numpy as np

from pipeline.pc_normalization import (
    PcNormCoeffs, coeffs_path, residuals_path, save_coeffs,
    DEFAULT_N_PCS, PCNORM_DIR,
)

SCORES_DIR = "/data/pgs2/ref_panel_stats/_scores"
PCA_EIGENVEC = "/data/pgs_cache/pca_1000g/ref.eigenvec"


def load_panel_pcs(n_pcs: int) -> dict[str, np.ndarray]:
    """Return {sample_id: pc_array of length n_pcs} from ref.eigenvec."""
    pcs: dict[str, np.ndarray] = {}
    if not os.path.exists(PCA_EIGENVEC):
        raise FileNotFoundError(f"PCA eigenvec missing: {PCA_EIGENVEC}")
    with open(PCA_EIGENVEC) as f:
        header = f.readline().rstrip("\n").split("\t")
        # plink2 eigenvec: #FID IID PC1 PC2 ... PCk
        # or #IID PC1 ...
        try:
            iid_i = header.index("IID")
        except ValueError:
            iid_i = header.index("#IID")
        pc1_i = next(i for i, h in enumerate(header)
                     if h.lstrip("#").upper().startswith("PC"))
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if not parts[iid_i]:
                continue
            iid = parts[iid_i]
            try:
                vals = [float(parts[pc1_i + j]) for j in range(n_pcs)]
            except (IndexError, ValueError):
                continue
            pcs[iid] = np.array(vals, dtype=np.float64)
    return pcs


def load_panel_scores(pgs_id: str) -> tuple[np.ndarray, list[str]]:
    """Concatenate all per-pop .npy + sample_ids.txt for one PGS.
    Returns (avg_scores, sample_ids)."""
    pgs_dir = Path(SCORES_DIR) / pgs_id
    if not pgs_dir.is_dir():
        raise FileNotFoundError(f"no scores dir for {pgs_id}: {pgs_dir}")
    all_scores: list[float] = []
    all_iids: list[str] = []
    for npy in sorted(pgs_dir.glob("*_scores.npy")):
        pop = npy.stem.replace("_scores", "")
        ids_path = pgs_dir / f"{pop}_sample_ids.txt"
        if not ids_path.exists():
            continue
        arr = np.load(npy)
        iids = ids_path.read_text().splitlines()
        if arr.size != len(iids):
            print(f"  ! {pgs_id}/{pop}: size mismatch ({arr.size} vs {len(iids)})")
            continue
        all_scores.extend(arr["avg"].tolist())
        all_iids.extend(iids)
    if not all_scores:
        raise ValueError(f"no scores loaded for {pgs_id}")
    return np.array(all_scores, dtype=np.float64), all_iids


def fit_mean_var_regression(
    scores: np.ndarray, pcs: np.ndarray, n_pcs: int
) -> tuple[np.ndarray, float, np.ndarray, float, float]:
    """Fit mean and variance regressions.

    scores: (n,) panel raw_scores (AVG scale)
    pcs:    (n, n_pcs) per-sample PC matrix

    Returns:
        beta_mean    (n_pcs,)
        intercept_mean (float)
        beta_var     (n_pcs,)
        intercept_var (float)
        residual_std  (float, overall — for mean-only fallback)
    """
    n = scores.size
    X = np.column_stack([np.ones(n), pcs])   # (n, n_pcs+1)
    # Mean fit: ordinary least squares
    coefs, *_ = np.linalg.lstsq(X, scores, rcond=None)
    intercept_mean = float(coefs[0])
    beta_mean = coefs[1:1 + n_pcs]
    fitted = X @ coefs
    residuals = scores - fitted
    residual_std = float(np.std(residuals, ddof=1))
    # Variance fit: regress log(eps^2 + eps) on PCs
    # Use a small floor to keep log finite for tiny residuals.
    eps2 = residuals ** 2
    floor = max(np.percentile(eps2, 1), 1e-30)
    log_eps2 = np.log(eps2 + floor)
    var_coefs, *_ = np.linalg.lstsq(X, log_eps2, rcond=None)
    intercept_var = float(var_coefs[0])
    beta_var = var_coefs[1:1 + n_pcs]
    return beta_mean, intercept_mean, beta_var, intercept_var, residual_std


def fit_one(pgs_id: str, n_pcs: int, panel_pcs: dict[str, np.ndarray]) -> dict:
    """Fit coefficients for one PGS. Returns a summary dict."""
    scores, iids = load_panel_scores(pgs_id)
    # Align scores with panel PCs by sample IID
    keep_scores: list[float] = []
    keep_pcs: list[np.ndarray] = []
    n_missing = 0
    for sc, iid in zip(scores, iids):
        if iid in panel_pcs:
            keep_scores.append(sc)
            keep_pcs.append(panel_pcs[iid])
        else:
            n_missing += 1
    if len(keep_scores) < 50:
        raise ValueError(f"too few aligned samples ({len(keep_scores)}); "
                         f"missing {n_missing} sample IDs from PCA file")
    sc_arr = np.array(keep_scores, dtype=np.float64)
    pc_arr = np.array(keep_pcs, dtype=np.float64)
    beta_mean, b0, beta_var, g0, std = fit_mean_var_regression(sc_arr, pc_arr, n_pcs)
    # Residuals for empirical percentile (sorted for searchsorted)
    fitted = b0 + pc_arr @ beta_mean
    residuals_sorted = np.sort(sc_arr - fitted)
    # Save
    c = PcNormCoeffs(
        pgs_id=pgs_id,
        n_pcs=n_pcs,
        intercept=b0,
        beta=beta_mean.tolist(),
        var_intercept=g0,
        var_beta=beta_var.tolist(),
        residual_std=std,
        n_panel_samples=int(sc_arr.size),
        scoring_method="plink2-nomi",
        imputation_policy="no-mean-imputation",
        generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        panel="GRCh38_1000G_ALL",
    )
    save_coeffs(c)
    np.save(residuals_path(pgs_id), residuals_sorted)
    return {
        "pgs_id": pgs_id,
        "n_aligned": int(sc_arr.size),
        "n_missing_iids": int(n_missing),
        "residual_std": std,
        "score_std": float(sc_arr.std(ddof=1)),
        "r2": float(1 - (residuals_sorted.var() / sc_arr.var())),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pgs", help="single PGS id")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--n-pcs", type=int, default=DEFAULT_N_PCS)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--resume", action="store_true", default=True,
                    help="Skip PGSes that already have coeffs.json. " + '# ITEM_4_FIT_RESUME')
    ap.add_argument("--force", action="store_false", dest="resume",
                    help="Re-fit even if coeffs.json exists.")
    args = ap.parse_args()

    print(f"loading panel PCs ({args.n_pcs} PCs)…")
    panel_pcs = load_panel_pcs(args.n_pcs)
    print(f"  {len(panel_pcs)} panel samples with PCs")

    if args.pgs:
        pgs_list = [args.pgs]
    elif args.all:
        pgs_list = sorted([d.name for d in Path(SCORES_DIR).iterdir() if d.is_dir()])
    else:
        ap.error("--pgs or --all required")

    if args.limit:
        pgs_list = pgs_list[: args.limit]

    print(f"fitting {len(pgs_list)} PGSes…")
    ok = 0
    failed: list[tuple[str, str]] = []
    for pgs in pgs_list:
        if args.resume and os.path.exists(coeffs_path(pgs)):
            continue
        try:
            res = fit_one(pgs, args.n_pcs, panel_pcs)
            ok += 1
            if ok % 25 == 0 or ok == len(pgs_list):
                print(f"  [{ok}/{len(pgs_list)}] {pgs}  "
                      f"n={res['n_aligned']}  residual_std={res['residual_std']:.4g}  "
                      f"r2={res['r2']:.3f}")
        except Exception as e:
            failed.append((pgs, str(e)))
    print(f"\ndone: {ok} ok, {len(failed)} failed")
    for pgs, err in failed[:10]:
        print(f"  ! {pgs}: {err}")


if __name__ == "__main__":
    main()
