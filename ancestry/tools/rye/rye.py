#!/usr/bin/env python3
"""Python port of rye.R — fast NNLS-based ancestry decomposition.

Mirrors the algorithm of rye.R exactly; only the language differs.
Drop-in CLI replacement: same arguments, same output filenames, same file format.

Usage:
    python rye.py --eigenvec=X.eigenvec --eigenval=X.eigenval \
                  --pop2group=pop2group.txt --output=out --pcs=20

Performance: single-threaded Python is much faster than 32 R processes
because per-process forking overhead and R-loop overhead dominate
the actual NNLS math (which runs in microseconds on the 9x20 means matrix).
"""

import argparse
import gzip
import multiprocessing as mp
import os
import sys
import time

import numpy as np
from scipy.optimize import nnls


# ---------------------------------------------------------------------------
# Worker pool for batched NNLS predictions
# ---------------------------------------------------------------------------
# nnls() in scipy is C/Fortran-backed but holds the GIL, so threading does
# not help. Using a persistent multiprocessing pool with the reference X
# pre-broadcast lets us amortize fork overhead across the entire run and
# parallelize the per-row solves over many cores.

_WORKER_REFX = None  # populated in workers via initializer


def _worker_init(refX):
    global _WORKER_REFX
    _WORKER_REFX = refX


def _worker_predict(args):
    """Predict ancestry for a chunk of rows of refX given (means, weight, idx_slice)."""
    means, weight, lo, hi = args
    A = np.ascontiguousarray(means.T)
    Xw = _WORKER_REFX[lo:hi] * weight
    out = np.empty((hi - lo, means.shape[0]), dtype=np.float64)
    for i in range(hi - lo):
        c, _ = nnls(A, Xw[i])
        s = c.sum()
        out[i] = c / s if s > 0 else c
    return lo, hi, out


_GLOBAL_POOL = None
_POOL_NPROC = 0
_POOL_REFX_ID = None  # id() of the reference matrix the pool was bound to


# ---------------------------------------------------------------------------
# Logging helpers (mirror rye.R style)
# ---------------------------------------------------------------------------

def _ts():
    return time.strftime("[ %b %d %Y - %X ]")


def log(msg):
    print(f"{_ts()} {msg}", flush=True)


def progress(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def read_eigenvec(path):
    """Read PLINK-style eigenvec: FID IID PC1 PC2 ... PCN.

    Returns (fids, iids, X) — X is float64 (n_samples, n_pcs).
    """
    fids, iids, rows = [], [], []
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        for line in f:
            if not line or line[0] == "#":
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            try:
                vals = [float(x) for x in parts[2:]]
            except ValueError:
                # header line like "FID IID PC1 PC2 ..."
                continue
            fids.append(parts[0])
            iids.append(parts[1])
            rows.append(vals)
    return np.array(fids), np.array(iids), np.asarray(rows, dtype=np.float64)


def read_eigenval(path):
    with open(path) as f:
        return np.array(
            [float(l.strip()) for l in f if l.strip()], dtype=np.float64
        )


def read_pop2group(path):
    """Returns (pop2group dict, ordered list of groups in first-seen order).

    Mirrors R's read.table(header=T): looks up "Pop" and "Group" by column
    name, so files with extra columns (e.g. Subgroup) are handled correctly.
    """
    p2g = {}
    groups = []
    seen = set()
    pop_idx, group_idx = 0, 1
    with open(path) as f:
        header = None
        for line in f:
            line = line.rstrip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if header is None:
                header = parts
                # Find column indices by name (case-insensitive)
                lc = [c.lower() for c in header]
                if "pop" in lc:
                    pop_idx = lc.index("pop")
                if "group" in lc:
                    group_idx = lc.index("group")
                continue
            if len(parts) > max(pop_idx, group_idx):
                pop, group = parts[pop_idx], parts[group_idx]
                p2g[pop] = group
                if group not in seen:
                    seen.add(group)
                    groups.append(group)
    return p2g, groups


# ---------------------------------------------------------------------------
# Core math (mirrors rye.R functions)
# ---------------------------------------------------------------------------

def rye_scale(X):
    """Min-max scale each PC column to [0, 1]. Mirrors rye.R rye.scale."""
    Y = X - X.min(axis=0, keepdims=True)
    rng = Y.max(axis=0, keepdims=True)
    rng = np.where(rng == 0, 1.0, rng)
    return Y / rng


def population_means(X, group_idx, n_groups, alpha, weight):
    """Median of X by group, with shrinkage and per-PC weighting.

    Mirrors rye.populationMeans (median aggregation, R fn=median default).

    X         : (n_ref, pcs)
    group_idx : (n_ref,) int — group index per ref sample
    n_groups  : int
    alpha     : (n_groups,) shrinkage strength per group
    weight    : (pcs,) per-PC weight

    Returns: (n_groups, pcs) — already weight-scaled.
    """
    means = np.empty((n_groups, X.shape[1]), dtype=np.float64)
    for g in range(n_groups):
        means[g] = np.median(X[group_idx == g], axis=0)
    # Shrinkage toward 0.5: i + ((1/2 - i)^2 * sign((i<=1/2) - (i>1/2)) * alpha)
    sign = np.where(means <= 0.5, 1.0, -1.0)
    means = means + ((0.5 - means) ** 2) * sign * alpha[:, None]
    # Weight each feature (column-wise broadcast)
    means = means * weight
    return means


def predict(X, means, weight):
    """For each row x in X, solve nnls(means.T, x*weight) and normalize.

    Mirrors rye.predict. Uses the global worker pool if it has been
    initialized for this X (see ensure_pool); otherwise runs serially.
    """
    global _GLOBAL_POOL, _POOL_NPROC, _POOL_REFX_ID

    n = X.shape[0]
    n_groups = means.shape[0]

    if _GLOBAL_POOL is not None and _POOL_REFX_ID == id(X) and n >= 256:
        chunks = max(1, _POOL_NPROC)
        # Round-robin chunking
        bounds = np.linspace(0, n, chunks + 1, dtype=int)
        args = [
            (means, weight, int(bounds[i]), int(bounds[i + 1]))
            for i in range(chunks)
            if bounds[i + 1] > bounds[i]
        ]
        out = np.empty((n, n_groups), dtype=np.float64)
        for lo, hi, sub in _GLOBAL_POOL.imap_unordered(_worker_predict, args):
            out[lo:hi] = sub
        return out

    # Serial fallback (also used for the final per-individual predict on
    # the full PCA, where pool overhead may not pay off).
    A = np.ascontiguousarray(means.T)
    Xw = X * weight
    out = np.empty((n, n_groups), dtype=np.float64)
    for i in range(n):
        c, _ = nnls(A, Xw[i])
        s = c.sum()
        out[i] = c / s if s > 0 else c
    return out


def ensure_pool(refX, n_workers):
    """Spin up a persistent worker pool bound to a reference matrix."""
    global _GLOBAL_POOL, _POOL_NPROC, _POOL_REFX_ID
    if _GLOBAL_POOL is not None:
        _GLOBAL_POOL.close()
        _GLOBAL_POOL.join()
        _GLOBAL_POOL = None
    if n_workers <= 1:
        return
    _POOL_NPROC = n_workers
    _POOL_REFX_ID = id(refX)
    _GLOBAL_POOL = mp.Pool(
        processes=n_workers, initializer=_worker_init, initargs=(refX,)
    )


def shutdown_pool():
    global _GLOBAL_POOL
    if _GLOBAL_POOL is not None:
        _GLOBAL_POOL.close()
        _GLOBAL_POOL.join()
        _GLOBAL_POOL = None


def absolute_error(expected, predicted, ref_group_idx, n_groups):
    """Mirrors the error metric in rye.gibbs:

    1. mean absolute error per ref sample (over group columns)
    2. mean per group of those per-sample errors
    3. mean over groups
    """
    per_sample = np.abs(expected - predicted).mean(axis=1)  # (n_ref,)
    group_means = np.empty(n_groups)
    for g in range(n_groups):
        mask = ref_group_idx == g
        group_means[g] = per_sample[mask].mean() if mask.any() else 0.0
    return float(group_means.mean())


# ---------------------------------------------------------------------------
# Optimization (Gibbs MCMC + rounds)
# ---------------------------------------------------------------------------

def gibbs(
    refX,
    ref_group_idx,
    expected,
    n_groups,
    alpha,
    weight,
    iterations,
    sd,
    optimize_alpha,
    optimize_weight,
    rng,
):
    """One MCMC chain. Returns (best_error, best_alpha, best_weight, best_means).

    NB: matches rye.R bug-for-bug — when a new best error is found,
    `minParams` stores PRE-update alpha/weight but POST-update means.
    """
    means = population_means(refX, ref_group_idx, n_groups, alpha, weight)
    pred = predict(refX, means, weight)
    old_error = absolute_error(expected, pred, ref_group_idx, n_groups)

    min_error = old_error
    min_alpha = alpha.copy()
    min_weight = weight.copy()
    min_means = means.copy()

    alpha_mom = np.zeros_like(alpha)
    weight_mom = np.zeros_like(weight)
    momentum = 0.1

    for _ in range(iterations):
        new_alpha = alpha.copy()
        if optimize_alpha:
            j = int(rng.integers(0, len(new_alpha)))
            jitter = rng.normal(0.0, (abs(new_alpha[j]) + 0.001) * sd)
            new_alpha[j] += jitter + alpha_mom[j]
            if new_alpha[j] < 0:
                new_alpha[j] = 0.0

        new_weight = weight.copy()
        if optimize_weight:
            j = int(rng.integers(0, len(new_weight)))
            jitter = rng.normal(0.0, (new_weight[j] + 0.001) * sd)
            new_weight[j] += jitter + weight_mom[j]
            if new_weight[j] < 0:
                new_weight[j] = 0.0

        means = population_means(
            refX, ref_group_idx, n_groups, new_alpha, new_weight
        )
        pred = predict(refX, means, new_weight)
        new_error = absolute_error(expected, pred, ref_group_idx, n_groups)

        # pnorm(new, mean=old, sd=old/1000)
        from math import erf, sqrt
        z = (new_error - old_error) / max(old_error / 1000.0, 1e-12)
        odds_jump_to_old = 0.5 * (1.0 + erf(z / sqrt(2)))  # = pnorm(...)
        odds_keep_new = 1.0 - odds_jump_to_old

        if new_error < min_error:
            # Bug-compat: store PRE-update alpha/weight, POST-update means
            min_error = new_error
            min_alpha = alpha.copy()
            min_weight = weight.copy()
            min_means = means.copy()

        if rng.uniform(0.0, 1.0) < odds_keep_new:
            old_error = new_error
            alpha_mom = (alpha_mom / 2.0) + ((new_alpha - alpha) * momentum)
            weight_mom = (weight_mom / 2.0) + ((new_weight - weight) * momentum)
            alpha = new_alpha
            weight = new_weight

    return min_error, min_alpha, min_weight, min_means


def optimize(
    refX,
    ref_group_idx,
    ref_pops,
    alpha,
    weight,
    attempts,
    iterations,
    rounds,
    start_sd,
    end_sd,
    rng,
    optimize_alpha=True,
    optimize_weight=True,
):
    """Top-level loop with `rounds` of `attempts` parallel restarts.

    Mirrors rye.optimize. Includes early stopping (5-round error spread <= 1e-5).

    refX must be the same array object the worker pool was bound to (if any).
    """
    n_groups = len(ref_pops)

    # Expected proportions: one-hot — each ref sample is 100% its own group
    expected = np.zeros((refX.shape[0], n_groups), dtype=np.float64)
    expected[np.arange(refX.shape[0]), ref_group_idx] = 1.0

    all_errors = []
    best_overall = None

    for rnd in range(1, rounds + 1):
        # SD anneals from start_sd toward end_sd over rounds
        if rounds > 1 and rnd > 1:
            sd = start_sd - (start_sd - end_sd) * np.log(rnd) / np.log(rounds)
        else:
            sd = start_sd

        attempt_results = []
        for _ in range(attempts):
            res = gibbs(
                refX,
                ref_group_idx,
                expected,
                n_groups,
                alpha.copy(),
                weight.copy(),
                iterations,
                sd,
                optimize_alpha,
                optimize_weight,
                rng,
            )
            attempt_results.append(res)

        errors = [r[0] for r in attempt_results]
        best_idx = int(np.argmin(errors))
        best = attempt_results[best_idx]
        mean_err = float(np.mean(errors))
        progress(
            f"Round {rnd}/{rounds} Mean error: {mean_err:.6f}, "
            f"Best error: {best[0]:.6f}"
        )

        # Mirror rye.R: use this round's best alpha/weight for next round
        alpha = best[1]
        weight = best[2]
        all_errors.append(best[0])

        if best_overall is None or best[0] < best_overall[0]:
            best_overall = best

        # Early stopping: 5-round error spread <= 1e-5
        if rnd > 5:
            window = all_errors[-6:]
            spread = max(window) - min(window)
            if spread <= 1e-5:
                log(
                    f"Converged at round {rnd}/{rounds} "
                    f"(5-round error spread {spread:.6f} <= 1e-5)"
                )
                break

    return best_overall


# ---------------------------------------------------------------------------
# Output writers (match rye.R write.table format)
# ---------------------------------------------------------------------------

def write_q(path, ids, col_names, estimates):
    """Mirror R's write.table(x, col.names=TRUE, row.names=TRUE, sep='\\t').

    R's quirk: when both col.names and row.names are TRUE, the header line
    has one *fewer* field than data rows (no leading row-label cell).
    """
    with open(path, "w") as f:
        f.write("\t".join(col_names) + "\n")
        for i, sid in enumerate(ids):
            vals = "\t".join(f"{v:.6g}" for v in estimates[i])
            f.write(f"{sid}\t{vals}\n")


def write_fam(path, fids, iids):
    """Mirror R fam output: header '\\tpopulation\\tid', then iid<tab>fid<tab>iid."""
    with open(path, "w") as f:
        f.write("\tpopulation\tid\n")
        for fid, iid in zip(fids, iids):
            f.write(f"{iid}\t{fid}\t{iid}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(
    eigenvec_file,
    eigenval_file,
    pop2group_file,
    output_file,
    pcs=20,
    optim_rounds=200,
    optim_iter=100,
    attempts=4,
    seed=42,
    workers=1,
):
    log("Reading in Eigenvector file")
    fids, iids, fullPCA = read_eigenvec(eigenvec_file)
    log("Reading in Eigenvalue file")
    eigenval = read_eigenval(eigenval_file)
    log("Reading in pop2group file")
    p2g, groups_in_order = read_pop2group(pop2group_file)

    log("Creating individual mapping")
    # In rye.R, fam[,1] = FID, fam[,2] = IID, rownames = IID

    log("Scaling PCs")
    fullPCA = rye_scale(fullPCA)

    log("Weighting PCs")
    weight_full = eigenval / eigenval.max()

    log("Aggregating individuals to population groups")
    # Remap each FID through pop2group → group label.
    # Samples not in pop2group (e.g. the query) keep their FID and will not
    # match any reference group, so they're excluded from the optimization.
    region_pops = np.array([p2g.get(f, f) for f in fids])
    ref_pops = np.array(groups_in_order)

    log("Optimizing estimates using NNLS")
    Xpcs = np.ascontiguousarray(fullPCA[:, :pcs])
    weight = weight_full[:pcs].copy()
    alpha = np.full(len(ref_pops), 0.001, dtype=np.float64)

    # Build reference subset (rows whose remapped group is in ref_pops)
    ref_mask = np.isin(region_pops, ref_pops)
    refX = np.ascontiguousarray(Xpcs[ref_mask])
    pop_to_idx = {p: i for i, p in enumerate(ref_pops)}
    ref_group_idx = np.array(
        [pop_to_idx[p] for p in region_pops[ref_mask]], dtype=np.int64
    )
    log(f"Reference panel: {refX.shape[0]} samples, {len(ref_pops)} groups, {pcs} PCs")

    if workers > 1:
        log(f"Starting worker pool with {workers} processes")
        ensure_pool(refX, workers)

    rng = np.random.default_rng(seed)
    best = optimize(
        refX=refX,
        ref_group_idx=ref_group_idx,
        ref_pops=ref_pops,
        alpha=alpha,
        weight=weight,
        attempts=attempts,
        iterations=optim_iter,
        rounds=optim_rounds,
        start_sd=0.01,
        end_sd=0.005,
        rng=rng,
        optimize_alpha=True,
        optimize_weight=True,
    )
    _, opt_alpha, opt_weight, opt_means = best
    shutdown_pool()

    log("Calculate per-individual ancestry estimates")
    estimates = predict(Xpcs, opt_means, opt_weight)
    row_sums = estimates.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1.0
    estimates = estimates / row_sums

    n_groups = len(ref_pops)
    log("Create output files")
    q_path = f"{output_file}-{pcs}.{n_groups}.Q"
    write_q(q_path, iids, ref_pops, estimates)
    # rye.R also writes a per-population file with ncol(optEstimates) groups.
    # Here pops==groups (after region remapping), so write the same file under
    # the alternate name only if it differs.
    q_path_alt = f"{output_file}-{pcs}.{estimates.shape[1]}.Q"
    if q_path_alt != q_path:
        write_q(q_path_alt, iids, ref_pops, estimates)
    write_fam(f"{output_file}-{pcs}.fam", fids, iids)


def main():
    p = argparse.ArgumentParser(
        description="Python port of rye.R — fast NNLS ancestry decomposition"
    )
    p.add_argument("--eigenvec", required=True, help="Eigenvector file [REQUIRED]")
    p.add_argument("--eigenval", required=True, help="Eigenvalue file [REQUIRED]")
    p.add_argument(
        "--pop2group", required=True, help="Population-to-group mapping [REQUIRED]"
    )
    p.add_argument("--output", default="output", help="Output prefix")
    p.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Number of worker processes for batched NNLS prediction (1 = serial)",
    )
    p.add_argument("--pcs", type=int, default=20, help="Number of PCs to use")
    p.add_argument("--rounds", type=int, default=50, help="Optimization rounds")
    p.add_argument("--iter", type=int, default=50, help="Iterations per round")
    p.add_argument("--attempts", type=int, default=4, help="Restart attempts per round")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    args = p.parse_args()

    for f, label in [
        (args.eigenvec, "eigenvec"),
        (args.eigenval, "eigenval"),
        (args.pop2group, "pop2group"),
    ]:
        if not os.path.exists(f):
            print(f"ERROR: {label} file not found: {f}", file=sys.stderr)
            sys.exit(1)

    t0 = time.time()
    log("Parsing user supplied arguments...")
    log("Arguments passed validation")
    log(f"Running core rye (Python port, {args.threads} workers)")
    run(
        eigenvec_file=args.eigenvec,
        eigenval_file=args.eigenval,
        pop2group_file=args.pop2group,
        output_file=args.output,
        pcs=args.pcs,
        optim_rounds=args.rounds,
        optim_iter=args.iter,
        attempts=args.attempts,
        seed=args.seed,
        workers=args.threads,
    )
    elapsed = time.time() - t0
    log("Process completed")
    log(f"The process took {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
