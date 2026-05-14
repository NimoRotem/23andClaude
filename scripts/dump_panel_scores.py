#!/usr/bin/env python3
"""Item 1 backfill — dump per-sample panel scores for already-blessed PGSes.

For each (PGS, pop) in the registry that lacks a corresponding
`_scores/<PGS>/<POP>_scores.npy`, re-run plink2 --score against the
panel subset and save the per-sample avg/sum/ct + sample IDs.

Does NOT touch the JSON ref-stats files. Does NOT re-quarantine. Pure
additive — only writes new .npy + sample_ids.txt.

Usage:
    python3 scripts/dump_panel_scores.py                  # all blessed
    python3 scripts/dump_panel_scores.py --pgs PGS002746  # one
    python3 scripts/dump_panel_scores.py --resume         # skip if exists
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

# Reuse load_pgs / write_score_file / panel_ids_for / run_score from the recompute script
# (they aren't currently importable as a module — we shell out to plink2 inline).
import gzip
import re

PLINK2 = "/home/nimo/miniconda3/envs/genomics/bin/plink2"
PANEL = "/data/pgs2/ref_panel/GRCh38_1000G_ALL"
STATS_DIR = "/data/pgs2/ref_panel_stats"
SCORES_DIR = os.path.join(STATS_DIR, "_scores")
POP_DIR = "/data/pgs2/ref_panel/pop_samples"
REGISTRY = os.path.join(STATS_DIR, "registry.json")
PGS_CACHE = "/data/pgs_cache"


def load_registry() -> list[dict]:
    return json.load(open(REGISTRY))["entries"]


def already_has_npy(pgs_id: str, pop: str) -> bool:
    return os.path.exists(os.path.join(SCORES_DIR, pgs_id, f"{pop}_scores.npy"))


def needs_work(pgs_id: str, pop: str) -> bool:
    return not already_has_npy(pgs_id, pop)


def fetch_panel_format_scoring(pgs_id: str, work: str) -> tuple[str, int]:
    """Use the same logic as recompute_ref_stats.py to build the
    panel-formatted scoring file for this PGS."""
    # Delegate to recompute_ref_stats.py's load_pgs + write_score_file by
    # importing it as a module. We hack sys.path to make that work.
    sys.path.insert(0, str(ROOT / "scripts"))
    import recompute_ref_stats as _r
    pgs, _scoring_path = _r.load_pgs(pgs_id)
    panel_ids = _r.panel_ids_for(pgs)
    score_file = os.path.join(work, f"{pgs_id}_panelfmt.tsv")
    rows = _r.write_score_file(pgs, panel_ids, score_file)
    return score_file, len(rows)


def run_score_dump(pgs_id: str, pop: str, work: str, score_file: str) -> dict:
    """Run plink2 --score for one pop and dump .npy. Returns metadata."""
    keep = os.path.join(work, f"{pop}.keep")
    with open(keep, "w") as fh:
        fh.write("#IID\n")
        for line in open(os.path.join(POP_DIR, f"{pop}.txt")):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            fh.write(line + "\n")

    out_prefix = os.path.join(work, f"{pop}_score")
    cmd = [
        PLINK2, "--pfile", PANEL, "vzs", "--keep", keep,
        "--threads", os.environ.get("RECOMPUTE_PLINK_THREADS", "4"),
        "--score", score_file, "header-read", "1", "2", "3",
        "cols=+scoresums", "no-mean-imputation",
        "--out", out_prefix,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"plink2 failed for {pgs_id}/{pop}: {r.stderr[-300:]}")

    sscore_path = out_prefix + ".sscore"
    rows = []
    iids = []
    with open(sscore_path) as f:
        h = f.readline().rstrip("\n").split("\t")
        a_i = h.index("WEIGHT_AVG")
        s_i = h.index("WEIGHT_SUM")
        c_i = h.index("ALLELE_CT")
        try:
            iid_i = h.index("#IID")
        except ValueError:
            iid_i = h.index("IID")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if not parts[0]:
                continue
            rows.append((float(parts[a_i]), float(parts[s_i]), int(parts[c_i])))
            iids.append(parts[iid_i])

    # Dump
    import numpy as np
    scores_dir = os.path.join(SCORES_DIR, pgs_id)
    os.makedirs(scores_dir, exist_ok=True)
    arr = np.empty(len(rows), dtype=[("avg", "f8"), ("sum", "f8"), ("ct", "i4")])
    for i, (a, s, c) in enumerate(rows):
        arr[i] = (a, s, c)
    np.save(os.path.join(scores_dir, f"{pop}_scores.npy"), arr)
    with open(os.path.join(scores_dir, f"{pop}_sample_ids.txt"), "w") as fh:
        for iid in iids:
            fh.write(iid + "\n")
    return {"n": len(rows), "scores_dir": scores_dir}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pgs", help="Single PGS ID")
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    reg = load_registry()
    # Group entries by PGS
    by_pgs: dict[str, list[str]] = {}
    for e in reg:
        if args.pgs and e["pgs_id"] != args.pgs:
            continue
        by_pgs.setdefault(e["pgs_id"], []).append(e["population"])
    pgs_list = sorted(by_pgs.keys())
    if args.limit:
        pgs_list = pgs_list[: args.limit]

    print(f"[{time.strftime('%H:%M:%S')}] {len(pgs_list)} PGSes to consider")
    n_pgs_done = 0
    n_pop_done = 0
    t_start = time.time()
    for i, pgs in enumerate(pgs_list, 1):
        pops = by_pgs[pgs]
        if args.resume and all(already_has_npy(pgs, p) for p in pops):
            continue
        try:
            with tempfile.TemporaryDirectory(prefix=f"dump_{pgs}_") as work:
                score_file, n_in_score = fetch_panel_format_scoring(pgs, work)
                for pop in pops:
                    if args.resume and already_has_npy(pgs, pop):
                        continue
                    try:
                        info = run_score_dump(pgs, pop, work, score_file)
                        n_pop_done += 1
                    except Exception as e:
                        print(f"  ! {pgs}/{pop}: {e}")
            n_pgs_done += 1
            avg = (time.time() - t_start) / max(n_pgs_done, 1)
            print(f"[{time.strftime('%H:%M:%S')}] ({i}/{len(pgs_list)}) "
                  f"{pgs} done  ({len(pops)} pops, avg {avg:.0f}s/pgs)")
        except Exception as e:
            print(f"  ! {pgs}: {e}")

    print()
    print(f"=== done: {n_pgs_done} PGSes, {n_pop_done} pop-files dumped, "
          f"elapsed {(time.time() - t_start)/60:.1f} min ===")


if __name__ == "__main__":
    main()
