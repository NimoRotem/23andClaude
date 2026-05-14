#!/usr/bin/env python3
"""Phase 1.4 — Re-identify the reference panel and apply KING-robust
2nd-degree exclusion.

Steps:
  1. Run `plink2 --make-king-table` over the full 3,202-sample panel.
  2. Read the KING table; drop samples that have any kinship >= 0.0884
     with another sample (2nd-degree threshold per spec §1.4).
  3. Persist the kept-sample list at
     `<DATA_ROOT>/pgs2/ref_panel/king_filtered.keep`.
  4. Regenerate each population keep file by intersecting the existing
     `<pop>.txt` with the kept set; emit
     `<pop>.king_filtered.txt` for each EUR/EAS/AFR/SAS/AMR.
  5. Emit `reference_panel.json` with:
       label = "1000G + NYGC high-coverage, GRCh38, 3,202 samples"
       n_samples_pre_king, n_samples_post_king
       sha256 for .psam, .pvar.zst, .pgen, each keep file, king table

The script is idempotent: re-running with all outputs present prints
hashes and exits. Pass `--apply` to actually run the KING table compute.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("rebuild_ref_panel")


PLINK2 = os.environ.get("PLINK2", "/home/nimo/miniconda3/envs/genomics/bin/plink2")
REF_DIR = Path(os.environ.get("REF_DIR", "/data/pgs2/ref_panel"))
PANEL = REF_DIR / "GRCh38_1000G_ALL"
POP_DIR = REF_DIR / "pop_samples"
POPS = ("EUR", "EAS", "AFR", "SAS", "AMR")
KING_2ND_DEGREE = 0.0884   # spec §1.4


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def run_king_table(out_prefix: Path) -> Path:
    """plink2 --make-king-table → .kin0 output (one row per related pair)."""
    log.info("plink2 --make-king-table (this scans 75M variants × 3,202 samples)")
    cmd = [
        PLINK2,
        "--pfile", str(PANEL), "vzs",
        "--make-king-table",
        "--king-table-filter", "0.0442",   # 3rd-degree threshold (cousin) lower bound
        "--threads", "16",
        "--memory", "32000",
        "--out", str(out_prefix),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60 * 60 * 6)
    if proc.returncode != 0:
        raise RuntimeError(f"king-table failed: {proc.stderr[-1500:]}")
    kin0 = Path(str(out_prefix) + ".kin0")
    if not kin0.exists():
        raise RuntimeError(f"king-table produced no .kin0 at {kin0}")
    log.info("king-table → %s", kin0)
    return kin0


def parse_kin0(kin0: Path) -> set[tuple[str, str]]:
    """Return set of (IID1, IID2) pairs with kinship ≥ 2nd-degree threshold."""
    out: set[tuple[str, str]] = set()
    with kin0.open() as f:
        header = None
        for line in f:
            if not line.strip():
                continue
            if line.startswith("#") or "KINSHIP" in line and header is None:
                header = line.lstrip("#").rstrip("\n").split("\t")
                continue
            if header is None:
                continue
            parts = line.rstrip("\n").split("\t")
            row = dict(zip(header, parts))
            try:
                k = float(row.get("KINSHIP", "0"))
            except ValueError:
                continue
            if k >= KING_2ND_DEGREE:
                iid1 = row.get("ID1") or row.get("IID1") or ""
                iid2 = row.get("ID2") or row.get("IID2") or ""
                if iid1 and iid2:
                    out.add((iid1, iid2))
    return out


def greedy_unrelated_keep(
    all_iids: set[str], related_pairs: set[tuple[str, str]],
) -> set[str]:
    """For each related pair, drop one member greedily (preferring the
    member with the most other related links). Produces a maximal
    unrelated subset of `all_iids`."""
    # Build neighbor count
    neigh: dict[str, set[str]] = {iid: set() for iid in all_iids}
    for a, b in related_pairs:
        if a in neigh and b in neigh:
            neigh[a].add(b)
            neigh[b].add(a)
    kept = set(all_iids)
    # Iteratively remove the node with the most remaining related neighbors
    while True:
        worst = max(neigh, key=lambda x: len(neigh[x]) if x in kept else -1)
        if not neigh[worst] or worst not in kept:
            break
        kept.discard(worst)
        for n in neigh[worst]:
            neigh[n].discard(worst)
        neigh[worst].clear()
    return kept


def load_all_iids(psam: Path) -> set[str]:
    iids: set[str] = set()
    with psam.open() as f:
        header = None
        for line in f:
            if line.startswith("#"):
                header = line.lstrip("#").rstrip("\n").split("\t")
                continue
            parts = line.rstrip("\n").split("\t")
            row = dict(zip(header or [], parts))
            iid = row.get("IID") or parts[0]
            if iid:
                iids.add(iid)
    return iids


def load_pop_iids(pop: str) -> set[str]:
    out: set[str] = set()
    p = POP_DIR / f"{pop}.txt"
    if not p.exists():
        return out
    with p.open() as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.add(line)
    return out


def write_keep(iids: set[str], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        f.write("#IID\n")
        for iid in sorted(iids):
            f.write(iid + "\n")
    return out_path


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--kin0", default=None, help="Reuse an existing .kin0 file")
    args = ap.parse_args(argv)

    out_prefix = REF_DIR / "king"
    if args.kin0:
        kin0 = Path(args.kin0)
    elif args.apply:
        kin0 = run_king_table(out_prefix)
    else:
        kin0 = Path(str(out_prefix) + ".kin0")
        if not kin0.exists():
            log.warning("no .kin0 yet; pass --apply to run plink2 --make-king-table")
            return 1

    related = parse_kin0(kin0)
    log.info("KING: %d pairs ≥ 2nd-degree (kinship ≥ %g)", len(related), KING_2ND_DEGREE)
    all_iids = load_all_iids(PANEL.with_suffix(".psam"))
    log.info("panel sample count: %d", len(all_iids))
    kept = greedy_unrelated_keep(all_iids, related)
    log.info("unrelated subset: %d (excluded %d)",
             len(kept), len(all_iids) - len(kept))
    global_keep = REF_DIR / "king_filtered.keep"
    if args.apply:
        write_keep(kept, global_keep)
        log.info("wrote %s", global_keep)
    else:
        log.info("(dry-run) would write %s", global_keep)

    # Per-pop keep files
    per_pop_paths = {}
    for pop in POPS:
        pop_iids = load_pop_iids(pop)
        pop_kept = pop_iids & kept
        out = POP_DIR / f"{pop}.king_filtered.txt"
        if args.apply:
            write_keep(pop_kept, out)
        per_pop_paths[pop] = {"path": str(out), "n_pre_king": len(pop_iids), "n_post_king": len(pop_kept)}
        log.info("%s: %d → %d", pop, len(pop_iids), len(pop_kept))

    # Emit reference_panel.json
    record = {
        "label": "1000G + NYGC high-coverage, GRCh38, 3,202 samples",
        "ref_dir": str(REF_DIR),
        "n_samples_pre_king": len(all_iids),
        "n_samples_post_king": len(kept),
        "king_2nd_degree_threshold": KING_2ND_DEGREE,
        "sha256": {
            "psam": sha256_file(PANEL.with_suffix(".psam")),
            "pvar_zst": sha256_file(Path(str(PANEL) + ".pvar.zst")),
            "pgen": sha256_file(PANEL.with_suffix(".pgen")),
            "king_table": sha256_file(kin0) if kin0.exists() else None,
            "kept_keep": sha256_file(global_keep) if global_keep.exists() else None,
        },
        "per_population": per_pop_paths,
    }
    out_json = REF_DIR / "reference_panel.json"
    if args.apply:
        out_json.write_text(json.dumps(record, indent=2))
        log.info("wrote %s", out_json)
    print(json.dumps(record, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
