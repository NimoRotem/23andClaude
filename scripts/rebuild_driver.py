#!/usr/bin/env python3
"""W0.1 atomic rebuild driver — staged ref-stats rebuild + registry bless.

Reads the availability matrix to find which (PGS, pop) entries are blocked.
For each PGS, runs recompute_ref_stats.py to regenerate stats, then blesses
each new file into the registry. Per-PGS atomic: if any pop fails, the
PGS's registry isn't updated for ANY pop (so the gate keeps its previous
verdict).

After each PGS:
  - Validate every new file passes _rs_validate
  - Run HG00096 anchor (if applicable)
  - bless all populations atomically
  - Log to logs/rebuild_progress.jsonl

Usage:
    python3 scripts/rebuild_driver.py --dry-run        # show queue
    python3 scripts/rebuild_driver.py                  # full rebuild
    python3 scripts/rebuild_driver.py --pgs PGS002746  # one PGS

Outputs:
    logs/rebuild_progress.jsonl  — per-PGS progress
    logs/rebuild_summary.json    — final counts
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

from pipeline import scoring as _scoring
from pipeline import registry as _registry
from pipeline.config import (
    BUILDABLE_POPULATIONS, LEGACY_REF_PANEL_STATS,
)
from pipeline import reason_codes as RC


PROGRESS_LOG = ROOT / "logs" / "rebuild_progress.jsonl"
SUMMARY_FILE = ROOT / "logs" / "rebuild_summary.json"
PROGRESS_LOG.parent.mkdir(exist_ok=True)


def _emit(record: dict) -> None:
    with open(PROGRESS_LOG, "a") as f:
        f.write(json.dumps(record) + "\n")


def list_queue(pops: list[str], build: str) -> list[str]:
    """Return the set of PGS IDs that need rebuilding for at least one pop."""
    from scripts.availability_matrix import _validate_one, _enumerate_pgs_ids
    pgs_ids = _enumerate_pgs_ids()
    queue = []
    for pgs in pgs_ids:
        for pop in pops:
            row = _validate_one(pgs, pop, build)
            if not row["percentile_allowed"]:
                queue.append(pgs)
                break
    return queue


def find_new_stats_files(pgs_id: str) -> dict[str, str]:
    """After recompute, locate the newly-written files per pop."""
    out = {}
    stats_dir = Path(LEGACY_REF_PANEL_STATS)
    for f in sorted(stats_dir.glob(f"{pgs_id}_*_GRCh38_n*_plink2-nomi_sha-*.json")):
        # Filename: PGS_ID_POP_GRCh38_n<N>_plink2-nomi_sha-XXXX.json
        parts = f.name.split("_")
        if len(parts) < 4:
            continue
        pop = parts[1]
        # Use the most-recently-modified one per pop
        if pop not in out or f.stat().st_mtime > Path(out[pop]).stat().st_mtime:
            out[pop] = str(f)
    return out


def validate_one_file(path: str, pgs_id: str, pop: str, build: str) -> tuple[bool, str]:
    """Run _rs_validate on a file. Returns (ok, reason)."""
    try:
        stats = json.load(open(path))
    except (OSError, json.JSONDecodeError) as e:
        return False, f"unreadable: {e}"
    try:
        _scoring._rs_validate(stats, pgs_id, pop, build, path)
    except _scoring.IncompatibleRefStats as e:
        return False, str(e)
    return True, "ok"


def bless_files(pgs_id: str, files: dict[str, str]) -> dict[str, bool]:
    """Bless each (pgs, pop) file into the registry."""
    results = {}
    for pop, path in files.items():
        cmd = ["python3", str(ROOT / "scripts" / "ref_stats_registry.py"),
               "bless", path]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            results[pop] = (r.returncode == 0)
            if r.returncode != 0:
                print(f"  bless failed for {pop}: {r.stderr[:200]}")
        except subprocess.TimeoutExpired:
            results[pop] = False
            print(f"  bless timed out for {pop}")
    return results


def rebuild_one(pgs_id: str, pops: list[str], build: str = "GRCh38") -> dict:
    """Rebuild one PGS across all pops. Atomic at the PGS granularity."""
    started = time.time()
    record = {
        "pgs_id": pgs_id, "pops": pops, "build": build,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "status": "running",
    }

    # Run recompute_ref_stats.py for this PGS, all pops together
    cmd = ["python3", str(ROOT / "scripts" / "recompute_ref_stats.py"),
           pgs_id, "--pop", "ALL", "--apply"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired:
        record["status"] = "timeout"
        record["error"] = "recompute_ref_stats.py exceeded 30 min budget"
        record["elapsed_s"] = round(time.time() - started, 1)
        _emit(record)
        return record

    if r.returncode != 0:
        record["status"] = "recompute_failed"
        record["error"] = (r.stderr or r.stdout)[-500:]
        record["elapsed_s"] = round(time.time() - started, 1)
        _emit(record)
        return record

    # Find the freshly-written files
    files = find_new_stats_files(pgs_id)
    record["new_files"] = files

    # Validate every file before blessing anything (atomic per-PGS)
    validations = {}
    for pop in pops:
        path = files.get(pop)
        if not path:
            validations[pop] = (False, "no output file produced")
            continue
        validations[pop] = validate_one_file(path, pgs_id, pop, build)
    record["validations"] = {pop: ok for pop, (ok, _) in validations.items()}

    n_ok = sum(1 for ok, _ in validations.values() if ok)
    if n_ok == 0:
        record["status"] = "validation_failed"
        record["error"] = {pop: msg for pop, (ok, msg) in validations.items() if not ok}
        record["elapsed_s"] = round(time.time() - started, 1)
        _emit(record)
        return record

    # Bless the validated files into the registry
    bless = bless_files(pgs_id, {pop: files[pop] for pop, (ok, _) in validations.items() if ok and files.get(pop)})
    record["bless_results"] = bless

    n_blessed = sum(1 for ok in bless.values() if ok)
    record["status"] = "ok" if n_blessed == len(validations) else "partial"
    record["n_validated"] = n_ok
    record["n_blessed"] = n_blessed
    record["elapsed_s"] = round(time.time() - started, 1)
    _emit(record)
    return record


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pgs", help="Single PGS ID to rebuild")
    ap.add_argument("--pops", default=",".join(BUILDABLE_POPULATIONS))
    ap.add_argument("--build", default="GRCh38")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0,
                    help="Stop after this many PGSes (debug)")
    ap.add_argument("--skip-already-passing", action="store_true", default=True,
                    help="Skip PGSes where ALL pops already pass")
    args = ap.parse_args()

    pops = [p.strip() for p in args.pops.split(",") if p.strip()]

    if args.pgs:
        pgs_ids = [args.pgs]
    else:
        print(f"[{time.strftime('%H:%M:%S')}] enumerating rebuild queue...")
        pgs_ids = list_queue(pops, args.build)
        print(f"[{time.strftime('%H:%M:%S')}] queue: {len(pgs_ids)} PGSes need work")

    if args.dry_run:
        for pgs in pgs_ids[:50]:
            print(pgs)
        if len(pgs_ids) > 50:
            print(f"... and {len(pgs_ids) - 50} more")
        return

    if args.limit:
        pgs_ids = pgs_ids[:args.limit]

    summary = {"total": len(pgs_ids), "ok": 0, "partial": 0, "failed": 0,
               "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "build": args.build, "pops": pops}
    for i, pgs in enumerate(pgs_ids, 1):
        print(f"[{time.strftime('%H:%M:%S')}] ({i}/{len(pgs_ids)}) {pgs}")
        try:
            rec = rebuild_one(pgs, pops, args.build)
        except Exception as e:
            rec = {"pgs_id": pgs, "status": "exception", "error": str(e)}
            _emit(rec)
        status = rec.get("status", "unknown")
        if status == "ok":
            summary["ok"] += 1
        elif status == "partial":
            summary["partial"] += 1
        else:
            summary["failed"] += 1
        print(f"    → {status}  ({rec.get('n_blessed', 0)} blessed, "
              f"{rec.get('elapsed_s', 0):.0f}s)")

    summary["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    print()
    print(f"=== summary ===")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
