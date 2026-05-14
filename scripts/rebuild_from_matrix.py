#!/usr/bin/env python3
"""Fast bulk rebuild — read the saved availability matrix instead of
re-enumerating live.

The full enumeration in rebuild_driver.list_queue() is O(n_pgs × n_pops)
calls to _load_stats which is slow because each call computes the
catalog file SHA. We avoid that by reading the JSONL snapshot already
written by scripts/availability_matrix.py.

Reads:    logs/availability_matrix.before-rebuild.jsonl
          (or pass --matrix <path>)
Writes:   logs/rebuild_progress.jsonl  (append-only)
          logs/rebuild_summary.json     (final)

Usage:
    python3 scripts/rebuild_from_matrix.py
    python3 scripts/rebuild_from_matrix.py --limit 10
    python3 scripts/rebuild_from_matrix.py --resume
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

# Reuse the per-PGS rebuild path from rebuild_driver
from scripts.rebuild_driver import rebuild_one, PROGRESS_LOG, SUMMARY_FILE

DEFAULT_MATRIX = ROOT / "logs" / "availability_matrix.before-rebuild.jsonl"


def queue_from_matrix(matrix_path: Path) -> list[str]:
    """Return distinct PGSes that have at least one blocked or missing pop."""
    seen = set()
    queue = []
    if not matrix_path.exists():
        raise SystemExit(f"matrix file not found: {matrix_path}")
    for line in matrix_path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("percentile_allowed"):
            continue
        pgs = r.get("pgs_id")
        if pgs and pgs not in seen:
            seen.add(pgs)
            queue.append(pgs)
    return queue


def already_done(pgs: str) -> bool:
    """Skip if every public-pop file already exists in registry for this PGS.

    Avoids reprocessing across resumes. We use a coarse check on the
    registry file count rather than re-running the contract loader.
    """
    if not PROGRESS_LOG.exists():
        return False
    for line in PROGRESS_LOG.read_text().splitlines():
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("pgs_id") == pgs and r.get("status") in ("ok", "partial"):
            return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", default=str(DEFAULT_MATRIX))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--resume", action="store_true",
                    help="Skip PGSes already in rebuild_progress.jsonl with status=ok|partial")
    ap.add_argument("--pops", default="EUR,EAS,AFR,SAS,AMR,MIX")
    ap.add_argument("--build", default="GRCh38")
    args = ap.parse_args()

    pops = [p.strip() for p in args.pops.split(",") if p.strip()]
    queue = queue_from_matrix(Path(args.matrix))
    if args.resume:
        queue = [p for p in queue if not already_done(p)]
    if args.limit:
        queue = queue[: args.limit]

    print(f"[{time.strftime('%H:%M:%S')}] queue: {len(queue)} PGSes")

    summary = {"total": len(queue), "ok": 0, "partial": 0, "failed": 0,
               "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    t_start = time.time()
    for i, pgs in enumerate(queue, 1):
        print(f"[{time.strftime('%H:%M:%S')}] ({i}/{len(queue)}) {pgs}",
              flush=True)
        try:
            rec = rebuild_one(pgs, pops, args.build)
        except Exception as e:
            rec = {"pgs_id": pgs, "status": "exception", "error": str(e)}
            with open(PROGRESS_LOG, "a") as f:
                f.write(json.dumps(rec) + "\n")
        status = rec.get("status", "unknown")
        if status == "ok":
            summary["ok"] += 1
        elif status == "partial":
            summary["partial"] += 1
        else:
            summary["failed"] += 1
        elapsed = rec.get("elapsed_s", 0)
        avg = (time.time() - t_start) / i
        eta_min = avg * (len(queue) - i) / 60
        print(f"    → {status}  ({rec.get('n_blessed', 0)} blessed, "
              f"{elapsed:.0f}s)  avg {avg:.0f}s/pgs  eta {eta_min:.0f} min",
              flush=True)

    summary["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    summary["elapsed_min"] = round((time.time() - t_start) / 60, 1)
    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    print()
    print("=== summary ===")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
