#!/usr/bin/env python3
"""Audit PGS tests against precomputed reference stats.

Uses the actual _load_precomputed_stats() from runners.py to determine
which PGS tests have valid precomputed EUR GRCh38 stats.

Writes pgs_stats_audit.json with classification into:
  - precomputed_ok: valid GRCh38 EUR stats with healthy std
  - precomputed_stale: stats exist but wrong build or collapsed std
  - missing: no stats file found by the loader
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_registry import TESTS
from runners import _load_precomputed_stats, REF_PANEL_STATS

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pgs_stats_audit.json")


def _validate_stats_file(stats_file):
    """Extra validation beyond what _load_precomputed_stats does."""
    path = os.path.join(REF_PANEL_STATS, stats_file)
    try:
        with open(path) as f:
            stats = json.load(f)
    except Exception as e:
        return False, {"reason": f"JSON parse error: {e}"}

    build = stats.get("genome_build", "")
    n_samples = stats.get("n_samples", 0)
    mean = stats.get("mean", 0)
    std = stats.get("std", 0)

    issues = []
    if build != "GRCh38":
        issues.append(f"genome_build={build}, expected GRCh38")
    if n_samples < 50:
        issues.append(f"n_samples={n_samples} (minimum 50)")
    if mean != 0 and std > 0 and (std / abs(mean)) < 0.001:
        issues.append(f"std/|mean| ratio = {std/abs(mean):.6f} (< 0.001)")

    return (not issues), {
        "mean": mean, "std": std, "n_samples": n_samples,
        "genome_build": build, "issues": issues,
    }


def main():
    pgs_tests = []
    seen = set()
    for t in TESTS:
        tt = t.get("test_type", "")
        if tt not in ("pgs_score", "rsid_pgs_score"):
            continue
        pgs_id = t.get("params", {}).get("pgs_id", "")
        if not pgs_id or pgs_id in seen:
            continue
        seen.add(pgs_id)
        pgs_tests.append({"test_id": t["id"], "pgs_id": pgs_id,
                           "test_type": tt, "name": t.get("name", "")})

    results = {}
    counts = {"precomputed_ok": 0, "precomputed_stale": 0, "missing": 0}

    for entry in sorted(pgs_tests, key=lambda x: x["pgs_id"]):
        pgs_id = entry["pgs_id"]
        loaded = _load_precomputed_stats(pgs_id)

        if loaded is None:
            results[pgs_id] = {
                "status": "missing",
                "reason": "No stats file found",
                "test_id": entry["test_id"],
                "test_type": entry["test_type"],
            }
            counts["missing"] += 1
            continue

        mean, std, stats_file = loaded
        ok, info = _validate_stats_file(stats_file)

        if ok:
            results[pgs_id] = {
                "status": "precomputed_ok",
                "stats_file": stats_file,
                "mean": mean,
                "std": std,
                "n_samples": info["n_samples"],
                "test_id": entry["test_id"],
                "test_type": entry["test_type"],
            }
            counts["precomputed_ok"] += 1
        else:
            results[pgs_id] = {
                "status": "precomputed_stale",
                "stats_file": stats_file,
                "reason": "; ".join(info.get("issues", ["unknown"])),
                "test_id": entry["test_id"],
                "test_type": entry["test_type"],
            }
            counts["precomputed_stale"] += 1

    audit = {
        "audit_date": datetime.now(timezone.utc).isoformat(),
        "stats_dir": REF_PANEL_STATS,
        "summary": {
            "precomputed_ok": counts["precomputed_ok"],
            "precomputed_stale": counts["precomputed_stale"],
            "missing": counts["missing"],
            "total": len(results),
        },
        "results": results,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(audit, f, indent=2)

    print(f"PGS Stats Audit — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Stats directory: {REF_PANEL_STATS}")
    print(f"Total unique PGS IDs: {len(results)}")
    print(f"  precomputed_ok:    {counts['precomputed_ok']}")
    print(f"  precomputed_stale: {counts['precomputed_stale']}")
    print(f"  missing:           {counts['missing']}")

    stale = [k for k, v in results.items() if v["status"] == "precomputed_stale"]
    if stale:
        print("\nStale stats:")
        for pid in sorted(stale):
            r = results[pid]
            print(f"  {pid}: {r['reason']}")

    print(f"\nAudit written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
