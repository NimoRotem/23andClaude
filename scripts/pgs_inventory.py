#!/usr/bin/env python3
"""Generate a nightly inventory of PGS ref-stats coverage and freshness.

For each (pgs_id, population) pair currently registered, emit:
  - n_scoring     (variants in the catalog scoring file for the PGS)
  - n_stats       (variants the cached stats were computed on)
  - coverage_pct  (n_stats / n_scoring × 100)
  - schema        (v1 / pre-v1)
  - status        (OK / STALE / MISSING / INCOMPATIBLE)

Writes a Markdown report to reports/pgs_inventory.md plus a JSON file at
reports/pgs_inventory.json (machine-readable). Designed to be cron-friendly.
"""
from __future__ import annotations

import datetime
import gzip
import hashlib
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

_THIS = Path(__file__).resolve()
_PKG_ROOT = _THIS.parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))

from pipeline import registry as _registry  # noqa: E402

STATS_DIR = Path("/data/pgs2/ref_panel_stats")
CATALOG_DIR = Path("/data/pgs_cache")
REPORTS_DIR = _PKG_ROOT / "reports"


def _catalog_n_variants(pgs_id: str) -> int | None:
    p = CATALOG_DIR / pgs_id / f"{pgs_id}_hmPOS_GRCh38.txt.gz"
    if not p.exists():
        return None
    try:
        with gzip.open(p, "rt") as f:
            count = 0
            for line in f:
                if line.startswith("#"):
                    continue
                count += 1
            return count
    except OSError:
        return None


def _build():
    reg = _registry._load()
    entries = reg.get("entries", [])
    rows = []

    by_pgs = defaultdict(list)
    for e in entries:
        by_pgs[e["pgs_id"]].append(e)

    for pgs_id in sorted(by_pgs):
        n_scoring = _catalog_n_variants(pgs_id)
        for e in sorted(by_pgs[pgs_id], key=lambda x: x["population"]):
            stats_path = STATS_DIR / e["filename"]
            n_stats = e.get("n_variants")
            if n_scoring and n_stats:
                cov = round(100.0 * n_stats / n_scoring, 1)
            else:
                cov = None
            if not stats_path.exists():
                status = "MISSING"
            elif n_scoring is None:
                status = "UNKNOWN_CATALOG"
            elif n_stats != n_scoring:
                status = "STALE" if cov is not None and cov < 95 else "MINOR_DRIFT"
            else:
                status = "OK"
            rows.append({
                "pgs_id": pgs_id,
                "population": e["population"],
                "n_scoring": n_scoring,
                "n_stats": n_stats,
                "coverage_pct": cov,
                "filename": e["filename"],
                "schema": "v1",
                "status": status,
                "blessed_at": e.get("blessed_at"),
            })

    # Also surface PGS ids that have NO registry entry but DO have a catalog file.
    catalog_ids = set()
    if CATALOG_DIR.exists():
        for d in CATALOG_DIR.iterdir():
            if d.is_dir() and d.name.startswith("PGS"):
                catalog_ids.add(d.name)
    registry_ids = set(by_pgs.keys())
    for pgs_id in sorted(catalog_ids - registry_ids):
        rows.append({
            "pgs_id": pgs_id,
            "population": None,
            "n_scoring": _catalog_n_variants(pgs_id),
            "n_stats": None,
            "coverage_pct": None,
            "filename": None,
            "schema": None,
            "status": "UNREGISTERED",
            "blessed_at": None,
        })

    return rows


def _write_markdown(rows, path):
    counts = defaultdict(int)
    for r in rows:
        counts[r["status"]] += 1
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")

    with open(path, "w") as f:
        f.write(f"# PGS ref-stats inventory\n\n_generated: {now}_\n\n")
        f.write("## Status summary\n\n")
        for status, n in sorted(counts.items()):
            f.write(f"- **{status}**: {n}\n")
        f.write("\n## Rows\n\n")
        f.write("| PGS | pop | n_scoring | n_stats | coverage | schema | status | blessed_at |\n")
        f.write("|-----|-----|----------:|--------:|---------:|--------|--------|------------|\n")
        for r in rows:
            f.write(f"| {r['pgs_id']} "
                    f"| {r['population'] or '—'} "
                    f"| {r['n_scoring'] if r['n_scoring'] is not None else '—'} "
                    f"| {r['n_stats'] if r['n_stats'] is not None else '—'} "
                    f"| {r['coverage_pct'] if r['coverage_pct'] is not None else '—'} "
                    f"| {r['schema'] or '—'} "
                    f"| {r['status']} "
                    f"| {(r['blessed_at'] or '')[:19]} |\n")


def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = _build()
    md_path = REPORTS_DIR / "pgs_inventory.md"
    json_path = REPORTS_DIR / "pgs_inventory.json"
    _write_markdown(rows, md_path)
    with open(json_path, "w") as f:
        json.dump({
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "rows": rows,
        }, f, indent=2)
    counts = defaultdict(int)
    for r in rows:
        counts[r["status"]] += 1
    print(f"wrote {md_path}")
    print(f"wrote {json_path}")
    print("status counts:", dict(counts))


if __name__ == "__main__":
    main()
