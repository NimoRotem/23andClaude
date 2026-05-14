#!/usr/bin/env python3
"""Wave 0 minimal availability matrix.

For every PGS we serve, for every supported population, on GRCh38, record:

  - stats_file_present
  - schema_valid (strict contract)
  - variant_hash_present
  - n_ref
  - n_variants
  - percentile_allowed (bool)
  - block_reason (reason code or null)

Read-only. Writes a JSON snapshot to logs/availability_matrix.jsonl and
a Markdown summary to stdout.

Usage:
    python3 scripts/availability_matrix.py
    python3 scripts/availability_matrix.py --json > out.json
    python3 scripts/availability_matrix.py --pops EUR,EAS --build GRCh38

This is the inventory the rebuild queue draws from. Per the advisor's
ch.15 W4.1 (minimal version pulled into Wave 0).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

# Allow running both from repo root and from scripts/
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_ROOT))

from pipeline import scoring as _scoring
from pipeline import registry as _registry
from pipeline.config import (
    POPULATIONS, BUILDABLE_POPULATIONS,
    REF_STATS_DIR, LEGACY_REF_PANEL_STATS, ref_stats_path,
)
from pipeline import reason_codes as RC


def _stats_file_present(pgs_id: str, pop: str, build: str) -> Dict[str, Any]:
    """Return {'present': bool, 'path': str|None, 'source': str}."""
    # Registry first
    reg_path = _registry.resolve(pgs_id, pop, build)
    if reg_path and os.path.exists(reg_path):
        return {"present": True, "path": reg_path, "source": "registry"}
    new_path = ref_stats_path(pgs_id, pop, build)
    if os.path.exists(new_path):
        return {"present": True, "path": new_path, "source": "new_store"}
    if pop == "EUR":
        legacy = _scoring._load_legacy_stats(pgs_id)
        if legacy:
            return {"present": True, "path": legacy.get("stats_file"),
                    "source": "legacy_eur_fallback"}
    return {"present": False, "path": None, "source": None}


def _validate_one(pgs_id: str, pop: str, build: str) -> Dict[str, Any]:
    """Build one matrix row."""
    present = _stats_file_present(pgs_id, pop, build)
    row = {
        "pgs_id": pgs_id, "population": pop, "build": build,
        **present,
        "schema_valid": False,
        "variant_hash_present": False,
        "n_ref": None,
        "n_variants": None,
        "scoring_method": None,
        "imputation_policy": None,
        "percentile_allowed": False,
        "block_reason": None,
    }
    if not present["present"]:
        row["block_reason"] = RC.POPULATION_STATS_MISSING
        return row

    # Load and try the strict contract.
    stats = _scoring._load_stats(pgs_id, pop, build)
    if stats is None:
        row["block_reason"] = RC.POPULATION_STATS_MISSING
        return row

    if stats.get("_incompatible_reason"):
        reason = stats["_incompatible_reason"]
        if "scoring-file content drift" in reason:
            row["block_reason"] = RC.REF_STATS_VARIANT_HASH_MISMATCH
        elif "std <= 0" in reason:
            row["block_reason"] = RC.REF_STATS_STD_NONPOSITIVE
        else:
            row["block_reason"] = RC.REF_STATS_SCHEMA_INVALID
        row["schema_valid"] = False
        # We can still surface what was readable from the file
        row["n_ref"] = stats.get("n_samples")
        row["n_variants"] = stats.get("n_variants") or stats.get("total_variants")
        row["scoring_method"] = stats.get("scoring_method")
        row["imputation_policy"] = stats.get("imputation_policy")
        row["variant_hash_present"] = bool(stats.get("variant_ids_sha256"))
        return row

    # Schema passed.
    row["schema_valid"] = True
    row["variant_hash_present"] = bool(stats.get("variant_ids_sha256"))
    row["n_ref"] = stats.get("n_samples")
    row["n_variants"] = stats.get("n_variants") or stats.get("total_variants")
    row["scoring_method"] = stats.get("scoring_method")
    row["imputation_policy"] = stats.get("imputation_policy")
    row["percentile_allowed"] = True
    return row


def _enumerate_pgs_ids() -> List[str]:
    """Union of PGS IDs across both stores. Sorted, deduplicated."""
    ids = set()
    # Legacy / registry store
    for f in Path(LEGACY_REF_PANEL_STATS).glob("PGS*.json"):
        ids.add(f.name.split("_")[0])
    # New store (directory-per-PGS layout)
    if os.path.isdir(REF_STATS_DIR):
        for d in Path(REF_STATS_DIR).iterdir():
            if d.is_dir() and d.name.startswith("PGS"):
                ids.add(d.name)
    return sorted(ids)


def build_matrix(pgs_ids: List[str], pops: List[str],
                 build: str = "GRCh38") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for pgs in pgs_ids:
        for pop in pops:
            rows.append(_validate_one(pgs, pop, build))
    return rows


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_pop: Dict[str, Dict[str, int]] = {}
    by_reason: Dict[str, int] = {}
    for r in rows:
        p = r["population"]
        b = by_pop.setdefault(p, {"total": 0, "allowed": 0, "blocked": 0, "missing": 0})
        b["total"] += 1
        if r["percentile_allowed"]:
            b["allowed"] += 1
        elif r["block_reason"] == RC.POPULATION_STATS_MISSING:
            b["missing"] += 1
        else:
            b["blocked"] += 1
        if r["block_reason"]:
            by_reason[r["block_reason"]] = by_reason.get(r["block_reason"], 0) + 1
    return {"by_pop": by_pop, "by_reason": by_reason}


def emit_markdown(rows: List[Dict[str, Any]], summary: Dict[str, Any]) -> str:
    lines = ["# Availability Matrix"]
    lines.append("")
    lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    lines.append(f"Total rows: {len(rows)}")
    lines.append("")
    lines.append("## By population")
    lines.append("")
    lines.append("| Population | Total | Allowed | Blocked | Missing |")
    lines.append("|------------|------:|--------:|--------:|--------:|")
    for pop, c in sorted(summary["by_pop"].items()):
        lines.append(f"| {pop} | {c['total']} | {c['allowed']} | "
                     f"{c['blocked']} | {c['missing']} |")
    lines.append("")
    lines.append("## By block reason")
    lines.append("")
    if summary["by_reason"]:
        lines.append("| Reason code | Count |")
        lines.append("|-------------|------:|")
        for reason, count in sorted(summary["by_reason"].items(),
                                    key=lambda kv: -kv[1]):
            lines.append(f"| {reason} | {count} |")
    else:
        lines.append("(no blocks)")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pops", default=",".join(BUILDABLE_POPULATIONS),
                    help="Comma-separated pops to evaluate")
    ap.add_argument("--build", default="GRCh38")
    ap.add_argument("--json", action="store_true",
                    help="Emit raw JSON rows (one per line)")
    ap.add_argument("--out", default=None,
                    help="Append JSONL snapshot to this file")
    ap.add_argument("--pgs", default=None,
                    help="Optional single PGS ID to inspect (debug)")
    args = ap.parse_args()

    pops = [p.strip() for p in args.pops.split(",") if p.strip()]
    pgs_ids = [args.pgs] if args.pgs else _enumerate_pgs_ids()
    rows = build_matrix(pgs_ids, pops, args.build)

    if args.out:
        snapshot_path = args.out
        Path(snapshot_path).parent.mkdir(parents=True, exist_ok=True)
        with open(snapshot_path, "a") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    if args.json:
        for r in rows:
            print(json.dumps(r))
        return

    summary = summarize(rows)
    print(emit_markdown(rows, summary))


if __name__ == "__main__":
    main()
