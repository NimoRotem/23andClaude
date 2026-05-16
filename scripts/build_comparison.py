#!/usr/bin/env python3
"""Build a profile × PGS comparison table (CSV + Markdown summary).

Output:
    logs/batch_score_comparison.csv       — full table
    logs/batch_score_comparison.md        — pretty Markdown rendering
    logs/batch_score_progress_summary.txt — counts
"""
from __future__ import annotations

import csv, json, os, sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, "/home/nimrod_rotem/simple-genomics")
from test_registry import TESTS, CURATED_IDS, COMMON_PGS_IDS

USER_ROOT = Path("/home/nimrod_rotem/simple-genomics/users/760bf12315642a1e")
REPORTS_ROOT = USER_ROOT / "reports"
LOGS_ROOT = Path("/home/nimrod_rotem/simple-genomics/logs")

profiles = json.load(open(USER_ROOT / "profiles.json"))["profiles"]
files    = json.load(open(USER_ROOT / "files.json"))["files"]

# Build PGS metadata
wanted = CURATED_IDS | COMMON_PGS_IDS
tests = sorted(
    [t for t in TESTS if t.get("test_type") == "pgs_score" and t["id"] in wanted],
    key=lambda t: (t.get("category", ""), t["params"]["pgs_id"]),
)
pgs_info = {t["params"]["pgs_id"]: t for t in tests}

# Per-profile most-recent meaningful report per PGS
def is_meaningful(res):
    pct = res.get("percentile") if isinstance(res, dict) else None
    if pct is None:
        return False
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return False
    return 0.5 < p < 99.5

results = defaultdict(dict)  # profile_name -> pgs_id -> result_dict
for pid, pr in profiles.items():
    name = pr.get("name")
    for fid in pr.get("file_ids", []):
        rdir = REPORTS_ROOT / fid
        if not rdir.is_dir():
            continue
        for p in rdir.glob("pgs_*.json"):
            try:
                d = json.load(open(p))
            except Exception:
                continue
            r = d.get("result") or {}
            pgs_id = r.get("pgs_id") or (r.get("pipeline_info") or {}).get("pgs_catalog_id")
            if not pgs_id or pgs_id not in pgs_info:
                continue
            prev = results[name].get(pgs_id)
            # Prefer meaningful, then most-recent
            cur_meaningful = is_meaningful(r)
            prev_meaningful = is_meaningful((prev or {}).get("result", {})) if prev else False
            if prev is None:
                results[name][pgs_id] = {"result": r, "completed_at": d.get("completed_at"), "path": str(p)}
            elif (cur_meaningful and not prev_meaningful):
                results[name][pgs_id] = {"result": r, "completed_at": d.get("completed_at"), "path": str(p)}
            elif (cur_meaningful == prev_meaningful) and (d.get("completed_at") or "") > (prev.get("completed_at") or ""):
                results[name][pgs_id] = {"result": r, "completed_at": d.get("completed_at"), "path": str(p)}

profile_order = ["Nimo", "Chichi", "Mina", "Efi", "SZ7A76M9LNU",
                 "B2XH", "B2XH_cycle3", "B3XH", "B3XH_cycle3",
                 "B4XH", "B6XH", "B8XH"]
profile_order = [p for p in profile_order if p in results or p in [pr.get("name") for pr in profiles.values()]]

# CSV
LOGS_ROOT.mkdir(exist_ok=True)
csv_path = LOGS_ROOT / "batch_score_comparison.csv"
with open(csv_path, "w") as fh:
    w = csv.writer(fh)
    header = ["pgs_id", "test_name", "category", "trait"]
    for p in profile_order:
        header.extend([f"{p}_pct", f"{p}_z", f"{p}_match", f"{p}_ref", f"{p}_status"])
    w.writerow(header)
    for t in tests:
        pgs_id = t["params"]["pgs_id"]
        row = [pgs_id, t["name"], t.get("category", ""), t["params"].get("trait", "")]
        for p in profile_order:
            r = results.get(p, {}).get(pgs_id, {}).get("result", {})
            pct = r.get("percentile")
            mr = r.get("match_rate_value")
            ref = r.get("selected_ref") or ""
            status = r.get("status") or ""
            z = (r.get("scoring_diagnostics") or {}).get("z_score")
            row.extend([
                pct if pct is not None else "",
                z if z is not None else "",
                mr if mr is not None else "",
                ref,
                status,
            ])
        w.writerow(row)
print(f"wrote {csv_path}")

# Pretty markdown — percentile only, one cell per (profile, PGS)
md_path = LOGS_ROOT / "batch_score_comparison.md"
with open(md_path, "w") as fh:
    fh.write("# Profile × PGS Comparison\n\n")
    fh.write(f"Generated from {sum(len(v) for v in results.values())} reports across "
             f"{len(profile_order)} profiles and {len(tests)} PGSes.\n\n")
    fh.write("Cells are percentile (z-score). Empty = no report. "
             "0.5/99.5 = clamped extreme. — = score computed but no percentile (e.g. ANCESTRY_UNRESOLVED).\n\n")
    cats = sorted({t.get("category", "Other") for t in tests})
    for cat in cats:
        cat_tests = [t for t in tests if t.get("category", "Other") == cat]
        if not cat_tests:
            continue
        fh.write(f"\n## {cat}\n\n")
        fh.write("| PGS | Trait | " + " | ".join(profile_order) + " |\n")
        fh.write("|---|---|" + "|".join(["---"] * len(profile_order)) + "|\n")
        for t in cat_tests:
            pgs_id = t["params"]["pgs_id"]
            cells = [pgs_id, (t["params"].get("trait", "") or t["name"])[:38]]
            for p in profile_order:
                r = results.get(p, {}).get(pgs_id, {}).get("result", {})
                pct = r.get("percentile")
                z = (r.get("scoring_diagnostics") or {}).get("z_score")
                if pct is None:
                    cells.append("—")
                else:
                    z_str = f" (z={z:+.2f})" if isinstance(z, (int, float)) else ""
                    cells.append(f"**{pct}**{z_str}")
            fh.write("| " + " | ".join(cells) + " |\n")
print(f"wrote {md_path}")

# Summary counts
summary_path = LOGS_ROOT / "batch_score_progress_summary.txt"
with open(summary_path, "w") as fh:
    n_total = 0
    n_meaningful = 0
    by_profile = defaultdict(lambda: [0, 0])
    by_pgs = defaultdict(lambda: [0, 0])
    for p in profile_order:
        for t in tests:
            n_total += 1
            r = results.get(p, {}).get(t["params"]["pgs_id"], {}).get("result", {})
            if is_meaningful(r):
                n_meaningful += 1
                by_profile[p][0] += 1
                by_pgs[t["params"]["pgs_id"]][0] += 1
            elif r:
                by_profile[p][1] += 1
                by_pgs[t["params"]["pgs_id"]][1] += 1
    fh.write(f"Total combinations: {n_total}\n")
    fh.write(f"Meaningful percentile: {n_meaningful} ({100*n_meaningful/max(n_total,1):.1f}%)\n")
    fh.write("\nPer-profile coverage:\n")
    for p in profile_order:
        m, b = by_profile[p]
        fh.write(f"  {p:14s}  meaningful={m:3d}  bad={b:3d}  missing={len(tests)-m-b:3d}\n")
    fh.write("\nPer-PGS coverage (only those with any bad/missing):\n")
    for pgs_id in sorted(by_pgs):
        m, b = by_pgs[pgs_id]
        missing = len(profile_order) - m - b
        if missing > 0 or b > 0:
            name = pgs_info.get(pgs_id, {}).get("name", "?")[:50]
            fh.write(f"  {pgs_id}  meaningful={m:2d}/{len(profile_order)}  bad={b}  missing={missing}  ({name})\n")

print(f"wrote {summary_path}")
print()
print(open(summary_path).read())
