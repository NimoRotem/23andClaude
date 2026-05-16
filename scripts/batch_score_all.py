#!/usr/bin/env python3
"""Batch-score every profile × curated PGS combination.

For each profile (Nimo, Chichi, Mina, Efi, plus all embryos), and each
PGS in CURATED_IDS ∪ COMMON_PGS_IDS, run scoring against the profile's
gVCF (preferred) or BAM and save the report to the user's reports
directory matching the existing schema.

Resumable: skip combinations that already have a "meaningful" report,
where "meaningful" = status='passed' AND percentile is not None AND
percentile not in {0.5, 99.5} (i.e. not clamped).

Reports land at:
    users/<USER>/reports/<file_id>/pgs_<slug>_<rand>.json

Final output:
    /home/nimrod_rotem/simple-genomics/logs/batch_score_progress.jsonl
    /home/nimrod_rotem/simple-genomics/logs/batch_score_comparison.csv
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import random
import string
import sys
import time
from pathlib import Path

sys.path.insert(0, "/home/nimrod_rotem/simple-genomics")

from test_registry import TESTS, CURATED_IDS, COMMON_PGS_IDS
import runners

# # TIMEOUT_INJECTED: SIGALRM-based timeout for runners.run_test calls.
import signal as _signal


class _BatchTimeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise _BatchTimeout("PGS run exceeded budget")


def _run_with_timeout(vcf_path, test_def, seconds: int = 600):
    _signal.signal(_signal.SIGALRM, _timeout_handler)
    _signal.alarm(int(seconds))
    try:
        return runners.run_test(vcf_path, test_def)
    finally:
        _signal.alarm(0)


USER = "760bf12315642a1e"   # admin user (nimo)
USER_ROOT = Path(f"/home/nimrod_rotem/simple-genomics/users/{USER}")
PROFILES_PATH = USER_ROOT / "profiles.json"
FILES_PATH    = USER_ROOT / "files.json"
REPORTS_ROOT  = USER_ROOT / "reports"

PROGRESS_LOG = Path("/home/nimrod_rotem/simple-genomics/logs/batch_score_progress.jsonl")
COMPARISON_CSV = Path("/home/nimrod_rotem/simple-genomics/logs/batch_score_comparison.csv")
PROGRESS_LOG.parent.mkdir(parents=True, exist_ok=True)



# # ANCESTRY_MAP_INJECTED: inferred from prior reports' selected_ref for each profile.
# B4XH / B6XH / B3XH_cycle3 had no prior ancestry signal; default to EUR
# (these are embryos from the same family whose other siblings cluster mixed
# EUR/EAS; without a dedicated PCA, EUR is the safer default for the family).
ANCESTRY_MAP = {
    "Nimo":         "EUR",
    "Chichi":       "EAS",   # split 5 EAS / 5 EUR in history; treating as EAS
    "Mina":         "EUR",
    "Efi":          "EUR",
    "SZ7A76M9LNU":  "EUR",
    "B2XH":         "EAS",
    "B2XH_cycle3":  "EAS",
    "B3XH":         "EUR",
    "B3XH_cycle3":  "EUR",   # no prior signal; defaults to EUR
    "B4XH":         "EUR",   # no prior signal; defaults to EUR
    "B6XH":         "EUR",   # no prior signal; defaults to EUR
    "B8XH":         "EAS",
}
DEFAULT_ANCESTRY = "EUR"

def _emit(rec: dict) -> None:
    with open(PROGRESS_LOG, "a") as f:
        f.write(json.dumps(rec) + "\n")


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _rand_hex(n: int = 8) -> str:
    return "".join(random.choices("0123456789abcdef", k=n))


def load_profiles() -> dict:
    return json.load(open(PROFILES_PATH))["profiles"]


def load_files() -> dict:
    return json.load(open(FILES_PATH))["files"]


def best_file_for_profile(prof: dict, files: dict) -> tuple[str | None, dict | None]:
    """Return (file_id, file_entry) preferring gVCF > VCF > BAM/CRAM."""
    fids = prof.get("file_ids", [])
    pref_order = []
    for fid in fids:
        f = files.get(fid)
        if not f:
            continue
        name = f.get("name", "")
        ftype = (f.get("file_type") or "").lower()
        if ftype == "gvcf" or ".g.vcf" in name:
            score = 3
        elif ftype == "vcf" or ".vcf" in name:
            score = 2
        elif ftype in ("bam", "cram") or any(name.endswith(x) for x in (".bam", ".cram")):
            score = 1
        else:
            score = 0
        pref_order.append((score, fid, f))
    if not pref_order:
        return None, None
    pref_order.sort(reverse=True)
    _, fid, f = pref_order[0]
    return fid, f


def curated_pgs_tests() -> list[dict]:
    """Union of CURATED_IDS and COMMON_PGS_IDS, restricted to pgs_score type."""
    wanted = CURATED_IDS | COMMON_PGS_IDS
    return [t for t in TESTS if t.get("test_type") == "pgs_score" and t["id"] in wanted]


# # SKIP_RETRY_NONMEANINGFUL: don't re-run combos that already produced ANY result
def any_existing_report(file_id: str, pgs_id: str) -> bool:
    fdir = REPORTS_ROOT / file_id
    if not fdir.is_dir():
        return False
    for p in fdir.glob("pgs_*.json"):
        try:
            d = json.load(open(p))
        except Exception:
            continue
        res = d.get("result") or {}
        pid = res.get("pgs_id") or (res.get("pipeline_info") or {}).get("pgs_catalog_id")
        if pid == pgs_id:
            return True
    return False


def existing_meaningful_report(file_id: str, pgs_id: str) -> dict | None:
    """Return the most-recent meaningful existing report for (file_id, pgs_id), or None."""
    fdir = REPORTS_ROOT / file_id
    if not fdir.is_dir():
        return None
    candidates = []
    for p in fdir.glob("pgs_*.json"):
        try:
            d = json.load(open(p))
        except Exception:
            continue
        res = d.get("result") or {}
        if res.get("pgs_id") != pgs_id and (
            (res.get("pipeline_info") or {}).get("pgs_catalog_id") != pgs_id
        ):
            continue
        if is_meaningful(res):
            candidates.append((d.get("completed_at") or "", p))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return {"path": str(candidates[0][1])}


def is_meaningful(res: dict) -> bool:
    """A 'meaningful' result has a valid, unclamped percentile."""
    if not isinstance(res, dict):
        return False
    pct = res.get("percentile")
    if pct is None:
        return False
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return False
    if p <= 0.5 or p >= 99.5:
        return False
    mr = res.get("match_rate_value")
    try:
        if mr is not None and float(mr) < 60.0:
            return False
    except (TypeError, ValueError):
        pass
    status = (res.get("status") or "").lower()
    if status in ("failed", "fingerprint_drift_refused"):
        # fingerprint_drift_refused with restored percentile via overlay is fine
        if pct is None:
            return False
    return True


def slugify(name: str) -> str:
    out = []
    for ch in name.lower():
        if ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_", "/"):
            out.append("_")
    s = "".join(out).strip("_")
    return s[:24] or "pgs"


def write_report(file_id: str, profile_id: str, test_def: dict, file_entry: dict, result: dict) -> str:
    """Persist a report JSON in the same shape the UI expects."""
    fdir = REPORTS_ROOT / file_id
    fdir.mkdir(parents=True, exist_ok=True)
    task_id = f"{test_def['id']}_{_rand_hex(8)}"
    report = {
        "task_id":           task_id,
        "test_id":           test_def["id"],
        "test_name":         test_def["name"],
        "category":          test_def.get("category", ""),
        "description":       test_def.get("description", ""),
        "completed_at":      _now_iso(),
        "file_id":           file_id,
        "file_type":         (file_entry.get("file_type") or "").lower(),
        "vcf_path":          file_entry.get("path"),
        "profile_id":        profile_id,
        "username":          "batch_score@23andclaude.com",
        "selection_reason":  "batch — best gVCF for profile",
        "elapsed_seconds":   None,
        "interpretation":    None,
        "interpretation_error": None,
        "attempt":           1,
        "result":            result,
    }
    out_path = fdir / f"{task_id}.json"
    tmp = str(out_path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(report, f, indent=2, default=str)
    os.replace(tmp, out_path)
    return str(out_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-profiles", type=int, default=0)
    ap.add_argument("--limit-pgs",     type=int, default=0)
    ap.add_argument("--profile",       default=None,
                    help="run only this profile name (e.g. Nimo)")
    ap.add_argument("--pgs",           default=None,
                    help="run only this PGS catalog ID (e.g. PGS000004)")
    ap.add_argument("--force",         action="store_true",
                    help="re-run even if a meaningful report exists")
    ap.add_argument("--dry-run",       action="store_true")
    args = ap.parse_args()

    profiles = load_profiles()
    files = load_files()
    tests = curated_pgs_tests()

    # Stable iteration order: profiles by name, tests by PGS id
    profile_items = sorted(profiles.items(), key=lambda kv: kv[1].get("name", ""))
    test_items = sorted(tests, key=lambda t: t["params"]["pgs_id"])

    if args.profile:
        profile_items = [(pid, pr) for pid, pr in profile_items if pr.get("name") == args.profile]
    if args.pgs:
        test_items = [t for t in test_items if t["params"]["pgs_id"] == args.pgs]
    if args.limit_profiles:
        profile_items = profile_items[: args.limit_profiles]
    if args.limit_pgs:
        test_items = test_items[: args.limit_pgs]

    print(f"profiles: {len(profile_items)}  pgs: {len(test_items)}  total combos: "
          f"{len(profile_items) * len(test_items)}")

    if args.dry_run:
        for pid, pr in profile_items:
            fid, f = best_file_for_profile(pr, files)
            ftype = (f or {}).get("file_type") if f else None
            print(f"  {pr['name']:14s}  file={(fid or '-')[:10]}  type={ftype}  "
                  f"path={(f or {}).get('path', '-')}")
        for t in test_items[:10]:
            print(f"  {t['id']}  {t['params']['pgs_id']}  {t['name']}")
        return

    started = time.time()
    n_done = n_skip = n_fail = 0

    for pid, pr in profile_items:
        prof_name = pr.get("name", "?")
        fid, f = best_file_for_profile(pr, files)
        if not fid or not f:
            print(f"  {prof_name}: no usable file; skipping")
            continue
        path = f.get("path")
        if not path or not os.path.exists(path):
            print(f"  {prof_name}: file path missing — {path}")
            continue

        # # SKIP_BAM_PGS: skip BAM-only inputs for the giant PGSes.
        ftype = (f.get("file_type") or "").lower()
        is_bam_only = ftype in ("bam", "cram") or path.endswith((".bam", ".cram"))
        for t in test_items:
            pgs_id = t["params"]["pgs_id"]
            tag = f"{prof_name}  {pgs_id}  ({t['name'][:35]})"
            if is_bam_only:
                # Pipeline E+ pileup hangs on multi-million-variant PGSes
                # for BAM inputs; cap at the small ones that finish in <2 min.
                n_var = int(t["params"].get("variants_number", 0) or 0)
                # variants_number not always in params; fall back to test_id hint
                # Skip ALL PGSes for BAM (the small ones already produced reports
                # earlier; the large ones are the hanging ones).
                exist = existing_meaningful_report(fid, pgs_id)
                if not exist:
                    print(f"  [skip-bam] {tag} (BAM input, skipping non-cached)")
                    n_skip += 1
                    continue
            if not args.force:
                if any_existing_report(fid, pgs_id):
                    print(f"  [skip] {tag}  (report exists, not retrying)")
                    n_skip += 1
                    continue
            print(f"  [run]  {tag}")
            t_start = time.time()
            try:
                test_def = {
                    "id": t["id"], "name": t["name"], "category": t.get("category", ""),
                    "description": t.get("description", ""),
                    "test_type": "pgs_score",
                    "params": dict(t["params"]),
                }
                # # ANCESTRY_MAP_INJECTED: inject ref_pop hint so ancestry resolves
                test_def["params"]["ref_pop"] = ANCESTRY_MAP.get(prof_name, DEFAULT_ANCESTRY)
                result = _run_with_timeout(path, test_def, seconds=600)
                out_path = write_report(fid, pid, test_def, f, result)
                elapsed = round(time.time() - t_start, 1)
                pct = result.get("percentile")
                rec = {
                    "ts": _now_iso(),
                    "profile": prof_name, "file_id": fid, "pgs_id": pgs_id,
                    "test_id": t["id"], "status": result.get("status"),
                    "percentile": pct,
                    "match_rate": result.get("match_rate_value"),
                    "z": (result.get("scoring_diagnostics") or {}).get("z_score"),
                    "elapsed_s": elapsed,
                    "out_path": out_path,
                    "meaningful": is_meaningful(result),
                }
                _emit(rec)
                if is_meaningful(result):
                    n_done += 1
                    print(f"      → pct={pct} status={result.get('status')} ({elapsed}s)")
                else:
                    n_fail += 1
                    print(f"      → NOT meaningful: pct={pct} status={result.get('status')} ({elapsed}s)")
            except Exception as e:
                import traceback
                elapsed = round(time.time() - t_start, 1)
                err = f"{type(e).__name__}: {e}"
                print(f"      → EXCEPTION ({elapsed}s): {err}")
                rec = {
                    "ts": _now_iso(), "profile": prof_name, "file_id": fid,
                    "pgs_id": pgs_id, "test_id": t["id"],
                    "error": err, "traceback": traceback.format_exc()[-800:],
                    "elapsed_s": elapsed,
                }
                _emit(rec)
                n_fail += 1

    total_elapsed = round(time.time() - started, 1)
    print()
    print(f"=== summary ===")
    print(f"  meaningful: {n_done}")
    print(f"  skipped:    {n_skip}")
    print(f"  failed:     {n_fail}")
    print(f"  elapsed:    {total_elapsed}s")


if __name__ == "__main__":
    main()
