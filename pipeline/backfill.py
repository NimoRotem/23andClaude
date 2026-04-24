"""Batch runner for PGS ingestion and reference stats building.

Usage:
    python3 -m pipeline.backfill [--pgs PGS000005,PGS000006] [--populations EUR,EAS,MIX] [--workers 4] [--force]

Default: process all cached PGS IDs for all buildable populations.
"""
import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

# Add parent directory to path so we can import as a module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import PGS_CACHE, BUILDABLE_POPULATIONS, REF_STATS_DIR, ref_stats_path
from pipeline.ingest_pgs import ingest_pgs
from pipeline.build_ref_stats import build_ref_stats
from pipeline import db as pipeline_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pgs-backfill")

# Persistent resume log — survives restarts
RESUME_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backfill_log.jsonl")
RESUME_LOG = os.path.normpath(RESUME_LOG)


def _log_result(pgs_id: str, status: str, detail: str = ""):
    """Append one line to the persistent resume log."""
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "pgs_id": pgs_id,
        "status": status,
        "detail": detail[:500],
    }
    try:
        with open(RESUME_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


def _load_completed_ids() -> set:
    """Load PGS IDs that have already been fully processed from the resume log."""
    done = set()
    if not os.path.exists(RESUME_LOG):
        return done
    try:
        with open(RESUME_LOG) as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    if entry.get("status") == "ok":
                        done.add(entry["pgs_id"])
                except (json.JSONDecodeError, KeyError):
                    continue
    except Exception:
        pass
    return done


def _verify_build(pgs_id: str, populations: List[str]) -> tuple:
    """Post-build verification: check JSON exists, n>100, std>0.
    Returns (ok: bool, issues: list[str])."""
    issues = []
    for pop in populations:
        if pop == "MID":
            continue
        json_path = ref_stats_path(pgs_id, pop, "GRCh38")
        if not os.path.exists(json_path):
            issues.append(f"{pop}: JSON missing")
            continue
        try:
            with open(json_path) as f:
                stats = json.load(f)
            n = stats.get("n_samples", 0)
            std = stats.get("std", 0)
            matched = stats.get("matched_variants", 0)
            total = stats.get("total_variants", 1)
            match_rate = matched / max(total, 1)

            if n < 50:
                issues.append(f"{pop}: n_samples={n} (too low)")
            if std <= 0:
                issues.append(f"{pop}: std={std} (zero or negative)")
            if match_rate < 0.01:
                issues.append(f"{pop}: match_rate={match_rate:.3f} (very low)")
        except (json.JSONDecodeError, Exception) as e:
            issues.append(f"{pop}: JSON parse error: {e}")

    return len(issues) == 0, issues


def get_all_cached_pgs_ids() -> List[str]:
    """Find all PGS IDs that have cached scoring files."""
    ids = []
    if os.path.isdir(PGS_CACHE):
        for name in sorted(os.listdir(PGS_CACHE)):
            if name.startswith("PGS") and os.path.isdir(os.path.join(PGS_CACHE, name)):
                ids.append(name)
    return ids


# Curated PGS IDs (high-priority, well-validated)
CURATED_IDS = [
    "PGS000001", "PGS000004", "PGS000005", "PGS000006", "PGS000013",
    "PGS000018", "PGS000039", "PGS000040", "PGS000043", "PGS000078",
    "PGS000119", "PGS000135", "PGS000297", "PGS000321", "PGS000323",
    "PGS000327", "PGS000334", "PGS000335", "PGS000451", "PGS000662",
    "PGS000743", "PGS000746", "PGS000899", "PGS001229", "PGS001780",
    "PGS001839", "PGS001852", "PGS002119", "PGS002302", "PGS002775",
    "PGS003446", "PGS003725", "PGS004768", "PGS005241", "PGS005387",
    "PGS005390", "PGS005391",
]


def backfill_one(pgs_id: str, populations: List[str], force: bool = False) -> dict:
    """Ingest one PGS ID and build ref stats for all requested populations."""
    result = {"pgs_id": pgs_id, "ingest": None, "stats": {}}

    # Step 1: Ingest
    try:
        ingest = ingest_pgs(pgs_id, force=force)
        result["ingest"] = "ok" if ingest.success else f"failed: {ingest.error}"
        if not ingest.success:
            _log_result(pgs_id, "ingest_fail", str(ingest.error))
            return result
    except Exception as e:
        result["ingest"] = f"error: {e}"
        _log_result(pgs_id, "ingest_error", str(e))
        return result

    # Step 2: Build ref stats for each population
    all_ok = True
    for pop in populations:
        try:
            stats = build_ref_stats(pgs_id, pop, force=force)
            if stats.success:
                result["stats"][pop] = f"ok (n={stats.n_samples}, mean={stats.mean:.4g})"
            else:
                result["stats"][pop] = f"failed: {stats.error}"
                all_ok = False
        except Exception as e:
            result["stats"][pop] = f"error: {e}"
            all_ok = False

    # Step 3: Post-build verification
    verified, issues = _verify_build(pgs_id, populations)
    if not verified:
        result["verify"] = issues
        _log_result(pgs_id, "verify_fail", "; ".join(issues))
    elif all_ok:
        _log_result(pgs_id, "ok", f"{len(populations)} pops built")

    return result


def main():
    parser = argparse.ArgumentParser(description="Backfill PGS pipeline data")
    parser.add_argument("--pgs", type=str, default=None,
                        help="Comma-separated PGS IDs (default: all cached)")
    parser.add_argument("--populations", type=str, default=None,
                        help=f"Comma-separated populations (default: {','.join(BUILDABLE_POPULATIONS)})")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of parallel workers (default: 4)")
    parser.add_argument("--force", action="store_true",
                        help="Force rebuild even if outputs exist")
    parser.add_argument("--curated-only", action="store_true",
                        help="Only process curated PGS IDs")
    parser.add_argument("--ingest-only", action="store_true",
                        help="Only run ingestion, skip ref stats")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore resume log and process all IDs")
    args = parser.parse_args()

    # Determine PGS IDs
    if args.pgs:
        pgs_ids = [x.strip() for x in args.pgs.split(",")]
    elif args.curated_only:
        pgs_ids = CURATED_IDS
    else:
        pgs_ids = get_all_cached_pgs_ids()

    # Determine populations
    if args.ingest_only:
        populations = []
    elif args.populations:
        populations = [x.strip() for x in args.populations.split(",")]
    else:
        populations = BUILDABLE_POPULATIONS

    # Resume: skip already-completed IDs (unless --force or --no-resume)
    if not args.force and not args.no_resume:
        completed = _load_completed_ids()
        # Also treat IDs that already have all ref stat JSONs as done
        for pgs_id in list(pgs_ids):
            if pgs_id in completed:
                continue
            all_exist = all(
                pop == "MID" or os.path.exists(ref_stats_path(pgs_id, pop, "GRCh38"))
                for pop in populations
            )
            if all_exist:
                completed.add(pgs_id)
        before = len(pgs_ids)
        pgs_ids = [p for p in pgs_ids if p not in completed]
        skipped = before - len(pgs_ids)
        if skipped > 0:
            logger.info(f"Resume: skipping {skipped} already-completed IDs ({len(pgs_ids)} remaining)")

    logger.info(f"Backfill: {len(pgs_ids)} PGS IDs x {len(populations)} populations "
                f"({args.workers} workers, force={args.force})")
    logger.info(f"Resume log: {RESUME_LOG}")

    if not pgs_ids:
        logger.info("Nothing to do — all IDs already completed!")
        return

    # Init DB
    pipeline_db.init_db()

    # Create ref_stats directory
    os.makedirs(REF_STATS_DIR, exist_ok=True)

    # Process
    t0 = time.time()
    ok = 0
    fail = 0
    verify_fail = 0

    if args.workers <= 1:
        # Sequential
        for i, pgs_id in enumerate(pgs_ids, 1):
            logger.info(f"[{i}/{len(pgs_ids)}] {pgs_id}")
            result = backfill_one(pgs_id, populations, force=args.force)
            if result["ingest"] == "ok":
                ok += 1
            else:
                fail += 1
                logger.warning(f"  {pgs_id}: {result['ingest']}")
            for pop, status in result["stats"].items():
                if "failed" in status or "error" in status:
                    logger.warning(f"  {pgs_id}/{pop}: {status}")
            if result.get("verify"):
                verify_fail += 1
                logger.warning(f"  {pgs_id} VERIFY FAIL: {result['verify']}")
    else:
        # Parallel
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(backfill_one, pgs_id, populations, args.force): pgs_id
                for pgs_id in pgs_ids
            }
            for i, future in enumerate(as_completed(futures), 1):
                pgs_id = futures[future]
                try:
                    result = future.result(timeout=1800)
                    if result["ingest"] == "ok":
                        ok += 1
                    else:
                        fail += 1
                        logger.warning(f"  {pgs_id}: {result['ingest']}")
                    for pop, status in result["stats"].items():
                        if "failed" in status or "error" in status:
                            logger.warning(f"  {pgs_id}/{pop}: {status}")
                    if result.get("verify"):
                        verify_fail += 1
                        logger.warning(f"  {pgs_id} VERIFY FAIL: {result['verify']}")
                    logger.info(f"[{i}/{len(pgs_ids)}] {pgs_id}: ingest={result['ingest']}")
                except Exception as e:
                    fail += 1
                    _log_result(pgs_id, "exception", str(e))
                    logger.error(f"[{i}/{len(pgs_ids)}] {pgs_id}: exception: {e}")

    elapsed = time.time() - t0
    logger.info(f"Done: {ok} ok, {fail} failed, {verify_fail} verify warnings, {elapsed:.0f}s elapsed")

    # Print summary of failures from resume log
    if fail > 0 or verify_fail > 0:
        logger.info(f"Check {RESUME_LOG} for failure details")


if __name__ == "__main__":
    main()
