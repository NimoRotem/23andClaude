"""Recompute percentile from stored raw_score on read.

Background
----------
Historical PGS reports bake percentile in at scoring time. When the underlying
ref-panel stats file is later corrected (e.g. PGS000334 n16→n22 fix), every
existing report carries the old percentile despite the live μ/σ being right.

This module computes percentile lazily, against the current registry-resolved
stats, whenever a stored report is read. The stored ``raw_score`` is the
authoritative quantity; ``percentile`` is a view over (raw_score, current
stats_file). The original at-scoring percentile is preserved under
``percentile_at_scoring`` for audit.

The overlay is a no-op if:
  - the report has no ``raw_score``
  - no current stats file resolves for (pgs_id, selected_ref)
  - the current stats file fails the strict schema contract
  - the recomputed value matches the stored value

The overlay is silent on per-call failure — it must never break a report read.
"""
from __future__ import annotations

import glob
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("pgs-pipeline.live_percentile")


_COHORT_SANITY_LOG = "/home/nimrod_rotem/simple-genomics/logs/cron_cohort_sanity.log"
_COHORT_FLAGGED_CACHE: Tuple[float, Set[str]] = (0.0, set())

# Cohort-relative percentile cache: (pgs_id, mtime_of_users_dir) -> [raw_scores]
_COHORT_RAW_SCORES_CACHE: Dict[Tuple[str, float], List[float]] = {}

_USERS_ROOT = "/home/nimrod_rotem/simple-genomics/users"


def _load_cohort_flagged_pgs() -> Set[str]:
    """Return the set of PGS IDs flagged by cron_cohort_sanity (KS p<0.01).

    Cached by mtime so we don't re-parse the log on every report read.
    Falls back to the empty set if the log is missing or unparseable.
    """
    global _COHORT_FLAGGED_CACHE
    try:
        mtime = os.path.getmtime(_COHORT_SANITY_LOG)
    except OSError:
        return set()
    cached_mtime, cached_set = _COHORT_FLAGGED_CACHE
    if cached_mtime == mtime:
        return cached_set
    flagged: Set[str] = set()
    try:
        with open(_COHORT_SANITY_LOG) as f:
            for line in f:
                # Format: "🚩  PGS001229  n=7  >80%ile=86%  <50%ile=0%  KS p=0.000"
                m = re.search(r'\s(PGS\d{6,})\s', line)
                if m and "🚩" in line:
                    # Only count the most recent run's flags — the log appends
                    # over time. We err on the side of inclusion: any PGS that
                    # appears with a flag in the file is considered suspect.
                    flagged.add(m.group(1))
    except OSError:
        pass
    _COHORT_FLAGGED_CACHE = (mtime, flagged)
    return flagged


def _collect_cohort_raw_scores(pgs_id: str, exclude_path: Optional[str] = None) -> List[float]:
    """Collect raw_score values for `pgs_id` across all users' reports.

    Cached by mtime of the users root. ``exclude_path`` can be used to drop
    the current report from the cohort to avoid self-reference when computing
    a sample's rank. The cohort INCLUDES the requested sample by default —
    callers that want self-exclusion handle it after this returns.
    """
    try:
        mtime = os.path.getmtime(_USERS_ROOT)
    except OSError:
        return []
    cache_key = (pgs_id, mtime)
    cached = _COHORT_RAW_SCORES_CACHE.get(cache_key)
    if cached is not None:
        return [s for s in cached if s is not None]
    scores: List[float] = []
    pat = os.path.join(_USERS_ROOT, "*", "reports", "*", "pgs_*.json")
    for f in glob.glob(pat):
        try:
            with open(f) as fh:
                d = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        r = d.get("result") or {}
        if not isinstance(r, dict):
            continue
        # Match by pgs_id (also check pipeline_info.pgs_catalog_id)
        pid = r.get("pgs_id") or (r.get("pipeline_info") or {}).get("pgs_catalog_id")
        if pid != pgs_id:
            continue
        rs = r.get("raw_score")
        if rs is None:
            continue
        try:
            scores.append(float(rs))
        except (TypeError, ValueError):
            continue
    _COHORT_RAW_SCORES_CACHE[cache_key] = scores
    return scores


def _cohort_relative_percentile(pgs_id: str, raw_score: float,
                                 exclude_self: bool = True) -> Optional[Dict[str, Any]]:
    """Return the rank of ``raw_score`` within all other reports for ``pgs_id``.

    Returns dict with:
      percentile: 0-100 (rank-based)
      n_cohort: how many other reports contributed
      cohort_median: median raw_score in cohort
    or None if cohort is too small (n<3) to be meaningful.
    """
    scores = _collect_cohort_raw_scores(pgs_id)
    if exclude_self:
        # Drop one occurrence of raw_score (the calling report's own value)
        try:
            scores = list(scores)
            scores.remove(raw_score)
        except ValueError:
            pass  # not in list, keep all
    if len(scores) < 3:
        return None
    sorted_scores = sorted(scores)
    n_below = sum(1 for s in sorted_scores if s < raw_score)
    n_eq = sum(1 for s in sorted_scores if s == raw_score)
    # Mid-rank for tied scores
    pctl = 100.0 * (n_below + 0.5 * n_eq) / len(sorted_scores)
    median = sorted_scores[len(sorted_scores) // 2]
    return {
        "percentile": round(pctl, 1),
        "n_cohort": len(sorted_scores),
        "cohort_median": median,
    }


def apply_live_overlay(report: Dict[str, Any]) -> Dict[str, Any]:
    """Mutate ``report`` in place to reflect the current ref-stats μ/σ.

    Safe to call on any report dict — non-PGS, failed, or partial reports
    are returned unchanged.
    """
    # # GATE_W0_4_FINALLY_WRAP: run the deterministic gate on EVERY exit path,
    # including the early-return branches where new_pctl is None
    # (schema-invalid stats). Per W0.4 / W0.5.
    try:
      try:
          result = report.get("result")
          if not isinstance(result, dict):
              return report
          if result.get("test_type") != "pgs_score":
              return report
          # # OVERLAY_FINGERPRINT_RECOVERY: include fingerprint_drift_refused as recoverable

          if result.get("status") not in ("passed", "fingerprint_drift_refused", "warning"):
              return report
          raw_score = result.get("raw_score")
          if raw_score is None:
              return report
          pgs_id = result.get("pgs_id")
          if not pgs_id:
              return report
          # Mark cohort-sanity-flagged PGS so the UI can show a warning.
          # These have demonstrated systematic bias in the user's cohort
          # vs the 1000G ref panel (KS p<0.01), so percentile interpretation
          # is unreliable until the bias is resolved upstream.
          if pgs_id in _load_cohort_flagged_pgs():
              result["cohort_sanity_flagged"] = True
              result.setdefault("cohort_sanity_warning", (
                  f"{pgs_id} percentile is statistically unreliable for this "
                  "cohort: the user's samples deviate significantly from the "
                  "1000G reference distribution (cron_cohort_sanity KS p<0.01). "
                  "Treat the percentile as indicative, not diagnostic."
              ))
          # Hardcoded "known low portability" warning — fires on first read of
          # a flagged PGS, before cohort_sanity has enough samples to detect
          # the bias from the data itself.
          try:
              from .portability_warnings import portability_warning
              pw = portability_warning(pgs_id)
              if pw:
                  result["portability_warning"] = pw
                  result["low_portability_pgs"] = True
          except Exception:
              pass
          # Cohort-relative percentile: rank within all OTHER reports for the
          # same PGS in this system. Useful when the absolute percentile is
          # unreliable due to AF divergence from 1000G — gives a within-cohort
          # comparison that doesn't depend on the ref panel matching.
          try:
              cohort = _cohort_relative_percentile(pgs_id, float(raw_score),
                                                    exclude_self=True)
              if cohort is not None:
                  result["cohort_relative_percentile"] = cohort["percentile"]
                  result["cohort_size"] = cohort["n_cohort"]
                  result["cohort_median_raw_score"] = cohort["cohort_median"]
          except Exception as e:
              logger.warning(f"cohort percentile failed for {pgs_id}: {e}")
          pop = result.get("selected_ref") or "EUR"
          score_sum = result.get("score_sum")

          from .scoring import compute_percentile_for_ref
          from . import registry as _ref_registry

          new_pctl, details = compute_percentile_for_ref(
              pgs_id, float(raw_score), pop, score_sum=score_sum
          )
          if new_pctl is None:
              # incompatible_ref_stats / unavailable / extreme z → don't overwrite,
              # but surface the reason so the UI can show the gap.
              result["live_percentile_unavailable_reason"] = details.get("reason") or details.get("method")
              return report

          stored_pctl = result.get("percentile")
          # Drift threshold — anything > 0.1 percentile change is worth recording.
          if stored_pctl is not None and abs(float(stored_pctl) - new_pctl) <= 0.1:
              return report

          current_entry = _ref_registry.entry(pgs_id, pop) or {}
          if stored_pctl is not None:
              result["percentile_at_scoring"] = stored_pctl
          result["percentile"] = new_pctl

          result["percentile_recomputed_on_read"] = True

          # # OVERLAY_FINGERPRINT_RECOVERY: promote status when recovered

          if result.get("status") == "fingerprint_drift_refused":

              result["status"] = "passed"

              result["fingerprint_drift_recovered"] = True
          result["live_ref_mean"] = details.get("ref_mean")
          result["live_ref_std"] = details.get("ref_std")
          result["live_z_score"] = details.get("z_score")
          result["live_stats_file"] = details.get("stats_file")
          if current_entry:
              result["live_stats_variant_ids_sha256"] = current_entry.get("variant_ids_sha256")

          # Patch derived strings so the UI doesn't show stale numbers
          # alongside the corrected percentile.
          summary = result.get("summary")
          if isinstance(summary, str) and stored_pctl is not None:
              old_str = f"{stored_pctl}%"
              new_str = f"{new_pctl}%"
              if old_str in summary:
                  result["summary"] = summary.replace(old_str, new_str)
          headline = result.get("headline")
          if isinstance(headline, str) and stored_pctl is not None:
              old_str = f"{stored_pctl}%ile"
              new_str = f"{new_pctl}%ile"
              if old_str in headline:
                  result["headline"] = headline.replace(old_str, new_str)

          # # GATE_W0_4_READ_TIME: run the deterministic gate on every read.
          # Old reports (pre-gate) get a fresh interpretability verdict
          # and stale unsafe percentiles are blanked. Per W0.4 / W1.5.
          try:
              from .result_gate import apply_gate as _apply_gate
              _apply_gate(result)
          except Exception as _gate_exc:
              logger.warning(f"result_gate (read-time) failed: {_gate_exc}")
      except Exception as e:
        # Never break a read because of overlay failure.
        logger.warning(f"live_percentile overlay failed: {e}")
      finally:
        try:
          result = report.get("result") if isinstance(report, dict) else None
          if isinstance(result, dict):
            from .result_gate import apply_gate as _apply_gate
            _apply_gate(result)
        except Exception as _gate_exc:
          logger.warning(f"result_gate (read-time) failed: {_gate_exc}")
    except Exception:
        pass
    return report


def overlay_summary(summary: Dict[str, Any], full_report: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Variant of apply_live_overlay for the truncated summary dicts that
    ``_load_reports_for_file`` returns. Updates only the ``percentile`` field.

    ``full_report`` is the on-disk report (so we can pull raw_score etc.);
    pass None to skip (no-op).
    """
    if full_report is None:
        return summary
    try:
        apply_live_overlay(full_report)
        result = full_report.get("result", {})
        new_pctl = result.get("percentile")
        if new_pctl is not None and summary.get("percentile") != new_pctl:
            summary["percentile_at_scoring"] = summary.get("percentile")
            summary["percentile"] = new_pctl
            summary["percentile_recomputed_on_read"] = True
    except Exception as e:
        logger.warning(f"live_percentile summary overlay failed: {e}")
    return summary
