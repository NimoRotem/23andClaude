"""Central deterministic percentile / interpretability gate.

Wave 0 safety patch (REMEDIATION_PLAN ch.15 W0.3 + W0.4). Every PGS
result — scored fresh or read from disk — flows through this gate
before it can render. The gate's job:

  1. Decide whether the result is INTERPRETABLE (percentile may render)
     or one of the deterministic non-interpretable statuses.
  2. Attach a reason_code (from `reason_codes`) when not interpretable.
  3. Attach a deterministic templated failure prose (NOT LLM prose).
  4. Erase the percentile from view if the gate says it must not render,
     while preserving the raw_score for the record.

The gate is read by: `runners.py::_postprocess_pgs_result` (fresh scores),
`pipeline/live_percentile.py::apply_live_overlay` (old reports on read),
`app.py::_compare_build_for_user` (the compare aggregator), and
`app.py::_interpret_result` (the LLM interpretation entry point).

Idempotent. Safe to call multiple times on the same result.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from . import reason_codes as RC

logger = logging.getLogger("pgs-pipeline.result_gate")


# Populations that exist as ancestry labels but for which we DO NOT emit
# a percentile (no reference panel). Per W0.3 (advisor's high-priority #7
# and P0 #7): MID is a placeholder with no panel; never fall back to EUR.
NO_PERCENTILE_POPULATIONS = {"MID", "OCE", "UNSUPPORTED"}

# Match-rate gate.
MIN_INTERPRETABLE_MATCH_RATE = 60.0

# z-score sanity gate — already applied in scoring.py, but we re-check
# here so old reports (pre-gate) are caught at read time.
Z_SCORE_HARD_MAX = 6.0

# Trait classes withheld from public rendering pending external review.
# Items are matched against `trait` (case-insensitive substring) AND
# against an explicit pgs_id list. Per W0.7 (advisor's #12).
HIDDEN_TRAIT_KEYWORDS = {
    "intelligence",
    "fluid intelligence",
    "cognitive ability",
    "cognitive performance",
    "educational attainment",
    "income",
    "iq score",
    "verbal-numerical reasoning",
}
HIDDEN_PGS_IDS = {
    # Curated list. Extend as policy decisions are made.
    "PGS001232",   # Fluid intelligence
    "PGS001919",   # Fluid intelligence
    "PGS002135",   # Fluid intelligence
    "PGS003510",   # Verbal-numerical reasoning
    "PGS003723",   # Cognitive performance
    "PGS003724",   # IQ
    "PGS004427",   # Fluid intelligence
}


def _is_hidden_trait(result: Dict[str, Any]) -> bool:
    pgs_id = (result.get("pgs_id") or "").upper().strip()
    if pgs_id in HIDDEN_PGS_IDS:
        return True
    trait = (result.get("trait") or "").lower()
    return any(k in trait for k in HIDDEN_TRAIT_KEYWORDS)


def _extract_match_rate(result: Dict[str, Any]) -> Optional[float]:
    v = result.get("match_rate_value")
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    # Older reports stored "match_rate" as a string like "98.7%"
    mr = result.get("match_rate")
    if isinstance(mr, str) and mr.endswith("%"):
        try:
            return float(mr[:-1])
        except ValueError:
            pass
    if isinstance(mr, (int, float)):
        return float(mr)
    return None


def _extract_selected_ref(result: Dict[str, Any]) -> Optional[str]:
    pop = result.get("selected_ref")
    if pop:
        return str(pop).upper()
    pd = ((result.get("pipeline_info") or {}).get("percentile_details") or {})
    return (pd.get("selected_ref") or "").upper() or None


def _extract_pctl_method_and_reason(result: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Pull the percentile-pipeline method + reason from the result dict."""
    pd = ((result.get("pipeline_info") or {}).get("percentile_details") or {})
    return pd.get("method"), pd.get("reason")


def _decide_reason_code(result: Dict[str, Any]) -> Optional[str]:
    """Return a reason code if the result should NOT render a percentile.

    Order is meaningful — earliest match wins so the most specific cause
    surfaces. Returns None when the result is interpretable.
    """
    # TRAIT_HIDDEN_BY_POLICY_DISABLED: per user request, show all traits
    # including cognitive/intelligence/education PGSes.
    # if _is_hidden_trait(result):
    #     return RC.TRAIT_HIDDEN_BY_POLICY

    # If we never computed a score at all, raw_only is the right verdict
    raw_score = result.get("raw_score")
    if raw_score is None:
        return RC.SCORE_NOT_COMPUTED

    # Ancestry: never emit percentile for populations without a panel
    pop = _extract_selected_ref(result)
    if pop in NO_PERCENTILE_POPULATIONS:
        return RC.UNSUPPORTED_ANCESTRY_PANEL
    if pop in ("UNRESOLVED", "MULTI"):
        return RC.ANCESTRY_UNRESOLVED

    # Percentile pipeline emitted a structured method/reason; respect it.
    method, reason = _extract_pctl_method_and_reason(result)
    if method == "incompatible_ref_stats":
        # The contract loader already classified the failure. Map it.
        if reason and "scoring-file content drift" in str(reason):
            return RC.REF_STATS_VARIANT_HASH_MISMATCH
        if reason and "std <= 0" in str(reason):
            return RC.REF_STATS_STD_NONPOSITIVE
        return RC.REF_STATS_SCHEMA_INVALID
    if method == "unavailable":
        return RC.POPULATION_STATS_MISSING

    # Match-rate gate (applied uniformly at read time, even for old reports)
    mr = _extract_match_rate(result)
    if mr is not None and mr < MIN_INTERPRETABLE_MATCH_RATE:
        return RC.MATCH_RATE_BELOW_THRESHOLD

    # Z-score gate (re-applied at read time to catch old extreme values)
    pd = ((result.get("pipeline_info") or {}).get("percentile_details") or {})
    z = pd.get("z_score") or result.get("z_score")
    try:
        if z is not None and abs(float(z)) > Z_SCORE_HARD_MAX:
            return RC.Z_SCORE_EXTREME
    except (TypeError, ValueError):
        pass
    if reason == "z_score_extreme":
        return RC.Z_SCORE_EXTREME
    if reason == "distribution_collapsed":
        return RC.DISTRIBUTION_COLLAPSED

    # Eligibility — only if a verdict is present on the result
    elig = result.get("eligibility") or {}
    elig_status = elig.get("status")
    if elig_status == "ancestry_mismatch":
        return RC.ELIGIBILITY_ANCESTRY_MISMATCH
    if elig_status == "weight_type_unknown":
        return RC.ELIGIBILITY_WEIGHT_TYPE_UNKNOWN
    if elig_status == "direction_unknown":
        return RC.ELIGIBILITY_DIRECTION_UNKNOWN
    if elig_status == "performance_insufficient":
        return RC.ELIGIBILITY_PERFORMANCE_INSUFFICIENT
    if elig_status == "complex_alleles":
        return RC.ELIGIBILITY_COMPLEX_ALLELES

    return None  # interpretable


def apply_gate(result: Dict[str, Any]) -> Dict[str, Any]:
    """Run the gate on a PGS result dict, in place. Returns the same dict.

    Sets the following keys on the result, regardless of outcome:

        score_computed:           bool
        percentile_available:     bool
        interpretability_status:  one of reason_codes.* statuses
        failure_reason_code:      reason code (str) or None
        failure_reason_human:     deterministic template prose, or None
        gate_version:             "1.0"

    When the gate decides percentile must NOT render, this function also
    blanks the rendered `percentile` (preserving `percentile_at_scoring`
    if the result had one) so any downstream UI that ignores the gate
    flags cannot accidentally surface a refused percentile.
    """
    if not isinstance(result, dict):
        return result

    # Only PGS results go through the gate. Other test types pass through
    # unchanged.
    if result.get("test_type") and result["test_type"] != "pgs_score":
        return result

    code = _decide_reason_code(result)
    raw_score = result.get("raw_score")
    score_computed = raw_score is not None

    if code is None:
        # Interpretable. Mark accordingly. Don't touch the percentile.
        result["score_computed"] = score_computed
        result["percentile_available"] = result.get("percentile") is not None
        result["interpretability_status"] = RC.INTERPRETABLE
        result["failure_reason_code"] = None
        result["failure_reason_human"] = None
        result["gate_version"] = "1.0"
        return result

    # Not interpretable.
    status = RC.status_for_code(code)
    pop = _extract_selected_ref(result) or "unknown"
    mr = _extract_match_rate(result)
    pd = ((result.get("pipeline_info") or {}).get("percentile_details") or {})
    z = pd.get("z_score") or result.get("z_score")

    human = RC.render_template(
        code,
        pgs_id=result.get("pgs_id"),
        pop=pop,
        trait=result.get("trait"),
        match_rate=mr,
        z_score=z,
        raw_score=raw_score,
    )

    # Erase the rendered percentile but preserve the audit fields.
    if result.get("percentile") is not None and "percentile_at_scoring" not in result:
        result["percentile_at_scoring"] = result["percentile"]
    result["percentile"] = None

    # Erase the LLM cross-ancestry warning when the gate has its own
    # deterministic reason — don't double up doomy prose.
    result.pop("cross_ancestry_warning", None)

    result["score_computed"]          = score_computed
    result["percentile_available"]    = False
    result["interpretability_status"] = status
    result["failure_reason_code"]     = code
    result["failure_reason_human"]    = human
    result["gate_version"]            = "1.0"

    return result


def is_interpretable(result: Dict[str, Any]) -> bool:
    """Quick predicate after the gate has run."""
    return result.get("interpretability_status") == RC.INTERPRETABLE


def gate_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    """Return only the gate-relevant fields, for /compare and friends."""
    return {
        "score_computed":          result.get("score_computed"),
        "percentile_available":    result.get("percentile_available"),
        "interpretability_status": result.get("interpretability_status"),
        "failure_reason_code":     result.get("failure_reason_code"),
        "failure_reason_human":    result.get("failure_reason_human"),
        "gate_version":            result.get("gate_version"),
    }
