"""Deterministic reason codes + templates for PGS result interpretability.

Wave 0 safety patch (REMEDIATION_PLAN ch.15 W0.4). Every PGS result that
does not render a percentile must surface a structured reason code from
this enum, and the user-facing prose must come from these templates —
not from the LLM.

The LLM is allowed to write explanatory prose ONLY when
`interpretability_status == "INTERPRETABLE"`; for every other status,
the templated text below is the single source of truth.
"""
from __future__ import annotations

from typing import Optional


# ── Reason code enum ────────────────────────────────────────────────
# Add codes here when the gate gains new failure paths. Each code maps
# to a deterministic template (see TEMPLATES below).
#
# Keep names UPPER_SNAKE_CASE; tests assert exact equality.

# Schema / data-integrity failures
REF_STATS_SCHEMA_INVALID        = "REF_STATS_SCHEMA_INVALID"
REF_STATS_VARIANT_HASH_MISMATCH = "REF_STATS_VARIANT_HASH_MISMATCH"
REF_STATS_STD_NONPOSITIVE       = "REF_STATS_STD_NONPOSITIVE"
POPULATION_STATS_MISSING        = "POPULATION_STATS_MISSING"

# Pipeline-state failures
MATCH_RATE_BELOW_THRESHOLD      = "MATCH_RATE_BELOW_THRESHOLD"
Z_SCORE_EXTREME                 = "Z_SCORE_EXTREME"
DISTRIBUTION_COLLAPSED          = "DISTRIBUTION_COLLAPSED"
BUILD_MISMATCH_UNLIFTED         = "BUILD_MISMATCH_UNLIFTED"
INPUT_UNSUPPORTED               = "INPUT_UNSUPPORTED"
SCORE_NOT_COMPUTED              = "SCORE_NOT_COMPUTED"
CHIP_COVERAGE_TOO_LOW           = "CHIP_COVERAGE_TOO_LOW"

# Ancestry / population failures
UNSUPPORTED_ANCESTRY_PANEL      = "UNSUPPORTED_ANCESTRY_PANEL"
ANCESTRY_UNRESOLVED             = "ANCESTRY_UNRESOLVED"

# Eligibility failures
ELIGIBILITY_ANCESTRY_MISMATCH       = "ELIGIBILITY_ANCESTRY_MISMATCH"
ELIGIBILITY_WEIGHT_TYPE_UNKNOWN     = "ELIGIBILITY_WEIGHT_TYPE_UNKNOWN"
ELIGIBILITY_DIRECTION_UNKNOWN       = "ELIGIBILITY_DIRECTION_UNKNOWN"
ELIGIBILITY_PERFORMANCE_INSUFFICIENT = "ELIGIBILITY_PERFORMANCE_INSUFFICIENT"
ELIGIBILITY_COMPLEX_ALLELES         = "ELIGIBILITY_COMPLEX_ALLELES"

# Trait-class / policy failures
TRAIT_HIDDEN_BY_POLICY          = "TRAIT_HIDDEN_BY_POLICY"


# ── Interpretability status enum ─────────────────────────────────────
# The status a result lands in after the gate. Mutually exclusive.

INTERPRETABLE             = "INTERPRETABLE"
RAW_ONLY                  = "RAW_ONLY"
UNSUPPORTED_ANCESTRY      = "UNSUPPORTED_ANCESTRY"
REF_STATS_INVALID         = "REF_STATS_INVALID"
LOW_MATCH_RATE            = "LOW_MATCH_RATE"
EXTREME_Z                 = "EXTREME_Z"
BUILD_MISMATCH            = "BUILD_MISMATCH"
INPUT_UNSUPPORTED_STATUS  = "INPUT_UNSUPPORTED"


# Mapping: reason code → interpretability status the result lands in.
# Used by the gate to map a per-step failure into the top-level status.
CODE_TO_STATUS = {
    REF_STATS_SCHEMA_INVALID:        REF_STATS_INVALID,
    REF_STATS_VARIANT_HASH_MISMATCH: REF_STATS_INVALID,
    REF_STATS_STD_NONPOSITIVE:       REF_STATS_INVALID,
    POPULATION_STATS_MISSING:        REF_STATS_INVALID,
    MATCH_RATE_BELOW_THRESHOLD:      LOW_MATCH_RATE,
    Z_SCORE_EXTREME:                 EXTREME_Z,
    DISTRIBUTION_COLLAPSED:          EXTREME_Z,
    BUILD_MISMATCH_UNLIFTED:         BUILD_MISMATCH,
    INPUT_UNSUPPORTED:               INPUT_UNSUPPORTED_STATUS,
    SCORE_NOT_COMPUTED:              INPUT_UNSUPPORTED_STATUS,
    CHIP_COVERAGE_TOO_LOW:           LOW_MATCH_RATE,
    UNSUPPORTED_ANCESTRY_PANEL:      UNSUPPORTED_ANCESTRY,
    ANCESTRY_UNRESOLVED:             UNSUPPORTED_ANCESTRY,
    ELIGIBILITY_ANCESTRY_MISMATCH:       UNSUPPORTED_ANCESTRY,
    ELIGIBILITY_WEIGHT_TYPE_UNKNOWN:     RAW_ONLY,
    ELIGIBILITY_DIRECTION_UNKNOWN:       RAW_ONLY,
    ELIGIBILITY_PERFORMANCE_INSUFFICIENT: RAW_ONLY,
    ELIGIBILITY_COMPLEX_ALLELES:         RAW_ONLY,
    TRAIT_HIDDEN_BY_POLICY:              RAW_ONLY,
}


# ── Deterministic templates ──────────────────────────────────────────
# Plain Python str.format-style. Use {pgs_id}, {pop}, {trait},
# {match_rate}, {z_score}, {raw_score}, {extras} as needed.
# Keep prose factual, not directional. Mention "pipeline issue" where
# applicable so the user understands it's not a biology verdict.

TEMPLATES = {
    REF_STATS_SCHEMA_INVALID: (
        "No percentile available — reason: REF_STATS_SCHEMA_INVALID. "
        "Reference statistics for population {pop} on {pgs_id} failed the "
        "schema validation contract (missing or invalid metadata fields). "
        "The raw score was computed (raw_score={raw_score}) but cannot be "
        "interpreted as a percentile without validated reference statistics. "
        "This is a pipeline data issue, not a biological finding; the affected "
        "statistics are being regenerated."
    ),
    REF_STATS_VARIANT_HASH_MISMATCH: (
        "No percentile available — reason: REF_STATS_VARIANT_HASH_MISMATCH. "
        "The reference statistics for {pgs_id}/{pop} were computed against a "
        "different variant set than the current scoring file. Percentile is "
        "withheld to avoid silently comparing the user score to the wrong "
        "distribution. Awaiting reference-statistics regeneration."
    ),
    REF_STATS_STD_NONPOSITIVE: (
        "No percentile available — reason: REF_STATS_STD_NONPOSITIVE. "
        "The reference distribution for {pgs_id}/{pop} has a non-positive "
        "standard deviation; the panel subset produced a degenerate "
        "distribution. Raw score is preserved (raw_score={raw_score})."
    ),
    POPULATION_STATS_MISSING: (
        "No percentile available — reason: POPULATION_STATS_MISSING. "
        "No reference statistics exist for population {pop} on {pgs_id}. "
        "The raw score was computed (raw_score={raw_score}) but cannot be "
        "ranked. A per-population sensitivity array is shown when available."
    ),
    MATCH_RATE_BELOW_THRESHOLD: (
        "No percentile available — reason: MATCH_RATE_BELOW_THRESHOLD. "
        "The input file contains only {match_rate}% of the variants {pgs_id} "
        "requires (threshold ≥60%). Below this threshold, a polygenic score "
        "becomes dominated by missing-variant noise. Use a higher-coverage "
        "input (low-coverage WGS or imputed array data) to score this PGS."
    ),
    Z_SCORE_EXTREME: (
        "No percentile available — reason: Z_SCORE_EXTREME. "
        "The user's score sits at |z|={z_score} standard deviations from the "
        "reference population mean for {pgs_id}/{pop}. This is outside the "
        "calibrated range of the reference distribution and is likely caused "
        "by allele-encoding mismatch, scale mismatch (AVG vs SUM), or "
        "user/panel match-rate divergence. Raw score is preserved "
        "(raw_score={raw_score})."
    ),
    DISTRIBUTION_COLLAPSED: (
        "No percentile available — reason: DISTRIBUTION_COLLAPSED. "
        "The reference standard deviation for {pgs_id}/{pop} is far smaller "
        "than expected from sibling populations; the panel distribution "
        "appears to have collapsed. Percentile is suppressed until the "
        "reference is regenerated. Raw score: {raw_score}."
    ),
    BUILD_MISMATCH_UNLIFTED: (
        "No percentile available — reason: BUILD_MISMATCH_UNLIFTED. "
        "The input genome build differs from the scoring file's build and "
        "automatic liftover failed. Re-run with a build-matched input or "
        "a scoring file that already covers the input build."
    ),
    INPUT_UNSUPPORTED: (
        "No percentile available — reason: INPUT_UNSUPPORTED. "
        "This input type is not supported for {pgs_id}. Submit a compatible "
        "input (VCF / gVCF / BAM / CRAM) per the PGS eligibility matrix."
    ),
    SCORE_NOT_COMPUTED: (
        "No score available — reason: SCORE_NOT_COMPUTED. "
        "The pipeline could not compute a raw score for {pgs_id} (see "
        "pipeline logs for details)."
    ),
    CHIP_COVERAGE_TOO_LOW: (
        "No percentile available — reason: CHIP_COVERAGE_TOO_LOW. "
        "Consumer-chip inputs cover only a small fraction of {pgs_id}'s "
        "variants (≈{match_rate}%). For modern PGS Catalog entries, chip "
        "data does not provide enough overlap to produce a meaningful "
        "percentile. Use WGS or imputed array data."
    ),
    UNSUPPORTED_ANCESTRY_PANEL: (
        "No percentile available — reason: UNSUPPORTED_ANCESTRY_PANEL. "
        "The sample's inferred ancestry ({pop}) is not represented in our "
        "current reference panel. We do not emit a percentile that would "
        "force-fit this sample into a related but mismatched population. "
        "The raw score is preserved (raw_score={raw_score})."
    ),
    ANCESTRY_UNRESOLVED: (
        "No percentile available — reason: ANCESTRY_UNRESOLVED. "
        "PCA-based ancestry inference was inconclusive for this sample. "
        "Run a dedicated ancestry analysis first, or provide a higher-"
        "coverage input. Raw score is preserved (raw_score={raw_score})."
    ),
    ELIGIBILITY_ANCESTRY_MISMATCH: (
        "No percentile available — reason: ELIGIBILITY_ANCESTRY_MISMATCH. "
        "{pgs_id} was developed and evaluated on a different ancestry; "
        "applying it to {pop} without validated reference statistics is "
        "not supported. The raw score is preserved (raw_score={raw_score})."
    ),
    ELIGIBILITY_WEIGHT_TYPE_UNKNOWN: (
        "Raw score only — reason: ELIGIBILITY_WEIGHT_TYPE_UNKNOWN. "
        "{pgs_id} uses a non-standard weight type; we don't render an "
        "interpretation until the weights are explicitly transformed."
    ),
    ELIGIBILITY_DIRECTION_UNKNOWN: (
        "Raw score only — reason: ELIGIBILITY_DIRECTION_UNKNOWN. "
        "{pgs_id} does not declare which direction of the score corresponds "
        "to higher trait values. Interpretation is withheld."
    ),
    ELIGIBILITY_PERFORMANCE_INSUFFICIENT: (
        "Raw score only — reason: ELIGIBILITY_PERFORMANCE_INSUFFICIENT. "
        "{pgs_id} has insufficient evaluation metrics (AUC/R²) for routine "
        "interpretation. Treat the score as exploratory."
    ),
    ELIGIBILITY_COMPLEX_ALLELES: (
        "No percentile available — reason: ELIGIBILITY_COMPLEX_ALLELES. "
        "{pgs_id} includes variants in HLA / complex-allele regions that "
        "this pipeline does not score reliably from short-read data."
    ),
    TRAIT_HIDDEN_BY_POLICY: (
        "This trait is currently hidden from public reporting pending "
        "external review. Raw score is preserved for record-keeping."
    ),
}


def render_template(code: str, **kwargs) -> str:
    """Render the deterministic template for a reason code.

    Missing format keys are tolerated (rendered as 'n/a') so a partial
    result dict doesn't crash the gate.
    """
    template = TEMPLATES.get(code)
    if not template:
        return (f"No percentile available — reason: {code}. "
                "No template registered for this reason code.")
    safe_kwargs = {
        "pgs_id":     kwargs.get("pgs_id")    or "n/a",
        "pop":        kwargs.get("pop")       or "n/a",
        "trait":      kwargs.get("trait")     or "n/a",
        "match_rate": kwargs.get("match_rate") if kwargs.get("match_rate") is not None else "n/a",
        "z_score":    kwargs.get("z_score")    if kwargs.get("z_score")    is not None else "n/a",
        "raw_score":  kwargs.get("raw_score")  if kwargs.get("raw_score")  is not None else "n/a",
        "extras":     kwargs.get("extras")    or "",
    }
    try:
        return template.format(**safe_kwargs)
    except (KeyError, ValueError):
        return template


def status_for_code(code: Optional[str]) -> str:
    """Map a reason code to the top-level interpretability status."""
    if not code:
        return INTERPRETABLE
    return CODE_TO_STATUS.get(code, RAW_ONLY)
