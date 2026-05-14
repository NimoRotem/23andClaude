"""Phase 2.8 — PGS eligibility gates.

Per REMEDIATION_PLAN §2.8, ALL must pass for percentile emission:

  - Harmonized scoring file present in target build OR strict liftover
    passes (Phase 2.3).
  - No unresolved complex alleles (HLA, repeat expansions, structural
    variants).
  - `weight_type` ∈ {beta, log_or, log_hr}; unknown blocked.
  - PGS Catalog development OR evaluation ancestry includes the user's
    assigned population.
  - Published performance metric above per-trait-class minimum: binary
    AUC ≥ 0.55, continuous R² ≥ 0.02. Else
    status="performance_insufficient".
  - Trait direction (higher score = higher trait) explicitly known.

Risk language ("elevated risk", etc.) generated ONLY when ALL gates
pass AND ancestry/sex match a validated evaluation set in PGS Catalog
metadata.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Optional


log = logging.getLogger("pgs-pipeline.eligibility_gates")


VALID_WEIGHT_TYPES = {"beta", "log_or", "log_hr"}
COMPLEX_ALLELE_REGIONS = (
    # HLA region
    ("chr6", 28_000_000, 34_000_000),
    # APOB / LDLR neighborhood (just an example of repeat-rich genes;
    # extend as needed).
)
# Approximate minimum acceptable AUC for binary traits / R² for continuous.
BINARY_AUC_MIN = 0.55
CONTINUOUS_R2_MIN = 0.02


@dataclass
class EligibilityVerdict:
    pgs_id: str
    eligible: bool
    status: str               # "ok" | "performance_insufficient" | "ancestry_mismatch" |
                              # "weight_type_unknown" | "complex_alleles" |
                              # "liftover_failed" | "direction_unknown"
    reasons: list[str] = field(default_factory=list)
    risk_language_allowed: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


def _has_complex_alleles(
    variants: list[tuple[str, int, str, str]],
) -> int:
    """Return count of variants falling inside flagged complex regions."""
    n = 0
    for chrom, pos, _ref, _alt in variants:
        c = chrom if chrom.startswith("chr") else f"chr{chrom}"
        for region_chrom, lo, hi in COMPLEX_ALLELE_REGIONS:
            if c == region_chrom and lo <= pos <= hi:
                n += 1
                break
    return n


def eligibility_for_pgs(
    pgs_id: str,
    *,
    pgs_metadata: dict,
    user_assigned_population: Optional[str],
    user_sex: Optional[str],
    liftover_passed: bool,
    has_harmonized_target_build: bool,
    variants: Optional[list[tuple[str, int, str, str]]] = None,
) -> EligibilityVerdict:
    """Apply the §2.8 eligibility gates and return a structured verdict.

    `pgs_metadata` is the dict parsed from the PGS Catalog scoring file
    header (keys like `weight_type`, `trait_reported`, `development_ancestry`,
    `evaluation_ancestry`, `auc`, `r2`, `trait_direction`).
    """
    reasons: list[str] = []
    status = "ok"

    # 1. Build availability (harmonized OR strict liftover)
    if not has_harmonized_target_build and not liftover_passed:
        return EligibilityVerdict(
            pgs_id=pgs_id, eligible=False, status="liftover_failed",
            reasons=["no harmonized scoring file for target build AND liftover failed gates"],
        )

    # 2. Complex alleles
    if variants:
        n_complex = _has_complex_alleles(variants)
        if n_complex > 0:
            return EligibilityVerdict(
                pgs_id=pgs_id, eligible=False, status="complex_alleles",
                reasons=[f"{n_complex} variants fall in HLA/complex regions"],
            )

    # 3. weight_type
    wt = (pgs_metadata.get("weight_type") or "").strip().lower()
    if wt not in VALID_WEIGHT_TYPES:
        return EligibilityVerdict(
            pgs_id=pgs_id, eligible=False, status="weight_type_unknown",
            reasons=[f"weight_type={wt!r} not in {sorted(VALID_WEIGHT_TYPES)}"],
        )

    # 4. Ancestry match (development OR evaluation must include user pop)
    user_pop = (user_assigned_population or "").upper()
    if not user_pop:
        return EligibilityVerdict(
            pgs_id=pgs_id, eligible=False, status="ancestry_unresolved",
            reasons=["user ancestry not assigned (see Phase 1.5)"],
        )
    dev = (pgs_metadata.get("development_ancestry") or "").upper()
    evl = (pgs_metadata.get("evaluation_ancestry") or "").upper()
    if user_pop not in dev and user_pop not in evl:
        return EligibilityVerdict(
            pgs_id=pgs_id, eligible=False, status="ancestry_mismatch",
            reasons=[f"PGS dev_ancestry={dev!r}, eval_ancestry={evl!r} — neither contains {user_pop!r}"],
        )

    # 5. Performance metric
    auc = pgs_metadata.get("auc")
    r2 = pgs_metadata.get("r2") or pgs_metadata.get("R2")
    binary = pgs_metadata.get("trait_type", "").lower() == "binary"
    if binary:
        try:
            auc_v = float(auc) if auc not in (None, "") else None
        except (TypeError, ValueError):
            auc_v = None
        if auc_v is None or auc_v < BINARY_AUC_MIN:
            return EligibilityVerdict(
                pgs_id=pgs_id, eligible=False, status="performance_insufficient",
                reasons=[f"binary trait AUC={auc_v} below {BINARY_AUC_MIN}"],
            )
    else:
        try:
            r2_v = float(r2) if r2 not in (None, "") else None
        except (TypeError, ValueError):
            r2_v = None
        if r2_v is None or r2_v < CONTINUOUS_R2_MIN:
            return EligibilityVerdict(
                pgs_id=pgs_id, eligible=False, status="performance_insufficient",
                reasons=[f"continuous trait R²={r2_v} below {CONTINUOUS_R2_MIN}"],
            )

    # 6. Trait direction known
    direction = pgs_metadata.get("trait_direction") or pgs_metadata.get("direction")
    if not direction:
        return EligibilityVerdict(
            pgs_id=pgs_id, eligible=False, status="direction_unknown",
            reasons=["trait_direction not declared in PGS metadata"],
        )

    # All gates passed. Risk language only when ancestry + sex match a
    # validated evaluation set (sex if the PGS Catalog metadata pins one).
    eval_sex = (pgs_metadata.get("evaluation_sex") or "").lower()
    sex_match = (not eval_sex) or eval_sex in ("both", (user_sex or "").lower())
    risk_lang = sex_match and user_pop in evl
    return EligibilityVerdict(
        pgs_id=pgs_id, eligible=True, status="ok",
        risk_language_allowed=risk_lang, reasons=reasons,
    )


def filter_risk_language(text: str, allowed: bool) -> str:
    """Strip 'elevated risk', 'high risk', etc. when `allowed=False`."""
    if allowed or not text:
        return text
    import re
    return re.sub(
        r"\b(elevated|increased|high)\s+risk\b",
        "(risk language withheld; PGS not validated for this ancestry/sex)",
        text, flags=re.IGNORECASE,
    )
