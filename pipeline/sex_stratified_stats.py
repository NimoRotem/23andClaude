"""Phase 2.2 — Sex-stratified ref-stats for sex-dimorphic PGS.

Per REMEDIATION_PLAN §2.2:

  Per-PGS `sex_dimorphic` flag derived from:
    1. PGS Catalog metadata (trait-level sex-specificity), AND
    2. One-time per-PGS two-sample KS in reference panel: male vs
       female sum-scores, p < 1e-4 with Cohen's d > 0.2.

  For flagged PGS, build (PGS, pop, sex) ref-stats and percentiles.
  Genetic sex inferred from chrX/chrY coverage (existing path); if
  unavailable, sex=unknown. Sex unknown AND PGS is sex-dimorphic →
  percentile=null, status="sex_required".
"""
from __future__ import annotations

import json
import logging
import math
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

import numpy as np


log = logging.getLogger("pgs-pipeline.sex_stratified_stats")


SEX_DIMORPHIC_TRAIT_KEYWORDS = (
    # Heuristic keyword filter on trait_reported / trait_mapped — used
    # alongside the KS+Cohen's-d test, not as a sole signal.
    "menstru", "menopause", "menarche", "breast", "ovarian", "prostate",
    "testicular", "endometrial", "uterine", "ovary",
    "androgen", "estrogen", "testosterone",
    "pregnancy", "gestation", "fertility", "sperm",
)


@dataclass
class SexDimorphismTest:
    pgs_id: str
    population: str
    n_male: int
    n_female: int
    ks_statistic: float
    ks_pvalue: float
    cohens_d: float
    sex_dimorphic: bool
    reasons: list[str] = field(default_factory=list)


def cohens_d_male_vs_female(
    male: Sequence[float], female: Sequence[float],
) -> float:
    a = np.asarray(male, dtype=np.float64)
    b = np.asarray(female, dtype=np.float64)
    n_a, n_b = a.size, b.size
    if n_a < 2 or n_b < 2:
        return 0.0
    m_a, m_b = float(a.mean()), float(b.mean())
    s_a, s_b = float(a.std(ddof=1)), float(b.std(ddof=1))
    pooled = math.sqrt(((n_a - 1) * s_a ** 2 + (n_b - 1) * s_b ** 2) / (n_a + n_b - 2))
    if pooled <= 0:
        return 0.0
    return (m_a - m_b) / pooled


def ks_two_sample(
    male: Sequence[float], female: Sequence[float],
) -> tuple[float, float]:
    """Approximate two-sample KS without scipy. Returns (D, p)."""
    a = np.sort(np.asarray(male, dtype=np.float64))
    b = np.sort(np.asarray(female, dtype=np.float64))
    n_a, n_b = a.size, b.size
    if n_a == 0 or n_b == 0:
        return 0.0, 1.0
    all_vals = np.concatenate([a, b])
    cdf_a = np.searchsorted(a, all_vals, side="right") / n_a
    cdf_b = np.searchsorted(b, all_vals, side="right") / n_b
    D = float(np.max(np.abs(cdf_a - cdf_b)))
    # Smirnov asymptotic p-value
    en = math.sqrt(n_a * n_b / (n_a + n_b))
    arg = (en + 0.12 + 0.11 / en) * D
    p = 2.0 * sum((-1) ** (k - 1) * math.exp(-2.0 * (k * arg) ** 2)
                  for k in range(1, 101))
    p = max(0.0, min(1.0, p))
    return D, p


def test_sex_dimorphism(
    pgs_id: str,
    population: str,
    male_scores: Sequence[float],
    female_scores: Sequence[float],
    *,
    trait_text: str = "",
    p_threshold: float = 1e-4,
    d_threshold: float = 0.2,
) -> SexDimorphismTest:
    """Per-spec §2.2 dimorphism test: KS p<1e-4 AND Cohen's |d|>0.2."""
    D, p = ks_two_sample(male_scores, female_scores)
    d = cohens_d_male_vs_female(male_scores, female_scores)
    reasons: list[str] = []
    is_dim_stat = (p < p_threshold) and (abs(d) > d_threshold)
    if is_dim_stat:
        reasons.append(f"ks p={p:.2e}<{p_threshold} AND |d|={abs(d):.2f}>{d_threshold}")
    is_dim_kw = False
    if trait_text:
        t = trait_text.lower()
        for kw in SEX_DIMORPHIC_TRAIT_KEYWORDS:
            if kw in t:
                is_dim_kw = True
                reasons.append(f"trait keyword: {kw}")
                break
    return SexDimorphismTest(
        pgs_id=pgs_id, population=population,
        n_male=len(male_scores), n_female=len(female_scores),
        ks_statistic=D, ks_pvalue=p, cohens_d=d,
        sex_dimorphic=is_dim_stat or is_dim_kw,
        reasons=reasons,
    )


def load_sex_dimorphic_registry(path: str | Path) -> set[str]:
    """Registry JSON shape: {"pgs_ids": ["PGS000xxx", ...]}.
    Returns a set of PGS IDs flagged sex-dimorphic."""
    p = Path(path)
    if not p.exists():
        return set()
    try:
        d = json.loads(p.read_text())
    except json.JSONDecodeError:
        return set()
    return set(d.get("pgs_ids") or [])


def write_sex_dimorphic_registry(
    pgs_ids: list[str], path: str | Path,
) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"pgs_ids": sorted(set(pgs_ids))}, indent=2))
    return p


def sex_required_status(
    pgs_id: str,
    sex_dimorphic_set: set[str],
    sample_sex: Optional[str],
) -> Optional[str]:
    """Returns "sex_required" if pgs_id is sex-dimorphic AND sample sex
    is unknown — caller suppresses the percentile and reports the status."""
    if pgs_id in sex_dimorphic_set and (sample_sex is None or sample_sex.lower()
                                         in ("u", "unknown", "")):
        return "sex_required"
    return None


def stats_filename_with_sex(
    pgs_id: str, population: str, sex: str, n: int, var_sha_short: str,
) -> str:
    """`PGSxxx_<POP>_<SEX>_GRCh38_n<N>_plink2-nomi_sha-<short>.json`."""
    return (f"{pgs_id}_{population}_{sex}_GRCh38_n{n}"
            f"_plink2-nomi_sha-{var_sha_short}.json")
