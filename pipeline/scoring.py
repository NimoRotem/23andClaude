"""Ancestry-aware reference selection and multi-population percentile computation.

This module replaces the EUR-only _compute_percentile() in runners.py with
an ancestry-aware version that selects the best reference population based
on the sample's ancestry composition.
"""
import json
import logging
import math
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .config import (
    LEGACY_REF_PANEL_STATS, REF_STATS_DIR,
    POPULATIONS, ref_stats_path,
)
from . import db as pipeline_db

logger = logging.getLogger("pgs-pipeline")


@dataclass
class RefSelection:
    """Which reference population(s) to use for percentile computation."""
    primary: str                    # e.g. "EUR", "MIX"
    secondary: List[str] = field(default_factory=list)  # e.g. ["EAS", "SAS"]
    reason: str = ""               # explanation of selection logic
    ancestry_proportions: Dict = field(default_factory=dict)


@dataclass
class PercentileResult:
    """Result of computing percentile against one or more reference populations."""
    primary_percentile: Optional[float] = None
    primary_ref: Optional[str] = None
    primary_details: Dict = field(default_factory=dict)
    secondary_percentiles: Dict = field(default_factory=dict)  # {pop: percentile}
    all_details: Dict = field(default_factory=dict)  # {pop: details_dict}
    selected_ref: Optional[str] = None
    available_refs: List[str] = field(default_factory=list)
    ancestry_model: Optional[str] = None
    reason: Optional[str] = None  # if percentile is null, why


def select_reference(ancestry_result: Optional[Dict], pgs_id: str,
                     genome_build: str = "GRCh38") -> RefSelection:
    """Select the best reference population based on ancestry.

    Rules:
      - If single-cluster (>80% one pop) → use that pop, top 2 others as secondary
      - If admixed (no pop >80%) → MIX primary, top 2 components as secondary
      - If no ancestry data → EUR primary, MIX secondary (legacy behavior)
      - If selected ref has no stats → still select it (caller handles null percentile)
    """
    if not ancestry_result:
        return RefSelection(
            primary="EUR",
            secondary=["MIX"],
            reason="no_ancestry_data (default EUR)",
        )

    # Extract proportions from ancestry result
    # Format from runners.py _run_admixture_from_pca: {pop: proportion}
    proportions = {}
    if isinstance(ancestry_result, dict):
        # Could be direct proportions dict or nested result
        if "proportions" in ancestry_result:
            proportions = ancestry_result["proportions"]
        elif "admixture" in ancestry_result:
            proportions = ancestry_result["admixture"]
        else:
            # Assume it IS the proportions dict if keys look like pop codes
            pop_codes = set(POPULATIONS.keys())
            if any(k in pop_codes for k in ancestry_result):
                proportions = {k: v for k, v in ancestry_result.items()
                               if k in pop_codes and isinstance(v, (int, float))}

    if not proportions:
        return RefSelection(
            primary="EUR",
            secondary=["MIX"],
            reason="ancestry_data_unparseable (default EUR)",
            ancestry_proportions=proportions,
        )

    # Sort populations by proportion (descending)
    sorted_pops = sorted(proportions.items(), key=lambda x: x[1], reverse=True)
    top_pop, top_prop = sorted_pops[0]

    if top_prop >= 0.80:
        # Single-cluster: use top population
        secondaries = [p for p, _ in sorted_pops[1:3] if p != top_pop]
        return RefSelection(
            primary=top_pop,
            secondary=secondaries,
            reason=f"single_cluster ({top_pop}={top_prop:.0%})",
            ancestry_proportions=proportions,
        )
    else:
        # Admixed: use MIX as primary
        secondaries = [p for p, _ in sorted_pops[:2]]
        return RefSelection(
            primary="MIX",
            secondary=secondaries,
            reason=f"admixed (top={top_pop}={top_prop:.0%})",
            ancestry_proportions=proportions,
        )


def compute_percentile_multipop(pgs_id: str, raw_score: float,
                                 ref_selection: RefSelection,
                                 score_sum: float = None,
                                 genome_build: str = "GRCh38") -> PercentileResult:
    """Compute percentile against primary + secondary reference populations.

    Uses the same z-score formula as the original _compute_percentile:
      z = (score - mean) / std
      p = Φ(z) * 100

    Same sanity gates: |z|>6 fail, |z|>4 warn, clamp [0.5, 99.5].
    Same scale reconciliation: detect AVG vs SUM mismatch.

    Falls back to legacy /data/pgs2/ref_panel_stats/ for EUR if new path missing.
    """
    result = PercentileResult(
        selected_ref=ref_selection.primary,
        ancestry_model=ref_selection.reason,
    )

    # Determine which refs are available
    available = _get_available_refs_list(pgs_id, genome_build)
    result.available_refs = available

    # Compute for all requested populations (primary + secondary)
    all_pops = [ref_selection.primary] + ref_selection.secondary
    for pop in all_pops:
        pctl, details = _compute_single_percentile(
            pgs_id, raw_score, pop, score_sum, genome_build)
        result.all_details[pop] = details

        if pop == ref_selection.primary:
            result.primary_percentile = pctl
            result.primary_ref = pop
            result.primary_details = details
            if pctl is None:
                result.reason = details.get("reason", "no_reference_available")
        else:
            result.secondary_percentiles[pop] = pctl

    return result


def compute_percentile_for_ref(pgs_id: str, raw_score: float,
                                population: str, score_sum: float = None,
                                genome_build: str = "GRCh38") -> Tuple[Optional[float], Dict]:
    """Compute percentile against a specific reference population.

    Used by the manual ref-switch API endpoint.
    Returns (percentile, details_dict).
    """
    return _compute_single_percentile(pgs_id, raw_score, population, score_sum, genome_build)


def _compute_single_percentile(pgs_id: str, raw_score: float,
                                population: str, score_sum: float = None,
                                genome_build: str = "GRCh38") -> Tuple[Optional[float], Dict]:
    """Compute percentile against a single reference population."""
    pop_label = POPULATIONS.get(population, {}).get("label", population)
    details = {
        "method": None,
        "reference_population": f"{population} ({pop_label})",
        "reference_panel": "1000 Genomes Phase 3 (GRCh38)",
        "formula": "percentile = Φ((score - μ_ref) / σ_ref) × 100",
        "ref_mean": None,
        "ref_std": None,
        "z_score": None,
    }

    # Load stats: try new path first, then legacy EUR path
    stats = _load_stats(pgs_id, population, genome_build)

    if not stats:
        details["method"] = "unavailable"
        details["reason"] = f"No reference stats for {population}"
        details["description"] = (f"No precomputed reference stats available for "
                                  f"{population}. Score computed but percentile "
                                  f"cannot be determined.")
        return None, details

    mean = stats.get("mean", 0)
    std = stats.get("std", 0)
    n_samples = stats.get("n_samples", 0)

    details["n_samples"] = n_samples

    if std <= 0:
        details["method"] = "precomputed_stats"
        details["reason"] = "ref_std_zero"
        details["description"] = "Reference std is zero — cannot compute percentile"
        return None, details

    # Scale reconciliation
    compare_score = raw_score
    if score_sum is not None:
        if abs(mean) > 1 and abs(raw_score) < abs(mean) * 0.001:
            compare_score = score_sum
            details["scale_correction"] = "Using score_sum vs precomputed SUM-scale stats"
            logger.info(f"{pgs_id}/{population}: scale mismatch — "
                       f"raw={raw_score:.4g} vs mean={mean:.4g}; using sum={score_sum:.4g}")

    # Compute z-score and percentile
    z = (compare_score - mean) / std
    p = 0.5 * (1 + math.erf(z / math.sqrt(2))) * 100

    details["method"] = "precomputed_stats"
    details["ref_mean"] = round(mean, 6)
    details["ref_std"] = round(std, 6)
    details["z_score"] = round(z, 3)
    details["description"] = f"Used precomputed {population} reference distribution stats"
    if stats.get("stats_file"):
        details["stats_file"] = stats["stats_file"]

    # Sanity gates
    sanity = {"gates_tripped": []}

    # Gate 1: |z| > 6 → fail
    if abs(z) > 6:
        sanity["gates_tripped"].append(f"|z|={abs(z):.1f} > 6 — beyond reference distribution")
        details["sanity"] = sanity
        details["reason"] = "z_score_extreme"
        logger.warning(f"{pgs_id}/{population}: |z|={abs(z):.1f} > 6, percentile unreliable")
        return None, details

    # Gate 2: |z| > 4 → warn
    if abs(z) > 4:
        sanity["gates_tripped"].append(f"|z|={abs(z):.1f} > 4 — extreme tail")

    # Gate 3: ref_std suspiciously small
    expected_std = _get_expected_std(pgs_id)
    if expected_std and std < expected_std * 0.1:
        sanity["gates_tripped"].append(
            f"ref_std={std:.6f} < 10% of expected ({expected_std:.6f}) — distribution collapsed")
        details["sanity"] = sanity
        details["reason"] = "distribution_collapsed"
        logger.warning(f"{pgs_id}/{population}: ref_std collapsed")
        return None, details

    # Gate 4: Clamp [0.5, 99.5]
    percentile_capped = False
    if p < 0.5:
        p = 0.5
        percentile_capped = True
        sanity["gates_tripped"].append("percentile capped at 0.5")
    elif p > 99.5:
        p = 99.5
        percentile_capped = True
        sanity["gates_tripped"].append("percentile capped at 99.5")

    details["sanity"] = sanity
    details["percentile_capped"] = percentile_capped

    return round(p, 1), details


def _load_stats(pgs_id: str, population: str,
                genome_build: str = "GRCh38") -> Optional[Dict]:
    """Load stats from new multi-pop path, falling back to legacy for EUR."""
    # Try new path first
    new_path = ref_stats_path(pgs_id, population, genome_build)
    if os.path.exists(new_path):
        try:
            with open(new_path) as f:
                stats = json.load(f)
            stats["stats_file"] = new_path
            if stats.get("std", 0) > 0:
                return stats
        except (json.JSONDecodeError, KeyError):
            pass

    # Try DB
    try:
        db_stats = pipeline_db.get_ref_stats(pgs_id, population, genome_build)
        if db_stats and db_stats.get("std", 0) > 0:
            return db_stats
    except Exception:
        pass

    # Legacy fallback for EUR only
    if population == "EUR":
        return _load_legacy_stats(pgs_id)

    return None


def _load_legacy_stats(pgs_id: str) -> Optional[Dict]:
    """Load from legacy /data/pgs2/ref_panel_stats/ (EUR-only)."""
    candidates = [
        f"{pgs_id}_EUR_GRCh38.json",
        f"{pgs_id}_EUR_GRCh37.json",
        f"{pgs_id}_EUR.json",
        f"{pgs_id}.json",
    ]
    for name in candidates:
        path = os.path.join(LEGACY_REF_PANEL_STATS, name)
        if os.path.exists(path):
            try:
                with open(path) as f:
                    stats = json.load(f)
                if stats.get("std", 0) > 0:
                    stats["stats_file"] = path
                    return stats
            except (json.JSONDecodeError, KeyError):
                pass

    # Prefix glob fallback
    prefix = f"{pgs_id}_EUR_GRCh38"
    try:
        for fname in os.listdir(LEGACY_REF_PANEL_STATS):
            if fname.startswith(prefix) and fname.endswith(".json"):
                path = os.path.join(LEGACY_REF_PANEL_STATS, fname)
                try:
                    with open(path) as f:
                        stats = json.load(f)
                    if stats.get("std", 0) > 0:
                        stats["stats_file"] = path
                        return stats
                except (json.JSONDecodeError, KeyError):
                    pass
    except OSError:
        pass

    return None


def _get_expected_std(pgs_id: str) -> Optional[float]:
    """Get expected std for sanity checking (from any available stats)."""
    # Try legacy EUR stats first (broadest coverage)
    stats = _load_legacy_stats(pgs_id)
    if stats:
        return stats.get("std")
    return None


def _get_available_refs_list(pgs_id: str, genome_build: str = "GRCh38") -> List[str]:
    """Return list of population codes that have stats available."""
    available = []

    # Check new paths
    for pop in POPULATIONS:
        if pop == "MID":
            continue
        path = ref_stats_path(pgs_id, pop, genome_build)
        if os.path.exists(path):
            available.append(pop)

    # Check legacy EUR if not already found
    if "EUR" not in available:
        legacy = _load_legacy_stats(pgs_id)
        if legacy:
            available.append("EUR")

    return available
