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
from . import registry as _ref_registry

logger = logging.getLogger("pgs-pipeline")


# === REF_STATS_CONTRACT_START — hard contract for ref-stats files ===
# Stats files must declare schema_version, pgs_id, population, scoring_method,
# variant_ids_sha256, n_variants, n_samples, mean, std, generated_at.
# At load time the file is rejected if any of those are missing OR if any of
# them disagree with what the live pipeline would produce. The percentile
# function then returns method='incompatible_ref_stats' with the specific
# mismatch reason — no silent z-score against the wrong distribution.
import hashlib as _rs_hashlib
import gzip as _rs_gzip

REF_STATS_SCHEMA_VERSION = 1

REF_STATS_REQUIRED_KEYS = {
    'pgs_id', 'population', 'genome_build',
    'n_variants', 'variant_ids_sha256',
    'scoring_method', 'imputation_policy',
    'n_samples', 'mean', 'std', 'generated_at',
}

# What the live pipeline does today. Stats files declaring a different scoring
# method are not comparable to live scores → refuse.
EXPECTED_SCORING_METHOD = 'plink2-nomi'
EXPECTED_IMPUTATION     = 'no-mean-imputation'

_PGS_CACHE_DIR = '/data/pgs_cache'


class IncompatibleRefStats(Exception):
    """Raised when a loaded ref-stats file disagrees with the live scoring pipeline."""

    def __init__(self, reason, stats_file=None, details=None):
        super().__init__(reason)
        self.reason = reason
        self.stats_file = stats_file
        self.details = details or {}


_VARIANT_SET_SHA_CACHE: Dict[str, tuple] = {}


def _rs_variant_set_sha_from_catalog(pgs_id):
    """Hash the variant set the live pipeline actually produces, for ref-stats
    compatibility checks.

    Calls match_logic.parse_pgs_scoring_file directly — the same parser that
    feeds plink2 — so a future parser drift triggers IncompatibleRefStats
    instead of silently scoring against the wrong distribution (PGS000327
    incident, 2026-05-14).

    Canonical form must match recompute_ref_stats.py::variant_set_sha:
        sorted '
'-joined 'chr|int(pos)|effect_allele|float(weight)' lines.

    Memoized by (pgs_id, file mtime).
    """
    from .match_logic import parse_pgs_scoring_file
    path = os.path.join(_PGS_CACHE_DIR, pgs_id, f'{pgs_id}_hmPOS_GRCh38.txt.gz')
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None, None
    cached = _VARIANT_SET_SHA_CACHE.get(pgs_id)
    if cached and cached[0] == mtime:
        return cached[1], cached[2]
    try:
        _meta, variants = parse_pgs_scoring_file(path)
    except Exception:
        return None, None
    rows = []
    for v in variants:
        try:
            rows.append(f"{v.chrom}|{int(v.pos)}|{v.effect_allele}|{float(v.weight)}")
        except (TypeError, ValueError):
            continue
    canon = "\n".join(sorted(rows))
    sha = _rs_hashlib.sha256(canon.encode()).hexdigest()
    _VARIANT_SET_SHA_CACHE[pgs_id] = (mtime, sha, len(rows))
    return sha, len(rows)


def _rs_validate(stats, pgs_id, population, genome_build, stats_file):
    """Raise IncompatibleRefStats if the loaded stats don't match the pipeline."""
    missing = REF_STATS_REQUIRED_KEYS - set(stats.keys())
    if missing:
        raise IncompatibleRefStats(
            f"missing required keys: {sorted(missing)}",
            stats_file=stats_file, details={'missing_keys': sorted(missing)})
    if stats.get('schema_version') != REF_STATS_SCHEMA_VERSION:
        raise IncompatibleRefStats(
            f"unsupported schema_version: {stats.get('schema_version')!r}",
            stats_file=stats_file, details={'got': stats.get('schema_version')})
    if stats.get('std', 0) <= 0:
        raise IncompatibleRefStats(
            "std <= 0", stats_file=stats_file, details={'std': stats.get('std')})
    if stats.get('pgs_id') != pgs_id:
        raise IncompatibleRefStats(
            f"pgs_id mismatch: {stats.get('pgs_id')!r} vs expected {pgs_id!r}",
            stats_file=stats_file)
    if stats.get('population') != population:
        raise IncompatibleRefStats(
            f"population mismatch: {stats.get('population')!r} vs expected {population!r}",
            stats_file=stats_file)
    if stats.get('genome_build') != genome_build:
        raise IncompatibleRefStats(
            f"genome_build mismatch: {stats.get('genome_build')!r} vs expected {genome_build!r}",
            stats_file=stats_file)
    if stats.get('scoring_method') != EXPECTED_SCORING_METHOD:
        raise IncompatibleRefStats(
            f"scoring_method mismatch: file={stats.get('scoring_method')!r}, "
            f"pipeline={EXPECTED_SCORING_METHOD!r}",
            stats_file=stats_file, details={'file_method': stats.get('scoring_method')})
    if stats.get('imputation_policy') != EXPECTED_IMPUTATION:
        raise IncompatibleRefStats(
            f"imputation_policy mismatch: file={stats.get('imputation_policy')!r}, "
            f"pipeline={EXPECTED_IMPUTATION!r}",
            stats_file=stats_file)
    expected_sha, _n_cat = _rs_variant_set_sha_from_catalog(pgs_id)
    file_sha = stats.get('scoring_file_content_sha256') or stats.get('variant_ids_sha256')
    if expected_sha is not None and file_sha and file_sha != expected_sha:
        raise IncompatibleRefStats(
            "scoring-file content drift — catalog file changed since stats were computed",
            stats_file=stats_file,
            details={'expected_sha_12': expected_sha[:12],
                     'file_sha_12':     file_sha[:12]})
# === REF_STATS_CONTRACT_END ===


@dataclass
class RefSelection:
    """Which reference population(s) to use for percentile computation.

    Phase 1.5 renamed `ancestry_proportions` → `pca_population_weights`
    because the value the caller passes today comes from inverse-distance
    PCA (NOT a true ADMIXTURE run). We keep the old name as a property so
    existing JSON consumers don't break.
    """
    primary: str                    # "EUR" | "EAS" | … | "MULTI" | "UNRESOLVED" | "UNSUPPORTED"
    secondary: List[str] = field(default_factory=list)
    reason: str = ""
    pca_population_weights: Dict = field(default_factory=dict)
    # Legacy alias — read-only @property below. Setter keeps both fields in
    # sync so callers that still pass `ancestry_proportions=` keep working.
    @property
    def ancestry_proportions(self) -> Dict:
        return self.pca_population_weights

    def __init__(self, primary: str, secondary: List[str] | None = None,
                 reason: str = "", pca_population_weights: Dict | None = None,
                 ancestry_proportions: Dict | None = None):
        self.primary = primary
        self.secondary = list(secondary or [])
        self.reason = reason
        # Accept the legacy kwarg as a synonym
        self.pca_population_weights = dict(
            pca_population_weights or ancestry_proportions or {}
        )


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
    # Best-AF-match hint: which population's mean is closest to this sample's
    # raw_score. NOT the same as ancestry-PCA selected_ref — this picks the
    # population that the user's ALLELE FREQUENCIES (as expressed in their
    # PGS-variant doses) most resemble, which often differs from PCA-based
    # ancestry for samples with admixture or non-1000G-represented background.
    af_match_ref: Optional[str] = None
    af_match_distance_z: Optional[float] = None  # |raw - pop_mean| / pop_std


def select_reference(ancestry_result: Optional[Dict], pgs_id: str,
                     genome_build: str = "GRCh38") -> RefSelection:
    """Select the best reference population based on ancestry.

    Phase 1.5 rules (REMEDIATION_PLAN §1.5):
      - Top-population posterior ≥ 0.80 → use that pop as primary.
      - Top-population posterior < 0.80 → primary="MULTI"; the percentile
        path emits `percentile_by_population: {EUR, EAS, AFR, SAS, AMR}`
        AND a weighted-mixture percentile sourced from supervised global-
        ancestry (when available) rather than inverse-distance PCA.
      - No ancestry data → primary="UNRESOLVED"; the caller emits
        `status="ancestry_unresolved"` and the multi-pop array; we do
        NOT default to EUR anymore.
      - Ancestries unsupported by 1000G (Middle Eastern, Pacific
        Islander, etc.) signaled by `ancestry_result["unsupported"]=True`
        → primary="UNSUPPORTED"; caller emits the multi-pop sensitivity
        array with status="ancestry_unsupported".

    The legacy fixed `MIX = 50%EUR + 50%EAS` default has been removed.
    Renamed `admixture_proportions` → `pca_population_weights` everywhere
    these come from inverse-distance PCA (which is what the upstream
    function in runners.py produces today).
    """
    POPS_5 = ["EUR", "EAS", "AFR", "SAS", "AMR"]
    if not ancestry_result:
        return RefSelection(
            primary="UNRESOLVED",
            secondary=POPS_5,
            reason="no_ancestry_data — multi-pop array required, EUR default removed (§1.5)",
        )

    proportions = {}
    if isinstance(ancestry_result, dict):
        if ancestry_result.get("unsupported"):
            return RefSelection(
                primary="UNSUPPORTED",
                secondary=POPS_5,
                reason=ancestry_result.get("reason", "ancestry not represented in 1000G"),
            )
        if "proportions" in ancestry_result:
            proportions = ancestry_result["proportions"]
        elif "admixture" in ancestry_result:
            proportions = ancestry_result["admixture"]
        else:
            pop_codes = set(POPULATIONS.keys())
            if any(k in pop_codes for k in ancestry_result):
                proportions = {k: v for k, v in ancestry_result.items()
                               if k in pop_codes and isinstance(v, (int, float))}

    if not proportions:
        return RefSelection(
            primary="UNRESOLVED",
            secondary=POPS_5,
            reason="ancestry_data_unparseable — multi-pop array required (§1.5)",
            ancestry_proportions=proportions,
        )

    sorted_pops = sorted(proportions.items(), key=lambda x: x[1], reverse=True)
    top_pop, top_prop = sorted_pops[0]
    if top_prop >= 0.80:
        secondaries = [p for p, _ in sorted_pops[1:3] if p != top_pop]
        return RefSelection(
            primary=top_pop,
            secondary=secondaries,
            reason=f"single_cluster ({top_pop}={top_prop:.0%})",
            ancestry_proportions=proportions,
        )
    # Posterior < 0.80 → emit multi-pop array; no fixed MIX.
    return RefSelection(
        primary="MULTI",
        secondary=POPS_5,
        reason=(f"admixed (top={top_pop}={top_prop:.0%} < 0.80) — emit "
                f"percentile_by_population array; no fixed MIX (§1.5)"),
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

    # Compute against EVERY available population, not just primary+secondary.
    # The user can compare their score against any of them, and we use the
    # full set to compute the AF-match hint (the population whose mean+std
    # best fits the sample's raw_score).
    all_pops = sorted(set([ref_selection.primary]
                          + ref_selection.secondary
                          + available))
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

    # AF-match hint: pick the population whose mean is closest to the
    # raw_score (in std-units). This often differs from the ancestry-PCA
    # primary when the sample is from a population not well-represented in
    # 1000G (e.g. Middle Eastern, mixed Ashkenazi). Surfacing both lets the
    # UI show the user "scored against EUR (PCA) | best-AF match would be
    # EAS" so they can re-score with the better-fit panel.
    best_pop = None
    best_z = None
    compare_score = raw_score
    if score_sum is not None:
        # Match the same scale-reconciliation logic used downstream
        any_mean = next((d.get("ref_mean") for d in result.all_details.values()
                         if d.get("ref_mean") is not None), None)
        if any_mean is not None and abs(any_mean) > 1 and abs(raw_score) < abs(any_mean) * 0.001:
            compare_score = score_sum
    for pop, d in result.all_details.items():
        m = d.get("ref_mean")
        s = d.get("ref_std")
        if m is None or not s or s <= 0:
            continue
        z = abs((compare_score - m) / s)
        if best_z is None or z < best_z:
            best_z = z
            best_pop = pop
    if best_pop is not None:
        result.af_match_ref = best_pop
        result.af_match_distance_z = round(best_z, 3) if best_z is not None else None

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
        "reference_panel": "1000G + NYGC high-coverage, GRCh38",
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

    if stats.get("_incompatible_reason"):
        details["method"] = "incompatible_ref_stats"
        details["reason"] = stats["_incompatible_reason"]
        details["incompatible_details"] = stats.get("_incompatible_details")
        details["stats_file"] = stats.get("stats_file")
        details["description"] = ("Reference stats file failed the pipeline contract "
                                  "("+stats["_incompatible_reason"]+"). "
                                  "Percentile refused.")
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
    """Load + STRICT-VALIDATE stats for (pgs_id, population, genome_build).

    Returns None if no stats file found. If a file is found but fails the
    schema/pipeline contract, attaches the IncompatibleRefStats reason as
    stats["_incompatible_reason"] and returns the dict (so the caller can
    surface the specific mismatch). std<=0 is treated as a schema failure.
    """
    def _candidate_stats():
        # Registry-first: the canonical pointer for (pgs,pop,build,method).
        # /data/pgs2/ref_panel_stats/registry.json is updated by recompute_ref_stats.py
        # so this is the only path that picks up sweep corrections without
        # restarting or rewriting the per-PGS layout.
        reg_path = _ref_registry.resolve(pgs_id, population, genome_build)
        if reg_path and os.path.exists(reg_path):
            try:
                with open(reg_path) as f:
                    s = json.load(f)
                s["stats_file"] = reg_path
                return s
            except (json.JSONDecodeError, OSError):
                pass
        # New multi-pop path
        new_path = ref_stats_path(pgs_id, population, genome_build)
        if os.path.exists(new_path):
            try:
                with open(new_path) as f:
                    s = json.load(f)
                s["stats_file"] = new_path
                return s
            except (json.JSONDecodeError, OSError):
                pass
        # DB
        try:
            db_stats = pipeline_db.get_ref_stats(pgs_id, population, genome_build)
            if db_stats:
                return db_stats
        except Exception:
            pass
        # Legacy fallback for EUR only
        if population == "EUR":
            return _load_legacy_stats(pgs_id)
        return None

    s = _candidate_stats()
    if not s:
        return None
    try:
        _rs_validate(s, pgs_id, population, genome_build, s.get("stats_file"))
    except IncompatibleRefStats as e:
        s["_incompatible_reason"] = e.reason
        s["_incompatible_details"] = e.details
        logger.debug(f"{pgs_id}/{population}: ref-stats incompatible — {e.reason} "
                     f"(file={s.get('stats_file')})")
    return s


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
    """Get expected std for sanity checking (from same source as _load_stats)."""
    # Try new multi-pop stats first (same scale as _load_stats)
    for pop in ["EUR", "MIX"]:
        new_path = ref_stats_path(pgs_id, pop, "GRCh38")
        if os.path.exists(new_path):
            try:
                with open(new_path) as f:
                    data = json.load(f)
                if data.get("std", 0) > 0:
                    return data["std"]
            except (json.JSONDecodeError, KeyError):
                pass
    # Fall back to legacy
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
