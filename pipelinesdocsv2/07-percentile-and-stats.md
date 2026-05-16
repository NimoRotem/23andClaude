# 07 — Percentile and Stats

This chapter covers the math, the schema contract, and the live-overlay
behavior. Chapter 06 covers where the stats files live; this chapter
covers how they are loaded and used.

## 7.1 The two percentile compute paths

### 7.1.1 Parametric (production default)

```
z = (score - μ_ref) / σ_ref
p = Φ(z) × 100         (cumulative normal CDF, math.erf)
```

`pipeline/scoring.py::_compute_single_percentile` is the only place
this is implemented. Used by all three scoring entry points
(full-pgen, fast-path, Pipeline E+) via the common
`_compute_percentile_multipop_wrapper` in `runners.py:4193`.

Why parametric? It's deterministic, cheap, and degrades gracefully —
when we lift μ/σ from a single JSON and the sample's z is in range,
the percentile is stable across runs. The downside is the assumption
of approximate normality of the reference distribution; for PGSes
with heavy tails this can over- or under-estimate the tails by a few
percentile points. Sanity gate Φ-z > 6 catches the catastrophic case.

### 7.1.2 ECDF (Phase 2.1 target; not yet primary)

`pipeline/ecdf_percentile.py::ecdf_pipeline` is the Phase-2.1
implementation:

```python
# Linear-interpolation rank percentile:
sort(reference_scores)
idx = searchsorted(arr, target_score, side="left")
rank = i + (target - arr[i-1]) / (arr[i] - arr[i-1])    # ∈ [0, n+1]
percentile = 100.0 * rank / (n + 1)
```

Plus a 1,000-sample bootstrap 95% CI on the ECDF percentile, and a
decile-only cap when `n_ref < 200`. Both EUR/EAS/etc. `<pop>_scores.npy`
arrays in `/data/ref_stats/` (chapter 06 §6.3.2) are the intended
source.

ECDF is the right primary because (a) it doesn't assume normality, (b)
it carries uncertainty (bootstrap CI), and (c) it gracefully handles
small-n panels by widening the CI. The migration to ECDF-primary is
gated on resolving the ref-stats schema issue (chapter 09) — the
`<pop>_scores.npy` files exist but live alongside ref-stats JSONs that
the loader currently refuses, so the live percentile path can't yet
rely on them.

## 7.2 The schema contract (`pipeline/scoring._rs_validate`)

Every ref-stats JSON must declare:

```python
REF_STATS_REQUIRED_KEYS = {
    'pgs_id', 'population', 'genome_build',
    'n_variants', 'variant_ids_sha256',
    'scoring_method', 'imputation_policy',
    'n_samples', 'mean', 'std', 'generated_at',
}
REF_STATS_SCHEMA_VERSION = 1
EXPECTED_SCORING_METHOD  = 'plink2-nomi'
EXPECTED_IMPUTATION      = 'no-mean-imputation'
```

`_rs_validate` raises `IncompatibleRefStats` with a specific reason
when:

| Failure                         | Reason string                                                               |
| ------------------------------- | --------------------------------------------------------------------------- |
| Missing required keys           | `"missing required keys: [...]"`                                            |
| Wrong schema_version            | `"unsupported schema_version: <got>"`                                       |
| Wrong pgs_id / population / build | `"pgs_id mismatch: ..." / "population mismatch: ..." / "genome_build mismatch: ..."` |
| Wrong scoring_method            | `"scoring_method mismatch: file=..., pipeline=plink2-nomi"`                 |
| Wrong imputation_policy         | `"imputation_policy mismatch: file=..., pipeline=no-mean-imputation"`       |
| `std <= 0`                      | `"std <= 0"`                                                                |
| Catalog drift                   | `"scoring-file content drift — catalog file changed since stats were computed"` |

The catalog-drift check computes the live `variant_ids_sha256` from the
current scoring file (memoized by mtime) and compares to
`stats['variant_ids_sha256']`. This is the primary defense against the
PGS000334 stale-cache class of bug.

`_load_stats` catches `IncompatibleRefStats` and attaches it as
`stats['_incompatible_reason']` rather than crashing — the percentile
function then returns `(None, {"method": "incompatible_ref_stats",
"reason": ..., ...})`. **Crucially, the loader does not fall back to
a different file when contract validation fails.** This is the design
intent: once we know a stats file is wrong for a given (PGS, pop), we
must not silently substitute another one. The downside, surfaced in
chapter 09, is that "wrong schema" and "wrong distribution" share a
failure path even though the former is recoverable.

## 7.3 Sanity gates (`_compute_single_percentile`)

After computing `z` and `p`, four gates fire in order:

1. **Gate 1 — |z| > 6 → fail**. `gates_tripped.append("|z|=X > 6 — beyond reference distribution")`, return `(None, details)` with `reason="z_score_extreme"`.
2. **Gate 2 — |z| > 4 → warn**. Kept in result but `gates_tripped` is appended; downstream `_compute_confidence` ratchets confidence to "low".
3. **Gate 3 — collapsed σ**. Look up an "expected std" for this PGS from the next available stats file (EUR or MIX) via `_get_expected_std`. If `ref_std < 0.1 × expected_std`, the distribution collapsed (the ref subset is the wrong panel or the variant set is broken). Return `(None, details)` with `reason="distribution_collapsed"`.
4. **Gate 4 — clamp [0.5, 99.5]**. Hard cap on either end. The clamped flag is surfaced in `details.percentile_capped`.

## 7.4 Scale reconciliation (`AVG` vs `SUM`)

plink2 emits two scoring columns: `SCORE1_AVG = SUM / (2 × N_scored)`
and `SCORE1_SUM`. Some ref-stats JSONs were built against the SUM
scale, some against AVG. When the orders of magnitude diverge, we
silently swap:

```python
compare_score = raw_score
if score_sum is not None:
    if abs(mean) > 1 and abs(raw_score) < abs(mean) * 0.001:
        compare_score = score_sum
        details["scale_correction"] = "Using score_sum vs precomputed SUM-scale stats"
```

The heuristic — "if `|mean| > 1` AND `|raw_score| < 0.001 × |mean|`,
use `score_sum`" — is conservative and only fires when the two scales
are clearly off by ~3 orders of magnitude. The fix is logged; the
report surfaces `details.scale_correction` so the reviewer can audit.

Open question for the reviewer: should we standardize on SUM scale
everywhere and remove this heuristic, or formally tag each stats file
with its scale and require an exact match?

## 7.5 Multi-population percentile (`compute_percentile_multipop`)

Once `select_reference` has chosen `primary, secondary[]` (chapter 05),
we compute percentile against every population for which a stats file
is available — not just primary + secondary. This makes
"compare against X" instant in the UI and gives the AF-match hint a
full panel.

The per-pop output goes into:

```json
"per_pop_percentiles": {
  "EUR": {"percentile": 67.3, "z_score": 0.448, "ref_mean": ..., "ref_std": ...},
  "EAS": {"percentile": 71.0, "z_score": 0.554, "ref_mean": ..., "ref_std": ...},
  "AFR": {"percentile": 82.1, "z_score": 0.921, "ref_mean": ..., "ref_std": ...},
  ...
}
```

`primary_percentile` is the one shown by default; the rest are
toggleable.

## 7.6 AF-match hint

`compute_percentile_multipop` also emits:

```json
"af_match_ref":        "EAS",
"af_match_distance_z": 0.554
```

This is the population whose `mean` is closest to the sample's
`raw_score` in std-units — i.e. the population the user's allele
frequencies most resemble, **independent of PCA**. It often diverges
from `selected_ref` for:

- samples whose ancestry isn't well-represented in 1000G (Middle
  Eastern, Ashkenazi Jewish, etc.) — PCA places them somewhere
  between two centroids, but the allele frequencies match one
  closely.
- samples scored against PGSes whose effect alleles are differentially
  fixed across pops — the dose ranges are themselves population-
  specific.

The UI surfaces this as "scored against EUR (PCA) | best-AF match
would be EAS" so the user can re-score with the better-fit panel if
they want.

## 7.7 `available_refs` and `secondary_percentiles` propagation

For Pipeline E+ (BAM/CRAM direct pileup), there's a special note in
`runners.py:8232`: the multi-pop ref selection results are mirrored
into the result root (`_eplus_result.selected_ref`,
`available_refs`, `secondary_percentiles`, `ancestry_model`) so the
UI and live overlay find them without digging through `pipeline_info`.
Previously these fields were only in `pipeline_info`, the live overlay
defaulted to EUR even when ancestry was correctly detected. This was
the 2026-04 "selected_ref=null" bug, now fixed.

## 7.8 Live overlay (`pipeline/live_percentile.apply_live_overlay`)

Every report-read passes through:

```python
def apply_live_overlay(result):
    raw_score = result["raw_score"]
    pgs_id    = result["pgs_id"]
    selected_ref = result.get("selected_ref", "EUR")

    # Re-load CURRENT stats (may have been rebuilt since this report was generated)
    pctl, details = compute_percentile_for_ref(pgs_id, raw_score, selected_ref)
    if pctl is not None and pctl != result.get("percentile"):
        result["percentile_live"] = pctl
        result["percentile_drift_pct_points"] = pctl - result["percentile"]
        result["percentile"] = pctl

    # Cohort-sanity flag from cron log
    if pgs_id in COHORT_SANITY_FLAGGED:
        result["cohort_sanity_flagged"] = True

    # Hardcoded "known low portability" warning
    pw = portability_warning(pgs_id)
    if pw:
        result["portability_warning"] = pw
        result["low_portability_pgs"] = True

    return result
```

The overlay is the safety net for the PGS000334 stale-cache class:
when ref-stats get rebuilt (because the catalog updated the scoring
file), every prior report's percentile is **re-derived** at read time
from the persisted `raw_score`. Old reports never become stale —
they just show a different percentile than the snapshot in the
underlying JSON, and the drift is surfaced as
`percentile_drift_pct_points`.

## 7.9 Confidence model (`_compute_confidence`)

The `confidence` field is `"high"` if `confidence_reasons` is empty,
else `"low"`. Reasons that demote confidence:

| Reason                       | Trigger |
| ---------------------------- | --- |
| `no_precomputed_stats`       | `method != "precomputed_stats"` (incompatible_ref_stats, unavailable, etc.) |
| `match_rate_below_95pct`     | `match_rate < 95` |
| `build_validation_not_passed`| build validation FAIL or WARN |
| `weak_build_inference`       | `spot_check.status == "WEAK"` |
| `sanity_gates_tripped`       | any sanity-gate appended to gates_tripped |
| `cross_ancestry_transfer`    | `selected_ref != "EUR"` (the PGS was scored against EUR weights) |

The `cross_ancestry_transfer` reason is unconditional for non-EUR
users: every PGS the system loads was originally trained on a
European cohort (we don't run PRS-CSx), so even when EAS ref-stats
exist and produce a clean percentile, the underlying score is
ancestrally biased. The `cross_ancestry_warning` prose string in the
result (chapter 04 §4.8) is the artifact the LLM interpretation sees.

## 7.10 ECDF parity check (planned)

The Phase 2.1 plan is to compute **both** parametric and ECDF
percentiles, report both, and flag samples where they disagree by
> 5 percentile points as a `ecdf_phi_disagreement` confidence reason.
Disagreement is itself diagnostic — a heavy-tailed PGS will produce
divergent estimates and the divergence is the signal that the
parametric assumption is the wrong model for that PGS. Wiring this
in is blocked on chapter-09 schema consolidation.
