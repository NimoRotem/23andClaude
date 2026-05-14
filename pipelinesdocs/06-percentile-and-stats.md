# 06 — Reference Stats, Percentile, and the Hard Contract

The single most safety-critical step is mapping a `raw_score` to a
`percentile`. This document covers the file format of the cached
reference distributions, the schema contract that prevents silent
mismatches between live and cached pipelines, the percentile formula,
sanity gates, and the read-time live-overlay recomputation.

Code: `pipeline/scoring.py`, `pipeline/registry.py`,
`pipeline/live_percentile.py`, `scripts/recompute_ref_stats.py`,
`scripts/ref_stats_selftest.py`.

## 6.1 Reference-stats JSON format

One JSON per (PGS, population, genome_build) at
`/data/pgs2/ref_panel_stats/` (legacy) or `/data/ref_stats/<PGS>/<POP>_<BUILD>.json`
(new). Filename convention encodes the pipeline fingerprint:

```
PGS000334_EUR_GRCh38_n22_plink2-nomi_sha-8e9c4f12.json
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
PGS_ID  POP  BUILD  n_variants  method  variant_set_sha[0:8]
```

Inside (schema v1):

```json
{
  "schema_version": 1,
  "pgs_id": "PGS000334",
  "population": "EUR",
  "genome_build": "GRCh38",
  "n_variants": 22,
  "variant_ids_sha256": "8e9c4f12fa2b6a0e0c1d4e8f7a3b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b",
  "scoring_method": "plink2-nomi",
  "imputation_policy": "no-mean-imputation",
  "ref_panel": "GRCh38_1000G_ALL",
  "ref_panel_sha256": "<sha256 of (path,size,mtime) tuples for .pgen/.pvar.zst/.psam>",
  "sample_filter": "1000G phase 3 unrelated, super_pop=EUR (n=633)",
  "n_samples": 633,
  "mean": 0.000124,
  "std":  0.000891,
  "median": 0.000118,
  "min":  -0.002301,
  "max":   0.003142,
  "sum_mean": 0.005456,
  "sum_std":  0.039204,
  "generated_at": "2026-05-11T05:48:50Z",
  "generated_by_pipeline_version": "simple-genomics@3ede0ee",
  "generated_by_commit": "3ede0ee...",
  "stale_replaces": "PGS000334_EUR_GRCh38_n16_plink2-nomi_sha-abc12345.json"
}
```

`stale_replaces` is set when a recompute supersedes an older file and
renames the predecessor to `*.stale-bias-YYYYMMDD.json`. Stale files
are kept on disk for forensics; the registry ignores any name
containing `.stale-bias`.

## 6.2 The hard contract (`pipeline/scoring.py::_rs_validate`)

A loaded stats file is **rejected** if any of these fail:

1. **Required keys present**: `pgs_id, population, genome_build,
   n_variants, variant_ids_sha256, scoring_method, imputation_policy,
   n_samples, mean, std, generated_at` (any missing → reject).
2. **`schema_version == 1`** (any other → reject).
3. **`std > 0`** (zero → reject, percentile undefined).
4. **`pgs_id` matches the requested PGS** (mismatch → reject).
5. **`scoring_method == "plink2-nomi"`** (we currently produce live
   scores with this exact mode; anything else is incomparable).
6. **`imputation_policy == "no-mean-imputation"`** (same reason).
7. **`variant_ids_sha256` matches the live recomputation of
   `_rs_variant_set_sha_from_catalog(pgs_id)`** — the canonical
   `chr|pos|ea|weight` sha over the current scoring file. Mismatch
   means the scoring file was edited (or re-ingested) after the stats
   were built; the stats no longer describe the distribution the live
   pipeline produces.

A failed load returns `IncompatibleRefStats(reason=...)`, and
`_compute_single_percentile` returns `(None, {"method":
"incompatible_ref_stats", "reason": <reason>, ...})`. The report shows
no percentile but explains why. This is deliberate: silently
z-scoring against a hash-mismatched ref produced PGS000334-class
20-percentile errors before the contract was added.

`_rs_variant_set_sha_from_catalog` is memoized by scoring-file mtime
because the canonical hash over a 6M-variant PGS would otherwise be
multi-second work per `/api/status` call.

## 6.3 Percentile computation

`pipeline/scoring.py::_compute_single_percentile(pgs_id, raw_score,
population, score_sum=None, genome_build="GRCh38")`:

```
# 1. Load stats (new path first, then legacy EUR-only path)
stats = _load_stats(pgs_id, population, genome_build)
if not stats:
    return (None, method="unavailable")
if stats["_incompatible_reason"]:
    return (None, method="incompatible_ref_stats", reason=...)

mean, std = stats["mean"], stats["std"]
if std <= 0:
    return (None, method="precomputed_stats", reason="ref_std_zero")

# 2. Scale reconciliation
compare_score = raw_score
if score_sum and abs(mean) > 1 and abs(raw_score) < abs(mean) * 0.001:
    compare_score = score_sum
    details["scale_correction"] = "Using score_sum vs SUM-scale stats"

# 3. z-score + percentile
z = (compare_score - mean) / std
p = 0.5 * (1 + math.erf(z / sqrt(2))) * 100

# 4. Sanity gates
if abs(z) > 6:
    return (None, reason="z_score_extreme")           # gate FAIL
if abs(z) > 4:
    sanity.gates_tripped += "extreme tail"            # gate WARN

if std < expected_std * 0.1:                          # collapsed σ
    return (None, reason="distribution_collapsed")    # gate FAIL

# 5. Clamp tails
if p < 0.5: p = 0.5;  details["percentile_capped"] = True
if p > 99.5: p = 99.5; details["percentile_capped"] = True
```

`_get_expected_std(pgs_id)` consults a registry of expected sigmas
(from the larger of "EUR" / aggregate panel rescores; persisted in
`pgs_stats_audit.json`) to detect "looks plausible but actually
ten-times-too-small" stats. This catches the failure mode where a ref
panel was rebuilt against only a handful of variants and σ collapsed.

## 6.4 Multi-population mode

`compute_percentile_multipop(pgs_id, raw_score, ref_selection,
score_sum)`:

```
1. Build all_pops = [primary] + secondary
2. For each pop:
     pctl, details = _compute_single_percentile(pgs_id, raw_score, pop, ...)
3. primary_percentile      = primary's pctl
   secondary_percentiles   = {pop: pctl for pop in secondary}
   all_details             = {pop: details for pop in all_pops}
```

The report ships all three by default. The UI shows the primary
prominently and the others as "vs EAS: 67.3%ile" pills.

`available_refs` lists every population that *has* a ref-stats file
for this PGS so the UI knows which "compare against" options to
enable.

## 6.5 Dynamic (matched-subset) fallback

For custom user-uploaded PGS, or when the precomputed stats are
incompatible but the raw score is sane, the runner can fall back to a
dynamic recompute:

`runners._score_ref_panel_matched(pgs_id, scoring_file, matched_vars_path,
tmpdir)`:

```
1. Read .sscore.vars from the live run → set of variant IDs (~thousands)
2. Filter the canonical scoring_refpanel.tsv to that subset
3. plink2 --score against GRCh38_1000G_ALL (no --keep) using only those vars
4. Parse → list of per-sample raw_scores
5. Compute μ, σ on the fly
6. Return (μ, σ, n_samples) for the percentile call
```

This is reported with `method=dynamic_matched_subset`. It is slower
(~30s for ~1000 variants) and has wider confidence intervals, so we
prefer the cached path whenever available.

## 6.6 Live overlay at report read

`pipeline/live_percentile.py::apply_live_overlay(report)`:

When a stored report is read (UI page load, /api/reports, etc.), the
overlay recomputes percentile from the stored `raw_score` against the
**current** ref-stats μ/σ:

```
if report has raw_score and pgs_id:
    new_pctl, _ = _compute_single_percentile(pgs_id, raw_score, selected_ref)
    if new_pctl is not None and new_pctl != stored_pctl:
        report.percentile_at_scoring = stored_pctl   # preserved for audit
        report.percentile            = new_pctl
        report.live_overlay_applied  = True
```

Why: when a ref-stats file is corrected (e.g. n16 → n22 after the
PGS000334 incident), all historical reports would otherwise show the
old biased percentile despite live μ/σ being right. The overlay makes
stored reports always read-consistent with current stats. The original
at-scoring percentile is preserved as `percentile_at_scoring` so the
audit trail isn't lost.

The overlay is a strict no-op when:
- the report has no `raw_score`
- no current stats file resolves for (pgs_id, selected_ref)
- the current stats file fails the schema contract
- the recomputed value equals the stored value

Cohort-flagged PGS (KS p<0.01 in the per-batch sanity check) are
tagged `cohort_sanity_flagged: True` and the UI emits a warning that
percentile is statistically unreliable for the user's cohort against
the 1000G ref — not because the math is wrong, but because the user's
samples deviate from 1000G in a way that biases the distribution
location.

## 6.7 Provenance attach (`result_guards.attach_provenance`)

Every report dict gets a `provenance` block:

```json
{
  "scoring_file_sha":  "sha256 of /data/pgs_cache/PGS000334/scoring_clean.tsv.gz",
  "stats_file_sha":    "sha256 of the loaded ref-stats JSON",
  "ref_panel_sha":     "sha256 of (path,size,mtime) for .pgen/.pvar.zst/.psam",
  "pipeline_commit":   "git rev-parse HEAD at score time",
  "stats_file_path":   "/data/ref_stats/PGS000334/EUR_GRCh38.json",
  "scoring_file_path": "/data/pgs_cache/PGS000334/scoring_clean.tsv.gz"
}
```

This is the audit trail. Given any historical result and its
provenance block, the entire computation can be replayed byte-exactly
(modulo plink2 version, which we track separately via the
`generated_by_pipeline_version` field on the ref-stats file).

## 6.8 Recomputing ref-stats safely (`scripts/recompute_ref_stats.py`)

```
recompute_ref_stats.py <PGS_id> [--pop EUR|EAS|AFR|SAS|AMR|MIX|ALL] [--apply]
recompute_ref_stats.py --all-mismatched [--coverage-max 0.55] [--apply]
```

Without `--apply` it dry-runs: prints proposed new μ/σ vs cached. With
`--apply` it:

1. Writes the new JSON with the strict schema (incl.
   `generated_by_commit`, `ref_panel_sha256`).
2. Renames any prior `<PGS>_<POP>_GRCh38*.json` →
   `*.stale-bias-YYYYMMDD.json`.

The new filename embeds `n<N>`, `<method>`, and `sha-<short>` so a
human can eyeball which stats file is current per PGS/pop.

Special handling inside `write_score_file`:
- skips variants whose panel variant ID is `.` (plink2 can't
  disambiguate)
- aggregates duplicate (vid, effect_allele) rows by summing weights
  (additive PGS semantics)
- resolves conflicting effect alleles at the same panel vid by
  keeping the larger |sum| with a warning (catalog-level ambiguity)

`--all-mismatched --coverage-max 0.55` was the recovery action after
the PGS000334 incident: rebuild every PGS whose cached ref-stats had a
`n_variants` lower than 55% of the catalog's `variant_count`, on the
hypothesis that they had been built from a partial reference subset.

## 6.9 The legacy stats dir

`/data/pgs2/ref_panel_stats/` is the older flat directory with
EUR-only files. The new `/data/ref_stats/<PGS>/<POP>_<BUILD>.json`
structure supersedes it. The loader checks the new path first and
falls back to legacy for EUR PGS that haven't been migrated. Reviewers
should treat both as authoritative for the populations they cover, but
the legacy directory has more historical churn.
