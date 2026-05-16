# 09 — The East Asian Ancestry-Mismatch Incident

## 9.1 What the user sees

Reports for East Asian (and other non-EUR) users currently surface a
verdict that reads, roughly:

> The individual has East Asian (EAS) ancestry but was scored against
> a European reference population, which significantly reduces
> prediction accuracy. No percentile ranking can be provided due to
> incompatible reference statistics and the lack of precomputed
> ancestral benchmarks.

This is the prose written by the LLM interpretation layer. It is
**synthesized** from three signals in the result dict and is not a
fixed string in our codebase. We confirmed (`pipeline/scoring.py`,
`runners.py`) that no Python file emits this exact phrasing. The
chain is:

1. The percentile path emits `method="incompatible_ref_stats"` (or
   `method="unavailable"`), `percentile=None`, and a `description`
   like `"Reference stats file failed the pipeline contract (missing
   required keys: [schema_version, scoring_method, ...]). Percentile
   refused."` or `"No precomputed reference stats available for EAS.
   Score computed but percentile cannot be determined."`
2. `_compute_confidence` adds `cross_ancestry_transfer` to
   `confidence_reasons` because `selected_ref != "EUR"`.
3. `_postprocess_pgs_result` attaches the `cross_ancestry_warning`
   prose:
   ```
   Sample classified as EAS; PGS scored against the EUR 1000G
   reference panel (EUR (European, n=503)). PRS performance across
   ancestries is unreliable — interpret directional risk with caution
   and validate against same-ancestry data before acting on this
   number.
   ```
4. `app.py::_interpret_result` ships this dict (minus
   `pipeline_info`, `scoring_diagnostics`, etc.) to Gemini / Claude /
   OpenAI with a "write a brief, matter-of-fact interpretation"
   prompt. The LLM rewrites the warning prose into the user-facing
   sentences above.

So the *visible message* is LLM-paraphrased, but the *underlying
signal* — a missing percentile + cross-ancestry warning — is real and
reproducible. The fix has to land in step 1 (give EAS users a real
percentile) or step 3 (relax the warning when ref-stats are valid).

## 9.2 The smoking gun

The two on-disk stores of ref-stats (chapter 06 §6.3) disagree on
schema:

| Store                                        | EAS PGS coverage | Schema fields present     | Loader verdict |
| -------------------------------------------- | ---------------- | ------------------------- | --- |
| `/data/pgs2/ref_panel_stats/` (registry+legacy) | 54 PGSes      | Full v1 schema            | ✅ pass |
| `/data/ref_stats/` (new)                     | 361 PGSes        | **Missing 5–6 required keys** | ❌ refused |

The new-store files are missing:

- `schema_version`
- `variant_ids_sha256`
- `scoring_method`
- `imputation_policy`
- `generated_at`
- `n_variants` (they use `total_variants` instead)

Example, `/data/ref_stats/PGS000001/EAS_GRCh38.json`:

```json
{
  "pgs_id":              "PGS000001",
  "population":          "EAS",
  "genome_build":        "GRCh38",
  "mean":                0.005420456268888889,
  "std":                 0.002621469190834381,
  "median":              0.00536997,
  "n_samples":           585,
  "min":                -0.00263506,
  "max":                 0.0127794,
  "quantiles":           { "1": ..., "5": ..., ..., "99": ... },
  "matched_variants":    74,
  "total_variants":      154,
  "score_sum_mean":      0.8022276065299144,
  "score_sum_std":       0.3879776909471559
}
```

This file is **scientifically usable** — μ, σ, quantiles are present
and computed against the EAS subset (n=585). It just lacks the
metadata fields the schema contract requires.

When `_load_stats` resolves to this file (which it does, because
`ref_stats_path("PGS000001", "EAS", "GRCh38")` points there and
nothing else is in the registry for that pair), `_rs_validate` raises:

```
IncompatibleRefStats: missing required keys: ['generated_at',
  'imputation_policy', 'n_variants', 'scoring_method',
  'variant_ids_sha256']
```

The loader attaches `_incompatible_reason`, the percentile function
returns `(None, {"method": "incompatible_ref_stats", ...})`, and the
report ends up with `percentile=null`.

## 9.3 The asymmetry: why EUR users still get percentiles

For EUR specifically, `_load_stats` has a **legacy fallback**:

```python
def _candidate_stats():
    # 1. registry → /data/pgs2/ref_panel_stats/registry.json
    # 2. new path  → /data/ref_stats/<pgs>/EUR_GRCh38.json   (FAILS contract)
    # 3. database
    # 4. legacy fallback for EUR only:
    if population == "EUR":
        return _load_legacy_stats(pgs_id)   # /data/pgs2/ref_panel_stats/PGS000XXX_EUR_GRCh38.json
    return None
```

The legacy fallback is the older EUR-only `<PGS>_EUR_GRCh38.json`
files that *do* have the full schema. They were preserved during the
remediation; the loader still accepts them. So EUR users get a
percentile on ~111 PGSes total (54 registry + ~57 legacy EUR-only).

There is **no legacy fallback for EAS, AFR, SAS, AMR, MIX**. For
those populations, the loader's only hits are (a) the registry (54
PGSes for EAS) and (b) the new store (which fails the contract).
Net: EAS gets a percentile on 54 PGSes vs EUR's 111.

## 9.4 Impact across the catalog we support

We commonly score against ~250 curated PGSes ("Common PGS" tab is the
top-23 subset). Of these:

| Status                                          | Count | Symptom for EAS user |
| ----------------------------------------------- | ----- | --- |
| In registry with EAS stats                      | ~54   | ✅ EAS percentile shown |
| In `/data/ref_stats/` only (schema gap)         | ~310  | ❌ "no precomputed ancestral benchmarks" |
| `ancestry_mismatch` eligibility failure         | varies| ❌ "ancestry not in PGS evaluation set" (chapter 03 §3.6) |

Reading SQL on `pgs_pipeline.db.sample_results` for the last 14 days:
about **85%** of EAS-user PGS reports had `percentile=null`. The
remaining 15% hit the registry. Hence the user's "lots of failed"
observation.

A small fraction (≈ 5%) of the failures are eligibility-gate
`ancestry_mismatch` (the PGS's evaluation_ancestry didn't include
EAS). Those are correct refusals — the PGS was never validated for
this ancestry — but they should produce a **different** user message
than the schema-failure case.

## 9.5 The four candidate fixes

### Fix A — Backfill schema metadata into existing `/data/ref_stats/` files

For every `/data/ref_stats/<PGS>/<POP>_GRCh38.json` in the new store,
add the missing fields **without re-running plink2**:

- `schema_version: 1`
- `scoring_method: "plink2-nomi"` (assumes the file was built that
  way, which it was — `recompute_ref_stats.py` is the only writer)
- `imputation_policy: "no-mean-imputation"`
- `n_variants`: copy from `total_variants`
- `variant_ids_sha256`: compute live from the current scoring file
  via `_rs_variant_set_sha_from_catalog(pgs_id)`
- `generated_at`: file mtime, in ISO format

Then `bless` each file into `/data/pgs2/ref_panel_stats/registry.json`
(or extend the registry to point into `/data/ref_stats/`).

**Pros**: cheapest, fastest. No plink2 re-runs needed. Restores ~310
PGS×pop combinations immediately.

**Cons**:

- We *assume* the existing μ/σ were produced by `plink2-nomi`
  against the current 1000G panel. If a previous sweep used different
  flags or a different panel subset, we'd stamp a wrong fingerprint.
  Mitigation: cross-check 5 known-good PGSes against the registry,
  fail loudly if μ/σ disagree by > 1%.
- Doesn't get us to ECDF (Phase 2.1 target) — we'd still be parametric.

### Fix B — Switch to ECDF-from-NPY everywhere

Skip the parametric path for any (PGS, pop) where
`/data/ref_stats/<PGS>/<POP>_scores.npy` exists. Use the rank-
percentile from the NPY directly:

```python
def _load_ecdf_stats(pgs_id, pop, genome_build):
    npy = f"/data/ref_stats/{pgs_id}/{pop}_scores.npy"
    if os.path.exists(npy):
        return np.load(npy)
    return None
```

The NPY files are pure score arrays — no schema, no contract; the
loader can use them safely without `_rs_validate`. Then
`pipeline/ecdf_percentile.py::ecdf_pipeline` returns
`(ecdf_percentile, ci95_low, ci95_high, n_reference)`.

**Pros**:

- Restores EAS percentiles immediately (the NPYs are there).
- Gets us to the Phase 2.1 target in one step.
- Bootstrap CI is more honest about uncertainty than parametric.
- Doesn't depend on the JSON schema at all.

**Cons**:

- Forces an architectural shift mid-pipeline. The downstream live-
  overlay code is written against parametric stats and would need an
  adapter.
- ECDF percentile from n=585 has ~1 percentile point of bootstrap
  jitter; parametric (when the panel is wide) is smoother. For
  consumer-facing percentiles, "65th vs 67th" might be perceived as
  inconsistency.
- We still need a sha-fingerprint somewhere to detect catalog drift
  — the NPYs don't have one.

### Fix C — Re-bless everything via `recompute_ref_stats.py`

Run:

```
python scripts/recompute_ref_stats.py \
    --all-mismatched --pop ALL --build GRCh38 --apply
```

This regenerates every (PGS × pop) stats file from scratch with the
full v1 schema, dropping the legacy/new split entirely. ~362 PGSes ×
5 pops ≈ 1,800 plink2 invocations against 1000G subsets. On the
n1-standard-32 host, this is roughly 8 hours of compute (≈ 12s per
invocation, 1800 / 4-way parallel ≈ 450 ×  12s ≈ 90 min, but the
panel scan is the bottleneck).

**Pros**:

- Clean: single store, single schema, single source of truth.
- Picks up any parser/liftover fixes that landed since the original
  sweep.
- Generates fresh NPYs at the same time, ready for Fix B if desired
  later.

**Cons**:

- 8-hour run; needs maintenance window or a side host.
- Risk of generating bad stats if the sweep itself has a bug. The
  HG00096 anchor and self-test runs should catch this, but the
  reviewer's confidence in the recompute pipeline is the bottleneck.

### Fix D — Relax the loader to attach a degraded percentile

When `_rs_validate` fails on schema but `mean`, `std`, `population`,
`n_samples` are all present and plausible, attach the percentile with
`method="precomputed_stats_schema_degraded"` and a confidence reason
`stats_schema_incomplete`.

**Pros**:

- Tiny one-file change.
- Preserves the post-PGS000334 hard contract for the catalog-drift
  case while being lenient for the schema-versioning case.

**Cons**:

- Erodes the strict contract that the team built precisely to prevent
  PGS000334-class incidents. The reviewer's call.
- Doesn't surface a `variant_ids_sha256` to detect catalog drift —
  we'd be flying blind on that channel for the lenient cases.

## 9.6 Our recommendation, for the reviewer to confirm or override

Combine **A + B**, in that order:

1. **Immediately**: run Fix A (backfill schema) to restore EAS
   percentiles in production. Cross-check 5 known-good PGSes
   (PGS000004, PGS000007, PGS000015 — all in both stores) by
   recomputing μ/σ live and comparing to the new-store files. If
   agreement is < 1% on all 5, proceed with backfill. If not, fall
   back to Fix C.
2. **In parallel**: complete Phase 2.1 by wiring ECDF-from-NPY into
   `_compute_single_percentile` as a fallback layer. ECDF reports
   alongside parametric; samples where they disagree by > 5
   percentile points get a `ecdf_phi_disagreement` reason.
3. **After A+B stabilize**: deprecate `/data/ref_stats/` as a
   primary store. Everything routes through
   `/data/pgs2/ref_panel_stats/` with the registry as the source of
   truth.

We are not asking the reviewer to ratify A vs B vs C blindly — we're
asking:

- Is the strictness of the schema contract (chapter 07 §7.2) still
  appropriate, or should it be split into "drift contract" (sha
  mismatch — hard fail) and "metadata contract" (missing schema
  fields — soft fail with warning)?
- For PGSes where `evaluation_ancestry` doesn't include EAS, should
  we refuse the percentile (current `ancestry_mismatch` gate) or
  emit a sensitivity array against all 5 pops with a strong caveat?
- Is parametric Φ-z acceptable as the live default with ECDF as
  diagnostic, or should ECDF be the live default for non-EUR users
  (where the heavy-tail assumption is more likely violated)?

## 9.7 Out-of-scope but related

- **PRS-CSx**: building ancestry-specific PGS weights would address
  the root issue (we're scoring an EUR-trained PRS on an EAS user),
  not just the percentile presentation. This is a substantially
  larger build and we'd want the reviewer's framing on whether it's
  worth committing to.
- **HGDP / 1000G phase 4**: expanding the reference panel to cover
  Middle Eastern, Pacific, Indigenous Australasian, and finer
  East/South Asian sub-populations would shrink the `UNSUPPORTED`
  cohort and improve the AF-match hint for currently-mislabeled
  users.
- **Local ancestry painting (e.g. RFMix, Loter)**: for genuinely
  admixed users (top posterior < 0.80), chromosome-level ancestry
  would let us route PGS variants to per-chrom-region-appropriate
  ref-stats. This is the long-term right answer for admixed AMR /
  Latino users; currently they get the `MULTI` sensitivity array
  treatment.

## 9.8 Specific reviewer asks

1. **Validate the diagnosis.** Does the schema-contract / no-fallback
   asymmetry match what the reviewer would expect from the code
   inspection? Is there a path we missed where the loader silently
   succeeds for non-EUR?
2. **Prioritize the four fixes.** A, B, C, or D — or some other
   combination?
3. **Re-rank the contract.** When does strictness become a self-DoS,
   and what's the right granularity (per-field vs per-incident-class)?
4. **Confirm the LLM interpretation gate.** Does the
   `cross_ancestry_warning` prose (chapter 04 §4.8) correctly steer
   the LLM toward hedged language, or is it inadvertently producing
   the catastrophizing tone the user sees?
