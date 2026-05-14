# 07 — QA, Validation, and Drift Safeguards

This document covers everything that runs **outside** the happy-path
scoring, intended to catch silent drift, bias, and miscomputation.
After the PGS000334 incident (cached EUR stats built from 16
variants — silently 20pp off true percentile), we added a layered set
of checks; each layer below should catch issues independently.

## 7.1 Layer 1 — Schema contract on every load

Already documented in `06-percentile-and-stats.md`. Summary:

- Every loaded ref-stats JSON is validated against the live pipeline's
  fingerprint (`scoring_method`, `imputation_policy`,
  `variant_ids_sha256`, schema version, required keys, σ > 0).
- A mismatch returns `IncompatibleRefStats` and the report shows
  `method="incompatible_ref_stats"` with the specific reason.

This catches: stats files older than the most recent ingest, stats
files generated under a different plink2 mode, stats files with the
wrong σ.

## 7.2 Layer 2 — Build validation per run

`runners._validate_genome_build(vcf_path, reference_build)` already
described in `02-input-and-alignment.md` §2.4. Summary:

- 3-SNP spot-check at expected positions in both builds
- header parse of `##reference` and `##contig`
- result: PASS / WARN / WEAK / FAIL
- FAIL on a build mismatch triggers auto-liftover of the scoring file
- every decision is appended to `<SCRATCH>/build_validation.log`

This catches: silent build mismatches (the single biggest class of
"valid score but wrong answer" bugs).

## 7.3 Layer 3 — Match-rate gating

Already documented in `04-scoring-pipeline.md` §4.5:

| Matched / total | Status   | Behavior |
| --------------- | -------- | --- |
| 0               | failed   | no report (likely chr-naming or build mismatch) |
| < 60            | failed   | no report (chip vs WGS PGS, etc.) |
| 60–85           | warning  | report shown, confidence ≤ medium |
| ≥ 85            | passed   | report shown, full confidence |

This catches: tier-3 chip data being scored against a 1M-variant PGS;
half-broken normalized gVCFs (after the v3 concat-D bug).

## 7.4 Layer 4 — Sanity gates on z

`scoring._compute_single_percentile`:

| Gate | Trigger | Action |
| ---- | ------- | --- |
| z_score_extreme | `|z| > 6` | percentile suppressed, reason recorded |
| extreme_tail | `|z| > 4` | kept, but flagged in `sanity.gates_tripped` |
| distribution_collapsed | `std < expected_std × 0.1` | suppressed, reason recorded |
| percentile_capped | computed p ∈ {<0.5, >99.5} | clamped to [0.5, 99.5], `percentile_capped=True` |

`expected_std` is loaded from `pgs_stats_audit.json`, a long-running
audit of plausible σ ranges per PGS computed from the cross-population
panel rescore.

## 7.5 Layer 5 — In-batch control sample

`scripts/batch_control.py`:

A fixed 1000G EUR sample (default `HG00096`) is scored end-to-end
through the production pipeline against every PGS with an EUR ref-stats
file. Its expected percentile per PGS is recorded once (the golden
file at `/data/pgs2/ref_panel_stats/batch_control_golden.json`).

```
# One-time per pipeline change
batch_control.py --sample HG00096 --bless

# In CI / cron
batch_control.py --sample HG00096 --check        # exits 1 on drift
batch_control.py --check --json --golden /var/lib/sg/golden_HG00096.json
```

Drift > `MAX_DRIFT_PP` (default ±10pp) on any PGS quarantines the
batch and exits non-zero. This catches: a global change in the live
pipeline (e.g. plink2 update) that we forgot to refresh ref-stats for.

The script intentionally uses the **same** production code paths the
live runner uses — not a fresh implementation — so it pins both the
"how we score" and the "what stats we read" sides together.

## 7.6 Layer 6 — Nightly ref-stats self-test

`scripts/ref_stats_selftest.py` (cron candidate):

For each `(PGS, pop)` ref-stats JSON:
1. Pick `N` random samples from the population's keep file
   (seeded for reproducibility)
2. Score them end-to-end with the live plink2 args
3. Compute observed μ_obs, σ_obs
4. Compare to cached μ_cached, σ_cached:

   ```
   PASS if  |μ_obs - μ_cached| / σ_cached < 0.1
       and  σ_obs / σ_cached in [0.7, 1.4]
   ```

5. Exit non-zero on any FAIL → cron alert

Defaults: `N=50`, `--max-delta-sigma=0.1`,
`SIGMA_RATIO_LO=0.7`, `SIGMA_RATIO_HI=1.4`. JSON output via `--json`.

This catches: per-PGS slow drift between cached and live (e.g. a
silently-corrupted scoring file).

## 7.7 Layer 7 — Cohort-level distribution sanity

`scripts/cohort_sanity.py`:

After every batch of N≥4 samples scored against the same PGS, check
the percentile distribution against U(0,100):

| Test | Threshold | Trip |
| --- | --- | --- |
| Frac > 80%ile | > 0.70 | bias toward high tail |
| Frac < 50%ile | < 0.30 | bias toward high (complement) |
| One-sample KS vs U(0,100) | p < 0.01 | distribution wrong shape |

A trip writes a 🚩 line to
`simple-genomics/logs/cron_cohort_sanity.log`, which
`live_percentile._load_cohort_flagged_pgs()` reads to tag affected PGS
on the read side.

This catches: stats files where the math is internally consistent but
the user's cohort just doesn't look like the reference panel for that
PGS — usually meaning the trait has population-of-origin effects
larger than 1000G captures.

Example flagged signature: PGS001229 in a small mixed-ancestry batch
showed n=7, >80%ile=86%, <50%ile=0%, KS p=0.000. The math is right;
the ref panel is wrong for those users.

## 7.8 Layer 8 — Live overlay on read

Already documented in `06-percentile-and-stats.md` §6.6:

- On every report read, percentile is recomputed from stored
  `raw_score` against current ref-stats μ/σ
- Original at-scoring value preserved as `percentile_at_scoring`
- Cohort-sanity-flagged PGS get an explicit UI warning

This catches: a corrected ref-stats file leaving historical reports
out of date.

## 7.9 Layer 9 — Provenance attach + interpretation guard

`pipeline/result_guards.py`:

- `attach_provenance(report)` adds sha256 of scoring file, stats file,
  ref panel, and pipeline commit — see `06-percentile-and-stats.md`
  §6.7.
- `check_interpretation_directional(report)` reads any LLM-generated
  interpretation text in the report, parses directional phrases ("you
  are at higher than average", "below average risk", ...), compares
  against the numeric percentile bin, and **drops** the interpretation
  with `interpretation_consistency_error` set if they disagree.

This catches: an LLM interpretation that contradicts the number it's
interpreting (rare but seen during the Common-PGS rewrite when an
interpretation prompt was re-used across percentile bins by accident).

## 7.10 Layer 10 — Regression suite

`scripts/test_pgs_regression.py` and `simple-genomics/test_registry.py`:

- `test_pgs_regression.py` is the per-PGS smoke test that runs a small
  set of known samples and asserts the percentile sits in an expected
  range. Run before deploying any change to scoring code.
- `test_registry.py` is the live registry validator: it boots the full
  pipeline against a fixture VCF and exercises the runners. It's
  expensive but acts as the integration ground truth.

## 7.11 Cron / scheduled jobs

These should be scheduled (some are already configured under
`~/.tmux-dashboard/` watchdogs; reviewer should confirm or formalize):

| Job | Frequency | Purpose |
| --- | --- | --- |
| `ref_stats_selftest.py --n 50 --json` | nightly | drift detection per (PGS, pop) |
| `cohort_sanity.py` | per batch (already in flow) | per-PGS cohort KS check |
| `batch_control.py --check` | on every deploy + nightly | HG00096 golden-percentile regression |
| `scripts/test_pgs_regression.py` | on every deploy | known-sample range checks |

## 7.12 Recovering from a failed layer

| Failed layer | Action |
| ------------ | --- |
| Schema contract | reject load (automatic) — repair by `recompute_ref_stats.py <PGS_id> --pop ALL --apply` |
| Build validation FAIL | auto-liftover the scoring file (in-pipeline) |
| Match rate < 60% | refuse to report — fix input data quality, not the pipeline |
| Sanity z > 6 | suppress percentile — investigate scoring file vs panel orientation |
| Sanity collapsed σ | recompute ref-stats — likely a too-narrow panel subset |
| In-batch control drift | quarantine deploy; bisect plink2/runners.py change |
| Self-test FAIL | regenerate the affected ref-stats file (`recompute_ref_stats.py <id> --pop <pop> --apply`) |
| Cohort sanity 🚩 | usually a panel-fit problem, not a pipeline bug; document and move on |
| Provenance mismatch | re-run scoring; older sha doesn't match current files |

## 7.13 What we do NOT verify automatically

These are gaps the reviewer should flag if they matter:

1. **plink2 version pin**: we don't currently assert plink2 version
   matches the version embedded in `generated_by_pipeline_version`.
   A plink2 update is invisible to the schema contract.
2. **Reference fasta sha**: `_pick_reference_for` returns the first
   matching candidate; we don't hash it. Reference fasta corruption
   would be silent.
3. **Cross-population calibration**: we assume `EUR.txt`, `EAS.txt`,
   etc. are correct subsets. A pop_samples file edit would silently
   re-scope every ref-stats rebuild.
4. **Multi-allelic dropping**: `--rm-dup force-first` silently drops
   alternates. We don't count this against match rate.
