# 08 — QA and Validation

This chapter inventories every QC, gate, and drift check that runs on
or behind a PGS result. The order matches the pipeline flow: input
validation → ingestion checks → scoring gates → percentile gates →
post-score eligibility → ongoing drift surveillance.

## 8.1 Input-level checks

| Check                                   | Where                                    | Failure mode |
| --------------------------------------- | ---------------------------------------- | --- |
| File-type detection                     | `runners._detect_file_type`              | unsupported extension → reject |
| BAM/CRAM indexability                   | `_ensure_alignment_indexed`              | samtools index error → reject |
| CRAM contig fasta match                 | `_pick_reference_for`                    | no matching ref → reject |
| VCF bgzip + tabix indexability          | `_ensure_indexed`                        | malformed VCF → reject |
| gVCF detection                          | `_is_gvcf`                               | (no failure — routes branch) |
| Targeted variant call coverage          | per-test pileup helpers                  | low coverage → low_coverage++ |

## 8.2 Genome-build validation (chapter 02 §2.4)

The 3-SNP spot-check panel (rs7412, rs429358, rs1801133) is the
primary canary. Failure outcomes feed back into the scoring path as
either a `WARN` (proceed, degrade confidence) or `FAIL` (liftover the
scoring file). See chapter 02 for the full decision matrix.

## 8.3 PGS Catalog ingestion checks (chapter 03)

| Check                          | Surfaced as |
| ------------------------------ | --- |
| Parser strip-tab regression    | `meta.json.parser_warnings`; CI test `tests/test_pgs_parser_strip.py` |
| Variant count plausibility     | Refuse PGS if parsed count < 50% of header `variants_number` |
| Liftover failure count         | `metadata.liftover_failures` |
| Catalog file drift             | `_rs_validate` raises `IncompatibleRefStats` on sha mismatch |

## 8.4 Scoring-time gates (chapter 04)

- **Match-rate gate**: <60 → status=failed; 60–85 → warning; ≥85 → passed.
- **plink2 silent-drop count**: surfaced in `scoring_diagnostics.skipped_due_to_mismatching_allele_code`.
- **Strand-flip recovery count**: surfaced as a positive signal (`recovered_by_strand_flip`).

## 8.5 Percentile gates (chapter 07 §7.3)

- |z| > 6 → percentile=None, `reason=z_score_extreme`.
- |z| > 4 → percentile kept, confidence demoted, `gates_tripped` populated.
- σ_ref < 10% of expected → percentile=None, `reason=distribution_collapsed`.
- Percentile clamped to [0.5, 99.5]; flag in `percentile_capped`.

## 8.6 Schema contract gate (chapter 07 §7.2)

Every ref-stats load passes through `_rs_validate`. Failures map to
`method="incompatible_ref_stats"` with one of:

- missing required keys
- unsupported schema_version
- pgs_id / population / genome_build mismatch
- scoring_method / imputation_policy mismatch
- catalog content drift (sha mismatch)
- std <= 0

Critically, the loader does **not** fall through to an alternate stats
file when validation fails. This is why the new-store schema gap
(chapter 06 §6.3.2) shows up as "no percentile" rather than "wrong
percentile" for affected (pgs, pop) pairs.

## 8.7 Eligibility gates (chapter 03 §3.6)

Six gates in order in `pipeline/eligibility_gates.py::eligibility_for_pgs`:

1. **Build availability** — harmonized target build present OR
   liftover passed.
2. **Complex alleles** — no variants in HLA region or other curated
   complex windows.
3. **Weight type** — `weight_type ∈ {beta, log_or, log_hr}`.
4. **Ancestry resolved** — user pop known.
5. **Ancestry match** — user pop ∈ dev_ancestry ∪ eval_ancestry.
6. **Performance metric** — binary AUC ≥ 0.55 OR continuous R² ≥ 0.02.
7. **Direction known** — `trait_direction` declared.

Eligibility decides `risk_language_allowed`, which gates the LLM's
ability to use phrases like "elevated risk" in the interpretation.

## 8.8 In-batch control sample (golden percentile)

Every batch run includes a hidden re-scoring of **HG00096** (1000G EUR,
present in the panel) against the same PGS the user is testing. The
HG00096 percentile is then compared to a stored golden value.

```python
golden_percentile = read_golden(pgs_id, "HG00096", "EUR")
drift = current_pctl - golden_percentile
if abs(drift) > 10:
    flag_batch("control_drift", drift)
```

Any drift > ±10 percentile points triggers an alert at the batch
level. This catches "the pipeline regressed since the golden was
written" — i.e. a code change broke scoring in a way the rest of QA
missed.

## 8.9 Cohort sanity (`scripts/cron_cohort_sanity.py`)

Runs hourly over the last 24h of `pgs_pipeline.db.sample_results`. For
each (PGS, population, day) cohort:

- **KS test against uniform**: percentiles should be approximately
  uniform on [0, 100]. KS p < 0.01 → flag.
- **Frac > 80%ile**: > 30% of the cohort above the 80th percentile is
  suspect.
- **Frac < 20%ile**: same, mirror image.

Flags land in `logs/cron_cohort_sanity.log` and feed
`live_percentile.apply_live_overlay`, which attaches
`cohort_sanity_flagged: True` to every affected report on read.

PGS001229 is the canonical positive: the cohort sanity tripped, but
investigation showed the cause was a real biology (sex-dimorphic trait
scored against sex-pooled refs) not a pipeline bug. The flag stays —
the UI shows the warning ribbon — but `recompute_ref_stats.py` won't
fix it. Sex-stratified ref-stats are the proper fix.

## 8.10 Nightly self-test (`scripts/ref_stats_selftest.py`)

Per (PGS, pop), re-score 50 randomly-sampled panel members and check:

- mean of the 50 scores stays within 2σ of the stored panel mean
- min/max of the 50 scores doesn't exceed the stored quantile bounds
- variance of the 50 scores stays within 0.5×–2× of the stored
  variance

Failures append to `logs/ref_stats_selftest.log` and route to a
Slack-like alert. This catches both:

- silent panel changes (someone re-ran `recompute_ref_stats.py` with
  the wrong `--pop` list)
- silent code changes (a plink2 flag changed, a parser behavior
  shifted)

The self-test runs on the *current* panel and *current* stats file,
so the comparison is internally consistent. The PGS000334 incident
(stats file from 16 variants but live runs scored 22 variants) would
have been caught by this test if it had existed at the time.

## 8.11 PCA validation (`pipeline/pca_projection_validation.py`)

Two anchor checks on every PCA projection:

1. **HG00096 anchor**: re-project HG00096 through the live pipeline,
   compare PC1..PC4 against pinned coordinates. Drift > 0.01 in any
   PC → fail.
2. **Cohort scatter**: when scoring a batch, all members' median
   pairwise PC distance should be < panel median pairwise distance.
   Excessive scatter implies a contig-naming or coordinate mismatch.

These tests exist as code but are CI-runnable, not blocking in
production. The reviewer may want to weigh in on which (if any) should
become hard pre-score gates.

## 8.12 Score-provenance bundle (`pipeline/score_provenance.py`)

Every result emits a `provenance` dict capturing:

```json
"provenance": {
  "pipeline_version":     "simple-genomics@3ede0ee",
  "plink2_version":       "v2.00a5LM 64-bit Intel (29 Mar 2024)",
  "bcftools_version":     "1.17",
  "samtools_version":     "1.17",
  "scoring_method":       "plink2-nomi",
  "imputation_policy":    "no-mean-imputation",
  "ref_panel_path":       "/data/pgs2/ref_panel/GRCh38_1000G_ALL",
  "ref_stats_path":       "/data/pgs2/ref_panel_stats/PGS000004_EAS_GRCh38_n303_plink2-nomi_sha-8856f202.json",
  "pgs_catalog_url":      "https://www.pgscatalog.org/score/PGS000004/",
  "ancestry_method":      "pca_inverse_distance",
  "build_validation":     "PASS|GRCh38|spot_check 3/3",
  "scoring_started":      "2026-05-14T11:23:11+00:00",
  "scoring_completed":    "2026-05-14T11:23:14+00:00",
  "host":                 "genom-beast-gpu"
}
```

This bundle is the reviewer's audit trail. Every report in
`users/<u>/reports/` carries it.

## 8.13 Interpretation gate (`pipeline/result_guards.py`)

`check_interpretation_directional(report)` parses the LLM's free-text
output for directional claims ("higher risk", "lower risk",
"protective", "elevated", "increased") and:

- if `risk_language_allowed=False` (eligibility gate failed for
  ancestry/sex/etc.), drops the phrase and replaces with a hedged
  alternative.
- if the directional claim disagrees with the numerical percentile
  (e.g., LLM says "lower than average" for a 75th-percentile result),
  appends a `directional_disagreement` flag.

This is a post-LLM filter; the LLM never sees the gate decisions
directly.

## 8.14 CI

```
tests/
├── test_pgs_parser_strip.py        # PGS000327-style strip regression
├── test_gvcf_refblock.py            # gVCF expansion correctness on synthetic blocks
├── test_indel_routing.py            # indel ALT handling for plink2
├── test_registry.py                 # ref_stats_registry resolve / bless
├── test_pca_projection.py           # HG00096 anchor
└── test_api.py / test_app.py        # FastAPI smoke
```

CI runs on every commit; the .github/ workflows are in
`ci-workflows-pending/` (pre-merge backlog) — the relevant tests are
the parser-strip and gVCF-refblock tests.

## 8.15 Reviewer-facing questions

- The match-rate gate (60/85) is empirically calibrated. Are these
  the right thresholds for the inputs we accept (gVCF, normalized
  gVCF, chip text, BAM pileup)?
- Pipeline E+ skips build validation. Should we add a fallback
  3-SNP pileup check?
- Cohort sanity flags PGSes but does not refuse to score them. Should
  it gate-out genuinely broken PGSes preemptively?
- The schema contract refuses any stats file with a missing field
  even when the data is recoverable. Is this strictness still the
  right call given the operational cost (chapter 09)?
