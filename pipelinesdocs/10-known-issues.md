# 10 — Known Issues, Past Incidents, and Open Items

This is the institutional memory of "why does the code look like that".
Each entry describes a failure mode the pipeline has actually
experienced, the symptom, the root cause, and the mitigation. The
reviewer should treat these as evidence that the pipeline accumulates
real-world bugs the same way any production system does — and use them
to assess whether the current safeguards cover the analogous classes of
failure.

## 10.1 PGS000334 stale-cache (May 2026)

**Symptom**: a sample's PGS000334 (CAD) percentile shifted by ~20pp
between two consecutive runs against the same VCF. Reports rendered
"95th percentile" yesterday, "75th percentile" today.

**Root cause**: the cached ref-stats JSON for PGS000334/EUR was built
months earlier from only 16 variants against an outdated catalog
release. The current catalog has 22 variants, and μ/σ at 22 variants
are materially different from μ/σ at 16. Live scoring used 22
variants; percentile divided by the 16-variant σ.

**Mitigation** (built after the incident):
- Schema contract on every load. The current loader recomputes
  `variant_ids_sha256` from the current scoring file and refuses any
  ref-stats whose stored hash doesn't match.
- `recompute_ref_stats.py --all-mismatched --coverage-max 0.55 --apply`
  rebuilt every PGS whose cached `n_variants` was <55% of the catalog's
  variant count.
- Stale files renamed to `*.stale-bias-YYYYMMDD.json` (kept for
  forensics).
- Nightly self-test (`ref_stats_selftest.py`) added.

**Reviewer takeaway**: the schema contract should be the first line of
defense against any future "ref panel and live drift apart" class of
issue. The reviewer is invited to stress-test the contract by editing
a scoring file by hand and confirming the loader refuses the
corresponding stats file.

## 10.2 PGS001229 cohort-sanity 🚩 (May 2026)

**Symptom**: a 7-sample family batch ran PGS001229 and reported >80th
percentile for 6 out of 7 members. The cohort-sanity check tripped
(KS p<0.01, frac>80%ile=86%).

**Initial hypothesis**: stale stats (like PGS000334). Rebuilt
ref-stats; self-test confirmed μ/σ unchanged.

**Root cause**: PGS001229 has sex-specific effect sizes that 1000G's
sex-pooled ref-stats don't capture. The user's family is mostly male,
so the cohort genuinely sits above the sex-pooled reference mean. Math
is internally consistent.

**Mitigation**: PGS001229 is flagged in
`logs/cron_cohort_sanity.log`; `live_percentile.apply_live_overlay`
adds `cohort_sanity_flagged: True` to every report, and the UI shows
a warning ribbon.

**Open follow-up**: build sex-stratified ref-stats for PGS where the
trait is sex-dimorphic. Tracked in `README.md` open questions.

## 10.3 Family6 prostate-cancer match collapse (Apr 2026)

**Symptom**: prostate cancer PGS match rate fell from 94% to 45% on a
gVCF input after a normalization refactor. No code change was visible
in the runner — only the cache schema was bumped.

**Root cause**: the `v3` normalization schema had concatenated the
PGS-positions expanded VCF with a genome-wide variants-only VCF, on
the theory that downstream tests could share the same file. `bcftools
concat -D` (deduplicate) silently dropped most of the hom-ref records
when both inputs contained the same positions, because `-D`
deduplicates on `chrom:pos:ref:alt` and the variants-only VCF won
those collisions. Result: ~4.6M hom-ref records → ~240K, scoring
collapsed.

**Mitigation**:
- Reverted to v4 schema: normalized VCF is **PGS+PCA only**;
  genome-wide tests read the raw gVCF directly.
- Cache schema bumped to v5 to invalidate any lingering v3 outputs.
- The `_normalize_gvcf` docstring carries a permanent warning about
  not retrying this design.

**Reviewer takeaway**: this is the cautionary tale for the bifurcation
of "normalize for plink2" vs "stream the raw gVCF for everything
else". A reviewer might prefer a unified normalization, but the v3
experience makes us hold the bifurcation tight.

## 10.4 PGS002753 16% match rate (Mar 2026)

**Symptom**: PGS002753 (1M-variant PGS) matched only 16% on a gVCF
input. Smaller PGS on the same gVCF matched 100%.

**Root cause**: the union-of-positions file at
`/data/pgs_cache/_all_pgs_pca_positions_chr.tsv` was older than the
newly-ingested PGS002753 scoring file, so PGS002753's positions
weren't in the union. `bcftools convert --gvcf2vcf -T <union>`
therefore expanded nothing at those positions.

**Mitigation**: `_normalize_gvcf` now checks if any scoring file (or
the PCA eigenvec) is newer than the union file and rebuilds it
automatically. Logged when it does.

## 10.5 plink2 variants skipped due to mismatching allele code (ongoing)

**Symptom**: plink2 silently drops scoring variants where the user
pgen's REF/ALT orientation doesn't match the scoring file's
effect/other allele (e.g. user has T/C, scoring file expects A/G on
the reverse strand). Drops appear only as a stderr warning.

**Mitigation**: `_parse_plink2_score_warnings` extracts the count and
populates `scoring_diagnostics`. >5% silent drops shows in the report.

**Open follow-up**: implement strand-flip recovery (rewrite the
scoring file to the user's strand orientation) so the warning becomes
a non-event. Currently we just surface the count.

## 10.6 PCA misclassification without variance-standardize (Feb 2026)

**Symptom**: PCA results placed a clearly-EUR sample as closer to AMR
than to EUR. Confidence "high" — so the user got a wrong assignment
with no flag.

**Root cause**: `plink2 --score ... allele_wts --score-col-nums 6-15`
without `variance-standardize` produced AVG scores in a different
coordinate system than the cached `ref.eigenvec` centroids. The
distance comparison was over incompatible spaces.

**Mitigation**: `variance-standardize` is now mandatory in
`_run_pca_1000g`; the docstring carries a never-remove warning.

## 10.7 chrEBV decode failure (early 2026)

**Symptom**: PCA from CRAM aborted with "ref length mismatch on
chrEBV".

**Root cause**: `samtools view` was being asked to decode reads from
the full CRAM, but some reference fastas don't include chrEBV at the
exact length the CRAM header declares.

**Mitigation**: PCA CRAM extraction explicitly restricts to autosomes
1..22.

## 10.8 Pre-fix cache truncated (Mar 2026)

**Symptom**: cached normalized gVCFs occasionally contained partial
contents (truncated mid-record). Subsequent runs read the truncated
file and produced biased percentiles silently.

**Root cause**: `_normalize_gvcf` was writing directly to its final
output path. A crash mid-concat (or mid-index) left a half-written
file that the next call's `os.path.getsize() > 0` check accepted as
valid.

**Mitigation**:
- Atomic write: concat to `out_path.tmp.<pid>`, then `os.replace`.
- Completeness validation (`_gvcf_normalized_is_complete`) on every
  cache hit; truncated files are removed and rebuilt.
- Fail-closed on any chrom convert failure (all 22 must succeed).

## 10.9 Open items (no incident yet, but reviewer-flagged)

- **plink2 version pin**: we record `generated_by_pipeline_version` (git
  sha) on ref-stats but not the plink2 binary version. A plink2 update
  is invisible to the contract.
- **Multi-allelic silent drop**: `--rm-dup force-first` drops alternates
  at the same position. We don't count this against match rate. For a
  multi-allelic PGS site (rare), the reported score will be too low.
- **chrM ploidy**: HaploGrep3 expects a haploid chrM call. Our pgen
  has chrM at output_chr=26 but we don't explicitly haploidize. So far
  no symptomatic miscall.
- **Liftover failure modes**: `liftOver` silently drops unmappable
  variants. If >50% of a scoring file drops, ingest marks it rejected;
  but a 49% drop is still scored and silently underestimates everything.
  Threshold should probably be tightened.
- **`MAX_DRIFT_PP=10`**: the batch-control tolerance is generous. A
  reviewer should pick a smaller value (e.g. 3pp) for traits with
  narrow percentile bands.

## 10.10 Where new incidents should be documented

When a new failure mode is found:

1. Add an entry to this file with: symptom, root cause, mitigation.
2. If the mitigation is code, add a comment with the incident reference
   at the affected function.
3. If the mitigation is data (ref-stats rebuild), record the
   `stale_replaces` chain in the new ref-stats JSON.
4. If the mitigation is a new gate, add it to `07-qa-and-validation.md`.

The aim is for any future engineer (or LLM) reading the code to find
the *reason* the code looks the way it does without needing to
re-derive it from outage post-mortems.
