# 12 — Known Issues and Past Incidents

The institutional memory behind "why does the code look like that".
Each entry is a real production failure: the symptom, the root cause,
the mitigation, and what the reviewer should take away. Several of
these are referenced from other chapters.

## 12.1 PGS000334 stale-cache (May 2026) → schema contract

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
issue. It is also what's causing the current EAS-percentile failures
(chapter 09) — the same strictness now refuses files whose schema is
incomplete but whose μ/σ are recoverable. Whether to split the
contract into "drift" and "metadata" channels is open.

## 12.2 PGS001229 cohort-sanity (May 2026) → sex-stratified gap

**Symptom**: a 7-sample family batch ran PGS001229 and reported >80th
percentile for 6 out of 7 members. The cohort-sanity check tripped
(KS p<0.01, frac>80%ile=86%).

**Initial hypothesis**: stale stats. Rebuilt ref-stats; self-test
confirmed μ/σ unchanged.

**Root cause**: PGS001229 has sex-specific effect sizes that 1000G's
sex-pooled ref-stats don't capture. The user's family is mostly male,
so the cohort genuinely sits above the sex-pooled reference mean. Math
is internally consistent.

**Mitigation**: PGS001229 is flagged in
`logs/cron_cohort_sanity.log`; `live_percentile.apply_live_overlay`
adds `cohort_sanity_flagged: True` to every report, and the UI shows
a warning ribbon.

**Open follow-up**: build sex-stratified ref-stats for PGS where the
trait is sex-dimorphic. `pipeline/sex_stratified_stats.py` exists for
this build but is not wired into the live percentile path.

## 12.3 Family6 prostate-cancer match collapse (Apr 2026) → v5 cache schema

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

**Reviewer takeaway**: this is the cautionary tale for bifurcation
between "normalize for plink2" vs "stream the raw gVCF for everything
else". A reviewer might prefer a unified normalization, but the v3
experience makes us hold the bifurcation tight.

## 12.4 PGS002753 16% match rate (Mar 2026) → union-positions invalidation

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

## 12.5 plink2 silent-skip ongoing → strand-flip recovery

**Symptom**: plink2 silently drops scoring variants where the user
pgen's REF/ALT orientation doesn't match the scoring file's
effect/other allele (e.g. user has T/C, scoring file expects A/G on
the reverse strand). Drops appear only as a stderr warning.

**Mitigation**:

- `_parse_plink2_score_warnings` extracts the count and populates
  `scoring_diagnostics`. >5% silent drops surfaces in the report.
- `_recover_strand_flips` re-scores the skipped subset with
  complement effect_alleles (`A↔T`, `C↔G`) and adds the recovered
  contributions back.
- Palindromic SNPs (A/T, C/G) can't be uniquely strand-flipped from
  alleles alone — we leave them and surface the count.

**Open follow-up**: detect palindromics earlier and use frequency-
based strand inference (compare to 1000G AF) for the unambiguous
cases.

## 12.6 PCA misclassification without variance-standardize (Feb 2026) → anchor fixture

**Symptom**: PCA results placed a clearly-EUR sample as closer to AMR
than EUR. Super-pop assignment was wrong, ref-stats lookup was wrong,
percentile was wrong.

**Root cause**: `_run_pca_1000g` was running plink2 `--score` without
the `variance-standardize` flag. The projected PCs were on a different
scale than `ref.eigenvec` (the panel centroids), so Euclidean distance
in this hybrid space gave nonsensical results.

**Mitigation**:

- Added `variance-standardize` to the projection invocation.
- Added `pipeline/pca_projection_validation.py` anchor test: HG00096
  re-projected through the live pipeline must land within ε of pinned
  PC1..PC4 coords.

**Open follow-up**: make the anchor test a hard pre-score gate, not
just CI.

## 12.7 PGS000327 parser strip-tab regression (May 2026) → no-strip rule

**Symptom**: PGS000327 was ingesting 843 variants instead of the
catalog's 35,087 — a 40× under-count. Match rates dropped to 0% for
any user on this PGS.

**Root cause**: the PGS Catalog scoring file format has an empty rsID
column on many rows (the rsID is unknown for those variants). The
parser was calling `line.strip()` defensively, which collapsed
trailing tabs and shifted columns left by one. Rows with the empty
trailing column became rows with one column missing, and the parser
silently dropped them.

**Mitigation**:

- Replaced `line.strip()` with `line.rstrip("\n")` to preserve
  empty trailing columns.
- Added CI test `tests/test_pgs_parser_strip.py` that loads a fixture
  with empty trailing columns and asserts the parser returns all
  rows.
- Memory entry pinned: never `.strip()` PGS Catalog TSV lines.

## 12.8 selected_ref=null masking ancestry (Apr 2026) → result-root mirroring

**Symptom**: even after ancestry was correctly detected (EAS), the
final report showed `selected_ref=null` and the live overlay
defaulted to EUR for percentile re-computation.

**Root cause**: `selected_ref` was only set in `pipeline_info`, not at
the result root. `live_percentile.apply_live_overlay` looked at the
result root only and defaulted when the field was absent.

**Mitigation**: `_postprocess_pgs_result` now mirrors `selected_ref`,
`available_refs`, `secondary_percentiles`, and `ancestry_model` into
the result root. The fast path, full path, and Pipeline E+ all set
these fields explicitly.

## 12.9 KING filter gap (open) → relatedness in PCA cache

**Symptom**: family-clustered samples in the panel pull centroids
toward their family's region, distorting super-pop assignment for
nearby ancestries.

**Root cause**: `GRCh38_1000G.king.cutoff.out.id` exists but the PCA
build script doesn't currently `--keep` against it. The "unrelated"
guarantee is for the panel pgen, not the PCA cache.

**Status**: open. The fix is one line in `_build_pca_reference_cache`.
Pending a re-validation cycle (HG00096 anchor + sample replays) before
landing.

## 12.10 No family6norm-as-gVCF substitution (rule)

**Symptom (historical)**: someone tried to substitute
`/data/pgs2/vcf_norm_split/family6norm_chr*.vcf.gz` for missing
per-sample gVCFs. These files are joint-genotyped VCFs — they don't
contain ref blocks, and using them as gVCFs collapses the match
rate.

**Mitigation**: documented in memory and in README of the
`vcf_norm_split` directory. The pipeline does not auto-substitute
these.

## 12.11 ECDF wiring (open) → Phase 2.1 follow-through

**Status**: `pipeline/ecdf_percentile.py` is fully implemented and
tested. The `<pop>_scores.npy` arrays exist in `/data/ref_stats/`.
But the live percentile path still routes through parametric Φ-z.
Wiring ECDF in as either (a) a parallel diagnostic with disagreement
flag, or (b) the new primary for non-EUR users — is open. Blocked on
the chapter 09 schema consolidation.

## 12.12 Reference panel not 1000G phase 4

**Status**: open. Phase 3 panel is the entire history of our PCA and
ref-stats build. The 30× re-release (NYGC high-coverage) has different
allele frequencies in places and might shift super-pop centroids by a
small but measurable amount. We haven't yet quantified the impact.
The reviewer's perspective on whether this is worth a migration cycle
is welcome.
