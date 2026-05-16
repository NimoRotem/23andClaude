# 23andclaude.com — Bioinformatics Pipeline Documentation (v2)

This is the bioinformatics-review packet for 23andclaude.com. It is
written for an outside specialist to audit our methodology end-to-end:
input handling, variant calling, PGS ingestion, scoring, ancestry
inference, reference-panel construction, percentile computation, QA, and
known failure modes.

Version 2 (May 2026) refreshes v1 to reflect the post-remediation state
(REMEDIATION_PLAN Phase 0/1/2 executed 2026-05-14) and adds a dedicated
chapter on the **East Asian ancestry-mismatch incident** — a current
class of "no percentile" failures the team needs the reviewer's opinion
on.

- **Host**: `genom-beast-gpu` (GCE, us-central1-c, n1-standard-32 + T4)
- **Code**: `/home/nimrod_rotem/simple-genomics/` (FastAPI on port 8600, served by `nginx` at `https://23andclaude.com/`)
- **Data**: `/data/pgs2/`, `/data/pgs_cache/`, `/data/ref_stats/`, `/data/ancestry_reference/`
- **Tooling**: plink2, bcftools, samtools, T1K, ExpansionHunter, UCSC `liftOver`, custom Python

## Reading order

| #   | Doc | Topic |
| --- | --- | --- |
| 00  | [README.md](README.md) | This file — index + TL;DR |
| 01  | [overview.md](01-overview.md) | System architecture and request dataflow |
| 02  | [input-and-alignment.md](02-input-and-alignment.md) | VCF / gVCF / BAM / CRAM / 23andMe ingestion, indexing, variant calling, build validation |
| 03  | [pgs-ingestion.md](03-pgs-ingestion.md) | PGS Catalog download, parsing, normalization, harmonization, liftover, eligibility |
| 04  | [scoring-pipeline.md](04-scoring-pipeline.md) | plink2 `--score` pipelines (full-pgen, fast-path, Pipeline E+ direct pileup), match-rate gating |
| 05  | [ancestry-inference.md](05-ancestry-inference.md) | PCA cache, projection, super-pop centroids, admixture estimation, reference selection |
| 06  | [reference-panel.md](06-reference-panel.md) | 1000 Genomes Phase 3 panel composition, population subsets, ref-stats stores |
| 07  | [percentile-and-stats.md](07-percentile-and-stats.md) | Parametric (Φ) and ECDF percentile, schema contract, sanity gates, scale reconciliation, live overlay |
| 08  | [qa-and-validation.md](08-qa-and-validation.md) | Build validation, eligibility gates, cohort sanity, in-batch control, nightly self-test, CI |
| 09  | [ancestry-mismatch-incident.md](09-ancestry-mismatch-incident.md) | **The current "EAS got no percentile" failure class — root cause + proposed solutions** |
| 10  | [data-layout.md](10-data-layout.md) | Directory inventory, cache schemas, DB tables, JSON formats |
| 11  | [examples.md](11-examples.md) | End-to-end worked examples for VCF, gVCF, CRAM, BAM, and 23andMe inputs |
| 12  | [known-issues.md](12-known-issues.md) | Past incidents, mitigations, and drift safeguards |
| 13  | [reviewer-questions.md](13-reviewer-questions.md) | Specific decisions and open questions we want the reviewer to opine on |
| 14  | [advisor-review.md](14-advisor-review.md) | External advisor's review (2026-05-14), captured verbatim |
| 15  | [action-plan.md](15-action-plan.md) | Action plan mapping the advisor's recommendations to concrete code paths + sequencing |
| +   | [modules/](modules/README.md) | Adjacent sub-apps reverse-proxied under the same hostname (file converter, ancestry app, translocation scanners, /compare, chat, profiles, ...) |

## TL;DR for the reviewer

- **Reference**: GRCh38, 1000 Genomes Phase 3
  (`/data/pgs2/ref_panel/GRCh38_1000G_ALL`), 3,202 unrelated samples
  (KING 2nd-degree cutoff), 5 super-populations + dynamic `MIX = EUR+EAS`.
  `MID` (Middle Eastern) is a placeholder — no panel.
- **PGS source**: PGS Catalog harmonized files
  (`*_hmPOS_GRCh38.txt.gz`). GRCh37 scoring files are lifted over to
  GRCh38 with UCSC `liftOver` before scoring.
- **Scoring engine**: `plink2 --score ... cols=+scoresums
  no-mean-imputation list-variants` (canonical method, hashed into
  ref-stats as `scoring_method=plink2-nomi`,
  `imputation_policy=no-mean-imputation`).
- **Three scoring entry points**: full-pgen path (cache PGS+PCA expanded
  VCF as pgen, run plink2), fast-path (skip pgen for ≤2000-variant PGS
  on gVCFs), Pipeline E+ (direct BAM/CRAM pileup at PGS positions, no
  VCF intermediate).
- **Ancestry**: PCA projection of the user onto the 1000G eigenvecs,
  super-pop assignment by min-Euclidean over PC1..PC4, admixture
  proportions by inverse-distance weighting (no formal ADMIXTURE/RFMix).
- **Percentile model**: Two compute paths share the same canonical
  variant set. Parametric `Φ((score − μ)/σ)` against precomputed
  per-(PGS × population) μ/σ stored in a JSON with a hard contract
  (`schema_version=1`, `variant_ids_sha256`, `scoring_method`,
  `imputation_policy`, `n_samples`, `mean`, `std`, `generated_at`).
  ECDF (linear-interpolation rank percentile) is the
  Phase-2.1-target primary; the parametric path is still the live
  default for ~99% of reads. Both paths refuse stats files whose
  fingerprint disagrees — no silent z-score against the wrong
  distribution.
- **Match-rate gating**: <60% match → `status=failed`, no percentile.
  60–85% → `status=warning`. ≥85% → `status=passed`. plink2 "skipped
  due to mismatching allele code" warnings are surfaced as a separate
  `scoring_diagnostics.skipped_due_to_mismatching_allele_code` count.
- **Sanity gates**: |z| > 6 → percentile suppressed
  (`reason=z_score_extreme`); |z| > 4 → kept but warned; percentile
  clamped to [0.5, 99.5]; collapsed-σ detection vs. an expected std
  per PGS.
- **Build validation**: 3-SNP spot-check panel (rs7412, rs429358,
  rs1801133) cross-referenced against header-declared build + scoring
  file's `HmPOS_build`; mismatch triggers `liftOver` of the scoring
  file (not the user's VCF) before scoring proceeds. The match-rate
  gate is the secondary safety net.
- **CRAM/BAM handling**: targeted variant calling at PGS positions and
  the 106K-SNP PCA panel, cached under `cram_vcf_cache/`; full-genome
  conversion available via `scripts/cram_to_vcf.sh`. Pipeline E+ runs
  direct pileup at PGS positions for ad-hoc scoring without ever
  writing a VCF.
- **gVCF handling**: `bcftools convert --gvcf2vcf` expansion at a union
  of PGS + PCA positions (~7.34M sites) with `--targets-overlap 1` to
  recover ref-block-spanning positions; `<*>`/`<NON_REF>` placeholder
  ALTs are rewritten to the catalog effect allele. The output is
  plink2-friendly and used only for PGS+PCA — genome-wide tests read
  the raw gVCF.
- **Drift safeguards**: nightly `ref_stats_selftest.py` (50 panel
  re-scores per PGS×pop), in-batch control sample HG00096 with
  golden-percentile drift bound ±10pp, cohort-level KS-vs-uniform
  check, `live_percentile.apply_live_overlay` that recomputes
  percentile from `raw_score` against current stats at every read.

## TL;DR of the EAS-mismatch incident (Chapter 09)

For about **85% of PGSes the system supports**, an East Asian (or any
non-EUR) user currently gets `percentile=None` with a downstream
LLM-generated message about "no precomputed ancestral benchmarks". The
root cause is **not** missing data — `/data/ref_stats/<pgs>/EAS_GRCh38.json`
exists for 361 PGSes. The root cause is that these files were generated
under an older schema and **do not carry the fields required by the
hard contract** (`schema_version`, `variant_ids_sha256`,
`scoring_method`, `imputation_policy`, `generated_at`, `n_variants`).
The loader correctly refuses them under the contract introduced after
the PGS000334 stale-cache incident — but for non-EUR there is no
legacy fallback path, so the percentile is dropped entirely. See
Chapter 09 for the trace and the four candidate fixes (re-bless,
regenerate, ECDF-from-NPY, schema-on-write).

## Where to read code first

```
simple-genomics/
├── app.py                          FastAPI routes + UI; LLM interpretation calls
├── runners.py                      variant calling, scoring, ancestry; ~8.5K lines
├── pipeline/                       modular pipeline
│   ├── config.py                   tool paths, POPULATIONS, ref_stats_path()
│   ├── scoring.py                  RefSelection, compute_percentile_multipop, _rs_validate
│   ├── ingest_pgs.py               PGS Catalog download + parse
│   ├── match_logic.py              parse_pgs_scoring_file (used to fingerprint catalog)
│   ├── liftover_v2.py              GRCh37/38 conversion of scoring files
│   ├── live_percentile.py          live overlay at report-read time
│   ├── ecdf_percentile.py          Phase 2.1 ECDF compute
│   ├── eligibility_gates.py        PGS-level eligibility (ancestry, weight_type, AUC, R²)
│   ├── result_guards.py            interpretation directional-language gate
│   ├── portability_warnings.py     hardcoded "known low cross-ancestry portability" list
│   ├── fingerprint.py              ref-stats variant-set hashing
│   ├── registry.py                 read-side registry resolver (canonical stats lookup)
│   ├── db.py                       SQLite: sample results, ref stats, audit
│   ├── pca_projection_validation.py PCA QC: variance-standardize, anchor checks
│   ├── sex_stratified_stats.py     per-(PGS, pop, sex) stats (rolling out)
│   ├── score_provenance.py         provenance bundle attached to every result
│   ├── matched_subset_stats.py     per-sample matched-variant μ/σ (proposed v3 ref)
│   ├── gvcf_ref_aware_rewrite.py   <*>/<NON_REF> → effect-allele rewriter
│   ├── chip_manifests.py           consumer-chip manifests for 23andMe-like inputs
│   ├── backfill.py                 historical-report backfill
│   ├── cram_reference_selection.py CRAM contig-name → reference fasta picker
│   └── build_ref_stats.py          recompute_ref_stats core
└── scripts/
    ├── cram_to_vcf.sh              CLI: per-chrom parallel CRAM→VCF
    ├── pgs_sites_call.sh           PGS-positions + hom-ref pileup
    ├── recompute_ref_stats.py      rebuild ref-stats JSONs for (pgs × pop)
    ├── ref_stats_registry.py       blesses canonical ref-stats files
    ├── ref_stats_selftest.py       nightly drift detector
    └── pgs_stats_audit.py          per-PGS audit
```

## Conventions used in this packet

- File paths are absolute and pinned to `genom-beast-gpu`.
- Code references use `file.py:LINE` so the reviewer can grep.
- Where a behavior changed recently, dates are given in `YYYY-MM-DD`.
- "Phase 0/1/2" refers to REMEDIATION_PLAN.md, the post-incident
  hardening plan executed on 2026-05-14.
