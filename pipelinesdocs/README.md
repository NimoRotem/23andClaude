# 23andclaude.com — Bioinformatics Pipeline Documentation

This documentation set describes the genomics and PGS (polygenic score)
pipelines that run behind https://23andclaude.com. It is written for a
bioinformatics specialist reviewing the methodology end-to-end.

The application accepts consumer-genomics files (23andMe / AncestryDNA
text dumps), VCF / gVCF, BAM, and CRAM. It runs variant lookups, PGS
scoring, ancestry / PCA, Y / mtDNA haplogrouping, ClinVar screening,
HLA typing, and a handful of repeat-expansion / specialized tests.

Host: `genom-beast-gpu` (GCE, us-central1-c, n1-standard-32 + T4)
Code: `/home/nimrod_rotem/simple-genomics/` (FastAPI app on port 8600)
Data: `/data/pgs2/`, `/data/pgs_cache/`, `/data/ref_stats/`
Tooling: plink2, bcftools, samtools, liftOver, ExpansionHunter, T1K

## Reading order

| #   | Doc                                           | Topic |
| --- | --------------------------------------------- | --- |
| 01  | [overview.md](01-overview.md)                 | System architecture & file-type dispatch |
| 02  | [input-and-alignment.md](02-input-and-alignment.md) | Raw / 23andMe TSV / VCF / gVCF / BAM / CRAM ingestion, variant calling, indexing, build detection |
| 03  | [pgs-ingestion.md](03-pgs-ingestion.md)       | PGS Catalog download, parsing, normalization, harmonization, liftover, eligibility QC |
| 04  | [scoring-pipeline.md](04-scoring-pipeline.md) | plink2 scoring (fast-path, full-pgen-cache path), gVCF expansion, allele rewrite, match-rate gating |
| 05  | [reference-panel.md](05-reference-panel.md)   | 1000 Genomes Phase 3 panel, population subsets, PCA cache, ancestry-aware ref selection |
| 06  | [percentile-and-stats.md](06-percentile-and-stats.md) | Reference-stats schema contract, z-score & percentile formula, sanity gates, scale reconciliation |
| 07  | [qa-and-validation.md](07-qa-and-validation.md) | Build validation, cohort sanity, in-batch control, nightly self-test, regression suite, live overlay |
| 08  | [data-layout.md](08-data-layout.md)           | Directory inventory, cache schemas, DB tables, key file formats |
| 09  | [examples.md](09-examples.md)                 | End-to-end worked examples for each input type |
| 10  | [known-issues.md](10-known-issues.md)         | Past incidents, mitigations, drift safeguards |

## TL;DR for the reviewer

- **Reference**: GRCh38, 1000 Genomes Phase 3 (`/data/pgs2/ref_panel/GRCh38_1000G_ALL`), 6 super-populations (`EUR`, `EAS`, `AFR`, `SAS`, `AMR`, plus a built-in `MIX = 50% EUR + 50% EAS`).
- **PGS source**: PGS Catalog harmonized files (`*_hmPOS_GRCh38.txt.gz`). GRCh37 files are lifted over to GRCh38 with UCSC `liftOver` before scoring.
- **Scoring engine**: `plink2 --score ... cols=+scoresums no-mean-imputation list-variants` (canonical method, hashed into ref-stats as `scoring_method=plink2-nomi`, `imputation_policy=no-mean-imputation`).
- **Percentile model**: parametric Φ((score − μ)/σ) against precomputed per-(PGS × population) μ/σ stored in a JSON with a hard contract (`schema_version=1`, `variant_ids_sha256`, n_samples, ref panel sha). Loaders **refuse** stats files whose live-pipeline fingerprint disagrees — no silent z-score against the wrong distribution.
- **Match-rate gating**: <60% match → reported as `failed`, no percentile. 60–85% → `warning`. ≥85% → `passed`. Plink2 "skipped due to mismatching allele code" warnings are surfaced separately.
- **Sanity gates**: |z| > 6 → percentile suppressed (`z_score_extreme`); |z| > 4 → kept but warned; percentile clamped to [0.5, 99.5]; collapsed-σ detection vs. expected std per PGS.
- **Build validation**: 3-SNP spot-check panel (rs7412, rs429358, rs1801133) cross-referenced against header-declared build + reference panel; weak match → WARN; wrong-build match → FAIL or auto-liftover of scoring file.
- **CRAM/BAM handling**: targeted variant calling at PGS positions and a 106K-SNP PCA panel, cached under `cram_vcf_cache/`; full-genome conversion available via `scripts/cram_to_vcf.sh`. CRAMs are decoded with a contig-naming-matched fasta auto-picked from a candidate list.
- **gVCF handling**: `bcftools convert --gvcf2vcf` expansion at a union of PGS + PCA positions (~7.34M sites) with `--targets-overlap 1` to recover ref-block-spanning positions; `<*>`/`<NON_REF>` placeholder ALTs are rewritten to the catalog effect allele. The output is plink2-friendly and used only for PGS+PCA — genome-wide tests read the raw gVCF.
- **Distribution drift safeguards**: nightly `ref_stats_selftest.py` (50 panel re-scores per PGS×pop), in-batch control sample HG00096 with golden-percentile drift bound ±10pp, cohort-level KS-vs-uniform check, live-percentile overlay that recomputes percentile from `raw_score` against current stats at report-read time.

## Where to read code first

```
simple-genomics/
├── app.py                          # FastAPI routes + UI HTML
├── runners.py                      # variant calling, scoring, ancestry; the heart
├── pipeline/
│   ├── config.py                   # all paths, populations, plink2 args
│   ├── ingest_pgs.py               # PGS Catalog → /data/pgs_cache normalization
│   ├── match_logic.py              # canonical PGS scoring-file parser
│   ├── scoring.py                  # ancestry-aware percentile, schema contract
│   ├── registry.py                 # resolves PGS+pop → current ref-stats file
│   ├── live_percentile.py          # at-read recompute of pctl vs current μ/σ
│   ├── result_guards.py            # provenance attach + interpretation-vs-pctl check
│   ├── build_ref_stats.py          # builds per-(PGS×pop) ref-stats JSON
│   └── db.py                       # SQLite (pgs_pipeline.db) writer
└── scripts/
    ├── cram_to_vcf.sh              # genome-wide per-chrom parallel CRAM→VCF
    ├── pgs_sites_call.sh           # hom-ref-including PGS-sites CRAM→VCF
    ├── recompute_ref_stats.py      # rebuilds ref-stats JSON in the strict schema
    ├── ref_stats_selftest.py       # nightly drift check
    ├── batch_control.py            # HG00096 golden-percentile regression
    ├── cohort_sanity.py            # batch percentile KS test
    └── ref_stats_registry.py       # discovery of latest non-stale stats files
```

## Open questions for the reviewer

These are explicit asks for an outside opinion — please push back where appropriate:

1. **Parametric percentile vs empirical**: we report z-score derived percentiles. Empirical (ECDF) over the per-population panel scores would be defensible for small / non-Gaussian PGS. We keep both: `n_samples`, `min`, `max`, `median` are stored in the stats file; only μ/σ are used. Worth switching to ECDF for any class of PGS?
2. **MIX population**: hand-rolled 50/50 EUR/EAS mix as a default for admixed-but-not-resolved samples. Defensible? Better practice for admixed individuals (e.g. PRS-CSx, per-ancestry component weighting)?
3. **Build validation panel size**: 3 SNPs (rs7412, rs429358, rs1801133) is small. We had the previous single-SNP regime mislead a PGS run. Should we expand to 10+ SNPs or move to a hash-of-positions trick?
4. **Liftover triggering**: we lift the *scoring file* (rare) rather than the VCF (heavy). Acceptable as long as we don't smuggle GRCh37 positions into a GRCh38-built pgen cache. Any edge cases we are missing?
5. **`no-mean-imputation` choice**: we deliberately do NOT impute missing dosages to the mean (so unmatched variants contribute zero). This is conservative; the match-rate-vs-percentile interaction needs eyeballing for any PGS with sparse rsID-only inputs.
6. **gVCF normalization scope**: we expand only at PGS + PCA positions. Other tests (ROH, ClinVar, sex) read the raw gVCF. This bifurcation has tripped concat bugs (see `known-issues.md`). Reviewer take?
