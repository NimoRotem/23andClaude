# 01 — System Overview

## 1.1 Application layout

23andclaude.com is a single FastAPI service with a small surface of
sidecar tools. There is no microservice split; the pipeline is mostly a
big Python process with subprocess invocations of `plink2`, `bcftools`,
`samtools`, `liftOver`, `T1K`, and `ExpansionHunter`.

```
nginx :443  ──► uvicorn :8600    simple-genomics/app.py        FastAPI routes + UI HTML
                  │
                  ├── runners.py                                ~8.5K lines: variant calling, scoring, ancestry
                  ├── pipeline/                                 modular package (see README)
                  ├── scripts/                                  CLI tools + cron jobs
                  └── pgs_pipeline.db                           SQLite: ref stats, sample results, audit
```

A second app (`/ancestry/`, port 8700) is the standalone ancestry
visualizer — out of scope for this packet except where its inputs are
shared with the PGS pipeline.

## 1.2 Conceptual dataflow

```
                user file upload (or sftp drop)
                              │
                              ▼
                  ┌───────────────────────┐
                  │  _detect_file_type    │  by extension only
                  │  vcf / bam / cram     │
                  └───────────────────────┘
                              │
            ┌─────────────────┼──────────────────┐
            ▼                 ▼                  ▼
         VCF/gVCF          BAM/CRAM         23andMe TSV
            │                 │                  │
            │                 │                  ▼
            │                 │            bam-converter
            │                 │            (sibling repo) →
            │                 │            synthetic VCF
            │                 │
            │     ┌───────────┼──────────────┐
            │     ▼           ▼              ▼
            │  _derive_   cram_to_vcf    Pipeline E+
            │  pca_vcf    .sh genome-    direct pileup
            │  (106K      wide VCF       at PGS sites
            │  PCA sites) (12-way        (no VCF
            │            parallel)        intermediate)
            ▼
   ┌──────────────────────┐
   │ Genome-build         │  3-SNP spot-check + header parse
   │ validation           │  PASS / WEAK / FAIL → maybe liftover scoring file
   └──────────────────────┘
            │
            ▼
   ┌──────────────────────┐
   │ Plink2 pgen cache    │  /data/pgen_cache/<key>/sample.{pgen,pvar,psam}
   │ (gVCF expansion if   │  schema-versioned (v5) cache; rebuild on mtime change
   │  needed)             │
   └──────────────────────┘
            │
            ▼
   ┌──────────────────────┐
   │ plink2 --score       │  cols=+scoresums no-mean-imputation list-variants
   │                      │  match-rate gate: <60 fail | 60-85 warn | ≥85 pass
   │                      │  scoring_method=plink2-nomi (hashed into ref-stats)
   └──────────────────────┘
            │
            ▼
   ┌──────────────────────┐
   │ Ancestry-aware       │  primary pop from admixture proportions:
   │ percentile           │   top ≥80% → that pop
   │                      │   else → MULTI (per-pop array; Phase 1.5)
   │                      │  parametric: z = (score - μ)/σ;  p = Φ(z) × 100
   │                      │  ECDF (Phase 2.1): rank/(n+1) with linear interp
   │                      │  sanity gates: |z|>6 fail, |z|>4 warn, clamp [0.5,99.5]
   └──────────────────────┘
            │
            ▼
   ┌──────────────────────┐
   │ Eligibility gates    │  weight_type, complex_alleles, performance_metric,
   │ (pipeline/           │  ancestry-match (dev/eval), direction known
   │  eligibility_        │  → risk_language_allowed flag
   │  gates.py)           │
   └──────────────────────┘
            │
            ▼
       result dict   ──►   SQLite (pgs_pipeline.db) + JSON report on disk
            │
            ▼
   live_percentile.apply_live_overlay   recomputes pctl from raw_score
            │                            against CURRENT μ/σ at every read
            ▼
       LLM interpretation (Gemini / Claude / OpenAI)
            │
            ▼
       UI / API rendering
```

## 1.3 Tests dispatched by the front end

The UI exposes nine families of tests; each is dispatched by
`run_test(vcf_path, test_def)` in `runners.py`:

| `test_type`         | Inputs accepted | What it does |
| ------------------- | --------------- | --- |
| `pgs_score`         | VCF, gVCF, BAM, CRAM, 23andMe | Polygenic score against a single PGS Catalog ID |
| `variant_lookup`    | VCF, gVCF, BAM, CRAM | Genotype lookup for a fixed rsID list (carrier panels, pharmacogenomics) |
| `clinvar_screen`    | VCF, gVCF | ClinVar-pathogenic intersection across the user's variants |
| `vcf_stats`         | VCF, gVCF | bcftools stats summary + chrom-level coverage |
| `pca_1000g`         | VCF, gVCF, BAM, CRAM | PCA projection onto 1000G + super-pop assignment + admixture |
| `y_haplogroup`      | VCF, gVCF, BAM, CRAM | Yfull-tree-style Y-SNP haplogroup |
| `mt_haplogroup`     | VCF, gVCF, BAM, CRAM | HaploGrep3 mtDNA haplogroup |
| `hla_typing`        | BAM, CRAM (VCF fallback) | T1K typing; sibling-VCF proxy-SNP fallback |
| `repeat_expansion`  | BAM, CRAM only | ExpansionHunter per gene |
| `roh`               | VCF, gVCF | Runs of homozygosity (bcftools roh) |
| `neanderthal`       | VCF, gVCF | Neanderthal-derived allele fraction |
| `specialized`       | many | Catch-all for additional configured tests |

This packet documents `pgs_score`, `pca_1000g`, and the surrounding
infrastructure in depth — the other tests are mentioned only where they
share code or data with PGS scoring.

## 1.4 Three PGS scoring entry points

Branches inside `run_pgs_score()` (`runners.py:3470` and downstream)
dispatch by input type and PGS size:

| Entry point         | Trigger                                 | Code                                  |
| ------------------- | --------------------------------------- | ------------------------------------- |
| **Fast path**       | gVCF + PGS variant count ≤ 2,000        | `runners.py::_score_pgs_fast`         |
| **Full pgen path**  | everything else (default)               | `runners.py::run_pgs_score`           |
| **Pipeline E+**     | input is BAM/CRAM                       | `runners.py::_run_pgs_score_pileup`   |

All three converge on the same `_postprocess_pgs_result()` hook in
`runners.py:4380` for confidence computation, cross-ancestry warnings,
and field-rename cleanup. Their outputs are byte-identical schema-wise.
Chapter 04 walks each one in detail.

## 1.5 Storage layout (one-screen summary)

```
/data/
├── pgs2/
│   ├── ref_panel/                        1000G Phase 3 GRCh38 panel (pgen, pvar.zst, psam)
│   ├── ref_panel_stats/                  legacy per-PGS×pop μ/σ JSONs + registry.json (canonical)
│   └── vcf_norm_split/                   joint-genotyped family6 normalized VCFs (NOT gVCFs)
├── ref_stats/                            new multi-pop μ/σ + score arrays (.npy) per PGS×pop
├── pgs_cache/
│   ├── <PGS_ID>/                         downloaded scoring file + plink2-format conversion
│   ├── _all_pgs_pca_positions_chr.tsv    union of PGS + PCA positions (rebuilt on mtime change)
│   ├── pca_1000g/                        ref.eigenvec, ref.eigenvec.allele, ref.afreq, ref.psam
│   └── cram_vcf_cache/<sha>/             per-CRAM cached PCA VCF, PGS-sites VCF, normalized gVCF
├── ancestry_reference/                   liftOver chain files, legacy ancestry app data
└── genom-nimo/                           user genomes (FASTQ, BAM, CRAM, VCF, gVCF)
```

Full inventory and file-format notes are in
[data-layout.md](10-data-layout.md).

## 1.6 Versioning and schema markers

A few constants document the cache schema versions in the code:

- `PGEN_CACHE_SCHEMA = "v5"` — bumping invalidates all cached
  `gvcf_normalized.v5.vcf.gz` artifacts. v3 was the bad concat
  experiment that collapsed the family6 prostate match rate; v4 reverted
  to PGS+PCA only; v5 also strips `<*>` from multi-allelic ALTs.
- `REF_STATS_SCHEMA_VERSION = 1` — top-level field in every
  ref-stats JSON. Stats lacking this field (or with `schema_version=0`)
  are silently rejected by the loader — the **direct cause** of the
  current EAS-percentile failures (chapter 09).
- `refstats_schema_version = 2` — the spec target after the May 2026
  remediation. Phase 0/1/2 wrote the schema definition; backfilling
  the existing JSONs to that schema is the work remaining.

## 1.7 What we trust, what we still don't

**Trusted**:
- plink2 `--score no-mean-imputation` is deterministic given a fixed
  variant set; ref-stats are computed against the exact same plink2
  invocation that scores user data.
- `_rs_validate` enforces a strict contract on every load. After the
  PGS000334 stale-cache incident, no silent z-score against the wrong
  distribution is possible — the failure mode is now "no percentile",
  not "wrong percentile".
- The build-validation 3-SNP spot-check has correctly disambiguated
  GRCh37/GRCh38 in every reviewed sample to date.
- Pipeline E+ (BAM/CRAM direct pileup) and gVCF expansion both match
  the full-pgen path within `<= 0.1%` raw_score for samples where we
  have a known-good reference, validated via the in-batch control
  sample HG00096.

**Still weak**:
- Ancestry inference is PCA + inverse-distance weighting, **not**
  ADMIXTURE/RFMix. Posteriors for admixed individuals are smooth-but-
  shallow; we can't distinguish e.g. 70%EUR/30%AMR from 60%EUR/40%AMR
  with confidence.
- Reference-stats coverage for non-EUR populations was generated under
  an older schema and currently fails the strict-load contract. See
  Chapter 09.
- We do not run PRS-CSx (or any multi-population PRS construction).
  Every PGS we score was trained on a single (usually European) cohort
  and is reported with a `cross_ancestry_warning` for non-EUR users.
- Sex-stratified ref-stats exist as a module (`sex_stratified_stats.py`)
  but are not yet built for any PGS. CAD-class PGS percentiles for sex-
  dimorphic traits are still sex-pooled.
- Middle Eastern, Pacific Islander, and Indigenous Australasian
  ancestries are unrepresented in 1000G Phase 3 and therefore in our
  panel. We currently flag these as `UNSUPPORTED` and emit a per-pop
  sensitivity array (Phase 1.5).
