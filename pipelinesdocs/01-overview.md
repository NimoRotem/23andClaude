# 01 — System Overview

## Application layout

The web app is a single FastAPI service:

```
nginx :443  ──►  uvicorn :8600   simple-genomics/app.py
                  └── runners.py (variant calling, scoring, ancestry)
                  └── pipeline/  (modular PGS pipeline)
                  └── scripts/   (CLI + cron jobs)
                  └── pgs_pipeline.db  (SQLite)
```

A second app (`/ancestry/`) proxies to port 8700 and is out of scope for
this document.

## Conceptual dataflow

```
                user file upload (or sftp drop)
                              │
                              ▼
                  ┌───────────────────────┐
                  │  _detect_file_type    │
                  │  vcf / bam / cram     │
                  └───────────────────────┘
                              │
            ┌─────────────────┼──────────────────┐
            ▼                 ▼                  ▼
         VCF/gVCF          BAM/CRAM         23andMe TSV
            │                 │                  │
            │     ┌───────────┼──────────────┐   │
            │     │           │              │   │
            ▼     ▼           ▼              ▼   ▼
        run_pgs   _derive_   cram_to_vcf   pgs_sites    converter
        _score    pca_vcf    .sh genome-   _call.sh     (bam-converter)
                  (106 K     wide          PGS sites
                  PCA sites) variants      (incl hom-ref)
            │
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
   └──────────────────────┘
            │
            ▼
   ┌──────────────────────┐
   │ Ancestry-aware       │  primary pop chosen from admixture proportions
   │ percentile           │  (top ≥80% → that pop; else MIX)
   │                      │  z = (score - μ)/σ;  p = Φ(z) × 100
   │                      │  sanity gates: |z|>6 fail, |z|>4 warn, clamp [0.5,99.5]
   └──────────────────────┘
            │
            ▼
       result dict   ──►   SQLite (pgs_pipeline.db) + JSON report on disk
            │
            ▼
       UI / API
            │
            ▼
   live_percentile.apply_live_overlay
   (recompute pctl from raw_score against CURRENT μ/σ at every read)
```

## Tests dispatched by the front end

`run_specialized` in `runners.py` maps a UI test method to a pipeline:

| Test                      | Accepts        | Output                                |
| ------------------------- | -------------- | ------------------------------------- |
| `pgs_score`               | VCF / gVCF     | raw score, match rate, percentile     |
| `pca_1000g`               | VCF / BAM / CRAM | 10 PCs, closest super-population    |
| `admixture`               | derives from PCA | fractional ancestry proportions     |
| `y_haplogroup`            | VCF / BAM / CRAM | Y-DNA haplogroup (Yleaf-like)       |
| `mt_haplogroup`           | VCF / BAM / CRAM | mtDNA haplogroup (HaploGrep3)       |
| `roh`                     | VCF / BAM / CRAM | runs of homozygosity, F_ROH          |
| `neanderthal`             | (delegates to PCA) | introgression %                    |
| `hla_typing`              | BAM / CRAM (T1K); VCF (proxy SNP) | HLA alleles |
| `repeat_expansion`        | BAM / CRAM only | ExpansionHunter per gene            |
| `clinvar_screen`          | VCF / gVCF     | pathogenic + carrier findings         |
| `vcf_stats`               | VCF / gVCF     | Ti/Tv, het/hom, sex, ploidy           |
| `variant_lookup`          | VCF / gVCF     | direct rsID/region lookup             |

Tests in `_CRAM_OK_METHODS` know how to derive what they need from BAM/CRAM
on demand (mostly via `_derive_pca_vcf_from_cram` and the cached
`cram_vcf_cache/<hash>/` directory). Anything else returns a clear error
asking the user to convert to VCF first.

## Cache hierarchy (read this once and remember it)

| Cache                                    | Purpose                                    | Invalidation |
| ---------------------------------------- | ------------------------------------------ | --- |
| `/data/pgs_cache/<PGS_ID>/`              | downloaded + normalized PGS scoring files  | manual; ingest is idempotent |
| `/data/pgs_cache/pca_1000g/`             | LD-pruned PCA reference cache (one-time)   | manual rebuild |
| `/data/pgs_cache/_all_pgs_pca_positions_{chr,bare}.tsv` | union of PGS+PCA positions | rebuild if any scoring file newer than cache |
| `/data/pgen_cache/<key>/sample.{pgen,pvar,psam}` | plink2 pgen built from user's VCF | mtime of source VCF + `PGEN_CACHE_SCHEMA` (currently `v5`) |
| `simple-genomics/cram_vcf_cache/<hash>/` | per-CRAM derived VCFs (PCA-sites, etc.)    | manual; keyed by CRAM realpath sha |
| `/data/ref_stats/` (new) + `/data/pgs2/ref_panel_stats/` (legacy) | per-(PGS × pop) reference μ/σ | regenerated by `recompute_ref_stats.py`; loader refuses on schema mismatch |

## Where the SQLite database fits

`simple-genomics/pgs_pipeline.db` records:

- `runs`: every PGS run with timestamps, params, status
- `sample_results`: per-(task × pgs × sample) raw score, percentile, selected_ref, match_rate
- `pgs_registry`: catalog of known PGS IDs + traits (curated + polygenic + common)

The DB is **append-only writable from the app**; ref-stats files are the
authoritative numeric source and live on disk.
