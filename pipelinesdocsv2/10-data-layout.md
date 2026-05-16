# 10 — Data Layout

Complete inventory of paths, files, and cache schemas. Use this as the
key when the other chapters reference a file.

## 10.1 Filesystem tree

```
/data/
├── pgs2/
│   ├── ref_panel/
│   │   ├── GRCh38_1000G_ALL.{pgen,pvar.zst,psam}        3,202-sample panel
│   │   ├── GRCh38_1000G.king.cutoff.out.id              KING 2nd-degree filter
│   │   ├── GRCh37_1000G_ALL.{pgen,pvar.zst,psam}        rare-use GRCh37 panel
│   │   ├── pop_samples/
│   │   │   ├── EUR.txt    EAS.txt    AFR.txt    SAS.txt    AMR.txt
│   │   └── ancestry_ref.{bed,bim,fam}                   legacy plink1 ref for /ancestry/
│   ├── ref_panel_stats/                                 LEGACY + REGISTRY store
│   │   ├── registry.json                                canonical pointer (chapter 6)
│   │   ├── PGS000004_EUR_GRCh38.json                    pre-remediation EUR-only
│   │   ├── PGS000004_EUR_GRCh38_n303_plink2-nomi_sha-8856f202.json
│   │   ├── PGS000004_EAS_GRCh38_n303_plink2-nomi_sha-8856f202.json
│   │   ├── ...
│   │   └── *.stale-bias-YYYYMMDD.json                   forensics for retired files
│   └── vcf_norm_split/
│       └── family6norm_chr*.vcf.gz                      joint-genotyped, NOT gVCFs
├── ref_stats/                                           NEW STORE (schema-incomplete)
│   ├── PGS000001/
│   │   ├── EUR_GRCh38.json                              μ/σ + quantiles, missing schema fields
│   │   ├── EAS_GRCh38.json
│   │   ├── AFR_GRCh38.json
│   │   ├── SAS_GRCh38.json
│   │   ├── AMR_GRCh38.json
│   │   ├── MIX_GRCh38.json
│   │   ├── EUR_scores.npy                               n=503 raw scores for ECDF
│   │   ├── EAS_scores.npy                               n=585
│   │   ├── AFR_scores.npy                               n=893
│   │   ├── SAS_scores.npy                               n=601
│   │   └── AMR_scores.npy                               n=490
│   ├── PGS000004/  ...                                  same structure per PGS
│   └── rsid_positions.json                              rsID → chr:pos lookup
├── pgs_cache/
│   ├── <PGS_ID>/
│   │   ├── <PGS_ID>_hmPOS_GRCh38.txt.gz                 canonical scoring file
│   │   ├── <PGS_ID>_hmPOS_GRCh37.txt.gz                 if catalog also publishes GRCh37
│   │   ├── <PGS_ID>_plink2.tsv                          plink2-format conversion
│   │   ├── meta.json                                    parser metadata + fingerprint
│   │   └── _lock                                        download flock
│   ├── _all_pgs_pca_positions_chr.tsv                   union(PGS, PCA) positions, chr-prefixed
│   ├── _all_pgs_pca_positions_bare.tsv                  same, bare-chrom (for some bcftools paths)
│   ├── pca_1000g/
│   │   ├── ref.eigenvec.allele                          per-variant PC weights
│   │   ├── ref.eigenvec                                 per-sample PC coords
│   │   ├── ref.eigenval                                 eigenvalues
│   │   ├── ref.afreq                                    read-freq for projection
│   │   └── ref.psam                                     super_pop labels
│   └── cram_vcf_cache/
│       └── <sha(realpath(input))>/
│           ├── pca.vcf.gz                               PCA-sites genotypes from CRAM
│           ├── pgs.vcf.gz                               PGS-sites + hom-ref
│           └── gvcf_normalized.v5.vcf.gz                expanded gVCF (PGS+PCA only)
├── pgen_cache/
│   └── <sha(realpath(vcf))>/
│       ├── sample.pgen / pvar / psam                    cached plink2 pgen
│       └── source.mtime                                 invalidation sentinel
├── ancestry_reference/
│   ├── hg19ToHg38.over.chain.gz                         UCSC chain
│   ├── hg38ToHg19.over.chain.gz                         (also under simple-genomics/liftover/)
│   └── <legacy /ancestry/ app data>
└── genom-nimo/
    ├── reference_chr.fa[.fai]                           GRCh38, chr-prefixed
    ├── reference.fasta[.fai]                            GRCh38, bare-chrom
    └── <user genomes: FASTQ, BAM, CRAM, VCF, gVCF>      per-sample-id folders
```

## 10.2 simple-genomics application tree

```
simple-genomics/
├── app.py                          ~14K-line FastAPI; UI HTML + API routes
├── runners.py                      ~8.5K-line: variant calling, scoring, ancestry
├── chat.py                         LLM chat UI for /chat/
├── pipeline/                       modular pipeline package (see README)
├── scripts/
│   ├── cram_to_vcf.sh              CLI: full-genome CRAM → VCF (parallel-per-chrom)
│   ├── pgs_sites_call.sh           CLI: PGS-positions + hom-ref pileup from CRAM
│   ├── recompute_ref_stats.py      rebuild ref-stats for (PGS × pop)
│   ├── ref_stats_registry.py       bless/list/rebuild registry.json
│   ├── ref_stats_selftest.py       nightly drift detection
│   ├── pgs_stats_audit.py          per-PGS audit (run on demand)
│   └── cron_cohort_sanity.py       hourly KS + frac-above-80%
├── liftover/                       chain files for in-process liftover
├── docs/                           older internal design docs (predates pipelinesdocs/)
├── pipelinesdocs/                  v1 of this packet (will be retained)
├── pipelinesdocsv2/                v2 (this packet) — symlinked to here from genom-beast-gpu
├── pgs_pipeline.db                 SQLite (see §10.5)
├── ref_cache/                      in-process: PGS scoring file fingerprints
├── logs/
│   ├── cron_cohort_sanity.log      hourly cohort sanity output
│   ├── ref_stats_selftest.log      nightly drift detection output
│   ├── build_validation.log        per-run build-decision JSON lines
│   └── ingestion.log               per-PGS download/parse log
├── reports/                        legacy report dir (per-PGS .json snapshots)
├── users/<sha-uid>/reports/        per-user generated report JSONs
└── trees/                          /chat/-conversation persistence
```

## 10.3 PGS Catalog scoring file format (canonical)

`<PGS_ID>_hmPOS_GRCh38.txt.gz` is a tab-separated file with a header
block of KEY=VALUE lines (each starting with `#`) followed by the
variant rows:

```
#format_version=2.0
#pgs_id=PGS000004
#trait_reported=Coronary Heart Disease
#trait_efo=EFO_0000378
#genome_build=GRCh37
#HmPOS_build=GRCh38
#variants_number=46
#weight_type=beta
#trait_direction=higher
#development_ancestry={'EUR'}
#evaluation_ancestry={'EUR', 'EAS'}
rsID    chr_name    chr_position    effect_allele    other_allele    effect_weight    locus_name
rs10455872    6    160589086    G    A    0.0857    LPA
rs17222842    6    160665604    C    T    -0.0625   LPA
...
```

We honor `HmPOS_build` for coordinates (it tells us the build the
harmonized positions are in), not `genome_build` (which tells us the
build the original GWAS was on — historically interesting, not what's
in the file's `chr_position` column).

## 10.4 plink2-format scoring file (our derived intermediate)

`<PGS_ID>_plink2.tsv`, three columns (no header):

```
chr1:12345    A     0.013
chr1:67890    G    -0.0024
chr2:11111    T     0.0042
```

| Column | Meaning |
| ------ | --- |
| 1      | variant ID matching the user pgen's `--set-all-var-ids chr@:#` |
| 2      | effect_allele (the allele whose dose × weight contributes) |
| 3      | effect_weight (numeric, beta / log_or / log_hr depending on the catalog file) |

## 10.5 SQLite schema (`pgs_pipeline.db`)

```sql
CREATE TABLE sample_results (
    id INTEGER PRIMARY KEY,
    task_id TEXT,
    pgs_id TEXT,
    sample_id TEXT,
    raw_score REAL,
    percentile REAL,
    selected_ref TEXT,             -- 'EUR' | 'EAS' | 'AFR' | 'SAS' | 'AMR' | 'MIX' | 'MULTI' | 'UNRESOLVED' | 'UNSUPPORTED'
    match_rate REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ref_stats (
    pgs_id TEXT,
    population TEXT,
    genome_build TEXT,
    mean REAL, std REAL, n_samples INTEGER,
    schema_version INTEGER,
    variant_ids_sha256 TEXT,
    n_variants INTEGER,
    scoring_method TEXT,
    imputation_policy TEXT,
    generated_at TIMESTAMP,
    PRIMARY KEY (pgs_id, population, genome_build)
);

CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY,
    event_type TEXT,        -- 'recompute_ref_stats' | 'bless' | 'selftest_drift' | ...
    pgs_id TEXT,
    population TEXT,
    details TEXT,           -- JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

`ref_stats` is a secondary cache; the JSONs on disk are the source
of truth (registry → DB → legacy). The DB row is checked third in
`_load_stats._candidate_stats()`.

## 10.6 Cache invalidation triggers

| Cache                              | Invalidated when |
| ---------------------------------- | --- |
| `/data/pgen_cache/<sha>/`          | source VCF mtime newer than cached sentinel |
| `/data/pgs_cache/cram_vcf_cache/<sha>/gvcf_normalized.v5.vcf.gz` | `PGEN_CACHE_SCHEMA` constant bumped (v5 → v6) |
| `_all_pgs_pca_positions_*.tsv`     | any PGS scoring file or PCA eigenvec newer than file |
| `_VARIANT_SET_SHA_CACHE` (in-proc) | scoring file mtime change |
| `pipeline/registry._cache`         | `registry.json` mtime change |

## 10.7 Environment variables (over-ridable paths)

| Env var              | Default                                          |
| -------------------- | ------------------------------------------------ |
| `PLINK2`             | `/home/nimo/miniconda3/envs/genomics/bin/plink2` |
| `BCFTOOLS`           | `/home/nimo/miniconda3/envs/genomics/bin/bcftools` |
| `PGS_CACHE`          | `/data/pgs_cache`                                |
| `REF_PANEL`          | `/data/pgs2/ref_panel/GRCh38_1000G_ALL`          |
| `REF_PANEL_STATS`    | `/data/pgs2/ref_panel_stats`                     |
| `REF_STATS_DIR`      | `/data/ref_stats`                                |
| `PGS_DB_PATH`        | `<simple-genomics>/pgs_pipeline.db`              |
| `PLINK_SCORE_THREADS`| 1                                                |
| `PLINK_REF_THREADS`  | 8                                                |
| `PLINK_MEMORY_MB`    | 16000                                            |
| `REF_FASTA`          | (none — `_pick_reference_for` candidates)        |

## 10.8 Two-store consolidation target

The end state we want (after chapter 09 fixes land):

```
/data/pgs2/ref_panel_stats/
├── registry.json                  single source of truth (~360 PGSes × 5 pops)
├── <PGS>_<POP>_GRCh38_n{n}_plink2-nomi_sha-{prefix}.json
├── <PGS>_<POP>_scores.npy         (moved from /data/ref_stats/)
└── archive/
    └── *.stale-bias-YYYYMMDD.json forensics, kept indefinitely
```

`/data/ref_stats/` is decommissioned and removed.
