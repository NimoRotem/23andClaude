# 08 — Data Layout, Caches, and Persistence

A consolidated map of every directory the pipeline reads or writes, the
schema(s) inside, and the invalidation rules.

## 8.1 Top-level directories on `genom-beast-gpu`

| Path                                           | Owner / role                                     |
| ---------------------------------------------- | ------------------------------------------------ |
| `/home/nimrod_rotem/simple-genomics/`          | application code, FastAPI on :8600               |
| `/home/nimrod_rotem/bam-converter/`            | 23andMe → VCF converter (separate repo)          |
| `/data/genom-nimo/`                            | reference fastas (`reference.fasta`, `reference_chr.fa`) + per-user BAMs |
| `/data/pgs2/`                                  | 1000G panel + ref-stats + scoring CSVs           |
| `/data/pgs_cache/`                             | per-PGS ingested scoring files + PCA cache       |
| `/data/pgen_cache/`                            | per-user plink2 pgen cache                       |
| `/data/ref_stats/`                             | new multi-pop ref-stats tree (PGS → pop → JSON)  |
| `/data/ancestry_reference/`                    | chain files (`hg19ToHg38.over.chain.gz`)         |
| `/scratch/simple-genomics/`                    | per-run temp dirs (under `SCRATCH`)              |

## 8.2 `/data/pgs2/`

```
/data/pgs2/
├── GRCh38_1000G_ALL.{pgen,pvar.zst,psam}    # 3,202 unrelated 1000G samples
├── GRCh37_1000G_ALL.{pgen,pvar.zst,psam}    # legacy, used only for liftover edge cases
├── ref_panel/
│   ├── pop_samples/{EUR,EAS,AFR,SAS,AMR}.txt
│   ├── ancestry_ref.{bed,bim,fam}           # legacy plink1 ref (ancestry app)
│   ├── ancestry_prune.{log,prune.in,prune.out}
│   └── king.cutoff.out.id                   # kept (unrelated) sample IDs
├── ref_panel_stats/                          # LEGACY EUR-only ref-stats (v0/v1)
│   ├── PGS000005_EUR_GRCh38.json
│   ├── PGS000016_EUR_GRCh38_n6648373_plink2-nomi_sha-a84b7a02.json
│   └── batch_control_golden.json
├── plink2_scoring_files/                    # raw PGS Catalog tsvs (kept for forensics)
│   ├── PGS000001.tsv
│   └── ...
├── vcf_norm_split/                          # family-cohort gVCFs (NOT per-sample gVCFs)
│   └── family6norm_chr{1..22,X,Y,M}.vcf.gz
├── ref_scoring/                             # working dir for nf-core/pgsc_calc runs
├── matched_ref/                             # dynamic match-subset ref pgens
├── apptainer_cache/ singularity_cache/ containers/ nxf_conda_cache/  (nf-core)
├── results/                                 # nf-core outputs
├── samplesheet_*.csv                        # nf-core inputs
└── PIPELINE_AUDIT_REPORT.md                 # static one-shot audit (pre-pipeline)
```

Note: `vcf_norm_split/family6norm_chr*.vcf.gz` are joint-called multi-sample
VCFs from the family cohort, **not** per-sample gVCFs. Don't substitute them
for missing gVCFs.

## 8.3 `/data/pgs_cache/`

```
/data/pgs_cache/
├── PGS000001/
│   ├── PGS000001_hmPOS_GRCh38.txt.gz        # downloaded from PGS Catalog
│   ├── scoring_original.txt.gz              # symlink
│   ├── metadata.json
│   ├── scoring_clean.tsv.gz                 # canonical normalized form
│   ├── scoring_plink2.tsv                   # user-format IDs
│   ├── scoring_refpanel.tsv                 # ref-panel-format IDs
│   └── eligibility.json
├── PGS000334/  ...
├── pca_1000g/
│   ├── ref.eigenvec.allele
│   ├── ref.afreq
│   ├── ref.eigenvec
│   ├── ref.eigenval
│   └── ref.psam
├── _all_pgs_pca_positions_chr.tsv           # union of PGS+PCA positions, chr-prefixed
├── _all_pgs_pca_positions_bare.tsv          # same, bare chroms
└── _all_pgs_allele_map.pickle               # placeholder→effect_allele lookup (~277 MB)
```

Union-positions files are rebuilt automatically when any scoring file
mtime exceeds the cache's mtime. The allele-map pickle is rebuilt on the
same trigger; it's loaded lazily during gVCF normalization (once per gVCF).

## 8.4 `/data/ref_stats/` (new layout)

```
/data/ref_stats/
├── PGS000001/
│   ├── EUR_GRCh38.json                       # filename in registry is canonical
│   └── EUR_scores.npy                        # raw 633-sample scores (optional)
├── PGS000004/
│   ├── EUR_GRCh38.json
│   ├── EAS_GRCh38.json
│   ├── AFR_GRCh38.json
│   ├── SAS_GRCh38.json
│   └── AMR_GRCh38.json
└── ...
```

`registry.py` discovers files via `ref_stats_path(pgs_id, pop, build)`
and prefers the new path. If missing it falls back to the legacy
`ref_panel_stats/` dir.

The optional `_scores.npy` per population is a 1D float array of the
raw per-sample scores from the panel rescore. Stored only when
`recompute_ref_stats.py --save-scores` is passed; useful for ECDF
percentile experiments without re-running plink2.

## 8.5 `/data/pgen_cache/`

```
/data/pgen_cache/
├── <16-char user-vcf sha>_<8-char param sha>/
│   ├── sample.pgen
│   ├── sample.pvar
│   ├── sample.psam
│   └── .vcf_mtime            # float seconds, mtime of source VCF at build time
```

`<key>` includes `PGEN_CACHE_SCHEMA` (currently `v5`); bumping the
constant invalidates every existing cache directory without manual cleanup.

## 8.6 `simple-genomics/cram_vcf_cache/`

```
cram_vcf_cache/
├── <16-char realpath sha>/
│   ├── pca.vcf.gz                            # 106K PCA-site VCF
│   ├── pgs.vcf.gz                            # union-PGS VCF (optional)
│   ├── y.vcf.gz                              # chrY-only VCF
│   ├── mt.vcf.gz                             # chrM-only VCF
│   ├── gvcf_normalized.v5.vcf.gz             # cached normalized gVCF (if input was gVCF)
│   └── gvcf_normalized.v5.vcf.gz.tbi
```

Schema-suffixed filenames (`.v5.`) so cache versions don't collide on
disk. Older versions are kept as a forensic trail (no garbage
collection); reviewers can `find cram_vcf_cache -name '*.v[1-4].*'` to
see which inputs predate the current schema.

## 8.7 `simple-genomics/pgs_pipeline.db` (SQLite)

```
runs(task_id PK, started_at, finished_at, status, params_json)
sample_results(
    id PK, task_id FK, pgs_id, sample_id,
    raw_score REAL, percentile REAL,
    selected_ref TEXT, match_rate REAL,
    created_at)
pgs_registry(
    pgs_id PK, trait TEXT, source TEXT,         -- 'curated' | 'polygenic' | 'common'
    variant_count INTEGER, last_ingested TEXT)
```

`db.py::insert_sample_result(...)` is called on every run finish. The
DB is the basis for the `/compare` page (per-user aggregation by
trait).

## 8.8 Logs

```
simple-genomics/
├── logs/
│   ├── cron_cohort_sanity.log     # 🚩 trips from per-batch KS check
│   ├── cron_self_test.log         # ref_stats_selftest.py output
│   └── batch_control.log          # HG00096 golden checks
├── backfill_log.jsonl             # historic re-scoring runs
├── backfill_output.log
└── build_validation.log           # under SCRATCH (see runners.py BUILD_VALIDATION_LOG)
```

The cohort-sanity log is the load-bearing file that
`live_percentile.py` reads to flag affected PGS in stored reports;
reviewers should not delete it.

## 8.9 Backups / archived versions

`simple-genomics/` keeps `.pre-*-backup` copies of `app.py` and
`runners.py` at every major refactor (visible in `ls`). These are
human snapshots, not under version control, and not part of the live
pipeline.

`*.pre-cohort-fix-20260514-015841` etc. are the most recent (May 2026)
backups before the cohort-sanity layer was added. Reviewers can diff
these against `runners.py` / `pipeline/scoring.py` to see exactly what
the cohort fix changed.

## 8.10 Authoritative source-of-truth for any number

For a reviewer trying to find "where is the actual value used":

| Question                                | Authoritative source |
| --------------------------------------- | --- |
| Which scoring file did this PGS use?    | `/data/pgs_cache/<PGS>/scoring_clean.tsv.gz` (canonical) |
| Which ref-stats μ/σ are live?           | First non-stale match in `/data/ref_stats/<PGS>/<POP>_<BUILD>.json`, then `/data/pgs2/ref_panel_stats/` |
| Which 1000G panel samples are EUR?      | `/data/pgs2/ref_panel/pop_samples/EUR.txt` |
| Which variant IDs were scored for run X? | `<scratch>/score_result.sscore.vars` (in the run's tmpdir, ephemeral) |
| Which reference fasta was picked?       | not persisted in result — reproducible via `_pick_reference_for(vcf_path)` |
| Latest pipeline commit                  | `git -C /home/nimrod_rotem/simple-genomics rev-parse HEAD` |
