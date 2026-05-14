# TOOLCHAIN — pinned tool, reference, and dataset versions

Any change to a pin below **forces a ref-stats rebuild** and bumps
`refstats_schema_version`. The container build CI fails if these pins
drift from the actual lockfile (see `.github/workflows/toolchain-check.yml`).

## Binaries

| Tool      | Pinned version | Source / install              |
|-----------|----------------|-------------------------------|
| plink2    | `v2.0.0-a.6.9` | `conda install -c bioconda plink2=2.0.0a.6.9` |
| bcftools  | `1.22`         | `conda install -c bioconda bcftools=1.22`      |
| samtools  | `1.22`         | `conda install -c bioconda samtools=1.22`      |
| liftOver  | UCSC `482`     | `kentutils` release           |
| zstd      | `1.5.x`        | system or conda               |
| ExpansionHunter | `5.0.0`  | https://github.com/Illumina/ExpansionHunter |
| T1K       | `1.0.5`        | https://github.com/mourisl/T1K |

The `plink2 --version` string must match exactly:
`PLINK v2.0.0-a.6.9LM 64-bit Intel (29 Jan 2025)`. The full string lands
in every PGS report's `pipeline_fingerprint.plink2_version`.

## Reference data

| Asset | Pin |
|---|---|
| GRCh38 FASTA | `hs38DH.fa` — GRCh38.p14 no-alt-analysis-set + 1000G decoys |
| GRCh38 FASTA sha256 | `<written by scripts/build_contig_md5_fixture.sh>` |
| Chain files | `hg19ToHg38.over.chain.gz`, `hg38ToHg19.over.chain.gz` from UCSC, sha256 logged per report |
| Reference panel | `1000G + NYGC high-coverage, GRCh38, 3,202 samples` (renamed in Phase 1.4) |
| Reference panel kept-IID set | `<DATA_ROOT>/pgs2/ref_panel/king_filtered.keep` (post-KING 2nd-degree filter) |
| Per-population keep files | `<DATA_ROOT>/pgs2/ref_panel/pop_samples/<POP>.king_filtered.txt` |

## Datasets

| Asset | Pin |
|---|---|
| PGS Catalog release | env-pinned via `<CACHE_ROOT>/pgs_cache/_catalog_release.txt` (one-line tag). Surfaces in `pipeline_fingerprint.pgs_catalog_release`. |
| gnomAD AJ AFs | gnomAD v4 — used for synthetic AJ fallback panel and build-validation anchor fixture. |
| Build-validation anchor fixture | `data/build_anchors_500snp.tsv` — 500 SNPs / 22 autosomes / AF 0.05–0.95 in ≥3 pops, both GRCh37 and GRCh38 coords. |

## Python environment

- Pinned via `requirements.txt` (sha256 surfaced in
  `pipeline_fingerprint.python_lockfile_sha256`).
- Container image SHA captured in
  `pipeline_fingerprint.container_digest` (or `"host"` when running
  directly).

## Versioning

- `refstats_schema_version` is currently **2** (post Phase 1.4 — KING
  filter + sum_scores.npy added).
- Stats files declaring `schema_version != current` are rejected by the
  strict loader and re-built via `scripts/recompute_ref_stats.py`.
- Mismatch at scoring time → refuse percentile unless
  `ACCEPT_FINGERPRINT_DRIFT=1` is set (caps confidence at MEDIUM).

## CI enforcement (planned + this PR)

- `.github/workflows/toolchain-check.yml`: ensures the pinned versions
  in this file match `requirements.txt` and the container's actual
  `plink2 --version` / `bcftools --version` / `samtools --version`
  outputs. Any drift fails the build.
- `.github/workflows/sanitize-docs-lint.yml`: forbids path/host/port
  leakage in `pipelinesdocs/` (Phase 0.3).
