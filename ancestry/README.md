# Ancestry Inference (`/ancestry/`)

A standalone FastAPI + React 19 application that decomposes a single sample's
ancestry against the gnomAD HGDP + 1000 Genomes reference panel using
LD-pruned PCA + Rye (NNLS over PC space). It also runs an ROH (runs of
homozygosity) scan to detect bottlenecks and consanguinity.

This is the second sub-app in the [23andClaude](../README.md) monorepo. It
runs on its own port (default `8700`) and is mounted at `/ancestry/` behind
the shared nginx vhost.

## What it does

- **Accepts BAM, CRAM, gVCF, VCF, or 23andMe-style raw text** as input.
- **Calls/normalizes variants** at ~240K LD-pruned reference sites and merges
  the sample into the reference panel.
- **Projects the sample onto a 20-PC PCA** computed from the reference panel.
- **Estimates ancestry proportions** with [Rye](https://github.com/healthdisparities/rye),
  an NNLS-on-PCA admixture estimator. Default panel: ~4,091 samples across 9
  continental groups (African, European, MiddleEastern, SouthAsian, EastAsian,
  SoutheastAsian, NativeAmerican, Oceanian, etc.).
- **Runs an ROH scan** with `plink2 --homozyg` to identify long runs of
  homozygosity (suggestive of recent shared ancestry / population bottlenecks).
- **Streams progress live** to the frontend over Server-Sent Events.
- **Persists results** as JSON files under `$APP_ROOT/results/` so they
  survive process restarts.
- **Exports** per-sample CSV or a compare-across-samples bundle, plus a
  PCA scatter PNG.

## Architecture

```
┌──────────────────────────────────┐
│  React 19 + Vite SPA (frontend)  │
│  Single-page analyzer + history  │
└──────────────┬───────────────────┘
               │
               ▼ proxied at /ancestry/ by nginx
┌──────────────────────────────────┐
│  FastAPI app (backend, :8700)    │
│  ├─ /api/analyze     start a job │
│  ├─ /api/jobs/...    poll/SSE    │
│  ├─ /api/reference   panel info  │
│  └─ /api/export      CSV / ZIP   │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  pipeline.py — orchestration     │
│  bcftools / samtools / plink2 /  │
│  Rye / GRCh38 reference          │
└──────────────────────────────────┘
```

## Layout

```
ancestry/
└── app/
    ├── backend/
    │   ├── main.py          # FastAPI app, routes, job tracker
    │   ├── pipeline.py      # The actual ancestry pipeline
    │   ├── requirements.txt
    │   └── test_roh.py
    ├── frontend/
    │   ├── src/
    │   │   ├── App.jsx      # SPA entry, all UI
    │   │   └── main.jsx
    │   ├── index.html
    │   ├── package.json
    │   └── vite.config.js
    └── reference/           # population/group mapping files
```

## Setup

### 1. Reference panel

The pipeline expects an LD-pruned plink2 binary panel built from the
[gnomAD HGDP + 1kGP](https://gnomad.broadinstitute.org/) joint VCF, plus a
`pop2group.txt` mapping each panel sample to a continental group.

Place the panel at `$APP_ROOT/reference/` (default `/data/ancestry_app/reference/`):

```
reference/
├── ref_pruned.pgen     # LD-pruned plink2 panel (~240K variants)
├── ref_pruned.pvar.zst
├── ref_pruned.psam
├── pop2group.txt       # sample_id<TAB>population<TAB>group
└── ref_pca.eigenvec    # precomputed 20-PC PCA (optional, will be computed if missing)
```

A reference build script is not bundled here — see
[gnomad-public-utils](https://github.com/broadinstitute/gnomad_methods)
for the canonical workflow. The expected file layout is documented in
`pipeline.py` (constants `REF_BED`, `POP2GROUP`).

### 2. GRCh38 FASTA

Used when calling variants from CRAM input or normalizing VCFs. Set
`DEFAULT_FASTA` in your `.env` (defaults to `/data/reference/GRCh38.fa`).

### 3. Bioinformatics tools

The pipeline shells out to `bcftools`, `samtools`, `plink2`, and `rye`. They
must be on `$PATH`, or you can point at a conda env's `bin/`:

```bash
export GENOMICS_BIN=/opt/conda/envs/genomics/bin
```

### 4. Python deps

```bash
cd ancestry/app/backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 5. Frontend build

```bash
cd ancestry/app/frontend
npm install
npm run build
```

The backend serves the built `dist/` directly when nothing else matches a
route, so no separate static server is needed.

### 6. Run

```bash
cd ancestry/app/backend
python main.py
```

By default this listens on `0.0.0.0:8700`. Override with `ANCESTRY_PORT`.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `ANCESTRY_PORT` | `8700` | Backend listen port |
| `APP_ROOT` | `/data/ancestry_app` | Where reference panel, results, uploads live |
| `SAMPLE_DIRS` | `/data/aligned_bams,$APP_ROOT/uploads` | Comma-separated dirs the file picker scans |
| `NIMOG_OUTPUT_ROOT` | `/scratch/nimog_output` | Extra dir to scan for nimog-produced VCFs |
| `DEFAULT_FASTA` | `/data/reference/reference.fasta` | GRCh38 FASTA for variant calling |
| `GENOMICS_BIN` | (none) | Path prepended to `$PATH` to find bcftools/samtools/plink2/rye |

## API

All endpoints are under `/api/` (or `/ancestry/api/` behind nginx).

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Liveness check |
| `/api/reference/status` | GET | Whether the reference panel is present and complete |
| `/api/reference/detail` | GET | Full panel metadata (samples, populations, groups) |
| `/api/server-files` | GET | List BAM/CRAM/VCF files visible under `SAMPLE_DIRS` |
| `/api/analyze` | POST | Start an ancestry job (file path or upload) |
| `/api/analyze/batch` | POST | Start multiple jobs at once |
| `/api/jobs/{id}` | GET | Poll job state |
| `/api/jobs/{id}/stream` | GET | SSE stream of live progress |
| `/api/jobs` | GET | List all jobs (history) |
| `/api/jobs/compare` | GET | Side-by-side comparison of multiple jobs |
| `/api/jobs/{id}/csv` | GET | Export one job's results as CSV |
| `/api/export/all-csv` | GET | Export all jobs as a single CSV |
| `/api/jobs/{id}` | DELETE | Delete a job and its persisted result |

## Pipeline stages

1. **Detect input type** — BAM / CRAM / VCF / gVCF / 23andMe text
2. **Variant calling / normalization** — bcftools call (BAM/CRAM) or
   bcftools norm + filter (VCF)
3. **Site intersection** — restrict to the ~240K LD-pruned panel sites
4. **Merge with reference panel** — plink2 `--pmerge`
5. **PCA projection** — plink2 `--score` against the panel's eigenvectors
6. **Rye admixture** — NNLS over the 20-PC space, mapped through
   `pop2group.txt`
7. **ROH scan** — plink2 `--homozyg`
8. **Result assembly** — group fractions, population fractions, ROH summary,
   PCA coordinates

## Validation

`pipeline.py` includes a leave-one-out self-classification helper that holds
out each reference sample, runs the pipeline, and reports per-group accuracy.
This is the recommended sanity check after rebuilding the panel or changing
the LD-pruning parameters.

## License

MIT — see [`../LICENSE`](../LICENSE).
