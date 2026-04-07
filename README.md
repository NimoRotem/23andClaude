# 23andClaude

A self-hosted, AI-augmented genomics workstation. 23andClaude takes whole-genome
sequencing data and turns it into things you can actually understand: ancestry
breakdowns, polygenic risk scores, carrier and monogenic-disease screens, sex
verification, and an interactive Claude Code assistant that knows your data.

> Think "23andMe, but you own the server, the model, and the report."

This repository is a monorepo containing two FastAPI + React applications that
run side by side behind a single nginx vhost (`23andclaude.com`):

| Sub-app | Path | Port | Mounted at | What it does |
|---------|------|------|------------|--------------|
| **Genomics Dashboard** | [`main/`](./main) | `8600` | `/` | File management, variant calling, PGS scoring, AI reports, Claude Code chat |
| **Ancestry Inference** | [`ancestry/`](./ancestry) | `8700` | `/ancestry/` | gnomAD HGDP+1kGP-based ancestry decomposition with NNLS / Rye and ROH analysis |

Both applications share `/data` and `/scratch` on the host so a single sample
flows: raw BAM/CRAM/FASTQ → `nimog` (BAM→VCF) → main pipeline (PGS, screens) →
ancestry app (continental decomposition + ROH).

## Screenshots / What it looks like

The main dashboard has five tabs: **AI Assistant**, **Data & Pipeline**,
**Score**, **Results**, and **Server**. The ancestry app is a single-page
analyzer with real-time progress streaming, PCA scatter, world-map visualization,
and CSV/PNG export.

## Architecture

```
                          ┌─────────────────────┐
                          │   23andclaude.com   │
                          │       (nginx)       │
                          └──────────┬──────────┘
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
              ▼                      ▼                      ▼
        / (root)              /ancestry/              /tmux/ (optional)
   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
   │ Genomics        │   │ Ancestry        │   │ tmux dashboard  │
   │ Dashboard       │   │ Inference       │   │ (separate repo) │
   │ FastAPI :8600   │   │ FastAPI :8700   │   │                 │
   │ React 19 SPA    │   │ React 19 SPA    │   └─────────────────┘
   └────────┬────────┘   └────────┬────────┘
            │                     │
            ▼                     ▼
   ┌─────────────────────────────────────┐
   │  Shared host filesystem             │
   │  /data    persistent (BAMs, refs)   │
   │  /scratch ephemeral (pipeline runs) │
   └─────────────────────────────────────┘
            │                     │
            ▼                     ▼
   ┌─────────────┐         ┌─────────────┐
   │ Bioinformatics tools  │  Reference panels         │
   │ bcftools, samtools,   │  GRCh38 FASTA,            │
   │ plink2, bwa, minimap2,│  1000G + HGDP,            │
   │ DeepVariant (GPU),    │  PGS Catalog,             │
   │ Apptainer, Rye        │  pop2group mapping        │
   └─────────────┘         └─────────────┘
```

## What you can do with it

### From the main dashboard (`/`)

- **Discover & validate files** — recursive scan of BAM/CRAM/FASTQ/VCF/gVCF
  files under your data roots, with header inspection and integrity checks.
- **Convert raw data** — drag-and-drop BAM → VCF via the **nimog** sub-app:
  fast bcftools mode for single samples, or DeepVariant + GLnexus for proper
  family/joint calling. Optional GPU acceleration.
- **Compute polygenic scores** — search the
  [PGS Catalog](https://www.pgscatalog.org/), download scoring files, run
  ancestry-adjusted scoring with plink2, and view per-trait z-scores and
  percentiles against a 1000 Genomes reference distribution.
- **Run clinical-style screens** — sex verification (5 independent checks),
  carrier-status screening, monogenic disease screening, and other QC metrics
  via a checklist UI.
- **Generate AI reports** — Claude Sonnet writes a human-readable report from
  your sample's results.
- **Chat with Claude Code** — an in-browser tab forwards messages to a
  Claude Code session running in tmux, with the project's `.claude/skills/`
  files as context.

### From the ancestry app (`/ancestry/`)

- **Decompose ancestry** against the gnomAD HGDP + 1000 Genomes panel
  (~4,091 samples, ~240K LD-pruned variants, 9 continental groups including
  SoutheastAsian).
- **Detect admixture** with the [Rye](https://github.com/healthdisparities/rye)
  NNLS-on-PCA estimator (20 PCs).
- **Run ROH analysis** to detect runs of homozygosity / population bottlenecks.
- **Visualize results** — group + population breakdown, PCA scatter plot, world
  map, methodology / context descriptions for each group.
- **Compare samples** side by side, export to CSV / PNG, share results via URL.
- **Validate panel changes** with leave-one-out self-classification (built-in).
- **Stream live progress** via Server-Sent Events while the pipeline runs.

## Quick start

Both apps run on Linux (tested on Debian 12 / Ubuntu 22.04+). They expect a
conda environment with the bioinformatics toolchain and a few large reference
datasets. None of those are included in this repo.

```bash
git clone git@github.com:YOUR_USER/23andclaude.git
cd 23andclaude
cp env.example .env
$EDITOR .env   # set JWT_SECRET, ANTHROPIC_API_KEY, etc.

# Set up the genomics dashboard (main/)
cd main
./setup.sh                # auto-detects hardware, builds frontend, prepares dirs
conda activate genomics
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8600

# In another shell, set up the ancestry app
cd ../ancestry/app/frontend
npm install && npm run build
cd ../backend
pip install -r requirements.txt
python main.py            # listens on $ANCESTRY_PORT (default 8700)
```

See [`main/README.md`](./main/README.md) for the detailed setup, including:

- conda environment install
- GRCh38 reference genome download (~3 GB)
- 1000 Genomes reference panel download (~700 MB)
- DeepVariant container pull (~3–11 GB)
- Redis service start
- nginx vhost wiring

The ancestry app additionally needs the gnomAD HGDP+1kGP panel, prepared and
LD-pruned, plus a `pop2group.txt` mapping file. See
[`ancestry/README.md`](./ancestry/README.md) for details.

## Reverse proxy

A sample nginx vhost that mounts both apps behind a single domain is in
[`deploy/nginx-23andclaude.conf`](./deploy/nginx-23andclaude.conf). Drop it
into `/etc/nginx/sites-available/`, edit the certificate paths, and symlink
into `sites-enabled/`.

## Environment variables

Everything is configured via env vars. Copy [`env.example`](./env.example)
to `.env` and edit. The most important ones:

| Variable | Purpose |
|----------|---------|
| `JWT_SECRET` | Auth signing key for the main dashboard |
| `ANTHROPIC_API_KEY` | Claude API key for AI report generation |
| `GENOMICS_DATA_DIR` | Persistent storage root (default `/data`) |
| `GENOMICS_SCRATCH_DIR` | Ephemeral storage root (default `/scratch`) |
| `GENOMICS_PORT` | Main dashboard port (default `8600`) |
| `ANCESTRY_PORT` | Ancestry app port (default `8700`) |
| `APP_ROOT` | Ancestry app data root (default `/data/ancestry_app`) |
| `SAMPLE_DIRS` | Comma-separated dirs for the ancestry app to scan |
| `REDIS_URL` | Redis for the main app's PGS search cache |
| `DATABASE_URL` | SQLite or Postgres URL for the main app |

## Hardware

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 8 cores | 32+ cores |
| RAM | 16 GB | 64+ GB |
| Disk | 100 GB SSD | 2+ TB SSD (WGS data is large) |
| GPU | none | NVIDIA T4/V100/A10/A100+ (for DeepVariant) |

## Security

- Authentication is JWT (HS256) with a server-side secret. Change the default.
- The chat tab spawns Claude Code in a tmux session — only enable it on a
  trusted host with a hardened SSH boundary.
- The main app's filesystem browser restricts access to allow-listed roots
  (`/data/`, `/scratch/`). Override with `NIMOG_ALLOWED_BROWSE_ROOTS` if needed.
- Never bind the FastAPI ports to a public interface without nginx + TLS in
  front of them.

## Repository layout

```
23andclaude/
├── README.md                  # this file
├── env.example                # template for the .env file
├── .gitignore
├── deploy/
│   └── nginx-23andclaude.conf # sample nginx vhost (both apps + TLS)
├── main/                      # Genomics Dashboard (port 8600)
│   ├── README.md
│   ├── backend/               # FastAPI app
│   ├── frontend/              # React 19 + Vite SPA
│   ├── nimog/                 # BAM/CRAM → VCF sub-app
│   ├── deploy/                # nginx + supervisor snippets
│   ├── scripts/               # standalone analysis scripts
│   ├── .claude/skills/        # AI assistant instruction files
│   ├── environment.yml        # conda env definition
│   ├── requirements.txt
│   └── setup.sh               # auto-setup
└── ancestry/                  # Ancestry Inference app (port 8700)
    └── app/
        ├── backend/           # FastAPI + pipeline orchestration
        ├── frontend/          # React 19 + Vite SPA
        └── reference/         # population/group mapping files
```

## License

MIT. See [LICENSE](./LICENSE).

This project bundles or interoperates with several third-party tools and
datasets, each with their own licenses and terms of use:

- [DeepVariant](https://github.com/google/deepvariant) (BSD-3)
- [bcftools](https://github.com/samtools/bcftools), [samtools](https://github.com/samtools/samtools) (MIT)
- [plink2](https://www.cog-genomics.org/plink/2.0/) (GPL-3)
- [Rye](https://github.com/healthdisparities/rye) (MIT)
- [PGS Catalog](https://www.pgscatalog.org/) (CC-BY-4.0)
- [gnomAD HGDP + 1kGP reference panel](https://gnomad.broadinstitute.org/) (research use)

If you publish results derived from these tools or datasets, please cite them
appropriately.
