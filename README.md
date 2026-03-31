# 23andClaude

A full-stack web application for whole-genome sequencing (WGS) analysis, variant calling, and polygenic score (PGS) computation. Built for personal and family genomics studies on GRCh38 data.

## What it does

- **File Management** — Scan, upload, inspect, and validate BAM/CRAM/FASTQ/VCF/gVCF files
- **Variant Calling** — Convert BAM/CRAM to VCF via bcftools (quick) or DeepVariant (GPU-accelerated)
- **PGS Scoring** — Search the PGS Catalog, download scoring files, compute polygenic scores with ancestry adjustment
- **Real-time Monitoring** — WebSocket progress tracking, GPU utilization, system stats
- **AI Assistant** — Integrated Claude Code terminal for interactive genomics queries

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  React 19 + Vite (SPA)                              │
│  5 tabs: AI | Data & Pipeline | Score | Results | Server │
└──────────────────────┬──────────────────────────────┘
                       │ nginx reverse proxy
┌──────────────────────▼──────────────────────────────┐
│  FastAPI backend (:8600)                             │
│  ├─ /api/*          REST endpoints (40+)            │
│  ├─ /nimog/         BAM→VCF converter (sub-app)     │
│  └─ /genomics/      SPA static files                │
├──────────────────────────────────────────────────────┤
│  Redis (:6379)      PGS search cache + pub/sub       │
│  SQLite             Users, VCFs, runs, results        │
├──────────────────────────────────────────────────────┤
│  Bioinformatics     bcftools, samtools, plink2,       │
│  Tools (conda)      bwa, minimap2, DeepVariant        │
└──────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Linux (tested on Debian 12, Ubuntu 22.04+)
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Mamba](https://mamba.readthedocs.io/)
- [Node.js](https://nodejs.org/) >= 18
- [Redis](https://redis.io/) >= 7.0
- [Apptainer](https://apptainer.org/) (for DeepVariant — optional if using bcftools only)
- NVIDIA GPU + drivers (optional, for GPU-accelerated DeepVariant)

### 1. Clone and run auto-setup

```bash
git clone https://github.com/NimoRotem/23andClaude.git
cd 23andClaude

# Auto-setup: detects hardware, installs conda env, downloads reference data
# Creates /data and /scratch directories, builds frontend
./setup.sh
```

This will:
- Detect CPU count, RAM, GPU (if present)
- Create/update the `genomics` conda environment with all bioinformatics tools
- Build the React frontend
- Download GRCh38 reference genome (~3.1 GB)
- Download 1000 Genomes reference panel (~700 MB)
- Download DeepVariant containers (CPU: ~2.8 GB; GPU: ~11 GB if GPU detected)
- Write a server config with optimal thread counts for your hardware

Alternatively, you can also run setup from the web UI — the AI Assistant tab will detect missing components and offer to run the setup automatically.

### Manual Setup (alternative to setup.sh)

<details>
<summary>Click to expand manual setup steps</summary>

#### 2a. Create conda environment

```bash
conda env create -f environment.yml
conda activate genomics
```

#### 2b. Build the frontend

```bash
cd frontend
npm install
npm run build
cd ..
```

### 3. Set up data directories

```bash
# Create required directories
sudo mkdir -p /data/{aligned_bams,vcfs,pgs_cache,runs,app,uploads,refs,pgen_cache}
sudo mkdir -p /scratch/{nimog_output,runs,pipeline,alignments,tmp,vcfs}
sudo chown -R $USER:$USER /data /scratch
```

### 4. Download reference genome (GRCh38)

```bash
# Download GRCh38 reference FASTA
cd /data/refs
wget https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz
gunzip GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz
mv GCA_000001405.15_GRCh38_no_alt_analysis_set.fna GRCh38.fa

# Index for samtools, bwa
samtools faidx GRCh38.fa
bwa index GRCh38.fa
```

### 5. Download 1000 Genomes reference panel (for PGS ancestry adjustment)

```bash
mkdir -p /data/pgs2/ref_panel
cd /data/pgs2/ref_panel

# GRCh38 panel (plink2 format) — from pgsc_calc resources
wget https://ftp.ebi.ac.uk/pub/databases/spot/pgs/resources/pgsc_1000G_v1/GRCh38/pgsc_1000G_v1_ALL_GRCh38_no_dups.pgen
wget https://ftp.ebi.ac.uk/pub/databases/spot/pgs/resources/pgsc_1000G_v1/GRCh38/pgsc_1000G_v1_ALL_GRCh38_no_dups.pvar.zst
wget https://ftp.ebi.ac.uk/pub/databases/spot/pgs/resources/pgsc_1000G_v1/GRCh38/pgsc_1000G_v1_ALL_GRCh38_no_dups.psam
# Rename to expected names
for f in pgsc_1000G_v1_ALL_GRCh38_no_dups.*; do
  mv "$f" "GRCh38_1000G_ALL.${f#*.}"
done
```

### 6. (Optional) Download DeepVariant container

Only needed if you want GPU-accelerated variant calling. bcftools mode works without it.

```bash
mkdir -p /data/containers

# GPU version (requires NVIDIA GPU + drivers)
apptainer pull /data/containers/deepvariant_1.6.1-gpu.sif docker://google/deepvariant:1.6.1-gpu

# CPU-only version
apptainer pull /data/containers/deepvariant.sif docker://google/deepvariant:1.6.1
```

#### 7. Configure

Paths are auto-detected, but you can override via environment variables:

```bash
export GENOMICS_DATA_DIR=/data             # Persistent storage root
export GENOMICS_SCRATCH_DIR=/scratch       # Fast ephemeral storage root
export JWT_SECRET="your-secure-random-string"
export REDIS_URL="redis://localhost:6379/0"  # default
```

</details>

### 2. Start services

```bash
# Start Redis
sudo systemctl start redis-server
# or: redis-server --daemonize yes

# Start the backend (serves both API and frontend)
conda activate genomics
cd /path/to/genomics
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8600
```

Open `http://localhost:8600/genomics/` in your browser.

Default login: `admin@genomics.local` / `changeme` (set via `GENOMICS_ADMIN_PASSWORD` env var).

## Production Deployment

### With nginx (recommended)

Copy and adapt the nginx config:

```bash
sudo cp deploy/nginx-genomics.conf /etc/nginx/sites-available/genomics
# Edit paths to match your installation
sudo ln -s /etc/nginx/sites-available/genomics /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

### With supervisor (process management)

```bash
sudo cp deploy/supervisor-genomics.conf /etc/supervisor/conf.d/genomics.conf
# Edit paths and user in the config
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start genomics-app
```

## Connecting Claude Code

The dashboard includes a built-in AI assistant tab that interfaces with [Claude Code](https://docs.anthropic.com/en/docs/claude-code). To set it up:

### 1. Install Claude Code

```bash
npm install -g @anthropic-ai/claude-code
```

### 2. Authenticate

```bash
claude login
```

### 3. Start Claude Code in a tmux session

The app expects Claude Code running in a tmux session named `genomics-claude`:

```bash
tmux new-session -d -s genomics-claude -c /path/to/genomics
tmux send-keys -t genomics-claude 'claude' Enter
```

### 4. Skill files

The `.claude/skills/` directory contains domain-specific instruction files that Claude reads for context about your server, data locations, and pipelines. These are editable from the AI Assistant tab in the web UI.

The `GENOMICS_CLAUDE.md` file in the project root serves as the main Claude Code instruction file (`CLAUDE.md` equivalent).

### 5. Using the AI tab

Once Claude Code is running in the tmux session, the AI Assistant tab provides:
- **Chat** — Send messages to Claude about your genomic data
- **Terminal** — View raw Claude Code terminal output
- **Skills** — Edit/create instruction files that guide Claude's behavior

## Project Structure

```
genomics/
├── backend/                    # FastAPI application
│   ├── main.py                 # App entry point, router mounts
│   ├── config.py               # All paths, settings, tool locations
│   ├── database.py             # SQLAlchemy setup
│   ├── api/                    # Route handlers
│   │   ├── auth.py             # JWT authentication
│   │   ├── files.py            # File scan/upload/validate/convert
│   │   ├── vcfs.py             # VCF registration and QC
│   │   ├── pgs.py              # PGS Catalog search and download
│   │   ├── runs.py             # Scoring run CRUD + WebSocket
│   │   ├── storage.py          # Disk usage management
│   │   ├── chat.py             # Claude Code tmux integration
│   │   └── system.py           # System monitoring
│   ├── models/schemas.py       # ORM models (User, VCF, Run, etc.)
│   ├── services/pgs_client.py  # PGS Catalog REST client
│   └── scoring/                # PGS scoring engines
│       ├── engine.py           # Main orchestrator
│       ├── fast_pipeline.py    # plink2-based scoring
│       ├── plink2_scorer.py    # plink2 wrapper + ancestry
│       └── plink2_convert.py   # VCF to pgen conversion
├── frontend/                   # React 19 + Vite SPA
│   ├── src/
│   │   ├── App.jsx             # Main layout, 5-tab navigation
│   │   ├── context.jsx         # Global state (useReducer)
│   │   ├── api.js              # Centralized API client
│   │   └── components/         # UI panels
│   │       ├── RawDataPanel.jsx    # Files + pipeline (merged)
│   │       ├── ScorePanel.jsx      # PGS scoring config
│   │       ├── ResultsPanel.jsx    # Score visualization
│   │       ├── ChatPanel.jsx       # AI assistant
│   │       └── ServerPanel.jsx     # System monitoring
│   └── package.json
├── nimog/                      # BAM/CRAM → VCF converter
│   ├── app.py                  # FastAPI sub-app
│   ├── pipeline.py             # bcftools + DeepVariant pipelines
│   └── static/                 # Converter web UI
├── deploy/                     # Production configs
│   ├── nginx-genomics.conf
│   └── supervisor-genomics.conf
├── scripts/
│   └── precompute_ref_stats.py # Pre-compute reference panel stats
├── .claude/skills/             # AI assistant instruction files
├── GENOMICS_CLAUDE.md          # Claude Code project instructions
├── environment.yml             # Conda environment definition
├── requirements.txt            # Python pip dependencies
└── README.md
```

## API Overview

All endpoints are under `http://localhost:8600/api/` (or `/genomics/api/` behind nginx).

| Endpoint Group | Description |
|---------------|-------------|
| `POST /api/auth/login` | JWT authentication |
| `GET /api/files/scan` | Discover BAM/CRAM/FASTQ/VCF files on disk |
| `POST /api/files/convert` | FASTQ → BAM alignment |
| `GET /api/pgs/search?q=...` | Search PGS Catalog by trait/ID |
| `POST /api/pgs/download` | Download scoring files |
| `POST /api/runs` | Create a PGS scoring run |
| `WS /api/runs/{id}/progress` | Real-time scoring progress |
| `GET /api/runs/{id}/results` | Scoring results (Z-scores, percentiles) |
| `GET /api/system/stats` | CPU, RAM, GPU, disk metrics |
| `POST /nimog/api/convert` | Start BAM/CRAM → VCF conversion |
| `GET /nimog/api/jobs/{id}/stream` | SSE pipeline progress |

See `.claude/skills/api-reference.md` for the full endpoint reference.

## Supported File Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| BAM | `.bam` + `.bai` | Binary alignment (reads mapped to reference) |
| CRAM | `.cram` + `.crai` | Compressed alignment (smaller than BAM) |
| FASTQ | `.fastq.gz`, `.fq.gz` | Raw sequencing reads (paired R1/R2) |
| VCF | `.vcf.gz` + `.tbi` | Variant calls (SNPs, indels) |
| gVCF | `.g.vcf.gz` | Genomic VCF (variants + reference blocks, best for PGS) |

## Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 8 cores | 32+ cores |
| RAM | 16 GB | 64+ GB |
| Storage | 100 GB SSD | 2+ TB SSD (WGS data is large) |
| GPU | None (bcftools mode) | NVIDIA T4/V100+ (for DeepVariant) |

## External Resources

- [PGS Catalog](https://www.pgscatalog.org/) — Polygenic score database
- [DeepVariant](https://github.com/google/deepvariant) — Deep learning variant caller
- [plink2](https://www.cog-genomics.org/plink/2.0/) — Genotype analysis toolkit
- [bcftools](https://samtools.github.io/bcftools/) — VCF/BCF manipulation
- [samtools](http://www.htslib.org/) — SAM/BAM/CRAM tools
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) — AI coding assistant

## License

MIT
