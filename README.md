# 23andClaude

Full-stack whole-genome sequencing (WGS) analysis platform with three integrated apps: a genomics testing dashboard, ancestry inference, and BAM-to-VCF conversion.

**Live at [23andclaude.com](https://23andclaude.com)**

---

## Architecture

The project runs as three independent FastAPI services behind a single Nginx reverse proxy on a GPU-equipped GCE instance:

| App | Path | Port | Description |
|-----|------|------|-------------|
| **Main Dashboard** | `/` | 8800 | Genomic testing checklist, PGS scoring, AI assistant |
| **Ancestry** | `/ancestry/` | 8710 | PCA-based ancestry inference with interactive visualization |
| **Converter** | `/convert/` | 8720 | BAM/CRAM to normalized VCF pipeline |

```
23andclaude.com
├── /              → main/app.py        (port 8800)
├── /ancestry/     → ancestry/          (port 8710)
│   ├── backend/   → FastAPI + pipeline
│   └── frontend/  → React/Vite SPA
├── /convert/      → converter/app.py   (port 8720)
└── nginx reverse proxy (TLS via Let's Encrypt)
```

## Apps

### Main Dashboard (`main/`)

A single-page genomics analysis workbench. Upload a VCF, pick tests from a categorized checklist, and run them with real-time progress tracking.

- **Test categories**: Sample QC, Sex Check, Monogenic Disease Screening, Carrier Status, Polygenic Scores (PGS), Ancestry, Pharmacogenomics
- **PGS scoring**: Downloads scoring files from the PGS Catalog, runs plink2 `--score`, computes percentiles against precomputed reference distributions
- **AI assistant**: Embedded Claude-powered chat for investigating results, running bioinformatics commands, and searching the PGS Catalog
- **Multi-user auth**: Email/password accounts with isolated per-user data directories
- **System monitoring**: Live CPU, memory, GPU stats in the status bar

Key files:
- `app.py` — FastAPI server with embedded HTML/JS frontend (~5,300 lines)
- `runners.py` — Test execution engine: variant lookups, VCF stats, PGS scoring, ClinVar screening (~4,300 lines)
- `test_registry.py` — All test definitions parsed from the master checklist
- `chat.py` — Claude-powered AI assistant with tool use (bcftools, plink2, samtools, PGS search)

### Ancestry (`ancestry/`)

PCA-based ancestry inference using a merged HGDP + 1000 Genomes reference panel.

- Supports VCF, BAM, CRAM, and 23andMe raw data input
- Projects samples onto the first 20 principal components of the reference panel
- K-nearest-neighbor classification into 10 ancestry groups (European, East Asian, African, South Asian, Middle Eastern, Ashkenazi Jewish, Finnish, Southeast Asian, American, Oceanian)
- Interactive results visualization with continental composition breakdown
- Runs of Homozygosity (ROH) analysis via plink `--homozyg`
- Multi-user auth with per-user result history

Key files:
- `app/backend/main.py` — FastAPI API server
- `app/backend/pipeline.py` — Full ancestry pipeline: VCF extraction, PCA projection, KNN classification, ROH analysis
- `app/frontend/src/App.jsx` — React SPA with results visualization
- `app/reference/signatures.yaml` — Reference population centroids and standard deviations for Mahalanobis distance classification
- `tools/rye/` — Standalone Rye ancestry decomposition tool (Python + R implementations)

### Converter (`converter/`)

BAM/CRAM to normalized VCF conversion with two pipeline modes.

- **Quick mode**: bcftools per-chromosome variant calling — fast, suitable for most analyses
- **Full mode**: DeepVariant + GLnexus joint genotyping — GPU-accelerated, best for family studies
- Configurable quality thresholds (base quality, mapping quality, read depth)
- Multi-sample batch processing with per-chromosome progress tracking
- Job persistence and resume support
- Real-time SSE progress streaming

Key files:
- `app.py` — FastAPI server with job management
- `pipeline.py` — bcftools and DeepVariant pipeline orchestration
- `static/` — Vanilla HTML/JS/CSS frontend

## External Tools

The pipeline depends on standard bioinformatics tools (expected on `$PATH`):

- [plink2](https://www.cog-genomics.org/plink/2.0/) — PGS scoring, PCA projection, data conversion
- [bcftools](https://samtools.github.io/bcftools/) — VCF manipulation, variant calling
- [samtools](http://www.htslib.org/) — BAM/CRAM processing
- [DeepVariant](https://github.com/google/deepvariant) — GPU-accelerated variant calling (optional, for full-mode conversion)

## Reference Data

The ancestry app requires a prebuilt reference panel (not included in this repo due to size):

- **HGDP + 1000 Genomes** merged panel: `reference/ref_pruned.{bed,bim,fam}`
- **gnomAD metadata**: `reference/gnomad_meta_updated.tsv`
- **Population mappings**: `reference/pop2group.txt`, `app/reference/pop2group_ea_detail.txt`
- **High-LD regions**: `reference/high_ld_regions_hg38.txt` (for LD pruning exclusion)

## Setup

```bash
# Clone
git clone https://github.com/NimoRotem/23andClaude.git
cd 23andClaude

# Python dependencies (use a conda/venv with Python 3.10+)
pip install fastapi uvicorn python-multipart aiofiles pyyaml bcrypt

# Ancestry frontend
cd ancestry/app/frontend
npm install
npm run build
cd ../../..

# Set environment variables
cp env.example .env
# Edit .env with your credentials

# Run each app (or use supervisor)
cd main && uvicorn app:app --host 0.0.0.0 --port 8800
cd ancestry/app/backend && uvicorn main:app --host 0.0.0.0 --port 8710
cd converter && uvicorn app:app --host 0.0.0.0 --port 8720
```

See `deploy/nginx-23andclaude.conf` for the production Nginx configuration.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DEFAULT_USER_USERNAME` | `admin@example.com` | Bootstrap admin email (main app) |
| `DEFAULT_USER_PASSWORD` | `changeme123456` | Bootstrap admin password (main app) |
| `SIMPLE_GENOMICS_PORT` | `8800` | Main app port |
| `SIMPLE_GENOMICS_DATA_ROOT` | (app directory) | Data storage root |
| `SIMPLE_GENOMICS_WORKERS` | `4` | Concurrent test workers |
| `APP_ROOT` | `/data/ancestry_app` | Ancestry app root (for reference data paths) |
| `PORT` | `8700` | Ancestry backend port |

## License

MIT — see [LICENSE](LICENSE) for details. Third-party tools and datasets have their own licenses.
