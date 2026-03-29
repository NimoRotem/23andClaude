# Server & Environment

## Machine Detection

This server's hardware is auto-detected. To check current specs, run:
```bash
# CPU and RAM
nproc && grep MemTotal /proc/meminfo

# GPU (if available)
nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null || echo "No GPU"

# Storage
df -h /data /scratch 2>/dev/null
```

Or check the API: `curl -s http://localhost:8600/api/system/capabilities | python3 -m json.tool`

## Bioinformatics Tools

All tools are in the `genomics` conda env:
```bash
conda activate genomics
```

Tools are auto-detected from PATH or common conda locations. Check with:
```bash
which bcftools samtools plink2 bwa minimap2 apptainer
```

| Tool | Purpose |
|------|---------|
| bcftools | VCF manipulation, variant calling (quick mode) |
| samtools | BAM/CRAM indexing, stats, quickcheck |
| plink2 | PGS scoring, ancestry estimation, format conversion |
| bwa | Short-read alignment (FASTQ → BAM) |
| minimap2 | Long-read alignment (FASTQ → BAM) |
| apptainer | Container runtime for DeepVariant (replaces Singularity) |

## Containers

| Image | Path | When needed |
|-------|------|-------------|
| DeepVariant CPU | `/data/containers/deepvariant_1.6.1.sif` | BAM→VCF without GPU |
| DeepVariant GPU | `/data/containers/deepvariant_1.6.1-gpu.sif` | BAM→VCF with GPU acceleration |

Use `apptainer` (not `singularity`). For GPU: add `--nv` flag.
GPU container is only downloaded during setup if a GPU is detected.

## Services

| Service | Port | Purpose |
|---------|------|---------|
| Genomics App (FastAPI) | 8600 | Main backend API + nimog at `/nimog/` |
| Redis | 6379 | PGS search caching and WebSocket pub/sub |

nimog is embedded in the main app (not a separate service).

## Key Configuration

`backend/config.py` auto-detects:
- Tool paths (searches conda env, then PATH, then common locations)
- GPU availability (via nvidia-smi)
- CPU core count (for optimal threading/sharding)
- Reference genome location (checks multiple common paths)

Override any path via environment variables:
```bash
GENOMICS_DATA_DIR=/data        # Persistent storage root
GENOMICS_SCRATCH_DIR=/scratch  # Fast ephemeral storage
GENOMICS_BCFTOOLS=/path/to/bcftools  # Override tool path
GENOMICS_PORT=8600             # Server port
JWT_SECRET=your-secret         # Auth secret
REDIS_URL=redis://host:6379/0  # Redis connection
```

## Working Directory

The project root is wherever you cloned the repository.
All file operations use `/data/` and `/scratch/` (configurable via env vars).
