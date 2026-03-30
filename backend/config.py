"""Application configuration — auto-detects tools, hardware, and paths."""

import json
import os
import shutil
import subprocess
from pathlib import Path

# --- Paths (override via env vars) ---
DATA_DIR = Path(os.getenv("GENOMICS_DATA_DIR", "/data"))
SCRATCH_DIR = Path(os.getenv("GENOMICS_SCRATCH_DIR", "/scratch"))

# Persistent storage
BAMS_DIR = DATA_DIR / "bams"
REFS_DIR = DATA_DIR / "refs"
VCFS_DIR = DATA_DIR / "vcfs"
PGS_CACHE_DIR = DATA_DIR / "pgs_cache"
RUNS_DIR = DATA_DIR / "runs"
REF_PANELS_DIR = DATA_DIR / "ref_panels"
APP_DIR = DATA_DIR / "app"

# Genomic file scan directories
ALIGNED_BAMS_DIR = DATA_DIR / "aligned_bams"
FASTQ_DIRS = [DATA_DIR / "organized" / "raw_data" / "fastq"]
NIMOG_OUTPUT_DIR = SCRATCH_DIR / "nimog_output"
UPLOADS_DIR = DATA_DIR / "uploads"

# Fast storage (ephemeral)
SCRATCH_BAMS = SCRATCH_DIR / "bams"
SCRATCH_REFS = SCRATCH_DIR / "refs"
SCRATCH_VCFS = SCRATCH_DIR / "vcfs"
SCRATCH_PGS_CACHE = SCRATCH_DIR / "pgs_cache"
SCRATCH_PIPELINE = SCRATCH_DIR / "pipeline"
SCRATCH_RUNS = SCRATCH_DIR / "runs"
SCRATCH_TMP = SCRATCH_DIR / "tmp"

# --- Database ---
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{APP_DIR}/db.sqlite")

# --- Auth ---
JWT_SECRET = os.getenv("JWT_SECRET", "genomics-app-secret-change-me-in-prod")
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours
JWT_REFRESH_TOKEN_EXPIRE_DAYS = 30

# --- Redis ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PGS_SEARCH_CACHE_TTL = 60 * 60 * 24  # 24 hours

# --- PGS Catalog ---
PGS_CATALOG_API = "https://www.pgscatalog.org/rest"

# --- AI Report Generation ---
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
AI_REPORT_MODEL = os.getenv("AI_REPORT_MODEL", "claude-sonnet-4-20250514")


# ── Hardware Detection ─────────────────────────────────────────────

def _detect_gpu() -> dict:
    """Detect NVIDIA GPU availability and details."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            parts = [p.strip() for p in result.stdout.strip().split(",")]
            return {
                "available": True,
                "name": parts[0] if len(parts) >= 1 else "Unknown",
                "memory_mb": int(parts[1]) if len(parts) >= 2 else 0,
            }
    except Exception:
        pass
    return {"available": False, "name": "none", "memory_mb": 0}


def _cpu_count() -> int:
    return os.cpu_count() or 4


def _ram_gb() -> int:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    return int(line.split()[1]) // 1024 // 1024
    except Exception:
        pass
    return 8


GPU_INFO = _detect_gpu()
GPU_AVAILABLE = GPU_INFO["available"]
GPU_NAME = GPU_INFO["name"]
GPU_MEMORY_MB = GPU_INFO["memory_mb"]
CPU_COUNT = _cpu_count()
RAM_GB = _ram_gb()

# Optimal thread counts based on CPU
if CPU_COUNT >= 32:
    DV_SHARDS = 20
    ALIGN_THREADS = 24
elif CPU_COUNT >= 16:
    DV_SHARDS = 12
    ALIGN_THREADS = 12
elif CPU_COUNT >= 8:
    DV_SHARDS = 8
    ALIGN_THREADS = 6
else:
    DV_SHARDS = 4
    ALIGN_THREADS = 3


# ── Tool Detection ─────────────────────────────────────────────────

def _find_tool(name: str, env_var: str = None) -> str:
    """Find a tool binary. Priority: env var > conda env > PATH > common paths."""
    # 1. Explicit env var
    if env_var:
        val = os.getenv(env_var)
        if val and os.path.isfile(val):
            return val

    # 2. Current PATH (works if conda env is activated)
    found = shutil.which(name)
    if found:
        return found

    # 3. Search common conda locations
    for base in [
        Path.home() / "miniconda3" / "envs" / "genomics" / "bin",
        Path.home() / "miniforge3" / "envs" / "genomics" / "bin",
        Path.home() / "mambaforge" / "envs" / "genomics" / "bin",
        Path("/opt/conda/envs/genomics/bin"),
    ]:
        candidate = base / name
        if candidate.is_file():
            return str(candidate)

    # 4. System paths
    for p in [f"/usr/bin/{name}", f"/usr/local/bin/{name}"]:
        if os.path.isfile(p):
            return p

    return name  # Fallback: bare name (will fail at runtime with clear error)


BCFTOOLS = _find_tool("bcftools", "GENOMICS_BCFTOOLS")
SAMTOOLS = _find_tool("samtools", "GENOMICS_SAMTOOLS")
PLINK2 = _find_tool("plink2", "GENOMICS_PLINK2")
BWA = _find_tool("bwa", "GENOMICS_BWA")
MINIMAP2 = _find_tool("minimap2", "GENOMICS_MINIMAP2")
SINGULARITY = _find_tool("apptainer") or _find_tool("singularity")
ALIGNMENTS_DIR = SCRATCH_DIR / "alignments"


# ── Reference Genome Detection ─────────────────────────────────────

def _find_reference(build: str = "GRCh38") -> str:
    """Find reference FASTA. Checks multiple common locations."""
    candidates = [
        DATA_DIR / "refs" / f"{build}.fa",
        DATA_DIR / "refs" / f"{build}.fasta",
        DATA_DIR / "reference" / "GRCh38.fasta",  # Legacy path
        Path.home() / "reference" / f"{build}.fa",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return str(DATA_DIR / "refs" / f"{build}.fa")  # Default (may not exist yet)


DEFAULT_REFERENCE_GRCH38 = _find_reference("GRCh38")
DEFAULT_REFERENCE_GRCH37 = _find_reference("GRCh37")
EXISTING_REFERENCE = DEFAULT_REFERENCE_GRCH38

# Reference panels
REF_PANEL_1KG_GRCH38 = str(DATA_DIR / "pgs2" / "ref_panel" / "GRCh38_1000G_ALL")
REF_PANEL_1KG_GRCH37 = str(DATA_DIR / "pgs2" / "ref_panel" / "GRCh37_1000G_ALL")

# --- Server ---
APP_HOST = "0.0.0.0"
APP_PORT = int(os.getenv("GENOMICS_PORT", "8600"))

# --- plink2 Fast Pipeline ---
PGEN_CACHE_DIR = DATA_DIR / "pgen_cache"
PLINK2_SCORING_DIR = DATA_DIR / "pgs2" / "plink2_scoring_files"
REF_PANEL_STATS_DIR = DATA_DIR / "pgs2" / "ref_panel_stats"
POP_SAMPLE_DIR = DATA_DIR / "pgs2" / "ref_panel" / "pop_samples"
CONTAINERS_DIR = DATA_DIR / "containers"

# DeepVariant containers
DV_CPU_IMAGE = str(CONTAINERS_DIR / "deepvariant_1.6.1.sif")
DV_GPU_IMAGE = str(CONTAINERS_DIR / "deepvariant_1.6.1-gpu.sif")


# ── Setup Status ───────────────────────────────────────────────────

def get_setup_status() -> dict:
    """Check what's installed and what's missing."""
    checks = {}

    # Reference genome
    ref = Path(DEFAULT_REFERENCE_GRCH38)
    checks["reference_genome"] = {
        "installed": ref.exists(),
        "path": str(ref),
        "indexed": (ref.parent / (ref.name + ".fai")).exists() if ref.exists() else False,
        "bwa_indexed": (ref.parent / (ref.name + ".bwt")).exists() if ref.exists() else False,
    }

    # Reference panel
    panel_pgen = Path(REF_PANEL_1KG_GRCH38 + ".pgen")
    checks["ref_panel"] = {
        "installed": panel_pgen.exists(),
        "path": REF_PANEL_1KG_GRCH38,
    }

    # Containers
    checks["dv_cpu_container"] = {
        "installed": os.path.isfile(DV_CPU_IMAGE),
        "path": DV_CPU_IMAGE,
    }
    checks["dv_gpu_container"] = {
        "installed": os.path.isfile(DV_GPU_IMAGE),
        "path": DV_GPU_IMAGE,
        "relevant": GPU_AVAILABLE,
    }

    # Tools
    for name, path in [("bcftools", BCFTOOLS), ("samtools", SAMTOOLS),
                        ("plink2", PLINK2), ("bwa", BWA), ("minimap2", MINIMAP2)]:
        checks[name] = {"installed": os.path.isfile(path), "path": path}

    checks["apptainer"] = {
        "installed": os.path.isfile(SINGULARITY),
        "path": SINGULARITY,
    }

    # Redis
    try:
        import redis as _redis
        r = _redis.from_url(REDIS_URL, socket_timeout=2)
        r.ping()
        checks["redis"] = {"installed": True, "url": REDIS_URL}
    except Exception:
        checks["redis"] = {"installed": False, "url": REDIS_URL}

    # Frontend
    frontend_dist = Path(__file__).parent.parent / "frontend" / "dist" / "index.html"
    checks["frontend"] = {"installed": frontend_dist.exists()}

    # Overall
    critical = ["reference_genome", "bcftools", "samtools", "plink2", "frontend"]
    checks["setup_complete"] = all(checks[k].get("installed", False) for k in critical)

    # Server config from setup.sh
    server_config = APP_DIR / "server_config.json"
    if server_config.exists():
        try:
            with open(server_config) as f:
                checks["server_config"] = json.load(f)
        except Exception:
            pass

    return checks
