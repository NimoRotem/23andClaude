"""Centralized configuration for the PGS pipeline.

All paths, tool locations, population definitions, and constants that were
previously scattered across runners.py and scripts/ are consolidated here.
"""
import os

# ── Tool paths ───────────────────────────────────────────────────────
PLINK2 = os.getenv("PLINK2", "/home/nimo/miniconda3/envs/genomics/bin/plink2")
BCFTOOLS = os.getenv("BCFTOOLS", "/home/nimo/miniconda3/envs/genomics/bin/bcftools")
LIFTOVER_BIN = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "liftover", "liftOver")

# ── Data paths ───────────────────────────────────────────────────────
PGS_CACHE = os.getenv("PGS_CACHE", "/data/pgs_cache")
REF_PANEL = os.getenv("REF_PANEL", "/data/pgs2/ref_panel/GRCh38_1000G_ALL")
REF_PANEL_PSAM = REF_PANEL + ".psam"

# Legacy stats dir (EUR-only precomputed JSONs)
LEGACY_REF_PANEL_STATS = os.getenv("REF_PANEL_STATS", "/data/pgs2/ref_panel_stats")

# New multi-pop stats directory
REF_STATS_DIR = os.getenv("REF_STATS_DIR", "/data/ref_stats")

# SQLite database
DB_PATH = os.getenv("PGS_DB_PATH",
                     os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "pgs_pipeline.db"))

# ── Chain files for liftOver ─────────────────────────────────────────
CHAIN_FILES = {
    ("GRCh37", "GRCh38"): "/data/ancestry_reference/hg19ToHg38.over.chain.gz",
    ("GRCh38", "GRCh37"): os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "liftover", "hg38ToHg19.over.chain.gz"),
}

# ── plink2 resource limits ───────────────────────────────────────────
PLINK_SCORE_THREADS = int(os.getenv("PLINK_SCORE_THREADS", "1"))
PLINK_REF_THREADS = int(os.getenv("PLINK_REF_THREADS", "8"))
PLINK_MEMORY_MB = int(os.getenv("PLINK_MEMORY_MB", "16000"))

# ── Population definitions ───────────────────────────────────────────
# Each entry: label, sample_file path, minimum sample count
POP_SAMPLES_DIR = "/data/pgs2/ref_panel/pop_samples"

POPULATIONS = {
    "EUR": {
        "label": "European",
        "sample_file": os.path.join(POP_SAMPLES_DIR, "EUR.txt"),
        "min_n": 633,
    },
    "EAS": {
        "label": "East Asian",
        "sample_file": os.path.join(POP_SAMPLES_DIR, "EAS.txt"),
        "min_n": 585,
    },
    "AFR": {
        "label": "African",
        "sample_file": os.path.join(POP_SAMPLES_DIR, "AFR.txt"),
        "min_n": 893,
    },
    "SAS": {
        "label": "South Asian",
        "sample_file": os.path.join(POP_SAMPLES_DIR, "SAS.txt"),
        "min_n": 601,
    },
    "AMR": {
        "label": "Admixed American",
        "sample_file": os.path.join(POP_SAMPLES_DIR, "AMR.txt"),
        "min_n": 490,
    },
    "MIX": {
        "label": "Mixed / Global",
        "sample_file": None,  # Built dynamically by subsampling
        "min_n": 2450,
    },
    "MID": {
        "label": "Middle Eastern",
        "sample_file": None,  # Placeholder — no panel available
        "min_n": 0,
    },
}

# Populations to build ref stats for (MID excluded — no panel)
BUILDABLE_POPULATIONS = ["EUR", "EAS", "AFR", "SAS", "AMR", "MIX"]

# Populations shown as first-class in UI
UI_POPULATIONS = ["EUR", "EAS", "MIX"]

# MIX population config: balanced subsample from all 5 real pops
MIX_SEED = 42
MIX_PER_POP_N = 490  # min of AMR

# ── Helpers ──────────────────────────────────────────────────────────

def ref_stats_path(pgs_id: str, population: str, genome_build: str = "GRCh38") -> str:
    """Return the path where ref stats JSON is stored for a PGS × population."""
    return os.path.join(REF_STATS_DIR, pgs_id, f"{population}_{genome_build}.json")


def ref_scores_npy_path(pgs_id: str, population: str, genome_build: str = "GRCh38") -> str:
    """Return the path where raw score numpy array is stored."""
    return os.path.join(REF_STATS_DIR, pgs_id, f"{population}_scores.npy")
