"""SQLite database for PGS pipeline metadata and results.

Schema:
  - pgs_catalog_scores: Ingested PGS entries (from ingest_pgs)
  - pgs_reference_stats: Per-population reference distributions
  - sample_pgs_results: Individual sample scoring results

WAL mode + 30s timeout for concurrent access from 12 worker threads.
"""
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

from .config import DB_PATH, REF_STATS_DIR

logger = logging.getLogger("pgs-pipeline")

SCHEMA_VERSION = 1

SCHEMA_SQL = """
-- PGS catalog entries (from ingestion)
CREATE TABLE IF NOT EXISTS pgs_catalog_scores (
    pgs_id TEXT PRIMARY KEY,
    trait_name TEXT,
    genome_build TEXT,
    weight_type TEXT,
    variant_count INTEGER,
    citation TEXT,
    eligibility_status TEXT,
    ingested_at TEXT DEFAULT (datetime('now')),
    metadata_json TEXT
);

-- Per-population reference stats
CREATE TABLE IF NOT EXISTS pgs_reference_stats (
    pgs_id TEXT NOT NULL,
    population TEXT NOT NULL,
    genome_build TEXT NOT NULL DEFAULT 'GRCh38',
    n_samples INTEGER,
    mean REAL,
    std REAL,
    quantiles_json TEXT,
    match_rate_mean REAL,
    match_rate_min REAL,
    stats_file_path TEXT,
    scores_npy_path TEXT,
    built_at TEXT DEFAULT (datetime('now')),
    UNIQUE(pgs_id, population, genome_build)
);

-- Sample scoring results
CREATE TABLE IF NOT EXISTS sample_pgs_results (
    task_id TEXT PRIMARY KEY,
    pgs_id TEXT,
    sample_id TEXT,
    file_id TEXT,
    username TEXT,
    raw_score REAL,
    percentile REAL,
    selected_ref TEXT,
    match_rate REAL,
    ancestry_model TEXT,
    computed_at TEXT DEFAULT (datetime('now'))
);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""


def _get_connection(db_path: str = None) -> sqlite3.Connection:
    """Create a new SQLite connection with WAL mode and 30s timeout."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db(db_path: str = None):
    """Context manager for database connections."""
    conn = _get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str = None):
    """Initialize the database schema. Idempotent."""
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    with get_db(path) as conn:
        conn.executescript(SCHEMA_SQL)
        # Check/set schema version
        cur = conn.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
        logger.info(f"PGS pipeline DB initialized at {path}")


# ── CRUD: pgs_catalog_scores ─────────────────────────────────────────

def upsert_pgs_catalog_score(pgs_id: str, trait_name: str = None,
                             genome_build: str = None, weight_type: str = None,
                             variant_count: int = None, citation: str = None,
                             eligibility_status: str = None,
                             metadata: Dict = None, db_path: str = None):
    """Insert or update a PGS catalog entry."""
    with get_db(db_path) as conn:
        conn.execute("""
            INSERT INTO pgs_catalog_scores
                (pgs_id, trait_name, genome_build, weight_type, variant_count,
                 citation, eligibility_status, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pgs_id) DO UPDATE SET
                trait_name = COALESCE(excluded.trait_name, trait_name),
                genome_build = COALESCE(excluded.genome_build, genome_build),
                weight_type = COALESCE(excluded.weight_type, weight_type),
                variant_count = COALESCE(excluded.variant_count, variant_count),
                citation = COALESCE(excluded.citation, citation),
                eligibility_status = COALESCE(excluded.eligibility_status, eligibility_status),
                metadata_json = COALESCE(excluded.metadata_json, metadata_json),
                ingested_at = datetime('now')
        """, (pgs_id, trait_name, genome_build, weight_type, variant_count,
              citation, eligibility_status,
              json.dumps(metadata) if metadata else None))


# ── CRUD: pgs_reference_stats ────────────────────────────────────────

def upsert_ref_stats(pgs_id: str, population: str, genome_build: str = "GRCh38",
                     n_samples: int = None, mean: float = None, std: float = None,
                     quantiles: Dict = None, match_rate_mean: float = None,
                     match_rate_min: float = None, stats_file_path: str = None,
                     scores_npy_path: str = None, db_path: str = None):
    """Insert or update reference stats for a PGS × population."""
    with get_db(db_path) as conn:
        conn.execute("""
            INSERT INTO pgs_reference_stats
                (pgs_id, population, genome_build, n_samples, mean, std,
                 quantiles_json, match_rate_mean, match_rate_min,
                 stats_file_path, scores_npy_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pgs_id, population, genome_build) DO UPDATE SET
                n_samples = excluded.n_samples,
                mean = excluded.mean,
                std = excluded.std,
                quantiles_json = excluded.quantiles_json,
                match_rate_mean = excluded.match_rate_mean,
                match_rate_min = excluded.match_rate_min,
                stats_file_path = excluded.stats_file_path,
                scores_npy_path = excluded.scores_npy_path,
                built_at = datetime('now')
        """, (pgs_id, population, genome_build, n_samples, mean, std,
              json.dumps(quantiles) if quantiles else None,
              match_rate_mean, match_rate_min,
              stats_file_path, scores_npy_path))


def get_ref_stats(pgs_id: str, population: str, genome_build: str = "GRCh38",
                  db_path: str = None) -> Optional[Dict]:
    """Load reference stats for a PGS × population. Returns dict or None."""
    with get_db(db_path) as conn:
        cur = conn.execute("""
            SELECT * FROM pgs_reference_stats
            WHERE pgs_id = ? AND population = ? AND genome_build = ?
        """, (pgs_id, population, genome_build))
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("quantiles_json"):
            d["quantiles"] = json.loads(d["quantiles_json"])
        return d


def get_available_refs(pgs_id: str, genome_build: str = "GRCh38",
                       db_path: str = None) -> List[Dict]:
    """Return refs where stats file actually exists on disk (not just in DB).

    Each entry: {population, n_samples, mean, std, stats_file_path}
    """
    with get_db(db_path) as conn:
        cur = conn.execute("""
            SELECT population, n_samples, mean, std, stats_file_path
            FROM pgs_reference_stats
            WHERE pgs_id = ? AND genome_build = ?
            ORDER BY population
        """, (pgs_id, genome_build))
        results = []
        for row in cur.fetchall():
            d = dict(row)
            # Verify stats file exists on disk
            stats_path = d.get("stats_file_path", "")
            if stats_path and os.path.exists(stats_path):
                results.append(d)
            else:
                # Also check the canonical path
                from .config import ref_stats_path
                canonical = ref_stats_path(pgs_id, d["population"], genome_build)
                if os.path.exists(canonical):
                    d["stats_file_path"] = canonical
                    results.append(d)
        return results


# ── CRUD: sample_pgs_results ─────────────────────────────────────────

def insert_sample_result(task_id: str, pgs_id: str, sample_id: str = None,
                         file_id: str = None, username: str = None,
                         raw_score: float = None, percentile: float = None,
                         selected_ref: str = None, match_rate: float = None,
                         ancestry_model: str = None, db_path: str = None):
    """Record a sample scoring result."""
    with get_db(db_path) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO sample_pgs_results
                (task_id, pgs_id, sample_id, file_id, username,
                 raw_score, percentile, selected_ref, match_rate, ancestry_model)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (task_id, pgs_id, sample_id, file_id, username,
              raw_score, percentile, selected_ref, match_rate, ancestry_model))


# ── Query helpers ────────────────────────────────────────────────────

def get_all_ingested_pgs(db_path: str = None) -> List[str]:
    """Return all PGS IDs that have been ingested."""
    with get_db(db_path) as conn:
        cur = conn.execute("SELECT pgs_id FROM pgs_catalog_scores ORDER BY pgs_id")
        return [row["pgs_id"] for row in cur.fetchall()]


def get_stats_coverage(db_path: str = None) -> Dict[str, List[str]]:
    """Return {pgs_id: [populations]} for all ref stats in DB."""
    with get_db(db_path) as conn:
        cur = conn.execute("""
            SELECT pgs_id, GROUP_CONCAT(population) as pops
            FROM pgs_reference_stats
            GROUP BY pgs_id
            ORDER BY pgs_id
        """)
        return {row["pgs_id"]: row["pops"].split(",") for row in cur.fetchall()}
