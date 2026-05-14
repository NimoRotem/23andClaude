"""W1.4 — PGS eligibility matrix (minimal Wave 0 version).

A single-table SQLite store that captures, per PGS, the policy and
calibration metadata the gate and UI need. The advisor's P1 list calls
this out as a foundational missing piece.

Fields (per the advisor's recommendation):
    pgs_id
    trait_name
    trait_class                  e.g. "cardiometabolic", "neuro_psych", "anthropometric"
    social_risk_tier             0 = none, 1 = caveated, 2 = hidden (sensitive)
    allowed_ancestries           JSON list, e.g. ["EUR"]
    validated_ancestries         JSON list, e.g. ["EUR"]
    weight_type                  beta | log_or | log_hr
    weight_transform             identity | log_or | log_hr
    supported_input_types        JSON list, e.g. ["vcf", "gvcf", "bam", "cram"]
    percentile_eligible          bool
    min_match_rate_default       float
    requires_sex_stratification  bool
    status                       active | raw_only | hidden | retired
    catalog_n_variants           int
    effective_n_variants         int (LD-collapsed, optional)
    last_validated_at            iso8601 timestamp

The table is read by:
    - pipeline/result_gate.apply_gate (sensitive-trait policy)
    - app.py /api/pgs/{id}/eligibility endpoint (new)
    - app.py /api/tests (filter the public picker)

Population happens at install time via pipeline/eligibility_bootstrap.py
(reads PGS Catalog metadata + curated_list.md + portability_warnings).
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("pgs-pipeline.eligibility_matrix")

_DB_PATH = "/home/nimrod_rotem/simple-genomics/pgs_pipeline.db"
_LOCK = threading.Lock()


SCHEMA = """
CREATE TABLE IF NOT EXISTS pgs_eligibility (
    pgs_id                        TEXT PRIMARY KEY,
    trait_name                    TEXT,
    trait_class                   TEXT,
    social_risk_tier              INTEGER NOT NULL DEFAULT 0,
    allowed_ancestries            TEXT,    -- JSON list
    validated_ancestries          TEXT,    -- JSON list
    weight_type                   TEXT,
    weight_transform              TEXT,
    supported_input_types         TEXT,    -- JSON list
    percentile_eligible           INTEGER NOT NULL DEFAULT 1,
    min_match_rate_default        REAL,
    requires_sex_stratification   INTEGER NOT NULL DEFAULT 0,
    status                        TEXT NOT NULL DEFAULT 'active',
    catalog_n_variants            INTEGER,
    effective_n_variants          INTEGER,
    last_validated_at             TEXT,
    notes                         TEXT,
    created_at                    TEXT,
    updated_at                    TEXT
);

CREATE INDEX IF NOT EXISTS pgs_eligibility_status      ON pgs_eligibility(status);
CREATE INDEX IF NOT EXISTS pgs_eligibility_risk_tier   ON pgs_eligibility(social_risk_tier);
"""


def init_db(db_path: str = _DB_PATH) -> None:
    """Create the table if it doesn't exist. Idempotent."""
    with _LOCK:
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()


def upsert(pgs_id: str, **fields) -> None:
    """Insert or update an eligibility row."""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    fields = dict(fields)
    for k in ("allowed_ancestries", "validated_ancestries", "supported_input_types"):
        if k in fields and not isinstance(fields[k], str):
            fields[k] = json.dumps(fields[k] or [])
    fields.setdefault("updated_at", now)
    fields["pgs_id"] = pgs_id

    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            cur = conn.execute(
                "SELECT created_at FROM pgs_eligibility WHERE pgs_id = ?",
                (pgs_id,),
            )
            existing = cur.fetchone()
            if existing is None:
                fields["created_at"] = now
            cols = list(fields.keys())
            placeholders = ", ".join(["?"] * len(cols))
            updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "pgs_id")
            sql = (
                f"INSERT INTO pgs_eligibility ({', '.join(cols)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT(pgs_id) DO UPDATE SET {updates}"
            )
            conn.execute(sql, [fields[c] for c in cols])
            conn.commit()
        finally:
            conn.close()


def get(pgs_id: str) -> Optional[Dict[str, Any]]:
    """Return the eligibility row for one PGS, or None."""
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT * FROM pgs_eligibility WHERE pgs_id = ?", (pgs_id,)
            )
            row = cur.fetchone()
        finally:
            conn.close()
    if row is None:
        return None
    out = dict(row)
    for k in ("allowed_ancestries", "validated_ancestries", "supported_input_types"):
        if out.get(k):
            try:
                out[k] = json.loads(out[k])
            except (json.JSONDecodeError, TypeError):
                pass
    out["percentile_eligible"] = bool(out["percentile_eligible"])
    out["requires_sex_stratification"] = bool(out["requires_sex_stratification"])
    return out


def list_all(status: Optional[str] = None,
             social_risk_tier: Optional[int] = None) -> List[Dict[str, Any]]:
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            conn.row_factory = sqlite3.Row
            q = "SELECT * FROM pgs_eligibility WHERE 1=1"
            params: list = []
            if status:
                q += " AND status = ?"
                params.append(status)
            if social_risk_tier is not None:
                q += " AND social_risk_tier = ?"
                params.append(social_risk_tier)
            q += " ORDER BY pgs_id"
            rows = conn.execute(q, params).fetchall()
        finally:
            conn.close()
    return [dict(r) for r in rows]


def is_eligible_for(pgs_id: str, user_population: Optional[str] = None,
                    user_input_type: Optional[str] = None) -> Dict[str, Any]:
    """High-level eligibility check used by the gate and the UI.

    Returns:
        {
          "eligible": bool,
          "reason_code": str | None,
          "row": dict | None,
        }

    A missing row means the PGS hasn't been bootstrapped yet — we fall
    open (`eligible=True, row=None`) so the gate can still apply its
    structural checks (schema/match-rate/etc.). The eligibility matrix
    only TIGHTENS, never silently relaxes.
    """
    row = get(pgs_id)
    if row is None:
        return {"eligible": True, "reason_code": None, "row": None}

    if row["status"] in ("hidden", "retired"):
        return {"eligible": False,
                "reason_code": "TRAIT_HIDDEN_BY_POLICY", "row": row}
    if row["social_risk_tier"] >= 2:
        return {"eligible": False,
                "reason_code": "TRAIT_HIDDEN_BY_POLICY", "row": row}
    if user_population and row.get("allowed_ancestries"):
        if user_population not in (row["allowed_ancestries"] or []):
            return {"eligible": False,
                    "reason_code": "ELIGIBILITY_ANCESTRY_MISMATCH", "row": row}
    if user_input_type and row.get("supported_input_types"):
        if user_input_type not in (row["supported_input_types"] or []):
            return {"eligible": False,
                    "reason_code": "INPUT_UNSUPPORTED", "row": row}
    return {"eligible": True, "reason_code": None, "row": row}
