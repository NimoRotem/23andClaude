"""Read-side helper for the ref-stats registry.

The authoritative registry lives at /data/pgs2/ref_panel_stats/registry.json,
maintained by scripts/ref_stats_registry.py (rebuild/bless). This module gives
the live pipeline a small, dependency-free way to consult it.

Caches the parsed registry by mtime so reads are cheap.
"""
from __future__ import annotations

import json
import os
import threading
from typing import Optional

from .config import LEGACY_REF_PANEL_STATS

REGISTRY_PATH = os.path.join(LEGACY_REF_PANEL_STATS, "registry.json")

_cache_lock = threading.Lock()
_cache: dict = {"mtime": None, "data": None}


def _load() -> dict:
    try:
        st = os.stat(REGISTRY_PATH)
    except OSError:
        return {"entries": []}
    with _cache_lock:
        if _cache["mtime"] == st.st_mtime and _cache["data"] is not None:
            return _cache["data"]
        try:
            with open(REGISTRY_PATH) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            data = {"entries": []}
        _cache["mtime"] = st.st_mtime
        _cache["data"] = data
        return data


def resolve(pgs_id: str, population: str,
            genome_build: str = "GRCh38",
            scoring_method: str = "plink2-nomi") -> Optional[str]:
    """Return absolute path of the canonical stats file, or None."""
    reg = _load()
    for e in reg.get("entries", []):
        if (e.get("pgs_id") == pgs_id
                and e.get("population") == population
                and e.get("genome_build") == genome_build
                and e.get("scoring_method") == scoring_method):
            return os.path.join(LEGACY_REF_PANEL_STATS, e["filename"])
    return None


def entry(pgs_id: str, population: str,
          genome_build: str = "GRCh38",
          scoring_method: str = "plink2-nomi") -> Optional[dict]:
    """Return the full registry entry dict, or None."""
    reg = _load()
    for e in reg.get("entries", []):
        if (e.get("pgs_id") == pgs_id
                and e.get("population") == population
                and e.get("genome_build") == genome_build
                and e.get("scoring_method") == scoring_method):
            return e
    return None
