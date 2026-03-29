"""PGS Catalog search, download, and cache management API endpoints."""

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.config import PGS_CACHE_DIR
from backend.database import get_db
from backend.models.schemas import PGSCacheEntry
from backend.services.pgs_client import pgs_client

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class DownloadRequest(BaseModel):
    pgs_ids: list[str]
    build: str = "GRCh38"


class DownloadResultItem(BaseModel):
    pgs_id: str
    build: str
    file_path: str | None = None
    file_size_bytes: int = 0
    error: str | None = None


class DownloadResponse(BaseModel):
    results: list[DownloadResultItem]
    succeeded: int
    failed: int


class CacheEntryOut(BaseModel):
    pgs_id: str
    trait_reported: str | None = None
    trait_efo: list[Any] = []
    variants_number: int = 0
    weight_type: str | None = None
    publication_info: dict[str, Any] = {}
    ancestry_gwas: dict[str, Any] = {}
    ancestry_eval: dict[str, Any] = {}
    method_name: str | None = None
    catalog_release_date: str | None = None
    builds_available: list[str] = []
    file_path_grch37: str | None = None
    file_path_grch38: str | None = None
    metadata_json_path: str | None = None
    downloaded_at: str | None = None
    file_size_bytes: int = 0

    class Config:
        from_attributes = True


class DownloadStatusResponse(BaseModel):
    pgs_id: str
    cached: bool
    builds: dict[str, bool]
    file_sizes: dict[str, int]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/search")
async def search_pgs(
    q: str = Query(..., min_length=1, description="Search term"),
    limit: int = Query(20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """Unified search across the PGS Catalog.

    Auto-detects PGS IDs, PGP IDs, EFO codes, and free text.
    """
    try:
        results = await pgs_client.search(q, limit=limit)
        return results
    except Exception as exc:
        logger.error("PGS search error: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"PGS Catalog API error: {exc}")


@router.get("/autocomplete")
async def autocomplete_pgs(
    q: str = Query(..., min_length=1, description="Autocomplete prefix"),
    limit: int = Query(8, ge=1, le=50),
) -> dict[str, list[dict]]:
    """Fast autocomplete suggestions grouped by traits and scores."""
    try:
        return await pgs_client.autocomplete(q, limit=limit)
    except Exception as exc:
        logger.error("PGS autocomplete error: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"PGS Catalog API error: {exc}")


@router.get("/cache")
def list_cache(db: Session = Depends(get_db)) -> list[CacheEntryOut]:
    """List all locally cached scoring files."""
    entries = db.query(PGSCacheEntry).order_by(PGSCacheEntry.downloaded_at.desc()).all()
    results = []
    for entry in entries:
        results.append(CacheEntryOut(
            pgs_id=entry.pgs_id,
            trait_reported=entry.trait_reported,
            trait_efo=entry.trait_efo or [],
            variants_number=entry.variants_number or 0,
            weight_type=entry.weight_type,
            publication_info=entry.publication_info or {},
            ancestry_gwas=entry.ancestry_gwas or {},
            ancestry_eval=entry.ancestry_eval or {},
            method_name=entry.method_name,
            catalog_release_date=entry.catalog_release_date,
            builds_available=entry.builds_available or [],
            file_path_grch37=entry.file_path_grch37,
            file_path_grch38=entry.file_path_grch38,
            metadata_json_path=entry.metadata_json_path,
            downloaded_at=entry.downloaded_at.isoformat() if entry.downloaded_at else None,
            file_size_bytes=entry.file_size_bytes or 0,
        ))
    return results


@router.delete("/cache/{pgs_id}")
def delete_cache(pgs_id: str, db: Session = Depends(get_db)) -> dict[str, str]:
    """Remove a cached scoring file from disk and DB."""
    pgs_id = pgs_id.upper().strip()
    entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail=f"Cache entry {pgs_id} not found")

    # Remove files from disk
    cache_dir = PGS_CACHE_DIR / pgs_id
    if cache_dir.exists():
        shutil.rmtree(cache_dir, ignore_errors=True)

    db.delete(entry)
    db.commit()
    return {"status": "deleted", "pgs_id": pgs_id}


@router.get("/{pgs_id}/download-status")
def download_status(pgs_id: str, db: Session = Depends(get_db)) -> DownloadStatusResponse:
    """Check whether scoring files are cached for a given PGS ID."""
    pgs_id = pgs_id.upper().strip()
    entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()

    if not entry:
        return DownloadStatusResponse(
            pgs_id=pgs_id,
            cached=False,
            builds={"grch37": False, "grch38": False},
            file_sizes={"grch37": 0, "grch38": 0},
        )

    grch37_exists = bool(entry.file_path_grch37 and Path(entry.file_path_grch37).exists())
    grch38_exists = bool(entry.file_path_grch38 and Path(entry.file_path_grch38).exists())

    grch37_size = 0
    grch38_size = 0
    if grch37_exists:
        grch37_size = Path(entry.file_path_grch37).stat().st_size
    if grch38_exists:
        grch38_size = Path(entry.file_path_grch38).stat().st_size

    return DownloadStatusResponse(
        pgs_id=pgs_id,
        cached=grch37_exists or grch38_exists,
        builds={"grch37": grch37_exists, "grch38": grch38_exists},
        file_sizes={"grch37": grch37_size, "grch38": grch38_size},
    )


@router.get("/{pgs_id}")
async def get_score(pgs_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    """Full score metadata. Checks local DB cache first, then PGS Catalog API."""
    pgs_id = pgs_id.upper().strip()

    # Check DB cache first
    entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
    if entry and entry.metadata_json_path:
        meta_path = Path(entry.metadata_json_path)
        if meta_path.exists():
            import json
            try:
                data = json.loads(meta_path.read_text())
                return data
            except Exception:
                pass  # Fall through to API

    # Fetch from API
    try:
        return await pgs_client.get_score(pgs_id)
    except Exception as exc:
        logger.error("PGS get_score error for %s: %s", pgs_id, exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"PGS Catalog API error: {exc}")


@router.post("/download")
async def download_scores(
    req: DownloadRequest,
    db: Session = Depends(get_db),
) -> DownloadResponse:
    """Download scoring files for one or more PGS IDs. Updates the DB cache."""
    results: list[DownloadResultItem] = []
    succeeded = 0
    failed = 0

    for raw_id in req.pgs_ids:
        pgs_id = raw_id.upper().strip()
        try:
            dl = await pgs_client.download_scoring_file(pgs_id, build=req.build)
            _upsert_cache_entry(db, pgs_id, dl, req.build)
            results.append(DownloadResultItem(
                pgs_id=pgs_id,
                build=req.build,
                file_path=dl["file_path"],
                file_size_bytes=dl["file_size_bytes"],
            ))
            succeeded += 1
        except Exception as exc:
            logger.error("Download failed for %s: %s", pgs_id, exc, exc_info=True)
            results.append(DownloadResultItem(
                pgs_id=pgs_id,
                build=req.build,
                error=str(exc),
            ))
            failed += 1

    db.commit()
    return DownloadResponse(results=results, succeeded=succeeded, failed=failed)


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def _upsert_cache_entry(
    db: Session,
    pgs_id: str,
    download_result: dict,
    build: str,
) -> PGSCacheEntry:
    """Create or update a PGSCacheEntry row after a successful download."""
    metadata = download_result.get("metadata", {})
    pub = metadata.get("publication") or {}
    trait_efo = metadata.get("trait_efo") or []

    entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
    if entry is None:
        entry = PGSCacheEntry(pgs_id=pgs_id)
        db.add(entry)

    entry.trait_reported = metadata.get("trait_reported", "")
    entry.trait_efo = trait_efo
    entry.variants_number = metadata.get("variants_number", 0)
    entry.weight_type = metadata.get("weight_type", "")
    entry.publication_info = pub
    entry.ancestry_gwas = metadata.get("ancestry_gwas") or {}
    entry.ancestry_eval = metadata.get("ancestry_eval") or {}
    entry.method_name = metadata.get("method_name", "")
    entry.catalog_release_date = metadata.get("date_release", "")
    entry.builds_available = metadata.get("builds_available") or []
    entry.metadata_json_path = download_result.get("metadata_json_path")
    entry.downloaded_at = datetime.now(timezone.utc)
    entry.file_size_bytes = download_result.get("file_size_bytes", 0)

    # Set the build-specific file path
    build_lower = build.lower()
    if "37" in build_lower:
        entry.file_path_grch37 = download_result.get("file_path")
    else:
        entry.file_path_grch38 = download_result.get("file_path")

    db.flush()
    return entry
