"""Storage management API — disk status, tracked files, restore, and backup."""

import asyncio
import os
import shutil
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.config import (
    DATA_DIR,
    SCRATCH_DIR,
    VCFS_DIR,
    SCRATCH_VCFS,
    PGS_CACHE_DIR,
    SCRATCH_PGS_CACHE,
    RUNS_DIR,
    SCRATCH_RUNS,
)
from backend.database import get_db
from backend.models.schemas import User, VCF, PGSCacheEntry, ScoringRun
from backend.utils.auth import get_current_user

router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class DiskTier(BaseModel):
    total: int
    used: int
    available: int
    pct: float


class StorageStatus(BaseModel):
    persistent: DiskTier
    fast: DiskTier


class TrackedFile(BaseModel):
    type: str  # vcf | pgs_cache | scoring_run
    id: str
    name: str
    path_persistent: str | None = None
    path_fast: str | None = None
    size_bytes: int = 0


class CopyResult(BaseModel):
    files_copied: list[str]
    total_bytes: int
    errors: list[str]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _disk_tier(path: Path) -> DiskTier:
    """Return disk usage stats for the given path. Falls back to zeros if the path doesn't exist."""
    try:
        usage = shutil.disk_usage(str(path))
        pct = round((usage.used / usage.total) * 100, 1) if usage.total > 0 else 0.0
        return DiskTier(total=usage.total, used=usage.used, available=usage.free, pct=pct)
    except (FileNotFoundError, OSError):
        return DiskTier(total=0, used=0, available=0, pct=0.0)


async def _async_copy(src: str, dst: str) -> tuple[str, int | None, str | None]:
    """Copy a file using rsync in an asyncio subprocess. Returns (dst, bytes_copied, error)."""
    src_path = Path(src)
    dst_path = Path(dst)

    if not src_path.exists():
        return dst, None, f"Source not found: {src}"

    dst_path.parent.mkdir(parents=True, exist_ok=True)

    proc = await asyncio.create_subprocess_exec(
        "rsync", "-a", "--whole-file", str(src_path), str(dst_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        return dst, None, f"rsync failed for {src}: {stderr.decode().strip()}"

    try:
        size = dst_path.stat().st_size if dst_path.is_file() else _dir_size(dst_path)
    except OSError:
        size = 0

    return dst, size, None


def _dir_size(path: Path) -> int:
    """Recursively compute directory size in bytes."""
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file():
            total += entry.stat().st_size
    return total


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/status", response_model=StorageStatus)
async def storage_status():
    """Return disk usage for persistent (/data) and fast (/scratch) tiers."""
    return StorageStatus(
        persistent=_disk_tier(DATA_DIR),
        fast=_disk_tier(SCRATCH_DIR),
    )


@router.get("/files", response_model=list[TrackedFile])
async def list_tracked_files(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List all tracked files across VCFs, PGS cache entries, and scoring runs."""
    files: list[TrackedFile] = []

    # VCFs
    for vcf in db.query(VCF).all():
        files.append(TrackedFile(
            type="vcf",
            id=vcf.id,
            name=vcf.filename,
            path_persistent=vcf.path_persistent,
            path_fast=vcf.path_fast,
            size_bytes=vcf.file_size_bytes or 0,
        ))

    # PGS cache entries
    for entry in db.query(PGSCacheEntry).all():
        # Each PGS entry can have GRCh37 and/or GRCh38 files
        name_parts = [entry.pgs_id]
        if entry.trait_reported:
            name_parts.append(entry.trait_reported)
        name = " — ".join(name_parts)

        path_persistent = entry.file_path_grch38 or entry.file_path_grch37
        files.append(TrackedFile(
            type="pgs_cache",
            id=entry.pgs_id,
            name=name,
            path_persistent=path_persistent,
            path_fast=None,
            size_bytes=entry.file_size_bytes or 0,
        ))

    # Scoring runs
    for run in db.query(ScoringRun).all():
        files.append(TrackedFile(
            type="scoring_run",
            id=run.id,
            name=f"Run {run.id} ({run.status})",
            path_persistent=run.results_path_persistent,
            path_fast=run.results_path_fast,
            size_bytes=0,
        ))

    return files


@router.post("/restore-workspace", response_model=CopyResult)
async def restore_workspace(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Bulk-copy files needed for pending runs from /data to /scratch.

    Copies VCFs and PGS cache files that are referenced by runs in
    'created' or 'downloading' status.
    """
    pending_runs = (
        db.query(ScoringRun)
        .filter(ScoringRun.status.in_(["created", "downloading"]))
        .all()
    )

    copy_tasks: list[tuple[str, str]] = []  # (src, dst)
    seen_sources: set[str] = set()

    for run in pending_runs:
        # VCF file for this run
        vcf = db.query(VCF).filter(VCF.id == run.vcf_id).first()
        if vcf and vcf.path_persistent:
            src = vcf.path_persistent
            dst = str(SCRATCH_VCFS / Path(src).name)
            if src not in seen_sources and Path(src).exists():
                copy_tasks.append((src, dst))
                seen_sources.add(src)

        # PGS scoring files for this run
        pgs_ids = run.pgs_ids or []
        for pgs_id in pgs_ids:
            entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
            if not entry:
                continue

            # Pick the file matching the run's genome build, fall back to any available
            if run.genome_build == "GRCh37" and entry.file_path_grch37:
                src = entry.file_path_grch37
            elif run.genome_build == "GRCh38" and entry.file_path_grch38:
                src = entry.file_path_grch38
            elif entry.file_path_grch38:
                src = entry.file_path_grch38
            elif entry.file_path_grch37:
                src = entry.file_path_grch37
            else:
                continue

            dst = str(SCRATCH_PGS_CACHE / Path(src).name)
            if src not in seen_sources and Path(src).exists():
                copy_tasks.append((src, dst))
                seen_sources.add(src)

    if not copy_tasks:
        return CopyResult(files_copied=[], total_bytes=0, errors=[])

    # Ensure destination directories exist
    SCRATCH_VCFS.mkdir(parents=True, exist_ok=True)
    SCRATCH_PGS_CACHE.mkdir(parents=True, exist_ok=True)

    # Run all copies concurrently
    results = await asyncio.gather(
        *[_async_copy(src, dst) for src, dst in copy_tasks],
        return_exceptions=True,
    )

    files_copied: list[str] = []
    total_bytes = 0
    errors: list[str] = []

    for result in results:
        if isinstance(result, Exception):
            errors.append(str(result))
            continue
        dst, size, error = result
        if error:
            errors.append(error)
        else:
            files_copied.append(dst)
            total_bytes += size or 0

    # Update VCF records with fast paths
    for vcf in db.query(VCF).all():
        if vcf.path_persistent:
            fast_path = str(SCRATCH_VCFS / Path(vcf.path_persistent).name)
            if fast_path in files_copied:
                vcf.path_fast = fast_path
    db.commit()

    return CopyResult(files_copied=files_copied, total_bytes=total_bytes, errors=errors)


@router.post("/backup", response_model=CopyResult)
async def backup_results(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Bulk-copy all /scratch results to /data persistent storage.

    Finds completed scoring run result directories on scratch and copies them
    to the persistent runs directory.
    """
    copy_tasks: list[tuple[str, str]] = []

    # Find scoring runs that have results on scratch
    runs_with_scratch = (
        db.query(ScoringRun)
        .filter(ScoringRun.results_path_fast.isnot(None))
        .all()
    )

    for run in runs_with_scratch:
        src = run.results_path_fast
        if not src or not Path(src).exists():
            continue

        # Determine persistent destination
        dst = str(RUNS_DIR / Path(src).name)
        copy_tasks.append((src, dst))

    # Also scan scratch runs directory for any untracked result directories
    if SCRATCH_RUNS.exists():
        for entry in SCRATCH_RUNS.iterdir():
            src = str(entry)
            dst = str(RUNS_DIR / entry.name)
            if src not in [t[0] for t in copy_tasks]:
                copy_tasks.append((src, dst))

    if not copy_tasks:
        return CopyResult(files_copied=[], total_bytes=0, errors=[])

    RUNS_DIR.mkdir(parents=True, exist_ok=True)

    # Copy all concurrently using rsync (handles both files and directories)
    results = await asyncio.gather(
        *[_async_copy(src, dst) for src, dst in copy_tasks],
        return_exceptions=True,
    )

    files_copied: list[str] = []
    total_bytes = 0
    errors: list[str] = []

    for result in results:
        if isinstance(result, Exception):
            errors.append(str(result))
            continue
        dst, size, error = result
        if error:
            errors.append(error)
        else:
            files_copied.append(dst)
            total_bytes += size or 0

    # Update run records with persistent paths
    for run in runs_with_scratch:
        if run.results_path_fast:
            persistent_path = str(RUNS_DIR / Path(run.results_path_fast).name)
            if persistent_path in files_copied:
                run.results_path_persistent = persistent_path
    db.commit()

    return CopyResult(files_copied=files_copied, total_bytes=total_bytes, errors=errors)
