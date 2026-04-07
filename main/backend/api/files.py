"""Genomic file management API — scan, register, inspect, validate, upload, and convert."""

import asyncio
import gzip
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from glob import glob
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import (
    ALIGNED_BAMS_DIR,
    ALIGNMENTS_DIR,
    BCFTOOLS,
    BWA,
    EXISTING_REFERENCE,
    FASTQ_DIRS,
    MINIMAP2,
    NIMOG_OUTPUT_DIR,
    SAMTOOLS,
    UPLOADS_DIR,
    user_uploads_dir,
    user_vcfs_dir,
    USERS_ROOT,
)
from backend.database import get_db
from backend.models.schemas import GenomicFile, User, VCF, gen_uuid
from backend.utils.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# In-memory caches and job stores
# ---------------------------------------------------------------------------

# Cache BAM idxstats results: path -> {total_reads, mapped_reads, mapped_pct, cached_at}
_bam_stats_cache: dict[str, dict[str, Any]] = {}

# Alignment conversion jobs: job_id -> {status, started_at, ..., user_id}
_conversion_jobs: dict[str, dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Multi-user ownership helpers
# ---------------------------------------------------------------------------


def _user_owns_path(filepath: str, user: User) -> bool:
    """Return True if `filepath` lives inside the user's per-user data dir."""
    try:
        resolved = Path(filepath).resolve()
    except (OSError, RuntimeError):
        return False
    user_root = (USERS_ROOT / user.id).resolve()
    try:
        resolved.relative_to(user_root)
        return True
    except ValueError:
        return False


def _user_can_access_path(filepath: str, user: User, db: Session) -> bool:
    """Check whether `user` may read/modify the genomic file at `filepath`.

    Admins can access anything. Regular users can access:
      * files inside their own per-user data dir, OR
      * files that have a GenomicFile DB row they own.
    """
    if user.role == "admin":
        return True
    if _user_owns_path(filepath, user):
        return True
    gf = db.query(GenomicFile).filter(GenomicFile.path == filepath).first()
    if gf and gf.user_id == user.id:
        return True
    return False


def _scope_genomic_files(query, user: User):
    """Restrict a GenomicFile query to rows visible to the current user."""
    if user.role == "admin":
        return query
    return query.filter(GenomicFile.user_id == user.id)


# ---------------------------------------------------------------------------
# Pydantic request/response models
# ---------------------------------------------------------------------------


class ScannedFile(BaseModel):
    id: Optional[str] = None
    file_type: str
    path: str
    sample_name: Optional[str] = None
    file_size_bytes: int = 0
    genome_build: Optional[str] = None
    has_index: bool = False
    qc_status: Optional[str] = None
    vcf_id: Optional[str] = None
    format_details: dict = {}
    mtime: Optional[str] = None
    created_display: Optional[str] = None
    # BAM-specific
    total_reads: Optional[int] = None
    mapped_reads: Optional[int] = None
    mapped_pct: Optional[float] = None
    # VCF/gVCF-specific
    variant_count: Optional[int] = None
    snp_count: Optional[int] = None
    titv_ratio: Optional[float] = None
    caller: Optional[str] = None
    samples: Optional[list[str]] = None
    # FASTQ-specific
    read_count_estimate: Optional[int] = None


class RegisterRequest(BaseModel):
    path: str = Field(..., description="Absolute path to the file on disk")
    file_type: str = Field(..., description="File type: bam, fastq, vcf, gvcf")


class RegisterResponse(BaseModel):
    id: str
    file_type: str
    path: str
    sample_name: Optional[str] = None
    genome_build: Optional[str] = None
    file_size_bytes: int = 0
    format_details: dict = {}
    vcf_id: Optional[str] = None

    class Config:
        from_attributes = True


class UploadResponse(BaseModel):
    filename: str
    path: str
    file_size_bytes: int
    file_type: Optional[str] = None


class InspectRequest(BaseModel):
    path: str


class InspectResponse(BaseModel):
    file_type: str
    path: str
    inspection: dict


class ValidateRequest(BaseModel):
    path: str


class ValidateResponse(BaseModel):
    valid: bool
    errors: list[str]
    file_type: str


class ConvertRequest(BaseModel):
    fastq_r1: str
    fastq_r2: str
    reference: str = EXISTING_REFERENCE
    aligner: str = Field("bwa", pattern=r"^(bwa|minimap2)$")
    threads: int = Field(8, ge=1, le=44)
    sample_name: str


class ConvertResponse(BaseModel):
    job_id: str
    status: str
    output_dir: str
    message: str


class ConvertStatusResponse(BaseModel):
    job_id: str
    status: str  # queued, running, completed, failed
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_sec: Optional[float] = None
    output_bam: Optional[str] = None
    error: Optional[str] = None
    aligner: Optional[str] = None
    sample_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers — sample name extraction
# ---------------------------------------------------------------------------


def _extract_sample_name(filepath: str, file_type: str) -> str:
    """Extract sample name from a file path based on its type.

    Examples:
        /data/aligned_bams/sample1.bam            -> sample1
        /data/.../sample_R1.fastq.gz              -> sample
        /scratch/nimog_output/sample1/final.vcf.gz -> sample1 (from parent dir)
        /scratch/nimog_output/sample1/dv/sample1.g.vcf.gz -> sample1
    """
    basename = os.path.basename(filepath)

    if file_type in ("bam", "cram"):
        return re.sub(r"\.(bam|cram)$", "", basename)

    if file_type == "fastq":
        name = re.sub(r"[._](R[12]|[12])[._]", ".", basename)
        name = re.sub(r"\.(fastq|fq)\.gz$", "", name)
        name = name.rstrip(".")
        return name

    if file_type == "vcf":
        parent = os.path.basename(os.path.dirname(filepath))
        if parent and parent != "." and basename == "final.vcf.gz":
            return parent
        return re.sub(r"\.vcf\.gz$", "", basename)

    if file_type == "gvcf":
        name = re.sub(r"\.g\.vcf\.gz$", "", basename)
        if name == basename:
            parent = os.path.basename(os.path.dirname(filepath))
            dv_parent = os.path.basename(os.path.dirname(os.path.dirname(filepath)))
            if parent == "dv" and dv_parent:
                return dv_parent
        return name

    return basename


def _detect_file_type(filepath: str) -> Optional[str]:
    """Detect genomic file type from extension."""
    if filepath.endswith(".g.vcf.gz"):
        return "gvcf"
    if filepath.endswith(".vcf.gz") or filepath.endswith(".vcf"):
        return "vcf"
    if filepath.endswith(".bam"):
        return "bam"
    if filepath.endswith(".cram"):
        return "cram"
    if filepath.endswith(".fastq.gz") or filepath.endswith(".fq.gz"):
        return "fastq"
    return None


def _find_fastq_pair(filepath: str) -> Optional[str]:
    """Given a FASTQ file path, try to find its R1/R2 pair."""
    basename = os.path.basename(filepath)
    dirname = os.path.dirname(filepath)

    for pattern, replacement in [
        (r"_R1([_.])", r"_R2\1"),
        (r"_R2([_.])", r"_R1\1"),
        (r"_1\.f", r"_2.f"),
        (r"_2\.f", r"_1.f"),
        (r"\.R1\.", r".R2."),
        (r"\.R2\.", r".R1."),
    ]:
        paired_name = re.sub(pattern, replacement, basename)
        if paired_name != basename:
            paired_path = os.path.join(dirname, paired_name)
            if os.path.isfile(paired_path):
                return paired_path

    return None


def _is_r1(filepath: str) -> bool:
    """Check if a FASTQ file is the R1 (forward) read."""
    basename = os.path.basename(filepath)
    return bool(re.search(r"[_.]R1[_.]|_1\.(fastq|fq)", basename))


# ---------------------------------------------------------------------------
# Helpers — file metadata enrichment
# ---------------------------------------------------------------------------


def _get_mtime_iso(filepath: str) -> Optional[str]:
    """Return file modification time as ISO format string."""
    try:
        mt = os.path.getmtime(filepath)
        return datetime.fromtimestamp(mt, tz=timezone.utc).isoformat()
    except OSError:
        return None


def _get_created_display(filepath: str) -> Optional[str]:
    """Return human-readable date like 'Jan 11, 2026'."""
    try:
        mt = os.path.getmtime(filepath)
        return datetime.fromtimestamp(mt, tz=timezone.utc).strftime("%b %d, %Y")
    except OSError:
        return None


async def _get_bam_idxstats(filepath: str) -> dict[str, Any]:
    """Run samtools idxstats and return total_reads, mapped_reads, mapped_pct.

    Results are cached in-memory keyed by path to avoid re-running every scan.
    """
    # Check cache
    cached = _bam_stats_cache.get(filepath)
    if cached is not None:
        return cached

    result: dict[str, Any] = {"total_reads": None, "mapped_reads": None, "mapped_pct": None}

    try:
        proc = await asyncio.create_subprocess_exec(
            SAMTOOLS, "idxstats", filepath,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)

        if proc.returncode != 0:
            logger.warning("samtools idxstats failed for %s: %s", filepath, stderr.decode().strip())
            return result

        total = 0
        mapped = 0
        for line in stdout.decode().splitlines():
            parts = line.split("\t")
            if len(parts) >= 4:
                try:
                    mapped += int(parts[2])
                    total += int(parts[2]) + int(parts[3])
                except ValueError:
                    continue

        result["total_reads"] = total
        result["mapped_reads"] = mapped
        result["mapped_pct"] = round(mapped / total * 100, 2) if total > 0 else 0.0

        _bam_stats_cache[filepath] = result

    except asyncio.TimeoutError:
        logger.warning("samtools idxstats timed out for %s", filepath)
    except Exception as exc:
        logger.warning("Error running samtools idxstats for %s: %s", filepath, exc)

    return result


def _estimate_fastq_read_count(file_size_bytes: int) -> int:
    """Estimate read count from compressed FASTQ file size.

    Average ~250 bytes per compressed read is a reasonable heuristic.
    """
    return file_size_bytes // 250 if file_size_bytes > 0 else 0


# ---------------------------------------------------------------------------
# Helpers — DB cross-reference
# ---------------------------------------------------------------------------


def _cross_reference_vcf(filepath: str, db: Session) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Try to match a VCF/gVCF path with an existing VCF record in the database.

    Returns (vcf_id, genome_build, qc_status) or (None, None, None).
    """
    vcf = db.query(VCF).filter(
        (VCF.path_persistent == filepath) | (VCF.path_fast == filepath)
    ).first()
    if vcf:
        return vcf.id, vcf.genome_build, vcf.qc_status

    filename = os.path.basename(filepath)
    vcf = db.query(VCF).filter(VCF.filename == filename).first()
    if vcf:
        return vcf.id, vcf.genome_build, vcf.qc_status

    return None, None, None


def _get_vcf_db_extras(filepath: str, db: Session) -> dict[str, Any]:
    """Get enriched VCF metadata from DB if available."""
    extras: dict[str, Any] = {}

    vcf = db.query(VCF).filter(
        (VCF.path_persistent == filepath) | (VCF.path_fast == filepath)
    ).first()
    if not vcf:
        filename = os.path.basename(filepath)
        vcf = db.query(VCF).filter(VCF.filename == filename).first()

    if vcf:
        extras["variant_count"] = vcf.variant_count
        extras["snp_count"] = vcf.snp_count
        extras["titv_ratio"] = vcf.titv_ratio
        extras["caller"] = vcf.caller
        extras["samples"] = vcf.samples if vcf.samples else []

    return extras


# ---------------------------------------------------------------------------
# Helpers — file scanning
# ---------------------------------------------------------------------------


def _scan_bam_files() -> list[dict]:
    """Scan for BAM and CRAM alignment files in the aligned BAMs directory."""
    results = []
    for ext, file_type in [("*.bam", "bam"), ("*.cram", "cram")]:
        pattern = str(ALIGNED_BAMS_DIR / ext)
        for filepath in sorted(glob(pattern)):
            if not os.path.isfile(filepath):
                continue

            # Find index file (.bai for BAM, .crai for CRAM)
            index_path = None
            has_index = False
            if file_type == "cram":
                for candidate in [filepath + ".crai", re.sub(r"\.cram$", ".crai", filepath)]:
                    if os.path.isfile(candidate):
                        index_path = candidate
                        has_index = True
                        break
            else:
                for candidate in [filepath + ".bai", re.sub(r"\.bam$", ".bai", filepath)]:
                    if os.path.isfile(candidate):
                        index_path = candidate
                        has_index = True
                        break

            try:
                size = os.path.getsize(filepath)
            except OSError:
                size = 0

            sample_name = _extract_sample_name(filepath, file_type)

            format_details = {}
            if has_index and index_path:
                format_details["index_path"] = index_path

            results.append({
                "file_type": file_type,
                "path": filepath,
                "sample_name": sample_name,
                "file_size_bytes": size,
                "has_index": has_index,
                "format_details": format_details,
            })
    return results


def _scan_fastq_files() -> list[dict]:
    """Scan for FASTQ files and pair R1/R2."""
    results = []
    seen_paths: set[str] = set()

    for fastq_dir in FASTQ_DIRS:
        for ext in ("*.fastq.gz", "*.fq.gz"):
            pattern = str(fastq_dir / "**" / ext)
            for filepath in sorted(glob(pattern, recursive=True)):
                if filepath in seen_paths or not os.path.isfile(filepath):
                    continue
                seen_paths.add(filepath)

                try:
                    size = os.path.getsize(filepath)
                except OSError:
                    size = 0

                sample_name = _extract_sample_name(filepath, "fastq")
                format_details = {}

                paired_path = _find_fastq_pair(filepath)
                if paired_path and paired_path not in seen_paths:
                    seen_paths.add(paired_path)
                    if _is_r1(filepath):
                        format_details["paired_path"] = paired_path
                        format_details["read"] = "R1"
                    else:
                        format_details["paired_path"] = filepath
                        format_details["read"] = "R2"
                        filepath_swap = paired_path
                        paired_path = filepath
                        filepath = filepath_swap
                        format_details["paired_path"] = paired_path
                        format_details["read"] = "R1"
                        try:
                            size = os.path.getsize(filepath)
                        except OSError:
                            size = 0
                elif not paired_path:
                    if _is_r1(filepath):
                        format_details["read"] = "R1"
                    else:
                        format_details["read"] = "single"

                results.append({
                    "file_type": "fastq",
                    "path": filepath,
                    "sample_name": sample_name,
                    "file_size_bytes": size,
                    "has_index": False,
                    "format_details": format_details,
                })
    return results


def _scan_vcf_files(db: Session) -> list[dict]:
    """Scan for VCF files from nimog output."""
    results = []
    pattern = str(NIMOG_OUTPUT_DIR / "*" / "final.vcf.gz")
    for filepath in sorted(glob(pattern)):
        if not os.path.isfile(filepath):
            continue

        try:
            size = os.path.getsize(filepath)
        except OSError:
            size = 0

        sample_name = _extract_sample_name(filepath, "vcf")
        vcf_id, genome_build, qc_status = _cross_reference_vcf(filepath, db)

        tbi_path = filepath + ".tbi"
        has_index = os.path.isfile(tbi_path)
        format_details = {}
        if has_index:
            format_details["index_path"] = tbi_path

        results.append({
            "file_type": "vcf",
            "path": filepath,
            "sample_name": sample_name,
            "file_size_bytes": size,
            "genome_build": genome_build,
            "has_index": has_index,
            "qc_status": qc_status,
            "vcf_id": vcf_id,
            "format_details": format_details,
        })
    return results


def _scan_user_dir(user_id: str, db: Session) -> list[dict]:
    """Scan a single user's per-user data directory for genomic files.

    Walks user_uploads_dir and user_vcfs_dir for BAM/FASTQ/VCF/gVCF files.
    """
    results: list[dict] = []
    seen: set[str] = set()
    try:
        roots = [user_uploads_dir(user_id), user_vcfs_dir(user_id)]
    except Exception:
        return results

    for root in roots:
        if not root.exists():
            continue
        for filepath in sorted(map(str, root.rglob("*"))):
            if filepath in seen or not os.path.isfile(filepath):
                continue
            seen.add(filepath)
            ftype = _detect_file_type(filepath)
            if ftype is None:
                continue
            try:
                size = os.path.getsize(filepath)
            except OSError:
                size = 0
            sample = _extract_sample_name(filepath, ftype)
            format_details: dict = {}

            # Index detection
            if ftype == "bam":
                for cand in (filepath + ".bai", re.sub(r"\.bam$", ".bai", filepath)):
                    if os.path.isfile(cand):
                        format_details["index_path"] = cand
                        break
            elif ftype == "cram":
                for cand in (filepath + ".crai", re.sub(r"\.cram$", ".crai", filepath)):
                    if os.path.isfile(cand):
                        format_details["index_path"] = cand
                        break
            elif ftype in ("vcf", "gvcf"):
                tbi = filepath + ".tbi"
                if os.path.isfile(tbi):
                    format_details["index_path"] = tbi

            entry: dict = {
                "file_type": ftype,
                "path": filepath,
                "sample_name": sample,
                "file_size_bytes": size,
                "has_index": "index_path" in format_details,
                "format_details": format_details,
            }

            if ftype in ("vcf", "gvcf"):
                vcf_id, gb, qc = _cross_reference_vcf(filepath, db)
                entry["vcf_id"] = vcf_id
                entry["genome_build"] = gb
                entry["qc_status"] = qc

            results.append(entry)
    return results


def _scan_gvcf_files(db: Session) -> list[dict]:
    """Scan for gVCF files from DeepVariant output."""
    results = []
    pattern = str(NIMOG_OUTPUT_DIR / "*" / "dv" / "*.g.vcf.gz")
    for filepath in sorted(glob(pattern)):
        if not os.path.isfile(filepath):
            continue

        try:
            size = os.path.getsize(filepath)
        except OSError:
            size = 0

        sample_name = _extract_sample_name(filepath, "gvcf")
        vcf_id, genome_build, qc_status = _cross_reference_vcf(filepath, db)

        tbi_path = filepath + ".tbi"
        has_index = os.path.isfile(tbi_path)
        format_details = {}
        if has_index:
            format_details["index_path"] = tbi_path

        results.append({
            "file_type": "gvcf",
            "path": filepath,
            "sample_name": sample_name,
            "file_size_bytes": size,
            "genome_build": genome_build,
            "has_index": has_index,
            "qc_status": qc_status,
            "vcf_id": vcf_id,
            "format_details": format_details,
        })
    return results


# ---------------------------------------------------------------------------
# Helpers — async subprocess runner
# ---------------------------------------------------------------------------


async def _run_cmd(cmd: list[str], timeout: int = 120) -> tuple[int, str, str]:
    """Run a command asynchronously, returning (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        return -1, "", f"Command timed out after {timeout}s"

    return proc.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")


async def _run_shell(cmd: str, timeout: int = 120) -> tuple[int, str, str]:
    """Run a shell command (with pipes) asynchronously."""
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        return -1, "", f"Command timed out after {timeout}s"

    return proc.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")


# ---------------------------------------------------------------------------
# Helpers — inspection parsers
# ---------------------------------------------------------------------------


def _parse_flagstat(output: str) -> dict[str, Any]:
    """Parse samtools flagstat output into a dict."""
    stats: dict[str, Any] = {}
    for line in output.splitlines():
        line = line.strip()
        # Pattern: "12345 + 0 in total ..."
        m = re.match(r"^(\d+)\s*\+\s*(\d+)\s+(.*)", line)
        if not m:
            continue
        primary = int(m.group(1))
        secondary = int(m.group(2))
        label = m.group(3).strip()

        if "in total" in label:
            stats["total"] = primary + secondary
            stats["total_qc_passed"] = primary
            stats["total_qc_failed"] = secondary
        elif "secondary" in label:
            stats["secondary"] = primary
        elif "supplementary" in label:
            stats["supplementary"] = primary
        elif "duplicates" in label:
            stats["duplicates"] = primary
        elif "mapped" in label and "mate" not in label and "primary" not in label:
            stats["mapped"] = primary
            # Extract percentage
            pct_m = re.search(r"\(([\d.]+)%", line)
            if pct_m:
                stats["mapped_pct"] = float(pct_m.group(1))
        elif "paired in sequencing" in label:
            stats["paired"] = primary
        elif "properly paired" in label:
            stats["properly_paired"] = primary
            pct_m = re.search(r"\(([\d.]+)%", line)
            if pct_m:
                stats["properly_paired_pct"] = float(pct_m.group(1))
        elif "singletons" in label:
            stats["singletons"] = primary
    return stats


def _parse_bcftools_stats_sn(output: str) -> dict[str, Any]:
    """Parse bcftools stats SN (summary numbers) lines."""
    stats: dict[str, Any] = {}
    for line in output.splitlines():
        if not line.startswith("SN"):
            continue
        parts = line.split("\t")
        if len(parts) >= 4:
            key = parts[2].strip().rstrip(":")
            try:
                val = int(parts[3].strip())
            except ValueError:
                try:
                    val = float(parts[3].strip())
                except ValueError:
                    val = parts[3].strip()
            stats[key] = val
    return stats


def _detect_quality_encoding(quality_line: str) -> str:
    """Detect FASTQ quality encoding from a quality string.

    Phred+33 (Sanger/Illumina 1.8+): chars from '!' (33) to '~' (126)
    Phred+64 (Illumina 1.3-1.7): chars from '@' (64) to '~' (126)
    """
    if not quality_line:
        return "unknown"
    min_ord = min(ord(c) for c in quality_line)
    if min_ord < 59:  # Below ';' strongly suggests Phred+33
        return "Phred+33 (Sanger/Illumina 1.8+)"
    elif min_ord >= 64:
        return "Phred+64 (Illumina 1.3-1.7)"
    else:
        return "ambiguous (likely Phred+33)"


# ===========================================================================
# ENDPOINTS
# ===========================================================================


# ---------------------------------------------------------------------------
# GET /scan — Enhanced file scanner
# ---------------------------------------------------------------------------


@router.get("/scan", response_model=list[ScannedFile])
async def scan_files(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Scan known directories for genomic files (BAM, FASTQ, VCF, gVCF).

    Returns enriched metadata including modification times, BAM read counts,
    VCF variant statistics, and FASTQ read count estimates.

    Admins see all global directories plus every user's per-user data dir.
    Regular users see only their own per-user data dir plus any GenomicFile
    rows they own.
    """
    all_files: list[dict] = []

    if current_user.role == "admin":
        all_files.extend(_scan_bam_files())
        all_files.extend(_scan_fastq_files())
        all_files.extend(_scan_vcf_files(db))
        all_files.extend(_scan_gvcf_files(db))

    # Always scan the current user's per-user dir (and, for admins, every user's dir).
    user_ids_to_scan = []
    if current_user.role == "admin" and USERS_ROOT.exists():
        user_ids_to_scan = [p.name for p in USERS_ROOT.iterdir() if p.is_dir()]
    else:
        user_ids_to_scan = [current_user.id]
    for uid in user_ids_to_scan:
        all_files.extend(_scan_user_dir(uid, db))

    # Filter to only files the user can see (admins see everything, users see
    # only files in their own dir or with a GenomicFile row they own).
    if current_user.role != "admin":
        owned_paths = {
            gf.path
            for gf in db.query(GenomicFile)
            .filter(GenomicFile.user_id == current_user.id)
            .all()
        }
        all_files = [
            f for f in all_files
            if _user_owns_path(f["path"], current_user) or f["path"] in owned_paths
        ]

    # Cross-reference with GenomicFile table for already-registered files
    registered = {
        gf.path: gf
        for gf in _scope_genomic_files(db.query(GenomicFile), current_user).all()
    }

    # Gather idxstats concurrently for all BAM/CRAM files with indices
    bam_files_with_index = [
        f for f in all_files
        if f["file_type"] in ("bam", "cram") and f.get("has_index", False)
    ]
    bam_stats_tasks = [_get_bam_idxstats(f["path"]) for f in bam_files_with_index]
    bam_stats_results = await asyncio.gather(*bam_stats_tasks, return_exceptions=True)
    bam_stats_map: dict[str, dict] = {}
    for f, result in zip(bam_files_with_index, bam_stats_results):
        if isinstance(result, dict):
            bam_stats_map[f["path"]] = result

    results: list[ScannedFile] = []
    for f in all_files:
        gf = registered.get(f["path"])
        filepath = f["path"]
        ftype = f["file_type"]

        # Base fields
        entry = ScannedFile(
            id=gf.id if gf else None,
            file_type=ftype,
            path=filepath,
            sample_name=(gf.sample_name if gf and gf.sample_name else f.get("sample_name")),
            file_size_bytes=f.get("file_size_bytes", 0),
            genome_build=f.get("genome_build") or (gf.genome_build if gf else None),
            has_index=f.get("has_index", False),
            qc_status=f.get("qc_status"),
            vcf_id=f.get("vcf_id") or (gf.vcf_id if gf else None),
            format_details=f.get("format_details", {}),
            mtime=_get_mtime_iso(filepath),
            created_display=_get_created_display(filepath),
        )

        # BAM enrichment: idxstats read counts
        if ftype == "bam":
            bam_info = bam_stats_map.get(filepath, {})
            entry.total_reads = bam_info.get("total_reads")
            entry.mapped_reads = bam_info.get("mapped_reads")
            entry.mapped_pct = bam_info.get("mapped_pct")

        # VCF/gVCF enrichment from DB
        elif ftype in ("vcf", "gvcf"):
            vcf_extras = _get_vcf_db_extras(filepath, db)
            if vcf_extras:
                entry.variant_count = vcf_extras.get("variant_count")
                entry.snp_count = vcf_extras.get("snp_count")
                entry.titv_ratio = vcf_extras.get("titv_ratio")
                entry.caller = vcf_extras.get("caller")
                entry.samples = vcf_extras.get("samples")

        # FASTQ enrichment: estimated read count
        elif ftype == "fastq":
            entry.read_count_estimate = _estimate_fastq_read_count(f.get("file_size_bytes", 0))

        results.append(entry)

    return results


# ---------------------------------------------------------------------------
# POST /register — Register a genomic file in the DB
# ---------------------------------------------------------------------------


@router.post("/register", response_model=RegisterResponse, status_code=status.HTTP_201_CREATED)
async def register_file(
    req: RegisterRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Register a genomic file in the GenomicFile table.

    If the file is a VCF or gVCF, also triggers VCF registration via the
    existing VCF registration logic.
    """
    filepath = req.path

    if not os.path.isfile(filepath):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File not found: {filepath}",
        )

    valid_types = ("bam", "fastq", "vcf", "gvcf")
    if req.file_type not in valid_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file_type '{req.file_type}'. Must be one of: {', '.join(valid_types)}",
        )

    # Regular users may only register files inside their own data dir.
    if current_user.role != "admin" and not _user_owns_path(filepath, current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only register files within your own user directory",
        )

    existing = db.query(GenomicFile).filter(GenomicFile.path == filepath).first()
    if existing:
        # Block hijacking another user's already-registered file.
        if (
            current_user.role != "admin"
            and existing.user_id is not None
            and existing.user_id != current_user.id
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This file is registered to another user",
            )
        if existing.user_id is None:
            existing.user_id = current_user.id
            db.commit()
            db.refresh(existing)
        return existing

    try:
        file_size = os.path.getsize(filepath)
    except OSError:
        file_size = 0

    sample_name = _extract_sample_name(filepath, req.file_type)
    format_details: dict = {}
    genome_build: Optional[str] = None
    vcf_id: Optional[str] = None

    if req.file_type in ("bam", "cram"):
        if req.file_type == "cram":
            idx_candidates = [filepath + ".crai", re.sub(r"\.cram$", ".crai", filepath)]
        else:
            idx_candidates = [filepath + ".bai", re.sub(r"\.bam$", ".bai", filepath)]
        for idx in idx_candidates:
            if os.path.isfile(idx):
                format_details["index_path"] = idx
                break

    elif req.file_type == "fastq":
        paired = _find_fastq_pair(filepath)
        if paired:
            format_details["paired_path"] = paired
        format_details["read"] = "R1" if _is_r1(filepath) else "single"

    elif req.file_type in ("vcf", "gvcf"):
        vid, gb, _ = _cross_reference_vcf(filepath, db)
        vcf_id = vid
        genome_build = gb

        if not vcf_id and req.file_type == "vcf":
            from backend.api.vcfs import register_vcf, VCFRegisterRequest
            try:
                vcf_req = VCFRegisterRequest(path=filepath)
                vcf_row = await register_vcf(vcf_req, db, current_user)
                vcf_id = vcf_row.id
                genome_build = vcf_row.genome_build
            except HTTPException:
                logger.warning("Failed to register VCF via vcfs API for %s", filepath)

        tbi_path = filepath + ".tbi"
        if os.path.isfile(tbi_path):
            format_details["index_path"] = tbi_path

    gf = GenomicFile(
        id=gen_uuid(),
        file_type=req.file_type,
        path=filepath,
        sample_name=sample_name,
        genome_build=genome_build,
        file_size_bytes=file_size,
        format_details=format_details,
        vcf_id=vcf_id,
        user_id=current_user.id,
    )

    db.add(gf)
    db.commit()
    db.refresh(gf)

    return gf


# ---------------------------------------------------------------------------
# POST /inspect — Deep file inspection
# ---------------------------------------------------------------------------


@router.post("/inspect", response_model=InspectResponse)
async def inspect_file(
    req: InspectRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Deep inspection of a genomic file.

    Returns detailed metadata depending on file type:
    - BAM: flagstat, idxstats, header (first 50 lines)
    - VCF/gVCF: bcftools stats (SN lines), sample list, header (first 30 lines)
    - FASTQ: first record (4 lines), read length, quality encoding
    """
    filepath = req.path
    if not os.path.isfile(filepath):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File not found: {filepath}",
        )

    if not _user_can_access_path(filepath, current_user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this file",
        )

    file_type = _detect_file_type(filepath)
    if file_type is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unrecognized file type for: {filepath}",
        )

    inspection: dict[str, Any] = {}

    # ----- BAM inspection -----
    if file_type == "bam":
        # Run flagstat, idxstats, and header concurrently
        flagstat_task = _run_cmd([SAMTOOLS, "flagstat", filepath], timeout=120)
        idxstats_task = _run_cmd([SAMTOOLS, "idxstats", filepath], timeout=120)
        header_task = _run_shell(
            f"{SAMTOOLS} view -H {filepath} | head -50",
            timeout=60,
        )

        flagstat_result, idxstats_result, header_result = await asyncio.gather(
            flagstat_task, idxstats_task, header_task
        )

        # Flagstat
        rc, stdout, stderr = flagstat_result
        if rc == 0:
            inspection["flagstat_raw"] = stdout
            inspection["stats"] = _parse_flagstat(stdout)
        else:
            inspection["flagstat_error"] = stderr.strip()

        # Idxstats
        rc, stdout, stderr = idxstats_result
        if rc == 0:
            inspection["idxstats_raw"] = stdout
            # Parse per-chromosome counts
            chroms = []
            total_mapped = 0
            total_unmapped = 0
            for line in stdout.splitlines():
                parts = line.split("\t")
                if len(parts) >= 4:
                    chrom = parts[0]
                    length = int(parts[1]) if parts[1] != "*" else 0
                    mapped = int(parts[2])
                    unmapped = int(parts[3])
                    total_mapped += mapped
                    total_unmapped += unmapped
                    if chrom != "*":
                        chroms.append({
                            "chrom": chrom,
                            "length": length,
                            "mapped": mapped,
                            "unmapped": unmapped,
                        })
            inspection["chromosomes"] = chroms
            inspection["total_mapped"] = total_mapped
            inspection["total_unmapped"] = total_unmapped
        else:
            inspection["idxstats_error"] = stderr.strip()

        # Header
        rc, stdout, stderr = header_result
        if rc == 0:
            inspection["header"] = stdout
        else:
            inspection["header_error"] = stderr.strip()

    # ----- VCF / gVCF inspection -----
    elif file_type in ("vcf", "gvcf"):
        # Run bcftools stats, query samples, and header concurrently
        stats_task = _run_shell(
            f"{BCFTOOLS} stats {filepath} | grep '^SN'",
            timeout=300,
        )
        samples_task = _run_cmd([BCFTOOLS, "query", "-l", filepath], timeout=60)
        header_task = _run_shell(
            f"{BCFTOOLS} view -h {filepath} | head -30",
            timeout=60,
        )

        stats_result, samples_result, header_result = await asyncio.gather(
            stats_task, samples_task, header_task
        )

        # Stats
        rc, stdout, stderr = stats_result
        if rc == 0:
            inspection["stats"] = _parse_bcftools_stats_sn(stdout)
            inspection["stats_raw"] = stdout
        else:
            inspection["stats_error"] = stderr.strip()

        # Samples
        rc, stdout, stderr = samples_result
        if rc == 0:
            inspection["samples"] = [s.strip() for s in stdout.splitlines() if s.strip()]
        else:
            inspection["samples_error"] = stderr.strip()

        # Header
        rc, stdout, stderr = header_result
        if rc == 0:
            inspection["header"] = stdout
        else:
            inspection["header_error"] = stderr.strip()

    # ----- FASTQ inspection -----
    elif file_type == "fastq":
        try:
            with gzip.open(filepath, "rt") as fh:
                lines = []
                for i, line in enumerate(fh):
                    if i >= 4:
                        break
                    lines.append(line.rstrip("\n"))

            if len(lines) >= 4:
                inspection["first_record"] = {
                    "header": lines[0],
                    "sequence": lines[1],
                    "plus_line": lines[2],
                    "quality": lines[3],
                }
                inspection["read_length"] = len(lines[1])
                inspection["quality_encoding"] = _detect_quality_encoding(lines[3])
            else:
                inspection["error"] = f"FASTQ file has only {len(lines)} lines in first record"

            # File size and estimated read count
            try:
                fsize = os.path.getsize(filepath)
                inspection["file_size_bytes"] = fsize
                inspection["read_count_estimate"] = _estimate_fastq_read_count(fsize)
            except OSError:
                pass

        except Exception as exc:
            inspection["error"] = f"Failed to read FASTQ: {exc}"

    return InspectResponse(
        file_type=file_type,
        path=filepath,
        inspection=inspection,
    )


# ---------------------------------------------------------------------------
# POST /validate — File integrity validation
# ---------------------------------------------------------------------------


@router.post("/validate", response_model=ValidateResponse)
async def validate_file(
    req: ValidateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Validate integrity of a genomic file.

    - BAM: samtools quickcheck (returns 0 if OK)
    - VCF/gVCF: bcftools view -h (exits 0 if valid header)
    - FASTQ: check first record has 4 lines with @ header
    """
    filepath = req.path
    if not os.path.isfile(filepath):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File not found: {filepath}",
        )

    if not _user_can_access_path(filepath, current_user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this file",
        )

    file_type = _detect_file_type(filepath)
    if file_type is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unrecognized file type for: {filepath}",
        )

    errors: list[str] = []

    # ----- BAM / CRAM validation -----
    if file_type in ("bam", "cram"):
        rc, stdout, stderr = await _run_cmd(
            [SAMTOOLS, "quickcheck", filepath], timeout=120
        )
        if rc != 0:
            errors.append(f"samtools quickcheck failed: {stderr.strip() or 'non-zero exit code'}")

        # Check for index (.bai for BAM, .crai for CRAM)
        if file_type == "cram":
            idx_candidates = [filepath + ".crai", re.sub(r"\.cram$", ".crai", filepath)]
            idx_label = ".crai"
        else:
            idx_candidates = [filepath + ".bai", re.sub(r"\.bam$", ".bai", filepath)]
            idx_label = ".bai"
        if not any(os.path.isfile(c) for c in idx_candidates):
            errors.append(f"No index ({idx_label}) file found")

    # ----- VCF / gVCF validation -----
    elif file_type in ("vcf", "gvcf"):
        rc, stdout, stderr = await _run_cmd(
            [BCFTOOLS, "view", "-h", filepath], timeout=120
        )
        if rc != 0:
            errors.append(f"bcftools header check failed: {stderr.strip() or 'non-zero exit code'}")
        else:
            # Verify that the header contains at minimum a #CHROM line
            if "#CHROM" not in stdout:
                errors.append("VCF header missing #CHROM line")

        # Check for tabix index
        tbi_path = filepath + ".tbi"
        csi_path = filepath + ".csi"
        if not os.path.isfile(tbi_path) and not os.path.isfile(csi_path):
            errors.append("No tabix index (.tbi) or CSI index (.csi) found")

    # ----- FASTQ validation -----
    elif file_type == "fastq":
        try:
            with gzip.open(filepath, "rt") as fh:
                lines = []
                for i, line in enumerate(fh):
                    if i >= 4:
                        break
                    lines.append(line.rstrip("\n"))

            if len(lines) < 4:
                errors.append(f"FASTQ file has only {len(lines)} lines, expected at least 4")
            else:
                if not lines[0].startswith("@"):
                    errors.append(f"First line does not start with '@': {lines[0][:50]}")
                if not lines[2].startswith("+"):
                    errors.append(f"Third line does not start with '+': {lines[2][:50]}")
                if len(lines[1]) != len(lines[3]):
                    errors.append(
                        f"Sequence length ({len(lines[1])}) != quality length ({len(lines[3])})"
                    )
        except gzip.BadGzipFile:
            errors.append("File is not a valid gzip file")
        except Exception as exc:
            errors.append(f"Failed to read FASTQ: {exc}")

    return ValidateResponse(
        valid=len(errors) == 0,
        errors=errors,
        file_type=file_type,
    )


# ---------------------------------------------------------------------------
# POST /upload — File upload (multipart or URL download)
# ---------------------------------------------------------------------------


@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: Optional[UploadFile] = File(None),
    url: Optional[str] = Form(None),
    filename: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user),
):
    """Upload a genomic file via multipart upload or download from a URL.

    - If `file` is provided: standard multipart upload
    - If `url` is provided: download via curl -L
    - `filename` form field can override the saved filename (for URL downloads)

    Files are saved into the current user's per-user uploads dir.
    """
    target_dir = user_uploads_dir(current_user.id)
    target_dir.mkdir(parents=True, exist_ok=True)

    # ---- URL download path ----
    if url:
        # Determine filename from URL or override
        if filename:
            safe_name = re.sub(r"[^\w.\-]", "_", filename)
        else:
            url_basename = url.split("/")[-1].split("?")[0]
            if not url_basename:
                url_basename = f"download_{uuid.uuid4().hex[:8]}"
            safe_name = re.sub(r"[^\w.\-]", "_", url_basename)

        dest_path = target_dir / safe_name
        dest_path = _unique_path(dest_path, safe_name, target_dir)

        # Download via curl
        rc, stdout, stderr = await _run_cmd(
            ["curl", "-L", "-f", "-o", str(dest_path), url],
            timeout=600,
        )
        if rc != 0:
            if dest_path.exists():
                dest_path.unlink()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to download from URL: {stderr.strip()}",
            )

        file_size = os.path.getsize(str(dest_path))
        file_type = _detect_file_type(str(dest_path))

        return UploadResponse(
            filename=dest_path.name,
            path=str(dest_path),
            file_size_bytes=file_size,
            file_type=file_type,
        )

    # ---- Multipart upload path ----
    if file is None or not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file or URL provided",
        )

    safe_name = re.sub(r"[^\w.\-]", "_", file.filename)
    dest_path = target_dir / safe_name
    dest_path = _unique_path(dest_path, safe_name, target_dir)

    try:
        with open(dest_path, "wb") as out_file:
            while chunk := await file.read(1024 * 1024):  # 1 MB chunks
                out_file.write(chunk)
    except Exception as e:
        if dest_path.exists():
            dest_path.unlink()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save file: {e}",
        )

    file_size = os.path.getsize(str(dest_path))
    file_type = _detect_file_type(str(dest_path))

    return UploadResponse(
        filename=dest_path.name,
        path=str(dest_path),
        file_size_bytes=file_size,
        file_type=file_type,
    )


def _unique_path(dest_path: Path, safe_name: str, target_dir: Optional[Path] = None) -> Path:
    """If dest_path exists, add a numeric suffix to make it unique."""
    if not dest_path.exists():
        return dest_path

    if target_dir is None:
        target_dir = dest_path.parent

    stem = dest_path.stem
    suffix = dest_path.suffix

    # Handle multi-part extensions
    for ext in (".g.vcf.gz", ".fastq.gz", ".fq.gz", ".vcf.gz"):
        if safe_name.endswith(ext):
            stem = safe_name[: -len(ext)]
            suffix = ext
            break

    counter = 1
    while dest_path.exists():
        dest_path = target_dir / f"{stem}_{counter}{suffix}"
        counter += 1

    return dest_path


# ---------------------------------------------------------------------------
# POST /convert/fastq-to-bam — FASTQ alignment pipeline
# ---------------------------------------------------------------------------


@router.post("/convert/fastq-to-bam", response_model=ConvertResponse)
async def convert_fastq_to_bam(
    req: ConvertRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Run FASTQ-to-BAM alignment pipeline (bwa mem or minimap2).

    Creates a background alignment job and returns a job_id immediately.
    Use GET /convert/status/{job_id} to track progress.

    The pipeline:
    1. Align reads with bwa mem or minimap2
    2. Pipe through samtools sort
    3. Index the output BAM

    Output: /scratch/alignments/{job_id}/{sample_name}.bam
    """
    # Validate inputs
    if not os.path.isfile(req.fastq_r1):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"R1 FASTQ not found: {req.fastq_r1}",
        )
    if not os.path.isfile(req.fastq_r2):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"R2 FASTQ not found: {req.fastq_r2}",
        )
    if not _user_can_access_path(req.fastq_r1, current_user, db) or not _user_can_access_path(req.fastq_r2, current_user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to one or both FASTQ files",
        )
    if not os.path.isfile(req.reference):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Reference not found: {req.reference}",
        )
    if req.aligner not in ("bwa", "minimap2"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid aligner: {req.aligner}. Use 'bwa' or 'minimap2'.",
        )

    # Sanitize sample name
    sample = re.sub(r"[^\w.\-]", "_", req.sample_name)
    if not sample:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="sample_name is required",
        )

    # Create output directory
    job_id = uuid.uuid4().hex[:12]
    output_dir = ALIGNMENTS_DIR / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_bam = str(output_dir / f"{sample}.bam")

    # Register job
    _conversion_jobs[job_id] = {
        "status": "queued",
        "started_at": None,
        "completed_at": None,
        "duration_sec": None,
        "output_bam": output_bam,
        "error": None,
        "aligner": req.aligner,
        "sample_name": sample,
        "fastq_r1": req.fastq_r1,
        "fastq_r2": req.fastq_r2,
        "reference": req.reference,
        "threads": req.threads,
        "user_id": current_user.id,
    }

    # Launch background task
    asyncio.create_task(_run_alignment(job_id))

    return ConvertResponse(
        job_id=job_id,
        status="queued",
        output_dir=str(output_dir),
        message=f"Alignment job queued. Aligner={req.aligner}, threads={req.threads}. "
                f"Output: {output_bam}",
    )


async def _run_alignment(job_id: str) -> None:
    """Execute the alignment pipeline in the background."""
    job = _conversion_jobs[job_id]
    job["status"] = "running"
    job["started_at"] = datetime.now(timezone.utc).isoformat()
    start_time = time.monotonic()

    sample = job["sample_name"]
    r1 = job["fastq_r1"]
    r2 = job["fastq_r2"]
    ref = job["reference"]
    threads = job["threads"]
    output_bam = job["output_bam"]
    aligner = job["aligner"]

    rg_string = f"@RG\\tID:{sample}\\tSM:{sample}\\tPL:ILLUMINA"

    try:
        # Build alignment command
        if aligner == "bwa":
            align_cmd = (
                f"{BWA} mem -t {threads} -R '{rg_string}' {ref} {r1} {r2} | "
                f"{SAMTOOLS} sort -@ {threads} -o {output_bam}"
            )
        else:  # minimap2
            align_cmd = (
                f"{MINIMAP2} -t {threads} -a -R '{rg_string}' {ref} {r1} {r2} | "
                f"{SAMTOOLS} sort -@ {threads} -o {output_bam}"
            )

        # Run alignment + sort
        rc, stdout, stderr = await _run_shell(align_cmd, timeout=86400)  # 24h timeout
        if rc != 0:
            job["status"] = "failed"
            job["error"] = f"Alignment failed (exit {rc}): {stderr[-2000:]}"
            job["completed_at"] = datetime.now(timezone.utc).isoformat()
            job["duration_sec"] = round(time.monotonic() - start_time, 1)
            return

        # Index the BAM
        rc, stdout, stderr = await _run_cmd(
            [SAMTOOLS, "index", output_bam], timeout=3600
        )
        if rc != 0:
            job["status"] = "failed"
            job["error"] = f"BAM indexing failed (exit {rc}): {stderr[-2000:]}"
            job["completed_at"] = datetime.now(timezone.utc).isoformat()
            job["duration_sec"] = round(time.monotonic() - start_time, 1)
            return

        job["status"] = "completed"
        job["completed_at"] = datetime.now(timezone.utc).isoformat()
        job["duration_sec"] = round(time.monotonic() - start_time, 1)

        logger.info(
            "Alignment job %s completed in %.1fs: %s",
            job_id, job["duration_sec"], output_bam,
        )

    except Exception as exc:
        job["status"] = "failed"
        job["error"] = f"Unexpected error: {exc}"
        job["completed_at"] = datetime.now(timezone.utc).isoformat()
        job["duration_sec"] = round(time.monotonic() - start_time, 1)
        logger.exception("Alignment job %s failed", job_id)


# ---------------------------------------------------------------------------
# GET /convert/status/{job_id} — Check conversion job status
# ---------------------------------------------------------------------------


@router.get("/convert/status/{job_id}", response_model=ConvertStatusResponse)
async def convert_status(
    job_id: str,
    current_user: User = Depends(get_current_user),
):
    """Check the status of a FASTQ-to-BAM conversion job."""
    job = _conversion_jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job not found: {job_id}",
        )

    if current_user.role != "admin" and job.get("user_id") != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this conversion job",
        )

    return ConvertStatusResponse(
        job_id=job_id,
        status=job["status"],
        started_at=job.get("started_at"),
        completed_at=job.get("completed_at"),
        duration_sec=job.get("duration_sec"),
        output_bam=job.get("output_bam"),
        error=job.get("error"),
        aligner=job.get("aligner"),
        sample_name=job.get("sample_name"),
    )


# ---------------------------------------------------------------------------
# POST /rename — Rename a file's display name
# ---------------------------------------------------------------------------

class RenameRequest(BaseModel):
    path: str
    new_name: str

@router.post("/rename")
async def rename_file(
    req: RenameRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update the display name for a file (stored in GenomicFile table)."""
    from backend.models.schemas import GenomicFile, VCF

    new_name = req.new_name.strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Name cannot be empty")

    if not _user_can_access_path(req.path, current_user, db):
        raise HTTPException(status_code=403, detail="You do not have access to this file")

    # Update or create GenomicFile record
    gf = db.query(GenomicFile).filter(GenomicFile.path == req.path).first()
    if gf:
        gf.sample_name = new_name
        if gf.user_id is None:
            gf.user_id = current_user.id
    else:
        # Create a new record for this file
        file_type = "bam"
        if req.path.endswith(".g.vcf.gz"): file_type = "gvcf"
        elif req.path.endswith(".vcf.gz") or req.path.endswith(".vcf"): file_type = "vcf"
        elif req.path.endswith(".fastq.gz") or req.path.endswith(".fq.gz"): file_type = "fastq"
        gf = GenomicFile(
            file_type=file_type,
            path=req.path,
            sample_name=new_name,
            file_size_bytes=os.path.getsize(req.path) if os.path.exists(req.path) else 0,
            user_id=current_user.id,
        )
        db.add(gf)
    db.commit()

    # Also update VCF record if this is a VCF/gVCF
    vcf = db.query(VCF).filter(
        (VCF.path_persistent == req.path) | (VCF.path_fast == req.path)
    ).first()
    if vcf:
        vcf.filename = new_name
        db.commit()

    return {"path": req.path, "new_name": new_name, "updated": True}


# ---------------------------------------------------------------------------
# POST /delete — Delete a file from disk and DB
# ---------------------------------------------------------------------------

class DeleteFileRequest(BaseModel):
    path: str

@router.post("/delete")
async def delete_file(
    req: DeleteFileRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Delete a genomic file from disk and remove DB records."""
    from backend.models.schemas import GenomicFile, VCF

    filepath = req.path
    if not _user_can_access_path(filepath, current_user, db):
        raise HTTPException(status_code=403, detail="You do not have access to this file")
    deleted_files = []
    errors = []

    # Delete from disk
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
            deleted_files.append(filepath)
            # Also delete index files
            for ext in [".bai", ".crai", ".tbi", ".csi", ".fai"]:
                idx = filepath + ext
                if os.path.exists(idx):
                    os.remove(idx)
                    deleted_files.append(idx)
                # Also check without double extension (e.g., .bam.bai → .bai)
                base_idx = filepath.rsplit(".", 1)[0] + ext
                if base_idx != idx and os.path.exists(base_idx):
                    os.remove(base_idx)
                    deleted_files.append(base_idx)
        except OSError as e:
            errors.append(str(e))

    # Remove from GenomicFile table
    gf = db.query(GenomicFile).filter(GenomicFile.path == filepath).first()
    if gf:
        db.delete(gf)

    # Remove from VCF table
    vcf = db.query(VCF).filter(
        (VCF.path_persistent == filepath) | (VCF.path_fast == filepath)
    ).first()
    if vcf:
        db.delete(vcf)

    db.commit()

    return {
        "path": filepath,
        "deleted": True,
        "deleted_files": deleted_files,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# POST /duplicate — Copy a file between storage tiers
# ---------------------------------------------------------------------------

class DuplicateFileRequest(BaseModel):
    path: str
    target: str  # "persistent" (/data) or "fast" (/scratch)

@router.post("/duplicate")
async def duplicate_file(
    req: DuplicateFileRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Copy a file between storage tiers (persistent ↔ fast SSD). Never moves — always copies."""
    import shutil
    from backend.config import DATA_DIR, SCRATCH_DIR, user_vcfs_dir
    from backend.models.schemas import GenomicFile, VCF

    src = req.path
    if not os.path.exists(src):
        raise HTTPException(status_code=400, detail=f"Source file not found: {src}")

    if not _user_can_access_path(src, current_user, db):
        raise HTTPException(status_code=403, detail="You do not have access to this file")

    filename = os.path.basename(src)

    # Per-user VCF dirs for regular users; admins keep using global dirs.
    if current_user.role == "admin":
        if req.target == "persistent":
            dest_dir = DATA_DIR / "vcfs"
        elif req.target == "fast":
            dest_dir = SCRATCH_DIR / "vcfs"
        else:
            raise HTTPException(status_code=400, detail="Target must be 'persistent' or 'fast'")
    else:
        # For regular users, both targets land inside their user dir
        if req.target not in ("persistent", "fast"):
            raise HTTPException(status_code=400, detail="Target must be 'persistent' or 'fast'")
        dest_dir = user_vcfs_dir(current_user.id)

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = str(dest_dir / filename)

    # Don't overwrite
    if os.path.exists(dest):
        # Add suffix
        base, ext = os.path.splitext(filename)
        if filename.endswith(".vcf.gz"):
            base = filename[:-7]
            ext = ".vcf.gz"
        elif filename.endswith(".g.vcf.gz"):
            base = filename[:-9]
            ext = ".g.vcf.gz"
        counter = 1
        while os.path.exists(dest):
            dest = str(dest_dir / f"{base}_{counter}{ext}")
            counter += 1

    # Copy file (async via subprocess for large files)
    proc = await asyncio.create_subprocess_exec(
        "cp", src, dest,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Copy failed: {stderr.decode()}")

    # Also copy index files
    for ext in [".tbi", ".bai", ".crai", ".csi"]:
        idx_src = src + ext
        if os.path.exists(idx_src):
            idx_dest = dest + ext
            await asyncio.create_subprocess_exec("cp", idx_src, idx_dest)

    file_size = os.path.getsize(dest)

    # Update VCF record if applicable
    vcf = db.query(VCF).filter(
        (VCF.path_persistent == src) | (VCF.path_fast == src)
    ).first()
    if vcf:
        if req.target == "persistent":
            vcf.path_persistent = dest
        else:
            vcf.path_fast = dest
        db.commit()

    return {
        "source": src,
        "destination": dest,
        "target": req.target,
        "size_bytes": file_size,
    }
