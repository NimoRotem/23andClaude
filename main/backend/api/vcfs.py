"""VCF management API — register, inspect, QC, duplicate, and delete VCFs."""

import asyncio
import json
import logging
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import (
    BCFTOOLS,
    SAMTOOLS,
    EXISTING_REFERENCE,
    VCFS_DIR,
    SCRATCH_VCFS,
    SCRATCH_PIPELINE,
    SCRATCH_TMP,
    DEFAULT_REFERENCE_GRCH37,
    DEFAULT_REFERENCE_GRCH38,
    user_vcfs_dir,
)
from backend.database import get_db
from backend.models.schemas import User, VCF, gen_uuid
from backend.utils.auth import get_current_user, require_admin

logger = logging.getLogger(__name__)


def _scope_vcfs(query, current_user: User):
    """Restrict a VCF query to the current user's VCFs (admins see all)."""
    if current_user.role == "admin":
        return query
    return query.filter(VCF.created_by_user_id == current_user.id)

router = APIRouter()

# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------


class VCFRegisterRequest(BaseModel):
    path: str = Field(..., description="Absolute path to an existing VCF file on disk")
    genome_build: Optional[str] = Field(
        None,
        description="Genome build (GRCh37 or GRCh38). Auto-detected from header if omitted.",
    )


class VCFCreateFromBAMRequest(BaseModel):
    bam_path: str = Field(..., description="Absolute path to the BAM file")
    reference_fasta: Optional[str] = Field(
        None, description="Reference FASTA path. Defaults to the existing reference."
    )
    genome_build: Optional[str] = Field(None, description="GRCh37 or GRCh38")
    caller: str = Field("bcftools", description="Variant caller to use (bcftools)")
    output_dir: Optional[str] = Field(
        None, description="Directory for pipeline output. Defaults to scratch pipeline dir."
    )


class VCFDuplicateRequest(BaseModel):
    target: str = Field(
        ..., description="Target storage tier: 'fast' (scratch/NVMe) or 'persistent' (/data)"
    )


class VCFSummary(BaseModel):
    id: str
    filename: str
    genome_build: str
    sample_count: int
    variant_count: int
    snp_count: int
    indel_count: int
    titv_ratio: Optional[float]
    qc_status: str
    file_size_bytes: int
    path_persistent: Optional[str]
    path_fast: Optional[str]
    created_at: Optional[datetime]

    class Config:
        from_attributes = True


class VCFDetail(VCFSummary):
    samples: list
    reference_fasta_path: Optional[str]
    reference_fasta_md5: Optional[str]
    caller: Optional[str]
    caller_version: Optional[str]
    qc_checks: Optional[dict]
    created_by_user_id: Optional[str]

    class Config:
        from_attributes = True


class JobResponse(BaseModel):
    job_id: str
    vcf_id: str
    status: str
    message: str


# ---------------------------------------------------------------------------
# Helpers — async subprocess execution
# ---------------------------------------------------------------------------


async def _run_cmd(cmd: str, timeout: int = 300) -> tuple[int, str, str]:
    """Run a shell command asynchronously, returning (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Command timed out after {timeout}s: {cmd[:120]}",
        )
    stdout = stdout_bytes.decode("utf-8", errors="replace")
    stderr = stderr_bytes.decode("utf-8", errors="replace")
    return proc.returncode, stdout, stderr


# ---------------------------------------------------------------------------
# VCF validation & metadata extraction
# ---------------------------------------------------------------------------


async def _validate_vcf(path: str) -> str:
    """Validate VCF with bcftools view -h. Returns the header text."""
    rc, stdout, stderr = await _run_cmd(f"{BCFTOOLS} view -h '{path}'")
    if rc != 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File is not a valid VCF or cannot be read by bcftools: {stderr.strip()[:500]}",
        )
    return stdout


async def _extract_samples(path: str) -> list[str]:
    """Extract sample names from a VCF using bcftools query -l."""
    rc, stdout, stderr = await _run_cmd(f"{BCFTOOLS} query -l '{path}'")
    if rc != 0:
        logger.warning("bcftools query -l failed for %s: %s", path, stderr.strip())
        return []
    samples = [s.strip() for s in stdout.strip().splitlines() if s.strip()]
    return samples


def _detect_genome_build(header: str) -> str:
    """Detect genome build from VCF header contigs, reference line, and contig lengths.

    Strategy:
      1. Check ##contig lines — if any contig ID starts with 'chr', it's GRCh38.
      2. Check ##reference line for known build indicators (grch38, hg38, grch37, hg19).
      3. Check contig lengths — GRCh38 chr1 = 248,956,422 vs GRCh37 chr1 = 249,250,621.
      4. Check if the reference FASTA header contains build info.
      5. Default to GRCh38 (modern data is more commonly GRCh38).
    """
    contig_ids = []
    contig_lengths = {}
    reference_path = ""

    for line in header.splitlines():
        m = re.match(r"^##contig=<ID=([^,>]+)(?:,length=(\d+))?", line)
        if m:
            contig_ids.append(m.group(1))
            if m.group(2):
                contig_lengths[m.group(1)] = int(m.group(2))

        if line.startswith("##reference="):
            reference_path = line

    ref_lower = reference_path.lower()

    # Check contigs for chr prefix
    if any(cid.startswith("chr") for cid in contig_ids):
        return "GRCh38"

    # Check reference line for build indicators
    if "grch38" in ref_lower or "hg38" in ref_lower:
        return "GRCh38"
    if "grch37" in ref_lower or "hg19" in ref_lower:
        return "GRCh37"

    # Check contig lengths to distinguish builds
    # GRCh38 chr1/1 = 248,956,422; GRCh37 chr1/1 = 249,250,621
    chr1_len = contig_lengths.get("1") or contig_lengths.get("chr1") or 0
    if chr1_len == 248956422:
        return "GRCh38"
    if chr1_len == 249250621:
        return "GRCh37"

    # If reference FASTA path is available, try reading its first line
    if reference_path:
        fasta_path = reference_path.replace("##reference=", "").replace("file://", "").strip()
        try:
            with open(fasta_path) as f:
                first_line = f.readline().lower()
                if "grch38" in first_line or "hg38" in first_line:
                    return "GRCh38"
                if "grch37" in first_line or "hg19" in first_line:
                    return "GRCh37"
        except Exception:
            pass

    # Default to GRCh38 (most modern WGS data)
    return "GRCh38"


def _detect_caller(header: str) -> tuple[Optional[str], Optional[str]]:
    """Try to detect the variant caller from VCF header lines.

    Returns (caller_name, caller_version) or (None, None).
    """
    caller_name = None
    caller_version = None

    for line in header.splitlines():
        line_lower = line.lower()

        # ##source=...
        if line.startswith("##source="):
            source_val = line.split("=", 1)[1].strip()
            # Common callers
            for name in [
                "GATK HaplotypeCaller",
                "HaplotypeCaller",
                "DeepVariant",
                "FreeBayes",
                "Strelka2",
                "Strelka",
                "Mutect2",
                "bcftools",
                "samtools",
                "VarScan",
                "Octopus",
                "Dragen",
            ]:
                if name.lower() in source_val.lower():
                    caller_name = name
                    # Try to extract a version
                    ver_match = re.search(r"[vV]?(\d+\.\d+[\.\d]*)", source_val)
                    if ver_match:
                        caller_version = ver_match.group(1)
                    break
            if caller_name is None:
                # Use the raw source value
                caller_name = source_val[:100]
            break

        # ##GATKCommandLine=<...>
        if "##GATKCommandLine" in line or "##GATK" in line.upper():
            caller_name = "GATK"
            ver_match = re.search(r"Version=([^,>\"]+)", line)
            if ver_match:
                caller_version = ver_match.group(1).strip()
            break

        # ##bcftools_callCommand
        if "##bcftools_call" in line_lower:
            caller_name = "bcftools call"
            ver_match = re.search(r"Version=([^,>\"]+)", line, re.IGNORECASE)
            if ver_match:
                caller_version = ver_match.group(1).strip()
            break

    return caller_name, caller_version


async def _run_bcftools_stats(path: str) -> dict:
    """Run bcftools stats and parse key metrics.

    Returns dict with keys:
      snp_count, indel_count, variant_count, titv_ratio, ts_count, tv_count,
      records_count, samples_count, plus raw qc_checks data.
    """
    rc, stdout, stderr = await _run_cmd(f"{BCFTOOLS} stats '{path}'", timeout=600)
    if rc != 0:
        logger.error("bcftools stats failed for %s: %s", path, stderr.strip()[:500])
        return {
            "snp_count": 0,
            "indel_count": 0,
            "variant_count": 0,
            "titv_ratio": None,
            "ts_count": 0,
            "tv_count": 0,
            "records_count": 0,
            "samples_count": 0,
            "qc_checks": {"error": stderr.strip()[:500]},
        }

    snp_count = 0
    indel_count = 0
    records_count = 0
    samples_count = 0
    ts_count = 0
    tv_count = 0
    titv_ratio = None

    # Parse SN (Summary Numbers) lines
    # Format: SN\t<id>\t<key>\t<value>
    # Example: SN	0	number of samples:	1
    #          SN	0	number of records:	12345
    #          SN	0	number of SNPs:	10000
    #          SN	0	number of indels:	2345
    for line in stdout.splitlines():
        if line.startswith("SN\t"):
            parts = line.split("\t")
            if len(parts) >= 4:
                key = parts[2].strip().rstrip(":")
                try:
                    value = int(parts[3].strip())
                except (ValueError, IndexError):
                    continue

                if "number of samples" in key:
                    samples_count = value
                elif "number of records" in key:
                    records_count = value
                elif key == "number of SNPs":
                    snp_count = value
                elif key == "number of indels":
                    indel_count = value

        # Parse TSTV (Transitions / Transversions) line
        # Format: TSTV\t<id>\tts\ttv\tts/tv\tts (1st ALT)\ttv (1st ALT)\tts/tv (1st ALT)
        # Example: TSTV	0	8000	4000	2.00	7500	3800	1.97
        if line.startswith("TSTV\t"):
            parts = line.split("\t")
            if len(parts) >= 5:
                try:
                    ts_count = int(parts[2].strip())
                    tv_count = int(parts[3].strip())
                    # Use the ratio from bcftools directly
                    titv_str = parts[4].strip()
                    if titv_str and titv_str not in ("nan", "inf", "-inf", "."):
                        titv_ratio = float(titv_str)
                    elif tv_count > 0:
                        titv_ratio = round(ts_count / tv_count, 4)
                except (ValueError, IndexError):
                    pass

    variant_count = snp_count + indel_count
    if variant_count == 0 and records_count > 0:
        variant_count = records_count

    return {
        "snp_count": snp_count,
        "indel_count": indel_count,
        "variant_count": variant_count,
        "titv_ratio": titv_ratio,
        "ts_count": ts_count,
        "tv_count": tv_count,
        "records_count": records_count,
        "samples_count": samples_count,
        "qc_checks": {
            "bcftools_stats_ran": True,
            "snp_count": snp_count,
            "indel_count": indel_count,
            "records_count": records_count,
            "ts_count": ts_count,
            "tv_count": tv_count,
            "titv_ratio": titv_ratio,
        },
    }


def _determine_qc_status(stats: dict) -> str:
    """Determine QC status from extracted stats.

    Rules:
      - 'failed'  if bcftools stats returned an error
      - 'issues'  if Ti/Tv ratio is suspiciously low (<1.0) or zero variants found
      - 'passed'  otherwise
    """
    if stats.get("qc_checks", {}).get("error"):
        return "failed"

    variant_count = stats.get("variant_count", 0)
    titv_ratio = stats.get("titv_ratio")

    issues = []

    if variant_count == 0:
        issues.append("zero_variants")

    if titv_ratio is not None and titv_ratio < 1.0:
        issues.append("low_titv_ratio")

    if issues:
        qc_checks = stats.get("qc_checks", {})
        qc_checks["issues"] = issues
        stats["qc_checks"] = qc_checks
        return "issues"

    return "passed"


def _determine_storage_tier(path: str) -> tuple[Optional[str], Optional[str]]:
    """Return (path_persistent, path_fast) based on where the file lives."""
    path_str = str(path)
    if path_str.startswith(str(SCRATCH_VCFS)) or path_str.startswith("/scratch/"):
        return None, path_str
    elif path_str.startswith(str(VCFS_DIR)) or path_str.startswith("/data/"):
        return path_str, None
    else:
        # External location — treat as persistent
        return path_str, None


# ---------------------------------------------------------------------------
# Background pipeline task
# ---------------------------------------------------------------------------

# In-memory job tracker (in production you'd use Redis or DB)
_pipeline_jobs: dict[str, dict] = {}


async def _run_bam_to_vcf_pipeline(
    job_id: str,
    vcf_id: str,
    bam_path: str,
    reference_fasta: str,
    genome_build: str,
    output_dir: str,
    db_url: str,
):
    """Background task: call variants from BAM and register the resulting VCF."""
    from backend.database import SessionLocal

    _pipeline_jobs[job_id]["status"] = "running"
    _pipeline_jobs[job_id]["step"] = "variant_calling"

    output_vcf = os.path.join(output_dir, f"{vcf_id}.vcf.gz")

    try:
        # Step 1: mpileup + call
        cmd = (
            f"{BCFTOOLS} mpileup -Ou -f '{reference_fasta}' '{bam_path}' "
            f"| {BCFTOOLS} call -mv -Oz -o '{output_vcf}'"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                f"bcftools mpileup/call failed (rc={proc.returncode}): "
                f"{stderr.decode('utf-8', errors='replace')[:1000]}"
            )

        _pipeline_jobs[job_id]["step"] = "indexing"

        # Step 2: Index
        idx_cmd = f"{BCFTOOLS} index -t '{output_vcf}'"
        proc = await asyncio.create_subprocess_shell(
            idx_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        # Index failure is non-fatal (some operations still work)

        _pipeline_jobs[job_id]["step"] = "qc"

        # Step 3: Validate + extract metadata
        header = await _validate_vcf(output_vcf)
        samples = await _extract_samples(output_vcf)
        stats = await _run_bcftools_stats(output_vcf)
        qc_status = _determine_qc_status(stats)
        caller_name, caller_version = _detect_caller(header)

        file_size = os.path.getsize(output_vcf) if os.path.exists(output_vcf) else 0
        path_persistent, path_fast = _determine_storage_tier(output_vcf)

        # Step 4: Update DB record
        _pipeline_jobs[job_id]["step"] = "saving"
        db = SessionLocal()
        try:
            vcf_row = db.query(VCF).filter(VCF.id == vcf_id).first()
            if vcf_row:
                vcf_row.filename = os.path.basename(output_vcf)
                vcf_row.path_persistent = path_persistent
                vcf_row.path_fast = path_fast
                vcf_row.genome_build = genome_build
                vcf_row.reference_fasta_path = reference_fasta
                vcf_row.samples = samples
                vcf_row.sample_count = len(samples)
                vcf_row.variant_count = stats["variant_count"]
                vcf_row.snp_count = stats["snp_count"]
                vcf_row.indel_count = stats["indel_count"]
                vcf_row.titv_ratio = stats["titv_ratio"]
                vcf_row.caller = caller_name or "bcftools"
                vcf_row.caller_version = caller_version
                vcf_row.qc_status = qc_status
                vcf_row.qc_checks = stats["qc_checks"]
                vcf_row.file_size_bytes = file_size
                db.commit()
        finally:
            db.close()

        _pipeline_jobs[job_id]["status"] = "completed"
        _pipeline_jobs[job_id]["step"] = "done"
        _pipeline_jobs[job_id]["output_vcf"] = output_vcf

    except Exception as exc:
        logger.exception("BAM-to-VCF pipeline failed for job %s", job_id)
        _pipeline_jobs[job_id]["status"] = "failed"
        _pipeline_jobs[job_id]["error"] = str(exc)[:1000]

        # Update the VCF DB record with failure
        db = SessionLocal()
        try:
            vcf_row = db.query(VCF).filter(VCF.id == vcf_id).first()
            if vcf_row:
                vcf_row.qc_status = "failed"
                vcf_row.qc_checks = {"pipeline_error": str(exc)[:1000]}
                db.commit()
        finally:
            db.close()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/", response_model=list[VCFDetail])
def list_vcfs(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List registered VCFs the current user owns (admins see all)."""
    vcfs = (
        _scope_vcfs(db.query(VCF), current_user)
        .order_by(VCF.created_at.desc())
        .all()
    )
    return vcfs


@router.get("/{vcf_id}", response_model=VCFDetail)
def get_vcf(
    vcf_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get full details of a registered VCF (must be owner or admin)."""
    vcf = _scope_vcfs(
        db.query(VCF).filter(VCF.id == vcf_id),
        current_user,
    ).first()
    if not vcf:
        raise HTTPException(status_code=404, detail=f"VCF {vcf_id} not found")
    return vcf


@router.post("/register", response_model=VCFDetail, status_code=status.HTTP_201_CREATED)
async def register_vcf(
    req: VCFRegisterRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Register an existing VCF file on disk.

    Steps:
      1. Validate the file exists and is a valid VCF (bcftools view -h)
      2. Extract: samples, variant count, genome build (auto-detect from header)
      3. Run QC: bcftools stats -> parse Ti/Tv, SNP count, indel count
      4. Store in DB with qc_status, owned by the calling user
    """
    vcf_path = req.path

    # --- Check file exists ---
    if not os.path.isfile(vcf_path):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File not found: {vcf_path}",
        )

    # --- Check not already registered ---
    existing = db.query(VCF).filter(
        (VCF.path_persistent == vcf_path) | (VCF.path_fast == vcf_path)
    ).first()
    if existing:
        # Only the owner (or admin) may "rediscover" an existing record
        if current_user.role != "admin" and existing.created_by_user_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A VCF with this path is already registered to another user",
            )
        return existing

    # --- 1. Validate VCF ---
    header = await _validate_vcf(vcf_path)

    # --- 2. Extract samples ---
    samples = await _extract_samples(vcf_path)

    # --- Detect genome build ---
    if req.genome_build:
        if req.genome_build not in ("GRCh37", "GRCh38"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="genome_build must be 'GRCh37' or 'GRCh38'",
            )
        genome_build = req.genome_build
    else:
        genome_build = _detect_genome_build(header)

    # --- Detect caller ---
    caller_name, caller_version = _detect_caller(header)

    # --- 3. Run QC ---
    stats = await _run_bcftools_stats(vcf_path)

    # --- Determine QC status ---
    qc_status = _determine_qc_status(stats)

    # --- File metadata ---
    file_size = os.path.getsize(vcf_path)
    path_persistent, path_fast = _determine_storage_tier(vcf_path)

    # --- Reference FASTA path ---
    if genome_build == "GRCh38":
        ref_path = DEFAULT_REFERENCE_GRCH38
    else:
        ref_path = DEFAULT_REFERENCE_GRCH37
    # Check if the defaults exist; fall back to the existing reference
    if not os.path.isfile(ref_path):
        ref_path = EXISTING_REFERENCE if os.path.isfile(EXISTING_REFERENCE) else None

    # --- 4. Store in DB ---
    vcf_row = VCF(
        id=gen_uuid(),
        filename=os.path.basename(vcf_path),
        path_persistent=path_persistent,
        path_fast=path_fast,
        genome_build=genome_build,
        reference_fasta_path=ref_path,
        samples=samples,
        sample_count=len(samples),
        variant_count=stats["variant_count"],
        snp_count=stats["snp_count"],
        indel_count=stats["indel_count"],
        titv_ratio=stats["titv_ratio"],
        caller=caller_name,
        caller_version=caller_version,
        qc_status=qc_status,
        qc_checks=stats["qc_checks"],
        file_size_bytes=file_size,
        created_by_user_id=current_user.id,
    )

    db.add(vcf_row)
    db.commit()
    db.refresh(vcf_row)

    return vcf_row


@router.post("/create-from-bam", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_from_bam(
    req: VCFCreateFromBAMRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Start a BAM-to-VCF variant-calling pipeline (runs asynchronously).

    Returns a job_id that can be polled. The resulting VCF will be registered
    in the database once the pipeline completes.
    """
    # Validate BAM exists
    if not os.path.isfile(req.bam_path):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"BAM file not found: {req.bam_path}",
        )

    # Reference
    reference_fasta = req.reference_fasta or EXISTING_REFERENCE
    if not os.path.isfile(reference_fasta):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Reference FASTA not found: {reference_fasta}",
        )

    # Genome build
    genome_build = req.genome_build or "GRCh37"
    if genome_build not in ("GRCh37", "GRCh38"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="genome_build must be 'GRCh37' or 'GRCh38'",
        )

    # Output directory — default to per-user vcfs dir
    output_dir = req.output_dir or str(user_vcfs_dir(current_user.id))
    os.makedirs(output_dir, exist_ok=True)

    # Create a placeholder VCF record in the DB
    vcf_id = gen_uuid()
    vcf_row = VCF(
        id=vcf_id,
        filename=f"{vcf_id}.vcf.gz",
        genome_build=genome_build,
        reference_fasta_path=reference_fasta,
        qc_status="pending",
        caller=req.caller,
        qc_checks={"pipeline": "bam_to_vcf", "bam_path": req.bam_path},
        created_by_user_id=current_user.id,
    )
    db.add(vcf_row)
    db.commit()

    # Create job tracking entry
    job_id = str(uuid.uuid4())[:12]
    _pipeline_jobs[job_id] = {
        "job_id": job_id,
        "vcf_id": vcf_id,
        "status": "queued",
        "step": "initializing",
        "bam_path": req.bam_path,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    # Launch background pipeline
    asyncio.create_task(
        _run_bam_to_vcf_pipeline(
            job_id=job_id,
            vcf_id=vcf_id,
            bam_path=req.bam_path,
            reference_fasta=reference_fasta,
            genome_build=genome_build,
            output_dir=output_dir,
            db_url="",  # Not needed — we import SessionLocal directly
        )
    )

    return JobResponse(
        job_id=job_id,
        vcf_id=vcf_id,
        status="queued",
        message=f"BAM-to-VCF pipeline started. Calling variants from {os.path.basename(req.bam_path)}.",
    )


@router.get("/jobs/{job_id}")
def get_job_status(
    job_id: str,
    _current_user: User = Depends(get_current_user),
):
    """Check the status of a BAM-to-VCF pipeline job."""
    job = _pipeline_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


@router.post("/{vcf_id}/duplicate", response_model=VCFDetail)
async def duplicate_vcf(
    vcf_id: str,
    req: VCFDuplicateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Copy a VCF between storage tiers (fast <-> persistent).

    Body: {"target": "fast"} or {"target": "persistent"}
    Uses asyncio subprocess for the copy operation.
    """
    if req.target not in ("fast", "persistent"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="target must be 'fast' or 'persistent'",
        )

    vcf = _scope_vcfs(
        db.query(VCF).filter(VCF.id == vcf_id),
        current_user,
    ).first()
    if not vcf:
        raise HTTPException(status_code=404, detail=f"VCF {vcf_id} not found")

    # Determine source path (prefer whichever exists)
    source_path = None
    if vcf.path_fast and os.path.isfile(vcf.path_fast):
        source_path = vcf.path_fast
    elif vcf.path_persistent and os.path.isfile(vcf.path_persistent):
        source_path = vcf.path_persistent
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No accessible VCF file found on disk for this record. "
            f"path_persistent={vcf.path_persistent}, path_fast={vcf.path_fast}",
        )

    # Determine target directory and path
    if req.target == "fast":
        target_dir = str(SCRATCH_VCFS)
        if vcf.path_fast and os.path.isfile(vcf.path_fast):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"VCF already exists on fast storage: {vcf.path_fast}",
            )
    else:
        target_dir = str(VCFS_DIR)
        if vcf.path_persistent and os.path.isfile(vcf.path_persistent):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"VCF already exists on persistent storage: {vcf.path_persistent}",
            )

    os.makedirs(target_dir, exist_ok=True)
    target_path = os.path.join(target_dir, vcf.filename)

    # Async copy using cp command
    cmd = f"cp -- '{source_path}' '{target_path}'"
    rc, stdout, stderr = await _run_cmd(cmd, timeout=600)
    if rc != 0:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Copy failed: {stderr.strip()[:500]}",
        )

    # Also copy the index if it exists
    for index_ext in (".tbi", ".csi"):
        src_idx = source_path + index_ext
        if os.path.isfile(src_idx):
            tgt_idx = target_path + index_ext
            await _run_cmd(f"cp -- '{src_idx}' '{tgt_idx}'", timeout=120)

    # Update DB
    if req.target == "fast":
        vcf.path_fast = target_path
    else:
        vcf.path_persistent = target_path

    db.commit()
    db.refresh(vcf)

    return vcf


@router.delete("/{vcf_id}", status_code=status.HTTP_200_OK)
async def delete_vcf(
    vcf_id: str,
    delete_file: bool = Query(False, description="Also delete the VCF file(s) from disk"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Unregister a VCF from the database.

    With ?delete_file=true, also removes the file(s) from disk.
    """
    vcf = _scope_vcfs(
        db.query(VCF).filter(VCF.id == vcf_id),
        current_user,
    ).first()
    if not vcf:
        raise HTTPException(status_code=404, detail=f"VCF {vcf_id} not found")

    deleted_files = []
    errors = []

    if delete_file:
        for path in [vcf.path_persistent, vcf.path_fast]:
            if path and os.path.isfile(path):
                try:
                    os.remove(path)
                    deleted_files.append(path)
                    # Also remove index files
                    for ext in (".tbi", ".csi"):
                        idx_path = path + ext
                        if os.path.isfile(idx_path):
                            os.remove(idx_path)
                            deleted_files.append(idx_path)
                except OSError as e:
                    errors.append(f"Failed to delete {path}: {e}")

    # Delete dependent scoring runs and their results first
    from backend.models.schemas import ScoringRun, RunResult
    dependent_runs = db.query(ScoringRun).filter(ScoringRun.vcf_id == vcf_id).all()
    for run in dependent_runs:
        db.query(RunResult).filter(RunResult.run_id == run.id).delete()
        db.delete(run)

    # Track deleted paths so sync-nimog doesn't re-register them
    deleted_paths = _load_deleted_paths()
    for p in [vcf.path_persistent, vcf.path_fast]:
        if p:
            deleted_paths.add(p)
    _save_deleted_paths(deleted_paths)

    db.delete(vcf)
    db.commit()

    result = {
        "id": vcf_id,
        "status": "deleted",
        "message": f"VCF {vcf_id} ({vcf.filename}) unregistered. {len(dependent_runs)} associated run(s) also removed.",
    }
    if delete_file:
        result["deleted_files"] = deleted_files
        if errors:
            result["file_errors"] = errors

    return result


# ---------------------------------------------------------------------------
# Nimog integration — scan completed nimog jobs and auto-register VCFs
# ---------------------------------------------------------------------------

NIMOG_JOBS_DIR = Path(os.environ.get("NIMOG_JOBS_DIR", str(Path(__file__).resolve().parents[2] / "nimog" / "jobs")))
DELETED_PATHS_FILE = Path(os.environ.get("DELETED_VCF_PATHS_FILE", "/data/app/deleted_vcf_paths.json"))


def _load_deleted_paths() -> set:
    try:
        if DELETED_PATHS_FILE.exists():
            return set(json.loads(DELETED_PATHS_FILE.read_text()))
    except Exception:
        pass
    return set()


def _save_deleted_paths(paths: set):
    DELETED_PATHS_FILE.parent.mkdir(parents=True, exist_ok=True)
    DELETED_PATHS_FILE.write_text(json.dumps(sorted(paths)))


@router.post("/sync-nimog")
async def sync_nimog_vcfs(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Scan nimog job directory for completed VCFs and register any that are new.

    Admin-only because it scans a system-wide directory. Newly registered
    VCFs are owned by the calling admin.
    """
    import json as _json

    if not NIMOG_JOBS_DIR.exists():
        return {"synced": 0, "skipped": 0, "errors": [], "message": "nimog jobs directory not found"}

    deleted_paths = _load_deleted_paths()
    synced = []
    skipped = []
    errors = []

    for job_file in sorted(NIMOG_JOBS_DIR.glob("*.json")):
        try:
            job_data = _json.loads(job_file.read_text())
        except Exception:
            continue

        if job_data.get("status") != "completed":
            continue

        vcf_path = job_data.get("output_vcf")
        if not vcf_path or not os.path.exists(vcf_path):
            continue

        # Skip if user previously deleted this VCF
        if vcf_path in deleted_paths:
            skipped.append({"job_id": job_data["job_id"], "path": vcf_path, "reason": "previously deleted"})
            continue

        # Check if already registered (by path)
        existing = db.query(VCF).filter(
            (VCF.path_persistent == vcf_path) | (VCF.path_fast == vcf_path)
        ).first()
        if existing:
            skipped.append({"job_id": job_data["job_id"], "vcf_id": existing.id, "path": vcf_path})
            continue

        # Register this VCF
        try:
            # Extract metadata using the same helpers as register_vcf
            samples = await _extract_samples(vcf_path)
            header_text = await _validate_vcf(vcf_path)
            build = _detect_genome_build(header_text) if header_text else "GRCh38"
            caller, caller_version = _detect_caller(header_text) if header_text else ("bcftools call", None)
            stats = await _run_bcftools_stats(vcf_path)
            file_size = os.path.getsize(vcf_path)

            # Determine storage tier
            path_fast = vcf_path if vcf_path.startswith("/scratch") else None
            path_persistent = vcf_path if vcf_path.startswith("/data") else None

            # Build the BAM source info
            bam_source = os.path.basename(job_data.get("bam_path", "unknown"))

            vcf_record = VCF(
                id=gen_uuid(),
                filename=f"{bam_source.replace('.bam', '')}.vcf.gz",
                path_persistent=path_persistent,
                path_fast=path_fast,
                genome_build=build,
                reference_fasta_path=job_data.get("reference", EXISTING_REFERENCE),
                samples=samples,
                sample_count=len(samples),
                variant_count=stats.get("variant_count", 0),
                snp_count=stats.get("snp_count", 0),
                indel_count=stats.get("indel_count", 0),
                titv_ratio=stats.get("titv_ratio"),
                caller=caller,
                caller_version=caller_version,
                qc_status="passed" if stats.get("variant_count", 0) > 0 else "issues",
                qc_checks={
                    "source": "nimog",
                    "nimog_job_id": job_data["job_id"],
                    "bam_path": job_data.get("bam_path"),
                    **stats,
                },
                file_size_bytes=file_size,
                created_by_user_id=current_user.id,
            )
            db.add(vcf_record)
            db.commit()
            synced.append({
                "job_id": job_data["job_id"],
                "vcf_id": vcf_record.id,
                "path": vcf_path,
                "filename": vcf_record.filename,
                "variants": vcf_record.variant_count,
                "samples": samples,
            })
        except Exception as e:
            errors.append({"job_id": job_data["job_id"], "path": vcf_path, "error": str(e)})
            logger.exception(f"Failed to register nimog VCF from job {job_data['job_id']}")

    return {
        "synced": len(synced),
        "skipped": len(skipped),
        "errors": errors,
        "new_vcfs": synced,
        "already_registered": skipped,
    }
