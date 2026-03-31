"""Scoring run CRUD API + WebSocket progress streaming."""

import asyncio
import json
import logging
import math
import os
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import redis.asyncio as aioredis
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import PGS_CACHE_DIR, REDIS_URL, RUNS_DIR, SCRATCH_RUNS
from backend.database import get_db
from backend.models.schemas import (
    PGSCacheEntry,
    RunResult,
    ScoringRun,
    VCF,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Placeholder auth — will be replaced by another agent
# ---------------------------------------------------------------------------
USER_ID = "default"

# ---------------------------------------------------------------------------
# Constants for time estimation (recalibrated Mar 2026)
# Run 30a0a6e0: 3 BAMs + 1 gVCF × 1 PGS (35087 var) = 120s wall time
# Tasks run in PARALLEL — estimate must account for concurrency
# ---------------------------------------------------------------------------
SECS_PER_VARIANT_VCF = 0.0015      # bcftools query + dosage (per variant per PGS)
SECS_PER_VARIANT_BAM = 0.0015      # pysam pileup (per variant per PGS) — similar speed
SECS_PER_VARIANT_GVCF_FAST = 0.00001  # plink2 native scoring (per variant per PGS) — 100x faster
SECS_BAM_STARTUP = 5.0             # BAM open + index load + pysam init per file
SECS_GVCF_PGEN_CONVERSION = 60.0   # one-time gVCF to pgen conversion per sample
SECS_REF_PANEL_PER_PGS = 30.0      # plink2 reference panel scoring per PGS (shared, runs once)
SECS_FREQ_LOOKUP_PER_PGS = 8.0     # plink2 frequency extraction per PGS


# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------

class CreateRunRequest(BaseModel):
    # Backward compat — single VCF
    vcf_id: str | None = None
    # NEW: multiple source files
    source_files: list[dict] | None = None
    # Each dict: {"type": "vcf"|"gvcf"|"bam", "path": str, "vcf_id": str|None}

    pgs_ids: list[str] = Field(..., min_length=1)
    engine: str = Field(default="auto")
    ref_population: str = Field(default="EUR")
    freq_source: str = Field(default="auto")


class RunSummary(BaseModel):
    id: str
    vcf_id: str | None = None
    pgs_ids: list[str]
    engine: str
    genome_build: str
    status: str
    progress_pct: float
    current_step: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_sec: Optional[float] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True


class RunDetail(RunSummary):
    results_path_persistent: Optional[str] = None
    results_path_fast: Optional[str] = None
    config_snapshot: Optional[dict] = None
    source_files: Optional[list[dict]] = None


class RunResultResponse(BaseModel):
    id: str
    pgs_id: str
    trait: Optional[str] = None
    source_file_path: Optional[str] = None
    source_file_type: Optional[str] = None
    variants_matched: int
    variants_total: int
    match_rate: float
    scores_json: list[dict]

    class Config:
        from_attributes = True


class RunResultsResponse(BaseModel):
    run_id: str
    status: str
    source_files: list[dict] = []
    results: list[RunResultResponse]
    results_by_source: dict[str, list[RunResultResponse]] = {}


class RawFileEntry(BaseModel):
    name: str
    size: int
    modified: str


class RerunResponse(BaseModel):
    new_run_id: str
    status: str


class EstimateRequest(BaseModel):
    source_files: list[dict]  # same format as CreateRunRequest.source_files
    pgs_ids: list[str]


class EstimateBreakdownEntry(BaseModel):
    source_file: dict
    file_type: str
    per_variant_sec: float
    total_variants: int
    scoring_sec: float
    ref_panel_sec: float
    subtotal_sec: float


class EstimateResponse(BaseModel):
    estimated_seconds: float
    estimated_display: str  # human readable "~2 min 30 sec"
    breakdown: list[dict]   # per source file
    warnings: list[str]


class DetailResultResponse(BaseModel):
    run_id: str
    pgs_id: str
    detail: Any


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _format_duration(seconds: float) -> str:
    """Format seconds into a human-readable estimated duration string."""
    if seconds < 60:
        return "<1 min"

    hours = int(seconds // 3600)
    remaining = seconds % 3600
    minutes = int(math.ceil(remaining / 60))

    if hours == 0:
        if minutes == 1:
            return "~1 min"
        return f"~{minutes} min"

    if minutes == 0:
        if hours == 1:
            return "~1 hr"
        return f"~{hours} hr"

    if hours == 1:
        return f"~1 hr {minutes} min"
    return f"~{hours} hr {minutes} min"


def _resolve_source_files(
    body: CreateRunRequest, db: Session
) -> list[dict]:
    """
    Normalise the request into a list of source_file dicts.

    If only ``vcf_id`` is provided (backward compat), convert it into a
    single-element list.  Validates VCF records and BAM file existence.
    """
    if body.source_files:
        return body.source_files

    if body.vcf_id:
        return [{"type": "vcf", "vcf_id": body.vcf_id}]

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Either 'vcf_id' or 'source_files' must be provided.",
    )


def _validate_source_files(
    source_files: list[dict], db: Session
) -> tuple[list[dict], str | None, str]:
    """
    Validate every source file entry.

    Returns:
        (enriched_source_files, primary_vcf_id, genome_build)

    For VCF/gVCF types the VCF record must exist.
    For BAM types the file on disk must exist.
    """
    enriched: list[dict] = []
    primary_vcf_id: str | None = None
    genome_build: str | None = None
    errors: list[str] = []

    for idx, sf in enumerate(source_files):
        file_type = sf.get("type", "vcf").lower()
        if file_type not in ("vcf", "gvcf", "bam"):
            errors.append(
                f"source_files[{idx}]: unsupported type '{file_type}'. "
                f"Allowed: vcf, gvcf, bam."
            )
            continue

        entry = dict(sf)
        entry["type"] = file_type

        if file_type in ("vcf", "gvcf"):
            vcf_id = sf.get("vcf_id")
            vcf = None

            if vcf_id:
                vcf = db.query(VCF).filter(VCF.id == vcf_id).first()

            # If no vcf_id or not found, try to look up by path
            if not vcf and sf.get("path"):
                file_path = sf["path"]
                vcf = db.query(VCF).filter(
                    (VCF.path_persistent == file_path) | (VCF.path_fast == file_path)
                ).first()

                # Auto-register if still not found but file exists on disk
                if not vcf and os.path.isfile(file_path):
                    from backend.config import EXISTING_REFERENCE
                    path_str = file_path
                    path_persistent = path_str if path_str.startswith("/data") else None
                    path_fast = path_str if path_str.startswith("/scratch") else None
                    if not path_persistent and not path_fast:
                        path_persistent = path_str  # fallback

                    vcf = VCF(
                        filename=os.path.basename(file_path),
                        path_persistent=path_persistent,
                        path_fast=path_fast,
                        genome_build="GRCh38",
                        reference_fasta_path=EXISTING_REFERENCE,
                    )
                    db.add(vcf)
                    db.commit()
                    db.refresh(vcf)

            if not vcf:
                errors.append(
                    f"source_files[{idx}]: VCF not found and file does not exist: "
                    f"{sf.get('path') or sf.get('vcf_id')}"
                )
                continue

            vcf_id = vcf.id

            # Attach resolved metadata
            entry["vcf_id"] = vcf_id
            entry["filename"] = vcf.filename
            entry["path"] = sf.get("path") or vcf.path_persistent or vcf.path_fast
            entry["genome_build"] = vcf.genome_build
            entry["samples"] = vcf.samples or []

            if primary_vcf_id is None:
                primary_vcf_id = vcf_id
            if genome_build is None:
                genome_build = vcf.genome_build
            elif genome_build != vcf.genome_build:
                errors.append(
                    f"source_files[{idx}]: build mismatch — "
                    f"expected {genome_build}, got {vcf.genome_build}."
                )

        elif file_type == "bam":
            bam_path = sf.get("path")
            if not bam_path:
                errors.append(
                    f"source_files[{idx}]: 'path' is required for type 'bam'."
                )
                continue
            if not Path(bam_path).is_file():
                errors.append(
                    f"source_files[{idx}]: BAM file not found: {bam_path}"
                )
                continue
            entry["path"] = bam_path
            entry["filename"] = Path(bam_path).name

            # BAM files don't carry explicit build info — assume GRCh38 unless
            # another source already pinned it.
            if genome_build is None:
                genome_build = sf.get("genome_build", "GRCh38")
            entry["genome_build"] = genome_build

        enriched.append(entry)

    if errors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="; ".join(errors),
        )

    if not enriched:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid source files provided.",
        )

    # Default build when everything was BAM-only
    if genome_build is None:
        genome_build = "GRCh38"

    return enriched, primary_vcf_id, genome_build


# ---------------------------------------------------------------------------
# POST / — Create a new scoring run
# ---------------------------------------------------------------------------

@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_run(body: CreateRunRequest, db: Session = Depends(get_db)):
    """
    Create a scoring run.
    Validates source files, checks build compatibility, creates ScoringRun,
    launches background scoring task, returns run_id.
    """
    # Resolve & validate source files
    raw_source_files = _resolve_source_files(body, db)
    enriched_source_files, primary_vcf_id, genome_build = _validate_source_files(
        raw_source_files, db
    )

    # Check build compatibility: verify PGS files support this build (if cached)
    incompatible: list[str] = []
    for pgs_id in body.pgs_ids:
        entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
        if entry and entry.builds_available:
            if genome_build not in entry.builds_available:
                incompatible.append(pgs_id)

    if incompatible:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"PGS IDs incompatible with build {genome_build}: "
                f"{', '.join(incompatible)}"
            ),
        )

    # Build config snapshot
    config_snapshot = {
        "vcf_id": primary_vcf_id,
        "source_files": enriched_source_files,
        "pgs_ids": body.pgs_ids,
        "engine": body.engine,
        "genome_build": genome_build,
        "ref_population": body.ref_population,
        "freq_source": body.freq_source,
    }

    # For backward compat, if a single VCF was used, include its metadata
    if primary_vcf_id:
        vcf = db.query(VCF).filter(VCF.id == primary_vcf_id).first()
        if vcf:
            config_snapshot["vcf_filename"] = vcf.filename
            config_snapshot["samples"] = vcf.samples or []

    run = ScoringRun(
        user_id=USER_ID,
        vcf_id=primary_vcf_id,
        pgs_ids=body.pgs_ids,
        engine=body.engine,
        genome_build=genome_build,
        status="created",
        progress_pct=0.0,
        current_step="created",
        config_snapshot=config_snapshot,
        source_files=enriched_source_files,
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    run_id = run.id

    # Launch background scoring task
    # Route to fast plink2-native pipeline when all inputs are gVCFs
    all_gvcf = all(
        sf.get("type", "vcf").lower() == "gvcf"
        for sf in enriched_source_files
    )

    if all_gvcf and body.engine in ("auto", "plink2"):
        from backend.scoring.fast_pipeline import run_fast_scoring
        from backend.scoring.plink2_convert import check_pgen_exists
        from backend.config import PGEN_CACHE_DIR, PGS_CACHE_DIR

        async def _run_fast(rid, sources, pgs_ids, population):
            """Wrapper to run fast scoring pipeline and update DB + Redis."""
            import time as _time
            from backend.database import SessionLocal
            from backend.config import RUNS_DIR, SCRATCH_RUNS

            redis_conn = None
            t0 = _time.monotonic()
            started_at = datetime.now(timezone.utc)

            # ---- Helper: publish progress via single Redis connection ----
            async def _publish(payload):
                nonlocal redis_conn
                try:
                    if redis_conn is None:
                        redis_conn = aioredis.from_url(REDIS_URL, decode_responses=True)
                    await redis_conn.publish(f"run:{rid}:progress", json.dumps(payload))
                except Exception:
                    pass

            # ---- Helper: update DB status ----
            def _db_update(**kwargs):
                sess = SessionLocal()
                try:
                    obj = sess.query(ScoringRun).filter(ScoringRun.id == rid).first()
                    if obj:
                        for k, v in kwargs.items():
                            setattr(obj, k, v)
                        sess.commit()
                finally:
                    sess.close()

            try:
                # Mark run as started
                _db_update(
                    status="scoring", started_at=started_at,
                    progress_pct=2.0, current_step="preparing source files",
                    results_path_persistent=str(RUNS_DIR / rid),
                    results_path_fast=str(SCRATCH_RUNS / rid),
                )
                await _publish({
                    "type": "progress", "run_id": rid,
                    "pct": 2, "step": "preparing source files",
                    "status": "scoring", "pgs_progress": [],
                })

                # Prepare source files for fast pipeline
                fast_sources = []
                for sf in sources:
                    samples = sf.get("samples", [])
                    if samples:
                        sample_name = samples[0]
                    else:
                        sample_name = sf.get("path", "").split("/")[-1].replace(".g.vcf.gz", "").replace(".vcf.gz", "")
                    fast_sources.append({
                        "path": sf["path"],
                        "sample_name": sample_name,
                        "population": population,
                        "type": "gvcf",
                    })

                # Look up traits from PGS cache for enriching results
                trait_map = {}
                sess = SessionLocal()
                try:
                    for pid in pgs_ids:
                        entry = sess.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pid).first()
                        if entry and entry.trait_reported:
                            trait_map[pid] = entry.trait_reported
                finally:
                    sess.close()

                # Progress callback — called per (sample, PGS) completion
                async def progress_cb(step, total, msg, pgs_progress_list=None):
                    pct = round(5 + (step / max(total, 1)) * 90, 1)
                    _db_update(progress_pct=pct, current_step=msg, status="scoring")
                    await _publish({
                        "type": "progress", "run_id": rid,
                        "pct": pct, "step": msg, "status": "scoring",
                        "pgs_progress": pgs_progress_list or [],
                    })

                results = await run_fast_scoring(
                    source_files=fast_sources,
                    pgs_ids=pgs_ids,
                    pgs_cache_dir=str(PGS_CACHE_DIR),
                    progress_callback=progress_cb,
                )

                # ---- Save results to DB and disk ----
                await _publish({
                    "type": "progress", "run_id": rid,
                    "pct": 96, "step": "saving results", "status": "scoring",
                })

                for base_dir in [RUNS_DIR, SCRATCH_RUNS]:
                    results_dir = base_dir / rid
                    results_dir.mkdir(parents=True, exist_ok=True)
                    (results_dir / "results.json").write_text(
                        json.dumps(results, indent=2, default=str)
                    )

                # Save variant detail JSON files
                for r_data in results:
                    vd = r_data.pop("variant_detail", None)
                    if vd and vd.get("variants"):
                        pgs_safe = r_data.get("pgs_id", "?").replace("/", "_")
                        sf_base = os.path.basename(r_data.get("source_path", "")).split(".")[0] or "unknown"
                        detail_name = f"{pgs_safe}_{sf_base}_gvcf_detail.json"
                        for base_dir in [RUNS_DIR, SCRATCH_RUNS]:
                            try:
                                (base_dir / rid / detail_name).write_text(
                                    json.dumps(vd, default=str)
                                )
                            except OSError:
                                pass

                # Group results by (pgs_id, source_path) and save RunResult records
                grouped = defaultdict(list)
                for r_data in results:
                    key = (r_data.get("pgs_id"), r_data.get("source_path"))
                    grouped[key].append(r_data)

                sess = SessionLocal()
                try:
                    for (pgs_id, source_path), group in grouped.items():
                        scores = []
                        for r_data in group:
                            sample_name = r_data.get("sample_name")
                            raw_score = r_data.get("raw_score")
                            z_score = r_data.get("z_score")
                            percentile = r_data.get("percentile")
                            # Handle nested scores from pipeline_e_plus
                            if "scores" in r_data and isinstance(r_data["scores"], list) and r_data["scores"]:
                                se = r_data["scores"][0]
                                sample_name = sample_name or se.get("sample")
                                raw_score = raw_score if raw_score is not None else se.get("raw_score")
                                z_score = z_score if z_score is not None else (se.get("z_score") or se.get("pop_z_score"))
                                percentile = percentile if percentile is not None else se.get("percentile")
                            scores.append({
                                "sample": sample_name,
                                "raw_score": raw_score,
                                "z_score": z_score,
                                "pop_z_score": z_score,
                                "percentile": percentile,
                                "rank": percentile,
                                "ref_mean": r_data.get("ref_mean"),
                                "ref_std": r_data.get("ref_std"),
                                "ref_population": r_data.get("ref_population"),
                                "ref_n_samples": r_data.get("ref_n_samples"),
                                "pipeline": r_data.get("pipeline", "plink2_native"),
                                "variants_used": r_data.get("matched_variants", 0),
                            })
                        sess.add(RunResult(
                            run_id=rid,
                            pgs_id=pgs_id,
                            trait=trait_map.get(pgs_id),
                            source_file_path=source_path,
                            source_file_type="gvcf",
                            variants_matched=group[0].get("matched_variants", 0),
                            variants_total=group[0].get("total_variants", 0),
                            match_rate=group[0].get("match_rate", 0),
                            scores_json=scores,
                        ))

                    # Mark run as complete
                    completed_at = datetime.now(timezone.utc)
                    duration = round(_time.monotonic() - t0, 2)
                    run_obj = sess.query(ScoringRun).filter(ScoringRun.id == rid).first()
                    if run_obj:
                        run_obj.status = "complete"
                        run_obj.progress_pct = 100.0
                        run_obj.current_step = "done"
                        run_obj.completed_at = completed_at
                        run_obj.duration_sec = duration
                    sess.commit()
                finally:
                    sess.close()

                # Publish completion
                await _publish({
                    "type": "complete", "run_id": rid,
                    "pct": 100, "step": "done", "status": "complete",
                })

                # Generate run report
                try:
                    from backend.api.reports import generate_run_report
                    report_path = generate_run_report(rid)
                    logger.info("Generated run report: %s", report_path)
                except Exception as e:
                    logger.warning("Failed to generate run report for %s: %s", rid, e)

                # Sync checklist
                try:
                    from backend.api.checklist import sync_checklist_from_db
                    sync_checklist_from_db()
                except Exception as e:
                    logger.warning("Failed to sync checklist for %s: %s", rid, e)

                logger.info(
                    "Fast scoring run %s completed in %.1fs: %d results",
                    rid, duration, len(results),
                )

            except Exception as e:
                duration = round(_time.monotonic() - t0, 2)
                logger.error("Fast scoring failed for run %s: %s", rid, e, exc_info=True)
                _db_update(
                    status="failed", error_message=str(e)[:500],
                    current_step="error",
                    completed_at=datetime.now(timezone.utc),
                    duration_sec=duration,
                )
                await _publish({
                    "type": "error", "run_id": rid,
                    "status": "failed", "error": str(e)[:500],
                })
            finally:
                if redis_conn:
                    try:
                        await redis_conn.aclose()
                    except Exception:
                        pass

        logger.info(f"Run {run_id}: Using plink2 fast pipeline (all gVCF inputs)")
        asyncio.create_task(_run_fast(
            run_id, enriched_source_files, body.pgs_ids, body.ref_population
        ))
    else:
        from backend.scoring.engine import run_scoring_job
        asyncio.create_task(run_scoring_job(run_id))

    return {"run_id": run_id, "status": "created"}


# ---------------------------------------------------------------------------
# POST /estimate — Estimate scoring run duration
# ---------------------------------------------------------------------------

@router.post("/estimate", response_model=EstimateResponse)
async def estimate_run(body: EstimateRequest, db: Session = Depends(get_db)):
    """
    Estimate the wall-clock time for a scoring run without actually creating one.

    Uses variant counts from PGSCacheEntry (or PGS Catalog API) and per-variant
    timing constants calibrated for this server.
    """
    warnings: list[str] = []
    breakdown: list[dict] = []

    # Gather variant counts per PGS
    pgs_variant_counts: dict[str, int] = {}
    for pgs_id in body.pgs_ids:
        entry = db.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
        if entry and entry.variants_number:
            pgs_variant_counts[pgs_id] = entry.variants_number
        else:
            # Try the API
            try:
                from backend.services.pgs_client import pgs_client
                score_meta = await pgs_client.get_score(pgs_id)
                vn = score_meta.get("variants_number", 0)
                pgs_variant_counts[pgs_id] = vn if vn else 0
                if not vn:
                    warnings.append(
                        f"Could not determine variant count for {pgs_id}; "
                        f"using 0 — estimate may be inaccurate."
                    )
            except Exception:
                pgs_variant_counts[pgs_id] = 0
                warnings.append(
                    f"Failed to look up variant count for {pgs_id}; "
                    f"using 0 — estimate may be inaccurate."
                )

    total_variants = sum(pgs_variant_counts.values())
    num_pgs = len(body.pgs_ids)

    # Reference panel scoring: runs once per PGS (shared across sources)
    # Includes frequency lookup + panel scoring
    ref_panel_sec_total = (SECS_REF_PANEL_PER_PGS + SECS_FREQ_LOOKUP_PER_PGS) * num_pgs

    # Per-source-file scoring — source files run SEQUENTIALLY
    # (the scoring engine loops over source files one by one)
    per_file_totals: list[float] = []

    for sf in body.source_files:
        file_type = sf.get("type", "vcf").lower()

        if file_type == "bam":
            per_variant_sec = SECS_PER_VARIANT_BAM
            startup_sec = SECS_BAM_STARTUP
            scoring_sec = startup_sec + (per_variant_sec * total_variants)
        elif file_type == "gvcf":
            # Fast plink2-native pipeline
            per_variant_sec = SECS_PER_VARIANT_GVCF_FAST
            startup_sec = SECS_GVCF_PGEN_CONVERSION  # one-time conversion
            scoring_sec = startup_sec + (per_variant_sec * total_variants)
        elif file_type == "vcf":
            per_variant_sec = SECS_PER_VARIANT_VCF
            startup_sec = 0
            scoring_sec = per_variant_sec * total_variants
        else:
            per_variant_sec = SECS_PER_VARIANT_VCF
            startup_sec = 0
            scoring_sec = per_variant_sec * total_variants
            warnings.append(f"Unknown file type '{file_type}'; estimating as VCF speed.")
        # Ref panel runs once globally, not per file
        file_ref_sec = 0

        subtotal = scoring_sec + file_ref_sec
        per_file_totals.append(subtotal)

        breakdown.append({
            "source_file": sf,
            "file_type": file_type,
            "per_variant_sec": round(per_variant_sec, 4),
            "total_variants": total_variants,
            "scoring_sec": round(scoring_sec, 1),
            "ref_panel_sec": round(file_ref_sec, 1),
            "subtotal_sec": round(subtotal, 1),
        })

    # Source files run in PARALLEL — estimate wall time as max(file times),
    # not sum, but with a concurrency factor for resource contention
    if not per_file_totals:
        effective_sec = 0.0
    elif len(per_file_totals) == 1:
        effective_sec = per_file_totals[0]
    else:
        # Parallel: wall time ≈ longest file + small overhead for contention
        longest = max(per_file_totals)
        # Add ~20% of remaining files' time for resource sharing overhead
        others_sum = sum(per_file_totals) - longest
        effective_sec = longest + others_sum * 0.2

    # Add shared reference panel time (runs once, not per file)
    effective_sec += ref_panel_sec_total

    estimated_display = _format_duration(effective_sec)

    return EstimateResponse(
        estimated_seconds=round(effective_sec, 2),
        estimated_display=estimated_display,
        breakdown=breakdown,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# GET / — List runs for current user
# ---------------------------------------------------------------------------

@router.get("/", response_model=list[RunSummary])
async def list_runs(
    status_filter: Optional[str] = Query(None, alias="status"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List scoring runs for the current user, ordered by most recent first."""
    query = db.query(ScoringRun).filter(ScoringRun.user_id == USER_ID)

    if status_filter:
        query = query.filter(ScoringRun.status == status_filter)

    runs = (
        query
        .order_by(ScoringRun.started_at.desc().nullslast())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return runs


# ---------------------------------------------------------------------------
# GET /{run_id} — Full run details
# ---------------------------------------------------------------------------

@router.get("/{run_id}", response_model=RunDetail)
async def get_run(run_id: str, db: Session = Depends(get_db)):
    """Return full details for a scoring run."""
    run = db.query(ScoringRun).filter(
        ScoringRun.id == run_id,
        ScoringRun.user_id == USER_ID,
    ).first()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run not found: {run_id}",
        )

    return run


# ---------------------------------------------------------------------------
# GET /{run_id}/results — Structured results from RunResult table
# ---------------------------------------------------------------------------

@router.get("/{run_id}/results", response_model=RunResultsResponse)
async def get_run_results(run_id: str, db: Session = Depends(get_db)):
    """Return structured scoring results for a run, grouped by source file."""
    run = db.query(ScoringRun).filter(
        ScoringRun.id == run_id,
        ScoringRun.user_id == USER_ID,
    ).first()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run not found: {run_id}",
        )

    results = (
        db.query(RunResult)
        .filter(RunResult.run_id == run_id)
        .all()
    )

    flat_results = [
        RunResultResponse(
            id=r.id,
            pgs_id=r.pgs_id,
            trait=r.trait,
            source_file_path=r.source_file_path,
            source_file_type=r.source_file_type,
            variants_matched=r.variants_matched,
            variants_total=r.variants_total,
            match_rate=r.match_rate,
            scores_json=r.scores_json or [],
        )
        for r in results
    ]

    # Group by source_file_path
    results_by_source: dict[str, list[RunResultResponse]] = defaultdict(list)
    for rr in flat_results:
        key = rr.source_file_path or "_default"
        results_by_source[key].append(rr)

    # Source files from config_snapshot or the source_files column
    source_files_list = run.source_files or []
    if not source_files_list and run.config_snapshot:
        source_files_list = run.config_snapshot.get("source_files", [])

    return RunResultsResponse(
        run_id=run_id,
        status=run.status,
        source_files=source_files_list,
        results=flat_results,
        results_by_source=dict(results_by_source),
    )


# ---------------------------------------------------------------------------
# GET /{run_id}/results/detail/{pgs_id} — Per-variant detail JSON
# ---------------------------------------------------------------------------

@router.get("/{run_id}/results/detail/{pgs_id}")
async def get_run_result_detail(
    run_id: str,
    pgs_id: str,
    db: Session = Depends(get_db),
):
    """
    Return the detailed per-variant JSON for a specific PGS score in a run.

    Looks for the file at /data/runs/{run_id}/{pgs_id}_detail.json.
    """
    run = db.query(ScoringRun).filter(
        ScoringRun.id == run_id,
        ScoringRun.user_id == USER_ID,
    ).first()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run not found: {run_id}",
        )

    # Sanitise pgs_id to prevent path traversal
    safe_pgs_id = pgs_id.replace("/", "").replace("..", "").replace("\\", "")
    if safe_pgs_id != pgs_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid PGS ID",
        )

    # Search for detail files matching this PGS ID (may have source type suffix)
    run_dir = RUNS_DIR / run_id
    if not run_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Run directory not found")

    # Try patterns: {pgs_id}_detail.json, {pgs_id}_{type}_detail.json
    detail_files = list(run_dir.glob(f"{safe_pgs_id}*_detail.json"))
    if not detail_files:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Detail file not found for {pgs_id} in run {run_id}",
        )

    # Read all matching detail files and merge
    all_details = []
    for detail_path in detail_files:
        try:
            detail_path.resolve().relative_to(RUNS_DIR.resolve())
        except ValueError:
            continue
        try:
            content = json.loads(detail_path.read_text())
            all_details.append(content)
        except (json.JSONDecodeError, OSError):
            continue

    if not all_details:
        raise HTTPException(status_code=404, detail="No readable detail files")

    return {
        "run_id": run_id,
        "pgs_id": pgs_id,
        "detail": all_details[0] if len(all_details) == 1 else all_details,
        "sources": all_details,
    }


# ---------------------------------------------------------------------------
# GET /{run_id}/results/raw — List raw output files
# ---------------------------------------------------------------------------

@router.get("/{run_id}/results/raw", response_model=list[RawFileEntry])
async def list_raw_files(run_id: str, db: Session = Depends(get_db)):
    """List raw output files in the run directory."""
    run = db.query(ScoringRun).filter(
        ScoringRun.id == run_id,
        ScoringRun.user_id == USER_ID,
    ).first()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run not found: {run_id}",
        )

    # Check both persistent and fast storage
    entries: list[RawFileEntry] = []
    seen_names: set[str] = set()

    for dir_path_str in (run.results_path_fast, run.results_path_persistent):
        if not dir_path_str:
            continue
        dir_path = Path(dir_path_str)
        if not dir_path.is_dir():
            continue

        for f in sorted(dir_path.rglob("*")):
            if not f.is_file():
                continue
            # Use relative path from the run directory
            rel = f.relative_to(dir_path)
            name = str(rel)
            if name in seen_names:
                continue
            seen_names.add(name)
            stat = f.stat()
            entries.append(RawFileEntry(
                name=name,
                size=stat.st_size,
                modified=datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat(),
            ))

    return entries


# ---------------------------------------------------------------------------
# GET /{run_id}/results/raw/{filename:path} — Download a specific raw file
# ---------------------------------------------------------------------------

@router.get("/{run_id}/results/raw/{filename:path}")
async def download_raw_file(
    run_id: str,
    filename: str,
    db: Session = Depends(get_db),
):
    """Download / serve a specific raw output file."""
    run = db.query(ScoringRun).filter(
        ScoringRun.id == run_id,
        ScoringRun.user_id == USER_ID,
    ).first()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run not found: {run_id}",
        )

    # Prevent path traversal
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid filename",
        )

    # Try fast storage first, then persistent
    for dir_path_str in (run.results_path_fast, run.results_path_persistent):
        if not dir_path_str:
            continue
        candidate = Path(dir_path_str) / filename
        # Ensure the resolved path is still within the run directory
        try:
            candidate.resolve().relative_to(Path(dir_path_str).resolve())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid filename",
            )
        if candidate.is_file():
            return FileResponse(
                path=str(candidate),
                filename=Path(filename).name,
                media_type="application/octet-stream",
            )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"File not found: {filename}",
    )


# ---------------------------------------------------------------------------
# POST /{run_id}/rerun — Clone config and create a new run
# ---------------------------------------------------------------------------

@router.post("/{run_id}/rerun", response_model=RerunResponse)
async def rerun(run_id: str, db: Session = Depends(get_db)):
    """Clone the configuration of an existing run and launch a new one."""
    original = db.query(ScoringRun).filter(
        ScoringRun.id == run_id,
        ScoringRun.user_id == USER_ID,
    ).first()

    if not original:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run not found: {run_id}",
        )

    # Clone configuration
    new_run = ScoringRun(
        user_id=USER_ID,
        vcf_id=original.vcf_id,
        pgs_ids=original.pgs_ids,
        engine=original.engine,
        genome_build=original.genome_build,
        status="created",
        progress_pct=0.0,
        current_step="created",
        config_snapshot={
            **(original.config_snapshot or {}),
            "rerun_of": run_id,
        },
        source_files=original.source_files or [],
    )
    db.add(new_run)
    db.commit()
    db.refresh(new_run)

    new_run_id = new_run.id

    # Launch background scoring task
    from backend.scoring.engine import run_scoring_job

    asyncio.create_task(run_scoring_job(new_run_id))

    return RerunResponse(new_run_id=new_run_id, status="created")


# ---------------------------------------------------------------------------
# DELETE /{run_id} — Delete run + results from DB and optionally disk
# ---------------------------------------------------------------------------

@router.delete("/{run_id}", status_code=status.HTTP_200_OK)
async def delete_run(
    run_id: str,
    delete_files: bool = Query(True, description="Also delete output files from disk"),
    db: Session = Depends(get_db),
):
    """Delete a scoring run and its results from the database, and optionally from disk."""
    run = db.query(ScoringRun).filter(
        ScoringRun.id == run_id,
        ScoringRun.user_id == USER_ID,
    ).first()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run not found: {run_id}",
        )

    # Collect paths before deleting the DB record
    paths_to_delete: list[str] = []
    if delete_files:
        if run.results_path_persistent:
            paths_to_delete.append(run.results_path_persistent)
        if run.results_path_fast:
            paths_to_delete.append(run.results_path_fast)

    # Delete RunResult entries (cascade should handle this, but be explicit)
    db.query(RunResult).filter(RunResult.run_id == run_id).delete()
    db.delete(run)
    db.commit()

    # Delete files from disk
    dirs_deleted: list[str] = []
    for p in paths_to_delete:
        dir_path = Path(p)
        if dir_path.is_dir():
            shutil.rmtree(dir_path, ignore_errors=True)
            dirs_deleted.append(p)

    return {
        "deleted": run_id,
        "files_deleted": dirs_deleted,
    }


# ---------------------------------------------------------------------------
# WS /{run_id}/progress — WebSocket progress events
# ---------------------------------------------------------------------------

@router.websocket("/{run_id}/progress")
async def ws_run_progress(websocket: WebSocket, run_id: str):
    """
    WebSocket endpoint that streams progress events for a scoring run.

    Subscribes to Redis pub/sub channel `run:{run_id}:progress` and
    forwards JSON events to the client approximately every second.
    Also sends an initial status snapshot from the database.
    """
    await websocket.accept()

    redis_conn = None
    pubsub = None

    try:
        # Send initial status from DB
        from backend.database import SessionLocal
        db = SessionLocal()
        try:
            run = db.query(ScoringRun).filter(ScoringRun.id == run_id).first()
            if not run:
                await websocket.send_json({
                    "type": "error",
                    "error": f"Run not found: {run_id}",
                })
                await websocket.close(code=4004)
                return

            await websocket.send_json({
                "type": "status",
                "run_id": run_id,
                "status": run.status,
                "pct": run.progress_pct or 0.0,
                "step": run.current_step or "",
            })

            # If already in a terminal state, close right away
            if run.status in ("complete", "failed"):
                await websocket.send_json({
                    "type": "progress",
                    "run_id": run_id,
                    "pct": run.progress_pct or 0.0,
                    "step": run.current_step or run.status,
                    "status": run.status,
                    "error": run.error_message,
                })
                await websocket.close(code=1000)
                return
        finally:
            db.close()

        # Subscribe to Redis channel
        redis_conn = aioredis.from_url(REDIS_URL, decode_responses=True)
        pubsub = redis_conn.pubsub()
        channel = f"run:{run_id}:progress"
        await pubsub.subscribe(channel)

        # Stream events until run completes or client disconnects
        while True:
            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=1.0,
                )
            except asyncio.TimeoutError:
                # Send a heartbeat ping to detect stale connections
                try:
                    await websocket.send_json({"type": "ping"})
                except Exception:
                    break
                continue

            if message is None:
                continue

            if message["type"] == "message":
                data_str = message["data"]
                try:
                    event = json.loads(data_str)
                except json.JSONDecodeError:
                    continue

                await websocket.send_json(event)

                # Close WebSocket when run reaches terminal state
                event_status = event.get("status", "")
                if event_status in ("complete", "failed"):
                    await websocket.close(code=1000)
                    return

    except WebSocketDisconnect:
        logger.debug("WebSocket client disconnected for run %s", run_id)
    except Exception as exc:
        logger.warning("WebSocket error for run %s: %s", run_id, exc)
        try:
            await websocket.close(code=1011)
        except Exception:
            pass
    finally:
        if pubsub:
            try:
                await pubsub.unsubscribe()
                await pubsub.aclose()
            except Exception:
                pass
        if redis_conn:
            try:
                await redis_conn.aclose()
            except Exception:
                pass
