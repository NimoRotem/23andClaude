"""Ancestry API routes — inference, PCA data, per-sample ancestry."""

import json
import subprocess
import os
from pathlib import Path
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.database import SessionLocal
from backend.models.schemas import SampleAncestry, AncestryPGSResult
from backend.config import DATA_DIR

router = APIRouter()

# Paths
_ANCESTRY_DIR = Path(DATA_DIR) / "ancestry"
_REFERENCE_DIR = Path(DATA_DIR) / "reference" / "1kg"
_SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"


# ── Pydantic models ──────────────────────────────────────────

class AncestryProportions(BaseModel):
    EUR: float = 0.0
    EAS: float = 0.0
    AFR: float = 0.0
    SAS: float = 0.0
    AMR: float = 0.0


class SampleAncestryOut(BaseModel):
    sample_id: str
    proportions: AncestryProportions
    primary_ancestry: str
    is_admixed: bool
    admixture_description: str | None
    pca_coordinates: list[float]
    inference_method: str | None

    class Config:
        from_attributes = True


class PCAPoint(BaseModel):
    sample_id: str
    pc1: float
    pc2: float
    pc3: float | None = None
    population: str
    is_reference: bool = False


class AncestryPGSResultOut(BaseModel):
    sample_id: str
    trait: str
    pgs_id: str
    scoring_method: str
    raw_score: float | None
    combined_score: float | None
    eur_component: float | None
    eas_component: float | None
    percentile: float | None
    reference_population: str | None
    reference_n: int | None
    confidence: str | None
    covered_fraction: float | None
    ancestry_warnings: list[str]
    pgs_training_pop: str | None
    pgs_training_pop_match: bool | None

    class Config:
        from_attributes = True


class InferenceStatusOut(BaseModel):
    status: str  # idle | running | complete | error
    message: str | None = None
    reference_panel_ready: bool = False
    pca_computed: bool = False
    classifier_ready: bool = False
    samples_inferred: int = 0


# ── Helpers ──────────────────────────────────────────────────

def _row_to_out(row: SampleAncestry) -> SampleAncestryOut:
    return SampleAncestryOut(
        sample_id=row.sample_id,
        proportions=AncestryProportions(
            EUR=row.eur_proportion or 0,
            EAS=row.eas_proportion or 0,
            AFR=row.afr_proportion or 0,
            SAS=row.sas_proportion or 0,
            AMR=row.amr_proportion or 0,
        ),
        primary_ancestry=row.primary_ancestry,
        is_admixed=row.is_admixed,
        admixture_description=row.admixture_description,
        pca_coordinates=[
            row.pc1 or 0, row.pc2 or 0, row.pc3 or 0, row.pc4 or 0, row.pc5 or 0,
            row.pc6 or 0, row.pc7 or 0, row.pc8 or 0, row.pc9 or 0, row.pc10 or 0,
        ],
        inference_method=row.inference_method,
    )


def _tag_ancestry(proportions: dict) -> tuple:
    """Returns (primary_ancestry, is_admixed, description)."""
    sorted_pops = sorted(proportions.items(), key=lambda x: -x[1])
    primary = sorted_pops[0][0]
    primary_frac = sorted_pops[0][1]

    if primary_frac >= 0.85:
        return primary, False, primary

    components = [(pop, frac) for pop, frac in sorted_pops if frac >= 0.10]
    desc = "/".join(pop for pop, _ in components)
    return primary, True, f"{desc} admixed"


# ── Pipeline status ──────────────────────────────────────────

_inference_status = {"status": "idle", "message": None}


def _check_pipeline_readiness() -> InferenceStatusOut:
    """Check what pipeline components are available."""
    ref_ready = (_REFERENCE_DIR / "integrated_call_samples_v3.20130502.ALL.panel").exists()
    pca_done = (_ANCESTRY_DIR / "ancestry_pca.eigenvec").exists()
    clf_ready = (_ANCESTRY_DIR / "ancestry_classifier.joblib").exists()

    db = SessionLocal()
    try:
        count = db.query(SampleAncestry).count()
    finally:
        db.close()

    return InferenceStatusOut(
        status=_inference_status["status"],
        message=_inference_status.get("message"),
        reference_panel_ready=ref_ready,
        pca_computed=pca_done,
        classifier_ready=clf_ready,
        samples_inferred=count,
    )


# ── Inference runner ─────────────────────────────────────────

def _run_inference_pipeline():
    """Background task: run the full ancestry inference pipeline."""
    global _inference_status
    _inference_status = {"status": "running", "message": "Starting ancestry inference..."}

    script = _SCRIPTS_DIR / "run_ancestry_inference.py"
    if not script.exists():
        _inference_status = {"status": "error", "message": f"Script not found: {script}"}
        return

    try:
        result = subprocess.run(
            ["python", str(script)],
            capture_output=True, text=True, timeout=7200,  # 2h timeout
            cwd=str(Path(__file__).parent.parent.parent),
        )
        if result.returncode == 0:
            _inference_status = {"status": "complete", "message": "Inference completed successfully"}
        else:
            _inference_status = {"status": "error", "message": result.stderr[-500:] if result.stderr else "Unknown error"}
    except subprocess.TimeoutExpired:
        _inference_status = {"status": "error", "message": "Pipeline timed out (>2h)"}
    except Exception as e:
        _inference_status = {"status": "error", "message": str(e)[:500]}


# ── API Endpoints ────────────────────────────────────────────

@router.get("/all")
async def get_all_ancestries() -> list[SampleAncestryOut]:
    """Returns ancestry for all samples."""
    db = SessionLocal()
    try:
        rows = db.query(SampleAncestry).all()
        return [_row_to_out(r) for r in rows]
    finally:
        db.close()


@router.get("/pca")
async def get_pca_plot_data() -> list[PCAPoint]:
    """Returns PC1/PC2/PC3 for all 1KG ref + our samples, labeled by superpop."""
    points = []

    # Our samples from DB
    db = SessionLocal()
    try:
        rows = db.query(SampleAncestry).all()
        for r in rows:
            points.append(PCAPoint(
                sample_id=r.sample_id,
                pc1=r.pc1 or 0, pc2=r.pc2 or 0, pc3=r.pc3 or 0,
                population=r.primary_ancestry,
                is_reference=False,
            ))
    finally:
        db.close()

    # 1KG reference PCA (pre-computed, stored as JSON)
    ref_pca_file = _ANCESTRY_DIR / "reference_pca_points.json"
    if ref_pca_file.exists():
        try:
            ref_data = json.loads(ref_pca_file.read_text())
            for p in ref_data:
                points.append(PCAPoint(
                    sample_id=p["sample_id"],
                    pc1=p["pc1"], pc2=p["pc2"], pc3=p.get("pc3", 0),
                    population=p["population"],
                    is_reference=True,
                ))
        except Exception:
            pass

    return points


@router.get("/status")
async def get_inference_status() -> InferenceStatusOut:
    """Check pipeline status and readiness."""
    return _check_pipeline_readiness()


@router.post("/run-inference")
async def run_inference(background_tasks: BackgroundTasks):
    """Trigger ancestry inference pipeline in the background."""
    global _inference_status
    if _inference_status["status"] == "running":
        raise HTTPException(400, "Inference pipeline is already running")

    background_tasks.add_task(_run_inference_pipeline)
    return {"status": "started", "message": "Ancestry inference pipeline started"}


@router.get("/samples/{sample_id}")
async def get_sample_ancestry(sample_id: str) -> SampleAncestryOut:
    """Returns ancestry for a specific sample."""
    db = SessionLocal()
    try:
        row = db.query(SampleAncestry).filter(SampleAncestry.sample_id == sample_id).first()
        if not row:
            raise HTTPException(404, f"No ancestry data for sample {sample_id}")
        return _row_to_out(row)
    finally:
        db.close()


@router.get("/scores")
async def get_ancestry_pgs_scores(
    sample_id: str | None = None,
    trait: str | None = None,
    pgs_id: str | None = None,
) -> list[AncestryPGSResultOut]:
    """Get ancestry-aware PGS results with optional filters."""
    db = SessionLocal()
    try:
        q = db.query(AncestryPGSResult)
        if sample_id:
            q = q.filter(AncestryPGSResult.sample_id == sample_id)
        if trait:
            q = q.filter(AncestryPGSResult.trait == trait)
        if pgs_id:
            q = q.filter(AncestryPGSResult.pgs_id == pgs_id)
        rows = q.all()
        return [AncestryPGSResultOut.from_orm(r) if hasattr(AncestryPGSResultOut, 'from_orm')
                else AncestryPGSResultOut.model_validate(r) for r in rows]
    finally:
        db.close()


@router.get("/confidence-summary")
async def get_confidence_summary():
    """Get per-sample confidence summary across all scored traits.
    Returns a map: {sample_id: {high: N, moderate: N, low: N}}"""
    db = SessionLocal()
    try:
        results = db.query(AncestryPGSResult).all()
        summary = {}
        for r in results:
            if r.sample_id not in summary:
                summary[r.sample_id] = {"high": 0, "moderate": 0, "low": 0}
            conf = r.confidence or "low"
            if conf in summary[r.sample_id]:
                summary[r.sample_id][conf] += 1
        return summary
    finally:
        db.close()


@router.get("/gwas-availability")
async def get_gwas_availability():
    """Check which GWAS summary stats are available for PRS-CSx."""
    sumstats_dir = Path(DATA_DIR) / "gwas_sumstats"
    availability = {}

    if sumstats_dir.exists():
        for trait_dir in sorted(sumstats_dir.iterdir()):
            if trait_dir.is_dir():
                pops = []
                for f in trait_dir.iterdir():
                    pop = f.stem.upper()
                    if pop in ("EUR", "EAS", "AFR", "SAS", "AMR"):
                        pops.append(pop)
                availability[trait_dir.name] = {
                    "populations": sorted(pops),
                    "prscsx_ready": len(pops) >= 2,
                }

    return availability


@router.post("/import-results")
async def import_ancestry_results(results: list[dict]):
    """Bulk import ancestry-aware PGS results (from pipeline scripts)."""
    db = SessionLocal()
    try:
        imported = 0
        for r in results:
            existing = db.query(AncestryPGSResult).filter(
                AncestryPGSResult.sample_id == r["sample_id"],
                AncestryPGSResult.pgs_id == r["pgs_id"],
                AncestryPGSResult.scoring_method == r["scoring_method"],
            ).first()

            if existing:
                for k, v in r.items():
                    if hasattr(existing, k):
                        setattr(existing, k, v)
            else:
                db.add(AncestryPGSResult(**r))
            imported += 1

        db.commit()
        return {"ok": True, "imported": imported}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))
    finally:
        db.close()
