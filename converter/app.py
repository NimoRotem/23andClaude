"""
nimog — BAM to VCF converter web application.

FastAPI backend with SSE progress streaming, file browser,
and job management. Supports bcftools (quick) and DeepVariant (full) modes.
"""

import asyncio
import json
import os
import time
import traceback
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from pipeline import (
    PipelineJob, PipelineMode, run_pipeline, resume_pipeline,
    detect_resume_point, save_job, ALL_AUTOSOMES, GPU_AVAILABLE,
)

PORT = 8720
APP_DIR = Path(__file__).parent
JOBS_DIR = APP_DIR / "jobs"
JOBS_DIR.mkdir(exist_ok=True)

ALLOWED_BROWSE_ROOTS = ["/data/", "/scratch/", "/home/nimo/"]

app = FastAPI()
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")

# In-memory job store
jobs: dict[str, PipelineJob] = {}


def load_saved_jobs():
    """Load completed/failed jobs from disk on startup."""
    for f in JOBS_DIR.glob("*.json"):
        try:
            data = json.loads(f.read_text())
            job = PipelineJob(
                job_id=data["job_id"],
                bam_path=data["bam_path"],
                output_dir=data["output_dir"],
            )
            job.status = data.get("status", "unknown")
            job.output_vcf = data.get("output_vcf")
            job.started_at = data.get("started_at")
            job.completed_at = data.get("completed_at")
            job.progress = data.get("progress", 0)
            job.step = data.get("step", "")
            job.phase = data.get("phase", 0)
            job.phase_name = data.get("phase_name", "")
            job.mode = data.get("mode", "bcftools")
            job.logs = data.get("logs", [])
            job.error = data.get("error")
            job.chroms_done = data.get("chroms_done", 0)
            job.chroms_completed = data.get("chroms_completed", [])
            job.total_chroms = data.get("total_chroms", 22)
            job.samples_done = data.get("samples_done", 0)
            job.total_samples = data.get("total_samples", 1)
            job.bam_paths = data.get("bam_paths", [])
            job.sample_names = data.get("sample_names", [])
            job.reference = data.get("reference", "/data/genom-nimo/reference.fasta")
            job.chromosomes = data.get("chromosomes", list(ALL_AUTOSOMES))
            job.use_gpu = data.get("use_gpu", True)
            job.dv_shards = data.get("dv_shards", 16)
            job.qual_filter = data.get("qual_filter", 30)
            job.min_dp = data.get("min_dp", 10)
            job.max_dp = data.get("max_dp", 1800)
            job.cores = data.get("cores", 8)
            # Mark interrupted jobs as failed
            if job.status == "running":
                job.status = "failed"
                job.error = "Job interrupted by server restart"
                job.log("Job interrupted by server restart")
                save_job(job, str(JOBS_DIR))
            jobs[job.job_id] = job
        except Exception:
            pass


class ConvertRequest(BaseModel):
    bam_path: str = ""
    bam_paths: list[str] = []
    sample_names: list[str] = []
    output_dir: str = "/scratch/nimog_output"
    reference: str = "/data/genom-nimo/reference.fasta"
    cores: int = 8
    min_base_qual: int = 20
    min_map_qual: int = 20
    max_depth: int = 5000
    qual_filter: int = 30
    min_dp: int = 10
    max_dp: int = 1800
    chromosomes: list[str] = list(ALL_AUTOSOMES)
    mode: str = "bcftools"
    dv_shards: int = 20
    use_gpu: bool = True


def is_path_safe(path: str) -> bool:
    """Prevent directory traversal outside allowed roots."""
    resolved = os.path.realpath(path)
    return any(resolved.startswith(root) for root in ALLOWED_BROWSE_ROOTS)


@app.on_event("startup")
async def startup():
    load_saved_jobs()


# Also load at import time for when nimog is mounted as a sub-app
# (sub-app startup events don't fire in FastAPI)
load_saved_jobs()


@app.get("/")
async def index():
    return FileResponse(str(APP_DIR / "static" / "index.html"))


@app.get("/api/gpu-status")
async def gpu_status():
    """Check if GPU is available."""
    gpu_name = ""
    if GPU_AVAILABLE:
        import subprocess
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader"],
                capture_output=True, text=True, timeout=5,
            )
            gpu_name = result.stdout.strip()
        except Exception:
            pass
    return {"available": GPU_AVAILABLE, "gpu": gpu_name}


def _scan_alignment_file(item: Path, display_name: Optional[str] = None) -> Optional[dict]:
    """Build a browse entry for a single .bam/.cram file. Returns None if
    the file isn't an alignment or is missing on disk."""
    name = item.name
    if not name.endswith((".bam", ".cram")):
        return None
    try:
        stat = item.stat()
    except OSError:
        return None
    has_index = False
    if name.endswith(".bam"):
        has_index = (item.parent / (name + ".bai")).exists() or \
                    (item.parent / name.replace(".bam", ".bai")).exists()
    elif name.endswith(".cram"):
        has_index = (item.parent / (name + ".crai")).exists()
    return {
        "name": display_name or name,
        "path": str(item),
        "is_dir": False,
        "size": stat.st_size,
        "has_index": has_index,
    }


@app.get("/api/browse")
async def browse_files(path: str = "/data/aligned_bams"):
    """Browse server filesystem for BAM/CRAM files.

    Special virtual path: `/data/uploads` returns a flat recursive listing of
    every .bam/.cram under `/data/uploads/users/*/` so files uploaded via the
    main genomics dashboard appear here without manual path navigation.
    """
    if not is_path_safe(path):
        return JSONResponse({"error": "Access denied"}, 403)

    # Virtual flat view of all uploaded alignment files
    if path.rstrip("/") == "/data/uploads":
        uploads_root = Path("/data/uploads/users")
        entries = []
        if uploads_root.is_dir():
            for user_dir in sorted(uploads_root.iterdir()):
                if not user_dir.is_dir():
                    continue
                # one level deep — every alignment file in each user dir
                for item in sorted(user_dir.iterdir()):
                    if item.is_file() and item.name.endswith((".bam", ".cram")):
                        entry = _scan_alignment_file(item)
                        if entry is None:
                            continue
                        # prefix display name with user-id slug so multiple
                        # users with the same filename are distinguishable
                        entry["name"] = f"{user_dir.name}/{item.name}"
                        entries.append(entry)
        return {
            "type": "directory",
            "path": "/data/uploads",
            "parent": "/data",
            "entries": entries,
            "virtual": True,
        }

    try:
        p = Path(path)
        if not p.exists():
            return JSONResponse({"error": "Path not found"}, 404)
        if p.is_file():
            return {"type": "file", "path": str(p), "size": p.stat().st_size}
        entries = []
        for item in sorted(p.iterdir()):
            try:
                stat = item.stat()
                is_dir = item.is_dir()
                name = item.name
                if not is_dir and not name.endswith((".bam", ".bam.bai", ".cram", ".cram.crai")):
                    continue
                if name.endswith((".bai", ".crai")):
                    continue
                size = stat.st_size if not is_dir else None
                has_index = False
                if name.endswith(".bam"):
                    has_index = (item.parent / (name + ".bai")).exists() or \
                                (item.parent / name.replace(".bam", ".bai")).exists()
                elif name.endswith(".cram"):
                    has_index = (item.parent / (name + ".crai")).exists()
                entries.append({
                    "name": name,
                    "path": str(item),
                    "is_dir": is_dir,
                    "size": size,
                    "has_index": has_index,
                })
            except PermissionError:
                continue

        # When viewing the default aligned-BAM directory, also surface any
        # alignment files uploaded via the genomics dashboard so users don't
        # have to know about /data/uploads. Each is shown with a [uploads]
        # tag in the display name and resolves to its real path.
        if path.rstrip("/") == "/data/aligned_bams":
            uploads_root = Path("/data/uploads/users")
            if uploads_root.is_dir():
                for user_dir in sorted(uploads_root.iterdir()):
                    if not user_dir.is_dir():
                        continue
                    for item in sorted(user_dir.iterdir()):
                        if item.is_file() and item.name.endswith((".bam", ".cram")):
                            entry = _scan_alignment_file(item)
                            if entry is None:
                                continue
                            entry["name"] = f"[uploads] {item.name}"
                            entries.append(entry)

        return {"type": "directory", "path": str(p), "parent": str(p.parent), "entries": entries}
    except PermissionError:
        return JSONResponse({"error": "Permission denied"}, 403)


@app.post("/api/convert")
async def start_convert(req: ConvertRequest):
    """Start a BAM/CRAM to VCF conversion job."""
    # Resolve alignment file paths
    bam_paths = req.bam_paths if req.bam_paths else ([req.bam_path] if req.bam_path else [])
    if not bam_paths:
        return JSONResponse({"error": "No alignment files specified"}, 400)

    for bam in bam_paths:
        if not os.path.exists(bam):
            return JSONResponse({"error": f"Alignment file not found: {bam}"}, 400)

    job_id = str(uuid.uuid4())[:8]
    job_output_dir = os.path.join(req.output_dir, job_id)

    job = PipelineJob(
        job_id=job_id,
        bam_path=bam_paths[0],
        bam_paths=bam_paths,
        sample_names=req.sample_names,
        output_dir=job_output_dir,
        reference=req.reference,
        cores=req.cores,
        min_base_qual=req.min_base_qual,
        min_map_qual=req.min_map_qual,
        max_depth=req.max_depth,
        qual_filter=req.qual_filter,
        min_dp=req.min_dp,
        max_dp=req.max_dp,
        chromosomes=req.chromosomes,
        total_chroms=len(req.chromosomes),
        mode=req.mode,
        dv_shards=req.dv_shards,
        use_gpu=req.use_gpu,
        total_samples=len(bam_paths),
    )
    jobs[job_id] = job

    def on_progress(j: PipelineJob):
        save_job(j, str(JOBS_DIR))

    async def run_safe():
        try:
            await run_pipeline(job, on_progress)
        except Exception as e:
            job.status = "failed"
            job.error = str(e)
            job.log(f"PIPELINE FAILED: {e}")
            job.log(traceback.format_exc())
            save_job(job, str(JOBS_DIR))

    asyncio.create_task(run_safe())
    return {"job_id": job_id}


@app.get("/api/jobs")
async def list_jobs():
    """List all jobs."""
    return [
        {
            "job_id": j.job_id,
            "status": j.status,
            "bam_path": os.path.basename(j.bam_path),
            "bam_count": len(j.bam_paths) if j.bam_paths else 1,
            "mode": j.mode,
            "progress": j.progress,
            "started_at": j.started_at,
            "step": j.step,
            "phase": j.phase,
            "phase_name": j.phase_name,
        }
        for j in sorted(jobs.values(), key=lambda x: x.started_at or 0, reverse=True)
    ]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get full job status."""
    job = jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "Job not found"}, 404)
    return job.to_dict()


@app.get("/api/jobs/{job_id}/stream")
async def stream_progress(job_id: str):
    """SSE endpoint for real-time progress updates."""
    async def event_generator():
        while True:
            job = jobs.get(job_id)
            if not job:
                yield f"event: error\ndata: {{\"error\": \"Job not found\"}}\n\n"
                break

            elapsed = time.time() - (job.started_at or time.time())
            eta = None

            if job.mode == "deepvariant":
                # ETA for DeepVariant mode
                import os as _os
                bam_path = job.bam_paths[0] if job.bam_paths else job.bam_path
                try:
                    bam_gb = _os.path.getsize(bam_path) / (1024**3)
                except Exception:
                    bam_gb = 50  # fallback estimate
                est_secs = bam_gb * (0.6 if getattr(job, 'use_gpu', True) else 2.5) * 60 * job.total_samples
                initial_remaining = max(0, est_secs - elapsed)
                if job.samples_done > 0 and job.step == "dv-calling":
                    per_sample = elapsed / job.samples_done
                    remaining = job.total_samples - job.samples_done
                    computed = per_sample * remaining
                    blend = min(1.0, job.samples_done / job.total_samples)
                    eta = (1 - blend) * initial_remaining + blend * computed
                elif job.step == "dv-calling" and job.progress > 0.01:
                    computed_total = elapsed / job.progress if job.progress > 0 else 0
                    computed_remaining = max(0, computed_total - elapsed)
                    blend = min(1.0, job.progress * 5)  # trust at 20%+
                    eta = (1 - blend) * initial_remaining + blend * computed_remaining
                    eta = min(eta, est_secs * 2)  # cap
                else:
                    eta = initial_remaining
            else:
                # ETA for bcftools mode
                if job.chroms_done > 0 and job.step == "calling":
                    per_chrom = elapsed / job.chroms_done
                    remaining_chroms = job.total_chroms - job.chroms_done
                    eta = per_chrom * remaining_chroms * 1.15
                elif job.step in ("concatenating", "normalizing", "filtering", "qc"):
                    eta = 60

            data = json.dumps({
                "status": job.status,
                "step": job.step,
                "phase": job.phase,
                "phase_name": job.phase_name,
                "mode": job.mode,
                "progress": job.progress,
                "chroms_done": job.chroms_done,
                "chroms_completed": job.chroms_completed,
                "total_chroms": job.total_chroms,
                "samples_done": job.samples_done,
                "total_samples": job.total_samples,
                "elapsed": elapsed,
                "eta": eta,
                "logs": job.logs[-30:],
                "error": job.error,
                "output_vcf": job.output_vcf,
                "qc_result": job.qc_result.to_dict() if job.qc_result else None,
            })
            yield f"event: progress\ndata: {data}\n\n"

            if job.status in ("completed", "failed"):
                break
            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/api/download/{job_id}")
async def download_vcf(job_id: str):
    """Download the final VCF file."""
    job = jobs.get(job_id)
    if not job or not job.output_vcf:
        return JSONResponse({"error": "VCF not available"}, 404)
    if not os.path.exists(job.output_vcf):
        return JSONResponse({"error": "VCF file missing from disk"}, 404)
    return FileResponse(
        job.output_vcf,
        filename=os.path.basename(job.output_vcf),
        media_type="application/gzip",
    )


@app.get("/api/download/{job_id}/qc")
async def download_qc(job_id: str):
    """Download the QC report JSON."""
    job = jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "Job not found"}, 404)
    qc_path = os.path.join(job.output_dir, "qc_report.json")
    if not os.path.exists(qc_path):
        return JSONResponse({"error": "QC report not available"}, 404)
    return FileResponse(qc_path, filename="qc_report.json", media_type="application/json")


@app.get("/api/jobs/{job_id}/resume-check")
async def check_resume(job_id: str):
    """Check if a failed job can be resumed and from which step."""
    job = jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "Job not found"}, 404)
    if job.status == "running":
        return {"resumable": False, "reason": "Job is still running"}
    if job.status == "completed":
        return {"resumable": False, "reason": "Job already completed"}

    resume_step = detect_resume_point(job)
    if not resume_step:
        return {"resumable": False, "reason": "No intermediate files found"}

    step_labels = {
        "concatenating": "Concatenation (per-chromosome VCFs found)",
        "normalizing": "Normalization (merged VCF found)",
        "filtering": "Filtering (normalized VCF found)",
        "qc": "QC Validation (final VCF found)",
        "dv-call_variants": "DeepVariant call_variants (make_examples tfrecords found)",
        "dv-postprocess": "DeepVariant postprocess (call_variants output found)",
        "dv-filter": "Filter & QC (DeepVariant VCFs found)",
    }
    return {
        "resumable": True,
        "resume_from": resume_step,
        "label": step_labels.get(resume_step, resume_step),
    }


@app.post("/api/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    """Resume a failed job from where it left off."""
    job = jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "Job not found"}, 404)
    if job.status == "running":
        return JSONResponse({"error": "Job is still running"}, 400)

    resume_step = detect_resume_point(job)
    if not resume_step:
        return JSONResponse({"error": "No intermediate files — need full rerun"}, 400)

    def on_progress(j: PipelineJob):
        save_job(j, str(JOBS_DIR))

    async def run_safe():
        try:
            await resume_pipeline(job, on_progress)
        except Exception as e:
            job.status = "failed"
            job.error = str(e)
            job.log(f"RESUME FAILED: {e}")
            job.log(traceback.format_exc())
            save_job(job, str(JOBS_DIR))

    asyncio.create_task(run_safe())
    return {"job_id": job_id, "resume_from": resume_step}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
