"""23andClaude Ancestry Inference API

FastAPI application that runs a full ancestry inference pipeline using
gnomAD HGDP+1kGP reference panel + plink2 + Rye.
"""

import asyncio
import bcrypt
import json
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

from fastapi import Cookie, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel

from pipeline import (
    APP_ROOT,
    REF_BED,
    POP2GROUP,
    PipelineError,
    JobTracker,
    detect_input_type,
    get_tracker,
    register_tracker,
    run_pipeline,
    unregister_tracker,
)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="23andClaude Ancestry Inference API",
    version="1.0.0",
    description="Ancestry inference pipeline using gnomAD HGDP+1kGP reference panel, plink2, and Rye.",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# User / Auth system
# ---------------------------------------------------------------------------

USERS_FILE = os.path.join(APP_ROOT, "users.json")
SESSIONS_FILE = os.path.join(APP_ROOT, "sessions.json")
SESSION_COOKIE = "ancestry_session"
SESSION_MAX_AGE = 30 * 24 * 3600  # 30 days

users_lock = threading.Lock()
sessions_lock = threading.Lock()

# In-memory stores, loaded from disk
_users: dict[str, dict] = {}    # user_id -> {id, email, password_hash, created_at}
_sessions: dict[str, dict] = {} # session_token -> {user_id, created_at}


def _load_users() -> None:
    global _users
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            data = json.load(f)
        _users = {u["id"]: u for u in data}


def _save_users() -> None:
    with open(USERS_FILE, "w") as f:
        json.dump(list(_users.values()), f, indent=2)
    os.chmod(USERS_FILE, 0o600)


def _load_sessions() -> None:
    global _sessions
    if os.path.exists(SESSIONS_FILE):
        with open(SESSIONS_FILE, "r") as f:
            _sessions = json.load(f)


def _save_sessions() -> None:
    with open(SESSIONS_FILE, "w") as f:
        json.dump(_sessions, f, indent=2)
    os.chmod(SESSIONS_FILE, 0o600)


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def _check_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())


def _find_user_by_email(email: str) -> Optional[dict]:
    email_lower = email.lower().strip()
    for u in _users.values():
        if u["email"].lower() == email_lower:
            return u
    return None


def _get_current_user(request: Request) -> Optional[dict]:
    """Extract user from session cookie. Returns None if not authed."""
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    with sessions_lock:
        sess = _sessions.get(token)
    if not sess:
        return None
    # Check expiry
    created = datetime.fromisoformat(sess["created_at"])
    if (datetime.now(timezone.utc) - created).total_seconds() > SESSION_MAX_AGE:
        with sessions_lock:
            _sessions.pop(token, None)
            _save_sessions()
        return None
    with users_lock:
        return _users.get(sess["user_id"])


def _require_user(request: Request) -> dict:
    """Like _get_current_user but raises 401 if not authenticated."""
    user = _get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


# Auth endpoints

class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/api/auth/login")
async def auth_login(req: LoginRequest):
    email = req.email.strip()
    password = req.password
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password required")

    user = _find_user_by_email(email)
    if not user or not _check_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # Create session
    token = secrets.token_urlsafe(48)
    with sessions_lock:
        _sessions[token] = {
            "user_id": user["id"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_sessions()

    response = JSONResponse({"ok": True, "user": {"id": user["id"], "email": user["email"]}})
    response.set_cookie(
        SESSION_COOKIE, token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=False,  # Set True if behind HTTPS
        path="/",
    )
    return response


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        with sessions_lock:
            _sessions.pop(token, None)
            _save_sessions()
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE, path="/")
    return response


@app.get("/api/auth/me")
async def auth_me(request: Request):
    user = _get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"id": user["id"], "email": user["email"]}


# ---------------------------------------------------------------------------
# In-memory job store + persistence
# ---------------------------------------------------------------------------

RESULTS_DIR = os.path.join(APP_ROOT, "results")
UPLOADS_DIR = os.path.join(APP_ROOT, "uploads")
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

# Jobs dict: job_id -> job dict
jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()


def _job_path(job_id: str) -> str:
    """Return the JSON persistence path for a job."""
    return os.path.join(RESULTS_DIR, f"{job_id}.json")


def _save_job(job: dict) -> None:
    """Persist a job to disk as JSON."""
    try:
        path = _job_path(job["id"])
        with open(path, "w") as f:
            json.dump(job, f, indent=2, default=str)
    except Exception:
        pass  # Best-effort persistence


def _load_jobs_from_disk() -> None:
    """Load all persisted jobs from disk into memory on startup.

    Jobs saved as "running" or "queued" are stale (the process restarted while
    they were in-flight) — mark them as failed so the UI doesn't show a
    perpetually-spinning progress bar.
    """
    if not os.path.isdir(RESULTS_DIR):
        return
    for fname in os.listdir(RESULTS_DIR):
        if not fname.endswith(".json"):
            continue
        try:
            with open(os.path.join(RESULTS_DIR, fname), "r") as f:
                job = json.load(f)
            if "id" not in job:
                continue
            # Mark stale in-flight jobs as failed
            if job.get("status") in ("running", "queued"):
                job["status"] = "failed"
                job["error"] = "Server restarted while job was in progress"
                job["completed_at"] = datetime.now(timezone.utc).isoformat()
                with open(os.path.join(RESULTS_DIR, fname), "w") as f:
                    json.dump(job, f, indent=2)
            jobs[job["id"]] = job
        except Exception:
            continue


def _make_job(sample_name: str, user_id: Optional[str] = None) -> dict:
    """Create a new job record."""
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    job = {
        "id": job_id,
        "sample_name": sample_name,
        "user_id": user_id,
        "status": "queued",
        "progress": 0,
        "current_step": "Queued",
        "result": None,
        "error": None,
        "created_at": now,
        "completed_at": None,
    }
    with jobs_lock:
        jobs[job_id] = job
    _save_job(job)
    return job


def _update_job(job_id: str, **kwargs) -> None:
    """Thread-safe update of job fields."""
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(kwargs)
            _save_job(jobs[job_id])


# ---------------------------------------------------------------------------
# Background pipeline runner
# ---------------------------------------------------------------------------


def _run_job(job_id: str, input_path: str, sample_name: str, fasta_path: Optional[str]) -> None:
    """Run the ancestry pipeline in a background thread."""
    tmpdir = None
    tracker = None
    try:
        _update_job(job_id, status="running", progress=0, current_step="Starting pipeline")

        # Detect input type
        input_type = detect_input_type(input_path)

        # Create JobTracker for live progress streaming
        tracker = JobTracker(input_type=input_type)
        tracker.status = "running"

        # Wire tracker updates to job store persistence
        def on_progress(pct: int, step: str) -> None:
            _update_job(job_id, progress=pct, current_step=step)

        tracker.on_update = on_progress
        register_tracker(job_id, tracker)

        # Create temp directory for pipeline work
        tmpdir = tempfile.mkdtemp(prefix=f"ancestry_{job_id[:8]}_")

        # Run the pipeline
        result = run_pipeline(
            sample_name=sample_name,
            input_path=input_path,
            input_type=input_type,
            tmpdir=tmpdir,
            fasta_path=fasta_path,
            on_progress=on_progress,
            tracker=tracker,
        )

        tracker.status = "complete"
        now = datetime.now(timezone.utc).isoformat()
        _update_job(
            job_id,
            status="complete",
            progress=100,
            current_step="Complete",
            result=result,
            completed_at=now,
        )

    except PipelineError as e:
        if tracker:
            tracker.status = "failed"
        now = datetime.now(timezone.utc).isoformat()
        _update_job(
            job_id,
            status="failed",
            current_step="Failed",
            error=str(e),
            completed_at=now,
        )

    except Exception as e:
        if tracker:
            tracker.status = "failed"
        now = datetime.now(timezone.utc).isoformat()
        tb = traceback.format_exc()
        _update_job(
            job_id,
            status="failed",
            current_step="Failed",
            error=f"{type(e).__name__}: {e}\n{tb}",
            completed_at=now,
        )

    finally:
        # Unregister tracker (keep it for a few seconds so final SSE events can be read)
        if tracker:
            tracker.status = tracker.status or "complete"
        # Clean up temp directory
        if tmpdir and os.path.isdir(tmpdir):
            try:
                shutil.rmtree(tmpdir)
            except Exception:
                pass
        # Delayed unregister to let SSE clients read final state
        def _delayed_unregister():
            time.sleep(10)
            unregister_tracker(job_id)
        threading.Thread(target=_delayed_unregister, daemon=True).start()


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


@app.on_event("startup")
async def startup_event():
    """Load persisted data on startup."""
    _load_users()
    _load_sessions()
    _load_jobs_from_disk()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "23andClaude Ancestry Inference API",
        "version": "1.0.0",
        "app_root": APP_ROOT,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/reference/status")
async def reference_status():
    """Check reference panel setup status."""
    checks = {}

    # Check reference BED/BIM/FAM
    for ext in [".bed", ".bim", ".fam"]:
        path = f"{REF_BED}{ext}"
        exists = os.path.exists(path)
        size = os.path.getsize(path) if exists else 0
        checks[f"ref_{ext.lstrip('.')}"] = {
            "path": path,
            "exists": exists,
            "size_mb": round(size / (1024 * 1024), 1) if exists else 0,
        }

    # Check pop2group.txt
    pop2group_exists = os.path.exists(POP2GROUP)
    pop2group_groups = 0
    if pop2group_exists:
        with open(POP2GROUP, "r") as f:
            groups = set()
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    parts = line.split()
                    if len(parts) >= 2:
                        groups.add(parts[1])
            pop2group_groups = len(groups)

    checks["pop2group"] = {
        "path": POP2GROUP,
        "exists": pop2group_exists,
        "groups": pop2group_groups,
    }

    # Check Python NNLS dependencies
    try:
        import numpy
        import scipy
        nnls_ok = True
        nnls_versions = f"numpy={numpy.__version__}, scipy={scipy.__version__}"
    except ImportError as e:
        nnls_ok = False
        nnls_versions = str(e)
    checks["nnls_deps"] = {
        "available": nnls_ok,
        "versions": nnls_versions,
    }

    # Check required tools
    tools = {}
    for tool in ["plink", "plink2", "bcftools", "tabix"]:
        tools[tool] = shutil.which(tool) is not None

    checks["tools"] = tools

    # Count variants in reference panel
    ref_variant_count = 0
    bim_path = f"{REF_BED}.bim"
    if os.path.exists(bim_path):
        try:
            with open(bim_path, "r") as f:
                ref_variant_count = sum(1 for _ in f)
        except Exception:
            pass

    checks["ref_variants"] = ref_variant_count

    all_ok = (
        all(checks[f"ref_{ext}"]["exists"] for ext in ["bed", "bim", "fam"])
        and pop2group_exists
        and nnls_ok
        and all(tools.values())
    )

    return {
        "ready": all_ok,
        "checks": checks,
    }


@app.get("/api/reference/detail")
async def reference_detail():
    """Detailed reference panel information including files, populations, and groups."""
    import glob
    from collections import Counter

    ref_dir = os.path.join(APP_ROOT, "reference")

    # List all files in reference directory
    files = []
    if os.path.isdir(ref_dir):
        for entry in sorted(os.listdir(ref_dir)):
            fpath = os.path.join(ref_dir, entry)
            if os.path.isfile(fpath):
                stat = os.stat(fpath)
                files.append({
                    "name": entry,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                })

    # Population counts from ref_pruned.fam
    populations = {}
    sample_count = 0
    fam_path = f"{REF_BED}.fam"
    if os.path.exists(fam_path):
        pop_counter = Counter()
        with open(fam_path, "r") as f:
            for line in f:
                parts = line.strip().split()
                if parts:
                    pop_counter[parts[0]] += 1
                    sample_count += 1
        populations = dict(pop_counter.most_common())

    # Group mapping from pop2group.txt
    groups = {}
    pop_to_group = {}
    if os.path.exists(POP2GROUP):
        with open(POP2GROUP, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("Pop") and "\t" in line:
                    parts = line.split("\t")
                    if len(parts) >= 2:
                        pop_to_group[parts[0]] = parts[1]
        # Count samples per group
        group_counter = Counter()
        for pop, count in populations.items():
            grp = pop_to_group.get(pop, "Unknown")
            group_counter[grp] += count
        groups = dict(group_counter.most_common())

    # Variant count
    variant_count = 0
    bim_path = f"{REF_BED}.bim"
    if os.path.exists(bim_path):
        with open(bim_path, "r") as f:
            variant_count = sum(1 for _ in f)

    # Total reference directory size
    total_size_gb = 0
    if os.path.isdir(ref_dir):
        total_size_gb = round(sum(
            os.path.getsize(os.path.join(ref_dir, f))
            for f in os.listdir(ref_dir)
            if os.path.isfile(os.path.join(ref_dir, f))
        ) / (1024**3), 2)

    # Tool versions
    tool_versions = {}
    for tool, cmd in [
        ("plink", "plink --version 2>&1 | head -1"),
        ("plink2", "plink2 --version 2>&1 | head -1"),
        ("bcftools", "bcftools --version 2>&1 | head -1"),
        ("Rscript", "Rscript --version 2>&1"),
        ("tabix", "tabix --version 2>&1 | head -1"),
    ]:
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
            ver = (result.stdout.strip() or result.stderr.strip()).split("\n")[0][:80]
            tool_versions[tool] = ver if ver else None
        except Exception:
            tool_versions[tool] = None

    return {
        "files": files,
        "populations": populations,
        "groups": groups,
        "stats": {
            "variant_count": variant_count,
            "sample_count": sample_count,
            "population_count": len(populations),
            "group_count": len(groups),
            "total_size_gb": total_size_gb,
        },
        "tool_versions": tool_versions,
    }


SAMPLE_DIRS = [
    "/data/aligned_bams",
    os.path.join(APP_ROOT, "uploads"),
]

# Also scan for VCF/gVCF files in nimog output directories
_NIMOG_ROOT = "/scratch/nimog_output"
if os.path.isdir(_NIMOG_ROOT):
    for _run_dir in os.listdir(_NIMOG_ROOT):
        _dv_dir = os.path.join(_NIMOG_ROOT, _run_dir, "dv")
        if os.path.isdir(_dv_dir):
            SAMPLE_DIRS.append(_dv_dir)
SUPPORTED_EXTENSIONS = {".vcf", ".vcf.gz", ".g.vcf", ".g.vcf.gz", ".gvcf", ".gvcf.gz", ".bam", ".cram"}


def _is_supported_file(filename: str) -> bool:
    """Check if a filename has a supported genomic extension."""
    lower = filename.lower()
    for ext in SUPPORTED_EXTENSIONS:
        if lower.endswith(ext):
            return True
    return False


@app.get("/api/server-files")
async def list_server_files(request: Request):
    """List genomic files available on the server for analysis.
    All authenticated users see all files."""
    _require_user(request)
    files = []
    for dirpath in SAMPLE_DIRS:
        if not os.path.isdir(dirpath):
            continue
        for entry in sorted(os.listdir(dirpath)):
            if not _is_supported_file(entry):
                continue
            fpath = os.path.join(dirpath, entry)
            if not os.path.isfile(fpath):
                continue
            stat = os.stat(fpath)
            # Derive sample name from filename (strip extensions)
            name = entry
            for ext in [".g.vcf.gz", ".gvcf.gz", ".vcf.gz", ".g.vcf", ".gvcf", ".vcf", ".bam", ".cram"]:
                if name.lower().endswith(ext):
                    name = name[: len(name) - len(ext)]
                    break
            files.append({
                "path": fpath,
                "name": entry,
                "sample_name": name,
                "size_mb": round(stat.st_size / (1024 * 1024), 1),
                "dir": dirpath,
            })
    return {"files": files}


DEFAULT_FASTA = "/data/genom-nimo/reference.fasta"


@app.post("/api/analyze/batch")
async def batch_analysis(request: Request):
    """Start ancestry analysis on all server-side sample files that haven't been analyzed yet."""
    user = _require_user(request)
    # List available files
    all_files = []
    for dirpath in SAMPLE_DIRS:
        if not os.path.isdir(dirpath):
            continue
        for entry in sorted(os.listdir(dirpath)):
            if not _is_supported_file(entry):
                continue
            fpath = os.path.join(dirpath, entry)
            if not os.path.isfile(fpath):
                continue
            name = entry
            for ext in [".g.vcf.gz", ".gvcf.gz", ".vcf.gz", ".g.vcf", ".gvcf", ".vcf", ".bam", ".cram"]:
                if name.lower().endswith(ext):
                    name = name[: len(name) - len(ext)]
                    break
            all_files.append({"path": fpath, "sample_name": name})

    if not all_files:
        raise HTTPException(status_code=400, detail="No sample files found on server")

    # Check which samples this user already has completed jobs for
    with jobs_lock:
        completed_names = {
            j["sample_name"]
            for j in jobs.values()
            if j.get("status") == "complete" and j.get("result") and j.get("user_id") == user["id"]
        }

    queued = []
    skipped = []
    for sf in all_files:
        if sf["sample_name"] in completed_names:
            skipped.append(sf["sample_name"])
            continue

        input_type = detect_input_type(sf["path"])
        fasta = DEFAULT_FASTA if input_type in ("bam", "cram") else None
        if fasta and not os.path.exists(fasta):
            fasta = None

        job = _make_job(sf["sample_name"], user_id=user["id"])
        thread = threading.Thread(
            target=_run_job,
            args=(job["id"], sf["path"], sf["sample_name"], fasta),
            daemon=True,
        )
        thread.start()
        queued.append({"job_id": job["id"], "sample_name": sf["sample_name"]})

    return {"queued": queued, "skipped": skipped, "total_queued": len(queued), "total_skipped": len(skipped)}


@app.post("/api/analyze")
async def start_analysis(
    request: Request,
    sample_name: str = Form(...),
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    fasta_path: Optional[str] = Form(None),
):
    """
    Start an ancestry analysis job.

    Accepts either a file upload or a server-local file path.
    Returns a job_id immediately; pipeline runs in background.
    """
    user = _require_user(request)

    # Validate input source
    if file is None and not file_path:
        raise HTTPException(
            status_code=400,
            detail="Either 'file' (upload) or 'file_path' (server-local path) must be provided.",
        )

    # Determine input path
    if file is not None:
        # Save uploaded file to uploads directory
        safe_name = file.filename.replace("/", "_").replace("\\", "_") if file.filename else "upload"
        upload_id = str(uuid.uuid4())[:8]
        upload_filename = f"{upload_id}_{safe_name}"
        upload_path = os.path.join(UPLOADS_DIR, upload_filename)

        try:
            contents = await file.read()
            with open(upload_path, "wb") as f:
                f.write(contents)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to save uploaded file: {e}",
            )

        input_path = upload_path
    else:
        # Server-local path — restrict to allowed directories
        ALLOWED_DIRS = ["/data/", "/scratch/", os.path.join(APP_ROOT, "uploads")]
        resolved = os.path.realpath(file_path)
        if not any(resolved.startswith(d) for d in ALLOWED_DIRS):
            raise HTTPException(
                status_code=403,
                detail="File path must be within /data/ or the uploads directory.",
            )
        if not os.path.exists(resolved):
            raise HTTPException(
                status_code=400,
                detail=f"Server-local file not found: {file_path}",
            )
        input_path = resolved

    # Validate file type
    try:
        input_type = detect_input_type(input_path)
    except PipelineError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Validate FASTA for BAM/CRAM
    if input_type in ("bam", "cram") and not fasta_path:
        raise HTTPException(
            status_code=400,
            detail="fasta_path is required for BAM/CRAM input files.",
        )
    if fasta_path and not os.path.exists(fasta_path):
        raise HTTPException(
            status_code=400,
            detail=f"Reference FASTA not found: {fasta_path}",
        )

    # Create job with user association
    job = _make_job(sample_name, user_id=user["id"])
    job_id = job["id"]

    # Start background thread
    thread = threading.Thread(
        target=_run_job,
        args=(job_id, input_path, sample_name, fasta_path),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "status": "queued"}


# ---------------------------------------------------------------------------
# Remote file fetch (HTTP / Google Drive)
# ---------------------------------------------------------------------------

# Drive URL patterns: drive.google.com/file/d/{id}/...,
# drive.google.com/open?id={id}, drive.google.com/uc?id={id},
# docs.google.com/uc?id={id}
_GDRIVE_HOST_RE = re.compile(r"(?:^|\.)(?:drive|docs)\.google\.com$", re.IGNORECASE)
_GDRIVE_ID_RE = re.compile(r"/file/d/([a-zA-Z0-9_-]{10,})")


def _is_gdrive_url(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    return bool(_GDRIVE_HOST_RE.search(host))


def _sanitize_filename(name: str) -> str:
    """Strip path separators and dangerous chars; cap length."""
    name = unquote(name or "").strip()
    name = name.replace("\x00", "")
    # Drop any path components
    name = os.path.basename(name)
    # Replace anything that's not safe
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    # Forbid leading dots (no hidden files)
    name = name.lstrip(".") or "download"
    # Cap length
    return name[:200]


def _filename_from_response(resp, fallback_url: str) -> str:
    """Extract a filename from Content-Disposition or URL path."""
    cd = resp.headers.get("content-disposition", "")
    # filename*=UTF-8''... (RFC 5987) takes priority
    m = re.search(r"filename\*\s*=\s*[^']*''([^;]+)", cd, re.IGNORECASE)
    if not m:
        m = re.search(r'filename\s*=\s*"([^"]+)"', cd, re.IGNORECASE)
    if not m:
        m = re.search(r"filename\s*=\s*([^;]+)", cd, re.IGNORECASE)
    if m:
        return _sanitize_filename(m.group(1))
    # Fall back to URL path basename
    path = urlparse(fallback_url).path
    return _sanitize_filename(os.path.basename(path) or "download")


def _unique_upload_path(filename: str) -> str:
    """Return UPLOADS_DIR/<8char>_<filename>, ensured to stay inside UPLOADS_DIR."""
    safe = _sanitize_filename(filename)
    short = uuid.uuid4().hex[:8]
    candidate = os.path.join(UPLOADS_DIR, f"{short}_{safe}")
    resolved = os.path.realpath(candidate)
    uploads_real = os.path.realpath(UPLOADS_DIR)
    if not resolved.startswith(uploads_real + os.sep):
        raise ValueError("Resolved path escapes uploads directory")
    return candidate


def _fetch_http(url: str, dest_path: str, job_id: str, requested_filename: Optional[str]) -> str:
    """Stream-download an HTTP(S) URL to dest_path with progress updates.

    Returns the final on-disk path (may differ from dest_path if the server
    suggested a filename via Content-Disposition and no name was requested).
    """
    import requests  # local import keeps cold-start cheap

    headers = {"User-Agent": "23andClaude-Ancestry/1.0 (+remote-fetch)"}
    with requests.get(url, stream=True, headers=headers, timeout=(15, 600), allow_redirects=True) as r:
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} from {url}")

        # If caller didn't pin a filename, take it from response
        final_path = dest_path
        if not requested_filename:
            inferred = _filename_from_response(r, url)
            if inferred:
                final_path = _unique_upload_path(inferred)

        total = int(r.headers.get("content-length") or 0)
        downloaded = 0
        last_pct = -1
        last_emit = 0.0

        with open(final_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)

                now = time.monotonic()
                if total > 0:
                    pct = int(downloaded * 100 / total)
                    if pct != last_pct and (now - last_emit) >= 0.5:
                        _update_job(
                            job_id,
                            progress=min(pct, 99),
                            current_step=f"Downloading {downloaded / (1024*1024):.1f} / {total / (1024*1024):.1f} MB",
                        )
                        last_pct = pct
                        last_emit = now
                else:
                    if (now - last_emit) >= 1.0:
                        _update_job(
                            job_id,
                            current_step=f"Downloading {downloaded / (1024*1024):.1f} MB",
                        )
                        last_emit = now

    return final_path


def _fetch_gdrive(url: str, job_id: str, requested_filename: Optional[str]) -> str:
    """Download a public Google Drive file via gdown.

    gdown handles the large-file confirmation token automatically.
    Progress is reported by polling the destination file size in a
    watcher thread (gdown's quiet mode hides its own bar).
    """
    import gdown  # local import

    # gdown.download(fuzzy=True) accepts the share-link form and figures out
    # the file id. We pass output as a directory so gdown picks the metadata
    # filename, then locate the resulting file.
    work_dir = tempfile.mkdtemp(prefix=f"gdrive_{job_id[:8]}_", dir=UPLOADS_DIR)
    stop_watcher = threading.Event()

    def _watcher():
        """Poll the work_dir size every second and surface progress."""
        last_size = -1
        while not stop_watcher.is_set():
            try:
                total = 0
                for entry in os.listdir(work_dir):
                    p = os.path.join(work_dir, entry)
                    if os.path.isfile(p):
                        total += os.path.getsize(p)
                if total != last_size:
                    _update_job(
                        job_id,
                        current_step=f"Downloading from Google Drive ({total / (1024*1024):.1f} MB)",
                    )
                    last_size = total
            except Exception:
                pass
            stop_watcher.wait(1.0)

    watcher_thread = threading.Thread(target=_watcher, daemon=True)
    watcher_thread.start()

    try:
        # gdown returns the path it wrote to, or None on failure.
        # output ending in os.sep tells gdown to use it as a directory.
        result_path = gdown.download(
            url=url,
            output=work_dir + os.sep,
            quiet=True,
            fuzzy=True,
        )
    finally:
        stop_watcher.set()
        watcher_thread.join(timeout=2)

    if not result_path or not os.path.exists(result_path):
        # gdown sometimes returns None even on success — fall back to scan
        files = [
            os.path.join(work_dir, n)
            for n in os.listdir(work_dir)
            if os.path.isfile(os.path.join(work_dir, n))
        ]
        if not files:
            shutil.rmtree(work_dir, ignore_errors=True)
            raise RuntimeError(
                "gdown returned no file. The Drive link must be public "
                "('Anyone with the link') and not be a folder."
            )
        result_path = max(files, key=os.path.getmtime)

    # Move the downloaded file out of the temp work_dir into UPLOADS_DIR
    # with a unique safe name.
    src_basename = os.path.basename(result_path)
    chosen_name = requested_filename or src_basename
    final_path = _unique_upload_path(chosen_name)
    shutil.move(result_path, final_path)
    shutil.rmtree(work_dir, ignore_errors=True)
    return final_path


def _run_remote_fetch(job_id: str, url: str, requested_filename: Optional[str]) -> None:
    """Background worker: download a remote URL into UPLOADS_DIR."""
    try:
        _update_job(job_id, status="running", progress=0, current_step="Resolving URL")

        if _is_gdrive_url(url):
            final_path = _fetch_gdrive(url, job_id, requested_filename)
        else:
            initial_name = _sanitize_filename(requested_filename) if requested_filename else "download"
            dest_path = _unique_upload_path(initial_name)
            final_path = _fetch_http(url, dest_path, job_id, requested_filename)

        size_bytes = os.path.getsize(final_path)
        _update_job(
            job_id,
            status="complete",
            progress=100,
            current_step="Complete",
            result={
                "path": final_path,
                "filename": os.path.basename(final_path),
                "size_bytes": size_bytes,
                "size_mb": round(size_bytes / (1024 * 1024), 2),
                "source_url": url,
            },
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as e:
        _update_job(
            job_id,
            status="failed",
            current_step="Failed",
            error=f"{type(e).__name__}: {e}",
            completed_at=datetime.now(timezone.utc).isoformat(),
        )


class RemoteFetchRequest(BaseModel):
    url: str
    filename: Optional[str] = None


@app.post("/api/fetch-remote")
async def fetch_remote(req: RemoteFetchRequest, request: Request):
    """Download a public remote file (HTTP/HTTPS or Google Drive) into the
    uploads directory in the background.

    Body:
      - url: HTTPS link, or a public Google Drive share link.
      - filename: optional override for the saved filename.

    Returns: {job_id, status}. Poll GET /api/jobs/{job_id} for progress.
    On completion, job.result contains {path, filename, size_bytes, ...}.
    """
    user = _require_user(request)

    url = (req.url or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="url is required")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="Only http/https URLs are allowed")
    if not parsed.netloc:
        raise HTTPException(status_code=400, detail="URL is missing a host")

    # Reuse the existing job machinery so the same /api/jobs/{id} endpoint works.
    # The "sample_name" slot doubles as a human label for the fetch job.
    label = req.filename or os.path.basename(parsed.path) or parsed.netloc
    job = _make_job(_sanitize_filename(label), user_id=user["id"])
    job_id = job["id"]

    threading.Thread(
        target=_run_remote_fetch,
        args=(job_id, url, req.filename),
        daemon=True,
    ).start()

    return {"job_id": job_id, "status": "queued"}


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str, request: Request):
    """Get job status and results. Users can only view their own jobs."""
    user = _require_user(request)

    with jobs_lock:
        job = jobs.get(job_id)

    if job is None:
        # Try loading from disk
        path = _job_path(job_id)
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    job = json.load(f)
                with jobs_lock:
                    jobs[job_id] = job
            except Exception:
                raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        else:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    # Check ownership
    if job.get("user_id") and job["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    # Normalize key name for frontend
    result = dict(job)
    result["job_id"] = result.pop("id", job_id)
    return result


@app.get("/api/jobs/{job_id}/stream")
async def stream_job_progress(job_id: str, request: Request):
    """Stream job progress via Server-Sent Events (SSE).

    Uses JobTracker.snapshot() for rich state when available (live_line,
    step timings, sub-tasks, log_lines). Falls back to basic job dict.
    Sends events every ~2s to match user's "update every 2-3 seconds" request.
    """
    user = _require_user(request)

    with jobs_lock:
        job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    if job.get("user_id") and job["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    async def event_generator():
        last_hash = ""
        while True:
            # Try rich tracker first
            tracker = get_tracker(job_id)
            if tracker:
                snap = tracker.snapshot()
                # Add job-level status
                with jobs_lock:
                    j = jobs.get(job_id)
                if j:
                    snap["status"] = j.get("status", "running")
                    if j.get("error"):
                        snap["error"] = j["error"]
            else:
                # Fallback to basic job dict
                with jobs_lock:
                    j = jobs.get(job_id)
                if j is None:
                    yield f"data: {json.dumps({'status': 'not_found'})}\n\n"
                    break
                snap = {
                    "progress": j.get("progress", 0),
                    "current_step": j.get("current_step", ""),
                    "status": j.get("status", "queued"),
                    "live_line": "",
                    "indeterminate": False,
                    "log_lines": [],
                    "steps": [],
                    "sub_tasks": {},
                    "step_elapsed_s": 0,
                }
                if j.get("error"):
                    snap["error"] = j["error"]

            # Always emit (frontend uses live_line which changes frequently)
            snap_hash = f"{snap.get('progress')}:{snap.get('current_step')}:{snap.get('live_line', '')}"
            if snap_hash != last_hash:
                yield f"data: {json.dumps(snap)}\n\n"
                last_hash = snap_hash

            status = snap.get("status", "queued")
            if status in ("complete", "failed"):
                yield f"data: {json.dumps(snap)}\n\n"
                break

            await asyncio.sleep(2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/jobs")
async def list_jobs(request: Request):
    """List current user's jobs, most recent first."""
    user = _require_user(request)

    with jobs_lock:
        all_jobs = [j for j in jobs.values() if j.get("user_id") == user["id"]]

    # Sort by created_at descending
    all_jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)

    # Return summary (omit full result for list view to keep response small)
    summaries = []
    for job in all_jobs:
        summary = {
            "job_id": job["id"],
            "sample_name": job["sample_name"],
            "status": job["status"],
            "progress": job["progress"],
            "current_step": job["current_step"],
            "created_at": job["created_at"],
            "completed_at": job.get("completed_at"),
            "error": job.get("error"),
            "has_result": job.get("result") is not None,
        }
        # Include a compact summary of the result if available
        if job.get("result"):
            result = job["result"]
            summary["result_summary"] = {
                "primary": result.get("primary"),
                "primary_pct": result.get("primary_pct"),
                "is_admixed": result.get("is_admixed"),
                "variants_used": result.get("variants_used"),
                "proportions": result.get("proportions"),
            }
        summaries.append(summary)

    return {"jobs": summaries, "total": len(summaries)}


@app.get("/api/jobs/compare")
async def compare_jobs(ids: str, request: Request):
    """Compare ancestry results across multiple jobs. Pass comma-separated job IDs."""
    user = _require_user(request)
    job_ids = [j.strip() for j in ids.split(",") if j.strip()]
    if len(job_ids) < 2:
        raise HTTPException(status_code=400, detail="At least 2 job IDs required for comparison")

    results = []
    for jid in job_ids:
        with jobs_lock:
            job = jobs.get(jid)
        if job is None:
            path = _job_path(jid)
            if os.path.exists(path):
                with open(path) as f:
                    job = json.load(f)
            else:
                raise HTTPException(status_code=404, detail=f"Job not found: {jid}")
        if job.get("user_id") and job["user_id"] != user["id"]:
            raise HTTPException(status_code=404, detail=f"Job not found: {jid}")
        if not job.get("result"):
            raise HTTPException(status_code=400, detail=f"Job {jid} has no results yet")
        results.append({
            "job_id": job["id"],
            "sample_name": job["result"]["sample_name"],
            "proportions": job["result"]["proportions"],
            "primary": job["result"]["primary"],
            "primary_pct": job["result"]["primary_pct"],
            "is_admixed": job["result"]["is_admixed"],
            "flags": job["result"].get("flags", []),
            "pca": job["result"].get("pca"),
        })

    return {"comparisons": results}


@app.get("/api/jobs/{job_id}/csv")
async def export_job_csv(job_id: str, request: Request):
    """Export a job's ancestry results as CSV."""
    user = _require_user(request)

    with jobs_lock:
        job = jobs.get(job_id)

    if job is None:
        path = _job_path(job_id)
        if os.path.exists(path):
            with open(path) as f:
                job = json.load(f)
        else:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    if job.get("user_id") and job["user_id"] != user["id"]:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    result = job.get("result")
    if not result:
        raise HTTPException(status_code=400, detail="Job has no results yet")

    import csv
    import io

    buf = io.StringIO()
    buf.write("\ufeff")  # UTF-8 BOM for Excel
    writer = csv.writer(buf)

    # Header section
    writer.writerow(["23andClaude Ancestry Report"])
    writer.writerow(["Sample", result.get("sample_name", "")])
    writer.writerow(["Panel", result.get("panel", "")])
    writer.writerow(["Variants Used", result.get("variants_used", "")])
    writer.writerow(["Primary Ancestry", result.get("primary", "")])
    writer.writerow(["Primary %", result.get("primary_pct", "")])
    writer.writerow(["Admixed", "Yes" if result.get("is_admixed") else "No"])
    writer.writerow([])

    # Group-level proportions
    writer.writerow(["Group", "Proportion"])
    for group, val in sorted(result.get("proportions", {}).items(), key=lambda x: -x[1]):
        writer.writerow([group, round(val * 100, 2)])
    writer.writerow([])

    # Population-level proportions
    pop_props = result.get("pop_proportions", {})
    if pop_props:
        writer.writerow(["Population", "Proportion"])
        for pop, val in sorted(pop_props.items(), key=lambda x: -x[1]):
            writer.writerow([pop, round(val * 100, 2)])
        writer.writerow([])

    # Detected populations
    detected = result.get("detected_populations", [])
    if detected:
        writer.writerow(["Detected Population", "Proportion", "Confidence"])
        for dp in detected:
            writer.writerow([dp.get("label", ""), round(dp.get("proportion", 0) * 100, 1), dp.get("confidence", "")])
        writer.writerow([])

    # Flags
    flags = result.get("flags", [])
    if flags:
        writer.writerow(["Flags"])
        for f in flags:
            writer.writerow([f])
        writer.writerow([])

    # ROH
    roh = result.get("roh")
    if roh:
        writer.writerow(["ROH Metric", "Value"])
        writer.writerow(["Total ROH (Mb)", roh.get("total_mb", "")])
        writer.writerow(["Segments", roh.get("n_segments", "")])
        writer.writerow(["Avg Segment (kb)", roh.get("avg_kb", "")])
        writer.writerow(["Bottleneck", "Yes" if roh.get("bottleneck") else "No"])

    filename = f"{result.get('sample_name', 'ancestry')}_results.csv"
    return PlainTextResponse(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/all-csv")
async def export_all_csv(request: Request):
    """Export all of this user's completed analyses as a single comparison CSV."""
    user = _require_user(request)

    with jobs_lock:
        completed = [
            j for j in jobs.values()
            if j.get("status") == "complete" and j.get("result") and j.get("user_id") == user["id"]
        ]

    if not completed:
        raise HTTPException(status_code=400, detail="No completed analyses to export")

    import csv
    import io

    # Collect all groups across all results
    all_groups = set()
    for j in completed:
        all_groups.update(j["result"].get("proportions", {}).keys())
    all_groups = sorted(all_groups)

    buf = io.StringIO()
    buf.write("\ufeff")
    writer = csv.writer(buf)

    header = ["Sample", "Primary", "Primary %", "Admixed", "Variants"] + all_groups
    writer.writerow(header)

    for j in sorted(completed, key=lambda x: x.get("sample_name", "")):
        r = j["result"]
        row = [
            r.get("sample_name", ""),
            r.get("primary", ""),
            r.get("primary_pct", ""),
            "Yes" if r.get("is_admixed") else "No",
            r.get("variants_used", ""),
        ]
        for g in all_groups:
            val = r.get("proportions", {}).get(g, 0)
            row.append(round(val * 100, 2) if val > 0.005 else "")
        writer.writerow(row)

    return PlainTextResponse(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="ancestry_comparison.csv"'},
    )


@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str, request: Request):
    """Delete a job and its persisted data. Users can only delete their own jobs."""
    user = _require_user(request)

    with jobs_lock:
        job = jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        if job.get("user_id") and job["user_id"] != user["id"]:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        jobs.pop(job_id, None)

    # Remove persisted file
    path = _job_path(job_id)
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

    return {"deleted": job_id}


# ---------------------------------------------------------------------------
# Serve frontend SPA
# ---------------------------------------------------------------------------

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

_frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
if _frontend_dist.exists():
    app.mount("/assets", StaticFiles(directory=str(_frontend_dist / "assets")), name="static-assets")

    @app.get("/{filename:path}")
    async def serve_frontend(filename: str):
        filepath = _frontend_dist / filename
        if filepath.exists() and filepath.is_file():
            return FileResponse(str(filepath))
        return FileResponse(str(_frontend_dist / "index.html"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8700))
    host = os.environ.get("HOST", "0.0.0.0")

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=False,
        workers=1,  # Single worker since we use in-memory job store
    )
