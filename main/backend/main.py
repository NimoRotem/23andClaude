from pathlib import Path
"""23andClaude — main FastAPI application."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.config import APP_DIR
from backend.database import init_db, migrate_user_columns_and_default_user

from backend.api.auth import router as auth_router, ensure_default_admin
from backend.api.admin import router as admin_router
from backend.api.vcfs import router as vcfs_router
from backend.api.pgs import router as pgs_router
from backend.api.runs import router as runs_router
from backend.api.storage import router as storage_router
from backend.api.files import router as files_router
from backend.api.chat import router as chat_router
from backend.api.system import router as system_router
from backend.api.ancestry import router as ancestry_router
from backend.api.reports import router as reports_router
from backend.api.checklist import router as checklist_router


def _cleanup_orphaned_runs():
    """Mark any 'scoring'/'downloading'/'created' runs as failed on startup.
    These were interrupted by a server restart."""
    from backend.database import SessionLocal
    from backend.models.schemas import ScoringRun
    from datetime import datetime, timezone
    db = SessionLocal()
    try:
        orphaned = db.query(ScoringRun).filter(
            ScoringRun.status.in_(["scoring", "downloading", "created"])
        ).all()
        for run in orphaned:
            run.status = "failed"
            run.error_message = "Interrupted by server restart"
            run.completed_at = datetime.now(timezone.utc)
        if orphaned:
            db.commit()
            print(f"Cleaned up {len(orphaned)} orphaned run(s)")
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    APP_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    ensure_default_admin()
    migrate_user_columns_and_default_user()
    _cleanup_orphaned_runs()
    yield


app = FastAPI(
    title="23andClaude",
    description="Personal genomics scoring platform",
    version="1.0.0",
    lifespan=lifespan,
    redirect_slashes=False,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes at /api (primary)
_api_routers = [
    (auth_router, "/auth", ["auth"]),
    (admin_router, "/admin", ["admin"]),
    (vcfs_router, "/vcfs", ["vcfs"]),
    (pgs_router, "/pgs", ["pgs"]),
    (runs_router, "/runs", ["runs"]),
    (storage_router, "/storage", ["storage"]),
    (files_router, "/files", ["files"]),
    (chat_router, "/chat", ["chat"]),
    (system_router, "/system", ["system"]),
    (ancestry_router, "/ancestry", ["ancestry"]),
    (reports_router, "/reports", ["reports"]),
    (checklist_router, "/checklist", ["checklist"]),
]
for _router, _path, _tags in _api_routers:
    app.include_router(_router, prefix=f"/api{_path}", tags=_tags)
    # Also mount under /genomics/api/ for frontend compatibility
    app.include_router(_router, prefix=f"/genomics/api{_path}", tags=_tags)


# Mount nimog BAM-to-VCF converter as sub-app at /nimog
import sys as _sys
_nimog_dir = Path(__file__).parent.parent / "nimog"
if _nimog_dir.exists():
    _sys.path.insert(0, str(_nimog_dir))
    import importlib as _il
    _nimog_mod = _il.import_module("app")
    app.mount("/nimog", _nimog_mod.app)
    _sys.path.remove(str(_nimog_dir))
    # Clean up module name to avoid conflicts
    _sys.modules["nimog_app"] = _sys.modules.pop("app", _nimog_mod)

# Frontend dist directory
_frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"

# Serve frontend assets under /genomics/assets (frontend built with base="/genomics/")
if _frontend_dist.exists() and (_frontend_dist / "assets").exists():
    app.mount("/genomics/assets", StaticFiles(directory=str(_frontend_dist / "assets")), name="genomics-static-assets")

# Redirect old /genomics/ paths to root (assets are handled by the mount above)
from fastapi.responses import RedirectResponse

@app.get("/genomics")
@app.get("/genomics/{rest:path}")
async def redirect_old_genomics(rest: str = ""):
    return RedirectResponse(url=f"/{rest}", status_code=301)

# Serve standalone reports site
@app.get("/reports/")
@app.get("/reports")
async def serve_reports_site():
    reports_html = Path(__file__).parent / "static" / "reports.html"
    if reports_html.exists():
        return FileResponse(str(reports_html), media_type="text/html")
    from fastapi.responses import HTMLResponse
    return HTMLResponse("<h1>Reports site not found</h1>", status_code=404)

# Serve raw markdown docs
_docs_dir = Path(__file__).parent / "data"

@app.get("/masterlist.md")
async def serve_masterlist():
    md_path = _docs_dir / "genomics_analysis_master_list.md"
    if md_path.exists():
        return FileResponse(str(md_path), media_type="text/markdown")
    from fastapi import HTTPException
    raise HTTPException(status_code=404, detail="File not found")

# Serve frontend SPA
if _frontend_dist.exists():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=str(_frontend_dist / "assets")), name="static-assets")

    # SPA fallback: serve index.html for all non-API routes
    @app.get("/{filename:path}")
    async def serve_frontend(filename: str):
        filepath = _frontend_dist / filename
        if filepath.exists() and filepath.is_file():
            return FileResponse(str(filepath))
        return FileResponse(str(_frontend_dist / "index.html"))


if __name__ == "__main__":
    import uvicorn
    from backend.config import APP_HOST, APP_PORT
    uvicorn.run("backend.main:app", host=APP_HOST, port=APP_PORT, reload=True)
