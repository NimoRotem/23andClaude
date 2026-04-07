"""Reports API — list, serve, create, and manage markdown reports.

Reports are stored per-user in /data/users/{user_id}/reports/ as .md files.
They are auto-generated on every scoring run completion (written to the run
owner's directory) and can also be created/managed manually via Claude or the UI.

Legacy reports created before multi-user mode live at /data/app/reports/ and
are visible to admins as a read-only fallback.
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from backend.config import APP_DIR, user_reports_dir, USERS_ROOT
from backend.database import SessionLocal, get_db
from backend.models.schemas import PGSCacheEntry, RunResult, ScoringRun, User
from backend.utils.auth import get_current_user
from sqlalchemy.orm import Session

router = APIRouter()

# Legacy global reports directory (pre-multi-user mode). Kept around so admins
# can still read historical reports. New writes always go to a per-user dir.
_LEGACY_REPORTS_DIR = APP_DIR / "reports"
_LEGACY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ── Pydantic models ──────────────────────────────────────────

class ReportMeta(BaseModel):
    filename: str
    title: str
    size_bytes: int
    modified: str
    category: str  # pgs | run | custom | summary
    pgs_id: str | None = None
    run_id: str | None = None
    owner_id: str | None = None  # which user owns this report (None == legacy)


class CreateReportRequest(BaseModel):
    filename: str
    content: str
    category: str = "custom"


class UpdateReportRequest(BaseModel):
    content: str


# ── Helpers ──────────────────────────────────────────────────

def _safe_filename(filename: str) -> str:
    """Sanitize a report filename, ensuring `.md` extension and no path traversal."""
    safe = filename.replace("..", "").replace("/", "").replace("\\", "")
    if not safe.endswith(".md"):
        safe += ".md"
    return safe


def _extract_title(content: str, filename: str) -> str:
    """Extract title from first H1 or use filename."""
    for line in content.split("\n")[:5]:
        m = re.match(r"^#\s+(.+)", line)
        if m:
            return m.group(1).strip()
    return filename.replace(".md", "").replace("_", " ")


def _categorize(filename: str) -> tuple[str, str | None, str | None]:
    """Determine category and extract IDs."""
    if re.match(r"^PGS\d+\.md$", filename):
        return "pgs", filename.replace(".md", ""), None
    if re.match(r"^run_", filename):
        run_id = filename.replace("run_", "").replace(".md", "")
        return "run", None, run_id
    if filename.startswith("sample_") and "_summary" in filename:
        return "sample", None, None
    if filename.startswith("section_"):
        return "section", None, None
    if re.match(r"^(sex|qc|ancestry|monogenic|pharmacogenomics|variant|trait|cancer|cardiac|neuro)_", filename):
        return "qc", None, None
    if filename.startswith("summary_") or filename.startswith("overview"):
        return "summary", None, None
    return "custom", None, None


def _report_meta(filepath: Path, owner_id: Optional[str]) -> ReportMeta:
    """Build metadata for a report file."""
    stat = filepath.stat()
    content = filepath.read_text(encoding="utf-8", errors="replace")
    title = _extract_title(content, filepath.name)
    cat, pgs_id, run_id = _categorize(filepath.name)

    return ReportMeta(
        filename=filepath.name,
        title=title,
        size_bytes=stat.st_size,
        modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        category=cat,
        pgs_id=pgs_id,
        run_id=run_id,
        owner_id=owner_id,
    )


def _iter_visible_dirs(current_user: User) -> Iterable[tuple[Path, Optional[str]]]:
    """Yield (directory, owner_id) tuples that the current user is allowed to read.

    Regular users see only their own per-user dir.
    Admins additionally see every other user's dir plus the legacy global dir.
    """
    own_dir = user_reports_dir(current_user.id)
    yield own_dir, current_user.id

    if current_user.role == "admin":
        if USERS_ROOT.exists():
            for sub in USERS_ROOT.iterdir():
                if not sub.is_dir() or sub.name == current_user.id:
                    continue
                rdir = sub / "reports"
                if rdir.exists() and rdir.is_dir():
                    yield rdir, sub.name
        if _LEGACY_REPORTS_DIR.exists():
            yield _LEGACY_REPORTS_DIR, None


def _resolve_report_path(filename: str, current_user: User) -> tuple[Path, Optional[str]]:
    """Find a report file for read access. Returns (path, owner_id).

    Searches the current user's dir first, then (for admins) every other user's
    dir and the legacy global dir. Raises 404 if not found.
    """
    safe = _safe_filename(filename)
    for dir_path, owner_id in _iter_visible_dirs(current_user):
        candidate = dir_path / safe
        if candidate.exists() and candidate.is_file():
            return candidate, owner_id
    raise HTTPException(404, f"Report not found: {safe}")


# ── Report generation ────────────────────────────────────────

def generate_run_report(run_id: str) -> str:
    """Generate a comprehensive report for a completed scoring run.

    Called automatically on run completion. The report is written to the run
    owner's per-user reports directory (or the legacy global dir if the run
    has no owner).
    """
    db = SessionLocal()
    try:
        run = db.query(ScoringRun).filter(ScoringRun.id == run_id).first()
        if not run:
            return ""

        results = db.query(RunResult).filter(RunResult.run_id == run_id).all()
        if not results:
            return ""

        # ── Per-run report ────────────────────────────
        completed = run.completed_at.strftime("%Y-%m-%d %H:%M UTC") if run.completed_at else "?"
        duration = f"{run.duration_sec:.0f}s" if run.duration_sec else "?"
        source_files = run.source_files or []
        source_names = [os.path.basename(s.get("path", "?")) for s in source_files] if isinstance(source_files, list) else []

        lines = [
            f"# Scoring Run Report — {run_id[:12]}",
            "",
            f"> **Completed**: {completed}  ",
            f"> **Duration**: {duration}  ",
            f"> **Engine**: {run.engine or 'auto'}  ",
            f"> **Build**: {run.genome_build or 'GRCh38'}  ",
            f"> **Source files**: {', '.join(source_names) or '?'}  ",
            f"> **PGS scored**: {len(results)}",
            "",
            "---",
            "",
            "## Results Summary",
            "",
            "| PGS ID | Trait | Sample | Raw Score | Z-Score | Percentile | Match Rate |",
            "|--------|-------|--------|-----------|---------|------------|------------|",
        ]

        for r in results:
            scores = r.scores_json if isinstance(r.scores_json, list) else json.loads(r.scores_json or "[]")
            trait = r.trait or "?"
            for s in scores:
                raw = f"{s.get('raw_score', 0):.6f}" if s.get("raw_score") is not None else "--"
                z = f"{s.get('z_score', 0):.2f}" if s.get("z_score") is not None else "--"
                pct_val = s.get('percentile') or s.get('rank')
                pct = f"{pct_val:.1f}%" if pct_val is not None else "--"
                mr = f"{r.match_rate * 100:.0f}%" if r.match_rate else "--"
                sample = s.get("sample", "?")
                lines.append(f"| {r.pgs_id} | {trait} | {sample} | {raw} | {z} | {pct} | {mr} |")

        lines.extend(["", "---", ""])

        # Per-PGS detail sections
        for r in results:
            scores = r.scores_json if isinstance(r.scores_json, list) else json.loads(r.scores_json or "[]")
            trait = r.trait or "?"
            source = os.path.basename(r.source_file_path or "?")

            lines.extend([
                f"## {r.pgs_id} — {trait}",
                "",
                f"**PGS Catalog**: https://www.pgscatalog.org/score/{r.pgs_id}/",
                "",
                f"| Field | Value |",
                f"|-------|-------|",
                f"| Variants matched | {r.variants_matched:,} / {r.variants_total:,} ({r.match_rate * 100:.1f}%) |" if r.variants_total else "",
                f"| Source file | {source} ({r.source_file_type or '?'}) |",
                f"| Engine | {run.engine or 'auto'} |",
                "",
            ])

            if scores:
                lines.extend([
                    "### Scores",
                    "",
                    "| Sample | Raw Score | Z-Score | Percentile | Risk Level |",
                    "|--------|-----------|---------|------------|------------|",
                ])
                for s in scores:
                    raw = f"{s.get('raw_score', 0):.6f}" if s.get("raw_score") is not None else "--"
                    z_val = s.get("z_score")
                    z = f"{z_val:.2f}" if z_val is not None else "--"
                    pct_val = s.get('percentile') or s.get('rank')
                    pct = f"{pct_val:.1f}%" if pct_val is not None else "--"

                    # Risk level
                    risk = "Average"
                    if z_val is not None:
                        if z_val >= 2: risk = "**High Risk**"
                        elif z_val >= 1: risk = "Above Average"
                        elif z_val <= -2: risk = "Low Risk"
                        elif z_val <= -1: risk = "Below Average"

                    lines.append(f"| {s.get('sample', '?')} | {raw} | {z} | {pct} | {risk} |")

                lines.append("")

            lines.extend(["---", ""])

        # Footer
        lines.extend([
            f"*Report generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*",
            f"*Run ID: {run_id}*",
        ])

        content = "\n".join(line for line in lines if line is not None) + "\n"

        # Write to the run owner's per-user reports dir, or legacy if unowned.
        if run.user_id:
            target_dir = user_reports_dir(run.user_id)
        else:
            target_dir = _LEGACY_REPORTS_DIR
            target_dir.mkdir(parents=True, exist_ok=True)
        report_path = target_dir / f"run_{run_id[:12]}.md"
        report_path.write_text(content, encoding="utf-8")

        return str(report_path)

    except Exception as e:
        print(f"Report generation failed for run {run_id}: {e}")
        return ""
    finally:
        db.close()


# ── API Endpoints ────────────────────────────────────────────

@router.get("/list")
async def list_reports(
    category: str | None = None,
    current_user: User = Depends(get_current_user),
) -> list[ReportMeta]:
    """List all reports visible to the current user."""
    seen: dict[str, ReportMeta] = {}
    for dir_path, owner_id in _iter_visible_dirs(current_user):
        if not dir_path.exists():
            continue
        for f in sorted(dir_path.iterdir()):
            if f.suffix == ".md" and f.is_file():
                meta = _report_meta(f, owner_id)
                if category and meta.category != category:
                    continue
                # Prefer the user's own copy if a name collision happens
                if f.name not in seen:
                    seen[f.name] = meta

    reports = list(seen.values())
    reports.sort(key=lambda r: r.modified, reverse=True)
    return reports


@router.get("/categories")
async def list_categories(current_user: User = Depends(get_current_user)):
    """List report categories with counts (scoped to current user)."""
    cats: dict[str, int] = {"pgs": 0, "run": 0, "custom": 0, "summary": 0}
    seen: set[str] = set()
    for dir_path, _ in _iter_visible_dirs(current_user):
        if not dir_path.exists():
            continue
        for f in dir_path.iterdir():
            if f.suffix == ".md" and f.is_file() and f.name not in seen:
                seen.add(f.name)
                cat, _, _ = _categorize(f.name)
                cats[cat] = cats.get(cat, 0) + 1
    return cats


@router.get("/content/{filename}")
async def get_report_content(
    filename: str,
    current_user: User = Depends(get_current_user),
):
    """Get a report's raw markdown content."""
    path, owner_id = _resolve_report_path(filename, current_user)
    content = path.read_text(encoding="utf-8")
    title = _extract_title(content, path.name)
    return {"filename": path.name, "title": title, "content": content, "owner_id": owner_id}


@router.get("/raw/{filename}")
async def get_report_raw(
    filename: str,
    current_user: User = Depends(get_current_user),
):
    """Get raw markdown (plain text) for external viewers."""
    path, _ = _resolve_report_path(filename, current_user)
    return PlainTextResponse(path.read_text(encoding="utf-8"), media_type="text/markdown")


@router.post("/create")
async def create_report(
    req: CreateReportRequest,
    current_user: User = Depends(get_current_user),
):
    """Create a new report in the current user's reports directory."""
    safe = _safe_filename(req.filename)
    target_dir = user_reports_dir(current_user.id)
    path = target_dir / safe
    if path.exists():
        raise HTTPException(409, f"Report already exists: {safe}")
    path.write_text(req.content, encoding="utf-8")
    return {"ok": True, "filename": safe}


@router.put("/content/{filename}")
async def update_report(
    filename: str,
    req: UpdateReportRequest,
    current_user: User = Depends(get_current_user),
):
    """Update an existing report's content.

    Users can only update reports in their own directory. Admins can update
    reports anywhere they can see them.
    """
    safe = _safe_filename(filename)
    own_path = user_reports_dir(current_user.id) / safe
    if own_path.exists():
        own_path.write_text(req.content, encoding="utf-8")
        return {"ok": True, "filename": safe}

    if current_user.role == "admin":
        path, _ = _resolve_report_path(filename, current_user)
        path.write_text(req.content, encoding="utf-8")
        return {"ok": True, "filename": safe}

    raise HTTPException(404, f"Report not found: {safe}")


@router.delete("/content/{filename}")
async def delete_report(
    filename: str,
    current_user: User = Depends(get_current_user),
):
    """Delete a report.

    Users can only delete reports in their own directory. Admins can delete
    reports anywhere they can see them.
    """
    safe = _safe_filename(filename)
    own_path = user_reports_dir(current_user.id) / safe
    if own_path.exists():
        own_path.unlink()
        return {"ok": True, "deleted": safe}

    if current_user.role == "admin":
        path, _ = _resolve_report_path(filename, current_user)
        path.unlink()
        return {"ok": True, "deleted": safe}

    raise HTTPException(404, f"Report not found: {safe}")


@router.post("/generate/{run_id}")
async def generate_report_for_run(
    run_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Manually trigger report generation for a specific run.

    Users can only regenerate reports for runs they own. Admins can do it for
    any run.
    """
    run = db.query(ScoringRun).filter(ScoringRun.id == run_id).first()
    if not run:
        raise HTTPException(404, "Run not found")
    if current_user.role != "admin" and run.user_id != current_user.id:
        raise HTTPException(403, "You can only regenerate your own runs")

    path = generate_run_report(run_id)
    if path:
        return {"ok": True, "path": path}
    raise HTTPException(404, "Run has no results")


@router.post("/regenerate-all")
async def regenerate_all_reports(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Regenerate reports for completed runs.

    Users only regenerate their own runs. Admins regenerate everyone's.
    """
    q = db.query(ScoringRun).filter(ScoringRun.status.in_(["complete", "completed"]))
    if current_user.role != "admin":
        q = q.filter(ScoringRun.user_id == current_user.id)
    runs = q.all()
    generated = 0
    for run in runs:
        path = generate_run_report(run.id)
        if path:
            generated += 1
    return {"ok": True, "generated": generated, "total_runs": len(runs)}
