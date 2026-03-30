"""Reports API — list, serve, create, and manage markdown reports.

Reports are stored in /data/app/reports/ as .md files.
They are auto-generated on every scoring run completion
and can also be created/managed manually via Claude or the UI.
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse
from pydantic import BaseModel

from backend.config import APP_DIR, DATA_DIR
from backend.database import SessionLocal
from backend.models.schemas import ScoringRun, RunResult, PGSCacheEntry

router = APIRouter()

_REPORTS_DIR = APP_DIR / "reports"
_REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ── Pydantic models ──────────────────────────────────────────

class ReportMeta(BaseModel):
    filename: str
    title: str
    size_bytes: int
    modified: str
    category: str  # pgs | run | custom | summary
    pgs_id: str | None = None
    run_id: str | None = None


class CreateReportRequest(BaseModel):
    filename: str
    content: str
    category: str = "custom"


class UpdateReportRequest(BaseModel):
    content: str


# ── Helpers ──────────────────────────────────────────────────

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


def _report_meta(filepath: Path) -> ReportMeta:
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
    )


# ── Report generation ────────────────────────────────────────

def generate_run_report(run_id: str) -> str:
    """Generate a comprehensive report for a completed scoring run.

    Called automatically on run completion. Creates both a per-run report
    and updates per-PGS reports.
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

        # Save run report
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        report_path = _REPORTS_DIR / f"run_{run_id[:12]}.md"
        report_path.write_text(content, encoding="utf-8")

        return str(report_path)

    except Exception as e:
        print(f"Report generation failed for run {run_id}: {e}")
        return ""
    finally:
        db.close()


# ── API Endpoints ────────────────────────────────────────────

@router.get("/list")
async def list_reports(category: str | None = None) -> list[ReportMeta]:
    """List all available reports with metadata."""
    reports = []
    if _REPORTS_DIR.exists():
        for f in sorted(_REPORTS_DIR.iterdir()):
            if f.suffix == ".md" and f.is_file():
                meta = _report_meta(f)
                if category and meta.category != category:
                    continue
                reports.append(meta)

    # Sort: most recent first
    reports.sort(key=lambda r: r.modified, reverse=True)
    return reports


@router.get("/categories")
async def list_categories():
    """List report categories with counts."""
    cats = {"pgs": 0, "run": 0, "custom": 0, "summary": 0}
    if _REPORTS_DIR.exists():
        for f in _REPORTS_DIR.iterdir():
            if f.suffix == ".md" and f.is_file():
                cat, _, _ = _categorize(f.name)
                cats[cat] = cats.get(cat, 0) + 1
    return cats


@router.get("/content/{filename}")
async def get_report_content(filename: str):
    """Get a report's raw markdown content."""
    safe = filename.replace("..", "").replace("/", "")
    if not safe.endswith(".md"):
        safe += ".md"
    path = _REPORTS_DIR / safe
    if not path.exists():
        raise HTTPException(404, f"Report not found: {safe}")
    content = path.read_text(encoding="utf-8")
    title = _extract_title(content, safe)
    return {"filename": safe, "title": title, "content": content}


@router.get("/raw/{filename}")
async def get_report_raw(filename: str):
    """Get raw markdown (plain text) for external viewers."""
    safe = filename.replace("..", "").replace("/", "")
    if not safe.endswith(".md"):
        safe += ".md"
    path = _REPORTS_DIR / safe
    if not path.exists():
        raise HTTPException(404, f"Report not found: {safe}")
    return PlainTextResponse(path.read_text(encoding="utf-8"), media_type="text/markdown")


@router.post("/create")
async def create_report(req: CreateReportRequest):
    """Create a new report."""
    safe = req.filename.replace("..", "").replace("/", "")
    if not safe.endswith(".md"):
        safe += ".md"
    path = _REPORTS_DIR / safe
    if path.exists():
        raise HTTPException(409, f"Report already exists: {safe}")
    path.write_text(req.content, encoding="utf-8")
    return {"ok": True, "filename": safe}


@router.put("/content/{filename}")
async def update_report(filename: str, req: UpdateReportRequest):
    """Update an existing report's content."""
    safe = filename.replace("..", "").replace("/", "")
    if not safe.endswith(".md"):
        safe += ".md"
    path = _REPORTS_DIR / safe
    if not path.exists():
        raise HTTPException(404, f"Report not found: {safe}")
    path.write_text(req.content, encoding="utf-8")
    return {"ok": True, "filename": safe}


@router.delete("/content/{filename}")
async def delete_report(filename: str):
    """Delete a report."""
    safe = filename.replace("..", "").replace("/", "")
    if not safe.endswith(".md"):
        safe += ".md"
    path = _REPORTS_DIR / safe
    if not path.exists():
        raise HTTPException(404, f"Report not found: {safe}")
    path.unlink()
    return {"ok": True, "deleted": safe}


@router.post("/generate/{run_id}")
async def generate_report_for_run(run_id: str):
    """Manually trigger report generation for a specific run."""
    path = generate_run_report(run_id)
    if path:
        return {"ok": True, "path": path}
    raise HTTPException(404, "Run not found or no results")


@router.post("/regenerate-all")
async def regenerate_all_reports():
    """Regenerate reports for ALL completed runs."""
    db = SessionLocal()
    try:
        runs = db.query(ScoringRun).filter(
            ScoringRun.status.in_(["complete", "completed"])
        ).all()
        generated = 0
        for run in runs:
            path = generate_run_report(run.id)
            if path:
                generated += 1
        return {"ok": True, "generated": generated, "total_runs": len(runs)}
    finally:
        db.close()
