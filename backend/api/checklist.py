"""Checklist API — serves checklist markdown, persists check state,
and auto-syncs with completed PGS scoring runs."""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.config import APP_DIR, DATA_DIR
from backend.database import SessionLocal
from backend.models.schemas import RunResult, ScoringRun

router = APIRouter()

_CHECKLIST_MD = Path(__file__).parent.parent / "data" / "checklist.md"
_STATE_FILE = APP_DIR / "checklist_state.json"
_REPORTS_DIR = APP_DIR / "reports"


def _load_state() -> dict:
    if _STATE_FILE.exists():
        try:
            return json.loads(_STATE_FILE.read_text())
        except Exception:
            pass
    return {"checked": {}, "notes": {}, "reports": {}, "last_sync_run_id": None}


def _save_state(state: dict):
    _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(json.dumps(state, indent=2))


# ── PGS ID extraction from checklist markdown ─────────────────────

def _extract_pgs_map(markdown: str) -> dict:
    """Parse the checklist markdown and build a map of PGS ID -> (section_id, row_index).

    Returns: {pgs_id: (section_id, row_idx)} for every PGS ID found in table rows.
    """
    pgs_map = {}
    current_section_id = None
    base_section_id = None  # The H2-level ID (e.g., "s1")
    row_idx = -1
    in_table = False

    for line in markdown.split("\n"):
        # Section header: ## N. Title
        h2 = re.match(r"^## (\d+)\.", line)
        h3 = re.match(r"^### (.+)", line)
        if h2:
            base_section_id = f"s{h2.group(1)}"
            current_section_id = base_section_id
            row_idx = -1
            in_table = False
            continue
        if h3 and base_section_id:
            sub = h3.group(1).strip().lower()
            sub = re.sub(r"[^a-z0-9]+", "-", sub).strip("-")
            current_section_id = f"{base_section_id}-{sub}"
            row_idx = -1
            in_table = False
            continue

        if not line.startswith("|") or not current_section_id:
            if in_table:
                in_table = False
                row_idx = -1
            continue

        cells = [c.strip() for c in line.split("|")[1:-1]]
        # Skip header and separator rows
        if all(re.match(r"^[-:]+$", c) for c in cells if c):
            continue
        if any(c.lower() in ("done", "condition", "trait", "marker", "gene",
                              "disease", "analysis", "database", "tool",
                              "pathway", "category") for c in cells):
            in_table = True
            row_idx = -1
            continue

        row_idx += 1

        # Find PGS IDs in any cell
        for cell in cells:
            for m in re.finditer(r"(PGS\d{6,})", cell):
                pgs_id = m.group(1)
                item_id = f"{current_section_id}:{row_idx}"
                if pgs_id not in pgs_map:
                    pgs_map[pgs_id] = item_id

    return pgs_map


# ── Report generation ──────────────────────────────────────────────

def _resolve_trait(pgs_id: str, results: list) -> str:
    """Get human-readable trait name from results, PGS cache, or checklist."""
    for r in results:
        if r.trait:
            return r.trait
    try:
        from backend.models.schemas import PGSCacheEntry
        db_tmp = SessionLocal()
        cache = db_tmp.query(PGSCacheEntry).filter(PGSCacheEntry.pgs_id == pgs_id).first()
        db_tmp.close()
        if cache and cache.trait_reported:
            return cache.trait_reported
    except Exception:
        pass
    try:
        md = _CHECKLIST_MD.read_text()
        for line in md.split("\n"):
            if pgs_id in line and "|" in line:
                cells = [c.strip() for c in line.split("|")]
                for c in cells:
                    clean = c.replace("**", "").strip()
                    if (clean and clean != pgs_id and not clean.startswith("[")
                            and not clean.startswith("http") and len(clean) > 3
                            and not clean.replace(",", "").isdigit()):
                        return clean
    except Exception:
        pass
    return pgs_id


def _generate_report(pgs_id: str, all_results: list, runs_by_id: dict) -> str:
    """Generate a markdown report aggregating ALL runs for a given PGS ID.

    Every time this is called it rebuilds the full report from all available data,
    so new runs are always reflected.
    """
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = _REPORTS_DIR / f"{pgs_id}.md"

    trait = _resolve_trait(pgs_id, all_results)

    # Group results by run
    results_by_run = {}
    for r in all_results:
        rid = str(r.run_id)
        results_by_run.setdefault(rid, []).append(r)

    lines = [
        f"# {pgs_id} — {trait}",
        "",
        f"**PGS Catalog**: https://www.pgscatalog.org/score/{pgs_id}/",
        "",
        f"*{len(results_by_run)} run(s), {len(all_results)} result(s) total*",
        "",
    ]

    # Aggregate all scores across all runs for a summary table
    all_scores = []
    for r in all_results:
        if not r.scores_json:
            continue
        scores = r.scores_json if isinstance(r.scores_json, list) else json.loads(r.scores_json)
        run = runs_by_id.get(str(r.run_id))
        run_date = ""
        if run and run.completed_at:
            run_date = run.completed_at.strftime("%Y-%m-%d")
        for s in scores:
            all_scores.append({
                "sample": s.get("sample", "?"),
                "raw_score": s.get("raw_score"),
                "z_score": s.get("z_score"),
                "percentile": s.get("rank", s.get("percentile")),
                "source": os.path.basename(r.source_file_path or ""),
                "source_type": r.source_file_type or "",
                "match_rate": r.match_rate,
                "matched": r.variants_matched,
                "total": r.variants_total,
                "run_id": str(r.run_id)[:8],
                "run_date": run_date,
            })

    # Summary scores table
    if all_scores:
        lines.extend([
            "## All Scores",
            "",
            "| Sample | Raw Score | Z-Score | Percentile | Match Rate | Source | Run | Date |",
            "|--------|-----------|---------|------------|------------|--------|-----|------|",
        ])
        for s in all_scores:
            raw = f"{s['raw_score']:.6f}" if s.get("raw_score") is not None else "--"
            z = f"{s['z_score']:.2f}" if s.get("z_score") is not None else "--"
            pct = f"{s['percentile']:.1f}%" if s.get("percentile") is not None else "--"
            mr = f"{s['match_rate']*100:.0f}%" if s.get("match_rate") is not None else "--"
            lines.append(f"| {s['sample']} | {raw} | {z} | {pct} | {mr} | {s['source']} | {s['run_id']} | {s['run_date']} |")
        lines.append("")

    # Per-run details
    lines.extend(["## Run History", ""])

    for rid, run_results in sorted(results_by_run.items(), key=lambda x: x[0]):
        run = runs_by_id.get(rid)
        if not run:
            continue
        completed = run.completed_at.strftime("%Y-%m-%d %H:%M UTC") if run.completed_at else "?"
        duration = f"{run.duration_sec:.0f}s" if run.duration_sec else "?"
        engine = run.engine or "auto"
        build = run.genome_build or "GRCh38"

        # Best match rate for this run
        best_mr = max((r.match_rate or 0 for r in run_results), default=0)
        best_matched = max((r.variants_matched or 0 for r in run_results), default=0)
        best_total = max((r.variants_total or 0 for r in run_results), default=0)

        sources = set()
        for r in run_results:
            if r.source_file_path:
                sources.add(os.path.basename(r.source_file_path))

        lines.extend([
            f"### Run `{rid[:12]}`",
            "",
            f"| Field | Value |",
            f"|-------|-------|",
            f"| Completed | {completed} |",
            f"| Duration | {duration} |",
            f"| Engine | {engine} |",
            f"| Build | {build} |",
            f"| Source Files | {', '.join(sources) or '?'} |",
            f"| Variants | {best_matched:,} / {best_total:,} ({best_mr*100:.1f}%) |" if best_total else "",
            "",
        ])

        # Result file paths
        for attr in ["results_path_persistent", "results_path_fast"]:
            rpath = getattr(run, attr, None)
            if rpath and os.path.isdir(rpath):
                lines.append(f"Results: `{rpath}`")
                lines.append("")
                break

    content = "\n".join(line for line in lines if line is not None) + "\n"
    report_path.write_text(content, encoding="utf-8")
    return str(report_path)


# ── Sync checklist from database ───────────────────────────────────

def sync_checklist_from_db():
    """Scan all completed RunResults and mark matching checklist items as done.
    Creates report files for each scored PGS. Works regardless of how the run
    was triggered (UI, CLI, Claude Code).
    """
    if not _CHECKLIST_MD.exists():
        return

    state = _load_state()
    md = _CHECKLIST_MD.read_text(encoding="utf-8")
    pgs_map = _extract_pgs_map(md)

    if not pgs_map:
        return

    db = SessionLocal()
    try:
        # Get all completed runs
        completed_runs = db.query(ScoringRun).filter(
            ScoringRun.status.in_(["complete", "completed"])
        ).all()

        if not completed_runs:
            return

        # Get all RunResults
        all_results = db.query(RunResult).all()

        # Group results by PGS ID
        results_by_pgs = {}
        for r in all_results:
            if r.pgs_id:
                results_by_pgs.setdefault(r.pgs_id, []).append(r)

        # Map run IDs to run objects
        runs_by_id = {str(r.id): r for r in completed_runs}

        # Track result counts per PGS to detect new data
        result_counts = {}
        for pgs_id, rlist in results_by_pgs.items():
            result_counts[pgs_id] = len(rlist)

        changed = False
        for pgs_id, item_id in pgs_map.items():
            if pgs_id not in results_by_pgs:
                continue

            pgs_results = results_by_pgs[pgs_id]

            # Mark as checked
            if item_id not in state.get("checked", {}):
                state.setdefault("checked", {})[item_id] = True
                changed = True

            # Always regenerate report if result count changed or report missing
            prev_count = state.get("report_counts", {}).get(pgs_id, 0)
            current_count = len(pgs_results)
            report_exists = (
                pgs_id in state.get("reports", {})
                and os.path.isfile(state["reports"][pgs_id])
            )

            if current_count != prev_count or not report_exists:
                try:
                    report_path = _generate_report(pgs_id, pgs_results, runs_by_id)
                    state.setdefault("reports", {})[pgs_id] = report_path
                    state.setdefault("report_counts", {})[pgs_id] = current_count

                    # Update note with report link
                    report_name = os.path.basename(report_path)
                    note = f"[Report](/genomics/api/checklist/report/{report_name}) ({current_count} result{'s' if current_count != 1 else ''})"
                    state.setdefault("notes", {})[item_id] = note
                    changed = True
                except Exception:
                    pass

            # Extract per-sample percentile scores for inline display
            sample_scores = {}
            for r in pgs_results:
                if not r.scores_json:
                    continue
                scores = r.scores_json if isinstance(r.scores_json, list) else json.loads(r.scores_json)
                for s in scores:
                    sample = s.get("sample", "?")
                    pct = s.get("rank", s.get("percentile"))
                    z = s.get("z_score")
                    # Keep best match rate result per sample
                    if sample not in sample_scores or (r.match_rate or 0) > sample_scores[sample].get("match_rate", 0):
                        sample_scores[sample] = {
                            "percentile": round(pct, 1) if pct is not None else None,
                            "z_score": round(z, 2) if z is not None else None,
                            "match_rate": r.match_rate,
                        }
            if sample_scores:
                state.setdefault("scores", {})[item_id] = sample_scores
                changed = True

        if changed:
            _save_state(state)

    finally:
        db.close()


# ── API Endpoints ──────────────────────────────────────────────────

@router.get("")
async def get_checklist():
    """Return checklist markdown + check state. Auto-syncs with DB first."""
    if not _CHECKLIST_MD.exists():
        return JSONResponse({"error": "checklist.md not found"}, 404)

    # Lazy sync — update checklist from completed runs every time it's read
    try:
        sync_checklist_from_db()
    except Exception:
        pass

    content = _CHECKLIST_MD.read_text(encoding="utf-8")
    state = _load_state()

    return {
        "markdown": content,
        "scores": state.get("scores", {}),
        "command_results": state.get("command_results", {}),
        "checked": state.get("checked", {}),
        "notes": state.get("notes", {}),
        "reports": state.get("reports", {}),
    }


class ToggleRequest(BaseModel):
    item_id: str
    checked: bool


@router.post("/toggle")
async def toggle_item(req: ToggleRequest):
    """Toggle a checklist item's done state."""
    state = _load_state()
    if req.checked:
        state["checked"][req.item_id] = True
    else:
        state["checked"].pop(req.item_id, None)
    _save_state(state)
    return {"ok": True, "item_id": req.item_id, "checked": req.checked}


class NoteRequest(BaseModel):
    item_id: str
    note: str


@router.post("/note")
async def set_note(req: NoteRequest):
    """Set a note on a checklist item."""
    state = _load_state()
    if req.note.strip():
        state["notes"][req.item_id] = req.note.strip()
    else:
        state["notes"].pop(req.item_id, None)
    _save_state(state)
    return {"ok": True}


class UpdateMarkdownRequest(BaseModel):
    markdown: str


@router.put("")
async def update_checklist(req: UpdateMarkdownRequest):
    """Update the checklist markdown content."""
    _CHECKLIST_MD.write_text(req.markdown, encoding="utf-8")
    return {"ok": True}


@router.get("/report/{filename}")
async def get_report(filename: str):
    """Serve a generated report markdown file."""
    safe = filename.replace("..", "").replace("/", "")
    if not safe.endswith(".md"):
        safe += ".md"
    path = _REPORTS_DIR / safe
    if not path.exists():
        return JSONResponse({"error": "Report not found"}, 404)
    return {"name": safe, "content": path.read_text(encoding="utf-8")}


@router.post("/sync")
async def force_sync():
    """Force sync checklist from database (manual trigger)."""
    try:
        sync_checklist_from_db()
        state = _load_state()
        return {
            "ok": True,
            "checked_count": len(state.get("checked", {})),
            "reports_count": len(state.get("reports", {})),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)


@router.get("/samples")
async def list_samples():
    """Return available source files for scoring (VCF/gVCF/BAM)."""
    from backend.config import ALIGNED_BAMS_DIR, NIMOG_OUTPUT_DIR
    from glob import glob as _glob

    samples = []
    # BAM files
    for pattern in [str(ALIGNED_BAMS_DIR / "*.bam"), str(ALIGNED_BAMS_DIR / "*.cram")]:
        for f in sorted(_glob(pattern)):
            name = os.path.basename(f).rsplit(".", 1)[0]
            samples.append({"name": name, "path": f, "type": "bam"})

    # gVCF files from nimog output
    for f in sorted(_glob(str(NIMOG_OUTPUT_DIR / "*/dv/*.g.vcf.gz"))):
        name = os.path.basename(f).replace(".g.vcf.gz", "")
        samples.append({"name": name, "path": f, "type": "gvcf"})

    # VCF files from nimog output
    for f in sorted(_glob(str(NIMOG_OUTPUT_DIR / "*/dv/*.vcf.gz"))):
        if ".g.vcf.gz" in f:
            continue
        name = os.path.basename(f).replace(".vcf.gz", "")
        samples.append({"name": name, "path": f, "type": "vcf"})

    # Deduplicate by name, prefer gvcf > vcf > bam
    prio = {"gvcf": 0, "vcf": 1, "bam": 2}
    seen = {}
    for s in samples:
        if s["name"] not in seen or prio.get(s["type"], 9) < prio.get(seen[s["name"]]["type"], 9):
            seen[s["name"]] = s
    return list(seen.values())


class RunPGSRequest(BaseModel):
    pgs_ids: list[str]
    source_file: dict  # {"path": ..., "type": ...}
    ref_population: str = "EUR"


@router.post("/run")
async def run_from_checklist(req: RunPGSRequest):
    """Trigger a PGS scoring run directly from the checklist.
    Creates a run via the same engine as the PGS Runs tab.
    """
    import asyncio
    from backend.api.runs import create_run, CreateRunRequest
    from backend.database import get_db

    # Build the request matching the runs API format
    body = CreateRunRequest(
        source_files=[req.source_file],
        pgs_ids=req.pgs_ids,
        engine="auto",
        ref_population=req.ref_population,
    )

    # Get a DB session
    db = next(get_db())
    try:
        result = await create_run(body, db)
        return result
    finally:
        db.close()


class RunCommandRequest(BaseModel):
    """Run a shell command from the checklist (QC, ancestry, variant checks)."""
    item_id: str  # Checklist item ID for storing results
    command: str  # Command template from the checklist
    sample_name: str
    sample_path: str  # BAM or VCF path
    check_name: str = ""  # Human-readable name of the check


# ── Command registry: maps check names to shell commands ────────────
# These are the actual commands for checklist items that don't have inline commands.
# The frontend sends the check name; the backend looks up the command here.

COMMAND_REGISTRY = {
    "Y chromosome read count": 'samtools idxstats {bam} 2>/dev/null | awk \'$1=="chrY"||$1=="Y"{{print "Y_reads="$3}}\'',
    "SRY gene presence": "samtools view -c {bam} chrY:2786855-2787741 2>/dev/null",
    "X:Y read ratio": 'samtools idxstats {bam} 2>/dev/null | awk \'$1=="chrX"||$1=="X"{{x=$3}} $1=="chrY"||$1=="Y"{{y=$3}} END{{if(y>0)printf "X:Y=%.1f (Y=%d)\\n",x/y,y; else print "X:Y=inf (female)"}}\'',
    "Het rate on chrX": "bcftools view -r chrX -v snps {vcf} 2>/dev/null | bcftools stats -s - 2>/dev/null | grep ^PSC | head -1 || echo 'No chrX het data (try with VCF not gVCF)'",
    "Variant count on chrY": "bcftools view -r chrY {vcf} 2>/dev/null | bcftools view -H | wc -l",
    "Flagstat summary": "samtools flagstat {bam} 2>/dev/null | head -6",
    "Ti/Tv ratio": "bcftools stats {vcf} 2>/dev/null | grep ^TSTV | head -1",
    "Het/Hom ratio": "bcftools stats -s - {vcf} 2>/dev/null | grep ^PSC | head -1",
    "SNP count": "bcftools view -v snps -H {vcf} 2>/dev/null | wc -l",
    "Indel count": "bcftools view -v indels -H {vcf} 2>/dev/null | wc -l",
    "Duplicate read rate": "samtools flagstat {bam} 2>/dev/null | grep duplicate",
    "Mapped read %": "samtools flagstat {bam} 2>/dev/null | grep mapped | head -1",
}


def _resolve_command(check_name: str, raw_cmd: str, sample_path: str) -> str | None:
    """Resolve a command: use registry if available, otherwise parse from markdown.
    Returns None if no executable command can be determined."""
    # Try registry first
    if check_name in COMMAND_REGISTRY:
        tpl = COMMAND_REGISTRY[check_name]
        return tpl.replace("{bam}", sample_path).replace("{vcf}", sample_path)
    # Fall back to raw command only if it looks like an actual shell command
    if raw_cmd and (raw_cmd.startswith("/") or raw_cmd.startswith("samtools")
                    or raw_cmd.startswith("bcftools") or raw_cmd.startswith("plink")
                    or "|" in raw_cmd or ">" in raw_cmd):
        return raw_cmd
    # Not executable
    return None


def _safe_command(cmd: str, sample_path: str, sample_name: str) -> str:
    """Substitute sample placeholders in a command template."""
    import subprocess as _sp
    from backend.config import BCFTOOLS, SAMTOOLS, PLINK2, EXISTING_REFERENCE

    # Clean markdown escaping
    result = cmd.replace("\\|", "|")

    # Substitute common placeholders
    result = result.replace("sample.bam", sample_path)
    result = result.replace("sample.vcf.gz", sample_path)
    result = result.replace("sample.bed", sample_path.replace(".vcf.gz", ".bed").replace(".bam", ".bed"))
    result = result.replace("sample_chrM.vcf.gz", sample_path)
    result = result.replace("ref.fa", EXISTING_REFERENCE)
    result = result.replace("reference.fasta", EXISTING_REFERENCE)

    # Fix tool paths to use full paths from config
    result = result.replace("bcftools ", f"{BCFTOOLS} ")
    result = result.replace("samtools ", f"{SAMTOOLS} ")
    result = result.replace("plink2 ", f"{PLINK2} ")

    # Find other bioinformatics tools in conda env
    import shutil as _shutil
    for tool in ["mosdepth", "king", "admixture", "yhaplo", "haplogrep3", "rfmix"]:
        tool_path = _shutil.which(tool)
        if tool_path and tool in result:
            result = result.replace(f"{tool} ", f"{tool_path} ")
            result = result.replace(f"{tool}\n", f"{tool_path}\n")

    # Detect chromosome naming convention (chr1 vs 1) and adjust
    if sample_path.endswith(".bam") or sample_path.endswith(".cram"):
        try:
            out = _sp.run(
                [SAMTOOLS, "idxstats", sample_path],
                capture_output=True, text=True, timeout=10,
            )
            first_contig = out.stdout.strip().split("\n")[0].split("\t")[0] if out.stdout else ""
            uses_chr = first_contig.startswith("chr")
        except Exception:
            uses_chr = True  # Default to chr-prefixed
    else:
        uses_chr = True  # VCFs from our pipeline use chr-prefixed

    if not uses_chr:
        # Replace chr-prefixed regions with bare numbers
        result = result.replace("chrY:", "Y:")
        result = result.replace("chrX:", "X:")
        result = result.replace("chrM:", "MT:")
        result = result.replace('"chrY"', '"Y"')
        result = result.replace('"chrX"', '"X"')

    return result


_ERROR_LOG = APP_DIR / "checklist_errors.jsonl"


def _log_error(item_id: str, check_name: str, cmd: str, exit_code: int, error_msg: str):
    """Append failed command to error log for QA review and auto-fix."""
    import datetime as _dt
    entry = {
        "timestamp": _dt.datetime.now().isoformat(),
        "item_id": item_id,
        "check": check_name,
        "command": cmd[:500],
        "exit_code": exit_code,
        "error": error_msg[:1000],
    }
    try:
        _ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(_ERROR_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


@router.get("/errors")
async def get_errors():
    """Return recent command errors for QA review."""
    if not _ERROR_LOG.exists():
        return []
    try:
        lines = _ERROR_LOG.read_text().strip().split("\n")
        return [json.loads(l) for l in lines[-50:]]  # Last 50 errors
    except Exception:
        return []


@router.post("/run-command")
async def run_command_from_checklist(req: RunCommandRequest):
    """Execute a shell command from the checklist and store results."""
    import asyncio
    import subprocess

    # Resolve command from registry or use provided command
    resolved = _resolve_command(req.check_name, req.command, req.sample_path)
    if not resolved:
        # This is a manual/descriptive task — mark as noted but don't execute
        state = _load_state()
        existing = state.get("notes", {}).get(req.item_id, "")
        if req.sample_name not in existing:
            state.setdefault("notes", {})[req.item_id] = f"{existing} | {req.sample_name}: manual check".strip(" |")
        _save_state(state)
        return {"ok": True, "exit_code": -1, "output": "Manual check — no automated command available. Complete this step manually and mark as done.", "item_id": req.item_id}

    cmd = _safe_command(resolved, req.sample_path, req.sample_name)

    # Safety: reject obviously dangerous commands
    dangerous = ["rm ", "rm -", "mkfs", "dd ", "> /dev", "chmod", "chown", "sudo"]
    if any(d in cmd.lower() for d in dangerous):
        return JSONResponse({"error": "Command rejected for safety"}, 400)

    async def _exec(shell_cmd: str):
        p = await asyncio.create_subprocess_shell(
            shell_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        so, se = await asyncio.wait_for(p.communicate(), timeout=600)
        return p.returncode, so.decode("utf-8", errors="replace").strip(), se.decode("utf-8", errors="replace").strip()

    try:
        exit_code, output, errors = await _exec(cmd)

        # Auto-fix common errors and retry once
        if exit_code != 0:
            fixed = None
            err_lower = (errors + output).lower()
            # Tool not found -> try finding it in conda env
            if "not found" in err_lower or exit_code == 127:
                import shutil as _sh
                for tok in cmd.split():
                    if "/" not in tok and _sh.which(tok):
                        fixed = cmd.replace(tok, _sh.which(tok), 1)
                        break
            # chrX/chrY not found in BAM
            elif "could not" in err_lower and ("chrx" in cmd.lower() or "chry" in cmd.lower()):
                fixed = cmd.replace("chrY", "Y").replace("chrX", "X").replace("chrM", "MT")
            # plink2 warnings treated as errors (exit code 3 or 7)
            elif exit_code in (3, 7) and output:
                # plink2 uses non-zero for warnings; treat output as success
                exit_code = 0

            if fixed and fixed != cmd:
                exit_code, output, errors = await _exec(fixed)
                cmd = fixed

        # Log errors for QA review
        if exit_code != 0:
            _log_error(req.item_id, req.check_name, cmd, exit_code, errors or output)

        result_text = output if exit_code == 0 else f"ERROR (exit {exit_code}): {errors or output}"

        # Save to checklist state — only mark checked on success
        state = _load_state()
        if exit_code == 0:
            state.setdefault("checked", {})[req.item_id] = True
        state.setdefault("command_results", {})[req.item_id] = {
            "sample": req.sample_name,
            "check": req.check_name,
            "output": result_text[:2000],  # Cap at 2KB
            "exit_code": exit_code,
            "command": cmd[:500],
            "timestamp": __import__("datetime").datetime.now().isoformat(),
        }

        # Also put a short summary in notes
        summary = result_text.split("\n")[0][:100] if result_text else "completed"
        existing_note = state.get("notes", {}).get(req.item_id, "")
        if req.sample_name not in existing_note:
            new_note = f"{existing_note} | {req.sample_name}: {summary}".strip(" |")
            state.setdefault("notes", {})[req.item_id] = new_note

        _save_state(state)

        return {
            "ok": True,
            "exit_code": exit_code,
            "output": result_text[:2000],
            "errors": errors[:500] if errors else None,
            "item_id": req.item_id,
        }

    except asyncio.TimeoutError:
        return JSONResponse({"error": "Command timed out (10 min limit)"}, 504)
    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)


@router.get("/command-results")
async def get_command_results():
    """Return all stored command results."""
    state = _load_state()
    return state.get("command_results", {})
