"""Checklist API — serves checklist markdown, persists check state,
and auto-syncs with completed PGS scoring runs."""

import hashlib
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
_ESTIMATES_FILE = APP_DIR / "checklist_estimates.json"


def _load_state() -> dict:
    if _STATE_FILE.exists():
        try:
            return json.loads(_STATE_FILE.read_text())
        except Exception:
            pass
    return {"checked": {}, "notes": {}, "reports": {}, "scores": {}, "command_results": {}, "report_counts": {}}


def _save_state(state: dict):
    _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(json.dumps(state, indent=2))


def _filter_state_for_sample(state: dict, sample: str) -> dict:
    """Return only the data relevant to a specific file. Empty when no file selected."""
    if not sample:
        return {"checked": {}, "notes": {}, "scores": {}, "command_results": {},
                "reports": {}, "report_counts": {}}
    # Derive sample name from path for fallback matching with old results
    sample_name = os.path.basename(sample).split(".")[0] if "/" in sample else sample

    f = {"checked": {}, "notes": {}, "scores": {}, "command_results": {},
         "reports": state.get("reports", {}), "report_counts": state.get("report_counts", {})}
    for k, v in state.get("command_results", {}).items():
        # Match by file path first, then by sample name (for old results without sample_path)
        if v.get("sample_path") == sample or v.get("sample") == sample or v.get("sample") == sample_name:
            f["command_results"][k] = v
            if k in state.get("checked", {}):
                f["checked"][k] = True
            if k in state.get("notes", {}):
                f["notes"][k] = state["notes"][k]
    for k, v in state.get("scores", {}).items():
        if sample_name in v:
            f["scores"][k] = {sample_name: v[sample_name]}
            if k in state.get("checked", {}):
                f["checked"][k] = True
            if k in state.get("notes", {}):
                f["notes"][k] = state["notes"][k]
    # Include section reports
    for k, v in state.get("notes", {}).items():
        if "_section_report" in k and (sample in str(v) or sample_name in str(v)):
            f["notes"][k] = v
    return f


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


# ── Runtime estimation ─────────────────────────────────────────────
#
# Estimates are intentionally rough — they're meant to give the user a
# sense of "is this 30 seconds or 30 minutes" before kicking off a check.
# Computed once per (checklist.md, server capabilities) pair and cached
# in APP_DIR/checklist_estimates.json. Recomputed automatically when
# either input changes, or manually via POST /api/checklist/estimates/refresh.

def _estimate_seconds_for_check(check_name: str, pgs_id: str | None,
                                variant_count: int | None,
                                capabilities: dict) -> int:
    """Rough runtime estimate (seconds) for a checklist row.

    Inputs:
        check_name      — the human-readable name of the check
        pgs_id          — set if the row is a PGS scoring entry
        variant_count   — variant count for PGS rows (parsed from the table)
        capabilities    — server capabilities dict from /api/system/capabilities
    """
    cpu_count = int(capabilities.get("cpu_count") or 8)
    # Baseline hardware: ~24 usable cores. Faster boxes get a discount,
    # slower boxes get penalized, clamped to a sane range.
    cpu_factor = max(0.4, min(1.6, 24 / max(cpu_count, 4)))

    name_lower = (check_name or "").lower()

    # ── PGS scoring: dominated by variant count ───────────────────
    if pgs_id:
        v = variant_count or 0
        if v <= 200:
            base = 25
        elif v <= 10_000:
            base = 40
        elif v <= 200_000:
            base = 60
        elif v <= 1_000_000:
            base = 100
        elif v <= 3_000_000:
            base = 150
        elif v <= 6_000_000:
            base = 200
        else:
            base = 260
        return max(10, int(base * cpu_factor))

    # ── Sex verification (5-check pipeline on BAM/CRAM) ───────────
    if any(t in name_lower for t in (
            "sex verification", "sex / gender", "sry gene", "msy reads",
            "x:y read", "x:y ratio", "het rate on chrx",
            "variant count on chry", "y chromosome read")):
        return max(20, int(90 * cpu_factor))

    # ── Ancestry pipeline (variant calling + plink2 + Rye) ────────
    if any(t in name_lower for t in (
            "ancestry pipeline", "admixture", "ancestry pca",
            "pca projection", "global ancestry", "deep ancestry",
            "fraposa", "rfmix")):
        return max(120, int(600 * cpu_factor))

    # ── Flagstat-derived (already-indexed BAM, very fast) ─────────
    if any(t in name_lower for t in (
            "flagstat", "duplicate read", "mapped read",
            "alignment qc", "mapping rate")):
        return max(5, int(15 * cpu_factor))

    # ── bcftools stats / view full-genome scans ───────────────────
    if any(t in name_lower for t in (
            "ti/tv", "het/hom", "snp count", "indel count",
            "het ratio", "hom ratio", "transition", "transversion")):
        return max(60, int(180 * cpu_factor))

    # ── Single-variant lookups (random-access bcftools query) ─────
    if any(t in name_lower for t in (
            "apoe", "factor v leiden", "mthfr", "prothrombin",
            "tcf7l2", "fto", "pcsk9", "hfe ", " hfe", "ldlr",
            " sry ", "sry presence")) or re.search(r"\brs\d+\b", name_lower):
        return max(3, int(5 * cpu_factor))

    # ── Monogenic disease region scans (region-restricted bcftools) ─
    if any(t in name_lower for t in (
            "cancer predisposition", "cardiovascular", "metabolism",
            "miscellaneous", "neuromuscular", "skeletal dysplasia",
            "deafness", "blood disorders", "vision")):
        return max(8, int(20 * cpu_factor))

    # ── Skill-based screens (carrier-status, monogenic disease, …) ─
    if any(t in name_lower for t in (
            "carrier status", "carrier-status", "carrier screen",
            "carrier screening", "monogenic disease screen",
            "monogenic-disease", "pharmacogenom", "pharma cogenom",
            "single-variant health", "single variant health")):
        return max(60, int(180 * cpu_factor))

    # ── Other "screening" rows (catch-all skill tasks) ────────────
    if "screening" in name_lower or "screen" in name_lower:
        return max(45, int(120 * cpu_factor))

    # ── ROH / runs of homozygosity ────────────────────────────────
    if "roh" in name_lower or "homozygos" in name_lower:
        return max(30, int(60 * cpu_factor))

    # ── Mitochondrial haplogroup ──────────────────────────────────
    if "mitochond" in name_lower or "haplogroup" in name_lower or "mtdna" in name_lower:
        return max(10, int(30 * cpu_factor))

    # Default: assume a moderate bcftools-style check
    return max(20, int(60 * cpu_factor))


def _format_seconds(s: int) -> str:
    """Format an estimate as a short human-readable label."""
    if s < 60:
        return f"~{s}s"
    if s < 120:
        return f"~{round(s/60, 1):g}m"
    if s < 3600:
        return f"~{int(round(s/60))}m"
    return f"~{round(s/3600, 1):g}h"


def _compute_estimates(markdown: str, capabilities: dict) -> dict:
    """Walk the checklist markdown and produce {item_id: {seconds, label}} for every row.

    Mirrors the row-walking logic of _extract_pgs_map so item_id values stay
    consistent with the rest of the checklist state.
    """
    estimates: dict = {}
    current_section_id = None
    base_section_id = None
    row_idx = -1
    in_table = False
    var_col_idx = -1
    name_col_idx = -1
    done_col_idx = -1

    for line in markdown.split("\n"):
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
        # Separator row
        if all(re.match(r"^[-:]+$", c) for c in cells if c):
            continue
        # Header row — same heuristic the rest of this file uses
        if any(c.lower() in ("done", "condition", "trait", "marker", "gene",
                              "disease", "analysis", "database", "tool",
                              "pathway", "category") for c in cells):
            in_table = True
            row_idx = -1
            lower = [c.lower() for c in cells]
            done_col_idx = next((i for i, h in enumerate(lower) if "done" in h), -1)
            var_col_idx = next((i for i, h in enumerate(lower) if "variant" in h), -1)
            name_col_idx = (done_col_idx + 1) if done_col_idx >= 0 else 0
            continue

        if not in_table:
            continue

        row_idx += 1
        item_id = f"{current_section_id}:{row_idx}"

        # PGS ID anywhere in the row
        pgs_id = None
        for cell in cells:
            m = re.search(r"PGS\d{6,}", cell)
            if m:
                pgs_id = m.group(0)
                break

        # Variant count from the "Variants" column, if present
        variant_count = None
        if 0 <= var_col_idx < len(cells):
            num_str = re.sub(r"[^\d]", "", cells[var_col_idx])
            if num_str:
                try:
                    variant_count = int(num_str)
                except ValueError:
                    variant_count = None

        # Check name from the column right after Done (or "Condition", etc.)
        check_name = ""
        if 0 <= name_col_idx < len(cells):
            raw = cells[name_col_idx]
            # Strip markdown link/bold/parenthetical noise
            raw = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", raw)
            raw = re.sub(r"\*+", "", raw)
            raw = re.sub(r"\(.*?\)", "", raw)
            check_name = raw.strip()

        # If a row had a PGS ID but no extractable check name, fall back to
        # the next non-empty cell so single-variant tables still get a name.
        if pgs_id and not check_name:
            for c in cells:
                clean = re.sub(r"\*+", "", c).strip()
                if clean and not re.match(r"^PGS\d{6,}$", clean):
                    check_name = clean
                    break

        seconds = _estimate_seconds_for_check(check_name, pgs_id, variant_count, capabilities)
        estimates[item_id] = {
            "seconds": seconds,
            "label": _format_seconds(seconds),
        }

    return estimates


def _current_capabilities() -> dict:
    """Snapshot of the server capabilities the estimates depend on."""
    try:
        from backend.config import CPU_COUNT, RAM_GB, GPU_AVAILABLE, ALIGN_THREADS
        return {
            "cpu_count": int(CPU_COUNT),
            "ram_gb": int(RAM_GB) if RAM_GB else 0,
            "gpu_available": bool(GPU_AVAILABLE),
            "align_threads": int(ALIGN_THREADS) if ALIGN_THREADS else int(CPU_COUNT),
        }
    except Exception:
        return {"cpu_count": 8, "ram_gb": 0, "gpu_available": False, "align_threads": 8}


def _get_estimates(force: bool = False) -> dict:
    """Return cached checklist estimates, recomputing only when checklist.md
    or server capabilities change (or when force=True).
    """
    if not _CHECKLIST_MD.exists():
        return {}

    md = _CHECKLIST_MD.read_text(encoding="utf-8")
    md_hash = hashlib.md5(md.encode("utf-8")).hexdigest()
    capabilities = _current_capabilities()
    cap_hash = hashlib.md5(
        json.dumps(capabilities, sort_keys=True).encode("utf-8")
    ).hexdigest()

    if not force and _ESTIMATES_FILE.exists():
        try:
            cached = json.loads(_ESTIMATES_FILE.read_text())
            if (cached.get("md_hash") == md_hash
                    and cached.get("cap_hash") == cap_hash
                    and isinstance(cached.get("estimates"), dict)):
                return cached["estimates"]
        except Exception:
            pass

    estimates = _compute_estimates(md, capabilities)
    try:
        _ESTIMATES_FILE.parent.mkdir(parents=True, exist_ok=True)
        _ESTIMATES_FILE.write_text(json.dumps({
            "md_hash": md_hash,
            "cap_hash": cap_hash,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "capabilities": capabilities,
            "estimates": estimates,
        }, indent=2))
    except Exception:
        pass
    return estimates


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
                "percentile": s.get("percentile") or s.get("rank"),
                "source": os.path.basename(r.source_file_path or ""),
                "source_type": r.source_file_type or "",
                "match_rate": r.match_rate,
                "matched": r.variants_matched,
                "total": r.variants_total,
                "run_id": str(r.run_id)[:8],
                "run_date": run_date,
            })

    # AI-enhanced narrative
    try:
        from backend.api.ai_reports import write_ai_pgs_report
        best_match = max((s.get("match_rate") or 0 for s in all_scores), default=0)
        best_total = max((s.get("total") or 0 for s in all_scores), default=0)
        ai_text = write_ai_pgs_report(
            pgs_id=pgs_id, trait=trait,
            scores=[{"sample": s["sample"], "percentile": s.get("percentile"), "z_score": s.get("z_score"), "raw_score": s.get("raw_score")} for s in all_scores],
            match_rate=best_match, variants_total=best_total,
        )
        if ai_text:
            lines.extend([ai_text, "", "---", ""])
    except Exception:
        pass

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
                    note = f"[Report](/api/checklist/report/{report_name}) ({current_count} result{'s' if current_count != 1 else ''})"
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
                    pct = s.get("percentile") or s.get("rank")
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
            # Regenerate sample summaries for all samples that have data
            try:
                all_samples = set()
                for r in all_results:
                    if r.scores_json:
                        scores = r.scores_json if isinstance(r.scores_json, list) else json.loads(r.scores_json)
                        for s in scores:
                            if s.get("sample"):
                                all_samples.add(s["sample"])
                for sample in all_samples:
                    generate_sample_summary(sample)
            except Exception:
                pass

    finally:
        db.close()


# ── API Endpoints ──────────────────────────────────────────────────

@router.get("")
async def get_checklist(sample: str = ""):
    """Return checklist markdown + check state, filtered for a specific sample."""
    if not _CHECKLIST_MD.exists():
        return JSONResponse({"error": "checklist.md not found"}, 404)

    # Lazy sync
    try:
        sync_checklist_from_db()
    except Exception:
        pass

    content = _CHECKLIST_MD.read_text(encoding="utf-8")
    state = _load_state()

    # Always filter — returns empty when no file selected
    filtered = _filter_state_for_sample(state, sample)

    # List samples that have results
    all_samples = set()
    for v in state.get("command_results", {}).values():
        if v.get("sample"):
            all_samples.add(v["sample"])
    for v in state.get("scores", {}).values():
        all_samples.update(v.keys())

    estimates = {}
    try:
        estimates = _get_estimates()
    except Exception:
        pass

    return {
        "markdown": content,
        "scores": filtered.get("scores", {}),
        "command_results": filtered.get("command_results", {}),
        "checked": filtered.get("checked", {}),
        "notes": filtered.get("notes", {}),
        "reports": filtered.get("reports", {}),
        "current_sample": sample,
        "available_samples": sorted(all_samples),
        "estimates": estimates,
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


@router.post("/estimates/refresh")
async def refresh_estimates():
    """Force recomputation of checklist runtime estimates."""
    try:
        estimates = _get_estimates(force=True)
        return {"ok": True, "count": len(estimates), "estimates": estimates}
    except Exception:
        return JSONResponse({"error": "Failed to refresh estimates"}, 500)


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

    # Return ALL files (no dedup) — user sees every BAM/VCF/gVCF
    # Sort: gVCF first, then VCF, then BAM
    prio = {"gvcf": 0, "vcf": 1, "bam": 2, "cram": 3}
    samples.sort(key=lambda s: (s["name"], prio.get(s["type"], 9)))
    return samples


class RunPGSRequest(BaseModel):
    pgs_ids: list[str]
    source_file: dict  # {"path": ..., "type": ...}
    ref_population: str = "EUR"


@router.post("/run")
async def run_from_checklist(req: RunPGSRequest):
    """Trigger a PGS scoring run directly from the checklist.
    Auto-downloads missing PGS files before scoring.
    """
    from backend.api.runs import create_run, CreateRunRequest
    from backend.database import get_db
    from backend.config import PGS_CACHE_DIR

    # Auto-download missing PGS scoring files
    missing = [pid for pid in req.pgs_ids
               if not (PGS_CACHE_DIR / pid).exists()
               or not any(str(f).endswith(".txt.gz") for f in (PGS_CACHE_DIR / pid).iterdir()
                          if (PGS_CACHE_DIR / pid).exists())]
    if missing:
        from urllib.request import urlopen, Request
        import time as _time
        for pid in missing:
            try:
                api_req = Request(f"https://www.pgscatalog.org/rest/score/{pid}",
                                  headers={"Accept": "application/json"})
                with urlopen(api_req, timeout=15) as r:
                    data = json.loads(r.read())
                hmz = data.get("ftp_harmonized_scoring_files", {})
                url = hmz.get("GRCh38", {}).get("positions")
                if not url:
                    continue
                outdir = PGS_CACHE_DIR / pid
                outdir.mkdir(parents=True, exist_ok=True)
                outpath = outdir / os.path.basename(url)
                if not outpath.exists():
                    with urlopen(url, timeout=120) as resp:
                        outpath.write_bytes(resp.read())
                _time.sleep(0.2)
            except Exception:
                pass  # Scoring will handle missing files

    # Build the request matching the runs API format
    body = CreateRunRequest(
        source_files=[req.source_file],
        pgs_ids=req.pgs_ids,
        engine="auto",
        ref_population=req.ref_population,
    )

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
    # --- Sex / Gender Verification ---
    "Sex verification pipeline": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_sex_check.py --sample-name {sample} --bam {bam} --vcf {vcf}",
    # Legacy aliases
    "Y chromosome read count": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_sex_check.py --sample-name {sample} --bam {bam} --vcf {vcf}",
    "SRY gene presence": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_sex_check.py --sample-name {sample} --bam {bam} --vcf {vcf}",
    "X:Y read ratio": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_sex_check.py --sample-name {sample} --bam {bam} --vcf {vcf}",
    "Het rate on chrX": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_sex_check.py --sample-name {sample} --bam {bam} --vcf {vcf}",
    "Variant count on chrY": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_sex_check.py --sample-name {sample} --bam {bam} --vcf {vcf}",

    # --- Sample Quality & Contamination ---
    "Flagstat summary": "samtools flagstat --threads {threads} {bam} 2>/dev/null | head -6",
    "Ti/Tv ratio": "bcftools stats {vcf} 2>/dev/null | grep ^TSTV | head -1",
    "Het/Hom ratio": "bcftools stats -s - {vcf} 2>/dev/null | grep ^PSC | head -1",
    "SNP count": "bcftools view --threads {threads} -v snps -H {vcf} 2>/dev/null | wc -l",
    "Indel count": "bcftools view --threads {threads} -v indels -H {vcf} 2>/dev/null | wc -l",
    "Duplicate read rate": "samtools flagstat --threads {threads} {bam} 2>/dev/null | grep -i duplicate || echo 'no duplicates found'",
    "Mapped read %": "samtools flagstat --threads {threads} {bam} 2>/dev/null | grep -i mapped | head -1 || echo 'no mapping data'",

    # --- Ancestry & Population Assignment ---
    "Ancestry pipeline": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_ancestry.py --sample-name {sample} --vcf {vcf} --bam {bam} --threads {threads}",
    # Legacy aliases
    "PCA projection onto 1000G": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_ancestry.py --sample-name {sample} --vcf {vcf} --bam {bam} --threads {threads}",
    "Admixture (global ancestry)": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_ancestry.py --sample-name {sample} --vcf {vcf} --bam {bam} --threads {threads}",
    "Ancestry PCA": "cd ${GENOMICS_WORK_DIR:-.} && python scripts/run_ancestry.py --sample-name {sample} --vcf {vcf} --bam {bam} --threads {threads}",
    "ADMIXTURE (K=5)": 'cd ' + os.getenv("GENOMICS_WORK_DIR", ".") + ' && python scripts/run_ancestry.py --sample-name {sample} --vcf {vcf} --bam {bam} --threads {threads}',
    "ADMIXTURE (K=7 to K=12)": 'cd ' + os.getenv("GENOMICS_WORK_DIR", ".") + ' && python scripts/run_ancestry.py --sample-name {sample} --vcf {vcf} --bam {bam} --threads {threads}',
    "ADMIXTURE": None,
    "Deep ancestry": None,
    "Admixture": None,
    "Ancestry PCA": 'cd ' + os.getenv("GENOMICS_WORK_DIR", ".") + ' && python scripts/run_ancestry.py --sample-name {sample} --vcf {vcf} --bam {bam} --threads {threads}',

    # --- Single Variant checks ---
    "APOE e4 (Alzheimer's)": "bcftools query -f '[%SAMPLE %GT]\\n' -r chr19:44908684 {vcf} 2>/dev/null && bcftools query -f '[%SAMPLE %GT]\\n' -r chr19:44908822 {vcf} 2>/dev/null || echo 'Check rs429358 + rs7412 manually'",
    "Factor V Leiden": "bcftools query -f '[%SAMPLE %GT]\\n' -r chr1:169549811 {vcf} 2>/dev/null || echo 'Check rs6025'",
    "MTHFR C677T": "bcftools query -f '[%SAMPLE %GT]\\n' -r chr1:11796321 {vcf} 2>/dev/null || echo 'Check rs1801133'",

    # --- Monogenic Disease Screening ---
    # Monogenic screening - match both with and without parenthetical count
    "Cancer Predisposition (~28)": "bcftools view --threads {threads} -r chr17:43044295-43170245,chr13:32315086-32400266,chr5:112043195-112181936 {vcf} 2>/dev/null | bcftools view -H -v snps,indels | wc -l | xargs -I{{}} echo '{{}} variants in BRCA1/BRCA2/APC region'",
    "Cancer Predisposition": "bcftools view --threads {threads} -r chr17:43044295-43170245,chr13:32315086-32400266,chr5:112043195-112181936 {vcf} 2>/dev/null | bcftools view -H -v snps,indels | wc -l | xargs -I{{}} echo '{{}} variants in BRCA1/BRCA2/APC region'",
    "Cardiovascular (~41)": "bcftools view --threads {threads} -r chr14:23382849-23403870,chr15:48408306-48434935,chr11:47332204-47388876 {vcf} 2>/dev/null | bcftools view -H -v snps,indels | wc -l | xargs -I{{}} echo '{{}} variants in MYH7/MYBPC3/LDLR region'",
    "Cardiovascular": "bcftools view --threads {threads} -r chr14:23382849-23403870,chr15:48408306-48434935,chr11:47332204-47388876 {vcf} 2>/dev/null | bcftools view -H -v snps,indels | wc -l | xargs -I{{}} echo '{{}} variants in MYH7/MYBPC3/LDLR region'",
    "Metabolism (~5)": "bcftools view --threads {threads} -r chr17:78073932-78084580,chrX:100652791-100663049 {vcf} 2>/dev/null | bcftools view -H -v snps,indels | wc -l | xargs -I{{}} echo '{{}} variants in GAA/GLA region'",
    "Metabolism": "bcftools view --threads {threads} -r chr17:78073932-78084580,chrX:100652791-100663049 {vcf} 2>/dev/null | bcftools view -H -v snps,indels | wc -l | xargs -I{{}} echo '{{}} variants in GAA/GLA region'",
    "Miscellaneous (~10)": "bcftools view --threads {threads} -r chr6:26090951-26098564,chr13:52513736-52587466 {vcf} 2>/dev/null | bcftools view -H -v snps,indels | wc -l | xargs -I{{}} echo '{{}} variants in HFE/ATP7B region'",
    "Miscellaneous": "bcftools view --threads {threads} -r chr6:26090951-26098564,chr13:52513736-52587466 {vcf} 2>/dev/null | bcftools view -H -v snps,indels | wc -l | xargs -I{{}} echo '{{}} variants in HFE/ATP7B region'",
}


def _find_sample_files(sample_name: str, sample_path: str) -> dict:
    """Return the selected file only. Each file is treated independently.
    Does NOT auto-discover other files for the same sample name."""
    bam = sample_path if sample_path.endswith((".bam", ".cram")) else ""
    vcf = sample_path if sample_path.endswith((".vcf.gz", ".vcf")) else ""
    return {"bam": bam, "vcf": vcf}


def _find_rs_number(check_name: str) -> str | None:
    """Extract an rs number from the check name or find it in the checklist markdown."""
    import re
    m = re.search(r'(rs\d+)', check_name)
    if m:
        return m.group(1)
    # Search the checklist for this check name and extract rs number from same row
    if _CHECKLIST_MD.exists():
        for line in _CHECKLIST_MD.read_text().split("\n"):
            if check_name in line.replace("**", "") and "|" in line:
                rm = re.search(r'(rs\d{2,})', line)
                if rm:
                    return rm.group(1)
    return None


# Map rs numbers to GRCh38 positions (common variants)
RS_POSITIONS = {
    "rs1801133": "chr1:11796321",   # MTHFR C677T
    "rs174546": "chr11:61597213",   # FADS1
    "rs2282679": "chr4:71742666",   # GC/VDBP
    "rs699": "chr1:230710048",      # AGT
    "rs10830963": "chr11:92975544", # MTNR1B
    "rs5082": "chr1:161222292",     # APOA2
    "rs1815739": "chr11:66560624",  # ACTN3
    "rs12722": "chr9:137149234",    # COL5A1
    "rs1800795": "chr7:22766246",   # IL6
    "rs8111989": "chr19:45388568",  # CKM
    "rs184039278": "chr12:107371753", # CRY1
    "rs73598374": "chr20:44619522", # ADA
    "rs5751876": "chr22:24441177",  # ADORA2A
    "rs1079610": "chr10:88426485",  # OPN4
    "rs1805007": "chr16:89919709",  # MC1R
    "rs12203592": "chr6:396321",    # IRF4
    "rs1799750": "chr11:102799725", # MMP1
    "rs16891982": "chr5:33951588",  # SLC45A2
    "rs8032158": "chr15:65197993",  # NEDD4
    "rs429358": "chr19:44908684",   # APOE
    "rs7412": "chr19:44908822",     # APOE
    "rs6025": "chr1:169549811",     # Factor V
    "rs1799963": "chr11:46761055",  # Prothrombin
    "rs9939609": "chr16:53786615",  # FTO
    "rs7903146": "chr10:112998590", # TCF7L2
    "rs10757278": "chr9:22124478",  # 9p21
    "rs10455872": "chr6:160589086", # LPA
    "rs11209026": "chr1:67222804",  # IL23R
    "rs34536443": "chr19:10352442", # TYK2
    "rs713598": "chr7:141972804",   # TAS2R38 bitter
    "rs72921001": "chr11:7296643",  # OR6A2 cilantro
    "rs17822931": "chr16:48224287", # ABCC11 earwax
    "rs4988235": "chr2:136608646",  # MCM6 lactose
    "rs671": "chr12:111803962",     # ALDH2 flush
    "rs762551": "chr15:74749576",   # CYP1A2 caffeine
    "rs12913832": "chr15:28120472", # HERC2 eye color
    "rs601338": "chr19:48703417",   # FUT2 norovirus
    "rs4680": "chr22:19963748",     # COMT warrior
    "rs8176746": "chr9:136131322",  # ABO blood type
    "rs6746030": "chr2:166248806",  # SCN9A pain
    "rs80357713": "chr17:43093449", # BRCA1
    "rs28929474": "chr14:94378610", # SERPINA1 A1AT
    "rs11591147": "chr1:55039974",  # PCSK9
    "rs34637584": "chr12:40340400", # LRRK2
    "rs555607708": "chr22:28725099", # CHEK2
    # Carrier status
    "rs113993960": "chr7:117559590", # CFTR F508del
    "rs387906309": "chr15:72346580", # HEXA Tay-Sachs
    "rs76763715": "chr1:155205634", # GBA1 Gaucher
    "rs5030858": "chr12:103234251", # PAH PKU
    "rs11549407": "chr11:5227002",  # HBB beta-thal
    "rs1800562": "chr6:26092913",   # HFE hemochromatosis
    "rs386834236": "chr17:78078371", # GAA Pompe
    # Pharmacogenomics
    "rs4244285": "chr10:94781859",  # CYP2C19*2
    "rs1799853": "chr10:94942290",  # CYP2C9*2
    "rs9923231": "chr16:31096368",  # VKORC1
    "rs3918290": "chr1:97515839",   # DPYD*2A
    "rs4149056": "chr12:21178615",  # SLCO1B1*5
    "rs1142345": "chr6:18130918",   # TPMT*3C
    "rs116855232": "chr13:48037825", # NUDT15*3
    # Additional fun traits
    "rs10427255": "chr2:145477415", # ZEB2 photic sneeze
    "rs4481887": "chr1:248078938",  # OR2M7 asparagus
    "rs35874116": "chr1:18868031",  # TAS1R2 sweet taste
    "rs12908553": "chr3:14088024",  # FOXL2 dimples
    "rs1801260": "chr4:56300048",   # CLOCK chronotype
    "rs2937573": "chr5:167920959",  # TENM2 misophonia
    "rs66800491": "chr3:112100000", # PVRL3 motion sickness
    "rs3827760": "chr2:108894473",  # EDAR thick hair
    "rs334": "chr11:5227002",       # HBB sickle cell
}


# ── Interpretation registries ──────────────────────────────────────

def _interpret_y_reads(output):
    """Interpret MSY (male-specific Y) read count, excluding PAR regions."""
    import re
    # New format: just a number (samtools view -c output)
    try:
        c = int(output.strip().split('\n')[-1].strip())
    except ValueError:
        m = re.search(r'Y_reads=(\d+)', output)
        if not m: return "Could not parse Y chromosome read count."
        c = int(m.group(1))
    # MSY thresholds are much lower than total chrY since PAR reads are excluded
    # Males: typically 500K-3M MSY reads at 30x WGS; Females: <1000 (mapping artifacts only)
    if c > 100_000: return f"**Male** — {c:,} reads in male-specific Y region (MSY). Definitive male signal."
    elif c > 5_000: return f"**Likely Male** — {c:,} MSY reads (elevated, likely male)."
    elif c > 1_000: return f"**Ambiguous** — {c:,} MSY reads. Low but present — may indicate mosaic or low-quality mapping."
    else: return f"**Female** — {c:,} MSY reads (negligible — consistent with XX)."

def _interpret_sry(output):
    """Interpret SRY gene read count. SRY is in the MSY with no X homolog — most definitive marker."""
    try:
        c = int(output.strip().split('\n')[-1].strip())
        if c > 10: return f"**Male** — {c} reads at the SRY gene locus (definitive Y-specific marker)."
        else: return f"**Female** — {c} reads at SRY. Zero/near-zero is definitive for XX (SRY has no X homolog)."
    except: return f"SRY result: {output}"

def _interpret_xy_ratio(output):
    """Interpret X:Y_MSY ratio (using only male-specific Y reads, excluding PAR)."""
    import re
    m = re.search(r'X:Y_MSY=([\d.]+)', output)
    if not m:
        m = re.search(r'X:Y=([\d.]+)', output)
    if m:
        ratio = float(m.group(1))
        # With MSY-only counts, male ratio is typically 1.5-5, female is very high or inf
        if ratio < 8: return f"**Male** — X:Y(MSY) ratio of {ratio:.1f} (typical male range: 1.5-5)."
        elif ratio < 20: return f"**Likely Male** — X:Y(MSY) ratio of {ratio:.1f} (somewhat elevated but suggests Y presence)."
        elif ratio > 50: return f"**Female** — X:Y(MSY) ratio of {ratio:.1f} (very high — negligible MSY reads, consistent with XX)."
        else: return f"X:Y(MSY) ratio: {ratio:.1f} — ambiguous range."
    if "inf" in output.lower() or "female" in output.lower():
        return "**Female** — no male-specific Y reads detected (X:Y=inf)."
    return f"X:Y result: {output}"

def _interpret_flagstat(output):
    lines = output.strip().split("\n")
    parts = {}
    for l in lines:
        if "in total" in l: parts["total"] = l.split("+")[0].strip()
        if "mapped" in l and "primary" not in l and "mate" not in l: parts["mapped"] = l
        if "duplicates" in l and "primary" not in l: parts["dups"] = l
    summary = []
    if "total" in parts: summary.append(f"Total reads: {parts['total']}")
    if "mapped" in parts: summary.append(f"Mapping: {parts['mapped'].strip()}")
    if "dups" in parts: summary.append(f"Duplicates: {parts['dups'].strip()}")
    return "\n".join(summary) if summary else output

def _interpret_titv(output):
    parts = output.strip().split("\t")
    if len(parts) >= 5:
        try:
            ratio = float(parts[4])
            quality = "excellent" if 2.0 <= ratio <= 2.1 else "acceptable" if 1.8 <= ratio <= 2.3 else "unusual — may indicate quality issues"
            return f"Ti/Tv ratio: **{ratio:.3f}** — {quality}. Expected range for WGS: 2.0-2.1."
        except: pass
    return f"Ti/Tv result: {output}"

def _interpret_snp(output):
    try:
        c = int(output.strip())
        quality = "normal" if 3_500_000 <= c <= 4_500_000 else "low — possible quality issue" if c < 3_000_000 else "high — may include artifacts" if c > 5_000_000 else "acceptable"
        return f"**{c:,} SNPs** — {quality}. Expected for WGS: 3.5-4.5 million."
    except: return f"SNP count: {output}"

def _interpret_indel(output):
    try:
        c = int(output.strip())
        return f"**{c:,} indels** — {'normal' if 400_000 <= c <= 1_000_000 else 'outside typical range'}. Expected: 500K-800K."
    except: return f"Indel count: {output}"


def _interpret_ancestry(output):
    try:
        data = json.loads(output)
        if "error" in data:
            return f"Ancestry inference failed: {data['error']}"
        primary = data.get("primary_ancestry", "?")
        primary_name = data.get("primary_name", primary)
        admixed = data.get("is_admixed", False)

        # Support both old ("proportions") and new ("admixture_proportions") format
        props = data.get("admixture_proportions", data.get("proportions", {}))
        primary_pct = props.get(primary, 0) * 100

        pop_names = {"EUR": "European", "AFR": "African", "EAS": "East Asian",
                     "SAS": "South Asian", "AMR": "American (admixed)"}

        lines = [f"**Primary Ancestry: {pop_names.get(primary, primary_name)}** ({primary_pct:.0f}%)"]
        if admixed:
            lines[0] += " — **ADMIXED**"

        lines.append("")
        lines.append("**Ancestry Composition (consensus of 3 methods):**")
        for pop, pct_raw in sorted(props.items(), key=lambda x: -x[1]):
            pct = pct_raw * 100
            if pct < 0.5:
                continue
            bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
            lines.append(f"- {pop_names.get(pop, pop)}: **{pct:.1f}%** `{bar}`")

        # Per-method breakdown
        methods = data.get("methods", {})
        if methods:
            lines.extend(["", "**Per-method breakdown:**"])
            for method, mp in methods.items():
                parts = [f"{pop_names.get(k,k)}: {v*100:.0f}%" for k, v in sorted(mp.items(), key=lambda x: -x[1]) if v > 0.005]
                if parts:
                    lines.append(f"- {method}: {', '.join(parts)}")

        # Sub-populations
        sub = data.get("sub_population_proportions", {})
        if sub:
            lines.extend(["", "**Fine-scale sub-population match:**"])
            for pop, pct_raw in list(sub.items())[:6]:
                if pct_raw * 100 >= 1:
                    lines.append(f"- {pop}: {pct_raw*100:.1f}%")

        nearest = data.get("nearest_subpopulations", [])
        if nearest:
            lines.extend(["", "**Nearest reference individuals:**"])
            for n in nearest[:5]:
                lines.append(f"- {n['population']}: {n['count']}/20")

        if admixed:
            lines.extend(["", "*This sample is admixed (no single ancestry >80%). Population-specific PGS may need multi-ancestry adjustment.*"])

        return "\n".join(lines)
    except (json.JSONDecodeError, KeyError):
        return f"Ancestry output: {output[:200]}"


def _interpret_sex_pipeline(output):
    """Interpret unified sex verification pipeline JSON output."""
    try:
        data = json.loads(output)
        if "error" in data:
            return f"Sex verification failed: {data['error']}"
        det = data.get("determination", {})
        checks = data.get("checks", {})
        not_run = data.get("checks_not_run", {})
        sex = det.get("sex", "?")
        confidence = det.get("confidence", "?")
        reasoning = det.get("reasoning", "")
        lines = [f"**Genetic Sex: {sex}** (confidence: {confidence.upper()})"]
        lines.append(f"*{reasoning}*")
        lines.append("")
        check_names = {"y_read_count": "Y Read Count (MSY)", "sry_gene": "SRY Gene",
                       "xy_read_ratio": "X:Y Ratio", "chrx_het_rate": "ChrX Het Rate",
                       "chry_variant_count": "ChrY Variants"}
        lines.append("**Individual Checks:**")
        for cid, cname in check_names.items():
            if cid in checks:
                c = checks[cid]
                call = c["call"].upper()
                lines.append(f"- {cname}: **{call}** — {c.get('detail', '')}")
            elif cid in not_run:
                lines.append(f"- {cname}: *skipped* ({not_run[cid]})")
        return "\n".join(lines)
    except (json.JSONDecodeError, KeyError):
        return f"Sex check output: {output[:200]}"


INTERPRETATION_REGISTRY = {
    "Sex verification pipeline": _interpret_sex_pipeline,
    "Y chromosome read count": _interpret_sex_pipeline,
    "SRY gene presence": _interpret_sex_pipeline,
    "X:Y read ratio": _interpret_sex_pipeline,
    "Het rate on chrX": _interpret_sex_pipeline,
    "Variant count on chrY": _interpret_sex_pipeline,
    "Ancestry pipeline": _interpret_ancestry,
    "PCA projection onto 1000G": _interpret_ancestry,
    "Admixture (global ancestry)": _interpret_ancestry,
    "Ancestry PCA": _interpret_ancestry,
    "Flagstat summary": _interpret_flagstat,
    "Ti/Tv ratio": _interpret_titv,
    "SNP count": _interpret_snp,
    "Indel count": _interpret_indel,
    "Y-DNA haplogroup": lambda o: f"Y chromosome: {o}" if o else "No Y data available.",
    "mtDNA haplogroup": lambda o: f"Mitochondrial DNA: {o}" if o else "No mtDNA data.",
    "Mapped read %": lambda o: f"Mapping rate: {o.strip()}. >95% is good quality." if o else "No data.",
    "Duplicate read rate": lambda o: f"Duplicates: {o.strip()}. <10% is normal for WGS." if o else "No data.",
}

GENOTYPE_INTERPRETATIONS = {
    "rs1801133": {"0/0": "CC wild-type — normal MTHFR enzyme activity.", "0/1": "CT heterozygous — ~65% enzyme activity. Mild impact on folate metabolism.", "1/1": "TT homozygous — ~30% enzyme activity. Consider methylfolate supplementation."},
    "rs1815739": {"0/0": "CC (RR) — functional alpha-actinin-3. Sprint/power advantage.", "0/1": "CT (RX) — one functional copy. Mixed endurance/power.", "1/1": "TT (XX) — alpha-actinin-3 deficient. Endurance advantage."},
    "rs429358": {"0/0": "TT — no APOE e4 alleles. Lower Alzheimer's risk.", "0/1": "TC — one APOE e4 allele. ~3x increased AD risk.", "1/1": "CC — two APOE e4 alleles. ~12-15x increased AD risk."},
    "rs6025": {"0/0": "GG — no Factor V Leiden. Normal clotting.", "0/1": "GA — heterozygous Factor V Leiden. 3-8x VTE risk.", "1/1": "AA — homozygous Factor V Leiden. ~80x VTE risk."},
    "rs4988235": {"0/0": "CC — lactose intolerant (ancestral). Cannot digest lactose as adult.", "0/1": "CT — lactose tolerant (heterozygous). Can digest lactose.", "1/1": "TT — lactose tolerant. Full lactase persistence."},
    "rs671": {"0/0": "GG — normal ALDH2. Can metabolize alcohol normally.", "0/1": "GA — reduced ALDH2. Alcohol flush reaction, increased esophageal cancer risk.", "1/1": "AA — near-zero ALDH2. Severe flush, should avoid alcohol."},
    "rs762551": {"0/0": "CC — slow CYP1A2 metabolizer. Caffeine stays in system longer. Hypertension risk with >3 cups/day.", "0/1": "CA — intermediate caffeine metabolism.", "1/1": "AA — fast CYP1A2 metabolizer. Rapid caffeine clearance."},
    "rs12913832": {"0/0": "AA — brown eyes (most likely).", "0/1": "AG — green/hazel eyes likely.", "1/1": "GG — blue eyes (high probability)."},
    "rs713598": {"0/0": "GG (AVI/AVI) — non-taster. Cannot taste PTC/PROP bitterness.", "0/1": "GC (PAV/AVI) — medium taster.", "1/1": "CC (PAV/PAV) — supertaster. Very sensitive to bitter compounds."},
    "rs17822931": {"0/0": "CC — wet earwax + normal body odor (ancestral).", "0/1": "CT — intermediate.", "1/1": "TT — dry earwax + less body odor. Common in East Asian populations."},
    "rs601338": {"0/0": "GG — secretor. Normal norovirus susceptibility.", "0/1": "GA — secretor.", "1/1": "AA — non-secretor. Strong norovirus resistance."},
    "rs4680": {"0/0": "GG (Val/Val) — 'Warrior'. Fast COMT, lower prefrontal dopamine. Stress-resilient, less pain sensitive.", "0/1": "GA (Val/Met) — intermediate COMT activity.", "1/1": "AA (Met/Met) — 'Worrier'. Slow COMT, higher prefrontal dopamine. Better cognitive performance but more stress-sensitive."},
    "rs9939609": {"0/0": "TT — normal FTO. No increased obesity risk.", "0/1": "TA — one risk allele. ~1.3x obesity risk.", "1/1": "AA — two risk alleles. ~1.67x obesity risk, ~3kg heavier on average."},
    "rs7903146": {"0/0": "CC — normal TCF7L2. Average T2D risk.", "0/1": "CT — one risk allele. ~1.4x T2D risk.", "1/1": "TT — two risk alleles. ~1.8x T2D risk (strongest common variant for diabetes)."},
    "rs1799963": {"0/0": "GG — normal prothrombin. No increased clotting risk.", "0/1": "GA — heterozygous. ~2.8x VTE risk.", "1/1": "AA — homozygous. High VTE risk."},
    "rs28929474": {"0/0": "CC — normal SERPINA1 (A1AT). No lung/liver risk.", "0/1": "CT — MZ heterozygous. Mild A1AT deficiency.", "1/1": "TT — ZZ homozygous. Severe A1AT deficiency. Risk of emphysema + liver disease."},
    "rs11591147": {"0/0": "GG — normal PCSK9. Average LDL cholesterol.", "0/1": "GT — one loss-of-function allele. ~15-28% lower LDL. Cardioprotective.", "1/1": "TT — two LoF alleles. ~50% lower LDL. Very cardioprotective."},
    "rs10830963": {"0/0": "CC — normal MTNR1B. Standard glucose response to meals.", "0/1": "CG — one risk allele. Late eating may worsen glucose control.", "1/1": "GG — two risk alleles. Meal timing significantly affects glucose. Avoid late eating."},
    "rs174546": {"0/0": "CC — efficient omega-3 conversion from plant sources.", "0/1": "CT — intermediate conversion.", "1/1": "TT — poor omega-3 conversion. May benefit from direct fish oil supplementation."},
    "rs5082": {"0/0": "GG — normal response to saturated fat.", "0/1": "GA — intermediate.", "1/1": "AA — CC genotype at APOA2. Higher BMI with saturated fat intake. Benefit from limiting saturated fat."},
}


def generate_command_report(check_name, sample_name, command, output, exit_code, category="qc"):
    """Generate a full markdown report for any command-based check."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    safe_check = re.sub(r'[^a-zA-Z0-9_-]', '_', check_name)[:40]
    safe_sample = re.sub(r'[^a-zA-Z0-9_-]', '_', sample_name)
    filename = f"{category}_{safe_check}_{safe_sample}.md"
    report_path = _REPORTS_DIR / filename

    ts = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Get interpretation
    interpretation = ""
    if check_name in INTERPRETATION_REGISTRY:
        try:
            interpretation = INTERPRETATION_REGISTRY[check_name](output)
        except Exception:
            pass
    if not interpretation:
        rs = _find_rs_number(check_name)
        if rs and rs in GENOTYPE_INTERPRETATIONS:
            gt_match = re.search(r'(\d/\d)', output)
            if gt_match:
                gt = gt_match.group(1)
                interpretation = GENOTYPE_INTERPRETATIONS[rs].get(gt, f"Genotype {gt} — interpretation not available for {rs}.")

    # Try AI-enhanced interpretation
    ai_narrative = None
    try:
        from backend.api.ai_reports import write_ai_report
        ai_narrative = write_ai_report(
            analysis_type=category, check_name=check_name,
            sample_name=sample_name, raw_output=output,
            static_interpretation=interpretation,
        )
    except Exception:
        pass

    lines = [
        f"# {check_name} — {sample_name}",
        "",
        f"| Field | Value |",
        f"|-------|-------|",
        f"| Sample | {sample_name} |",
        f"| Check | {check_name} |",
        f"| Status | {'Passed' if exit_code == 0 else 'Manual' if exit_code == -1 else f'Failed (exit {exit_code})'} |",
        f"| Date | {ts} |",
        f"| Category | {category} |",
        "",
    ]

    if ai_narrative:
        lines.extend([ai_narrative, ""])
    elif interpretation:
        lines.extend(["## Interpretation", "", interpretation, ""])

    lines.extend(["## Raw Result", "", "```", output if output else "(no output)", "```", ""])

    if command and len(command) > 5:
        lines.extend(["## Command", "", f"```bash", command, "```", ""])

    content = "\n".join(lines) + "\n"
    report_path.write_text(content, encoding="utf-8")
    return filename


def generate_sample_summary(sample_name: str):
    """Generate a comprehensive summary report aggregating ALL results for one sample."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    state = _load_state()
    cmd_results = state.get("command_results", {})
    scores_map = state.get("scores", {})
    ts = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Collect command results for this sample, categorized
    categories = {"Sex Verification": [], "Sample Quality": [], "Ancestry": [], "Variants": [], "Monogenic": [], "Pharmacogenomics": [], "Other": []}
    for item_id, cr in cmd_results.items():
        if cr.get("sample") != sample_name:
            continue
        cn = cr.get("check", "").lower()
        cat = ("Sex Verification" if any(k in cn for k in ["chromosome", "sry", "x:y", "het rate", "variant count on chr"]) else
               "Sample Quality" if any(k in cn for k in ["flagstat", "ti/tv", "het/hom", "snp", "indel", "duplicate", "mapped"]) else
               "Ancestry" if any(k in cn for k in ["pca", "roh", "haplo", "admixture", "ancestry", "neanderthal", "ibd", "hla"]) else
               "Monogenic" if any(k in cn for k in ["predisposition", "cardiovascular", "metabolism", "miscellaneous"]) else
               "Pharmacogenomics" if any(k in cn for k in ["cyp", "vkorc", "dpyd", "slco", "tpmt"]) else
               "Variants")
        # Get interpretation
        interp = ""
        check_name = cr.get("check", "")
        output = cr.get("output", "")
        if check_name in INTERPRETATION_REGISTRY:
            try: interp = INTERPRETATION_REGISTRY[check_name](output)
            except: pass
        if not interp:
            rs = _find_rs_number(check_name)
            if rs and rs in GENOTYPE_INTERPRETATIONS:
                gt_match = re.search(r'(\d/\d)', output)
                if gt_match:
                    interp = GENOTYPE_INTERPRETATIONS[rs].get(gt_match.group(1), "")
        categories[cat].append({"check": check_name, "output": output, "interpretation": interp, "exit_code": cr.get("exit_code")})

    # Collect PGS scores for this sample
    pgs_results = []
    for item_id, samples in scores_map.items():
        if sample_name in samples:
            d = samples[sample_name]
            pgs_results.append({"item_id": item_id, "percentile": d.get("percentile"), "z_score": d.get("z_score")})
    pgs_results.sort(key=lambda x: -(x.get("percentile") or 0))

    # Build report
    lines = [f"# Sample Summary — {sample_name}", "", f"*Generated {ts}*", ""]

    # AI executive summary
    try:
        from backend.api.ai_reports import write_ai_sample_summary
        ai_summary = write_ai_sample_summary(sample_name, categories, pgs_results)
        if ai_summary:
            lines.extend([ai_summary, "", "---", "", "# Detailed Results", ""])
    except Exception:
        pass

    for cat_name in ["Sex Verification", "Sample Quality", "Ancestry", "Monogenic", "Pharmacogenomics", "Variants"]:
        items = categories[cat_name]
        if not items:
            continue
        lines.extend([f"## {cat_name}", ""])
        for item in items:
            status = "Pass" if item["exit_code"] == 0 else "Manual" if item["exit_code"] == -1 else "Fail"
            lines.append(f"**{item['check']}**: {item['interpretation'] or item['output'][:100] or status}")
            lines.append("")

    if pgs_results:
        lines.extend(["## Polygenic Risk Scores", "", "| Percentile | Risk Level | Checklist Item |", "|-----------|-----------|----------------|"])
        for p in pgs_results:
            pct = p.get("percentile")
            if pct is None: continue
            risk = "HIGH RISK" if pct >= 90 else "Above Average" if pct >= 75 else "Average" if pct >= 25 else "Below Average" if pct >= 10 else "Low Risk"
            lines.append(f"| {pct:.1f}% | {risk} | {p['item_id']} |")
        lines.append("")

    other = categories["Other"]
    if other:
        lines.extend(["## Other Checks", ""])
        for item in other:
            lines.append(f"**{item['check']}**: {item['interpretation'] or item['output'][:100]}")
            lines.append("")

    filename = f"sample_{re.sub(r'[^a-zA-Z0-9_-]', '_', sample_name)}_summary.md"
    (_REPORTS_DIR / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return filename


def _resolve_command(check_name: str, raw_cmd: str, sample_path: str, sample_name: str = "") -> str | None:
    """Resolve a command: use registry if available, otherwise parse from markdown.
    Returns None if no executable command can be determined."""
    # Try registry first
    if check_name in COMMAND_REGISTRY:
        tpl = COMMAND_REGISTRY[check_name]
        if tpl is None:
            return None  # Tool not installed — manual check
        files = _find_sample_files(sample_name, sample_path)
        # Resolve thread count dynamically
        from backend.config import CPU_COUNT
        threads = str(min(CPU_COUNT, 16))
        cmd = tpl.replace("{sample}", sample_name)
        cmd = cmd.replace("{threads}", threads)
        # Strip --flag {placeholder} when file is empty to avoid argparse errors
        import re as _re_resolve
        if files["vcf"]:
            cmd = cmd.replace("{vcf}", files["vcf"])
        else:
            cmd = _re_resolve.sub(r'--vcf\s+\{vcf\}', '', cmd)
            cmd = cmd.replace("{vcf}", "")
        if files["bam"]:
            cmd = cmd.replace("{bam}", files["bam"])
        else:
            cmd = _re_resolve.sub(r'--bam\s+\{bam\}', '', cmd)
            cmd = cmd.replace("{bam}", "")
        return cmd
    # Try rs number lookup — search for rs# in the check name or checklist row
    rs = _find_rs_number(check_name)
    if rs and rs in RS_POSITIONS:
        pos = RS_POSITIONS[rs]
        files = _find_sample_files(sample_name, sample_path)
        vcf = files["vcf"]
        if not vcf:
            return f"echo 'No VCF/gVCF available for {sample_name}. Variant lookup ({rs} at {pos}) requires a VCF file. Run DeepVariant on the BAM first.'"
        return f"bcftools query -f '[%SAMPLE\\t%CHROM:%POS\\t%REF/%ALT\\t%GT]\\n' -r {pos} {vcf} 2>/dev/null || echo 'Variant {rs} not found at {pos}'"

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
    from backend.config import BCFTOOLS, SAMTOOLS, PLINK2, EXISTING_REFERENCE, CPU_COUNT

    # Clean markdown escaping
    result = cmd.replace("\\|", "|")

    # Resolve dynamic thread count
    threads = str(min(CPU_COUNT, 16))
    result = result.replace("{threads}", threads)

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
    uses_chr = True  # default
    if sample_path.endswith(".bam") or sample_path.endswith(".cram"):
        try:
            out = _sp.run(
                [SAMTOOLS, "idxstats", sample_path],
                capture_output=True, text=True, timeout=10,
            )
            first_contig = out.stdout.strip().split("\n")[0].split("\t")[0] if out.stdout else ""
            uses_chr = first_contig.startswith("chr")
        except Exception:
            pass
    elif sample_path.endswith(".vcf.gz") or sample_path.endswith(".vcf"):
        try:
            out = _sp.run(
                [BCFTOOLS, "view", "-h", sample_path],
                capture_output=True, text=True, timeout=10,
            )
            # Check first contig line
            for line in out.stdout.split("\n"):
                if line.startswith("##contig=<ID="):
                    contig_id = line.split("ID=")[1].split(",")[0].split(">")[0]
                    uses_chr = contig_id.startswith("chr")
                    break
        except Exception:
            pass

    if not uses_chr:
        # Replace chr-prefixed regions with bare names
        import re as _re_chr
        result = _re_chr.sub(r'\bchr(\d+):', r'\1:', result)
        result = _re_chr.sub(r'\bchr(\d+)\b', r'\1', result)
        result = result.replace("chrY:", "Y:").replace("chrX:", "X:")
        result = result.replace("chrM:", "M:").replace("chrMT:", "MT:")
        result = result.replace('"chrY"', '"Y"').replace('"chrX"', '"X"')
        result = result.replace('"chrM"', '"M"').replace('"chrMT"', '"MT"')

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
    resolved = _resolve_command(req.check_name, req.command, req.sample_path, req.sample_name)
    if not resolved:
        # This is a manual/descriptive task — mark as noted but don't execute
        state = _load_state()
        existing = state.get("notes", {}).get(req.item_id, "")
        if req.sample_name not in existing:
            state.setdefault("notes", {})[req.item_id] = f"{existing} | {req.sample_name}: manual check".strip(" |")
        _save_state(state)
        return {"ok": True, "exit_code": -1, "output": "Manual check — no automated command available. Complete this step manually and mark as done.", "item_id": req.item_id}

    cmd = _safe_command(resolved, req.sample_path, req.sample_name)

    # Safety: reject obviously dangerous commands (word-boundary check)
    import re as _re
    dangerous_patterns = [
        r'\brm\s+-', r'\brm\s+/', r'\bmkfs\b', r'>\s*/dev/(?!null)',
        r'\bchmod\b', r'\bchown\b', r'\bsudo\b', r'\bdd\s+if=',
    ]
    if any(_re.search(p, cmd) for p in dangerous_patterns):
        return JSONResponse({"error": "Command rejected for safety"}, 400)

    async def _exec(shell_cmd: str):
        p = await asyncio.create_subprocess_shell(
            shell_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        so, se = await asyncio.wait_for(p.communicate(), timeout=1800)  # 30 min for ancestry pipeline
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

        # Generate a full report for this check
        report_filename = None
        try:
            cn = req.check_name.lower()
            cat = ("sex" if any(k in cn for k in ["chromosome", "sry", "x:y", "het rate", "variant count on chr"]) else
                   "qc" if any(k in cn for k in ["flagstat", "ti/tv", "het/hom", "snp count", "indel", "duplicate", "mapped"]) else
                   "ancestry" if any(k in cn for k in ["pca", "roh", "haplo", "admixture", "ancestry", "neanderthal", "ibd", "hla"]) else
                   "monogenic" if any(k in cn for k in ["predisposition", "cardiovascular", "metabolism", "miscellaneous"]) else
                   "pharmacogenomics" if any(k in cn for k in ["cyp", "vkorc", "dpyd", "slco", "tpmt", "nudt", "hla-b", "g6pd"]) else
                   "variant")
            report_filename = generate_command_report(
                check_name=req.check_name, sample_name=req.sample_name,
                command=cmd, output=output, exit_code=exit_code, category=cat,
            )
        except Exception:
            pass

        # Update notes with report link
        if report_filename:
            summary = result_text.split("\n")[0][:60] if result_text else "done"
            state.setdefault("notes", {})[req.item_id] = f"[Report](/api/checklist/report/{report_filename}) | {req.sample_name}: {summary}"
        else:
            summary = result_text.split("\n")[0][:100] if result_text else "completed"
            existing_note = state.get("notes", {}).get(req.item_id, "")
            if req.sample_name not in existing_note:
                state.setdefault("notes", {})[req.item_id] = f"{existing_note} | {req.sample_name}: {summary}".strip(" |")

        _save_state(state)

        # Regenerate sample summary
        try:
            generate_sample_summary(req.sample_name)
        except Exception:
            pass

        return {
            "ok": True,
            "exit_code": exit_code,
            "output": result_text[:2000],
            "errors": errors[:500] if errors else None,
            "item_id": req.item_id,
            "report": report_filename,
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


# ── Section-level pipeline ─────────────────────────────────────────

class RunSectionRequest(BaseModel):
    section_id: str       # e.g. "s0-sex-gender-verification"
    section_title: str    # e.g. "Sex / Gender Verification"
    items: list[dict]     # [{item_id, check_name, pgs_id?}, ...]
    sample_name: str
    sample_path: str
    sample_type: str = "gvcf"


def generate_section_report(section_title, sample_name, results, pgs_results=None):
    """Generate ONE aggregated report for a complete section."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    safe_title = re.sub(r'[^a-zA-Z0-9_-]', '_', section_title)[:50]
    safe_sample = re.sub(r'[^a-zA-Z0-9_-]', '_', sample_name)
    filename = f"section_{safe_title}_{safe_sample}.md"
    report_path = _REPORTS_DIR / filename
    ts = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"# {section_title} — {sample_name}",
        "",
        f"*Report generated {ts} | {len(results)} checks completed*",
        "",
    ]

    # Generate a clear BOTTOM LINE conclusion
    successful = [r for r in results if r.get("exit_code") == 0]
    failed = [r for r in results if r.get("exit_code", -1) > 0]
    manual = [r for r in results if r.get("exit_code") == -1]

    # Auto-generate conclusion from interpretations
    interpretations = [r.get("interpretation", "") for r in results if r.get("interpretation")]
    pgs_items = [r for r in results if r.get("percentile") is not None]

    conclusion = ""
    title_lower = section_title.lower()

    # Sex-specific conclusion — parse from unified sex pipeline JSON
    if "sex" in title_lower or "gender" in title_lower:
        for r in results:
            raw = r.get("output", "")
            try:
                data = json.loads(raw)
                if "determination" in data:
                    det = data["determination"]
                    conclusion = f"**Conclusion: {sample_name} is {det['sex'].upper()}.** {det.get('reasoning', '')} ({len(data.get('checks', {}))} checks, confidence: {det.get('confidence', '?')}.)"
                    break
            except (json.JSONDecodeError, KeyError, TypeError):
                pass
            interp = r.get("interpretation", "")
            if "Genetic Sex:" in interp:
                for line in interp.split("\n"):
                    if "Genetic Sex:" in line:
                        conclusion = f"**Conclusion:** {line.strip()}"
                        break
                if conclusion:
                    break
        if not conclusion:
            conclusion = f"**Conclusion: Sex determination inconclusive.** Could not parse results."

    # QC-specific conclusion
    elif "quality" in title_lower or "contamination" in title_lower:
        conclusion = f"**Sample Quality: {len(successful)}/{len(results)} checks passed.** "
        if failed:
            conclusion += f"{len(failed)} check(s) flagged for review."
        else:
            conclusion += "All quality metrics are within acceptable ranges."

    # PGS-specific conclusion
    elif pgs_items:
        high_risk = [r for r in pgs_items if (r.get("percentile") or 0) >= 90]
        above_avg = [r for r in pgs_items if 75 <= (r.get("percentile") or 0) < 90]
        if high_risk:
            conclusion = f"**{len(high_risk)} HIGH RISK score(s) detected** (>90th percentile). "
            conclusion += "Elevated genetic risk identified — consider genetic counseling."
        elif above_avg:
            conclusion = f"**{len(above_avg)} above-average risk score(s)** detected. No high-risk findings."
        else:
            conclusion = f"**No elevated risk detected.** All {len(pgs_items)} scores are within the average range."

    # Ancestry conclusion
    elif "ancestry" in title_lower or "population" in title_lower:
        for r in results:
            # Try interpretation first
            interp = r.get("interpretation", "")
            if "Primary Ancestry" in interp:
                conclusion = interp.split("\n")[0]
                break
            # Try parsing raw JSON
            raw = r.get("output", "")
            if raw.startswith("{"):
                try:
                    data = json.loads(raw)
                    if "admixture_proportions" in data or "primary_ancestry" in data:
                        pop_names = {"EUR": "European", "AFR": "African", "EAS": "East Asian",
                                     "SAS": "South Asian", "AMR": "American (admixed)"}
                        primary = data.get("primary_ancestry", "?")
                        props = data.get("admixture_proportions", data.get("proportions", {}))
                        parts = [f"{pop_names.get(k,k)}: {v*100:.0f}%" for k, v in sorted(props.items(), key=lambda x: -x[1]) if v > 0.01]
                        conclusion = f"**Primary Ancestry: {pop_names.get(primary, primary)}.** Composition: {', '.join(parts)}."
                        if data.get("is_admixed"):
                            conclusion += " *Admixed sample.*"
                        break
                except (json.JSONDecodeError, KeyError):
                    pass
        if not conclusion:
            conclusion = f"Ancestry analysis completed with {len(successful)} checks."

    # Generic conclusion
    if not conclusion:
        conclusion = f"**{len(successful)}/{len(results)} checks completed successfully.**"

    lines.extend(["## Bottom Line", "", conclusion, ""])

    # Try AI-enhanced summary
    try:
        from backend.api.ai_reports import write_ai_report
        all_output = "\n".join(f"**{r['check']}**: {r.get('interpretation') or r.get('output', '')[:200]}" for r in results)
        ai = write_ai_report(
            analysis_type="section_report",
            check_name=section_title,
            sample_name=sample_name,
            raw_output=all_output,
            static_interpretation=conclusion,
            extra_context=f"Section report: {len(results)} checks for {section_title}. Conclusion: {conclusion}",
        )
        if ai:
            lines.extend(["## Detailed Analysis", "", ai, ""])
    except Exception:
        pass

    lines.extend(["---", "", "## Individual Results", ""])

    for r in results:
        status_icon = "\u2705" if r.get("exit_code") == 0 else "\u274C" if r.get("exit_code", -1) > 0 else "\u2B1C"
        lines.append(f"### {status_icon} {r['check']}")
        lines.append("")

        if r.get("interpretation"):
            lines.append(r["interpretation"])
            lines.append("")

        if r.get("output") and r["output"] != r.get("interpretation", ""):
            lines.extend(["**Raw output:**", "", "```", r["output"][:500], "```", ""])

        if r.get("percentile") is not None:
            pct = r["percentile"]
            risk = "HIGH RISK" if pct >= 90 else "Above Average" if pct >= 75 else "Average" if pct >= 25 else "Below Average" if pct >= 10 else "Low Risk"
            lines.append(f"**Percentile:** {pct:.1f}% ({risk})")
            if r.get("z_score") is not None:
                lines.append(f"**Z-score:** {r['z_score']:.2f}")
            lines.append("")

    # PGS summary table if any
    pgs_items = [r for r in results if r.get("percentile") is not None]
    if pgs_items:
        lines.extend(["## PGS Summary", "", "| Score | Percentile | Risk | Z-score |", "|-------|-----------|------|---------|"])
        for r in sorted(pgs_items, key=lambda x: -(x.get("percentile") or 0)):
            pct = r["percentile"]
            risk = "HIGH" if pct >= 90 else "Above Avg" if pct >= 75 else "Average" if pct >= 25 else "Below Avg"
            z = f"{r['z_score']:.2f}" if r.get("z_score") is not None else "--"
            lines.append(f"| {r['check']} | {pct:.1f}% | {risk} | {z} |")
        lines.append("")

    content = "\n".join(lines) + "\n"
    report_path.write_text(content, encoding="utf-8")
    return filename


@router.post("/run-section")
async def run_section(req: RunSectionRequest):
    """Run an entire checklist section as one pipeline. Returns section report."""
    import asyncio

    results = []
    pgs_ids = []
    cmd_items = []

    # Separate PGS items from command items
    for item in req.items:
        if item.get("pgs_id"):
            pgs_ids.append(item["pgs_id"])
        else:
            cmd_items.append(item)

    # Run PGS batch (if any)
    pgs_run_id = None
    if pgs_ids:
        try:
            from backend.api.runs import create_run, CreateRunRequest
            from backend.database import get_db
            body = CreateRunRequest(
                source_files=[{"path": req.sample_path, "type": req.sample_type}],
                pgs_ids=pgs_ids,
                engine="auto",
                ref_population="EUR",
            )
            db = next(get_db())
            try:
                run_resp = await create_run(body, db)
                pgs_run_id = run_resp.get("run_id")
            finally:
                db.close()
        except Exception as e:
            results.append({"check": f"PGS batch ({len(pgs_ids)} scores)", "output": f"Failed: {e}", "exit_code": 1})

    # Run command items sequentially
    for item in cmd_items:
        check_name = item.get("check_name", "")
        item_id = item.get("item_id", "")

        resolved = _resolve_command(check_name, "", req.sample_path, req.sample_name)
        if not resolved:
            results.append({"check": check_name, "output": "Manual check", "exit_code": -1, "item_id": item_id})
            continue

        cmd = _safe_command(resolved, req.sample_path, req.sample_name)

        # Safety check
        import re as _re
        dangerous = [r'\brm\s+-', r'\brm\s+/', r'\bmkfs\b', r'>\s*/dev/(?!null)', r'\bchmod\b', r'\bchown\b', r'\bsudo\b', r'\bdd\s+if=']
        if any(_re.search(p, cmd) for p in dangerous):
            results.append({"check": check_name, "output": "Rejected for safety", "exit_code": -2, "item_id": item_id})
            continue

        try:
            proc = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=1800)
            output = stdout.decode("utf-8", errors="replace").strip()
            errors = stderr.decode("utf-8", errors="replace").strip()
            exit_code = proc.returncode

            if exit_code in (3, 7) and output:
                exit_code = 0

            # Get interpretation
            interpretation = ""
            if check_name in INTERPRETATION_REGISTRY:
                try: interpretation = INTERPRETATION_REGISTRY[check_name](output)
                except: pass
            if not interpretation:
                rs = _find_rs_number(check_name)
                if rs and rs in GENOTYPE_INTERPRETATIONS:
                    gt_match = re.search(r'(\d/\d)', output)
                    if gt_match:
                        interpretation = GENOTYPE_INTERPRETATIONS[rs].get(gt_match.group(1), "")

            # Save per-item state
            state = _load_state()
            if exit_code == 0:
                state.setdefault("checked", {})[item_id] = True
            state.setdefault("command_results", {})[item_id] = {
                "sample": req.sample_name, "check": check_name,
                "output": (output if exit_code == 0 else f"ERROR (exit {exit_code}): {errors or output}")[:2000],
                "exit_code": exit_code, "timestamp": __import__("datetime").datetime.now().isoformat(),
            }

            # Generate individual report too
            try:
                cn = check_name.lower()
                cat = ("sex" if any(k in cn for k in ["chromosome", "sry", "x:y", "het rate", "variant count on chr"]) else
                       "qc" if any(k in cn for k in ["flagstat", "ti/tv", "het/hom", "snp count", "indel", "duplicate", "mapped"]) else
                       "ancestry" if any(k in cn for k in ["pca", "roh", "haplo", "admixture", "ancestry", "neanderthal"]) else
                       "variant")
                report_fn = generate_command_report(
                    check_name=check_name, sample_name=req.sample_name,
                    command=cmd, output=output, exit_code=exit_code, category=cat,
                )
                if report_fn:
                    state.setdefault("notes", {})[item_id] = f"[Report](/api/checklist/report/{report_fn}) | {req.sample_name}: {(output or 'done')[:50]}"
            except:
                pass

            _save_state(state)

            results.append({
                "check": check_name, "output": output, "exit_code": exit_code,
                "interpretation": interpretation, "item_id": item_id,
            })

        except asyncio.TimeoutError:
            results.append({"check": check_name, "output": "Timed out (30 min)", "exit_code": -3, "item_id": item_id})
        except Exception as e:
            results.append({"check": check_name, "output": str(e), "exit_code": 1, "item_id": item_id})

    # Wait for PGS run if started
    if pgs_run_id:
        from backend.database import SessionLocal
        from backend.models.schemas import ScoringRun, RunResult
        import time
        for _ in range(120):  # Wait up to 10 min
            await asyncio.sleep(5)
            db2 = SessionLocal()
            run = db2.query(ScoringRun).filter(ScoringRun.id == pgs_run_id).first()
            if run and run.status in ("complete", "failed"):
                if run.status == "complete":
                    run_results = db2.query(RunResult).filter(RunResult.run_id == run.id).all()
                    for rr in run_results:
                        scores = rr.scores_json if isinstance(rr.scores_json, list) else json.loads(rr.scores_json) if rr.scores_json else []
                        for s in scores:
                            results.append({
                                "check": f"{rr.pgs_id}",
                                "output": f"matched={rr.variants_matched}/{rr.variants_total}",
                                "exit_code": 0,
                                "percentile": s.get("percentile") or s.get("rank"),
                                "z_score": s.get("z_score"),
                                "interpretation": "",
                            })
                    # Sync checklist
                    try: sync_checklist_from_db()
                    except: pass
                else:
                    results.append({"check": f"PGS batch", "output": f"Failed: {run.error_message}", "exit_code": 1})
                db2.close()
                break
            db2.close()

    # Generate section report
    report_filename = None
    try:
        report_filename = generate_section_report(req.section_title, req.sample_name, results)
        # Save section report link in state so the UI can find it
        if report_filename:
            state = _load_state()
            state.setdefault("notes", {})[f"{req.section_id}:_section_report"] = f"[Report](/api/checklist/report/{report_filename})"
            _save_state(state)
    except Exception:
        pass

    # Update sample summary
    try:
        generate_sample_summary(req.sample_name)
    except Exception:
        pass

    return {
        "ok": True,
        "results": results,
        "report": report_filename,
        "section": req.section_title,
        "sample": req.sample_name,
        "total_items": len(results),
        "successful": sum(1 for r in results if r.get("exit_code") == 0),
    }
