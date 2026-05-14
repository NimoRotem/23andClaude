"""Result-assembly guards: provenance + interpretation consistency.

Two responsibilities:

1. attach_provenance(report) — add a `provenance` block to the report dict
   with sha256 of scoring file, stats file, ref panel, and pipeline commit.
   Enables retroactive identification of results from stale stats and full
   replay of any historical computation.

2. check_interpretation_directional(report) — parse the LLM interpretation
   text for directional phrases, compare to the numeric percentile bin, and
   DROP the interpretation if they disagree. Sets
   `interpretation_consistency_error` with the specific mismatch.

Both are idempotent and safe to re-invoke.
"""
from __future__ import annotations
import hashlib
import os
import re
import subprocess
from typing import Optional

PGS_CACHE_DIR = '/data/pgs_cache'
REF_PANEL     = '/data/pgs2/ref_panel/GRCh38_1000G_ALL'
SIMPLE_GENOMICS_REPO = '/home/nimrod_rotem/simple-genomics'


# ── Provenance ────────────────────────────────────────────────────
def _sha256_file(path):
    h = hashlib.sha256()
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(1 << 20), b''):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def _ref_panel_sha():
    h = hashlib.sha256()
    any_file = False
    for ext in ('.pgen', '.pvar.zst', '.psam'):
        p = REF_PANEL + ext
        try:
            st = os.stat(p)
            h.update(f"{p}|{st.st_size}|{int(st.st_mtime)}".encode())
            any_file = True
        except OSError:
            continue
    return h.hexdigest() if any_file else None


_PIPELINE_COMMIT = None


def _pipeline_commit():
    global _PIPELINE_COMMIT
    if _PIPELINE_COMMIT is not None:
        return _PIPELINE_COMMIT
    try:
        out = subprocess.run(['git', '-C', SIMPLE_GENOMICS_REPO, 'rev-parse', 'HEAD'],
                             capture_output=True, text=True, check=True, timeout=5)
        _PIPELINE_COMMIT = out.stdout.strip()
    except Exception:
        _PIPELINE_COMMIT = ''
    return _PIPELINE_COMMIT or None


def attach_provenance(report):
    """Mutate report dict to add a `provenance` sub-dict. Safe to call twice."""
    res = report.get('result') or {}
    pgs_id = res.get('pgs_id') or (report.get('test_id') or '').split('_')[-2:][-1] \
        if report.get('test_id') else None
    pi = res.get('pipeline_info') or {}
    pd = pi.get('percentile_details') or {}

    scoring_path = None
    if pgs_id:
        p = os.path.join(PGS_CACHE_DIR, pgs_id, f'{pgs_id}_hmPOS_GRCh38.txt.gz')
        if os.path.exists(p):
            scoring_path = p
    stats_path = pd.get('stats_file') or (
        res.get('scoring_diagnostics') or {}).get('stats_file')
    # Older code paths stored just the basename — fall back to the canonical dir
    if stats_path and not os.path.isabs(stats_path) and not os.path.exists(stats_path):
        alt = os.path.join('/data/pgs2/ref_panel_stats', os.path.basename(stats_path))
        if os.path.exists(alt):
            stats_path = alt

    prov = {
        'pgs_catalog_id': pgs_id,
        'scoring_file_path': scoring_path,
        'scoring_file_sha256': _sha256_file(scoring_path) if scoring_path else None,
        'stats_file_path': stats_path,
        'stats_file_sha256': _sha256_file(stats_path) if stats_path else None,
        'ref_panel_path': REF_PANEL,
        'ref_panel_sha256': _ref_panel_sha(),
        'pipeline_commit': _pipeline_commit(),
    }
    report['provenance'] = prov
    return report


# ── Interpretation directional check ──────────────────────────────
_HIGH_RX = re.compile(
    r'\b(elevated|increased|higher( than)?( average)?|above average|'
    r'high(er)? risk|substantially elevated|markedly increased|'
    r'top \d+%|upper percentile)\b', re.IGNORECASE)
_LOW_RX = re.compile(
    r'\b(low(er)?( than)?( average)?|decreased|reduced|below average|'
    r'protective|lower risk|bottom \d+%)\b', re.IGNORECASE)
_MID_RX = re.compile(
    r'\b(average|typical|near average|middle of the (distribution|range)|'
    r'within (normal|expected) range)\b', re.IGNORECASE)


def _percentile_bin(pct):
    """Map a percentile to a directional bin. Returns 'LOW', 'MID', 'HIGH', or None."""
    if pct is None:
        return None
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return None
    if p < 25:    return 'LOW'
    if p > 75:    return 'HIGH'
    return 'MID'


def _interpretation_directional_bin(text):
    """Scan interpretation text for directional phrases. Returns the dominant bin or None."""
    if not text:
        return None
    high_n = len(_HIGH_RX.findall(text))
    low_n  = len(_LOW_RX.findall(text))
    mid_n  = len(_MID_RX.findall(text))
    counts = {'HIGH': high_n, 'LOW': low_n, 'MID': mid_n}
    top = max(counts.values())
    if top == 0:
        return None    # interpretation makes no directional claim
    winners = [k for k, v in counts.items() if v == top]
    return winners[0] if len(winners) == 1 else None


_INCOMPAT = {
    ('LOW', 'HIGH'), ('HIGH', 'LOW'),
    ('LOW', 'MID'),  ('HIGH', 'MID'),       # "average" claim against extreme percentile
    ('MID', 'LOW'),  ('MID', 'HIGH'),       # "low/high" claim against middling percentile
}


def check_interpretation_directional(report):
    """Sanity-check the LLM interpretation against the numeric percentile.

    Drops the interpretation in three cases:
      1. Interpretation makes a directional claim (HIGH/LOW/MID) that contradicts
         the numeric percentile bin.
      2. Interpretation makes a directional claim but percentile is None
         (incompatible_ref_stats / unavailable / refused). Without a percentile
         there is no population context to support a directional statement;
         allowing the LLM to say "lower predisposition" anyway is the misleading
         output the PGS000898 audit flagged.
      3. Interpretation makes a directional claim while the runner self-reported
         confidence=low (e.g., weak build inference, no precomputed stats,
         cross-ancestry transfer). Low-confidence results should not carry
         directional clinical phrasing.
    """
    interp = report.get('interpretation')
    if not interp:
        return report
    res = report.get('result') or {}
    pct = res.get('percentile')
    pbin = _percentile_bin(pct)
    ibin = _interpretation_directional_bin(interp)

    # Case (1) — directional contradiction
    if pbin is not None and ibin is not None and (ibin, pbin) in _INCOMPAT:
        report['interpretation_dropped'] = report.pop('interpretation')
        report['interpretation_consistency_error'] = (
            f"directional contradiction: interpretation says {ibin} but "
            f"percentile={pct} ({pbin}). Interpretation dropped.")
        return report

    # Case (2) — directional claim but no percentile to back it up
    if pbin is None and ibin is not None:
        method = ((res.get('pipeline_info') or {})
                  .get('percentile_details') or {}).get('method')
        report['interpretation_dropped'] = report.pop('interpretation')
        report['interpretation_consistency_error'] = (
            f"directional interpretation ({ibin}) emitted without a valid "
            f"percentile (method={method!r}). Without population context the "
            f"directional claim is unsupported. Interpretation dropped.")
        return report

    # Case (3) — directional claim under explicitly low confidence
    if ibin is not None and (res.get('confidence') or '').lower() == 'low':
        reason = (res.get('confidence_reason')
                  or res.get('cross_ancestry_warning')
                  or 'confidence=low')
        report['interpretation_dropped'] = report.pop('interpretation')
        report['interpretation_consistency_error'] = (
            f"directional interpretation ({ibin}) emitted under low confidence "
            f"({reason}). Interpretation dropped.")
    return report
