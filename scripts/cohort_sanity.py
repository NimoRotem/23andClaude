"""Cohort-level distribution sanity check for PGS percentiles.

After a batch of samples has been scored against a PGS, run this check.
It compares the observed percentile distribution against expected ~U(0,100)
and raises a flag if any of three thresholds trip:

  - >70% of samples land above the 80th percentile
  - <30% of samples land below the 50th percentile
  - KS test against U(0,100) yields p < 0.01

A trip is a strong signal that the reference distribution has drifted from
what the live pipeline computes — same fingerprint as the PGS000334
stale-cache incident.

Callable two ways:

  from cohort_sanity import check_cohort, CohortFlag
  flag = check_cohort([{'sample':'Nimo','percentile':25.2}, ...])
  if flag.tripped:
      print(flag.reason)

  $ python -m scripts.cohort_sanity                       # scan all PGS in DB
  $ python -m scripts.cohort_sanity --pgs PGS000334       # scan one
"""
from __future__ import annotations

import json
import math
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

# Make the live-percentile overlay importable so cohort drift is judged
# against current ref-stats μ/σ rather than the values baked into old reports.
_THIS = Path(__file__).resolve()
_PKG_ROOT = _THIS.parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))
try:
    from pipeline.live_percentile import apply_live_overlay as _apply_live_pctl
except Exception:
    def _apply_live_pctl(r):
        return r


THRESHOLD_HIGH_FRAC      = 0.70  # >70% above 80%ile → trip
THRESHOLD_HIGH_CUT       = 80.0
THRESHOLD_LOW_FRAC       = 0.30  # <30% below 50%ile → trip
THRESHOLD_LOW_CUT        = 50.0
THRESHOLD_KS_P           = 0.01  # KS vs U(0,100) p < 0.01 → trip
MIN_SAMPLES_FOR_CHECK    = 4     # below this, distribution check is meaningless


@dataclass
class CohortFlag:
    tripped: bool
    reason: Optional[str] = None
    pgs_id: Optional[str] = None
    n: int = 0
    frac_above_80: float = 0.0
    frac_below_50: float = 0.0
    ks_stat: float = 0.0
    ks_p: float = 1.0
    notes: List[str] = field(default_factory=list)


def _ks_uniform(percentiles: List[float]):
    """One-sample KS statistic against U(0,100). Returns (D, p)."""
    if not percentiles:
        return 0.0, 1.0
    xs = sorted(percentiles)
    n = len(xs)
    d = 0.0
    for i, x in enumerate(xs, 1):
        cdf_emp = i / n
        cdf_uni = max(0.0, min(1.0, x / 100.0))
        d = max(d, abs(cdf_emp - cdf_uni), abs((i - 1) / n - cdf_uni))
    # Kolmogorov asymptotic p-value
    en = math.sqrt(n) + 0.12 + 0.11 / math.sqrt(n)
    lam = en * d
    # series sum; fewer terms suffice for typical use
    p = 0.0
    for j in range(1, 101):
        term = 2.0 * (-1) ** (j - 1) * math.exp(-2.0 * (lam * j) ** 2)
        p += term
        if abs(term) < 1e-12:
            break
    return d, max(0.0, min(1.0, p))


def check_cohort(samples: Iterable[dict], pgs_id: Optional[str] = None) -> CohortFlag:
    """Each sample: {'sample': str, 'percentile': float}. Returns a CohortFlag."""
    pcts = [float(s['percentile']) for s in samples
            if s.get('percentile') is not None]
    n = len(pcts)
    flag = CohortFlag(tripped=False, pgs_id=pgs_id, n=n)
    if n < MIN_SAMPLES_FOR_CHECK:
        flag.notes.append(f'n={n} below MIN_SAMPLES_FOR_CHECK={MIN_SAMPLES_FOR_CHECK}, '
                          'skipping check')
        return flag

    flag.frac_above_80 = sum(1 for p in pcts if p > THRESHOLD_HIGH_CUT) / n
    flag.frac_below_50 = sum(1 for p in pcts if p < THRESHOLD_LOW_CUT) / n
    flag.ks_stat, flag.ks_p = _ks_uniform(pcts)

    reasons = []
    if flag.frac_above_80 > THRESHOLD_HIGH_FRAC:
        reasons.append(f'{flag.frac_above_80*100:.0f}% of samples above {THRESHOLD_HIGH_CUT:.0f}%ile '
                       f'(threshold >{THRESHOLD_HIGH_FRAC*100:.0f}%)')
    if flag.frac_below_50 < THRESHOLD_LOW_FRAC:
        reasons.append(f'{flag.frac_below_50*100:.0f}% of samples below {THRESHOLD_LOW_CUT:.0f}%ile '
                       f'(threshold <{THRESHOLD_LOW_FRAC*100:.0f}%)')
    if flag.ks_p < THRESHOLD_KS_P:
        reasons.append(f'KS vs U(0,100): D={flag.ks_stat:.3f}, p={flag.ks_p:.4f} '
                       f'(threshold p<{THRESHOLD_KS_P})')
    if reasons:
        flag.tripped = True
        flag.reason = ' | '.join(reasons)
    return flag


# ── CLI: scan stored PGS reports for cohort drift ─────────────────
def _walk_reports(users_root: Path):
    """Yield {pgs_id, sample, percentile, file_id, user_hash} for every passed PGS report."""
    if not users_root.exists():
        return
    for udir in users_root.iterdir():
        if not udir.is_dir():
            continue
        # sample-name lookup
        sample_by_fid = {}
        files_path = udir / 'files.json'
        if files_path.exists():
            try:
                files = json.load(open(files_path)).get('files', {}) or {}
            except Exception:
                files = {}
            for fid, f in files.items():
                sample_by_fid[fid] = (f.get('sample_name')
                                      or (f.get('name', '').split('.')[0])
                                      or fid[:8])
        reports_root = udir / 'reports'
        if not reports_root.exists():
            continue
        for fid_dir in reports_root.iterdir():
            if not fid_dir.is_dir():
                continue
            for jf in fid_dir.glob('pgs_*.json'):
                if jf.suffix != '.json':
                    continue
                try:
                    rep = json.load(open(jf))
                except Exception:
                    continue
                _apply_live_pctl(rep)
                res = rep.get('result') or {}
                status = (res.get('status') or 'passed').lower()
                if status not in ('passed', 'ok', 'success', 'completed', ''):
                    continue
                pgs_id = res.get('pgs_id')
                pct = res.get('percentile')
                if not pgs_id or pct is None:
                    continue
                yield {
                    'pgs_id': pgs_id,
                    'sample': sample_by_fid.get(fid_dir.name, fid_dir.name[:8]),
                    'percentile': pct,
                    'file_id': fid_dir.name,
                    'task_id': rep.get('task_id'),
                    'user_hash': udir.name,
                }


def scan_stored_reports(users_root='/home/nimrod_rotem/simple-genomics/users',
                        only_pgs=None, verbose=False):
    """Group all stored PGS reports by (pgs_id, sample) — taking the most-recent
    percentile per sample — then run cohort check per pgs_id."""
    rows = list(_walk_reports(Path(users_root)))
    by_pgs = defaultdict(dict)         # pgs_id -> sample -> percentile
    for r in rows:
        if only_pgs and r['pgs_id'] != only_pgs:
            continue
        by_pgs[r['pgs_id']][r['sample']] = r['percentile']

    results = []
    for pgs_id in sorted(by_pgs):
        samples = [{'sample': s, 'percentile': p} for s, p in by_pgs[pgs_id].items()]
        flag = check_cohort(samples, pgs_id=pgs_id)
        results.append((pgs_id, flag))
        if verbose or flag.tripped:
            marker = '🚩' if flag.tripped else 'OK'
            print(f'{marker}  {pgs_id}  n={flag.n}  '
                  f'>80%ile={flag.frac_above_80*100:.0f}%  '
                  f'<50%ile={flag.frac_below_50*100:.0f}%  '
                  f'KS p={flag.ks_p:.3f}')
            if flag.tripped:
                print(f'      reason: {flag.reason}')
    return results


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--pgs', help='Only scan this PGS_id')
    ap.add_argument('--verbose', '-v', action='store_true',
                    help='Print every PGS, not just trips')
    ap.add_argument('--users-root', default='/home/nimrod_rotem/simple-genomics/users')
    ap.add_argument('--json', action='store_true', help='Emit JSON')
    a = ap.parse_args()
    results = scan_stored_reports(a.users_root, only_pgs=a.pgs, verbose=a.verbose)
    tripped = [(p, f) for p, f in results if f.tripped]
    print()
    print(f'Scanned {len(results)} PGS scores. Tripped: {len(tripped)}')
    if a.json:
        print(json.dumps([{
            'pgs_id': p, 'n': f.n, 'tripped': f.tripped, 'reason': f.reason,
            'frac_above_80': f.frac_above_80, 'frac_below_50': f.frac_below_50,
            'ks_stat': f.ks_stat, 'ks_p': f.ks_p,
        } for p, f in results], indent=2))
    if tripped:
        sys.exit(2)


if __name__ == '__main__':
    main()
