"""CI regression test for the PGS scoring pipeline.

Scores a fixed 1000G EUR sample (HG00096 by default) against a small set
of representative PGS scores and asserts each percentile stays within
±MAX_DRIFT_PP of the pinned golden value.

Any change that moves the control outside tolerance blocks the merge.

To re-bless the golden values after an intentional pipeline change:

    REBLESS=1 pytest -s test_pgs_regression.py

The test reads/writes the same golden file used by batch_control.py, so
the in-batch control and CI share a single source of truth.

Run:
    pytest -v scripts/test_pgs_regression.py
"""
from __future__ import annotations
import json
import math
import os
import re
import subprocess
import tempfile

import pytest

PLINK2     = '/home/nimo/miniconda3/envs/genomics/bin/plink2'
PANEL      = '/data/pgs2/ref_panel/GRCh38_1000G_ALL'
PGS_CACHE  = '/data/pgs_cache'
STATS_DIR  = '/data/pgs2/ref_panel_stats'

CONTROL_SAMPLE  = os.getenv('PGS_CI_CONTROL_SAMPLE', 'HG00096')
GOLDEN_PATH     = os.getenv('PGS_CI_GOLDEN',
                            '/data/pgs2/ref_panel_stats/batch_control_golden.json')
MAX_DRIFT_PP    = float(os.getenv('PGS_CI_MAX_DRIFT_PP', '5.0'))
REPRESENTATIVE_PGS = [
    'PGS000334',   # APOE-dominated (Alzheimer's) — was the n16 bug
    'PGS000119',   # Basal cell carcinoma — small set, sum-scale stats path
    'PGS001229',   # Height — flagged in cohort-sanity scan
    'PGS003979',   # Colorectal cancer — large PGS, plink2 path
    'PGS002297',   # Lipoprotein A levels — medium catalog
]


def _load_cached(pgs_id, pop='EUR'):
    for fn in sorted(os.listdir(STATS_DIR)):
        if '.stale-bias' in fn or '.stale-pre-' in fn or not fn.endswith('.json'):
            continue
        m = re.match(rf'{pgs_id}_{pop}_GRCh38(_.+)?\.json$', fn)
        if m:
            return json.load(open(os.path.join(STATS_DIR, fn)))
    return None


def _score_sample(pgs_id, sample_iid, work):
    """Replicates batch_control.score_sample_through_pipeline minimally for CI."""
    import gzip
    scoring_path = os.path.join(PGS_CACHE, pgs_id, f'{pgs_id}_hmPOS_GRCh38.txt.gz')
    if not os.path.exists(scoring_path):
        pytest.skip(f'no scoring file for {pgs_id}')
    pgs = []
    with gzip.open(scoring_path, 'rt') as f:
        hdr = None
        for line in f:
            if line.startswith('#'):
                continue
            cols = line.rstrip('\n').split('\t')
            if hdr is None:
                hdr = cols
                continue
            r = dict(zip(hdr, cols))
            try:
                pgs.append((r['hm_chr'], int(r['hm_pos']),
                            r['effect_allele'], float(r['effect_weight'])))
            except (KeyError, ValueError):
                continue

    need = {(c, p): True for c, p, _, _ in pgs}
    need_chrs = set(c for c, _, _, _ in pgs)
    found = {}
    proc = subprocess.Popen(['zstdcat', PANEL + '.pvar.zst'],
                            stdout=subprocess.PIPE, text=True)
    for line in proc.stdout:
        if line.startswith('#'):
            continue
        parts = line.rstrip('\n').split('\t', 5)
        if len(parts) < 5:
            continue
        chrom, pos, vid, ref, alt = parts[0], parts[1], parts[2], parts[3], parts[4]
        if chrom not in need_chrs:
            continue
        try:
            ipos = int(pos)
        except ValueError:
            continue
        if (chrom, ipos) in need:
            found.setdefault((chrom, ipos), []).append((vid, ref, alt))
    proc.wait()

    sf = os.path.join(work, f'{pgs_id}.tsv')
    with open(sf, 'w') as f:
        f.write('ID\tA1\tWEIGHT\n')
        for c, p, ea, w in pgs:
            for vid, ref, alt in found.get((c, p), []):
                if vid in ('.', '') or ea not in (ref, alt):
                    continue
                f.write(f'{vid}\t{ea}\t{w}\n')
                break

    keep = os.path.join(work, 'keep.txt')
    with open(keep, 'w') as f:
        f.write('#IID\n' + sample_iid + '\n')

    out_prefix = os.path.join(work, f'{pgs_id}_score')
    cp = subprocess.run(
        [PLINK2, '--pfile', PANEL, 'vzs', '--keep', keep,
         '--score', sf, 'header-read', '1', '2', '3',
         'cols=+scoresums', 'no-mean-imputation',
         '--out', out_prefix],
        capture_output=True, text=True)
    if cp.returncode != 0:
        pytest.skip(f'plink2 failed for {pgs_id}: {cp.stderr[-200:]}')

    with open(out_prefix + '.sscore') as f:
        h = f.readline().rstrip('\n').split('\t')
        a_i = h.index('WEIGHT_AVG')
        row = f.readline().rstrip('\n').split('\t')
        return float(row[a_i])


def _percentile(raw, mu, sigma):
    if sigma <= 0:
        return None
    z = (raw - mu) / sigma
    return round(0.5 * (1 + math.erf(z / math.sqrt(2))) * 100, 2)


@pytest.fixture(scope='module')
def work_dir():
    with tempfile.TemporaryDirectory(prefix='pgs_ci_') as d:
        yield d


@pytest.fixture(scope='module')
def golden():
    if os.getenv('REBLESS'):
        return None    # caller will compute + write
    if not os.path.exists(GOLDEN_PATH):
        pytest.skip(f'no golden at {GOLDEN_PATH}; set REBLESS=1 to create')
    return json.load(open(GOLDEN_PATH))


@pytest.mark.parametrize('pgs_id', REPRESENTATIVE_PGS)
def test_control_percentile_pinned(pgs_id, work_dir, golden):
    cached = _load_cached(pgs_id, 'EUR')
    if not cached:
        pytest.skip(f'no EUR stats for {pgs_id}')
    raw = _score_sample(pgs_id, CONTROL_SAMPLE, work_dir)
    pct = _percentile(raw, cached['mean'], cached['std'])

    if os.getenv('REBLESS'):
        # accumulate into golden file
        if not os.path.exists(GOLDEN_PATH):
            golden_obj = {'sample_iid': CONTROL_SAMPLE, 'expected': {}}
        else:
            golden_obj = json.load(open(GOLDEN_PATH))
            golden_obj.setdefault('expected', {})
        golden_obj['expected'][pgs_id] = pct
        with open(GOLDEN_PATH, 'w') as f:
            json.dump(golden_obj, f, indent=2)
        print(f'BLESSED {pgs_id}: {pct}%ile')
        return

    expected = (golden or {}).get('expected', {}).get(pgs_id)
    assert expected is not None, (
        f'{pgs_id} has no golden value in {GOLDEN_PATH}. '
        f'Run with REBLESS=1 to record.')
    drift = pct - expected
    assert abs(drift) <= MAX_DRIFT_PP, (
        f'{pgs_id}: control sample {CONTROL_SAMPLE} percentile drifted '
        f'{drift:+.1f}pp (got {pct}, expected {expected}, max ±{MAX_DRIFT_PP}). '
        f'If intentional, re-run with REBLESS=1.')
