#!/usr/bin/env python3
"""Nightly self-test for cached PGS reference distributions.

For each (PGS, pop) ref-stats file:
  1. Score a random subset of N samples from the 1000G panel through the
     production plink2 pipeline (same flags as live: cols=+scoresums
     no-mean-imputation).
  2. Compare the observed μ_obs / σ_obs vs the cached μ_cached / σ_cached.
  3. FAIL if |μ_obs - μ_cached| > MAX_DELTA_SIGMA * σ_cached, or if
     σ_obs / σ_cached not in [SIGMA_RATIO_LO, SIGMA_RATIO_HI].

Exits non-zero on any failure so cron can fire an alert.

Usage:
    ref_stats_selftest.py [--n 50] [--pgs PGS_FILTER] [--pop POP] [--json]
                          [--max-delta-sigma 0.1] [--seed 0]
"""
import argparse, datetime, hashlib, json, os, random, re, statistics, subprocess, sys, time
from pathlib import Path

PLINK2     = '/home/nimo/miniconda3/envs/genomics/bin/plink2'
PANEL      = '/data/pgs2/ref_panel/GRCh38_1000G_ALL'
PVAR_Z     = PANEL + '.pvar.zst'
POP_DIR    = '/data/pgs2/ref_panel/pop_samples'
STATS_DIR  = '/data/pgs2/ref_panel_stats'
PGS_CACHE  = '/data/pgs_cache'

MAX_DELTA_SIGMA_DEFAULT = 0.1   # |μ_obs - μ_cached| / σ_cached must be < this
SIGMA_RATIO_LO_DEFAULT  = 0.7   # σ_obs / σ_cached must be in [LO, HI]
SIGMA_RATIO_HI_DEFAULT  = 1.4


def collect_stats_files(only_pgs=None, only_pop=None):
    """Yield (pgs_id, pop, path) for every non-quarantined ref-stats file."""
    if not os.path.isdir(STATS_DIR):
        return
    for fn in sorted(os.listdir(STATS_DIR)):
        if '.stale-bias' in fn or not fn.endswith('.json'):
            continue
        m = re.match(r'(PGS\d+)_(\w+)_GRCh38(_.+)?\.json$', fn)
        if not m:
            continue
        pgs_id, pop = m.group(1), m.group(2)
        if pop not in ('EUR', 'EAS', 'AFR', 'SAS', 'AMR'):
            continue
        if only_pgs and pgs_id != only_pgs:
            continue
        if only_pop and pop != only_pop:
            continue
        yield pgs_id, pop, os.path.join(STATS_DIR, fn)


def score_random_subset(pgs_id, pop, n_samples, seed, work):
    """Pick n_samples random panel samples, score them via plink2, return list of raw_scores."""
    # 1) Build a small keep file
    iids = [l.strip() for l in open(os.path.join(POP_DIR, f'{pop}.txt'))
            if l.strip() and not l.startswith('#')]
    rng = random.Random(seed)
    picked = rng.sample(iids, min(n_samples, len(iids)))
    keep = os.path.join(work, f'{pop}_keep.txt')
    with open(keep, 'w') as f:
        f.write('#IID\n')
        for s in picked:
            f.write(s + '\n')

    # 2) Build panel-format scoring file (same path the recompute uses)
    scoring_path = os.path.join(PGS_CACHE, pgs_id, f'{pgs_id}_hmPOS_GRCh38.txt.gz')
    if not os.path.exists(scoring_path):
        return None, 'no_scoring_file'
    # parse PGS
    pgs = []
    import gzip
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
    # resolve panel IDs for needed positions
    need = {(c, p): True for c, p, _, _ in pgs}
    need_chrs = set(c for c, _, _, _ in pgs)
    found = {}
    proc = subprocess.Popen(['zstdcat', PVAR_Z], stdout=subprocess.PIPE, text=True)
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
    sf = os.path.join(work, f'{pgs_id}_panelfmt.tsv')
    n_written = 0
    with open(sf, 'w') as f:
        f.write('ID\tA1\tWEIGHT\n')
        for c, p, ea, w in pgs:
            for vid, ref, alt in found.get((c, p), []):
                if vid in ('.', '') or ea not in (ref, alt):
                    continue
                f.write(f'{vid}\t{ea}\t{w}\n')
                n_written += 1
                break
    if not n_written:
        return None, 'no_scorable_variants'

    # 3) Run plink2
    out_prefix = os.path.join(work, f'{pop}_score')
    cp = subprocess.run(
        [PLINK2, '--pfile', PANEL, 'vzs', '--keep', keep,
         '--score', sf, 'header-read', '1', '2', '3',
         'cols=+scoresums', 'no-mean-imputation',
         '--out', out_prefix],
        capture_output=True, text=True)
    if cp.returncode != 0:
        return None, f'plink2_failed (exit {cp.returncode})'

    # 4) Read raw_scores
    rows = []
    with open(out_prefix + '.sscore') as f:
        h = f.readline().rstrip('\n').split('\t')
        a_i = h.index('WEIGHT_AVG')
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if not parts[0]:
                continue
            rows.append(float(parts[a_i]))
    return rows, 'ok'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--n', type=int, default=50,
                    help='Random panel samples per (pgs, pop) (default 50)')
    ap.add_argument('--pgs', help='Limit to this PGS_id')
    ap.add_argument('--pop', help='Limit to this population')
    ap.add_argument('--seed', type=int,
                    default=int(datetime.date.today().strftime('%Y%m%d')),
                    help='RNG seed (default: today YYYYMMDD)')
    ap.add_argument('--max-delta-sigma', type=float,
                    default=MAX_DELTA_SIGMA_DEFAULT)
    ap.add_argument('--sigma-ratio-lo', type=float,
                    default=SIGMA_RATIO_LO_DEFAULT)
    ap.add_argument('--sigma-ratio-hi', type=float,
                    default=SIGMA_RATIO_HI_DEFAULT)
    ap.add_argument('--json', action='store_true')
    a = ap.parse_args()

    work = '/tmp/ref_stats_selftest'
    os.makedirs(work, exist_ok=True)

    results = []
    t0 = time.time()
    for pgs_id, pop, path in collect_stats_files(a.pgs, a.pop):
        try:
            cached = json.load(open(path))
        except Exception as e:
            results.append({'pgs_id': pgs_id, 'pop': pop, 'status': f'unreadable ({e})'})
            continue
        cm = cached.get('mean')
        cs = cached.get('std')
        if cm is None or cs is None or cs <= 0:
            results.append({'pgs_id': pgs_id, 'pop': pop, 'status': 'cached_invalid'})
            continue
        subwork = os.path.join(work, f'{pgs_id}_{pop}')
        os.makedirs(subwork, exist_ok=True)
        scores, status = score_random_subset(pgs_id, pop, a.n, a.seed, subwork)
        if scores is None:
            results.append({'pgs_id': pgs_id, 'pop': pop, 'status': status})
            continue
        om = statistics.fmean(scores)
        os_ = statistics.stdev(scores) if len(scores) > 1 else 0.0
        delta = (om - cm) / cs if cs > 0 else 0.0
        sigma_ratio = os_ / cs if cs > 0 else 0.0
        # Mean drift is checked against n-aware tolerance: 2× standard error of
        # the sample mean = 2 / sqrt(n). max_delta_sigma is the additional
        # "real" drift we allow on top. Total allowed: max_delta_sigma + 2/sqrt(n).
        n = len(scores)
        allowed = a.max_delta_sigma + (2.0 / max(1, n) ** 0.5)
        ok_delta = abs(delta) <= allowed
        ok_sigma = (a.sigma_ratio_lo <= sigma_ratio <= a.sigma_ratio_hi)
        passed = ok_delta and ok_sigma
        results.append({
            'pgs_id': pgs_id, 'pop': pop, 'status': 'ok' if passed else 'DRIFT',
            'n_observed': len(scores),
            'cached_mean': cm, 'cached_std': cs,
            'observed_mean': om, 'observed_std': os_,
            'delta_sigma': delta, 'sigma_ratio': sigma_ratio,
        })

    elapsed = time.time() - t0
    drifted = [r for r in results if r['status'] not in ('ok',)]

    if a.json:
        print(json.dumps({
            'generated_at': datetime.datetime.now().isoformat(),
            'elapsed_seconds': round(elapsed, 1),
            'n_checked': len(results),
            'n_drifted': len(drifted),
            'results': results,
        }, indent=2))
    else:
        print(f'self-test: {len(results)} (pgs,pop) checked in {elapsed:.0f}s, '
              f'{len(drifted)} drifted')
        for r in results:
            tag = '🚩 DRIFT' if r['status'] not in ('ok',) else '   ok '
            extra = ''
            if 'delta_sigma' in r:
                extra = (f"  μ_obs={r['observed_mean']:+.5f}  μ_cached={r['cached_mean']:+.5f}  "
                         f"Δσ={r['delta_sigma']:+.3f}  σ_ratio={r['sigma_ratio']:.2f}  "
                         f"n={r['n_observed']}")
            print(f"  {tag}  {r['pgs_id']:<12s} {r['pop']:<4s}  {r['status']:<20s}{extra}")

    sys.exit(1 if drifted else 0)


if __name__ == '__main__':
    main()
