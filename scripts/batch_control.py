#!/usr/bin/env python3
"""In-batch control sample regression test.

Picks a fixed 1000G EUR sample (default: HG00096, a published GBR sample) and
scores it through the production pipeline against each PGS. The expected
percentile per PGS is recorded once (golden file) and compared on every run.
Drift > MAX_DRIFT_PP (default ±10pp) on the control quarantines the batch.

Two modes:
    --bless              compute current percentiles and write the golden file
    --check (default)    score the control and compare against golden

Usage:
    batch_control.py --sample HG00096 --bless
    batch_control.py --sample HG00096 --check       # exits 1 on drift
    batch_control.py --check --json --golden /var/lib/sg/golden_HG00096.json
"""
import argparse, gzip, json, math, os, re, subprocess, sys, time
from pathlib import Path

PLINK2     = '/home/nimo/miniconda3/envs/genomics/bin/plink2'
PANEL      = '/data/pgs2/ref_panel/GRCh38_1000G_ALL'
PVAR_Z     = PANEL + '.pvar.zst'
POP_DIR    = '/data/pgs2/ref_panel/pop_samples'
STATS_DIR  = '/data/pgs2/ref_panel_stats'
PGS_CACHE  = '/data/pgs_cache'
GOLDEN_DEFAULT = '/data/pgs2/ref_panel_stats/batch_control_golden.json'
MAX_DRIFT_PP_DEFAULT = 10.0


def list_pgs_with_eur_stats():
    """Yield PGS_ids that have any non-stale EUR ref-stats file."""
    seen = set()
    for fn in sorted(os.listdir(STATS_DIR)):
        if '.stale-bias' in fn or not fn.endswith('.json'):
            continue
        m = re.match(r'(PGS\d+)_EUR_GRCh38(_.+)?\.json$', fn)
        if m:
            seen.add(m.group(1))
    return sorted(seen)


def load_cached_stats(pgs_id, pop='EUR'):
    """Return the current (non-stale) ref-stats file for (pgs_id, pop), or None."""
    for fn in sorted(os.listdir(STATS_DIR)):
        if '.stale-bias' in fn or not fn.endswith('.json'):
            continue
        m = re.match(rf'{pgs_id}_{pop}_GRCh38(_.+)?\.json$', fn)
        if m:
            try:
                return json.load(open(os.path.join(STATS_DIR, fn)))
            except Exception:
                return None
    return None


def score_sample_through_pipeline(sample_iid, pgs_id, work):
    """Run plink2 --score with the live pipeline flags for a single sample.
    Returns (raw_score, score_sum, n_matched) or (None, None, reason)."""
    os.makedirs(work, exist_ok=True)
    scoring_path = os.path.join(PGS_CACHE, pgs_id, f'{pgs_id}_hmPOS_GRCh38.txt.gz')
    if not os.path.exists(scoring_path):
        return None, None, 'no_scoring_file'

    # Parse PGS variants
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

    # Resolve panel IDs
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

    # Build score file
    sf = os.path.join(work, f'{pgs_id}.tsv')
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
        return None, None, 'no_scorable_variants'

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
        return None, None, f'plink2_failed (exit {cp.returncode})'

    sscore = out_prefix + '.sscore'
    if not os.path.exists(sscore):
        return None, None, 'no_sscore_output'
    with open(sscore) as f:
        h = f.readline().rstrip('\n').split('\t')
        a_i = h.index('WEIGHT_AVG'); s_i = h.index('WEIGHT_SUM'); c_i = h.index('ALLELE_CT')
        line = f.readline()
        parts = line.rstrip('\n').split('\t')
        if not parts or not parts[0]:
            return None, None, 'no_data_row'
        return float(parts[a_i]), float(parts[s_i]), int(parts[c_i]) // 2


def percentile_from_stats(raw_score, mu, sigma):
    if sigma <= 0:
        return None
    z = (raw_score - mu) / sigma
    return round(0.5 * (1 + math.erf(z / math.sqrt(2))) * 100, 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sample', default='HG00096',
                    help='Panel sample IID (default HG00096, GBR EUR)')
    ap.add_argument('--bless', action='store_true',
                    help='Compute golden percentiles and write to --golden file')
    ap.add_argument('--check', action='store_true',
                    help='Compare current percentiles against golden (default)')
    ap.add_argument('--golden', default=GOLDEN_DEFAULT,
                    help=f'Path to golden percentile file (default {GOLDEN_DEFAULT})')
    ap.add_argument('--max-drift-pp', type=float, default=MAX_DRIFT_PP_DEFAULT,
                    help='Max drift in percentile points to allow (default ±10pp)')
    ap.add_argument('--pgs', help='Limit to this PGS_id')
    ap.add_argument('--json', action='store_true')
    a = ap.parse_args()

    mode = 'bless' if a.bless else 'check'
    pgs_list = [a.pgs] if a.pgs else list_pgs_with_eur_stats()
    work = f'/tmp/batch_control_{a.sample}'
    os.makedirs(work, exist_ok=True)

    results = []
    t0 = time.time()
    for pgs_id in pgs_list:
        cached = load_cached_stats(pgs_id, 'EUR')
        if not cached:
            results.append({'pgs_id': pgs_id, 'status': 'no_stats'})
            continue
        mu = cached.get('mean'); sigma = cached.get('std')
        if mu is None or sigma is None:
            results.append({'pgs_id': pgs_id, 'status': 'stats_invalid'})
            continue
        raw, ss, n_matched = score_sample_through_pipeline(
            a.sample, pgs_id, os.path.join(work, pgs_id))
        if raw is None:
            results.append({'pgs_id': pgs_id, 'status': n_matched})  # error string in third return
            continue
        pct = percentile_from_stats(raw, mu, sigma)
        results.append({
            'pgs_id': pgs_id, 'status': 'ok',
            'raw_score': raw, 'score_sum': ss, 'n_matched': n_matched,
            'percentile': pct, 'cached_mean': mu, 'cached_std': sigma,
        })

    elapsed = time.time() - t0

    if mode == 'bless':
        golden = {
            'sample_iid': a.sample,
            'generated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'n_pgs': len(results),
            'pipeline_max_drift_pp': a.max_drift_pp,
            'expected': {r['pgs_id']: r['percentile'] for r in results if r['status'] == 'ok'},
            'audit': {r['pgs_id']: r for r in results},
        }
        os.makedirs(os.path.dirname(a.golden), exist_ok=True)
        with open(a.golden, 'w') as f:
            json.dump(golden, f, indent=2)
        print(f'blessed: wrote {a.golden} ({sum(1 for r in results if r["status"]=="ok")} '
              f'PGS scored for {a.sample}, elapsed {elapsed:.0f}s)')
        return

    # check mode (default)
    if not os.path.exists(a.golden):
        print(f'ERROR: golden file {a.golden} not found. Run --bless first.', file=sys.stderr)
        sys.exit(2)
    golden = json.load(open(a.golden))
    expected = golden.get('expected') or {}
    sample = golden.get('sample_iid')
    if sample != a.sample:
        print(f'WARNING: golden was blessed for {sample}, running with {a.sample}',
              file=sys.stderr)

    drifted = []
    for r in results:
        if r['status'] != 'ok':
            continue
        exp = expected.get(r['pgs_id'])
        if exp is None:
            r['drift_pp'] = None
            r['drift_status'] = 'no_golden'
            continue
        r['expected_percentile'] = exp
        drift = r['percentile'] - exp
        r['drift_pp'] = drift
        r['drift_status'] = 'DRIFT' if abs(drift) > a.max_drift_pp else 'ok'
        if abs(drift) > a.max_drift_pp:
            drifted.append(r)

    if a.json:
        print(json.dumps({
            'sample': a.sample,
            'elapsed_seconds': round(elapsed, 1),
            'n_checked': len(results),
            'n_drifted': len(drifted),
            'max_drift_pp': a.max_drift_pp,
            'results': results,
        }, indent=2))
    else:
        print(f'control[{a.sample}]: {len(results)} PGS checked in {elapsed:.0f}s, '
              f'{len(drifted)} drifted > ±{a.max_drift_pp}pp')
        for r in results:
            if r['status'] != 'ok':
                print(f'   skip  {r["pgs_id"]:<12s}  {r["status"]}')
                continue
            tag = '🚩 DRIFT' if r.get('drift_status') == 'DRIFT' else '   ok '
            exp = r.get('expected_percentile')
            drift = r.get('drift_pp')
            extra = f"  obs={r['percentile']:.1f}%ile"
            if exp is not None:
                extra += f"  golden={exp:.1f}%ile  Δ={drift:+.1f}pp"
            elif r.get('drift_status') == 'no_golden':
                extra += "  [new PGS, no golden]"
            print(f"  {tag}  {r['pgs_id']:<12s}{extra}")
    sys.exit(1 if drifted else 0)


if __name__ == '__main__':
    main()
