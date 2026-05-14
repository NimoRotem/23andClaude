#!/usr/bin/env python3
"""Recompute a PGS reference distribution against the 1000G GRCh38 panel.

Usage:
    recompute_ref_stats.py <PGS_id> [--pop EUR|EAS|AFR|SAS|AMR|ALL] [--apply]
    recompute_ref_stats.py --all-mismatched [--coverage-max 0.55] [--apply]

Emits the strict ref-stats schema (v1):
    pgs_id, population, genome_build, n_variants, variant_ids_sha256,
    scoring_method, imputation_policy, ref_panel, ref_panel_sha256,
    sample_filter, n_samples, mean, std, median, min, max,
    sum_mean, sum_std, generated_at, generated_by_pipeline_version,
    generated_by_commit, schema_version

Without --apply: prints new μ/σ next to cached, no files written.
With --apply: writes <PGS>_<POP>_GRCh38_n<N>_<method>_sha-<short>.json
  and renames any prior <PGS>_<POP>_GRCh38*.json → *.stale-bias-<date>
"""
import os, sys, json, gzip, statistics, subprocess, argparse, datetime, re, hashlib, time

PLINK2     = '/home/nimo/miniconda3/envs/genomics/bin/plink2'
PANEL      = '/data/pgs2/ref_panel/GRCh38_1000G_ALL'
PVAR_Z     = PANEL + '.pvar.zst'
POP_DIR    = '/data/pgs2/ref_panel/pop_samples'
STATS_DIR  = '/data/pgs2/ref_panel_stats'
PGS_CACHE  = '/data/pgs_cache'
SCORING_METHOD = 'plink2-nomi'        # plink2 --score, no-mean-imputation, cols=+scoresums, header-read 1 2 3
IMPUTATION     = 'no-mean-imputation'
SCHEMA_VERSION = 1


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _sha256_str(s):
    return hashlib.sha256(s.encode()).hexdigest()


def _git_commit(repo='/home/nimrod_rotem/simple-genomics'):
    try:
        out = subprocess.run(['git', '-C', repo, 'rev-parse', 'HEAD'],
                             capture_output=True, text=True, check=True, timeout=5)
        return out.stdout.strip()
    except Exception:
        return None


def load_pgs(pgs_id):
    """Parse a PGS Catalog harmonized scoring file. Returns (variants, source_path)."""
    p = os.path.join(PGS_CACHE, pgs_id, f'{pgs_id}_hmPOS_GRCh38.txt.gz')
    if not os.path.exists(p):
        sys.exit(f'no scoring file at {p}')
    pgs = []
    with gzip.open(p, 'rt') as f:
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
                pgs.append({'rsid': r.get('rsID', ''),
                            'chr':  r['hm_chr'],
                            'pos':  int(r['hm_pos']),
                            'ea':   r['effect_allele'],
                            'w':    float(r['effect_weight'])})
            except (KeyError, ValueError):
                continue
    return pgs, p


def panel_ids_for(pgs):
    """Map (chr,pos) → list of {vid,ref,alt} from panel pvar."""
    need_chrs = set(p['chr'] for p in pgs)
    need = {(p['chr'], p['pos']): True for p in pgs}
    out = {}
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
            out.setdefault((chrom, ipos), []).append({'vid': vid, 'ref': ref, 'alt': alt})
    proc.wait()
    return out


def write_score_file(pgs, panel_ids, dest):
    """Build plink2 score file using panel-format variant IDs. Returns list of (vid, ea, w).

    Skips entries whose panel vid is '.' (missing rsID — plink2 can't disambiguate
    when many variants share '.' as ID).

    Deduplicates by (vid, effect-allele): when two PGS scoring rows collapse
    to the same panel variant after harmonization/liftover (PGS000898 has
    one such pair at 17:46275856:T:G), plink2 rejects the score file with
    'REF allele for variant X appears multiple times'. PGS scoring is
    additive, so we merge by summing the weights when the effect allele
    agrees. Conflicting effect alleles at the same vid keep the larger
    |weight| with a warning — those represent an ambiguous catalog entry.
    """
    rows = []
    skipped_dot = 0
    # Aggregate before writing: (vid, ea) -> summed weight, plus a per-vid
    # ea map to detect conflicting alleles.
    agg = {}                 # (vid, ea) -> summed_weight
    vid_alleles = {}         # vid -> {ea -> (sum_w, n_merged)}
    for p in pgs:
        for m in panel_ids.get((p['chr'], p['pos']), []):
            if p['ea'] in (m['ref'], m['alt']):
                if m['vid'] == '.' or not m['vid']:
                    skipped_dot += 1
                    break
                key = (m['vid'], p['ea'])
                agg[key] = agg.get(key, 0.0) + float(p['w'])
                slot = vid_alleles.setdefault(m['vid'], {})
                prev_sum, prev_n = slot.get(p['ea'], (0.0, 0))
                slot[p['ea']] = (prev_sum + float(p['w']), prev_n + 1)
                break

    # Resolve same-vid different-EA conflicts by keeping the larger |sum|.
    chosen = {}              # vid -> (ea, weight)
    n_merged = 0
    n_conflicts = 0
    for vid, alleles in vid_alleles.items():
        if len(alleles) == 1:
            ea, (s, n) = next(iter(alleles.items()))
            if n > 1:
                n_merged += 1
            chosen[vid] = (ea, s)
        else:
            n_conflicts += 1
            ea_best, s_best = max(alleles.items(), key=lambda kv: abs(kv[1][0]))
            print(f'  WARN: vid {vid} has conflicting effect alleles '
                  f'{sorted(alleles.keys())}; keeping {ea_best} '
                  f'(|sum|={abs(s_best[0]):.4g})')
            chosen[vid] = (ea_best, s_best[0])

    with open(dest, 'w') as f:
        f.write('ID\tA1\tWEIGHT\n')
        for vid, (ea, w) in chosen.items():
            f.write(f'{vid}\t{ea}\t{w}\n')
            rows.append((vid, ea, w))

    if skipped_dot:
        print(f'  (skipped {skipped_dot} variants with panel vid=".")')
    if n_merged:
        print(f'  (merged {n_merged} duplicate vid+EA pairs — weights summed)')
    if n_conflicts:
        print(f'  (resolved {n_conflicts} vid w/ conflicting EAs — kept larger |sum|)')
    return rows


def variant_set_sha(pgs_variants):
    """Hash a stable representation of the catalog variant set.

    Canonical form: sorted '\\n'-joined 'chr|pos|effect_allele|weight' lines.
    This is panel-independent so the same scoring file always hashes the same
    way, regardless of which panel-format IDs end up resolving the variants.
    """
    canon = '\n'.join(sorted(f"{p['chr']}|{p['pos']}|{p['ea']}|{p['w']}"
                              for p in pgs_variants))
    return _sha256_str(canon)


def run_score(work, score_file, pop):
    """Run plink2 --score on a population subset. Returns list of {avg,sum,ct} rows."""
    keep = os.path.join(work, f'{pop}.keep')
    with open(keep, 'w') as f:
        f.write('#IID\n')
        for line in open(os.path.join(POP_DIR, f'{pop}.txt')):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            f.write(line + '\n')

    out_prefix = os.path.join(work, f'{pop}_score')
    subprocess.run(
        [PLINK2, '--pfile', PANEL, 'vzs', '--keep', keep,
         '--score', score_file, 'header-read', '1', '2', '3',
         'cols=+scoresums', 'no-mean-imputation',
         '--out', out_prefix],
        check=True, capture_output=True,
    )

    rows = []
    with open(out_prefix + '.sscore') as f:
        h = f.readline().rstrip('\n').split('\t')
        a_i = h.index('WEIGHT_AVG')
        s_i = h.index('WEIGHT_SUM')
        c_i = h.index('ALLELE_CT')
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if not parts[0]:
                continue
            rows.append({'avg': float(parts[a_i]),
                         'sum': float(parts[s_i]),
                         'ct':  int(parts[c_i])})
    return rows


def find_cached(pgs_id, pop):
    """Locate any current ref-stats file for (PGS,pop), excluding stale-quarantined ones."""
    for f in os.listdir(STATS_DIR):
        m = re.match(rf'{pgs_id}_{pop}_GRCh38(_.+)?\.json$', f)
        if m:
            return os.path.join(STATS_DIR, f)
    return None


def find_all_pgs_in_stats_dir():
    """Return set of (PGS_id, pop) pairs that have a current (non-stale) ref-stats file."""
    out = set()
    for f in os.listdir(STATS_DIR):
        m = re.match(r'(PGS\d+)_(\w+)_GRCh38(_.+)?\.json$', f)
        if m and m.group(2) in ('EUR', 'EAS', 'AFR', 'SAS', 'AMR'):
            out.add((m.group(1), m.group(2)))
    return out


def panel_sha_short():
    """Cheap SHA of pgen+pvar.zst+psam — used to fingerprint the panel version."""
    h = hashlib.sha256()
    for ext in ('.pgen', '.pvar.zst', '.psam'):
        p = PANEL + ext
        if os.path.exists(p):
            st = os.stat(p)
            h.update(f"{p}|{st.st_size}|{int(st.st_mtime)}".encode())
    return h.hexdigest()


def build_stats_payload(pgs_id, pop, score_rows, n_variants, var_set_sha,
                       scoring_file_path, scoring_file_sha):
    avgs = [r['avg'] for r in score_rows]
    sums = [r['sum'] for r in score_rows]
    cts  = [r['ct']  for r in score_rows]
    n_used = max(set(cts), key=cts.count) // 2
    return {
        'schema_version':              SCHEMA_VERSION,
        'pgs_id':                      pgs_id,
        'population':                  pop,
        'genome_build':                'GRCh38',
        'n_variants':                  n_used,
        'variant_ids_sha256':          var_set_sha,
        'scoring_method':              SCORING_METHOD,
        'imputation_policy':           IMPUTATION,
        'scoring_file_path':           scoring_file_path,
        'scoring_file_sha256':         scoring_file_sha,        # raw .gz file hash
        'scoring_file_content_sha256': var_set_sha,             # panel-independent canonical content hash

        'ref_panel':                   '1000 Genomes Phase 3 (GRCh38)',
        'ref_panel_path':              PANEL,
        'ref_panel_sha256':            panel_sha_short(),
        'sample_filter':               os.path.join(POP_DIR, f'{pop}.txt'),
        'n_samples':                   len(score_rows),
        'mean':                        statistics.fmean(avgs),
        'std':                         statistics.stdev(avgs),
        'median':                      statistics.median(avgs),
        'min':                         min(avgs),
        'max':                         max(avgs),
        'sum_mean':                    statistics.fmean(sums),
        'sum_std':                     statistics.stdev(sums),
        'generated_at':                datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'generated_by_pipeline_version': 'simple-genomics',
        'generated_by_commit':         _git_commit(),
    }


def stats_filename(pgs_id, pop, n_variants, var_set_sha):
    short = var_set_sha[:8]
    return f'{pgs_id}_{pop}_GRCh38_n{n_variants}_{SCORING_METHOD}_sha-{short}.json'


def quarantine_existing(pgs_id, pop, today):
    """Rename any current (non-stale) ref-stats files for (PGS,pop) so picker won't find them."""
    moved = []
    for f in os.listdir(STATS_DIR):
        if f.endswith('.stale-bias') or '.stale-bias-' in f:
            continue
        m = re.match(rf'{pgs_id}_{pop}_GRCh38(_.+)?\.json$', f)
        if not m:
            continue
        src = os.path.join(STATS_DIR, f)
        dst = os.path.join(STATS_DIR, f + f'.stale-bias-{today}')
        os.rename(src, dst)
        moved.append((f, os.path.basename(dst)))
    return moved


def _scoring_file_catalog_count(pgs_id):
    """Count variants from header (#variants_number=) or row count."""
    p = os.path.join(PGS_CACHE, pgs_id, f'{pgs_id}_hmPOS_GRCh38.txt.gz')
    if not os.path.exists(p):
        return None
    n = 0
    with gzip.open(p, 'rt') as f:
        hdr = None
        for line in f:
            if line.startswith('#'):
                m = re.match(r'#variants_number=(\d+)', line)
                if m:
                    return int(m.group(1))
                continue
            if hdr is None:
                hdr = line
                continue
            n += 1
    return n


def recompute_one(pgs_id, pops, apply_changes, log_fh=sys.stdout):
    """Return list of dicts with results per pop."""
    pgs, scoring_path = load_pgs(pgs_id)
    print(f'{pgs_id}: catalog has {len(pgs)} variants', file=log_fh)
    panel_ids = panel_ids_for(pgs)
    work = f'/tmp/recompute_{pgs_id}'
    os.makedirs(work, exist_ok=True)
    sf = os.path.join(work, f'{pgs_id}_panelfmt.tsv')
    rows = write_score_file(pgs, panel_ids, sf)
    n_in_score = len(rows)
    var_sha = variant_set_sha(pgs)              # catalog content sha (panel-independent)
    scoring_sha = _sha256_file(scoring_path)    # raw-file sha
    print(f'  score file: {n_in_score} variants matched in panel, var_set_sha={var_sha[:12]}', file=log_fh)

    today = datetime.datetime.now().strftime('%Y%m%d')
    results = []
    for pop in pops:
        score_rows = run_score(work, sf, pop)
        payload = build_stats_payload(pgs_id, pop, score_rows, n_in_score, var_sha,
                                      scoring_path, scoring_sha)

        # Compare to existing cached file
        cached_path = find_cached(pgs_id, pop)
        cached = None
        if cached_path:
            try:
                cached = json.load(open(cached_path))
            except Exception:
                cached = None
        cm = cached.get('mean') if cached else None
        cn = cached.get('matched_variants') or cached.get('n_variants') if cached else None
        delta = (payload['mean'] - cm) if (cm is not None and isinstance(cm, (int, float))) else None
        line = (f'  {pop}: n={payload["n_samples"]:4d}  μ={payload["mean"]:+.5f}  σ={payload["std"]:.5f}  '
                f'matched={payload["n_variants"]}')
        if cached:
            line += f'   cached(n_var={cn} μ={cm:+.5f})'
            if delta is not None and cached.get('std', 0) > 0:
                line += f'   Δμ={delta:+.5f} ({delta/cached["std"]:+.2f}σ_cached)'
        print(line, file=log_fh)

        if apply_changes:
            quarantined = quarantine_existing(pgs_id, pop, today)
            for src, dst in quarantined:
                print(f'    quarantined: {src} → {dst}', file=log_fh)
            new_name = stats_filename(pgs_id, pop, payload['n_variants'], var_sha)
            new_path = os.path.join(STATS_DIR, new_name)
            with open(new_path, 'w') as fh:
                json.dump(payload, fh, indent=2)
            print(f'    wrote {new_name}', file=log_fh)

        results.append({
            'pgs_id': pgs_id, 'pop': pop,
            'new_mean': payload['mean'], 'new_std': payload['std'],
            'new_n_variants': payload['n_variants'],
            'cached_mean': cm, 'cached_n_variants': cn,
            'delta_mu': delta,
        })
    return results


def collect_mismatched():
    """Return list of PGS ids whose cached n_variants < catalog (under coverage_max)."""
    mismatches = []
    for f in sorted(os.listdir(STATS_DIR)):
        m = re.match(r'(PGS\d+)_EUR_GRCh38(_.+)?\.json$', f)
        if not m or '.stale-bias' in f:
            continue
        pgs_id = m.group(1)
        path = os.path.join(STATS_DIR, f)
        try:
            d = json.load(open(path))
        except Exception:
            continue
        nv_cached = d.get('matched_variants') or d.get('n_variants')
        nv_cat = _scoring_file_catalog_count(pgs_id)
        if nv_cached is None or nv_cat is None or nv_cat == 0:
            continue
        ratio = nv_cached / nv_cat
        mismatches.append({'pgs_id': pgs_id, 'cached': nv_cached, 'catalog': nv_cat,
                           'ratio': ratio, 'file': f})
    return mismatches


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('pgs_id', nargs='?')
    ap.add_argument('--pop', default='EUR')
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--all-mismatched', action='store_true')
    ap.add_argument('--coverage-max', type=float, default=1.0,
                    help='Only recompute PGS with cached/catalog ratio at or below this (default 1.0 = all)')
    ap.add_argument('--inventory-out',
                    help='Write inventory CSV to this path')
    a = ap.parse_args()

    pops = ['EUR', 'EAS', 'AFR', 'SAS', 'AMR'] if a.pop == 'ALL' else [a.pop]

    if a.all_mismatched:
        targets = [m for m in collect_mismatched() if m['ratio'] < a.coverage_max]
        targets.sort(key=lambda x: x['ratio'])
        print(f'== {len(targets)} PGS scores below coverage {a.coverage_max} ==')
        for m in targets:
            print(f'  {m["pgs_id"]:<12s}  ratio={m["ratio"]:.3f}  '
                  f'(cached={m["cached"]}, catalog={m["catalog"]})')
        print()
        inventory = []
        t0 = time.time()
        for i, m in enumerate(targets, 1):
            print(f'\n=== [{i}/{len(targets)}] {m["pgs_id"]} (coverage {m["ratio"]:.3f}) ===')
            try:
                rs = recompute_one(m['pgs_id'], pops, a.apply)
                inventory.extend(rs)
            except Exception as e:
                print(f'  ERROR: {e}')
                inventory.append({'pgs_id': m['pgs_id'], 'pop': 'ALL', 'error': str(e)})
        elapsed = time.time() - t0
        print(f'\nDone in {elapsed:.0f}s')
        if a.inventory_out:
            import csv
            with open(a.inventory_out, 'w', newline='') as fh:
                w = csv.DictWriter(fh, fieldnames=[
                    'pgs_id', 'pop', 'cached_mean', 'cached_n_variants',
                    'new_mean', 'new_std', 'new_n_variants', 'delta_mu', 'error'])
                w.writeheader()
                for r in inventory:
                    w.writerow({k: r.get(k) for k in w.fieldnames})
            print(f'wrote inventory CSV → {a.inventory_out}')
        return

    if not a.pgs_id:
        ap.error('PGS_id required unless --all-mismatched')
    recompute_one(a.pgs_id, pops, a.apply)


if __name__ == '__main__':
    main()
