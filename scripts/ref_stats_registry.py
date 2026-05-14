#!/usr/bin/env python3
"""Ref-stats registry — replaces filename-glob picking with explicit resolution.

Each (pgs_id, population, genome_build, scoring_method) tuple maps to exactly
one current stats file. Stats files become write-once: new computations
produce new files; the registry pointer is what changes. Old files stay on
disk as historical record but can never be re-selected.

Registry location: /data/pgs2/ref_panel_stats/registry.json
Format:
    {
      "schema_version": 1,
      "updated_at": "2026-05-11T05:00:00Z",
      "entries": [
        {
          "pgs_id": "PGS000334",
          "population": "EUR",
          "genome_build": "GRCh38",
          "scoring_method": "plink2-nomi",
          "filename": "PGS000334_EUR_GRCh38_n22_plink2-nomi_sha-a1b2c3d4.json",
          "n_variants": 22,
          "variant_ids_sha256": "a1b2c3d4...",
          "blessed_at": "2026-05-11T05:00:00Z"
        },
        ...
      ]
    }

CLI:
    ref_stats_registry.py rebuild       # scan dir, write registry covering current files
    ref_stats_registry.py list          # pretty-print
    ref_stats_registry.py resolve PGS_id POP        # show the canonical filename
    ref_stats_registry.py bless <file>  # add/replace entry pointing at <file>
"""
from __future__ import annotations
import argparse, datetime, json, os, re, sys

STATS_DIR = '/data/pgs2/ref_panel_stats'
REGISTRY  = os.path.join(STATS_DIR, 'registry.json')
SCHEMA_VERSION = 1


def _read_stats(path):
    try:
        return json.load(open(path))
    except Exception:
        return None


def _entry_from_stats_file(path):
    """Build a registry entry from a fully-conformant stats file."""
    d = _read_stats(path)
    if not d:
        return None
    required = ('pgs_id', 'population', 'genome_build', 'scoring_method',
                'n_variants', 'variant_ids_sha256')
    if any(k not in d for k in required):
        return None
    return {
        'pgs_id':            d['pgs_id'],
        'population':        d['population'],
        'genome_build':      d['genome_build'],
        'scoring_method':    d['scoring_method'],
        'filename':          os.path.basename(path),
        'n_variants':        d['n_variants'],
        'variant_ids_sha256': d['variant_ids_sha256'],
        'blessed_at':        d.get('generated_at') or
                              datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def rebuild():
    """Scan STATS_DIR, build registry covering every non-stale conformant file.
    If multiple files exist for the same (pgs,pop,build,method), pick the newest."""
    entries = {}
    for fn in sorted(os.listdir(STATS_DIR)):
        if '.stale-bias' in fn or not fn.endswith('.json'):
            continue
        if fn == 'registry.json':
            continue
        e = _entry_from_stats_file(os.path.join(STATS_DIR, fn))
        if not e:
            continue
        key = (e['pgs_id'], e['population'], e['genome_build'], e['scoring_method'])
        existing = entries.get(key)
        if existing is None or e['blessed_at'] > existing['blessed_at']:
            entries[key] = e
    reg = {
        'schema_version': SCHEMA_VERSION,
        'updated_at':     datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'entries':        sorted(entries.values(),
                                 key=lambda e: (e['pgs_id'], e['population'])),
    }
    with open(REGISTRY, 'w') as f:
        json.dump(reg, f, indent=2)
    print(f'wrote {REGISTRY}  ({len(entries)} entries)')
    return reg


def load():
    if not os.path.exists(REGISTRY):
        return {'schema_version': SCHEMA_VERSION, 'updated_at': None, 'entries': []}
    return json.load(open(REGISTRY))


def resolve(pgs_id, population, genome_build='GRCh38', scoring_method='plink2-nomi'):
    """Return absolute path of the registered stats file, or None."""
    reg = load()
    for e in reg.get('entries', []):
        if (e['pgs_id'] == pgs_id and e['population'] == population
                and e['genome_build'] == genome_build
                and e['scoring_method'] == scoring_method):
            return os.path.join(STATS_DIR, e['filename'])
    return None


def bless(stats_file):
    """Register a (PGS,pop,build,method) → filename mapping, replacing prior."""
    if not os.path.exists(stats_file):
        sys.exit(f'no such file: {stats_file}')
    e = _entry_from_stats_file(stats_file)
    if not e:
        sys.exit('stats file is not conformant (missing required keys)')
    reg = load()
    key = (e['pgs_id'], e['population'], e['genome_build'], e['scoring_method'])
    new_entries = [x for x in reg.get('entries', [])
                   if (x['pgs_id'], x['population'], x['genome_build'],
                       x['scoring_method']) != key]
    new_entries.append(e)
    reg['entries'] = sorted(new_entries, key=lambda x: (x['pgs_id'], x['population']))
    reg['updated_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    reg['schema_version'] = SCHEMA_VERSION
    with open(REGISTRY, 'w') as f:
        json.dump(reg, f, indent=2)
    print(f'blessed {e["filename"]} for ({e["pgs_id"]}, {e["population"]}, '
          f'{e["genome_build"]}, {e["scoring_method"]})')


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    sub.add_parser('rebuild')
    sub.add_parser('list')
    r = sub.add_parser('resolve')
    r.add_argument('pgs_id'); r.add_argument('population')
    r.add_argument('--genome-build', default='GRCh38')
    r.add_argument('--method', default='plink2-nomi')
    b = sub.add_parser('bless'); b.add_argument('stats_file')
    a = ap.parse_args()

    if a.cmd == 'rebuild':
        rebuild()
    elif a.cmd == 'list':
        reg = load()
        print(f'registry updated_at: {reg.get("updated_at")}')
        print(f'entries: {len(reg.get("entries", []))}')
        for e in reg.get('entries', []):
            print(f'  {e["pgs_id"]:<12s} {e["population"]:<4s} {e["genome_build"]:<8s} '
                  f'{e["scoring_method"]:<12s} n_var={e["n_variants"]:<8d} '
                  f'sha={e["variant_ids_sha256"][:8]}  {e["filename"]}')
    elif a.cmd == 'resolve':
        p = resolve(a.pgs_id, a.population, a.genome_build, a.method)
        if p:
            print(p)
        else:
            print(f'no entry for ({a.pgs_id}, {a.population}, {a.genome_build}, {a.method})')
            sys.exit(1)
    elif a.cmd == 'bless':
        bless(a.stats_file)


if __name__ == '__main__':
    main()
