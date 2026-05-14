# 03 — PGS Ingestion and Normalization

The PGS Catalog distributes scoring files in heterogeneous formats:
GRCh37 or GRCh38 (or "NR" unknown), with or without harmonized columns,
with or without an `other_allele`, multiple `effect_weight` precisions,
etc. The pipeline normalizes every catalog file into a single canonical
shape before it is ever scored.

Code: `simple-genomics/pipeline/ingest_pgs.py` + `match_logic.py`.

## 3.1 The canonical cache layout

For each PGS, `/data/pgs_cache/<PGS_ID>/` ends up containing:

```
PGS000334/
├── PGS000334_hmPOS_GRCh38.txt.gz   # downloaded from PGS Catalog
├── scoring_original.txt.gz         # symlink to the above
├── metadata.json                   # parsed header + provenance
├── scoring_clean.tsv.gz            # normalized parse (canonical form)
├── scoring_plink2.tsv              # user-format (chr-prefixed IDs)
├── scoring_refpanel.tsv            # ref-panel format (bare chroms, both orientations)
└── eligibility.json                # QC verdict
```

The ingest is **idempotent**: if all six files exist and `force=False`,
the function returns immediately. `eligibility.json` is the canonical
"can we trust this PGS?" signal.

## 3.2 The ingest pipeline

`pipeline.ingest_pgs.ingest_pgs(pgs_id, force=False)`:

```
 ┌─────────────────────────────────────────────────────────┐
 │ 1. Download                                             │
 │    PGS Catalog FTP → /data/pgs_cache/<id>/scoring_*.gz  │
 │    (skipped if scoring_original.txt.gz already exists)  │
 ├─────────────────────────────────────────────────────────┤
 │ 2. Parse header                                         │
 │    `#key=value` lines into metadata dict                │
 │    Records: trait, genome_build, HmPOS_build,           │
 │             variants_number, citation, weight_type      │
 ├─────────────────────────────────────────────────────────┤
 │ 3. Parse + canonicalize variants                        │
 │    match_logic.parse_pgs_scoring_file →                 │
 │       list[ScoringVariant(chrom, pos, ea, oa, weight)]  │
 │    Prefers harmonized columns (hm_chr/hm_pos) when      │
 │    present; falls back to chr_name/chr_position.        │
 │    `chr` prefix stripped on chrom; weight kept as str   │
 │    to preserve precision.                               │
 ├─────────────────────────────────────────────────────────┤
 │ 4. Liftover (if needed)                                 │
 │    positions_build = metadata['HmPOS_build']            │
 │                      or metadata['genome_build']        │
 │    If GRCh37 → run UCSC liftOver to GRCh38 with         │
 │    /data/ancestry_reference/hg19ToHg38.over.chain.gz    │
 │    Variants that fail to map are dropped; if <50%       │
 │    survive, ingest is marked rejected.                  │
 ├─────────────────────────────────────────────────────────┤
 │ 5. QC                                                   │
 │    _run_qc(variants, metadata) → eligibility.json       │
 │      Check 1: n ≥ 5  (else rejected/too_few_variants)   │
 │      Check 2: chroms ∈ {1..22, X, Y, M}                 │
 │      Check 3: ≤10% invalid alleles (non-ACGT)           │
 │      Check 4: weights parseable as float, no NaN        │
 │    Status: ok | flagged | rejected                      │
 │    Flagged PGS still score; rejected PGS do not.        │
 └─────────────────────────────────────────────────────────┘
```

`scoring_clean.tsv.gz` is the canonical post-liftover view; all later
operations read from it (and never re-parse the catalog file).

## 3.3 Why two scoring-file representations exist

- **`scoring_plink2.tsv`** (used at scoring time against the user's VCF):
  variant IDs in the `chrN:pos` form to match the user's pgen
  (`--set-all-var-ids chr@:#`).
- **`scoring_refpanel.tsv`** (used by `recompute_ref_stats.py`): variant
  IDs that match the 1000G reference panel's pvar IDs (bare chrom).
  When the panel has a real rsID, we use that; when the panel emits `.`,
  the panel-side row is dropped (plink2 can't disambiguate multiple
  rows with ID `.`).

This split is the entire reason ref-stats and live-scoring stay in sync:
both sides apply the *same* parse function (`match_logic.parse_pgs_scoring_file`)
and the *same* canonical variant_ids hash to detect drift (see
`06-percentile-and-stats.md`).

## 3.4 Header → metadata

The parser extracts every `#key=value` line in the header. Important keys:

| Key                  | Meaning                                              |
| -------------------- | ---------------------------------------------------- |
| `trait_efo`          | EFO trait label                                      |
| `trait_reported`     | publication-specific label                           |
| `genome_build`       | original study build (may be NR / unknown)           |
| `HmPOS_build`        | build of harmonized positions (almost always GRCh38) |
| `variants_number`    | publication-declared count                           |
| `weight_type`        | beta / logOR / etc.                                  |
| `pgs_id`             | self-reference                                       |
| `pgp_id`             | publication ID                                       |

We treat `positions_build` (= `HmPOS_build` if present, else
`genome_build`) as authoritative for coordinate-level operations. The
original `genome_build` is kept in `build_notes` so reviewers can see
the chain. A liftover applied is recorded as
`metadata['liftover'] = "GRCh37→GRCh38"`.

## 3.5 Liftover

We use UCSC `liftOver` against the canonical chain files:

```
GRCh37 → GRCh38 : /data/ancestry_reference/hg19ToHg38.over.chain.gz
GRCh38 → GRCh37 : simple-genomics/liftover/hg38ToHg19.over.chain.gz
```

Two contexts trigger liftover:

1. **Ingest-time** (`pipeline/ingest_pgs._liftover_variants`): the PGS
   header declares positions in a build different from our canonical
   GRCh38 panel. Variants that fail to map are dropped; if fewer than
   50% map, the ingest marks the scoring file as rejected.

2. **Runtime** (`runners._liftover_pgs_scoring`): the user's VCF is on
   a different build than the (ingested) scoring file's
   `positions_build`. The scoring file is lifted to the VCF's build for
   that one run; the cached canonical scoring file in
   `/data/pgs_cache/` is **not** modified.

We never lift the user's VCF on the fly. Lifting the scoring file
(thousands of lines) is cheap; lifting a multi-GB VCF is wasteful and
opens a class of partial-mapping bugs.

## 3.6 Variant-set hashing (drift detection seed)

`variant_set_sha` (in `scripts/recompute_ref_stats.py` and mirrored in
`pipeline/scoring.py::_rs_variant_set_sha_from_catalog`) computes:

```python
canon = '\n'.join(sorted(f"{chr}|{pos}|{effect_allele}|{weight}" 
                          for v in variants))
sha = sha256(canon.encode()).hexdigest()
```

This hash is stored in every ref-stats JSON as `variant_ids_sha256`. At
percentile time, the loader recomputes the hash from the current
scoring file and refuses the stats file if it disagrees. The hash is
panel-independent: re-ingesting a PGS without changes will produce the
same hash, and changing any variant / weight / allele invalidates every
cached ref-stats file for that PGS automatically.

## 3.7 Eligibility states

`eligibility.json` is the persisted QC verdict:

```json
{
  "status": "ok",
  "reasons": [],
  "variant_count": 1735924,
  "genome_build": "GRCh38"
}
```

| `status`   | Behavior |
| ---------- | --- |
| `ok`       | Scored normally |
| `flagged`  | Scored but emits a warning in the report (e.g. >10% invalid alleles) |
| `rejected` | Scoring refused before any plink2 call |

## 3.8 Pre-curated PGS inventory

`reports/pgs_inventory.json` + `reports/pgs_inventory.md` are
regenerated by `scripts/pgs_inventory.py` and are the
human-readable catalog of which PGS are ingested, which ref-stats are
built, and which traits they cover. Reviewers can use this for
spot-checks; the live source of truth remains the on-disk state.

## 3.9 Custom PGS (user upload)

The app supports uploading a custom PGS scoring file via
`/api/pgs/custom`. The uploaded file goes through the same
`parse_pgs_scoring_file` parser and is cached at
`/data/pgs_cache/<USER_ID>/` (no PGS Catalog round-trip). It does
**not** get a ref-stats file — percentile is unavailable for custom
PGS until the user runs the runtime z-score against the matched-subset
ref panel (`_score_ref_panel_matched`, used as a fallback in
`scoring.compute_percentile_dynamic`).
