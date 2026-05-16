# 06 — Reference Panel and Ref-Stats Stores

This chapter covers the 1000 Genomes Phase 3 panel that anchors PCA
and ref-stats, plus the two on-disk stores of per-(PGS × population)
μ/σ JSONs.

## 6.1 Panel files

```
/data/pgs2/ref_panel/
├── GRCh38_1000G_ALL.pgen                3,202 unrelated 1000G samples, GRCh38, all chromosomes
├── GRCh38_1000G_ALL.pvar.zst            compressed; zstdcat on demand
├── GRCh38_1000G_ALL.psam                sample metadata: super_pop, pop, sex
├── GRCh38_1000G.king.cutoff.out.id      kept (unrelated) IIDs (KING 2nd-degree cutoff)
├── pop_samples/
│   ├── EUR.txt                          633 IIDs
│   ├── EAS.txt                          585 IIDs
│   ├── AFR.txt                          893 IIDs
│   ├── SAS.txt                          601 IIDs
│   └── AMR.txt                          490 IIDs
└── ancestry_ref.{bed,bim,fam}           legacy plink1 reference for /ancestry/ app
```

A duplicate GRCh37 version (`GRCh37_1000G_ALL.*`) is kept for
auto-liftover-into-GRCh37 edge cases but is not used by the live PGS
pipeline.

`.pvar.zst` is read on demand via `zstdcat` (no full decompression
ever). This is the bottleneck for ref-stats recomputation; everything
else is plink2-fast.

## 6.2 Population definitions (`pipeline/config.py::POPULATIONS`)

| Code  | Label              | Sample file              | Min n |
| ----- | ------------------ | ------------------------ | ----- |
| EUR   | European           | `pop_samples/EUR.txt`    | 633   |
| EAS   | East Asian         | `pop_samples/EAS.txt`    | 585   |
| AFR   | African            | `pop_samples/AFR.txt`    | 893   |
| SAS   | South Asian        | `pop_samples/SAS.txt`    | 601   |
| AMR   | Admixed American   | `pop_samples/AMR.txt`    | 490   |
| MIX   | Mixed (EUR+EAS)    | dynamic, seed=42         | 1170  |
| MID   | Middle Eastern     | placeholder              | 0     |

`MIX` is built on the fly inside `recompute_ref_stats.py`: equal
counts from `EUR.txt` and `EAS.txt`
(`min(len(EUR), len(EAS)) = 585` each), random seed 42 for
reproducibility. The intent is to give "mostly admixed but not
assignable" users a less biased reference than blanket EUR. We do
**not** currently build a true admixed ref panel from individual-level
admixture proportions — this is on the list (see
[reviewer-questions.md](13-reviewer-questions.md)).

`MID` has no panel because 1000G Phase 3 super-pops don't include
Middle Eastern. Samples that admixture-classify as MID fall back to
`UNSUPPORTED` with a per-pop sensitivity array.

`BUILDABLE_POPULATIONS = ["EUR", "EAS", "AFR", "SAS", "AMR", "MIX"]`
controls which ref-stats files are created by
`recompute_ref_stats.py --pop ALL`.

`UI_POPULATIONS = ["EUR", "EAS", "MIX"]` are the first-class options
shown in the report UI's "compare to" dropdown.

## 6.3 Two on-disk stores of ref-stats

This is the source of the EAS-percentile incident (see chapter 09).
There are **two** physical stores of per-(PGS × population) μ/σ
JSONs, with **different schemas**:

### 6.3.1 Legacy store — `/data/pgs2/ref_panel_stats/`

Originally EUR-only, recently expanded to all 5 super-pops + MIX for
a subset of PGSes. Files follow naming:

```
PGS000004_EUR_GRCh38.json                          # old, EUR-only
PGS000004_EUR_GRCh38_n303_plink2-nomi_sha-XXXX.json  # post-remediation, all pops
```

A `registry.json` in the same directory points at the canonical
("blessed") file per (pgs_id, population, genome_build,
scoring_method):

```json
{
  "entries": [
    {
      "pgs_id": "PGS000004",
      "population": "EAS",
      "genome_build": "GRCh38",
      "scoring_method": "plink2-nomi",
      "filename": "PGS000004_EAS_GRCh38_n303_plink2-nomi_sha-8856f202.json",
      "n_variants": 303,
      "variant_ids_sha256": "8856f2028e46caeb8041f34d2565513e7bc90936c03e6241bdb252c6001d15f6",
      "blessed_at": "2026-05-11T20:30:08.094172+00:00"
    },
    ...
  ]
}
```

Coverage as of 2026-05-14: **57 unique PGSes**, ~5 pops each.

`pipeline/registry.py::resolve(pgs_id, population, genome_build,
scoring_method)` is the read-side resolver. The loader
(`pipeline/scoring.py::_load_stats`) checks the registry first. Files
in this store carry the full schema fields:

```json
{
  "schema_version":         1,
  "pgs_id":                "PGS000004",
  "population":            "EAS",
  "genome_build":          "GRCh38",
  "scoring_method":        "plink2-nomi",
  "imputation_policy":     "no-mean-imputation",
  "n_variants":            303,
  "variant_ids_sha256":    "8856f2028e46caeb...",
  "n_samples":             585,
  "mean":                  -0.0001234,
  "std":                    0.0008765,
  "generated_at":          "2026-05-11T20:30:08+00:00"
}
```

These pass the strict `_rs_validate` contract every time and are the
ones that actually serve a percentile to users.

### 6.3.2 New store — `/data/ref_stats/`

Built by an earlier sweep before the schema contract landed. ~362
PGSes covered, with files at the path that `ref_stats_path` produces:

```
/data/ref_stats/PGS000007/EUR_GRCh38.json
/data/ref_stats/PGS000007/EAS_GRCh38.json
/data/ref_stats/PGS000007/AFR_GRCh38.json
/data/ref_stats/PGS000007/SAS_GRCh38.json
/data/ref_stats/PGS000007/AMR_GRCh38.json
/data/ref_stats/PGS000007/MIX_GRCh38.json
/data/ref_stats/PGS000007/EUR_scores.npy   # raw per-sample scores (Phase 2.1 ECDF source)
/data/ref_stats/PGS000007/EAS_scores.npy
...
```

These files are **missing required schema fields**. Concretely, every
file in this store lacks:

- `schema_version`
- `variant_ids_sha256`
- `scoring_method`
- `imputation_policy`
- `generated_at`
- `n_variants` (uses `total_variants` instead)

When `_load_stats` resolves to one of these files, `_rs_validate`
raises `IncompatibleRefStats("missing required keys: [...]")`. The
loader attaches `_incompatible_reason` to the dict and the percentile
path emits `method="incompatible_ref_stats"` with `percentile=None`.

**Coverage tally** (as of 2026-05-14):

| Store                                  | Total PGS | EUR pass | EAS pass |
| -------------------------------------- | --------- | -------- | -------- |
| `/data/pgs2/ref_panel_stats/` registry | 57        | 57       | 54       |
| `/data/pgs2/ref_panel_stats/` legacy   | 103       | 103      | 0 *      |
| `/data/ref_stats/` new                 | 362       | 0 **     | 0 **     |
| **net unique PGS with usable EAS**     |           |          | **54**   |
| **net unique PGS with usable EUR**     |           | **~111** (registry + legacy EUR fallback) |   |

\* Legacy EUR-only fallback only fires for EUR; no fallback for non-EUR.
\** New store files exist but fail the schema contract.

This asymmetry is the heart of chapter 09: EAS users get a percentile
on ~54 PGSes, EUR users get one on ~111 PGSes. EAS users fail on the
PGSes for which only `/data/ref_stats/` has data, EUR users succeed
because of a legacy fallback path that only applies to EUR.

## 6.4 Building / rebuilding ref-stats

`scripts/recompute_ref_stats.py` is the canonical builder. It re-runs
the live plink2 scoring invocation against the 1000G panel restricted
to the target population, then writes a JSON with the full schema:

```
python scripts/recompute_ref_stats.py \
    --pgs PGS000334 --pop ALL --build GRCh38 --apply
```

Options:

- `--all-mismatched` — find every (pgs, pop) whose stored
  `variant_ids_sha256` doesn't match the live catalog file and rebuild.
- `--coverage-max 0.55` — only rebuild PGSes whose
  `stored_n_variants / catalog_n_variants < 0.55`. Used after the
  PGS000334 incident to identify drastically-stale files.
- `--apply` — without this flag, runs in dry-run mode (lists what
  would change but doesn't write).

The output JSON carries every field `_rs_validate` checks for plus
diagnostics:

```json
{
  "schema_version":     1,
  "pgs_id":             "PGS000334",
  "population":         "EUR",
  "genome_build":       "GRCh38",
  "scoring_method":     "plink2-nomi",
  "imputation_policy":  "no-mean-imputation",
  "n_variants":         22,
  "variant_ids_sha256": "0a3c... (sha256 of canonical variant set)",
  "n_samples":          503,
  "mean":               -0.000045,
  "std":                 0.000812,
  "min":                -0.0031,
  "max":                 0.0029,
  "median":              0.00001,
  "quantiles":          {"1": -0.0021, "5": -0.0013, ..., "99": 0.0023},
  "generated_at":       "2026-05-12T14:23:11+00:00",
  "build_command":      "plink2 --pfile ... --score ... --keep EUR.txt ..."
}
```

`ref_stats_registry.py bless --pgs PGS000334 --pop EUR --file ...`
adds the new file to `registry.json` so the loader picks it up.

## 6.5 What the panel does NOT cover

- **Phase 4 / NYGC 30× rerelease**: we use Phase 3. Some Y/mt analyses
  would benefit; not done yet.
- **Non-1000G ancestries**: Middle Eastern, Indigenous Australasian,
  some Pacific populations are absent. Today we flag these
  `UNSUPPORTED` and emit a sensitivity array.
- **Family / pedigree panels (trio-aware PRS)**: the panel is
  unrelated samples by design (`king.cutoff.out` applied). Within-
  family PGS calibration is out of scope.
- **Sex-stratified references**: we compute one ref-stats file per
  (PGS, pop), not per (PGS, pop, sex). For traits with strong sex
  effects (e.g. CAD) percentiles are sex-pooled. The
  `pipeline/sex_stratified_stats.py` module exists for the future
  build but is not wired into the live percentile path yet.

## 6.6 Two-store consolidation (proposed)

The two-store split (§6.3) is a transitional artifact. The plan is to
consolidate everything under `/data/pgs2/ref_panel_stats/` with the
`registry.json` as the only source of truth. The migration that needs
to happen:

1. For every PGS in `/data/ref_stats/`, read the underlying
   `<pop>_scores.npy` (raw per-sample score arrays).
2. Recompute the canonical JSON via `recompute_ref_stats.py` (cheap
   when we have the scores; it just computes μ/σ + adds schema
   metadata).
3. `bless` each new file into `registry.json`.
4. Delete `/data/ref_stats/` once parity is confirmed.

This is exactly the work proposed in chapter 09 as Solution C
(re-bless). The reviewer's input on whether this is the right
direction — or whether to switch the live loader to ECDF-from-NPY
directly (Solution B) — is the key question.
