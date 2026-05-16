# 03 — PGS Catalog Ingestion, Normalization, and Liftover

## 3.1 Source of truth

We score against PGS Catalog harmonized files, fetched from the PGS
Catalog FTP:

```
https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/{PGS_ID}/ScoringFiles/
    {PGS_ID}.txt.gz                       # original build (GRCh37 or 38)
    Harmonized/
        {PGS_ID}_hmPOS_GRCh37.txt.gz      # harmonized to GRCh37 (rsID + chrom:pos)
        {PGS_ID}_hmPOS_GRCh38.txt.gz      # harmonized to GRCh38 (preferred)
```

We always prefer `_hmPOS_GRCh38.txt.gz`. If unavailable, we fall back to
`_hmPOS_GRCh37.txt.gz` and lift over inside the pipeline (§3.5).

## 3.2 Cache layout

```
/data/pgs_cache/<PGS_ID>/
├── <PGS_ID>_hmPOS_GRCh38.txt.gz       canonical, the file plink2 sees
├── <PGS_ID>_hmPOS_GRCh37.txt.gz       kept for liftover fallback (optional)
├── <PGS_ID>_plink2.tsv                pre-parsed plink2-format scoring file (variant_id, effect_allele, weight)
├── meta.json                          parser metadata (variant_count, weight_type, header dict)
└── _lock                              flock during downloads to prevent races
```

The plink2-format file uses one row per variant:

```
chr1:12345    A     0.013
chr1:67890    G    -0.0024
```

with the variant ID matching the user's pgen variant naming
(`chr@:#`). plink2 looks up each row against the user's pgen and
contributes `dose * weight` to `SCORE1_SUM`.

## 3.3 Header parsing (`pipeline/ingest_pgs.py`)

PGS Catalog headers are KEY=VALUE on lines starting with `#`:

```
#format_version=2.0
#pgs_id=PGS000004
#trait_reported=Coronary Heart Disease
#trait_efo=EFO_0000378
#genome_build=GRCh37
#HmPOS_build=GRCh38                  ← we use this if present
#variants_number=46
#weight_type=beta
#trait_direction=higher              ← needed by eligibility gate
#development_ancestry={'EUR'}
#evaluation_ancestry={'EUR', 'EAS'}
```

`parse_pgs_scoring_file()` returns a `(metadata_dict, variants)` tuple.
The metadata dict carries `weight_type`, `trait_direction`,
`development_ancestry`, `evaluation_ancestry`, `auc`, `r2`,
`HmPOS_build` (the build the harmonized positions are in — used by
build validation; differs from `genome_build` for many files), and the
parsed variant count.

The parser used to call `line.strip()` defensively. **It must not.**
Stripping trailing tabs corrupts rows where the last column is empty,
which is the canonical layout when `rsID` is missing. The PGS000327
incident (2026-05-14) demonstrated the impact: a parser that stripped
trailing tabs reduced 35,087 catalog rows to 843 — a 40× under-count.
The fix is encoded in the `match_logic.py.pre-strip-fix-20260514` diff
and pinned in memory.

## 3.4 Variant ingestion

```python
@dataclass
class PgsVariant:
    chrom: str           # 'chr1' or '1' depending on source; normalized to 'chr1' in plink2 output
    pos: int             # 1-based, build-specific
    effect_allele: str   # the allele whose dose × weight contributes
    other_allele: str | None
    weight: float
    rsid: str | None
```

`_prepare_plink2_scoring(scoring_file, plink2_out)` materializes the
plink2-format TSV from the parsed variants. It:

1. **Normalizes chromosome naming** to `chr1..chr22, chrX, chrY, chrM`.
2. **Coerces position to int**. Non-numeric rows (legacy catalog
   artifacts) are skipped with a count surfaced in
   `scoring_diagnostics.parser_warnings`.
3. **Dedupes by `(chrom, pos)`** keeping the first occurrence — plink2
   refuses scoring files with duplicate variant IDs. The dropped
   duplicates are logged.
4. **Records the fingerprint** in `meta.json`. The fingerprint is the
   sha256 of `sorted("chr|pos|effect_allele|float(weight)" per row)`
   — this is the `variant_ids_sha256` field stamped into ref-stats.

## 3.5 Liftover

`pipeline/liftover_v2.py::lift_scoring_file(from_build, to_build,
scoring_file, out_file, tmpdir)` lifts the harmonized scoring file
between GRCh37 and GRCh38 using UCSC `liftOver` and the chain files
documented in §10.

```
chain files:
    /data/ancestry_reference/hg19ToHg38.over.chain.gz   GRCh37 → GRCh38
    simple-genomics/liftover/hg38ToHg19.over.chain.gz   GRCh38 → GRCh37
```

Liftover is on the scoring file (~thousands of rows), never on the
user's VCF (millions). For each variant we write a single-line BED
input (`chr\tpos-1\tpos\trsid`), run `liftOver`, and rewrite the
scoring TSV with the new coordinates. Failures (multi-mapped or
unmapped) are dropped and counted in `metadata['liftover_failures']`;
the count is surfaced in the report.

Edge cases:

- **Build-mismatched harmonized files**: a PGS may have
  `genome_build=GRCh37` but `HmPOS_build=GRCh38`. We honor `HmPOS_build`
  for positions because that's what's actually in the file.
- **Strand flips during liftover**: liftOver doesn't reorient strands.
  We don't currently strand-flip the effect allele either — multi-base
  REF/ALT pairs lifted across a strand-flip-prone region (rare in
  modern catalog files) silently miss. The match-rate gate eventually
  catches the systematic case.
- **GRCh37 catalog only**: rare for new PGSes. When it happens, we
  liftover GRCh37 → GRCh38 once and cache the result; subsequent runs
  use the cached lifted file directly.

## 3.6 Eligibility gates (`pipeline/eligibility_gates.py`)

After ingestion but before scoring, every (PGS, user) pair goes through
`eligibility_for_pgs(pgs_id, pgs_metadata, user_assigned_population,
user_sex, liftover_passed, has_harmonized_target_build, variants)`.
The verdict is a `EligibilityVerdict(eligible: bool, status: str,
reasons: list[str], risk_language_allowed: bool)`.

The six gates (in order, first failure short-circuits):

| Gate                | Failure status            | Check |
| ------------------- | ------------------------- | --- |
| Build availability  | `liftover_failed`         | Harmonized target build exists OR liftover passed |
| Complex alleles     | `complex_alleles`         | No variants in HLA region (chr6:28-34Mb) or other curated complex-allele windows |
| Weight type         | `weight_type_unknown`     | `weight_type ∈ {beta, log_or, log_hr}` |
| Ancestry resolved   | `ancestry_unresolved`     | User population is known (Phase 1.5 — no EUR default) |
| Ancestry match      | `ancestry_mismatch`       | User pop ∈ `development_ancestry` ∪ `evaluation_ancestry` |
| Performance metric  | `performance_insufficient`| Binary trait AUC ≥ 0.55 OR continuous trait R² ≥ 0.02 |
| Direction known     | `direction_unknown`       | `trait_direction` declared (higher / lower / unspecified) |

The `risk_language_allowed` flag is `True` only when all six gates pass
AND the user's ancestry/sex match a validated PGS Catalog evaluation
set. When `False`, `pipeline/result_guards.py::filter_risk_language`
substitutes phrases like "elevated risk" with "(risk language withheld;
PGS not validated for this ancestry/sex)" in the LLM interpretation.

Today the `ancestry_mismatch` gate is the **second** route to a
no-percentile outcome for non-EUR users (the **first** is the
ref-stats schema rejection covered in chapter 09). Some PGS Catalog
entries declare `evaluation_ancestry={'EUR'}` only; an EAS user will
fail this gate even if EAS ref-stats exist. That is the correct
behavior for the eligibility layer — the PGS was never evaluated for
this ancestry — but it should produce a different user-facing message
than the ref-stats-schema failure (see chapter 09 §9.5).

## 3.7 Portability list (`pipeline/portability_warnings.py`)

A small hardcoded list of PGSes known to transfer poorly across
ancestries even when the eligibility gate passes:

```python
KNOWN_LOW_PORTABILITY = {
    "PGS000898": "Type 2 diabetes — EUR-derived weights show ~50% reduced \
                  R² in EAS; published comparison (Marquez-Luna 2017).",
    # … add as confirmed.
}
```

`live_percentile.py:187` calls `portability_warning(pgs_id)` on every
report read; if the PGS is in the list, `low_portability_pgs=True` is
attached and the UI renders an "ancestry-portability caveat" ribbon.

## 3.8 Curated PGS subsets

`pgs_curated_list.md` / `pgs_reorganized.md` define the hand-picked
~250 PGSes the UI shows by default. The "Common PGS" tab is a stricter
top-23 list, one per condition. The Compare page (`/compare`) auto-
aggregates these per user. These are presentation choices — they don't
affect scoring math.

## 3.9 Catalog refresh

`pipeline/ingest_pgs.py::download_scoring_file(pgs_id)` downloads
once and caches under `/data/pgs_cache/<PGS_ID>/`. We do not have a
scheduled re-fetch — catalog updates only land when a user requests a
PGS we haven't cached, OR when an operator runs
`scripts/refresh_pgs_cache.py --pgs <id>`. The PGS000334 stale-cache
incident showed why this matters: when the catalog updates a scoring
file, our ref-stats become stale relative to the user's live score.
The mitigation is the `variant_ids_sha256` contract enforced at every
load (chapter 07), not a refresh schedule.
