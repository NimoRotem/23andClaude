# 04 — Scoring Pipeline (plink2)

Once an input is normalized to a pgen and a PGS scoring file is in
canonical form, scoring itself is a single `plink2 --score` invocation.
The runner code wraps it with two parallel paths for performance and
correctness.

Entry point: `runners.run_pgs_score(vcf_path, params, progress_cb)`.

## 4.1 Canonical plink2 invocation

```
plink2
    --pfile          <user.pgen.prefix>
    --score          <scoring_plink2.tsv> header-read 1 2 3
                     cols=+scoresums
                     no-mean-imputation
                     list-variants
    --allow-extra-chr
    --threads        1     (PLINK_SCORE_THREADS)
    --memory         16000 (PLINK_MEMORY_MB)
    --out            <score_prefix>
```

The flags are deliberate and load-bearing:

| Flag                       | Why |
| -------------------------- | --- |
| `header-read 1 2 3`        | scoring file uses columns 1=ID, 2=A1 (effect allele), 3=WEIGHT; plus a header line. |
| `cols=+scoresums`          | emit `SCORE1_SUM` (= Σ dosage × weight) **in addition** to `SCORE1_AVG`. Both are parsed; `raw_score` defaults to `AVG`, `score_sum` is reported for scale reconciliation. |
| `no-mean-imputation`       | unmatched variants contribute 0, not the panel mean. We pair this with `imputation_policy=no-mean-imputation` in the ref-stats contract so live and reference are always computed consistently. |
| `list-variants`            | writes `<prefix>.sscore.vars`; this is the per-run matched-variant set. We use it to compute the match rate and (for the dynamic fallback) to restrict the reference rescore. |
| `--allow-extra-chr`        | the 1000G panel uses chrom code `26` for chrM under `--output-chr 26`; the user pgen may or may not. Allows mismatched codes through validation rather than failing. |

The string `"plink2-nomi"` is what gets recorded in
`scoring_method` / `EXPECTED_SCORING_METHOD` and validated against
loaded ref-stats. Switching to a different plink2 mode requires bumping
this constant *and* regenerating every ref-stats JSON; the loader will
otherwise refuse them.

## 4.2 Two scoring paths

### 4.2.1 Fast path — gVCF + small PGS

`runners._score_pgs_fast` is taken when:
- input is a gVCF, **and**
- `metadata.variant_count <= 2000`

Why: building the full normalized gVCF + pgen cache is expensive
(~minutes) and only worthwhile if many PGS will reuse it. For a
small-variant PGS run in isolation, we instead expand only the PGS-set
positions on the fly:

```
1. Write per-PGS positions file (chrom \t pos) from scoring_plink2.tsv
2. bcftools convert --gvcf2vcf -f <ref> -R positions.tsv \
      --regions-overlap 1 -Oz -o expanded.vcf.gz <gvcf>
   #   -R (regions-file, index-based random access)
   #   --regions-overlap 1 (include ref blocks that span a target)
3. In-process rewrite of <*>/<NON_REF> ALTs to the PGS effect allele
   (per-PGS allele map, not the global ~277MB map)
4. plink2 --make-pgen on the small VCF
5. plink2 --score (canonical args above)
6. Parse .sscore, compute match rate, percentile, etc.
```

Typical wall time: 2–10 seconds vs. ~15 minutes for the full normalize
pipeline. If the fast path fails (e.g. bcftools error), the runner
falls back to the slow path.

### 4.2.2 Full path — gVCF (large PGS) or non-gVCF VCF

`runners.run_pgs_score` after the fast-path check:

```
 ┌─────────────────────────────────────────────────────────┐
 │ Build / reuse pgen cache                                │
 │   _get_or_build_pgen(vcf_path)                          │
 │   → if cache fresh, return existing prefix              │
 │   → otherwise: lock(per-key), normalize gVCF if needed, │
 │     run _vcf_to_pgen (2 plink2 stages), stamp mtime,    │
 │     atomic rename                                       │
 └─────────────────────────────────────────────────────────┘
 ┌─────────────────────────────────────────────────────────┐
 │ plink2 --score with canonical args (4.1)                │
 └─────────────────────────────────────────────────────────┘
 ┌─────────────────────────────────────────────────────────┐
 │ Parse sscore + warnings                                 │
 │   raw_score = SCORE1_AVG                                │
 │   score_sum = SCORE1_SUM                                │
 │   matched   = lines in .sscore.vars                     │
 │   plink2_warnings = grep "skipped due to ..." etc.      │
 └─────────────────────────────────────────────────────────┘
 ┌─────────────────────────────────────────────────────────┐
 │ Match-rate adjustment                                   │
 │   ALLELE_CT = 2 * (non-missing variants)                │
 │   If ALLELE_CT/2 < listed-vars count, that diff is the  │
 │   "./. genotyped" subset → not counted as matched.      │
 │   matched = min(matched, ALLELE_CT // 2)                │
 └─────────────────────────────────────────────────────────┘
 ┌─────────────────────────────────────────────────────────┐
 │ Match-rate gate                                         │
 │   matched == 0                  → return failed (no report) │
 │   match_rate_pct < 60           → return failed (no report) │
 │   60 ≤ match_rate_pct < 85      → status='warning'          │
 │   match_rate_pct ≥ 85           → status='passed'           │
 └─────────────────────────────────────────────────────────┘
 ┌─────────────────────────────────────────────────────────┐
 │ Percentile (see doc 06)                                 │
 │   _compute_percentile(... ancestry_result=ancestry_hint)│
 │   → ancestry-aware ref selection                        │
 │   → z-score + Φ + sanity gates                          │
 └─────────────────────────────────────────────────────────┘
 ┌─────────────────────────────────────────────────────────┐
 │ Confidence score (see doc 07)                           │
 │   _compute_confidence(result, pctl_details, build_check)│
 └─────────────────────────────────────────────────────────┘
 ┌─────────────────────────────────────────────────────────┐
 │ Persist                                                 │
 │   pipeline_db.insert_sample_result(task_id, pgs_id,     │
 │       sample_id, raw_score, percentile, selected_ref,   │
 │       match_rate)                                       │
 │   _postprocess_pgs_result(d) → result_guards.attach_*   │
 └─────────────────────────────────────────────────────────┘
```

### 4.2.3 plink2 warnings we surface explicitly

`_parse_plink2_score_warnings` extracts:

- "N variants skipped due to mismatching allele code(s)": the variant
  is in the pgen but the REF/ALT orientation doesn't match the scoring
  file. Silent in plink2 — we treat ≥5% of total as a hard warning.
- "N variants skipped due to missing input": variants in the scoring
  file with no pgen counterpart. Already counted in match rate.
- "N variants skipped due to all genotypes missing": all-`./.` rows.
  Reported in `genotyped` calculation above.

These are surfaced into `scoring_diagnostics` on every report so a
reviewer can audit silent drop counts without re-running.

## 4.3 The pgen cache (`_get_or_build_pgen`)

```
/data/pgen_cache/
  <key>/
    sample.pgen
    sample.pvar
    sample.psam
    .vcf_mtime   # float seconds, compared on every read
```

`<key> = sha1(realpath)[0:16] + "_" + sha1(var_id_template + output_chr + SCHEMA)[0:8]`

Concurrent callers serialize on `_get_pgen_lock(key)` so the first
worker builds and the rest reuse. Multi-stage build:

1. **Stage 1 — VCF→pgen**:
   ```
   plink2 --vcf <input> --make-pgen
       --allow-extra-chr
       --split-par b38
       --update-sex <sex_file>  (all unknown)
       --vcf-half-call m         (treat ./1 as missing)
       --set-all-var-ids chr@:#
       --new-id-max-allele-len 100 missing
       --rm-dup force-first
       --threads <PLINK_BUILD_THREADS>
       --memory  <PLINK_MEMORY_MB>
       --out <stage1>_unsorted
   ```
2. **Stage 2 — sort variants**:
   ```
   plink2 --pfile <stage1>_unsorted --make-pgen --sort-vars
       --allow-extra-chr
       --out <prefix>
   ```

Stage 1 outputs are unsorted because `--make-pgen` and `--sort-vars`
can't be combined in a single invocation.

For the PCA path the same function is called with `var_id_template =
"@:#:$r:$a"` and `output_chr = "26"`, which materializes a separate
cache directory keyed by those params. The two pgen flavors coexist on
disk.

## 4.4 What "raw_score" actually means

`_parse_sscore(sscore_path)` returns:

| Field              | Source            | Meaning                                      |
| ------------------ | ----------------- | -------------------------------------------- |
| `raw_score`        | SCORE1_AVG        | Σ(dosage × weight) / (2 × n_matched)         |
| `score_sum`        | SCORE1_SUM        | Σ(dosage × weight) — used for scale fallback |
| `allele_count`     | ALLELE_CT         | 2 × n_non-missing variants                   |
| `sample_id`        | IID column        | usually the file basename                    |

A common confusion: some published PGS reports raw score as **sum**,
some as **per-allele average**. Our ref-stats μ/σ are computed against
SCORE1_AVG (the plink2 default). If the user's `raw_score` is wildly
smaller than the ref mean (specifically `|raw_score| < |mean| * 0.001`)
we automatically swap in `score_sum` as the compare-score and tag the
detail `scale_correction: "Using score_sum vs precomputed SUM-scale stats"`.

## 4.5 Match rate and the 60/85 thresholds

| Match rate (matched / total catalog variants) | Status   | UI |
| --------------------------------------------- | -------- | --- |
| 0                                             | failed   | no report (chip-build mismatch?) |
| < 60                                          | failed   | no report ("match rate too low") |
| 60–85                                         | warning  | report shown, red chip |
| 85–95                                         | passed   | yellow chip |
| ≥ 95                                          | passed   | green chip |

The threshold targets:
- < 60: scoring is dominated by mean-coverage variants; the percentile
  estimate is intrinsically biased even with `no-mean-imputation` because
  the missing tail of the score distribution isn't represented.
- 85: empirical break point at which WGS-derived gVCF inputs reach a
  stable percentile estimate compared to a fully-genotyped reference.

These numbers come from the in-batch HG00096 control's drift behaviour
over PGS001229 / PGS000334 / PGS002753 — see `10-known-issues.md`.

## 4.6 Confidence score (`_compute_confidence`)

Each report carries a `confidence ∈ {high, medium, low}` and a list of
machine-readable `confidence_reasons`. Downgrades include:

- `match_rate < 95%` → at most `medium`
- `match_rate < 85%` → at most `low`
- `build_validation.status == "WARN"` → at most `medium`
- `build_validation.status == "FAIL"` → at most `low`
- `sanity.gates_tripped` contains an item → `low`
- `available_refs` does not include the ancestry-selected primary → `medium`
- `cohort_sanity_flagged` (PGS in the KS-fail set) → `low` and adds a UI banner

The reviewer should treat `low` results as exploratory, not
diagnostic.
