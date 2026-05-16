# 04 — Scoring Pipeline

## 4.1 The canonical plink2 invocation

Every PGS score (across all three entry points) is ultimately computed
via the same `plink2 --score` invocation:

```
plink2 \
  --pfile <user_prefix> \
  --score <pgs_plink2_format.tsv> 1 2 3 header-read \
       cols=+scoresums no-mean-imputation list-variants \
  --score-col-nums 3 \
  --allow-extra-chr \
  --threads <PLINK_SCORE_THREADS> \
  --memory <PLINK_MEMORY_MB> \
  --out <out_prefix>
```

The four key flags:

- `no-mean-imputation` — missing alleles do **not** get imputed to the
  mean. Either the variant matched and contributes its true dose, or
  it's missing and contributes 0. Without this flag, the same user gets
  different scores depending on which other samples are in the pgen.
- `cols=+scoresums` — emit `SCORE1_SUM` alongside `SCORE1_AVG`.
  Some PGS Catalog weight columns are scaled assuming SUM scale, not
  AVG. The percentile path uses SUM when the reference distribution
  was computed on SUM scale (§7.4).
- `list-variants` — emit `<out>.sscore.vars` listing every variant
  plink2 used. We diff this against the input scoring file to detect
  silent drops.
- `--score-col-nums 3` — explicitly select column 3 as the weight.
  Default behavior is undefined when extra columns are present.

The output we consume:

| File                  | Field        | Used for |
| --------------------- | ------------ | --- |
| `<out>.sscore`        | `SCORE1_AVG` | `raw_score` (the "average effect per scored allele") |
| `<out>.sscore`        | `SCORE1_SUM` | `score_sum` (sum of dose × weight) |
| `<out>.sscore.vars`   | rows         | `variants_matched` count |
| `<out>.log`           | text         | `--score: N variants processed.` → `_parse_plink2_score_match_count` |
| `<out>.log`           | text         | `--score: N variants skipped due to mismatching allele code.` → `scoring_diagnostics` |

`scoring_method=plink2-nomi`, `imputation_policy=no-mean-imputation`
are stamped into every ref-stats JSON so a future invocation that drops
`no-mean-imputation` would be detected as schema-incompatible.

## 4.2 Entry point A — Full-pgen path (`run_pgs_score`)

This is the default for VCFs and the fallback for everything else. The
flow is:

1. **Download scoring file** (`_download_pgs_scoring_file`) → cache.
2. **Prepare plink2-format TSV** (`_prepare_plink2_scoring`) → write
   `<pgs>_plink2.tsv` alongside `meta.json`.
3. **Build validation** (`_validate_genome_build` § 02.4) → maybe
   liftover the scoring file.
4. **Fast-path probe**: if input is a gVCF and PGS variant count
   ≤ 2,000, branch to `_score_pgs_fast` (§4.3).
5. **Ensure indexed**: `_ensure_indexed` → `.tbi`.
6. **VCF → pgen** (`_get_or_build_pgen`): converts the user's VCF (or
   gVCF-normalized output from §02.3.3) into a plink2 pgen file. The
   resulting pgen is cached under
   `/data/pgen_cache/<sha(realpath(vcf))>/sample.pgen` with an mtime
   sentinel; subsequent PGS runs on the same VCF skip this step.
   Cache schema is `PGEN_CACHE_SCHEMA = "v5"`.

   Command shape:
   ```
   plink2 --vcf <vcf> dosage=DS \
       --double-id --allow-extra-chr \
       --max-alleles 2 --rm-dup force-first \
       --set-all-var-ids 'chr@:#' \
       --split-par b38 \
       --make-pgen --out <pgen_prefix>
   ```
7. **Score** (`plink2 --score`).
8. **Strand-flip recovery** (`_recover_strand_flips`): for variants
   plink2 silently skipped because effect_allele doesn't match the
   pgen REF/ALT, we try the complement (`A↔T`, `C↔G`) and re-run the
   skipped subset. Result is summed back into the score. The count of
   recovered variants is reported in `scoring_diagnostics`.
9. **Compute percentile** (`_compute_percentile_multipop_wrapper` →
   `pipeline/scoring.py`, §07).
10. **Post-process** (`_postprocess_pgs_result`): rename pipeline
    fields, attach `cross_ancestry_warning` if applicable, propagate
    `vcf_build`, finalize `confidence_reason`.

## 4.3 Entry point B — Fast path (`_score_pgs_fast`)

Triggered when the input is a gVCF AND the PGS scoring file has ≤2,000
variants. Skips the pgen cache entirely — uses indexed random access
(`bcftools view -R`) to read only the target positions:

```
1. Write PGS positions BED (chrom\tstart\tend)
2. bcftools view -R positions.bed -O z -o subset.vcf.gz <gvcf>
3. (gVCF block expansion of subset.vcf.gz, same as §2.3.3 but scoped to subset)
4. plink2 --vcf subset.vcf.gz dosage=DS --make-pgen --out small_pgen
5. plink2 --pfile small_pgen --score <plink2_scoring> ...
```

Empirically ~2–10s for a 100-variant PGS on a 30× WGS gVCF, vs.
~15 minutes for the full-pgen path. The 2,000-variant cap is a
ballpark — bigger PGS amortize the pgen cache cost. The fast path
preserves the same `no-mean-imputation` and `--score` flags, so the
percentile math is unchanged.

## 4.4 Entry point C — Pipeline E+ (`_run_pgs_score_pileup`)

Direct BAM/CRAM pileup, no VCF, no plink2. Selected automatically when
input is BAM/CRAM (`runners.py:8259`). Steps:

1. **Load PGS variants** from the cached scoring file.
2. **Per-variant pileup** (`_pileup_genotype` for SNPs,
   `_pileup_genotype_indel` for indels), reading the BAM/CRAM via
   pysam with `min_mapq=20, min_baseq=20, max_depth=250` (matching
   the CLI tool's parameters).
3. **Genotype call** from allele-balance arithmetic:

   ```
   AD = (ref_supporting_reads, alt_supporting_reads)
   DP = AD.sum()
   AB = AD[1] / DP
   if DP < _PILEUP_MIN_DEPTH:  -> low_coverage += 1
   elif AB <= 0.15:            -> 0/0
   elif AB >= 0.85:            -> 1/1
   else:                        -> 0/1
   ```
4. **Dose contribution**: `dose * variant.weight` summed across
   variants. `dose = 0/1/2` from the genotype call against the
   catalog's effect allele.
5. **Match-rate counting**: matched / total / missing.
6. **Percentile** via the same `_compute_percentile_multipop_wrapper`
   as the other paths.

Pipeline E+ skips build validation (BAM coordinates are implicit in
the aligner reference); the match-rate gate is the safety net. The
provenance bundle records `method="pileup (Pipeline E+)"` and
`normalization="direct BAM pileup at target positions (no VCF
intermediate)"`.

## 4.5 Match-rate gate

After scoring, `match_rate = variants_matched / variants_total` is the
single most important quality signal:

| match_rate     | status   | UI shows           | percentile emitted? |
| -------------- | -------- | ------------------ | --- |
| < 0.60         | failed   | red banner         | no |
| 0.60 – 0.85    | warning  | yellow ribbon      | yes (with caveat) |
| ≥ 0.85         | passed   | normal             | yes |

Failures don't surface raw_score either — the report shows "Pipeline
gate failed: match_rate=42.3% < 60%; this VCF likely doesn't carry the
SNPs this PGS scores against".

The match-rate gate is independent of the ancestry/eligibility gates.
A PGS can pass the match-rate gate and still fail eligibility
(`ancestry_mismatch`, `weight_type_unknown`, etc.) — and vice versa.

## 4.6 plink2 "skipped due to mismatching allele code"

plink2 silently drops a scoring variant when the pgen REF/ALT
orientation doesn't match the catalog's effect/other allele. Example:
catalog says effect=A other=G, user's pgen has REF=T ALT=C (the
reverse strand). plink2 prints:

```
--score: 17 variants skipped due to mismatching allele code.
```

`_parse_plink2_score_warnings` captures this count into
`scoring_diagnostics.skipped_due_to_mismatching_allele_code`. The
strand-flip recovery (§4.2 step 8) re-scores these by computing the
complement of the catalog effect_allele and matching against the
pgen REF/ALT explicitly.

Edge case: palindromic SNPs (A/T or C/G) can't be unambiguously
strand-flipped from alleles alone — we leave those as silent skips
and surface the count. The match-rate gate still applies.

## 4.7 Scoring diagnostics in the report

Every result includes a `scoring_diagnostics` dict that surfaces what
might be quietly going wrong:

```json
"scoring_diagnostics": {
  "variants_in_scoring_file": 974,
  "variants_after_dedup":      973,
  "variants_skipped_mismatching_allele_code": 12,
  "variants_recovered_by_strand_flip":         7,
  "match_rate_value":                         91.4,
  "raw_score":                              0.0013,
  "score_sum":                              1.2674,
  "ref_mean":                               0.0009,
  "ref_std":                                0.0004,
  "z_score":                                1.07,
  "method_used":                            "precomputed_stats",
  "parser_warnings": ["1 non-numeric POS row dropped (PGS Catalog artifact)"]
}
```

The fields are deliberately verbose so an outside reviewer can audit
without grepping logs.

## 4.8 Result dataclass (after _postprocess_pgs_result)

```json
{
  "test_type": "pgs_score",
  "pgs_id": "PGS000004",
  "trait": "Coronary heart disease",
  "status": "passed",
  "headline": "PGS000004 percentile: 71.0 (raw 0.0013)",
  "raw_score": 0.0013,
  "score_sum": 1.2674,
  "percentile": 71.0,
  "match_rate_value": 91.4,
  "variants_matched": 891,
  "variants_total": 974,
  "variants_missing": 83,
  "selected_ref": "EAS",
  "available_refs": ["EUR","EAS","AFR","SAS","AMR"],
  "secondary_percentiles": {"EUR": 67.3, "AFR": 82.1, "SAS": 68.0, "AMR": 70.5},
  "ancestry_model": "single_cluster (EAS=87%)",
  "confidence": "low",
  "confidence_reasons": ["cross_ancestry_transfer"],
  "cross_ancestry_warning": "Sample classified as EAS; PGS scored against the EUR 1000G reference panel (EUR (European, n=503)). PRS performance across ancestries is unreliable — interpret directional risk with caution and validate against same-ancestry data before acting on this number.",
  "pipeline_info": {
    "method": "plink2 --score (full pgen path)",
    "scoring_panel_population": "EUR (European, n=503)",
    "reference_panel": "1000G + NYGC high-coverage, GRCh38, 3,202 samples",
    "normalization": "gVCF expansion at union(PGS, PCA) positions; v5 cache",
    "percentile_details": { ... full _compute_single_percentile dict ... }
  },
  "scoring_diagnostics": { ... },
  "build_validation": { ... },
  "vcf_build": "GRCh38"
}
```

The `cross_ancestry_warning` is the prose the LLM (Gemini, Claude, or
OpenAI per user setting) sees. The phrasing in the user-visible
interpretation is **synthesized by the LLM** from this signal plus the
percentile-details `description` field — it is not a fixed string in
our codebase. See chapter 09 §9.3 for the trace.
