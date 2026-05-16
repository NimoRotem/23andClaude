# 11 — End-to-end Worked Examples

Four examples covering the input types the pipeline accepts. Each
example is the actual log trace the reviewer would see when running
the pipeline directly. Genuine sample IDs, PGS IDs, and file paths are
used; numeric values are taken from sample reports in
`users/<u>/reports/`.

## 11.1 Example 1 — VCF input, EUR sample, PGS000004 (CAD)

**Input**:
- `/data/genom-nimo/HG00096/HG00096.vcf.gz` (1000G EUR control sample)
- `test_def`: `{test_type: "pgs_score", params: {pgs_id: "PGS000004", trait: "CAD"}}`

**Flow** (`runners.py::run_pgs_score`):

```
[1/9] Detect file type → 'vcf'
[2/9] Download scoring file → /data/pgs_cache/PGS000004/PGS000004_hmPOS_GRCh38.txt.gz  (cached)
[3/9] Parse + prep plink2 TSV: 46 variants ingested, 0 dropped
[4/9] Build validation:
       header_build = GRCh38 (matches scoring_file)
       spot_check: rs7412 GRCh38=YES, rs429358 GRCh38=YES, rs1801133 GRCh38=YES
       → PASS
[5/9] Build pgen from VCF → /data/pgen_cache/<sha>/sample.pgen   (cached after first run, ~90s first time)
[6/9] plink2 --score (1 thread, 16GB):
       --score: 46 variants processed.
       --score: 0 variants skipped due to mismatching allele code.
       raw_score (SCORE1_AVG) = 0.001247
       score_sum (SCORE1_SUM) = 0.1147
       variants_matched = 46/46  (100% match)
[7/9] Compute percentile:
       Ancestry: HG00096 PCA → super_pop=EUR (confidence=high), proportions={EUR: 0.84, ...}
       select_reference → primary=EUR, secondary=[AMR, AFR]
       _load_stats(PGS000004, EUR, GRCh38) → /data/pgs2/ref_panel_stats/PGS000004_EUR_GRCh38_n303_plink2-nomi_sha-8856f202.json
       _rs_validate: PASS
       μ_EUR=-0.000045, σ_EUR=0.000812, n=303
       z = (0.001247 - (-0.000045)) / 0.000812 = 1.589
       p = Φ(1.589) × 100 = 94.4
       sanity gates: all pass
[8/9] Post-process:
       scoring_panel_population = "EUR (European, n=503)"
       selected_ref = "EUR"
       confidence_reasons = []  (no cross_ancestry, match_rate=100, build PASS)
       confidence = "high"
[9/9] Result:
       percentile=94.4, raw_score=0.001247, score_sum=0.1147
       headline="PGS000004 percentile: 94.4 (raw 0.001247)"
       status="passed"
```

**LLM interpretation** (Gemini): "Polygenic score for Coronary Heart
Disease. This individual's score (94th percentile) is in the top
decile of the European reference distribution; consistent with
substantially elevated genetic predisposition. Standard CAD prevention
advice (lipids, blood pressure, smoking cessation) is appropriate."

## 11.2 Example 2 — gVCF input, EAS sample, PGS000007 (BMI) — happy path

**Input**:
- `/data/genom-nimo/SZ7A76M9LNU/SZ7A76M9LNU.g.vcf.gz` (Chinese-Han 30× WGS)
- `test_def`: `{test_type: "pgs_score", params: {pgs_id: "PGS000007", trait: "BMI"}}`

**Flow**:

```
[1/9] Detect: 'vcf', _is_gvcf=True
[2/9] Download → /data/pgs_cache/PGS000007/  (cached)
[3/9] Parse: 97 variants ingested
[4/9] Build validation: GRCh38, all 3 spot-check rsIDs present → PASS
[5/9] Fast path? variant_count=97 ≤ 2000 AND _is_gvcf → YES
       bcftools view -R positions.bed → subset.vcf.gz (~50ms)
       bcftools convert --gvcf2vcf -T positions.bed --targets-overlap 1
         → expanded.vcf.gz (~120ms)
       Rewrite <*>/<NON_REF> ALTs to effect alleles
       plink2 --vcf expanded.vcf.gz --make-pgen → small_pgen
       plink2 --pfile small_pgen --score scoring.tsv → result
       (total ~3s for the fast path)
[6/9] plink2 --score:
       --score: 97 variants processed.
       variants_matched = 93/97  (95.9% match)
       raw_score = 0.000823, score_sum = 0.1597
[7/9] Compute percentile:
       Ancestry: PCA → super_pop=EAS (confidence=high), proportions={EAS: 0.87, ...}
       select_reference → primary=EAS, secondary=[SAS, EUR]
       _load_stats(PGS000007, EAS, GRCh38) →
         /data/pgs2/ref_panel_stats/PGS000007_EAS_GRCh38_n3686_plink2-nomi_sha-ad47ad35.json
       _rs_validate: PASS  (registry file, full schema)
       μ_EAS=0.000681, σ_EAS=0.000354, n=585
       z = (0.000823 - 0.000681) / 0.000354 = 0.401
       p = Φ(0.401) × 100 = 65.6
       per_pop_percentiles also computed for EUR (61.2), AFR (74.0), SAS (66.8), AMR (62.3)
[8/9] Post-process:
       selected_ref = "EAS"
       confidence_reasons = ["cross_ancestry_transfer"]
         (because selected_ref != EUR; the EUR-trained PRS warning still applies)
       confidence = "low"
       cross_ancestry_warning attached
[9/9] Result:
       percentile=65.6, status="passed"
       BUT: confidence=low, cross_ancestry_warning visible
```

**LLM interpretation** (Gemini): "Polygenic score for body mass index.
This individual's score (66th percentile against the East Asian
reference distribution) is mildly above average. Note: the score
itself was computed from European-derived weights and may transfer
imperfectly to East Asian populations; treat as exploratory rather
than clinically actionable."

This is the **right** EAS outcome — percentile + transparent caveat.
The failure case in §11.3 is what we see for most PGSes.

## 11.3 Example 3 — gVCF input, EAS sample, PGS000001 (BMI alt) — current failure

Same sample as §11.2, different PGS:

**Input**:
- `/data/genom-nimo/SZ7A76M9LNU/SZ7A76M9LNU.g.vcf.gz`
- `test_def`: `{test_type: "pgs_score", params: {pgs_id: "PGS000001", trait: "BMI"}}`

**Flow** (same as §11.2 through step 6):

```
[6/9] plink2 --score: 154 variants → matched 145 (94.2%), raw_score=0.005203
[7/9] Compute percentile:
       Ancestry: EAS as before
       select_reference → primary=EAS
       _load_stats(PGS000001, EAS, GRCh38):
         registry → no entry  (PGS000001 not blessed)
         new path → /data/ref_stats/PGS000001/EAS_GRCh38.json  EXISTS
         _rs_validate raises IncompatibleRefStats(
           "missing required keys: ['generated_at', 'imputation_policy',
            'n_variants', 'scoring_method', 'variant_ids_sha256']"
         )
         loader attaches _incompatible_reason and returns the dict
       _compute_single_percentile sees _incompatible_reason set:
         method = "incompatible_ref_stats"
         reason = "missing required keys: [...]"
         percentile = None
       For EUR (as secondary): same failure (also schema-incomplete)
       For AFR/SAS/AMR: same
[8/9] Post-process:
       percentile = None
       selected_ref = "EAS"
       confidence_reasons = ["no_precomputed_stats", "cross_ancestry_transfer"]
       confidence = "low"
       cross_ancestry_warning attached
[9/9] Result:
       percentile=null, raw_score=0.005203 (still emitted)
       headline = "PGS000001: percentile unavailable (incompatible_ref_stats)"
       status = "warning"
```

**LLM interpretation** (Gemini): "The individual has East Asian (EAS)
ancestry but was scored against a European reference population, which
significantly reduces prediction accuracy. No percentile ranking can be
provided due to incompatible reference statistics and the lack of
precomputed ancestral benchmarks."

This is the **wrong** outcome. The EAS ref-stats file exists with valid
μ/σ — only the metadata fields are missing. See chapter 09 §9.5 for
the proposed fixes. Note also that the LLM's framing conflates two
separate failure modes ("scored against EUR" — true; "incompatible
reference stats" — true; "lack of precomputed ancestral benchmarks" —
false, they exist but were refused).

## 11.4 Example 4 — CRAM input, AFR sample, PGS000898 (T2D), Pipeline E+

**Input**:
- `/data/genom-nimo/NA19238/NA19238.cram` (Yoruba 1000G)
- `test_def`: `{test_type: "pgs_score", params: {pgs_id: "PGS000898", trait: "T2D"}}`

**Flow** (`runners.py::_run_pgs_score_pileup`, Pipeline E+):

```
[1/8] Detect: 'cram'; dispatch → Pipeline E+ direct pileup
[2/8] Reference selection:
       _pick_reference_for: candidates → /data/genom-nimo/reference_chr.fa
       contig naming match (chr-prefixed)
[3/8] Load PGS variants from scoring file: 5,317 variants
[4/8] Per-variant pileup (pysam, min_mapq=20, min_baseq=20, max_depth=250):
       SNPs: 5,289 → handled by _pileup_genotype
       indels: 28 → handled by _pileup_genotype_indel
       parallelized per chrom across 12 workers
[5/8] Genotype calls aggregated:
       matched = 5,041 / 5,317  (94.8% match)
       missing = 276 (mostly low_coverage < 8x, some indel misalign)
       score_sum (Python-summed dose × weight) = 2.1583
       raw_score = 2.1583 / (2 × 5041) = 0.000214
[6/8] (build_validation skipped — BAM coordinates are aligner-implicit)
[7/8] Compute percentile:
       Ancestry: PCA from cached pca.vcf.gz → super_pop=AFR (high), {AFR: 0.92, ...}
       select_reference → primary=AFR
       _load_stats(PGS000898, AFR, GRCh38):
         registry → /data/pgs2/ref_panel_stats/PGS000898_AFR_GRCh38_n893_plink2-nomi_sha-cb43...json
         _rs_validate: PASS
       μ_AFR=0.000198, σ_AFR=0.000045
       z = (0.000214 - 0.000198) / 0.000045 = 0.356
       p = Φ(0.356) × 100 = 63.9
       portability_warning("PGS000898") → "known low cross-ancestry portability..." attached
[8/8] Post-process:
       selected_ref = "AFR"
       confidence_reasons = ["cross_ancestry_transfer"]
       low_portability_pgs = True   (PGS000898 is in KNOWN_LOW_PORTABILITY)
       portability_warning text attached
       confidence = "low"
```

**LLM interpretation** (Gemini): "Type 2 diabetes polygenic score. The
score (64th percentile against an African reference) is moderately
elevated. Important caveat: PGS000898 was developed in a European
cohort, and the published comparison (Márquez-Luna 2017) reports ~50%
reduced R² when applied to African ancestry. This estimate is for
exploratory purposes; clinical risk should be assessed via standard
T2D screening (HbA1c, fasting glucose, family history)."

This is the right outcome for AFR — registry-blessed stats exist for
PGS000898, percentile renders, the hardcoded portability warning gives
the LLM the specific reference to cite.

## 11.5 Example 5 — 23andMe text input, EUR sample, PGS000007 (BMI)

**Input**:
- `genome_James_Smith_v5_Full_20210315.txt` (23andMe v5, ~640K SNPs)
- `test_def`: `{test_type: "pgs_score", params: {pgs_id: "PGS000007", trait: "BMI"}}`

**Flow**:

```
[0/9] bam-converter (external, on upload): TSV → synthetic VCF
       640,134 sites, REF/ALT resolved via dbSNP
       output: <upload>/James_Smith.vcf.gz
[1/9] _detect_file_type: 'vcf'
[2/9] Download PGS000007 (97 variants)
[3/9] Parse, prep plink2 TSV
[4/9] Build validation:
       Header has no ##reference; spot_check rs7412 + rs1801133 → GRCh38 hits
       → PASS  (chip data with 2/3 spot SNPs is acceptable)
[5/9] Fast path: PGS variants=97, but not gVCF → skip; full pgen path
[6/9] Build pgen, run plink2 --score:
       --score: 97 variants processed.
       But only 41/97 of the PGS positions were on this v5 chip
       match_rate = 41/97 = 42.3%
       → status=failed; match-rate gate trips below 60%
[7/9] Compute percentile:
       (gate failed; percentile not computed)
[8/9] Post-process:
       headline = "Match rate 42.3% < 60%: chip too sparse for this PGS"
       status = "failed"
[9/9] Result:
       percentile=null, raw_score=null
       status="failed", with explanatory headline
```

**LLM interpretation** (Gemini): "This consumer-chip file contains
42% of the SNPs the PGS000007 BMI score requires. The pipeline does
not produce a result for matches below 60% because the score becomes
dominated by missing-variant noise. For polygenic scoring against
modern PGS Catalog entries, low-coverage WGS or imputed chip data is
recommended."

## 11.6 Example 6 — gVCF input, admixed user, MULTI fallback

**Input**:
- `~/genom-nimo/family6/F6S2.g.vcf.gz`
- Same PGS as §11.2 (PGS000007 BMI)

**Ancestry result**: `proportions={EUR: 0.43, AMR: 0.31, AFR: 0.19, SAS: 0.04, EAS: 0.03}`

```
[7/9] Compute percentile:
       Top posterior = EUR at 0.43 < 0.80 → primary = "MULTI"
       select_reference returns RefSelection(
         primary="MULTI", secondary=["EUR","EAS","AFR","SAS","AMR"],
         reason="admixed (top=EUR=43% < 0.80) — emit percentile_by_population array; no fixed MIX (§1.5)"
       )
       compute_percentile_multipop loads stats for all 5 pops, computes per-pop percentiles
       primary_percentile = (look up MULTI in _load_stats → not found, falls through to UNAVAILABLE)
[8/9] Post-process:
       percentile = None  (no MULTI stats file exists)
       per_pop_percentiles = {EUR: 67.3, AMR: 70.5, AFR: 82.1, SAS: 68.0, EAS: 71.0}
       confidence_reasons = ["no_precomputed_stats", "cross_ancestry_transfer"]
[9/9] Result:
       status = "warning"
       headline = "PGS000007: admixed sample; per-population sensitivity shown"
       UI renders the per_pop bar chart
```

This is Phase 1.5's intended behavior — admixed users get the
sensitivity grid rather than a misleading single number. The reviewer
may want to opine on whether the AF-match hint (`af_match_ref`)
should be promoted to a "best-fit" primary in this case, vs. leaving
the user to pick.
