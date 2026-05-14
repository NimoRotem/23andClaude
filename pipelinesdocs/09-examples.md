# 09 — End-to-End Examples

Concrete worked examples per input type, with the exact commands and
intermediate files. All examples use real paths on `genom-beast-gpu`.

## 9.1 Example A — Score a 30× WGS gVCF against PGS000334 (CAD)

Input:
```
/data/user_data/<USER>/sample.g.vcf.gz   # GRCh38 gVCF, 30× WGS
```

Step-by-step:

```python
# 1. file-type detect
ftype = _detect_file_type(vcf_path)            # → 'vcf'  (gVCF .vcf.gz)
_is_gvcf(vcf_path)                              # → True

# 2. Validate build (sample lookups at rs7412, rs429358, rs1801133)
build_check = _validate_genome_build(vcf_path, "GRCh38")
# → {"status": "PASS", "vcf_build": "GRCh38",
#    "spot_check": {"matches_expected": 3, "status": "PASS"}, ...}

# 3. Variant count for PGS000334 (CAD) is small (22 variants after
#    panel resolution), and input is a gVCF, so the FAST path is used.

# 4. Fast path: positions file → bcftools convert --gvcf2vcf -R ... 
#    → rewrite <*>/<NON_REF> → mini pgen → plink2 --score
expanded_vcf  = fast_expanded.vcf.gz   # 22 records at PGS positions
rewritten_vcf = fast_rewritten.vcf.gz  # placeholders rewritten
pgen          = fast_pgen.{pgen,pvar,psam}
sscore        = fast_score.sscore

# 5. Parse sscore → raw_score, score_sum, allele_count
raw_score    = 0.000142
score_sum    = 0.006248
allele_count = 44                              # 2 * 22 matched
matched      = 22                              # all matched (no missing)
total        = 22
match_rate   = 100%

# 6. Ancestry — runs PCA against 1000G; admixture proportions ≈ EUR 95%
#    → ref_selection: primary=EUR, secondary=[MIX], reason="single_cluster (EUR=95%)"

# 7. Percentile (single-pop, EUR)
stats = load(/data/ref_stats/PGS000334/EUR_GRCh38.json)
# schema_version=1, n_samples=633, n_variants=22, variant_ids_sha256 matches
mean = 0.000124
std  = 0.000891
z    = (0.000142 - 0.000124) / 0.000891 = 0.0202
p    = Φ(z) × 100 = 50.81%

# 8. Sanity gates: all OK; not capped.

# 9. Result
{
  "test_type": "pgs_score",
  "pgs_id": "PGS000334", "trait": "CAD",
  "raw_score": 0.000142, "score_sum": 0.006248,
  "matched_variants": 22, "total_variants": 22, "match_rate": "100.0%",
  "percentile": 50.8, "selected_ref": "EUR",
  "secondary_percentiles": {"MIX": 51.2},
  "ancestry_model": "single_cluster (EUR=95%)",
  "scoring_method": "fast_direct",
  "build_validation": {"status": "PASS", ...},
  "scoring_diagnostics": {
    "ref_std": 0.000891, "ref_mean": 0.000124,
    "z_score": 0.020, "z_sanity": "ok",
    "ref_variants_matched": 22, "method_used": "precomputed_stats",
    "sanity_gates_tripped": []
  },
  "confidence": "high",
  "provenance": {
    "scoring_file_sha": "...", "stats_file_sha": "...",
    "ref_panel_sha": "...", "pipeline_commit": "3ede0ee..."
  }
}
```

Total wall time on the fast path: ~3 seconds.

## 9.2 Example B — Score a CRAM against PGS000018 (Schizophrenia, 1.74M variants)

Input:
```
/data/genom-nimo/<USER>/sample.cram          # GRCh38 CRAM
/data/genom-nimo/<USER>/sample.cram.crai     # may need to be created
```

```python
# 1. file-type
_detect_file_type(...)  # → 'cram'

# 2. The PGS scoring runner ONLY accepts vcf/gvcf. For BAM/CRAM input,
#    the user is asked to convert first, OR (for tests in _CRAM_OK_METHODS,
#    not pgs_score) the test derives a VCF on demand.

# For PGS scoring, the typical flow is to pre-convert:
$ bash simple-genomics/scripts/cram_to_vcf.sh \
       /data/genom-nimo/USER/sample.cram \
       /data/vcfs/USER/sample.vcf.gz

# Or for fast PGS-positions-only scoring on a CRAM:
$ bash simple-genomics/scripts/pgs_sites_call.sh \
       /data/genom-nimo/USER/sample.cram \
       /data/pgs_cache/_all_pgs_pca_positions_chr.tsv \
       /data/vcfs/USER/sample.pgs_sites.vcf.gz

# 3. The resulting VCF then enters the standard pipeline.
#    PGS000018 has 1.74M variants → goes through the FULL path:
#    a. _ensure_indexed (bgzip + tabix if needed)
#    b. _get_or_build_pgen
#       - Source is a non-gVCF VCF (cram_to_vcf produced var-only VCF)
#       - Stage 1: plink2 --vcf --make-pgen --split-par b38 ... --set-all-var-ids chr@:# --rm-dup
#       - Stage 2: plink2 --pfile <stage1> --make-pgen --sort-vars
#       - Cached at /data/pgen_cache/<sha>_<param>/
#    c. plink2 --score with the canonical flags
#    d. Parse: matched=1,468,512  total=1,735,924  match_rate=84.6%
#    e. Build validation: PASS
#    f. Ancestry: PCA → admixture (EUR 0.62, AMR 0.21, EAS 0.10, AFR 0.05, SAS 0.02)
#       → top_prop < 0.80 → primary="MIX", secondary=["EUR","AMR"]
#    g. Percentile against MIX ref-stats:
#       stats = /data/ref_stats/PGS000018/MIX_GRCh38.json (n_samples=1170)
#       z = (raw_score - μ_MIX) / σ_MIX
#       p ≈ 72.3
#    h. Secondary percentiles against EUR and AMR
#    i. match_rate 84.6% < 85% → status='warning' (red chip in UI)
#       confidence: "medium" (match rate < 95%)
```

For users who do not want to pre-convert, the UI now has a "PGS-sites
only" mode that runs `pgs_sites_call.sh` transparently and feeds the
resulting per-PGS-sites VCF straight in. Match rate stays high
(~99%) because hom-ref sites are emitted.

## 9.3 Example C — Mix-pop run with build mismatch

Input:
```
/tmp/sample.GRCh37.vcf.gz   # 23andMe converted to VCF (GRCh37)
PGS request: PGS000004 (BMI)
```

```python
# 1. Build validation
build_check = _validate_genome_build("/tmp/sample.GRCh37.vcf.gz", "GRCh38")
# → status="FAIL", vcf_build="GRCh37", message="Build mismatch ..."

# 2. Auto-liftover triggered
_liftover_pgs_scoring(plink2_scoring="/data/pgs_cache/PGS000004/scoring_plink2.tsv",
                      from_build="GRCh38", to_build="GRCh37", tmpdir=...)
# → writes a lifted scoring file in tmpdir; metadata.liftover="GRCh38→GRCh37"
# build_check is downgraded to WARN, scoring proceeds.

# 3. Match rate ends up at 21% (23andMe is a sparse chip) → status='failed', no report
#    Reason: BMI PGS has too many variants for chip data; bug-free behavior.
```

## 9.4 Example D — PCA / admixture for a CRAM (no PGS run)

```python
# UI test: 'PCA projection onto 1000G' on /data/genom-nimo/USER/sample.cram

# 1. ftype='cram' → method='pca_1000g' in _CRAM_OK_METHODS → proceed
# 2. PCA reference cache present? if not, build (one-time ~10 min)
# 3. _derive_pca_vcf_from_cram(sample.cram):
#    - read 106K positions from /data/pgs_cache/pca_1000g/ref.eigenvec.allele
#    - samtools view --input-fmt-option ignore_md5=1 -T <ref> -b -L positions.bed
#      sample.cram chr1 chr2 ... chr22  →  slice.bam (~hundreds of MB)
#    - bcftools mpileup -f <ref> -R positions.tsv ... | bcftools call -m
#      → pca.vcf.gz (all genotypes incl hom-ref)
#    - cached at cram_vcf_cache/<sha>/pca.vcf.gz
# 4. _get_or_build_pgen(pca.vcf.gz, var_id_template="@:#:$r:$a", output_chr="26")
# 5. plink2 --pfile <user> --read-freq <ref.afreq> 
#       --score <ref.eigenvec.allele> 2 5 header-read 
#           no-mean-imputation variance-standardize
#       --score-col-nums 6-15
# 6. Parse projected.sscore → 10 PCs
# 7. Compute distances to 5 super-pop centroids over PC1..PC4
# 8. Best = EAS (distance 0.0421), second = SAS (distance 0.0689)
#    confidence: (0.0689 - 0.0421) / 0.0421 = 64% → "high"

# Result:
{
  "test_type": "specialized", "method": "PCA projection onto 1000G",
  "pcs": [0.0421, 0.0331, -0.0072, 0.0033, -0.0011],
  "closest_population": "EAS",
  "distances": {"EAS": 0.0421, "SAS": 0.0689, "EUR": 0.0741, ...},
  "confidence": "high",
  ...
}
```

The cached `pca.vcf.gz` is reused for `admixture`, `roh`, `neanderthal`
in the same session, so the subsequent tests run in seconds.

## 9.5 Example E — Reference stats recomputation after a PGS update

Scenario: PGS Catalog updates PGS000016 with corrected weights. We
re-ingest, then rebuild ref-stats:

```bash
# 1. Force re-ingest (canonical scoring files refreshed)
python -c "from pipeline.ingest_pgs import ingest_pgs; ingest_pgs('PGS000016', force=True)"

# 2. The new scoring file has a different variant_ids_sha256
#    → existing ref-stats fail the contract → loader returns method='incompatible_ref_stats'
#    → percentile suppressed in any future read until ref-stats are rebuilt.

# 3. Rebuild ref-stats for all 6 populations
python scripts/recompute_ref_stats.py PGS000016 --pop ALL --apply

# 4. Output files
ls /data/pgs2/ref_panel_stats/PGS000016_*GRCh38*
# → new: PGS000016_{EUR,EAS,AFR,SAS,AMR,MIX}_GRCh38_n6648373_plink2-nomi_sha-<new>.json
#    old: PGS000016_{EUR,EAS,...}_GRCh38.stale-bias-20260514.json

# 5. Live overlay
#    Any historical report for PGS000016 with a stored raw_score will
#    now recompute its percentile against the new μ/σ. The UI will
#    surface the diff under percentile_at_scoring vs percentile.

# 6. Self-test
python scripts/ref_stats_selftest.py --pgs PGS000016 --n 100 --json
# → PASS expected: σ_obs / σ_cached ∈ [0.7, 1.4]
```

## 9.6 Example F — Cohort sanity 🚩 investigation

A small batch flags PGS001229:

```
🚩  PGS001229  n=7  >80%ile=86%  <50%ile=0%  KS p=0.000
```

Reviewer steps:

```bash
# 1. Check the PGS in question
ls /data/pgs_cache/PGS001229/
cat /data/pgs_cache/PGS001229/metadata.json   # trait, original build
cat /data/pgs_cache/PGS001229/eligibility.json

# 2. Look at the live ref-stats
ls /data/pgs2/ref_panel_stats/PGS001229_*GRCh38*
python -c "import json; print(json.load(open('/data/pgs2/ref_panel_stats/PGS001229_EUR_GRCh38_*.json')))"

# 3. Re-run the self-test for just this PGS
python scripts/ref_stats_selftest.py --pgs PGS001229 --n 200 --json
# If PASS: the math is right; the cohort just doesn't look like 1000G
# If FAIL: rebuild ref-stats and re-run cohort check

# 4. Check the per-user variant_set_sha matches what's cached
python -c "from pipeline.scoring import _rs_variant_set_sha_from_catalog; \
           print(_rs_variant_set_sha_from_catalog('PGS001229'))"
# Compare to the variant_ids_sha256 in the stats file

# 5. Look at the n=7 sample percentiles directly
sqlite3 simple-genomics/pgs_pipeline.db \
  "select sample_id, raw_score, percentile, selected_ref, match_rate
     from sample_results where pgs_id='PGS001229' order by created_at desc"
```

In practice the 🚩 for PGS001229 turned out to be a panel-fit
issue: the trait has strong sex-specific effects and our sex-pooled
ref-stats over-estimate male tails. Documented; no pipeline change.
