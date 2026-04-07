---
name: ancestry-population-analysis
description: |
  Run ancestry and population structure analysis on WGS samples. Uses joint PCA
  with a WGS-based reference panel followed by Rye (MCMC-optimized NNLS) for
  ancestry proportions, plus optional ROH for bottleneck detection. Supports two
  reference panels: 1KG (continental-level) and HGDP+1kGP (adds Middle Eastern
  for Jewish/Levantine detection). Handles VCF, gVCF, BAM, and CRAM inputs.
---

# Ancestry & Population Analysis Pipeline

## Architecture

```
INPUT (VCF/gVCF/BAM/CRAM)
    │
    ├─→ [Step 1] Select reference panel (HGDP+1kGP > 1KG)
    ├─→ [Step 2] Extract variants → sample.{bed,bim,fam}
    ├─→ [Step 3] Intersect with reference + align alleles
    ├─→ [Step 4] Merge with reference + PCA (plink2 --pca 20)
    ├─→ [Step 5] Rye (MCMC-optimized NNLS on eigenvec/eigenval)
    ├─→ [Step 6] ROH (VCF/gVCF only — skip for BAM)
    └─→ [Step 7] Interpret + report
```

Runtime: ~5 min VCF, ~20 min BAM.

## Critical Rules (Read First)

**1. Only use WGS-based reference panels.**
Do NOT use array-based panels (Human Origins, Illumina arrays) with WGS samples.
WGS VCFs only contain variant sites — ref/ref positions are absent. Array panels
need genotypes at ALL positions (including ref/ref). Imputing missing as ref/ref
introduces reference-allele bias that makes every sample look African. This was
tested extensively and is unfixable.

**2. Joint PCA, not projection.**
For <100 query samples, merge with reference and run plink2 --pca. No FRAPOSA,
no cached .dat files that go stale, no allele-format bugs. The query sample's
influence on PCA axes is negligible among >3000 reference individuals.

**3. ROH requires full VCF.**
ROH needs dense contiguous genotypes from a proper variant caller (DeepVariant,
GATK). mpileup at scattered reference panel positions produces het-biased calls
that break ROH detection. Skip ROH for BAM-only input.

**4. BAM mpileup only calls variant sites.**
`bcftools mpileup | call | view -v snps` only outputs positions where the sample
has an ALT allele. It does NOT output ref/ref sites. This is fine for WGS-based
panels (you intersect on variant IDs and both datasets have the same types of
calls). It is fatal for array-based panels.

---

## Reference Panels

| Panel | Location | Groups | MID? | ASJ detection? |
|-------|----------|--------|------|----------------|
| **HGDP+1kGP** | `$ANCESTRY_REF/hgdp_1kg/ref_pruned` | 7+ (EUR, AFR, EAS, SAS, AMR, MID, OCE, FIN) | ✅ | EUR+MID pattern |
| **1KG Phase 3** | `$ANCESTRY_REF/1kg/ref_pruned` | 5 (EUR, AFR, EAS, SAS, AMR) | ❌ | ❌ |

**Do NOT include Human Origins or other array-based panels in auto-detection.**

```bash
ANCESTRY_REF="${ANCESTRY_REF:-/data/ancestry_reference}"

if [ -f "$ANCESTRY_REF/hgdp_1kg/ref_pruned.bed" ]; then
    REF_PANEL="$ANCESTRY_REF/hgdp_1kg/ref_pruned"
    POP2GROUP="$ANCESTRY_REF/hgdp_1kg/pop2group_7regions.txt"
    PANEL_NAME="HGDP+1kGP"
elif [ -f "$ANCESTRY_REF/1kg/ref_pruned.bed" ]; then
    REF_PANEL="$ANCESTRY_REF/1kg/ref_pruned"
    POP2GROUP="$ANCESTRY_REF/1kg/pop2group_5superpop.txt"
    PANEL_NAME="1KG Phase 3"
else
    echo "ERROR: No reference panel found" && exit 1
fi
```

---

## Step 2: Variant Extraction

### VCF / gVCF

```bash
TMPDIR=$(mktemp -d /tmp/ancestry_${SAMPLE_NAME}_XXXXXX)

bcftools norm -m-any "$SAMPLE_VCF" | \
  bcftools view --types snps -m2 -M2 | \
  bcftools annotate --set-id '%CHROM:%POS:%REF:%ALT' \
  -Oz -o "$TMPDIR/norm.vcf.gz"
tabix -p vcf "$TMPDIR/norm.vcf.gz"

plink2 --vcf "$TMPDIR/norm.vcf.gz" \
  --chr 1-22 --allow-extra-chr \
  --make-bed --out "$TMPDIR/sample"
```

### BAM / CRAM

```bash
REF_BIM="${REF_PANEL}.bim"

# Targets file: tab-separated CHR\tPOS. Use -T not -R.
awk -v OFS='\t' '{print $1, $4}' "$REF_BIM" > "$TMPDIR/targets.tsv"

bcftools mpileup -f "$REFERENCE_FASTA" -T "$TMPDIR/targets.tsv" \
  --min-MQ 20 --min-BQ 20 --max-depth 500 --threads 8 \
  "$INPUT_BAM" 2>"$TMPDIR/mpileup.log" | \
bcftools call -m --ploidy GRCh38 --threads 4 2>"$TMPDIR/call.log" | \
bcftools view --types snps -m2 -M2 | \
bcftools annotate --set-id '%CHROM:%POS:%REF:%ALT' | \
bcftools sort -Oz -o "$TMPDIR/called.vcf.gz"
tabix -p vcf "$TMPDIR/called.vcf.gz"

plink2 --vcf "$TMPDIR/called.vcf.gz" \
  --chr 1-22 --allow-extra-chr \
  --make-bed --out "$TMPDIR/sample"
```

**Checklist before proceeding:**
- Chromosome naming matches between sample and reference (`1` vs `chr1`)
- Sample .bim has variant IDs in `CHR:POS:REF:ALT` format
- plink2 completed without error (check for exit code 7 = chrX/unplaced contigs)

---

## Step 3: Intersect and Align

```bash
REF_BIM="${REF_PANEL}.bim"

comm -12 \
  <(awk '{print $2}' "$TMPDIR/sample.bim" | sort) \
  <(awk '{print $2}' "$REF_BIM" | sort) \
  > "$TMPDIR/overlap.txt"

N_OVERLAP=$(wc -l < "$TMPDIR/overlap.txt")
echo "Overlap: $N_OVERLAP"

# QC gate
if [ "$N_OVERLAP" -lt 50000 ]; then
    echo "FAIL: <50K overlap. Check genome build and chr naming."
    exit 1
fi

plink2 --bfile "$TMPDIR/sample" \
  --extract "$TMPDIR/overlap.txt" \
  --ref-allele force "$REF_BIM" 5 2 \
  --make-bed --out "$TMPDIR/sample_aligned"
```

**Expected overlap:**
- VCF against 1KG (~287K panel): 100-130K
- VCF against HGDP+1kGP (~150-200K panel): 80-120K
- BAM mpileup: same ranges (only variant sites emitted)

---

## Step 4: Merge + PCA

```bash
# Subset reference to overlapping variants
awk '{print $2}' "$TMPDIR/sample_aligned.bim" > "$TMPDIR/ov_ids.txt"
plink2 --bfile "$REF_PANEL" --extract "$TMPDIR/ov_ids.txt" \
  --make-bed --out "$TMPDIR/ref_ov"

# Merge
plink --bfile "$TMPDIR/ref_ov" \
  --bmerge "$TMPDIR/sample_aligned" \
  --make-bed --out "$TMPDIR/merged" --allow-no-sex

# Handle strand errors
if [ -f "$TMPDIR/merged-merge.missnp" ]; then
    plink2 --bfile "$TMPDIR/sample_aligned" \
      --exclude "$TMPDIR/merged-merge.missnp" \
      --make-bed --out "$TMPDIR/sample_flipped"
    plink --bfile "$TMPDIR/ref_ov" \
      --bmerge "$TMPDIR/sample_flipped" \
      --make-bed --out "$TMPDIR/merged" --allow-no-sex
fi

# QC: remove high-missingness samples/variants
plink2 --bfile "$TMPDIR/merged" \
  --mind 0.1 --geno 0.1 --maf 0.01 \
  --make-bed --out "$TMPDIR/merged_clean"

# PCA
plink2 --bfile "$TMPDIR/merged_clean" --pca 20 --out "$TMPDIR/pca"
```

If PCA fails with "NaN in GRM": the --mind 0.1 filter should have removed
problematic samples. If it persists, try `--pca approx` or reduce to `--pca 10`.

---

## Step 5: Rye

```bash
RYE="$ANCESTRY_REF/tools/rye/rye.R"

conda run -n genomics "$RYE" \
  --eigenvec="$TMPDIR/pca.eigenvec" \
  --eigenval="$TMPDIR/pca.eigenval" \
  --pop2group="$POP2GROUP" \
  --rounds=50 --iter=50 \
  --threads=$(nproc) \
  --pcs=10 \
  --out="$TMPDIR/rye_result"
```

### Parse Rye Output

```python
def parse_rye(rye_q_file, eigenvec_file, pop2group_file, sample_name):
    """Extract query sample's ancestry proportions from Rye .Q output."""
    # Get group names (ordered by first appearance in pop2group)
    groups = []
    seen = set()
    with open(pop2group_file) as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 2 and parts[1] not in seen:
                groups.append(parts[1])
                seen.add(parts[1])

    # Read .Q file — each row is one sample, columns are group proportions
    with open(rye_q_file) as f:
        q_lines = [l.strip() for l in f if l.strip() and l.strip()[0].isdigit()]

    # Sample is the last row (appended to reference during merge)
    values = [float(x) for x in q_lines[-1].split()]

    # Pad groups if needed
    while len(groups) < len(values):
        groups.append(f"Group_{len(groups)+1}")

    return {g: round(v, 4) for g, v in zip(groups, values) if v >= 0.005}
```

**If Rye is not installed**, fall back to the pipeline's built-in NNLS
(reads eigenvec directly, computes population centroids, runs scipy NNLS).
Rye is better (MCMC-optimized weights) but the built-in NNLS works.

---

## Step 6: ROH (VCF/gVCF Only)

```bash
# Use the FULL sample .bed (pre-intersection, dense variants)
# NOT the reference-intersected subset
if [ "$INPUT_TYPE" = "vcf" ] || [ "$INPUT_TYPE" = "gvcf" ]; then
    plink --bfile "$TMPDIR/sample" \
      --homozyg --homozyg-window-snp 50 --homozyg-snp 50 \
      --homozyg-kb 300 --homozyg-density 50 --homozyg-gap 1000 \
      --out "$TMPDIR/roh"
fi
```

**Skip for BAM/CRAM. Do not attempt.**

### ROH Interpretation

| Total ROH | Class B (1-5 Mb) | Signature | Populations |
|-----------|-----------------|-----------|-------------|
| < 30 Mb | < 10 Mb | Outbred | Most EUR, EAS, AFR |
| 40-80 Mb | 15-30 Mb | Mild bottleneck | Finnish |
| 50-100 Mb | 20-45 Mb | Strong bottleneck | **Ashkenazi Jewish**, some isolates |
| 80-150 Mb | 25-50 Mb | Extreme bottleneck | Amish |
| > 150 Mb | — (mostly >5 Mb) | Recent consanguinity | — |

---

## Step 7: Interpretation and Report

```python
GROUP_DISPLAY = {
    "European": "European", "Finnish": "Finnish",
    "EastAsian": "East Asian", "African": "African",
    "SouthAsian": "South Asian", "American": "Admixed American",
    "MiddleEastern": "Middle Eastern", "Oceanian": "Oceanian",
}

def interpret(proportions, roh, panel_name):
    primary = max(proportions, key=proportions.get)
    primary_pct = proportions[primary]
    is_admixed = primary_pct < 0.85
    flags = []

    # ── EUR+MID pattern (HGDP+1kGP panel) ──
    eur = proportions.get("European", 0) + proportions.get("Finnish", 0)
    mid = proportions.get("MiddleEastern", 0)

    if 0.30 < eur < 0.70 and mid > 0.25:
        flag = (
            f"EUR+MID pattern ({eur*100:.0f}% European + {mid*100:.0f}% "
            f"Middle Eastern) detected. Characteristic of Jewish ancestry "
            f"(Ashkenazi ~50/50, Sephardi ~40/60)."
        )
        if roh and roh.get("bottleneck_signal"):
            flag += (
                f" ROH confirms bottleneck ({roh['total_roh_mb']:.0f} Mb) "
                f"— strongly suggests Ashkenazi Jewish."
            )
        flags.append(flag)

    # ── Ghost artifact warnings (1KG panel) ──
    if "1KG" in panel_name or "1kg" in panel_name:
        amr = proportions.get("American", 0)
        afr = proportions.get("African", 0)
        sas = proportions.get("SouthAsian", 0)
        if amr > 0.10 or (afr > 0.10 and eur > 0.40):
            flags.append(
                f"1KG panel lacks Middle Eastern populations. The "
                f"AMR ({amr*100:.0f}%), AFR ({afr*100:.0f}%), and SAS "
                f"({sas*100:.0f}%) components may be artifacts from "
                f"missing MID reference. Upgrade to HGDP+1kGP panel "
                f"for accurate decomposition."
            )

    return {
        "proportions": proportions,
        "primary": primary,
        "primary_display": GROUP_DISPLAY.get(primary, primary),
        "primary_pct": primary_pct,
        "is_admixed": is_admixed,
        "flags": flags,
    }
```

### Report Template

```markdown
# Ancestry Report: {sample_name}

**Input**: {input_type}
**Panel**: {panel_name} ({n_overlap:,} variants)
**Method**: Joint PCA + Rye (MCMC-optimized NNLS)

## Determination

**{primary_display}** ({primary_pct}%)
{ADMIXED if is_admixed}

## Composition

{for each group: bar chart}

## Notes

{flags}

## ROH

{if available: total, class breakdown, bottleneck signal}
{if BAM: "Skipped — requires full VCF"}

## Limitations

- {panel-specific limitations}
- {BAM-specific caveats if applicable}
- Proportions reflect genetic similarity to reference populations,
  not self-identified ethnicity.
```

---

## Execution Checklist

```
 1. Detect input type (vcf/gvcf/bam/cram)
 2. Select best WGS-based panel (HGDP+1kGP > 1KG)
 3. Find reference FASTA (BAM/CRAM only)
 4. Create temp dir
 5. Extract variants:
      VCF: bcftools norm | view | annotate → plink2 --chr 1-22
      BAM: bcftools mpileup -T targets | call | view -v snps | sort → plink2
 6. Intersect: comm -12 on sorted variant IDs
 7. QC: fail if <50K
 8. Align: plink2 --ref-allele force
 9. Subset ref to overlap: plink2 --extract
10. Merge: plink --bmerge (handle --flip if strand error)
11. QC merged: plink2 --mind 0.1 --geno 0.1 --maf 0.01
12. PCA: plink2 --pca 20
13. Rye: rye.R --eigenvec --eigenval --pop2group
14. Parse Rye .Q: last row = sample
15. ROH (VCF/gVCF only): plink --homozyg on full sample
16. Interpret: check EUR+MID, ghost artifacts
17. Write JSON + markdown report
18. Save, cleanup
```

---

## Error Reference

| Error | Cause | Fix |
|-------|-------|-----|
| plink2 exit 7 | chrX/unplaced contigs | `--chr 1-22 --allow-extra-chr` |
| plink --bmerge strand error | REF/ALT flipped | `--flip missnp`, retry |
| Rye "Error in solve.default" | Group has <5 samples | Check pop2group mapping |
| Rye wrong column count | FID in .fam doesn't match Pop in pop2group | `comm -23 <(cut -f1 fam) <(cut -f1 p2g)` |
| PCA NaN | High-missingness samples | `--mind 0.1` before PCA |
| 0 overlap | Build mismatch or chr naming | Compare `head -3` of both .bim files |
| ROH = 0 from BAM | Expected | Skip ROH for BAM input |
| PCA >10 min | Too many variants | `plink2 --thin-count 150000` before PCA |
| AMR/AFR ghost with 1KG | Missing MID reference | Upgrade to HGDP+1kGP |
| African bias with HO panel | Array vs WGS technology mismatch | Do not use HO with WGS data |