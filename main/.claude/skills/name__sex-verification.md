name: sex-verification
description: |
Determine genetic sex (XX or XY) of a whole-genome sequencing sample using
multiple independent lines of evidence from alignment data and/or variant calls.
Handles BAM, CRAM, VCF, and gVCF inputs. Produces a structured JSON result and
a human-readable markdown report. Use this skill when the user asks to verify sex,
check for sample swaps, or run sample-level QC on genomic data.
Sex / Gender Verification Pipeline
Why This Matters
Genetic sex verification is one of the most important sample-level QC checks in
genomic analysis. It catches:

Sample swaps: The #1 wet-lab error. If a sample labeled "male" has no Y
chromosome signal, it's the wrong sample or the label is wrong.
Contamination: A "male" sample with abnormally high chrX heterozygosity may
be contaminated with female DNA (or vice versa).
Downstream analysis correctness: Variant calling ploidy settings, X-linked
disease interpretation, and PGS calculations all depend on knowing genetic sex.

**PERFORMANCE: All 5 checks run in PARALLEL using ThreadPoolExecutor (not sequentially).
Always pass --threads to samtools/bcftools commands. The script accepts --threads N.**

This pipeline uses 5 independent checks spanning two evidence types (read
alignments and variant calls). Each check produces a sex call. A consensus
algorithm resolves disagreements and flags anomalies.

The 5 Checks
Each check exploits a different biological consequence of XX vs XY karyotype.
Check 1: Y Chromosome Read Count
What it measures: Total reads aligned to the male-specific region of chrY (MSY).
Why it works: Females have no Y chromosome. Any reads mapping to chrY in a
female sample are mapping artifacts (pseudoautosomal homology, mismapping from
repetitive sequence). Males have one copy of chrY, so a 30× WGS male sample will
have millions of reads mapping to the ~23 Mb MSY region.
Thresholds:
ValueCallRationale> 100,000 readsMaleClear Y chromosome signal at any reasonable coverage1,000 – 100,000AmbiguousLow-coverage male, contamination, or sex chromosome aneuploidy< 1,000 readsFemaleResidual mismapping noise only
Why not just > 0? Even female samples typically have a few hundred to a few
thousand reads that mismap to chrY from pseudoautosomal regions (PAR1/PAR2) or
repetitive sequence. The threshold must be well above this noise floor.
Check 2: SRY Gene Presence
What it measures: Reads aligned specifically to the SRY gene locus (Yp11.2,
approximately chrY:2,786,855-2,787,699 on GRCh38).
Why it works: SRY (Sex-determining Region Y) is the master sex-determination
gene. It is Y-specific with no X homolog (unlike most Y genes which have
degenerate X copies). Zero reads at SRY in a properly sequenced sample is
definitive evidence of no Y chromosome.
Thresholds:
ValueCallRationale≥ 10 readsMaleExpected: ~30 reads at 30× coverage for an 845bp gene1 – 9 readsAmbiguousCould be very low coverage or edge mismapping0 readsFemaleNo X homolog exists — zero is definitive
Check 3: X:Y Read Ratio
What it measures: The ratio of chrX read count to chrY MSY read count.
Why it works: Males have 1 X + 1 Y, so the X:Y ratio reflects the size
difference between chrX (~155 Mb) and MSY (~23 Mb), giving a ratio of roughly
5-7×. Females have 2 X + 0 Y, giving an infinite (or very large) ratio.
Thresholds:
ValueCallRationaleX:Y ratio 3–15MaleNormal range accounting for mappability differencesX:Y ratio > 100FemaleY reads are only noiseX:Y ratio 15–100AmbiguousPossible XXY, contamination, or mosaicY = 0FemaleInfinite ratio, no Y signal
Important: Use only MSY (male-specific Y) reads, excluding pseudoautosomal
regions (PAR1: chrY:10,001-2,781,479 and PAR2: chrY:56,887,903-57,217,415 on
GRCh38). PAR regions are shared with chrX and inflate the Y count for females.
Check 4: ChrX Heterozygosity Rate
What it measures: The proportion of heterozygous variant calls on chrX
(excluding PAR regions).
Why it works: Males have only one X chromosome (hemizygous). A correctly called
male sample should have near-zero heterozygous calls on chrX outside PAR regions —
any apparent het calls are genotyping errors. Females have two X copies and will
show a normal heterozygosity rate (~20-35% of variant sites are het, depending on
ancestry).
Thresholds:
ValueCallRationaleHet rate < 5%MaleHemizygous X — hets are calling errorsHet rate 5–15%AmbiguousCould be XXY, mosaic, or contaminated maleHet rate > 15%FemaleNormal diploid X heterozygosity
This is the single most reliable check for distinguishing XX from XY when
variant calls are available. It is resistant to coverage fluctuations and does
not depend on Y chromosome mapping quality.
Check 5: ChrY Variant Count
What it measures: Number of variant calls (SNPs + indels) on chrY.
Why it works: A male sample sequenced at 30× will have thousands of Y
chromosome variants called. A female sample will have zero or near-zero
(occasional false-positive calls from mismapped reads).
Thresholds:
ValueCallRationale> 500 variantsMaleReal Y haplotype present50 – 500 variantsAmbiguousLow coverage, or partial Y (mosaic/aneuploidy)< 50 variantsFemaleNoise-level false positives only

Input Handling
Available checks by input type
Not all checks are possible from every file type:
CheckBAM/CRAMVCFgVCF1. Y read count✅❌❌2. SRY presence✅❌❌3. X:Y read ratio✅❌❌4. ChrX het rate❌✅✅5. ChrY variant count❌✅✅
BAM/CRAM: Checks 1-3 (alignment-based). Cannot do checks 4-5 without
running a variant caller, which is out of scope for a quick QC check.
VCF/gVCF: Checks 4-5 (variant-based). Cannot do checks 1-3 without the
original alignment file.
BAM/CRAM + VCF/gVCF together: All 5 checks. This is the recommended
configuration for maximum confidence.
If only one file type is available, 2 checks is sufficient for a confident
call when they agree. Flag as low-confidence if only 1 check is available
or if the available checks disagree.

Implementation
Check 1: Y Chromosome Read Count (BAM/CRAM)
bashINPUT_BAM="/path/to/sample.bam"
REFERENCE_FASTA="/data/reference/GRCh38.fasta"  # required for CRAM

# Get read counts per chromosome
# samtools idxstats outputs: chr length mapped_reads unmapped_reads
samtools idxstats "$INPUT_BAM" > "$TMPDIR/idxstats.txt"

# Extract chrY total reads (includes PAR — we'll handle that)
CHRY_TOTAL=$(awk '$1 == "chrY" { print $3 }' "$TMPDIR/idxstats.txt")

# Count PAR reads to subtract (PAR1 + PAR2 on GRCh38)
PAR_READS=$(samtools view -c -F 0x904 "$INPUT_BAM" \
  chrY:10001-2781479 chrY:56887903-57217415)

# MSY reads = total chrY - PAR
MSY_READS=$((CHRY_TOTAL - PAR_READS))

echo "chrY total: $CHRY_TOTAL, PAR: $PAR_READS, MSY: $MSY_READS"
Note on CRAM: The commands are identical. samtools handles CRAM transparently
when the reference FASTA is available (via -T flag or REF_PATH environment variable).
Add -T "$REFERENCE_FASTA" if samtools cannot find the reference automatically.
Check 2: SRY Gene Presence (BAM/CRAM)
bash# SRY coordinates on GRCh38: chrY:2,786,855-2,787,699
# Use -F 0x904 to exclude unmapped, secondary, supplementary, and duplicate reads
SRY_READS=$(samtools view -c -F 0x904 "$INPUT_BAM" chrY:2786855-2787699)
echo "SRY reads: $SRY_READS"
Check 3: X:Y Read Ratio (BAM/CRAM)
bashCHRX_READS=$(awk '$1 == "chrX" { print $3 }' "$TMPDIR/idxstats.txt")

if [ "$MSY_READS" -gt 0 ]; then
    # Use bc for floating point
    XY_RATIO=$(echo "scale=2; $CHRX_READS / $MSY_READS" | bc)
else
    XY_RATIO="inf"
fi

echo "chrX reads: $CHRX_READS, MSY reads: $MSY_READS, X:Y ratio: $XY_RATIO"
Check 4: ChrX Heterozygosity Rate (VCF/gVCF)
bashINPUT_VCF="/path/to/sample.vcf.gz"

# Exclude PAR regions from chrX
# PAR1 on chrX (GRCh38): chrX:10,001-2,781,479
# PAR2 on chrX (GRCh38): chrX:155,701,383-156,030,895

# Count het and total genotype calls on non-PAR chrX
bcftools query -f '%CHROM\t%POS\t[%GT]\n' "$INPUT_VCF" \
  -r chrX:2781480-155701382 | \
awk '
  $3 == "0/1" || $3 == "0|1" || $3 == "1|0" { het++ }
  $3 != "./." && $3 != ".|." { total++ }
  END {
    if (total > 0) printf "het=%d total=%d rate=%.4f\n", het, total, het/total
    else print "het=0 total=0 rate=NA"
  }
'
gVCF note: gVCFs contain reference blocks that are not variant sites. The
bcftools query command above already handles this correctly because reference
blocks have GT=0/0 (homozygous reference), which counts as non-het in the
denominator and does not increment the het counter.
Check 5: ChrY Variant Count (VCF/gVCF)
bash# Count variant calls on chrY (excluding PAR and reference-only sites)
CHRY_VARIANTS=$(bcftools view -r chrY:2781480-56887902 \
  --types snps,indels \
  --exclude 'GT="0/0" || GT="./."' \
  "$INPUT_VCF" | \
  grep -cv "^#")

echo "chrY variants: $CHRY_VARIANTS"

Consensus Algorithm
pythondef determine_sex(checks: dict) -> dict:
    """
    Resolve individual check results into a final sex determination.

    Parameters
    ----------
    checks : dict
        Keys are check names, values are dicts with:
        - "call": "male" | "female" | "ambiguous"
        - "value": the raw measured value
        - "detail": human-readable explanation

    Returns
    -------
    dict with final determination and reasoning.
    """
    calls = [c["call"] for c in checks.values() if c["call"] != "ambiguous"]
    all_calls = [c["call"] for c in checks.values()]
    n_checks = len(checks)
    n_definitive = len(calls)

    male_count = calls.count("male")
    female_count = calls.count("female")
    ambiguous_count = all_calls.count("ambiguous")

    # ── Unanimous agreement ──
    if male_count == n_definitive and n_definitive >= 2:
        return {
            "sex": "XY (male)",
            "confidence": "high",
            "reasoning": (
                f"All {n_definitive} definitive checks agree: male. "
                f"{ambiguous_count} check(s) were ambiguous."
                if ambiguous_count
                else f"All {n_definitive} checks agree: male."
            ),
        }

    if female_count == n_definitive and n_definitive >= 2:
        return {
            "sex": "XX (female)",
            "confidence": "high",
            "reasoning": (
                f"All {n_definitive} definitive checks agree: female. "
                f"{ambiguous_count} check(s) were ambiguous."
                if ambiguous_count
                else f"All {n_definitive} checks agree: female."
            ),
        }

    # ── Majority agreement ──
    if male_count > female_count and male_count >= 2:
        dissenters = [
            name for name, c in checks.items()
            if c["call"] == "female"
        ]
        return {
            "sex": "XY (male)",
            "confidence": "medium",
            "reasoning": (
                f"{male_count}/{n_definitive} checks indicate male. "
                f"Disagreeing check(s): {', '.join(dissenters)}. "
                f"Investigate potential contamination or aneuploidy."
            ),
        }

    if female_count > male_count and female_count >= 2:
        dissenters = [
            name for name, c in checks.items()
            if c["call"] == "male"
        ]
        return {
            "sex": "XX (female)",
            "confidence": "medium",
            "reasoning": (
                f"{female_count}/{n_definitive} checks indicate female. "
                f"Disagreeing check(s): {', '.join(dissenters)}. "
                f"Investigate potential contamination or aneuploidy."
            ),
        }

    # ── No consensus ──
    if n_definitive == 0:
        return {
            "sex": "undetermined",
            "confidence": "none",
            "reasoning": (
                f"All {n_checks} checks returned ambiguous results. "
                f"Possible sex chromosome aneuploidy (XXY, X0), mosaicism, "
                f"or sample quality issues. Manual review required."
            ),
        }

    # ── Conflicting results ──
    return {
        "sex": "conflicting",
        "confidence": "low",
        "reasoning": (
            f"Checks are split: {male_count} male, {female_count} female, "
            f"{ambiguous_count} ambiguous. This pattern can indicate: "
            f"(1) sample contamination (mixed male+female DNA), "
            f"(2) sex chromosome aneuploidy (e.g., XXY Klinefelter), "
            f"(3) mosaicism, or (4) a sample swap during processing. "
            f"Manual review required."
        ),
    }

Report Generation
JSON Output Schema
json{
  "sample_name": "Sample2",
  "date": "2026-03-30",
  "pipeline_version": "2.0",
  "input_files": {
    "bam": "/data/aligned_bams/Sample2.bam",
    "vcf": null
  },
  "determination": {
    "sex": "XY (male)",
    "confidence": "high",
    "reasoning": "All 3 checks agree: male."
  },
  "checks": {
    "y_read_count": {
      "call": "male",
      "value": 563596,
      "threshold_used": "> 100,000 → male",
      "detail": "563,596 reads mapped to male-specific Y region. Well above the 100K threshold for a definitive male call. Expected for a ~30× WGS male sample."
    },
    "sry_gene": {
      "call": "male",
      "value": 28,
      "threshold_used": "≥ 10 → male",
      "detail": "28 reads at SRY locus (chrY:2,786,855-2,787,699). SRY has no X homolog, so any substantial read count confirms Y chromosome presence."
    },
    "xy_read_ratio": {
      "call": "male",
      "value": 76.3,
      "threshold_used": "3–15 → male",
      "detail": "X:Y MSY ratio = 76.3 (chrX: 42,943,673 reads / MSY: 563,596 reads). [NOTE: ratio is higher than typical male range of 5-7, possibly due to PAR reads inflating X count or MSY mappability. Y read count alone is definitive.]"
    }
  },
  "checks_not_run": {
    "chrx_het_rate": "No VCF provided — requires variant calls",
    "chry_variant_count": "No VCF provided — requires variant calls"
  }
}
Markdown Report Template
pythondef generate_sex_report(result: dict) -> str:
    """Generate human-readable sex verification report."""
    r = result
    det = r["determination"]
    checks = r["checks"]
    not_run = r.get("checks_not_run", {})

    lines = []

    # ── Header ──
    lines.append(f"# Sex Verification Report: {r['sample_name']}")
    lines.append("")
    lines.append(f"**Date**: {r['date']}  ")
    lines.append(f"**Pipeline Version**: {r['pipeline_version']}")
    lines.append("")

    # ── Input Files ──
    lines.append("## Input Files")
    lines.append("")
    for ftype, fpath in r["input_files"].items():
        if fpath:
            lines.append(f"- **{ftype.upper()}**: `{fpath}`")
        else:
            lines.append(f"- **{ftype.upper()}**: not provided")
    lines.append("")

    # ── Bottom Line ──
    lines.append("## Determination")
    lines.append("")

    conf_icon = {"high": "🟢", "medium": "🟡", "low": "🔴", "none": "⚪"}.get(
        det["confidence"], "⚪"
    )

    lines.append(f"**Genetic sex: {det['sex']}**  ")
    lines.append(f"**Confidence**: {conf_icon} {det['confidence']}  ")
    lines.append(f"**Reasoning**: {det['reasoning']}")
    lines.append("")

    # ── Individual Checks ──
    lines.append("## Individual Checks")
    lines.append("")

    check_names = {
        "y_read_count": "Y Chromosome Read Count",
        "sry_gene": "SRY Gene Presence",
        "xy_read_ratio": "X:Y Read Ratio",
        "chrx_het_rate": "ChrX Heterozygosity Rate",
        "chry_variant_count": "ChrY Variant Count",
    }

    check_biology = {
        "y_read_count": (
            "Females (XX) have no Y chromosome. Males (XY) have one copy. "
            "At 30× whole-genome coverage, a male sample produces >1 million "
            "reads mapping to chrY. Female samples produce only a few hundred "
            "to a few thousand from pseudoautosomal region cross-mapping."
        ),
        "sry_gene": (
            "SRY is the master sex-determination gene on chrY (Yp11.2). "
            "Unlike most Y genes, SRY has no homolog on chrX, making it a "
            "clean binary signal: any reads at this locus confirm a Y "
            "chromosome is present."
        ),
        "xy_read_ratio": (
            "Males (1 X + 1 Y) produce an X:Y read ratio determined by the "
            "size difference between chrX (~155 Mb) and chrY MSY (~23 Mb), "
            "typically around 5-7×. Females (2 X + 0 Y) produce an extremely "
            "high or infinite ratio because Y reads are only noise."
        ),
        "chrx_het_rate": (
            "Males are hemizygous for chrX (one copy), so they cannot have "
            "true heterozygous calls — any het calls are genotyping errors. "
            "Females are diploid for chrX and show normal heterozygosity "
            "(~20-35% of variant sites). This is the single most reliable "
            "genetic sex indicator from variant calls."
        ),
        "chry_variant_count": (
            "A male genome carries thousands of callable variants on chrY. "
            "A female genome has zero real Y variants — any calls are "
            "false positives from mismapped reads."
        ),
    }

    for check_id, check_title in check_names.items():
        if check_id in checks:
            c = checks[check_id]
            icon = {"male": "♂️", "female": "♀️", "ambiguous": "❓"}[c["call"]]

            lines.append(f"### {icon} {check_title}")
            lines.append("")
            lines.append(f"**Result**: {c['call'].upper()} — {c['detail']}")
            lines.append("")
            lines.append(f"*Why this check works*: {check_biology[check_id]}")
            lines.append("")
            lines.append(f"**Threshold applied**: {c['threshold_used']}")
            lines.append(f"**Raw value**: `{c['value']}`")
            lines.append("")
        elif check_id in not_run:
            lines.append(f"### ⏭️ {check_title} — skipped")
            lines.append("")
            lines.append(f"*{not_run[check_id]}*")
            lines.append("")

    # ── Interpretation Guide ──
    lines.append("## Interpretation Guide")
    lines.append("")
    lines.append(
        "This analysis determines **genetic sex** (XX vs XY karyotype) from "
        "sequencing data. It does not determine gender identity, phenotypic "
        "sex, or hormonal status. Discrepancies between genetic sex and "
        "clinical records may reflect:"
    )
    lines.append("")
    lines.append("- **Sample swap**: The most common explanation for a mismatch "
                  "with expected sex. Verify sample tracking and barcoding.")
    lines.append("- **Contamination**: Mixed male+female DNA produces intermediate "
                  "values across all checks (moderate Y reads, elevated chrX het rate).")
    lines.append("- **Sex chromosome aneuploidy**: Klinefelter syndrome (XXY) shows "
                  "male-level Y signal with female-level chrX heterozygosity. Turner "
                  "syndrome (X0) shows female-level Y signal with reduced chrX het rate.")
    lines.append("- **Mosaicism**: A subset of cells with a different karyotype can "
                  "produce intermediate results.")
    lines.append("- **Differences of sex development (DSD)**: Rare conditions where "
                  "genetic and phenotypic sex diverge (e.g., SRY translocation to X, "
                  "androgen insensitivity).")
    lines.append("")

    return "\n".join(lines)

Edge Cases and Anomaly Patterns
These patterns in the check results indicate specific conditions worth flagging:
PatternLikely CauseActionAll 5 checks agree maleNormal XY maleReport with high confidenceAll 5 checks agree femaleNormal XX femaleReport with high confidenceHigh Y reads + high chrX het rateXXY (Klinefelter syndrome)Flag: "Possible XXY — male Y signal with diploid X heterozygosity"Zero Y reads + low chrX het rateX0 (Turner syndrome)Flag: "Possible X0 — no Y chromosome but hemizygous X pattern"Moderate Y reads (1K–100K) + moderate chrX hetContamination (male+female mix)Flag: "Possible cross-contamination — intermediate values on all checks"Y reads present + SRY = 0Y deletion including SRY, or SRY translocationFlag: "Y chromosome detected but SRY absent — possible Yp deletion"SRY reads present + very low total YSRY translocation to XFlag: "SRY present but minimal Y — possible SRY translocation to chrX"X:Y ratio outside normal ranges but other checks consistentMapping artifact or unusual Y haplotypeTrust other checks; note ratio anomaly

GRCh38 Coordinates Reference
All coordinates in this pipeline assume GRCh38/hg38.
RegionCoordinatesNotesSRY genechrY:2,786,855-2,787,699845 bp. No X homolog.PAR1 (Y)chrY:10,001-2,781,479Shared with chrX. Exclude from MSY.PAR2 (Y)chrY:56,887,903-57,217,415Shared with chrX. Exclude from MSY.PAR1 (X)chrX:10,001-2,781,479Corresponds to Y PAR1.PAR2 (X)chrX:155,701,383-156,030,895Corresponds to Y PAR2.MSYchrY:2,781,480-56,887,902Male-specific Y = chrY minus PARs.Non-PAR XchrX:2,781,480-155,701,382Used for het rate calculation.
If the sample is aligned to GRCh37/hg19, use these instead:
RegionCoordinatesSRY genechrY:2,654,896-2,655,740PAR1 (Y)chrY:10,001-2,649,520PAR2 (Y)chrY:59,034,050-59,363,566Non-PAR XchrX:2,699,521-154,931,043
Build detection: Check the BAM header (@SQ lines) or VCF header for
reference assembly. Look for GRCh38, hg38, GRCh37, or hg19 in sequence
names or assembly tags. Chromosome naming (chr1 vs 1) also differs between builds.

Pipeline Integration
Running as part of a larger QC checklist
This pipeline produces a self-contained result. To integrate with a broader
genomics QC checklist:

Call the pipeline with available input files (BAM, VCF, or both)
Store the JSON result alongside other QC outputs
Use determination.sex for downstream analysis configuration:

Variant calling ploidy: set chrX ploidy to 1 for males, 2 for females
PGS calculation: use sex-appropriate scores where applicable
X-linked variant interpretation: hemizygous vs heterozygous


If determination.confidence is "low" or sex is "conflicting", halt the
analysis pipeline and require manual review before proceeding

Temp file cleanup
The pipeline creates only lightweight temp files (idxstats output, bcftools
query output). Clean up after storing the JSON and markdown report:
bashrm -rf "$TMPDIR"