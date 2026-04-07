name: polygenic-score-calculation
description: |
Calculate Polygenic Scores (PGS) for WGS/WES samples using scoring files from the
PGS Catalog or custom sources. Handles genome build liftover, variant matching,
score calculation via plink2, ancestry-aware normalization, and report generation.
Supports batch runs across multiple PGS IDs for a single sample. Use this skill
when the user provides a list of PGS IDs (e.g., PGS000335, PGS000004) and a sample
file (VCF, gVCF, BAM, CRAM, or PLINK binary), and wants risk scores calculated.
Polygenic Score (PGS) Calculation Pipeline
Overview
A Polygenic Score aggregates the effects of many genetic variants into a single
number that estimates genetic predisposition to a trait or disease. This pipeline:

Downloads scoring files from the PGS Catalog by PGS ID
Resolves genome build mismatches (GRCh37 ↔ GRCh38)
Matches scoring file variants against the sample's genotypes
Calculates raw scores using plink2
Normalizes scores using ancestry-matched reference distributions
Produces a structured report with percentiles and clinical context


Key Concepts
What's in a PGS scoring file
Each scoring file contains rows of variants with effect weights:
chr_name  chr_position  effect_allele  other_allele  effect_weight
1         1005806       C              T             -0.0234
1         1058562       A              G              0.0118
...
The score for an individual = Σ (effect_weight × dosage of effect_allele).
Dosage is 0, 1, or 2 copies of the effect allele.
Why ancestry matters
PGS distributions differ by ancestry because allele frequencies differ. A score
at the 90th percentile in a European reference may be at the 50th percentile in
an East Asian reference. Without ancestry adjustment, PGS are unreliable for
non-European individuals, and many published PGS were developed in EUR cohorts.
Variant match rate
A PGS is only as good as the fraction of its variants present in the sample.
If a 1M-variant PGS only matches 300K variants, the score is degraded.
Match rateInterpretation> 90%Excellent — score is reliable75-90%Good — minor degradation50-75%Moderate — interpret with caution< 50%Poor — score is unreliable, flag prominently

Prerequisites
Software
ToolPurposeInstallplink2Score calculation, format conversionconda / binarybcftoolsVCF manipulationcondapython3Orchestration, reportingcondapgscatalog-downloadDownload scoring files from PGS Catalogpip install pgscatalog-utils
Reference Data
FileLocationPurposeAncestry PCA scoresFrom ancestry pipelineAncestry-aware normalization1KG reference PGSComputed per PGSReference distribution for percentilesChain filesDownloaded onceGRCh37↔GRCh38 liftover

Pipeline Steps
Step 1: Download Scoring Files
bashPGS_IDS="PGS000335,PGS000004,PGS000001,PGS000005,PGS000006"
TARGET_BUILD="GRCh38"  # Must match sample genome build
SCORE_DIR="$TMPDIR/scores"
mkdir -p "$SCORE_DIR"

# Download from PGS Catalog, harmonized to target build
# pgscatalog-download handles build harmonization automatically
download_scorefiles \
  --pgs_id $PGS_IDS \
  --target_build $TARGET_BUILD \
  --output_dir "$SCORE_DIR"

# Output: one .txt.gz file per PGS ID
# Format: harmonized scoring file with chr_name, chr_position,
#         effect_allele, other_allele, effect_weight
ls "$SCORE_DIR"/*.txt.gz
If pgscatalog-download is not available, use the API directly:
bashfor PGS_ID in PGS000335 PGS000004 PGS000001; do
    # The PGS Catalog provides harmonized files via direct URL
    wget -O "$SCORE_DIR/${PGS_ID}_hmPOS_GRCh38.txt.gz" \
      "https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/${PGS_ID}/ScoringFiles/Harmonized/${PGS_ID}_hmPOS_GRCh38.txt.gz"
done
Step 2: Parse Scoring File Metadata
Before calculating, extract metadata from each scoring file header:
pythonimport gzip, json

def parse_pgs_header(filepath: str) -> dict:
    """Extract metadata from PGS Catalog scoring file header."""
    meta = {}
    with gzip.open(filepath, 'rt') as f:
        for line in f:
            if not line.startswith('#'):
                break
            line = line.strip('#').strip()
            if '=' in line:
                key, val = line.split('=', 1)
                meta[key.strip()] = val.strip()
    return {
        "pgs_id": meta.get("pgs_id", "unknown"),
        "pgs_name": meta.get("pgs_name", ""),
        "trait_reported": meta.get("trait_reported", ""),
        "trait_efo": meta.get("trait_efo", ""),
        "genome_build": meta.get("genome_build", meta.get("HmPOS_build", "")),
        "variants_number": int(meta.get("variants_number", 0)),
        "weight_type": meta.get("weight_type", "beta"),  # beta or OR
        "pgp_id": meta.get("pgp_id", ""),
        "citation": meta.get("citation", ""),
    }
Step 3: Prepare Sample Genotypes
The sample must be in a format plink2 can read. If starting from VCF:
bash# From VCF/gVCF (already called variants)
plink2 --vcf "$SAMPLE_VCF" \
  --double-id \
  --make-pgen \
  --out "$TMPDIR/sample"

# From BAM/CRAM: must call variants first (use DeepVariant or GATK)
# BAM alone is NOT sufficient for PGS — you need called genotypes.
# If only BAM is available, either:
#   a) Run the full variant calling pipeline first, or
#   b) Call only at PGS variant positions (faster but less reliable)
#
# Option b (fast, targeted calling):
# Extract all positions from all scoring files
cat "$SCORE_DIR"/*.txt.gz | zcat | \
  grep -v "^#" | awk 'NR>1 {print $1":"$2"-"$2}' | \
  sort -u > "$TMPDIR/pgs_regions.txt"
#
# Then mpileup + call at those positions:
bcftools mpileup -f "$REFERENCE_FASTA" \
  -R "$TMPDIR/pgs_regions.txt" \
  --min-MQ 20 --min-BQ 20 --threads 8 "$INPUT_BAM" | \
bcftools call -m --ploidy GRCh38 | \
bcftools view --types snps -m2 -M2 -Oz -o "$TMPDIR/called.vcf.gz"
tabix -p vcf "$TMPDIR/called.vcf.gz"
plink2 --vcf "$TMPDIR/called.vcf.gz" --double-id --make-pgen --out "$TMPDIR/sample"
Important note on BAM/CRAM input: PGS calculated from mpileup-called
variants at targeted positions are less reliable than PGS from a full
variant calling pipeline (DeepVariant, GATK). The genotype quality at
individual sites may be lower. Flag this in the report.
Step 4: Variant Matching
For each PGS, identify which scoring file variants are present in the sample:
bashPGS_FILE="$SCORE_DIR/PGS000335_hmPOS_GRCh38.txt.gz"
PGS_ID="PGS000335"

# Convert scoring file to plink2 --score format
# plink2 expects: variant_id, effect_allele, effect_weight
# We need to create variant IDs that match the sample's .pvar

zcat "$PGS_FILE" | grep -v "^#" | awk 'NR>1 {
    # Create CHR:POS:REF:ALT style ID
    printf "%s:%s:%s:%s\t%s\t%s\n", $1, $2, $4, $3, $3, $5
}' > "$TMPDIR/${PGS_ID}_score_input.txt"

# Count total variants in scoring file
TOTAL_VARIANTS=$(wc -l < "$TMPDIR/${PGS_ID}_score_input.txt")

# Check how many are in the sample
# First, list sample variant IDs
awk '{print $1":"$4":"$5":"$6}' "$TMPDIR/sample.pvar" 2>/dev/null | \
  sort > "$TMPDIR/sample_var_ids.txt"
# Or from bim:
awk '{print $1":"$4":"$6":"$5}' "$TMPDIR/sample.bim" 2>/dev/null | \
  sort >> "$TMPDIR/sample_var_ids.txt"

cut -f1 "$TMPDIR/${PGS_ID}_score_input.txt" | sort > "$TMPDIR/pgs_var_ids.txt"

MATCHED=$(comm -12 "$TMPDIR/sample_var_ids.txt" "$TMPDIR/pgs_var_ids.txt" | wc -l)
MATCH_RATE=$(echo "scale=4; $MATCHED / $TOTAL_VARIANTS" | bc)

echo "$PGS_ID: $MATCHED / $TOTAL_VARIANTS variants matched ($MATCH_RATE)"
Note: Variant ID matching is the most error-prone step. Common issues:
IssueSymptomFixchr prefix mismatch1 vs chr1Normalize both to same conventionAllele flipREF/ALT swappedplink2 handles this with --score-col-numsMultiallelic sitesMultiple ALT allelesSplit multiallelics firstIndelsDifferent representationsLeft-normalize with bcftools normrsID vs position IDID format mismatchMatch on CHR:POS:REF:ALT, not rsID
Step 5: Calculate Scores
bash# plink2 --score calculates: sum(dosage * weight) for each individual

plink2 --pfile "$TMPDIR/sample" \  # or --bfile for bed/bim/fam
  --score "$TMPDIR/${PGS_ID}_score_input.txt" \
    1       \  # column for variant ID
    2       \  # column for effect allele
    3       \  # column for effect weight
    cols=+scoresums \
  --out "$TMPDIR/${PGS_ID}_result"

# Output: ${PGS_ID}_result.sscore
# Columns: #FID, IID, ALLELE_CT, NAMED_ALLELE_DOSAGE_SUM, SCORE1_AVG, SCORE1_SUM
#
# SCORE1_SUM = the raw PGS (sum of dosage * weight)
# NAMED_ALLELE_DOSAGE_SUM = how many effect alleles were counted
**THREADING: ALWAYS pass --threads 16 to ALL plink2 commands. This machine has 32 CPUs.**
**Reference panel scoring MUST use --threads too — it's the slowest step.**
**For bcftools/samtools commands, always use --threads 16 or -@ 16.**

For multiple PGS at once (more efficient):
bash# Combine all scoring files into one multi-column file
# plink2 can calculate multiple scores in parallel from a single run

# Create combined scoring file with PGS IDs as score column names
python3 combine_scores.py "$SCORE_DIR"/*.txt.gz > "$TMPDIR/combined_scores.txt"

plink2 --pfile "$TMPDIR/sample" \
  --score "$TMPDIR/combined_scores.txt" \
    header-read \
    cols=+scoresums \
  --out "$TMPDIR/all_pgs_results"
Step 6: Ancestry-Aware Normalization
Raw PGS values are meaningless without a reference distribution. The standard
approach is to compute Z-scores relative to an ancestry-matched reference.
pythonimport numpy as np

def normalize_pgs(
    raw_score: float,
    ancestry_label: str,
    pgs_id: str,
    reference_scores_file: str,
) -> dict:
    """
    Normalize a raw PGS using an ancestry-matched reference distribution.

    The reference_scores_file contains PGS values calculated on the 1000 Genomes
    reference panel (or similar), with ancestry labels.

    Returns Z-score, percentile, and reference distribution stats.
    """
    import pandas as pd
    from scipy.stats import norm

    ref = pd.read_csv(reference_scores_file, sep='\t')

    # Map sample ancestry to reference group
    # Use the ancestry pipeline's determination
    ancestry_map = {
        "EUR": "EUR", "NFE": "EUR", "nfe": "EUR",
        "ASJ": "EUR",  # Normalize ASJ against EUR (closest large reference)
        "FIN": "EUR",  # Or FIN-specific if available
        "EAS": "EAS", "eas": "EAS",
        "AFR": "AFR", "afr": "AFR",
        "SAS": "SAS", "sas": "SAS", "CSA": "SAS",
        "AMR": "AMR", "amr": "AMR",
    }
    ref_pop = ancestry_map.get(ancestry_label, "EUR")

    # Filter reference to ancestry-matched samples
    ref_matched = ref[ref["superpop"] == ref_pop][pgs_id]

    if len(ref_matched) < 20:
        # Fall back to all populations if too few ancestry-matched samples
        ref_matched = ref[pgs_id]
        ref_pop = "ALL"

    ref_mean = ref_matched.mean()
    ref_std = ref_matched.std()

    if ref_std == 0 or np.isnan(ref_std):
        return {
            "raw_score": raw_score,
            "z_score": None,
            "percentile": None,
            "reference_population": ref_pop,
            "error": "Zero variance in reference distribution",
        }

    z = (raw_score - ref_mean) / ref_std
    percentile = norm.cdf(z) * 100

    return {
        "raw_score": round(raw_score, 6),
        "z_score": round(z, 3),
        "percentile": round(percentile, 1),
        "reference_population": ref_pop,
        "reference_n": len(ref_matched),
        "reference_mean": round(ref_mean, 6),
        "reference_std": round(ref_std, 6),
    }
Computing reference distributions (one-time per PGS):
bash# Run the same PGS on the 1000 Genomes reference panel
plink2 --bfile "$ANCESTRY_REF/1kg/1kg_phase3_grch38" \
  --score "$TMPDIR/${PGS_ID}_score_input.txt" 1 2 3 \
    cols=+scoresums \
  --out "$TMPDIR/${PGS_ID}_reference"

# Join with population labels for per-ancestry normalization
# Output: sample_id, superpop, pgs_score
Step 7: Clinical Context
For each PGS result, add context about what the score means:
pythondef interpret_pgs(
    pgs_id: str,
    percentile: float,
    trait: str,
    ancestry_label: str,
    pgs_ancestry: str,       # development population of the PGS
    match_rate: float,
    pgs_meta: dict,
) -> dict:
    """
    Add clinical interpretation to a PGS result.
    """
    warnings = []

    # Cross-ancestry warning
    ancestry_match = is_ancestry_compatible(ancestry_label, pgs_ancestry)
    if not ancestry_match:
        warnings.append(
            f"This PGS ({pgs_id}) was developed in {pgs_ancestry} populations. "
            f"The sample's ancestry is {ancestry_label}. Cross-ancestry "
            f"PGS transferability is limited — effect sizes and predictive "
            f"accuracy may differ substantially. Interpret with caution."
        )

    # Match rate warning
    if match_rate < 0.75:
        warnings.append(
            f"Only {match_rate*100:.0f}% of scoring file variants were found "
            f"in the sample ({pgs_meta['variants_number']} expected). "
            f"Score reliability is reduced."
        )

    # Risk tier (generic — actual thresholds depend on the specific PGS)
    if percentile is None:
        risk_tier = "undetermined"
        risk_detail = "Could not compute percentile."
    elif percentile >= 95:
        risk_tier = "high"
        risk_detail = (
            f"Score is at the {percentile:.0f}th percentile — top 5% of the "
            f"ancestry-matched reference population. This indicates elevated "
            f"genetic predisposition for {trait}."
        )
    elif percentile >= 80:
        risk_tier = "above_average"
        risk_detail = (
            f"Score is at the {percentile:.0f}th percentile — above average "
            f"genetic predisposition for {trait}."
        )
    elif percentile >= 20:
        risk_tier = "average"
        risk_detail = (
            f"Score is at the {percentile:.0f}th percentile — average range "
            f"for genetic predisposition to {trait}."
        )
    else:
        risk_tier = "below_average"
        risk_detail = (
            f"Score is at the {percentile:.0f}th percentile — below average "
            f"genetic predisposition for {trait}."
        )

    return {
        "risk_tier": risk_tier,
        "risk_detail": risk_detail,
        "warnings": warnings,
        "ancestry_matched": ancestry_match,
    }


def is_ancestry_compatible(sample_ancestry: str, pgs_ancestry: str) -> bool:
    """Check if sample ancestry is compatible with PGS development population."""
    eur_labels = {"EUR", "NFE", "nfe", "ASJ", "asj", "FIN", "fin", "European"}
    eas_labels = {"EAS", "eas", "East Asian"}
    afr_labels = {"AFR", "afr", "African"}

    # PGS ancestry string often looks like "European" or "EUR" or "Multi-ancestry"
    pgs_lower = pgs_ancestry.lower()

    if "multi" in pgs_lower or "trans" in pgs_lower:
        return True  # Multi-ancestry PGS are broadly applicable

    if sample_ancestry in eur_labels and ("eur" in pgs_lower or "european" in pgs_lower):
        return True
    if sample_ancestry in eas_labels and ("eas" in pgs_lower or "asian" in pgs_lower):
        return True
    if sample_ancestry in afr_labels and ("afr" in pgs_lower or "african" in pgs_lower):
        return True

    return False

Report Generation
JSON output per PGS
json{
  "pgs_id": "PGS000335",
  "trait": "Breast cancer",
  "citation": "Mars N et al., Nat Commun 2020",
  "pgs_variants_total": 1079089,
  "variants_matched": 987234,
  "match_rate": 0.915,
  "score": {
    "raw": 0.04523,
    "z_score": 1.82,
    "percentile": 96.6,
    "reference_population": "EUR",
    "reference_n": 503
  },
  "interpretation": {
    "risk_tier": "high",
    "risk_detail": "Score is at the 97th percentile — top 5% of the ancestry-matched reference population.",
    "ancestry_matched": true,
    "warnings": []
  },
  "pgs_metadata": {
    "development_ancestry": "EUR",
    "weight_type": "beta",
    "genome_build": "GRCh38"
  }
}
Markdown report template
The report should contain:
1. Summary table — All PGS results at a glance:
| PGS ID | Trait | Percentile | Risk | Match % | Ancestry OK? |
|--------|-------|-----------|------|---------|-------------|
| PGS000335 | Breast cancer | 96.6 | 🔴 High | 91.5% | ✅ |
| PGS000004 | Breast cancer | 72.3 | 🟢 Average | 88.2% | ✅ |
| PGS000001 | Breast cancer | 68.1 | 🟢 Average | 95.0% | ✅ |
2. Per-PGS detail sections with:

Score value, Z-score, percentile, and reference distribution context
Variant match rate and any match warnings
Ancestry compatibility assessment
Citation and PGS Catalog link
Histogram or percentile visualization (text-based)

3. Methodology section explaining:

What PGS are and what they are not (not diagnostic)
How scores were calculated (plink2 dosage sum)
How normalization was done (ancestry-matched Z-score)
Limitations (SNP coverage, cross-ancestry, environment not captured)

4. Disclaimers (always include):
IMPORTANT LIMITATIONS:
- PGS estimate GENETIC predisposition only. They do not account for
  environmental, lifestyle, or other non-genetic risk factors.
- A high PGS does not mean disease will occur. A low PGS does not
  mean disease will not occur.
- Most PGS were developed and validated in European-ancestry populations.
  Accuracy in other ancestry groups may be substantially lower.
- PGS are NOT diagnostic tests. Clinical decisions should not be based
  solely on PGS results.
- Variant match rate affects score reliability. Scores with <75% match
  rate should be interpreted with extra caution.

Batch Processing
When running multiple PGS for a single sample (the typical case):
bash#!/bin/bash
# run_all_pgs.sh — Calculate all PGS for a single sample

SAMPLE="$1"          # VCF, BAM, or PLINK prefix
SAMPLE_NAME="$2"     # e.g., "SAMPLE_001"
PGS_LIST="$3"        # comma-separated PGS IDs
ANCESTRY="$4"        # from ancestry pipeline (e.g., "EUR", "ASJ", "EAS")
BUILD="GRCh38"
OUTDIR="$5"

mkdir -p "$OUTDIR"
TMPDIR=$(mktemp -d /tmp/pgs_XXXXXXXX)

# 1. Download all scoring files
download_scorefiles --pgs_id "$PGS_LIST" --target_build "$BUILD" \
  --output_dir "$TMPDIR/scores"

# 2. Prepare sample (convert to pgen if needed)
prepare_sample "$SAMPLE" "$TMPDIR/sample"

# 3. Run each PGS
for SCORE_FILE in "$TMPDIR/scores"/*.txt.gz; do
    PGS_ID=$(basename "$SCORE_FILE" | cut -d_ -f1)

    # Parse, match, calculate, normalize
    calculate_single_pgs "$SCORE_FILE" "$TMPDIR/sample" "$ANCESTRY" \
      > "$OUTDIR/${PGS_ID}_result.json"
done

# 4. Generate combined report
generate_pgs_report "$OUTDIR" "$SAMPLE_NAME" "$ANCESTRY" \
  > "$OUTDIR/pgs_report.md"

rm -rf "$TMPDIR"

pgsc_calc Alternative
The PGS Catalog provides pgsc_calc, a Nextflow pipeline that automates the
entire workflow including ancestry adjustment. If Nextflow is available:
bashnextflow run pgscatalog/pgsc_calc \
  -profile conda \
  --input samplesheet.csv \
  --pgs_id PGS000335,PGS000004 \
  --target_build GRCh38 \
  --run_ancestry true
Pros: Handles everything automatically, including FRAPOSA ancestry projection.
Cons: Nextflow dependency, slower startup, less control over individual steps.
For most use cases, the manual plink2 approach described above is faster and
gives more control over ancestry normalization (using our ancestry pipeline's
results rather than pgsc_calc's built-in ancestry analysis).

Error Handling
ErrorCauseResolution0 variants matchedBuild mismatch, chr prefix mismatch, or wrong scoring file formatCheck genome build; normalize chr prefixes; verify scoring file formatplink2 "Warning: N score allele(s) flipped"REF/ALT orientation differs between scoring file and sampleNormal — plink2 handles this automaticallyVery different scores between runsVariant set changed (different VCF quality thresholds)Ensure consistent variant calling parametersPercentile >99.9 or <0.1Score is extreme outlierVerify match rate; check for data quality issues; may be genuine"No phenotype data" from plink2Missing phenotype column in .fam/.psamIgnore — not needed for score calculationScoring file has OR weights, not betasweight_type = "OR"Convert: beta = ln(OR). Apply before scoring.

Appendix: Score Interpretation by Trait Category
Different trait categories require different interpretation framing:
CategoryExample traitsHow to interpret percentileDisease riskBreast cancer, CAD, T2DHigher percentile = higher genetic riskProtective factorsLongevityHigher percentile = more protective variantsQuantitative traitsHeight, BMI, LDLPercentile indicates expected trait value relative to populationPharmacogenomicDrug metabolismMay not be linear — specific thresholds matter
Never state absolute risk. PGS give relative genetic predisposition compared
to a reference population, not absolute probability of developing a disease.