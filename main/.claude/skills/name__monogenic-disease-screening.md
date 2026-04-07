name: monogenic-disease-screening
description: |
Screen WGS/WES samples for pathogenic and likely pathogenic variants in curated
gene panels (ACMG SF v3.3, cancer predisposition, cardiovascular, metabolism, etc.).
Uses ClinVar annotations, gnomAD population frequencies, and consequence predictions
to identify reportable findings. Handles VCF and gVCF inputs. Produces a structured
JSON result and a clinical-style markdown report per sample. Use this skill when
the user asks to screen for monogenic diseases, actionable findings, carrier status,
or pathogenic variants in specific gene lists.
Monogenic Disease Screening Pipeline
Overview
This pipeline screens a sample's variant calls against curated gene panels to
identify clinically significant variants — those known or predicted to cause
Mendelian (single-gene) diseases. Unlike PGS (which aggregate many small effects),
monogenic screening looks for individual high-impact variants.
The pipeline:

Extracts variants in target gene regions from the sample VCF
Annotates with ClinVar pathogenicity, gnomAD frequency, and predicted consequence
Filters to reportable findings (Pathogenic, Likely Pathogenic, or novel Loss-of-Function)
Assesses zygosity and inheritance pattern compatibility
Generates a clinical report with actionable findings


Gene Panels
Standard panels
Panels are defined as lists of gene symbols with associated disease categories
and inheritance patterns. The pipeline accepts panels as JSON:
json{
  "panel_name": "Cancer Predisposition",
  "panel_version": "ACMG SF v3.3",
  "genes": [
    {
      "symbol": "BRCA1",
      "hgnc_id": "HGNC:1100",
      "disease": "Hereditary breast/ovarian cancer",
      "inheritance": "AD",
      "actionability": "high",
      "chromosomal_location": "17q21.31"
    },
    {
      "symbol": "TP53",
      "hgnc_id": "HGNC:11998",
      "disease": "Li-Fraumeni syndrome",
      "inheritance": "AD",
      "actionability": "high",
      "chromosomal_location": "17p13.1"
    }
  ]
}
Inheritance patterns:

AD — Autosomal Dominant: a single pathogenic variant is reportable
AR — Autosomal Recessive: report if homozygous or compound heterozygous
XL — X-linked: consider hemizygosity in XY individuals
AD/AR — Both patterns described; report based on variant and zygosity

ACMG Secondary Findings v3.3 (81 genes)
The American College of Medical Genetics maintains a list of genes for which
incidental findings should be reported. The full list should be stored as a
JSON panel file. Major categories:
CategoryN genesKey genesCancer predisposition~28APC, BRCA1, BRCA2, MLH1, MSH2, MSH6, PALB2, TP53, RET, VHLCardiovascular~41MYBPC3, MYH7, SCN5A, KCNQ1, KCNH2, LDLR, FBN1, LMNA, TTNMetabolism~5GAA, GLA, OTC, BTD, CYP27A1Miscellaneous~10HFE, ATP7B, RYR1, TTR, RPE65

Prerequisites
Software
ToolPurposeInstallbcftoolsVCF extraction, annotation, filteringcondaSnpSift / SnpEffClinVar annotation, consequence predictionconda / jarvep (optional)Ensembl Variant Effect Predictor — most comprehensiveconda / dockerplink2Homozygosity, sample statscondapython3Orchestration, filtering logic, report generationconda
Reference Data
FileSourcePurposeUpdate frequencyclinvar.vcf.gzNCBI FTPPathogenicity assertionsMonthlygnomad.sites.vcf.gzgnomAD downloadsPopulation allele frequenciesPer releaseGene coordinates BEDUCSC / Ensembl / NCBIGene region extractionPer assemblyPanel JSON filesCurated internallyGene lists per disease categoryAs ACMG updates
ClinVar download:
bash# GRCh38
wget -O clinvar.vcf.gz \
  https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz
wget -O clinvar.vcf.gz.tbi \
  https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz.tbi

# Update monthly — ClinVar is actively curated
Gene coordinates BED file:
bash# Generate from NCBI RefSeq or Ensembl GTF
# Include UTR padding (±5000 bp for splice site coverage)
# Format: chr  start  end  gene_symbol

# Example for a single gene:
# chr17  43044295  43170245  BRCA1

Pipeline Steps
Step 1: Build Target Regions from Gene Panel
pythondef build_regions_bed(panel: dict, gene_coords_file: str, padding: int = 5000) -> str:
    """
    Create a BED file of genomic regions for the target gene panel.

    padding: bp to add on each side for splice sites and regulatory regions.
    """
    # Load gene coordinate database
    gene_db = {}
    with open(gene_coords_file) as f:
        for line in f:
            chrom, start, end, symbol = line.strip().split('\t')[:4]
            gene_db[symbol] = (chrom, int(start), int(end))

    regions = []
    missing_genes = []

    for gene in panel["genes"]:
        symbol = gene["symbol"]
        if symbol in gene_db:
            chrom, start, end = gene_db[symbol]
            regions.append((chrom, max(0, start - padding), end + padding, symbol))
        else:
            missing_genes.append(symbol)

    if missing_genes:
        print(f"WARNING: {len(missing_genes)} genes not found in coordinate database: "
              f"{', '.join(missing_genes[:5])}{'...' if len(missing_genes) > 5 else ''}")

    # Write BED file
    bed_path = f"/tmp/panel_regions.bed"
    with open(bed_path, 'w') as f:
        for chrom, start, end, symbol in sorted(regions):
            f.write(f"{chrom}\t{start}\t{end}\t{symbol}\n")

    return bed_path
Step 2: Extract Variants in Target Regions
bashSAMPLE_VCF="$1"        # Input VCF or gVCF
REGIONS_BED="$2"       # From Step 1
TMPDIR=$(mktemp -d)

# Extract variants in target regions
# For gVCF: this automatically resolves reference blocks
bcftools view \
  -R "$REGIONS_BED" \
  --types snps,indels \
  --exclude 'GT="./." || GT="0/0" || GT="0|0"' \
  "$SAMPLE_VCF" \
  -Oz -o "$TMPDIR/panel_variants.vcf.gz"

tabix -p vcf "$TMPDIR/panel_variants.vcf.gz"

N_VARIANTS=$(bcftools view -H "$TMPDIR/panel_variants.vcf.gz" | wc -l)
echo "Variants in target regions: $N_VARIANTS"
Step 3: Annotate with ClinVar
bashCLINVAR_VCF="/data/reference/clinvar.vcf.gz"

# Annotate sample variants with ClinVar CLNSIG and CLNDN
bcftools annotate \
  -a "$CLINVAR_VCF" \
  -c INFO/CLNSIG,INFO/CLNDN,INFO/CLNREVSTAT,INFO/CLNACC \
  "$TMPDIR/panel_variants.vcf.gz" \
  -Oz -o "$TMPDIR/panel_clinvar.vcf.gz"

tabix -p vcf "$TMPDIR/panel_clinvar.vcf.gz"
ClinVar CLNSIG values:
ValueMeaningReportable?PathogenicDisease-causing✅ AlwaysLikely_pathogenicProbably disease-causing✅ AlwaysUncertain_significanceUnknownReport only if in high-priority gene and novel LOFLikely_benignProbably not disease-causing❌ NoBenignNot disease-causing❌ NoConflictingMultiple labs disagreeFlag for manual review
CLNREVSTAT (review status) — indicates confidence in the ClinVar assertion:
StarsMeaningReliabilitypractice_guideline★★★★Highest — expert panel reviewedreviewed_by_expert_panel★★★Highcriteria_provided,_multiple_submitters★★Moderate — multiple labs agreecriteria_provided,_single_submitter★Low — single lab onlyno_assertion_criteria_provided☆Very low — interpret cautiously
Step 4: Annotate with gnomAD Frequency
bashGNOMAD_VCF="/data/reference/gnomad.genomes.sites.vcf.gz"

# Add gnomAD allele frequency (AF) and population-specific frequencies
bcftools annotate \
  -a "$GNOMAD_VCF" \
  -c INFO/gnomAD_AF:=INFO/AF,INFO/gnomAD_AF_popmax:=INFO/AF_popmax \
  "$TMPDIR/panel_clinvar.vcf.gz" \
  -Oz -o "$TMPDIR/panel_annotated.vcf.gz"

tabix -p vcf "$TMPDIR/panel_annotated.vcf.gz"
Frequency interpretation:
gnomAD AFInterpretationAbsentNovel variant — may be de novo or extremely rare< 0.00001 (1e-5)Ultra-rare — consistent with pathogenic0.00001–0.001Rare — could be pathogenic for recessive conditions0.001–0.01Low frequency — unlikely pathogenic for dominant conditions> 0.01 (1%)Common — almost certainly benign for Mendelian disease
Step 5: Predict Variant Consequence
If VEP is available:
bashvep --input_file "$TMPDIR/panel_annotated.vcf.gz" \
  --output_file "$TMPDIR/panel_vep.vcf.gz" \
  --vcf --compress_output bgzip \
  --offline --cache --assembly GRCh38 \
  --sift b --polyphen b \
  --canonical --symbol --biotype \
  --pick  # one consequence per variant (most severe)
If only bcftools/SnpEff available:
bashjava -jar snpEff.jar -v GRCh38.p14 \
  "$TMPDIR/panel_annotated.vcf.gz" | \
  bgzip > "$TMPDIR/panel_snpeff.vcf.gz"
Consequence severity ranking (high to low):
ConsequenceSeverityLOF?Exampletranscript_ablationHighestYesWhole gene deletionsplice_acceptor_variantVery HighYesDisrupts splice sitesplice_donor_variantVery HighYesDisrupts splice sitestop_gainedVery HighYesPremature stop codonframeshift_variantVery HighYesReading frame disruptedstop_lostHighNo*Stop codon removedstart_lostHighNo*Start codon removedmissense_variantModerateNoAmino acid changeinframe_insertionModerateNoIn-frame AA insertioninframe_deletionModerateNoIn-frame AA deletionsynonymous_variantLowNoNo AA changeintronic_variantLowNoDeep intronic
*LOF = Loss of Function — these are the highest priority for novel variants.
Step 6: Filter to Reportable Findings
pythondef filter_reportable(variants: list, panel: dict, sample_sex: str) -> list:
    """
    Filter annotated variants to those meeting reporting criteria.

    Criteria for inclusion:
    1. ClinVar Pathogenic or Likely Pathogenic (any gene in panel), OR
    2. Novel/ultra-rare LOF variant in a panel gene (regardless of ClinVar), OR
    3. ClinVar Conflicting with at least one P/LP submission in high-priority gene

    Additional zygosity/inheritance checks:
    - AD genes: het or hom is reportable
    - AR genes: only report if hom or compound het (two different het variants in same gene)
    - XL genes in XY: hemizygous = effectively homozygous
    """
    gene_lookup = {g["symbol"]: g for g in panel["genes"]}
    reportable = []
    gene_het_variants = {}  # for compound het detection in AR genes

    for var in variants:
        gene = var.get("gene_symbol")
        if gene not in gene_lookup:
            continue

        gene_info = gene_lookup[gene]
        inheritance = gene_info.get("inheritance", "AD")
        clnsig = var.get("clinvar_clnsig", "")
        gnomad_af = var.get("gnomad_af", None)
        consequence = var.get("consequence", "")
        zygosity = var.get("zygosity", "")  # "het", "hom", "hemi"
        is_lof = consequence in [
            "frameshift_variant", "stop_gained",
            "splice_donor_variant", "splice_acceptor_variant",
            "transcript_ablation",
        ]

        # ── Criterion 1: ClinVar P/LP ──
        is_pathogenic = any(
            x in clnsig.lower()
            for x in ["pathogenic", "likely_pathogenic"]
        ) and "benign" not in clnsig.lower()

        # ── Criterion 2: Novel/ultra-rare LOF ──
        is_novel_lof = (
            is_lof
            and (gnomad_af is None or gnomad_af < 0.00001)
            and "benign" not in clnsig.lower()
        )

        # ── Criterion 3: Conflicting with P/LP ──
        is_conflicting_plp = (
            "conflicting" in clnsig.lower()
            and gene_info.get("actionability") == "high"
        )

        if not (is_pathogenic or is_novel_lof or is_conflicting_plp):
            continue

        # ── Zygosity / inheritance check ──
        report_this = False
        inheritance_note = ""

        if inheritance == "AD":
            # Any carrier (het or hom) is reportable
            report_this = True
            if zygosity == "hom":
                inheritance_note = "Homozygous (AD gene — single copy sufficient)"
            else:
                inheritance_note = "Heterozygous carrier (AD — sufficient for risk)"

        elif inheritance == "AR":
            if zygosity == "hom":
                report_this = True
                inheritance_note = "Homozygous (AR gene — affected)"
            elif zygosity == "het":
                # Track for compound het detection
                gene_het_variants.setdefault(gene, []).append(var)
                # Will resolve compound hets after loop
                inheritance_note = "Heterozygous carrier (AR gene — carrier only unless compound het)"
                report_this = True  # Report as carrier; flag compound het separately

        elif inheritance == "XL":
            if sample_sex == "XY" and var.get("chrom") == "chrX":
                report_this = True
                inheritance_note = "Hemizygous (X-linked gene in XY individual — affected)"
            elif zygosity == "hom":
                report_this = True
                inheritance_note = "Homozygous (X-linked)"
            else:
                report_this = True
                inheritance_note = "Heterozygous carrier (X-linked)"

        elif inheritance == "AD/AR":
            report_this = True
            inheritance_note = f"{zygosity.capitalize()} (gene has AD and AR disease associations)"

        if report_this:
            var["inheritance_note"] = inheritance_note
            var["classification_reason"] = (
                "ClinVar P/LP" if is_pathogenic
                else "Novel/ultra-rare LOF" if is_novel_lof
                else "Conflicting with P/LP assertion"
            )
            reportable.append(var)

    # ── Compound heterozygote detection (AR genes) ──
    for gene, het_vars in gene_het_variants.items():
        if len(het_vars) >= 2:
            for v in het_vars:
                if v in reportable:
                    v["compound_het_flag"] = True
                    v["inheritance_note"] += (
                        f" — COMPOUND HET: {len(het_vars)} heterozygous variants "
                        f"found in this AR gene. If on different alleles (trans), "
                        f"individual may be affected."
                    )

    return reportable

Report Generation
JSON output schema
json{
  "sample_name": "SAMPLE_001",
  "date": "2026-03-30",
  "pipeline_version": "1.0",
  "sample_sex": "XX",
  "panels_screened": [
    {
      "panel_name": "Cancer Predisposition",
      "genes_screened": 28,
      "variants_in_regions": 342,
      "reportable_findings": 1
    }
  ],
  "findings": [
    {
      "gene": "BRCA2",
      "variant": "chr13:32340300:G:A",
      "hgvs_c": "c.5946delT",
      "hgvs_p": "p.Ser1982ArgfsTer22",
      "consequence": "frameshift_variant",
      "zygosity": "het",
      "clinvar_clnsig": "Pathogenic",
      "clinvar_review_status": "reviewed_by_expert_panel",
      "clinvar_accession": "RCV000077529",
      "gnomad_af": 0.0000032,
      "disease": "Hereditary breast/ovarian cancer",
      "inheritance": "AD",
      "inheritance_note": "Heterozygous carrier (AD — sufficient for risk)",
      "classification_reason": "ClinVar P/LP",
      "actionability": "high",
      "clinical_implication": "Increased lifetime risk of breast (45-85%), ovarian (11-35%), and other cancers. Eligible for enhanced screening and risk-reduction strategies."
    }
  ],
  "negative_panels": ["Cardiovascular", "Metabolism", "Miscellaneous"],
  "qc": {
    "total_variants_screened": 1247,
    "genes_with_coverage": 79,
    "genes_missing_coverage": ["SDHAF2", "TMEM127"],
    "mean_gene_variant_count": 15.8
  }
}
Markdown report structure
The report must be clear enough for a genetics-literate reader (genetic counselor,
clinical geneticist) while being cautious about direct clinical claims.
Sections:
1. Executive Summary
## Summary

**Panels screened**: Cancer Predisposition (28 genes), Cardiovascular (41 genes),
Metabolism (5 genes), Miscellaneous (10 genes)

**Reportable findings: 1**

| Gene | Variant | Classification | Disease | Zygosity |
|------|---------|---------------|---------|----------|
| BRCA2 | c.5946delT (p.Ser1982ArgfsTer22) | Pathogenic | Hereditary breast/ovarian cancer | Het |

**Panels with no findings**: Cardiovascular, Metabolism, Miscellaneous
2. Detailed Findings — one section per finding with:

Variant details (genomic coordinates, cDNA, protein change)
ClinVar classification with review status (star rating)
gnomAD frequency (or "absent" if not observed)
Consequence type and predicted impact
Inheritance pattern and zygosity interpretation
Disease association and clinical implications
Link to ClinVar entry

3. Negative Results — explicitly state which panels were screened with no
pathogenic findings. This is clinically important — absence of findings in a
screened panel is a meaningful negative result.
4. Limitations and Coverage — list genes where coverage may be insufficient
(low variant count suggests poor sequencing coverage in that region).
5. Methodology — tools used, databases and versions, filtering criteria.
6. Disclaimers:
IMPORTANT:
- This analysis screens for known pathogenic variants and predicted loss-of-function
  variants in curated gene panels. It does not detect all possible disease-causing
  mutations (e.g., large structural variants, repeat expansions, deep intronic variants).
- Variant classification relies on ClinVar assertions, which may change over time
  as evidence accumulates. Variants of Uncertain Significance (VUS) are not reported
  unless they are predicted loss-of-function in high-priority genes.
- A negative result (no findings) does not rule out disease risk. It means no
  pathogenic variants were identified in the screened genes with current databases.
- This is a screening tool, not a clinical diagnostic test. Findings should be
  confirmed by an accredited clinical laboratory and interpreted by a qualified
  genetics professional.
- Compound heterozygosity assessment requires phasing data to confirm variants
  are on different alleles (trans). Without phasing, compound het status is
  presumptive.

Coverage QC
Why coverage matters for monogenic screening
Unlike PGS (where missing a few variants slightly degrades the score), monogenic
screening has a binary failure mode: if the pathogenic variant site has zero
sequencing coverage, it will be missed entirely with no warning.
pythondef check_gene_coverage(
    sample_vcf: str,
    gene_bed: str,
    min_variants_per_gene: int = 3,
) -> dict:
    """
    Check whether each target gene has sufficient variant calls to indicate
    adequate sequencing coverage.

    A gene with zero variants in a WGS sample likely has a coverage gap.
    """
    import subprocess

    # Count variants per gene region
    result = subprocess.run(
        f"bedtools intersect -a {sample_vcf} -b {gene_bed} -wa -wb | "
        f"cut -f$(awk '{{print NF}}' {gene_bed} | head -1) | sort | uniq -c | sort -rn",
        shell=True, capture_output=True, text=True
    )

    gene_counts = {}
    for line in result.stdout.strip().split('\n'):
        if line.strip():
            parts = line.strip().split()
            count = int(parts[0])
            gene = parts[1]
            gene_counts[gene] = count

    # Identify genes with low or no variants
    # For a 30× WGS, a typical gene should have dozens to hundreds of variants
    warnings = []
    for gene in open(gene_bed):
        g = gene.strip().split('\t')[3] if '\t' in gene else gene.strip()
        if g not in gene_counts or gene_counts.get(g, 0) < min_variants_per_gene:
            warnings.append(g)

    return {
        "gene_variant_counts": gene_counts,
        "low_coverage_genes": warnings,
        "coverage_adequate": len(warnings) == 0,
    }
If BAM/CRAM is available, also check read depth directly:
bash# Calculate mean depth per gene region
samtools depth -b "$GENE_BED" "$INPUT_BAM" | \
awk '{sum[$1":"$2]+=$3; count[$1":"$2]++}
     END {for (k in sum) print k, sum[k]/count[k]}' > gene_depths.txt
Genes with mean depth <15× should be flagged as potentially under-covered.

Handling Ancestry for Monogenic Screening
Ancestry affects monogenic screening in two ways:

Population-specific allele frequencies: A variant common in one population
but absent in gnomAD's training populations may be miscategorized. Always check
population-specific AF (e.g., gnomAD_AF_asj for Ashkenazi Jewish samples).
Founder mutations: Certain populations have known founder mutations at
elevated frequencies. These should be highlighted:

pythonFOUNDER_MUTATIONS = {
    "ASJ": [
        {"gene": "BRCA1", "variant": "c.68_69delAG", "disease": "Breast/ovarian cancer", "carrier_freq": "1/40"},
        {"gene": "BRCA1", "variant": "c.5266dupC", "disease": "Breast/ovarian cancer", "carrier_freq": "1/40"},
        {"gene": "BRCA2", "variant": "c.5946delT", "disease": "Breast/ovarian cancer", "carrier_freq": "1/40"},
        {"gene": "HEXA", "variant": "c.1274_1277dupTATC", "disease": "Tay-Sachs disease", "carrier_freq": "1/30"},
        {"gene": "GBA", "variant": "c.1226A>G (N370S)", "disease": "Gaucher disease", "carrier_freq": "1/15"},
        {"gene": "FANCC", "variant": "c.456+4A>T (IVS4+4)", "disease": "Fanconi anemia C", "carrier_freq": "1/89"},
        {"gene": "BLM", "variant": "c.2207_2212delinsTAGATTC", "disease": "Bloom syndrome", "carrier_freq": "1/100"},
        {"gene": "MCOLN1", "variant": "c.406-2A>G", "disease": "Mucolipidosis IV", "carrier_freq": "1/100"},
    ],
    "FIN": [
        {"gene": "AIRE", "variant": "c.769C>T (R257X)", "disease": "APS-1", "carrier_freq": "1/250"},
    ],
    # Add other population-specific founder mutations as needed
}

def check_founder_mutations(findings: list, ancestry: str) -> list:
    """Flag any findings that match known founder mutations for this ancestry."""
    founders = FOUNDER_MUTATIONS.get(ancestry, [])
    for finding in findings:
        for fm in founders:
            if (finding["gene"] == fm["gene"] and
                fm["variant"] in finding.get("hgvs_c", "")):
                finding["founder_mutation"] = True
                finding["founder_carrier_freq"] = fm["carrier_freq"]
                finding["founder_note"] = (
                    f"This is a known founder mutation in the {ancestry} population "
                    f"(carrier frequency ~{fm['carrier_freq']}). "
                    f"Population-specific screening is available."
                )
    return findings

Error Handling
ErrorCauseResolution0 variants in target regionsgVCF with only ref blocks, or wrong buildCheck build matches panel coordinates; for gVCF verify non-ref sites existClinVar annotation adds no CLNSIGClinVar VCF build doesn't match sampleEnsure both are GRCh38 (or both GRCh37)Gene not found in coordinates DBGene symbol changed, or non-standard symbolCheck HGNC for current approved symbolMany genes with 0 variantsSample may be exome (not WGS), or very low coverageCheck if WES — adjust expectations; check BAM depthCompound het called but variants are cisCannot determine phase without trio/long-readsFlag as "presumptive compound het — phasing not confirmed"VUS in ClinVar but predicted LOFAmbiguous classificationReport with note: "VUS with predicted loss-of-function — may warrant reclassification"

Batch Processing (Multiple Panels)
When running all panels for a single sample:
bash# Run all panels in sequence (they share the same extracted VCF)
for PANEL in cancer_predisposition cardiovascular metabolism miscellaneous; do
    run_panel "$SAMPLE_VCF" "$PANELS_DIR/${PANEL}.json" "$SAMPLE_SEX" "$ANCESTRY" \
      >> "$OUTDIR/all_findings.json"
done

# Generate combined report
generate_monogenic_report "$OUTDIR/all_findings.json" "$SAMPLE_NAME" \
  > "$OUTDIR/monogenic_report.md"

Appendix: ACMG SF v3.3 Gene Categories
Reference list for panel construction. Verify against the latest ACMG publication
as updates occur periodically.
Cancer Predisposition (~28 genes):
APC, BRCA1, BRCA2, BMPR1A, MAX, MEN1, MLH1, MSH2, MSH6, MUTYH, NF2, PALB2,
PMS2, PTEN, RB1, RET, SDHAF2, SDHB, SDHC, SDHD, SMAD4, STK11, TMEM127,
TP53, TSC1, TSC2, VHL, WT1
Cardiovascular (~41 genes):
ACTA2, ACTC1, APOB, BAG3, CALM1, CALM2, CALM3, CASQ2, COL3A1, DES, DSC2,
DSG2, DSP, FBN1, FLNC, GLA, KCNH2, KCNQ1, LDLR, LMNA, MYH7, MYH11,
MYBPC3, MYL2, MYL3, PCSK9, PKP2, PLN, PRKAG2, RBM20, RYR2, SCN5A, SMAD3,
TGFBR1, TGFBR2, TMEM43, TNNC1, TNNI3, TNNT2, TPM1, TRDN, TTN
Metabolism (~5 genes):
BTD, CYP27A1, GAA, GLA, OTC
Miscellaneous (~10 genes):
ABCD1, ACVRL1, ATP7B, CACNA1S, ENG, HFE, HNF1A, RPE65, RYR1, TTR
Note: GLA appears in both cardiovascular (Fabry disease) and metabolism
categories in some classifications. Include it once in the analysis but
reference both disease associations.