name: carrier-status-screening
description: |
Screen WGS/WES samples for carrier status of autosomal recessive diseases.
Checks specific known pathogenic variants and performs gene-level LOF screening
for each disease in a configurable disease list. Reports carrier status, zygosity,
population-specific carrier frequencies, and reproductive risk context. Use this
skill when the user asks about carrier screening, recessive disease risk, or
reproductive genetics.
Carrier Status Screening — Recessive Diseases
Overview
Carrier screening identifies individuals who carry one copy of a pathogenic variant
for an autosomal recessive disease. Carriers are typically unaffected but can pass
the variant to offspring. When both parents are carriers for the same disease, each
child has a 25% chance of being affected.
This pipeline:

Checks a configurable list of disease-gene-variant combinations
For each gene, also scans for any other known pathogenic or LOF variants beyond
the listed key variants (ClinVar-based gene sweep)
Reports carrier status with ancestry-contextualized carrier frequencies
Flags homozygous or compound heterozygous findings (affected, not just carrier)


Disease List Format
The disease list is a JSON array. Each entry defines a disease, gene, key variants
to check, and population context. The pipeline checks all listed variants AND
performs a gene-wide ClinVar sweep for additional pathogenic variants.
json[
  {
    "disease": "Cystic fibrosis",
    "gene": "CFTR",
    "inheritance": "AR",
    "key_variants": [
      {
        "name": "F508del",
        "rsid": "rs113993960",
        "hgvs_c": "c.1521_1523delCTT",
        "hgvs_p": "p.Phe508del",
        "grch38": {"chr": "chr7", "pos": 117559590, "ref": "ATCT", "alt": "A"},
        "carrier_freq": "1 in 25",
        "population": "Northern European",
        "notes": "Most common CF mutation worldwide (~70% of CF alleles)"
      },
      {
        "name": "G542X",
        "rsid": "rs113993959",
        "hgvs_c": "c.1624G>T",
        "hgvs_p": "p.Gly542Ter",
        "grch38": {"chr": "chr7", "pos": 117587738, "ref": "G", "alt": "T"},
        "carrier_freq": "varies",
        "population": "Pan-ethnic",
        "notes": "Second most common CF mutation globally"
      }
    ],
    "gene_sweep": true,
    "clinical_summary": "Progressive lung disease, pancreatic insufficiency, infertility in males. Newborn screening available. Treatment with CFTR modulators (e.g., elexacaftor/tezacaftor/ivacaftor) has transformed outcomes for many genotypes."
  },
  {
    "disease": "Tay-Sachs disease",
    "gene": "HEXA",
    "inheritance": "AR",
    "key_variants": [
      {
        "name": "1278insTATC",
        "rsid": "rs387906309",
        "hgvs_c": "c.1274_1277dupTATC",
        "hgvs_p": "p.Tyr427IlefsTer5",
        "grch38": {"chr": "chr15", "pos": 72346580, "ref": "C", "alt": "CTATC"},
        "carrier_freq": "1 in 30",
        "population": "Ashkenazi Jewish",
        "notes": "Most common TSD mutation in Ashkenazi Jewish population"
      }
    ],
    "gene_sweep": true,
    "clinical_summary": "Progressive neurodegeneration. Infantile form is fatal by age 4-5. No effective treatment. Carrier screening widely recommended for Ashkenazi Jewish individuals."
  }
]
Required fields per variant: name, grch38 (chr/pos/ref/alt), carrier_freq, population.
Optional but recommended: rsid, hgvs_c, hgvs_p, notes.
The list is extensible — add new diseases/variants by appending entries to the JSON.
Special cases requiring non-standard detection
Some diseases require detection methods beyond simple SNV/indel lookup:
DiseaseGeneChallengeApproachSMASMN1Exon 7 deletion (copy number)Count reads mapping to SMN1 vs SMN2 exon 7; or use SMNCopyNumberCallerAlpha-thalassemiaHBA1/HBA2Large deletions (~3.7kb, ~4.2kb)Requires structural variant caller or depth analysisFragile XFMR1CGG repeat expansionNot detectable from short-read WGS; requires specialized assayCAHCYP21A2Gene conversion with pseudogene CYP21A1PRequires specialized caller (e.g., Cyrius-like approach)
For these cases, flag in the report that standard variant-based screening cannot
reliably detect the relevant mutations. Recommend targeted testing if clinically indicated.

Pipeline Steps
Step 1: Extract Variants at Key Positions
For each disease entry, check whether the sample carries the listed variant(s):
bashSAMPLE_VCF="$1"
DISEASE_LIST="$2"  # JSON file
TMPDIR=$(mktemp -d)

# Build a positions file from all key variants
python3 -c "
import json, sys
diseases = json.load(open('$DISEASE_LIST'))
for d in diseases:
    for v in d.get('key_variants', []):
        pos = v['grch38']
        print(f\"{pos['chr']}\t{pos['pos']}\")
" | sort -k1,1 -k2,2n | uniq > "$TMPDIR/target_positions.tsv"

# Query the VCF at target positions
bcftools query \
  -f '%CHROM\t%POS\t%REF\t%ALT\t[%GT]\t[%GQ]\t[%DP]\n' \
  -T "$TMPDIR/target_positions.tsv" \
  "$SAMPLE_VCF" > "$TMPDIR/key_variant_calls.tsv"
Step 2: Check Each Key Variant
pythondef check_key_variants(
    calls_file: str,
    disease_list: list,
) -> list:
    """
    Check sample genotypes at key variant positions.

    Returns list of findings (one per detected variant).
    """
    # Parse sample calls into lookup: (chr, pos, ref, alt) → genotype info
    calls = {}
    with open(calls_file) as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 5:
                chrom, pos, ref, alt, gt = parts[0], int(parts[1]), parts[2], parts[3], parts[4]
                gq = int(parts[5]) if len(parts) > 5 and parts[5] != '.' else None
                dp = int(parts[6]) if len(parts) > 6 and parts[6] != '.' else None
                # ALT may be multiallelic — split
                for a in alt.split(','):
                    calls[(chrom, pos, ref, a)] = {
                        "genotype": gt, "gq": gq, "dp": dp
                    }

    findings = []
    for disease in disease_list:
        for var in disease.get("key_variants", []):
            pos = var["grch38"]
            key = (pos["chr"], pos["pos"], pos["ref"], pos["alt"])

            call = calls.get(key)
            if call is None:
                # Variant position not in VCF — either ref/ref or no coverage
                # Check if position is covered at all
                status = "not_detected"
                zygosity = None
                gt = None
            else:
                gt = call["genotype"]
                if gt in ("0/1", "0|1", "1|0"):
                    status = "carrier"
                    zygosity = "heterozygous"
                elif gt in ("1/1", "1|1"):
                    status = "affected"
                    zygosity = "homozygous"
                elif gt in ("0/0", "0|0"):
                    status = "not_detected"
                    zygosity = None
                else:
                    status = "unclear"
                    zygosity = gt

            if status in ("carrier", "affected"):
                findings.append({
                    "disease": disease["disease"],
                    "gene": disease["gene"],
                    "variant_name": var["name"],
                    "rsid": var.get("rsid"),
                    "hgvs_c": var.get("hgvs_c"),
                    "hgvs_p": var.get("hgvs_p"),
                    "coordinates": f"{pos['chr']}:{pos['pos']}:{pos['ref']}>{pos['alt']}",
                    "genotype": gt,
                    "zygosity": zygosity,
                    "status": status,
                    "carrier_freq": var["carrier_freq"],
                    "population": var["population"],
                    "gq": call.get("gq") if call else None,
                    "dp": call.get("dp") if call else None,
                    "notes": var.get("notes", ""),
                    "clinical_summary": disease.get("clinical_summary", ""),
                })

    return findings
Step 3: Gene-Wide ClinVar Sweep
Beyond the key listed variants, scan each gene for ANY pathogenic/likely pathogenic
variant in ClinVar. This catches rare or private mutations not on the key list.
bashCLINVAR_VCF="/data/reference/clinvar.vcf.gz"
GENE_BED="$TMPDIR/carrier_genes.bed"  # BED file of all genes in the disease list

# Extract sample variants in gene regions
bcftools view -R "$GENE_BED" "$SAMPLE_VCF" | \
bcftools view --types snps,indels --exclude 'GT="0/0" || GT="./."' | \
bcftools annotate -a "$CLINVAR_VCF" -c INFO/CLNSIG,INFO/CLNDN,INFO/CLNREVSTAT | \
bcftools query -f '%CHROM\t%POS\t%REF\t%ALT\t%INFO/CLNSIG\t%INFO/CLNDN\t[%GT]\n' | \
grep -i "pathogenic" | grep -vi "benign" > "$TMPDIR/gene_sweep_hits.tsv"
pythondef gene_sweep(sweep_file: str, disease_list: list, key_findings: list) -> list:
    """
    Find additional ClinVar P/LP variants in carrier screening genes
    that were NOT in the key variant list.
    """
    # Build set of already-found variant coordinates
    already_found = set()
    for f in key_findings:
        already_found.add(f["coordinates"])

    gene_to_disease = {}
    for d in disease_list:
        gene_to_disease[d["gene"]] = d

    additional = []
    with open(sweep_file) as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 7:
                continue
            chrom, pos, ref, alt, clnsig, clndn, gt = parts[:7]
            coord = f"{chrom}:{pos}:{ref}>{alt}"
            if coord in already_found:
                continue

            # Determine which gene this variant belongs to
            # (requires cross-referencing with gene coordinates)
            gene = lookup_gene_for_position(chrom, int(pos))
            if gene not in gene_to_disease:
                continue

            disease_info = gene_to_disease[gene]
            zygosity = "heterozygous" if gt in ("0/1", "0|1", "1|0") else \
                       "homozygous" if gt in ("1/1", "1|1") else gt

            additional.append({
                "disease": disease_info["disease"],
                "gene": gene,
                "variant_name": f"ClinVar: {clndn}",
                "coordinates": coord,
                "genotype": gt,
                "zygosity": zygosity,
                "status": "affected" if zygosity == "homozygous" else "carrier",
                "clinvar_clnsig": clnsig,
                "source": "gene_sweep",
                "notes": "Found by gene-wide ClinVar scan (not in primary variant list)",
                "clinical_summary": disease_info.get("clinical_summary", ""),
            })

    return additional
Step 4: Compound Heterozygosity Check
For AR diseases, if two different heterozygous pathogenic variants are found in
the same gene, the individual may be affected (compound heterozygous):
pythondef check_compound_het(findings: list) -> list:
    """Flag potential compound heterozygotes."""
    from collections import defaultdict

    gene_hets = defaultdict(list)
    for f in findings:
        if f["zygosity"] == "heterozygous":
            gene_hets[f["gene"]].append(f)

    for gene, variants in gene_hets.items():
        if len(variants) >= 2:
            for v in variants:
                v["compound_het_flag"] = True
                other_vars = [x["variant_name"] for x in variants if x is not v]
                v["compound_het_note"] = (
                    f"COMPOUND HETEROZYGOUS: {len(variants)} pathogenic variants "
                    f"detected in {gene} ({', '.join(other_vars)}). "
                    f"If these variants are on different chromosomes (trans), "
                    f"individual may be AFFECTED, not just a carrier. "
                    f"Phasing (trio analysis or long-read sequencing) is needed "
                    f"to confirm."
                )

    return findings
Step 5: Contextualize with Sample Ancestry
pythondef add_ancestry_context(findings: list, sample_ancestry: str) -> list:
    """
    Add ancestry-specific context to each finding.
    Flag when the finding matches a known founder mutation for the sample's ancestry.
    """
    # Ancestry-specific carrier frequency adjustments
    # When the variant's listed population matches the sample's ancestry,
    # the listed frequency is directly applicable.
    # When they don't match, note this.

    ancestry_aliases = {
        "ASJ": ["Ashkenazi", "Ashkenazi Jewish", "AJ"],
        "EUR": ["European", "Northern European", "N. European", "Mediterranean", "Pan-ethnic"],
        "NFE": ["European", "Northern European", "N. European"],
        "AFR": ["African", "African Amer.", "African American"],
        "EAS": ["East Asian", "Asian"],
        "SAS": ["South Asian"],
        "FIN": ["Finnish", "European"],
    }

    sample_pops = ancestry_aliases.get(sample_ancestry, [sample_ancestry])

    for f in findings:
        variant_pop = f.get("population", "")
        pop_match = any(
            sp.lower() in variant_pop.lower()
            for sp in sample_pops
        ) or "pan-ethnic" in variant_pop.lower()

        if pop_match:
            f["ancestry_match"] = True
            f["ancestry_note"] = (
                f"Carrier frequency ({f.get('carrier_freq', 'unknown')}) is "
                f"for {variant_pop}, which matches the sample's ancestry ({sample_ancestry})."
            )
        else:
            f["ancestry_match"] = False
            f["ancestry_note"] = (
                f"Carrier frequency ({f.get('carrier_freq', 'unknown')}) is "
                f"reported for {variant_pop}. The sample's ancestry is {sample_ancestry} — "
                f"actual carrier frequency may differ."
            )

    return findings

Report Generation
JSON Output Schema
json{
  "sample_name": "SAMPLE_001",
  "date": "2026-03-30",
  "sample_ancestry": "ASJ",
  "diseases_screened": 9,
  "carriers_found": 2,
  "affected_found": 0,
  "findings": [
    {
      "disease": "Gaucher disease",
      "gene": "GBA1",
      "variant_name": "N370S",
      "rsid": "rs76763715",
      "hgvs_c": "c.1226A>G",
      "hgvs_p": "p.Asn370Ser",
      "coordinates": "chr1:155237290:A>G",
      "genotype": "0/1",
      "zygosity": "heterozygous",
      "status": "carrier",
      "carrier_freq": "1 in 15",
      "population": "Ashkenazi Jewish",
      "ancestry_match": true,
      "ancestry_note": "Carrier frequency (1 in 15) is for Ashkenazi Jewish, which matches the sample's ancestry.",
      "source": "key_variant",
      "clinical_summary": "Lysosomal storage disorder. Type 1 (non-neuronopathic) is most common in Ashkenazi Jewish. Enzyme replacement therapy available."
    }
  ],
  "negative_diseases": ["Cystic fibrosis", "Sickle cell disease", "PKU", "Beta-thalassemia", "Hemochromatosis", "SMA", "Pompe disease"],
  "detection_limitations": ["SMA (SMN1 exon 7 deletion) — requires copy number analysis, not reliably detected from SNV/indel calls"]
}
Markdown Report Structure
1. Summary Table
| Disease | Gene | Status | Variant | Zygosity |
|---------|------|--------|---------|----------|
| Gaucher disease | GBA1 | 🟡 CARRIER | N370S (rs76763715) | Het |
| Tay-Sachs disease | HEXA | 🟡 CARRIER | 1278insTATC | Het |
| Cystic fibrosis | CFTR | ✅ Not detected | — | — |
| Sickle cell disease | HBB | ✅ Not detected | — | — |
| ... | | | | |
Status icons: 🔴 AFFECTED (homozygous/compound het), 🟡 CARRIER (heterozygous),
✅ Not detected, ⚠️ Detection limited.
2. Detailed Findings — one section per positive finding with variant details,
carrier frequency in context of sample ancestry, reproductive implications,
and clinical summary.
3. Reproductive Risk Context — if the sample is a carrier, note that
partner testing for the same disease is recommended before conception.
If both partners are carriers for the same AR disease, each pregnancy has a 25%
chance of producing an affected child.
4. Negative Results — list all screened diseases with no findings.
5. Detection Limitations — explicitly list diseases where the standard
pipeline cannot reliably detect the causative mutation (SMA, alpha-thal, etc.).
6. Disclaimers:
IMPORTANT:
- Carrier screening checks for common known pathogenic variants and gene-level
  ClinVar pathogenic entries. Rare or novel variants may be missed.
- A negative result does not guarantee non-carrier status — it means the specific
  variants tested were not detected.
- Some diseases (SMA, alpha-thalassemia, Fragile X) require specialized assays
  not included in standard WGS variant calling.
- Carrier frequencies are population-specific. Values shown are from published
  literature and may not precisely apply to the sample's specific background.
- Clinical decisions about reproductive planning should involve a certified
  genetic counselor.

Error Handling
ErrorCauseResolutionKey variant position not in VCFNo coverage, or gVCF ref block with no ALTCheck BAM depth at position; if covered and ref/ref, report as not detectedVariant detected but low GQ (<20)Uncertain genotype callReport with warning: "low genotype quality — confirm with orthogonal method"rsID mismatchClinVar rsID points to different position than expectedAlways match on genomic coordinates, not rsIDGene sweep finds too many hits (>10)Likely annotation artifact or gene with many benign ClinVar entriesCheck CLNSIG carefully; only report P/LP with ≥1 star reviewHomozygous finding in a rare diseaseMay be affected, not just carrierElevate to urgent finding; recommend clinical follow-up