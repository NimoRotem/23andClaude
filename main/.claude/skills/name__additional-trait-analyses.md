name: additional-trait-analyses
description: |
  Genotype lifestyle and wellness trait markers from WGS/WES data across three
  categories: nutrigenomics (nutrient metabolism, dietary response), fitness and
  sports genetics (muscle type, injury risk, recovery), and sleep/circadian traits.
  Each marker is a single-variant or HLA-based lookup with genotype-specific
  interpretation. The marker list is extensible via JSON configuration. Use this
  skill when the user asks about diet genetics, nutrigenomics, sports/fitness
  genetics, sleep genetics, caffeine sensitivity, or lifestyle trait analysis.


# Additional Trait Analyses

## Overview

These analyses cover genetic variants that influence lifestyle-relevant traits —
how the body processes nutrients, responds to exercise, and regulates sleep. Unlike
disease-risk markers, these are generally low-stakes informational findings that
can guide personalized lifestyle choices but do not indicate disease.

**Important framing**: These are trait associations, not medical diagnoses. Effect
sizes are typically modest (10-40% differences in metabolism or response), and
environmental factors (diet, training, habits) usually dominate over genotype.
The report should convey actionable insight without overstating genetic determinism.

The pipeline:

1. Genotypes each marker from the sample VCF
2. Interprets genotype using a variant-specific effect table
3. Groups results by category (nutrigenomics, fitness, sleep)
4. Generates a report with practical recommendations per finding

---

## Category Definitions

### Nutrigenomics

Variants that affect how the body metabolizes nutrients, responds to dietary
components, or processes specific food groups. These inform dietary optimization
— not food allergies or intolerances (which are mostly non-genetic or polygenic).

### Fitness & Sports Genetics

Variants associated with muscle fiber composition, aerobic capacity, injury
predisposition, and recovery characteristics. These suggest training style
preferences, not athletic destiny. Genotype explains ~20-50% of variance in
these traits; training history, age, and other factors explain the rest.

### Sleep & Circadian

Variants that influence chronotype (morning/evening preference), sleep depth,
and sensitivity to stimulants. These can guide sleep hygiene and timing strategies.

---

## Marker List Format

Same JSON structure as the single-variant health markers skill. Each marker
defines its genotyping logic, per-genotype interpretation, and a practical
recommendation:

```json
[
  {
    "marker_name": "Folate metabolism (MTHFR C677T)",
    "gene": "MTHFR",
    "category": "nutrigenomics",
    "subcategory": "Vitamin/mineral metabolism",
    "genotyping": {
      "type": "single_snv",
      "variant": {
        "name": "rs1801133",
        "grch38": {"chr": "chr1", "pos": 11796321, "ref": "G", "alt": "A"},
        "risk_allele": "A",
        "note": "Ref=G (677C, normal), Alt=A (677T, thermolabile enzyme)"
      }
    },
    "interpretation": {
      "GG": {
        "label": "CC (normal)",
        "effect": "100% enzyme activity. Standard folate metabolism.",
        "level": "normal",
        "recommendation": "No specific dietary adjustment needed. Standard folate intake from diet or prenatal vitamins is sufficient."
      },
      "GA": {
        "label": "CT (heterozygous)",
        "effect": "~65% enzyme activity. Mildly reduced folate conversion.",
        "level": "mild",
        "recommendation": "Generally no clinical significance. Adequate dietary folate (leafy greens, legumes) is sufficient. Methylfolate (5-MTHF) supplements are an option but not required."
      },
      "AA": {
        "label": "TT (homozygous)",
        "effect": "~30% enzyme activity. Significantly reduced conversion of folic acid to active methylfolate.",
        "level": "notable",
        "recommendation": "Consider methylfolate (5-MTHF, 400-800 µg/day) instead of synthetic folic acid, which requires MTHFR for activation. Especially relevant during pregnancy planning. Ensure adequate B12 and B6 intake."
      }
    },
    "evidence_strength": "strong",
    "context": "MTHFR C677T is one of the most-studied nutrigenomic variants. The TT genotype reduces conversion of folic acid to its active form (5-methyltetrahydrofolate). This is clinically significant primarily in the context of pregnancy (neural tube defect risk) and hyperhomocysteinemia. The widespread marketing of MTHFR testing has led to over-interpretation — most TT carriers have no clinical issues with adequate dietary folate."
  },
  {
    "marker_name": "Muscle fiber type",
    "gene": "ACTN3",
    "category": "fitness",
    "subcategory": "Muscle composition",
    "genotyping": {
      "type": "single_snv",
      "variant": {
        "name": "rs1815739",
        "grch38": {"chr": "chr11", "pos": 66560624, "ref": "C", "alt": "T"},
        "risk_allele": "T",
        "note": "Ref=C (R allele, alpha-actinin-3 present), Alt=T (X allele, protein absent)"
      }
    },
    "interpretation": {
      "CC": {
        "label": "RR",
        "effect": "Alpha-actinin-3 present in fast-twitch (type II) muscle fibers. Favors power and sprint performance.",
        "level": "power",
        "recommendation": "Genotype associated with advantage in power/sprint sports (sprinting, jumping, weightlifting). Fast-twitch fibers respond well to high-intensity, explosive training. Does not preclude endurance ability."
      },
      "CT": {
        "label": "RX",
        "effect": "One functional copy. Mixed fiber profile.",
        "level": "mixed",
        "recommendation": "Balanced fast-twitch/slow-twitch profile. No strong genetic predisposition toward either power or endurance. Training type preference can be guided by individual response."
      },
      "TT": {
        "label": "XX",
        "effect": "Alpha-actinin-3 absent. Shift toward slow-twitch (type I) fiber characteristics. Favors endurance.",
        "level": "endurance",
        "recommendation": "Genotype associated with advantage in endurance sports (distance running, cycling, swimming). Slow-twitch fibers are more fatigue-resistant and efficient at aerobic metabolism. ~18% of Europeans are XX."
      }
    },
    "evidence_strength": "strong",
    "context": "ACTN3 R577X is the most replicated sports genetics variant. Alpha-actinin-3 deficiency (XX) is not a disease — it is present in ~18% of Europeans and ~25% of Asians. It represents a trade-off: power vs endurance, not ability vs disability. Many elite endurance athletes are XX; many elite sprinters are RR. Genotype is one factor among many."
  }
]
```

---

## Special Genotyping Cases

### HLA-DQ2/DQ8 (Celiac disease predisposition)

Celiac disease has a strong genetic component: ~95% of celiac patients carry
HLA-DQ2.5, and most of the remainder carry HLA-DQ8. However, ~30-40% of the
general population also carries these alleles — presence is necessary but not
sufficient for celiac disease.

HLA typing from WGS is more complex than single-SNV genotyping:

```json
{
  "marker_name": "Celiac disease predisposition (HLA-DQ2/DQ8)",
  "gene": "HLA-DQ",
  "category": "nutrigenomics",
  "subcategory": "Food sensitivity",
  "genotyping": {
    "type": "hla_proxy",
    "method": "Tag SNP proxies for HLA-DQ2.5 and HLA-DQ8. Full HLA typing from WGS is possible but requires specialized tools (HLA-LA, OptiType). Tag SNPs provide a reasonable approximation.",
    "variants": [
      {
        "name": "rs2187668",
        "grch38": {"chr": "chr6", "pos": 32605884, "ref": "T", "alt": "C"},
        "tags": "HLA-DQ2.5 (DQA1*05:01-DQB1*02:01)",
        "note": "Alt=C tags HLA-DQ2.5 haplotype with high sensitivity"
      },
      {
        "name": "rs7454108",
        "grch38": {"chr": "chr6", "pos": 32713862, "ref": "T", "alt": "C"},
        "tags": "HLA-DQ8 (DQA1*03-DQB1*03:02)",
        "note": "Alt=C tags HLA-DQ8 haplotype"
      }
    ]
  },
  "interpretation": {
    "dq2_positive": {
      "label": "HLA-DQ2.5 carrier",
      "effect": "Carries the primary genetic risk factor for celiac disease. ~95% of celiac patients are DQ2.5-positive.",
      "level": "predisposition",
      "recommendation": "Genetic predisposition to celiac disease is present, but ~30-40% of the general population carries DQ2.5 without ever developing celiac. If symptomatic (chronic GI issues, fatigue, iron deficiency), celiac-specific antibody testing (tTG-IgA) is warranted. A negative HLA-DQ2/DQ8 result effectively rules out celiac disease."
    },
    "dq8_positive": {
      "label": "HLA-DQ8 carrier",
      "effect": "Carries a secondary genetic risk factor for celiac disease.",
      "level": "mild_predisposition",
      "recommendation": "HLA-DQ8 confers lower celiac risk than DQ2.5. Same clinical guidance applies — test if symptomatic."
    },
    "negative": {
      "label": "DQ2/DQ8 negative",
      "effect": "Does not carry the primary genetic risk factors for celiac disease.",
      "level": "protective",
      "recommendation": "Celiac disease is extremely unlikely (<1% of celiac patients are DQ2/DQ8-negative). This is one of the few cases where a negative genetic result has strong negative predictive value."
    }
  },
  "evidence_strength": "strong",
  "context": "HLA-DQ2/DQ8 testing is the rare case in genetics where a NEGATIVE result is more informative than a positive one. Absence of DQ2 and DQ8 effectively rules out celiac disease (NPV >99%). Presence of DQ2/DQ8 only indicates susceptibility — most carriers never develop celiac. Tag SNP proxies are ~95% concordant with full HLA typing for DQ2.5 and ~90% for DQ8."
}
```

### ACE I/D polymorphism (Endurance/power)

The ACE insertion/deletion is a 287-bp Alu element in intron 16. It is NOT a
SNP and cannot be directly genotyped from a standard VCF. Options:

1. **Proxy SNP**: rs4340 or rs1799752 tags the I/D with ~95% accuracy in Europeans
2. **Read depth**: Count reads spanning the insertion breakpoint in the BAM
3. **Flag as undetectable**: If only VCF is available, note the limitation

```json
{
  "marker_name": "ACE I/D (Endurance capacity)",
  "gene": "ACE",
  "category": "fitness",
  "subcategory": "Endurance capacity",
  "genotyping": {
    "type": "single_snv",
    "variant": {
      "name": "rs4340",
      "grch38": {"chr": "chr17", "pos": 63488529, "ref": "A", "alt": "D"},
      "note": "Proxy SNP for the ACE I/D. May appear as a complex variant or indel in VCF. Check ALT field carefully."
    },
    "detection_caveat": "The ACE I/D is a 287-bp Alu insertion, not a SNP. Direct detection from short-read WGS is unreliable. The proxy SNP rs4340 provides ~95% concordance in Europeans. If the proxy is not found in the VCF, report as 'not assessable from available data.'"
  },
  "interpretation": {
    "II": {
      "label": "II (insertion/insertion)",
      "effect": "Lower ACE activity. Higher circulating bradykinin. Associated with endurance performance.",
      "level": "endurance",
      "recommendation": "Genotype associated with enhanced endurance exercise capacity and altitude acclimatization. May benefit more from aerobic/endurance training protocols."
    },
    "ID": {
      "label": "ID (heterozygous)",
      "effect": "Intermediate ACE activity. Balanced profile.",
      "level": "mixed",
      "recommendation": "Mixed endurance/power profile. Training response is not strongly biased."
    },
    "DD": {
      "label": "DD (deletion/deletion)",
      "effect": "Higher ACE activity. Associated with power/strength performance and greater muscle strength gains.",
      "level": "power",
      "recommendation": "Genotype associated with greater strength gains from resistance training. May have enhanced anaerobic capacity."
    }
  },
  "evidence_strength": "moderate",
  "context": "The ACE I/D is one of the earliest sports genetics findings (1998). Effect sizes are modest and the association has not replicated consistently across all populations. It should not be used to make athletic selection decisions."
}
```

---

## Pipeline Steps

### Step 1: Extract Genotypes

Identical to single-variant health markers — query the VCF at all marker
positions, then genotype each marker:

```bash
SAMPLE_VCF="$1"
MARKER_LIST="$2"  # JSON file with all markers across all categories
TMPDIR=$(mktemp -d)

# Build positions file
python3 extract_positions.py "$MARKER_LIST" > "$TMPDIR/positions.tsv"

# Query VCF
bcftools query \
  -f '%CHROM\t%POS\t%REF\t%ALT\t[%GT]\t[%GQ]\t[%DP]\n' \
  -T "$TMPDIR/positions.tsv" \
  "$SAMPLE_VCF" > "$TMPDIR/calls.tsv"
```

### Step 2: Genotype and Interpret

Use the same genotyping functions from the single-variant markers skill.
The `single_snv` type handles most markers. Special cases:

- **HLA-DQ2/DQ8**: Use `hla_proxy` type — check two tag SNPs, classify as
  DQ2-positive, DQ8-positive, or negative
- **ACE I/D**: Use proxy SNP with detection caveat

```python
def genotype_additional_markers(calls_file, marker_list):
    """Genotype all markers and return categorized results."""
    results = {"nutrigenomics": [], "fitness": [], "sleep": []}

    for marker in marker_list:
        gt_type = marker["genotyping"]["type"]

        if gt_type == "single_snv":
            result = genotype_single_snv(marker, calls)
        elif gt_type == "hla_proxy":
            result = genotype_hla_proxy(marker, calls)
        else:
            result = {"error": f"Unknown type: {gt_type}"}

        result["marker_name"] = marker["marker_name"]
        result["gene"] = marker["gene"]
        result["subcategory"] = marker.get("subcategory", "")
        result["evidence_strength"] = marker.get("evidence_strength", "moderate")
        result["context"] = marker.get("context", "")
        result["recommendation"] = result.get("recommendation", "")

        category = marker.get("category", "other")
        results.setdefault(category, []).append(result)

    return results


def genotype_hla_proxy(marker, calls):
    """Genotype HLA-DQ2/DQ8 from tag SNPs."""
    variants = marker["genotyping"]["variants"]
    interp = marker["interpretation"]

    dq2_snp = next((v for v in variants if "DQ2" in v.get("tags", "")), None)
    dq8_snp = next((v for v in variants if "DQ8" in v.get("tags", "")), None)

    dq2_positive = False
    dq8_positive = False

    if dq2_snp:
        pos = dq2_snp["grch38"]
        call = calls.get((pos["chr"], pos["pos"]))
        if call and call["gt"] not in ("0/0", "0|0"):
            dq2_positive = True

    if dq8_snp:
        pos = dq8_snp["grch38"]
        call = calls.get((pos["chr"], pos["pos"]))
        if call and call["gt"] not in ("0/0", "0|0"):
            dq8_positive = True

    if dq2_positive:
        result = dict(interp["dq2_positive"])
        result["genotype"] = "DQ2.5 positive"
        if dq8_positive:
            result["genotype"] = "DQ2.5 + DQ8 positive"
    elif dq8_positive:
        result = dict(interp["dq8_positive"])
        result["genotype"] = "DQ8 positive"
    else:
        result = dict(interp["negative"])
        result["genotype"] = "DQ2/DQ8 negative"

    result["proxy_based"] = True
    result["proxy_note"] = "Result based on tag SNP proxies, not full HLA typing. ~95% concordance for DQ2.5, ~90% for DQ8."

    return result
```

### Step 3: Add Evidence Strength Context

Each marker has an `evidence_strength` field that the report uses to calibrate
how confidently to present the finding:

| Strength | Meaning | Report framing |
|----------|---------|----------------|
| `strong` | Replicated in large GWAS, clear biological mechanism | "Well-established genetic association" |
| `moderate` | Replicated in some studies, plausible mechanism | "Moderate evidence supports this association" |
| `preliminary` | Single study or inconsistent replication | "Preliminary finding — more research needed" |

---

## Report Generation

### JSON Output Schema

```json
{
  "sample_name": "SAMPLE_001",
  "date": "2026-03-30",
  "categories": {
    "nutrigenomics": {
      "markers_checked": 7,
      "results": [
        {
          "marker_name": "Folate metabolism (MTHFR C677T)",
          "gene": "MTHFR",
          "genotype": "AA",
          "label": "TT (homozygous)",
          "effect": "~30% enzyme activity",
          "level": "notable",
          "recommendation": "Consider methylfolate instead of folic acid...",
          "evidence_strength": "strong"
        }
      ]
    },
    "fitness": {
      "markers_checked": 5,
      "results": []
    },
    "sleep": {
      "markers_checked": 3,
      "results": []
    }
  }
}
```

### Markdown Report Structure

**1. Category: Nutrigenomics**

```
## Nutrigenomics

How your genetics may influence nutrient metabolism and dietary response.

| Marker | Gene | Genotype | Effect | Action |
|--------|------|----------|--------|--------|
| Folate metabolism | MTHFR | TT | 30% enzyme activity | 🟡 Consider methylfolate |
| Omega-3 conversion | FADS1/2 | TC | Reduced conversion | 🟡 Direct fish oil may help |
| Vitamin D | GC/VDBP | AA | Normal binding | 🟢 Standard supplementation |
| Salt sensitivity | AGT | AG | Mild sensitivity | 🟡 Moderate sodium intake |
| Celiac (HLA-DQ) | HLA-DQ | DQ2.5+ | Predisposition present | 🟡 Test if symptomatic |
| Saturated fat response | APOA2 | CC | Higher BMI with sat fat | 🟡 Limit saturated fat |
| Melatonin/glucose | MTNR1B | CG | Mild effect | 🟢 Avoid very late eating |
```

Level indicators: 🟢 Normal/no action, 🟡 Consider adjustment, 🔴 Strong recommendation.

**2. Category: Fitness & Sports Genetics**

```
## Fitness & Sports Genetics

Genetic factors that may influence athletic performance characteristics.
These are tendencies, not destinies — training, nutrition, and consistency
matter far more than any single gene variant.

| Trait | Gene | Genotype | Profile | Suggestion |
|-------|------|----------|---------|------------|
| Muscle fiber type | ACTN3 | RR | Power-oriented | Responds well to HIIT/strength |
| Endurance capacity | ACE | ID | Mixed | Balanced training approach |
| Tendon injury risk | COL5A1 | CT | Average risk | Standard warm-up/recovery |
| Recovery speed | IL6 | GG | Slower recovery | Prioritize rest days |
| VO2max trainability | CKM | GA | Moderate response | Consistent training yields gains |
```

**3. Category: Sleep & Circadian**

```
## Sleep & Circadian Genetics

Variants that influence your body's internal clock, sleep depth,
and stimulant sensitivity.

| Trait | Gene | Genotype | Effect | Suggestion |
|-------|------|----------|--------|------------|
| Chronotype | CRY1 | Normal | No delayed phase variant | Standard sleep timing |
| Deep sleep quality | ADA | GA | Somewhat deeper sleep | — |
| Caffeine sensitivity | ADORA2A | TT | High sensitivity | Limit caffeine after noon |
```

**4. Evidence and Limitations Section**

```
## About These Results

### Evidence strength
Each marker is rated by the strength of its scientific evidence:
- **Strong**: Replicated in large studies with clear biological mechanism
- **Moderate**: Supported by multiple studies but with some inconsistency
- **Preliminary**: Early-stage finding that needs more research

### Important context
- Genetic variants explain only part of the picture. Diet, exercise habits,
  environment, microbiome, and other factors often have larger effects.
- "Associated with" does not mean "caused by." These are statistical
  associations observed in population studies.
- Most findings are from European-ancestry cohorts. Effect sizes and allele
  frequencies may differ in other populations.
- These results are informational. They are not medical advice and should
  not replace guidance from healthcare professionals or registered dietitians.
- Sports genetics findings should not be used for talent selection or
  exclusion — they describe tendencies at the population level, not
  individual athletic potential.
```

---

## Error Handling

| Error | Cause | Resolution |
|-------|-------|------------|
| Marker position not in VCF | No coverage or no variant at site | Assume homozygous reference if sample has adequate overall coverage; note as "inferred" |
| ACE I/D proxy not found | Proxy SNP not called; indel may be filtered | Report as "not assessable — proxy SNP not detected in VCF. Consider targeted testing." |
| HLA-DQ tag SNP discordant with clinical HLA typing | Tag SNP proxy is ~90-95% accurate, not perfect | Note proxy-based limitation; recommend clinical HLA typing for definitive celiac workup |
| Low GQ (<20) at a trait marker | Unreliable call | Report with caveat: "Low genotype quality — interpret with caution" |
| Allele frequency doesn't match expected for ancestry | Population-specific frequency differences | Note that listed frequencies are EUR-centric; actual frequency may vary |

---

## Extending the Marker List

To add a new marker, append a JSON entry with these fields:

```json
{
  "marker_name": "Human-readable name",
  "gene": "GENE_SYMBOL",
  "category": "nutrigenomics|fitness|sleep",
  "subcategory": "Optional grouping",
  "genotyping": {
    "type": "single_snv",
    "variant": {
      "name": "rsXXXXXX",
      "grch38": {"chr": "chrN", "pos": 12345678, "ref": "A", "alt": "G"},
      "risk_allele": "G"
    }
  },
  "interpretation": {
    "AA": {"label": "...", "effect": "...", "level": "normal", "recommendation": "..."},
    "AG": {"label": "...", "effect": "...", "level": "mild", "recommendation": "..."},
    "GG": {"label": "...", "effect": "...", "level": "notable", "recommendation": "..."}
  },
  "evidence_strength": "strong|moderate|preliminary",
  "context": "Brief scientific context and caveats."
}
```

The pipeline handles it automatically — no code changes needed.
Coordinates must be GRCh38. Verify rsID → position mapping at dbSNP
(https://www.ncbi.nlm.nih.gov/snp/) before adding.