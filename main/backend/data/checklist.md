# Comprehensive Genomics Analysis Master Checklist

> A detailed catalog of every analysis that can be performed on whole-genome sequencing (WGS) data. Each entry includes specific identifiers, tools, variant counts, and performance metrics. Sections with multiple PGS options list the recommended score first, with alternatives indented below.
>
> **Last updated**: 2026-03-31

---

## 0. Sample QC, Sex Check & Ancestry

Run these first for every sample before any downstream analysis. These establish ground truth about each sample's quality, biological sex, and genetic ancestry — which determine the correct reference population for PGS scoring.

### Sex / Gender Verification

| Done | Check | Method | Expected Result |
|:----:|-------|--------|-----------------|
| [ ] | **Sex verification pipeline** | 5-check consensus: MSY reads, SRY gene, X:Y ratio, chrX het rate, chrY variants | XX (female) or XY (male) with confidence level |

### Sample Quality & Contamination

| Done | Check | Method | Pass Criteria |
|:----:|-------|--------|---------------|
| [ ] | **Flagstat summary** | samtools flagstat: alignment QC metrics | Check mapped %, paired %, duplicates |
| [ ] | **Ti/Tv ratio** | bcftools stats: transition/transversion ratio | WGS: 2.0-2.1 |
| [ ] | **Het/Hom ratio** | bcftools stats: heterozygous/homozygous ratio | ~1.5-2.0 for outbred |
| [ ] | **SNP count** | bcftools: count SNPs | WGS: 3.5-4.5M SNPs |
| [ ] | **Indel count** | bcftools: count indels | WGS: 500K-800K |
| [ ] | **Duplicate read rate** | samtools flagstat: duplicate fraction | <10% for WGS |
| [ ] | **Mapped read %** | samtools flagstat: mapping rate | >95% for good WGS |

### Ancestry & Population Assignment

| Done | Check | Method | Output |
|:----:|-------|--------|--------|
| [ ] | **Ancestry pipeline** | PCA + NNLS + KNN via 1000G reference (FRAPOSA/ADMIXTURE skill) | Continental admixture %, sub-population affinity, PCA coordinates, PGS reference population assignment |

---

## 1. Polygenic Scores -- Cancer

### Breast cancer
> Breast cancer develops from breast tissue, most commonly from the inner lining of milk ducts or lobules. It is one of the most common cancers in women, with genetic factors (BRCA1/2) playing a significant role in hereditary cases. Polygenic risk adds to monogenic risk and can stratify screening recommendations.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **PGS004153** | 1,127,015 | AUROC 0.663, OR 1.83/SD | Monti R et al., *AJHG* 2024 | EUR |
| [ ] | PGS005349 | 5,438,842 | AUROC 0.647, C-index 0.66 | Tanha HM et al., *EJHG* 2026 | Multi (SBayesRC) |
| [ ] | PGS002242 | 6,510,869 | OR 1.80, HR 1.71 | Mars N et al., *Cell Genom* 2022 | EUR |
| [ ] | PGS000335 | 1,079,089 | OR 1.80, HR 1.71 (EUR) | Mars N et al., *Nat Commun* 2020 | EUR |
| [ ] | PGS005378 (African ancestry) | 2,300,000 | OR 1.34/SD (AFR) | Li B et al., *Nat Genet* 2026 | AFR |
| [ ] | PGS000004 | 313 | AUROC 0.63, OR 1.61 | Mavaddat N et al., *AJHG* 2018 | EUR (158K) |
| [ ] | PGS000005 (ER-negative) | 313 | AUROC 0.60, OR 1.45 | Mavaddat N et al., *AJHG* 2018 | EUR |
| [ ] | PGS000006 (ER-positive) | 313 | AUROC 0.65, OR 1.68 | Mavaddat N et al., *AJHG* 2018 | EUR |
| [ ] | PGS005382 (ER-neg African) | ~2,300,000 | African-specific ER-neg | Li B et al., *Nat Genet* 2026 | AFR |
| [ ] | PGS005387 (Triple-neg African) | 162 | African-specific TNBC | Li B et al., *Nat Genet* 2026 | AFR |

### Prostate cancer
> The second most common cancer in men worldwide. Highly heritable (~57% from genetic factors). PGS can identify men at >4x average risk who may benefit from early PSA screening.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **PGS003766** | 451 | OR/SD 2.21-2.32, multi-ancestry | Wang A et al., *Nat Genet* 2023 | Multi |
| [ ] | PGS005241 | 3,800,000 | AUROC 0.805 (SAS), multi-ancestry | Tanha HM et al., *HGG Adv* 2025 | Multi (SBayesRC) |
| [ ] | PGS000333 | 6,606,785 | C-index 0.866 | Mars N et al., *Nat Med* 2020 | EUR |
| [ ] | PGS000662 | 269 | AUROC 0.833, OR 4.17 | Conti DV et al., *Nat Genet* 2021 | Multi (234K) |
| [ ] | PGS003765 (EUR-specific) | 451 | OR/SD 2.21 (EUR) | Wang A et al., *Nat Genet* 2023 | EUR |
| [ ] | PGS000067 (hazard) | 54 | HR 2.9 (top 2%) | Seibert TM et al., *BMJ* 2018 | EUR (31.7K) |
| [ ] | PGS000663 (aggressive) | 269 | OR 5.54 (top vs bottom decile) | Conti DV et al., *Nat Genet* 2021 | Multi |

### Colorectal cancer
> Third most common cancer globally. Early-onset cases (<50y) are increasing. PGS can help identify individuals for earlier colonoscopy screening. Genome-wide scores capture more risk than SNP-count-limited versions.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **PGS003852** | 1,000,000 | OR 1.67, multi-ancestry | Thomas M et al., *Nat Commun* 2023 | Multi (255K) |
| [ ] | PGS003979 | ~1,000,000 | AUROC 0.795 (Finnish) | Tamlander M et al., *Br J Cancer* 2024 | EUR |
| [ ] | PGS004904 (early-onset) | ~200 | OR 2.51 (top vs bottom decile) | Jiang L et al., *Int J Cancer* 2023 | EUR |
| [ ] | PGS004580 (genome-wide) | 1,099,906 | OR 1.50/SD | Youssef O et al., *Lab Invest* 2024 | EUR (93K) |
| [ ] | PGS004586 (prognostic/survival) | ~200 | HR 1.34 (survival) | Xin J et al., *Nat Commun* 2024 | EUR |
| [ ] | PGS003850 | 205 | OR 1.62, AUROC 0.61 | Fernandez-Rozadilla C et al., *Nat Genet* 2022 | Multi (255K) |
| [ ] | PGS000055 (early-onset) | 95 | OR 2.10 (top vs bottom quintile) | Huyghe JR et al., *Nat Genet* 2019 | EUR |

### Lung cancer
> Leading cause of cancer death. PGS can identify never-smokers at elevated genetic risk and stratify low-dose CT screening eligibility among smokers. Subtype-specific scores exist for squamous and adenocarcinoma.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **PGS004860** | 1,100,000 | Genome-wide, multi-ancestry | Boumtje L et al., *EBioMedicine* 2024 | Multi |
| [ ] | PGS003393 (adenocarcinoma) | ~144,000 | AUROC 0.743 | Namba S et al., *Cancer Res* 2022 | Multi |
| [ ] | PGS003392 (squamous) | ~144,000 | AUROC 0.778 | Namba S et al., *Cancer Res* 2022 | Multi |
| [ ] | PGS005169 (never-smoker EAS) | ~1,000,000 | EAS never-smoker specific | Blechter B et al., *JAMA Netw Open* 2023 | EAS |
| [ ] | PGS000078 | 109 | AUROC 0.846, HR 1.26 | Graff RE et al., *Nat Commun* 2021 | EUR (184K) |
| [ ] | PGS000082 (squamous) | 109 | AUROC 0.74 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | PGS000081 (adenocarcinoma) | 109 | AUROC 0.80 | Graff RE et al., *Nat Commun* 2021 | EUR |

### Ovarian cancer
> Often diagnosed late due to lack of screening. Genetic risk (BRCA1/2, Lynch syndrome) is a major factor. PGS can supplement monogenic testing for risk stratification.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **PGS003385** | 144,000 | AUROC 0.717 | Namba S et al., *Cancer Res* 2022 | Multi |
| [ ] | PGS005086 | 64,518 | AUROC 0.607, OR 1.46 | Barnes DR et al., *NPJ Genom Med* 2025 | EUR (151K) |
| [ ] | PGS005166 (EAS) | ~64,000 | EAS-specific | Zhu M et al., *PLoS Med* 2025 | EAS |
| [ ] | PGS000049 (serous) | 30 | OR 1.55 | Phelan CM et al., *Nat Genet* 2017 | EUR |

### Pancreatic cancer
> One of the deadliest cancers with ~10% 5-year survival. Early detection is critical. Combined PGS + clinical factors reach AUROC 0.83.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **PGS002264** | 49 | AUROC 0.605, 0.83 (w/clinical) | Sharma S et al., *Gastroenterology* 2022 | EUR (436K) |
| [ ] | PGS002740 | ~50 | OR 6.91 (top vs bottom, age≤60) | Yuan C et al., *Ann Oncol* 2022 | EUR |
| [ ] | PGS000794 | 22 | AUROC 0.745 (w/covariates) | Kachuri L et al., *Nat Commun* 2020 | EUR |

### Melanoma
> Most dangerous skin cancer. Strongly influenced by UV exposure and pigmentation genetics (MC1R). PGS can identify those at >5x risk who need aggressive sun protection and skin checks.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **PGS002247** | 68 | AUROC 0.685-0.691, HR 1.80 | Steinberg J et al., *Br J Dermatol* 2021 | EUR |
| [ ] | PGS004886 | 692,000 | Genome-wide, multi-trait | Jermy B et al., *Nat Commun* 2024 | EUR |
| [ ] | PGS000743 | 45 | AUROC 0.74, OR 5.88 (decile) | Cust AE et al., *J Invest Dermatol* 2018 | EUR (36K) |
| [ ] | PGS000636 (genome-wide) | 954 | OR 3.21 (top 1%) | Fritsche LG et al., *AJHG* 2020 | EUR |

### Other cancers

| Done | Condition | PGS ID | Variants | Performance | Study | Pop |
|:----:|-----------|--------|----------|-------------|-------|-----|
| [ ] | **Bladder cancer** | PGS000782 | 15 | AUROC 0.804 | Kachuri L et al., *Nat Commun* 2020 | EUR |
| [ ] | Bladder cancer | PGS000071 | 15 | AUROC 0.803 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | **Kidney cancer (RCC)** | PGS004908 | 107 | AUROC 0.74 | Purdue MP et al., *Nat Genet* 2024 | EUR |
| [ ] | Kidney cancer (RCC) | PGS000787 | 19 | AUROC 0.722 | Kachuri L et al., *Nat Commun* 2020 | EUR |
| [ ] | **Testicular cancer** | PGS000796 | 52 | AUROC 0.69 | Kachuri L et al., *Nat Commun* 2020 | EUR |
| [ ] | Testicular cancer | PGS000086 | 52 | AUROC 0.783 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | **Thyroid cancer** | PGS004954 | 26 | AUROC 0.70, multi-ancestry | Pozdeyev N et al., *J Clin Endocrinol Metab* 2024 | Multi |
| [ ] | Thyroid cancer | PGS000636 | 954 | AUROC 0.578 | Fritsche LG et al., *AJHG* 2020 | EUR |
| [ ] | **Basal cell carcinoma** | PGS004592 | 78 | AUROC 0.74 | Liyanage UE et al., *J Eur Acad Dermatol* 2022 | EUR |
| [ ] | Basal cell carcinoma | PGS000119 | 32 | OR 1.65, AUROC 0.64 | Fritsche LG et al., *PLoS Genet* 2019 | EUR |
| [ ] | **Squamous cell carcinoma** | PGS004592 | 78 | AUROC 0.74 (keratinocyte) | Liyanage UE et al., *J Eur Acad Dermatol* 2022 | EUR |
| [ ] | Squamous cell carcinoma | PGS000072 | 15 | AUROC 0.77 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | **Gastric cancer** | PGS005161 | 12 | HR 1.27/SD | Zhu M et al., *PLoS Med* 2025 | EAS |
| [ ] | **Endometrial cancer** | PGS003381 | 529,000 | AUROC 0.761 | Namba S et al., *Cancer Res* 2022 | Multi |
| [ ] | Endometrial cancer | PGS002735 | 19 | AUROC 0.56, OR 1.55 | Bafligil C et al., *Genet Med* 2022 | EUR |
| [ ] | **Glioma / brain cancer** | PGS003384 | 910 | AUROC 0.758 | Namba S et al., *Cancer Res* 2022 | Multi |
| [ ] | Glioma / brain cancer | PGS000073 | 15 | AUROC 0.69 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | **Non-Hodgkin lymphoma** | PGS004248 | 20 | Multi-subtype | Kim WJ et al., *NPJ Precis Oncol* 2023 | EUR |
| [ ] | Non-Hodgkin lymphoma | PGS000075 | 15 | AUROC 0.73 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | **CLL** | PGS000874 | 41 | AUROC 0.79 | Kleinstern G et al., *Blood* 2018 | EUR |
| [ ] | CLL | PGS003453 | 43 | Updated CLL-specific | Berndt SI et al., *Leukemia* 2022 | EUR |
| [ ] | CLL | PGS000076 | 15 | AUROC 0.83 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | **Multiple myeloma** | PGS002281 | 23 | AUROC 0.644, OR 3.18 (quintile) | Canzian F et al., *EJHG* 2021 | EUR |
| [ ] | Multiple myeloma | PGS000074 | 15 | AUROC 0.72 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | **Esophageal cancer** | PGS003387 | 601,000 | AUROC 0.819 (adenocarcinoma) | Namba S et al., *Cancer Res* 2022 | Multi |
| [ ] | Esophageal cancer | PGS000070 | 15 | AUROC 0.71 | Graff RE et al., *Nat Commun* 2021 | EUR |
| [ ] | **Cervical cancer** | PGS001299 | 24 | AUROC 0.77-0.92 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | Cervical cancer | PGS001286 | 73 | AUROC 0.58 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | **Hepatocellular carcinoma** | PGS000872 | 5 | OR 3.4-11.9 (NAFLD/cirrhosis) | Bianco C et al., *J Hepatol* 2020 | EUR |
| [ ] | Hepatocellular carcinoma | PGS002254 | 8 | HR 1.33 | Sarin SK et al., *Hepatology* 2022 | Multi |

---

## 2. Polygenic Scores -- Cardiovascular

### Coronary artery disease (CAD)
> The leading cause of death worldwide. PGS for CAD is one of the best-validated, with top-decile individuals at 3-4x average risk. Equivalent clinical utility to monogenic FH in some studies. Multi-ancestry versions now available.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS003725](https://www.pgscatalog.org/score/PGS003725/)** (multi-ancestry) | 1,296,172 | HR 1.75, OR 2.14 | Patel AP et al., *Nat Med* 2023 | Multi |
| [ ] | [PGS005091](https://www.pgscatalog.org/score/PGS005091/) (JAMA 2024) | 1,428,772 | OR 1.45, AUROC 0.776-0.800 | Abramowitz SA et al., *JAMA* 2024 | Multi |
| [ ] | [PGS005112](https://www.pgscatalog.org/score/PGS005112/) (EAS+EUR) | 1,106,628 | OR 1.46 (EUR) | Loesch DP et al., *Nat Commun* 2025 | Multi |
| [ ] | [PGS004696](https://www.pgscatalog.org/score/PGS004696/) (SAS-strong) | 1,289,980 | OR 1.65 (EUR), OR 2.67 (SAS) | Smith JL et al., *Circ Genom Precis Med* 2024 | Multi |
| [ ] | [PGS000018](https://www.pgscatalog.org/score/PGS000018/) (metaGRS) | 1,745,179 | AUROC 0.79, HR 1.71/SD | Inouye M et al., *JACC* 2018 | EUR (382K) |
| [ ] | [PGS000013](https://www.pgscatalog.org/score/PGS000013/) (GPS) | 6,630,150 | AUROC 0.81 | Khera AV et al., *Nat Genet* 2018 | EUR (120K) |
| [ ] | [PGS002297](https://www.pgscatalog.org/score/PGS002297/) (LDpred2) | 1,259,754 | AUROC 0.78 | Ge T et al., *Genome Med* 2022 | Multi |

### Atrial fibrillation
> Most common sustained cardiac arrhythmia, affecting ~2% of adults. Increases stroke risk 5-fold. PGS can identify young individuals at elevated risk before symptoms appear.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS005168](https://www.pgscatalog.org/score/PGS005168/)** (Nat Genet 2025) | 382,963 | HR 1.67, C-index 0.87 | Roselli C et al., *Nat Genet* 2025 | Multi |
| [ ] | [PGS005313](https://www.pgscatalog.org/score/PGS005313/) (PRS-CSx) | 1,271,239 | OR 1.82, AUROC 0.78 | Yuan S et al., *Nat Commun* 2025 | Multi |
| [ ] | [PGS000016](https://www.pgscatalog.org/score/PGS000016/) (GPS) | 6,730,541 | AUROC 0.78 | Khera AV et al., *Nat Genet* 2018 | Multi |
| [ ] | [PGS004878](https://www.pgscatalog.org/score/PGS004878/) (INTERVENE) | 785,779 | HR 1.56-1.68 (7 biobanks) | Jermy B et al., *Nat Commun* 2024 | EUR |
| [ ] | [PGS003724](https://www.pgscatalog.org/score/PGS003724/) (multi-ancestry) | 1,296,172 | HR 1.60 | Patel AP et al., *Nat Med* 2023 | Multi |
| [ ] | [PGS000035](https://www.pgscatalog.org/score/PGS000035/) (focused) | 97 | AUROC 0.74 | Khera AV et al., *Circ* 2018 | EUR |

### Heart failure
> A syndrome where the heart cannot pump enough blood. Multiple genetic subtypes including dilated cardiomyopathy. New 2025 multi-ancestry PGS with 2.3M individuals.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS005097](https://www.pgscatalog.org/score/PGS005097/)** | 1,274,692 | AUROC 0.72 | Lee DSM et al., *Nat Genet* 2025 | Multi (2.3M) |
| [ ] | [PGS005285](https://www.pgscatalog.org/score/PGS005285/) (EAS) | 993,899 | R2=0.074 (EAS) | Enzan N et al., *Nat Commun* 2025 | Multi |
| [ ] | [PGS005073](https://www.pgscatalog.org/score/PGS005073/) (All of Us) | 1,286,612 | C-index 0.72-0.79 | Gunn S et al., *HGG Adv* 2024 | Multi |
| [ ] | [PGS001790](https://www.pgscatalog.org/score/PGS001790/) (GBMI) | 910,146 | AUROC 0.75 | Wang Y et al., *Cell Genomics* 2023 | Multi |
| [ ] | [PGS001236](https://www.pgscatalog.org/score/PGS001236/) (dilated CM) | 1,138 | AUROC 0.61 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |

### Stroke
> Second leading cause of death globally. Ischemic stroke (85% of cases) has the strongest genetic component. The GIGASTROKE consortium produced landmark multi-ancestry scores.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS002724](https://www.pgscatalog.org/score/PGS002724/)** (ischemic) | 1,213,574 | HR 1.19, C-index 0.645 | Mishra A et al., *Nature* 2022 | Multi (12.8M) |
| [ ] | [PGS002725](https://www.pgscatalog.org/score/PGS002725/) (integrative iPGS) | 6,010,730 | OR 1.18-1.33 | Mishra A et al., *Nature* 2022 | Multi |
| [ ] | [PGS001793](https://www.pgscatalog.org/score/PGS001793/) (GBMI) | 910,099 | AUROC 0.71 (EUR), 0.75 (Asian) | Wang Y et al., *Cell Genomics* 2023 | Multi |
| [ ] | [PGS004835](https://www.pgscatalog.org/score/PGS004835/) (PRSmix) | 2,263,784 | Incr. R2=0.007 | Truong B et al., *Cell Genomics* 2024 | EUR |
| [ ] | [PGS000039](https://www.pgscatalog.org/score/PGS000039/) (all subtypes) | 3,200,000 | AUROC 0.64 | Abraham G et al., *Circ GMP* 2019 | EUR |

### VTE (venous thromboembolism)
> Deep vein thrombosis and pulmonary embolism. Strong genetic component (Factor V Leiden, prothrombin). PGS adds polygenic background risk beyond known single variants. Top 5% have ~3x risk.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS003332](https://www.pgscatalog.org/score/PGS003332/)** (genome-wide) | 1,092,045 | OR 1.51/SD, AUROC 0.68 | Ghouse J et al., *Nat Genet* 2023 | EUR (1.06M) |
| [ ] | [PGS004854](https://www.pgscatalog.org/score/PGS004854/) (PRSmixPlus) | 2,268,993 | Incr. R2=0.058 | Truong B et al., *Cell Genomics* 2024 | EUR |
| [ ] | [PGS001796](https://www.pgscatalog.org/score/PGS001796/) (multi-ancestry) | 910,337 | AUROC 0.675 (EUR), 0.672 (AFR) | Wang Y et al., *Cell Genomics* 2023 | Multi |
| [ ] | [PGS000043](https://www.pgscatalog.org/score/PGS000043/) (focused) | 297 | OR 2.89 (top 5%) | Klarin D et al., *Nat Genet* 2019 | Multi (650K) |

### Other cardiovascular

| Done | Condition | PGS ID | Variants | Performance | Study | Pop |
|:----:|-----------|--------|----------|-------------|-------|-----|
| [ ] | **Dilated cardiomyopathy** | [PGS004946](https://www.pgscatalog.org/score/PGS004946/) | 1,098,677 | OR 1.66/SD, AUC 0.65 | Jurgens SJ et al., *Nat Genet* 2024 | Multi |
| [ ] | **PAD** | [PGS005217](https://www.pgscatalog.org/score/PGS005217/) | 1,296,292 | OR 1.66, C-index 0.731 | Flores AM et al., *JAMA Cardiol* 2025 | EUR |
| [ ] | **Aortic aneurysm (AAA)** | [PGS003972](https://www.pgscatalog.org/score/PGS003972/) | 1,118,997 | AUROC 0.64-0.69 | Roychowdhury T et al., *Nat Genet* 2023 | Multi |
| [ ] | Aortic aneurysm (AAA) | [PGS003429](https://www.pgscatalog.org/score/PGS003429/) (shaPRS) | 831,447 | AUROC 0.708 | Kelemen M et al., *Nat Commun* 2024 | EUR |
| [ ] | **Aortic stenosis** | [PGS005252](https://www.pgscatalog.org/score/PGS005252/) | 1,119,377 | HR 1.92, C-index 0.87 | Small AM et al., *Nat Genet* 2025 | EUR |
| [ ] | Aortic stenosis | [PGS001285](https://www.pgscatalog.org/score/PGS001285/) | 11,285 | AUROC 0.63 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | **Hypertension** | [PGS004192](https://www.pgscatalog.org/score/PGS004192/) | 9,430 | AUROC 0.703 | Raben TG et al., *Sci Rep* 2023 | EUR |
| [ ] | Hypertension (PRSmix) | [PGS004785](https://www.pgscatalog.org/score/PGS004785/) | 1,170,615 | Incr. R2=0.066 | Truong B et al., *Cell Genomics* 2024 | EUR |
| [ ] | **Systolic BP** | [PGS004603](https://www.pgscatalog.org/score/PGS004603/) | 7,356,519 | R2=0.114; 16.85 mmHg diff | Keaton JM et al., *Nat Genet* 2024 | EUR+AFR |
| [ ] | Systolic BP | [PGS002611](https://www.pgscatalog.org/score/PGS002611/) | 1,103,034 | R2=0.108 | Weissbrod O et al., *Nat Genet* 2022 | EUR |
| [ ] | **Diastolic BP** | [PGS002610](https://www.pgscatalog.org/score/PGS002610/) | 1,103,034 | R2=0.080 | Weissbrod O et al., *Nat Genet* 2022 | EUR |
| [ ] | **Hypertrophic CM** | [PGS001284](https://www.pgscatalog.org/score/PGS001284/) | 4,236 | AUROC 0.61 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | **Resting heart rate** | [PGS002603](https://www.pgscatalog.org/score/PGS002603/) | 1,060,971 | R2=0.041 | Weissbrod O et al., *Nat Genet* 2022 | EUR |

---

---

## 3. Polygenic Scores -- Metabolic & Endocrine

### Type 2 diabetes
> Affects ~10% of adults globally. PGS captures polygenic risk beyond single-gene causes (e.g., MODY). Multi-ancestry PGS now performs well across EUR, EAS, AFR, and SAS populations. Partitioned scores can distinguish beta-cell vs insulin resistance subtypes.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS004923](https://www.pgscatalog.org/score/PGS004923/)** (metaGRS) | 1,349,896 | AUROC 0.777 (EUR), 0.725 (AFR) | Ritchie SC et al., *medRxiv* 2024 | Multi |
| [ ] | [PGS003867](https://www.pgscatalog.org/score/PGS003867/) (multi-ancestry) | 1,068,166 | AUROC 0.73 (EUR), 0.776 (HIS) | Shim I et al., *Nat Commun* 2023 | Multi |
| [ ] | [PGS002308](https://www.pgscatalog.org/score/PGS002308/) (LDpred2) | 1,259,754 | AUROC 0.793 (EUR), 0.81 (EAS) | Ge T et al., *Genome Med* 2022 | Multi |
| [ ] | [PGS003103](https://www.pgscatalog.org/score/PGS003103/) (ExPRSweb) | 945,820 | AUROC 0.725 (EUR) | ExPRSweb consortium | EUR |
| [ ] | [PGS000014](https://www.pgscatalog.org/score/PGS000014/) (classic GPS) | 6,917,436 | AUROC 0.73 | Khera AV et al., *Nat Genet* 2018 | EUR (120K) |
| [ ] | [PGS002771](https://www.pgscatalog.org/score/PGS002771/) (beta-cell) | varies | Cluster-specific risk | Udler MS et al., *PLoS Med* 2018 | EUR |

### Type 1 diabetes
> Autoimmune destruction of pancreatic beta cells, primarily HLA-driven. GRS2 (67 variants) can distinguish T1D from T2D with AUROC 0.92 and is used clinically for ambiguous cases.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS000339](https://www.pgscatalog.org/score/PGS000339/)** (GRS2) | 67 | AUROC 0.92 (T1D vs T2D) | Sharp SA et al., *Diabetes Care* 2019 | EUR |
| [ ] | [PGS004102](https://www.pgscatalog.org/score/PGS004102/) (PRS-CS) | 61,651 | AUROC 0.741, R2=0.095 | Monti R et al., *AJHG* 2024 | EUR |
| [ ] | [PGS004874](https://www.pgscatalog.org/score/PGS004874/) (INTERVENE) | 56,916 | HR 2.37, C-index 0.77 | Jermy B et al., *Nat Commun* 2024 | Multi |
| [ ] | [PGS004174](https://www.pgscatalog.org/score/PGS004174/) | 49 | AUROC 0.71 | Raben TG et al., *Sci Rep* 2023 | EUR |

### BMI / Obesity
> One of the most heritable common traits (~40-70%). The GIANT 2025 meta-analysis (5.1M individuals) produced the strongest BMI PGS ever, explaining 17.6% of variance. Top-decile PGS individuals are 13kg heavier on average.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS005198](https://www.pgscatalog.org/score/PGS005198/)** (GIANT 2025) | 1,217,710 | R2=0.176 (EUR), OR 4.08 top 3% | Smit RAJ et al., *Nat Med* 2025 | Multi (5.1M) |
| [ ] | [PGS005202](https://www.pgscatalog.org/score/PGS005202/) (EAS-optimized) | 1,022,487 | R2=0.101 (EAS) | Smit RAJ et al., *Nat Med* 2025 | EAS |
| [ ] | [PGS005235](https://www.pgscatalog.org/score/PGS005235/) (obesity) | 709,828 | OR 1.97/SD (obesity) | Arehart CH et al., *Nat Commun* 2025 | EUR |
| [ ] | [PGS000027](https://www.pgscatalog.org/score/PGS000027/) (classic) | 2,100,302 | R2=0.085; top decile 13kg heavier | Khera AV et al., *Cell* 2019 | EUR (120K) |
| [ ] | [PGS002303](https://www.pgscatalog.org/score/PGS002303/) (multi-ancestry) | 1,259,754 | R2=0.09 | Ge T et al., *Genome Med* 2022 | Multi |

### Lipids
> Blood lipid levels (LDL, HDL, triglycerides, total cholesterol) are major cardiovascular risk factors. Genetic contribution is ~50%. PGS can identify individuals with polygenic hyperlipidemia that mimics familial hypercholesterolemia.

| Done | Trait | PGS ID | Variants | Performance | Study | Pop |
|:----:|-------|--------|----------|-------------|-------|-----|
| [ ] | **LDL Cholesterol** | [PGS002609](https://www.pgscatalog.org/score/PGS002609/) | 1,103,034 | R2=0.172 | Weissbrod O et al., *Nat Genet* 2022 | EUR |
| [ ] | LDL (focused) | [PGS000115](https://www.pgscatalog.org/score/PGS000115/) | 223 | R2=0.09 | Trinder M et al., *JAMA Cardiol* 2020 | Multi |
| [ ] | LDL (AFR-optimized) | [PGS003788](https://www.pgscatalog.org/score/PGS003788/) | 1,679,610 | R2=0.044 (AFR) | Zhang H et al., *Nat Genet* 2023 | AFR |
| [ ] | LDL (EAS-tuned) | [PGS004644](https://www.pgscatalog.org/score/PGS004644/) | 1,354,681 | R2=0.067 (EAS) | Zhang J et al., *Nat Commun* 2024 | EAS |
| [ ] | **HDL Cholesterol** | [PGS004775](https://www.pgscatalog.org/score/PGS004775/) | 1,120,830 | R2=0.155 | Truong B et al., *Cell Genomics* 2024 | EUR |
| [ ] | HDL (EAS) | [PGS004631](https://www.pgscatalog.org/score/PGS004631/) | 1,871,796 | R2=0.167 (EAS) | Zhang J et al., *Nat Commun* 2024 | EAS |
| [ ] | **Total Cholesterol** | [PGS002608](https://www.pgscatalog.org/score/PGS002608/) | 1,103,034 | R2=0.155 | Weissbrod O et al., *Nat Genet* 2022 | EUR |
| [ ] | Total Cholesterol (AFR) | [PGS004669](https://www.pgscatalog.org/score/PGS004669/) | 1,728,954 | R2=0.132 (AFR) | Zhang J et al., *Nat Commun* 2024 | AFR |
| [ ] | **Triglycerides** | [PGS002607](https://www.pgscatalog.org/score/PGS002607/) | 1,103,034 | R2=0.115 | Weissbrod O et al., *Nat Genet* 2022 | EUR |
| [ ] | Triglycerides (PRSmix) | [PGS004845](https://www.pgscatalog.org/score/PGS004845/) | 1,095,976 | R2=0.113 (EUR) | Truong B et al., *Cell Genomics* 2024 | EUR |

### Other metabolic

| Done | Condition | PGS ID | Variants | Performance | Study | Pop |
|:----:|-----------|--------|----------|-------------|-------|-----|
| [ ] | **Uric acid** | [PGS000700](https://www.pgscatalog.org/score/PGS000700/) | 20,171 | R2=0.421 (EUR), 0.338 (EAS) | Sinnott-Armstrong N et al., *Nat Genet* 2021 | Multi |
| [ ] | **Celiac disease** | [PGS000040](https://www.pgscatalog.org/score/PGS000040/) | 228 | AUROC 0.90 | Abraham G et al., *PLoS Genet* 2014 | EUR |
| [ ] | **Metabolic syndrome** | [PGS004928](https://www.pgscatalog.org/score/PGS004928/) | 916,017 | OR 1.24, R2=0.046 | Park S et al., *Nat Genet* 2024 | EUR+EAS |
| [ ] | **Gout** | [PGS004768](https://www.pgscatalog.org/score/PGS004768/) | 1,580,311 | R2=0.081 | Truong B et al., *Cell Genomics* 2024 | EUR |
| [ ] | Gout (SnpNet) | [PGS004931](https://www.pgscatalog.org/score/PGS004931/) | 1,138 | AUROC 0.73 | Moreno-Grau S et al., *Hum Genomics* 2024 | EUR |
| [ ] | **Hypothyroidism** | [PGS004935](https://www.pgscatalog.org/score/PGS004935/) | 6,127 | AUROC 0.70 | Moreno-Grau S et al., *Hum Genomics* 2024 | EUR |
| [ ] | Hypothyroidism | [PGS000820](https://www.pgscatalog.org/score/PGS000820/) | 890,908 | OR 1.33, AUROC 0.60 | Luo J et al., *Clin Cancer Res* 2021 | EUR |
| [ ] | **Osteoporosis / BMD** | [PGS002632](https://www.pgscatalog.org/score/PGS002632/) | 432,286 | R2=0.250 | Weissbrod O et al., *Nat Genet* 2022 | EUR |
| [ ] | **HbA1c** | [PGS004044](https://www.pgscatalog.org/score/PGS004044/) | 907,906 | R2=0.039 | Monti R et al., *AJHG* 2024 | EUR |
| [ ] | **NAFLD** | [PGS002283](https://www.pgscatalog.org/score/PGS002283/) | 15 | beta=0.094 | Schnurr TM et al., *Hepatol Commun* 2022 | Multi |
| [ ] | **eGFR / kidney function** | [PGS002605](https://www.pgscatalog.org/score/PGS002605/) | 1,103,034 | R2=0.048 | Weissbrod O et al., *Nat Genet* 2022 | EUR |
| [ ] | **Vitamin D levels** | [PGS001907](https://www.pgscatalog.org/score/PGS001907/) | 8,505 | R2=0.035 | Prive F et al., *AJHG* 2022 | EUR |

---

---

## 4. Polygenic Scores -- Autoimmune & Inflammatory

### IBD / Crohn's / UC
> Inflammatory bowel diseases with strong genetic component (~200 risk loci). Crohn's disease and ulcerative colitis have partially overlapping but distinct genetic architectures. PGS can help predict disease course and treatment response.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS004151](https://www.pgscatalog.org/score/PGS004151/)** (IBD, best) | 1,102,205 | AUROC 0.695, OR 2.06 | Monti R et al., *AJHG* 2024 | Multi |
| [ ] | [PGS004081](https://www.pgscatalog.org/score/PGS004081/) (IBD) | 1,073,268 | AUROC 0.68 | Monti R et al., *AJHG* 2024 | EUR |
| [ ] | [PGS004254](https://www.pgscatalog.org/score/PGS004254/) (Crohn's) | 744,682 | AUROC 0.72, OR 2.18 | Middha P et al., *Nat Commun* 2024 | EUR |
| [ ] | [PGS004253](https://www.pgscatalog.org/score/PGS004253/) (UC) | 744,575 | OR 1.84, AUROC 0.66 | Middha P et al., *Nat Commun* 2024 | EUR |
| [ ] | [PGS000020](https://www.pgscatalog.org/score/PGS000020/) (classic IBD) | 228 | AUROC 0.63 | Khera AV et al., *Nat Genet* 2018 | EUR |

### Multiple sclerosis
> Strongly driven by HLA-DRB1*15:01 and ~200 non-HLA loci. Combined HLA+genome-wide PGS achieves AUROC 0.80 and can identify top-decile individuals at 15x risk.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS002726](https://www.pgscatalog.org/score/PGS002726/)** (HLA+genome-wide) | 476,399 | AUROC 0.80, OR 15.0 (top 10%) | Shams H et al., *Brain* 2022 | EUR |
| [ ] | [PGS004700](https://www.pgscatalog.org/score/PGS004700/) (HLA-GRS) | 12 | AUROC 0.76 | Loginovic P et al., *Nat Commun* 2024 | EUR |
| [ ] | [PGS004699](https://www.pgscatalog.org/score/PGS004699/) (HLA+nonHLA) | 307 | AUROC 0.764 | Loginovic P et al., *Nat Commun* 2024 | EUR |
| [ ] | [PGS002312](https://www.pgscatalog.org/score/PGS002312/) (genome-wide) | 1,109,311 | AUROC 0.69 | Weissbrod O et al., *Nat Genet* 2022 | EUR |

### Rheumatoid arthritis
> Chronic autoimmune joint disease affecting ~1% of adults. HLA-DRB1 shared epitope is the strongest risk factor. Multi-ancestry PGS achieves AUROC 0.75 and transfers well to South Asian populations.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS004163](https://www.pgscatalog.org/score/PGS004163/)** (multi-ancestry) | 778,275 | AUROC 0.747, OR 2.46 | Monti R et al., *AJHG* 2024 | Multi |
| [ ] | [PGS004873](https://www.pgscatalog.org/score/PGS004873/) (INTERVENE) | 551,074 | HR 1.87, C-index 0.65 | Jermy B et al., *Nat Commun* 2024 | Multi |
| [ ] | [PGS002745](https://www.pgscatalog.org/score/PGS002745/) | 2,575 | AUROC 0.66 | Ishigaki K et al., *Nat Genet* 2022 | Multi |

### Asthma
> Common chronic inflammatory airway disease, highly heritable (~60%). New 2024 scores validated across 7 biobanks show consistent hazard ratios of 1.4-1.5 per SD.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS004877](https://www.pgscatalog.org/score/PGS004877/)** (INTERVENE) | 870,454 | HR 1.42-1.48 (7 biobanks) | Jermy B et al., *Nat Commun* 2024 | EUR |
| [ ] | [PGS004723](https://www.pgscatalog.org/score/PGS004723/) (PRSmix) | 985,316 | Incr. R2=0.033 | Truong B et al., *Cell Genomics* 2024 | EUR |
| [ ] | [PGS004537](https://www.pgscatalog.org/score/PGS004537/) (metaPRS) | 1,059,939 | OR 1.40/SD | Jung H et al., *Commun Biol* 2024 | EUR |
| [ ] | [PGS002311](https://www.pgscatalog.org/score/PGS002311/) | 1,109,311 | R2=0.024 | Weissbrod O et al., *Nat Genet* 2022 | EUR |

### Other autoimmune

| Done | Condition | PGS ID | Variants | Performance | Study | Pop |
|:----:|-----------|--------|----------|-------------|-------|-----|
| [ ] | **Lupus (SLE)** | [PGS000328](https://www.pgscatalog.org/score/PGS000328/) | 57 | AUROC 0.83, OR 12.32 | Reid S et al., *Ann Rheum Dis* 2019 | EUR |
| [ ] | Lupus (SLE, multi) | [PGS004917](https://www.pgscatalog.org/score/PGS004917/) | 97 | AUROC 0.696, OR 2.01 | Cui J et al., *Arthritis Rheumatol* 2020 | Multi |
| [ ] | **Psoriasis** | [PGS005309](https://www.pgscatalog.org/score/PGS005309/) | 513,461 | OR 1.49 | Saklatvala JR et al., *Genome Med* 2025 | EUR |
| [ ] | Psoriasis | [PGS001288](https://www.pgscatalog.org/score/PGS001288/) | 7,534 | AUROC 0.70 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | **Psoriatic arthropathy** | [PGS001287](https://www.pgscatalog.org/score/PGS001287/) | 36 | AUROC 0.73 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | **Ankylosing spondylitis** | [PGS001289](https://www.pgscatalog.org/score/PGS001289/) | 2,874 | AUROC 0.85 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | **Graves' disease** | [PGS005265](https://www.pgscatalog.org/score/PGS005265/) | 1,085,173 | AUROC 0.665, OR 1.63 | White SL et al., *medRxiv* 2025 | Multi |
| [ ] | **Sjogren's syndrome** | [PGS001308](https://www.pgscatalog.org/score/PGS001308/) | 7 | AUROC 0.80 (SAS), 0.77 (EUR) | Tanigawa Y et al., *PLoS Genet* 2022 | Multi |
| [ ] | **Atopic dermatitis** | [PGS004903](https://www.pgscatalog.org/score/PGS004903/) | 38 | Significant in EUR | Al-Janabi A et al., *JACI* 2023 | EUR |
| [ ] | **Vitiligo** | [PGS001290](https://www.pgscatalog.org/score/PGS001290/) | 3,672 | AUROC 0.72 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |

---

---

## 5. Polygenic Scores -- Neurological & Psychiatric

### Alzheimer's disease
> Most common cause of dementia. APOE e4 is the strongest single genetic risk factor (12-15x for homozygotes), but polygenic risk beyond APOE adds significant predictive value. The Bellenguez 2022 GWAS (487K individuals) produced the definitive non-APOE risk loci.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS004590](https://www.pgscatalog.org/score/PGS004590/)** (excl APOE) | 363 | AUROC 0.68 | Lake J et al., *Mol Psychiatry* 2023 | Multi (644K) |
| [ ] | [PGS002280](https://www.pgscatalog.org/score/PGS002280/) (Bellenguez) | 83 | HR 1.93 (top vs bottom decile) | Bellenguez C et al., *Nat Genet* 2022 | EUR |
| [ ] | [PGS004092](https://www.pgscatalog.org/score/PGS004092/) (genome-wide) | 1,109,233 | AUROC 0.665, OR 1.78/SD | Monti R et al., *AJHG* 2024 | EUR |
| [ ] | [PGS004863](https://www.pgscatalog.org/score/PGS004863/) (multi-ancestry) | 74 | AUROC 0.746 (EUR), 0.751 (EAS) | Sleiman PM et al., *Alzheimers Dement* 2023 | Multi |
| [ ] | [PGS000334](https://www.pgscatalog.org/score/PGS000334/) (incl APOE) | 21 | AUROC 0.83 | Desikan RS et al., *PLoS Med* 2017 | EUR |
| [ ] | [PGS000025](https://www.pgscatalog.org/score/PGS000025/) (GPS) | 6,630,150 | AUROC 0.75 | Khera AV et al., *Nat Genet* 2018 | EUR |

### Parkinson's disease
> Second most common neurodegenerative disease. GBA1 and LRRK2 are the strongest single-gene risk factors. PGS from the Nalls 2019 Lancet Neurology landmark paper identifies individuals at 3-4x risk.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS000903](https://www.pgscatalog.org/score/PGS000903/)** | 1,805 | AUROC 0.692, OR 6.25 | Nalls MA et al., *Lancet Neurol* 2019 | Multi (1.5M) |
| [ ] | [PGS003763](https://www.pgscatalog.org/score/PGS003763/) (JAMA Neurol) | 44 | HR 1.72, HR 3.22 (w/ frailty) | Zheng Z et al., *JAMA Neurol* 2023 | EUR |
| [ ] | [PGS004924](https://www.pgscatalog.org/score/PGS004924/) | 90 | OR 3.79 (top vs bottom quartile) | Cao Z et al., *Parkinsonism Relat Disord* 2023 | EUR |
| [ ] | [PGS000902](https://www.pgscatalog.org/score/PGS000902/) (multi-ancestry) | 90 | AUROC 0.651, OR 1.575/SD | Nalls MA et al., *Lancet Neurol* 2019 | Multi |
| [ ] | [PGS002940](https://www.pgscatalog.org/score/PGS002940/) (genome-wide) | 1,805 | AUROC 0.72 | Kim JJ et al., *Genome Med* 2023 | EUR |

### Schizophrenia
> Highly heritable psychiatric disorder (~80%). PGC3 GWAS identified 270+ risk loci. PGS achieves AUROC 0.74-0.76, among the best-performing psychiatric PGS. Also predicts response to clozapine.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS000135](https://www.pgscatalog.org/score/PGS000135/)** | 972,439 | AUROC 0.74 | Zheutlin AB et al., *Am J Psychiatry* 2019 | EUR/EAS |
| [ ] | [PGS002785](https://www.pgscatalog.org/score/PGS002785/) (SDPR) | 964,422 | R2=0.008 | Gui Y et al., *Transl Psychiatry* 2022 | EUR |
| [ ] | [PGS003472](https://www.pgscatalog.org/score/PGS003472/) (PGC3) | varies | AUROC 0.76 | Trubetskoy V et al., *Nature* 2022 | Multi (320K) |

### Major depression
> Most common psychiatric disorder, affecting ~20% lifetime. Highly polygenic with modest per-variant effects. Recent 2024 scores use meta-PRS approaches combining multiple GWAS sources.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS004760](https://www.pgscatalog.org/score/PGS004760/)** (PRSmixPlus) | 2,141,267 | Incr. R2=0.024 | Truong B et al., *Cell Genomics* 2024 | EUR |
| [ ] | [PGS004885](https://www.pgscatalog.org/score/PGS004885/) (INTERVENE) | 801,544 | HR 1.24/SD, C-index 0.58 | Jermy B et al., *Nat Commun* 2024 | EUR |
| [ ] | [PGS003333](https://www.pgscatalog.org/score/PGS003333/) | 1,088,415 | R2=0.022 | Fang Y et al., *Biol Psychiatry* 2022 | EUR |
| [ ] | [PGS002759](https://www.pgscatalog.org/score/PGS002759/) (PRS-CS) | 1,091,613 | OR 1.26/SD | Mars N et al., *AJHG* 2022 | EUR |

### Other neuropsych

| Done | Condition | PGS ID | Variants | Performance | Study | Pop |
|:----:|-----------|--------|----------|-------------|-------|-----|
| [ ] | **Bipolar disorder** | [PGS002787](https://www.pgscatalog.org/score/PGS002787/) | 937,511 | Modest R2 | Gui Y et al., *Transl Psychiatry* 2022 | EUR |
| [ ] | Bipolar II | [PGS002788](https://www.pgscatalog.org/score/PGS002788/) | 935,292 | R2=0.003 | Gui Y et al., *Transl Psychiatry* 2022 | EUR |
| [ ] | **ADHD** | [PGS002746](https://www.pgscatalog.org/score/PGS002746/) | 513,659 | beta=0.11 | Lahey BB et al., *J Psychiatr Res* 2022 | Multi |
| [ ] | ADHD (Latin Am eval) | [PGS003753](https://www.pgscatalog.org/score/PGS003753/) | 35,445 | Validated in Brazilian | Sato JR et al., *Genes Brain Behav* 2023 | Multi |
| [ ] | **Autism spectrum** | [PGS000327](https://www.pgscatalog.org/score/PGS000327/) | 35,087 | OR 1.33, R2=0.025 | Grove J et al., *Nat Genet* 2019 | EUR |
| [ ] | Autism spectrum | [PGS002790](https://www.pgscatalog.org/score/PGS002790/) | 916,713 | R2=0.005 | Gui Y et al., *Transl Psychiatry* 2022 | EUR |
| [ ] | **Addiction risk (multi-substance)** | [PGS003849](https://www.pgscatalog.org/score/PGS003849/) | 584,753 | OR 1.73 (opioid), 1.57 (alcohol) | Hatoum AS et al., *Nat Ment Health* 2023 | EUR |
| [ ] | **Anxiety** | [PGS004451](https://www.pgscatalog.org/score/PGS004451/) | 1,059,939 | OR 1.19/SD | Jung H et al., *Commun Biol* 2024 | EUR |
| [ ] | **PTSD** | [PGS005393](https://www.pgscatalog.org/score/PGS005393/) | 53,705 | R2=0.087 | Bugiga AVG et al., *Braz J Psychiatry* 2024 | Multi |
| [ ] | **Anorexia nervosa** | [PGS000379](https://www.pgscatalog.org/score/PGS000379/) | 66,177 | OR 1.24/SD | Watson HJ et al., *Nat Genet* 2019 | EUR |
| [ ] | **Epilepsy** | [PGS004881](https://www.pgscatalog.org/score/PGS004881/) | 605,432 | HR 1.12/SD | Jermy B et al., *Nat Commun* 2024 | EUR |
| [ ] | **Migraine** | [PGS004798](https://www.pgscatalog.org/score/PGS004798/) | 3,984,158 | R2=0.004 | Truong B et al., *Cell Genomics* 2024 | SAS |

---

---

## 6. Polygenic Scores -- Traits & Behavioral

### Height
> The most studied polygenic trait. Height PGS now explains >70% of genetic variance in Europeans -- the best-performing PGS for any trait. Useful as a positive control to verify your scoring pipeline is working correctly.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS001229](https://www.pgscatalog.org/score/PGS001229/)** | 51,209 | R2=0.717 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | [PGS004214](https://www.pgscatalog.org/score/PGS004214/) (LASSO) | 23,686 | R2=0.712 (sibling-validated) | Raben TG et al., *Sci Rep* 2023 | EUR |
| [ ] | [PGS002596](https://www.pgscatalog.org/score/PGS002596/) (genome-wide) | 1,103,034 | R2=0.654 | Weissbrod O et al., *Nat Genet* 2022 | EUR |
| [ ] | [PGS005006](https://www.pgscatalog.org/score/PGS005006/) (multi-ancestry) | 1,273,897 | R2=0.159 (EUR), 0.051 (AFR) | Gunn S et al., *HGG Adv* 2024 | Multi |
| [ ] | [PGS002305](https://www.pgscatalog.org/score/PGS002305/) (multi-ancestry) | 1,259,754 | R2=0.61 | Ge T et al., *Genome Med* 2022 | Multi |

### Intelligence / Cognitive Ability
> Fluid intelligence (gF) measures reasoning ability independent of learned knowledge. It is highly heritable (~50-80%) and among the most polygenic traits known. The best PGS now explains ~22% of variance in Europeans. Multiple scores exist using different GWAS sources and methods -- running several provides a more robust estimate. These scores capture population-level statistical associations and should not be interpreted as deterministic individual predictions.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS004427](https://www.pgscatalog.org/score/PGS004427/)** (fluid intelligence, best) | 1,059,939 | R2=0.223 (EUR) | Jung H et al., *Commun Biol* 2024 | EUR |
| [ ] | [PGS003724](https://www.pgscatalog.org/score/PGS003724/) (IQ) | 6,680,000 | R2=0.12 | Hatoum AS et al., *Nat Genet* 2022 | EUR |
| [ ] | [PGS003723](https://www.pgscatalog.org/score/PGS003723/) (cognitive performance / cEF) | 6,680,000 | R2=0.11 | Hatoum AS et al., *Nat Genet* 2022 | EUR |
| [ ] | [PGS003510](https://www.pgscatalog.org/score/PGS003510/) (verbal-numerical reasoning) | 979,739 | R2=0.15 | Ding Y et al., *bioRxiv* 2022 | EUR |
| [ ] | [PGS002135](https://www.pgscatalog.org/score/PGS002135/) (fluid intelligence) | 903,000 | R2=0.10 | Prive F et al., *AJHG* 2022 | EUR |
| [ ] | [PGS001919](https://www.pgscatalog.org/score/PGS001919/) (fluid intelligence) | 26,000 | R2=0.06 | Prive F et al., *AJHG* 2022 | EUR |
| [ ] | [PGS001232](https://www.pgscatalog.org/score/PGS001232/) (fluid intelligence) | 10,000 | R2=0.04 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | [PGS001550](https://www.pgscatalog.org/score/PGS001550/) (working memory) | varies | R2=0.02 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | [PGS001923](https://www.pgscatalog.org/score/PGS001923/) (reaction time) | varies | R2=0.01 | Prive F et al., *AJHG* 2022 | EUR |
| [ ] | [PGS001541](https://www.pgscatalog.org/score/PGS001541/) (brain volume) | varies | R2=0.05 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |

### Educational Attainment & Socioeconomic Proxies
> Years of education completed -- correlates ~r=0.70 with intelligence and has the largest GWAS sample sizes (N>3M). Socioeconomic proxies (income, occupation) track real-world cognitive outcomes but are heavily confounded by environment.

| Done | PGS ID | Variants | Performance | Study | Pop |
|:----:|--------|----------|-------------|-------|-----|
| [ ] | **[PGS003390](https://www.pgscatalog.org/score/PGS003390/)** (EA4, latest) | varies | r=0.19 (EUR) | Okbay A et al., *Nat Genet* 2022 | EUR |
| [ ] | [PGS002319](https://www.pgscatalog.org/score/PGS002319/) (college, multi eval) | 1,109,311 | Incr. R2=0.055 (EUR), 0.022 (SAS) | Weissbrod O et al., *Nat Genet* 2022 | Multi |
| [ ] | [PGS002012](https://www.pgscatalog.org/score/PGS002012/) (educational attainment) | 50,413 | r=0.175 | Prive F et al., *AJHG* 2022 | EUR |
| [ ] | [PGS002231](https://www.pgscatalog.org/score/PGS002231/) (years of schooling) | varies | r=0.17 | Prive F et al., *AJHG* 2022 | EUR |
| [ ] | [PGS002235](https://www.pgscatalog.org/score/PGS002235/) (household income) | varies | R2=0.03 | Prive F et al., *AJHG* 2022 | EUR |
| [ ] | [PGS002232](https://www.pgscatalog.org/score/PGS002232/) (occupational status) | varies | R2=0.02 | Prive F et al., *AJHG* 2022 | EUR |
| [ ] | [PGS001712](https://www.pgscatalog.org/score/PGS001712/) (highest math class) | varies | R2=0.02 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | [PGS000947](https://www.pgscatalog.org/score/PGS000947/) (reading/literacy) | varies | R2=0.03 | Tanigawa Y et al., *PLoS Genet* 2022 | EUR |
| [ ] | [PGS003306](https://www.pgscatalog.org/score/PGS003306/) (openness to experience) | varies | R2=0.02 | Ding Y et al., *bioRxiv* 2022 | EUR |

### Other traits

| Done | Trait | PGS ID | Variants | Performance | Study |
|:----:|-------|--------|----------|-------------|-------|
| [ ] | **Chronotype** (morn/eve) | [PGS002318](https://www.pgscatalog.org/score/PGS002318/) | 1,109,311 | Incr. R2=0.036 (EUR) | Weissbrod O et al., *Nat Genet* 2022 |
| [ ] | **Handedness** (left) | [PGS002151](https://www.pgscatalog.org/score/PGS002151/) | 734,236 | partial-r=0.025-0.042 | Prive F et al., *AJHG* 2022 |
| [ ] | **Hearing difficulty** | [PGS000762](https://www.pgscatalog.org/score/PGS000762/) | 100,325 | R2=0.091 | Cherny SS et al., *EJHG* 2020 |
| [ ] | **# Children born** | [PGS002309](https://www.pgscatalog.org/score/PGS002309/) | 1,109,311 | Incr. R2=0.008 | Weissbrod O et al., *Nat Genet* 2022 |
| [ ] | **Hair color** | [PGS002598](https://www.pgscatalog.org/score/PGS002598/) | 8,312 | R2=0.182 | Weissbrod O et al., *Nat Genet* 2022 |
| [ ] | **Skin pigmentation** | [PGS001897](https://www.pgscatalog.org/score/PGS001897/) | 15,817 | r=0.387 | Prive F et al., *AJHG* 2022 |
| [ ] | **Male pattern baldness** | [PGS002314](https://www.pgscatalog.org/score/PGS002314/) | 1,109,311 | R2=0.143 | Weissbrod O et al., *Nat Genet* 2022 |
| [ ] | **Body fat %** | [PGS003899](https://www.pgscatalog.org/score/PGS003899/) | 34,374 | R2=0.056 | Tanigawa Y et al., *AJHG* 2023 |
| [ ] | **Grip strength** | [PGS001162](https://www.pgscatalog.org/score/PGS001162/) | 10,872 | R2=0.033 | Tanigawa Y et al., *PLoS Genet* 2022 |
| [ ] | **Neuroticism** | [PGS003565](https://www.pgscatalog.org/score/PGS003565/) | 979,739 | R2=0.05 | Ding Y et al., *bioRxiv* 2022 |
| [ ] | **Risk tolerance** | [PGS000205](https://www.pgscatalog.org/score/PGS000205/) | 1,110,737 | dR2=0.016 | Barr PB et al., *Transl Psychiatry* 2020 |
| [ ] | **Age at menarche** | [PGS001183](https://www.pgscatalog.org/score/PGS001183/) | 25,172 | R2=0.10 | Tanigawa Y et al., *PLoS Genet* 2022 |
| [ ] | **Voice pitch** | [PGS001197](https://www.pgscatalog.org/score/PGS001197/) | 4,621 | R2=0.015 | Tanigawa Y et al., *PLoS Genet* 2022 |

---

---

## 7. Polygenic Scores -- Lifestyle, Aging & Other

| Done | Trait | PGS ID | Variants | Performance | Study |
|:----:|-------|--------|----------|-------------|-------|
| [ ] | **Coffee consumption** | PGS001123 | 48 | AUROC 0.617 | Tanigawa Y et al., *PLoS Genet* 2022 |
| [ ] | **Alcohol use disorder** | PGS005213 | 336,813 | R2=0.05 | Deng WQ et al., *Alcohol Alcoholism* 2024 |
| [ ] | **Smoking initiation** | PGS003357 | 1,194,472 | dAUC=0.015 | Saunders GRB et al., *Nature* 2022 |
| [ ] | **Insomnia** | PGS000908 | 2,746,982 | OR 1.12-1.28 | Campos AI et al., *Commun Med* 2021 |
| [ ] | **Longevity** | PGS000906 | 330 | HR 0.89/SD | Tesi N et al., *J Gerontol* 2021 |
| [ ] | **Telomere length** | PGS002616 | 1,103,034 | R2=0.024 | Weissbrod O et al., *Nat Genet* 2022 |
| [ ] | **CKD** | PGS004889 | 1,117,375 | HR 1.33, C-index 0.727 | Mandla R et al., *Genome Med* 2024 |
| [ ] | **AMD (macular degen.)** | PGS004606 | 1,000,946 | OR 1.76, AUROC 0.71 | Gorman BR et al., *Nat Genet* 2024 |
| [ ] | **Glaucoma** | PGS001797 | 885,417 | AUROC 0.749 | Wang Y et al., *Cell Genomics* 2023 |
| [ ] | **Myopia** | PGS001204 | 25,543 | R2=0.06 | Tanigawa Y et al., *PLoS Genet* 2022 |
| [ ] | **Kidney stones** | PGS004493 | 1,059,939 | OR 1.23/SD | Jung H et al., *Commun Biol* 2024 |
| [ ] | **Gallstones** | PGS001291 | 5,387 | AUROC 0.63 | Tanigawa Y et al., *PLoS Genet* 2022 |
| [ ] | **Osteoarthritis (knee)** | PGS001296 | 6,234 | AUROC 0.58 | Tanigawa Y et al., *PLoS Genet* 2022 |

---

## 8. Monogenic Disease Screening

ACMG SF v3.3 (2025) -- 84 genes. Method: Filter VCF for pathogenic/likely-pathogenic variants in ClinVar within these gene panels.

| Done | Category | Genes |
|:----:|----------|-------|
| [ ] | **Cancer Predisposition (~28)** | APC, BRCA1, BRCA2, BMPR1A, MAX, MEN1, MLH1, MSH2, MSH6, MUTYH, NF2, PALB2, PMS2, PTEN, RB1, RET, SDHAF2, SDHB, SDHC, SDHD, SMAD4, STK11, TMEM127, TP53, TSC1, TSC2, VHL, WT1 |
| [ ] | **Cardiovascular (~41)** | ACTA2, ACTC1, APOB, BAG3, CALM1-3, CASQ2, COL3A1, DES, DSC2, DSG2, DSP, FBN1, FLNC, KCNH2, KCNQ1, LDLR, LMNA, MYH7, MYH11, MYBPC3, MYL2, MYL3, PCSK9, PKP2, PLN, PRKAG2, RBM20, RYR2, SCN5A, SMAD3, TGFBR1/2, TMEM43, TNNC1, TNNI3, TNNT2, TPM1, TRDN, TTN |
| [ ] | **Metabolism (~5)** | BTD, CYP27A1, GAA, GLA, OTC |
| [ ] | **Miscellaneous (~10)** | ABCD1, ACVRL1, ATP7B, CACNA1S, ENG, HFE, HNF1A, RPE65, RYR1, TTR |

---

## 9. Carrier Status -- Recessive Diseases

| Done | Disease | Gene | Key Variant | Carrier Freq | Population |
|:----:|---------|------|-------------|--------------|------------|
| [ ] | **Cystic fibrosis** | CFTR | F508del (rs113993960) | 1 in 25 | N. European |
| [ ] | **Sickle cell disease** | HBB | Glu6Val (rs334) | 1 in 13 | African Amer. |
| [ ] | **Tay-Sachs** | HEXA | 1278insTATC (rs387906309) | 1 in 30 | Ashkenazi |
| [ ] | **Gaucher disease** | GBA1 | N370S (rs76763715) | 1 in 15 | Ashkenazi |
| [ ] | **PKU** | PAH | R408W (rs5030858) | 1 in 50 | European |
| [ ] | **Beta-thalassemia** | HBB | Codon 39 C>T (rs11549407) | 5-30% | Mediterranean |
| [ ] | **Hemochromatosis** | HFE | C282Y (rs1800562) | 1 in 9 | N. European |
| [ ] | **SMA** | SMN1 | Exon 7 deletion | 1 in 40-50 | Pan-ethnic |
| [ ] | **Pompe disease** | GAA | c.-32-13T>G (rs386834236) | 1 in 50 | EUR |

---

## 10. Single-Variant Health Risk Markers

| Done | Marker | Gene | rs Number | Effect | Freq |
|:----:|--------|------|-----------|--------|------|
| [ ] | **APOE e4 (Alzheimer's)** | APOE | rs429358 + rs7412 | e4/e4 = 12-15x AD risk | e4: 14% |
| [ ] | **Factor V Leiden** | F5 | rs6025 | 3-8x VTE risk (het) | 5% EUR |
| [ ] | **Prothrombin G20210A** | F2 | rs1799963 | 2.8x VTE risk | 2-3% EUR |
| [ ] | **BRCA1 185delAG** | BRCA1 | rs80357713 | 45-85% lifetime breast cancer | 1% Ashkenazi |
| [ ] | **MTHFR C677T** | MTHFR | rs1801133 | TT = 30% enzyme activity | 10-15% EUR |
| [ ] | **A1AT Z allele** | SERPINA1 | rs28929474 | ZZ = emphysema + liver disease | 2-3% N.EUR |
| [ ] | **PCSK9 R46L (protective)** | PCSK9 | rs11591147 | ~50% lower LDL | 2% EUR |
| [ ] | **LRRK2 G2019S (PD)** | LRRK2 | rs34637584 | 25-42% lifetime PD risk | 0.84% Ashkenazi |
| [ ] | **FTO obesity** | FTO | rs9939609 | AA = 1.67x obesity risk | A: 42% EUR |
| [ ] | **TCF7L2 diabetes** | TCF7L2 | rs7903146 | TT = 1.8x T2D risk | T: 30% EUR |
| [ ] | **9p21 CAD locus** | CDKN2A/B | rs10757278 | GG = 1.6x MI risk | G: 49% EUR |
| [ ] | **LPA (Lp(a))** | LPA | rs10455872 | G = elevated Lp(a), 1.5x CAD | G: 7% EUR |

---

## 11. Pharmacogenomics (PGx)

CPIC: 34 genes, 164 drugs. Method: Call star alleles from VCF using Cyrius (CYP2D6) and PharmCAT (all others).

| Done | Gene | Key Variants | Primary Drugs | CPIC | Metabolizer Types |
|:----:|------|-------------|---------------|------|-------------------|
| [ ] | **CYP2D6** | *3, *4, *10, *17, *41 + CNV | Codeine, tamoxifen, SSRIs | A | UM, NM, IM, PM |
| [ ] | **CYP2C19** | *2 (rs4244285), *3, *17 | Clopidogrel, PPIs, SSRIs | A | UM, RM, NM, IM, PM |
| [ ] | **CYP2C9** | *2 (rs1799853), *3 (rs1057910) | Warfarin, NSAIDs | A | NM, IM, PM |
| [ ] | **VKORC1** | rs9923231 | Warfarin | A | Low/Int/High sensitivity |
| [ ] | **DPYD** | *2A (rs3918290), *13, D949V | 5-FU, capecitabine (PM = fatal) | A | NM, IM, PM |
| [ ] | **TPMT** | *2, *3A, *3B, *3C | Azathioprine, 6-MP | A | NM, IM, PM |
| [ ] | **SLCO1B1** | *5 (rs4149056) | All statins (esp. simvastatin) | A | Normal, Decreased, Poor |
| [ ] | **HLA-B*57:01** | HLA typing | Abacavir | A | Positive/Negative |
| [ ] | **HLA-B*15:02** | HLA typing | Carbamazepine (SJS/TEN) | A | Positive/Negative |
| [ ] | **G6PD** | A-, Med variants | Rasburicase, primaquine | A | Normal, Deficient |
| [ ] | **CYP1A2** | *1F (rs762551) | Caffeine, clozapine | B | Ultra-rapid, Normal, Slow |
| [ ] | **COMT** | Val158Met (rs4680) | Pain meds | B | High/Low activity |

---

## 12. Fun & Interesting Trait Variants

| Done | Trait | Gene | SNP | Effect |
|:----:|-------|------|-----|--------|
| [ ] | **Bitter taste (PTC)** | TAS2R38 | rs713598 | PAV/PAV = supertaster |
| [ ] | **Cilantro = soap** | OR6A2 | rs72921001 | A allele = soapy |
| [ ] | **Earwax type** | ABCC11 | rs17822931 | TT = dry + less body odor |
| [ ] | **Lactose tolerance** | MCM6 | rs4988235 | T = persistent; CC = intolerant |
| [ ] | **Alcohol flush** | ALDH2 | rs671 | AA = cannot drink |
| [ ] | **Caffeine metabolism** | CYP1A2 | rs762551 | AA = fast; CC = slow |
| [ ] | **Sprint vs endurance** | ACTN3 | rs1815739 | CC = sprint; TT = endurance |
| [ ] | **Eye color** | HERC2 | rs12913832 | GG = blue; AA = brown |
| [ ] | **Photic sneeze** | ZEB2 | rs10427255 | C = sneeze from light |
| [ ] | **Norovirus resistance** | FUT2 | rs601338 | AA = strong resistance |
| [ ] | **Blood type (ABO)** | ABO | rs8176746 | Predicts A/B/AB/O |
| [ ] | **Pain sensitivity** | SCN9A | rs6746030 | A = increased pain |

---

## 13. Ancestry & Population Genetics

| Done | Analysis | Tool / Method |
|:----:|----------|---------------|
| [ ] | **Ancestry pipeline** | PCA + NNLS + KNN via 1000G reference (FRAPOSA/ADMIXTURE skill) |

---

## 14. Advanced Analyses

### Nutrigenomics

| Done | Pathway | Gene | rs Number | Effect |
|:----:|---------|------|-----------|--------|
| [ ] | Folate metabolism | MTHFR | rs1801133 | TT = needs methylfolate |
| [ ] | Omega-3 conversion | FADS1/2 | rs174546 | Some need direct fish oil |
| [ ] | Vitamin D bioavailability | GC/VDBP | rs2282679 | Affects absorption |
| [ ] | Salt sensitivity | AGT | rs699 | Stronger BP response |
| [ ] | Celiac (gluten) | HLA-DQ2/8 | HLA typing | 95% of celiacs carry DQ2.5 |
| [ ] | Melatonin/glucose | MTNR1B | rs10830963 | Late eating worsens glucose |
| [ ] | Saturated fat response | APOA2 | rs5082 | CC = higher BMI with sat fat |

### Sports & Fitness

| Done | Trait | Gene | rs Number | Effect |
|:----:|-------|------|-----------|--------|
| [ ] | Muscle fiber type | ACTN3 | rs1815739 | RR = sprint; XX = endurance |
| [ ] | Endurance capacity | ACE | I/D polymorphism | II = endurance; DD = power |
| [ ] | Tendon injury risk | COL5A1 | rs12722 | TT = higher risk |
| [ ] | Recovery speed | IL6 | rs1800795 | G = longer recovery needed |
| [ ] | VO2max trainability | CKM | rs8111989 | Aerobic training response |

### Sleep & Circadian

| Done | Trait | Gene | rs Number | Effect |
|:----:|-------|------|-----------|--------|
| [ ] | Delayed sleep phase | CRY1 | rs184039278 | Dominant late-sleeper (~1% EUR) |
| [ ] | Deep sleep quality | ADA | rs73598374 | A = deeper slow-wave sleep |
| [ ] | Caffeine + sleep | ADORA2A | rs5751876 | TT = high caffeine sensitivity |

---

## 15. Databases, Tools & References

### Key Databases

| Done | Database | URL | Purpose |
|:----:|----------|-----|---------|
| [ ] | **PGS Catalog** | https://www.pgscatalog.org/ | 4,000+ polygenic scores |
| [ ] | **ClinVar** | https://www.ncbi.nlm.nih.gov/clinvar/ | Variant-disease relationships |
| [ ] | **PharmGKB** | https://www.pharmgkb.org/ | Pharmacogenomics |
| [ ] | **CPIC** | https://cpicpgx.org/ | Clinical PGx guidelines |
| [ ] | **gnomAD** | https://gnomad.broadinstitute.org/ | Population frequencies |
| [ ] | **GWAS Catalog** | https://www.ebi.ac.uk/gwas/ | Published GWAS |

### Analysis Tools

| Done | Tool | Purpose |
|:----:|------|---------|
| [ ] | **PLINK 2.0** | PGS, IBD, ROH, QC |
| [ ] | **Cyrius** | CYP2D6 star alleles from WGS |
| [ ] | **PharmCAT** | PGx clinical annotation from VCF |
| [ ] | **HLA-LA** | HLA typing from WGS |
| [ ] | **ADMIXTURE** | Ancestry admixture |
| [ ] | **HIrisPlex-S** | Eye/hair/skin prediction from 41 SNPs |
