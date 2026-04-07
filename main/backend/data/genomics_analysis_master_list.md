# Comprehensive Genomics Analysis Master List

> A detailed catalog of every interesting analysis that can be performed on whole-genome sequencing (WGS) data — polygenic scores, monogenic screening, pharmacogenomics, carrier status, ancestry, traits, and more. Each entry includes specific identifiers, URLs, variant counts, study references, and performance metrics.
>
> **Last updated**: 2026-03-29

---

## Table of Contents

1. [Polygenic Scores — Cancer](#1-polygenic-scores--cancer)
2. [Polygenic Scores — Cardiovascular](#2-polygenic-scores--cardiovascular)
3. [Polygenic Scores — Metabolic & Endocrine](#3-polygenic-scores--metabolic--endocrine)
4. [Polygenic Scores — Autoimmune & Inflammatory](#4-polygenic-scores--autoimmune--inflammatory)
5. [Polygenic Scores — Neurological & Psychiatric](#5-polygenic-scores--neurological--psychiatric)
6. [Polygenic Scores — Traits & Behavioral](#6-polygenic-scores--traits--behavioral)
7. [Polygenic Scores — Lifestyle, Aging & Other Medical](#7-polygenic-scores--lifestyle-aging--other-medical)
8. [Monogenic Disease Screening](#8-monogenic-disease-screening)
9. [Carrier Status — Recessive Diseases](#9-carrier-status--recessive-diseases)
10. [Single-Variant Health Risk Markers](#10-single-variant-health-risk-markers)
11. [Pharmacogenomics (PGx)](#11-pharmacogenomics-pgx)
12. [Fun & Interesting Trait Variants](#12-fun--interesting-trait-variants)
13. [Ancestry & Population Genetics](#13-ancestry--population-genetics)
14. [Advanced Analyses](#14-advanced-analyses)
15. [Databases, Tools & References](#15-databases-tools--references)

---

## 1. Polygenic Scores — Cancer

| Condition | Best PGS ID | Variants | Performance | Study | Pop | PGS Catalog URL |
|-----------|------------|----------|-------------|-------|-----|-----------------|
| **Breast cancer** | [PGS000335](https://www.pgscatalog.org/score/PGS000335/) | 1,079,089 | OR 1.80, HR 1.71 (EUR) | Mars N et al., *Nat Commun* 2020 | EUR | https://www.pgscatalog.org/score/PGS000335/ |
| Breast cancer (classic) | [PGS000004](https://www.pgscatalog.org/score/PGS000004/) | 313 | AUROC 0.63, OR 1.61 | Mavaddat N et al., *AJHG* 2018 | EUR (158K) | https://www.pgscatalog.org/score/PGS000004/ |
| Breast cancer (first PGS ever) | [PGS000001](https://www.pgscatalog.org/score/PGS000001/) | 77 | OR 1.55 | Mavaddat N et al., *JNCI* 2015 | EUR | https://www.pgscatalog.org/score/PGS000001/ |
| **Prostate cancer** | [PGS000662](https://www.pgscatalog.org/score/PGS000662/) | 269 | AUROC 0.833, OR 4.17 (top 10% vs mid) | Conti DV et al., *Nat Genet* 2021 | Multi (234K) | https://www.pgscatalog.org/score/PGS000662/ |
| Prostate cancer (hazard) | [PGS000067](https://www.pgscatalog.org/score/PGS000067/) | 54 | HR 2.9 (top 2%), cross-ancestry | Seibert TM et al., *BMJ* 2018 | EUR (31.7K) | https://www.pgscatalog.org/score/PGS000067/ |
| **Colorectal cancer** | [PGS003850](https://www.pgscatalog.org/score/PGS003850/) | 205 | OR 1.62, AUROC 0.61 (EUR) | Fernandez-Rozadilla C et al., *Nat Genet* 2022 | Multi (255K) | https://www.pgscatalog.org/score/PGS003850/ |
| Colorectal cancer (genome-wide) | [PGS004580](https://www.pgscatalog.org/score/PGS004580/) | 1,099,906 | OR 1.50/SD | Youssef O et al., *Lab Invest* 2024 | EUR (93K) | https://www.pgscatalog.org/score/PGS004580/ |
| **Lung cancer** | [PGS000078](https://www.pgscatalog.org/score/PGS000078/) | 109 | AUROC 0.846, HR 1.26 | Graff RE et al., *Nat Commun* 2021 | EUR (184K) | https://www.pgscatalog.org/score/PGS000078/ |
| **Ovarian cancer** | [PGS005086](https://www.pgscatalog.org/score/PGS005086/) | 64,518 | AUROC 0.607, OR 1.46 (EUR) | Barnes DR et al., *NPJ Genom Med* 2025 | EUR (151K) | https://www.pgscatalog.org/score/PGS005086/ |
| **Pancreatic cancer** | [PGS002264](https://www.pgscatalog.org/score/PGS002264/) | 49 | AUROC 0.605 (alone), 0.83 (w/clinical) | Sharma S et al., *Gastroenterology* 2022 | EUR (436K) | https://www.pgscatalog.org/score/PGS002264/ |
| **Melanoma** | [PGS000743](https://www.pgscatalog.org/score/PGS000743/) | 45 | AUROC 0.74, OR 5.88 (decile) | Cust AE et al., *J Invest Dermatol* 2018 | EUR (36K) | https://www.pgscatalog.org/score/PGS000743/ |
| **Bladder cancer** | [PGS000071](https://www.pgscatalog.org/score/PGS000071/) | 15 | AUROC 0.803 | Graff RE et al., *Nat Commun* 2021 | EUR (464K) | https://www.pgscatalog.org/score/PGS000071/ |
| **Kidney cancer** | [PGS000787](https://www.pgscatalog.org/score/PGS000787/) | 19 | AUROC 0.722 | Kachuri L et al., *Nat Commun* 2020 | EUR (392K) | https://www.pgscatalog.org/score/PGS000787/ |
| **Testicular cancer** | [PGS000086](https://www.pgscatalog.org/score/PGS000086/) | 52 | AUROC 0.783, OR 2.29 | Graff RE et al., *Nat Commun* 2021 | EUR (57K) | https://www.pgscatalog.org/score/PGS000086/ |
| **Thyroid cancer** | [PGS000636](https://www.pgscatalog.org/score/PGS000636/) | 954 | AUROC 0.578, OR 3.21 (top 1%) | Fritsche LG et al., *AJHG* 2020 | EUR (408K) | https://www.pgscatalog.org/score/PGS000636/ |
| **Basal cell carcinoma** | [PGS000119](https://www.pgscatalog.org/score/PGS000119/) | 32 | OR 1.65, AUROC 0.64 | Fritsche LG et al., *PLoS Genet* 2019 | EUR (10K) | https://www.pgscatalog.org/score/PGS000119/ |
| **Gastric cancer** | [PGS005161](https://www.pgscatalog.org/score/PGS005161/) | 12 | HR 1.27/SD | Zhu M et al., *PLoS Med* 2025 | EAS (21K) | https://www.pgscatalog.org/score/PGS005161/ |
| **Endometrial cancer** | [PGS002735](https://www.pgscatalog.org/score/PGS002735/) | 19 | AUROC 0.56, OR 1.55 | Bafligil C et al., *Genet Med* 2022 | EUR (119K) | https://www.pgscatalog.org/score/PGS002735/ |

---

## 2. Polygenic Scores — Cardiovascular

| Condition | Best PGS ID | Variants | Performance | Study | Pop | PGS Catalog URL |
|-----------|------------|----------|-------------|-------|-----|-----------------|
| **CAD (multi-ancestry)** | [PGS003725](https://www.pgscatalog.org/score/PGS003725/) | 1,296,172 | HR 1.75, OR 2.14 (EUR) | Patel AP et al., *Nat Med* 2023 | Multi | https://www.pgscatalog.org/score/PGS003725/ |
| CAD (metaGRS) | [PGS000018](https://www.pgscatalog.org/score/PGS000018/) | 1,745,179 | AUROC 0.79, HR 1.71/SD | Inouye M et al., *JACC* 2018 | EUR (382K) | https://www.pgscatalog.org/score/PGS000018/ |
| CAD (GPS) | [PGS000013](https://www.pgscatalog.org/score/PGS000013/) | 6,630,150 | AUROC 0.81 | Khera AV et al., *Nat Genet* 2018 | EUR (120K UKB) | https://www.pgscatalog.org/score/PGS000013/ |
| **Atrial fibrillation** | [PGS000016](https://www.pgscatalog.org/score/PGS000016/) | 6,730,541 | AUROC 0.78 | Khera AV et al., *Nat Genet* 2018 | Multi | https://www.pgscatalog.org/score/PGS000016/ |
| **Heart failure** | [PGS005097](https://www.pgscatalog.org/score/PGS005097/) | 1,274,692 | AUROC 0.72 | Lee DSM et al., *Nat Genet* 2025 | Multi (2.3M) | https://www.pgscatalog.org/score/PGS005097/ |
| **Ischemic stroke** | [PGS002724](https://www.pgscatalog.org/score/PGS002724/) | 1,213,574 | HR 1.19, C-index 0.645 | Mishra A et al., *Nature* 2022 | Multi (12.8M) | https://www.pgscatalog.org/score/PGS002724/ |
| **VTE** | [PGS000043](https://www.pgscatalog.org/score/PGS000043/) | 297 | OR 2.89 (top 5% vs rest) | Klarin D et al., *Nat Genet* 2019 | Multi (650K) | https://www.pgscatalog.org/score/PGS000043/ |
| VTE (genome-wide) | [PGS003332](https://www.pgscatalog.org/score/PGS003332/) | 1,092,045 | OR 1.51/SD, AUROC 0.68 | Ghouse J et al., *Nat Genet* 2023 | EUR (1.06M) | https://www.pgscatalog.org/score/PGS003332/ |
| **PAD** | [PGS005217](https://www.pgscatalog.org/score/PGS005217/) | 1,296,292 | OR 1.66, C-index 0.731 | Flores AM et al., *JAMA Cardiol* 2025 | EUR (96K) | https://www.pgscatalog.org/score/PGS005217/ |
| **Aortic aneurysm** | [PGS003972](https://www.pgscatalog.org/score/PGS003972/) | 1,118,997 | AUROC 0.64-0.69 | Roychowdhury T et al., *Nat Genet* 2023 | Multi (1.1M) | https://www.pgscatalog.org/score/PGS003972/ |
| **Hypertension** | [PGS004192](https://www.pgscatalog.org/score/PGS004192/) | 9,430 | AUROC 0.703 | Raben TG et al., *Sci Rep* 2023 | EUR (200K) | https://www.pgscatalog.org/score/PGS004192/ |

---

## 3. Polygenic Scores — Metabolic & Endocrine

| Condition | Best PGS ID | Variants | Performance | Study | Pop | PGS Catalog URL |
|-----------|------------|----------|-------------|-------|-----|-----------------|
| **Type 2 diabetes (multi)** | [PGS002308](https://www.pgscatalog.org/score/PGS002308/) | 1,259,754 | AUROC 0.793 (EUR), 0.81 (EAS) | Ge T et al., *Genome Med* 2022 | Multi | https://www.pgscatalog.org/score/PGS002308/ |
| T2D (classic) | [PGS000014](https://www.pgscatalog.org/score/PGS000014/) | 6,917,436 | AUROC 0.73 | Khera AV et al., *Nat Genet* 2018 | EUR (120K) | https://www.pgscatalog.org/score/PGS000014/ |
| **Type 1 diabetes** | [PGS004174](https://www.pgscatalog.org/score/PGS004174/) | 49 | AUROC 0.71 | Raben TG et al., *Sci Rep* 2023 | EUR (200K) | https://www.pgscatalog.org/score/PGS004174/ |
| **BMI / Obesity** | [PGS000027](https://www.pgscatalog.org/score/PGS000027/) | 2,100,302 | R²=0.085; top decile 13kg heavier | Khera AV et al., *Cell* 2019 | EUR (120K) | https://www.pgscatalog.org/score/PGS000027/ |
| **Celiac disease** | [PGS000040](https://www.pgscatalog.org/score/PGS000040/) | 228 | **AUROC 0.90** (best PGS ever) | Abraham G et al., *PLoS Genet* 2014 | EUR (6.8K) | https://www.pgscatalog.org/score/PGS000040/ |
| **Gout** | [PGS004768](https://www.pgscatalog.org/score/PGS004768/) | 1,580,311 | R²=0.081 | Truong B et al., *Cell Genomics* 2024 | EUR (9.5K) | https://www.pgscatalog.org/score/PGS004768/ |
| **Hypothyroidism** | [PGS000820](https://www.pgscatalog.org/score/PGS000820/) | 890,908 | OR 1.33, AUROC 0.60 | Luo J et al., *Clin Cancer Res* 2021 | EUR (459K) | https://www.pgscatalog.org/score/PGS000820/ |
| **Osteoporosis / BMD** | [PGS002632](https://www.pgscatalog.org/score/PGS002632/) | 432,286 | R²=0.250 (EUR) | Weissbrod O et al., *Nat Genet* 2022 | EUR (328K) | https://www.pgscatalog.org/score/PGS002632/ |
| **LDL Cholesterol** | [PGS000115](https://www.pgscatalog.org/score/PGS000115/) | 223 | R²=0.09 (EUR) | Trinder M et al., *JAMA Cardiol* 2020 | Multi (298K) | https://www.pgscatalog.org/score/PGS000115/ |
| **HDL Cholesterol** | [PGS004775](https://www.pgscatalog.org/score/PGS004775/) | 1,120,830 | R²=0.155 | Truong B et al., *Cell Genomics* 2024 | EUR (16K) | https://www.pgscatalog.org/score/PGS004775/ |
| **Triglycerides** | [PGS000066](https://www.pgscatalog.org/score/PGS000066/) | 101 | r=0.235 (EUR) | Kuchenbaecker K et al., *Nat Commun* 2019 | Multi (331K) | https://www.pgscatalog.org/score/PGS000066/ |
| **HbA1c** | [PGS004044](https://www.pgscatalog.org/score/PGS004044/) | 907,906 | R²=0.039 | Monti R et al., *AJHG* 2024 | EUR (344K) | https://www.pgscatalog.org/score/PGS004044/ |
| **NAFLD** | [PGS002283](https://www.pgscatalog.org/score/PGS002283/) | 15 | beta=0.094 (ALT levels) | Schnurr TM et al., *Hepatol Commun* 2022 | Multi (219K) | https://www.pgscatalog.org/score/PGS002283/ |

---

## 4. Polygenic Scores — Autoimmune & Inflammatory

| Condition | Best PGS ID | Variants | Performance | Study | Pop | PGS Catalog URL |
|-----------|------------|----------|-------------|-------|-----|-----------------|
| **IBD** | [PGS004081](https://www.pgscatalog.org/score/PGS004081/) | 1,073,268 | AUROC 0.68 (EUR) | Monti R et al., *AJHG* 2024 | EUR | https://www.pgscatalog.org/score/PGS004081/ |
| **Crohn's disease** | [PGS004254](https://www.pgscatalog.org/score/PGS004254/) | 744,682 | AUROC 0.72, OR 2.18 | Middha P et al., *Nat Commun* 2024 | EUR (12K) | https://www.pgscatalog.org/score/PGS004254/ |
| **Ulcerative colitis** | [PGS004253](https://www.pgscatalog.org/score/PGS004253/) | 744,575 | OR 1.84, AUROC 0.66 | Middha P et al., *Nat Commun* 2024 | EUR (13K) | https://www.pgscatalog.org/score/PGS004253/ |
| **Rheumatoid arthritis** | [PGS002745](https://www.pgscatalog.org/score/PGS002745/) | 2,575 | AUROC 0.66 (EUR & EAS) | Ishigaki K et al., *Nat Genet* 2022 | Multi (276K) | https://www.pgscatalog.org/score/PGS002745/ |
| **Multiple sclerosis** | [PGS004700](https://www.pgscatalog.org/score/PGS004700/) | 12 (HLA-GRS) | AUROC 0.76 (combined) | Loginovic P et al., *Nat Commun* 2024 | EUR (48K) | https://www.pgscatalog.org/score/PGS004700/ |
| **Lupus (SLE)** | [PGS000328](https://www.pgscatalog.org/score/PGS000328/) | 57 | AUROC 0.83, OR 12.32 (quartile) | Reid S et al., *Ann Rheum Dis* 2019 | EUR (14K) | https://www.pgscatalog.org/score/PGS000328/ |
| **Asthma** | [PGS002311](https://www.pgscatalog.org/score/PGS002311/) | 1,109,311 | R²=0.024 (EUR) | Weissbrod O et al., *Nat Genet* 2022 | EUR (337K) | https://www.pgscatalog.org/score/PGS002311/ |
| **Psoriatic arthropathy** | [PGS001287](https://www.pgscatalog.org/score/PGS001287/) | 36 | AUROC 0.73 (EUR non-British) | Tanigawa Y et al., *PLoS Genet* 2022 | EUR (270K) | https://www.pgscatalog.org/score/PGS001287/ |
| **Atopic dermatitis** | [PGS004903](https://www.pgscatalog.org/score/PGS004903/) | 38 | Significant in EUR | Al-Janabi A et al., *JACI* 2023 | EUR (361K) | https://www.pgscatalog.org/score/PGS004903/ |

---

## 5. Polygenic Scores — Neurological & Psychiatric

| Condition | Best PGS ID | Variants | Performance | Study | Pop | PGS Catalog URL |
|-----------|------------|----------|-------------|-------|-----|-----------------|
| **Alzheimer's disease** | [PGS004590](https://www.pgscatalog.org/score/PGS004590/) | 363 | AUROC 0.68 (excl APOE) | Lake J et al., *Mol Psychiatry* 2023 | Multi (644K) | https://www.pgscatalog.org/score/PGS004590/ |
| **Parkinson's disease** | [PGS000903](https://www.pgscatalog.org/score/PGS000903/) | 1,805 | AUROC 0.692, OR 6.25 (Q4 vs Q1) | Nalls MA et al., *Lancet Neurol* 2019 | Multi (1.5M) | https://www.pgscatalog.org/score/PGS000903/ |
| **Schizophrenia** | [PGS000135](https://www.pgscatalog.org/score/PGS000135/) | 972,439 | AUROC 0.74 | Zheutlin AB et al., *Am J Psychiatry* 2019 | EUR/EAS (82K) | https://www.pgscatalog.org/score/PGS000135/ |
| **Major depression** | [PGS003333](https://www.pgscatalog.org/score/PGS003333/) | 1,088,415 | R²=0.022 | Fang Y et al., *Biol Psychiatry* 2022 | EUR (808K) | https://www.pgscatalog.org/score/PGS003333/ |
| **Bipolar disorder** | [PGS002787](https://www.pgscatalog.org/score/PGS002787/) | 937,511 | Modest R² | Gui Y et al., *Transl Psychiatry* 2022 | EUR (46K PGC) | https://www.pgscatalog.org/score/PGS002787/ |
| **ADHD** | [PGS002746](https://www.pgscatalog.org/score/PGS002746/) | 513,659 | beta=0.11 for symptoms | Lahey BB et al., *J Psychiatr Res* 2022 | Multi (55K) | https://www.pgscatalog.org/score/PGS002746/ |
| **Autism spectrum** | [PGS000327](https://www.pgscatalog.org/score/PGS000327/) | 35,087 | OR 1.33, R²=0.025 | Grove J et al., *Nat Genet* 2019 | EUR (46K) | https://www.pgscatalog.org/score/PGS000327/ |
| **Anxiety disorders** | [PGS004451](https://www.pgscatalog.org/score/PGS004451/) | 1,059,939 | OR 1.19/SD | Jung H et al., *Commun Biol* 2024 | EUR (174K) | https://www.pgscatalog.org/score/PGS004451/ |
| **PTSD** | [PGS005393](https://www.pgscatalog.org/score/PGS005393/) | 53,705 | R²=0.087 | Bugiga AVG et al., *Braz J Psychiatry* 2024 | Multi | https://www.pgscatalog.org/score/PGS005393/ |
| **Migraine** | [PGS004798](https://www.pgscatalog.org/score/PGS004798/) | 3,984,158 | R²=0.004 | Truong B et al., *Cell Genomics* 2024 | SAS (35K) | https://www.pgscatalog.org/score/PGS004798/ |
| **Epilepsy** | [PGS004881](https://www.pgscatalog.org/score/PGS004881/) | 605,432 | HR 1.12/SD | Jermy B et al., *Nat Commun* 2024 | EUR (447K) | https://www.pgscatalog.org/score/PGS004881/ |

---

## 6. Polygenic Scores — Traits & Behavioral

| Trait | Best PGS ID | Variants | Performance | Study | PGS Catalog URL |
|-------|------------|----------|-------------|-------|-----------------|
| **Height** | [PGS001229](https://www.pgscatalog.org/score/PGS001229/) | 51,209 | R²=0.717 (EUR) | Tanigawa Y et al., *PLoS Genet* 2022 | https://www.pgscatalog.org/score/PGS001229/ |
| **Hair color** | [PGS002598](https://www.pgscatalog.org/score/PGS002598/) | 8,312 | R²=0.182 (EUR) | Weissbrod O et al., *Nat Genet* 2022 | https://www.pgscatalog.org/score/PGS002598/ |
| Hair color (blonde) | [PGS001093](https://www.pgscatalog.org/score/PGS001093/) | 6,970 | AUROC 0.863 | Tanigawa Y et al., *PLoS Genet* 2022 | https://www.pgscatalog.org/score/PGS001093/ |
| **Skin pigmentation** | [PGS001897](https://www.pgscatalog.org/score/PGS001897/) | 15,817 | r=0.387 (EUR) | Prive F et al., *AJHG* 2022 | https://www.pgscatalog.org/score/PGS001897/ |
| **Baldness** | [PGS002314](https://www.pgscatalog.org/score/PGS002314/) | 1,109,311 | R²=0.143 (EUR) | Weissbrod O et al., *Nat Genet* 2022 | https://www.pgscatalog.org/score/PGS002314/ |
| **Body fat %** | [PGS003899](https://www.pgscatalog.org/score/PGS003899/) | 34,374 | R²=0.056 (EUR, PGS-only) | Tanigawa Y et al., *AJHG* 2023 | https://www.pgscatalog.org/score/PGS003899/ |
| **WHR (adj BMI)** | [PGS000299](https://www.pgscatalog.org/score/PGS000299/) | 462 | R²=0.020 | Xie T et al., *Circ GMP* 2020 | https://www.pgscatalog.org/score/PGS000299/ |
| **Educational attainment** | [PGS002012](https://www.pgscatalog.org/score/PGS002012/) | 50,413 | r=0.175 (EUR) | Prive F et al., *AJHG* 2022 | https://www.pgscatalog.org/score/PGS002012/ |
| **Intelligence (fluid)** | [PGS004427](https://www.pgscatalog.org/score/PGS004427/) | 1,059,939 | **R²=0.223** (EUR) | Jung H et al., *Commun Biol* 2024 | https://www.pgscatalog.org/score/PGS004427/ |
| **Risk tolerance** | [PGS000205](https://www.pgscatalog.org/score/PGS000205/) | 1,110,737 | dR²=0.016 | Barr PB et al., *Transl Psychiatry* 2020 | https://www.pgscatalog.org/score/PGS000205/ |
| **Neuroticism** | [PGS003565](https://www.pgscatalog.org/score/PGS003565/) | 979,739 | R²=0.05 (EUR) | Ding Y et al., *bioRxiv* 2022 | https://www.pgscatalog.org/score/PGS003565/ |

---

## 7. Polygenic Scores — Lifestyle, Aging & Other Medical

| Trait | Best PGS ID | Variants | Performance | Study | PGS Catalog URL |
|-------|------------|----------|-------------|-------|-----------------|
| **Coffee consumption** | [PGS001123](https://www.pgscatalog.org/score/PGS001123/) | 48 | AUROC 0.617 | Tanigawa Y et al., *PLoS Genet* 2022 | https://www.pgscatalog.org/score/PGS001123/ |
| **Alcohol (drinks/week)** | [PGS002752](https://www.pgscatalog.org/score/PGS002752/) | 1,089,551 | OR 1.19/SD | Mars N et al., *AJHG* 2022 | https://www.pgscatalog.org/score/PGS002752/ |
| **Alcohol use disorder** | [PGS005213](https://www.pgscatalog.org/score/PGS005213/) | 336,813 | R²=0.05 | Deng WQ et al., *Alcohol Alcoholism* 2024 | https://www.pgscatalog.org/score/PGS005213/ |
| **Smoking initiation** | [PGS003357](https://www.pgscatalog.org/score/PGS003357/) | 1,194,472 | dAUC=0.015 | Saunders GRB et al., *Nature* 2022 | https://www.pgscatalog.org/score/PGS003357/ |
| **Chronotype** | [PGS002586](https://www.pgscatalog.org/score/PGS002586/) | 255 | R²=0.004 (EUR) | Weissbrod O et al., *Nat Genet* 2022 | https://www.pgscatalog.org/score/PGS002586/ |
| **Sleep duration** | [PGS003764](https://www.pgscatalog.org/score/PGS003764/) | 78 | beta=0.62 (AFR) | Scammell BH et al., *Hum Mol Genet* 2023 | https://www.pgscatalog.org/score/PGS003764/ |
| **Insomnia** | [PGS000908](https://www.pgscatalog.org/score/PGS000908/) | 2,746,982 | OR 1.12-1.28 | Campos AI et al., *Commun Med* 2021 | https://www.pgscatalog.org/score/PGS000908/ |
| **Longevity** | [PGS000906](https://www.pgscatalog.org/score/PGS000906/) | 330 | HR 0.89/SD (11% lower mortality) | Tesi N et al., *J Gerontol* 2021 | https://www.pgscatalog.org/score/PGS000906/ |
| **CKD** | [PGS004889](https://www.pgscatalog.org/score/PGS004889/) | 1,117,375 | HR 1.33, C-index 0.727 | Mandla R et al., *Genome Med* 2024 | https://www.pgscatalog.org/score/PGS004889/ |
| **AMD** | [PGS004606](https://www.pgscatalog.org/score/PGS004606/) | 1,000,946 | OR 1.76, AUROC 0.71 | Gorman BR et al., *Nat Genet* 2024 | https://www.pgscatalog.org/score/PGS004606/ |
| **Glaucoma** | [PGS001797](https://www.pgscatalog.org/score/PGS001797/) | 885,417 | AUROC 0.749 | Wang Y et al., *Cell Genomics* 2023 | https://www.pgscatalog.org/score/PGS001797/ |
| **Endometriosis** | [PGS003447](https://www.pgscatalog.org/score/PGS003447/) | 14 | OR 1.28, AUROC 0.57 | Kloeve-Mogensen K et al., *Front Reprod Health* 2021 | https://www.pgscatalog.org/score/PGS003447/ |
| **Kidney stones** | [PGS004493](https://www.pgscatalog.org/score/PGS004493/) | 1,059,939 | OR 1.23/SD | Jung H et al., *Commun Biol* 2024 | https://www.pgscatalog.org/score/PGS004493/ |

---

## 8. Monogenic Disease Screening

Recommended gene panels for WGS-based screening, based on [ACMG SF v3.3 (2025)](https://pubmed.ncbi.nlm.nih.gov/40568962/) — **84 genes** total.

### Cancer Predisposition (~28 genes)
APC, BRCA1, BRCA2, BMPR1A, MAX, MEN1, MLH1, MSH2, MSH6, MUTYH, NF2, PALB2, PMS2, PTEN, RB1, RET, SDHAF2, SDHB, SDHC, SDHD, SMAD4, STK11, TMEM127, TP53, TSC1, TSC2, VHL, WT1

### Cardiovascular (~41 genes)
ACTA2, ACTC1, APOB, BAG3, CALM1-3, CASQ2, COL3A1, DES, DSC2, DSG2, DSP, FBN1, FLNC, KCNH2, KCNQ1, LDLR, LMNA, MYH7, MYH11, MYBPC3, MYL2, MYL3, PCSK9, PKP2, PLN, PRKAG2, RBM20, RYR2, SCN5A, SMAD3, TGFBR1/2, TMEM43, TNNC1, TNNI3, TNNT2, TPM1, TRDN, TTN

### Metabolism (~5 genes)
BTD, CYP27A1, GAA, GLA, OTC

### Miscellaneous (~10 genes)
ABCD1, ACVRL1, ATP7B, CACNA1S, ENG, HFE, HNF1A, RPE65, RYR1, TTR

---

## 9. Carrier Status — Recessive Diseases

| Disease | Gene | Key Variant | rs / ClinVar | Carrier Freq | Population | Links |
|---------|------|-------------|-------------|--------------|------------|-------|
| **Cystic fibrosis** | CFTR | F508del | [rs113993960](https://www.ncbi.nlm.nih.gov/snp/rs113993960) | 1 in 25 | N. European | [ClinVar](https://www.ncbi.nlm.nih.gov/clinvar/variation/7105/) |
| **Sickle cell disease** | HBB | Glu6Val (HbS) | [rs334](https://www.ncbi.nlm.nih.gov/snp/rs334) | 1 in 13 | African Amer. | [ClinVar](https://www.ncbi.nlm.nih.gov/clinvar/variation/15126/) |
| **Tay-Sachs** | HEXA | 1278insTATC | [rs387906309](https://www.ncbi.nlm.nih.gov/snp/rs387906309) | 1 in 30 | Ashkenazi | [GeneReviews](https://www.ncbi.nlm.nih.gov/books/NBK1218/) |
| **Gaucher disease** | GBA1 | N370S / Asn409Ser | [rs76763715](https://www.ncbi.nlm.nih.gov/snp/rs76763715) | 1 in 15 | Ashkenazi | [ClinVar 4290](https://www.ncbi.nlm.nih.gov/clinvar/variation/4290/) |
| **PKU** | PAH | R408W | [rs5030858](https://www.ncbi.nlm.nih.gov/snp/rs5030858) | 1 in 50 | European | [ClinVar 577](https://www.ncbi.nlm.nih.gov/clinvar/variation/577/) |
| **Beta-thalassemia** | HBB | Codon 39 C>T | [rs11549407](https://www.ncbi.nlm.nih.gov/snp/rs11549407) | 5-30% | Mediterranean | [ClinVar 15402](https://www.ncbi.nlm.nih.gov/clinvar/variation/15402/) |
| **FMF** | MEFV | M694V | [rs61752717](https://www.ncbi.nlm.nih.gov/snp/rs61752717) | 1 in 3-5 | Turkish/Armenian | [ClinVar 2538](https://www.ncbi.nlm.nih.gov/clinvar/variation/2538/) |
| **Hemochromatosis** | HFE | C282Y | [rs1800562](https://www.ncbi.nlm.nih.gov/snp/rs1800562) | 1 in 9 | N. European | [ClinVar 9](https://www.ncbi.nlm.nih.gov/clinvar/variation/9/) |
| **Wilson disease** | ATP7B | H1069Q | [rs76151636](https://www.ncbi.nlm.nih.gov/snp/rs76151636) | 1 in 90 | European | [ClinVar 3848](https://www.ncbi.nlm.nih.gov/clinvar/variation/3848/) |
| **SMA** | SMN1 | Exon 7 deletion | Structural variant | 1 in 40-50 | Pan-ethnic | [GeneReviews](https://www.ncbi.nlm.nih.gov/books/NBK1352/) |
| **Bloom syndrome** | BLM | blmAsh | [rs113993962](https://www.ncbi.nlm.nih.gov/snp/rs113993962) | 1 in 107 | Ashkenazi | [ClinVar 5454](https://www.ncbi.nlm.nih.gov/clinvar/variation/5454/) |
| **Canavan disease** | ASPA | E285A | [rs28940279](https://www.ncbi.nlm.nih.gov/snp/rs28940279) | 1 in 40 | Ashkenazi | [ClinVar 2605](https://www.ncbi.nlm.nih.gov/clinvar/variation/2605/) |
| **Niemann-Pick A** | SMPD1 | R608del | [rs120074118](https://www.ncbi.nlm.nih.gov/snp/rs120074118) | 1 in 90 | Ashkenazi | [ClinVar](https://www.ncbi.nlm.nih.gov/clinvar/?term=SMPD1) |
| **Fanconi anemia C** | FANCC | IVS4+4A>T | [rs104886456](https://www.ncbi.nlm.nih.gov/snp/rs104886456) | 1 in 89 | Ashkenazi | [ClinVar](https://www.ncbi.nlm.nih.gov/clinvar/RCV000012825/) |
| **Fragile X** | FMR1 | CGG expansion | Repeat expansion | 1 in 250 F | Pan-ethnic | [GeneReviews](https://www.ncbi.nlm.nih.gov/books/NBK1384/) |

---

## 10. Single-Variant Health Risk Markers

| Marker | Gene | rs Number | Effect | Freq | Links |
|--------|------|-----------|--------|------|-------|
| **APOE e4 (Alzheimer's)** | APOE | [rs429358](https://www.ncbi.nlm.nih.gov/snp/rs429358) + [rs7412](https://www.ncbi.nlm.nih.gov/snp/rs7412) | e4/e4 = 12-15x AD risk; e2 = protective | e4: 14% | [ClinVar 17864](https://www.ncbi.nlm.nih.gov/clinvar/variation/17864/) |
| **Factor V Leiden** | F5 | [rs6025](https://www.ncbi.nlm.nih.gov/snp/rs6025) | 3-8x VTE risk (het); 80x (hom) | 5% EUR | [ClinVar 642](https://www.ncbi.nlm.nih.gov/clinvar/variation/642/), [SNPedia](https://www.snpedia.com/index.php/Rs6025) |
| **Prothrombin G20210A** | F2 | [rs1799963](https://www.ncbi.nlm.nih.gov/snp/rs1799963) | 2.8x VTE risk | 2-3% EUR | [ClinVar 13310](https://www.ncbi.nlm.nih.gov/clinvar/variation/13310/) |
| **BRCA1 185delAG** | BRCA1 | [rs80357713](https://www.ncbi.nlm.nih.gov/snp/rs80357713) | 45-85% lifetime breast cancer risk | 1% Ashkenazi | [ClinVar 17662](https://www.ncbi.nlm.nih.gov/clinvar/variation/17662/) |
| **BRCA1 5382insC** | BRCA1 | [rs80357906](https://www.ncbi.nlm.nih.gov/snp/rs80357906) | Same as above | Ashkenazi/Slavic | [ClinVar 17677](https://www.ncbi.nlm.nih.gov/clinvar/variation/17677/) |
| **BRCA2 6174delT** | BRCA2 | [rs80359550](https://www.ncbi.nlm.nih.gov/snp/rs80359550) | High breast/ovarian/prostate risk | 1.5% Ashkenazi | [ClinVar 9325](https://www.ncbi.nlm.nih.gov/clinvar/variation/9325/) |
| **MTHFR C677T** | MTHFR | [rs1801133](https://www.ncbi.nlm.nih.gov/snp/rs1801133) | TT = 30% enzyme activity; elevated homocysteine | 10-15% EUR (TT) | [ClinVar 3520](https://www.ncbi.nlm.nih.gov/clinvar/variation/3520/), [SNPedia](https://www.snpedia.com/index.php/Rs1801133) |
| **A1AT Z allele** | SERPINA1 | [rs28929474](https://www.ncbi.nlm.nih.gov/snp/rs28929474) | ZZ = emphysema + liver disease | Z: 2-3% N.EUR | [ClinVar 17967](https://www.ncbi.nlm.nih.gov/clinvar/variation/17967/) |
| **A1AT S allele** | SERPINA1 | [rs17580](https://www.ncbi.nlm.nih.gov/snp/rs17580) | SZ compound = moderate risk | S: 5-10% S.EUR | [ClinVar 17969](https://www.ncbi.nlm.nih.gov/clinvar/variation/17969/) |
| **PCSK9 R46L (protective)** | PCSK9 | [rs11591147](https://www.ncbi.nlm.nih.gov/snp/rs11591147) | Loss-of-function: ~50% lower LDL | 2% EUR | [ClinVar 2878](https://www.ncbi.nlm.nih.gov/clinvar/variation/2878/) |
| **LRRK2 G2019S (Parkinson's)** | LRRK2 | [rs34637584](https://www.ncbi.nlm.nih.gov/snp/rs34637584) | 25-42% lifetime PD risk (dominant) | 0.84% Ashkenazi | [ClinVar 1940](https://www.ncbi.nlm.nih.gov/clinvar/variation/1940/), [SNPedia](https://www.snpedia.com/index.php/Rs34637584) |
| **CHEK2 1100delC** | CHEK2 | [rs555607708](https://www.ncbi.nlm.nih.gov/snp/rs555607708) | ~2x breast cancer risk | 0.7% N.EUR | [ClinVar 128042](https://www.ncbi.nlm.nih.gov/clinvar/variation/128042/) |
| **FTO obesity** | FTO | [rs9939609](https://www.ncbi.nlm.nih.gov/snp/rs9939609) | AA = 1.67x obesity risk, ~3kg heavier | A: 42% EUR | [SNPedia](https://www.snpedia.com/index.php/Rs9939609) |
| **TCF7L2 diabetes** | TCF7L2 | [rs7903146](https://www.ncbi.nlm.nih.gov/snp/rs7903146) | TT = 1.8x T2D risk (strongest common variant) | T: 30% EUR | [SNPedia](https://www.snpedia.com/index.php/Rs7903146) |
| **9p21 CAD locus** | CDKN2A/B | [rs10757278](https://www.ncbi.nlm.nih.gov/snp/rs10757278) | GG = 1.6x MI risk | G: 49% EUR | [SNPedia](https://www.snpedia.com/index.php/Rs10757278) |

---

## 11. Pharmacogenomics (PGx)

As of 2025, CPIC covers **34 genes and 164 drugs** across 28 guidelines. The [PREPARE trial](https://upgx.eu/study/) (Lancet 2023, 6,944 patients, 7 EU countries) showed a 12-gene PGx panel reduces adverse drug reactions by **30%**.

| Gene | Chr | Key Variants (rs numbers) | Primary Drugs | CPIC Level | Metabolizer Types | Links |
|------|-----|--------------------------|---------------|------------|-------------------|-------|
| **CYP2D6** | 22q13 | *3 (rs35742686), *4 (rs3892097), *10 (rs1065852), *17 (rs28371706), *41 (rs28371725) + CNV | Codeine, tamoxifen, SSRIs, TCAs, ondansetron | A | UM, NM, IM, PM | [PharmGKB](https://www.pharmgkb.org/gene/PA128), [CPIC](https://cpicpgx.org/gene/cyp2d6/) |
| **CYP2C19** | 10q23 | *2 ([rs4244285](https://www.ncbi.nlm.nih.gov/snp/rs4244285)), *3 ([rs4986893](https://www.ncbi.nlm.nih.gov/snp/rs4986893)), *17 ([rs12248560](https://www.ncbi.nlm.nih.gov/snp/rs12248560)) | Clopidogrel, PPIs, voriconazole, SSRIs | A | UM, RM, NM, IM, PM | [PharmGKB](https://www.pharmgkb.org/gene/PA124), [CPIC](https://cpicpgx.org/guidelines/guideline-for-clopidogrel-and-cyp2c19/) |
| **CYP2C9** | 10q23 | *2 ([rs1799853](https://www.ncbi.nlm.nih.gov/snp/rs1799853)), *3 ([rs1057910](https://www.ncbi.nlm.nih.gov/snp/rs1057910)) | Warfarin, NSAIDs, phenytoin | A | NM, IM, PM | [PharmGKB](https://www.pharmgkb.org/gene/PA126), [CPIC](https://cpicpgx.org/guidelines/guideline-for-warfarin-and-cyp2c9-and-vkorc1/) |
| **VKORC1** | 16p11 | [rs9923231](https://www.ncbi.nlm.nih.gov/snp/rs9923231) (-1639G>A) | Warfarin | A | Low/Int/High sensitivity | [PharmGKB](https://www.pharmgkb.org/gene/PA133787052), [CPIC](https://cpicpgx.org/guidelines/guideline-for-warfarin-and-cyp2c9-and-vkorc1/) |
| **CYP3A5** | 7q22 | *3 ([rs776746](https://www.ncbi.nlm.nih.gov/snp/rs776746)) | Tacrolimus | A | Expresser, Non-expresser | [PharmGKB](https://www.pharmgkb.org/gene/PA131), [CPIC](https://cpicpgx.org/guidelines/guideline-for-tacrolimus-and-cyp3a5/) |
| **DPYD** | 1p21 | *2A ([rs3918290](https://www.ncbi.nlm.nih.gov/snp/rs3918290)), *13 ([rs55886062](https://www.ncbi.nlm.nih.gov/snp/rs55886062)), D949V ([rs67376798](https://www.ncbi.nlm.nih.gov/snp/rs67376798)), HapB3 ([rs75017182](https://www.ncbi.nlm.nih.gov/snp/rs75017182)) | 5-FU, capecitabine (chemo) | A | NM, IM, PM (**PM = fatal**) | [PharmGKB](https://www.pharmgkb.org/gene/PA145), [CPIC](https://cpicpgx.org/guidelines/guideline-for-fluoropyrimidines-and-dpyd/) |
| **TPMT** | 6p22 | *2 ([rs1800462](https://www.ncbi.nlm.nih.gov/snp/rs1800462)), *3A (*3B+*3C), *3B ([rs1800460](https://www.ncbi.nlm.nih.gov/snp/rs1800460)), *3C ([rs1142345](https://www.ncbi.nlm.nih.gov/snp/rs1142345)) | Azathioprine, 6-MP, thioguanine | A | NM, IM, PM | [PharmGKB](https://www.pharmgkb.org/gene/PA356), [CPIC](https://cpicpgx.org/guidelines/guideline-for-thiopurines-and-tpmt/) |
| **NUDT15** | 13q14 | *3 ([rs116855232](https://www.ncbi.nlm.nih.gov/snp/rs116855232)) | Same thiopurines | A | NM, IM, PM | [PharmGKB](https://www.pharmgkb.org/gene/PA166228076), [CPIC](https://cpicpgx.org/guidelines/guideline-for-thiopurines-and-tpmt/) |
| **UGT1A1** | 2q37 | *28 ([rs8175347](https://www.ncbi.nlm.nih.gov/snp/rs8175347)), *6 ([rs4148323](https://www.ncbi.nlm.nih.gov/snp/rs4148323)) | Atazanavir, irinotecan | A | NM, IM, PM | [PharmGKB](https://www.pharmgkb.org/gene/PA420), [CPIC](https://cpicpgx.org/guidelines/guideline-for-atazanavir-and-ugt1a1/) |
| **HLA-B\*57:01** | 6p21 | HLA typing | Abacavir (**global standard**) | A | Positive/Negative | [PharmGKB](https://www.pharmgkb.org/gene/PA35056), [CPIC](https://cpicpgx.org/guidelines/guideline-for-abacavir-and-hla-b/) |
| **HLA-B\*15:02** | 6p21 | HLA typing | Carbamazepine (SJS/TEN risk) | A | Positive/Negative | [CPIC](https://cpicpgx.org/guidelines/guideline-for-carbamazepine-and-hla-b/) |
| **HLA-A\*31:01** | 6p21 | HLA typing | Carbamazepine (DRESS/SJS) | A | Positive/Negative | [CPIC](https://cpicpgx.org/guidelines/guideline-for-carbamazepine-and-hla-b/) |
| **SLCO1B1** | 12p12 | *5 ([rs4149056](https://www.ncbi.nlm.nih.gov/snp/rs4149056)), *14 ([rs2306283](https://www.ncbi.nlm.nih.gov/snp/rs2306283)) | All statins (esp. simvastatin) | A | Normal, Decreased, Poor | [PharmGKB](https://www.pharmgkb.org/gene/PA134865839), [CPIC](https://cpicpgx.org/guidelines/cpic-guideline-for-statins/) |
| **ABCG2** | 4q22 | [rs2231142](https://www.ncbi.nlm.nih.gov/snp/rs2231142) (Q141K) | Rosuvastatin | A | Normal, Decreased, Poor | [PharmGKB](https://www.pharmgkb.org/gene/PA390), [CPIC](https://cpicpgx.org/guidelines/cpic-guideline-for-statins/) |
| **G6PD** | Xq28 | A- ([rs1050828](https://www.ncbi.nlm.nih.gov/snp/rs1050828)+[rs1050829](https://www.ncbi.nlm.nih.gov/snp/rs1050829)), Med ([rs5030868](https://www.ncbi.nlm.nih.gov/snp/rs5030868)) | Rasburicase (**contraindicated**), primaquine, dapsone + 42 more | A | Normal, Deficient | [PharmGKB](https://www.pharmgkb.org/gene/PA28469), [CPIC](https://cpicpgx.org/guidelines/cpic-guideline-for-g6pd/) |
| **IFNL3** | 19q13 | [rs12979860](https://www.ncbi.nlm.nih.gov/snp/rs12979860), [rs8099917](https://www.ncbi.nlm.nih.gov/snp/rs8099917) | PEG-IFN + ribavirin (HCV) | A | Favorable (CC), Unfavorable | [PharmGKB](https://www.pharmgkb.org/gene/PA134952671), [CPIC](https://cpicpgx.org/guidelines/guideline-for-peg-interferon-alpha-based-regimens-and-ifnl3/) |
| **NAT2** | 8p22 | *5 ([rs1801280](https://www.ncbi.nlm.nih.gov/snp/rs1801280)), *6 ([rs1799930](https://www.ncbi.nlm.nih.gov/snp/rs1799930)), *7 ([rs1799931](https://www.ncbi.nlm.nih.gov/snp/rs1799931)) | Hydralazine (**new 2025 guideline**), isoniazid | A | Rapid, Intermediate, Poor | [PharmGKB](https://www.pharmgkb.org/gene/PA18), [CPIC](https://cpicpgx.org/guidelines/cpic-guideline-for-hydralazine-and-nat2/) |
| **CYP2B6** | 19q13 | *6 ([rs3745274](https://www.ncbi.nlm.nih.gov/snp/rs3745274)), *9 ([rs3211371](https://www.ncbi.nlm.nih.gov/snp/rs3211371)) | Efavirenz, methadone, bupropion | A | UM, RM, NM, IM, PM | [PharmGKB](https://www.pharmgkb.org/gene/PA123), [CPIC](https://cpicpgx.org/guidelines/cpic-guideline-for-efavirenz-based-on-cyp2b6-genotype/) |
| **RYR1/CACNA1S** | 19q13/1q32 | 50+ pathogenic variants | Volatile anesthetics, succinylcholine (**malignant hyperthermia**) | A | MH Susceptible / Not | [PharmGKB RYR1](https://www.pharmgkb.org/gene/PA34896), [CPIC](https://cpicpgx.org/guidelines/cpic-guideline-for-ryr1-and-cacna1s/) |
| **CYP4F2** | 19p13 | *3 ([rs2108622](https://www.ncbi.nlm.nih.gov/snp/rs2108622)) | Warfarin (modifier) | A | — | [PharmGKB](https://www.pharmgkb.org/gene/PA27121), [CPIC](https://cpicpgx.org/guidelines/guideline-for-warfarin-and-cyp2c9-and-vkorc1/) |
| **CFTR** | 7q31 | G551D + 37 responsive variants | Ivacaftor, Trikafta | A | Responsive / Non-responsive | [PharmGKB](https://www.pharmgkb.org/gene/PA109), [CPIC](https://cpicpgx.org/guidelines/guideline-for-ivacaftor-and-cftr/) |

---

## 12. Fun & Interesting Trait Variants

| Trait | Gene | SNP(s) | Genotype Effect | Links |
|-------|------|--------|-----------------|-------|
| **Bitter taste (PTC)** | TAS2R38 | rs713598, rs1726866, rs10246939 | PAV/PAV = supertaster; AVI/AVI = non-taster | [SNPedia](https://www.snpedia.com/index.php/Rs713598) |
| **Cilantro = soap** | OR6A2 | rs72921001 | A allele = soapy perception | [SNPedia](https://www.snpedia.com/index.php/Rs72921001) |
| **Asparagus smell** | OR2M7 | rs4481887 | G allele = can detect it | [SNPedia](https://www.snpedia.com/index.php/Rs4481887) |
| **Earwax type** | ABCC11 | rs17822931 | TT = dry earwax + less body odor; CC/CT = wet | [SNPedia](https://www.snpedia.com/index.php/Rs17822931) |
| **Lactose tolerance** | MCM6 | rs4988235 | T = persistent (can drink milk); CC = intolerant | [SNPedia](https://www.snpedia.com/index.php/Rs4988235) |
| **Alcohol flush** | ALDH2 | rs671 | AA = near-zero ALDH2, cannot drink; also esophageal cancer risk | [SNPedia](https://www.snpedia.com/index.php/Rs671) |
| **Caffeine metabolism** | CYP1A2 | rs762551 | AA = fast metabolizer; CC = slow (hypertension risk) | [SNPedia](https://www.snpedia.com/index.php/Rs762551) |
| **Sprint vs endurance** | ACTN3 | rs1815739 | CC (RR) = sprint/power; TT (XX) = endurance | [SNPedia](https://www.snpedia.com/index.php/Rs1815739) |
| **Red hair** | MC1R | rs1805007, rs1805008 | Two variants = red hair; one = reddish tints + pain sensitivity | [SNPedia](https://www.snpedia.com/index.php/Rs1805007) |
| **Eye color** | HERC2 | rs12913832 | GG = blue; AA = brown; AG = green/hazel | [SNPedia](https://www.snpedia.com/index.php/Rs12913832) |
| **Freckling** | IRF4 | rs12203592 | T allele = freckling + sun sensitivity | [SNPedia](https://www.snpedia.com/index.php/Rs12203592) |
| **Skin pigmentation** | SLC24A5 | rs1426654 | A (Thr111) = lighter skin; nearly fixed in Europeans | [SNPedia](https://www.snpedia.com/index.php/Rs1426654) |
| **Photic sneeze** | ZEB2 | rs10427255 | C allele = sneeze when exposed to bright light (18-35% of people) | [SNPedia](https://www.snpedia.com/index.php/Rs10427255) |
| **Norovirus resistance** | FUT2 | rs601338 | AA (non-secretor) = strong norovirus resistance | [SNPedia](https://www.snpedia.com/index.php/Rs601338) |
| **Warrior/Worrier** | COMT | rs4680 | GG (Val/Val) = warrior; AA (Met/Met) = worrier | [SNPedia](https://www.snpedia.com/index.php/Rs4680) |
| **Chronotype** | CLOCK | rs1801260 | C allele = night owl tendency | [SNPedia](https://www.snpedia.com/index.php/Rs1801260) |
| **Short sleep** | DEC2 | rs121912617 | P385R = 4-6 hrs sleep without impairment (extremely rare) | [SNPedia](https://www.snpedia.com/index.php/Rs121912617) |
| **Blood type (ABO)** | ABO | rs7853989, rs8176722, rs8176746 | Predicts A/B/AB/O blood type from DNA | [SNPedia](https://www.snpedia.com/index.php/Rs8176746) |
| **Misophonia** | TENM2 | rs2937573 | Top GWAS hit (p=2.6e-43) for sound sensitivity | [SNPedia](https://www.snpedia.com/index.php/Rs2937573) |
| **Perfect pitch** | ADCY8 | rs3057 | Linkage at 8q24 (requires early musical training) | [SNPedia](https://www.snpedia.com/index.php/Rs3057) |
| **Motion sickness** | PVRL3 | rs66800491 + 34 others | 35 genome-wide-significant hits (23andMe 80K GWAS) | [dbSNP](https://www.ncbi.nlm.nih.gov/snp/rs66800491) |
| **Pain sensitivity** | SCN9A | rs6746030 | A allele = increased pain sensitivity | [SNPedia](https://www.snpedia.com/index.php/Rs6746030) |
| **Unibrow** | PAX3 | rs7544825 | Influences nasion position | [dbSNP](https://www.ncbi.nlm.nih.gov/snp/rs7544825) |

---

## 13. Ancestry & Population Genetics

### Admixture & Global Ancestry
- **ADMIXTURE**: Maximum likelihood ancestry decomposition — [Software](https://dalexander.github.io/admixture/)
- **DNAGENICS G25**: 25-dimensional PCA with 100+ calculators, 9,000+ ancient samples — [Website](https://www.dnagenics.com/)

### Haplogroups
- **Y-DNA**: yhaplo by 23andMe — [GitHub](https://github.com/23andMe/yhaplo)
- **mtDNA**: HaploGrep 3 — [Web Tool](https://haplogrep.i-med.ac.at/) | [GitHub](https://github.com/genepi/haplogrep3)

### Archaic Ancestry
- **Neanderthal/Denisovan %**: admixfrog (HMM-based, works at 0.2x coverage) — [GitHub](https://github.com/BenjaminPeter/admixfrog)

### Population Structure
- **ROH & IBD**: PLINK — [v1.9](https://www.cog-genomics.org/plink/) | [v2.0](https://www.cog-genomics.org/plink/2.0/)
- **HLA Typing**: HLA-LA (WGS-based, highly accurate) — [GitHub](https://github.com/DiltheyLab/HLA-LA)
- **HLA Imputation**: SNP2HLA (from SNP arrays) — [Broad Institute](https://software.broadinstitute.org/mpg/snp2hla/)

---

## 14. Advanced Analyses

### Nutrigenomics
| Pathway | Gene | rs Number | Effect | Evidence |
|---------|------|-----------|--------|----------|
| Folate metabolism | MTHFR | [rs1801133](https://www.ncbi.nlm.nih.gov/snp/rs1801133) | TT = needs methylfolate supplementation | Strong |
| Omega-3 conversion | FADS1/2 | [rs174546](https://www.ncbi.nlm.nih.gov/snp/rs174546) | Some genotypes need direct fish oil | Moderate |
| Vitamin D | GC/VDBP | [rs2282679](https://www.ncbi.nlm.nih.gov/snp/rs2282679) | Affects bioavailability | Moderate |
| Salt sensitivity | AGT | [rs699](https://www.ncbi.nlm.nih.gov/snp/rs699) | Some genotypes have stronger BP response | Moderate |
| Beta-carotene to Vit A | BCMO1 | [rs12934922](https://www.ncbi.nlm.nih.gov/snp/rs12934922) | Poor converters may need preformed Vit A | Moderate |
| Celiac (gluten) | HLA-DQ2/8 | HLA typing | 95% of celiacs carry DQ2.5; without it, celiac virtually excluded | Strong |
| Iron absorption | HFE/TMPRSS6 | [rs855791](https://www.ncbi.nlm.nih.gov/snp/rs855791) | Affects hepcidin regulation | Strong |
| Melatonin/glucose | MTNR1B | [rs10830963](https://www.ncbi.nlm.nih.gov/snp/rs10830963) | G allele: late eating worsens glucose; meal timing matters | Strong |

### Sports & Fitness Genetics
| Trait | Gene | rs Number | Effect |
|-------|------|-----------|--------|
| Muscle fiber type | ACTN3 | [rs1815739](https://www.ncbi.nlm.nih.gov/snp/rs1815739) | RR = sprint; XX = endurance |
| Endurance capacity | ACE | I/D polymorphism | II = endurance; DD = power |
| Mitochondrial biogenesis | PPARGC1A | [rs8192678](https://www.ncbi.nlm.nih.gov/snp/rs8192678) | Gly482Ser affects training response |
| Tendon injury risk | COL5A1 | [rs12722](https://www.ncbi.nlm.nih.gov/snp/rs12722) | TT = stiffer tendons, higher injury risk |
| Lactate transport | MCT1/SLC16A1 | [rs1049434](https://www.ncbi.nlm.nih.gov/snp/rs1049434) | AA = 60-65% greater lactate transport |
| Nitric oxide | NOS3 | [rs2070744](https://www.ncbi.nlm.nih.gov/snp/rs2070744) | T allele = endurance advantage |
| Recovery | IL6 | [rs1800795](https://www.ncbi.nlm.nih.gov/snp/rs1800795) | G allele = higher IL-6 response; longer recovery needed |

### Sleep & Circadian Genetics
| Trait | Gene | rs Number | Effect |
|-------|------|-----------|--------|
| Advanced sleep phase | PER2 | [rs2304672](https://www.ncbi.nlm.nih.gov/snp/rs2304672) | Early morning tendency |
| Delayed sleep phase | CRY1 | [rs184039278](https://www.ncbi.nlm.nih.gov/snp/rs184039278) | CRY1-delta-11: dominant late-sleeper mutation (~1% EUR) |
| Deep sleep quality | ADA | [rs73598374](https://www.ncbi.nlm.nih.gov/snp/rs73598374) | A allele = 20-30% less ADA activity = deeper slow-wave sleep |
| Caffeine + sleep | ADORA2A | [rs5751876](https://www.ncbi.nlm.nih.gov/snp/rs5751876) | TT = high caffeine sensitivity (avoid PM coffee) |

### Dermatogenomics
| Trait | Gene | rs Number | Effect |
|-------|------|-----------|--------|
| UV sensitivity | MC1R | [rs1805007](https://www.ncbi.nlm.nih.gov/snp/rs1805007) | R151C: dramatically increased burn risk |
| Freckling | IRF4 | [rs12203592](https://www.ncbi.nlm.nih.gov/snp/rs12203592) | T allele = sun-induced freckling |
| Photoaging/wrinkles | MMP1 | [rs1799750](https://www.ncbi.nlm.nih.gov/snp/rs1799750) | 2G/2G = faster collagen breakdown |
| Skin color | SLC45A2 | [rs16891982](https://www.ncbi.nlm.nih.gov/snp/rs16891982) | L374F: European lighter-skin allele |

### Appearance Prediction
- **HIrisPlex-S**: Eye, hair, skin color from 41 SNPs — [Web Tool](https://hirisplex.erasmusmc.nl/)
- Eye color: >90% accuracy (blue vs brown); Hair: ~85%; Skin: ~85%

### Immune System Profiling
- **HLA diversity**: Higher heterozygosity = broader immune response
- **C4A/C4B copy number**: Low C4A = lupus risk; high C4A = schizophrenia risk
- **COVID-19 susceptibility**: OAS1, IFNAR2, TYK2 ([rs74956615](https://www.ncbi.nlm.nih.gov/snp/rs74956615)), ABO (type O slightly protective)
- **KIR gene content**: NK cell receptor repertoire; coevolves with HLA

### Blood Type from DNA
| System | Gene | Variants | Prediction |
|--------|------|----------|------------|
| ABO | ABO | rs7853989, rs8176722, rs8176746 | A/B/AB/O |
| Rh | RHD | rs590787 + structural | Rh+/Rh- |
| Duffy | ACKR1 | [rs2814778](https://www.ncbi.nlm.nih.gov/snp/rs2814778) | CC = Duffy-null (malaria resistance) |

---

## 15. Databases, Tools & References

### Key Databases

| Database | URL | Purpose |
|----------|-----|---------|
| **PGS Catalog** | https://www.pgscatalog.org/ | 4,000+ polygenic scores with coefficients |
| **ClinVar** | https://www.ncbi.nlm.nih.gov/clinvar/ | Variant-disease relationships |
| **OMIM** | https://omim.org/ | Gene-disease catalog |
| **PharmGKB** | https://www.pharmgkb.org/ | Pharmacogenomics knowledge base |
| **CPIC** | https://cpicpgx.org/ | Clinical PGx guidelines (34 genes, 164 drugs) |
| **gnomAD** | https://gnomad.broadinstitute.org/ | Population allele frequencies (76K+ genomes) |
| **SNPedia** | https://www.snpedia.com/ | Wiki of SNP associations |
| **GWAS Catalog** | https://www.ebi.ac.uk/gwas/ | All published GWAS associations |
| **dbSNP** | https://www.ncbi.nlm.nih.gov/snp/ | SNP reference database |
| **ClinGen** | https://clinicalgenome.org/ | Gene/variant clinical validity |

### Analysis Tools

| Tool | Purpose | URL |
|------|---------|-----|
| **PLINK 2.0** | PGS, IBD, ROH, QC | https://www.cog-genomics.org/plink/2.0/ |
| **Cyrius** | CYP2D6 star alleles + CNV from WGS | https://github.com/Illumina/Cyrius |
| **PharmCAT** | PGx clinical annotation from VCF | https://pharmcat.org/ |
| **HLA-LA** | HLA typing from WGS | https://github.com/DiltheyLab/HLA-LA |
| **ADMIXTURE** | Ancestry admixture | https://dalexander.github.io/admixture/ |
| **admixfrog** | Neanderthal/Denisovan ancestry | https://github.com/BenjaminPeter/admixfrog |
| **HaploGrep 3** | mtDNA haplogroups | https://haplogrep.i-med.ac.at/ |
| **yhaplo** | Y-DNA haplogroups | https://github.com/23andMe/yhaplo |
| **HIrisPlex-S** | Eye/hair/skin color prediction | https://hirisplex.erasmusmc.nl/ |
| **PRScalc** | In-browser PRS calculator (privacy-first) | https://episphere.github.io/prs/ |
| **OpenCRAVAT** | Variant annotation platform | https://www.opencravat.org/ |
| **Genetic Genie** | Free methylation/detox profiles | https://geneticgenie.org/ |
| **DNAGENICS** | Deep ancestry + G25 + 500 reports | https://www.dnagenics.com/ |

### Key Publications

| Study | Citation | Significance |
|-------|----------|-------------|
| Khera et al. 2018 | *Nat Genet* | Landmark 5-disease genome-wide PGS |
| Khera et al. 2019 | *Cell* | BMI PGS equivalent to monogenic obesity |
| PREPARE Trial 2023 | *Lancet* | 12-gene PGx → 30% fewer ADRs |
| Patel et al. 2023 | *Nat Med* | Multi-ancestry CAD PGS |
| Abraham et al. 2014 | *PLoS Genet* | Celiac PGS with AUROC 0.90 |
| Conti et al. 2021 | *Nat Genet* | Multi-ancestry prostate cancer PGS (AUROC 0.833) |
| Mishra et al. 2022 | *Nature* | Multi-ancestry stroke PGS |
| Caudle et al. 2025 | *Clin Pharm Ther* | CPIC: 34 genes, 164 drugs, 10K+ citations |
| ACMG SF v3.3 2025 | *Genet Med* | 84 genes for secondary findings |
