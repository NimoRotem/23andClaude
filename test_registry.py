"""
Test registry — all genomic tests.
Each test has: id, category, name, description, test_type, params.

test_type determines which runner handles it:
  - variant_lookup / vcf_stats / pgs_score / clinvar_screen / specialized / rsid_pgs_score
"""

TESTS = []

def _t(id, category, name, description, test_type, params):
    TESTS.append({
        "id": id, "category": category, "name": name,
        "description": description, "test_type": test_type, "params": params,
    })

# ── Sex Check ───────────────────────────────────────────────────
_t('sex_y_reads', 'Sex Check', 'Y chromosome read count',
   'Count Y-mapped reads. Males: >1M reads; Females: ~0',
   'vcf_stats', {'method': 'y_read_count'})

_t('sex_sry', 'Sex Check', 'SRY gene presence',
   'Count reads at SRY locus (Yp11.2). Males: >10 reads; Females: 0',
   'vcf_stats', {'method': 'sry_presence'})

_t('sex_xy_ratio', 'Sex Check', 'X:Y read ratio',
   'Ratio of X to Y reads. Males: X:Y ~3-5; Females: X:Y=inf',
   'vcf_stats', {'method': 'xy_ratio'})

_t('sex_het_chrx', 'Sex Check', 'Het rate on chrX',
   'Heterozygosity rate on chrX. Males: low het; Females: ~normal het',
   'vcf_stats', {'method': 'het_chrx'})

_t('sex_var_chry', 'Sex Check', 'Variant count on chrY',
   'Count variants on chrY. Males: >1000 variants; Females: ~0',
   'vcf_stats', {'method': 'var_chry'})

# ── Sample QC ───────────────────────────────────────────────────
_t('qc_titv', 'Sample QC', 'Ti/Tv ratio',
   'Transition/transversion ratio. WGS: 2.0-2.1; Exome: 2.8-3.0',
   'vcf_stats', {'method': 'titv_ratio'})

_t('qc_hethom', 'Sample QC', 'Het/Hom ratio',
   'Heterozygous/homozygous ratio. ~1.5-2.0 for outbred',
   'vcf_stats', {'method': 'het_hom_ratio'})

_t('qc_snp_count', 'Sample QC', 'SNP count',
   'Count SNPs. WGS: 3.5-4.5M SNPs',
   'vcf_stats', {'method': 'snp_count'})

_t('qc_indel_count', 'Sample QC', 'Indel count',
   'Count indels. WGS: 500K-800K',
   'vcf_stats', {'method': 'indel_count'})

# ── Ancestry ────────────────────────────────────────────────────
_t('ancestry_pca', 'Ancestry', 'PCA projection onto 1000G',
   'Project sample onto 1000 Genomes PCA. Outputs PC1-PC10 and population cluster',
   'specialized', {'method': 'pca_1000g'})

_t('ancestry_admixture', 'Ancestry', 'ADMIXTURE (K=5)',
   'Supervised admixture: % for AFR/EUR/EAS/SAS/AMR',
   'specialized', {'method': 'admixture', 'k': 5})

_t('ancestry_y_haplo', 'Ancestry', 'Y-DNA haplogroup',
   'Determine Y-DNA haplogroup (e.g., R1b-L21, J2a, E1b)',
   'specialized', {'method': 'y_haplogroup'})

_t('ancestry_mt_haplo', 'Ancestry', 'mtDNA haplogroup',
   'Determine mtDNA haplogroup (e.g., H1a, T2b, L3e)',
   'specialized', {'method': 'mt_haplogroup'})

_t('ancestry_neanderthal', 'Ancestry', 'Neanderthal %',
   'Archaic ancestry estimate. Typically 1-4% for non-Africans',
   'specialized', {'method': 'neanderthal'})

_t('ancestry_roh', 'Ancestry', 'Runs of homozygosity',
   'Total ROH length and FROH coefficient using plink2',
   'specialized', {'method': 'roh'})

_t('ancestry_hla', 'Ancestry', 'HLA typing',
   'HLA-A, B, C, DRB1, DQB1, DPB1 typing from WGS',
   'specialized', {'method': 'hla_typing'})

# ── PGS - Cancer ────────────────────────────────────────────────
_t('pgs_breast_335', 'PGS - Cancer', 'Breast cancer (PGS000335)',
   '1,079,089 variants. OR 1.80, HR 1.71 (EUR). Mars N et al., Nat Commun 2020',
   'pgs_score', {'pgs_id': 'PGS000335', 'trait': 'Breast cancer'})

_t('pgs_breast_004', 'PGS - Cancer', 'Breast cancer (PGS000004)',
   '313 variants. AUROC 0.63, OR 1.61. Mavaddat N et al., AJHG 2018',
   'pgs_score', {'pgs_id': 'PGS000004', 'trait': 'Breast cancer'})

_t('pgs_breast_4153', 'PGS - Cancer', 'Breast cancer (PGS004153)',
   '1,127,015 variants. AUROC 0.663, OR 1.83/SD. Monti R et al., AJHG 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004153', 'trait': 'Breast cancer'})

_t('pgs_breast_5349', 'PGS - Cancer', 'Breast cancer (PGS005349)',
   '5,438,842 variants. AUROC 0.647, C-index 0.66. Tanha HM et al., EJHG 2026 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005349', 'trait': 'Breast cancer'})

_t('pgs_breast_2242', 'PGS - Cancer', 'Breast cancer (PGS002242)',
   '6,510,869 variants. OR 1.80, HR 1.71. Mars N et al., Cell Genom 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002242', 'trait': 'Breast cancer'})

_t('pgs_breast_5378', 'PGS - Cancer', 'Breast cancer (African ancestry) (PGS005378)',
   '2,300,000 variants. OR 1.34/SD (AFR). Li B et al., Nat Genet 2026 (AFR)',
   'pgs_score', {'pgs_id': 'PGS005378', 'trait': 'Breast cancer'})

_t('pgs_breast_5', 'PGS - Cancer', 'Breast cancer (ER-negative) (PGS000005)',
   '313 variants. AUROC 0.60, OR 1.45. Mavaddat N et al., AJHG 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000005', 'trait': 'Breast cancer'})

_t('pgs_breast_6', 'PGS - Cancer', 'Breast cancer (ER-positive) (PGS000006)',
   '313 variants. AUROC 0.65, OR 1.68. Mavaddat N et al., AJHG 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000006', 'trait': 'Breast cancer'})

_t('pgs_breast_5382', 'PGS - Cancer', 'Breast cancer (ER-neg African) (PGS005382)',
   '~2,300,000 variants. African-specific ER-neg. Li B et al., Nat Genet 2026 (AFR)',
   'pgs_score', {'pgs_id': 'PGS005382', 'trait': 'Breast cancer'})

_t('pgs_breast_5387', 'PGS - Cancer', 'Breast cancer (Triple-neg African) (PGS005387)',
   '162 variants. African-specific TNBC. Li B et al., Nat Genet 2026 (AFR)',
   'pgs_score', {'pgs_id': 'PGS005387', 'trait': 'Breast cancer'})

_t('pgs_squamous_72', 'PGS - Cancer', 'Breast cancer (PGS000072)',
   '15 variants. AUROC 0.77. Graff RE et al., Nat Commun 2021 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000072', 'trait': 'Breast cancer'})

_t('custom_pgs000001', 'PGS - Cancer', 'Breast cancer (PGS000001)',
   '77 variants. Mavaddat N et al., 2015. J Natl Cancer Inst',
   'pgs_score', {'pgs_id': 'PGS000001', 'trait': 'Breast cancer'})

_t('pgs_prostate_662', 'PGS - Cancer', 'Prostate cancer (PGS000662)',
   '269 variants. AUROC 0.833, OR 4.17. Conti DV et al., Nat Genet 2021',
   'pgs_score', {'pgs_id': 'PGS000662', 'trait': 'Prostate cancer'})

_t('pgs_prostate_3766', 'PGS - Cancer', 'Prostate cancer (PGS003766)',
   '451 variants. OR/SD 2.21-2.32, multi-ancestry. Wang A et al., Nat Genet 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003766', 'trait': 'Prostate cancer'})

_t('pgs_prostate_5241', 'PGS - Cancer', 'Prostate cancer (PGS005241)',
   '3,800,000 variants. AUROC 0.805 (SAS), multi-ancestry. Tanha HM et al., HGG Adv 2025 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005241', 'trait': 'Prostate cancer'})

_t('pgs_prostate_333', 'PGS - Cancer', 'Prostate cancer (PGS000333)',
   '6,606,785 variants. C-index 0.866. Mars N et al., Nat Med 2020 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000333', 'trait': 'Prostate cancer'})

_t('pgs_prostate_3765', 'PGS - Cancer', 'Prostate cancer (EUR-specific) (PGS003765)',
   '451 variants. OR/SD 2.21 (EUR). Wang A et al., Nat Genet 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003765', 'trait': 'Prostate cancer'})

_t('pgs_prostate_67', 'PGS - Cancer', 'Prostate cancer (hazard) (PGS000067)',
   '54 variants. HR 2.9 (top 2%). Seibert TM et al., BMJ 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000067', 'trait': 'Prostate cancer'})

_t('pgs_ovarian_49', 'PGS - Cancer', 'Prostate cancer (serous) (PGS000049)',
   '30 variants. OR 1.55. Phelan CM et al., Nat Genet 2017 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000049', 'trait': 'Prostate cancer'})

_t('pgs_gallstones_1291', 'PGS - Cancer', 'Prostate cancer (PGS001291)',
   '5,387 variants. AUROC 0.63. Tanigawa Y et al., PLoS Genet 2022',
   'pgs_score', {'pgs_id': 'PGS001291', 'trait': 'Prostate cancer'})

_t('pgs_colorectal_3850', 'PGS - Cancer', 'Colorectal cancer (PGS003850)',
   '205 variants. OR 1.62, AUROC 0.61. Fernandez-Rozadilla C et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS003850', 'trait': 'Colorectal cancer'})

_t('pgs_colorectal_3852', 'PGS - Cancer', 'Colorectal cancer (PGS003852)',
   '1,000,000 variants. OR 1.67, multi-ancestry. Thomas M et al., Nat Commun 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003852', 'trait': 'Colorectal cancer'})

_t('pgs_colorectal_3979', 'PGS - Cancer', 'Colorectal cancer (PGS003979)',
   '~1,000,000 variants. AUROC 0.795 (Finnish). Tamlander M et al., Br J Cancer 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003979', 'trait': 'Colorectal cancer'})

_t('pgs_colorectal_4904', 'PGS - Cancer', 'Colorectal cancer (early-onset) (PGS004904)',
   '~200 variants. OR 2.51 (top vs bottom decile). Jiang L et al., Int J Cancer 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004904', 'trait': 'Colorectal cancer'})

_t('pgs_colorectal_4580', 'PGS - Cancer', 'Colorectal cancer (genome-wide) (PGS004580)',
   '1,099,906 variants. OR 1.50/SD. Youssef O et al., Lab Invest 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004580', 'trait': 'Colorectal cancer'})

_t('pgs_colorectal_4586', 'PGS - Cancer', 'Colorectal cancer (prognostic/survival) (PGS004586)',
   '~200 variants. HR 1.34 (survival). Xin J et al., Nat Commun 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004586', 'trait': 'Colorectal cancer'})

_t('pgs_colorectal_55', 'PGS - Cancer', 'Colorectal cancer (early-onset) (PGS000055)',
   '95 variants. OR 2.10 (top vs bottom quintile). Huyghe JR et al., Nat Genet 2019 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000055', 'trait': 'Colorectal cancer'})

_t('pgs_multiple_74', 'PGS - Cancer', 'Colorectal cancer (PGS000074)',
   '15 variants. AUROC 0.72. Graff RE et al., Nat Commun 2021 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000074', 'trait': 'Colorectal cancer'})

_t('pgs_anorexia_379', 'PGS - Cancer', 'Colon cancer (PGS000379)',
   '66,177 variants. OR 1.24/SD. Watson HJ et al., Nat Genet 2019 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000379', 'trait': 'Colon cancer'})

_t('pgs_prostate_663', 'PGS - Cancer', 'Pancreatic cancer (aggressive) (PGS000663)',
   '269 variants. OR 5.54 (top vs bottom decile). Conti DV et al., Nat Genet 2021 (Multi)',
   'pgs_score', {'pgs_id': 'PGS000663', 'trait': 'Pancreatic cancer'})

_t('pgs_pancreatic_2264', 'PGS - Cancer', 'Pancreatic cancer (PGS002264)',
   '49 variants. AUROC 0.605. Sharma S et al., Gastroenterology 2022',
   'pgs_score', {'pgs_id': 'PGS002264', 'trait': 'Pancreatic cancer'})

_t('pgs_pancreatic_2740', 'PGS - Cancer', 'Pancreatic cancer (PGS002740)',
   '~50 variants. OR 6.91 (top vs bottom, age<=60). Yuan C et al., Ann Oncol 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002740', 'trait': 'Pancreatic cancer'})

_t('pgs_pancreatic_794', 'PGS - Cancer', 'Pancreatic cancer (PGS000794)',
   '22 variants. AUROC 0.745 (w/covariates). Kachuri L et al., Nat Commun 2020 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000794', 'trait': 'Pancreatic cancer'})

_t('pgs_lung_078', 'PGS - Cancer', 'Lung cancer (PGS000078)',
   '109 variants. AUROC 0.846, HR 1.26. Graff RE et al., Nat Commun 2021',
   'pgs_score', {'pgs_id': 'PGS000078', 'trait': 'Lung cancer'})

_t('pgs_lung_4860', 'PGS - Cancer', 'Lung cancer (PGS004860)',
   '1,100,000 variants. Genome-wide, multi-ancestry. Boumtje L et al., EBioMedicine 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004860', 'trait': 'Lung cancer'})

_t('pgs_lung_3393', 'PGS - Cancer', 'Lung cancer (adenocarcinoma) (PGS003393)',
   '~144,000 variants. AUROC 0.743. Namba S et al., Cancer Res 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003393', 'trait': 'Lung cancer'})

_t('pgs_lung_3392', 'PGS - Cancer', 'Lung cancer (squamous) (PGS003392)',
   '~144,000 variants. AUROC 0.778. Namba S et al., Cancer Res 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003392', 'trait': 'Lung cancer'})

_t('pgs_lung_5169', 'PGS - Cancer', 'Lung cancer (never-smoker EAS) (PGS005169)',
   '~1,000,000 variants. EAS never-smoker specific. Blechter B et al., JAMA Netw Open 2023 (EAS)',
   'pgs_score', {'pgs_id': 'PGS005169', 'trait': 'Lung cancer'})

_t('pgs_esophageal_70', 'PGS - Cancer', 'Lung cancer (PGS000070)',
   '15 variants. AUROC 0.71. Graff RE et al., Nat Commun 2021 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000070', 'trait': 'Lung cancer'})

_t('pgs_lung_82', 'PGS - Cancer', 'Ovarian cancer (squamous) (PGS000082)',
   '109 variants. AUROC 0.74. Graff RE et al., Nat Commun 2021 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000082', 'trait': 'Ovarian cancer'})

_t('pgs_ovarian_5086', 'PGS - Cancer', 'Ovarian cancer (PGS005086)',
   '64,518 variants. AUROC 0.607, OR 1.46. Barnes DR et al., NPJ Genom Med 2025',
   'pgs_score', {'pgs_id': 'PGS005086', 'trait': 'Ovarian cancer'})

_t('pgs_ovarian_3385', 'PGS - Cancer', 'Ovarian cancer (PGS003385)',
   '144,000 variants. AUROC 0.717. Namba S et al., Cancer Res 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003385', 'trait': 'Ovarian cancer'})

_t('pgs_ovarian_5166', 'PGS - Cancer', 'Ovarian cancer (EAS) (PGS005166)',
   '~64,000 variants. EAS-specific. Zhu M et al., PLoS Med 2025 (EAS)',
   'pgs_score', {'pgs_id': 'PGS005166', 'trait': 'Ovarian cancer'})

_t('pgs_endometrial_2735', 'PGS - Cancer', 'Endometrial cancer (PGS002735)',
   '19 variants. AUROC 0.56, OR 1.55. Bafligil C et al., Genet Med 2022',
   'pgs_score', {'pgs_id': 'PGS002735', 'trait': 'Endometrial cancer'})

_t('pgs_endometrial_3381', 'PGS - Cancer', 'Endometrial cancer (PGS003381)',
   '529,000 variants. AUROC 0.761. Namba S et al., Cancer Res 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003381', 'trait': 'Endometrial cancer'})

_t('pgs_cervical_073', 'PGS - Cancer', 'Cervical cancer (PGS000073)',
   '10 variants. AUROC 0.69. Graff RE et al., Nat Commun 2021',
   'pgs_score', {'pgs_id': 'PGS000073', 'trait': 'Cervical cancer'})

_t('pgs_cervical_1299', 'PGS - Cancer', 'Cervical cancer (PGS001299)',
   '24 variants. AUROC 0.77-0.92. Tanigawa Y et al., PLoS Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS001299', 'trait': 'Cervical cancer'})

_t('pgs_esophageal_3387', 'PGS - Cancer', 'Esophageal cancer (PGS003387)',
   '601,000 variants. AUROC 0.819 (adenocarcinoma). Namba S et al., Cancer Res 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003387', 'trait': 'Esophageal cancer'})

_t('pgs_gastric_5161', 'PGS - Cancer', 'Gastric cancer (PGS005161)',
   '12 variants. HR 1.27/SD. Zhu M et al., PLoS Med 2025',
   'pgs_score', {'pgs_id': 'PGS005161', 'trait': 'Gastric cancer'})

_t('pgs_lung_81', 'PGS - Cancer', 'Oral cavity and pharyngeal cancers (adenocarcinoma) (PGS000081)',
   '109 variants. AUROC 0.80. Graff RE et al., Nat Commun 2021 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000081', 'trait': 'Oral cavity and pharyngeal cancers'})

_t('pgs_educational_3390', 'PGS - Cancer', 'Head and neck squamous cell carcinoma (EA4, latest) (PGS003390)',
   'varies variants. r=0.19 (EUR). Okbay A et al., Nat Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003390', 'trait': 'Head and neck squamous cell carcinoma'})

_t('pgs_melanoma_743', 'PGS - Cancer', 'Melanoma (PGS000743)',
   '45 variants. AUROC 0.74, OR 5.88 (decile). Cust AE et al., J Invest Dermatol 2018',
   'pgs_score', {'pgs_id': 'PGS000743', 'trait': 'Melanoma'})

_t('pgs_melanoma_2247', 'PGS - Cancer', 'Melanoma (PGS002247)',
   '68 variants. AUROC 0.685-0.691, HR 1.80. Steinberg J et al., Br J Dermatol 2021 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002247', 'trait': 'Melanoma'})

_t('pgs_melanoma_4886', 'PGS - Cancer', 'Melanoma (PGS004886)',
   '692,000 variants. Genome-wide, multi-trait. Jermy B et al., Nat Commun 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004886', 'trait': 'Melanoma'})

_t('pgs_type_339', 'PGS - Cancer', 'Cutaneous melanoma (GRS2) (PGS000339)',
   '67 variants. AUROC 0.92 (T1D vs T2D). Sharp SA et al., Diabetes Care 2019 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000339', 'trait': 'Cutaneous melanoma'})

_t('pgs_bcc_119', 'PGS - Cancer', 'Basal cell carcinoma (PGS000119)',
   '32 variants. OR 1.65, AUROC 0.64. Fritsche LG et al., PLoS Genet 2019',
   'pgs_score', {'pgs_id': 'PGS000119', 'trait': 'Basal cell carcinoma'})

_t('pgs_basal_4592', 'PGS - Cancer', 'Basal cell carcinoma (PGS004592)',
   '78 variants. AUROC 0.74. Liyanage UE et al., J Eur Acad Dermatol 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004592', 'trait': 'Basal cell carcinoma'})

_t('pgs_bladder_071', 'PGS - Cancer', 'Bladder cancer (PGS000071)',
   '15 variants. AUROC 0.803. Graff RE et al., Nat Commun 2021',
   'pgs_score', {'pgs_id': 'PGS000071', 'trait': 'Bladder cancer'})

_t('pgs_bladder_782', 'PGS - Cancer', 'Bladder cancer (PGS000782)',
   '15 variants. AUROC 0.804. Kachuri L et al., Nat Commun 2020 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000782', 'trait': 'Bladder cancer'})

_t('pgs_kidney_787', 'PGS - Cancer', 'Kidney cancer (PGS000787)',
   '19 variants. AUROC 0.722. Kachuri L et al., Nat Commun 2020',
   'pgs_score', {'pgs_id': 'PGS000787', 'trait': 'Kidney cancer (RCC)'})

_t('pgs_kidney_4908', 'PGS - Cancer', 'Kidney cancer (RCC) (PGS004908)',
   '107 variants. AUROC 0.74. Purdue MP et al., Nat Genet 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004908', 'trait': 'Kidney cancer (RCC)'})

_t('pgs_testicular_086', 'PGS - Cancer', 'Testicular cancer (PGS000086)',
   '52 variants. AUROC 0.783. Graff RE et al., Nat Commun 2021',
   'pgs_score', {'pgs_id': 'PGS000086', 'trait': 'Testicular cancer'})

_t('pgs_testicular_796', 'PGS - Cancer', 'Testicular cancer (PGS000796)',
   '52 variants. AUROC 0.69. Kachuri L et al., Nat Commun 2020 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000796', 'trait': 'Testicular cancer'})

_t('pgs_thyroid_636', 'PGS - Cancer', 'Thyroid cancer (PGS000636)',
   '954 variants. AUROC 0.578. Fritsche LG et al., AJHG 2020',
   'pgs_score', {'pgs_id': 'PGS000636', 'trait': 'Thyroid cancer'})

_t('pgs_thyroid_4954', 'PGS - Cancer', 'Thyroid cancer (PGS004954)',
   '26 variants. AUROC 0.70, multi-ancestry. Pozdeyev N et al., J Clin Endocrinol Metab 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004954', 'trait': 'Thyroid cancer'})

_t('pgs_ankylosing_1289', 'PGS - Cancer', 'Thyroid cancer (PGS001289)',
   '2,874 variants. AUROC 0.85. Tanigawa Y et al., PLoS Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS001289', 'trait': 'Thyroid cancer'})

_t('pgs_glioma_3384', 'PGS - Cancer', 'Glioma / brain cancer (PGS003384)',
   '910 variants. AUROC 0.758. Namba S et al., Cancer Res 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003384', 'trait': 'Glioma / brain cancer'})

_t('pgs_nhl_080', 'PGS - Cancer', 'Non-Hodgkin lymphoma (PGS000080)',
   '19 variants. AUROC 0.73. Graff RE et al., Nat Commun 2021',
   'pgs_score', {'pgs_id': 'PGS000080', 'trait': 'Non-Hodgkin lymphoma'})

_t('pgs_nonhodgkin_4248', 'PGS - Cancer', 'Non-Hodgkin lymphoma (PGS004248)',
   '20 variants. Multi-subtype. Kim WJ et al., NPJ Precis Oncol 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004248', 'trait': 'Non-Hodgkin lymphoma'})

_t('pgs_bmi_2303', 'PGS - Cancer', 'Diffuse large B-cell lymphoma (multi-ancestry) (PGS002303)',
   '1,259,754 variants. R2=0.09. Ge T et al., Genome Med 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS002303', 'trait': 'Diffuse large B-cell lymphoma'})

_t('pgs_cll_077', 'PGS - Cancer', 'CLL (PGS000077)',
   '75 variants. AUROC 0.83. Graff RE et al., Nat Commun 2021',
   'pgs_score', {'pgs_id': 'PGS000077', 'trait': 'CLL (lymphocytic leukemia)'})

_t('pgs_cll_874', 'PGS - Cancer', 'CLL (PGS000874)',
   '41 variants. AUROC 0.79. Kleinstern G et al., Blood 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000874', 'trait': 'CLL'})

_t('pgs_cll_3453', 'PGS - Cancer', 'CLL (PGS003453)',
   '43 variants. Updated CLL-specific. Berndt SI et al., Leukemia 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003453', 'trait': 'CLL'})

_t('pgs_height_2305', 'PGS - Cancer', 'Chronic lymphoid leukemia (multi-ancestry) (PGS002305)',
   '1,259,754 variants. R2=0.61. Ge T et al., Genome Med 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS002305', 'trait': 'Chronic lymphoid leukemia'})

_t('pgs_multiple_2281', 'PGS - Cancer', 'Multiple myeloma (PGS002281)',
   '23 variants. AUROC 0.644, OR 3.18 (quintile). Canzian F et al., EJHG 2021 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002281', 'trait': 'Multiple myeloma'})


# ── PGS - Cardiovascular ────────────────────────────────────────
_t('pgs_cad_3725', 'PGS - Cardiovascular', 'Coronary artery disease (PGS003725)',
   '1,296,172 variants. HR 1.75, OR 2.14 (multi-ancestry). Patel AP et al., Nat Med 2023',
   'pgs_score', {'pgs_id': 'PGS003725', 'trait': 'CAD'})

_t('pgs_coronary_5091', 'PGS - Cardiovascular', 'Coronary artery disease (CAD) (JAMA 2024) (PGS005091)',
   '1,428,772 variants. OR 1.45, AUROC 0.776-0.800. Abramowitz SA et al., JAMA 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005091', 'trait': 'Coronary artery disease (CAD)'})

_t('pgs_coronary_5112', 'PGS - Cardiovascular', 'Coronary artery disease (CAD) (EAS+EUR) (PGS005112)',
   '1,106,628 variants. OR 1.46 (EUR). Loesch DP et al., Nat Commun 2025 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005112', 'trait': 'Coronary artery disease (CAD)'})

_t('pgs_coronary_4696', 'PGS - Cardiovascular', 'Coronary artery disease (CAD) (SAS-strong) (PGS004696)',
   '1,289,980 variants. OR 1.65 (EUR), OR 2.67 (SAS). Smith JL et al., Circ Genom Precis Med 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004696', 'trait': 'Coronary artery disease (CAD)'})

_t('pgs_coronary_18', 'PGS - Cardiovascular', 'Coronary artery disease (CAD) (metaGRS) (PGS000018)',
   '1,745,179 variants. AUROC 0.79, HR 1.71/SD. Inouye M et al., JACC 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000018', 'trait': 'Coronary artery disease (CAD)'})

_t('pgs_coronary_13', 'PGS - Cardiovascular', 'Coronary artery disease (CAD) (GPS) (PGS000013)',
   '6,630,150 variants. AUROC 0.81. Khera AV et al., Nat Genet 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000013', 'trait': 'Coronary artery disease (CAD)'})

_t('pgs_afib_016', 'PGS - Cardiovascular', 'Atrial fibrillation (PGS000016)',
   '6,730,541 variants. AUROC 0.78. Khera AV et al., Nat Genet 2018',
   'pgs_score', {'pgs_id': 'PGS000016', 'trait': 'Atrial fibrillation'})

_t('pgs_atrial_5168', 'PGS - Cardiovascular', 'Atrial fibrillation (Nat Genet 2025) (PGS005168)',
   '382,963 variants. HR 1.67, C-index 0.87. Roselli C et al., Nat Genet 2025 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005168', 'trait': 'Atrial fibrillation'})

_t('pgs_atrial_5313', 'PGS - Cardiovascular', 'Atrial fibrillation (PRS-CSx) (PGS005313)',
   '1,271,239 variants. OR 1.82, AUROC 0.78. Yuan S et al., Nat Commun 2025 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005313', 'trait': 'Atrial fibrillation'})

_t('pgs_atrial_4878', 'PGS - Cardiovascular', 'Atrial fibrillation (INTERVENE) (PGS004878)',
   '785,779 variants. HR 1.56-1.68 (7 biobanks). Jermy B et al., Nat Commun 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004878', 'trait': 'Atrial fibrillation'})

_t('pgs_atrial_35', 'PGS - Cardiovascular', 'Atrial fibrillation (focused) (PGS000035)',
   '97 variants. AUROC 0.74. Khera AV et al., Circ 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000035', 'trait': 'Atrial fibrillation'})

_t('pgs_hf_5097', 'PGS - Cardiovascular', 'Heart failure (PGS005097)',
   '1,274,692 variants. AUROC 0.72 (multi-ancestry 2.3M). Lee DSM et al., Nat Genet 2025',
   'pgs_score', {'pgs_id': 'PGS005097', 'trait': 'Heart failure'})

_t('pgs_heart_5285', 'PGS - Cardiovascular', 'Heart failure (EAS) (PGS005285)',
   '993,899 variants. R2=0.074 (EAS). Enzan N et al., Nat Commun 2025 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005285', 'trait': 'Heart failure'})

_t('pgs_heart_5073', 'PGS - Cardiovascular', 'Heart failure (All of Us) (PGS005073)',
   '1,286,612 variants. C-index 0.72-0.79. Gunn S et al., HGG Adv 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005073', 'trait': 'Heart failure'})

_t('pgs_heart_1790', 'PGS - Cardiovascular', 'Heart failure (GBMI) (PGS001790)',
   '910,146 variants. AUROC 0.75. Wang Y et al., Cell Genomics 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS001790', 'trait': 'Heart failure'})

_t('pgs_stroke_2724', 'PGS - Cardiovascular', 'Ischemic stroke (PGS002724)',
   '1,213,574 variants. HR 1.19, C-index 0.645. Mishra A et al., Nature 2022',
   'pgs_score', {'pgs_id': 'PGS002724', 'trait': 'Ischemic stroke'})

_t('pgs_stroke_2725', 'PGS - Cardiovascular', 'Stroke (integrative iPGS) (PGS002725)',
   '6,010,730 variants. OR 1.18-1.33. Mishra A et al., Nature 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS002725', 'trait': 'Stroke'})

_t('pgs_stroke_1793', 'PGS - Cardiovascular', 'Stroke (GBMI) (PGS001793)',
   '910,099 variants. AUROC 0.71 (EUR), 0.75 (Asian). Wang Y et al., Cell Genomics 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS001793', 'trait': 'Stroke'})

_t('pgs_stroke_39', 'PGS - Cardiovascular', 'Stroke (all subtypes) (PGS000039)',
   '3,200,000 variants. AUROC 0.64. Abraham G et al., Circ GMP 2019 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000039', 'trait': 'Stroke'})

_t('pgs_vte_043', 'PGS - Cardiovascular', 'VTE (PGS000043)',
   '297 variants. OR 2.89 (top 5%). Klarin D et al., Nat Genet 2019',
   'pgs_score', {'pgs_id': 'PGS000043', 'trait': 'Venous thromboembolism'})

_t('pgs_vte_3332', 'PGS - Cardiovascular', 'VTE (venous thromboembolism) (genome-wide) (PGS003332)',
   '1,092,045 variants. OR 1.51/SD, AUROC 0.68. Ghouse J et al., Nat Genet 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003332', 'trait': 'VTE (venous thromboembolism)'})

_t('pgs_vte_4854', 'PGS - Cardiovascular', 'VTE (venous thromboembolism) (PRSmixPlus) (PGS004854)',
   '2,268,993 variants. Incr. R2=0.058. Truong B et al., Cell Genomics 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004854', 'trait': 'VTE (venous thromboembolism)'})

_t('pgs_vte_1796', 'PGS - Cardiovascular', 'VTE (venous thromboembolism) (multi-ancestry) (PGS001796)',
   '910,337 variants. AUROC 0.675 (EUR), 0.672 (AFR). Wang Y et al., Cell Genomics 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS001796', 'trait': 'VTE (venous thromboembolism)'})

_t('pgs_pad_5217', 'PGS - Cardiovascular', 'PAD (PGS005217)',
   '1,296,292 variants. OR 1.66, C-index 0.731. Flores AM et al., JAMA Cardiol 2025',
   'pgs_score', {'pgs_id': 'PGS005217', 'trait': 'Peripheral artery disease'})

_t('pgs_dilated_4946', 'PGS - Cardiovascular', 'Dilated cardiomyopathy (PGS004946)',
   '1,098,677 variants. OR 1.66/SD, AUC 0.65. Jurgens SJ et al., Nat Genet 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004946', 'trait': 'Dilated cardiomyopathy'})

_t('pgs_aaa_3972', 'PGS - Cardiovascular', 'Aortic aneurysm (PGS003972)',
   '1,118,997 variants. AUROC 0.64-0.69. Roychowdhury T et al., Nat Genet 2023',
   'pgs_score', {'pgs_id': 'PGS003972', 'trait': 'Aortic aneurysm'})

_t('pgs_aortic_3429', 'PGS - Cardiovascular', 'Aortic aneurysm (AAA) (shaPRS) (PGS003429)',
   '831,447 variants. AUROC 0.708. Kelemen M et al., Nat Commun 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003429', 'trait': 'Aortic aneurysm (AAA)'})

_t('pgs_aortic_5252', 'PGS - Cardiovascular', 'Aortic stenosis (PGS005252)',
   '1,119,377 variants. HR 1.92, C-index 0.87. Small AM et al., Nat Genet 2025 (EUR)',
   'pgs_score', {'pgs_id': 'PGS005252', 'trait': 'Aortic stenosis'})

_t('pgs_htn_4192', 'PGS - Cardiovascular', 'Hypertension (PGS004192)',
   '9,430 variants. AUROC 0.703. Raben TG et al., Sci Rep 2023',
   'pgs_score', {'pgs_id': 'PGS004192', 'trait': 'Hypertension'})

_t('pgs_hypertension_4785', 'PGS - Cardiovascular', 'Hypertension (PRSmix) (PGS004785)',
   '1,170,615 variants. Incr. R2=0.066. Truong B et al., Cell Genomics 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004785', 'trait': 'Hypertension (PRSmix)'})

_t('pgs_resting_2603', 'PGS - Cardiovascular', 'Hypertension (PGS002603)',
   '1,060,971 variants. R2=0.041. Weissbrod O et al., Nat Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002603', 'trait': 'Hypertension'})

_t('pgs_sbp_2349', 'PGS - Cardiovascular', 'Systolic BP (PGS002349)',
   '1,109,311 variants. R2=0.108. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002349', 'trait': 'Systolic blood pressure'})

_t('pgs_systolic_4603', 'PGS - Cardiovascular', 'Systolic BP (PGS004603)',
   '7,356,519 variants. R2=0.114; 16.85 mmHg diff. Keaton JM et al., Nat Genet 2024 (EUR+AFR)',
   'pgs_score', {'pgs_id': 'PGS004603', 'trait': 'Systolic BP'})

_t('pgs_dbp_2322', 'PGS - Cardiovascular', 'Diastolic BP (PGS002322)',
   '1,109,311 variants. R2=0.080. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002322', 'trait': 'Diastolic blood pressure'})

_t('pgs_coronary_2297', 'PGS - Cardiovascular', 'Lipoprotein A levels (LDpred2) (PGS002297)',
   '1,259,754 variants. AUROC 0.78. Ge T et al., Genome Med 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS002297', 'trait': 'Lipoprotein A levels'})

_t('pgs_schizophrenia_3472', 'PGS - Cardiovascular', 'Heart rate (PGC3) (PGS003472)',
   'varies variants. AUROC 0.76. Trubetskoy V et al., Nature 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003472', 'trait': 'Heart rate'})

_t('pgs_vitamin_1907', 'PGS - Cardiovascular', 'RR interval (PGS001907)',
   '8,505 variants. R2=0.035. Prive F et al., AJHG 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS001907', 'trait': 'RR interval'})


# ── PGS - Metabolic / Endocrine ─────────────────────────────────
_t('pgs_t2d_2308', 'PGS - Metabolic / Endocrine', 'Type 2 diabetes (PGS002308)',
   '1,259,754 variants. AUROC 0.793 (EUR), 0.81 (EAS). Ge T et al., Genome Med 2022',
   'pgs_score', {'pgs_id': 'PGS002308', 'trait': 'Type 2 diabetes'})

_t('pgs_type_4923', 'PGS - Metabolic / Endocrine', 'Type 2 diabetes (metaGRS) (PGS004923)',
   '1,349,896 variants. AUROC 0.777 (EUR), 0.725 (AFR). Ritchie SC et al., medRxiv 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004923', 'trait': 'Type 2 diabetes'})

_t('pgs_type_3867', 'PGS - Metabolic / Endocrine', 'Type 2 diabetes (multi-ancestry) (PGS003867)',
   '1,068,166 variants. AUROC 0.73 (EUR), 0.776 (HIS). Shim I et al., Nat Commun 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003867', 'trait': 'Type 2 diabetes'})

_t('pgs_type_14', 'PGS - Metabolic / Endocrine', 'Type 2 diabetes (classic GPS) (PGS000014)',
   '6,917,436 variants. AUROC 0.73. Khera AV et al., Nat Genet 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000014', 'trait': 'Type 2 diabetes'})

_t('pgs_t1d_4174', 'PGS - Metabolic / Endocrine', 'Type 1 diabetes (PGS004174)',
   '49 variants. AUROC 0.71. Raben TG et al., Sci Rep 2023',
   'pgs_score', {'pgs_id': 'PGS004174', 'trait': 'Type 1 diabetes'})

_t('pgs_type_4102', 'PGS - Metabolic / Endocrine', 'Type 1 diabetes (PRS-CS) (PGS004102)',
   '61,651 variants. AUROC 0.741, R2=0.095. Monti R et al., AJHG 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004102', 'trait': 'Type 1 diabetes'})

_t('pgs_type_4874', 'PGS - Metabolic / Endocrine', 'Type 1 diabetes (INTERVENE) (PGS004874)',
   '56,916 variants. HR 2.37, C-index 0.77. Jermy B et al., Nat Commun 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004874', 'trait': 'Type 1 diabetes'})

_t('pgs_osteoarthritis_1296', 'PGS - Metabolic / Endocrine', 'Insulin-dependent diabetes mellitus (time-to-event) (PGS001296)',
   '6,234 variants. AUROC 0.58. Tanigawa Y et al., PLoS Genet 2022',
   'pgs_score', {'pgs_id': 'PGS001296', 'trait': 'Insulin-dependent diabetes mellitus (time-to-event)'})

_t('pgs_bmi_027', 'PGS - Metabolic / Endocrine', 'BMI / Obesity (PGS000027)',
   '2,100,302 variants. R2=0.085; top decile 13kg heavier. Khera AV et al., Cell 2019',
   'pgs_score', {'pgs_id': 'PGS000027', 'trait': 'BMI'})

_t('pgs_bmi_5198', 'PGS - Metabolic / Endocrine', 'BMI / Obesity (GIANT 2025) (PGS005198)',
   '1,217,710 variants. R2=0.176 (EUR), OR 4.08 top 3%. Smit RAJ et al., Nat Med 2025 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005198', 'trait': 'BMI / Obesity'})

_t('pgs_bmi_5202', 'PGS - Metabolic / Endocrine', 'BMI / Obesity (EAS-optimized) (PGS005202)',
   '1,022,487 variants. R2=0.101 (EAS). Smit RAJ et al., Nat Med 2025 (EAS)',
   'pgs_score', {'pgs_id': 'PGS005202', 'trait': 'BMI / Obesity'})

_t('pgs_ldl_2337', 'PGS - Metabolic / Endocrine', 'LDL Cholesterol (PGS002337)',
   '1,109,311 variants. R2=0.172. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002337', 'trait': 'LDL Cholesterol'})

_t('pgs_ldl_115', 'PGS - Metabolic / Endocrine', 'LDL (focused) (PGS000115)',
   '223 variants. R2=0.09. Trinder M et al., JAMA Cardiol 2020 (Multi)',
   'pgs_score', {'pgs_id': 'PGS000115', 'trait': 'LDL (focused)'})

_t('pgs_ldl_3788', 'PGS - Metabolic / Endocrine', 'LDL (AFR-optimized) (PGS003788)',
   '1,679,610 variants. R2=0.044 (AFR). Zhang H et al., Nat Genet 2023 (AFR)',
   'pgs_score', {'pgs_id': 'PGS003788', 'trait': 'LDL (AFR-optimized)'})

_t('pgs_ldl_4644', 'PGS - Metabolic / Endocrine', 'LDL (EAS-tuned) (PGS004644)',
   '1,354,681 variants. R2=0.067 (EAS). Zhang J et al., Nat Commun 2024 (EAS)',
   'pgs_score', {'pgs_id': 'PGS004644', 'trait': 'LDL (EAS-tuned)'})

_t('pgs_egfr_2605', 'PGS - Metabolic / Endocrine', 'LDL cholesterol (PGS002605)',
   '1,103,034 variants. R2=0.048. Weissbrod O et al., Nat Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002605', 'trait': 'LDL cholesterol'})

_t('pgs_hdl_4775', 'PGS - Metabolic / Endocrine', 'HDL Cholesterol (PGS004775)',
   '1,120,830 variants. R2=0.155. Truong B et al., Cell Genomics 2024',
   'pgs_score', {'pgs_id': 'PGS004775', 'trait': 'HDL Cholesterol'})

_t('pgs_hdl_4631', 'PGS - Metabolic / Endocrine', 'HDL (EAS) (PGS004631)',
   '1,871,796 variants. R2=0.167 (EAS). Zhang J et al., Nat Commun 2024 (EAS)',
   'pgs_score', {'pgs_id': 'PGS004631', 'trait': 'HDL (EAS)'})

_t('pgs_tc_2352', 'PGS - Metabolic / Endocrine', 'Total Cholesterol (PGS002352)',
   '1,109,311 variants. R2=0.155. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002352', 'trait': 'Total Cholesterol'})

_t('pgs_total_4669', 'PGS - Metabolic / Endocrine', 'Total Cholesterol (AFR) (PGS004669)',
   '1,728,954 variants. R2=0.132 (AFR). Zhang J et al., Nat Commun 2024 (AFR)',
   'pgs_score', {'pgs_id': 'PGS004669', 'trait': 'Total Cholesterol (AFR)'})

_t('pgs_tg_2353', 'PGS - Metabolic / Endocrine', 'Triglycerides (PGS002353)',
   '1,109,311 variants. R2=0.115. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002353', 'trait': 'Triglycerides'})

_t('pgs_triglycerides_4845', 'PGS - Metabolic / Endocrine', 'Triglycerides (PRSmix) (PGS004845)',
   '1,095,976 variants. R2=0.113 (EUR). Truong B et al., Cell Genomics 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004845', 'trait': 'Triglycerides (PRSmix)'})

_t('pgs_uric_700', 'PGS - Metabolic / Endocrine', 'Uric acid (PGS000700)',
   '20,171 variants. R2=0.421 (EUR), 0.338 (EAS). Sinnott-Armstrong N et al., Nat Genet 2021 (Multi)',
   'pgs_score', {'pgs_id': 'PGS000700', 'trait': 'Uric acid'})

_t('pgs_gout_4768', 'PGS - Metabolic / Endocrine', 'Gout (PGS004768)',
   '1,580,311 variants. R2=0.081. Truong B et al., Cell Genomics 2024',
   'pgs_score', {'pgs_id': 'PGS004768', 'trait': 'Gout'})

_t('pgs_gout_4931', 'PGS - Metabolic / Endocrine', 'Gout (SnpNet) (PGS004931)',
   '1,138 variants. AUROC 0.73. Moreno-Grau S et al., Hum Genomics 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004931', 'trait': 'Gout (SnpNet)'})

_t('pgs_celiac_040', 'PGS - Metabolic / Endocrine', 'Celiac disease (PGS000040)',
   '228 variants. AUROC 0.90. Abraham G et al., PLoS Genet 2014',
   'pgs_score', {'pgs_id': 'PGS000040', 'trait': 'Celiac disease'})

_t('pgs_metabolic_4928', 'PGS - Metabolic / Endocrine', 'Metabolic syndrome (PGS004928)',
   '916,017 variants. OR 1.24, R2=0.046. Park S et al., Nat Genet 2024 (EUR+EAS)',
   'pgs_score', {'pgs_id': 'PGS004928', 'trait': 'Metabolic syndrome'})

_t('pgs_hypothyroid_820', 'PGS - Metabolic / Endocrine', 'Hypothyroidism (PGS000820)',
   '890,908 variants. OR 1.33, AUROC 0.60. Luo J et al., Clin Cancer Res 2021',
   'pgs_score', {'pgs_id': 'PGS000820', 'trait': 'Hypothyroidism'})

_t('pgs_hypothyroidism_4935', 'PGS - Metabolic / Endocrine', 'Hypothyroidism (PGS004935)',
   '6,127 variants. AUROC 0.70. Moreno-Grau S et al., Hum Genomics 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004935', 'trait': 'Hypothyroidism'})

_t('pgs_bmd_2632', 'PGS - Metabolic / Endocrine', 'Osteoporosis / BMD (PGS002632)',
   '432,286 variants. R2=0.250. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002632', 'trait': 'Bone mineral density'})

_t('pgs_hba1c_4044', 'PGS - Metabolic / Endocrine', 'HbA1c (PGS004044)',
   '907,906 variants. R2=0.039. Monti R et al., AJHG 2024',
   'pgs_score', {'pgs_id': 'PGS004044', 'trait': 'HbA1c'})

_t('pgs_nafld_2283', 'PGS - Metabolic / Endocrine', 'NAFLD (PGS002283)',
   '15 variants. beta=0.094. Schnurr TM et al., Hepatol Commun 2022',
   'pgs_score', {'pgs_id': 'PGS002283', 'trait': 'NAFLD'})

_t('pgs_hepatocellular_872', 'PGS - Metabolic / Endocrine', 'Non-alcoholic fatty liver disease (PGS000872)',
   '5 variants. OR 3.4-11.9 (NAFLD/cirrhosis). Bianco C et al., J Hepatol 2020 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000872', 'trait': 'Non-alcoholic fatty liver disease'})

_t('pgs_vitd_3554', 'PGS - Metabolic / Endocrine', 'Vitamin D levels (PGS003554)',
   '979,739 variants. R2=0.035. Ding Y et al., bioRxiv 2022',
   'pgs_score', {'pgs_id': 'PGS003554', 'trait': 'Vitamin D levels'})

_t('pgs_height_2596', 'PGS - Metabolic / Endocrine', 'Glucose (genome-wide) (PGS002596)',
   '1,103,034 variants. R2=0.654. Weissbrod O et al., Nat Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002596', 'trait': 'Glucose'})

_t('custom_pgs000323', 'PGS - Metabolic / Endocrine', 'Serum testosterone levels in males (PGS000323)',
   '8,235 variants. Flynn E et al., 2020. Eur J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS000323', 'trait': 'Serum testosterone levels in males'})

_t('custom_pgs000321', 'PGS - Metabolic / Endocrine', 'Serum testosterone levels (PGS000321)',
   '7,319 variants. Flynn E et al., 2020. Eur J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS000321', 'trait': 'Serum testosterone levels'})

_t('custom_pgs000696', 'PGS - Metabolic / Endocrine', 'Testosterone [nmol/L] (PGS000696)',
   '8,223 variants. Sinnott-Armstrong N et al., 2021. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS000696', 'trait': 'Testosterone [nmol/L]'})

_t('custom_pgs001988', 'PGS - Metabolic / Endocrine', 'Testosterone (male only) (PGS001988)',
   '3,985 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS001988', 'trait': 'Testosterone (male only)'})

_t('custom_pgs002205', 'PGS - Metabolic / Endocrine', 'Testosterone (male only) (PGS002205)',
   '584,991 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS002205', 'trait': 'Testosterone (male only)'})

_t('custom_pgs003559', 'PGS - Metabolic / Endocrine', 'Testosterone (male only) (PGS003559)',
   '979,739 variants. Ding Y et al., 2022. bioRxiv',
   'pgs_score', {'pgs_id': 'PGS003559', 'trait': 'Testosterone (male only)'})


# ── PGS - Autoimmune / Inflammatory ─────────────────────────────
_t('pgs_ibd_4081', 'PGS - Autoimmune / Inflammatory', 'IBD (PGS004081)',
   '1,073,268 variants. AUROC 0.68. Monti R et al., AJHG 2024',
   'pgs_score', {'pgs_id': 'PGS004081', 'trait': 'Inflammatory bowel disease'})

_t('pgs_ibd_4151', 'PGS - Autoimmune / Inflammatory', 'IBD / Crohn\'s / UC (IBD, best) (PGS004151)',
   '1,102,205 variants. AUROC 0.695, OR 2.06. Monti R et al., AJHG 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004151', 'trait': 'IBD / Crohn\'s / UC'})

_t('pgs_ibd_20', 'PGS - Autoimmune / Inflammatory', 'IBD / Crohn\'s / UC (classic IBD) (PGS000020)',
   '228 variants. AUROC 0.63. Khera AV et al., Nat Genet 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000020', 'trait': 'IBD / Crohn\'s / UC'})

_t('pgs_psoriasis_1288', 'PGS - Autoimmune / Inflammatory', 'Inflammatory bowel disease (PGS001288)',
   '7,534 variants. AUROC 0.70. Tanigawa Y et al., PLoS Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS001288', 'trait': 'Inflammatory bowel disease'})

_t('pgs_crohns_4254', 'PGS - Autoimmune / Inflammatory', 'Crohn\'s disease (PGS004254)',
   '744,682 variants. AUROC 0.72, OR 2.18. Middha P et al., Nat Commun 2024',
   'pgs_score', {'pgs_id': 'PGS004254', 'trait': 'Crohn\'s disease'})

_t('pgs_uc_4253', 'PGS - Autoimmune / Inflammatory', 'Ulcerative colitis (PGS004253)',
   '744,575 variants. OR 1.84, AUROC 0.66. Middha P et al., Nat Commun 2024',
   'pgs_score', {'pgs_id': 'PGS004253', 'trait': 'Ulcerative colitis'})

_t('pgs_ms_4700', 'PGS - Autoimmune / Inflammatory', 'Multiple sclerosis (PGS004700)',
   '12 variants. AUROC 0.76 (HLA-GRS). Loginovic P et al., Nat Commun 2024',
   'pgs_score', {'pgs_id': 'PGS004700', 'trait': 'Multiple sclerosis'})

_t('pgs_multiple_2726', 'PGS - Autoimmune / Inflammatory', 'Multiple sclerosis (HLA+genome-wide) (PGS002726)',
   '476,399 variants. AUROC 0.80, OR 15.0 (top 10%). Shams H et al., Brain 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002726', 'trait': 'Multiple sclerosis'})

_t('pgs_multiple_4699', 'PGS - Autoimmune / Inflammatory', 'Multiple sclerosis (HLA+nonHLA) (PGS004699)',
   '307 variants. AUROC 0.764. Loginovic P et al., Nat Commun 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004699', 'trait': 'Multiple sclerosis'})

_t('pgs_multiple_2312', 'PGS - Autoimmune / Inflammatory', 'Multiple sclerosis (genome-wide) (PGS002312)',
   '1,109,311 variants. AUROC 0.69. Weissbrod O et al., Nat Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002312', 'trait': 'Multiple sclerosis'})

_t('pgs_ra_2745', 'PGS - Autoimmune / Inflammatory', 'Rheumatoid arthritis (PGS002745)',
   '2,575 variants. AUROC 0.66. Ishigaki K et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002745', 'trait': 'Rheumatoid arthritis'})

_t('pgs_rheumatoid_4163', 'PGS - Autoimmune / Inflammatory', 'Rheumatoid arthritis (multi-ancestry) (PGS004163)',
   '778,275 variants. AUROC 0.747, OR 2.46. Monti R et al., AJHG 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004163', 'trait': 'Rheumatoid arthritis'})

_t('pgs_rheumatoid_4873', 'PGS - Autoimmune / Inflammatory', 'Rheumatoid arthritis (INTERVENE) (PGS004873)',
   '551,074 variants. HR 1.87, C-index 0.65. Jermy B et al., Nat Commun 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004873', 'trait': 'Rheumatoid arthritis'})

_t('pgs_sle_328', 'PGS - Autoimmune / Inflammatory', 'Lupus / SLE (PGS000328)',
   '57 variants. AUROC 0.83, OR 12.32. Reid S et al., Ann Rheum Dis 2019',
   'pgs_score', {'pgs_id': 'PGS000328', 'trait': 'Lupus (SLE)'})

_t('pgs_lupus_4917', 'PGS - Autoimmune / Inflammatory', 'Lupus (SLE, multi) (PGS004917)',
   '97 variants. AUROC 0.696, OR 2.01. Cui J et al., Arthritis Rheumatol 2020 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004917', 'trait': 'Lupus (SLE, multi)'})

_t('pgs_asthma_2311', 'PGS - Autoimmune / Inflammatory', 'Asthma (PGS002311)',
   '1,109,311 variants. R2=0.024. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002311', 'trait': 'Asthma'})

_t('pgs_asthma_4877', 'PGS - Autoimmune / Inflammatory', 'Asthma (INTERVENE) (PGS004877)',
   '870,454 variants. HR 1.42-1.48 (7 biobanks). Jermy B et al., Nat Commun 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004877', 'trait': 'Asthma'})

_t('pgs_asthma_4723', 'PGS - Autoimmune / Inflammatory', 'Asthma (PRSmix) (PGS004723)',
   '985,316 variants. Incr. R2=0.033. Truong B et al., Cell Genomics 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004723', 'trait': 'Asthma'})

_t('pgs_asthma_4537', 'PGS - Autoimmune / Inflammatory', 'Asthma (metaPRS) (PGS004537)',
   '1,059,939 variants. OR 1.40/SD. Jung H et al., Commun Biol 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004537', 'trait': 'Asthma'})

_t('pgs_psoriasis_1312', 'PGS - Autoimmune / Inflammatory', 'Psoriasis (PGS001312)',
   '204 variants. AUROC 0.70. Tanigawa Y et al., PLoS Genet 2022',
   'pgs_score', {'pgs_id': 'PGS001312', 'trait': 'Psoriasis'})

_t('pgs_psoriasis_5309', 'PGS - Autoimmune / Inflammatory', 'Psoriasis (PGS005309)',
   '513,461 variants. OR 1.49. Saklatvala JR et al., Genome Med 2025 (EUR)',
   'pgs_score', {'pgs_id': 'PGS005309', 'trait': 'Psoriasis'})

_t('pgs_psoriatic_1287', 'PGS - Autoimmune / Inflammatory', 'Psoriatic arthropathy (PGS001287)',
   '36 variants. AUROC 0.73. Tanigawa Y et al., PLoS Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS001287', 'trait': 'Psoriatic arthropathy'})

_t('pgs_ankspon_1267', 'PGS - Autoimmune / Inflammatory', 'Ankylosing spondylitis (PGS001267)',
   '10 variants. AUROC 0.85. Tanigawa Y et al., PLoS Genet 2022',
   'pgs_score', {'pgs_id': 'PGS001267', 'trait': 'Ankylosing spondylitis'})

_t('pgs_graves_5265', 'PGS - Autoimmune / Inflammatory', 'Graves\' disease (PGS005265)',
   '1,085,173 variants. AUROC 0.665, OR 1.63. White SL et al., medRxiv 2025 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005265', 'trait': 'Graves\' disease'})

_t('pgs_sjogrens_1308', 'PGS - Autoimmune / Inflammatory', 'Sjogren\'s syndrome (PGS001308)',
   '7 variants. AUROC 0.80 (SAS), 0.77 (EUR). Tanigawa Y et al., PLoS Genet 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS001308', 'trait': 'Sjogren\'s syndrome'})

_t('pgs_atopic_4903', 'PGS - Autoimmune / Inflammatory', 'Atopic dermatitis (PGS004903)',
   '38 variants. Significant in EUR. Al-Janabi A et al., JACI 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004903', 'trait': 'Atopic dermatitis'})

_t('pgs_aortic_1285', 'PGS - Autoimmune / Inflammatory', 'Allergic disease (hay fever, rhinitis, or eczema) (diagnosed by doctor) (PGS001285)',
   '11,285 variants. AUROC 0.63. Tanigawa Y et al., PLoS Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS001285', 'trait': 'Allergic disease (hay fever, rhinitis, or eczema) (diagnosed by doctor)'})

_t('pgs_hypertrophic_1284', 'PGS - Autoimmune / Inflammatory', 'Allergic disease (hay fever, allergic rhinitis, or eczema) (PGS001284)',
   '4,236 variants. AUROC 0.61. Tanigawa Y et al., PLoS Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS001284', 'trait': 'Allergic disease (hay fever, allergic rhinitis, or eczema)'})

_t('pgs_vitiligo_1290', 'PGS - Autoimmune / Inflammatory', 'Osteoarthritis (PGS001290)',
   '3,672 variants. AUROC 0.72. Tanigawa Y et al., PLoS Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS001290', 'trait': 'Osteoarthritis'})


# ── PGS - Neurological / Mental Health ──────────────────────────
_t('pgs_alzheimer_4590', 'PGS - Neurological / Mental Health', 'Alzheimer\'s disease (PGS004590)',
   '363 variants. AUROC 0.68 (excl APOE). Lake J et al., Mol Psychiatry 2023',
   'pgs_score', {'pgs_id': 'PGS004590', 'trait': 'Alzheimer\'s disease'})

_t('pgs_alzheimers_2280', 'PGS - Neurological / Mental Health', 'Alzheimer\'s disease (Bellenguez) (PGS002280)',
   '83 variants. HR 1.93 (top vs bottom decile). Bellenguez C et al., Nat Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002280', 'trait': 'Alzheimer\'s disease'})

_t('pgs_alzheimers_4092', 'PGS - Neurological / Mental Health', 'Alzheimer\'s disease (genome-wide) (PGS004092)',
   '1,109,233 variants. AUROC 0.665, OR 1.78/SD. Monti R et al., AJHG 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004092', 'trait': 'Alzheimer\'s disease'})

_t('pgs_alzheimers_4863', 'PGS - Neurological / Mental Health', 'Alzheimer\'s disease (multi-ancestry) (PGS004863)',
   '74 variants. AUROC 0.746 (EUR), 0.751 (EAS). Sleiman PM et al., Alzheimers Dement 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS004863', 'trait': 'Alzheimer\'s disease'})

_t('pgs_alzheimers_334', 'PGS - Neurological / Mental Health', 'Alzheimer\'s disease (incl APOE) (PGS000334)',
   '21 variants. AUROC 0.83. Desikan RS et al., PLoS Med 2017 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000334', 'trait': 'Alzheimer\'s disease'})

_t('pgs_alzheimers_25', 'PGS - Neurological / Mental Health', 'Alzheimer\'s disease (GPS) (PGS000025)',
   '6,630,150 variants. AUROC 0.75. Khera AV et al., Nat Genet 2018 (EUR)',
   'pgs_score', {'pgs_id': 'PGS000025', 'trait': 'Alzheimer\'s disease'})

_t('pgs_parkinson_903', 'PGS - Neurological / Mental Health', 'Parkinson\'s disease (PGS000903)',
   '1,805 variants. AUROC 0.692, OR 6.25. Nalls MA et al., Lancet Neurol 2019',
   'pgs_score', {'pgs_id': 'PGS000903', 'trait': 'Parkinson\'s disease'})

_t('pgs_parkinsons_3763', 'PGS - Neurological / Mental Health', 'Parkinson\'s disease (JAMA Neurol) (PGS003763)',
   '44 variants. HR 1.72, HR 3.22 (w/ frailty). Zheng Z et al., JAMA Neurol 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003763', 'trait': 'Parkinson\'s disease'})

_t('pgs_parkinsons_2940', 'PGS - Neurological / Mental Health', 'Parkinson\'s disease (genome-wide) (PGS002940)',
   '1,805 variants. AUROC 0.72. Kim JJ et al., Genome Med 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002940', 'trait': 'Parkinson\'s disease'})

_t('pgs_schiz_135', 'PGS - Neurological / Mental Health', 'Schizophrenia (PGS000135)',
   '972,439 variants. AUROC 0.74. Zheutlin AB et al., Am J Psychiatry 2019',
   'pgs_score', {'pgs_id': 'PGS000135', 'trait': 'Schizophrenia'})

_t('pgs_depression_3333', 'PGS - Neurological / Mental Health', 'Major depression (PGS003333)',
   '1,088,415 variants. R2=0.022. Fang Y et al., Biol Psychiatry 2022',
   'pgs_score', {'pgs_id': 'PGS003333', 'trait': 'Major depression'})

_t('pgs_major_4760', 'PGS - Neurological / Mental Health', 'Major depression (PRSmixPlus) (PGS004760)',
   '2,141,267 variants. Incr. R2=0.024. Truong B et al., Cell Genomics 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004760', 'trait': 'Major depression'})

_t('pgs_major_4885', 'PGS - Neurological / Mental Health', 'Major depression (INTERVENE) (PGS004885)',
   '801,544 variants. HR 1.24/SD, C-index 0.58. Jermy B et al., Nat Commun 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004885', 'trait': 'Major depression'})

_t('pgs_bipolar_2787', 'PGS - Neurological / Mental Health', 'Bipolar disorder (PGS002787)',
   '937,511 variants. Gui Y et al., Transl Psychiatry 2022',
   'pgs_score', {'pgs_id': 'PGS002787', 'trait': 'Bipolar disorder'})

_t('pgs_adhd_2746', 'PGS - Neurological / Mental Health', 'ADHD (PGS002746)',
   '513,659 variants. beta=0.11. Lahey BB et al., J Psychiatr Res 2022',
   'pgs_score', {'pgs_id': 'PGS002746', 'trait': 'ADHD'})

_t('pgs_adhd_3753', 'PGS - Neurological / Mental Health', 'ADHD (Latin Am eval) (PGS003753)',
   '35,445 variants. Validated in Brazilian. Sato JR et al., Genes Brain Behav 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003753', 'trait': 'ADHD (Latin Am eval)'})

_t('pgs_autism_327', 'PGS - Neurological / Mental Health', 'Autism spectrum (PGS000327)',
   '35,087 variants. OR 1.33, R2=0.025. Grove J et al., Nat Genet 2019',
   'pgs_score', {'pgs_id': 'PGS000327', 'trait': 'Autism spectrum'})

_t('pgs_autism_2790', 'PGS - Neurological / Mental Health', 'Autism spectrum (PGS002790)',
   '916,713 variants. R2=0.005. Gui Y et al., Transl Psychiatry 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS002790', 'trait': 'Autism spectrum'})

_t('pgs_epilepsy_4881', 'PGS - Neurological / Mental Health', 'Epilepsy (PGS004881)',
   '605,432 variants. HR 1.12/SD. Jermy B et al., Nat Commun 2024',
   'pgs_score', {'pgs_id': 'PGS004881', 'trait': 'Epilepsy'})

_t('pgs_addiction_3849', 'PGS - Neurological / Mental Health', 'Addiction risk (multi-substance) (PGS003849)',
   '584,753 variants. OR 1.73 (opioid), 1.57 (alcohol). Hatoum AS et al., Nat Ment Health 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003849', 'trait': 'Addiction risk (multi-substance)'})

_t('pgs_anxiety_4451', 'PGS - Neurological / Mental Health', 'Anxiety (PGS004451)',
   '1,059,939 variants. OR 1.19/SD. Jung H et al., Commun Biol 2024 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004451', 'trait': 'Anxiety'})

_t('pgs_ptsd_5393', 'PGS - Neurological / Mental Health', 'PTSD (PGS005393)',
   '53,705 variants. R2=0.087. Bugiga AVG et al., Braz J Psychiatry 2024 (Multi)',
   'pgs_score', {'pgs_id': 'PGS005393', 'trait': 'PTSD'})

_t('pgs_migraine_4798', 'PGS - Neurological / Mental Health', 'Migraine (PGS004798)',
   '3,984,158 variants. R2=0.004. Truong B et al., Cell Genomics 2024 (SAS)',
   'pgs_score', {'pgs_id': 'PGS004798', 'trait': 'Migraine'})

_t('pgs_neuroticism_3565', 'PGS - Neurological / Mental Health', 'Neuroticism (PGS003565)',
   '979,739 variants. R2=0.05. Ding Y et al., bioRxiv 2022',
   'pgs_score', {'pgs_id': 'PGS003565', 'trait': 'Neuroticism'})


# ── PGS - Renal / Urinary ───────────────────────────────────────
_t('pgs_ckd_4889', 'PGS - Renal / Urinary', 'CKD (PGS004889)',
   '1,117,375 variants. HR 1.33, C-index 0.727. Mandla R et al., Genome Med 2024',
   'pgs_score', {'pgs_id': 'PGS004889', 'trait': 'Chronic kidney disease'})

_t('pgs_kidney_4493', 'PGS - Renal / Urinary', 'Kidney stones (PGS004493)',
   '1,059,939 variants. OR 1.23/SD. Jung H et al., Commun Biol 2024',
   'pgs_score', {'pgs_id': 'PGS004493', 'trait': 'Kidney stones'})


# ── PGS - Eye / Vision ──────────────────────────────────────────
_t('pgs_amd_4606', 'PGS - Eye / Vision', 'AMD (PGS004606)',
   '1,000,946 variants. OR 1.76, AUROC 0.71. Gorman BR et al., Nat Genet 2024',
   'pgs_score', {'pgs_id': 'PGS004606', 'trait': 'Age-related macular degeneration'})

_t('pgs_glaucoma_1797', 'PGS - Eye / Vision', 'Glaucoma (PGS001797)',
   '885,417 variants. AUROC 0.749. Wang Y et al., Cell Genomics 2023',
   'pgs_score', {'pgs_id': 'PGS001797', 'trait': 'Glaucoma'})

_t('pgs_myopia_3564', 'PGS - Eye / Vision', 'Myopia (PGS003564)',
   '979,739 variants. R2=0.06. Ding Y et al., bioRxiv 2022',
   'pgs_score', {'pgs_id': 'PGS003564', 'trait': 'Myopia'})


# ── PGS - Cognitive & Educational ───────────────────────────────
_t('pgs_atrial_3724', 'PGS - Cognitive & Educational', 'Intelligence quotient (multi-ancestry) (PGS003724)',
   '1,296,172 variants. HR 1.60. Patel AP et al., Nat Med 2023 (Multi)',
   'pgs_score', {'pgs_id': 'PGS003724', 'trait': 'Intelligence quotient'})

_t('pgs_intelligence_3724', 'PGS - Cognitive & Educational', 'Intelligence / Cognitive Ability (IQ) (PGS003724)',
   '6,680,000 variants. R2=0.12. Hatoum AS et al., Nat Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003724', 'trait': 'Intelligence / Cognitive Ability'})

_t('pgs_intelligence_3723', 'PGS - Cognitive & Educational', 'Intelligence / Cognitive Ability (cognitive performance / cEF) (PGS003723)',
   '6,680,000 variants. R2=0.11. Hatoum AS et al., Nat Genet 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003723', 'trait': 'Intelligence / Cognitive Ability'})

_t('pgs_intelligence_3510', 'PGS - Cognitive & Educational', 'Intelligence / Cognitive Ability (verbal-numerical reasoning) (PGS003510)',
   '979,739 variants. R2=0.15. Ding Y et al., bioRxiv 2022 (EUR)',
   'pgs_score', {'pgs_id': 'PGS003510', 'trait': 'Intelligence / Cognitive Ability'})

_t('pgs_iq_4427', 'PGS - Cognitive & Educational', 'Fluid intelligence (PGS004427)',
   '1,059,939 variants. R2=0.223 (EUR). Jung H et al., Commun Biol 2024',
   'pgs_score', {'pgs_id': 'PGS004427', 'trait': 'Fluid intelligence'})

_t('custom_pgs001232', 'PGS - Cognitive & Educational', 'Fluid intelligence score (PGS001232)',
   '10,055 variants. Tanigawa Y et al., 2022. PLoS Genet',
   'pgs_score', {'pgs_id': 'PGS001232', 'trait': 'Fluid intelligence score'})

_t('custom_pgs001919', 'PGS - Cognitive & Educational', 'Fluid intelligence score (PGS001919)',
   '26,145 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS001919', 'trait': 'Fluid intelligence score'})

_t('custom_pgs002135', 'PGS - Cognitive & Educational', 'Fluid intelligence score (PGS002135)',
   '903,259 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS002135', 'trait': 'Fluid intelligence score'})

_t('pgs_edu_2012', 'PGS - Cognitive & Educational', 'Educational attainment (PGS002012)',
   '50,413 variants. r=0.175. Prive F et al., AJHG 2022',
   'pgs_score', {'pgs_id': 'PGS002012', 'trait': 'Educational attainment'})

_t('custom_pgs002231', 'PGS - Cognitive & Educational', 'Qualifications (years of education) (PGS002231)',
   '950,845 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS002231', 'trait': 'Qualifications (years of education)'})

_t('custom_pgs003573', 'PGS - Cognitive & Educational', 'Qualifications (years of education) (PGS003573)',
   '979,739 variants. Ding Y et al., 2022. bioRxiv',
   'pgs_score', {'pgs_id': 'PGS003573', 'trait': 'Qualifications (years of education)'})

_t('custom_pgs002319', 'PGS - Cognitive & Educational', 'College education (PGS002319)',
   '1,109,311 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002319', 'trait': 'College education'})

_t('custom_pgs002391', 'PGS - Cognitive & Educational', 'College education (PGS002391)',
   '6,535 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002391', 'trait': 'College education'})

_t('custom_pgs002440', 'PGS - Cognitive & Educational', 'College education (PGS002440)',
   '24,405 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002440', 'trait': 'College education'})

_t('custom_pgs002489', 'PGS - Cognitive & Educational', 'College education (PGS002489)',
   '126,911 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002489', 'trait': 'College education'})

_t('custom_pgs002538', 'PGS - Cognitive & Educational', 'College education (PGS002538)',
   '1,136 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002538', 'trait': 'College education'})

_t('custom_pgs002587', 'PGS - Cognitive & Educational', 'College education (PGS002587)',
   '514 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002587', 'trait': 'College education'})

_t('custom_pgs002636', 'PGS - Cognitive & Educational', 'College education (PGS002636)',
   '577,448 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002636', 'trait': 'College education'})

_t('custom_pgs002685', 'PGS - Cognitive & Educational', 'College education (PGS002685)',
   '980,529 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002685', 'trait': 'College education'})

_t('custom_pgs001931', 'PGS - Cognitive & Educational', 'Average total household income before tax (PGS001931)',
   '41,836 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS001931', 'trait': 'Average total household income before tax'})

_t('custom_pgs002148', 'PGS - Cognitive & Educational', 'Average total household income before tax (PGS002148)',
   '932,197 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS002148', 'trait': 'Average total household income before tax'})

_t('custom_pgs003516', 'PGS - Cognitive & Educational', 'Average total household income before tax (PGS003516)',
   '979,739 variants. Ding Y et al., 2022. bioRxiv',
   'pgs_score', {'pgs_id': 'PGS003516', 'trait': 'Average total household income before tax'})


# ── PGS - Physical Traits ───────────────────────────────────────
_t('pgs_height_1229', 'PGS - Physical Traits', 'Height (PGS001229)',
   '51,209 variants. R2=0.717. Tanigawa Y et al., PLoS Genet 2022',
   'pgs_score', {'pgs_id': 'PGS001229', 'trait': 'Height'})

_t('pgs_height_4214', 'PGS - Physical Traits', 'Height (LASSO) (PGS004214)',
   '23,686 variants. R2=0.712 (sibling-validated). Raben TG et al., Sci Rep 2023 (EUR)',
   'pgs_score', {'pgs_id': 'PGS004214', 'trait': 'Height'})

_t('custom_pgs000297', 'PGS - Physical Traits', 'Height (PGS000297)',
   '3,290 variants. Xie T et al., 2020. Circ Genom Precis Med',
   'pgs_score', {'pgs_id': 'PGS000297', 'trait': 'Height'})

_t('custom_pgs000758', 'PGS - Physical Traits', 'Adult standing height (PGS000758)',
   '33,938 variants. Lu T et al., 2021. J Clin Endocrinol Metab',
   'pgs_score', {'pgs_id': 'PGS000758', 'trait': 'Adult standing height'})

_t('custom_pgs001405', 'PGS - Physical Traits', 'Height (PGS001405)',
   '3,166 variants. Tanigawa Y et al., 2022. PLoS Genet',
   'pgs_score', {'pgs_id': 'PGS001405', 'trait': 'Height'})

_t('custom_pgs001929', 'PGS - Physical Traits', 'Standing height (PGS001929)',
   '156,514 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS001929', 'trait': 'Standing height'})

_t('custom_pgs002146', 'PGS - Physical Traits', 'Standing height (PGS002146)',
   '922,538 variants. Privé F et al., 2022. Am J Hum Genet',
   'pgs_score', {'pgs_id': 'PGS002146', 'trait': 'Standing height'})

_t('custom_pgs002332', 'PGS - Physical Traits', 'Height (PGS002332)',
   '1,109,311 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002332', 'trait': 'Height'})

_t('custom_pgs002368', 'PGS - Physical Traits', 'Height (PGS002368)',
   '920,927 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002368', 'trait': 'Height'})

_t('custom_pgs002404', 'PGS - Physical Traits', 'Height (PGS002404)',
   '56,984 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002404', 'trait': 'Height'})

_t('custom_pgs002453', 'PGS - Physical Traits', 'Height (PGS002453)',
   '103,911 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002453', 'trait': 'Height'})

_t('custom_pgs002600', 'PGS - Physical Traits', 'Height (PGS002600)',
   '18,937 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002600', 'trait': 'Height'})

_t('custom_pgs002502', 'PGS - Physical Traits', 'Height (PGS002502)',
   '262,080 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002502', 'trait': 'Height'})

_t('custom_pgs002551', 'PGS - Physical Traits', 'Height (PGS002551)',
   '27,070 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002551', 'trait': 'Height'})

_t('custom_pgs002649', 'PGS - Physical Traits', 'Height (PGS002649)',
   '478,839 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002649', 'trait': 'Height'})

_t('custom_pgs002698', 'PGS - Physical Traits', 'Height (PGS002698)',
   '986,966 variants. Weissbrod O et al., 2022. Nat Genet',
   'pgs_score', {'pgs_id': 'PGS002698', 'trait': 'Height'})

_t('custom_pgs002748', 'PGS - Physical Traits', 'Height (PGS002748)',
   '251 variants. Chiou JS et al., 2022. BMC Med',
   'pgs_score', {'pgs_id': 'PGS002748', 'trait': 'Height'})

_t('custom_pgs002800', 'PGS - Physical Traits', 'Height (PGS002800)',
   '1,156,741 variants. Yengo L et al., 2022. Nature',
   'pgs_score', {'pgs_id': 'PGS002800', 'trait': 'Height'})

_t('custom_pgs002802', 'PGS - Physical Traits', 'Height (PGS002802)',
   '1,103,042 variants. Yengo L et al., 2022. Nature',
   'pgs_score', {'pgs_id': 'PGS002802', 'trait': 'Height'})

_t('custom_pgs002801', 'PGS - Physical Traits', 'Height (PGS002801)',
   '975,455 variants. Yengo L et al., 2022. Nature',
   'pgs_score', {'pgs_id': 'PGS002801', 'trait': 'Height'})

_t('custom_pgs002803', 'PGS - Physical Traits', 'Height (PGS002803)',
   '990,792 variants. Yengo L et al., 2022. Nature',
   'pgs_score', {'pgs_id': 'PGS002803', 'trait': 'Height'})

_t('pgs_hair_2598', 'PGS - Physical Traits', 'Hair color (PGS002598)',
   '8,312 variants. R2=0.182. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002598', 'trait': 'Hair color'})

_t('pgs_skin_1897', 'PGS - Physical Traits', 'Skin pigmentation (PGS001897)',
   '15,817 variants. r=0.387. Prive F et al., AJHG 2022',
   'pgs_score', {'pgs_id': 'PGS001897', 'trait': 'Skin pigmentation'})

_t('pgs_baldness_2314', 'PGS - Physical Traits', 'Male pattern baldness (PGS002314)',
   '1,109,311 variants. R2=0.143. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002314', 'trait': 'Male pattern baldness'})

_t('pgs_bodyfat_3899', 'PGS - Physical Traits', 'Body fat % (PGS003899)',
   '34,374 variants. R2=0.056. Tanigawa Y et al., AJHG 2023',
   'pgs_score', {'pgs_id': 'PGS003899', 'trait': 'Body fat %'})

_t('pgs_grip_1162', 'PGS - Physical Traits', 'Hip circumference (PGS001162)',
   '10,872 variants. R2=0.033. Tanigawa Y et al., PLoS Genet 2022',
   'pgs_score', {'pgs_id': 'PGS001162', 'trait': 'Hip circumference'})

_t('pgs_age_1183', 'PGS - Physical Traits', 'Age at menarche (PGS001183)',
   '25,172 variants. R2=0.10. Tanigawa Y et al., PLoS Genet 2022',
   'pgs_score', {'pgs_id': 'PGS001183', 'trait': 'Age at menarche'})

_t('pgs_hearing_762', 'PGS - Physical Traits', 'Hearing difficulty (PGS000762)',
   '100,325 variants. R2=0.091. Cherny SS et al., EJHG 2020',
   'pgs_score', {'pgs_id': 'PGS000762', 'trait': 'Hearing difficulty'})


# ── PGS - Lifestyle / Behavioral ────────────────────────────────
_t('pgs_coffee_1123', 'PGS - Lifestyle / Behavioral', 'Coffee consumption (PGS001123)',
   '48 variants. AUROC 0.617. Tanigawa Y et al., PLoS Genet 2022',
   'pgs_score', {'pgs_id': 'PGS001123', 'trait': 'Coffee consumption'})

_t('pgs_alcohol_5213', 'PGS - Lifestyle / Behavioral', 'Alcohol use disorder (PGS005213)',
   '336,813 variants. R2=0.05. Deng WQ et al., Alcohol Alcoholism 2024',
   'pgs_score', {'pgs_id': 'PGS005213', 'trait': 'Alcohol use disorder'})

_t('pgs_smoking_3357', 'PGS - Lifestyle / Behavioral', 'Smoking initiation (PGS003357)',
   '1,194,472 variants. dAUC=0.015. Saunders GRB et al., Nature 2022',
   'pgs_score', {'pgs_id': 'PGS003357', 'trait': 'Smoking initiation'})

_t('pgs_telomere_2616', 'PGS - Lifestyle / Behavioral', 'Smoking status (PGS002616)',
   '1,103,034 variants. R2=0.024. Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002616', 'trait': 'Smoking status'})

_t('pgs_insomnia_908', 'PGS - Lifestyle / Behavioral', 'Insomnia (PGS000908)',
   '2,746,982 variants. OR 1.12-1.28. Campos AI et al., Commun Med 2021',
   'pgs_score', {'pgs_id': 'PGS000908', 'trait': 'Insomnia'})

_t('pgs_chronotype_2318', 'PGS - Lifestyle / Behavioral', 'Chronotype (morn/eve) (PGS002318)',
   '1,109,311 variants. Incr. R2=0.036 (EUR). Weissbrod O et al., Nat Genet 2022',
   'pgs_score', {'pgs_id': 'PGS002318', 'trait': 'Chronotype (morn/eve)'})

_t('pgs_risk_205', 'PGS - Lifestyle / Behavioral', 'Risk tolerance (PGS000205)',
   '1,110,737 variants. dR2=0.016. Barr PB et al., Transl Psychiatry 2020',
   'pgs_score', {'pgs_id': 'PGS000205', 'trait': 'Risk tolerance'})

_t('pgs_hepatocellular_2254', 'PGS - Lifestyle / Behavioral', 'Physical activity (self-reported) (PGS002254)',
   '8 variants. HR 1.33. Sarin SK et al., Hepatology 2022 (Multi)',
   'pgs_score', {'pgs_id': 'PGS002254', 'trait': 'Physical activity (self-reported)'})

_t('pgs_longevity_906', 'PGS - Lifestyle / Behavioral', 'Longevity (PGS000906)',
   '330 variants. HR 0.89/SD. Testi N et al., J Gerontol 2021',
   'pgs_score', {'pgs_id': 'PGS000906', 'trait': 'Longevity'})

# ── Monogenic ───────────────────────────────────────────────────
_t('mono_cancer', 'Monogenic', 'Cancer predisposition genes (ACMG SF v3.3)',
   'Screen 28 genes: APC, BRCA1, BRCA2, BMPR1A, MAX, MEN1, MLH1, MSH2, MSH6, MUTYH, NF2, PALB2, PMS2, PTEN, RB1, RET, SDHAF2, SDHB, SDHC, SDHD, SMAD4, STK11, TMEM127, TP53, TSC1, TSC2, VHL, WT1',
   'clinvar_screen', {'genes': ['APC', 'BRCA1', 'BRCA2', 'BMPR1A', 'MAX', 'MEN1', 'MLH1', 'MSH2', 'MSH6', 'MUTYH', 'NF2', 'PALB2', 'PMS2', 'PTEN', 'RB1', 'RET', 'SDHAF2', 'SDHB', 'SDHC', 'SDHD', 'SMAD4', 'STK11', 'TMEM127', 'TP53', 'TSC1', 'TSC2', 'VHL', 'WT1'], 'panel': 'Cancer Predisposition'})

_t('mono_cardio', 'Monogenic', 'Cardiovascular genes (ACMG SF v3.3)',
   'Screen ~41 genes: ACTA2, ACTC1, APOB, BAG3, CALM1-3, CASQ2, COL3A1, DES, DSC2, DSG2, DSP, FBN1, FLNC, KCNH2, KCNQ1, LDLR, LMNA, MYH7, MYH11, MYBPC3, MYL2, MYL3, PCSK9, PKP2, PLN, PRKAG2, RBM20, RYR2, SCN5A, SMAD3, TGFBR1, TGFBR2, TMEM43, TNNC1, TNNI3, TNNT2, TPM1, TRDN, TTN',
   'clinvar_screen', {'genes': ['ACTA2', 'ACTC1', 'APOB', 'BAG3', 'CALM1', 'CALM2', 'CALM3', 'CASQ2', 'COL3A1', 'DES', 'DSC2', 'DSG2', 'DSP', 'FBN1', 'FLNC', 'KCNH2', 'KCNQ1', 'LDLR', 'LMNA', 'MYH7', 'MYH11', 'MYBPC3', 'MYL2', 'MYL3', 'PCSK9', 'PKP2', 'PLN', 'PRKAG2', 'RBM20', 'RYR2', 'SCN5A', 'SMAD3', 'TGFBR1', 'TGFBR2', 'TMEM43', 'TNNC1', 'TNNI3', 'TNNT2', 'TPM1', 'TRDN', 'TTN'], 'panel': 'Cardiovascular'})

_t('mono_metabolism', 'Monogenic', 'Metabolism genes (ACMG SF v3.3)',
   'Screen 5 genes: BTD, CYP27A1, GAA, GLA, OTC',
   'clinvar_screen', {'genes': ['BTD', 'CYP27A1', 'GAA', 'GLA', 'OTC'], 'panel': 'Metabolism'})

_t('mono_misc', 'Monogenic', 'Miscellaneous ACMG genes',
   'Screen 10 genes: ABCD1, ACVRL1, ATP7B, CACNA1S, ENG, HFE, HNF1A, RPE65, RYR1, TTR',
   'clinvar_screen', {'genes': ['ABCD1', 'ACVRL1', 'ATP7B', 'CACNA1S', 'ENG', 'HFE', 'HNF1A', 'RPE65', 'RYR1', 'TTR'], 'panel': 'Miscellaneous'})

# ── Carrier Status ──────────────────────────────────────────────
_t('carrier_cf', 'Carrier Status', 'Cystic fibrosis (CFTR)',
   'F508del (rs113993960). Carrier freq: 1 in 25 (N. European)',
   'variant_lookup', {'variants': [{'rs': 'rs113993960', 'gene': 'CFTR', 'name': 'F508del', 'expected_ref': 'ATCT', 'expected_alt': 'A'}], 'disease': 'Cystic fibrosis'})

_t('carrier_sickle', 'Carrier Status', 'Sickle cell disease (HBB)',
   'Glu6Val (rs334). Carrier freq: 1 in 13 (African Amer.)',
   'variant_lookup', {'variants': [{'rs': 'rs334', 'gene': 'HBB', 'name': 'Glu6Val'}], 'disease': 'Sickle cell disease'})

_t('carrier_taysachs', 'Carrier Status', 'Tay-Sachs (HEXA)',
   '1278insTATC (rs387906309). Carrier freq: 1 in 30 (Ashkenazi)',
   'variant_lookup', {'variants': [{'rs': 'rs387906309', 'gene': 'HEXA', 'name': '1278insTATC', 'expected_ref': 'G', 'expected_alt': 'GTATC'}], 'disease': 'Tay-Sachs'})

_t('carrier_gaucher', 'Carrier Status', 'Gaucher disease (GBA1)',
   'N370S (rs76763715). Carrier freq: 1 in 15 (Ashkenazi)',
   'variant_lookup', {'variants': [{'rs': 'rs76763715', 'gene': 'GBA1', 'name': 'N370S'}], 'disease': 'Gaucher disease'})

_t('carrier_pku', 'Carrier Status', 'PKU (PAH)',
   'R408W (rs5030858). Carrier freq: 1 in 50 (European)',
   'variant_lookup', {'variants': [{'rs': 'rs5030858', 'gene': 'PAH', 'name': 'R408W'}], 'disease': 'PKU'})

_t('carrier_thalassemia', 'Carrier Status', 'Beta-thalassemia (HBB)',
   'Codon 39 C>T (rs11549407). Carrier freq: 5-30% (Mediterranean)',
   'variant_lookup', {'variants': [{'rs': 'rs11549407', 'gene': 'HBB', 'name': 'Codon39'}], 'disease': 'Beta-thalassemia'})

_t('carrier_hemochromatosis', 'Carrier Status', 'Hemochromatosis (HFE)',
   'C282Y (rs1800562). Carrier freq: 1 in 9 (N. European)',
   'variant_lookup', {'variants': [{'rs': 'rs1800562', 'gene': 'HFE', 'name': 'C282Y'}], 'disease': 'Hemochromatosis'})

_t('carrier_pompe', 'Carrier Status', 'Pompe disease (GAA)',
   'c.-32-13T>G (rs386834236). Carrier freq: 1 in 50 (EUR)',
   'variant_lookup', {'variants': [{'rs': 'rs386834236', 'gene': 'GAA', 'name': 'c.-32-13T>G', 'expected_ref': 'T', 'expected_alt': 'G'}], 'disease': 'Pompe disease'})

_t('carrier_sma', 'Carrier Status', 'Spinal muscular atrophy (SMN1)',
   'SMN1 deletion. Carrier freq: 1 in 40-50',
   'variant_lookup', {'variants': [{'gene': 'SMN1', 'name': 'SMN1 deletion'}], 'disease': 'Spinal muscular atrophy'})

_t('carrier_fragx', 'Carrier Status', 'Fragile X premutation (FMR1)',
   'CGG repeat 55-200. Carrier freq: 1 in 150-250 (females)',
   'specialized', {'method': 'repeat_expansion', 'gene': 'FMR1', 'disease': 'Fragile X premutation'})

_t('carrier_mcad', 'Carrier Status', 'MCAD deficiency (ACADM)',
   'K329E (rs77931234). Carrier freq: 1 in 50 (EUR)',
   'variant_lookup', {'variants': [{'rs': 'rs77931234', 'gene': 'ACADM', 'name': 'K329E'}], 'disease': 'MCAD deficiency'})

# ── Single Variants ─────────────────────────────────────────────
_t('var_apoe', 'Single Variants', "APOE e4 (Alzheimer's)",
   'rs429358 + rs7412. e4/e4 = 12-15x AD risk. e4 freq: 14%',
   'variant_lookup', {'variants': [{'rs': 'rs429358', 'gene': 'APOE', 'name': 'APOE-C112R'}, {'rs': 'rs7412', 'gene': 'APOE', 'name': 'APOE-R158C'}], 'disease': "Alzheimer's risk (APOE)", 'interpretation': 'apoe'})

_t('var_fvl', 'Single Variants', 'Factor V Leiden',
   'rs6025. 3-8x VTE risk (het). 5% EUR',
   'variant_lookup', {'variants': [{'rs': 'rs6025', 'gene': 'F5', 'name': 'Factor V Leiden'}], 'disease': 'VTE risk'})

_t('var_prothrombin', 'Single Variants', 'Prothrombin G20210A',
   'rs1799963. 2.8x VTE risk. 2-3% EUR',
   'variant_lookup', {'variants': [{'rs': 'rs1799963', 'gene': 'F2', 'name': 'G20210A'}], 'disease': 'VTE risk'})

_t('var_brca1_185', 'Single Variants', 'BRCA1 185delAG',
   'rs80357713. 45-85% lifetime breast cancer. 1% Ashkenazi',
   'variant_lookup', {'variants': [{'rs': 'rs80357713', 'gene': 'BRCA1', 'name': '185delAG'}], 'disease': 'Breast cancer'})

_t('var_mthfr', 'Single Variants', 'MTHFR C677T',
   'rs1801133. TT = 30% enzyme activity. 10-15% EUR',
   'variant_lookup', {'variants': [{'rs': 'rs1801133', 'gene': 'MTHFR', 'name': 'C677T'}], 'disease': 'Folate metabolism'})

_t('var_a1at', 'Single Variants', 'A1AT Z allele',
   'rs28929474. ZZ = emphysema + liver disease. 2-3% N.EUR',
   'variant_lookup', {'variants': [{'rs': 'rs28929474', 'gene': 'SERPINA1', 'name': 'Z allele'}], 'disease': 'Alpha-1 antitrypsin deficiency'})

_t('var_pcsk9', 'Single Variants', 'PCSK9 R46L (protective)',
   'rs11591147. ~50% lower LDL. 2% EUR',
   'variant_lookup', {'variants': [{'rs': 'rs11591147', 'gene': 'PCSK9', 'name': 'R46L'}], 'disease': 'Cardioprotective'})

_t('var_lrrk2', 'Single Variants', 'LRRK2 G2019S (PD)',
   'rs34637584. 25-42% lifetime PD risk. 0.84% Ashkenazi',
   'variant_lookup', {'variants': [{'rs': 'rs34637584', 'gene': 'LRRK2', 'name': 'G2019S'}], 'disease': "Parkinson's disease"})

_t('var_fto', 'Single Variants', 'FTO obesity',
   'rs9939609. AA = 1.67x obesity risk. A freq: 42% EUR',
   'variant_lookup', {'variants': [{'rs': 'rs9939609', 'gene': 'FTO', 'name': 'FTO'}], 'disease': 'Obesity risk'})

_t('var_tcf7l2', 'Single Variants', 'TCF7L2 diabetes',
   'rs7903146. TT = 1.8x T2D risk. T freq: 30% EUR',
   'variant_lookup', {'variants': [{'rs': 'rs7903146', 'gene': 'TCF7L2', 'name': 'TCF7L2'}], 'disease': 'Type 2 diabetes risk'})

_t('var_9p21', 'Single Variants', '9p21 CAD locus',
   'rs10757278. GG = 1.6x MI risk. G freq: 49% EUR',
   'variant_lookup', {'variants': [{'rs': 'rs10757278', 'gene': 'CDKN2A/B', 'name': '9p21'}], 'disease': 'CAD risk'})

_t('var_lpa', 'Single Variants', 'LPA (Lp(a))',
   'rs10455872. G = elevated Lp(a), 1.5x CAD. G freq: 7% EUR',
   'variant_lookup', {'variants': [{'rs': 'rs10455872', 'gene': 'LPA', 'name': 'LPA'}], 'disease': 'Elevated Lp(a)'})

# ── Fun Traits ──────────────────────────────────────────────────
_t('fun_bitter', 'Fun Traits', 'Bitter taste (PTC)',
   'TAS2R38 rs713598. PAV/PAV = supertaster',
   'variant_lookup', {'variants': [{'rs': 'rs713598', 'gene': 'TAS2R38', 'name': 'PTC taste'}], 'disease': 'Bitter taste perception'})

_t('fun_cilantro', 'Fun Traits', 'Cilantro = soap',
   'OR6A2 rs72921001. A allele = soapy taste',
   'variant_lookup', {'variants': [{'rs': 'rs72921001', 'gene': 'OR6A2', 'name': 'Cilantro'}], 'disease': 'Cilantro preference'})

_t('fun_earwax', 'Fun Traits', 'Earwax type',
   'ABCC11 rs17822931. TT = dry + less body odor',
   'variant_lookup', {'variants': [{'rs': 'rs17822931', 'gene': 'ABCC11', 'name': 'Earwax'}], 'disease': 'Earwax type'})

_t('fun_lactose', 'Fun Traits', 'Lactose tolerance',
   'MCM6 rs4988235. T = persistent lactase; CC = intolerant',
   'variant_lookup', {'variants': [{'rs': 'rs4988235', 'gene': 'MCM6', 'name': 'Lactase persistence'}], 'disease': 'Lactose tolerance'})

_t('fun_alcohol_flush', 'Fun Traits', 'Alcohol flush',
   'ALDH2 rs671. AA = cannot drink (Asian flush)',
   'variant_lookup', {'variants': [{'rs': 'rs671', 'gene': 'ALDH2', 'name': 'Alcohol flush'}], 'disease': 'Alcohol flush reaction'})

_t('fun_caffeine', 'Fun Traits', 'Caffeine metabolism',
   'CYP1A2 rs762551. AA = fast; CC = slow',
   'variant_lookup', {'variants': [{'rs': 'rs762551', 'gene': 'CYP1A2', 'name': 'Caffeine'}], 'disease': 'Caffeine metabolism'})

_t('fun_sprint', 'Fun Traits', 'Sprint vs endurance',
   'ACTN3 rs1815739. CC = sprint; TT = endurance',
   'variant_lookup', {'variants': [{'rs': 'rs1815739', 'gene': 'ACTN3', 'name': 'Sprint/Endurance'}], 'disease': 'Muscle fiber type'})

_t('fun_eye_color', 'Fun Traits', 'Eye color',
   'HERC2 rs12913832. GG = blue; AA = brown',
   'variant_lookup', {'variants': [{'rs': 'rs12913832', 'gene': 'HERC2', 'name': 'Eye color'}], 'disease': 'Eye color prediction'})

_t('fun_sneeze', 'Fun Traits', 'Photic sneeze',
   'ZEB2 rs10427255. C allele = sneeze from light',
   'variant_lookup', {'variants': [{'rs': 'rs10427255', 'gene': 'ZEB2', 'name': 'Photic sneeze'}], 'disease': 'Photic sneeze reflex'})

_t('fun_norovirus', 'Fun Traits', 'Norovirus resistance',
   'FUT2 rs601338. AA = strong resistance',
   'variant_lookup', {'variants': [{'rs': 'rs601338', 'gene': 'FUT2', 'name': 'Norovirus'}], 'disease': 'Norovirus resistance'})

_t('fun_blood_type', 'Fun Traits', 'Blood type (ABO)',
   'ABO rs8176746. Predicts A/B/AB/O',
   'variant_lookup', {'variants': [{'rs': 'rs8176746', 'gene': 'ABO', 'name': 'ABO blood type'}], 'disease': 'Blood type'})

_t('fun_pain', 'Fun Traits', 'Pain sensitivity',
   'SCN9A rs6746030. A = increased pain sensitivity',
   'variant_lookup', {'variants': [{'rs': 'rs6746030', 'gene': 'SCN9A', 'name': 'Pain'}], 'disease': 'Pain sensitivity'})

# ── Nutrigenomics ───────────────────────────────────────────────
_t('nutri_folate', 'Nutrigenomics', 'Folate metabolism (MTHFR)',
   'rs1801133. TT = needs methylfolate',
   'variant_lookup', {'variants': [{'rs': 'rs1801133', 'gene': 'MTHFR', 'name': 'MTHFR C677T'}], 'disease': 'Folate metabolism'})

_t('nutri_omega3', 'Nutrigenomics', 'Omega-3 conversion (FADS1/2)',
   'rs174546. Some need direct fish oil supplementation',
   'variant_lookup', {'variants': [{'rs': 'rs174546', 'gene': 'FADS1', 'name': 'Omega-3'}], 'disease': 'Omega-3 conversion'})

_t('nutri_vitd', 'Nutrigenomics', 'Vitamin D bioavailability (GC/VDBP)',
   'rs2282679. Affects vitamin D absorption',
   'variant_lookup', {'variants': [{'rs': 'rs2282679', 'gene': 'GC', 'name': 'Vitamin D'}], 'disease': 'Vitamin D metabolism'})

_t('nutri_salt', 'Nutrigenomics', 'Salt sensitivity (AGT)',
   'rs699. Stronger BP response to sodium',
   'variant_lookup', {'variants': [{'rs': 'rs699', 'gene': 'AGT', 'name': 'Salt sensitivity'}], 'disease': 'Salt sensitivity'})

_t('nutri_melatonin', 'Nutrigenomics', 'Melatonin/glucose (MTNR1B)',
   'rs10830963. Late eating worsens glucose',
   'variant_lookup', {'variants': [{'rs': 'rs10830963', 'gene': 'MTNR1B', 'name': 'Melatonin'}], 'disease': 'Melatonin-glucose interaction'})

_t('nutri_satfat', 'Nutrigenomics', 'Saturated fat response (APOA2)',
   'rs5082. CC = higher BMI with saturated fat intake',
   'variant_lookup', {'variants': [{'rs': 'rs5082', 'gene': 'APOA2', 'name': 'Sat fat'}], 'disease': 'Saturated fat response'})

# ── Sports & Fitness ────────────────────────────────────────────
_t('sport_ace', 'Sports & Fitness', 'Endurance capacity (ACE I/D)',
   'ACE I/D polymorphism. II = endurance; DD = power',
   'specialized', {'method': 'ace_id'})

_t('sport_tendon', 'Sports & Fitness', 'Tendon injury risk (COL5A1)',
   'rs12722. TT = higher tendon injury risk',
   'variant_lookup', {'variants': [{'rs': 'rs12722', 'gene': 'COL5A1', 'name': 'Tendon'}], 'disease': 'Tendon injury risk'})

_t('sport_recovery', 'Sports & Fitness', 'Recovery speed (IL6)',
   'rs1800795. G allele = longer recovery needed',
   'variant_lookup', {'variants': [{'rs': 'rs1800795', 'gene': 'IL6', 'name': 'Recovery'}], 'disease': 'Recovery speed'})

# ── Sleep & Circadian ───────────────────────────────────────────
_t('sleep_delayed', 'Sleep & Circadian', 'Delayed sleep phase (CRY1)',
   'rs184039278. Dominant late-sleeper (~1% EUR)',
   'variant_lookup', {'variants': [{'rs': 'rs184039278', 'gene': 'CRY1', 'name': 'Delayed sleep'}], 'disease': 'Delayed sleep phase'})

_t('sleep_deep', 'Sleep & Circadian', 'Deep sleep quality (ADA)',
   'rs73598374. A allele = deeper slow-wave sleep',
   'variant_lookup', {'variants': [{'rs': 'rs73598374', 'gene': 'ADA', 'name': 'Deep sleep'}], 'disease': 'Deep sleep quality'})

_t('sleep_caffeine', 'Sleep & Circadian', 'Caffeine + sleep (ADORA2A)',
   'rs5751876. TT = high caffeine sensitivity for sleep',
   'variant_lookup', {'variants': [{'rs': 'rs5751876', 'gene': 'ADORA2A', 'name': 'Caffeine-sleep'}], 'disease': 'Caffeine sleep sensitivity'})

# ── Pharmacogenomics ────────────────────────────────────────────
_t('pgx_cyp2d6', 'Pharmacogenomics', 'CYP2D6',
   'Star alleles *3, *4, *10, *17, *41 (SNP-based). *5 deletion and *2xN duplication require BAM for CNV analysis. Drugs: codeine, tramadol, tamoxifen, fluoxetine, paroxetine, atomoxetine, metoprolol, ondansetron. CPIC Level A',
   'variant_lookup', {'variants': [
       {'rs': 'rs35742686', 'gene': 'CYP2D6', 'name': '*3 (frameshift)',
        'expected_ref': 'GA', 'expected_alt': 'G', 'star_allele': '*3'},
       {'rs': 'rs3892097',  'gene': 'CYP2D6', 'name': '*4 (splicing)',
        'expected_ref': 'G', 'expected_alt': 'A', 'star_allele': '*4'},
       {'rs': 'rs1065852',  'gene': 'CYP2D6', 'name': '*10 (Pro34Ser)',
        'expected_ref': 'C', 'expected_alt': 'T', 'star_allele': '*10'},
       {'rs': 'rs28371706', 'gene': 'CYP2D6', 'name': '*17 (Thr107Ile)',
        'expected_ref': 'C', 'expected_alt': 'T', 'star_allele': '*17'},
       {'rs': 'rs28371725', 'gene': 'CYP2D6', 'name': '*41 (splicing)',
        'expected_ref': 'G', 'expected_alt': 'A', 'star_allele': '*41'},
   ], 'disease': 'CYP2D6 metabolizer status',
      'use_star_caller_for_bam': True,
      'star_caller': 'cyrius'})

_t('pgx_cyp2c19', 'Pharmacogenomics', 'CYP2C19',
   '*2 (rs4244285), *3 (rs4986893), *17 (rs12248560). Drugs: clopidogrel, omeprazole, pantoprazole, citalopram, escitalopram, sertraline, voriconazole, diazepam. CPIC Level A',
   'variant_lookup', {'variants': [
       {'rs': 'rs4244285', 'gene': 'CYP2C19', 'name': '*2',
        'expected_ref': 'G', 'expected_alt': 'A', 'star_allele': '*2'},
       {'rs': 'rs4986893', 'gene': 'CYP2C19', 'name': '*3',
        'expected_ref': 'G', 'expected_alt': 'A', 'star_allele': '*3'},
       {'rs': 'rs12248560', 'gene': 'CYP2C19', 'name': '*17',
        'expected_ref': 'C', 'expected_alt': 'T', 'star_allele': '*17'},
   ], 'disease': 'CYP2C19 metabolizer status'})

_t('pgx_cyp2c9', 'Pharmacogenomics', 'CYP2C9',
   '*2 (rs1799853), *3 (rs1057910). Drugs: warfarin, phenytoin, celecoxib, flurbiprofen, NSAIDs. CPIC Level A',
   'variant_lookup', {'variants': [
       {'rs': 'rs1799853', 'gene': 'CYP2C9', 'name': '*2',
        'expected_ref': 'C', 'expected_alt': 'T', 'star_allele': '*2'},
       {'rs': 'rs1057910', 'gene': 'CYP2C9', 'name': '*3',
        'expected_ref': 'A', 'expected_alt': 'C', 'star_allele': '*3'},
   ], 'disease': 'CYP2C9 metabolizer status'})

_t('pgx_vkorc1', 'Pharmacogenomics', 'VKORC1',
   'rs9923231. Drugs: warfarin. CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs9923231', 'gene': 'VKORC1', 'name': 'VKORC1'}], 'disease': 'Warfarin sensitivity'})

_t('pgx_dpyd', 'Pharmacogenomics', 'DPYD',
   '*2A (rs3918290), D949V (rs67376798), HapB3 (rs56038477), c.2846A>T (rs67376798). Drugs: 5-fluorouracil, capecitabine, tegafur. PM = potentially fatal toxicity. CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs3918290', 'gene': 'DPYD', 'name': '*2A'}, {'rs': 'rs67376798', 'gene': 'DPYD', 'name': 'D949V'}, {'rs': 'rs56038477', 'gene': 'DPYD', 'name': 'HapB3'}], 'disease': 'DPYD deficiency (fluoropyrimidine toxicity)'})

_t('pgx_tpmt', 'Pharmacogenomics', 'TPMT',
   '*2 (rs1800462), *3A (rs1800460+rs1142345), *3C (rs1142345). Drugs: azathioprine, 6-mercaptopurine, thioguanine. PM = myelosuppression. CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs1800460', 'gene': 'TPMT', 'name': '*3A-A'}, {'rs': 'rs1142345', 'gene': 'TPMT', 'name': '*3C'}], 'disease': 'TPMT metabolizer status'})

_t('pgx_nudt15', 'Pharmacogenomics', 'NUDT15',
   'R139C (rs116855232). Drugs: azathioprine, 6-mercaptopurine, thioguanine. PM = myelosuppression (most important in EAS). CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs116855232', 'gene': 'NUDT15', 'name': 'R139C'}], 'disease': 'NUDT15 metabolizer status'})

_t('pgx_slco1b1', 'Pharmacogenomics', 'SLCO1B1',
   '*5 (rs4149056). Drugs: simvastatin, atorvastatin, rosuvastatin, methotrexate. Poor function = 5-17x myopathy risk. CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs4149056', 'gene': 'SLCO1B1', 'name': '*5'}], 'disease': 'Statin myopathy risk'})

_t('pgx_hla_b5701', 'Pharmacogenomics', 'HLA-B*57:01',
   'rs2395029 (proxy). Drug: abacavir. Carrier = severe hypersensitivity, potentially fatal. CPIC Level A',
   'specialized', {'method': 'hla_typing', 'allele': 'HLA-B*57:01', 'disease': 'Abacavir hypersensitivity'})

_t('pgx_hla_b5801', 'Pharmacogenomics', 'HLA-B*58:01',
   'rs9263726 (proxy). Drug: allopurinol. Carrier = SJS/TEN/DRESS. High risk in Han Chinese (~6-8%). CPIC Level A',
   'specialized', {'method': 'hla_typing', 'allele': 'HLA-B*58:01', 'disease': 'Allopurinol SJS/TEN'})

_t('pgx_hla_b1502', 'Pharmacogenomics', 'HLA-B*15:02',
   'rs2844682 (proxy). Drugs: carbamazepine, oxcarbazepine. Carrier = SJS/TEN, life-threatening. SE Asian ~8%. CPIC Level A',
   'specialized', {'method': 'hla_typing', 'allele': 'HLA-B*15:02', 'disease': 'Carbamazepine SJS/TEN'})

_t('pgx_hla_a3101', 'Pharmacogenomics', 'HLA-A*31:01',
   'rs1633021 (proxy). Drug: carbamazepine. Carrier = DRESS. EUR ~5%. CPIC Level A',
   'specialized', {'method': 'hla_typing', 'allele': 'HLA-A*31:01', 'disease': 'Carbamazepine DRESS'})

_t('pgx_ugt1a1', 'Pharmacogenomics', 'UGT1A1',
   '*28 (rs8175347), *6 (rs4148323, EAS). Drugs: irinotecan, atazanavir. *28/*28 = severe neutropenia. CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs8175347', 'gene': 'UGT1A1', 'name': '*28'}, {'rs': 'rs4148323', 'gene': 'UGT1A1', 'name': '*6'}], 'disease': 'UGT1A1 irinotecan toxicity'})

_t('pgx_g6pd', 'Pharmacogenomics', 'G6PD',
   'Mediterranean (rs5030868), A- (rs1050828+rs1050829), Canton (rs72554665). Drugs: rasburicase, primaquine, dapsone, nitrofurantoin. Deficiency = hemolytic anemia. CPIC Level A',
   'variant_lookup', {'variants': [
       {'rs': 'rs5030868', 'gene': 'G6PD', 'name': 'Mediterranean (Ser188Phe)'},
       {'rs': 'rs1050828', 'gene': 'G6PD', 'name': 'A- (Val68Met)'},
       {'rs': 'rs1050829', 'gene': 'G6PD', 'name': 'A+ (Asn126Asp)'},
       {'rs': 'rs72554665', 'gene': 'G6PD', 'name': 'Canton (Arg459Leu)'},
   ], 'disease': 'G6PD deficiency'})

_t('pgx_mt_rnr1', 'Pharmacogenomics', 'MT-RNR1',
   'm.1555A>G (rs267606617), m.1494C>T (rs869025270). Drugs: aminoglycosides (gentamicin, tobramycin, amikacin). Variant = permanent deafness from single dose. Carrier freq ~1 in 500. CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs267606617', 'gene': 'MT-RNR1', 'name': 'm.1555A>G'}, {'rs': 'rs869025270', 'gene': 'MT-RNR1', 'name': 'm.1494C>T'}], 'disease': 'Aminoglycoside-induced hearing loss'})

_t('pgx_cyp3a5', 'Pharmacogenomics', 'CYP3A5',
   '*3 (rs776746). Drugs: tacrolimus, vincristine. Non-expresser (*3/*3) = higher tacrolimus levels; lower starting dose needed. ~80% EUR are non-expressers; ~70% AFR are expressers. CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs776746', 'gene': 'CYP3A5', 'name': '*3'}], 'disease': 'CYP3A5 tacrolimus dosing'})

_t('pgx_cyp2b6', 'Pharmacogenomics', 'CYP2B6',
   '*6 (rs2279343+rs3745274), *18 (rs28399499). Drugs: efavirenz, bupropion, methadone, ketamine. PM = efavirenz CNS toxicity. CPIC Level A',
   'variant_lookup', {'variants': [{'rs': 'rs2279343', 'gene': 'CYP2B6', 'name': '*6-A'}, {'rs': 'rs3745274', 'gene': 'CYP2B6', 'name': '*6-B'}, {'rs': 'rs28399499', 'gene': 'CYP2B6', 'name': '*18'}], 'disease': 'CYP2B6 metabolizer status'})

_t('pgx_nat2', 'Pharmacogenomics', 'NAT2',
   'rs1801280, rs1799929, rs1208, rs1799930. Drugs: isoniazid, hydralazine, procainamide. Slow acetylator = peripheral neuropathy with isoniazid. CPIC Level A',
   'variant_lookup', {'variants': [
       {'rs': 'rs1801280', 'gene': 'NAT2', 'name': '*5 (Ile114Thr)'},
       {'rs': 'rs1799929', 'gene': 'NAT2', 'name': '*6A (Arg197Gln)'},
       {'rs': 'rs1208', 'gene': 'NAT2', 'name': '*13 (Lys268Arg)'},
       {'rs': 'rs1799930', 'gene': 'NAT2', 'name': '*7 (Gly286Glu)'},
   ], 'disease': 'NAT2 acetylator status'})

_t('pgx_comt', 'Pharmacogenomics', 'COMT',
   'Val158Met (rs4680). Pain meds, dopaminergic drugs. CPIC Level B',
   'variant_lookup', {'variants': [{'rs': 'rs4680', 'gene': 'COMT', 'name': 'Val158Met'}], 'disease': 'COMT activity'})


# ── PGS - rsID Lists (from rsid-list.md) ───────────────────
try:
    from rsid_list_pgs import RSID_PGS as _RSID_PGS_LIST
except ImportError:
    _RSID_PGS_LIST = []

def _slug(s):
    import re as _re
    return _re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")

_seen_ids = set()
for _idx, _pgs in enumerate(_RSID_PGS_LIST):
    _title = _pgs["title"]
    _citation = _pgs["citation"]
    _author = _citation.split(",")[0].strip().lower() if _citation else f"v{_idx}"
    _id = f"rsid_{_slug(_title)}_{_slug(_author)}"
    _base = _id
    _suffix = 2
    while _id in _seen_ids:
        _id = f"{_base}_{_suffix}"
        _suffix += 1
    _seen_ids.add(_id)
    _name = f"{_title} ({_citation})" if _citation else _title
    _desc = f"{len(_pgs['variants'])} variants from {_citation or 'rsid-list.md'}. " \
            f"Score = sum(dosage × effect) across rsIDs."
    _t(_id, "PGS - rsID Lists", _name, _desc,
       "rsid_pgs_score", {"title": _title, "citation": _citation, "variants": _pgs["variants"]})

TESTS_BY_ID = {t["id"]: t for t in TESTS}
CATEGORIES = []
_seen = set()
for t in TESTS:
    if t["category"] not in _seen:
        CATEGORIES.append(t["category"])
        _seen.add(t["category"])

# ── Curated Short List ──────────────────────────────────────────
# Hand-picked subset of high-value tests shown in a dedicated tab.
CURATED_IDS = {
    # Cancer
    'pgs_breast_4153', 'pgs_prostate_662', 'pgs_colorectal_3979',
    'pgs_lung_078', 'pgs_pancreatic_794', 'pgs_melanoma_743',
    # Cardiovascular
    'pgs_coronary_5091', 'pgs_atrial_5168', 'pgs_hf_5097',
    'pgs_stroke_2724', 'pgs_vte_043', 'pgs_aortic_3429', 'pgs_coronary_2297',
    # Metabolic / Endocrine
    'pgs_t2d_2308', 'pgs_type_4874', 'pgs_ldl_2337',
    'pgs_bmi_5198', 'pgs_celiac_040', 'pgs_bmd_2632',
    # Autoimmune / Inflammatory
    'pgs_ibd_4151', 'pgs_multiple_2726', 'pgs_rheumatoid_4163', 'pgs_asthma_4877',
    # Neurological / Mental Health
    'pgs_alzheimers_334', 'pgs_parkinsons_2940', 'pgs_schiz_135',
    'pgs_depression_3333', 'pgs_adhd_2746', 'pgs_autism_327', 'pgs_addiction_3849',
    # Renal / Urinary
    'pgs_ckd_4889',
    # Eye / Vision
    'pgs_glaucoma_1797', 'pgs_amd_4606',
    # Cognitive & Educational
    'pgs_edu_2012', 'pgs_intelligence_3723',
    # Lifestyle / Behavioral
    'pgs_longevity_906',
    # Validation (one each)
    'sex_xy_ratio', 'ancestry_pca',
}
