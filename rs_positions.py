"""
Curated GRCh38 positions for rsIDs referenced in the test registry.

Each entry maps rsID → (chrom, pos, ref, alt).
Positions verified against dbSNP b156 / GRCh38.

When a VCF lacks rsID annotations, runners fall back to position-based lookup.
"""

RS_POSITIONS = {
    # APOE (Alzheimer's)
    "rs429358": ("19", 44908684, "T", "C"),   # APOE e4
    "rs7412":   ("19", 44908822, "C", "T"),   # APOE e2

    # Cardiovascular / Thrombosis
    "rs6025":     ("1",  169549811, "C", "T"),   # Factor V Leiden
    "rs1799963":  ("11", 46761055,  "G", "A"),   # Prothrombin G20210A
    "rs11591147": ("1",  55039974,  "G", "T"),   # PCSK9 R46L
    "rs10757278": ("9",  22124478,  "A", "G"),   # 9p21 CDKN2A/B
    "rs10455872": ("6",  160589086, "A", "G"),   # LPA Lp(a)

    # Cancer
    "rs80357713": ("17", 43124031, "T",  "TT"),  # BRCA1 5382insC (c.5266dupC) [verified GRCh38]

    # Metabolic
    "rs1801133":  ("1",  11796321, "G", "A"),    # MTHFR C677T [verified: FASTA=REF, Nimo=hom-alt]
    "rs28929474": ("14", 94378610, "C", "T"),    # SERPINA1 Z
    "rs9939609":  ("16", 53786615, "T", "A"),    # FTO
    "rs7903146":  ("10", 112998590, "C", "T"),   # TCF7L2
    "rs34637584": ("12", 40340400, "G", "A"),    # LRRK2 G2019S

    # Carrier recessive
    "rs113993960": ("7",  117559593, "ATCT", "A"), # CFTR F508del
    "rs334":       ("11", 5227002,   "T", "A"),    # HBB Glu6Val (sickle)
    "rs76763715":  ("1",  155235843, "T", "C"),    # GBA1 N370S [verified GRCh38]
    "rs5030858":   ("12", 102852858, "G", "A"),    # PAH R408W
    "rs11549407":  ("11", 5226925,   "G", "A"),    # HBB Codon39 beta-thal
    "rs1800562":   ("6",  26092913,  "G", "A"),    # HFE C282Y
    "rs387906309": ("15", 72349074,  "G", "GTATC"), # HEXA 1278insTATC
    "rs386834236": ("17", 80104542,  "T", "G"),    # GAA c.-32-13T>G (Pompe) [verified GRCh38 from dbSNP]

    # Pharmacogenomics
    "rs4244285":  ("10", 94781859, "G", "A"),    # CYP2C19 *2
    "rs4986893":  ("10", 94780653, "G", "A"),    # CYP2C19 *3
    "rs12248560": ("10", 94761900, "C", "T"),    # CYP2C19 *17
    "rs1799853":  ("10", 94942290, "C", "T"),    # CYP2C9 *2
    "rs1057910":  ("10", 94981296, "A", "C"),    # CYP2C9 *3
    "rs9923231":  ("16", 31096368, "C", "T"),    # VKORC1
    "rs3918290":  ("1",  97450058, "C", "T"),    # DPYD *2A
    "rs1800460":  ("6",  18130918, "C", "T"),    # TPMT *3A
    "rs1142345":  ("6",  18130687, "T", "C"),    # TPMT *3C
    "rs4149056":  ("12", 21178615, "T", "C"),    # SLCO1B1 *5
    "rs4680":     ("22", 19963748, "G", "A"),    # COMT Val158Met

    # Fun traits
    "rs713598":   ("7",  141973545, "C", "G"),   # TAS2R38 PTC [REF/ALT swapped to match GRCh38]
    "rs72921001": ("11", 6925303,   "A", "C"),   # OR6A2 cilantro
    "rs17822931": ("16", 48224287,  "C", "T"),   # ABCC11 earwax
    "rs4988235":  ("2",  135851076, "G", "A"),   # MCM6 lactase
    "rs671":      ("12", 111803962, "G", "A"),   # ALDH2 alcohol flush
    "rs762551":   ("15", 74749576,  "C", "A"),   # CYP1A2 caffeine [verified: FASTA=REF, Nimo=hom-alt]
    "rs1815739":  ("11", 66560624,  "C", "T"),   # ACTN3 sprint/endurance
    "rs12913832": ("15", 28120472,  "A", "G"),   # HERC2 eye color
    "rs10427255": ("2",  145156822, "T", "C"),   # ZEB2 photic sneeze
    "rs601338":   ("19", 48703417,  "G", "A"),   # FUT2 norovirus
    "rs8176746":  ("9",  133255928, "C", "A"),   # ABO blood type
    "rs6746030":  ("2",  166199346, "G", "A"),   # SCN9A pain

    # Nutrigenomics
    "rs174546":   ("11", 61802358,  "C", "T"),   # FADS1 omega-3
    "rs2282679":  ("4",  72618323,  "G", "T"),   # GC vitamin D [REF/ALT swapped to match GRCh38]
    "rs699":      ("1",  230710048, "A", "G"),   # AGT salt sensitivity
    "rs10830963": ("11", 92975544,  "C", "G"),   # MTNR1B melatonin
    "rs5082":     ("1",  161193633, "T", "C"),   # APOA2 sat fat [REF/ALT swapped to match GRCh38]

    # Sports & Sleep
    "rs12722":      ("9",  134854707, "T", "C"),  # COL5A1 tendon [REF/ALT swapped to match GRCh38]
    "rs1800795":    ("7",  22727026,  "C", "G"),  # IL6 recovery [REF/ALT swapped to match GRCh38]
    "rs184039278":  ("12", 106991359, "G", "A"),  # CRY1 delayed sleep
    "rs73598374":   ("20", 44651586,  "G", "A"),  # ADA deep sleep
    "rs5751876":    ("22", 24423941,  "C", "T"),  # ADORA2A caffeine-sleep [REF/ALT swapped to match GRCh38]

    # Carrier — missing positions
    "rs77931234":  ("1",  75761161,  "A", "G"),    # ACADM K329E (c.985A>G) [verified GRCh38 from dbSNP]

    # Pharmacogenomics — missing entries
    "rs116855232": ("13", 48045719,  "C", "T"),    # NUDT15 R139C [verified GRCh38]
    "rs67376798":  ("1",  97515839,  "T", "A"),    # DPYD D949V
    "rs56038477":  ("1",  97547947,  "C", "T"),    # DPYD HapB3

    # CYP2D6 SNP star alleles
    "rs35742686":  ("22", 42128945, "GA", "G"),    # CYP2D6 *3
    "rs3892097":   ("22", 42130692, "G", "A"),    # CYP2D6 *4 [verified: FASTA=REF, Nimo=hom-alt]
    "rs1065852":   ("22", 42126611, "C",  "T"),    # CYP2D6 *10
    "rs28371706":  ("22", 42126938, "C",  "T"),    # CYP2D6 *17
    "rs28371725":  ("22", 42127941, "G",  "A"),    # CYP2D6 *41

    # HLA proxy SNPs
    "rs2395029":   ("6",  31464003, "T", "G"),     # HLA-B*57:01 proxy [verified GRCh38]
    "rs9263726":   ("6",  31357516, "C", "T"),     # HLA-B*58:01 proxy [REF/ALT swapped to match GRCh38]
    "rs2844682":   ("6",  31323274, "A", "G"),     # HLA-B*15:02 proxy
    "rs1633021":   ("6",  29942854, "C", "T"),     # HLA-A*31:01 proxy [REF/ALT swapped to match GRCh38]

    # G6PD (X-linked)
    "rs5030868":  ("X", 154534419, "G", "A"),    # G6PD Mediterranean (Ser188Phe)
    "rs1050828":  ("X", 154536002, "C", "T"),    # G6PD A- (Val68Met)
    "rs1050829":  ("X", 154535277, "T", "C"),    # G6PD A+ (Asn126Asp)
    "rs72554665": ("X", 154532269, "C", "A"),    # G6PD Canton (Arg459Leu)

    # NAT2 (chr8)
    "rs1801280":  ("8", 18400344, "T", "C"),     # NAT2 *5 (Ile114Thr)
    "rs1799929":  ("8", 18400484, "C", "T"),     # NAT2 *6A (Arg197Gln)
    "rs1208":     ("8", 18400806, "G", "A"),     # NAT2 *13 (Lys268Arg) [verified: FASTA=REF, Nimo=hom-alt]
    "rs1799930":  ("8", 18400593, "G", "A"),     # NAT2 *7 (Gly286Glu) [verified: FASTA=REF, Nimo=hom-alt]

    # UGT1A1 (chr2)
    "rs8175347":  ("2", 233760233, "C", "CA"),   # UGT1A1 *28 (TA repeat, simplified)
    "rs4148323":  ("2", 233760498, "G", "A"),    # UGT1A1 *6 (Gly71Arg)

    # MT-RNR1 (mitochondrial)
    "rs267606617": ("MT", 1555, "A", "G"),       # m.1555A>G (aminoglycoside deafness)
    "rs869025270": ("MT", 1494, "C", "T"),       # m.1494C>T (alt rsID: rs267606619)

    # CYP3A5 (chr7)
    "rs776746":   ("7", 99672916, "T", "C"),     # CYP3A5 *3 (splice) [verified: FASTA=REF, Nimo=hom-alt]

    # CYP2B6 (chr19)
    "rs2279343":  ("19", 41009358, "A", "G"),    # CYP2B6 *6-A (Lys262Arg)
    "rs3745274":  ("19", 41006936, "G", "T"),    # CYP2B6 *6-B (Gln172His)
    "rs28399499": ("19", 41012316, "T", "C"),    # CYP2B6 *18 (Ile328Thr)
}
