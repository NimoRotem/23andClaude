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
    "rs80357713": ("17", 43124027, "CT", "C"),   # BRCA1 185delAG

    # Metabolic
    "rs1801133":  ("1",  11796321, "G", "A"),    # MTHFR C677T
    "rs28929474": ("14", 94378610, "C", "T"),    # SERPINA1 Z
    "rs9939609":  ("16", 53786615, "T", "A"),    # FTO
    "rs7903146":  ("10", 112998590, "C", "T"),   # TCF7L2
    "rs34637584": ("12", 40340400, "G", "A"),    # LRRK2 G2019S

    # Carrier recessive
    "rs113993960": ("7",  117559593, "ATCT", "A"), # CFTR F508del
    "rs334":       ("11", 5227002,   "T", "A"),    # HBB Glu6Val (sickle)
    "rs76763715":  ("1",  155235878, "T", "C"),    # GBA1 N370S
    "rs5030858":   ("12", 102852858, "G", "A"),    # PAH R408W
    "rs11549407":  ("11", 5226925,   "G", "A"),    # HBB Codon39 beta-thal
    "rs1800562":   ("6",  26092913,  "G", "A"),    # HFE C282Y
    "rs387906309": ("15", 72349074,  "G", "GTATC"), # HEXA 1278insTATC
    "rs386834236": ("17", 80104568,  "T", "G"),    # GAA c.-32-13T>G (Pompe)

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
    "rs713598":   ("7",  141973545, "G", "C"),   # TAS2R38 PTC
    "rs72921001": ("11", 6925303,   "A", "C"),   # OR6A2 cilantro
    "rs17822931": ("16", 48224287,  "C", "T"),   # ABCC11 earwax
    "rs4988235":  ("2",  135851076, "G", "A"),   # MCM6 lactase
    "rs671":      ("12", 111803962, "G", "A"),   # ALDH2 alcohol flush
    "rs762551":   ("15", 74749576,  "C", "A"),   # CYP1A2 caffeine
    "rs1815739":  ("11", 66560624,  "C", "T"),   # ACTN3 sprint/endurance
    "rs12913832": ("15", 28120472,  "A", "G"),   # HERC2 eye color
    "rs10427255": ("2",  145156822, "T", "C"),   # ZEB2 photic sneeze
    "rs601338":   ("19", 48703417,  "G", "A"),   # FUT2 norovirus
    "rs8176746":  ("9",  133255928, "C", "A"),   # ABO blood type
    "rs6746030":  ("2",  166199346, "G", "A"),   # SCN9A pain

    # Nutrigenomics
    "rs174546":   ("11", 61802358,  "C", "T"),   # FADS1 omega-3
    "rs2282679":  ("4",  72618323,  "T", "G"),   # GC vitamin D
    "rs699":      ("1",  230710048, "A", "G"),   # AGT salt sensitivity
    "rs10830963": ("11", 92975544,  "C", "G"),   # MTNR1B melatonin
    "rs5082":     ("1",  161193633, "C", "T"),   # APOA2 sat fat

    # Sports & Sleep
    "rs12722":      ("9",  134854707, "C", "T"),  # COL5A1 tendon
    "rs1800795":    ("7",  22727026,  "G", "C"),  # IL6 recovery
    "rs184039278":  ("12", 106991359, "G", "A"),  # CRY1 delayed sleep
    "rs73598374":   ("20", 44651586,  "G", "A"),  # ADA deep sleep
    "rs5751876":    ("22", 24423941,  "T", "C"),  # ADORA2A caffeine-sleep
}
