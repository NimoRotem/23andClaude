#!/usr/bin/env python
"""Build GRCh38 haplogroup SNP lookup tables for Y-DNA and mtDNA.

Y-DNA: lifts yhaplo's ISOGG 2016 SNP set (GRCh37) to GRCh38.
mtDNA: uses PhyloTree Build 17 major haplogroup markers (rCRS positions,
       same in hg19/hg38 since mito is identical).

Outputs two JSON files in /data/haplogroup_data/:
  - ydna_snps_grch38.json: [{"pos": int, "ref": str, "alt": str, "haplogroup": str, "name": str}, ...]
  - mtdna_snps.json: [{"pos": int, "ref": str, "alt": str, "haplogroup": str}, ...]
"""
import json
import os
from pathlib import Path

OUT_DIR = Path("/data/haplogroup_data")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def build_ydna():
    from pyliftover import LiftOver
    lo = LiftOver('hg19', 'hg38')

    isogg_file = "/home/nimrod_rotem/conda/envs/genomics/lib/python3.10/site-packages/yhaplo/data/variants/isogg.2016.01.04.txt"

    snps = []
    skipped = 0
    with open(isogg_file) as f:
        next(f)  # header
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 6:
                skipped += 1
                continue
            name = parts[0].strip()
            hg = parts[1].strip()
            pos_str = parts[4].strip()
            mut = parts[5].strip()
            if not pos_str.isdigit() or "->" not in mut:
                skipped += 1
                continue
            ref, _, alt = mut.partition("->")
            ref = ref.strip()
            alt = alt.strip()
            if len(ref) != 1 or len(alt) != 1:
                skipped += 1
                continue
            # Strip " (Notes)" suffix and other annotations on haplogroup
            hg = hg.split(" ")[0]
            # Normalize notes — sometimes the haplogroup field starts with
            # "(" indicating a note. Skip those.
            if hg.startswith("(") or not hg:
                skipped += 1
                continue

            # Liftover from GRCh37 → GRCh38 (0-based internally)
            lifted = lo.convert_coordinate("chrY", int(pos_str) - 1)
            if not lifted:
                skipped += 1
                continue
            g38 = lifted[0][1] + 1  # back to 1-based

            snps.append({
                "pos": g38,
                "ref": ref,
                "alt": alt,
                "haplogroup": hg,
                "name": name,
            })

    out = OUT_DIR / "ydna_snps_grch38.json"
    with open(out, "w") as f:
        json.dump(snps, f)
    print(f"Y-DNA: wrote {len(snps)} SNPs (skipped {skipped}) → {out}")


def build_mtdna():
    """Build mtDNA haplogroup marker list from PhyloTree Build 17 major branches.

    Positions are rCRS (NC_012920.1) 1-based, same as chrM in GRCh37/GRCh38.
    Only major/common branches — not a full tree. Good enough for top-level call.
    """
    # Each entry: (position, alt_allele_vs_rCRS, haplogroup_assigned_by_this_mutation)
    # From PhyloTree Build 17 (2016) major branch markers. Alleles are the
    # state OBSERVED IN the haplogroup, which — because rCRS is H2a2a1 — is
    # often the derived state when comparing to rCRS. Non-H lineages in
    # particular carry many "reversions" at positions that look like
    # derived calls in a VCF aligned to GRCh38/rCRS.
    markers = [
        # ─── L (deep African macrohaplogroup) ─────────────────────
        # 16223 C>T is the canonical L3/L2/N/M defining transition —
        # present in essentially all non-H2a2a1 lineages. Weighted
        # heavily because it's the broadest African signal.
        (16223, "T", "L"),
        (146,   "C", "L"),
        (182,   "T", "L1"),
        (247,   "A", "L1b"),
        (769,   "A", "L1"),
        (825,   "A", "L0"),
        (1018,  "A", "L0"),
        (2758,  "A", "L0"),
        (2885,  "C", "L1"),
        (7256,  "T", "L2"),
        (8655,  "T", "L2"),
        (10115, "C", "L2"),
        (12693, "A", "L2"),
        (13789, "C", "L2"),
        (15784, "C", "L2"),
        (16278, "T", "L2"),
        (16390, "A", "L2"),
        (2352,  "C", "L3"),
        (3594,  "T", "L3"),
        (4104,  "G", "L3"),
        (4312,  "T", "L3"),
        (8618,  "C", "L3"),
        (9540,  "C", "L3"),
        (10398, "G", "L3"),
        (15301, "A", "L3"),
        # ─── Out-of-Africa backbone ───────────────────────────────
        # These positions are shared across many non-L lineages — keep
        # them as specific single-letter labels so they don't clobber
        # more informative L hits via family-roll-up.
        (10873, "C", "M"),
        (10400, "T", "M"),
        (10398, "G", "N"),  # (also in J/I — recurrent)
        (12705, "T", "R"),
        # ─── H / HV (European common) ─────────────────────────────
        # rCRS IS H2a2a1 — so "H" is recognized by NOT having any
        # derived markers (near-identity to rCRS). Positions 2706 A>G
        # and 14766 C>T are shared by essentially *all* non-H lineages
        # (including all L lineages), so they're useless as HV/H
        # signals and are intentionally NOT listed here — otherwise
        # they'd swamp the true family signal at 16223 T (L).
        # ─── V ────────────────────────────────────────────────────
        (4580, "A", "V"),
        (15904, "T", "V"),
        # ─── J ────────────────────────────────────────────────────
        (12612, "G", "J"),
        (13708, "A", "J"),
        (295,   "T", "J"),
        # ─── T ────────────────────────────────────────────────────
        (13368, "A", "T"),
        (14905, "A", "T"),
        (15607, "G", "T"),
        (15928, "A", "T"),
        # ─── K / U ────────────────────────────────────────────────
        (10550, "G", "K"),
        (11299, "C", "K"),
        (14798, "C", "K"),
        (11467, "G", "U"),
        (12308, "G", "U"),
        (12372, "A", "U"),
        # ─── W ────────────────────────────────────────────────────
        (8251, "A", "W"),
        (8994, "A", "W"),
        (11947, "G", "W"),
        # ─── X ────────────────────────────────────────────────────
        (6371, "T", "X"),
        # ─── I / N1 ───────────────────────────────────────────────
        (10034, "C", "I"),
        # ─── D / A / B / C / G / F (Asian / NA) ──────────────────
        (4883, "T", "D"),
        (5178, "A", "D"),
        (4824, "G", "A"),
        (8794, "T", "A"),
        (16290, "T", "A"),
        (16189, "C", "B"),
        (3552, "A", "C"),
        (13263, "G", "C"),
        (709, "A", "G"),
        (4833, "G", "G"),
        (3970, "T", "F"),
        (10310, "A", "F"),
        (12406, "A", "F"),
    ]

    # Filter out indels and normalize
    snps = []
    for pos, alt, hg in markers:
        if alt == "-" or len(alt) != 1:
            continue
        snps.append({
            "pos": pos,
            "alt": alt,
            "haplogroup": hg,
        })

    out = OUT_DIR / "mtdna_snps.json"
    with open(out, "w") as f:
        json.dump(snps, f)
    print(f"mtDNA: wrote {len(snps)} markers → {out}")


def build_neanderthal():
    """Neanderthal-informative SNVs.

    Curated subset of common Neanderthal-tagging SNVs. These are sites where
    modern humans typically have the ancestral allele, but Neanderthal-admixed
    samples carry the derived (Neanderthal) allele at elevated frequency.

    Simple scoring: count the fraction of these sites where the sample carries
    the Neanderthal allele, compared to an expected baseline (~2% for non-
    Africans, ~0% for Africans).

    Source: a small curated set from Sankararaman et al. 2014 and Vernot & Akey
    2016 high-confidence Neanderthal introgressed haplotypes.
    """
    # Each entry: (chrom, grch38_pos, ref, neanderthal_allele)
    # These are well-known tag SNVs from published Neanderthal introgression
    # studies. Small set for a lightweight estimate, not a rigorous s_stat.
    markers = [
        # BNC2 (skin)
        ("chr9", 16409501, "C", "T"),
        # POU2F3 (skin/keratin)
        ("chr11", 120315373, "G", "A"),
        # HYAL2 (immune)
        ("chr3", 50319389, "T", "C"),
        # HLA-DPB1 region
        ("chr6", 33071555, "A", "G"),
        # STAT2 (immune)
        ("chr12", 56738814, "G", "A"),
        # FOXP2 (language-related)
        ("chr7", 114654925, "A", "G"),
        # SLC16A11 (metabolism, Mexican-enriched)
        ("chr17", 6878655, "T", "C"),
        ("chr17", 6879190, "G", "C"),
        # BRCA2 region
        ("chr13", 32912813, "C", "G"),
        # KRT77 (hair keratin)
        ("chr12", 52813570, "G", "T"),
        # SPAG17
        ("chr1", 118594854, "C", "T"),
        # KEAP1
        ("chr19", 10491135, "G", "A"),
        # Chr4 introgressed haplotype
        ("chr4", 33977175, "A", "G"),
        # Chr18 introgressed
        ("chr18", 34877650, "T", "C"),
        # LOC401336
        ("chr8", 6327185, "C", "T"),
        # OAS cluster (immune)
        ("chr12", 112913362, "T", "C"),
        # IL18RAP
        ("chr2", 102412147, "G", "A"),
        # TLR6
        ("chr4", 38822136, "A", "G"),
        # BTNL2
        ("chr6", 32362488, "C", "T"),
        # HLA-A
        ("chr6", 29942437, "G", "A"),
    ]

    snps = [
        {"chrom": c, "pos": p, "ref": r, "neanderthal_allele": n}
        for c, p, r, n in markers
    ]

    out = OUT_DIR / "neanderthal_snps_grch38.json"
    with open(out, "w") as f:
        json.dump(snps, f)
    print(f"Neanderthal: wrote {len(snps)} tag SNVs → {out}")


if __name__ == "__main__":
    build_ydna()
    build_mtdna()
    build_neanderthal()
