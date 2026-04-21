#!/usr/bin/env python
"""Rewrite ALT='.' in a hom-ref VCF to carry the PGS effect allele.

plink2 --score can't match a PGS effect allele against records where ALT is
missing ('.'), which is what bcftools call -m (no -v) emits for hom-ref sites.
By plugging in the PGS-expected ALT at each position (taken from the union of
all PGS Catalog scoring files in /data/pgs_cache), we restore plink2's ability
to apply 0-dose scoring at those positions — lifting PGS match rates from
~50% (variant-only) to ~99% (all-sites).

Usage:
  fix_pgs_sites_alt.py <input_vcf_gz> <output_vcf_gz>
"""
import gzip
import os
import subprocess
import sys
from pathlib import Path

BCFTOOLS = "/home/nimrod_rotem/conda/envs/genomics/bin/bcftools"
PGS_CACHE = "/data/pgs_cache"


def build_allele_map():
    """Build a dict {(chrom, pos): set(alleles)} from all cached PGS files.

    Tracks every base observed at each position across both effect_allele and
    (hm_inferOtherAllele | other_allele) columns. At rewrite time we'll pick
    whichever observed allele ≠ REF as the ALT. Keeping a set handles the
    rare tri-allelic case and files that disagree between sources.
    """
    lookup = {}
    pgs_dir = Path(PGS_CACHE)
    for pgs_subdir in pgs_dir.glob("PGS*"):
        for f in pgs_subdir.glob("*_hmPOS_GRCh38.txt.gz"):
            with gzip.open(f, "rt") as fh:
                header = None
                for line in fh:
                    if line.startswith("#"):
                        continue
                    parts = line.rstrip("\n").split("\t")
                    if header is None:
                        header = parts
                        try:
                            chr_idx = header.index("hm_chr") if "hm_chr" in header else header.index("chr_name")
                            pos_idx = header.index("hm_pos") if "hm_pos" in header else header.index("chr_position")
                            ea_idx = header.index("effect_allele")
                        except ValueError:
                            break
                        # Optional "other" allele columns — the preferred
                        # source is hm_inferOtherAllele (harmonized), then
                        # other_allele (as-reported).
                        oa_idx = None
                        for col in ("hm_inferOtherAllele", "other_allele"):
                            if col in header:
                                oa_idx = header.index(col)
                                break
                        continue
                    if len(parts) <= max(chr_idx, pos_idx, ea_idx):
                        continue
                    chrom = parts[chr_idx]
                    pos = parts[pos_idx]
                    ea = parts[ea_idx].strip() if parts[ea_idx] else ""
                    oa = parts[oa_idx].strip() if oa_idx is not None and oa_idx < len(parts) and parts[oa_idx] else ""
                    if not chrom or not pos or chrom == "NA" or not pos.isdigit():
                        continue
                    key = (f"chr{chrom}" if not chrom.startswith("chr") else chrom,
                           int(pos))
                    alleles = lookup.setdefault(key, set())
                    # Only keep single-base alleles (SNVs) — indels need more
                    # careful handling and are rare in PGS scoring files.
                    if ea and len(ea) == 1 and ea in "ACGT":
                        alleles.add(ea)
                    if oa and len(oa) == 1 and oa in "ACGT":
                        alleles.add(oa)
    return lookup


def rewrite_vcf(input_vcf, output_vcf, effect_map):
    """Stream input VCF through bcftools view -h + data, rewriting ALT='.'
    records to carry effect_map[(chrom, pos)] as ALT, and leaving GT 0/0.
    """
    # Pipe through bcftools view to get plain text, then write via bcftools view -O z.
    reader = subprocess.Popen(
        [BCFTOOLS, "view", input_vcf],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    writer = subprocess.Popen(
        [BCFTOOLS, "view", "-Oz", "-o", output_vcf, "-"],
        stdin=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )

    n_total = 0
    n_rewritten = 0
    n_dropped = 0

    for line in reader.stdout:
        if line.startswith("#"):
            writer.stdin.write(line)
            continue
        n_total += 1

        # Split and inspect ALT column (index 4). If ALT is '.', look up an
        # effect allele and substitute; otherwise pass through unchanged.
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 5:
            writer.stdin.write(line)
            continue

        chrom, pos, _id, ref, alt = parts[0], parts[1], parts[2], parts[3], parts[4]

        if alt != ".":
            writer.stdin.write(line)
            continue

        alleles = effect_map.get((chrom, int(pos)))
        if not alleles or len(ref) != 1:
            n_dropped += 1
            continue
        # Pick any allele that's not REF. For a bi-allelic site this is
        # uniquely determined; for tri-allelic we just take the first.
        non_ref = [a for a in alleles if a != ref]
        if not non_ref:
            # All PGS alleles equal REF — pick a dummy ALT. plink2 will still
            # score hom-ref positions when A1 == REF as long as the record
            # has a valid ALT field. Using N is generally accepted.
            alt_allele = next(iter(alleles))
            # If the only allele equals REF, pick any distinct base:
            if alt_allele == ref:
                alt_allele = "N" if ref != "N" else "A"
        else:
            alt_allele = non_ref[0]

        parts[4] = alt_allele
        n_rewritten += 1
        writer.stdin.write("\t".join(parts) + "\n")

    writer.stdin.close()
    writer.wait()
    reader.wait()

    print(f"total records:  {n_total}")
    print(f"rewritten ALT:  {n_rewritten}")
    print(f"dropped (no match): {n_dropped}")
    print(f"output: {output_vcf}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: fix_pgs_sites_alt.py <input_vcf_gz> <output_vcf_gz>", file=sys.stderr)
        sys.exit(1)

    input_vcf, output_vcf = sys.argv[1], sys.argv[2]
    print("Building PGS allele lookup from /data/pgs_cache/...")
    allele_map = build_allele_map()
    print(f"Loaded {len(allele_map):,} (chr, pos) → allele_set entries")
    rewrite_vcf(input_vcf, output_vcf, allele_map)

    print("Indexing output...")
    subprocess.run([BCFTOOLS, "index", "-t", output_vcf], check=True)
    print("Done.")
