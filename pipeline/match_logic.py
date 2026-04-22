"""Shared variant matching logic for PGS ingest and runtime scoring.

Both ingest_pgs and runners.py MUST use the same parse + format functions
so that variant IDs are guaranteed to match between the scoring file used
at ingest time (against the ref panel) and at runtime (against user VCFs).
"""
import gzip
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger("pgs-pipeline")


@dataclass
class ScoringVariant:
    """A single variant from a PGS Catalog scoring file."""
    chrom: str          # bare chromosome (no "chr" prefix), e.g. "1", "22", "X"
    pos: str            # 1-based position as string
    effect_allele: str  # allele whose dosage is multiplied by weight
    other_allele: str   # the other allele (may be empty)
    weight: str         # effect_weight as string (preserve precision)


def parse_pgs_scoring_file(path: str) -> Tuple[Dict, List[ScoringVariant]]:
    """Parse a PGS Catalog scoring file (gzipped or plain text).

    Returns (metadata_dict, list_of_ScoringVariant).

    metadata_dict contains all header comments (lines starting with #key=value)
    plus 'variant_count', 'used_harmonized_columns', and 'positions_build'.
    """
    metadata = {}
    col_names = None
    variants = []

    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt') as f:
        for line in f:
            if line.startswith('#'):
                if '=' in line:
                    key, _, val = line.lstrip('#').strip().partition('=')
                    metadata[key.strip()] = val.strip()
                continue

            parts = line.strip().split('\t')
            if col_names is None:
                col_names = parts
                continue

            try:
                chr_idx = col_names.index('hm_chr') if 'hm_chr' in col_names else col_names.index('chr_name')
                pos_idx = col_names.index('hm_pos') if 'hm_pos' in col_names else col_names.index('chr_position')
                ea_idx = col_names.index('effect_allele')
                weight_idx = col_names.index('effect_weight')
            except ValueError:
                continue

            chrom = parts[chr_idx] if chr_idx < len(parts) else ''
            pos = parts[pos_idx] if pos_idx < len(parts) else ''
            ea = parts[ea_idx] if ea_idx < len(parts) else ''
            weight = parts[weight_idx] if weight_idx < len(parts) else ''

            if not chrom or not pos or chrom == 'NA' or pos == 'NA':
                continue
            if not ea or not weight:
                continue

            # Normalize: strip "chr" prefix
            if chrom.startswith('chr'):
                chrom = chrom[3:]

            # Get other allele if available
            oa = ''
            for oa_col in ('other_allele', 'hm_inferOtherAllele'):
                if oa_col in col_names:
                    oa_idx = col_names.index(oa_col)
                    if oa_idx < len(parts):
                        oa = parts[oa_idx].strip()
                        if oa and oa != 'NA':
                            break
                        oa = ''

            try:
                float(weight)
            except ValueError:
                continue

            variants.append(ScoringVariant(
                chrom=chrom, pos=pos,
                effect_allele=ea, other_allele=oa,
                weight=weight,
            ))

    # Determine which coordinate columns were used
    used_hm = col_names is not None and 'hm_chr' in col_names and 'hm_pos' in col_names
    metadata['used_harmonized_columns'] = used_hm
    if used_hm:
        metadata['positions_build'] = metadata.get('HmPOS_build', 'GRCh38')
    else:
        metadata['positions_build'] = metadata.get('genome_build', 'unknown')
    metadata['variant_count'] = len(variants)

    return metadata, variants


def to_user_plink2_format(variants: List[ScoringVariant], output_path: str,
                          chr_prefix: bool = True) -> int:
    """Write a plink2 --score file for scoring user VCFs.

    IDs use chr{N}:{pos} format (matching the pgen built from user VCFs).
    Deduplicates by (ID, allele).

    Returns the number of unique variants written.
    """
    seen = set()
    lines = []
    for v in variants:
        chrom = f"chr{v.chrom}" if chr_prefix else v.chrom
        var_id = f"{chrom}:{v.pos}"
        key = (var_id, v.effect_allele)
        if key in seen:
            continue
        seen.add(key)
        lines.append(f"{var_id}\t{v.effect_allele}\t{v.weight}")

    with open(output_path, 'w') as f:
        f.write("ID\tA1\tWEIGHT\n")
        for line in lines:
            f.write(line + "\n")

    return len(lines)


def to_refpanel_plink2_format(variants: List[ScoringVariant], output_path: str) -> int:
    """Write a plink2 --score file for scoring the 1000G reference panel.

    IDs use {N}:{pos}:{ref}:{alt} format (matching the ref panel pvar which
    uses bare chromosomes with ref:alt allele IDs). Both allele orientations
    are emitted so that whichever matches the pvar will hit.

    Returns the number of unique variants written.
    """
    seen = set()
    lines = []
    for v in variants:
        ea = v.effect_allele
        oa = v.other_allele
        if oa:
            # Emit both orientations: oa:ea and ea:oa
            id1 = f"{v.chrom}:{v.pos}:{oa}:{ea}"
            id2 = f"{v.chrom}:{v.pos}:{ea}:{oa}"
            key1 = (id1, ea)
            key2 = (id2, ea)
            if key1 not in seen:
                seen.add(key1)
                lines.append(f"{id1}\t{ea}\t{v.weight}")
            if key2 not in seen:
                seen.add(key2)
                lines.append(f"{id2}\t{ea}\t{v.weight}")
        else:
            # No other allele — use N placeholder
            var_id = f"{v.chrom}:{v.pos}:N:{ea}"
            key = (var_id, ea)
            if key not in seen:
                seen.add(key)
                lines.append(f"{var_id}\t{ea}\t{v.weight}")

    with open(output_path, 'w') as f:
        f.write("ID\tA1\tWEIGHT\n")
        for line in lines:
            f.write(line + "\n")

    return len(lines)
