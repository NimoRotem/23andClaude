"""Convert DeepVariant gVCF to plink2 binary format for fast PGS scoring."""

import subprocess
import os
import logging

from ..config import BCFTOOLS, PLINK2

logger = logging.getLogger(__name__)


def gvcf_to_pgen(gvcf_path: str, output_prefix: str, ref_fasta: str = None) -> dict:
    """
    Convert a gVCF file to plink2 pgen/pvar/psam format.

    plink2 can score a 1M-variant PGS against a pgen file in ~5 seconds,
    vs ~30 minutes in the current Python engine.

    Args:
        gvcf_path: Path to input .g.vcf.gz file
        output_prefix: Output path prefix (will create .pgen, .pvar.zst, .psam)
        ref_fasta: Optional reference FASTA for resolving REF alleles in blocks

    Returns:
        dict with paths to output files and variant count
    """
    output_dir = os.path.dirname(output_prefix)
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Normalize the gVCF - remove block records
    normalized_vcf = f"{output_prefix}.norm.vcf.gz"

    norm_cmd = [
        BCFTOOLS, "view",
        "--exclude", 'N_ALT=1 && (ALT="<NON_REF>" || ALT="<*>")',
        "-Oz", "-o", normalized_vcf,
        gvcf_path
    ]
    logger.info(f"Normalizing gVCF: {' '.join(norm_cmd)}")
    subprocess.run(norm_cmd, check=True, capture_output=True, text=True)

    # Index the normalized VCF
    subprocess.run([BCFTOOLS, "index", "-t", normalized_vcf], check=True)

    # Step 2: Convert to plink2 format (use all available threads)
    from ..config import CPU_COUNT
    plink_cmd = [
        PLINK2,
        "--vcf", normalized_vcf,
        "--make-pgen", "vzs",
        "--out", output_prefix,
        "--vcf-half-call", "m",
        "--allow-extra-chr",
        "--autosome",
        "--set-all-var-ids", "chr@:#",
        "--rm-dup", "force-first",
        "--threads", str(min(CPU_COUNT, 16)),
    ]
    logger.info(f"Converting to pgen: {' '.join(plink_cmd)}")
    subprocess.run(plink_cmd, check=True, capture_output=True, text=True)

    # Count variants
    var_count = 0
    pvar_path = f"{output_prefix}.pvar.zst"
    if not os.path.exists(pvar_path):
        pvar_path = f"{output_prefix}.pvar"

    # Use vzs flag since we created .pvar.zst
    count_cmd = [PLINK2, "--pfile", output_prefix, "vzs", "--write-snplist", "--out", f"{output_prefix}.tmp"]
    subprocess.run(count_cmd, check=True, capture_output=True, text=True)
    snplist = f"{output_prefix}.tmp.snplist"
    if os.path.exists(snplist):
        with open(snplist) as f:
            var_count = sum(1 for _ in f)
        os.remove(snplist)
    for tmp in [f"{output_prefix}.tmp.log"]:
        if os.path.exists(tmp):
            os.remove(tmp)

    # Clean up intermediate normalized VCF
    os.remove(normalized_vcf)
    if os.path.exists(normalized_vcf + ".tbi"):
        os.remove(normalized_vcf + ".tbi")

    return {
        "pgen": f"{output_prefix}.pgen",
        "pvar": pvar_path,
        "psam": f"{output_prefix}.psam",
        "variant_count": var_count,
    }


def check_pgen_exists(output_prefix: str) -> bool:
    """Check if pgen conversion has already been done for this sample."""
    return (
        os.path.exists(f"{output_prefix}.pgen") and
        (os.path.exists(f"{output_prefix}.pvar.zst") or os.path.exists(f"{output_prefix}.pvar")) and
        os.path.exists(f"{output_prefix}.psam")
    )
