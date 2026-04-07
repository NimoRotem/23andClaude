#!/usr/bin/env python3
"""
Sex verification pipeline: 5 independent checks for genetic sex determination.

Supports BAM, CRAM, VCF, and gVCF input. Uses MSY-only reads (excludes PAR).
Based on skill: .claude/skills/ancestry-population-pipeline.md

Checks:
  BAM/CRAM: 1) Y read count (MSY), 2) SRY gene reads, 3) X:Y ratio
  VCF/gVCF: 4) chrX het rate, 5) chrY variant count

Usage:
    python scripts/run_sex_check.py --sample-name Sample1 --bam /data/aligned_bams/Sample1.bam
    python scripts/run_sex_check.py --sample-name Sample1 --bam /path/to.bam --vcf /path/to.g.vcf.gz
"""

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

SAMTOOLS = "samtools"
BCFTOOLS = "bcftools"

# GRCh38 PAR coordinates
PAR1_Y = "chrY:10001-2781479"
PAR2_Y = "chrY:56887903-57217415"
MSY_REGION = "chrY:2781480-56887902"
SRY_REGION = "chrY:2786855-2787699"
CHRX_NON_PAR = "chrX:2781480-155701382"


def _run(cmd, timeout=120):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return r


def _shell(cmd, timeout=120):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    return r.stdout.strip()


def _detect_chr_naming(filepath, is_bam=False):
    """Detect chr prefix convention."""
    if is_bam:
        r = _run([SAMTOOLS, "idxstats", filepath])
        for line in r.stdout.split("\n")[:5]:
            if line.startswith("chr"):
                return "chr"
            if line and line[0].isdigit():
                return ""
    else:
        r = _shell(f"{BCFTOOLS} view -h {filepath} 2>/dev/null | head -30")
        if "chrX" in r or "chr1" in r:
            return "chr"
        if "##contig" in r:
            return ""
    return "chr"


def _fix_region(region, prefix):
    """Adjust chr prefix in region string."""
    if prefix == "":
        return region.replace("chrY:", "Y:").replace("chrX:", "X:")
    return region


def check_y_reads(bam_path, prefix):
    """Check 1: MSY read count + coverage uniformity.

    Total MSY read count alone can be misleading — female BAMs can show 500K+
    reads in X-transposed/ampliconic regions near the start of chrY. A real male
    has reads distributed across the entire MSY. We sample 10 windows across MSY
    and require at least 6/10 to have coverage.
    """
    region = _fix_region(MSY_REGION, prefix)
    r = _run([SAMTOOLS, "view", "-c", "-F", "0x904", bam_path, region])
    try:
        count = int(r.stdout.strip())
    except (ValueError, AttributeError):
        return {"call": "ambiguous", "value": None, "detail": f"Could not count MSY reads: {r.stderr[:100]}"}

    # Coverage uniformity: sample 10 windows across MSY, focusing on unique Y regions
    # Avoid the first 5Mb (X-transposed region with high homology) and ampliconic regions
    windows_with_reads = 0
    window_details = []
    for start_mb in [7, 12, 15, 18, 21, 25, 30, 35, 40, 45]:
        start = start_mb * 1_000_000
        end = start + 100_000
        win_region = _fix_region(f"chrY:{start}-{end}", prefix)
        wr = _run([SAMTOOLS, "view", "-c", "-F", "0x904", bam_path, win_region])
        try:
            wc = int(wr.stdout.strip())
        except (ValueError, AttributeError):
            wc = 0
        if wc > 5:
            windows_with_reads += 1
        window_details.append(f"Y:{start_mb}M={wc}")

    uniformity = windows_with_reads / 10.0

    if count > 100_000 and uniformity >= 0.6:
        call = "male"
        detail = f"{count:,} MSY reads, coverage in {windows_with_reads}/10 windows ({uniformity*100:.0f}% uniformity). Definitive male."
    elif count > 100_000 and uniformity < 0.3:
        call = "female"
        detail = f"{count:,} MSY reads BUT only {windows_with_reads}/10 windows have coverage ({uniformity*100:.0f}%). Reads concentrated in X-homologous regions — consistent with XX."
    elif count > 100_000:
        call = "ambiguous"
        detail = f"{count:,} MSY reads, {windows_with_reads}/10 windows ({uniformity*100:.0f}% uniformity). Partial Y coverage — could be mosaic, contamination, or low-coverage male."
    elif count > 1_000:
        call = "ambiguous"
        detail = f"{count:,} MSY reads, {windows_with_reads}/10 windows. Low but present."
    else:
        call = "female"
        detail = f"{count:,} MSY reads. Below threshold — consistent with XX."

    return {"call": call, "value": count, "uniformity": uniformity,
            "threshold_used": ">100K+uniform=male, >100K+clustered=female",
            "detail": detail}


def check_sry(bam_path, prefix):
    """Check 2: SRY gene reads."""
    region = _fix_region(SRY_REGION, prefix)
    r = _run([SAMTOOLS, "view", "-c", "-F", "0x904", bam_path, region])
    try:
        count = int(r.stdout.strip())
    except (ValueError, AttributeError):
        return {"call": "ambiguous", "value": None, "detail": f"Could not count SRY reads: {r.stderr[:100]}"}

    if count >= 10:
        call = "male"
        detail = f"{count} reads at SRY locus. SRY has no X homolog — any reads confirm Y presence."
    elif count >= 1:
        call = "ambiguous"
        detail = f"{count} read(s) at SRY. Very low — could be edge mismapping or very low coverage male."
    else:
        call = "female"
        detail = f"0 reads at SRY gene. SRY has no X homolog — zero is definitive for XX."

    return {"call": call, "value": count, "threshold_used": ">=10=male, 0=female", "detail": detail}


def check_xy_ratio(bam_path, prefix):
    """Check 3: X:Y MSY read ratio."""
    r = _run([SAMTOOLS, "idxstats", bam_path])
    x_reads = 0
    for line in r.stdout.split("\n"):
        parts = line.split("\t")
        if parts[0] in ("chrX", "X"):
            x_reads = int(parts[2])

    msy_region = _fix_region(MSY_REGION, prefix)
    r2 = _run([SAMTOOLS, "view", "-c", "-F", "0x904", bam_path, msy_region])
    try:
        msy_reads = int(r2.stdout.strip())
    except (ValueError, AttributeError):
        return {"call": "ambiguous", "value": None, "detail": "Could not count MSY reads for ratio."}

    if msy_reads == 0:
        return {"call": "female", "value": float("inf"),
                "threshold_used": "Y=0 → female",
                "detail": f"X:Y ratio = infinity (chrX: {x_reads:,}, MSY: 0). No Y signal — definitive female."}

    ratio = x_reads / msy_reads

    if 3 <= ratio <= 15:
        call = "male"
        detail = f"X:Y(MSY) ratio = {ratio:.1f} (chrX: {x_reads:,} / MSY: {msy_reads:,}). Within normal male range (3-15)."
    elif ratio > 100:
        call = "female"
        detail = f"X:Y(MSY) ratio = {ratio:.1f}. Very high — Y reads are noise only."
    elif ratio > 15:
        call = "ambiguous"
        detail = f"X:Y(MSY) ratio = {ratio:.1f}. Between 15-100 — could be XXY, contamination, or mosaic."
    else:
        call = "ambiguous"
        detail = f"X:Y(MSY) ratio = {ratio:.1f}. Below expected male range — unusual."

    return {"call": call, "value": round(ratio, 2), "threshold_used": "3-15=male, >100=female", "detail": detail}


def check_chrx_het(vcf_path, prefix):
    """Check 4: ChrX heterozygosity rate (non-PAR).

    For gVCFs: only count actual variant sites (het + hom-alt), not reference blocks.
    The ratio is het / (het + hom-alt). Males: ~0%, Females: ~60-70% of variant sites.
    """
    region = _fix_region(CHRX_NON_PAR, prefix)
    # Count only at variant sites: het (0/1) and hom-alt (1/1), skip ref (0/0) and missing
    cmd = (
        f"{BCFTOOLS} view -v snps -r {region} {vcf_path} 2>/dev/null | "
        f"{BCFTOOLS} query -f '[%GT]\\n' 2>/dev/null | "
        f"awk '$1==\"0/1\"||$1==\"0|1\"||$1==\"1|0\"{{het++}} "
        f"$1==\"1/1\"||$1==\"1|1\"{{hom++}} "
        f"END{{total=het+hom; printf \"het=%d hom=%d total=%d rate=%.4f\\n\",het,hom,total,(total>0?het/total:0)}}'"
    )
    out = _shell(cmd, timeout=300)

    import re
    m = re.search(r'het=(\d+)\s+(?:hom=\d+\s+)?total=(\d+)\s+rate=([\d.]+)', out)
    if not m:
        return {"call": "ambiguous", "value": None, "detail": f"Could not parse chrX het rate: {out[:100]}"}

    het = int(m.group(1))
    total = int(m.group(2))
    rate = float(m.group(3))

    if total < 100:
        return {"call": "ambiguous", "value": rate,
                "detail": f"Only {total} chrX variants — too few for reliable het rate."}

    # Thresholds: het/(het+homalt) at variant sites only
    # Males (hemizygous X): almost all variant calls are hom-alt → rate <10%
    # Females (diploid X): ~50-70% of variant sites are het → rate >40%
    if rate < 0.10:
        call = "male"
        detail = f"ChrX het rate = {rate:.1%} ({het:,} het / {total:,} variant sites). <10% — hemizygous X, consistent with XY."
    elif rate < 0.30:
        call = "ambiguous"
        detail = f"ChrX het rate = {rate:.1%} ({het:,} het / {total:,} variant sites). 10-30% — could be XXY, contamination, or mosaic."
    else:
        call = "female"
        detail = f"ChrX het rate = {rate:.1%} ({het:,} het / {total:,} variant sites). >30% — normal diploid X (XX), consistent with female."

    return {"call": call, "value": round(rate, 4), "threshold_used": "<5%=male, >15%=female", "detail": detail}


def check_chry_variants(vcf_path, prefix):
    """Check 5: ChrY variant count in unique MSY regions only.

    Exclude X-transposed region (3-6M), ampliconic regions (10-12M, 26M),
    and near-PAR2 (>55M) which produce false variants in female samples.
    Only count variants in unique single-copy Y regions.
    """
    # Unique Y regions (avoiding X-transposed, ampliconic, and near-PAR)
    unique_y_regions = [
        "chrY:6500000-9500000",   # Unique Yp
        "chrY:12500000-13000000", # Between ampliconic
        "chrY:13500000-20000000", # Main unique MSY block
        "chrY:20500000-25000000", # Unique Yq
        "chrY:27000000-55000000", # Large unique Yq block
    ]
    regions_str = ",".join(_fix_region(r, prefix) for r in unique_y_regions)

    cmd = (
        f"{BCFTOOLS} view -r {regions_str} -v snps,indels "
        f"-e 'GT=\"0/0\" || GT=\"./.\" || GT=\".|.\" || GQ<20' "
        f"{vcf_path} 2>/dev/null | grep -cv '^#'"
    )
    out = _shell(cmd, timeout=120)

    try:
        count = int(out.strip())
    except (ValueError, AttributeError):
        count = 0

    if count > 500:
        call = "male"
        detail = f"{count:,} high-quality (GQ>=20) variants on chrY MSY. Definitive Y haplotype."
    elif count > 50:
        call = "ambiguous"
        detail = f"{count} chrY variants (GQ>=20). Low but present — could be low-coverage male or noise."
    else:
        call = "female"
        detail = f"{count} chrY variants (GQ>=20). Consistent with XX — any calls are mismapping noise."

    return {"call": call, "value": count, "threshold_used": ">500(GQ>=20)=male, <50=female", "detail": detail}


def determine_sex(checks):
    """Consensus algorithm from multiple checks."""
    calls = [c["call"] for c in checks.values() if c["call"] != "ambiguous"]
    all_calls = [c["call"] for c in checks.values()]
    n_checks = len(checks)
    n_definitive = len(calls)
    male_count = calls.count("male")
    female_count = calls.count("female")
    ambiguous_count = all_calls.count("ambiguous")

    if male_count == n_definitive and n_definitive >= 2:
        return {"sex": "XY (male)", "confidence": "high",
                "reasoning": f"All {n_definitive} definitive checks agree: male." +
                (f" {ambiguous_count} check(s) were ambiguous." if ambiguous_count else "")}

    if female_count == n_definitive and n_definitive >= 2:
        return {"sex": "XX (female)", "confidence": "high",
                "reasoning": f"All {n_definitive} definitive checks agree: female." +
                (f" {ambiguous_count} check(s) were ambiguous." if ambiguous_count else "")}

    if male_count > female_count and male_count >= 2:
        dissenters = [n for n, c in checks.items() if c["call"] == "female"]
        return {"sex": "XY (male)", "confidence": "medium",
                "reasoning": f"{male_count}/{n_definitive} checks indicate male. Disagreeing: {', '.join(dissenters)}."}

    if female_count > male_count and female_count >= 2:
        dissenters = [n for n, c in checks.items() if c["call"] == "male"]
        return {"sex": "XX (female)", "confidence": "medium",
                "reasoning": f"{female_count}/{n_definitive} checks indicate female. Disagreeing: {', '.join(dissenters)}."}

    if n_definitive == 0:
        return {"sex": "undetermined", "confidence": "none",
                "reasoning": f"All {n_checks} checks returned ambiguous. Possible aneuploidy, mosaicism, or quality issue."}

    return {"sex": "conflicting", "confidence": "low",
            "reasoning": f"Checks split: {male_count} male, {female_count} female, {ambiguous_count} ambiguous. "
                         f"Could indicate contamination, aneuploidy, or sample swap."}


def run_pipeline(sample_name, bam_path=None, vcf_path=None, max_workers=4):
    """Run all applicable sex verification checks in parallel."""
    checks = {}
    checks_not_run = {}

    # Detect chr naming from whichever file is available
    if bam_path and os.path.exists(bam_path):
        prefix = _detect_chr_naming(bam_path, is_bam=True)
    elif vcf_path and os.path.exists(vcf_path):
        prefix = _detect_chr_naming(vcf_path, is_bam=False)
    else:
        return {"error": f"No valid input files for {sample_name}"}

    # Build list of (check_name, callable) for all applicable checks
    tasks = {}
    has_bam = bam_path and os.path.exists(bam_path)
    has_vcf = vcf_path and os.path.exists(vcf_path)

    if has_bam:
        tasks["y_read_count"] = lambda: check_y_reads(bam_path, prefix)
        tasks["sry_gene"] = lambda: check_sry(bam_path, prefix)
        tasks["xy_read_ratio"] = lambda: check_xy_ratio(bam_path, prefix)
    else:
        checks_not_run["y_read_count"] = "No BAM/CRAM provided"
        checks_not_run["sry_gene"] = "No BAM/CRAM provided"
        checks_not_run["xy_read_ratio"] = "No BAM/CRAM provided"

    if has_vcf:
        tasks["chrx_het_rate"] = lambda: check_chrx_het(vcf_path, prefix)
        tasks["chry_variant_count"] = lambda: check_chry_variants(vcf_path, prefix)
    else:
        checks_not_run["chrx_het_rate"] = "No VCF/gVCF provided"
        checks_not_run["chry_variant_count"] = "No VCF/gVCF provided"

    # Run all checks in parallel
    if tasks:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(tasks))) as executor:
            future_to_name = {executor.submit(fn): name for name, fn in tasks.items()}
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    checks[name] = future.result()
                except Exception as e:
                    checks[name] = {"call": "ambiguous", "value": None,
                                    "detail": f"Check failed with error: {e}"}

    determination = determine_sex(checks)

    return {
        "sample_name": sample_name,
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "input_files": {"bam": bam_path, "vcf": vcf_path},
        "determination": determination,
        "checks": checks,
        "checks_not_run": checks_not_run,
    }


def main():
    parser = argparse.ArgumentParser(description="Sex verification via 5 independent checks")
    parser.add_argument("--sample-name", required=True)
    parser.add_argument("--bam", help="BAM or CRAM path")
    parser.add_argument("--vcf", help="VCF or gVCF path")
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()

    from shutil import which
    global SAMTOOLS, BCFTOOLS
    SAMTOOLS = which("samtools") or SAMTOOLS
    BCFTOOLS = which("bcftools") or BCFTOOLS

    bam = args.bam
    vcf = args.vcf

    # Auto-detect: if vcf is actually a BAM, swap
    if vcf and vcf.endswith((".bam", ".cram")):
        bam = vcf
        vcf = None
    if bam and bam.endswith((".vcf.gz", ".vcf", ".g.vcf.gz")):
        vcf = bam
        bam = None

    # Auto-discover files
    if not bam or not os.path.exists(bam):
        for ext in ["bam", "cram"]:
            candidate = f"/data/aligned_bams/{args.sample_name}.{ext}"
            if os.path.exists(candidate):
                bam = candidate
                break

    if not vcf or not os.path.exists(vcf):
        from glob import glob
        candidates = glob(f"/scratch/nimog_output/*/dv/{args.sample_name}.g.vcf.gz")
        if candidates:
            vcf = sorted(candidates)[-1]
        else:
            candidates = glob(f"/scratch/nimog_output/*/dv/{args.sample_name}.vcf.gz")
            if candidates:
                vcf = sorted(candidates)[-1]

    if (not bam or not os.path.exists(bam)) and (not vcf or not os.path.exists(vcf)):
        print(json.dumps({"error": f"No BAM or VCF found for {args.sample_name}"}))
        sys.exit(1)

    bam = bam if bam and os.path.exists(bam) else None
    vcf = vcf if vcf and os.path.exists(vcf) else None

    result = run_pipeline(args.sample_name, bam, vcf, max_workers=args.threads)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
