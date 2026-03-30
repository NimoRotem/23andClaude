#!/usr/bin/env python3
"""
Ancestry inference: PCA with 1000 Genomes reference panel.

Supports BAM, CRAM, VCF, and gVCF input.
- VCF/gVCF: extract biallelic SNPs directly
- BAM/CRAM: call variants at reference panel sites via bcftools mpileup

Steps:
1. Prepare variant calls from sample (VCF pipeline or BAM mpileup)
2. LD-prune reference panel (cached)
3. Extract overlapping variants
4. Merge sample + reference via bcftools
5. Run PCA on merged dataset
6. Classify by nearest-centroid + nearest-neighbor

Usage:
    python scripts/run_ancestry.py --sample-name SampleA --vcf /path/to/sample.g.vcf.gz
    python scripts/run_ancestry.py --sample-name B2XH --bam /data/aligned_bams/B2XH.bam
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

import numpy as np

PLINK2 = os.environ.get("PLINK2", "plink2")
BCFTOOLS = os.environ.get("BCFTOOLS", "bcftools")
SAMTOOLS = os.environ.get("SAMTOOLS", "samtools")
REF_PANEL = os.environ.get("REF_PANEL", "/data/pgs2/ref_panel/GRCh38_1000G_ALL")
REF_FASTA = os.environ.get("REF_FASTA", "/data/refs/GRCh38.fa")
PSAM = REF_PANEL + ".psam"
TABIX = "tabix"
BGZIP = "bgzip"


def _run(cmd, timeout=600):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return r


def _log(msg):
    print(f"[ancestry] {msg}", file=sys.stderr, flush=True)


def load_population_labels():
    labels = {}
    with open(PSAM) as f:
        f.readline()
        for line in f:
            parts = line.strip().split("\t")
            labels[parts[0]] = {"superpop": parts[4], "population": parts[5] if len(parts) > 5 else ""}
    return labels


def _detect_chr_naming(input_path, is_bam=False):
    """Detect if file uses 'chr1' or '1' naming convention."""
    if is_bam:
        r = _run([SAMTOOLS, "idxstats", input_path])
        if r.returncode == 0:
            for line in r.stdout.split("\n")[:5]:
                if line.startswith("chr"):
                    return "chr"
                if line and line[0].isdigit():
                    return "numeric"
    else:
        r = subprocess.run(f"{BCFTOOLS} view -h {input_path} 2>/dev/null | grep '^##contig' | head -3",
                           shell=True, capture_output=True, text=True, timeout=30)
        if "chr" in r.stdout:
            return "chr"
        if r.stdout:
            return "numeric"
    return "numeric"


def _create_sites_file(ref_bed_cache, tmpdir, chr_naming="numeric"):
    """Create a sites file from reference BIM for targeted mpileup calling."""
    sites_file = os.path.join(tmpdir, "ancestry_sites.tsv")
    sites_vcf = os.path.join(tmpdir, "ancestry_sites.vcf.gz")

    # Extract positions from reference BIM: chr:pos:ref:alt format → chr\tpos
    bim_file = ref_bed_cache + ".bim"
    prefix = "chr" if chr_naming == "chr" else ""

    with open(bim_file) as f, open(sites_file, "w") as out:
        for line in f:
            parts = line.strip().split("\t")
            chrom = parts[0]
            pos = parts[3]
            out.write(f"{prefix}{chrom}\t{pos}\n")

    # Also create a regions file for bcftools
    regions_file = os.path.join(tmpdir, "ancestry_regions.txt")
    with open(bim_file) as f, open(regions_file, "w") as out:
        for line in f:
            parts = line.strip().split("\t")
            chrom = parts[0]
            pos = parts[3]
            out.write(f"{prefix}{chrom}\t{pos}\t{pos}\n")

    return sites_file, regions_file


def _vcf_from_bam(bam_path, sample_name, ref_bed_cache, tmpdir, threads=8):
    """Call variants from BAM/CRAM at reference panel sites using bcftools mpileup."""
    _log(f"Calling variants from BAM at {sum(1 for _ in open(ref_bed_cache + '.bim')):,} reference sites...")

    chr_naming = _detect_chr_naming(bam_path, is_bam=True)
    _log(f"BAM chromosome naming: {chr_naming}")

    sites_file, regions_file = _create_sites_file(ref_bed_cache, tmpdir, chr_naming)

    sample_snps = os.path.join(tmpdir, "sample_snps.vcf.gz")

    # Use bcftools mpileup for fast targeted calling at known sites
    # --regions-file limits calling to exact positions
    # --min-MQ 20 filters low-quality mappings
    # --min-BQ 20 filters low-quality bases
    cmd = (
        f"{BCFTOOLS} mpileup -f {REF_FASTA} -R {regions_file} "
        f"--min-MQ 20 --min-BQ 20 --threads {threads} "
        f"-a FORMAT/AD,FORMAT/DP {bam_path} 2>/dev/null | "
        f"{BCFTOOLS} call -m --threads {threads} 2>/dev/null | "
        f"{BCFTOOLS} view -v snps -m2 -M2 2>/dev/null | "
        f"{BCFTOOLS} annotate --set-id '%CHROM:%POS:%REF:%ALT' -O z -o {sample_snps} 2>/dev/null"
    )

    _log("Running bcftools mpileup → call → filter...")
    r = subprocess.run(cmd, shell=True, timeout=3600, capture_output=True, text=True)

    if not os.path.exists(sample_snps) or os.path.getsize(sample_snps) < 100:
        # Try without regions file limitation (slower but may work with different chr naming)
        _log("First attempt failed, trying with explicit chromosome conversion...")
        # Convert regions to match BAM naming
        alt_regions = os.path.join(tmpdir, "ancestry_regions_alt.txt")
        alt_naming = "numeric" if chr_naming == "chr" else "chr"
        alt_prefix = "chr" if alt_naming == "chr" else ""
        with open(ref_bed_cache + ".bim") as f, open(alt_regions, "w") as out:
            for line in f:
                parts = line.strip().split("\t")
                out.write(f"{alt_prefix}{parts[0]}\t{parts[3]}\t{parts[3]}\n")

        cmd = (
            f"{BCFTOOLS} mpileup -f {REF_FASTA} -R {alt_regions} "
            f"--min-MQ 20 --min-BQ 20 --threads {threads} "
            f"-a FORMAT/AD,FORMAT/DP {bam_path} 2>/dev/null | "
            f"{BCFTOOLS} call -m --threads {threads} 2>/dev/null | "
            f"{BCFTOOLS} view -v snps -m2 -M2 2>/dev/null | "
            f"{BCFTOOLS} annotate --set-id '%CHROM:%POS:%REF:%ALT' -O z -o {sample_snps} 2>/dev/null"
        )
        subprocess.run(cmd, shell=True, timeout=3600)

    if not os.path.exists(sample_snps) or os.path.getsize(sample_snps) < 100:
        return None, "bcftools mpileup failed to produce variants from BAM"

    subprocess.run([TABIX, "-p", "vcf", sample_snps], capture_output=True, timeout=60)

    n_called = int(subprocess.run(f"{BCFTOOLS} view -H {sample_snps} 2>/dev/null | wc -l",
                                   shell=True, capture_output=True, text=True, timeout=30).stdout.strip() or "0")
    _log(f"Called {n_called:,} SNPs from BAM at reference panel sites")

    return sample_snps, None


def _vcf_from_vcf(vcf_path, tmpdir):
    """Extract biallelic SNPs from VCF/gVCF."""
    _log(f"Extracting biallelic SNPs from VCF: {os.path.basename(vcf_path)}")

    sample_snps = os.path.join(tmpdir, "sample_snps.vcf.gz")
    cmd = (
        f"{BCFTOOLS} norm -m -any {vcf_path} 2>/dev/null | "
        f"{BCFTOOLS} view -e 'ALT=\"<*>\"' 2>/dev/null | "
        f"{BCFTOOLS} view -v snps -m2 -M2 2>/dev/null | "
        f"{BCFTOOLS} annotate --set-id '%CHROM:%POS:%REF:%ALT' -O z -o {sample_snps} 2>/dev/null"
    )
    subprocess.run(cmd, shell=True, timeout=1800)
    subprocess.run([TABIX, "-p", "vcf", sample_snps], capture_output=True, timeout=60)

    if not os.path.exists(sample_snps) or os.path.getsize(sample_snps) < 100:
        return None, "bcftools failed to extract SNPs from VCF"

    return sample_snps, None


def run_pipeline(sample_name, vcf_path, bam_path, tmpdir, threads=8, input_type=None):
    """Full ancestry pipeline. Accepts VCF, gVCF, BAM, or CRAM."""

    # Step 1: LD-prune reference (reuse if cached)
    prune_cache = "/data/pgs2/ref_panel/ancestry_prune.prune.in"
    ref_bed_cache = "/data/pgs2/ref_panel/ancestry_ref"

    if not os.path.exists(prune_cache):
        _log("LD-pruning reference panel (first time, will be cached)...")
        _run([PLINK2, "--pfile", REF_PANEL, "vzs", "--autosome", "--snps-only", "--max-alleles", "2",
              "--maf", "0.05", "--indep-pairwise", "200", "50", "0.2",
              "--out", "/data/pgs2/ref_panel/ancestry_prune", "--threads", str(threads)], timeout=600)

    if not os.path.exists(ref_bed_cache + ".bed"):
        _log("Creating reference BED (first time, will be cached)...")
        _run([PLINK2, "--pfile", REF_PANEL, "vzs", "--extract", prune_cache,
              "--make-bed", "--out", ref_bed_cache, "--threads", str(threads)], timeout=300)

    if not os.path.exists(ref_bed_cache + ".bed"):
        return {"error": "Failed to prepare reference panel"}

    # Step 2: Get sample variants based on input type
    if input_type in ("bam", "cram") or (bam_path and not vcf_path):
        if not bam_path or not os.path.exists(bam_path):
            return {"error": f"BAM/CRAM file not found: {bam_path}"}
        if not os.path.exists(REF_FASTA):
            return {"error": f"Reference FASTA not found: {REF_FASTA}. Set REF_FASTA env var."}
        sample_snps, err = _vcf_from_bam(bam_path, sample_name, ref_bed_cache, tmpdir, threads)
    elif vcf_path and os.path.exists(vcf_path):
        sample_snps, err = _vcf_from_vcf(vcf_path, tmpdir)
    else:
        return {"error": f"No valid input file found. VCF={vcf_path}, BAM={bam_path}"}

    if err:
        return {"error": err}

    # Step 3: Convert sample to BED (autosomes only)
    _log("Converting sample to BED format...")
    sample_bed = os.path.join(tmpdir, "sample_bed")
    r = _run([PLINK2, "--vcf", sample_snps, "--output-chr", "26", "--autosome",
              "--make-bed", "--out", sample_bed, "--threads", str(threads)])
    if not os.path.exists(sample_bed + ".bed"):
        return {"error": f"Sample BED conversion failed: {r.stderr[:300]}"}

    # Step 4: Extract overlapping variants
    _log("Finding overlapping variants with reference panel...")
    ref_ids = os.path.join(tmpdir, "ref_ids.txt")
    subprocess.run(f"awk '{{print $2}}' {ref_bed_cache}.bim > {ref_ids}", shell=True, timeout=30)

    sample_ov = os.path.join(tmpdir, "sample_ov")
    _run([PLINK2, "--bfile", sample_bed, "--extract", ref_ids, "--make-bed", "--out", sample_ov, "--threads", str(threads)])

    ov_ids = os.path.join(tmpdir, "ov_ids.txt")
    subprocess.run(f"awk '{{print $2}}' {sample_ov}.bim > {ov_ids}", shell=True, timeout=30)

    ref_ov = os.path.join(tmpdir, "ref_ov")
    _run([PLINK2, "--bfile", ref_bed_cache, "--extract", ov_ids, "--make-bed", "--out", ref_ov, "--threads", str(threads)])

    # Count overlap
    if not os.path.exists(sample_ov + ".bim") or not os.path.exists(ref_ov + ".bim"):
        return {"error": "No overlapping variants found between sample and reference"}

    n_variants = sum(1 for _ in open(sample_ov + ".bim"))
    _log(f"Found {n_variants:,} overlapping variants")
    if n_variants < 1000:
        return {"error": f"Only {n_variants} overlapping variants (need >1000)"}

    # Step 5: Merge via bcftools (VCF roundtrip)
    _log("Merging sample with reference panel...")
    ref_vcf = os.path.join(tmpdir, "ref_vcf.vcf.gz")
    sample_vcf = os.path.join(tmpdir, "sample_vcf.vcf.gz")
    _run([PLINK2, "--bfile", ref_ov, "--export", "vcf", "bgz", "--out", ref_vcf.replace(".vcf.gz", ""), "--threads", str(threads)])
    _run([PLINK2, "--bfile", sample_ov, "--export", "vcf", "bgz", "--out", sample_vcf.replace(".vcf.gz", ""), "--threads", str(threads)])
    subprocess.run([TABIX, "-p", "vcf", ref_vcf], capture_output=True, timeout=60)
    subprocess.run([TABIX, "-p", "vcf", sample_vcf], capture_output=True, timeout=60)

    merged_vcf = os.path.join(tmpdir, "merged.vcf.gz")
    _run([BCFTOOLS, "merge", ref_vcf, sample_vcf, "-O", "z", "-o", merged_vcf])

    merged_pgen = os.path.join(tmpdir, "merged")
    _run([PLINK2, "--vcf", merged_vcf, "--autosome", "--make-pgen", "--out", merged_pgen, "--threads", str(threads)])

    if not os.path.exists(merged_pgen + ".pgen"):
        return {"error": "Merge failed"}

    # Step 6: PCA
    _log("Running PCA (10 components)...")
    pca_out = os.path.join(tmpdir, "pca")
    r = _run([PLINK2, "--pfile", merged_pgen, "--pca", "10", "--out", pca_out, "--threads", str(threads)], timeout=600)

    eigenvec = pca_out + ".eigenvec"
    if not os.path.exists(eigenvec):
        return {"error": f"PCA failed: {r.stderr[:200]}"}

    # Step 7: Classify
    _log("Classifying ancestry...")
    result = classify(eigenvec, sample_name, n_variants)
    if "error" not in result:
        result["input_type"] = input_type or ("bam" if bam_path and not vcf_path else "vcf")
        result["input_file"] = bam_path if result["input_type"] in ("bam", "cram") else vcf_path
    return result


def classify(eigenvec_file, sample_name, n_variants):
    pop_labels = load_population_labels()

    samples = {}
    with open(eigenvec_file) as f:
        f.readline()
        for line in f:
            parts = line.strip().split("\t")
            samples[parts[0]] = [float(x) for x in parts[1:11]]

    if sample_name not in samples:
        return {"error": f"Sample {sample_name} not found in PCA"}

    sample_pcs = np.array(samples[sample_name])

    # Centroids per superpopulation
    pop_pcs = {sp: [] for sp in ["EUR", "AFR", "EAS", "SAS", "AMR"]}
    for iid, pcs in samples.items():
        sp = pop_labels.get(iid, {}).get("superpop", "")
        if sp in pop_pcs:
            pop_pcs[sp].append(pcs)

    centroids = {sp: np.mean(v, axis=0) for sp, v in pop_pcs.items() if v}

    # Distances
    distances = {sp: float(np.linalg.norm(sample_pcs - c)) for sp, c in centroids.items()}
    inv = {sp: 1.0 / (d + 1e-10) for sp, d in distances.items()}
    total = sum(inv.values())
    proportions = {sp: round(v / total, 4) for sp, v in inv.items()}
    primary = min(distances, key=distances.get)

    # Nearest neighbors (more reliable than centroid distance)
    ref_dists = []
    for iid, pcs in samples.items():
        info = pop_labels.get(iid, {})
        if info.get("superpop"):
            d = np.linalg.norm(sample_pcs - np.array(pcs))
            ref_dists.append((iid, d, info))
    ref_dists.sort(key=lambda x: x[1])

    # K-nearest neighbor classification (K=20)
    k = 20
    nn_counts = {}
    for _, _, info in ref_dists[:k]:
        sp = info["superpop"]
        nn_counts[sp] = nn_counts.get(sp, 0) + 1

    nn_primary = max(nn_counts, key=nn_counts.get) if nn_counts else primary
    nn_confidence = nn_counts.get(nn_primary, 0) / k

    nearest_subpops = {}
    for _, _, info in ref_dists[:10]:
        p = info.get("population", "?")
        nearest_subpops[p] = nearest_subpops.get(p, 0) + 1

    pop_names = {"EUR": "European", "AFR": "African", "EAS": "East Asian", "SAS": "South Asian", "AMR": "American (admixed)"}

    return {
        "primary_ancestry": nn_primary,
        "primary_name": pop_names.get(nn_primary, nn_primary),
        "proportions": proportions,
        "nn_proportions": {sp: round(c / k, 3) for sp, c in nn_counts.items()},
        "distances": {sp: round(d, 4) for sp, d in distances.items()},
        "is_admixed": nn_confidence < 0.80,
        "confidence": round(nn_confidence, 3),
        "pca_coordinates": [round(x, 6) for x in sample_pcs.tolist()],
        "nearest_subpopulations": [{"population": p, "count": c} for p, c in sorted(nearest_subpops.items(), key=lambda x: -x[1])[:5]],
        "variants_used": n_variants,
        "sample_name": sample_name,
    }


def main():
    parser = argparse.ArgumentParser(description="Ancestry inference via PCA with 1000 Genomes")
    parser.add_argument("--sample-name", required=True)
    parser.add_argument("--vcf", help="gVCF or VCF path")
    parser.add_argument("--bam", help="BAM or CRAM path")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--keep-tmp", action="store_true")
    args = parser.parse_args()

    from shutil import which
    global PLINK2, BCFTOOLS, SAMTOOLS, TABIX, BGZIP
    PLINK2 = which("plink2") or PLINK2
    BCFTOOLS = which("bcftools") or BCFTOOLS
    SAMTOOLS = which("samtools") or SAMTOOLS
    TABIX = which("tabix") or TABIX
    BGZIP = which("bgzip") or BGZIP

    # Determine input type and find files
    vcf = args.vcf
    bam = args.bam
    input_type = None

    # Auto-detect input type from provided paths
    if vcf and vcf.endswith((".bam", ".cram")):
        # User passed BAM as --vcf (common from checklist)
        bam = vcf
        vcf = None

    if bam and bam.endswith((".vcf.gz", ".vcf", ".g.vcf.gz")):
        # User passed VCF as --bam
        vcf = bam
        bam = None

    # Determine type
    if bam and os.path.exists(bam):
        input_type = "cram" if bam.endswith(".cram") else "bam"
    if vcf and os.path.exists(vcf):
        input_type = "gvcf" if ".g.vcf" in vcf else "vcf"

    # Auto-discover files if not provided
    if not vcf or not os.path.exists(vcf):
        from glob import glob
        candidates = glob(f"/scratch/nimog_output/*/dv/{args.sample_name}.g.vcf.gz")
        if candidates:
            vcf = sorted(candidates)[-1]
            input_type = "gvcf"
        else:
            candidates = glob(f"/scratch/nimog_output/*/dv/{args.sample_name}.vcf.gz")
            if candidates:
                vcf = sorted(candidates)[-1]
                input_type = "vcf"

    if not bam or not os.path.exists(bam):
        for ext in ["bam", "cram"]:
            candidate = f"/data/aligned_bams/{args.sample_name}.{ext}"
            if os.path.exists(candidate):
                bam = candidate
                if not input_type:
                    input_type = ext
                break

    # Prefer VCF/gVCF if available (faster), fall back to BAM
    if vcf and os.path.exists(vcf):
        _log(f"Using VCF input: {vcf}")
    elif bam and os.path.exists(bam):
        _log(f"Using BAM input (will call variants via mpileup): {bam}")
        vcf = None  # Force BAM path
        input_type = "cram" if bam.endswith(".cram") else "bam"
    else:
        print(json.dumps({"error": f"No input file found for {args.sample_name}. Searched for VCF in /scratch/nimog_output and BAM/CRAM in /data/aligned_bams/"}))
        sys.exit(1)

    with tempfile.TemporaryDirectory(prefix="ancestry_") as tmpdir:
        if args.keep_tmp:
            tmpdir = "/tmp/ancestry_run"
            os.makedirs(tmpdir, exist_ok=True)
        result = run_pipeline(args.sample_name, vcf, bam, tmpdir, args.threads, input_type=input_type)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
