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
    python scripts/run_ancestry.py --sample-name Sample1 --vcf /path/to/sample.g.vcf.gz
    python scripts/run_ancestry.py --sample-name Sample2 --bam /data/aligned_bams/Sample2.bam
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
REF_FASTA = os.environ.get("REF_FASTA", "/data/reference/GRCh38.fa")
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


def _vcf_from_bam(bam_path, sample_name, ref_bed_cache, tmpdir, threads=8, keep_ref_calls=False):
    """Call variants from BAM/CRAM at reference panel sites using bcftools mpileup.
    If keep_ref_calls=True, builds a synthetic VCF with biallelic records at ALL
    panel positions (uncalled = ref/ref with correct panel ALT allele)."""
    n_panel = sum(1 for _ in open(ref_bed_cache + ".bim"))
    _log(f"Calling variants from BAM at {n_panel:,} reference sites...")

    chr_naming = _detect_chr_naming(bam_path, is_bam=True)
    _log(f"BAM chromosome naming: {chr_naming}")

    sites_file, regions_file = _create_sites_file(ref_bed_cache, tmpdir, chr_naming)

    # Step 1: Run mpileup → call at all target positions (keep ALL output)
    mpileup_all = os.path.join(tmpdir, "mpileup_all.vcf.gz")
    cmd = (
        f"{BCFTOOLS} mpileup -f {REF_FASTA} -R {regions_file} "
        f"--min-MQ 20 --min-BQ 20 --threads {threads} "
        f"-a FORMAT/AD,FORMAT/DP {bam_path} 2>/dev/null | "
        f"{BCFTOOLS} call -m --threads {threads} "
        f"-O z -o {mpileup_all} 2>/dev/null"
    )
    _log("Running bcftools mpileup → call...")
    subprocess.run(cmd, shell=True, timeout=3600)

    if not os.path.exists(mpileup_all) or os.path.getsize(mpileup_all) < 100:
        return None, "bcftools mpileup failed"

    subprocess.run([TABIX, "-p", "vcf", mpileup_all], capture_output=True, timeout=60)

    if not keep_ref_calls:
        # Standard mode: extract only biallelic SNPs
        sample_snps = os.path.join(tmpdir, "sample_snps.vcf.gz")
        cmd2 = (
            f"{BCFTOOLS} view -v snps -m2 -M2 {mpileup_all} 2>/dev/null | "
            f"{BCFTOOLS} annotate --set-id '%CHROM:%POS:%REF:%ALT' -O z -o {sample_snps} 2>/dev/null"
        )
        subprocess.run(cmd2, shell=True, timeout=300)
        subprocess.run([TABIX, "-p", "vcf", sample_snps], capture_output=True, timeout=60)
        n_called = int(subprocess.run(f"{BCFTOOLS} view -H {sample_snps} 2>/dev/null | wc -l",
                                       shell=True, capture_output=True, text=True, timeout=30).stdout.strip() or "0")
        _log(f"Called {n_called:,} SNPs from BAM")
        return sample_snps, None

    # Step 2: Array panel mode — build synthetic VCF at ALL panel positions
    _log("Building complete genotype VCF at all panel positions...")

    # Parse mpileup results: (chr, pos) → (ref, alt, gt)
    called = {}
    r = subprocess.run(
        f"{BCFTOOLS} query -f '%CHROM\\t%POS\\t%REF\\t%ALT\\t[%GT]\\n' {mpileup_all}",
        shell=True, capture_output=True, text=True, timeout=120)
    for line in r.stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) >= 5:
            called[(parts[0], parts[1])] = (parts[2], parts[3], parts[4])

    _log(f"  mpileup produced calls at {len(called):,} positions")

    # Write synthetic VCF with biallelic record at every panel position
    prefix = chr_naming
    sample_vcf = os.path.join(tmpdir, "sample_complete.vcf")
    n_variant = 0
    n_ref = 0
    with open(ref_bed_cache + ".bim") as bim, open(sample_vcf, "w") as out:
        out.write("##fileformat=VCFv4.2\n")
        out.write('##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n')
        out.write(f"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t{sample_name}\n")

        for line in bim:
            parts = line.strip().split("\t")
            ch, vid, _, pos = parts[0], parts[1], parts[2], parts[3]
            panel_alt, panel_ref = parts[4], parts[5]  # plink: col5=A1(alt), col6=A2(ref)

            # Look up mpileup call at this position
            call = called.get((f"{prefix}{ch}", pos)) or called.get((ch, pos))

            if call:
                ref_allele, alt_allele, gt = call
                if alt_allele not in (".", "<*>") and gt not in ("./.", ".|.") and gt != "0/0":
                    # Real variant call
                    out.write(f"{ch}\t{pos}\t{vid}\t{ref_allele}\t{alt_allele}\t30\tPASS\t.\tGT\t{gt}\n")
                    n_variant += 1
                    continue

            # No variant call → ref/ref with panel alleles
            out.write(f"{ch}\t{pos}\t{vid}\t{panel_ref}\t{panel_alt}\t30\tPASS\t.\tGT\t0/0\n")
            n_ref += 1

    _log(f"  Synthetic VCF: {n_variant:,} variant + {n_ref:,} ref/ref = {n_variant + n_ref:,} total")

    # Compress and index
    subprocess.run(f"{BGZIP} -f {sample_vcf}", shell=True, timeout=120)
    subprocess.run([TABIX, "-p", "vcf", sample_vcf + ".gz"], capture_output=True, timeout=60)

    return sample_vcf + ".gz", None


def _vcf_from_vcf(vcf_path, tmpdir, ref_bim=None, threads=8):
    """Extract biallelic SNPs from VCF/gVCF. Optimized 3 ways:
    1. Target regions (-R) to skip 99% of gVCF — only read panel positions
    2. Parallel-by-chromosome — split into 22 jobs, use all cores
    3. BAM-only input skips this entirely (handled by _vcf_from_bam)
    """
    _log(f"Extracting biallelic SNPs from VCF: {os.path.basename(vcf_path)}")

    # Create target regions file from reference panel if available
    regions_arg = ""
    if ref_bim and os.path.exists(ref_bim):
        regions_file = os.path.join(tmpdir, "ref_regions.tsv")
        with open(ref_bim) as fin, open(regions_file, "w") as fout:
            for line in fin:
                parts = line.strip().split("\t")
                fout.write(f"{parts[0]}\t{parts[3]}\n")  # CHR\tPOS
        n_regions = sum(1 for _ in open(regions_file))
        _log(f"  Using {n_regions:,} target positions from reference panel (skipping rest of genome)")
        regions_arg = f"-T {regions_file}"

    # Check if file is indexed (required for -T and parallel)
    has_index = os.path.exists(vcf_path + ".tbi") or os.path.exists(vcf_path + ".csi")

    if has_index and regions_arg and threads >= 4:
        # FAST PATH: parallel-by-chromosome with target regions
        _log(f"  Parallel extraction across {min(threads, 22)} chromosomes...")
        import concurrent.futures

        def _extract_chr(chrom):
            out = os.path.join(tmpdir, f"chr{chrom}.vcf.gz")
            cmd = (
                f"{BCFTOOLS} view -r {chrom} {regions_arg} {vcf_path} 2>/dev/null | "
                f"{BCFTOOLS} norm -m -any 2>/dev/null | "
                f"{BCFTOOLS} view -e 'ALT=\"<*>\"' 2>/dev/null | "
                f"{BCFTOOLS} view -v snps -m2 -M2 2>/dev/null | "
                f"{BCFTOOLS} annotate --set-id '%CHROM:%POS:%REF:%ALT' -O z -o {out} 2>/dev/null"
            )
            subprocess.run(cmd, shell=True, timeout=300)
            if os.path.exists(out) and os.path.getsize(out) > 100:
                return out
            return None

        # Get chromosome names from the regions file (fast) or VCF header
        chroms = set()
        if ref_bim and os.path.exists(ref_bim):
            with open(ref_bim) as f:
                for line in f:
                    chroms.add(line.split("\t")[0])
        else:
            r = subprocess.run(f"{BCFTOOLS} view -h {vcf_path} 2>/dev/null | grep '^##contig' | sed 's/.*ID=//;s/,.*//'",
                               shell=True, capture_output=True, text=True, timeout=10)
            chroms = set(r.stdout.strip().split("\n"))
        chroms = sorted([c for c in chroms if c and c not in ("X", "Y", "M", "MT", "chrX", "chrY", "chrM")],
                        key=lambda x: int(x.replace("chr", "")) if x.replace("chr", "").isdigit() else 99)

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(threads, 22)) as executor:
            results = list(executor.map(_extract_chr, chroms))

        chr_files = [f for f in results if f]
        if not chr_files:
            _log("  Parallel extraction failed, falling back to serial...")
        else:
            # Concatenate chromosome files
            sample_snps = os.path.join(tmpdir, "sample_snps.vcf.gz")
            file_list = os.path.join(tmpdir, "chr_files.txt")
            with open(file_list, "w") as f:
                for cf in chr_files:
                    subprocess.run([TABIX, "-p", "vcf", cf], capture_output=True, timeout=30)
                    f.write(cf + "\n")
            _run([BCFTOOLS, "concat", "-f", file_list, "-O", "z", "-o", sample_snps], timeout=120)
            subprocess.run([TABIX, "-p", "vcf", sample_snps], capture_output=True, timeout=60)

            if os.path.exists(sample_snps) and os.path.getsize(sample_snps) > 100:
                n = int(subprocess.run(f"{BCFTOOLS} view -H {sample_snps} 2>/dev/null | wc -l",
                                        shell=True, capture_output=True, text=True, timeout=30).stdout.strip() or "0")
                _log(f"  Extracted {n:,} SNPs (parallel, {len(chr_files)} chromosomes)")
                return sample_snps, None

    # SERIAL PATH: with or without target regions
    sample_snps = os.path.join(tmpdir, "sample_snps.vcf.gz")
    if regions_arg:
        cmd = (
            f"{BCFTOOLS} view {regions_arg} {vcf_path} 2>/dev/null | "
            f"{BCFTOOLS} norm -m -any 2>/dev/null | "
            f"{BCFTOOLS} view -e 'ALT=\"<*>\"' 2>/dev/null | "
            f"{BCFTOOLS} view -v snps -m2 -M2 2>/dev/null | "
            f"{BCFTOOLS} annotate --set-id '%CHROM:%POS:%REF:%ALT' -O z -o {sample_snps} 2>/dev/null"
        )
    else:
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
    """Full ancestry pipeline per skills/ancestry-population-pipeline.md.

    Uses FRAPOSA OADP projection onto precomputed 1000G reference PCA (fast,
    no need to re-run PCA on all 3202 reference samples). Falls back to joint
    PCA if FRAPOSA reference data is not available.

    Accepts VCF, gVCF, BAM, or CRAM input.
    """
    ANCESTRY_REF = os.environ.get("ANCESTRY_REF", "/data/ancestry_reference")

    # Panel selection: 1KG is the reliable default for WGS (122K+ genuine variant overlap).
    # HO panel requires array genotyping data — WGS overlap is too biased (ref allele inflation).
    panel_candidates = [
        ("1kg", os.path.join(ANCESTRY_REF, "1kg", "ref_pruned")),
        ("hgdp_1kg", os.path.join(ANCESTRY_REF, "hgdp_1kg", "ref_pruned")),
        ("1kg_legacy", "/data/pgs2/ref_panel/ancestry_ref"),
    ]
    ref_bed = None
    panel_name = None
    for name, path in panel_candidates:
        if os.path.exists(path + ".bed"):
            ref_bed = path
            panel_name = name
            break
    if not ref_bed:
        return {"error": "No reference panel found. Run ancestry server setup first."}

    _log(f"Using panel: {panel_name} ({ref_bed})")
    ref_pcs = ref_bed + ".pcs"

    # Load pop2group file if available (for Rye)
    pop2group = None
    for p2g in [ref_bed.rsplit("/", 1)[0] + "/pop2group_fine.txt",
                ref_bed.rsplit("/", 1)[0] + "/pop2group_5superpop.txt"]:
        if os.path.exists(p2g):
            pop2group = p2g
            break

    is_array_panel = False

    # Step 1: Get sample variants
    if input_type in ("bam", "cram") or (bam_path and not vcf_path):
        if not bam_path or not os.path.exists(bam_path):
            return {"error": f"BAM/CRAM file not found: {bam_path}"}
        if not os.path.exists(REF_FASTA):
            return {"error": f"Reference FASTA not found: {REF_FASTA}. Set REF_FASTA env var."}
        sample_snps, err = _vcf_from_bam(bam_path, sample_name, ref_bed, tmpdir, threads)
    elif vcf_path and os.path.exists(vcf_path):
        sample_snps, err = _vcf_from_vcf(vcf_path, tmpdir, ref_bim=ref_bed + ".bim", threads=threads)
    else:
        return {"error": f"No valid input file found. VCF={vcf_path}, BAM={bam_path}"}

    if err:
        return {"error": err}

    # Step 2: Convert sample to BED (autosomes only)
    _log("Converting sample to BED format...")
    sample_bed = os.path.join(tmpdir, "sample_bed")
    r = _run([PLINK2, "--vcf", sample_snps, "--output-chr", "26", "--autosome",
              "--make-bed", "--out", sample_bed, "--threads", str(threads)])
    if not os.path.exists(sample_bed + ".bed"):
        return {"error": f"Sample BED conversion failed: {r.stderr[:300]}"}

    # Step 3: Extract overlapping variants with reference
    _log("Finding overlapping variants with reference panel...")
    ref_ids = os.path.join(tmpdir, "ref_ids.txt")
    subprocess.run(f"awk '{{print $2}}' {ref_bed}.bim > {ref_ids}", shell=True, timeout=30)

    sample_ov = os.path.join(tmpdir, "sample_ov")
    _run([PLINK2, "--bfile", sample_bed, "--extract", ref_ids, "--make-bed", "--out", sample_ov, "--threads", str(threads)])

    if not os.path.exists(sample_ov + ".bim"):
        return {"error": "No overlapping variants found between sample and reference"}

    n_variants = sum(1 for _ in open(sample_ov + ".bim"))
    _log(f"Found {n_variants:,} overlapping variants")
    if n_variants < 1000:
        return {"error": f"Only {n_variants} overlapping variants (need >1000)"}

    # Step 4: PCA — use FRAPOSA projection if available, else joint PCA
    use_fraposa = os.path.exists(ref_pcs) and os.path.exists(ref_bed + "_U.dat")

    if use_fraposa:
        _log("Using FRAPOSA OADP projection onto precomputed reference PCA...")
        try:
            from fraposa_pgsc import fraposa
            import io, contextlib

            # Get common variants between ref and sample
            ref_var_ids = set()
            with open(ref_bed + ".bim") as f:
                for line in f:
                    ref_var_ids.add(line.split("\t")[1])
            sample_var_ids = set()
            with open(sample_ov + ".bim") as f:
                for line in f:
                    sample_var_ids.add(line.split("\t")[1])
            common = ref_var_ids & sample_var_ids
            _log(f"Common variants: {len(common):,} (ref: {len(ref_var_ids):,}, sample: {len(sample_var_ids):,})")

            # Write common IDs
            common_ids = os.path.join(tmpdir, "common_ids.txt")
            with open(common_ids, "w") as f:
                for vid in common:
                    f.write(vid + "\n")

            # Subset both to common variants
            ref_common = os.path.join(tmpdir, "ref_common")
            stu_common = os.path.join(tmpdir, "stu_common")
            _run([PLINK2, "--bfile", ref_bed, "--extract", common_ids, "--make-bed", "--out", ref_common, "--threads", str(threads)])
            _run([PLINK2, "--bfile", sample_ov, "--extract", common_ids, "--make-bed", "--out", stu_common, "--threads", str(threads)])

            n_common = sum(1 for _ in open(ref_common + ".bim")) if os.path.exists(ref_common + ".bim") else 0
            _log(f"After extraction: ref={n_common}, study={sum(1 for _ in open(stu_common + '.bim')) if os.path.exists(stu_common + '.bim') else 0}")

            if n_common < 1000:
                _log(f"Too few common variants ({n_common}), falling back to joint PCA...")
                raise ValueError(f"Only {n_common} common variants")

            # Rename sample variant IDs to match FRAPOSA format (CHR:POS:A1:A2 = col5:col6)
            # FRAPOSA vars.dat uses the A1:A2 column order from the .bim
            _log("Aligning sample variant IDs to FRAPOSA format...")
            bim_path = stu_common + ".bim"
            with open(bim_path) as f:
                bim_lines = f.readlines()
            with open(bim_path, "w") as f:
                for line in bim_lines:
                    parts = line.strip().split("\t")
                    # Rename variant ID to CHR:POS:A1:A2 matching how FRAPOSA indexes
                    parts[1] = f"{parts[0]}:{parts[3]}:{parts[4]}:{parts[5]}"
                    f.write("\t".join(parts) + "\n")

            # Do the same for ref_common to ensure they match
            bim_path_ref = ref_common + ".bim"
            with open(bim_path_ref) as f:
                bim_lines = f.readlines()
            with open(bim_path_ref, "w") as f:
                for line in bim_lines:
                    parts = line.strip().split("\t")
                    parts[1] = f"{parts[0]}:{parts[3]}:{parts[4]}:{parts[5]}"
                    f.write("\t".join(parts) + "\n")

            # Delete any cached .dat for ref_common so FRAPOSA recomputes
            for ext in ["_U.dat", "_V.dat", "_s.dat", "_mnsd.dat", "_vars.dat", ".pcs"]:
                p = ref_common + ext
                if os.path.exists(p):
                    os.remove(p)

            # Run FRAPOSA
            _log(f"Running FRAPOSA OADP on {n_common:,} common variants...")
            fraposa_log = io.StringIO()
            with contextlib.redirect_stdout(fraposa_log):
                fraposa.pca(ref_common, stu_filepref=stu_common, method="oadp",
                            dim_ref=10, out_filepref=os.path.join(tmpdir, "projected"))
            _log(f"FRAPOSA done")

            # Find output file
            proj_file = None
            for candidate in [stu_common + ".pcs",
                              os.path.join(tmpdir, "projected_stu.pcs"),
                              os.path.join(tmpdir, "projected.pcs")]:
                if os.path.exists(candidate):
                    proj_file = candidate
                    break

            if proj_file:
                _log(f"FRAPOSA projection complete: {proj_file}")
                return _classify_fraposa(ref_common, ref_common + ".pcs", proj_file, sample_name,
                                         n_common, input_type, vcf_path, bam_path)
            else:
                _log("FRAPOSA output not found, falling back to joint PCA...")
        except Exception as e:
            _log(f"FRAPOSA failed ({e}), falling back to joint PCA...")

    # Fallback: Joint PCA (merge sample + reference via plink --bmerge, then PCA)
    _log("Running joint PCA (plink --bmerge + plink2 --pca)...")

    PLINK1 = os.environ.get("PLINK1", "plink")
    from shutil import which
    plink1 = which("plink") or PLINK1

    # Subset ref to overlapping variants
    ov_ids = os.path.join(tmpdir, "ov_ids.txt")
    subprocess.run(f"awk '{{print $2}}' {sample_ov}.bim > {ov_ids}", shell=True, timeout=30)
    ref_ov = os.path.join(tmpdir, "ref_ov")
    _run([PLINK2, "--bfile", ref_bed, "--extract", ov_ids, "--make-bed", "--out", ref_ov, "--threads", str(threads)])

    # Align sample alleles to reference
    sample_aligned = os.path.join(tmpdir, "sample_aligned")
    _run([PLINK2, "--bfile", sample_ov, "--ref-allele", "force", ref_ov + ".bim", "5", "2",
          "--make-bed", "--out", sample_aligned, "--threads", str(threads)])

    # Merge using plink 1.9 --bmerge
    merged = os.path.join(tmpdir, "merged")
    r = _run([plink1, "--bfile", ref_ov, "--bmerge", sample_aligned, "--make-bed",
              "--out", merged, "--allow-no-sex"], timeout=600)

    if not os.path.exists(merged + ".bed"):
        missnp = merged + "-merge.missnp"
        if os.path.exists(missnp):
            _log(f"Removing {sum(1 for _ in open(missnp))} mismatched SNPs and retrying merge...")
            _run([PLINK2, "--bfile", sample_aligned, "--exclude", missnp,
                  "--make-bed", "--out", sample_aligned + "_clean", "--threads", str(threads)])
            r = _run([plink1, "--bfile", ref_ov, "--bmerge", sample_aligned + "_clean",
                      "--make-bed", "--out", merged, "--allow-no-sex"], timeout=600)

    if not os.path.exists(merged + ".bed"):
        return {"error": f"Merge failed: {r.stderr[:300]}"}

    _log(f"Merged: {sum(1 for _ in open(merged + '.fam'))} samples, {sum(1 for _ in open(merged + '.bim'))} variants")

    # Filter high-missingness samples and variants before PCA
    merged_clean = os.path.join(tmpdir, "merged_clean")
    _run([PLINK2, "--bfile", merged, "--mind", "0.1", "--geno", "0.1", "--maf", "0.01",
          "--make-bed", "--out", merged_clean, "--threads", str(threads)])
    if os.path.exists(merged_clean + ".bed"):
        _log(f"After QC: {sum(1 for _ in open(merged_clean + '.fam'))} samples, {sum(1 for _ in open(merged_clean + '.bim'))} variants")
        merged = merged_clean

    pca_out = os.path.join(tmpdir, "pca")
    r = _run([PLINK2, "--bfile", merged, "--pca", "20", "--out", pca_out, "--threads", str(threads)], timeout=600)

    eigenvec = pca_out + ".eigenvec"
    if not os.path.exists(eigenvec):
        return {"error": f"PCA failed: {r.stderr[:200]}"}

    _log("Classifying ancestry...")
    result = classify(eigenvec, sample_name, n_variants, ref_fam=ref_ov + ".fam", pop2group=pop2group)
    if "error" not in result:
        result["method"] = "joint_pca"
        result["input_type"] = input_type or ("bam" if bam_path and not vcf_path else "vcf")
        result["input_file"] = bam_path if result["input_type"] in ("bam", "cram") else vcf_path
    return result


def _classify_fraposa(ref_bed, ref_pcs_file, stu_pcs_file, sample_name, n_variants,
                       input_type, vcf_path, bam_path):
    """Classify ancestry from FRAPOSA-projected PCA coordinates."""
    pop_labels = load_population_labels()

    # Load reference PCs (skip header if present)
    ref_samples = {}
    with open(ref_pcs_file) as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 12:
                continue
            # Skip header line
            try:
                float(parts[2])
            except ValueError:
                continue
            iid = parts[1]
            ref_samples[iid] = [float(x) for x in parts[2:12]]

    # Load study (sample) projected PCs (skip header if present)
    sample_pcs = None
    with open(stu_pcs_file) as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            try:
                vals = [float(x) for x in parts[2:12]]
                sample_pcs = np.array(vals)
                break  # Only one study sample
            except ValueError:
                continue  # Skip header

    if sample_pcs is None:
        return {"error": "Could not read projected PCA coordinates"}

    # Build population centroids from reference
    superpops = ["EUR", "AFR", "EAS", "SAS", "AMR"]
    pop_pcs = {sp: [] for sp in superpops}
    subpop_pcs = {}
    all_ref_with_pcs = {}

    for iid, pcs in ref_samples.items():
        info = pop_labels.get(iid, {})
        sp = info.get("superpop", "")
        if sp in pop_pcs:
            pop_pcs[sp].append(pcs)
            all_ref_with_pcs[iid] = (pcs, info)
            subpop = info.get("population", "")
            if subpop:
                subpop_pcs.setdefault(subpop, []).append(pcs)

    centroids = {sp: np.mean(v, axis=0) for sp, v in pop_pcs.items() if v}
    pop_names = {"EUR": "European", "AFR": "African", "EAS": "East Asian",
                 "SAS": "South Asian", "AMR": "American (admixed)"}

    # NNLS admixture
    nnls_props = _nnls_admixture(sample_pcs, centroids)
    _log(f"NNLS admixture: {nnls_props}")

    # Distance-weighted KNN
    ref_dists = []
    for iid, (pcs, info) in all_ref_with_pcs.items():
        d = np.linalg.norm(sample_pcs - np.array(pcs))
        ref_dists.append((iid, d, info))
    ref_dists.sort(key=lambda x: x[1])

    dwknn_props = _distance_weighted_knn(sample_pcs, ref_dists, k=100)
    _log(f"Distance-weighted K-NN: {dwknn_props}")

    k = 50
    nn_counts = {}
    for _, _, info in ref_dists[:k]:
        sp = info["superpop"]
        nn_counts[sp] = nn_counts.get(sp, 0) + 1
    nn_props = {sp: round(c / k, 4) for sp, c in nn_counts.items()}
    _log(f"K-NN (K={k}): {nn_props}")

    # Consensus
    all_pops = set(list(nnls_props.keys()) + list(dwknn_props.keys()) + list(nn_props.keys()))
    consensus = {}
    for sp in all_pops:
        v = 0.70 * nnls_props.get(sp, 0) + 0.20 * dwknn_props.get(sp, 0) + 0.10 * nn_props.get(sp, 0)
        consensus[sp] = v
    total_c = sum(consensus.values())
    if total_c > 0:
        consensus = {sp: round(v / total_c, 4) for sp, v in consensus.items()}
    consensus = {sp: v for sp, v in sorted(consensus.items(), key=lambda x: -x[1]) if v >= 0.005}

    primary = max(consensus, key=consensus.get)
    primary_pct = consensus[primary]

    # Nearest subpopulations
    nearest_subpops = {}
    for _, _, info in ref_dists[:20]:
        p = info.get("population", "?")
        nearest_subpops[p] = nearest_subpops.get(p, 0) + 1

    # Sub-population NNLS
    sub_centroids = {sp: np.mean(v, axis=0) for sp, v in subpop_pcs.items() if len(v) >= 10}
    sub_nnls = {}
    if sub_centroids:
        sub_nnls = _nnls_admixture(sample_pcs, sub_centroids)
        sub_nnls = {k: v for k, v in sorted(sub_nnls.items(), key=lambda x: -x[1]) if v >= 0.01}

    distances = {sp: round(float(np.linalg.norm(sample_pcs - c)), 4) for sp, c in centroids.items()}

    return {
        "primary_ancestry": primary,
        "primary_name": pop_names.get(primary, primary),
        "admixture_proportions": consensus,
        "is_admixed": primary_pct < 0.85,
        "method": "fraposa_oadp",
        "methods": {"nnls": nnls_props, "distance_weighted_knn": dwknn_props, "knn_50": nn_props},
        "sub_population_proportions": sub_nnls,
        "nearest_subpopulations": [{"population": p, "count": c} for p, c in sorted(nearest_subpops.items(), key=lambda x: -x[1])[:8]],
        "distances": distances,
        "pca_coordinates": [round(x, 6) for x in sample_pcs.tolist()],
        "variants_used": n_variants,
        "sample_name": sample_name,
        "input_type": input_type or "vcf",
        "input_file": bam_path if input_type in ("bam", "cram") else vcf_path,
    }


def _nnls_admixture(sample_pcs, centroids):
    """Estimate admixture proportions via Non-Negative Least Squares.

    Solves: sample ≈ w1*centroid_EUR + w2*centroid_AFR + ... with w_i >= 0, sum(w) = 1
    This properly handles admixed individuals (e.g., 50% EUR + 50% EAS).
    """
    from scipy.optimize import nnls

    pops = sorted(centroids.keys())
    C = np.column_stack([centroids[p] for p in pops])  # (n_pcs, n_pops)

    # Add a row of ones to enforce sum-to-1 constraint (with weight)
    weight = 10.0  # Strong constraint on sum=1
    C_aug = np.vstack([C, weight * np.ones(len(pops))])
    b_aug = np.append(sample_pcs, weight)

    coeffs, _ = nnls(C_aug, b_aug)

    # Normalize to sum to 1
    total = coeffs.sum()
    if total > 0:
        coeffs = coeffs / total

    return {p: round(float(c), 4) for p, c in zip(pops, coeffs)}


def _distance_weighted_knn(sample_pcs, ref_dists, k=100):
    """K-NN with inverse-distance weighting.

    Closer neighbors contribute more to the ancestry estimate.
    Much better than unweighted K-NN for admixed individuals.
    """
    pop_weight = {}
    for _, d, info in ref_dists[:k]:
        sp = info["superpop"]
        w = 1.0 / (d + 1e-10)
        pop_weight[sp] = pop_weight.get(sp, 0) + w

    total = sum(pop_weight.values())
    return {sp: round(float(w / total), 4) for sp, w in pop_weight.items()}


def _load_pop2group(pop2group_file):
    """Load pop2group mapping: population_name -> group_name."""
    mapping = {}
    if not pop2group_file or not os.path.exists(pop2group_file):
        return mapping
    with open(pop2group_file) as f:
        f.readline()  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                mapping[parts[0]] = parts[1]
    return mapping


def _load_labels_from_fam(fam_file, pop2group=None):
    """Load population labels from .fam FID column. If pop2group provided, map to groups."""
    p2g = _load_pop2group(pop2group) if pop2group else {}
    labels = {}  # iid -> {"population": pop, "group": group}
    with open(fam_file) as f:
        for line in f:
            parts = line.strip().split()
            pop = parts[0]  # FID = population
            iid = parts[1]
            # If pop2group exists, only assign group if the population is mapped
            # Unmapped populations (Ignore_*, rare groups) get group=None and are excluded
            if p2g:
                group = p2g.get(pop)  # None if not mapped
            else:
                group = pop  # No pop2group file — use population as group
            labels[iid] = {"population": pop, "group": group}
    return labels


def classify(eigenvec_file, sample_name, n_variants, ref_fam=None, pop2group=None):
    # Load labels — from .fam if provided, else from PSAM
    if ref_fam and os.path.exists(ref_fam):
        fam_labels = _load_labels_from_fam(ref_fam, pop2group)
    else:
        # Fallback to PSAM-based labels
        psam_labels = load_population_labels()
        fam_labels = {iid: {"population": v.get("population", ""), "group": v.get("superpop", "")}
                      for iid, v in psam_labels.items()}

    # Parse eigenvec — handle both tab and space separated, with or without header
    samples = {}
    with open(eigenvec_file) as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            # Skip header
            try:
                float(parts[2])
            except ValueError:
                continue
            # plink2 eigenvec: FID IID PC1 PC2 ... (tab or space)
            iid = parts[1]
            pcs = [float(x) for x in parts[2:22]]  # up to 20 PCs
            samples[iid] = pcs[:10]  # use first 10

    if sample_name not in samples:
        # Try matching by partial name
        for iid in samples:
            if sample_name in iid:
                sample_name = iid
                break
        if sample_name not in samples:
            return {"error": f"Sample {sample_name} not found in PCA output"}

    sample_pcs = np.array(samples[sample_name])

    # Collect per-group PCA coordinates (only mapped populations)
    p2g = _load_pop2group(pop2group) if pop2group else {}
    mapped_groups = set(p2g.values()) if p2g else None

    group_pcs = {}  # group -> list of PC arrays
    subpop_pcs = {}  # population -> list of PC arrays
    for iid, pcs in samples.items():
        info = fam_labels.get(iid, {})
        group = info.get("group", "")
        pop = info.get("population", "")
        if not group or group == sample_name:
            continue
        # Only include groups that are in the pop2group mapping (skip unmapped individual pops)
        if mapped_groups and group not in mapped_groups:
            continue
        group_pcs.setdefault(group, []).append(pcs)
        if pop:
            subpop_pcs.setdefault(pop, []).append(pcs)

    centroids = {g: np.mean(v, axis=0) for g, v in group_pcs.items() if len(v) >= 3}
    _log(f"Groups with centroids: {len(centroids)} ({', '.join(sorted(centroids.keys())[:10])}...)")

    # ── Method 1: NNLS Admixture ──
    nnls_props = _nnls_admixture(sample_pcs, centroids)
    _log(f"NNLS: {dict(sorted(nnls_props.items(), key=lambda x: -x[1])[:5])}")

    # ── Method 2: Distance-weighted K-NN ──
    ref_dists = []
    for iid, pcs in samples.items():
        info = fam_labels.get(iid, {})
        if info.get("group"):
            d = np.linalg.norm(sample_pcs - np.array(pcs))
            ref_dists.append((iid, d, info))
    ref_dists.sort(key=lambda x: x[1])

    # DW-KNN using groups
    k_dw = min(100, len(ref_dists))
    dw_weights = {}
    for _, d, info in ref_dists[:k_dw]:
        g = info["group"]
        dw_weights[g] = dw_weights.get(g, 0) + 1.0 / (d + 1e-10)
    total_dw = sum(dw_weights.values())
    dwknn_props = {g: round(w / total_dw, 4) for g, w in dw_weights.items()}
    _log(f"DW-KNN: {dict(sorted(dwknn_props.items(), key=lambda x: -x[1])[:5])}")

    # ── Method 3: Simple K-NN ──
    k = min(50, len(ref_dists))
    nn_counts = {}
    for _, _, info in ref_dists[:k]:
        g = info["group"]
        nn_counts[g] = nn_counts.get(g, 0) + 1
    nn_props = {g: round(c / k, 4) for g, c in nn_counts.items()}
    _log(f"KNN: {dict(sorted(nn_props.items(), key=lambda x: -x[1])[:5])}")

    # ── Consensus: NNLS 70%, DW-KNN 20%, KNN 10% ──
    all_groups = set(list(nnls_props.keys()) + list(dwknn_props.keys()) + list(nn_props.keys()))
    consensus = {}
    for g in all_groups:
        consensus[g] = 0.70 * nnls_props.get(g, 0) + 0.20 * dwknn_props.get(g, 0) + 0.10 * nn_props.get(g, 0)
    total_c = sum(consensus.values())
    if total_c > 0:
        consensus = {g: round(v / total_c, 4) for g, v in consensus.items()}
    consensus = {g: v for g, v in sorted(consensus.items(), key=lambda x: -x[1]) if v >= 0.005}

    primary = max(consensus, key=consensus.get)
    primary_pct = consensus[primary]

    # Nearest individuals (fine-scale)
    nearest_pops = {}
    for _, _, info in ref_dists[:20]:
        p = info.get("population", "?")
        nearest_pops[p] = nearest_pops.get(p, 0) + 1

    # Sub-population NNLS
    sub_centroids = {p: np.mean(v, axis=0) for p, v in subpop_pcs.items() if len(v) >= 5}
    sub_nnls = {}
    if sub_centroids:
        sub_nnls = _nnls_admixture(sample_pcs, sub_centroids)
        sub_nnls = {k: v for k, v in sorted(sub_nnls.items(), key=lambda x: -x[1]) if v >= 0.01}

    distances = {g: round(float(np.linalg.norm(sample_pcs - c)), 4) for g, c in centroids.items()}

    return {
        "primary_ancestry": primary,
        "primary_name": primary,
        "admixture_proportions": consensus,
        "is_admixed": bool(primary_pct < 0.85),
        "panel": panel_name if 'panel_name' in dir() else "unknown",
        "methods": {"nnls": nnls_props, "distance_weighted_knn": dwknn_props, "knn_50": nn_props},
        "sub_population_proportions": sub_nnls,
        "nearest_subpopulations": [{"population": p, "count": c} for p, c in sorted(nearest_pops.items(), key=lambda x: -x[1])[:10]],
        "distances": {g: d for g, d in sorted(distances.items(), key=lambda x: x[1])[:10]},
        "pca_coordinates": [round(x, 6) for x in sample_pcs.tolist()],
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
        print(json.dumps(result, indent=2, default=lambda o: float(o) if hasattr(o, '__float__') else bool(o) if isinstance(o, (np.bool_,)) else str(o)))


if __name__ == "__main__":
    main()
