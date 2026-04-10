"""
Test runners for all genomic analysis types.

Each runner takes a VCF path and test params, returns a dict report.

Every result dict includes:
  status:   "passed" | "warning" | "failed"
  headline: short one-line result (shown inline in UI checklist)
  error:    present only if failed (reason string)
"""

import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from threading import Lock

from rs_positions import RS_POSITIONS
try:
    from rsid_list_positions import RSID_LIST_POSITIONS
except ImportError:
    RSID_LIST_POSITIONS = {}

logger = logging.getLogger("simple-genomics")


def _result(status, headline, **extra):
    """Build a standard result dict."""
    d = {"status": status, "headline": headline}
    d.update(extra)
    return d


def _pass(headline, **extra):
    return _result("passed", headline, **extra)


def _warn(headline, error=None, **extra):
    d = _result("warning", headline, **extra)
    if error:
        d["error"] = error
    return d


def _fail(headline, error, **extra):
    return _result("failed", headline, error=error, **extra)

# Tool paths
BCFTOOLS = os.getenv("BCFTOOLS", "/home/nimo/miniconda3/envs/genomics/bin/bcftools")
SAMTOOLS = os.getenv("SAMTOOLS", "/home/nimo/miniconda3/envs/genomics/bin/samtools")
PLINK = os.getenv("PLINK", "/home/nimo/miniconda3/envs/genomics/bin/plink")
PLINK2 = os.getenv("PLINK2", "/home/nimo/miniconda3/envs/genomics/bin/plink2")
REF_FASTA = os.getenv("REF_FASTA", "/data/refs/GRCh38.fa")
# Scaling factor used when extrapolating chr22-only variant counts to a
# whole-genome estimate for BAM/CRAM inputs. Ratio of GRCh38 primary
# assembly length (chr1..22 + chrX + chrY + chrM ≈ 3.088 Gb) to chr22
# length (50.82 Mb). Autosomes only differ from sex-chr density, but for
# a WGS sample-QC heuristic this is close enough.
CHR22_GENOME_SCALE = 3_088_286_401 / 50_818_468  # ≈ 60.77
REF_PANEL = os.getenv("REF_PANEL", "/data/pgs2/ref_panel/GRCh38_1000G_ALL")
REF_PANEL_STATS = os.getenv("REF_PANEL_STATS", "/data/pgs2/ref_panel_stats")
PGS_CACHE = os.getenv("PGS_CACHE", "/data/pgs_cache")
SCRATCH = os.getenv("SCRATCH", "/scratch/simple-genomics")

# Persistent cache for VCF→pgen conversions. Without this every PGS test
# re-converts the same gVCF from scratch (the dominant cost). The cache
# is keyed by VCF realpath + mtime so a file edit invalidates it
# automatically. Lives under /data so it survives /scratch wipes.
PGEN_CACHE = os.getenv("SG_PGEN_CACHE", "/data/pgen_cache/sg")

# Persistent cache for auto-annotated VCFs (CLNSIG/GENEINFO from ClinVar).
# Same keying as the pgen cache — realpath + mtime — so edits invalidate.
CLINVAR_ANNOTATED_CACHE = os.getenv(
    "SG_CLINVAR_ANNOTATED_CACHE", "/data/pgen_cache/clinvar_annotated"
)
# Pre-built ClinVar VCFs on this server: the `_chr` variant has `chr`-prefixed
# contigs, the bare variant has plain integers. We pick based on the input.
CLINVAR_VCF_CHR  = os.getenv("CLINVAR_VCF_CHR",  "/data/clinvar/clinvar_chr.vcf.gz")
CLINVAR_VCF_BARE = os.getenv("CLINVAR_VCF_BARE", "/data/clinvar/clinvar.vcf.gz")

# Thread budgets. The 44-core box can comfortably run NUM_WORKERS test
# tasks concurrently, each spawning plink2/bcftools with these thread
# counts. Defaults are tuned for 4 workers × ~10 threads avg = ~40 cores.
PLINK_BUILD_THREADS = int(os.getenv("PLINK_BUILD_THREADS", "16"))
PLINK_SCORE_THREADS = int(os.getenv("PLINK_SCORE_THREADS", "4"))
BCFTOOLS_THREADS    = int(os.getenv("BCFTOOLS_THREADS", "4"))
# plink2 --memory is a hard cap on its internal allocations. 16 GB is too
# low for a raw gVCF (plink2 OOMs before we can even strip the ref blocks).
# 32 GB × 4 workers = 128 GB fits comfortably in 176 GB of system RAM,
# and the build step happens once per file so even the peak is short.
PLINK_MEMORY_MB     = int(os.getenv("PLINK_MEMORY_MB", "32000"))

# Ensure scratch + pgen cache exist
os.makedirs(SCRATCH, exist_ok=True)
os.makedirs(PGEN_CACHE, exist_ok=True)

# ── pgen build locks ─────────────────────────────────────────────────
# Concurrent workers may all want the same pgen on cold start. The
# per-key lock makes the first worker do the build while the others
# block; once the cache is populated they all pull from disk.
_pgen_locks_lock = Lock()
_pgen_locks = {}  # cache_key -> Lock

_clinvar_locks_lock = Lock()
_clinvar_locks = {}  # cache_key -> Lock

_normgvcf_locks_lock = Lock()
_normgvcf_locks = {}  # normalized-gvcf path -> Lock


def _get_pgen_lock(key):
    with _pgen_locks_lock:
        lk = _pgen_locks.get(key)
        if lk is None:
            lk = Lock()
            _pgen_locks[key] = lk
        return lk


def _get_clinvar_lock(key):
    with _clinvar_locks_lock:
        lk = _clinvar_locks.get(key)
        if lk is None:
            lk = Lock()
            _clinvar_locks[key] = lk
        return lk


def _get_normgvcf_lock(key):
    """Per-output-path lock so two parallel _vcf_to_pgen calls on the same
    gVCF don't both try to write `gvcf_normalized.v3.vcf.gz` simultaneously.
    The two calls have *different* pgen cache keys (PGS-style chr@:# vs
    PCA-style @:#:$r:$a) so the existing _get_pgen_lock doesn't help.
    """
    with _normgvcf_locks_lock:
        lk = _normgvcf_locks.get(key)
        if lk is None:
            lk = Lock()
            _normgvcf_locks[key] = lk
        return lk


def _run(cmd, timeout=600):
    """Run a shell command, return (stdout, stderr, returncode)."""
    logger.info(f"Running: {' '.join(cmd[:6])}...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return result.stdout, result.stderr, result.returncode


def _is_gvcf(vcf_path):
    """Check if a VCF file is actually a gVCF (contains reference blocks).

    Streams only the first data record via bcftools and kills the process
    as soon as we have it — reading the entire VCF through subprocess.run
    accumulates millions of lines in Python memory on a non-gVCF and
    blocks the worker for minutes on a 240 MB VCF.

    Format signals:
      - GATK gVCFs mark blocks with ALT=<NON_REF>
      - DeepVariant gVCFs use ALT=<*>
      - Both set INFO/END=<pos> on block records

    Short-circuits on the filename convention (`.g.vcf.gz` / `.gvcf.gz`)
    first, which is by far the common case and avoids shelling out.
    """
    p = str(vcf_path).lower()
    if p.endswith((".g.vcf.gz", ".g.vcf", ".gvcf.gz", ".gvcf")):
        return True

    proc = None
    try:
        proc = subprocess.Popen(
            [BCFTOOLS, "view", "--no-header", str(vcf_path)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 8:
                return False
            alt = parts[4]
            info = parts[7]
            alleles = set(alt.split(","))
            if "<*>" in alleles or "<NON_REF>" in alleles:
                return True
            for kv in info.split(";"):
                if kv.startswith("END="):
                    return True
            return False
    except Exception:
        return False
    finally:
        if proc is not None:
            try:
                if proc.stdout is not None:
                    proc.stdout.close()
            except Exception:
                pass
            try:
                proc.kill()
                proc.wait(timeout=5)
            except Exception:
                pass
    return False


def _detect_file_type(path):
    """Classify an input file as 'vcf', 'bam', 'cram', or 'unknown' by extension.

    Used by the dispatcher to decide whether a test needs variant data (VCF)
    or read data (BAM/CRAM), and by runners to pick the right tool.
    """
    p = str(path).lower()
    if p.endswith(('.vcf', '.vcf.gz', '.vcf.bgz', '.bcf',
                   '.gvcf', '.gvcf.gz', '.g.vcf.gz')):
        return 'vcf'
    if p.endswith('.bam'):
        return 'bam'
    if p.endswith('.cram'):
        return 'cram'
    return 'unknown'


def _ensure_indexed(vcf_path):
    """Ensure VCF is bgzipped and indexed."""
    if not vcf_path.endswith('.gz'):
        # bgzip it
        gz_path = vcf_path + '.gz'
        if not os.path.exists(gz_path):
            _run([BCFTOOLS, "view", vcf_path, "-Oz", "-o", gz_path])
            _run([BCFTOOLS, "index", "-t", gz_path])
        return gz_path
    # Check for index
    if not os.path.exists(vcf_path + '.tbi') and not os.path.exists(vcf_path + '.csi'):
        _run([BCFTOOLS, "index", "-t", vcf_path])
    return vcf_path


def _ensure_alignment_indexed(aln_path):
    """Ensure a BAM/CRAM has a sibling .bai/.crai index; create one if missing."""
    ftype = _detect_file_type(aln_path)
    if ftype == 'bam':
        if os.path.exists(aln_path + '.bai') or os.path.exists(aln_path[:-4] + '.bai'):
            return aln_path
        _run([SAMTOOLS, 'index', aln_path], timeout=1800)
    elif ftype == 'cram':
        if os.path.exists(aln_path + '.crai'):
            return aln_path
        _run([SAMTOOLS, 'index', '-@', '4', aln_path], timeout=1800)
    return aln_path


def _samtools_count_reads(aln_path, region, min_mapq=20):
    """Count primary alignments in a region of a BAM/CRAM.

    Filters: -q {min_mapq} (min MAPQ), -F 260 (exclude unmapped + secondary).
    For CRAM, passes -T <reference> matched to the alignment's contig naming.
    """
    ftype = _detect_file_type(aln_path)
    cmd = [SAMTOOLS, 'view', '-c', '-q', str(min_mapq), '-F', '260']
    if ftype == 'cram':
        ref = _pick_reference_for(aln_path)
        if not os.path.exists(ref):
            raise RuntimeError(f"CRAM decoding needs a reference at {ref}")
        cmd += ['-T', ref]
    cmd += [aln_path, region]
    stdout, stderr, rc = _run(cmd, timeout=1800)
    if rc != 0:
        raise RuntimeError(f"samtools view failed: {stderr.strip()[:300]}")
    # samtools prints a single number on the last non-empty line
    for line in reversed(stdout.strip().splitlines()):
        try:
            return int(line.strip())
        except ValueError:
            continue
    return 0


def _alignment_has_chrom(aln_path, chrom):
    """Return True if the BAM/CRAM header lists a contig matching `chrom`
    (either bare or with `chr` prefix)."""
    cmd = [SAMTOOLS, 'view', '-H']
    if _detect_file_type(aln_path) == 'cram':
        cmd += ['-T', REF_FASTA]
    cmd += [aln_path]
    stdout, _, rc = _run(cmd, timeout=120)
    if rc != 0:
        return False
    targets = {chrom, f"chr{chrom}"} if not chrom.startswith("chr") else {chrom, chrom[3:]}
    for line in stdout.splitlines():
        if line.startswith('@SQ'):
            for field in line.split('\t'):
                if field.startswith('SN:') and field[3:] in targets:
                    return True
    return False


# Candidate reference fastas. The first whose contig naming matches the
# alignment wins. REF_FASTA from env takes top priority.
_REF_CANDIDATES = [
    REF_FASTA,
    "/data/genom-nimo/reference_chr.fa",  # chr-prefixed GRCh38
    "/data/genom-nimo/reference.fasta",   # bare-chrom GRCh38
    "/data/refs/GRCh38.fa",
]


def _fasta_has_chr_prefix(fasta_path):
    """Return True iff the fasta index lists `chr`-prefixed contigs."""
    fai = fasta_path + ".fai"
    if not os.path.exists(fai):
        return None
    try:
        with open(fai) as f:
            first = f.readline().strip().split("\t")[0]
            return first.startswith("chr")
    except OSError:
        return None


def _pick_reference_for(aln_path):
    """Choose a fasta whose contig naming matches the given BAM/CRAM.

    Reading a CRAM requires a reference whose @SQ SN: names exactly match
    the CRAM header. Our two candidate GRCh38 fastas differ only in chr
    prefix, so picking by prefix is sufficient.
    """
    try:
        aln_has_chr = _detect_alignment_chr_prefix(aln_path)
    except Exception:
        aln_has_chr = True
    for path in _REF_CANDIDATES:
        if not path or not os.path.exists(path):
            continue
        fa_has_chr = _fasta_has_chr_prefix(path)
        if fa_has_chr is None:
            continue
        if fa_has_chr == aln_has_chr:
            return path
    # Fall back to REF_FASTA if nothing matched; the caller will see a
    # clear error from samtools/bcftools.
    return REF_FASTA


def _detect_alignment_chr_prefix(aln_path):
    """Detect whether a BAM/CRAM uses `chr`-prefixed chromosome names.

    Reads the header with `samtools view -H`. For CRAM we don't need the
    reference to read the header, so we pass nothing (avoids a chicken-
    and-egg with _pick_reference_for).
    """
    cmd = [SAMTOOLS, 'view', '-H', str(aln_path)]
    stdout, _, _ = _run(cmd, timeout=120)
    for line in stdout.splitlines():
        if line.startswith('@SQ'):
            for field in line.split('\t'):
                if field.startswith('SN:'):
                    return field[3:].startswith('chr')
    return True


# Cache dir for VCFs derived from BAM/CRAM inputs by on-demand variant
# calling. Keyed by a short hash of the alignment path so repeated tests on
# the same file reuse the cached regions.
CRAM_VCF_CACHE = Path(os.getenv(
    "SIMPLE_GENOMICS_CRAM_VCF_CACHE",
    "/home/nimrod_rotem/simple-genomics/cram_vcf_cache",
))


def _cram_cache_dir(aln_path):
    import hashlib
    h = hashlib.sha1(str(aln_path).encode()).hexdigest()[:12]
    d = CRAM_VCF_CACHE / h
    d.mkdir(parents=True, exist_ok=True)
    return d


def _call_variants_region(aln_path, region, out_vcf_gz, timeout=3600):
    """Extract a region from a BAM/CRAM to a temporary BAM, then call variants.

    Two-step pipeline:
      1. `samtools view --input-fmt-option ignore_md5=1 -b -T REF cram region > slice.bam`
         The ignore_md5 is essential for CRAMs encoded against a reference that
         differs in masked/alt regions (e.g. DRAGEN's hg38_alt_masked_v5 vs
         vanilla GRCh38). We accept that calls inside masked regions may be
         slightly wrong — fine for sex checks and Ti/Tv estimates.
      2. `bcftools mpileup -f REF slice.bam -o pile.bcf`
         `bcftools call -mv pile.bcf -Oz -o out.vcf.gz`

    Returns (ok, error_message).
    """
    _ensure_alignment_indexed(aln_path)
    ref = _pick_reference_for(aln_path)
    if not os.path.exists(ref):
        return False, f"No matching reference fasta found (tried {ref})"

    out_dir = os.path.dirname(str(out_vcf_gz))
    safe = region.replace(":", "_").replace("-", "_")
    slice_bam = os.path.join(out_dir, f"_slice_{safe}.bam")
    pile_bcf = os.path.join(out_dir, f"_pile_{safe}.bcf")

    def cleanup():
        for p in (slice_bam, slice_bam + ".bai", pile_bcf):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass

    try:
        logger.info(f"Extracting {region} slice from {os.path.basename(aln_path)}…")
        _, stderr, rc = _run([
            SAMTOOLS, "view",
            "--input-fmt-option", "ignore_md5=1",
            "-T", ref,
            "-b", "-o", slice_bam,
            str(aln_path), region,
        ], timeout=timeout)
        if rc != 0:
            cleanup()
            err = "\n".join(l for l in stderr.splitlines()
                           if "no version information" not in l)
            return False, f"samtools view failed (rc={rc}): {err[:500]}"
        if not os.path.exists(slice_bam) or os.path.getsize(slice_bam) < 100:
            cleanup()
            return False, "samtools produced an empty slice BAM"

        _, stderr, rc = _run([SAMTOOLS, "index", slice_bam], timeout=600)
        if rc != 0:
            cleanup()
            return False, f"samtools index failed: {stderr[:500]}"

        logger.info(f"Running mpileup on {region} slice…")
        _, stderr, rc = _run([
            BCFTOOLS, "mpileup",
            "-f", ref,
            "--max-depth", "250",
            "-q", "20", "-Q", "20",
            "-a", "FORMAT/AD,FORMAT/DP",
            "-Ou", "-o", pile_bcf,
            slice_bam,
        ], timeout=timeout)
        if rc != 0:
            cleanup()
            return False, f"bcftools mpileup failed: {stderr[:500]}"

        logger.info(f"Running bcftools call on {region}…")
        _, stderr, rc = _run([
            BCFTOOLS, "call", "-mv",
            "-Oz", "-o", str(out_vcf_gz),
            pile_bcf,
        ], timeout=timeout)
        if rc != 0:
            cleanup()
            return False, f"bcftools call failed: {stderr[:500]}"

        # Index so downstream bcftools query/view can use -r.
        _, stderr, rc = _run([BCFTOOLS, "index", "-t", str(out_vcf_gz)], timeout=600)
        if rc != 0:
            cleanup()
            return False, f"bcftools index failed: {stderr[:500]}"
    finally:
        cleanup()

    return True, None


def _get_or_call_region_vcf(aln_path, region):
    """Return a cached per-region VCF derived from the given BAM/CRAM.

    Calls variants lazily on first request. Cached under CRAM_VCF_CACHE/{hash}/{region}.vcf.gz
    so subsequent test runs are instant.
    """
    safe = region.replace(":", "_").replace("-", "_")
    cache_dir = _cram_cache_dir(aln_path)
    cached = cache_dir / f"{safe}.vcf.gz"
    if cached.exists() and cached.stat().st_size > 0:
        return str(cached), None
    ok, err = _call_variants_region(aln_path, region, cached)
    if not ok:
        # Remove the partial file so the next request retries cleanly.
        try:
            if cached.exists():
                cached.unlink()
        except OSError:
            pass
        return None, err
    return str(cached), None


def _detect_chr_prefix(vcf_path):
    """Detect if VCF uses 'chr' prefix on chromosome names."""
    opener = gzip.open if vcf_path.endswith('.gz') else open
    try:
        with opener(vcf_path, 'rt') as f:
            for line in f:
                if line.startswith('#'):
                    if line.startswith('##contig=<ID=chr'):
                        return True
                    if line.startswith('##contig=<ID=') and not line.startswith('##contig=<ID=chr'):
                        return False
                    continue
                # First data line
                return line.startswith('chr')
    except Exception:
        pass
    return True  # default to chr prefix


def _vcf_sample_names(vcf_path):
    """Return the list of sample IIDs in a VCF."""
    stdout, _, rc = _run([BCFTOOLS, "query", "-l", vcf_path])
    if rc != 0:
        return []
    return [s.strip() for s in stdout.splitlines() if s.strip()]


def _normalize_gvcf(vcf_path, out_path):
    """Convert a gVCF into a plink2-friendly VCF for PGS scoring + PCA.

    A naive `bcftools view --exclude '<*>'` discards every reference-block
    record, which makes plink2 fast but kills PGS scoring: ~half of any
    PGS scoring file's positions are hom-ref in the sample and need to
    appear in the pgen as 0-dose records, otherwise --score skips them
    and the match rate collapses to ~50%.

    The pipeline:
      1. Build a union of PGS catalog positions + 1000G PCA panel positions
         (~7.34M sites). PCA needs the panel positions filled in or its
         projection lands in the wrong region of PC space, exactly the
         same class of bug we already fixed for the variant-only VCF case.
      2. `bcftools convert --gvcf2vcf -T <union> --targets-overlap 1` —
         expands gVCF blocks into per-position records, including blocks
         whose START isn't a target but which span one (default
         --targets-overlap=0 silently drops them).
      3. Python pass to rewrite leftover `<*>`/`<NON_REF>` ALTs to the
         PGS/PCA-panel-expected allele so plink2 --score can A1-match
         at zero-dose hom-ref calls. Variant records (real ALT) pass
         through unchanged.

    NOTE: this output is *only* for plink2 (PGS scoring + PCA via
    `_vcf_to_pgen`). Tests that need genome-wide variants — ROH, ClinVar
    screens, sex checks, Y/mtDNA — read the *raw* gVCF directly through
    bcftools query/stats and don't go through this function. Conflating
    the two needs in one normalized output has been tried; concat'ing
    a 199M-record expanded VCF with a 9M-record variants VCF tickles a
    bcftools concat -D bug that drops most of the hom-ref records
    (rewritten_homref collapsed from 4.6M → 240K, prostate match rate
    fell from 94% → 45%). Keeping the normalize PGS/PCA-only avoids this.
    """
    work_dir = os.path.dirname(out_path) or "/tmp"
    os.makedirs(work_dir, exist_ok=True)
    expanded_vcf = out_path + ".expanded.vcf.gz"

    # 1. Build / re-use the union-of-positions TSV (PGS + PCA panel).
    union_positions_tsv = "/data/pgs_cache/_all_pgs_pca_positions_chr.tsv"
    if not os.path.exists(union_positions_tsv):
        logger.info(f"Building PGS+PCA union positions file at {union_positions_tsv}...")
        _build_all_pgs_positions(union_positions_tsv, include_pca_panel=True)

    # 2. Pick a reference fasta whose chrom naming matches the gVCF
    ref = _pick_reference_for(vcf_path)
    if not os.path.exists(ref):
        raise RuntimeError(f"Reference fasta not found at {ref} (needed for gVCF expansion)")

    # 3. Expand blocks at PGS+PCA panel positions. --targets-overlap 1 is
    # critical — see docstring.
    logger.info(f"Expanding gVCF blocks at PGS+PCA positions: {vcf_path}")
    _, stderr, rc = _run([
        BCFTOOLS, "convert",
        "--gvcf2vcf",
        "-f", ref,
        "-T", union_positions_tsv,
        "--targets-overlap", "1",
        "-Oz", "-o", expanded_vcf,
        str(vcf_path),
    ], timeout=3600)
    if rc != 0:
        raise RuntimeError(f"bcftools convert --gvcf2vcf failed: {stderr[:500]}")
    _run([BCFTOOLS, "index", "-t", expanded_vcf], timeout=600)

    # 4. Rewrite ALT='<*>' / '<NON_REF>' to the PGS/PCA-expected effect
    # allele at each position so plink2 --score can match A1. Variant
    # records (real ALT) are passed through unchanged.
    logger.info(f"Rewriting <*> ALT to PGS effect alleles → {out_path}")
    _rewrite_gvcf_placeholder_alts(expanded_vcf, out_path)
    _run([BCFTOOLS, "index", "-t", out_path], timeout=600)

    # Clean up intermediates
    for f in (expanded_vcf, expanded_vcf + ".tbi"):
        try:
            if os.path.exists(f):
                os.remove(f)
        except OSError:
            pass

    return out_path


def _build_all_pgs_positions(out_path, include_pca_panel=False):
    """Build the union of (chr, pos) across every cached PGS scoring file.

    Output is a sorted, deduplicated TSV with chr-prefixed CHROM, suitable
    for `bcftools convert -T` / `bcftools view -T`.

    When `include_pca_panel=True`, also adds the ~106K LD-pruned 1000G
    PCA panel positions from the PCA reference cache (`ref.eigenvec.allele`).
    Without this, gVCF normalization would build a pgen with PGS positions
    only — and PCA projection on that pgen silently mislabels the sample
    because the panel positions weren't filled in.
    """
    seen = set()
    pgs_dir = Path(PGS_CACHE)
    for sub in sorted(pgs_dir.glob("PGS*")):
        for f in sub.glob("*_hmPOS_GRCh38.txt.gz"):
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
                        except ValueError:
                            break
                        continue
                    if len(parts) <= max(chr_idx, pos_idx):
                        continue
                    chrom, pos = parts[chr_idx], parts[pos_idx]
                    if not chrom or not pos or chrom == "NA" or not pos.isdigit():
                        continue
                    key = (f"chr{chrom}" if not chrom.startswith("chr") else chrom,
                           int(pos))
                    seen.add(key)

    if include_pca_panel:
        eigenvec_allele = os.path.join(PGS_CACHE, "pca_1000g", "ref.eigenvec.allele")
        if os.path.exists(eigenvec_allele):
            n_added = 0
            with open(eigenvec_allele) as f:
                next(f)  # header
                for line in f:
                    parts = line.split("\t")
                    if len(parts) < 2:
                        continue
                    var_id = parts[1]   # e.g. "1:13868:A:G" or "chr1:13868:A:G"
                    bits = var_id.split(":")
                    if len(bits) < 2 or not bits[1].isdigit():
                        continue
                    chrom = bits[0]
                    if not chrom.startswith("chr"):
                        chrom = f"chr{chrom}"
                    key = (chrom, int(bits[1]))
                    if key not in seen:
                        seen.add(key)
                        n_added += 1
            logger.info(f"Added {n_added:,} PCA panel positions to union")
        else:
            logger.warning(f"PCA panel allele file missing at {eigenvec_allele}; "
                           f"normalized gVCF won't have PCA panel coverage")

    # Sort by chrom (natural order chr1..22, X, Y, M) then position
    def chrom_key(c):
        c = c.removeprefix("chr")
        return (0, int(c)) if c.isdigit() else (1, c)
    rows = sorted(seen, key=lambda x: (chrom_key(x[0]), x[1]))
    # Atomic write: build the full file under a unique temp name in the
    # same directory, then rename. Two parallel callers (PGS-style and
    # PCA-style pgen builders racing on the same gVCF) won't see a
    # half-written union file. Without this, a SIGTERM mid-build leaves
    # a truncated file that silently breaks every subsequent expansion.
    out_dir = os.path.dirname(out_path) or "."
    os.makedirs(out_dir, exist_ok=True)
    tmp_path = f"{out_path}.tmp.{os.getpid()}"
    with open(tmp_path, "w") as f:
        for chrom, pos in rows:
            f.write(f"{chrom}\t{pos}\n")
    os.replace(tmp_path, out_path)
    label = "PGS+PCA union" if include_pca_panel else "PGS union"
    logger.info(f"Wrote {len(rows):,} {label} positions to {out_path}")


def _rewrite_gvcf_placeholder_alts(in_vcf, out_vcf):
    """Replace ALT='<*>'/'<NON_REF>' with the PGS catalog's effect allele.

    Same idea as scripts/fix_pgs_sites_alt.py, but operates on the
    expanded-gVCF VCF (which is much smaller than the source gVCF). For
    positions where every PGS-listed allele equals REF, the record is
    dropped — plink2 would skip it anyway.
    """
    # Build allele lookup once. For each PGS row we need *both* alleles
    # so the rewrite step can pick whichever one isn't REF. Two columns
    # may carry the "other" base:
    #   - other_allele:        the original (possibly unharmonized) base
    #   - hm_inferOtherAllele: filled in when harmonization had to infer it
    # Many PGS files (e.g. PGS000662) leave hm_inferOtherAllele empty
    # because the original other_allele already matches the GRCh38 strand,
    # so we read BOTH columns and store any single-base ACGT we see.
    #
    # Also seed the lookup with the 1000G PCA panel REF/ALT pairs from
    # ref.eigenvec.allele — those positions need rewriting too so PCA
    # projection on a gVCF works (without this, plink2 --score sees
    # ALT='<*>' at the panel positions and skips them, recreating the
    # AFR mislabel bug we hit on the original CRAM run).
    allele_map = {}
    pgs_dir = Path(PGS_CACHE)
    for sub in pgs_dir.glob("PGS*"):
        for f in sub.glob("*_hmPOS_GRCh38.txt.gz"):
            with gzip.open(f, "rt") as fh:
                header = None
                ea_idx = chr_idx = pos_idx = None
                oa_indices = []
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
                        # Collect *all* candidate other-allele columns
                        oa_indices = [header.index(c) for c in
                                      ("hm_inferOtherAllele", "other_allele")
                                      if c in header]
                        continue
                    if len(parts) <= max(chr_idx, pos_idx, ea_idx):
                        continue
                    chrom, pos = parts[chr_idx], parts[pos_idx]
                    if not chrom or not pos.isdigit():
                        continue
                    key = (f"chr{chrom}" if not chrom.startswith("chr") else chrom,
                           int(pos))
                    alleles = allele_map.setdefault(key, set())
                    ea = parts[ea_idx].strip() if parts[ea_idx] else ""
                    if ea and len(ea) == 1 and ea in "ACGT":
                        alleles.add(ea)
                    for oai in oa_indices:
                        if oai >= len(parts):
                            continue
                        oa = parts[oai].strip()
                        if oa and len(oa) == 1 and oa in "ACGT":
                            alleles.add(oa)

    eigenvec_allele = os.path.join(PGS_CACHE, "pca_1000g", "ref.eigenvec.allele")
    if os.path.exists(eigenvec_allele):
        with open(eigenvec_allele) as f:
            header = f.readline().strip().split("\t")
            try:
                id_idx = header.index("ID")
                ref_idx = header.index("REF")
                alt_idx = header.index("ALT")
            except ValueError:
                id_idx = ref_idx = alt_idx = None
            if id_idx is not None:
                for line in f:
                    parts = line.split("\t")
                    if len(parts) <= max(id_idx, ref_idx, alt_idx):
                        continue
                    var_id = parts[id_idx]
                    bits = var_id.split(":")
                    if len(bits) < 2 or not bits[1].isdigit():
                        continue
                    chrom = bits[0]
                    if not chrom.startswith("chr"):
                        chrom = f"chr{chrom}"
                    key = (chrom, int(bits[1]))
                    alleles = allele_map.setdefault(key, set())
                    for a in (parts[ref_idx], parts[alt_idx]):
                        if a and len(a) == 1 and a in "ACGT":
                            alleles.add(a)

    placeholders = {"<*>", "<NON_REF>"}
    reader = subprocess.Popen(
        [BCFTOOLS, "view", str(in_vcf)],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    writer = subprocess.Popen(
        [BCFTOOLS, "view", "-Oz", "-o", str(out_vcf), "-"],
        stdin=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    n_total = n_rewritten = n_kept = n_stripped = 0
    n_drop_multibase = n_drop_no_alleles = n_drop_alt_eq_ref = 0
    for line in reader.stdout:
        if line.startswith("#"):
            writer.stdin.write(line)
            continue
        n_total += 1
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 5:
            continue
        chrom, pos, _, ref, alt = parts[0], parts[1], parts[2], parts[3], parts[4]

        if alt not in placeholders:
            # Variant record. gvcf2vcf appends ',<*>'/'<NON_REF>' to every
            # ALT to denote "any other allele" — that placeholder confuses
            # plink2's variance-standardize at variant 1:16949:A:C-style
            # sites because the second allele has frequency 0/NaN in
            # --read-freq. Strip the placeholder so we end up with a
            # clean biallelic ALT (e.g. 'C,<*>' → 'C').
            cleaned = ",".join(a for a in alt.split(",")
                              if a not in placeholders)
            if cleaned and cleaned != alt:
                parts[4] = cleaned
                n_stripped += 1
                writer.stdin.write("\t".join(parts) + "\n")
            else:
                n_kept += 1
                writer.stdin.write(line)
            continue

        # Normalise the REF to uppercase — gvcf2vcf preserves the soft-
        # masked lower-case bases from the reference fasta, but PGS
        # effect alleles are always uppercase ACGT and plink2 expects a
        # consistent case for matching.
        ref_uc = ref.upper()
        if len(ref_uc) != 1 or ref_uc not in "ACGT":
            n_drop_multibase += 1
            continue
        parts[3] = ref_uc

        alleles = allele_map.get((chrom, int(pos)))
        if not alleles:
            n_drop_no_alleles += 1
            continue
        non_ref = [a for a in alleles if a != ref_uc]
        alt_allele = non_ref[0] if non_ref else next(iter(alleles))
        if alt_allele == ref_uc:
            n_drop_alt_eq_ref += 1
            continue
        parts[4] = alt_allele
        n_rewritten += 1
        writer.stdin.write("\t".join(parts) + "\n")

    writer.stdin.close()
    writer.wait()
    reader.wait()
    n_dropped = n_drop_multibase + n_drop_no_alleles + n_drop_alt_eq_ref
    logger.info(
        f"gVCF rewrite: total={n_total} kept_variants={n_kept} "
        f"stripped_placeholder={n_stripped} "
        f"rewritten_homref={n_rewritten} dropped={n_dropped} "
        f"(multibase={n_drop_multibase}, no_alleles={n_drop_no_alleles}, "
        f"alt_eq_ref={n_drop_alt_eq_ref})"
    )


def _vcf_to_pgen(vcf_path, output_prefix, var_id_template="chr@:#",
                 output_chr=None):
    """Convert VCF to plink2 pgen format with chr-prefix variant IDs.

    Multi-allelic sites and overlapping records at the same position would all
    get ID "chrN:pos", which breaks --score (plink2 errors with "appears
    multiple times in main dataset"). Use --rm-dup force-first to drop the
    duplicates while keeping the first record at each position.

    var_id_template: plink2 --set-all-var-ids template. Default "chr@:#" matches
    the existing PGS scoring file format. For PCA projection against the 1000G
    reference (which uses bare-chrom REF:ALT IDs), pass "@:#:$r:$a" with
    output_chr="26".
    output_chr: optional --output-chr code to normalize chromosome naming
    (e.g. "26" for bare numeric, "chrM" for chr-prefixed).

    If the VCF contains chrX, plink2 requires sex information to decide whether
    males should be haploid on non-PAR chrX. We write a sidecar sex file with
    all samples set to 'unknown' so plink2 proceeds (chrX is kept as diploid),
    and use --split-par b38 to isolate the PAR regions.
    """
    work_dir = os.path.dirname(output_prefix) or "."

    # If the input is a gVCF, expand its hom-ref blocks at PGS positions
    # and rewrite the <*> placeholder ALT to the PGS effect allele. The
    # result is plink2-friendly for both PGS scoring and other tests.
    # This is heavy work (~minutes) so it's cached under CRAM_VCF_CACHE
    # keyed by a hash of the gVCF path; subsequent tests on the same
    # gVCF reuse the cache instantly.
    input_for_plink = vcf_path
    cleanup_norm = False
    if _is_gvcf(vcf_path):
        cache_dir = _cram_cache_dir(vcf_path)
        # Schema-versioned filename so future _normalize_gvcf changes auto-
        # invalidate stale cached normalized VCFs without manual cleanup.
        norm_path = str(cache_dir / f"gvcf_normalized.{PGEN_CACHE_SCHEMA}.vcf.gz")
        # Per-path lock: PGS-style and PCA-style pgen builds run in
        # parallel and would both try to write this same file. Lock so
        # the first worker does the build and the rest reuse the cache.
        norm_lock = _get_normgvcf_lock(norm_path)
        with norm_lock:
            if not (os.path.exists(norm_path) and os.path.getsize(norm_path) > 0):
                logger.info(f"Normalizing gVCF (expanding ref blocks at PGS+PCA sites): {vcf_path} → {norm_path}")
                _normalize_gvcf(vcf_path, norm_path)
            else:
                logger.info(f"Using cached normalized gVCF: {norm_path}")
        input_for_plink = norm_path
        cleanup_norm = False  # cache is persistent; do not delete

    # Write a sex file with a header so plink2 resolves the SEX column by name.
    # sex=0 = unknown; combined with --split-par this lets chrX import succeed.
    samples = _vcf_sample_names(input_for_plink)
    sex_file = output_prefix + ".sex.txt"
    with open(sex_file, "w") as fh:
        fh.write("#IID\tSEX\n")
        for iid in samples:
            fh.write(f"{iid}\t0\n")

    # Stage 1: VCF → pgen with --split-par + sex update + ID normalization +
    # dedup. --split-par is required when chrX PAR variants are present; it
    # moves PAR into a separate XY chromosome which then needs resorting.
    stage1_prefix = output_prefix + "_unsorted"
    cmd1 = [
        PLINK2,
        "--vcf", input_for_plink,
        "--make-pgen",
        "--allow-extra-chr",
        "--split-par", "b38",
        "--update-sex", sex_file,
        "--vcf-half-call", "m",
        "--threads", str(PLINK_BUILD_THREADS),
        "--memory", str(PLINK_MEMORY_MB),
    ]
    if output_chr:
        cmd1 += ["--output-chr", output_chr]
    cmd1 += [
        "--set-all-var-ids", var_id_template,
        "--new-id-max-allele-len", "100", "missing",
        "--rm-dup", "force-first",
        "--out", stage1_prefix,
    ]
    stdout, stderr, rc = _run(cmd1, timeout=3600)
    if rc != 0:
        raise RuntimeError(f"plink2 VCF->pgen failed: {stderr[:500]}")

    # Stage 2: sort variants so downstream --score etc. work. plink2 requires
    # --sort-vars be the only command, so this is a separate invocation.
    cmd2 = [
        PLINK2,
        "--pfile", stage1_prefix,
        "--make-pgen",
        "--sort-vars",
        "--allow-extra-chr",
        "--threads", str(PLINK_BUILD_THREADS),
        "--memory", str(PLINK_MEMORY_MB),
    ]
    if output_chr:
        cmd2 += ["--output-chr", output_chr]
    cmd2 += ["--out", output_prefix]
    stdout, stderr, rc = _run(cmd2, timeout=3600)
    if rc != 0:
        raise RuntimeError(f"plink2 sort-vars failed: {stderr[:500]}")

    # Clean up stage1 intermediate
    for ext in (".pgen", ".pvar", ".psam", ".log"):
        f = stage1_prefix + ext
        if os.path.exists(f):
            try:
                os.remove(f)
            except OSError:
                pass
    # Clean up the normalized intermediate (we keep only the pgen)
    if cleanup_norm and norm_path:
        for suffix in ("", ".tbi", ".csi"):
            try:
                os.remove(norm_path + suffix)
            except OSError:
                pass

    return output_prefix


# Schema version for the pgen + normalized-gVCF caches. Bump whenever
# `_normalize_gvcf` or `_vcf_to_pgen` changes how the on-disk artifacts
# are produced — the new version segregates the cache so old clients
# don't pick up stale builds. Just changing this constant is enough; no
# manual `rm -rf` needed.
#
# Versions:
#   v1 (implicit): original schema, no version in key
#   v2: --gvcf2vcf normalization with PGS positions only
#   v3: PCA panel positions added, *with* genome-wide variants concat —
#       broken: bcftools concat -D dropped most hom-ref records, prostate
#       collapsed back from 94% → 45% match
#   v4: PCA panel positions kept, but variant concat reverted (genome-
#       wide-variant tests read the raw gVCF directly via bcftools, not
#       through this normalize). Restores prostate ≥94% match while
#       still feeding PCA the panel positions it needs.
#   v5: also strip the gvcf2vcf `<*>`/`<NON_REF>` "any other allele"
#       placeholder from multi-allelic ALTs (e.g. 'C,<*>' → 'C'). Without
#       this, plink2 --score variance-standardize barfs on records like
#       1:16949:A:C-style PCA panel hits whose second allele has zero
#       reference frequency, breaking PCA on gVCF input.
PGEN_CACHE_SCHEMA = "v5"


def _pgen_cache_key(vcf_path, var_id_template, output_chr):
    """Stable key for the pgen cache: realpath + variant args + schema."""
    real = os.path.realpath(vcf_path)
    h_path = hashlib.sha1(real.encode()).hexdigest()[:16]
    h_var = hashlib.sha1(
        f"{var_id_template}|{output_chr or ''}|{PGEN_CACHE_SCHEMA}".encode()
    ).hexdigest()[:8]
    return f"{h_path}_{h_var}"


def _get_or_build_pgen(vcf_path, var_id_template="chr@:#", output_chr=None):
    """Return a cached pgen prefix for this VCF, building it if missing/stale.

    Cache layout: ``$PGEN_CACHE/<key>/sample.{pgen,pvar,psam}`` where the key
    encodes both the VCF realpath and the requested var_id_template/output_chr
    (so PGS-style and PCA-style conversions don't collide).

    Freshness is checked against the source VCF mtime; if it changes the
    cache rebuilds. Concurrent callers serialize on a per-key Lock so the
    first caller does the build and the rest reuse the result.
    """
    key = _pgen_cache_key(vcf_path, var_id_template, output_chr)
    cache_dir = os.path.join(PGEN_CACHE, key)
    prefix = os.path.join(cache_dir, "sample")
    stamp = os.path.join(cache_dir, ".vcf_mtime")

    try:
        vcf_mtime = os.path.getmtime(vcf_path)
    except OSError:
        vcf_mtime = 0.0

    def _cache_is_fresh():
        if not (os.path.exists(prefix + ".pgen") and
                os.path.exists(prefix + ".pvar") and
                os.path.exists(prefix + ".psam") and
                os.path.exists(stamp)):
            return False
        try:
            with open(stamp) as f:
                cached = float(f.read().strip())
            return abs(cached - vcf_mtime) < 1e-6
        except (OSError, ValueError):
            return False

    # Fast path: cache hit without taking the lock
    if _cache_is_fresh():
        logger.info(f"pgen cache hit: {prefix}")
        return prefix

    lock = _get_pgen_lock(key)
    with lock:
        # Re-check inside the lock — another worker may have built it
        # while we were waiting.
        if _cache_is_fresh():
            logger.info(f"pgen cache hit (after wait): {prefix}")
            return prefix

        # Wipe any stale partial cache
        if os.path.exists(cache_dir):
            try:
                shutil.rmtree(cache_dir)
            except OSError:
                pass
        os.makedirs(cache_dir, exist_ok=True)

        logger.info(f"Building pgen cache: {vcf_path} → {prefix}")
        try:
            _vcf_to_pgen(vcf_path, prefix,
                         var_id_template=var_id_template,
                         output_chr=output_chr)
        except Exception:
            # Don't leave a half-built cache that future calls would
            # mistake for fresh.
            try:
                shutil.rmtree(cache_dir)
            except OSError:
                pass
            raise

        with open(stamp, "w") as f:
            f.write(str(vcf_mtime))
        logger.info(f"pgen cache built: {prefix}")
        return prefix


def _vcf_has_clinvar_annotations(vcf_path):
    """Return True iff the VCF header declares CLNSIG and GENEINFO fields,
    which are the two fields our ClinVar screening runner needs."""
    stdout, _, rc = _run([BCFTOOLS, "view", "-h", str(vcf_path)], timeout=120)
    if rc != 0:
        return False
    return "ID=CLNSIG" in stdout and "ID=GENEINFO" in stdout


def _pick_clinvar_vcf(vcf_path):
    """Pick the ClinVar VCF whose contig naming matches the sample VCF."""
    has_chr = _detect_chr_prefix(vcf_path)
    return CLINVAR_VCF_CHR if has_chr else CLINVAR_VCF_BARE


def _ensure_clinvar_annotated(vcf_path):
    """Return a VCF path that definitely has CLNSIG+GENEINFO annotations.

    Fast path: the input already has them → returned unchanged.
    Slow path: shell out to `bcftools annotate` once per file, cache the
    output under CLINVAR_ANNOTATED_CACHE keyed by realpath + mtime, and
    return the cached path. Subsequent ClinVar-screen tests on the same
    VCF reuse the cache.

    For gVCF inputs the pipeline is `view --exclude blocks | annotate`,
    streamed in one pass — annotating the raw gVCF would otherwise be 10×
    slower because every reference-block record is annotated and then
    discarded.
    """
    if _vcf_has_clinvar_annotations(vcf_path):
        return vcf_path

    clinvar_vcf = _pick_clinvar_vcf(vcf_path)
    if not os.path.exists(clinvar_vcf):
        raise RuntimeError(
            f"ClinVar VCF not found at {clinvar_vcf}. "
            f"Set CLINVAR_VCF_CHR / CLINVAR_VCF_BARE env vars or install it."
        )

    key = hashlib.sha1(os.path.realpath(vcf_path).encode()).hexdigest()[:16]
    cache_dir = os.path.join(CLINVAR_ANNOTATED_CACHE, key)
    annotated = os.path.join(cache_dir, "sample.annotated.vcf.gz")
    stamp = os.path.join(cache_dir, ".vcf_mtime")

    try:
        vcf_mtime = os.path.getmtime(vcf_path)
    except OSError:
        vcf_mtime = 0.0

    def _fresh():
        if not (os.path.exists(annotated) and
                os.path.exists(annotated + ".tbi") and
                os.path.exists(stamp)):
            return False
        try:
            with open(stamp) as f:
                return abs(float(f.read().strip()) - vcf_mtime) < 1e-6
        except (OSError, ValueError):
            return False

    if _fresh():
        logger.info(f"ClinVar-annotated cache hit: {annotated}")
        return annotated

    lock = _get_clinvar_lock(key)
    with lock:
        if _fresh():
            return annotated
        if os.path.exists(cache_dir):
            try:
                shutil.rmtree(cache_dir)
            except OSError:
                pass
        os.makedirs(cache_dir, exist_ok=True)

        # Make sure the input is bgzipped+indexed so bcftools annotate can
        # stream it efficiently.
        src = _ensure_indexed(vcf_path)

        if _is_gvcf(src):
            # Two-step: drop ref blocks to a temp VCF, index it, then
            # annotate. We can't stream this through a pipe because
            # bcftools annotate requires an index on its input (its joins
            # are random-access). Even with the disk write, this is still
            # ~10× faster than annotating the raw gVCF in place because
            # the stripped intermediate is ~250 MB instead of ~3.9 GB.
            stripped = os.path.join(cache_dir, "stripped.vcf.gz")
            logger.info(f"Stripping gVCF blocks: {src} → {stripped}")
            stdout, stderr, rc = _run([
                BCFTOOLS, "view",
                "--threads", str(BCFTOOLS_THREADS),
                "--exclude", 'N_ALT=1 && (ALT="<NON_REF>" || ALT="<*>")',
                "-Oz", "-o", stripped,
                src,
            ], timeout=3600)
            if rc != 0:
                try:
                    shutil.rmtree(cache_dir)
                except OSError:
                    pass
                raise RuntimeError(
                    f"bcftools view (strip blocks) failed: {stderr[:500]}"
                )
            stdout, stderr, rc = _run(
                [BCFTOOLS, "index", "--threads", str(BCFTOOLS_THREADS), "-t", stripped],
                timeout=600,
            )
            if rc != 0:
                try:
                    shutil.rmtree(cache_dir)
                except OSError:
                    pass
                raise RuntimeError(
                    f"bcftools index (stripped) failed: {stderr[:500]}"
                )

            logger.info(f"Annotating stripped {stripped} → {annotated}")
            stdout, stderr, rc = _run([
                BCFTOOLS, "annotate",
                "--threads", str(BCFTOOLS_THREADS),
                "-a", clinvar_vcf,
                "-c", "INFO/CLNSIG,INFO/GENEINFO",
                "-Oz", "-o", annotated,
                stripped,
            ], timeout=3600)
            if rc != 0:
                try:
                    shutil.rmtree(cache_dir)
                except OSError:
                    pass
                raise RuntimeError(
                    f"bcftools annotate (stripped) failed: {stderr[:500]}"
                )

            # Drop the intermediate; we only keep the final annotated cache.
            for ext in ("", ".tbi", ".csi"):
                try:
                    os.remove(stripped + ext)
                except OSError:
                    pass
        else:
            logger.info(f"Annotating {src} with ClinVar → {annotated}")
            stdout, stderr, rc = _run([
                BCFTOOLS, "annotate",
                "--threads", str(BCFTOOLS_THREADS),
                "-a", clinvar_vcf,
                "-c", "INFO/CLNSIG,INFO/GENEINFO",
                "-Oz", "-o", annotated,
                src,
            ], timeout=3600)
            if rc != 0:
                try:
                    shutil.rmtree(cache_dir)
                except OSError:
                    pass
                raise RuntimeError(
                    f"bcftools annotate failed: {stderr[:500] or stdout[:500]}"
                )

        stdout, stderr, rc = _run(
            [BCFTOOLS, "index", "--threads", str(BCFTOOLS_THREADS), "-t", annotated],
            timeout=600,
        )
        if rc != 0:
            try:
                shutil.rmtree(cache_dir)
            except OSError:
                pass
            raise RuntimeError(f"bcftools index failed: {stderr[:500]}")

        with open(stamp, "w") as f:
            f.write(str(vcf_mtime))
        logger.info(f"ClinVar-annotated cache built: {annotated}")
        return annotated


# ─── Variant Lookup Runner ───────────────────────────────────────

def _lookup_variant(vcf_path, rs, has_chr_prefix):
    """Query a VCF for a single variant. Tries rsID first, then GRCh38 position.
    Returns dict with keys: found, chrom, pos, ref, alt, genotype, source."""
    # 1. Try by rsID first (fast when annotated)
    stdout, _, _ = _run([
        BCFTOOLS, "query",
        "-f", "%CHROM\t%POS\t%ID\t%REF\t%ALT\t[%GT]\n",
        "-i", f'ID="{rs}"',
        vcf_path
    ])
    if stdout.strip():
        parts = stdout.strip().split('\n')[0].split('\t')
        if len(parts) >= 6:
            return {"found": True, "chrom": parts[0], "pos": parts[1],
                    "ref": parts[3], "alt": parts[4], "genotype": parts[5],
                    "source": "rsID"}

    # 2. Fallback: position-based lookup. Prefer the curated RS_POSITIONS
    # table (hand-verified, trait-specific entries); fall back to the much
    # larger RSID_LIST_POSITIONS auto-generated from NCBI dbSNP.
    pos_entry = RS_POSITIONS.get(rs) or RSID_LIST_POSITIONS.get(rs)
    if pos_entry:
        chrom_bare, pos, ref, alt = pos_entry
        chrom = f"chr{chrom_bare}" if has_chr_prefix else chrom_bare
        region = f"{chrom}:{pos}-{pos}"

        stdout, _, _ = _run([
            BCFTOOLS, "query",
            "-r", region,
            "-f", "%CHROM\t%POS\t%REF\t%ALT\t[%GT]\n",
            vcf_path
        ])
        if stdout.strip():
            # There may be multiple records at this position; pick the one matching alt
            for line in stdout.strip().split('\n'):
                parts = line.split('\t')
                if len(parts) >= 5:
                    return {"found": True, "chrom": parts[0], "pos": parts[1],
                            "ref": parts[2], "alt": parts[3], "genotype": parts[4],
                            "source": "position", "expected_ref": ref, "expected_alt": alt}

        # Position known but no variant call — means homozygous reference
        return {"found": False, "chrom": chrom, "pos": str(pos),
                "ref": ref, "alt": alt, "genotype": "0/0 (ref/ref — no variant)",
                "source": "position-inferred"}

    # 3. No rsID match and no position data
    return {"found": False, "genotype": "Not in VCF (no position data)", "source": "none"}


def run_variant_lookup(vcf_path, params):
    """Look up specific variants by rs number in a VCF."""
    vcf_path = _ensure_indexed(vcf_path)
    has_chr = _detect_chr_prefix(vcf_path)
    variants = params.get("variants", [])
    disease = params.get("disease", "Unknown")
    interpretation = params.get("interpretation", None)

    results = []
    for var in variants:
        rs = var["rs"]
        gene = var.get("gene", "")
        name = var.get("name", rs)

        look = _lookup_variant(vcf_path, rs, has_chr)
        results.append({
            "variant": rs,
            "gene": gene,
            "name": name,
            **look,
        })

    # Classify: how many have a position-resolvable result?
    resolved = [r for r in results if r.get("source") in ("rsID", "position", "position-inferred")]
    found_alt = [r for r in results if r["found"]]
    unresolvable = [r for r in results if r.get("source") == "none"]

    # APOE interpretation
    apoe_status = None
    if interpretation == "apoe" and len(results) == 2:
        apoe_status = _interpret_apoe(results)

    # Status logic:
    # - FAILED: no variants resolvable at all (no rsID and no position data)
    # - WARNING: variants resolved but all are ref/ref (genotype inferred, not a direct call)
    # - PASSED: at least one variant has a real genotype call
    if not resolved:
        status = "failed"
        err = f"None of the {len(variants)} variant(s) are in the VCF and no position data is available for fallback lookup."
        headline = f"No data — {len(variants)} variant(s) unresolvable"
    elif not found_alt:
        status = "passed"  # ref/ref is still a valid result
        headline = "All ref/ref (no alt calls)"
    else:
        status = "passed"
        if apoe_status:
            headline = f"APOE: {apoe_status['genotype']} — {apoe_status['risk']}"
        elif len(found_alt) == 1:
            r = found_alt[0]
            headline = f"{r['gene']} {r['name']}: {r['genotype']}"
        else:
            headline = f"{len(found_alt)}/{len(results)} alt calls: " + ", ".join(
                f"{r['gene']}={r['genotype']}" for r in found_alt[:3])

    report = {
        "test_type": "variant_lookup",
        "disease": disease,
        "variants": results,
        "summary": _summarize_variants(results, disease),
        "status": status,
        "headline": headline,
    }
    if status == "failed":
        report["error"] = err
    if apoe_status:
        report["apoe_status"] = apoe_status

    return report


def _normalize_gt(gt_string):
    """Return a canonical dosage code for a genotype string.
    Returns: "ref" (0/0), "het" (0/1 or 1/0), "hom" (1/1), or "unknown"."""
    if not gt_string:
        return "unknown"
    # Handle inferred ref/ref from position-based lookup
    if "0/0" in gt_string or "0|0" in gt_string or "ref/ref" in gt_string:
        return "ref"
    if "1/1" in gt_string or "1|1" in gt_string:
        return "hom"
    if "0/1" in gt_string or "1/0" in gt_string or "0|1" in gt_string or "1|0" in gt_string:
        return "het"
    return "unknown"


def _interpret_apoe(results):
    """Interpret APOE genotype from rs429358 and rs7412.

    APOE alleles are defined by the combination of two SNPs:
      rs429358 (T>C at position 112): C is the e4 allele
      rs7412   (C>T at position 158): T is the e2 allele
    Haplotype table:
      rs429358=T, rs7412=C → e3
      rs429358=T, rs7412=T → e2
      rs429358=C, rs7412=C → e4
      rs429358=C, rs7412=T → e1 (very rare)
    """
    # Accept both found and position-inferred results (ref/ref is valid info)
    gt_map = {r["variant"]: r.get("genotype", "") for r in results}

    g429 = _normalize_gt(gt_map.get("rs429358", ""))
    g7412 = _normalize_gt(gt_map.get("rs7412", ""))

    if g429 == "unknown" or g7412 == "unknown":
        return {"genotype": "Could not determine", "risk": "Unknown"}

    # rs429358 dosage of C allele (e4-defining): ref=0, het=1, hom=2
    # rs7412   dosage of T allele (e2-defining): ref=0, het=1, hom=2
    dose_429 = {"ref": 0, "het": 1, "hom": 2}[g429]
    dose_7412 = {"ref": 0, "het": 1, "hom": 2}[g7412]

    # From dosages, derive the two alleles. Each person has 2 copies.
    # Non-e1 combinations:
    #   (0,0) = e3/e3
    #   (0,1) = e2/e3   (one e2 = one T at 7412)
    #   (0,2) = e2/e2
    #   (1,0) = e3/e4   (one e4 = one C at 429)
    #   (1,1) = e2/e4  (one e2 from 7412, one e4 from 429 — assumes not in cis)
    #   (2,0) = e4/e4
    #   (2,1) = e1/e4  (very rare; flagged)
    combo = (dose_429, dose_7412)
    mapping = {
        (0, 0): ["e3", "e3"],
        (0, 1): ["e2", "e3"],
        (0, 2): ["e2", "e2"],
        (1, 0): ["e3", "e4"],
        (1, 1): ["e2", "e4"],
        (1, 2): ["e1", "e2"],  # rare
        (2, 0): ["e4", "e4"],
        (2, 1): ["e1", "e4"],  # rare
        (2, 2): ["e1", "e1"],  # extremely rare
    }
    alleles = mapping.get(combo)
    if not alleles:
        return {"genotype": "Could not determine", "risk": "Unknown"}

    genotype = "/".join(sorted(alleles))
    risk = {
        "e2/e2": "Reduced risk (~0.6x)",
        "e2/e3": "Slightly reduced risk (~0.6x)",
        "e2/e4": "Average to slightly elevated risk",
        "e3/e3": "Average risk (reference)",
        "e3/e4": "Elevated risk (~3-4x)",
        "e4/e4": "High risk (~12-15x)",
    }
    return {
        "genotype": genotype,
        "risk": risk.get(genotype, f"Unknown (rare combination {combo})"),
    }


def _summarize_variants(results, disease):
    """Generate a text summary of variant lookup results."""
    lines = [f"Variant lookup for: {disease}"]
    for r in results:
        src = r.get("source", "none")
        if r["found"]:
            lines.append(f"  {r['gene']} {r['name']} ({r['variant']}): {r['genotype']} [via {src}]")
        else:
            gt = r.get("genotype", "not found")
            lines.append(f"  {r['gene']} {r['name']} ({r['variant']}): {gt} [via {src}]")
    return "\n".join(lines)


# ─── VCF Stats Runner ────────────────────────────────────────────

def run_vcf_stats(vcf_path, params):
    """Run VCF/BAM statistics for QC and sex checks.

    The three read-count sex methods (`y_read_count`, `sry_presence`,
    `xy_ratio`) work on BAM/CRAM inputs via samtools. Everything else
    requires a VCF and will fail cleanly if given an alignment file.
    """
    method = params.get("method", "")
    ftype = _detect_file_type(vcf_path)

    # Read-count sex checks dispatch on file type — these are the only
    # methods in this runner that work directly on alignments without
    # needing variant calls at all.
    if method in ("y_read_count", "sry_presence", "xy_ratio"):
        if ftype in ("bam", "cram"):
            return _sex_from_alignment(vcf_path, method)
        vcf_path = _ensure_indexed(vcf_path)
        return _sex_from_vcf(vcf_path, method)

    # For BAM/CRAM inputs, some variant-based methods can still run after
    # on-demand region-scoped variant calling. The derived VCF is cached
    # under CRAM_VCF_CACHE so subsequent tests on the same region are fast.
    if ftype in ("bam", "cram"):
        has_chr = _detect_alignment_chr_prefix(vcf_path)
        if method == "var_chry":
            region = "chrY" if has_chr else "Y"
            derived, err = _get_or_call_region_vcf(vcf_path, region)
            if not derived:
                return _fail("var_chry: variant calling failed",
                             err or "unknown error", test_type="vcf_stats")
            return _count_chry_variants(derived)

        if method == "het_chrx":
            region = "chrX" if has_chr else "X"
            derived, err = _get_or_call_region_vcf(vcf_path, region)
            if not derived:
                return _fail("het_chrx: variant calling failed",
                             err or "unknown error", test_type="vcf_stats")
            return _het_chrx(derived)

        # Ti/Tv and Het/Hom are chromosome-agnostic ratios, so a chr22
        # slice (~70K SNVs on WGS) gives a stable estimate without the
        # cost of genome-wide calling. SNP/indel counts need the raw
        # chr22 count scaled to genome-wide by length ratio.
        if method in ("titv_ratio", "het_hom_ratio", "snp_count", "indel_count"):
            region = "chr22" if has_chr else "22"
            derived, err = _get_or_call_region_vcf(vcf_path, region)
            if not derived:
                label = {
                    "titv_ratio": "titv_ratio",
                    "het_hom_ratio": "het_hom_ratio",
                    "snp_count": "snp_count",
                    "indel_count": "indel_count",
                }[method]
                return _fail(f"{label}: variant calling failed",
                             err or "unknown error", test_type="vcf_stats")

            if method == "snp_count":
                return _count_variants(derived, "snps",
                                       scale_factor=CHR22_GENOME_SCALE,
                                       source_region=region)
            if method == "indel_count":
                return _count_variants(derived, "indels",
                                       scale_factor=CHR22_GENOME_SCALE,
                                       source_region=region)

            # Ti/Tv and Het/Hom are ratios — compute on chr22 directly
            # and decorate the summary so the user knows this is a
            # chr22-only estimate rather than the usual genome-wide metric.
            result = (_titv_ratio(derived) if method == "titv_ratio"
                      else _het_hom_ratio(derived))
            if isinstance(result, dict):
                note = f"(estimated from {region} variant calling on {os.path.basename(vcf_path)})"
                if result.get("summary"):
                    result["summary"] = f"{result['summary']} {note}"
                if result.get("headline"):
                    result["headline"] = f"{result['headline']} [{region}]"
                result["source_region"] = region
            return result

        # Any other VCF-only methods still need full-genome calling, which
        # is too expensive to do on demand. Fail cleanly.
        return _fail(
            f"{method}: requires VCF input",
            f"This test needs genome-wide variant calls and cannot run "
            f"on a {ftype.upper()} file ({os.path.basename(vcf_path)}). "
            f"Convert to VCF first (e.g. `bcftools mpileup -f REF | bcftools call -mv`) "
            f"or add a VCF in the file manager.",
            test_type="vcf_stats",
        )

    vcf_path = _ensure_indexed(vcf_path)

    # bcftools stats counts every record, including the millions of `<*>`
    # gVCF reference-block records, which spectacularly inflates SNP /
    # indel counts and tanks Ti/Tv (1.15 instead of ~2.0 on a real WGS).
    # For genome-wide QC stats on a gVCF, work off a stripped variants-only
    # version. This is also fast (~1 min) once cached, and we share the
    # same per-input lock to avoid parallel rebuilds.
    #
    # We also need to strip the `<*>` placeholder from *multi-allelic*
    # variant records (e.g. `G,<*>` → `G`). gVCFs add the placeholder to
    # every variant ALT to denote "any other allele"; bcftools stats then
    # counts each `G,<*>` as a multi-allelic SNP and double-counts it,
    # which inflated SNP count from the real ~3.9M to 7.9M and tanked
    # Ti/Tv. Both fixes are applied in the same `bcftools view` pass:
    # `--exclude` drops pure-placeholder records, `--trim-alt-alleles` +
    # a Python sed loop trims the trailing `<*>` from the survivors.
    if _is_gvcf(vcf_path) and method in (
        "titv_ratio", "het_hom_ratio", "snp_count", "indel_count",
    ):
        cache_dir = _cram_cache_dir(vcf_path)
        stripped_path = str(cache_dir / f"variants_only.{PGEN_CACHE_SCHEMA}.vcf.gz")
        norm_lock = _get_normgvcf_lock(stripped_path)
        with norm_lock:
            if not (os.path.exists(stripped_path) and os.path.getsize(stripped_path) > 0):
                logger.info(f"Stripping gVCF ref blocks for QC stats: {vcf_path} → {stripped_path}")
                # Two-stage: bcftools drops pure-placeholder records AND
                # any records where the sample is hom-ref (GT="ref") or
                # missing (GT="miss") — gVCFs include these as
                # "candidate variant" records with a real ALT and 0/0 GT,
                # and bcftools stats counts them as SNPs (the 7.9M figure
                # we kept seeing). Then a Python pipe rewrites multi-
                # allelic ALTs to drop the trailing <*>/<NON_REF>.
                placeholders = {"<*>", "<NON_REF>"}
                reader = subprocess.Popen(
                    [BCFTOOLS, "view",
                     "--exclude",
                     '(N_ALT=1 && (ALT="<NON_REF>" || ALT="<*>")) '
                     '|| GT="ref" || GT="miss"',
                     str(vcf_path)],
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                )
                writer = subprocess.Popen(
                    [BCFTOOLS, "view", "-Oz", "-o", stripped_path, "-"],
                    stdin=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                )
                try:
                    for line in reader.stdout:
                        if line.startswith("#"):
                            writer.stdin.write(line)
                            continue
                        parts = line.rstrip("\n").split("\t")
                        if len(parts) >= 5:
                            alt = parts[4]
                            if "," in alt:
                                cleaned = ",".join(a for a in alt.split(",")
                                                   if a not in placeholders)
                                if cleaned:
                                    parts[4] = cleaned
                                    writer.stdin.write("\t".join(parts) + "\n")
                                    continue
                        writer.stdin.write(line)
                finally:
                    writer.stdin.close()
                    writer.wait()
                    reader.wait()
                if not (os.path.exists(stripped_path) and os.path.getsize(stripped_path) > 0):
                    return _fail(f"{method}: gVCF strip failed",
                                 "Stripped output is missing or empty",
                                 test_type="vcf_stats")
                _run([BCFTOOLS, "index", "-t", stripped_path], timeout=600)
            else:
                logger.info(f"Using cached variants-only gVCF: {stripped_path}")
        vcf_path = stripped_path

    if method == "titv_ratio":
        return _titv_ratio(vcf_path)
    elif method == "het_hom_ratio":
        return _het_hom_ratio(vcf_path)
    elif method == "snp_count":
        return _count_variants(vcf_path, "snps")
    elif method == "indel_count":
        return _count_variants(vcf_path, "indels")
    elif method == "var_chry":
        return _count_chry_variants(vcf_path)
    elif method == "het_chrx":
        return _het_chrx(vcf_path)
    else:
        return _fail(f"Unknown stats method: {method}",
                     f"Method '{method}' not recognized", test_type="vcf_stats")


def _titv_ratio(vcf_path):
    stdout, stderr, rc = _run([BCFTOOLS, "stats", vcf_path])
    if rc != 0:
        return _fail("Ti/Tv: bcftools failed", stderr[:300], test_type="vcf_stats")

    titv = None
    for line in stdout.split('\n'):
        if line.startswith('TSTV'):
            parts = line.split('\t')
            if len(parts) > 4:
                try:
                    titv = float(parts[4])
                except (ValueError, IndexError):
                    pass

    if titv is None:
        return _fail("Ti/Tv: could not parse", "TSTV line not found in bcftools stats output",
                     test_type="vcf_stats")

    in_range = 1.9 <= titv <= 2.2
    headline = f"Ti/Tv = {titv:.3f} ({'PASS' if in_range else 'out of range'})"
    summary = f"Ti/Tv ratio: {titv:.3f} (expected 2.0-2.1 for WGS)"
    return (_pass(headline, test_type="vcf_stats", method="Ti/Tv ratio",
                  value=titv, expected="2.0-2.1 for WGS", summary=summary)
            if in_range else
            _warn(headline, test_type="vcf_stats", method="Ti/Tv ratio",
                  value=titv, expected="2.0-2.1 for WGS", summary=summary))


def _het_hom_ratio(vcf_path):
    # -s - enables per-sample stats (the PSC line we need)
    stdout, stderr, rc = _run([BCFTOOLS, "stats", "-s", "-", vcf_path], timeout=1800)
    if rc != 0:
        return _fail("Het/Hom: bcftools failed", stderr[:300], test_type="vcf_stats")

    n_het = n_hom = 0
    # PSC columns: id, sample, nRefHom, nNonRefHom, nHets, ...
    for line in stdout.split('\n'):
        if line.startswith('PSC\t'):
            parts = line.split('\t')
            if len(parts) >= 7:
                try:
                    n_hom = int(parts[4])  # nNonRefHom
                    n_het = int(parts[5])  # nHets
                except (ValueError, IndexError):
                    pass
                break

    if n_hom == 0 and n_het == 0:
        return _fail("Het/Hom: no counts", "Could not parse PSC line from bcftools stats",
                     test_type="vcf_stats")

    ratio = n_het / n_hom if n_hom > 0 else None
    if ratio is None:
        return _fail("Het/Hom: no homozygous variants", "n_hom = 0", test_type="vcf_stats")

    in_range = 1.3 <= ratio <= 2.5
    headline = f"Het/Hom = {ratio:.3f} (het={n_het:,}, hom={n_hom:,})"
    extra = dict(test_type="vcf_stats", method="Het/Hom ratio",
                 het_count=n_het, hom_count=n_hom, value=round(ratio, 3),
                 expected="~1.5-2.0 for outbred",
                 summary=f"Het/Hom: {ratio:.3f} (het={n_het:,}, hom={n_hom:,})")
    return _pass(headline, **extra) if in_range else _warn(headline, **extra)


def _count_variants(vcf_path, vtype, scale_factor=1.0, source_region=None):
    """Count SNPs or indels in a VCF and compare against WGS expectations.

    When called with a chr22-only VCF (derived from lazy BAM/CRAM slice
    calling), pass `scale_factor` ≈ 60.77 and `source_region="chr22"` so
    the raw count is extrapolated to a whole-genome estimate before the
    range check, and the result makes it explicit that the number is an
    estimate.
    """
    stdout, stderr, rc = _run([BCFTOOLS, "stats", vcf_path])
    if rc != 0:
        return _fail(f"{vtype.upper()} count: bcftools failed", stderr[:300], test_type="vcf_stats")

    raw_count = 0
    key = "number of SNPs" if vtype == "snps" else "number of indels"
    for line in stdout.split('\n'):
        if line.startswith('SN') and key in line:
            parts = line.split('\t')
            try:
                raw_count = int(parts[-1].strip())
            except (ValueError, IndexError):
                pass

    if raw_count == 0:
        return _fail(f"{vtype.upper()} count: 0", f"No {vtype} found in VCF",
                     test_type="vcf_stats")

    count = int(round(raw_count * scale_factor)) if scale_factor != 1.0 else raw_count

    if vtype == "snps":
        in_range = 3_000_000 <= count <= 5_000_000
        expected = "3.5-4.5M for WGS"
    else:
        in_range = 400_000 <= count <= 1_000_000
        expected = "500K-800K for WGS"

    if source_region:
        headline = f"{vtype.upper()}: ~{count:,} [{source_region}×{scale_factor:.1f}]"
        summary = (f"{vtype.upper()} count: ~{count:,} (expected {expected}) — "
                   f"extrapolated from {raw_count:,} {source_region} variants")
    else:
        headline = f"{vtype.upper()}: {count:,}"
        summary = f"{vtype.upper()} count: {count:,} (expected {expected})"

    extra = dict(test_type="vcf_stats", method=f"{vtype.upper()} count",
                 value=count, expected=expected, summary=summary)
    if source_region:
        extra["raw_count"] = raw_count
        extra["source_region"] = source_region
        extra["scale_factor"] = round(scale_factor, 2)
    return _pass(headline, **extra) if in_range else _warn(headline, **extra)


def _count_chry_variants(vcf_path):
    has_chr = _detect_chr_prefix(vcf_path)
    region = "chrY" if has_chr else "Y"
    stdout, stderr, rc = _run([
        BCFTOOLS, "view", "-r", region, vcf_path, "--no-header"
    ])
    if rc != 0:
        return _fail("chrY: query failed", stderr[:300], test_type="vcf_stats")

    count = len([l for l in stdout.strip().split('\n') if l.strip()]) if stdout.strip() else 0

    # If chrY has zero records, we cannot distinguish "female" from
    # "autosomes-only VCF where chrY was never called". Check the companion
    # chrX as well: a real female call requires chrX to be present and chrY
    # absent. If BOTH chrX and chrY are empty, the VCF is autosomes-only.
    if count == 0:
        x_present = _vcf_has_chrom(vcf_path, "X")
        if not x_present:
            return _warn(
                "chrY variants: 0 — VCF lacks sex-chromosome data",
                "This VCF has no variant calls on chrY or chrX, so sex cannot "
                "be determined from it. Use a VCF that includes sex-chromosome "
                "variant calls.",
                test_type="vcf_stats", method="chrY variant count",
                value=0, inferred_sex="unknown",
                expected="Males: >1000; Females: ~0",
                summary="chrY variants: 0 — cannot infer sex (VCF lacks chrY/chrX)")
        sex = "Female"
    else:
        sex = "Male" if count > 500 else "Female"

    headline = f"chrY variants: {count:,} → {sex}"
    return _pass(headline, test_type="vcf_stats", method="chrY variant count",
                 value=count, inferred_sex=sex,
                 expected="Males: >1000; Females: ~0",
                 summary=f"chrY variants: {count:,} — Inferred sex: {sex}")


def _vcf_has_chrom(vcf_path, chrom):
    """Check if VCF has any data lines on the given chromosome.
    `chrom` should be bare (e.g. 'X', 'Y'); chr prefix is added automatically if needed."""
    has_chr = _detect_chr_prefix(vcf_path)
    region = f"chr{chrom}" if has_chr else chrom
    stdout, _, _ = _run([BCFTOOLS, "view", "-r", region, vcf_path, "--no-header"])
    return bool(stdout.strip())


def _het_chrx(vcf_path):
    has_chr = _detect_chr_prefix(vcf_path)
    region = "chrX" if has_chr else "X"

    stdout, stderr, rc = _run([
        BCFTOOLS, "query", "-r", region,
        "-f", "[%GT]\n",
        "-i", 'GT="het"',
        vcf_path
    ])
    if rc != 0:
        return _fail("chrX het: query failed", stderr[:300], test_type="vcf_stats")
    het_count = len(stdout.strip().split('\n')) if stdout.strip() else 0

    stdout2, _, _ = _run([
        BCFTOOLS, "query", "-r", region,
        "-f", "[%GT]\n",
        vcf_path
    ])
    total = len(stdout2.strip().split('\n')) if stdout2.strip() else 0

    if total == 0:
        return _warn(
            "chrX het: no chrX variants (autosomes-only VCF)",
            "This VCF appears to be autosomes-only (no variants on chromosome X). "
            "Sex-chromosome checks require a VCF that includes chrX/chrY variant calls.",
            test_type="vcf_stats", method="chrX heterozygosity",
            summary="chrX het rate: not computable — VCF lacks chrX variant calls")

    het_rate = het_count / total
    # Males under diploid calling show ~5-20% het on chrX (PAR regions + call
    # noise in non-PAR hemizygous sites). Females show ~35-55% het because
    # both X copies produce genuine heterozygous sites. 0.30 cleanly
    # separates the two regimes for standard WGS callsets.
    sex = "Female" if het_rate > 0.30 else "Male"
    headline = f"chrX het rate: {het_rate:.3f} → {sex}"
    return _pass(headline, test_type="vcf_stats", method="chrX heterozygosity",
                 het_count=het_count, total_chrx_variants=total,
                 het_rate=round(het_rate, 4), inferred_sex=sex,
                 summary=f"chrX het rate: {het_rate:.4f} ({het_count:,}/{total:,}) — {sex}")


def _sex_from_alignment(aln_path, method):
    """Sex checks that count actual reads in a BAM/CRAM.

    For WGS, male chrY ≈ 1M+ primary reads, female ≈ low thousands from
    mapping artefacts. SRY region typically has >10 reads in males and 0
    in females. These thresholds are loose but robust.
    """
    _ensure_alignment_indexed(aln_path)
    has_chr = _detect_alignment_chr_prefix(aln_path)
    chrY = "chrY" if has_chr else "Y"
    chrX = "chrX" if has_chr else "X"

    if method == "y_read_count":
        try:
            count = _samtools_count_reads(aln_path, chrY)
        except RuntimeError as e:
            return _fail("chrY: samtools failed", str(e), test_type="alignment_stats")
        sex = "Male" if count > 500_000 else "Female"
        headline = f"chrY reads: {count:,} → {sex}"
        return _pass(headline, test_type="alignment_stats", method="chrY read count",
                     value=count, inferred_sex=sex,
                     expected="Males: >1M reads; Females: ~0",
                     summary=f"chrY reads: {count:,} — Inferred sex: {sex}")

    elif method == "sry_presence":
        # SRY is at chrY:2786989-2787603 (GRCh38)
        region = f"{chrY}:2786989-2787603"
        try:
            count = _samtools_count_reads(aln_path, region)
        except RuntimeError as e:
            return _fail("SRY: samtools failed", str(e), test_type="alignment_stats")
        sex = "Male (SRY reads present)" if count >= 10 else "Female (no SRY reads)"
        headline = f"SRY reads: {count} → {sex.split(' (')[0]}"
        return _pass(headline, test_type="alignment_stats", method="SRY gene reads",
                     value=count, inferred_sex=sex,
                     expected="Males: >10 reads; Females: 0",
                     summary=f"SRY region reads: {count} — {sex}")

    elif method == "xy_ratio":
        try:
            x_reads = _samtools_count_reads(aln_path, chrX)
            y_reads = _samtools_count_reads(aln_path, chrY)
        except RuntimeError as e:
            return _fail("X:Y ratio: samtools failed", str(e), test_type="alignment_stats")
        # Typical male WGS X:Y read ratio is ~2-15 (chrX is 155 Mb, chrY is
        # 57 Mb, so even with equal per-base coverage males expect ~2.7×;
        # mappability, PAR, repeat masking push it higher in practice). For
        # a female chrY mapping is noise-floor, so the ratio blows up to
        # the hundreds or is effectively infinite.
        if y_reads == 0:
            ratio_str = "inf"
            sex = "Female"
        else:
            ratio = x_reads / y_reads
            sex = "Male" if 1.5 < ratio < 50 else "Female"
            ratio_str = f"{ratio:.1f}"
        headline = f"X:Y = {ratio_str} ({x_reads:,}:{y_reads:,}) → {sex}"
        return _pass(headline, test_type="alignment_stats", method="X:Y read ratio",
                     x_reads=x_reads, y_reads=y_reads, ratio=ratio_str,
                     inferred_sex=sex,
                     summary=f"X:Y reads: {x_reads:,}:{y_reads:,} = {ratio_str} — {sex}")

    return _fail(f"Unknown alignment sex method: {method}",
                 f"Method '{method}' not implemented for BAM/CRAM",
                 test_type="alignment_stats")


def _sex_from_vcf(vcf_path, method):
    """Sex checks that work from VCF (without BAM)."""
    has_chr = _detect_chr_prefix(vcf_path)

    if method == "y_read_count":
        return _count_chry_variants(vcf_path)

    elif method == "sry_presence":
        # SRY is at chrY:2786989-2787603 (GRCh38)
        region = "chrY:2786989-2787603" if has_chr else "Y:2786989-2787603"
        stdout, stderr, rc = _run([BCFTOOLS, "view", "-r", region, vcf_path, "--no-header"])
        if rc != 0:
            return _fail("SRY: query failed", stderr[:300], test_type="vcf_stats")

        count = len([l for l in stdout.strip().split('\n') if l.strip()]) if stdout.strip() else 0

        # 0 SRY variants is ambiguous: could be female, or could be a VCF that
        # lacks any chrY calls at all. Disambiguate by checking whole chrY.
        if count == 0 and not _vcf_has_chrom(vcf_path, "Y"):
            return _warn(
                "SRY: no chrY variant calls in VCF",
                "This VCF contains no variant calls on chromosome Y, so SRY "
                "presence cannot be evaluated. The sample could still be male; "
                "use a VCF with chrY variant calls to check sex.",
                test_type="vcf_stats", method="SRY gene variants",
                value=0, inferred_sex="unknown",
                summary="SRY: not evaluable — VCF has no chrY variant calls")

        sex = "Male (SRY variants present)" if count > 0 else "Female (no SRY variants)"
        headline = f"SRY: {count} variants → {sex.split(' (')[0]}"
        return _pass(headline, test_type="vcf_stats", method="SRY gene variants",
                     value=count, inferred_sex=sex,
                     summary=f"SRY region variants: {count} — {sex}")

    elif method == "xy_ratio":
        x_region = "chrX" if has_chr else "X"
        y_region = "chrY" if has_chr else "Y"
        xout, _, xrc = _run([BCFTOOLS, "view", "-r", x_region, vcf_path, "--no-header"])
        yout, _, yrc = _run([BCFTOOLS, "view", "-r", y_region, vcf_path, "--no-header"])

        if xrc != 0:
            return _fail("X:Y ratio: chrX query failed", "bcftools error", test_type="vcf_stats")

        x_count = len([l for l in xout.strip().split('\n') if l.strip()]) if xout.strip() else 0
        y_count = len([l for l in yout.strip().split('\n') if l.strip()]) if yout.strip() else 0

        if x_count == 0:
            return _warn(
                "X:Y ratio: no chrX variants (autosomes-only VCF)",
                "This VCF appears to be autosomes-only (no variants on chromosome X). "
                "Sex-chromosome checks require a VCF that includes chrX/chrY variant calls.",
                test_type="vcf_stats", method="X:Y variant ratio",
                x_variants=0, y_variants=y_count,
                summary="X:Y ratio: not computable — VCF lacks chrX variant calls")

        if y_count == 0:
            # chrX present but chrY empty: this could legitimately be a female
            # sample, OR a VCF where chrY was simply not called. We can't
            # distinguish without more context, so flag as a warning rather
            # than confidently claiming "Female".
            return _warn(
                f"X:Y = {x_count:,}:0 — chrY absent (female or chrY not called)",
                "chrY has no variant calls. This is consistent with a female "
                "sample, but could also indicate a VCF where chrY variants "
                "were not called. Check Y-chromosome coverage before "
                "concluding sex.",
                test_type="vcf_stats", method="X:Y variant ratio",
                x_variants=x_count, y_variants=0, ratio="inf",
                inferred_sex="unknown (likely female)",
                summary=f"X:Y: {x_count:,}:0 — ambiguous (female, or chrY not called)")

        ratio = x_count / y_count
        sex = "Male" if 1 < ratio < 100 else "Female"
        headline = f"X:Y = {ratio:.1f} ({x_count:,}:{y_count:,}) → {sex}"
        return _pass(headline, test_type="vcf_stats", method="X:Y variant ratio",
                     x_variants=x_count, y_variants=y_count, ratio=round(ratio, 2),
                     inferred_sex=sex,
                     summary=f"X:Y ratio: {x_count:,}:{y_count:,} = {ratio:.1f} — {sex}")

    return _fail(f"Unknown sex method: {method}", f"Method '{method}' not implemented",
                 test_type="vcf_stats")


# ─── PGS Scoring Runner ─────────────────────────────────────────

def run_pgs_score(vcf_path, params):
    """Download PGS scoring file from catalog, convert VCF to pgen, run plink2 --score."""
    pgs_id = params["pgs_id"]
    trait = params.get("trait", pgs_id)

    with tempfile.TemporaryDirectory(dir=SCRATCH, prefix=f"pgs_{pgs_id}_") as tmpdir:
        # Step 1: Download scoring file
        scoring_file = _download_pgs_scoring_file(pgs_id, tmpdir)
        if not scoring_file:
            return _fail(f"{pgs_id}: download failed",
                         f"Could not download scoring file from PGS Catalog for {pgs_id}",
                         test_type="pgs_score", pgs_id=pgs_id, trait=trait)

        # Step 2: Prepare plink2-format scoring file
        plink2_scoring = os.path.join(tmpdir, f"{pgs_id}_plink2.tsv")
        metadata = _prepare_plink2_scoring(scoring_file, plink2_scoring)

        if metadata.get("variant_count", 0) == 0:
            return _fail(f"{pgs_id}: no usable variants",
                         f"Scoring file has 0 parseable variants",
                         test_type="pgs_score", pgs_id=pgs_id, trait=trait)

        # Step 3: Convert VCF to pgen — but reuse a persistent cache so we
        # only pay this cost once per file across all PGS tests.
        vcf_path = _ensure_indexed(vcf_path)
        try:
            pgen_prefix = _get_or_build_pgen(vcf_path)
        except RuntimeError as e:
            return _fail(f"{pgs_id}: VCF→pgen conversion failed", str(e),
                         test_type="pgs_score", pgs_id=pgs_id, trait=trait)

        # Step 4: Run plink2 --score
        score_prefix = os.path.join(tmpdir, "score_result")
        cmd = [
            PLINK2,
            "--pfile", pgen_prefix,
            "--score", plink2_scoring,
            "header-read", "1", "2", "3",
            "cols=+scoresums",
            "no-mean-imputation",
            "list-variants",
            "--allow-extra-chr",
            "--threads", str(PLINK_SCORE_THREADS),
            "--memory", str(PLINK_MEMORY_MB),
            "--out", score_prefix,
        ]
        stdout, stderr, rc = _run(cmd, timeout=600)

        # Step 5: Parse results
        sscore_path = f"{score_prefix}.sscore"
        if not os.path.exists(sscore_path):
            return _fail(f"{pgs_id}: plink2 scoring failed", stderr[:500],
                         test_type="pgs_score", pgs_id=pgs_id, trait=trait)

        result = _parse_sscore(sscore_path)

        # Count matched variants
        vars_file = f"{score_prefix}.sscore.vars"
        matched = 0
        if os.path.exists(vars_file):
            with open(vars_file) as f:
                matched = sum(1 for _ in f)

        total = metadata.get("variant_count", 0)
        match_rate_pct = (matched / total * 100) if total else 0

        if matched == 0:
            return {
                "status": "failed",
                "test_type": "pgs_score",
                "pgs_id": pgs_id,
                "trait": trait,
                "matched_variants": 0,
                "total_variants": total,
                "match_rate": "0.0%",
                "match_rate_value": 0.0,
                "no_report": True,
                "headline": f"{trait}: PGS failed (0/{total:,} variants matched — chromosome/build mismatch?)",
                "error": f"plink2 scored 0 of {total:,} variants — chromosome naming or build mismatch likely",
            }

        # Match rate too low to produce a meaningful PGS. Skip percentile
        # computation entirely and return a failure stub flagged as
        # no_report so the UI hides the View button.
        if match_rate_pct < 60:
            return {
                "status": "failed",
                "test_type": "pgs_score",
                "pgs_id": pgs_id,
                "trait": trait,
                "matched_variants": matched,
                "total_variants": total,
                "match_rate": f"{match_rate_pct:.1f}%",
                "match_rate_value": round(match_rate_pct, 1),
                "no_report": True,
                "headline": f"{trait}: PGS failed (match rate {match_rate_pct:.0f}% — too low)",
                "error": f"Match rate too low ({match_rate_pct:.1f}%) — PGS failed",
            }

        # Step 6: Percentile. Prefer dynamic scoring against the 1000G ref
        # panel restricted to the user's matched variant subset (fall back to
        # a precomputed stats file inside the helper if this isn't possible).
        raw_score = result.get("raw_score")
        percentile = _compute_percentile(pgs_id, raw_score or 0,
                                         scoring_file=scoring_file,
                                         matched_vars_path=vars_file,
                                         tmpdir=tmpdir)

        # Status reflects match-rate quality:
        #   ≥85%  → passed   (≥95% green, 85–95% yellow in the UI chip)
        #   60–85 → warning  (red chip in the UI, but report still useful)
        #   <60   → failed   (handled above, no report)
        status = "warning" if match_rate_pct < 85 else "passed"
        pct_str = f", {percentile}%ile" if percentile is not None else ""
        headline = f"{trait}: score={raw_score:.4g}{pct_str} ({matched:,}/{total:,} = {match_rate_pct:.0f}%)"

        d = {
            "test_type": "pgs_score",
            "pgs_id": pgs_id,
            "trait": trait,
            "raw_score": raw_score,
            "score_sum": result.get("score_sum"),
            "sample_id": result.get("sample_id"),
            "matched_variants": matched,
            "total_variants": total,
            "match_rate": f"{match_rate_pct:.1f}%",
            "match_rate_value": round(match_rate_pct, 1),
            "percentile": percentile,
            "genome_build": metadata.get("genome_build", metadata.get("HmPOS_build", "unknown")),
            "summary": _summarize_pgs(pgs_id, trait, result, matched, metadata, percentile),
            "status": status,
            "headline": headline,
        }
        if status == "warning":
            d["error"] = f"Low match rate: only {match_rate_pct:.1f}% of variants matched"
        return d


def _download_pgs_scoring_file(pgs_id, tmpdir):
    """Download harmonized scoring file from PGS Catalog."""
    cache_dir = os.path.join(PGS_CACHE, pgs_id)
    os.makedirs(cache_dir, exist_ok=True)

    # Check if already cached
    for suffix in ["_hmPOS_GRCh38.txt.gz", "_hmPOS_GRCh37.txt.gz", ".txt.gz"]:
        cached = os.path.join(cache_dir, f"{pgs_id}{suffix}")
        if os.path.exists(cached):
            logger.info(f"Using cached scoring file: {cached}")
            return cached

    # Download GRCh38 harmonized version
    url = f"https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/{pgs_id}/ScoringFiles/Harmonized/{pgs_id}_hmPOS_GRCh38.txt.gz"
    dest = os.path.join(cache_dir, f"{pgs_id}_hmPOS_GRCh38.txt.gz")

    logger.info(f"Downloading {pgs_id} from PGS Catalog...")
    stdout, stderr, rc = _run(["wget", "-q", "-O", dest, url], timeout=300)

    if rc == 0 and os.path.exists(dest) and os.path.getsize(dest) > 100:
        return dest

    # Try without harmonization
    url2 = f"https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/{pgs_id}/ScoringFiles/{pgs_id}.txt.gz"
    dest2 = os.path.join(cache_dir, f"{pgs_id}.txt.gz")
    stdout, stderr, rc = _run(["wget", "-q", "-O", dest2, url2], timeout=300)
    if rc == 0 and os.path.exists(dest2) and os.path.getsize(dest2) > 100:
        return dest2

    logger.error(f"Failed to download scoring file for {pgs_id}")
    return None


def _prepare_plink2_scoring(scoring_file, output_path):
    """Convert PGS Catalog format to plink2 --score format."""
    metadata = {}
    col_names = None
    data_lines = []

    opener = gzip.open if scoring_file.endswith('.gz') else open
    with opener(scoring_file, 'rt') as f:
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
            if not chrom.startswith('chr'):
                chrom = f"chr{chrom}"

            var_id = f"{chrom}:{pos}"
            data_lines.append(f"{var_id}\t{ea}\t{weight}")

    # Deduplicate
    seen = set()
    unique = []
    for line in data_lines:
        parts = line.split('\t')
        key = (parts[0], parts[1])
        if key not in seen:
            seen.add(key)
            unique.append(line)

    with open(output_path, 'w') as f:
        f.write("ID\tA1\tWEIGHT\n")
        for line in unique:
            f.write(line + "\n")

    metadata['variant_count'] = len(unique)
    return metadata


def _parse_sscore(sscore_path):
    """Parse plink2 .sscore output file."""
    with open(sscore_path) as f:
        header = f.readline().strip().split('\t')
        values = f.readline().strip().split('\t')

    result = {}
    for h, v in zip(header, values):
        h = h.strip('#').strip()
        if h in ('IID', 'FID'):
            result['sample_id'] = v
        elif 'AVG' in h:  # SCORE1_AVG or WEIGHT_AVG
            try:
                result['raw_score'] = float(v)
            except ValueError:
                result['raw_score'] = 0.0
        elif 'SUM' in h and 'DOSAGE' not in h:  # SCORE1_SUM or WEIGHT_SUM
            try:
                result['score_sum'] = float(v)
            except ValueError:
                result['score_sum'] = 0.0
        elif 'ALLELE_CT' in h:
            try:
                result['allele_count'] = int(v)
            except ValueError:
                pass
        elif 'NAMED_ALLELE_DOSAGE_SUM' in h:
            try:
                result['dosage_sum'] = float(v)
            except ValueError:
                pass

    return result


def _compute_percentile(pgs_id, raw_score, scoring_file=None,
                        matched_vars_path=None, tmpdir=None):
    """Compute percentile of raw_score against the 1000G EUR reference panel.

    Uses on-the-fly scoring of the 1000G reference panel restricted to the
    same variant subset that matched in the user's VCF. This gives an
    apples-to-apples comparison; otherwise, different match counts between
    user and reference produce biased AVG comparisons.

    Falls back to a precomputed stats file at
    REF_PANEL_STATS/{pgs_id}_EUR_GRCh38.json when dynamic scoring isn't
    possible (e.g. no matched_vars file or harmonized scoring file missing).
    """
    # Try dynamic scoring first (more accurate, ~5-10s overhead).
    if scoring_file and matched_vars_path and tmpdir and os.path.exists(matched_vars_path):
        try:
            result = _score_ref_panel_matched(pgs_id, scoring_file,
                                              matched_vars_path, tmpdir)
            if result is not None:
                mean, std = result
                if std > 0:
                    z = (raw_score - mean) / std
                    import math
                    p = 0.5 * (1 + math.erf(z / math.sqrt(2))) * 100
                    return round(p, 1)
        except Exception as e:
            logger.warning(f"{pgs_id}: dynamic percentile failed ({e}); "
                           f"falling back to precomputed stats")

    # Fallback: precomputed stats file.
    candidates = [
        f"{pgs_id}_EUR_GRCh38.json",
        f"{pgs_id}_EUR_GRCh37.json",
        f"{pgs_id}_EUR.json",
        f"{pgs_id}.json",
    ]
    stats_file = None
    for name in candidates:
        path = os.path.join(REF_PANEL_STATS, name)
        if os.path.exists(path):
            stats_file = path
            break
    if stats_file is None:
        return None

    try:
        with open(stats_file) as f:
            stats = json.load(f)
        mean = stats.get("mean", 0)
        std = stats.get("std", 1)
        if std == 0:
            return None
        z = (raw_score - mean) / std
        import math
        percentile = 0.5 * (1 + math.erf(z / math.sqrt(2))) * 100
        return round(percentile, 1)
    except Exception:
        return None


def _score_ref_panel_matched(pgs_id, scoring_file, matched_vars_path, tmpdir):
    """Score the 1000G ref panel on the SAME variant subset that matched in the
    user's VCF, then return (mean, std) of SCORE1_AVG over EUR samples.

    Returns None if anything fails or there are too few EUR samples/variants.
    """
    # 1. Parse matched variants from user's plink2 output. IDs are in the form
    #    'chr1:12345' (matching the `_prepare_plink2_scoring` format).
    matched_positions = set()
    with open(matched_vars_path) as f:
        for line in f:
            v = line.strip()
            if not v:
                continue
            if v.startswith("chr"):
                v = v[3:]
            # v is "N:pos" (ignore any trailing :ref:alt if present)
            parts = v.split(":")
            if len(parts) >= 2:
                matched_positions.add((parts[0], parts[1]))

    if not matched_positions:
        return None

    # 2. Re-read the harmonized scoring file, filter rows by (chrom, pos),
    #    and emit a 1000G-format plink2 score file. We emit BOTH allele
    #    orientations because 1000G pvar uses bare 'chrom:pos:ref:alt' IDs
    #    and we don't know which allele is REF/ALT up front — plink2 will
    #    only match the orientation that actually exists.
    opener = gzip.open if scoring_file.endswith(".gz") else open
    cols = None
    score_lines = []
    with opener(scoring_file, "rt") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if cols is None:
                cols = parts
                continue

            def col(name):
                return parts[cols.index(name)] if name in cols and cols.index(name) < len(parts) else ""

            chrom = col("hm_chr") or col("chr_name")
            pos = col("hm_pos") or col("chr_position")
            ea = col("effect_allele")
            oa = col("other_allele") or col("hm_inferOtherAllele")
            w = col("effect_weight")
            if not chrom or not pos or chrom == "NA" or pos == "NA":
                continue
            if chrom.startswith("chr"):
                chrom = chrom[3:]
            if (chrom, pos) not in matched_positions:
                continue
            if not ea or not w:
                continue
            try:
                float(w)
            except ValueError:
                continue
            if oa:
                score_lines.append(f"{chrom}:{pos}:{oa}:{ea}\t{ea}\t{w}")
                score_lines.append(f"{chrom}:{pos}:{ea}:{oa}\t{ea}\t{w}")
            else:
                score_lines.append(f"{chrom}:{pos}:N:{ea}\t{ea}\t{w}")

    if not score_lines:
        return None

    ref_score_file = os.path.join(tmpdir, f"{pgs_id}_ref_subset.tsv")
    with open(ref_score_file, "w") as f:
        f.write("ID\tA1\tWEIGHT\n")
        f.write("\n".join(score_lines) + "\n")

    # 3. Run plink2 --score against the 1000G pgen.
    out_prefix = os.path.join(tmpdir, f"{pgs_id}_ref_subset")
    cmd = [
        PLINK2,
        "--pfile", REF_PANEL, "vzs",
        "--score", ref_score_file, "header-read", "1", "2", "3",
        "cols=+scoresums",
        "no-mean-imputation",
        "--threads", "4",
        "--memory", "8000",
        "--out", out_prefix,
    ]
    stdout, stderr, rc = _run(cmd, timeout=300)
    sscore_path = out_prefix + ".sscore"
    if rc != 0 or not os.path.exists(sscore_path):
        return None

    # 4. Parse psam -> IID -> SuperPop map.
    psam_path = REF_PANEL + ".psam"
    superpop = {}
    try:
        with open(psam_path) as f:
            header = f.readline().lstrip("#").strip().split("\t")
            iid_i = header.index("IID")
            sp_i = header.index("SuperPop")
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) > max(iid_i, sp_i):
                    superpop[parts[iid_i]] = parts[sp_i]
    except (OSError, ValueError):
        return None

    # 5. Parse sscore, keep EUR AVGs.
    with open(sscore_path) as f:
        header = f.readline().lstrip("#").strip().split("\t")
        if "IID" not in header:
            return None
        iid_i = header.index("IID")
        avg_i = None
        for name in ("SCORE1_AVG", "WEIGHT_AVG"):
            if name in header:
                avg_i = header.index(name)
                break
        if avg_i is None:
            return None
        avgs = []
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) <= max(iid_i, avg_i):
                continue
            if superpop.get(parts[iid_i]) != "EUR":
                continue
            try:
                avgs.append(float(parts[avg_i]))
            except ValueError:
                continue

    if len(avgs) < 50:
        return None

    import statistics as _stats
    return _stats.mean(avgs), _stats.stdev(avgs)


def _summarize_pgs(pgs_id, trait, result, matched, metadata, percentile):
    """Generate text summary of PGS result."""
    lines = [f"Polygenic Score: {trait} ({pgs_id})"]
    lines.append(f"Raw score: {result.get('raw_score', 'N/A')}")
    lines.append(f"Matched variants: {matched:,} / {metadata.get('variant_count', '?'):,}")
    if percentile is not None:
        lines.append(f"Percentile (EUR ref): {percentile:.1f}%")
    else:
        lines.append("Percentile: N/A (no reference panel stats)")
    return "\n".join(lines)


# ─── rsID-list PGS Runner ───────────────────────────────────────

def _batch_lookup_variants(vcf_path, variants_in, has_chr_prefix):
    """Resolve a list of rsIDs in one VCF pass.

    Much faster than calling `_lookup_variant` in a loop: a single
    bcftools query with a regions file returns all matching records, and
    variants with no record are marked 'position-inferred' (homozygous ref).

    Returns: dict rsid -> {found, chrom, pos, ref, alt, genotype, source, ...}
    """
    # Build position index from the curated tables.
    rsid_pos = {}  # rsid -> (chrom_bare, pos, ref, alt)
    for v in variants_in:
        rs = v["rsid"]
        entry = RS_POSITIONS.get(rs) or RSID_LIST_POSITIONS.get(rs)
        if entry:
            rsid_pos[rs] = entry

    # Write a regions file: "chrom\tpos\tpos" one per line (bcftools -R).
    result = {rs: {"found": False, "genotype": "Not in VCF (no position data)",
                   "source": "none"}
              for v in variants_in for rs in [v["rsid"]]}

    if not rsid_pos:
        return result

    # Write a tab-separated regions file. Using `.tsv` suffix (not `.bed`)
    # so bcftools interprets positions as 1-based inclusive, matching the
    # GRCh38 coordinates stored in RSID_LIST_POSITIONS / RS_POSITIONS.
    import tempfile
    tmp = tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False)
    try:
        # Index by (chrom, pos) -> list of rsIDs so we can match records back.
        by_cp = {}
        for rs, (chrom_bare, pos, ref, alt) in rsid_pos.items():
            chrom = f"chr{chrom_bare}" if has_chr_prefix else chrom_bare
            tmp.write(f"{chrom}\t{pos}\t{pos}\n")
            by_cp.setdefault((chrom, pos), []).append(rs)
        tmp.close()

        stdout, _, rc = _run([
            BCFTOOLS, "query",
            "-R", tmp.name,
            "-f", "%CHROM\t%POS\t%REF\t%ALT\t[%GT]\n",
            vcf_path,
        ], timeout=300)

        if rc == 0:
            for line in stdout.splitlines():
                parts = line.split("\t")
                if len(parts) < 5:
                    continue
                chrom, pos_s, ref, alt, gt = parts[0], parts[1], parts[2], parts[3], parts[4]
                try:
                    pos = int(pos_s)
                except ValueError:
                    continue
                rsids = by_cp.get((chrom, pos), [])
                for rs in rsids:
                    # A position may have multiple records (split multiallelics);
                    # prefer the first one that matches the expected alt allele
                    # from the curated table.
                    _, _, exp_ref, exp_alt = rsid_pos[rs]
                    existing = result.get(rs)
                    keep = True
                    if existing and existing.get("found"):
                        # Prefer the record whose alt matches expected alt.
                        if existing.get("alt") == exp_alt:
                            keep = False
                    if keep:
                        result[rs] = {
                            "found": True,
                            "chrom": chrom,
                            "pos": pos_s,
                            "ref": ref,
                            "alt": alt,
                            "genotype": gt,
                            "source": "position",
                            "expected_ref": exp_ref,
                            "expected_alt": exp_alt,
                        }
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass

    # Variants with a known position but no VCF record → homozygous reference.
    for rs, (chrom_bare, pos, ref, alt) in rsid_pos.items():
        if result[rs]["source"] == "none":
            chrom = f"chr{chrom_bare}" if has_chr_prefix else chrom_bare
            result[rs] = {
                "found": False,
                "chrom": chrom,
                "pos": str(pos),
                "ref": ref,
                "alt": alt,
                "genotype": "0/0 (ref/ref — no variant)",
                "source": "position-inferred",
            }

    return result


def run_rsid_pgs_score(vcf_path, params):
    """Compute a small-panel polygenic score from a hand-curated list of
    (rsID, effect, risk_allele) triples.

    Unlike `run_pgs_score` (which uses plink2 against a PGS Catalog scoring
    file), this runner looks up each rsID directly in the VCF, determines
    the dosage of the *risk* allele from the genotype, and sums
    `effect * dosage` across variants. It's aimed at short published
    trait panels like those in `rsid-list.md` (typically 3-100 rsIDs).
    """
    vcf_path = _ensure_indexed(vcf_path)
    has_chr = _detect_chr_prefix(vcf_path)

    title = params.get("title", "Custom rsID PGS")
    citation = params.get("citation", "")
    variants_in = params.get("variants", [])

    # Batch-resolve all variants in one bcftools query (huge speedup over
    # one-query-per-variant). Returns dict rsid -> lookup record with the
    # same shape as _lookup_variant.
    lookups = _batch_lookup_variants(vcf_path, variants_in, has_chr)

    per_variant = []
    total = 0.0
    matched = 0
    unresolved = 0

    for v in variants_in:
        rs = v["rsid"]
        effect = float(v["effect"])
        risk = v["risk"]

        look = lookups.get(rs, {"found": False, "source": "none"})
        dosage = None
        allele_source = look.get("source", "none")

        if look.get("found"):
            # We have REF, ALT, and a genotype like "0/1" or "0|1".
            dosage = _risk_allele_dosage(look.get("ref", ""), look.get("alt", ""),
                                         look.get("genotype", ""), risk)
        elif allele_source == "position-inferred":
            # Position known, but no variant called here → homozygous reference.
            # dosage of risk allele = 2 if risk == ref, else 0.
            ref = look.get("ref", "")
            if ref and risk == ref:
                dosage = 2
            elif ref:
                dosage = 0
            # else leave as None (ref not known)

        if dosage is None:
            unresolved += 1
            per_variant.append({
                "rsid": rs,
                "effect": effect,
                "risk_allele": risk,
                "genotype": look.get("genotype", "unresolved"),
                "dosage": None,
                "contribution": 0.0,
                "source": allele_source,
            })
            continue

        contribution = effect * dosage
        total += contribution
        matched += 1
        per_variant.append({
            "rsid": rs,
            "effect": effect,
            "risk_allele": risk,
            "genotype": look.get("genotype", ""),
            "dosage": dosage,
            "contribution": round(contribution, 4),
            "source": allele_source,
        })

    n = len(variants_in)
    match_rate = (matched / n * 100) if n else 0.0

    # Match rate too low (or nothing resolved) — bail out with a no_report
    # failure stub so the UI hides the View button and just shows that
    # the PGS failed.
    if matched == 0 or match_rate < 60:
        if matched == 0:
            err = f"None of {n} variants could be resolved in the VCF"
            headline = f"{title}: PGS failed (no variants resolved)"
        else:
            err = f"Match rate too low ({match_rate:.1f}%) — PGS failed"
            headline = f"{title}: PGS failed (match rate {match_rate:.0f}% — too low)"
        return {
            "test_type": "rsid_pgs_score",
            "title": title,
            "citation": citation,
            "n_variants": n,
            "matched_variants": matched,
            "unresolved_variants": unresolved,
            "match_rate": f"{match_rate:.1f}%",
            "match_rate_value": round(match_rate, 1),
            "no_report": True,
            "status": "failed",
            "headline": headline,
            "error": err,
        }

    # 60–85 → warning (still report); ≥85 → passed.
    if match_rate < 85:
        status = "warning"
        err = f"Low match rate: {matched}/{n} variants resolved ({match_rate:.0f}%)"
    else:
        status = "passed"
        err = None
    headline = f"{title}: score={total:+.3f} ({matched}/{n} = {match_rate:.0f}%)"

    summary_lines = [
        f"{title} ({citation})" if citation else title,
        f"Raw score: {total:+.4f}",
        f"Resolved variants: {matched}/{n} ({match_rate:.0f}%)",
    ]
    if unresolved:
        summary_lines.append(f"Unresolved: {unresolved}")

    d = {
        "test_type": "rsid_pgs_score",
        "title": title,
        "citation": citation,
        "raw_score": round(total, 4),
        "n_variants": n,
        "matched_variants": matched,
        "unresolved_variants": unresolved,
        "match_rate": f"{match_rate:.1f}%",
        "match_rate_value": round(match_rate, 1),
        "per_variant": per_variant,
        "summary": "\n".join(summary_lines),
        "status": status,
        "headline": headline,
    }
    if err:
        d["error"] = err
    return d


def _risk_allele_dosage(ref, alt, genotype, risk):
    """Given VCF REF, ALT (possibly comma-separated), a genotype string like
    '0/1', and the risk allele, return the dosage (0, 1, 2) of the risk allele.

    Returns None if we cannot determine dosage (unknown genotype, risk allele
    not present in REF/ALT, etc.)."""
    if not genotype or not ref:
        return None

    alts = alt.split(",") if alt else []
    # Build allele index: 0 → ref, 1..n → alts[0..]
    alleles = [ref] + alts

    # Normalize genotype separators and strip phasing info.
    gt = genotype.replace("|", "/")
    # Take only the first sample's genotype if multiple fields.
    gt = gt.split()[0] if " " in gt else gt
    parts = gt.split("/")
    if len(parts) < 2:
        return None

    dose = 0
    for p in parts:
        if not p or p == ".":
            return None
        try:
            idx = int(p)
        except ValueError:
            return None
        if idx < 0 or idx >= len(alleles):
            return None
        if alleles[idx] == risk:
            dose += 1
    return dose


# ─── ClinVar Screening Runner ───────────────────────────────────

def run_clinvar_screen(vcf_path, params):
    """Screen VCF for pathogenic/likely pathogenic variants in specified genes.

    Transparently ensures the VCF is annotated with CLNSIG/GENEINFO — if
    it isn't, we run `bcftools annotate` against the pre-built ClinVar VCF
    once per file and cache the result.
    """
    genes = params.get("genes", [])
    panel = params.get("panel", "Unknown")
    vcf_path = _ensure_indexed(vcf_path)

    # Auto-annotate if needed. The helper is a no-op on VCFs that already
    # carry CLNSIG+GENEINFO (fast path).
    try:
        vcf_path = _ensure_clinvar_annotated(vcf_path)
    except RuntimeError as e:
        return _fail(f"{panel}: ClinVar annotation failed", str(e),
                     test_type="clinvar_screen", panel=panel,
                     genes_screened=len(genes))

    # Do a single query for *all* Pathogenic/Likely_pathogenic variants and
    # filter to the panel genes in Python. This turns N separate VCF scans
    # (one per gene, ~20 s each on a 9 M-record VCF) into a single scan
    # that runs in a few seconds, cutting the ACMG panels from ~10 min to
    # ~15 s.
    gene_set = set(genes)
    q_stdout, q_stderr, q_rc = _run([
        BCFTOOLS, "query",
        "-f", "%CHROM\t%POS\t%ID\t%REF\t%ALT\t%INFO/CLNSIG\t%INFO/GENEINFO\t[%GT]\n",
        "-i", '(INFO/CLNSIG~"Pathogenic" || INFO/CLNSIG~"Likely_pathogenic")',
        vcf_path,
    ], timeout=600)

    results = []
    if q_rc == 0 and q_stdout.strip():
        for line in q_stdout.strip().split('\n'):
            parts = line.split('\t')
            if len(parts) < 8:
                continue
            geneinfo = parts[6]
            # GENEINFO format: "GENE1:ID1|GENE2:ID2|...". Extract all gene
            # symbols and keep the record if any overlaps the panel.
            variant_genes = set()
            for entry in geneinfo.split('|'):
                sym = entry.split(':', 1)[0].strip()
                if sym:
                    variant_genes.add(sym)
            matched = variant_genes & gene_set
            if not matched:
                continue
            # Only count hom-ref (sample doesn't actually carry it) skips —
            # we preserve het and hom-alt findings.
            gt = parts[7]
            if not _has_alt_allele(gt):
                continue
            for gene in sorted(matched):
                results.append({
                    "gene": gene,
                    "chrom": parts[0],
                    "pos": parts[1],
                    "id": parts[2],
                    "ref": parts[3],
                    "alt": parts[4],
                    "clnsig": parts[5],
                    "genotype": gt,
                })

    found_genes = set(r["gene"] for r in results)
    clean_genes = [g for g in genes if g not in found_genes]

    if results:
        headline = f"{len(results)} pathogenic finding(s) in {len(found_genes)} gene(s): " + \
                   ", ".join(sorted(found_genes)[:5])
    else:
        headline = f"No pathogenic variants in {len(genes)} {panel.lower()} genes"

    return _pass(headline,
                 test_type="clinvar_screen",
                 panel=panel,
                 genes_screened=len(genes),
                 genes_with_findings=len(found_genes),
                 findings=results,
                 clean_genes=clean_genes,
                 summary=_summarize_clinvar(panel, genes, results, clean_genes))


def _summarize_clinvar(panel, genes, findings, clean_genes):
    lines = [f"Monogenic Screening: {panel} Panel"]
    lines.append(f"Genes screened: {len(genes)}")
    if findings:
        lines.append(f"FINDINGS: {len(findings)} pathogenic/likely pathogenic variant(s) found!")
        for f in findings:
            lines.append(f"  {f['gene']}: {f['chrom']}:{f['pos']} {f['ref']}>{f['alt']} [{f['clnsig']}] GT={f['genotype']}")
    else:
        lines.append("No pathogenic/likely pathogenic variants found in screened genes.")
        lines.append("Note: VCF must have ClinVar annotations for this screen to work. " +
                      "Run with an annotated VCF for complete results.")
    return "\n".join(lines)


# ─── Specialized Runners ─────────────────────────────────────────

def run_specialized(vcf_path, params):
    """Handle specialized analyses like ancestry, PGx, etc."""
    method = params.get("method", "")
    ftype = _detect_file_type(vcf_path)

    # Methods that handle CRAM internally — either by deriving an on-demand
    # VCF (via cram_vcf_cache) or by reading the alignment directly (HLA
    # typing). Everything else falls through to the generic VCF requirement.
    _CRAM_OK_METHODS = {
        "pca_1000g",       # derives a 106K-site VCF on demand
        "y_haplogroup",    # derives chrY VCF on demand
        "mt_haplogroup",   # derives chrM VCF on demand
        "roh",             # reuses the PCA cached VCF (sparse FROH estimate)
        "neanderthal",     # delegates to PCA, which already handles CRAM
        "admixture",       # delegates to PCA
        "hla_typing",      # reads CRAM directly via T1K
        "pgx",             # not implemented — returns warning
        "ace_id",          # not implemented — returns warning
    }

    if ftype in ("bam", "cram") and method not in _CRAM_OK_METHODS:
        return _fail(
            f"{method}: requires VCF input",
            f"This test cannot run on a {ftype.upper()} file "
            f"({os.path.basename(vcf_path)}). Convert to VCF first or "
            f"add a VCF in the file manager.",
            test_type="specialized",
        )

    if method == "roh":
        return _run_roh(vcf_path)
    elif method == "pca_1000g":
        return _run_pca_1000g(vcf_path)
    elif method == "admixture":
        return _run_admixture_from_pca(vcf_path)
    elif method == "y_haplogroup":
        return _run_y_haplogroup(vcf_path)
    elif method == "mt_haplogroup":
        return _run_mt_haplogroup(vcf_path)
    elif method == "neanderthal":
        return _run_neanderthal(vcf_path)
    elif method == "hla_typing":
        return _run_hla_typing(vcf_path)
    elif method == "pgx":
        gene = params.get("gene", "CYP2D6")
        return _warn(f"{gene} star alleles: not available",
                     f"{gene} star allele calling requires Cyrius/PharmCAT and BAM input "
                     f"(for CNVs).",
                     test_type="specialized", method=f"{gene} star alleles",
                     summary=f"{gene} star allele caller requires BAM input — not available for VCF-only samples.")
    elif method == "ace_id":
        return _warn("ACE I/D polymorphism: not available",
                     "ACE I/D is an Alu insertion that is not reliably called from "
                     "short-read VCFs. Requires PCR assay or long-read sequencing.",
                     test_type="specialized", method="ACE I/D",
                     summary="ACE I/D Alu insertion not callable from short-read VCF.")
    else:
        return _fail(f"Unknown method: {method}",
                     f"Specialized method '{method}' is not recognized",
                     test_type="specialized", method=method)


def _derive_pca_vcf_from_cram(aln_path):
    """Call variants at the ~106K PCA projection positions from a CRAM/BAM.

    Instead of calling the entire genome (~30-60 min for WGS), we extract
    only reads overlapping the 106K pruned PCA variant positions and call
    genotypes there.  Result is cached under cram_vcf_cache/{hash}/pca.vcf.gz
    so subsequent runs are instant.

    Returns (vcf_path, error_message).
    """
    cache_dir = _cram_cache_dir(aln_path)
    cached = cache_dir / "pca.vcf.gz"
    if cached.exists() and cached.stat().st_size > 0:
        return str(cached), None

    # 1. Collect the unique autosomal positions from the allele-weight file.
    weights = os.path.join(PGS_CACHE, "pca_1000g", "ref.eigenvec.allele")
    if not os.path.exists(weights):
        return None, f"PCA allele-weight cache not found at {weights}"

    # Parse positions: format is "chrom:pos:ref:alt" with bare chrom numbers.
    positions = set()
    with open(weights) as f:
        for line in f:
            if line.startswith("#"):
                continue
            vid = line.split("\t")[1]   # ID column
            parts = vid.split(":")
            if len(parts) >= 2:
                positions.add((parts[0], int(parts[1])))
    logger.info(f"PCA: {len(positions)} unique positions to call from CRAM")

    # 2. Map bare-chrom positions to the CRAM's chromosome naming and write a
    #    BED file (samtools -L) and a regions TSV (bcftools -R).
    has_chr = _detect_alignment_chr_prefix(aln_path)
    ref = _pick_reference_for(aln_path)

    bed_path = str(cache_dir / "_pca_positions.bed")
    tsv_path = str(cache_dir / "_pca_positions.tsv")
    with open(bed_path, "w") as fb, open(tsv_path, "w") as ft:
        for chrom, pos in sorted(positions, key=lambda x: (x[0].zfill(2), x[1])):
            c = f"chr{chrom}" if has_chr else chrom
            fb.write(f"{c}\t{pos - 1}\t{pos}\n")   # BED: 0-based half-open
            ft.write(f"{c}\t{pos}\t{pos}\n")        # bcftools regions: 1-based inclusive

    # 3. Extract reads at those positions from the CRAM → temporary BAM.
    slice_bam = str(cache_dir / "_pca_slice.bam")
    pile_bcf = str(cache_dir / "_pca_pile.bcf")

    def cleanup():
        for p in (slice_bam, slice_bam + ".bai", pile_bcf, bed_path, tsv_path):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass

    try:
        # Restrict to autosomes only (chr1-22 / 1-22) so samtools doesn't
        # try to decode problematic contigs like chrEBV whose reference
        # length doesn't match the CRAM header.
        autosomes = [f"chr{i}" if has_chr else str(i) for i in range(1, 23)]

        logger.info("PCA: extracting reads at PCA positions from CRAM (autosomes)…")
        _, stderr, rc = _run([
            SAMTOOLS, "view",
            "--input-fmt-option", "ignore_md5=1",
            "-T", ref,
            "-b", "-o", slice_bam,
            "-L", bed_path,
            str(aln_path),
        ] + autosomes, timeout=3600)
        if rc != 0:
            cleanup()
            # Strip library-version warnings so the real error is visible.
            err = "\n".join(l for l in stderr.splitlines()
                           if "no version information" not in l)
            return None, f"samtools view failed (rc={rc}): {err[:500]}"
        if not os.path.exists(slice_bam) or os.path.getsize(slice_bam) < 100:
            cleanup()
            return None, "samtools produced an empty PCA slice BAM"
        _, _, _ = _run([SAMTOOLS, "index", slice_bam], timeout=600)

        # 4. mpileup + call → VCF (restrict to the exact positions).
        logger.info("PCA: running mpileup on extracted slice…")
        _, stderr, rc = _run([
            BCFTOOLS, "mpileup",
            "-f", ref,
            "-R", tsv_path,
            "--max-depth", "250",
            "-q", "20", "-Q", "20",
            "-a", "FORMAT/AD,FORMAT/DP",
            "-Ou", "-o", pile_bcf,
            slice_bam,
        ], timeout=3600)
        if rc != 0:
            cleanup()
            return None, f"bcftools mpileup failed: {stderr[:300]}"

        logger.info("PCA: calling variants…")
        _, stderr, rc = _run([
            BCFTOOLS, "call", "-m",     # emit ALL genotypes, not just -v (variants)
            "-Oz", "-o", str(cached),
            pile_bcf,
        ], timeout=3600)
        if rc != 0:
            cleanup()
            return None, f"bcftools call failed: {stderr[:300]}"

        _, _, _ = _run([BCFTOOLS, "index", "-t", str(cached)], timeout=600)
    finally:
        cleanup()

    logger.info(f"PCA: derived VCF at {cached} ({cached.stat().st_size / 1e6:.1f} MB)")
    return str(cached), None


def _run_pca_1000g(vcf_path):
    """Project a sample onto 1000 Genomes PCs.

    Strategy:
      1. Use a pre-computed allele-weight cache (built once from the 1000G ref
         panel restricted to a pruned variant set). Each cache build takes a
         few minutes; subsequent projections are fast.
      2. Score the sample with plink2 --score against the cached eigenvec
         allele weights.
      3. Compare projected coordinates to per-population centroids in the
         reference eigenvec to assign a population label.

    BAM/CRAM inputs are handled automatically: variants are called on-demand
    at the ~106K PCA positions and cached so subsequent runs are instant.
    """
    import statistics

    ftype = _detect_file_type(vcf_path)

    cache_dir = os.path.join(PGS_CACHE, "pca_1000g")
    os.makedirs(cache_dir, exist_ok=True)
    weights_file = os.path.join(cache_dir, "ref.eigenvec.allele")
    afreq_file = os.path.join(cache_dir, "ref.afreq")
    refvec_file = os.path.join(cache_dir, "ref.eigenvec")
    eigenval_file = os.path.join(cache_dir, "ref.eigenval")
    psam_file = os.path.join(cache_dir, "ref.psam")

    # Step 1: build cache if missing
    if not (os.path.exists(weights_file) and os.path.exists(afreq_file)
            and os.path.exists(refvec_file)):
        try:
            _build_pca_reference_cache(cache_dir)
        except Exception as e:
            return _fail("PCA: reference cache build failed", str(e),
                         test_type="specialized", method="PCA projection onto 1000G")

    # Step 1b: if input is BAM/CRAM, derive a VCF with genotypes at PCA sites.
    if ftype in ("bam", "cram"):
        derived, err = _derive_pca_vcf_from_cram(vcf_path)
        if derived is None:
            return _fail("PCA: variant calling from CRAM failed", err or "unknown error",
                         test_type="specialized", method="PCA projection onto 1000G")
        vcf_path = derived

    # Step 2: project sample
    with tempfile.TemporaryDirectory(dir=SCRATCH, prefix="pca_") as tmpdir:
        vcf_path = _ensure_indexed(vcf_path)
        try:
            # Use the same ID format as the 1000G reference panel
            # (bare chrom + position + REF + ALT, no "chr" prefix).
            # The cache keys this variant separately from the PGS-style
            # chr@:# pgen, so the two coexist on disk.
            sample_prefix = _get_or_build_pgen(vcf_path,
                                               var_id_template="@:#:$r:$a",
                                               output_chr="26")
        except RuntimeError as e:
            return _fail("PCA: VCF→pgen failed", str(e),
                         test_type="specialized", method="PCA projection onto 1000G")

        # eigenvec.allele cols (plink2 default): #CHROM, ID, REF, ALT, A1, PC1, PC2, ...
        # `variance-standardize` is REQUIRED so the projected coordinates
        # live in the same space as the cached ref.projected.sscore
        # centroids that _load_pca_centroids reads. Dropping it silently
        # misclassifies samples because the raw-score coordinate system
        # differs from the eigenvec / projected-score system.
        proj_prefix = os.path.join(tmpdir, "projected")
        cmd = [
            PLINK2,
            "--pfile", sample_prefix,
            "--read-freq", afreq_file,
            "--score", weights_file, "2", "5", "header-read",
            "no-mean-imputation", "variance-standardize",
            "--score-col-nums", "6-15",
            "--allow-extra-chr",
            "--out", proj_prefix,
        ]
        stdout, stderr, rc = _run(cmd, timeout=600)

        sscore_path = f"{proj_prefix}.sscore"
        if not os.path.exists(sscore_path):
            return _fail("PCA: projection failed", stderr[:500] or stdout[-500:],
                         test_type="specialized", method="PCA projection onto 1000G")

        # Parse projected PC coordinates
        with open(sscore_path) as f:
            header = f.readline().strip().split('\t')
            row = f.readline().strip().split('\t')

        # Find PC columns: PC1_AVG ... PC10_AVG  (or PC1, ...)
        pc_idxs = [(i, h) for i, h in enumerate(header) if h.startswith('PC')]
        if len(pc_idxs) < 2:
            return _fail("PCA: could not parse projected PCs",
                         f"score header: {header}",
                         test_type="specialized", method="PCA projection onto 1000G")

        sample_pcs = []
        for i, _ in pc_idxs:
            try:
                # plink2 score with variance-standardize returns AVG values; multiply
                # by allele count to be on same scale as reference PCs:
                sample_pcs.append(float(row[i]))
            except (ValueError, IndexError):
                sample_pcs.append(0.0)

        # Step 3: load population labels and centroids
        centroids = _load_pca_centroids(refvec_file, psam_file)
        if not centroids:
            return _fail("PCA: could not compute population centroids",
                         "Reference centroid computation failed",
                         test_type="specialized", method="PCA projection onto 1000G")

        # Find closest super-population by Euclidean distance over first 4 PCs
        ndim = min(4, len(sample_pcs))
        distances = {}
        for pop, centroid in centroids.items():
            d = sum((sample_pcs[i] - centroid[i]) ** 2 for i in range(min(ndim, len(centroid)))) ** 0.5
            distances[pop] = d

        sorted_pops = sorted(distances.items(), key=lambda x: x[1])
        best_pop, best_dist = sorted_pops[0]
        second_pop, second_dist = sorted_pops[1] if len(sorted_pops) > 1 else (None, None)

        confidence = "high" if second_dist and (second_dist - best_dist) / best_dist > 0.3 else "moderate"

        pcs_str = ", ".join(f"PC{i+1}={v:.4f}" for i, v in enumerate(sample_pcs[:5]))
        headline = f"PCA: closest population = {best_pop} (PC1={sample_pcs[0]:.3f}, PC2={sample_pcs[1]:.3f})"

        return _pass(headline,
                     test_type="specialized", method="PCA projection onto 1000G",
                     pcs=[round(v, 5) for v in sample_pcs],
                     closest_population=best_pop,
                     distances={p: round(d, 4) for p, d in sorted_pops[:5]},
                     confidence=confidence,
                     summary=(f"PCA projection onto 1000G:\n"
                              f"  {pcs_str}\n"
                              f"  Closest super-population: {best_pop} "
                              f"(distance {best_dist:.4f}, {confidence} confidence)\n"
                              f"  Next closest: {second_pop} (distance {second_dist:.4f})"))


def _build_pca_reference_cache(cache_dir):
    """One-time build of reference PCA cache from 1000G panel.

    Two-stage:
      1. LD-prune the reference panel and write a normalized pgen with
         standardized variant IDs (chrN:pos:REF:ALT).
      2. Compute PCA + allele weights on the pruned set.

    Writes:
      ref.eigenvec.allele  - allele weights for projection
      ref.afreq            - allele frequencies
      ref.eigenvec         - reference sample PCs (for centroids)
      ref.eigenval         - eigenvalues
      ref.psam             - reference sample metadata (copy)
    """
    logger.info("Building PCA reference cache (one-time, ~10-20 min)...")
    ref_pfile = "/data/pgs2/ref_panel/GRCh38_1000G_ALL"
    out_prefix = os.path.join(cache_dir, "ref")
    pruned_prefix = os.path.join(cache_dir, "ref_pruned")

    # Stage 1: filter + LD-prune + standardize IDs in a single normalized pgen.
    # We use an autosomes-only QC pass with reasonable defaults for PCA.
    cmd1 = [
        PLINK2,
        "--pfile", ref_pfile, "vzs",
        "--allow-extra-chr",
        "--chr", "1-22",
        "--maf", "0.05",
        "--geno", "0.02",
        "--snps-only",
        "--max-alleles", "2",
        "--rm-dup", "force-first",
        # plink2 doesn't accept template-style --set-all-var-ids on extract step;
        # we just keep the default IDs and rely on chr:pos matching.
        "--indep-pairwise", "1000", "50", "0.1",
        "--threads", "16",
        "--memory", "48000",
        "--out", pruned_prefix + "_ld",
    ]
    stdout, stderr, rc = _run(cmd1, timeout=3600)
    if rc != 0:
        raise RuntimeError(f"plink2 LD-prune failed: {stderr[:500] or stdout[-500:]}")

    prune_in = pruned_prefix + "_ld.prune.in"
    if not os.path.exists(prune_in):
        raise RuntimeError("plink2 --indep-pairwise produced no .prune.in file")

    # Stage 2: extract pruned variants, run PCA, get allele weights + freqs
    cmd2 = [
        PLINK2,
        "--pfile", ref_pfile, "vzs",
        "--allow-extra-chr",
        "--rm-dup", "force-first",
        "--extract", prune_in,
        "--freq",
        "--pca", "10", "approx", "allele-wts",
        "--threads", "16",
        "--memory", "48000",
        "--out", out_prefix,
    ]
    stdout, stderr, rc = _run(cmd2, timeout=3600)
    if rc != 0:
        raise RuntimeError(f"plink2 --pca failed: {stderr[:500] or stdout[-500:]}")

    # Copy psam for centroid lookup
    src_psam = ref_pfile + ".psam"
    if os.path.exists(src_psam):
        shutil.copy(src_psam, os.path.join(cache_dir, "ref.psam"))

    if not os.path.exists(out_prefix + ".eigenvec.allele"):
        raise RuntimeError("PCA cache build did not produce eigenvec.allele")

    # Stage 3: re-project every reference sample through the same --score
    # pipeline used at test time, so centroids end up in the projected
    # coordinate system (see _load_pca_centroids for why this matters).
    logger.info("Projecting reference samples into projected PCA space for centroids...")
    projected_sscore = os.path.join(cache_dir, "ref.projected.sscore")
    # We need a pgen of the ref panel restricted to the pruned variants
    refall_prefix = pruned_prefix + "_refall"
    cmd_extract = [
        PLINK2, "--pfile", ref_pfile, "vzs",
        "--extract", prune_in,
        "--make-pgen",
        "--allow-extra-chr",
        "--threads", "16",
        "--memory", "48000",
        "--out", refall_prefix,
    ]
    stdout, stderr, rc = _run(cmd_extract, timeout=3600)
    if rc != 0:
        logger.warning(f"Could not build ref.projected.sscore (extract failed): {stderr[:300]}")
    else:
        cmd_score = [
            PLINK2, "--pfile", refall_prefix,
            "--read-freq", out_prefix + ".afreq",
            "--score", out_prefix + ".eigenvec.allele", "2", "5",
            "header-read", "no-mean-imputation", "variance-standardize",
            "--score-col-nums", "6-15",
            "--allow-extra-chr",
            "--threads", "16",
            "--out", os.path.join(cache_dir, "ref.projected"),
        ]
        stdout, stderr, rc = _run(cmd_score, timeout=3600)
        if rc != 0 or not os.path.exists(projected_sscore):
            logger.warning(f"Could not build ref.projected.sscore: {stderr[:300]}")
        else:
            logger.info(f"Wrote projected-space reference PCs: {projected_sscore}")
        # Clean up the refall pgen — it was only needed for projection
        for ext in (".pgen", ".pvar", ".psam", ".log"):
            f = refall_prefix + ext
            if os.path.exists(f):
                try:
                    os.remove(f)
                except OSError:
                    pass

    # Cleanup intermediate LD-prune output
    for ext in (".prune.in", ".prune.out", ".log"):
        f = pruned_prefix + "_ld" + ext
        if os.path.exists(f):
            try:
                os.remove(f)
            except OSError:
                pass

    logger.info("PCA reference cache built successfully.")


def _load_pca_centroids(refvec_file, psam_file):
    """Compute mean PC1-PC10 per super-population from reference samples.

    CRITICAL: plink2 `--pca approx allele-wts` produces allele weights that do
    NOT reconstruct the eigenvec coordinates when fed through `--score`. A
    sample projected via `--score` lives in a *different* coordinate system
    from the `ref.eigenvec` output of `--pca`. Comparing a projected sample
    to eigenvec-derived centroids therefore silently mislabels populations
    (e.g. a clear EUR sample at PC1=+0.07 landed near the AFR eigenvec
    centroid at PC1=+0.03 in our first pass).

    The fix: we re-project every reference sample through the same `--score`
    pipeline as test samples and compute centroids in that projected space.
    The result is cached in `ref.projected.sscore`; this function prefers
    that cache when it exists and falls back to `ref.eigenvec` only if the
    projected cache is missing (which will give wrong labels).
    """
    if not os.path.exists(psam_file):
        return {}

    # Load sample → super-population map
    pop_map = {}
    with open(psam_file) as f:
        header = f.readline().strip().lstrip('#').split('\t')
        try:
            iid_idx = header.index('IID')
            sp_idx = header.index('SuperPop')
        except ValueError:
            return {}
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) > max(iid_idx, sp_idx):
                pop_map[parts[iid_idx]] = parts[sp_idx]

    # Prefer the projected-coordinate centroids when available.
    projected_sscore = os.path.join(
        os.path.dirname(refvec_file) or ".", "ref.projected.sscore")
    source_file = projected_sscore if os.path.exists(projected_sscore) else refvec_file
    if not os.path.exists(source_file):
        return {}

    pop_pcs = {}
    with open(source_file) as f:
        first = f.readline()
        if first.startswith('#'):
            header = first.lstrip('#').strip().split('\t')
            iid_idx = header.index('IID') if 'IID' in header else 0
            # Find the first column name starting with "PC"
            pc_start = None
            for i, h in enumerate(header):
                if h.startswith('PC'):
                    pc_start = i
                    break
            if pc_start is None:
                return {}
        else:
            f.seek(0)
            iid_idx = 0
            pc_start = 2

        for line in f:
            parts = line.strip().split()
            if len(parts) <= pc_start:
                continue
            iid = parts[iid_idx]
            sp = pop_map.get(iid)
            if not sp:
                continue
            try:
                pcs = [float(x) for x in parts[pc_start:pc_start + 10]]
            except ValueError:
                continue
            pop_pcs.setdefault(sp, []).append(pcs)

    centroids = {}
    for sp, rows in pop_pcs.items():
        if not rows:
            continue
        n_pcs = len(rows[0])
        centroids[sp] = [sum(r[i] for r in rows) / len(rows) for i in range(n_pcs)]
    return centroids


HAPLOGROUP_DATA_DIR = "/data/haplogroup_data"
HAPLOGREP3_BIN = "/home/nimrod_rotem/tools/haplogrep3/haplogrep3"
HAPLOGREP3_TREE = "phylotree-rcrs@17.2"
T1K_BIN = "/home/nimo/miniconda3/envs/genomics/bin/run-t1k"
T1K_HLA_REF = "/data/t1k_ref/hla/hla_dna_seq.fa"
T1K_HLA_COORD = "/data/t1k_ref/hla/hla_dna_coord.fa"


def _query_vcf_genotypes(vcf_path, region_list):
    """Batch query a VCF at many positions, return dict {(chrom, pos): (ref, alt, gt)}.

    region_list: iterable of (chrom, pos) tuples.
    Writes a temp regions file so bcftools -R can handle more positions
    than would fit on the command line. GT is returned as-is from bcftools
    (e.g. '0/1', '1/1', '0/0'). Returns empty dict on failure.
    """
    if not region_list:
        return {}
    with tempfile.NamedTemporaryFile(
            mode="w", suffix=".tsv", delete=False, dir=SCRATCH) as rf:
        regions_file = rf.name
        for c, p in region_list:
            rf.write(f"{c}\t{p}\n")

    try:
        stdout, _, rc = _run([
            BCFTOOLS, "query",
            "-R", regions_file,
            "-f", "%CHROM\t%POS\t%REF\t%ALT\t[%GT]\n",
            vcf_path,
        ], timeout=300)
    finally:
        try:
            os.remove(regions_file)
        except OSError:
            pass

    if rc != 0:
        return {}
    out = {}
    for line in stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 5:
            continue
        chrom, pos, ref, alt, gt = parts[0], int(parts[1]), parts[2], parts[3], parts[4]
        out[(chrom, pos)] = (ref, alt, gt)
    return out


def _has_alt_allele(gt):
    """Return True if a bcftools GT string contains the ALT allele (non-ref)."""
    if not gt or gt in (".", "./.", ".|."):
        return False
    # Split on | or / separators
    for sep in ("|", "/"):
        if sep in gt:
            return any(x.isdigit() and int(x) > 0 for x in gt.split(sep))
    # Haploid single-allele call
    return gt.isdigit() and int(gt) > 0


def _run_y_haplogroup(vcf_path):
    """Assign a major Y-DNA haplogroup using a curated ISOGG-2016 SNP panel.

    Loads the lifted-over GRCh38 SNP table from HAPLOGROUP_DATA_DIR, queries
    the VCF in a single chrY range, and identifies which haplogroup labels are
    supported by derived (ALT) alleles. Picks the *deepest* label (longest
    dotted name) with at least `min_support` derived SNVs, which approximates
    yhaplo's tree traversal without the full build-specific reference.

    For BAM/CRAM inputs, derives a chrY VCF on demand (cached per file).
    """
    import json as _json
    snp_file = os.path.join(HAPLOGROUP_DATA_DIR, "ydna_snps_grch38.json")
    if not os.path.exists(snp_file):
        return _fail("Y-DNA haplogroup: SNP table missing",
                     f"Expected GRCh38 ISOGG table at {snp_file}. "
                     f"Run scripts/build_haplogroup_data.py first.",
                     test_type="specialized", method="Y-DNA haplogroup")

    with open(snp_file) as f:
        snps = _json.load(f)

    # If we were handed a BAM/CRAM, call variants on chrY first (or reuse the
    # cached VCF written by sex_var_chry / earlier runs).
    ftype = _detect_file_type(vcf_path)
    if ftype in ("bam", "cram"):
        has_chr_aln = _detect_alignment_chr_prefix(vcf_path)
        chrY_region = "chrY" if has_chr_aln else "Y"
        derived, err = _get_or_call_region_vcf(vcf_path, chrY_region)
        if not derived:
            return _fail("Y-DNA haplogroup: chrY variant calling failed",
                         err or "unknown error",
                         test_type="specialized", method="Y-DNA haplogroup")
        vcf_path = derived

    # Detect chr prefix from the VCF
    has_chr = _detect_chr_prefix(vcf_path)
    chrY = "chrY" if has_chr else "Y"

    # Are there any chrY variants at all? A female sample has none.
    n_chry, _, rc = _run([
        BCFTOOLS, "view", "-H", "-r", chrY, vcf_path,
    ], timeout=300)
    chry_variant_count = len([l for l in n_chry.splitlines() if l.strip()]) if rc == 0 else 0

    if chry_variant_count == 0:
        return _warn(
            "Y-DNA haplogroup: no chrY variants",
            "Sample has no chrY variant calls — likely female or VCF lacks "
            "chrY coverage. Y-DNA haplogroup cannot be determined.",
            test_type="specialized", method="Y-DNA haplogroup",
            summary="Not determinable: no chrY variants in VCF.",
            chry_variant_count=0,
        )

    # Batch-query all SNP positions on chrY from the VCF. 20K regions on
    # the command line overflows ARG_MAX, so write them to a regions file
    # and use bcftools -R. Multiple ISOGG entries can share a position, so
    # keep them all to avoid silently dropping subclade-specific markers.
    pos_to_snps = {}
    for s in snps:
        key = (chrY, s["pos"])
        pos_to_snps.setdefault(key, []).append(s)
    with tempfile.NamedTemporaryFile(
            mode="w", suffix=".tsv", delete=False, dir=SCRATCH) as rf:
        regions_file = rf.name
        for (c, p) in pos_to_snps.keys():
            rf.write(f"{c}\t{p}\n")

    try:
        stdout, _, rc = _run([
            BCFTOOLS, "query",
            "-R", regions_file,
            "-f", "%CHROM\t%POS\t%REF\t%ALT\t[%GT]\n",
            vcf_path,
        ], timeout=600)
    finally:
        try:
            os.remove(regions_file)
        except OSError:
            pass

    if rc != 0:
        return _fail("Y-DNA haplogroup: bcftools query failed",
                     "Could not query chrY positions from VCF.",
                     test_type="specialized", method="Y-DNA haplogroup")

    # Precompute how many ISOGG markers each haplogroup label has in total
    # across the whole (lifted) table. We use this to compute a per-label
    # "match rate" so we can distinguish labels that are strongly supported
    # (high fraction of their markers derived) from ones where a few
    # markers coincidentally match.
    total_in_isogg = {}
    for s in snps:
        total_in_isogg[s["haplogroup"]] = total_in_isogg.get(s["haplogroup"], 0) + 1

    # Count derived-allele SNPs per haplogroup label.
    derived_by_hg = {}
    derived_snps = []
    for line in stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 5:
            continue
        chrom = parts[0]
        pos = int(parts[1])
        ref = parts[2]
        alt = parts[3].split(",")[0]  # primary ALT only
        gt = parts[4]

        if not _has_alt_allele(gt):
            continue

        candidates = pos_to_snps.get((chrom, pos), [])
        for snp in candidates:
            # Sample must carry the exact derived ALT allele listed in ISOGG.
            if ref != snp["ref"] or alt != snp["alt"]:
                continue
            derived_by_hg[snp["haplogroup"]] = derived_by_hg.get(snp["haplogroup"], 0) + 1
            derived_snps.append(f"{snp['name']}({snp['haplogroup']})")

    if not derived_by_hg:
        return _warn(
            "Y-DNA haplogroup: no derived SNPs found",
            f"Queried {len(snps)} ISOGG chrY positions; none matched a derived "
            f"allele in this sample ({chry_variant_count:,} chrY variants total).",
            test_type="specialized", method="Y-DNA haplogroup",
            summary="Root (A0-T): no derived alleles observed.",
            chry_variant_count=chry_variant_count,
            snps_queried=len(snps),
        )

    # Pick the deepest strongly-supported haplogroup label.
    #
    # The earlier ranking of (count, label_len) was broken: "E" at the root
    # of the tree has ~340 markers while its European subclade E1b1b1a1b1
    # (E-V13) has ~30, so the root swamped the specific call even when the
    # sample was clearly V13. Here we:
    #   1. Filter to labels with a meaningful number of derived markers
    #      (≥5) AND a ≥50% match rate (matches / total_ISOGG_for_label).
    #   2. Among those, pick the LONGEST label (= deepest tree branch),
    #      tie-breaking on raw count then match rate.
    # This picks E1b1b1a1b1 over its "E" ancestor whenever the subclade
    # markers are actually present.
    MIN_MATCHES = 5
    MIN_RATE = 0.5
    strong = {
        hg: cnt for hg, cnt in derived_by_hg.items()
        if cnt >= MIN_MATCHES
        and total_in_isogg.get(hg, 0) > 0
        and cnt / total_in_isogg[hg] >= MIN_RATE
    }
    if strong:
        def deep_rank(item):
            hg, count = item
            rate = count / total_in_isogg.get(hg, 1)
            return (len(hg), count, rate)
        best_hg, best_count = max(strong.items(), key=deep_rank)
    else:
        # Fall back to the old (count, len) ordering when nothing meets
        # the strict thresholds — usually a very low-coverage sample.
        def count_rank(item):
            return (item[1], len(item[0]))
        best_hg, best_count = max(derived_by_hg.items(), key=count_rank)

    # Also report top-5 strongly-supported labels (by depth) for context.
    def report_rank(item):
        hg, cnt = item
        return (cnt, len(hg))
    top5 = sorted(derived_by_hg.items(), key=report_rank, reverse=True)[:5]
    top3_str = ", ".join(
        f"{h}({c}/{total_in_isogg.get(h, '?')})" for h, c in top5
    )

    headline = f"Y-DNA: {best_hg} ({best_count} derived SNPs)"
    return _pass(
        headline,
        test_type="specialized",
        method="Y-DNA haplogroup",
        haplogroup=best_hg,
        derived_snp_count=best_count,
        top_candidates=top3_str,
        chry_variant_count=chry_variant_count,
        snps_queried=len(snps),
        summary=f"Top Y-DNA haplogroup: {best_hg} with {best_count} derived SNPs. "
                f"Runners-up: {top3_str}. Based on {len(snps):,} ISOGG markers "
                f"(lifted GRCh37→GRCh38).",
    )


def _run_mt_haplogroup(vcf_path):
    """Call a detailed mtDNA haplogroup via HaploGrep3 and PhyloTree 17.2.

    We attempted a custom marker-matching classifier first; it was brittle
    because a few markers are ubiquitous outside of H (2706G, 14766T, …)
    which swamped the signal of the sample's true subclade. HaploGrep3
    walks the full PhyloTree tree and assigns a quality score, which is
    both more accurate and the de-facto community tool.

    For BAM/CRAM inputs, derives a chrM VCF on demand (cached per file).
    """
    # If we were handed a BAM/CRAM, call variants on chrM first.
    ftype = _detect_file_type(vcf_path)
    if ftype in ("bam", "cram"):
        has_chr_aln = _detect_alignment_chr_prefix(vcf_path)
        chrM_region = "chrM" if has_chr_aln else "MT"
        derived, err = _get_or_call_region_vcf(vcf_path, chrM_region)
        if not derived:
            return _fail("mtDNA haplogroup: chrM variant calling failed",
                         err or "unknown error",
                         test_type="specialized", method="mtDNA haplogroup")
        vcf_path = derived

    has_chr = _detect_chr_prefix(vcf_path)
    chrM = "chrM" if has_chr else "MT"

    # Quick check: does the VCF have any chrM variants at all?
    stdout, _, rc = _run([
        BCFTOOLS, "view", "-H", "-r", chrM, vcf_path,
    ], timeout=120)
    if rc != 0 or not stdout.strip():
        return _warn(
            "mtDNA haplogroup: no chrM variants",
            "Sample VCF has no variants on chrM/MT, cannot classify.",
            test_type="specialized", method="mtDNA haplogroup",
            summary="Not determinable: no chrM variants in VCF.",
        )
    n_variants = len([l for l in stdout.splitlines() if l.strip()])

    if not os.path.exists(HAPLOGREP3_BIN):
        return _fail(
            "mtDNA haplogroup: HaploGrep3 not installed",
            f"Expected HaploGrep3 at {HAPLOGREP3_BIN}. "
            f"Install from https://github.com/genepi/haplogrep3/releases.",
            test_type="specialized", method="mtDNA haplogroup")

    # HaploGrep3 expects contig name "MT" (rCRS convention). Rewrite
    # "chrM" → "MT" in a temporary VCF copy.
    with tempfile.TemporaryDirectory(dir=SCRATCH, prefix="mthap_") as td:
        chrm_vcf = os.path.join(td, "chrM.vcf")
        out_file = os.path.join(td, "result.txt")
        stdout, stderr, rc = _run([
            BCFTOOLS, "view", "-r", chrM, vcf_path,
        ], timeout=120)
        if rc != 0:
            return _fail("mtDNA haplogroup: bcftools view failed", stderr[:300],
                         test_type="specialized", method="mtDNA haplogroup")
        with open(chrm_vcf, "w") as f:
            for line in stdout.splitlines():
                if line.startswith("##contig=<ID=chrM,"):
                    f.write(line.replace("ID=chrM,", "ID=MT,") + "\n")
                elif line.startswith("#"):
                    f.write(line + "\n")
                else:
                    parts = line.split("\t")
                    if parts and parts[0] == "chrM":
                        parts[0] = "MT"
                    f.write("\t".join(parts) + "\n")

        # Put the conda env's Java on PATH so HaploGrep3 finds it.
        env = os.environ.copy()
        env["PATH"] = f"/home/nimrod_rotem/conda/envs/genomics/bin:{env.get('PATH', '')}"
        import subprocess as _sp
        try:
            proc = _sp.run(
                [HAPLOGREP3_BIN, "classify",
                 "--in", chrm_vcf,
                 "--tree", HAPLOGREP3_TREE,
                 "--out", out_file],
                capture_output=True, text=True, timeout=300, env=env,
            )
        except _sp.TimeoutExpired:
            return _fail("mtDNA haplogroup: HaploGrep3 timeout",
                         "HaploGrep3 exceeded 5 min", test_type="specialized",
                         method="mtDNA haplogroup")

        if proc.returncode != 0 or not os.path.exists(out_file):
            return _fail("mtDNA haplogroup: HaploGrep3 failed",
                         (proc.stderr or proc.stdout)[:500],
                         test_type="specialized", method="mtDNA haplogroup")

        # Result format: TSV with header row then one row per sample.
        # "SampleID"\t"Haplogroup"\t"Rank"\t"Quality"\t"Range"
        with open(out_file) as f:
            lines = [l.strip() for l in f if l.strip()]
        if len(lines) < 2:
            return _fail("mtDNA haplogroup: empty HaploGrep3 result",
                         "HaploGrep3 produced no rows",
                         test_type="specialized", method="mtDNA haplogroup")
        fields = [x.strip('"') for x in lines[1].split("\t")]
        if len(fields) < 4:
            return _fail("mtDNA haplogroup: unexpected HaploGrep3 format",
                         f"row: {lines[1]!r}",
                         test_type="specialized", method="mtDNA haplogroup")
        haplogroup = fields[1]
        try:
            quality = float(fields[3])
        except ValueError:
            quality = None

    q_str = f", quality={quality:.3f}" if quality is not None else ""
    headline = f"mtDNA: {haplogroup} ({n_variants} chrM variants{q_str})"
    return _pass(
        headline,
        test_type="specialized",
        method="mtDNA haplogroup",
        haplogroup=haplogroup,
        quality=quality,
        chrm_variant_count=n_variants,
        tree=HAPLOGREP3_TREE,
        summary=(f"HaploGrep3 assignment: {haplogroup} "
                 f"(quality {quality:.3f}" + (")" if quality is not None else "")
                 + f". Based on {n_variants} chrM variants in the VCF, "
                 f"classified against PhyloTree ({HAPLOGREP3_TREE})."),
    )


def _run_neanderthal(vcf_path):
    """Estimate Neanderthal introgression from Y-DNA / ADMIXTURE context.

    A first attempt used a hand-curated 20-SNV tag panel, but (a) the panel
    was too small for statistical power (0/20 positive is consistent with
    anything from 0% to ~10%) and (b) several of the hardcoded REF bases
    didn't match the GRCh38 reference FASTA. Proper genome-wide Neanderthal
    estimation requires tools like `admixfrog` or `S_star` with Altai /
    Vindija / Chagyrskaya reference VCFs, which aren't installed.

    As a pragmatic fallback we report an expected-range estimate based on
    the sample's ADMIXTURE-inferred super-population — 1000G and Prüfer et
    al. populations have well-characterised mean Neanderthal fractions:
      AFR ≈ 0.0-0.3%   (Yoruba baseline)
      EUR ≈ 1.8-2.4%
      EAS ≈ 1.8-2.6%
      SAS ≈ 1.7-2.3%
      AMR ≈ 1.5-2.0%
    Clearly labelled as a population-based estimate, not a direct measurement.
    """
    # Delegate ancestry inference to the PCA runner so we get the same
    # closest-population call as the PCA / ADMIXTURE tests.
    pca_result = _run_pca_1000g(vcf_path)
    if not isinstance(pca_result, dict) or pca_result.get("status") == "failed":
        return _fail(
            "Neanderthal: PCA prerequisite failed",
            pca_result.get("error", "PCA projection failed") if isinstance(pca_result, dict) else "PCA failed",
            test_type="specialized", method="Neanderthal %",
        )
    top_pop = pca_result.get("closest_population") or "unknown"

    # Published mean Neanderthal fractions (Prüfer et al. 2014, 2017;
    # 1000G phase 3 archaic analysis).
    POP_NEANDERTHAL = {
        "AFR": (0.0, 0.3),
        "EUR": (1.8, 2.4),
        "EAS": (1.8, 2.6),
        "SAS": (1.7, 2.3),
        "AMR": (1.5, 2.0),
    }
    lo, hi = POP_NEANDERTHAL.get(top_pop, (None, None))

    if lo is None:
        return _warn(
            "Neanderthal %: not directly measured",
            "This test currently reports a population-based estimate from "
            "the PCA result, since admixfrog / S_star and the Altai / "
            "Vindija / Chagyrskaya archaic reference VCFs aren't installed. "
            "PCA closest population is unknown for this sample, so no "
            "range is available.",
            test_type="specialized", method="Neanderthal %",
            summary="Not determinable: PCA population unknown.",
        )

    mid = (lo + hi) / 2
    headline = f"Neanderthal: ~{mid:.1f}% (population estimate, {top_pop} range {lo}-{hi}%)"
    return _pass(
        headline,
        test_type="specialized",
        method="Neanderthal % (population estimate)",
        pca_population=top_pop,
        estimated_percent=mid,
        population_range=f"{lo}-{hi}%",
        summary=(
            f"Population-based estimate: {top_pop} samples typically carry "
            f"{lo}-{hi}% Neanderthal ancestry (Prüfer 2014/2017, 1000G). "
            f"Direct measurement requires admixfrog or S_star with archaic "
            f"reference VCFs (not installed); this test reports the mean "
            f"for the sample's inferred super-population instead."
        ),
    )


def _run_hla_typing(aln_path):
    """Type HLA-A/B/C/DRB1/DQB1/DPB1 (and other class I/II loci) via T1K.

    T1K's bam-extractor only supports BAM (not CRAM), so for CRAM inputs we
    first slice the MHC region (chr6:28-34Mb) into a temporary BAM with the
    same `--input-fmt-option ignore_md5=1` we use for the chr22 / chrX QC
    pipeline. The MHC slice is small (~60 MB on a 30x WGS), and T1K runs
    against the IPD-IMGT/HLA reference at /data/t1k_ref/hla in ~1 minute.

    Reports the top two alleles per HLA-class-I/II gene of clinical interest
    plus a sample of class-Ib / non-classical loci that T1K also calls.
    """
    ftype = _detect_file_type(aln_path)
    if ftype == "vcf":
        return _warn(
            "HLA typing: requires BAM/CRAM input",
            "HLA typing infers alleles from raw read alignments to the IMGT "
            "HLA reference; it cannot run on a VCF (which only contains "
            "variants relative to GRCh38, not full HLA-locus reads). Switch "
            "the active file to the underlying BAM/CRAM and re-run.",
            test_type="specialized", method="HLA typing",
            summary="HLA typing not possible from a VCF — needs aligned reads.",
        )

    if not (os.path.exists(T1K_BIN) and os.path.exists(T1K_HLA_REF)
            and os.path.exists(T1K_HLA_COORD)):
        return _fail(
            "HLA typing: T1K not configured",
            f"Expected T1K at {T1K_BIN} and reference at {T1K_HLA_REF} / "
            f"{T1K_HLA_COORD}. Install via "
            "`mamba install -n genomics -c bioconda t1k` and build the HLA "
            "reference with t1k-build.pl --download IPD-IMGT/HLA -g <gtf> "
            "into /data/t1k_ref/hla/.",
            test_type="specialized", method="HLA typing")

    has_chr = _detect_alignment_chr_prefix(aln_path)
    region = "chr6:28000000-34000000" if has_chr else "6:28000000-34000000"

    with tempfile.TemporaryDirectory(dir=SCRATCH, prefix="hla_") as td:
        slice_bam = os.path.join(td, "mhc.bam")

        if ftype == "cram":
            ref = _pick_reference_for(aln_path)
            if not os.path.exists(ref):
                return _fail("HLA typing: reference fasta not found",
                             f"CRAM decoding needs a matching reference; tried {ref}",
                             test_type="specialized", method="HLA typing")
            _, stderr, rc = _run([
                SAMTOOLS, "view",
                "--input-fmt-option", "ignore_md5=1",
                "-T", ref,
                "-b", "-o", slice_bam,
                str(aln_path), region,
            ], timeout=1800)
            if rc != 0:
                err = "\n".join(l for l in stderr.splitlines()
                               if "no version information" not in l)
                return _fail("HLA typing: MHC extraction failed",
                             f"samtools view failed (rc={rc}): {err[:500]}",
                             test_type="specialized", method="HLA typing")
        else:  # plain BAM
            _, stderr, rc = _run([
                SAMTOOLS, "view", "-b", "-o", slice_bam,
                str(aln_path), region,
            ], timeout=1800)
            if rc != 0:
                return _fail("HLA typing: MHC extraction failed",
                             stderr[:500], test_type="specialized",
                             method="HLA typing")

        if not os.path.exists(slice_bam) or os.path.getsize(slice_bam) < 100:
            return _fail("HLA typing: empty MHC slice",
                         "samtools produced an empty slice for chr6:28-34Mb",
                         test_type="specialized", method="HLA typing")

        _, _, _ = _run([SAMTOOLS, "index", slice_bam], timeout=600)

        # Run T1K. --abnormalUnmapFlag is required for BAMs where unmapped
        # mate pairs aren't co-located (T1K complains otherwise).
        _, stderr, rc = _run([
            T1K_BIN,
            "-b", slice_bam,
            "-f", T1K_HLA_REF,
            "-c", T1K_HLA_COORD,
            "--preset", "hla-wgs",
            "--abnormalUnmapFlag",
            "-t", "8",
            "--od", td,
            "-o", "sample",
        ], timeout=1800)

        geno_tsv = os.path.join(td, "sample_genotype.tsv")
        if not os.path.exists(geno_tsv):
            return _fail("HLA typing: T1K produced no output",
                         (stderr[:500] or "no error message").strip(),
                         test_type="specialized", method="HLA typing")

        # Parse the genotype TSV. Each row:
        #   gene  num_alleles  allele1  abundance1  qual1  allele2  abundance2  qual2  [extras]
        results = {}
        with open(geno_tsv) as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 5:
                    continue
                gene = parts[0]
                try:
                    n_alleles = int(parts[1])
                except ValueError:
                    continue
                alleles = []
                if n_alleles >= 1 and parts[2] not in (".", ""):
                    alleles.append(parts[2].split(",")[0])  # primary call
                if n_alleles >= 2 and len(parts) > 5 and parts[5] not in (".", ""):
                    alleles.append(parts[5].split(",")[0])
                if alleles:
                    results[gene] = alleles

    if not results:
        return _warn(
            "HLA typing: no calls",
            "T1K produced no high-confidence HLA calls. Coverage may be too "
            "low at the MHC region.",
            test_type="specialized", method="HLA typing",
            summary="No HLA calls (low coverage?)",
        )

    # Show the canonical class-I and class-II loci first
    headline_loci = ["HLA-A", "HLA-B", "HLA-C", "HLA-DRB1", "HLA-DQB1", "HLA-DPB1"]
    headline_parts = []
    for g in headline_loci:
        if g in results:
            short = "/".join(_short_hla_allele(a) for a in results[g])
            headline_parts.append(f"{g.removeprefix('HLA-')}*{short}")
    headline = "HLA: " + (", ".join(headline_parts) if headline_parts else "no class-I/II calls")

    summary_lines = ["HLA typing (T1K, IPD-IMGT/HLA reference):"]
    for g in headline_loci:
        if g in results:
            summary_lines.append(f"  {g}: {' / '.join(results[g])}")
    other_loci = sorted(g for g in results if g not in headline_loci)
    if other_loci:
        summary_lines.append("")
        summary_lines.append("Additional loci:")
        for g in other_loci:
            summary_lines.append(f"  {g}: {' / '.join(results[g])}")

    return _pass(
        headline,
        test_type="specialized",
        method="HLA typing",
        alleles={g: results[g] for g in headline_loci if g in results},
        all_loci={g: results[g] for g in sorted(results)},
        summary="\n".join(summary_lines),
    )


def _short_hla_allele(allele):
    """Strip the HLA-GENE* prefix from a T1K allele string for the headline.

    e.g. 'HLA-A*33:03:01' -> '33:03:01'
    """
    if "*" in allele:
        return allele.split("*", 1)[1]
    return allele


def _run_admixture_from_pca(vcf_path):
    """Soft ADMIXTURE-like proportions derived from PCA distances.

    Instead of running the actual ADMIXTURE binary (which needs a supervised
    reference build), we reuse the 1000G PCA projection and compute a softmax
    over inverse distances to AFR/EUR/EAS/SAS/AMR centroids. This gives
    qualitatively similar proportions for cleanly one-population samples and
    reasonable approximations for admixed samples.

    The caller lists this as a K=5 supervised run — we produce the same 5
    superpopulation fractions, clearly labeled as a PCA-derived approximation.
    """
    import math

    # Delegate the heavy lifting to the PCA runner so we get a consistent
    # projection and the same centroid data it uses.
    pca_result = _run_pca_1000g(vcf_path)
    if not isinstance(pca_result, dict) or pca_result.get("status") == "failed":
        return _fail(
            "ADMIXTURE: PCA prerequisite failed",
            pca_result.get("error", "PCA projection failed") if isinstance(pca_result, dict) else "PCA failed",
            test_type="specialized", method="ADMIXTURE (K=5)",
        )

    sample_pcs = pca_result.get("pcs")
    if not sample_pcs:
        return _fail(
            "ADMIXTURE: missing PCA data",
            "PCA projection returned no sample PCs.",
            test_type="specialized", method="ADMIXTURE (K=5)",
        )

    # Recompute centroids from the cached PCA reference (PCA runner discards
    # them after picking the closest population, so we reload here).
    cache_dir = os.path.join(PGS_CACHE, "pca_1000g")
    refvec_file = os.path.join(cache_dir, "ref.eigenvec")
    psam_file = os.path.join(cache_dir, "ref.psam")
    centroids = _load_pca_centroids(refvec_file, psam_file)
    if not centroids:
        return _fail(
            "ADMIXTURE: centroid data unavailable",
            f"Could not load PCA centroids from {refvec_file} / {psam_file}.",
            test_type="specialized", method="ADMIXTURE (K=5)",
        )

    # Filter to the 5 superpopulations
    superpops = ["AFR", "EUR", "EAS", "SAS", "AMR"]
    dist = {}
    # Use first 4 PCs (captures most of the continental variance)
    n_pcs_use = min(4, len(sample_pcs))
    for sp in superpops:
        c = centroids.get(sp)
        if not c:
            continue
        n = min(n_pcs_use, len(c))
        d = math.sqrt(sum((float(sample_pcs[i]) - float(c[i])) ** 2
                          for i in range(n)))
        dist[sp] = d

    if not dist:
        return _fail("ADMIXTURE: no superpopulation centroids",
                     "PCA did not return AFR/EUR/EAS/SAS/AMR centroids.",
                     test_type="specialized", method="ADMIXTURE (K=5)")

    # Softmax over negative distances with a sharpness parameter. Larger
    # temperature → softer (more spread); smaller → sharper (near 1-hot).
    temperature = 0.02
    scores = {sp: -d / temperature for sp, d in dist.items()}
    m = max(scores.values())
    exp_scores = {sp: math.exp(s - m) for sp, s in scores.items()}
    total = sum(exp_scores.values())
    props = {sp: v / total for sp, v in exp_scores.items()}

    # Format: percentages summing to ~100
    pct = {sp: round(100 * p, 1) for sp, p in props.items()}
    # Top population
    top_sp = max(pct.items(), key=lambda x: x[1])

    headline = f"ADMIXTURE (K=5): {top_sp[0]} {top_sp[1]:.1f}% (" + ", ".join(
        f"{sp}:{pct.get(sp, 0):.0f}" for sp in superpops) + ")"
    return _pass(
        headline,
        test_type="specialized",
        method="ADMIXTURE (K=5) — PCA proxy",
        proportions=pct,
        top_population=top_sp[0],
        top_proportion=top_sp[1],
        distances={sp: round(d, 4) for sp, d in dist.items()},
        summary=(
            f"Supervised ADMIXTURE approximation via PCA distances. Top: "
            f"{top_sp[0]} ({top_sp[1]:.1f}%). Full proportions: " +
            ", ".join(f"{sp}={pct.get(sp, 0):.1f}%" for sp in superpops) +
            ". This is derived from PCA centroid distances, not the actual "
            "ADMIXTURE binary, so admixed samples may be under-resolved."
        ),
    )


def _run_roh(vcf_path):
    """Run ROH analysis using plink1.9 --homozyg (plink2 doesn't support --homozyg yet).

    For BAM/CRAM inputs, derives an autosomal VCF on demand by reusing the
    PCA cache (calls genotypes at the ~106K LD-pruned PCA sites). The
    resulting FROH is necessarily a coarse estimate from sparse data —
    plink can still detect long ROH segments, but the absolute total ROH
    length will be underestimated relative to a dense genome-wide callset.
    """
    ftype = _detect_file_type(vcf_path)
    is_sparse_pca_callset = False
    if ftype in ("bam", "cram"):
        derived, err = _derive_pca_vcf_from_cram(vcf_path)
        if not derived:
            return _fail("ROH: variant calling from CRAM failed",
                         err or "unknown error",
                         test_type="specialized", method="Runs of Homozygosity")
        vcf_path = derived
        is_sparse_pca_callset = True

    with tempfile.TemporaryDirectory(dir=SCRATCH, prefix="roh_") as tmpdir:
        vcf_path = _ensure_indexed(vcf_path)

        roh_prefix = os.path.join(tmpdir, "roh_result")
        cmd = [
            PLINK,
            "--vcf", vcf_path,
            "--double-id",
            "--allow-extra-chr",
            "--vcf-half-call", "missing",
            "--homozyg",
            "--homozyg-snp", "50",
            "--homozyg-kb", "500",
            "--homozyg-density", "50",
            "--homozyg-gap", "1000",
            "--homozyg-window-snp", "50",
            "--homozyg-window-het", "1",
            "--homozyg-window-missing", "5",
            "--out", roh_prefix,
        ]
        stdout, stderr, rc = _run(cmd, timeout=1800)

        hom_file = f"{roh_prefix}.hom"
        indiv_file = f"{roh_prefix}.hom.indiv"

        if not os.path.exists(indiv_file):
            return _fail("ROH: plink --homozyg failed",
                         stderr[:500] or stdout[-500:] or "no indiv file produced",
                         test_type="specialized", method="ROH")

        # Parse indiv file: NSEG, KB, KBAVG
        try:
            with open(indiv_file) as f:
                header = f.readline().strip().split()
                row = f.readline().strip().split()
            data = dict(zip(header, row))
            nseg = int(data.get("NSEG", 0))
            total_kb = float(data.get("KB", 0))
            avg_kb = float(data.get("KBAVG", 0))
        except Exception as e:
            return _fail("ROH: could not parse result", str(e),
                         test_type="specialized", method="ROH")

        # FROH ~ total ROH / autosomal length (~2.88 Gb)
        froh = total_kb * 1000 / 2_881_000_000
        suffix = " [sparse PCA callset — FROH underestimated]" if is_sparse_pca_callset else ""
        headline = f"ROH: {nseg} segments, {total_kb/1000:.1f} Mb total, FROH={froh:.4f}{suffix}"
        summary = (f"ROH: {nseg} segments totaling {total_kb:,.0f} kb; "
                   f"average segment {avg_kb:,.0f} kb; FROH={froh:.4f}")
        if is_sparse_pca_callset:
            summary += (" (Computed from the ~106K LD-pruned PCA sites because the input "
                        "is a CRAM. Long ROH are detectable but the total ROH length and "
                        "FROH are systematically lower than a dense genome-wide callset "
                        "would give.)")
        return _pass(headline, test_type="specialized", method="Runs of Homozygosity",
                     n_segments=nseg, total_kb=total_kb, avg_kb=avg_kb, froh=round(froh, 5),
                     sparse_callset=is_sparse_pca_callset,
                     summary=summary)


# ─── Main Dispatcher ─────────────────────────────────────────────

# Test-type compatibility with input file type. Only a handful of tests can
# run on BAM/CRAM (the read-count sex checks inside vcf_stats); everything
# else needs variant calls. Failing early in `run_test` gives the user a
# clear "requires VCF" message instead of a cryptic bcftools error.
VCF_ONLY_TEST_TYPES = {
    "variant_lookup",
    "pgs_score",
    "rsid_pgs_score",
    "clinvar_screen",
    # "specialized" is NOT listed here — some specialized methods (PCA) can
    # handle CRAM/BAM inputs internally via on-demand variant calling.
    # Methods that can't are gated inside run_specialized() instead.
}

# Order matters: prefer the most processed (annotated > merged > raw call
# set > gVCF), so a ClinVar screen on a CRAM picks up annotations
# automatically when a sibling annotated VCF is available.
_SIBLING_VCF_SUFFIXES = (
    ".annotated.vcf.gz",
    ".merged.vcf.gz",
    ".vcf.gz",
    ".g.vcf.gz",
)


def _find_sibling_vcf(aln_path):
    """When the user picks a BAM/CRAM, look for a sibling VCF derived from
    the same sample (e.g. ``sample.cram`` ↔ ``sample.vcf.gz``). Returns the
    sibling path or None.

    The reason this exists: WGS variant calling from a CRAM takes hours,
    so for any variant-dependent test the only practical thing to do on a
    user's "click run" is to delegate to a pre-existing call set if one
    is on disk for the same sample.
    """
    p = Path(aln_path)
    # Strip the alignment extension(s) to get the base sample name.
    stem = p.name
    for ext in (".cram", ".bam"):
        if stem.lower().endswith(ext):
            stem = stem[: -len(ext)]
            break
    candidates = []
    # Same directory as the CRAM
    for suffix in _SIBLING_VCF_SUFFIXES:
        candidates.append(p.parent / f"{stem}{suffix}")
    # Common neighbor: a vcfs/ directory next to the CRAM dir
    for suffix in _SIBLING_VCF_SUFFIXES:
        candidates.append(p.parent.parent / "vcfs" / f"{stem}{suffix}")
    # Absolute well-known location used on this machine
    for suffix in _SIBLING_VCF_SUFFIXES:
        candidates.append(Path("/data/vcfs") / f"{stem}{suffix}")

    seen = set()
    for c in candidates:
        cs = str(c)
        if cs in seen:
            continue
        seen.add(cs)
        if c.exists() and c.is_file() and c.stat().st_size > 0:
            return cs
    return None


def run_test(vcf_path, test_def):
    """Main dispatcher: run a test based on its definition.
    Guarantees that every return dict has a 'status' and 'headline' field."""
    test_type = test_def["test_type"]
    params = test_def["params"]

    ftype = _detect_file_type(vcf_path)

    # If the user picked a BAM/CRAM but the test needs variant calls,
    # transparently substitute a sibling VCF from the same sample (e.g.
    # SZ7A76M9LNU.cram → SZ7A76M9LNU.vcf.gz). Genome-wide variant calling
    # from a CRAM is hours-long, so for an interactive dashboard this is
    # the only realistic pipeline. The substitution is logged so the user
    # can see what happened.
    if test_type in VCF_ONLY_TEST_TYPES and ftype in ("bam", "cram"):
        sibling = _find_sibling_vcf(vcf_path)
        if sibling:
            logger.info(
                f"{test_def.get('id', '?')}: substituting sibling VCF "
                f"{sibling} for CRAM {vcf_path}"
            )
            vcf_path = sibling
            ftype = "vcf"
        else:
            return _fail(
                f"{test_def.get('name', test_type)}: requires VCF input",
                f"This test analyzes variant calls and cannot run on a "
                f"{ftype.upper()} file ({os.path.basename(vcf_path)}). "
                f"No sibling VCF was found in the same directory or in "
                f"/data/vcfs/. Add a VCF for this sample via the file "
                f"manager and re-run.",
            )

    try:
        if test_type == "variant_lookup":
            result = run_variant_lookup(vcf_path, params)
        elif test_type == "vcf_stats":
            result = run_vcf_stats(vcf_path, params)
        elif test_type == "pgs_score":
            result = run_pgs_score(vcf_path, params)
        elif test_type == "rsid_pgs_score":
            result = run_rsid_pgs_score(vcf_path, params)
        elif test_type == "clinvar_screen":
            result = run_clinvar_screen(vcf_path, params)
        elif test_type == "specialized":
            result = run_specialized(vcf_path, params)
        else:
            return _fail(f"Unknown test type: {test_type}",
                         f"Test type '{test_type}' not recognized")
    except Exception as e:
        import traceback
        return _fail(f"Exception: {type(e).__name__}",
                     f"{e}\n{traceback.format_exc()[:500]}")

    # Guarantee status and headline are set
    if "status" not in result:
        if "error" in result:
            result["status"] = "failed"
            result.setdefault("headline", f"Error: {result['error'][:80]}")
        else:
            result["status"] = "passed"
            result.setdefault("headline", result.get("summary", "Completed")[:120])
    return result
