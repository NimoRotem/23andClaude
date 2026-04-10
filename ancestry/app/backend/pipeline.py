"""
Ancestry inference pipeline using gnomAD HGDP+1kGP reference panel + plink2 + Rye.

Runs the full pipeline: input detection -> variant extraction -> intersect/align ->
merge/PCA -> Rye ancestry decomposition -> ROH analysis -> interpretation.
"""

import collections
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Optional

import yaml

APP_ROOT = os.environ.get("APP_ROOT", "/data/ancestry_app")
REF_DIR = os.path.join(APP_ROOT, "reference")
REF_BED = os.path.join(REF_DIR, "ref_pruned")
POP2GROUP = os.path.join(REF_DIR, "pop2group.txt")
POP2GROUP_EA_DETAIL = os.path.join(REF_DIR, "pop2group_ea_detail.txt")
SIGNATURES_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reference", "signatures.yaml"
)
RYE_SCRIPT = os.path.join(APP_ROOT, "tools", "rye", "rye.R")
RYE_PY_SCRIPT = os.path.join(APP_ROOT, "tools", "rye", "rye.py")
# Toggle: prefer the Python port (much faster) over the R script.
RYE_USE_PYTHON = os.environ.get("RYE_USE_PYTHON", "1") not in ("0", "", "false", "False")

THREADS = max(1, os.cpu_count() or 2)
MIN_OVERLAP = 50000

# Optional chr-prefixed GRCh38 reference for decoding chr1..chr22-style BAM/CRAM files
# (e.g. CRAMs from DRAGEN). Sequence bytes match the standard hs38DH GRCh38 build.
CHR_PREFIXED_FASTA = "/data/refs/hs38DH.fa"

# Ensure bioinformatics tools are on PATH (conda env may not be inherited)
_GENOMICS_BIN = '/home/nimo/miniconda3/envs/genomics/bin'
if _GENOMICS_BIN not in os.environ.get('PATH', ''):
    os.environ['PATH'] = _GENOMICS_BIN + ':' + os.environ.get('PATH', '')


# ---------------------------------------------------------------------------
# Typical step durations (seconds) for ETA estimation — indexed by input type.
# Rough estimates for WGS VCF (~4M variants). BAM is slower due to mpileup.
# ---------------------------------------------------------------------------
_TYPICAL_DURATIONS = {
    "vcf": {
        "Normalizing VCF variants": 120,
        "Indexing normalized VCF": 10,
        "Converting VCF to PLINK binary format": 20,
        "Computing variant overlap with reference panel": 15,
        "Aligning overlapping variants to reference alleles": 15,
        "Subsetting reference panel to overlapping variants": 10,
        "Merging sample with reference panel": 30,
        "Cleaning merged dataset": 10,
        "Running PCA (20 components)": 60,
        "Running Rye ancestry decomposition": 300,
        "Running Runs of Homozygosity (ROH) analysis": 30,
        "Detecting population signatures": 2,
        "Finalizing results": 2,
    },
    "bam": {
        "Extracting target positions from reference panel": 15,
        "Calling variants from BAM/CRAM": 600,
        "Concatenating chromosome VCFs": 10,
        "Converting called VCF to PLINK binary format": 20,
        "Computing variant overlap with reference panel": 15,
        "Aligning overlapping variants to reference alleles": 15,
        "Subsetting reference panel to overlapping variants": 10,
        "Merging sample with reference panel": 30,
        "Cleaning merged dataset": 10,
        "Running PCA (20 components)": 60,
        "Running Rye ancestry decomposition": 300,
        "Running Runs of Homozygosity (ROH) analysis": 30,
        "Detecting population signatures": 2,
        "Finalizing results": 2,
    },
}


class PipelineError(Exception):
    """Raised when a pipeline step fails."""
    pass


class JobTracker:
    """Thread-safe tracker for live pipeline progress, logs, step timings, and sub-tasks.

    Stored per-job in main.py and streamed to the frontend via SSE.
    """

    def __init__(self, input_type: str = "vcf"):
        self._lock = threading.Lock()
        self.input_type = input_type
        self.progress = 0
        self.current_step = "Queued"
        self.status = "queued"
        # Ring buffer of recent log lines (last 200)
        self.log_lines: collections.deque = collections.deque(maxlen=200)
        # Step history: list of {name, started_at, ended_at, duration_s}
        self.steps: list = []
        self._current_step_start: float = 0
        # Sub-tasks for parallel stages (e.g., per-chromosome)
        # {task_id: {label, status, elapsed_s, detail}}
        self.sub_tasks: dict = {}
        # Live activity line (last stdout line from running process)
        self.live_line: str = ""
        # Whether current step has indeterminate progress
        self.indeterminate: bool = False
        # External callback (set by main.py to persist to job store)
        self.on_update: Optional[Callable] = None

    def set_progress(self, pct: int, step: str):
        """Called by pipeline steps to update progress."""
        with self._lock:
            # Close previous step
            if self.steps and self.steps[-1].get("ended_at") is None:
                now = time.time()
                self.steps[-1]["ended_at"] = now
                self.steps[-1]["duration_s"] = round(now - self.steps[-1]["started_at"], 1)

            self.progress = pct
            self.current_step = step
            self.indeterminate = False
            self.sub_tasks = {}  # Reset sub-tasks on new step
            self.live_line = ""

            # Start new step record
            self.steps.append({
                "name": step,
                "started_at": time.time(),
                "ended_at": None,
                "duration_s": None,
            })
            self._current_step_start = time.time()

            self.log_lines.append(f"[{pct}%] {step}")

        if self.on_update:
            self.on_update(pct, step)

    @staticmethod
    def _clean_line(raw: str) -> str:
        """Strip ANSI escapes, backspaces, and carriage returns from terminal output."""
        # Process backspaces: each \b deletes the preceding character
        result = []
        for ch in raw:
            if ch == '\b':
                if result:
                    result.pop()
            elif ch == '\r':
                result.clear()  # carriage return resets to start of line
            else:
                result.append(ch)
        cleaned = "".join(result).rstrip()
        # Strip ANSI escape sequences
        cleaned = re.sub(r'\x1b\[[0-9;]*[mGKHJ]', '', cleaned)
        return cleaned

    def add_log(self, line: str):
        """Add a log line from subprocess output."""
        with self._lock:
            cleaned = self._clean_line(line)
            if cleaned:
                self.log_lines.append(cleaned)
                self.live_line = cleaned

    def set_sub_task(self, task_id: str, label: str, status: str, detail: str = ""):
        """Update a sub-task (e.g., per-chromosome status)."""
        with self._lock:
            self.sub_tasks[task_id] = {
                "label": label,
                "status": status,
                "detail": detail,
                "updated_at": time.time(),
            }

    def set_indeterminate(self, val: bool = True):
        """Mark current step as indeterminate (no sub-progress tracking)."""
        with self._lock:
            self.indeterminate = val

    def snapshot(self) -> dict:
        """Return a JSON-serializable snapshot of the current state."""
        with self._lock:
            typical = _TYPICAL_DURATIONS.get(self.input_type, _TYPICAL_DURATIONS["vcf"])

            # Build step list with elapsed + ETA
            steps_out = []
            for st in self.steps:
                elapsed = st["duration_s"]
                if elapsed is None:
                    elapsed = round(time.time() - st["started_at"], 1)
                # Find typical duration for ETA
                typ = None
                for key, dur in typical.items():
                    if key.lower() in st["name"].lower() or st["name"].lower() in key.lower():
                        typ = dur
                        break
                steps_out.append({
                    "name": st["name"],
                    "elapsed_s": elapsed,
                    "typical_s": typ,
                    "done": st["ended_at"] is not None,
                })

            return {
                "progress": self.progress,
                "current_step": self.current_step,
                "status": self.status,
                "live_line": self.live_line,
                "indeterminate": self.indeterminate,
                "log_lines": list(self.log_lines)[-50:],  # Last 50 for SSE
                "steps": steps_out,
                "sub_tasks": dict(self.sub_tasks),
                "step_elapsed_s": round(time.time() - self._current_step_start, 1) if self._current_step_start else 0,
            }


# Global tracker registry (job_id -> JobTracker). main.py sets these.
_job_trackers: dict = {}
_trackers_lock = threading.Lock()


def get_tracker(job_id: str) -> Optional["JobTracker"]:
    with _trackers_lock:
        return _job_trackers.get(job_id)


def register_tracker(job_id: str, tracker: "JobTracker"):
    with _trackers_lock:
        _job_trackers[job_id] = tracker


def unregister_tracker(job_id: str):
    with _trackers_lock:
        _job_trackers.pop(job_id, None)


def _run(cmd: str, cwd: str = None, check: bool = True, capture: bool = True,
         tracker: "JobTracker" = None) -> subprocess.CompletedProcess:
    """Run a shell command with optional live output streaming to a JobTracker.

    When tracker is provided, stdout/stderr are streamed line-by-line into
    tracker.add_log() so the UI shows live activity. The full output is still
    captured and returned as a CompletedProcess.
    """
    if tracker is None:
        # Legacy path: blocking capture
        result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=capture, text=True)
        if check and result.returncode != 0:
            stderr = result.stderr.strip() if result.stderr else ""
            stdout = result.stdout.strip() if result.stdout else ""
            detail = stderr or stdout or f"exit code {result.returncode}"
            raise PipelineError(f"Command failed: {cmd}\n{detail}")
        return result

    # Streaming path: pipe stdout+stderr and feed to tracker
    proc = subprocess.Popen(
        cmd, shell=True, cwd=cwd,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )

    stdout_lines = []
    for line in proc.stdout:
        stdout_lines.append(line)
        tracker.add_log(line)

    proc.wait()
    stdout_text = "".join(stdout_lines)

    result = subprocess.CompletedProcess(
        args=cmd, returncode=proc.returncode,
        stdout=stdout_text, stderr="",
    )

    if check and result.returncode != 0:
        detail = stdout_text.strip() or f"exit code {result.returncode}"
        raise PipelineError(f"Command failed: {cmd}\n{detail}")

    return result


def detect_input_type(input_path: str) -> str:
    """Detect input file type from extension."""
    path_lower = input_path.lower()
    if path_lower.endswith(".g.vcf.gz") or path_lower.endswith(".g.vcf"):
        return "gvcf"
    elif path_lower.endswith(".gvcf.gz") or path_lower.endswith(".gvcf"):
        return "gvcf"
    elif path_lower.endswith(".vcf.gz") or path_lower.endswith(".vcf"):
        return "vcf"
    elif path_lower.endswith(".bam"):
        return "bam"
    elif path_lower.endswith(".cram"):
        return "cram"
    else:
        raise PipelineError(
            f"Unrecognized file extension for: {input_path}. "
            "Supported: .vcf, .vcf.gz, .g.vcf, .g.vcf.gz, .gvcf, .gvcf.gz, .bam, .cram"
        )


def _check_ref_chr_format() -> str:
    """Check whether reference panel uses 'chr1' or '1' format. Returns 'chr' or 'num'."""
    bim_path = f"{REF_BED}.bim"
    if not os.path.exists(bim_path):
        raise PipelineError(f"Reference BIM file not found: {bim_path}")
    with open(bim_path, "r") as f:
        for line in f:
            chrom = line.split("\t")[0]
            if chrom.startswith("chr"):
                return "chr"
            else:
                return "num"
    raise PipelineError("Reference BIM file is empty")


def _check_sample_chr_format(bim_path: str) -> str:
    """Check whether sample uses 'chr1' or '1' format. Returns 'chr' or 'num'."""
    with open(bim_path, "r") as f:
        for line in f:
            chrom = line.split("\t")[0]
            if chrom.startswith("chr"):
                return "chr"
            else:
                return "num"
    raise PipelineError("Sample BIM file is empty")


def _detect_bam_chr_format(input_path: str, fasta_path: Optional[str]) -> str:
    """Detect BAM/CRAM chromosome naming convention from header. Returns 'chr' or 'num'.

    Reads @SQ lines via `samtools view -H` and inspects the first SN: field.
    """
    cmd = ["samtools", "view", "-H"]
    if fasta_path:
        cmd.extend(["--reference", fasta_path])
    cmd.append(input_path)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        raise PipelineError(f"Failed to read BAM/CRAM header: {e}")
    if result.returncode != 0:
        raise PipelineError(
            f"samtools view -H failed for {input_path}: {result.stderr.strip()}"
        )
    for line in result.stdout.split("\n"):
        if not line.startswith("@SQ"):
            continue
        for field in line.split("\t"):
            if field.startswith("SN:"):
                sn = field[3:]
                return "chr" if sn.startswith("chr") else "num"
    raise PipelineError(f"No @SQ records found in BAM/CRAM header: {input_path}")


def _get_ref_id_format() -> str:
    """Check whether reference panel variant IDs use 'chr' prefix. Returns 'chr' or 'num'."""
    bim_path = f"{REF_BED}.bim"
    with open(bim_path, "r") as f:
        for line in f:
            vid = line.split("\t")[1]
            return "chr" if vid.startswith("chr") else "num"
    return "num"


def _fix_chr_names(tmpdir: str, sample_prefix: str, ref_format: str, sample_format: str) -> str:
    """Fix chromosome and variant ID naming to match reference panel. Returns new prefix.

    Handles two independent mismatches:
    1. Chromosome column (col 0): ref may use 'chr1' or '1'
    2. Variant IDs (col 1): ref may use 'chr1:POS:REF:ALT' or '1:POS:REF:ALT'
       Note: these can differ (e.g. ref has numeric chr but chr-prefixed IDs)
    """
    bim_path = f"{sample_prefix}.bim"
    fixed_bim = os.path.join(tmpdir, "sample_chrfix.bim")

    # Check what format the reference IDs use (may differ from chromosome column)
    ref_id_format = _get_ref_id_format()

    # Check if any fixes are actually needed
    needs_fix = False
    with open(bim_path, "r") as f:
        for line in f:
            parts = line.split("\t")
            chrom = parts[0]
            vid = parts[1]
            chrom_has_chr = chrom.startswith("chr")
            vid_has_chr = vid.startswith("chr")

            if ref_format == "num" and chrom_has_chr:
                needs_fix = True
            elif ref_format == "chr" and not chrom_has_chr:
                needs_fix = True
            if ref_id_format == "chr" and not vid_has_chr:
                needs_fix = True
            elif ref_id_format == "num" and vid_has_chr:
                needs_fix = True
            break

    if not needs_fix:
        return sample_prefix

    with open(bim_path, "r") as fin, open(fixed_bim, "w") as fout:
        for line in fin:
            parts = line.split("\t")
            chrom = parts[0]

            # Fix chromosome column to match reference
            if ref_format == "num" and chrom.startswith("chr"):
                parts[0] = chrom.replace("chr", "")
            elif ref_format == "chr" and not chrom.startswith("chr"):
                parts[0] = f"chr{chrom}"

            # Fix variant ID to match reference ID format
            vid = parts[1]
            vid_has_chr = vid.startswith("chr")
            if ref_id_format == "chr" and not vid_has_chr:
                # Reference IDs have chr prefix, sample doesn't — add it
                parts[1] = f"chr{vid}"
            elif ref_id_format == "num" and vid_has_chr:
                # Reference IDs don't have chr prefix, sample does — strip it
                parts[1] = vid.replace("chr", "", 1)

            fout.write("\t".join(parts))

    fixed_prefix = os.path.join(tmpdir, "sample_chrfix")
    shutil.copy2(f"{sample_prefix}.bed", f"{fixed_prefix}.bed")
    shutil.copy2(f"{sample_prefix}.fam", f"{fixed_prefix}.fam")

    return fixed_prefix


def step_extract_variants_vcf(input_path: str, tmpdir: str, on_progress: Callable,
                               tracker: "JobTracker" = None) -> str:
    """Extract and normalize variants from VCF/gVCF input. Returns plink bed prefix."""
    on_progress(5, "Normalizing VCF variants")
    if tracker:
        tracker.set_indeterminate(True)

    norm_vcf = os.path.join(tmpdir, "norm.vcf.gz")

    _run(
        f'bcftools norm -m-any "{input_path}" '
        f'| bcftools view --types snps -m2 -M2 '
        f'| bcftools annotate --set-id \'%CHROM:%POS:%REF:%ALT\' -Oz -o "{norm_vcf}"',
        cwd=tmpdir,
        tracker=tracker,
    )

    on_progress(10, "Indexing normalized VCF")
    _run(f'tabix -p vcf "{norm_vcf}"', cwd=tmpdir, tracker=tracker)

    on_progress(12, "Converting VCF to PLINK binary format")
    sample_prefix = os.path.join(tmpdir, "sample")
    _run(
        f'plink2 --vcf "{norm_vcf}" --chr 1-22 --allow-extra-chr '
        f'--make-bed --out "{sample_prefix}"',
        cwd=tmpdir,
        tracker=tracker,
    )

    return sample_prefix


def _call_variants_for_chr(args: tuple) -> str:
    """Call variants for a single chromosome. Returns path to per-chr VCF or empty string on failure."""
    chrom, input_path, fasta_path, targets_tsv, tmpdir, tracker, chr_prefix, rename_chrs_file = args
    chr_vcf = os.path.join(tmpdir, f"chr{chrom}.vcf.gz")
    region = f"{chr_prefix}{chrom}"
    if tracker:
        tracker.set_sub_task(f"chr{chrom}", f"chr{chrom}", "running", "bcftools mpileup")
    try:
        rename_step = (
            f'| bcftools annotate --rename-chrs "{rename_chrs_file}" '
            if rename_chrs_file
            else ""
        )
        _run(
            f'bcftools mpileup -f "{fasta_path}" -T "{targets_tsv}" '
            f'-r {region} --min-MQ 20 --min-BQ 20 --max-depth 500 "{input_path}" '
            f'| bcftools call -m --ploidy GRCh38 '
            f'| bcftools view --types snps -m2 -M2 '
            f'{rename_step}'
            f"| bcftools annotate --set-id '%CHROM:%POS:%REF:%ALT' "
            f'-Oz -o "{chr_vcf}"',
            cwd=tmpdir,
        )
        _run(f'tabix -p vcf "{chr_vcf}"', cwd=tmpdir)
        if tracker:
            tracker.set_sub_task(f"chr{chrom}", f"chr{chrom}", "complete", "done")
        return chr_vcf
    except PipelineError:
        if tracker:
            tracker.set_sub_task(f"chr{chrom}", f"chr{chrom}", "failed", "error")
        return ""


def step_extract_variants_bam(input_path: str, tmpdir: str, fasta_path: str, on_progress: Callable,
                               tracker: "JobTracker" = None) -> str:
    """Extract variants from BAM/CRAM by calling against reference panel sites.

    Runs variant calling in parallel across chromosomes 1-22, then concatenates
    and converts to PLINK format. Returns plink bed prefix.
    """
    import concurrent.futures

    if not fasta_path:
        raise PipelineError(
            "A reference FASTA path (fasta_path) is required for BAM/CRAM input. "
            "Provide it via the fasta_path parameter."
        )
    if not os.path.exists(fasta_path):
        raise PipelineError(f"Reference FASTA not found: {fasta_path}")

    # Detect BAM/CRAM chromosome naming so we can match targets and region args.
    # Some files (e.g. CRAMs from DRAGEN) use 'chr1'..'chr22'; others use '1'..'22'.
    bam_chr_format = _detect_bam_chr_format(input_path, fasta_path)
    ref_bim_chr_format = _check_ref_chr_format()
    chr_prefix = "chr" if bam_chr_format == "chr" else ""

    # If the BAM/CRAM is chr-prefixed, the supplied fasta_path may not match
    # (it may have numeric sequence names). Switch to the chr-prefixed
    # reference if it exists. This is critical for CRAMs because CRAM decoding
    # looks up sequences by name.
    if bam_chr_format == "chr" and os.path.exists(CHR_PREFIXED_FASTA):
        fasta_path = CHR_PREFIXED_FASTA

    on_progress(5, f"Extracting target positions from reference panel ({bam_chr_format}-style)")
    targets_tsv = os.path.join(tmpdir, "targets.tsv")
    # Build targets file with chromosome names that match the BAM/CRAM
    if bam_chr_format == "chr" and ref_bim_chr_format == "num":
        awk_expr = '{print "chr"$1, $4}'
    elif bam_chr_format == "num" and ref_bim_chr_format == "chr":
        awk_expr = '{print substr($1,4), $4}'
    else:
        awk_expr = '{print $1, $4}'
    _run(
        f"awk -v OFS='\\t' '{awk_expr}' \"{REF_BED}.bim\" > \"{targets_tsv}\"",
        cwd=tmpdir,
        tracker=tracker,
    )

    # If the BAM/CRAM is chr-prefixed, build a rename file so the per-chr VCFs
    # are normalized to numeric chromosomes (matching the rest of the pipeline).
    rename_chrs_file = ""
    if bam_chr_format == "chr":
        rename_chrs_file = os.path.join(tmpdir, "chr_rename.txt")
        with open(rename_chrs_file, "w") as f:
            for c in range(1, 23):
                f.write(f"chr{c}\t{c}\n")
            f.write("chrX\tX\n")
            f.write("chrY\tY\n")
            f.write("chrM\tMT\n")

    # Run per-chromosome variant calling in parallel (22 chromosomes across available cores)
    n_workers = min(22, THREADS)
    on_progress(8, f"Calling variants from BAM/CRAM ({n_workers} chromosomes in parallel)")

    # Initialize sub-tasks for all chromosomes
    if tracker:
        for c in range(1, 23):
            tracker.set_sub_task(f"chr{c}", f"chr{c}", "queued", "waiting")

    chr_args = [
        (str(c), input_path, fasta_path, targets_tsv, tmpdir, tracker, chr_prefix, rename_chrs_file)
        for c in range(1, 23)
    ]

    chr_vcfs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        results = list(pool.map(_call_variants_for_chr, chr_args))
    chr_vcfs = [v for v in results if v and os.path.exists(v)]

    if not chr_vcfs:
        raise PipelineError("Variant calling produced no output for any chromosome")

    on_progress(18, f"Concatenating {len(chr_vcfs)} chromosome VCFs")
    vcf_list = os.path.join(tmpdir, "chr_vcfs.txt")
    with open(vcf_list, "w") as f:
        for v in sorted(chr_vcfs, key=lambda p: int(os.path.basename(p).replace("chr", "").replace(".vcf.gz", ""))):
            f.write(v + "\n")

    called_vcf = os.path.join(tmpdir, "called.vcf.gz")
    _run(
        f'bcftools concat --file-list "{vcf_list}" -Oz -o "{called_vcf}"',
        cwd=tmpdir,
        tracker=tracker,
    )

    on_progress(20, "Indexing called VCF")
    _run(f'tabix -p vcf "{called_vcf}"', cwd=tmpdir, tracker=tracker)

    on_progress(22, "Converting called VCF to PLINK binary format")
    sample_prefix = os.path.join(tmpdir, "sample")
    _run(
        f'plink2 --vcf "{called_vcf}" --chr 1-22 --allow-extra-chr '
        f'--make-bed --out "{sample_prefix}"',
        cwd=tmpdir,
        tracker=tracker,
    )

    return sample_prefix


def step_intersect_and_align(sample_prefix: str, tmpdir: str, on_progress: Callable,
                              tracker: "JobTracker" = None) -> tuple:
    """Find overlapping variants, align alleles, and return (aligned_prefix, overlap_count)."""
    on_progress(30, "Computing variant overlap with reference panel")

    sample_bim = f"{sample_prefix}.bim"
    ref_bim = f"{REF_BED}.bim"

    # Extract sorted variant IDs
    sample_ids_file = os.path.join(tmpdir, "sample_ids_sorted.txt")
    ref_ids_file = os.path.join(tmpdir, "ref_ids_sorted.txt")

    _run(f"awk '{{print $2}}' \"{sample_bim}\" | sort > \"{sample_ids_file}\"", cwd=tmpdir, tracker=tracker)
    _run(f"awk '{{print $2}}' \"{ref_bim}\" | sort > \"{ref_ids_file}\"", cwd=tmpdir, tracker=tracker)

    overlap_file = os.path.join(tmpdir, "overlap.txt")
    _run(f'comm -12 "{sample_ids_file}" "{ref_ids_file}" > "{overlap_file}"', cwd=tmpdir, tracker=tracker)

    # Count overlapping variants
    result = _run(f'wc -l < "{overlap_file}"', cwd=tmpdir, tracker=tracker)
    overlap_count = int(result.stdout.strip())

    if overlap_count < MIN_OVERLAP:
        raise PipelineError(
            f"Insufficient variant overlap with reference panel: {overlap_count} variants "
            f"(minimum required: {MIN_OVERLAP}). This may indicate a file format issue, "
            f"wrong genome build, or too few variants in the input."
        )

    on_progress(35, f"Aligning {overlap_count} overlapping variants to reference alleles")

    aligned_prefix = os.path.join(tmpdir, "sample_aligned")
    _run(
        f'plink2 --bfile "{sample_prefix}" '
        f'--extract "{overlap_file}" '
        f'--ref-allele force "{ref_bim}" 5 2 '
        f'--make-bed --out "{aligned_prefix}"',
        cwd=tmpdir,
        tracker=tracker,
    )

    return aligned_prefix, overlap_count


def step_merge_and_pca(aligned_prefix: str, tmpdir: str, overlap_file: str, on_progress: Callable,
                        tracker: "JobTracker" = None) -> str:
    """Merge sample with reference panel and run PCA. Returns PCA output prefix."""
    on_progress(40, "Subsetting reference panel to overlapping variants")

    ref_ov_prefix = os.path.join(tmpdir, "ref_ov")
    ov_ids_file = overlap_file

    _run(
        f'plink2 --bfile "{REF_BED}" '
        f'--extract "{ov_ids_file}" '
        f'--make-bed --out "{ref_ov_prefix}"',
        cwd=tmpdir,
        tracker=tracker,
    )

    on_progress(50, "Merging sample with reference panel")

    merged_prefix = os.path.join(tmpdir, "merged")

    # First merge attempt
    merge_result = _run(
        f'plink --bfile "{ref_ov_prefix}" '
        f'--bmerge "{aligned_prefix}" '
        f'--make-bed --out "{merged_prefix}" --allow-no-sex',
        cwd=tmpdir,
        check=False,
        tracker=tracker,
    )

    missnp_file = f"{merged_prefix}-merge.missnp"
    if os.path.exists(missnp_file):
        on_progress(53, "Resolving strand mismatches and retrying merge")

        missnp_count_result = _run(f'wc -l < "{missnp_file}"', cwd=tmpdir, tracker=tracker)
        missnp_count = int(missnp_count_result.stdout.strip())

        # Exclude strand-error SNPs from both datasets and retry
        aligned_fixed = os.path.join(tmpdir, "sample_aligned_fixed")
        _run(
            f'plink2 --bfile "{aligned_prefix}" '
            f'--exclude "{missnp_file}" '
            f'--make-bed --out "{aligned_fixed}"',
            cwd=tmpdir,
            tracker=tracker,
        )

        ref_ov_fixed = os.path.join(tmpdir, "ref_ov_fixed")
        _run(
            f'plink2 --bfile "{ref_ov_prefix}" '
            f'--exclude "{missnp_file}" '
            f'--make-bed --out "{ref_ov_fixed}"',
            cwd=tmpdir,
            tracker=tracker,
        )

        # Retry merge
        _run(
            f'plink --bfile "{ref_ov_fixed}" '
            f'--bmerge "{aligned_fixed}" '
            f'--make-bed --out "{merged_prefix}" --allow-no-sex',
            cwd=tmpdir,
            tracker=tracker,
        )
    elif merge_result.returncode != 0:
        stderr = merge_result.stderr.strip() if merge_result.stderr else ""
        raise PipelineError(f"PLINK merge failed: {stderr}")

    on_progress(60, "Cleaning merged dataset (mind/geno/maf filters)")

    merged_clean = os.path.join(tmpdir, "merged_clean")
    _run(
        f'plink2 --bfile "{merged_prefix}" '
        f'--mind 0.1 --geno 0.1 --maf 0.01 '
        f'--make-bed --out "{merged_clean}"',
        cwd=tmpdir,
        tracker=tracker,
    )

    on_progress(65, "Running PCA (20 components)")
    if tracker:
        tracker.set_indeterminate(True)

    pca_prefix = os.path.join(tmpdir, "pca")
    _run(
        f'plink2 --bfile "{merged_clean}" '
        f'--pca 20 --out "{pca_prefix}"',
        cwd=tmpdir,
        tracker=tracker,
    )

    return pca_prefix


def _validate_pop2group_vs_fam() -> list:
    """
    Validate that pop2group.txt populations match reference panel .fam FIDs.
    Returns list of warning strings (empty = all good).
    """
    warnings = []
    fam_path = f"{REF_BED}.fam"

    if not os.path.exists(fam_path) or not os.path.exists(POP2GROUP):
        return warnings

    # Collect unique FIDs from .fam
    fam_fids = set()
    with open(fam_path, "r") as f:
        for line in f:
            parts = line.strip().split()
            if parts:
                fam_fids.add(parts[0])

    # Collect populations from pop2group.txt and check coverage
    pop2group_pops = set()
    group_pops = {}  # group -> list of populations
    with open(POP2GROUP, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("Pop"):
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                pop, group = parts[0], parts[1]
                pop2group_pops.add(pop)
                group_pops.setdefault(group, []).append(pop)

    # Check for groups with zero matching samples in .fam
    for group, pops in group_pops.items():
        matched = [p for p in pops if p in fam_fids]
        if not matched:
            warnings.append(
                f"Group '{group}' has 0 matching samples in reference panel "
                f"(populations: {', '.join(pops)}). Rye decomposition will be unreliable."
            )

    # Check for unmapped FIDs in .fam
    unmapped = fam_fids - pop2group_pops
    if unmapped:
        warnings.append(
            f"{len(unmapped)} population(s) in .fam are not in pop2group.txt: "
            f"{', '.join(sorted(unmapped)[:10])}. These samples will be ignored by Rye."
        )

    return warnings


def _rye_pass(
    pca_prefix: str,
    tmpdir: str,
    pop2group_path: str,
    out_label: str,
    pcs: int = 20,
    eigenval_power: float = 0.25,
    filter_groups: list = None,
    tracker: "JobTracker" = None,
) -> str:
    """Run a single Rye decomposition pass. Returns Rye output prefix.

    Args:
        pca_prefix: Path prefix for .eigenvec/.eigenval files.
        tmpdir: Working directory.
        pop2group_path: Path to population-to-group mapping file.
        out_label: Label for output files (e.g. "pass1", "pass2_ea").
        pcs: Number of PCs to use.
        eigenval_power: Fractional power for eigenvalue compression.
        filter_groups: If set, filter eigenvec to only include samples from these groups.
    """
    rye_out = os.path.join(tmpdir, f"rye_{out_label}")
    eigenvec = f"{pca_prefix}.eigenvec"
    eigenval = f"{pca_prefix}.eigenval"

    if not os.path.exists(eigenvec):
        raise PipelineError(f"PCA eigenvec file not found: {eigenvec}")
    if not os.path.exists(eigenval):
        raise PipelineError(f"PCA eigenval file not found: {eigenval}")

    # Optionally filter eigenvec to only include samples from specified groups
    if filter_groups:
        eigenvec = _filter_eigenvec_to_groups(eigenvec, pop2group_path, filter_groups, tmpdir, out_label)

    # Transform eigenvalues to reduce PC1 dominance
    transformed_eigenval = os.path.join(tmpdir, f"pca_transformed_{out_label}.eigenval")
    with open(eigenval, "r") as fin:
        raw_vals = [float(line.strip()) for line in fin if line.strip()]
    with open(transformed_eigenval, "w") as fout:
        for v in raw_vals:
            fout.write(f"{v ** eigenval_power:.6f}\n")
    eigenval = transformed_eigenval

    if not os.path.exists(pop2group_path):
        raise PipelineError(f"pop2group file not found: {pop2group_path}")

    if RYE_USE_PYTHON and os.path.exists(RYE_PY_SCRIPT):
        # Python port: ~25-30x faster than the R version, single attempt is
        # sufficient (the R per-attempt parallelism existed only to amortize
        # R's per-iteration loop overhead, which Python doesn't have).
        py = os.path.join(_GENOMICS_BIN, 'python')
        if not os.path.exists(py):
            py = 'python'
        _run(
            f'{py} "{RYE_PY_SCRIPT}" '
            f'--eigenvec="{eigenvec}" '
            f'--eigenval="{eigenval}" '
            f'--pop2group="{pop2group_path}" '
            f'--rounds=50 --iter=50 --threads={THREADS} --attempts=1 --pcs={pcs} '
            f'--output="{rye_out}"',
            cwd=tmpdir,
            tracker=tracker,
        )
    else:
        rye_cmd = RYE_SCRIPT if os.path.exists(RYE_SCRIPT) else "rye.R"
        _run(
            f'Rscript "{rye_cmd}" '
            f'--eigenvec="{eigenvec}" '
            f'--eigenval="{eigenval}" '
            f'--pop2group="{pop2group_path}" '
            f'--rounds=50 --iter=50 --threads={THREADS} --attempts={THREADS} --pcs={pcs} '
            f'--out="{rye_out}"',
            cwd=tmpdir,
            tracker=tracker,
        )

    return rye_out


def _filter_eigenvec_to_groups(
    eigenvec_path: str,
    pop2group_path: str,
    allowed_groups: list,
    tmpdir: str,
    label: str,
) -> str:
    """Filter an eigenvec file to only include reference samples from allowed groups,
    plus the query sample (last line or non-reference sample). Returns path to filtered file."""
    # Build set of populations belonging to allowed groups
    allowed_pops = set()
    with open(pop2group_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("Pop"):
                continue
            parts = line.split("\t")
            if len(parts) >= 2 and parts[1] in allowed_groups:
                allowed_pops.add(parts[0])

    # Also read all reference FIDs for query-sample detection
    ref_fids = set()
    fam_path = f"{REF_BED}.fam"
    if os.path.exists(fam_path):
        with open(fam_path, "r") as f:
            for line in f:
                parts = line.strip().split()
                if parts:
                    ref_fids.add(parts[0])

    filtered_path = os.path.join(tmpdir, f"eigenvec_filtered_{label}.eigenvec")
    with open(eigenvec_path, "r") as fin, open(filtered_path, "w") as fout:
        for line in fin:
            parts = line.strip().split()
            if not parts:
                continue
            fid = parts[0]
            # Keep if: belongs to an allowed population, or is the query sample
            if fid in allowed_pops or fid not in ref_fids:
                fout.write(line)

    return filtered_path


def _merge_hierarchical_results(
    pass1_proportions: dict,
    pass2_proportions: dict,
    combined_fraction: float,
    parent_groups: list,
) -> dict:
    """Merge pass-2 sub-proportions back into pass-1 continental proportions.

    pass2_proportions contains fine-grained splits (e.g. EastAsian: 0.8, SoutheastAsian: 0.2).
    These are scaled by combined_fraction (the sum of parent_groups from pass 1) and
    replace the parent_groups entries in pass1_proportions.
    """
    result = dict(pass1_proportions)

    # Remove parent group entries
    for g in parent_groups:
        result.pop(g, None)

    # Scale and insert pass-2 results
    for group, fraction in pass2_proportions.items():
        result[group] = round(fraction * combined_fraction, 6)

    return result


def step_rye(pca_prefix: str, tmpdir: str, on_progress: Callable,
              tracker: "JobTracker" = None) -> str:
    """Run Rye ancestry decomposition using all 20 PCs.

    Returns Rye output prefix.
    """
    on_progress(70, "Running Rye ancestry decomposition")
    if tracker:
        tracker.set_indeterminate(True)

    if not os.path.exists(POP2GROUP):
        raise PipelineError(f"pop2group.txt not found: {POP2GROUP}")

    # Validate reference panel vs pop2group consistency
    panel_warnings = _validate_pop2group_vs_fam()
    if panel_warnings:
        for w in panel_warnings:
            if "0 matching samples" in w:
                raise PipelineError(
                    f"Reference panel validation failed: {w}\n"
                    "Fix pop2group.txt to only include groups with samples in the reference panel."
                )

    rye_out = _rye_pass(
        pca_prefix=pca_prefix,
        tmpdir=tmpdir,
        pop2group_path=POP2GROUP,
        out_label="pass1",
        pcs=20,
        tracker=tracker,
    )

    return rye_out


# GRCh38 autosomal total length (chromosomes 1-22) in megabases.
# Used as the denominator for the inbreeding coefficient F_ROH:
#     F_ROH = sum of ROH segment lengths / total autosomal length
# 2881 Mb is the commonly-cited GRCh38 autosomal total used in the ROH
# literature (e.g., gnomAD, 1000 Genomes FROH calculations).
AUTOSOMAL_MB_GRCH38 = 2881.0


def _compute_froh(total_mb: float) -> float:
    """Return F_ROH = total_mb / AUTOSOMAL_MB_GRCH38, rounded to 4 decimals."""
    if not total_mb or total_mb <= 0:
        return 0.0
    return round(total_mb / AUTOSOMAL_MB_GRCH38, 4)


def step_roh(sample_prefix: str, tmpdir: str, on_progress: Callable,
              tracker: "JobTracker" = None) -> Optional[dict]:
    """Run ROH analysis. Returns ROH summary dict or None."""
    on_progress(82, "Running Runs of Homozygosity (ROH) analysis")

    roh_prefix = os.path.join(tmpdir, "roh")

    roh_result = _run(
        f'plink --bfile "{sample_prefix}" '
        f'--homozyg '
        f'--homozyg-window-snp 50 '
        f'--homozyg-snp 50 '
        f'--homozyg-kb 300 '
        f'--homozyg-density 50 '
        f'--homozyg-gap 1000 '
        f'--out "{roh_prefix}"',
        cwd=tmpdir,
        check=False,
        tracker=tracker,
    )

    indiv_file = f"{roh_prefix}.hom.indiv"
    if not os.path.exists(indiv_file):
        return None

    return _parse_roh_indiv(indiv_file)


def _parse_roh_indiv(indiv_file: str) -> Optional[dict]:
    """Parse plink .hom.indiv (per-sample ROH summary) and return ROH stats.

    Format (whitespace-delimited, one row per sample):
        FID  IID  PHE  NSEG  KB  KBAVG

    Note: PLINK .hom.summary is per-SNP population data (CHR SNP BP AFF
    UNAFF) and is NOT what we want. The per-sample summary lives in
    .hom.indiv. NSEG=0 is a legitimate result (no ROH detected).
    """
    with open(indiv_file, "r") as f:
        lines = f.readlines()

    if len(lines) < 2:
        return None

    header = lines[0].strip().split()
    data_line = lines[-1].strip().split()

    if len(data_line) < len(header):
        return None

    try:
        header_map = {h: data_line[i] for i, h in enumerate(header)}

        n_segments = int(header_map.get("NSEG", 0))
        total_kb = float(header_map.get("KB", 0))
        avg_kb = float(header_map.get("KBAVG", 0))
        total_mb = total_kb / 1000.0

        bottleneck = total_mb > 50 or (n_segments > 10 and avg_kb > 1000)

        return {
            "total_mb": round(total_mb, 2),
            "n_segments": n_segments,
            "avg_kb": round(avg_kb, 2),
            "bottleneck": bottleneck,
            "froh": _compute_froh(total_mb),
            "autosomal_mb": AUTOSOMAL_MB_GRCH38,
        }
    except (ValueError, KeyError, IndexError):
        return None


def _parse_roh_hom_file(hom_file: str) -> Optional[dict]:
    """Parse plink .hom file for detailed segment info (fallback if .hom.summary missing)."""
    if not os.path.exists(hom_file):
        return None

    segments = []
    with open(hom_file, "r") as f:
        header = f.readline()
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 9:
                try:
                    kb = float(parts[8])
                    segments.append(kb)
                except ValueError:
                    continue

    if not segments:
        return None

    total_kb = sum(segments)
    total_mb = total_kb / 1000.0
    avg_kb = total_kb / len(segments)
    bottleneck = total_mb > 50 or (len(segments) > 10 and avg_kb > 1000)

    return {
        "total_mb": round(total_mb, 2),
        "n_segments": len(segments),
        "avg_kb": round(avg_kb, 2),
        "bottleneck": bottleneck,
        "froh": _compute_froh(total_mb),
        "autosomal_mb": AUTOSOMAL_MB_GRCH38,
    }


def _parse_rye_output(rye_prefix: str, sample_name: str = None) -> dict:
    """Parse Rye .Q file and group labels. Returns {group: proportion}.

    Rye outputs a .Q file with:
      - Header line: tab-separated group names
      - Data lines: sample_id<tab>prop1<tab>prop2<tab>...<tab>propN
    File is named {prefix}-{pcs}.{groups}.Q (e.g. rye_result-10.9.Q).
    """
    import glob as globmod

    q_file = f"{rye_prefix}.Q"
    if not os.path.exists(q_file):
        # Rye names output as {prefix}-{pcs}.{groups}.Q
        candidates = sorted(globmod.glob(f"{rye_prefix}*.Q") + globmod.glob(f"{rye_prefix}*.q"))
        if candidates:
            q_file = candidates[0]
        else:
            raise PipelineError(f"Rye output .Q file not found at {rye_prefix}.Q")

    with open(q_file, "r") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]

    if not lines:
        raise PipelineError("Rye .Q file is empty")

    # First line is the header with group names
    header = lines[0].split("\t")

    # Read all reference panel FIDs so we can identify the query sample
    ref_fids = set()
    fam_path = f"{REF_BED}.fam"
    if os.path.exists(fam_path):
        with open(fam_path, "r") as f:
            for line in f:
                parts = line.strip().split()
                if parts:
                    ref_fids.add(parts[0])
                    ref_fids.add(parts[1])  # also add IID

    # Find the query sample row (not in reference panel, or matching sample_name)
    query_values = None
    for line in lines[1:]:
        parts = line.split("\t")
        if len(parts) < 2:
            parts = line.split()
        sid = parts[0]

        # Check if this is the query sample
        is_query = False
        if sample_name and sid == sample_name:
            is_query = True
        elif sid not in ref_fids:
            is_query = True

        if is_query:
            try:
                query_values = [float(x) for x in parts[1:]]
            except ValueError:
                continue
            break

    if query_values is None:
        # Fallback: use the first data row (Rye sometimes puts query first)
        parts = lines[1].split("\t")
        if len(parts) < 2:
            parts = lines[1].split()
        try:
            query_values = [float(x) for x in parts[1:]]
        except ValueError:
            raise PipelineError("Could not parse query sample proportions from Rye output")

    # Match values to group names from header
    groups = header
    if len(query_values) != len(groups):
        # Fallback to pop2group.txt groups
        groups = _read_group_names()

    if len(query_values) != len(groups):
        raise PipelineError(
            f"Rye output has {len(query_values)} components but found "
            f"{len(groups)} groups. Cannot reconcile."
        )

    proportions = {}
    for group, value in zip(groups, query_values):
        proportions[group] = round(value, 6)

    return proportions


def _read_group_names() -> list:
    """Read unique group names from pop2group.txt in order of first appearance."""
    if not os.path.exists(POP2GROUP):
        raise PipelineError(f"pop2group.txt not found: {POP2GROUP}")

    groups = []
    seen = set()
    with open(POP2GROUP, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("Pop"):
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                group = parts[1]
                if group not in seen:
                    groups.append(group)
                    seen.add(group)

    if not groups:
        raise PipelineError("No groups found in pop2group.txt")

    return groups


def _get_proportion(proportions: dict, *keywords: str) -> float:
    """Sum proportions for groups matching any of the given keywords (case-insensitive)."""
    total = 0.0
    for k, v in proportions.items():
        k_lower = k.lower().replace(" ", "").replace("-", "").replace("_", "")
        for kw in keywords:
            if kw in k_lower:
                total += v
                break
    return total


# ---------------------------------------------------------------------------
# Signature-based pattern detection
# ---------------------------------------------------------------------------

# Cache for loaded signatures (auto-reload on file change)
_signatures_cache: dict = {"mtime": 0, "data": None}

# Group aliases: normalized key -> list of group name patterns that match
_GROUP_ALIASES = {
    "european": ["europ", "finnish"],
    "middleeastern": ["middleeast", "westasia"],
    "african": ["african"],
    "eastasian": ["eastasian"],
    "southeastasian": ["southeastasian"],
    "southasian": ["southasian"],
    "american": ["american"],
    "oceanian": ["oceanian"],
}


def _normalize_group_key(name: str) -> str:
    """Normalize a group name for matching: lowercase, strip spaces/hyphens/underscores."""
    return name.lower().replace(" ", "").replace("-", "").replace("_", "")


def _get_proportion_for_sig(proportions: dict, sig_group: str) -> float:
    """Get proportion for a signature group name, resolving aliases.

    'European' in a signature means European + Finnish combined.
    """
    normalized = _normalize_group_key(sig_group)
    aliases = _GROUP_ALIASES.get(normalized)
    if aliases:
        return _get_proportion(proportions, *aliases)
    # Fallback: direct match
    return _get_proportion(proportions, normalized)


def _load_signatures() -> list:
    """Load signatures from YAML, with file-change caching."""
    global _signatures_cache

    if not os.path.exists(SIGNATURES_PATH):
        return []

    mtime = os.path.getmtime(SIGNATURES_PATH)
    if _signatures_cache["data"] is not None and mtime <= _signatures_cache["mtime"]:
        return _signatures_cache["data"]

    with open(SIGNATURES_PATH, "r") as f:
        raw = yaml.safe_load(f)

    sigs = raw.get("signatures", []) if raw else []
    _signatures_cache["mtime"] = mtime
    _signatures_cache["data"] = sigs
    return sigs


def _confidence_label(score: float) -> str:
    """Map numeric confidence to human-readable label."""
    if score >= 0.7:
        return "high"
    elif score >= 0.4:
        return "moderate"
    return "low"


def _match_signature(proportions: dict, sig: dict, roh: dict = None) -> dict:
    """Check proportions against one signature definition.

    Returns a match dict with:
      matched (bool), confidence (0.0-1.0), confidence_label,
      display_name, description, components (with sub_labels), id
    """
    required = sig.get("required", {})
    optional = sig.get("optional", {})
    max_other = sig.get("max_other", 0.05)
    sub_labels = sig.get("sub_labels", {})

    components = []
    required_scores = []
    all_matched_groups = set()
    matched_proportion_total = 0.0

    # Check required groups
    for group, (lo, hi) in required.items():
        actual = _get_proportion_for_sig(proportions, group)
        in_range = lo <= actual <= hi
        if not in_range:
            return {"matched": False, "confidence": 0.0, "id": sig.get("id", "")}
        # Confidence: how centered is the value within the range?
        mid = (lo + hi) / 2
        span = (hi - lo) / 2
        distance = abs(actual - mid) / span if span > 0 else 0
        score = max(0.0, 1.0 - distance)
        required_scores.append(score)
        all_matched_groups.add(_normalize_group_key(group))
        matched_proportion_total += actual
        components.append({
            "rye_group": group,
            "label": sub_labels.get(group, group),
            "proportion": round(actual, 4),
        })

    # Check optional groups
    optional_scores = []
    for group, (lo, hi) in (optional or {}).items():
        actual = _get_proportion_for_sig(proportions, group)
        in_range = lo <= actual <= hi
        all_matched_groups.add(_normalize_group_key(group))
        if in_range and actual > 0.005:
            mid = (lo + hi) / 2
            span = (hi - lo) / 2
            distance = abs(actual - mid) / span if span > 0 else 0
            optional_scores.append(max(0.0, 1.0 - distance))
            matched_proportion_total += actual
            components.append({
                "rye_group": group,
                "label": sub_labels.get(group, group),
                "proportion": round(actual, 4),
            })

    # Check max_other: sum of proportions not covered by required/optional groups
    other_total = 0.0
    for k, v in proportions.items():
        nk = _normalize_group_key(k)
        is_covered = False
        for mg in all_matched_groups:
            aliases = _GROUP_ALIASES.get(mg, [mg])
            for alias in aliases:
                if alias in nk:
                    is_covered = True
                    break
            if is_covered:
                break
        if not is_covered:
            other_total += v

    if other_total > max_other:
        return {"matched": False, "confidence": 0.0, "id": sig.get("id", "")}

    # Compute overall confidence
    base_confidence = sum(required_scores) / len(required_scores) if required_scores else 0.5
    if optional_scores:
        opt_avg = sum(optional_scores) / len(optional_scores)
        base_confidence = base_confidence * 0.8 + opt_avg * 0.2

    # Penalise for high "other" proportion relative to max_other
    if max_other > 0:
        other_penalty = min(1.0, other_total / max_other)
        base_confidence *= (1.0 - 0.3 * other_penalty)

    # ROH boost
    if sig.get("roh_boost") and roh and roh.get("bottleneck"):
        base_confidence = min(1.0, base_confidence * sig["roh_boost"])

    conf = round(base_confidence, 3)
    return {
        "matched": True,
        "confidence": conf,
        "confidence_label": _confidence_label(conf),
        "id": sig.get("id", ""),
        "display_name": sig.get("display_name", ""),
        "description": sig.get("description", ""),
        "components": components,
        "matched_proportion": round(matched_proportion_total, 4),
    }


def match_all_signatures(proportions: dict, roh: dict = None) -> list:
    """Match proportions against all known signatures. Returns list sorted by confidence."""
    signatures = _load_signatures()
    results = []
    for sig in signatures:
        match = _match_signature(proportions, sig, roh)
        if match["matched"] and match["confidence"] > 0:
            results.append(match)
    results.sort(key=lambda m: m["confidence"], reverse=True)
    return results


def _build_detected_populations(proportions: dict, signature_matches: list) -> list:
    """Build detected_populations list from signature matches.

    For multi-ancestry samples (e.g., half-Chinese half-ASJ), detect each
    segment separately. If proportions are left over after the best match,
    attempt to match the remainder against other signatures.
    """
    if not signature_matches:
        # No signature matched — return as "Unresolved Admixture"
        components = [
            {"rye_group": g, "label": g, "proportion": round(v, 4)}
            for g, v in sorted(proportions.items(), key=lambda x: x[1], reverse=True)
            if v >= 0.01
        ]
        return [{
            "label": "Unresolved Admixture",
            "proportion": 1.0,
            "confidence": "low",
            "components": components,
        }]

    detected = []
    used_proportion = 0.0

    for match in signature_matches:
        mp = match["matched_proportion"]
        # Skip if this match's proportion overlaps too much with already-used
        if used_proportion + mp > 1.15:  # allow small rounding overlap
            continue
        detected.append({
            "label": match["display_name"],
            "proportion": round(mp, 4),
            "confidence": match["confidence_label"],
            "components": match["components"],
            "description": match.get("description", ""),
            "signature_id": match["id"],
        })
        used_proportion += mp
        if used_proportion >= 0.95:
            break

    # If significant proportion unaccounted, add "Other" entry
    remaining = 1.0 - used_proportion
    if remaining > 0.05:
        other_components = []
        claimed_groups = set()
        for d in detected:
            for c in d["components"]:
                claimed_groups.add(_normalize_group_key(c["rye_group"]))
        for g, v in proportions.items():
            ng = _normalize_group_key(g)
            is_claimed = False
            for cg in claimed_groups:
                aliases = _GROUP_ALIASES.get(cg, [cg])
                for alias in aliases:
                    if alias in ng:
                        is_claimed = True
                        break
                if is_claimed:
                    break
            if not is_claimed and v >= 0.01:
                other_components.append({
                    "rye_group": g,
                    "label": g,
                    "proportion": round(v, 4),
                })
        if other_components:
            detected.append({
                "label": "Other",
                "proportion": round(remaining, 4),
                "confidence": "low",
                "components": other_components,
            })

    # Normalize proportions to sum to 1.0
    total = sum(d["proportion"] for d in detected)
    if total > 0 and abs(total - 1.0) > 0.01:
        for d in detected:
            d["proportion"] = round(d["proportion"] / total, 4)

    return detected


def _interpret_results(
    proportions: dict,
    roh: Optional[dict],
    input_type: str,
) -> tuple:
    """Interpret ancestry proportions.

    Returns (primary, primary_pct, is_admixed, flags, signature_matches, detected_populations).
    """
    if not proportions:
        raise PipelineError("No ancestry proportions to interpret")

    # Sort by proportion descending
    sorted_components = sorted(proportions.items(), key=lambda x: x[1], reverse=True)

    primary = sorted_components[0][0]
    primary_pct = round(sorted_components[0][1] * 100, 1)

    # Admixed if no single component > 85%
    is_admixed = primary_pct < 85.0

    flags = []

    # --- Signature-based detection ---
    signature_matches = match_all_signatures(proportions, roh)

    # Build detected_populations from matches
    detected_populations = _build_detected_populations(proportions, signature_matches)

    # Populate backward-compatible flags from signature matches
    for match in signature_matches:
        comp_parts = [
            f"{c['rye_group']} ({round(c['proportion'] * 100, 1)}%)"
            for c in match.get("components", [])
        ]
        detail_str = " + ".join(comp_parts)
        msg = (
            f"{match['id']}: {detail_str} "
            f"consistent with {match['display_name']} (confidence: {match['confidence_label']})"
        )
        if roh and roh.get("bottleneck") and "asj" in match["id"].lower():
            msg += f". ROH bottleneck ({roh['total_mb']:.0f} Mb) confirms founder-effect."
        flags.append(msg)

    # Check for significant secondary components
    if len(sorted_components) > 1:
        secondary = sorted_components[1]
        if secondary[1] >= 0.10:
            flags.append(
                f"significant_secondary: {secondary[0]} at {round(secondary[1] * 100, 1)}%"
            )

    # Multi-way admixture
    significant_components = [k for k, v in proportions.items() if v >= 0.05]
    if len(significant_components) >= 4:
        flags.append(f"complex_admixture: {len(significant_components)} components above 5%")
    elif len(significant_components) == 3:
        flags.append(f"three_way_admixture: {', '.join(significant_components)}")

    # ROH-based flags
    if roh:
        if roh["bottleneck"]:
            flags.append(
                f"population_bottleneck: {roh['total_mb']} Mb total ROH, "
                f"{roh['n_segments']} segments"
            )
        if roh["total_mb"] > 100:
            flags.append("high_consanguinity: >100 Mb total ROH indicates recent consanguinity")
        elif roh["total_mb"] > 20:
            flags.append("moderate_ROH: 20-100 Mb total ROH, possible distant consanguinity or bottleneck")

    # Low primary component
    if primary_pct < 40:
        flags.append("highly_admixed: no single component above 40%")

    return primary, primary_pct, is_admixed, flags, signature_matches, detected_populations


def run_pipeline(
    sample_name: str,
    input_path: str,
    input_type: str,
    tmpdir: str,
    fasta_path: Optional[str],
    on_progress: Callable,
    tracker: "JobTracker" = None,
) -> dict:
    """
    Run the full ancestry inference pipeline.

    Args:
        sample_name: Human-readable sample name
        input_path: Path to input file (VCF/gVCF/BAM/CRAM)
        input_type: Detected input type (vcf, gvcf, bam, cram)
        tmpdir: Temporary working directory
        fasta_path: Path to reference FASTA (required for BAM/CRAM)
        on_progress: Callback(pct: int, step: str) for progress updates

    Returns:
        Dict with ancestry results including proportions, primary component,
        admixture flags, ROH stats, and variant counts.
    """
    # Wire up tracker to on_progress so both stay in sync
    if tracker:
        _orig_on_progress = on_progress
        def on_progress(pct, step):
            tracker.set_progress(pct, step)
            _orig_on_progress(pct, step)

    on_progress(1, "Validating inputs and reference panel")

    # Validate input file exists
    if not os.path.exists(input_path):
        raise PipelineError(f"Input file not found: {input_path}")

    # Validate reference panel exists
    for ext in [".bed", ".bim", ".fam"]:
        ref_file = f"{REF_BED}{ext}"
        if not os.path.exists(ref_file):
            raise PipelineError(
                f"Reference panel file not found: {ref_file}. "
                "Run the reference setup script first."
            )

    # --- Step 1: Input Detection ---
    on_progress(2, f"Input type: {input_type}")

    # --- Step 2: Variant Extraction ---
    if input_type in ("vcf", "gvcf"):
        sample_prefix = step_extract_variants_vcf(input_path, tmpdir, on_progress, tracker=tracker)
    elif input_type in ("bam", "cram"):
        sample_prefix = step_extract_variants_bam(input_path, tmpdir, fasta_path, on_progress, tracker=tracker)
    else:
        raise PipelineError(f"Unsupported input type: {input_type}")

    # Verify sample bed/bim/fam were created
    for ext in [".bed", ".bim", ".fam"]:
        if not os.path.exists(f"{sample_prefix}{ext}"):
            raise PipelineError(f"Sample {ext} file not created. Variant extraction may have failed.")

    # Count sample variants
    variant_count_result = _run(f'wc -l < "{sample_prefix}.bim"', cwd=tmpdir, tracker=tracker)
    sample_variant_count = int(variant_count_result.stdout.strip())
    on_progress(25, f"Extracted {sample_variant_count} biallelic SNPs from input")

    # --- Step 3: Chromosome naming + variant ID fix ---
    on_progress(27, "Checking chromosome naming convention")
    ref_chr_format = _check_ref_chr_format()
    sample_chr_format = _check_sample_chr_format(f"{sample_prefix}.bim")
    ref_id_format = _get_ref_id_format()

    # Check if sample variant ID format matches reference
    sample_id_format = "num"
    with open(f"{sample_prefix}.bim", "r") as f:
        for line in f:
            vid = line.split("\t")[1]
            sample_id_format = "chr" if vid.startswith("chr") else "num"
            break

    needs_chr_fix = ref_chr_format != sample_chr_format
    needs_id_fix = ref_id_format != sample_id_format

    if needs_chr_fix or needs_id_fix:
        reasons = []
        if needs_chr_fix:
            reasons.append(f"chr column: sample={sample_chr_format}, ref={ref_chr_format}")
        if needs_id_fix:
            reasons.append(f"variant IDs: sample={sample_id_format}, ref={ref_id_format}")
        on_progress(28, f"Fixing naming mismatch ({'; '.join(reasons)})")
        sample_prefix = _fix_chr_names(tmpdir, sample_prefix, ref_chr_format, sample_chr_format)

    # --- Step 4: Intersect + Align ---
    aligned_prefix, overlap_count = step_intersect_and_align(sample_prefix, tmpdir, on_progress, tracker=tracker)

    # --- Step 5: Merge + PCA ---
    overlap_file = os.path.join(tmpdir, "overlap.txt")
    pca_prefix = step_merge_and_pca(aligned_prefix, tmpdir, overlap_file, on_progress, tracker=tracker)

    # --- Step 6: Rye ---
    rye_prefix = step_rye(pca_prefix, tmpdir, on_progress, tracker=tracker)

    # --- Step 7: ROH (VCF/gVCF only) ---
    roh = None
    if input_type in ("vcf", "gvcf"):
        roh = step_roh(sample_prefix, tmpdir, on_progress, tracker=tracker)
        # Fallback: try .hom file if .hom.summary parse failed
        if roh is None:
            hom_file = os.path.join(tmpdir, "roh.hom")
            roh = _parse_roh_hom_file(hom_file)
    else:
        on_progress(82, "Skipping ROH analysis (not applicable for BAM/CRAM input)")

    # --- Step 8: Parse results + Interpret ---
    on_progress(85, "Parsing Rye ancestry proportions")
    proportions = _parse_rye_output(rye_prefix, sample_name=sample_name)

    on_progress(90, "Detecting population signatures")
    primary, primary_pct, is_admixed, flags, signature_matches, detected_populations = (
        _interpret_results(proportions, roh, input_type)
    )

    # --- Build result ---
    on_progress(95, "Finalizing results")

    # pop_proportions: fine-grained population-level proportions (same as proportions for now,
    # will be enriched with sub-population resolution when available)
    pop_proportions = dict(proportions)

    result = {
        "sample_name": sample_name,
        "proportions": proportions,
        "pop_proportions": pop_proportions,
        "primary": primary,
        "primary_pct": primary_pct,
        "is_admixed": is_admixed,
        "flags": flags,
        "signatures": signature_matches,
        "detected_populations": detected_populations,
        "roh": roh,
        "variants_used": overlap_count,
        "panel": "gnomAD_HGDP_1kGP",
    }

    on_progress(100, "Analysis complete")
    return result
