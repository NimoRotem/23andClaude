"""
nimog pipeline — BAM/CRAM to normalized VCF conversion.

Supports two modes:
  1. bcftools (quick) — per-chromosome mpileup/call, good for single-sample fast analysis
  2. deepvariant (full) — DeepVariant + GLnexus joint genotyping, proper for family studies

Accepts both BAM and CRAM alignment files as input.
Includes QC validation phase with pass/fail gates.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional

BCFTOOLS = "/home/nimo/miniconda3/envs/genomics/bin/bcftools"
SAMTOOLS = "/home/nimo/miniconda3/envs/genomics/bin/samtools"
APPTAINER = "/usr/bin/apptainer"
# Container images — GPU and CPU variants
DV_GPU_SIF = "/data/containers/deepvariant_1.6.1-gpu.sif"
DV_CPU_SIF = "/data/pgs2/containers/deepvariant.sif"
DV_GPU_DOCKER = "docker://google/deepvariant:1.6.1-gpu"
DV_CPU_DOCKER = "docker://google/deepvariant:1.6.1"
GLNEXUS_SIF = "/scratch/tmp/glnexus_v1.4.1.sif"
GLNEXUS_IMAGE = GLNEXUS_SIF if os.path.exists(GLNEXUS_SIF) else "docker://ghcr.io/dnanexus-rnd/glnexus:v1.4.1"
# Bind mounts for Apptainer — container needs read/write access to data paths
APPTAINER_BINDS = "-B /data:/data -B /scratch:/scratch -B /home/nimo:/home/nimo"


def detect_gpu() -> bool:
    """Check if an NVIDIA GPU is available."""
    import subprocess
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0 and len(result.stdout.strip()) > 0
    except Exception:
        return False


def get_dv_image(use_gpu: bool) -> str:
    """Return the correct DeepVariant image path based on GPU preference."""
    if use_gpu:
        if os.path.exists(DV_GPU_SIF):
            return DV_GPU_SIF
        return DV_GPU_DOCKER
    else:
        if os.path.exists(DV_CPU_SIF):
            return DV_CPU_SIF
        return DV_CPU_DOCKER


GPU_AVAILABLE = detect_gpu()

ALL_AUTOSOMES = [str(i) for i in range(1, 23)]

# QC thresholds (from the pipeline plan)
QC_VARIANT_MIN = 3_000_000
QC_VARIANT_MAX = 15_000_000
QC_TITV_MIN = 1.9
QC_TITV_MAX = 2.2
# For single-sample bcftools mode, thresholds are different
QC_SINGLE_VARIANT_MIN = 500_000
QC_SINGLE_VARIANT_MAX = 8_000_000


class PipelineMode(str, Enum):
    BCFTOOLS = "bcftools"
    DEEPVARIANT = "deepvariant"


@dataclass
class QCResult:
    """Results from QC validation phase."""
    variant_count: int = 0
    variant_count_pass: bool = False
    titv_ratio: float = 0.0
    titv_pass: bool = False
    sample_count: int = 0
    sample_count_pass: bool = False
    per_sample_counts: Dict[str, int] = field(default_factory=dict)
    per_sample_pass: bool = False
    het_hom_extracted: bool = False
    overall_pass: bool = False
    checks_passed: int = 0
    checks_total: int = 0

    def to_dict(self):
        return {
            "variant_count": self.variant_count,
            "variant_count_pass": self.variant_count_pass,
            "titv_ratio": round(self.titv_ratio, 3),
            "titv_pass": self.titv_pass,
            "sample_count": self.sample_count,
            "sample_count_pass": self.sample_count_pass,
            "per_sample_counts": self.per_sample_counts,
            "per_sample_pass": self.per_sample_pass,
            "het_hom_extracted": self.het_hom_extracted,
            "overall_pass": self.overall_pass,
            "checks_passed": self.checks_passed,
            "checks_total": self.checks_total,
        }


@dataclass
class PipelineJob:
    job_id: str
    bam_path: str  # Primary BAM (bcftools mode) or first BAM
    output_dir: str
    reference: str = "/data/genom-nimo/reference.fasta"
    chromosomes: List[str] = field(default_factory=lambda: list(ALL_AUTOSOMES))
    cores: int = 8
    min_base_qual: int = 20
    min_map_qual: int = 20
    max_depth: int = 5000
    qual_filter: int = 30
    min_dp: int = 10
    max_dp: int = 1800
    # Multi-BAM for joint calling
    bam_paths: List[str] = field(default_factory=list)
    sample_names: List[str] = field(default_factory=list)
    # Pipeline mode
    mode: str = "bcftools"
    dv_shards: int = 16
    use_gpu: bool = True
    # Runtime state
    status: str = "pending"
    step: str = ""
    phase: int = 0
    phase_name: str = ""
    progress: float = 0.0
    chroms_done: int = 0
    chroms_completed: List[str] = field(default_factory=list)
    total_chroms: int = 22
    samples_done: int = 0
    total_samples: int = 1
    logs: List[str] = field(default_factory=list)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    output_vcf: Optional[str] = None
    chrom_prefix: str = ""
    qc_result: Optional[QCResult] = None

    def log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        self.logs.append(entry)

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "bam_path": self.bam_path,
            "bam_paths": self.bam_paths,
            "sample_names": self.sample_names,
            "output_dir": self.output_dir,
            "reference": self.reference,
            "chromosomes": self.chromosomes,
            "cores": self.cores,
            "min_base_qual": self.min_base_qual,
            "min_map_qual": self.min_map_qual,
            "max_depth": self.max_depth,
            "qual_filter": self.qual_filter,
            "min_dp": self.min_dp,
            "max_dp": self.max_dp,
            "mode": self.mode,
            "use_gpu": self.use_gpu,
            "dv_shards": self.dv_shards,
            "status": self.status,
            "step": self.step,
            "phase": self.phase,
            "phase_name": self.phase_name,
            "progress": self.progress,
            "chroms_done": self.chroms_done,
            "chroms_completed": self.chroms_completed,
            "total_chroms": self.total_chroms,
            "samples_done": self.samples_done,
            "total_samples": self.total_samples,
            "logs": self.logs[-50:],
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "output_vcf": self.output_vcf,
            "qc_result": self.qc_result.to_dict() if self.qc_result else None,
        }


def save_job(job: PipelineJob, jobs_dir: str):
    path = os.path.join(jobs_dir, f"{job.job_id}.json")
    with open(path, "w") as f:
        json.dump(job.to_dict(), f, indent=2)


async def detect_chrom_prefix(bam_path: str) -> str:
    """Detect whether the BAM uses 'chr1' or '1' naming."""
    proc = await asyncio.create_subprocess_exec(
        SAMTOOLS, "idxstats", bam_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    lines = [l for l in stdout.decode().strip().split("\n") if l and not l.startswith("*")]
    if lines:
        first_contig = lines[0].split("\t")[0]
        return "chr" if first_contig.startswith("chr") else ""
    # Fallback: check BAM header
    proc2 = await asyncio.create_subprocess_shell(
        f"{SAMTOOLS} view -H {bam_path} 2>/dev/null | grep '^@SQ' | head -1",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout2, _ = await proc2.communicate()
    header_line = stdout2.decode().strip()
    return "chr" if "SN:chr" in header_line else ""


async def run_cmd(cmd: str, job: PipelineJob) -> tuple:
    """Run a shell command, return (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout.decode(), stderr.decode()


def get_sample_name(bam_path: str) -> str:
    """Extract sample name from BAM filename."""
    return os.path.basename(bam_path).replace(".bam", "").replace(".cram", "")


# ═══════════════════════════════════════════════════════════════════════
# BCFTOOLS MODE (Quick single-sample)
# ═══════════════════════════════════════════════════════════════════════

async def process_chromosome(
    job: PipelineJob, chrom: str, semaphore: asyncio.Semaphore, on_progress: Callable
):
    """Call variants on a single chromosome using bcftools."""
    async with semaphore:
        region = f"{job.chrom_prefix}{chrom}"
        out_file = os.path.join(job.output_dir, f"chr{chrom}.vcf.gz")

        job.log(f"Starting {region}...")
        on_progress(job)

        # Build BAM list for multi-sample calling
        bam_list = " ".join(job.bam_paths) if job.bam_paths else job.bam_path

        cmd = (
            f"{BCFTOOLS} mpileup -f {job.reference} -r {region} "
            f"-q {job.min_map_qual} -Q {job.min_base_qual} -d {job.max_depth} "
            f"-a FORMAT/AD,FORMAT/DP -Ou {bam_list} 2>/dev/null | "
            f"{BCFTOOLS} call -m -v -Oz -o {out_file} 2>/dev/null"
        )
        rc, _, stderr = await run_cmd(cmd, job)
        if rc != 0:
            raise RuntimeError(f"{region} mpileup/call failed: {stderr}")

        # Index
        rc, _, stderr = await run_cmd(f"{BCFTOOLS} index -t {out_file}", job)
        if rc != 0:
            raise RuntimeError(f"{region} index failed: {stderr}")

        # Get variant count
        rc, stdout, _ = await run_cmd(f"{BCFTOOLS} view -H {out_file} | wc -l", job)
        variant_count = stdout.strip() if rc == 0 else "?"

        size = os.path.getsize(out_file)
        size_mb = size / (1024 * 1024)

        job.chroms_done += 1
        job.chroms_completed.append(chrom)
        job.progress = job.chroms_done / (job.total_chroms + 4)  # +4 for concat/norm/filter/qc
        job.log(f"Done {region}: {variant_count} variants, {size_mb:.1f} MB "
                f"({job.chroms_done}/{job.total_chroms})")
        on_progress(job)
        return chrom


async def bcftools_pipeline(job: PipelineJob, on_progress: Callable):
    """bcftools mode: parallel per-chrom calling -> concat -> normalize -> filter -> QC."""
    total_steps = job.total_chroms + 4  # chroms + concat + normalize + filter + qc

    # --- Phase 1: Per-chromosome calling ---
    job.phase = 1
    job.phase_name = "Variant Calling"
    job.step = "calling"
    job.log(f"Phase 1: Variant calling ({job.cores} parallel)...")
    on_progress(job)

    semaphore = asyncio.Semaphore(job.cores)
    tasks = [
        process_chromosome(job, chrom, semaphore, on_progress)
        for chrom in job.chromosomes
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    failed = [r for r in results if isinstance(r, Exception)]
    if failed:
        job.log(f"WARNING: {len(failed)} chromosome(s) failed")
        for f in failed:
            job.log(f"  Error: {f}")

    succeeded = [r for r in results if not isinstance(r, Exception)]
    if not succeeded:
        raise RuntimeError("All chromosomes failed")

    # --- Phase 2: Concatenate ---
    job.phase = 2
    job.phase_name = "Concatenation"
    job.step = "concatenating"
    job.log("Phase 2: Concatenating chromosome VCFs...")
    on_progress(job)

    chr_order = sorted(succeeded, key=lambda x: int(x))
    chr_files = [os.path.join(job.output_dir, f"chr{c}.vcf.gz") for c in chr_order]
    existing_files = [f for f in chr_files if os.path.exists(f)]

    merged_path = os.path.join(job.output_dir, "merged.vcf.gz")
    rc, _, stderr = await run_cmd(
        f"{BCFTOOLS} concat -Oz -o {merged_path} {' '.join(existing_files)}", job
    )
    if rc != 0:
        raise RuntimeError(f"Concat failed: {stderr}")
    await run_cmd(f"{BCFTOOLS} index -t {merged_path}", job)

    job.progress = (job.total_chroms + 1) / total_steps
    job.log("Concatenation complete")
    on_progress(job)

    # --- Phase 3: Normalize ---
    job.phase = 3
    job.phase_name = "Normalization"
    job.step = "normalizing"
    job.log("Phase 3: Normalizing (left-align, split multiallelics)...")
    on_progress(job)

    norm_path = os.path.join(job.output_dir, "merged.norm.vcf.gz")
    rc, stdout, stderr = await run_cmd(
        f"{BCFTOOLS} norm -m -any -f {job.reference} {merged_path} -Oz -o {norm_path} 2>&1",
        job,
    )
    if rc != 0:
        raise RuntimeError(f"Normalize failed: {stderr}")

    for line in (stdout + stderr).strip().split("\n"):
        if line.strip():
            job.log(f"  norm: {line.strip()}")

    await run_cmd(f"{BCFTOOLS} index -t {norm_path}", job)
    job.progress = (job.total_chroms + 2) / total_steps
    on_progress(job)

    # --- Phase 3b: Filter ---
    job.step = "filtering"
    job.log(f"Filtering (QUAL>={job.qual_filter}, DP {job.min_dp}-{job.max_dp})...")
    on_progress(job)

    final_path = os.path.join(job.output_dir, "final.vcf.gz")
    filter_expr = f"QUAL<{job.qual_filter}"
    if job.min_dp > 0 or job.max_dp < 50000:
        filter_expr += f" || INFO/DP<{job.min_dp} || INFO/DP>{job.max_dp}"

    rc, _, stderr = await run_cmd(
        f"{BCFTOOLS} filter -e '{filter_expr}' -s 'LowQual' {norm_path} | "
        f"{BCFTOOLS} view -f 'PASS' -Oz -o {final_path}",
        job,
    )
    if rc != 0:
        raise RuntimeError(f"Filter failed: {stderr}")

    await run_cmd(f"{BCFTOOLS} index -t {final_path}", job)
    job.progress = (job.total_chroms + 3) / total_steps
    on_progress(job)

    # --- Phase 4: QC Validation ---
    job.phase = 4
    job.phase_name = "QC Validation"
    job.step = "qc"
    job.log("Phase 4: QC validation...")
    on_progress(job)

    is_multi = len(job.bam_paths) > 1
    qc = await run_qc_validation(job, final_path, multi_sample=is_multi)
    job.qc_result = qc
    job.progress = (job.total_chroms + 4) / total_steps
    on_progress(job)

    # --- Cleanup ---
    job.step = "cleanup"
    job.log("Cleaning up temporary files...")
    for f in existing_files:
        try:
            os.remove(f)
            os.remove(f + ".tbi")
        except OSError:
            pass
    for p in [merged_path, norm_path]:
        try:
            os.remove(p)
            os.remove(p + ".tbi")
        except OSError:
            pass

    return final_path


# ═══════════════════════════════════════════════════════════════════════
# DEEPVARIANT MODE (Full pipeline with joint genotyping)
# ═══════════════════════════════════════════════════════════════════════

async def deepvariant_call_sample(
    job: PipelineJob, bam_path: str, sample_name: str, on_progress: Callable
):
    """Run DeepVariant on a single sample to produce a gVCF."""
    gvcf_path = os.path.join(job.output_dir, "dv", f"{sample_name}.g.vcf.gz")
    vcf_path = os.path.join(job.output_dir, "dv", f"{sample_name}.vcf.gz")
    tmp_dir = os.path.join(job.output_dir, "dv", f"tmp_{sample_name}")
    os.makedirs(tmp_dir, exist_ok=True)

    # Estimate time based on BAM file size and shards
    bam_size_gb = os.path.getsize(bam_path) / (1024**3)
    # Empirical: GPU (T4) ~0.6 min/GB, CPU ~2.5 min/GB
    est_minutes = bam_size_gb * (0.6 if job.use_gpu else 2.5)
    est_hours = est_minutes / 60

    job.log(f"DeepVariant: {sample_name} ({os.path.basename(bam_path)}, {bam_size_gb:.1f} GB)")
    job.log(f"  Estimated time: ~{est_hours:.1f} hours ({est_minutes:.0f} min) with {job.dv_shards} shards")
    job.log(f"  Stage 1/3: make_examples (extracting candidate variants from reads)...")
    on_progress(job)

    dv_image = get_dv_image(job.use_gpu)
    nv_flag = "--nv" if job.use_gpu else ""
    job.log(f"  GPU: {'ENABLED' if job.use_gpu else 'DISABLED'} | Image: {os.path.basename(dv_image)}")

    cmd = (
        f"{APPTAINER} run {nv_flag} {APPTAINER_BINDS} {dv_image} "
        f"/opt/deepvariant/bin/run_deepvariant "
        f"--model_type=WGS "
        f"--ref={job.reference} "
        f"--reads={bam_path} "
        f"--output_vcf={vcf_path} "
        f"--output_gvcf={gvcf_path} "
        f"--num_shards={job.dv_shards} "
        f"--intermediate_results_dir={tmp_dir}"
    )

    # Run DeepVariant as a subprocess but monitor intermediate files for progress.
    # CRITICAL: redirect stdout/stderr to a log file rather than asyncio.PIPE.
    # The monitor loop below sleeps for the entire run and doesn't read from
    # `proc.stdout`/`proc.stderr`. With PIPE, the OS pipe buffer (64 KB on
    # Linux) fills up after a minute or two of DV's verbose output and the
    # child blocks forever in pipe_write. Logging to a file is simpler than
    # spawning a background reader task and gives us a persistent log to
    # debug failures from later.
    dv_log_path = os.path.join(job.output_dir, "dv", f"{sample_name}.dv.log")
    os.makedirs(os.path.dirname(dv_log_path), exist_ok=True)
    dv_log_fh = open(dv_log_path, "ab", buffering=0)
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=dv_log_fh,
        stderr=asyncio.subprocess.STDOUT,
    )
    job.log(f"  DeepVariant stdout/stderr -> {dv_log_path}")

    # Monitor progress by checking intermediate files
    dv_start = time.time()
    last_log_time = dv_start
    last_stage = ""

    while proc.returncode is None:
        await asyncio.sleep(10)  # check every 10 seconds
        try:
            proc_poll = proc.returncode  # check if done
        except Exception:
            pass

        if proc.returncode is not None:
            break

        elapsed = time.time() - dv_start
        elapsed_min = elapsed / 60

        # Detect stage by checking running processes (file counting is unreliable
        # because run_deepvariant creates all output files immediately)
        import subprocess as _sp
        try:
            _me_result = _sp.run(['pgrep', '-c', '-f', 'make_examples.py.*--task'],
                                capture_output=True, text=True, timeout=3)
            me_running = int(_me_result.stdout.strip()) if _me_result.returncode == 0 else 0
        except Exception:
            me_running = 0
        try:
            _cv_result = _sp.run(['pgrep', '-c', '-f', 'call_variants.py'],
                                capture_output=True, text=True, timeout=3)
            cv_running = _cv_result.returncode == 0
        except Exception:
            cv_running = False
        try:
            _pp_result = _sp.run(['pgrep', '-c', '-f', 'postprocess_variants.py'],
                                capture_output=True, text=True, timeout=3)
            pp_running = _pp_result.returncode == 0
        except Exception:
            pp_running = False

        # Determine current stage based on active processes
        if pp_running:
            stage = "Stage 3/3: postprocess_variants (generating VCF)"
            stage_pct = 0.92
        elif cv_running:
            # call_variants is running — GPU stage
            stage = f"Stage 2/3: call_variants (GPU deep learning inference)"
            # Estimate progress within call_variants based on elapsed vs expected
            cv_expected_min = est_minutes * 0.3  # call_variants is ~30% of total time
            me_elapsed = elapsed_min - (est_minutes * 0.6)  # rough time after make_examples
            cv_progress = min(0.95, max(0, me_elapsed / cv_expected_min)) if cv_expected_min > 0 else 0.5
            stage_pct = 0.6 + 0.3 * cv_progress
        elif me_running > 0:
            # make_examples still running — count how many shards are done
            completed_shards = max(0, job.dv_shards - me_running)
            # Also use time-based estimate since shards finish at different rates
            time_pct = min(0.95, elapsed_min / (est_minutes * 0.6)) if est_minutes > 0 else 0
            shard_pct = completed_shards / job.dv_shards if job.dv_shards > 0 else 0
            # Use the higher of the two estimates
            me_pct = max(time_pct, shard_pct)
            stage = f"Stage 1/3: make_examples ({me_running} shards active, {completed_shards} done)"
            stage_pct = 0.6 * me_pct
        else:
            # No DV processes found — either just started or between stages
            stage = "Stage 1/3: make_examples (initializing...)"
            stage_pct = max(0.01, 0.6 * min(0.95, elapsed_min / (est_minutes * 0.6)) if est_minutes > 0 else 0.01)

        # Update progress (DV is ~80% of the total pipeline)
        job.progress = stage_pct * 0.8 / max(job.total_samples + 3, 1)

        # Compute ETA — use blended approach for stability
        # Initial estimate is based on BAM size; computed estimate from progress
        initial_remaining = max(0, est_minutes - elapsed_min)
        if stage_pct > 0.05:
            computed_total = elapsed / stage_pct
            computed_remaining = max(0, (computed_total - elapsed) / 60)
            # Blend: trust computed estimate more as we progress
            # At 10% done, 80% initial / 20% computed
            # At 50% done, 0% initial / 100% computed
            blend = min(1.0, (stage_pct - 0.05) / 0.45)
            remaining_min = (1 - blend) * initial_remaining + blend * computed_remaining
            # Cap: never exceed 2x initial estimate
            remaining_min = min(remaining_min, est_minutes * 2)
        else:
            remaining_min = initial_remaining
        eta_str = f"~{remaining_min:.0f} min remaining" if remaining_min < 120 else f"~{remaining_min/60:.1f} hrs remaining"

        # Log every 60 seconds with a new message
        now = time.time()
        if now - last_log_time >= 60 or stage != last_stage:
            job.log(f"  {stage} [{elapsed_min:.0f} min elapsed, {eta_str}]")
            on_progress(job)
            last_log_time = now
            last_stage = stage

    # Process completed — wait for it (stdout/stderr were redirected to the
    # log file, so there's nothing to read).
    rc = await proc.wait()
    try:
        dv_log_fh.close()
    except Exception:
        pass

    if rc != 0:
        # Surface the tail of the log file so the failure is debuggable
        tail = ""
        try:
            with open(dv_log_path, "rb") as fh:
                fh.seek(0, 2)
                size = fh.tell()
                fh.seek(max(0, size - 1500))
                tail = fh.read().decode(errors="replace")
        except Exception:
            pass
        raise RuntimeError(
            f"DeepVariant failed for {sample_name} (rc={rc}). "
            f"Log: {dv_log_path}\n{tail[-1500:]}"
        )

    elapsed_total = (time.time() - dv_start) / 60
    job.samples_done += 1
    job.progress = job.samples_done / (job.total_samples + 3)
    job.log(f"DeepVariant done: {sample_name} in {elapsed_total:.1f} min ({job.samples_done}/{job.total_samples})")
    on_progress(job)

    # Cleanup intermediate files
    try:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass

    return gvcf_path


async def glnexus_joint_genotype(job: PipelineJob, gvcf_paths: List[str], on_progress: Callable):
    """Run GLnexus to joint-genotype multiple gVCFs."""
    joint_vcf = os.path.join(job.output_dir, "joint", "joint.vcf.gz")
    os.makedirs(os.path.join(job.output_dir, "joint"), exist_ok=True)

    gvcf_list = " ".join(gvcf_paths)
    mem_gb = min(64, max(16, len(gvcf_paths) * 8))
    threads = min(40, job.cores)

    job.log(f"GLnexus: joint genotyping {len(gvcf_paths)} samples "
            f"(mem={mem_gb}GB, threads={threads})...")
    on_progress(job)

    cmd = (
        f"{APPTAINER} run {APPTAINER_BINDS} {GLNEXUS_IMAGE} "
        f"glnexus_cli --config DeepVariantWGS "
        f"--mem-gbytes {mem_gb} --threads {threads} "
        f"{gvcf_list} | {BCFTOOLS} view -Oz -o {joint_vcf}"
    )

    rc, stdout, stderr = await run_cmd(cmd, job)
    if rc != 0:
        raise RuntimeError(f"GLnexus failed: {stderr[-500:]}")

    # Index
    await run_cmd(f"tabix -p vcf {joint_vcf}", job)

    # Verify sample count
    rc, stdout, _ = await run_cmd(f"{BCFTOOLS} query -l {joint_vcf}", job)
    if rc == 0:
        samples = [s for s in stdout.strip().split("\n") if s]
        job.log(f"Joint VCF samples: {len(samples)} -> {', '.join(samples)}")

    return joint_vcf


async def deepvariant_pipeline(job: PipelineJob, on_progress: Callable):
    """DeepVariant mode: DV per-sample -> GLnexus joint -> filter -> QC."""
    bams = job.bam_paths if job.bam_paths else [job.bam_path]
    job.total_samples = len(bams)
    names = job.sample_names if job.sample_names else [get_sample_name(b) for b in bams]

    os.makedirs(os.path.join(job.output_dir, "dv"), exist_ok=True)

    # --- Phase 1: DeepVariant per-sample calling ---
    job.phase = 1
    job.phase_name = "DeepVariant Calling"
    job.step = "dv-calling"
    job.log(f"Phase 1: DeepVariant per-sample calling ({len(bams)} samples)...")
    on_progress(job)

    # Run samples with limited parallelism (DV is already multi-threaded)
    max_parallel = max(1, job.cores // job.dv_shards)
    semaphore = asyncio.Semaphore(max_parallel)

    async def call_with_sem(bam, name):
        async with semaphore:
            return await deepvariant_call_sample(job, bam, name, on_progress)

    tasks = [call_with_sem(bam, name) for bam, name in zip(bams, names)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    gvcfs = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            job.log(f"ERROR: {names[i]} failed: {r}")
            raise r
        gvcfs.append(r)

    # --- Phase 2: GLnexus joint genotyping ---
    job.phase = 2
    job.phase_name = "Joint Genotyping"
    job.step = "joint-genotyping"
    job.log(f"Phase 2: GLnexus joint genotyping...")
    on_progress(job)

    if len(gvcfs) > 1:
        joint_vcf = await glnexus_joint_genotype(job, gvcfs, on_progress)
    else:
        # Single sample — use the DV VCF directly
        sample_name = names[0]
        dv_vcf = os.path.join(job.output_dir, "dv", f"{sample_name}.vcf.gz")
        joint_vcf = dv_vcf
        job.log(f"Single sample — skipping joint genotyping, using DV VCF directly")

    job.progress = (job.total_samples + 1) / (job.total_samples + 3)
    on_progress(job)

    # --- Phase 3: Filtering & Normalization ---
    job.phase = 3
    job.phase_name = "Filter & Normalize"
    job.step = "filtering"
    job.log("Phase 3: Filtering & normalization...")
    on_progress(job)

    filtered_path = os.path.join(job.output_dir, "filtered.vcf.gz")

    # DeepVariant VCFs use FORMAT/DP (per-sample) not INFO/DP (site-level).
    # They also have their own FILTER field (PASS/RefCall).
    # Use QUAL filter only, then keep PASS variants.
    filter_expr = f"QUAL<{job.qual_filter}"

    # Check if INFO/DP exists before adding DP filter
    header_cmd = f"{BCFTOOLS} view -h {joint_vcf} 2>/dev/null"
    rc_h, header_out, _ = await run_cmd(header_cmd, job)
    has_info_dp = "##INFO=<ID=DP," in header_out if rc_h == 0 else False

    if has_info_dp and (job.min_dp > 0 or job.max_dp < 50000):
        filter_expr += f" || INFO/DP<{job.min_dp} || INFO/DP>{job.max_dp}"
        job.log(f"  Filter: QUAL>={job.qual_filter}, INFO/DP {job.min_dp}-{job.max_dp}")
    else:
        job.log(f"  Filter: QUAL>={job.qual_filter} (no INFO/DP field — DeepVariant uses FORMAT/DP)")

    rc, _, stderr = await run_cmd(
        f"{BCFTOOLS} filter -e '{filter_expr}' -s 'LowQual' {joint_vcf} | "
        f"{BCFTOOLS} view -f 'PASS,.' -Oz -o {filtered_path}",
        job,
    )
    if rc != 0:
        raise RuntimeError(f"Filter failed: {stderr}")

    # Split multi-allelic + normalize
    split_path = os.path.join(job.output_dir, "split.vcf.gz")
    rc, _, stderr = await run_cmd(
        f"{BCFTOOLS} norm -m -any -f {job.reference} -Oz -o {split_path} {filtered_path}",
        job,
    )
    if rc != 0:
        raise RuntimeError(f"Split multi-allelic failed: {stderr}")

    final_path = os.path.join(job.output_dir, "final.vcf.gz")
    rc, stdout, stderr = await run_cmd(
        f"{BCFTOOLS} norm -f {job.reference} -Oz -o {final_path} {split_path} 2>&1",
        job,
    )
    if rc != 0:
        raise RuntimeError(f"Left-align failed: {stderr}")

    for line in (stdout + stderr).strip().split("\n"):
        if line.strip():
            job.log(f"  norm: {line.strip()}")

    await run_cmd(f"{BCFTOOLS} index -t {final_path}", job)
    job.progress = (job.total_samples + 2) / (job.total_samples + 3)
    on_progress(job)

    # Cleanup intermediate files
    for p in [filtered_path, split_path]:
        try:
            os.remove(p)
        except OSError:
            pass

    # --- Phase 4: QC Validation ---
    job.phase = 4
    job.phase_name = "QC Validation"
    job.step = "qc"
    job.log("Phase 4: QC validation...")
    on_progress(job)

    is_multi = len(bams) > 1
    qc = await run_qc_validation(job, final_path, multi_sample=is_multi)
    job.qc_result = qc
    job.progress = 1.0
    on_progress(job)

    return final_path


# ═══════════════════════════════════════════════════════════════════════
# QC VALIDATION (shared between modes)
# ═══════════════════════════════════════════════════════════════════════

async def run_qc_validation(
    job: PipelineJob, vcf_path: str, multi_sample: bool = False
) -> QCResult:
    """Run QC validation checks on the final VCF."""
    qc = QCResult()
    checks_passed = 0
    checks_total = 0

    var_min = QC_VARIANT_MIN if multi_sample else QC_SINGLE_VARIANT_MIN
    var_max = QC_VARIANT_MAX if multi_sample else QC_SINGLE_VARIANT_MAX

    # 1 — Variant count
    job.log("  QC: Counting variants...")
    rc, stdout, _ = await run_cmd(f"{BCFTOOLS} view -H {vcf_path} | wc -l", job)
    if rc == 0:
        nv = int(stdout.strip())
        qc.variant_count = nv
        qc.variant_count_pass = var_min <= nv <= var_max
        checks_total += 1
        checks_passed += qc.variant_count_pass
        status = "PASS" if qc.variant_count_pass else "FAIL"
        job.log(f"  QC: [{status}] Variant count: {nv:,} "
                f"(expect {var_min:,}-{var_max:,})")
        if nv > 50_000_000:
            job.log(f"  QC: WARNING — {nv:,} is way too many, check filters!")

    # 2 — Ti/Tv ratio
    job.log("  QC: Computing Ti/Tv ratio...")
    rc, stdout, _ = await run_cmd(
        f"{BCFTOOLS} stats {vcf_path} | grep '^TSTV'",
        job,
    )
    if rc == 0:
        # TSTV line format: TSTV  0  ts  tv  ts/tv  ts  tv  ts/tv
        titv = 0.0
        for ln in stdout.strip().split("\n"):
            if not ln.startswith("TSTV"):
                continue
            parts = ln.split("\t")
            if len(parts) >= 5:
                try:
                    titv = float(parts[4])
                except ValueError:
                    pass
                break
        qc.titv_ratio = titv
        qc.titv_pass = QC_TITV_MIN <= titv <= QC_TITV_MAX
        checks_total += 1
        checks_passed += qc.titv_pass
        status = "PASS" if qc.titv_pass else "FAIL"
        job.log(f"  QC: [{status}] Ti/Tv: {titv:.3f} "
                f"(expect {QC_TITV_MIN}-{QC_TITV_MAX})")

    # 3 — Sample count
    job.log("  QC: Checking samples...")
    rc, stdout, _ = await run_cmd(f"{BCFTOOLS} query -l {vcf_path}", job)
    if rc == 0:
        samples = [s for s in stdout.strip().split("\n") if s]
        qc.sample_count = len(samples)
        expected_count = len(job.bam_paths) if job.bam_paths else 1
        qc.sample_count_pass = len(samples) == expected_count
        checks_total += 1
        checks_passed += qc.sample_count_pass
        status = "PASS" if qc.sample_count_pass else "FAIL"
        job.log(f"  QC: [{status}] Samples: {len(samples)} "
                f"(expected {expected_count}) -> {', '.join(samples)}")

    # 4 — Per-sample variant counts (only for multi-sample)
    if multi_sample and qc.sample_count > 1:
        job.log("  QC: Per-sample variant counts...")
        sc = {}
        for s in samples:
            rc, stdout, _ = await run_cmd(
                f"{BCFTOOLS} view -s {s} -c1 {vcf_path} | {BCFTOOLS} view -H | wc -l",
                job,
            )
            if rc == 0:
                sc[s] = int(stdout.strip())
        qc.per_sample_counts = sc
        if sc:
            mean_c = sum(sc.values()) / len(sc)
            max_dev = max(abs(c - mean_c) / mean_c for c in sc.values()) if mean_c > 0 else 0
            qc.per_sample_pass = max_dev < 0.30
            checks_total += 1
            checks_passed += qc.per_sample_pass
            status = "PASS" if qc.per_sample_pass else "FAIL"
            job.log(f"  QC: [{status}] Per-sample counts (max deviation {max_dev:.1%}):")
            for s, c in sc.items():
                job.log(f"       {s:12s}: {c:>10,}")

    # 5 — Het/Hom ratio
    job.log("  QC: Het/Hom stats...")
    rc, stdout, _ = await run_cmd(
        f"{BCFTOOLS} stats -s - {vcf_path} | grep '^PSC'", job
    )
    if rc == 0:
        qc.het_hom_extracted = len(stdout.strip()) > 0
        checks_total += 1
        checks_passed += qc.het_hom_extracted
        status = "PASS" if qc.het_hom_extracted else "FAIL"
        job.log(f"  QC: [{status}] Het/Hom stats extracted")

    # Summary
    qc.checks_passed = checks_passed
    qc.checks_total = checks_total
    qc.overall_pass = checks_passed == checks_total

    job.log(f"  QC SUMMARY: {checks_passed}/{checks_total} checks passed")
    if qc.overall_pass:
        job.log(f"  QC: ALL CLEAR")
    else:
        job.log(f"  QC: REVIEW FAILURES — {checks_total - checks_passed} check(s) failed")

    # Save QC report
    qc_path = os.path.join(job.output_dir, "qc_report.json")
    with open(qc_path, "w") as f:
        json.dump(qc.to_dict(), f, indent=2)
    job.log(f"  QC report: {qc_path}")

    return qc


# ═══════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

async def run_pipeline(job: PipelineJob, on_progress: Callable):
    """Main pipeline entry point — dispatches to bcftools or deepvariant mode."""
    job.status = "running"
    job.started_at = time.time()
    job.total_chroms = len(job.chromosomes)

    os.makedirs(job.output_dir, exist_ok=True)
    job.log(f"Output directory: {job.output_dir}")
    job.log(f"Pipeline mode: {job.mode}")

    # --- Validate ---
    job.step = "validating"
    job.log("Validating inputs...")
    on_progress(job)

    bams_to_check = job.bam_paths if job.bam_paths else [job.bam_path]
    for aln in bams_to_check:
        if not os.path.exists(aln):
            raise FileNotFoundError(f"Alignment file not found: {aln}")
        if aln.endswith(".cram"):
            idx_candidates = [aln + ".crai", aln.replace(".cram", ".crai")]
        else:
            idx_candidates = [aln + ".bai", aln.replace(".bam", ".bai")]
        if not any(os.path.exists(b) for b in idx_candidates):
            raise FileNotFoundError(f"Index not found for: {aln}")

    if not os.path.exists(job.reference):
        raise FileNotFoundError(f"Reference not found: {job.reference}")

    # Detect chrom prefix from first alignment file
    job.chrom_prefix = await detect_chrom_prefix(bams_to_check[0])
    job.log(f"Chromosome prefix: '{job.chrom_prefix}' (e.g. {job.chrom_prefix}1)")

    for aln in bams_to_check:
        job.log(f"Input: {aln}")
    job.log(f"Reference: {job.reference}")
    job.log(f"Cores: {job.cores}")

    if job.mode == PipelineMode.BCFTOOLS:
        job.log(f"Chromosomes: {len(job.chromosomes)}")
        job.log(f"Params: -q {job.min_map_qual} -Q {job.min_base_qual} "
                f"-d {job.max_depth} QUAL>={job.qual_filter} "
                f"DP {job.min_dp}-{job.max_dp}")
    else:
        job.log(f"DeepVariant shards: {job.dv_shards}")
        job.log(f"GPU: {'ENABLED' if job.use_gpu else 'DISABLED'}")
        job.log(f"Filter: QUAL>={job.qual_filter} DP {job.min_dp}-{job.max_dp}")
    on_progress(job)

    # --- Dispatch ---
    if job.mode == PipelineMode.DEEPVARIANT:
        final_path = await deepvariant_pipeline(job, on_progress)
    else:
        final_path = await bcftools_pipeline(job, on_progress)

    # --- Final stats ---
    stats_cmd = f"{BCFTOOLS} stats {final_path} | grep '^SN'"
    rc, stdout, _ = await run_cmd(stats_cmd, job)
    if rc == 0:
        for line in stdout.strip().split("\n"):
            if line.strip():
                job.log(f"  {line.strip()}")

    # --- Done ---
    final_size = os.path.getsize(final_path) / (1024 * 1024)
    job.output_vcf = final_path
    job.status = "completed"
    job.completed_at = time.time()
    job.step = "done"
    job.phase = 5
    job.phase_name = "Complete"
    elapsed = job.completed_at - job.started_at
    job.log(f"Pipeline complete! {final_size:.1f} MB in {elapsed/60:.1f} minutes")

    if job.qc_result and not job.qc_result.overall_pass:
        job.log(f"WARNING: QC validation had failures — review before using this VCF")

    on_progress(job)


# ═══════════════════════════════════════════════════════════════════════
# RESUME LOGIC
# ═══════════════════════════════════════════════════════════════════════

def _try_recover_example_info(tmp_dir: str, shards: int):
    """Recover missing example_info.json files from other job directories.

    call_variants needs these files to determine tensor shape. If make_examples
    completed but these were lost, find them from a prior run with the same
    sample and shard count.
    """
    import glob as _glob
    sample_dir_name = os.path.basename(tmp_dir)  # e.g. "tmp_Chichi"
    parent = os.path.dirname(os.path.dirname(os.path.dirname(tmp_dir)))  # nimog_output/

    for job_dir in _glob.glob(os.path.join(parent, "*/dv", sample_dir_name)):
        if job_dir == tmp_dir:
            continue
        donor_info = _glob.glob(os.path.join(job_dir, "*.example_info.json"))
        if len(donor_info) >= shards:
            for src in donor_info:
                dst = os.path.join(tmp_dir, os.path.basename(src))
                if not os.path.exists(dst):
                    try:
                        import shutil
                        shutil.copy2(src, dst)
                    except Exception:
                        pass
            return


def detect_resume_point(job: PipelineJob) -> Optional[str]:
    """Check what files exist and return the step we can resume from, or None."""
    final = os.path.join(job.output_dir, "final.vcf.gz")
    final_tbi = final + ".tbi"
    norm = os.path.join(job.output_dir, "merged.norm.vcf.gz")
    merged = os.path.join(job.output_dir, "merged.vcf.gz")

    if os.path.exists(final) and os.path.exists(final_tbi):
        return "qc"        # final VCF exists, just need QC + cleanup
    if os.path.exists(norm):
        return "filtering"  # normalized VCF exists, need filter + QC
    if os.path.exists(merged):
        return "normalizing" # merged VCF exists, need norm + filter + QC

    # Check for per-chromosome files (bcftools mode)
    chr_files = [os.path.join(job.output_dir, f"chr{c}.vcf.gz") for c in job.chromosomes]
    existing = [f for f in chr_files if os.path.exists(f)]
    if existing:
        return "concatenating"  # some chr files exist, need concat + norm + filter + QC

    # --- DeepVariant mode resume points ---
    if job.mode == PipelineMode.DEEPVARIANT:
        bams = job.bam_paths if job.bam_paths else [job.bam_path]
        names = job.sample_names if job.sample_names else [get_sample_name(b) for b in bams]

        # Check if all per-sample DV VCFs exist (skip to filter/QC)
        dv_vcfs = [os.path.join(job.output_dir, "dv", f"{n}.vcf.gz") for n in names]
        if all(os.path.exists(v) for v in dv_vcfs):
            return "dv-filter"

        # Check for completed call_variants output (skip to postprocess)
        for name in names:
            tmp_dir = os.path.join(job.output_dir, "dv", f"tmp_{name}")
            # call_variants output can be sharded (call_variants_output-XXXXX-of-YYYYY.tfrecord.gz)
            # or unsharded (call_variants_output.tfrecord.gz)
            import glob as _glob2
            cv_shards = _glob2.glob(os.path.join(tmp_dir, "call_variants_output-*-of-*.tfrecord.gz"))
            cv_single = os.path.join(tmp_dir, "call_variants_output.tfrecord.gz")
            if (cv_shards and all(os.path.getsize(f) > 0 for f in cv_shards)) or \
               (os.path.exists(cv_single) and os.path.getsize(cv_single) > 0):
                return "dv-postprocess"

        # Check for completed make_examples tfrecords (skip to call_variants)
        for name in names:
            tmp_dir = os.path.join(job.output_dir, "dv", f"tmp_{name}")
            if not os.path.isdir(tmp_dir):
                continue
            # Auto-detect shard count from filenames on disk
            import glob as _glob
            me_files = sorted(_glob.glob(os.path.join(tmp_dir, "make_examples.tfrecord-*-of-*.gz")))
            if not me_files:
                continue
            # Extract shard count from filename pattern (e.g. -00000-of-00020.gz)
            last = os.path.basename(me_files[-1])
            try:
                shards = int(last.split("-of-")[1].replace(".gz", ""))
            except (IndexError, ValueError):
                shards = job.dv_shards or 20
            if len(me_files) == shards and all(os.path.getsize(f) > 0 for f in me_files):
                # Update job.dv_shards to match what's actually on disk
                job.dv_shards = shards
                # Ensure example_info.json files exist (needed by call_variants
                # for tensor shape info). If missing, search other jobs for them.
                info_files = [f + ".example_info.json" for f in me_files]
                if not all(os.path.exists(f) for f in info_files):
                    _try_recover_example_info(tmp_dir, shards)
                return "dv-call_variants"

    return None  # nothing salvageable


async def _dv_resume_call_variants(job: PipelineJob, sample_name: str, on_progress: Callable):
    """Resume DeepVariant from call_variants stage using existing make_examples tfrecords."""
    tmp_dir = os.path.join(job.output_dir, "dv", f"tmp_{sample_name}")
    gvcf_path = os.path.join(job.output_dir, "dv", f"{sample_name}.g.vcf.gz")
    vcf_path = os.path.join(job.output_dir, "dv", f"{sample_name}.vcf.gz")
    shards = job.dv_shards or 20

    examples = os.path.join(tmp_dir, f"make_examples.tfrecord@{shards}.gz")
    cv_output = os.path.join(tmp_dir, "call_variants_output.tfrecord.gz")
    gvcf_tfrecords = os.path.join(tmp_dir, f"gvcf.tfrecord@{shards}.gz")

    dv_image = get_dv_image(job.use_gpu)
    nv_flag = "--nv" if job.use_gpu else ""

    # --- Stage 2: call_variants (GPU) ---
    job.log(f"  Stage 2/3: call_variants (GPU inference) for {sample_name}...")
    on_progress(job)

    cv_cmd = (
        f"{APPTAINER} exec {nv_flag} {APPTAINER_BINDS} {dv_image} "
        f"/opt/deepvariant/bin/call_variants "
        f"--outfile \"{cv_output}\" "
        f"--examples \"{examples}\" "
        f"--checkpoint /opt/models/wgs "
        f"--batch_size 512 --num_readers 4"
    )

    cv_start = time.time()
    # Same pipe-buffer-deadlock fix as deepvariant_call_sample: never PIPE
    # an output we don't actively read.
    cv_log_path = os.path.join(tmp_dir, f"{sample_name}.call_variants.log")
    cv_log_fh = open(cv_log_path, "ab", buffering=0)
    proc = await asyncio.create_subprocess_shell(
        cv_cmd, stdout=cv_log_fh, stderr=asyncio.subprocess.STDOUT,
    )

    # Monitor call_variants progress
    while proc.returncode is None:
        await asyncio.sleep(15)
        try:
            _ = proc.returncode
        except Exception:
            pass
        if proc.returncode is not None:
            break
        elapsed_min = (time.time() - cv_start) / 60
        job.log(f"  call_variants running... [{elapsed_min:.0f} min elapsed]")
        on_progress(job)

    rc = await proc.wait()
    try:
        cv_log_fh.close()
    except Exception:
        pass
    if rc != 0:
        tail = ""
        try:
            with open(cv_log_path, "rb") as fh:
                fh.seek(0, 2); size = fh.tell(); fh.seek(max(0, size - 1500))
                tail = fh.read().decode(errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"call_variants failed for {sample_name} (rc={rc}). Log: {cv_log_path}\n{tail[-1500:]}")

    cv_elapsed = (time.time() - cv_start) / 60
    job.log(f"  call_variants done for {sample_name} in {cv_elapsed:.1f} min")
    on_progress(job)

    # --- Stage 3: postprocess_variants ---
    job.log(f"  Stage 3/3: postprocess_variants for {sample_name}...")
    on_progress(job)

    pp_cmd = (
        f"{APPTAINER} exec {nv_flag} {APPTAINER_BINDS} {dv_image} "
        f"/opt/deepvariant/bin/postprocess_variants "
        f"--ref \"{job.reference}\" "
        f"--infile \"{cv_output}\" "
        f"--outfile \"{vcf_path}\" "
        f"--cpus {shards} "
        f"--nonvariant_site_tfrecord_path \"{gvcf_tfrecords}\" "
        f"--gvcf_outfile \"{gvcf_path}\""
    )

    rc, stdout_pp, stderr_pp = await run_cmd(pp_cmd, job)
    if rc != 0:
        raise RuntimeError(f"postprocess_variants failed for {sample_name}: {stderr_pp[-500:]}")

    job.log(f"  postprocess_variants done for {sample_name}")
    on_progress(job)

    # Cleanup intermediate files
    try:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass

    return gvcf_path


async def _dv_resume_postprocess(job: PipelineJob, sample_name: str, on_progress: Callable):
    """Resume DeepVariant from postprocess_variants using existing call_variants output."""
    tmp_dir = os.path.join(job.output_dir, "dv", f"tmp_{sample_name}")
    gvcf_path = os.path.join(job.output_dir, "dv", f"{sample_name}.g.vcf.gz")
    vcf_path = os.path.join(job.output_dir, "dv", f"{sample_name}.vcf.gz")
    shards = job.dv_shards or 20

    # call_variants output may be sharded — detect and use @N notation
    import glob as _glob_pp
    cv_shards = sorted(_glob_pp.glob(os.path.join(tmp_dir, "call_variants_output-*-of-*.tfrecord.gz")))
    if cv_shards:
        n_cv = len(cv_shards)
        cv_output = os.path.join(tmp_dir, f"call_variants_output@{n_cv}.tfrecord.gz")
    else:
        cv_output = os.path.join(tmp_dir, "call_variants_output.tfrecord.gz")
    gvcf_tfrecords = os.path.join(tmp_dir, f"gvcf.tfrecord@{shards}.gz")

    dv_image = get_dv_image(job.use_gpu)
    nv_flag = "--nv" if job.use_gpu else ""

    job.log(f"  Stage 3/3: postprocess_variants for {sample_name}...")
    on_progress(job)

    pp_cmd = (
        f"{APPTAINER} exec {nv_flag} {APPTAINER_BINDS} {dv_image} "
        f"/opt/deepvariant/bin/postprocess_variants "
        f"--ref \"{job.reference}\" "
        f"--infile \"{cv_output}\" "
        f"--outfile \"{vcf_path}\" "
        f"--cpus {shards} "
        f"--nonvariant_site_tfrecord_path \"{gvcf_tfrecords}\" "
        f"--gvcf_outfile \"{gvcf_path}\""
    )

    rc, stdout_pp, stderr_pp = await run_cmd(pp_cmd, job)
    if rc != 0:
        raise RuntimeError(f"postprocess_variants failed for {sample_name}: {stderr_pp[-500:]}")

    job.log(f"  postprocess_variants done for {sample_name}")
    on_progress(job)

    # Cleanup intermediate files
    try:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass

    return gvcf_path


async def resume_pipeline(job: PipelineJob, on_progress: Callable):
    """Resume a failed pipeline from the earliest resumable step."""
    resume_from = detect_resume_point(job)
    if not resume_from:
        raise RuntimeError("No intermediate files found — need full rerun")

    job.status = "running"
    job.error = None
    job.log(f"--- RESUMING from step: {resume_from} ---")
    on_progress(job)

    final_path = os.path.join(job.output_dir, "final.vcf.gz")
    norm_path = os.path.join(job.output_dir, "merged.norm.vcf.gz")
    merged_path = os.path.join(job.output_dir, "merged.vcf.gz")
    total_steps = job.total_chroms + 4

    is_multi = len(job.bam_paths) > 1

    # --- DeepVariant resume paths ---
    if resume_from.startswith("dv-"):
        bams = job.bam_paths if job.bam_paths else [job.bam_path]
        names = job.sample_names if job.sample_names else [get_sample_name(b) for b in bams]
        job.total_samples = len(bams)

        if resume_from == "dv-call_variants":
            job.phase = 1
            job.phase_name = "DeepVariant Resume (call_variants)"
            job.step = "dv-call_variants"
            job.log(f"Resuming DeepVariant from call_variants for {len(names)} sample(s)...")
            on_progress(job)

            gvcfs = []
            for name in names:
                gvcf = await _dv_resume_call_variants(job, name, on_progress)
                gvcfs.append(gvcf)
                job.samples_done += 1
                job.progress = job.samples_done / (job.total_samples + 3)
                on_progress(job)

            resume_from = "dv-filter"  # fall through

        if resume_from == "dv-postprocess":
            job.phase = 1
            job.phase_name = "DeepVariant Resume (postprocess)"
            job.step = "dv-postprocess"
            job.log(f"Resuming DeepVariant from postprocess for {len(names)} sample(s)...")
            on_progress(job)

            gvcfs = []
            for name in names:
                gvcf = await _dv_resume_postprocess(job, name, on_progress)
                gvcfs.append(gvcf)
                job.samples_done += 1
                job.progress = job.samples_done / (job.total_samples + 3)
                on_progress(job)

            resume_from = "dv-filter"  # fall through

        if resume_from == "dv-filter":
            # We have per-sample VCFs — run the same filter/QC as deepvariant_pipeline
            job.phase = 3
            job.phase_name = "Filter & Normalize"
            job.step = "filtering"

            if len(names) > 1:
                # Multi-sample: need GLnexus joint genotyping first
                gvcf_paths = [os.path.join(job.output_dir, "dv", f"{n}.g.vcf.gz") for n in names]
                job.log("Running GLnexus joint genotyping...")
                on_progress(job)
                joint_vcf = await glnexus_joint_genotype(job, gvcf_paths, on_progress)
            else:
                joint_vcf = os.path.join(job.output_dir, "dv", f"{names[0]}.vcf.gz")
                job.log(f"Single sample — using DV VCF directly")

            job.progress = (job.total_samples + 1) / (job.total_samples + 3)
            on_progress(job)

            job.log("Filtering & normalization...")
            on_progress(job)

            filtered_path = os.path.join(job.output_dir, "filtered.vcf.gz")
            filter_expr = f"QUAL<{job.qual_filter}"

            header_cmd = f"{BCFTOOLS} view -h {joint_vcf} 2>/dev/null"
            rc_h, header_out, _ = await run_cmd(header_cmd, job)
            has_info_dp = "##INFO=<ID=DP," in header_out if rc_h == 0 else False

            if has_info_dp and (job.min_dp > 0 or job.max_dp < 50000):
                filter_expr += f" || INFO/DP<{job.min_dp} || INFO/DP>{job.max_dp}"

            rc, _, stderr = await run_cmd(
                f"{BCFTOOLS} filter -e '{filter_expr}' -s 'LowQual' {joint_vcf} | "
                f"{BCFTOOLS} view -f 'PASS,.' -Oz -o {filtered_path}",
                job,
            )
            if rc != 0:
                raise RuntimeError(f"Filter failed: {stderr}")

            split_path = os.path.join(job.output_dir, "split.vcf.gz")
            rc, _, stderr = await run_cmd(
                f"{BCFTOOLS} norm -m -any -f {job.reference} -Oz -o {split_path} {filtered_path}",
                job,
            )
            if rc != 0:
                raise RuntimeError(f"Split multi-allelic failed: {stderr}")

            rc, stdout_norm, stderr_norm = await run_cmd(
                f"{BCFTOOLS} norm -f {job.reference} -Oz -o {final_path} {split_path} 2>&1",
                job,
            )
            if rc != 0:
                raise RuntimeError(f"Left-align failed: {stderr_norm}")

            for line in (stdout_norm + stderr_norm).strip().split("\n"):
                if line.strip():
                    job.log(f"  norm: {line.strip()}")

            await run_cmd(f"{BCFTOOLS} index -t {final_path}", job)
            job.progress = (job.total_samples + 2) / (job.total_samples + 3)
            on_progress(job)

            for p in [filtered_path, split_path]:
                try:
                    os.remove(p)
                except OSError:
                    pass

            # QC
            job.phase = 4
            job.phase_name = "QC Validation"
            job.step = "qc"
            job.log("QC validation...")
            on_progress(job)

            qc = await run_qc_validation(job, final_path, multi_sample=is_multi)
            job.qc_result = qc
            job.progress = 1.0
            on_progress(job)

        # --- Final stats & done ---
        stats_cmd = f"{BCFTOOLS} stats {final_path} | grep '^SN'"
        rc, stdout_stats, _ = await run_cmd(stats_cmd, job)
        if rc == 0:
            for line in stdout_stats.strip().split("\n"):
                if line.strip():
                    job.log(f"  {line.strip()}")

        final_size = os.path.getsize(final_path) / (1024 * 1024)
        job.output_vcf = final_path
        job.status = "completed"
        job.completed_at = time.time()
        job.step = "done"
        job.phase = 5
        job.phase_name = "Complete"
        job.log(f"Pipeline complete! {final_size:.1f} MB")

        if job.qc_result and not job.qc_result.overall_pass:
            job.log(f"WARNING: QC validation had failures — review before using this VCF")

        on_progress(job)
        return

    # --- bcftools mode resume paths ---
    if resume_from == "concatenating":
        # --- Concatenate ---
        job.phase = 2
        job.phase_name = "Concatenation"
        job.step = "concatenating"
        job.log("Concatenating chromosome VCFs...")
        on_progress(job)

        chr_files = [os.path.join(job.output_dir, f"chr{c}.vcf.gz")
                     for c in sorted(job.chromosomes, key=int)]
        existing_files = [f for f in chr_files if os.path.exists(f)]

        rc, _, stderr = await run_cmd(
            f"{BCFTOOLS} concat -Oz -o {merged_path} {' '.join(existing_files)}", job
        )
        if rc != 0:
            raise RuntimeError(f"Concat failed: {stderr}")
        await run_cmd(f"{BCFTOOLS} index -t {merged_path}", job)
        job.progress = (job.total_chroms + 1) / total_steps
        job.log("Concatenation complete")
        on_progress(job)
        resume_from = "normalizing"  # fall through

    if resume_from == "normalizing":
        # --- Normalize ---
        job.phase = 3
        job.phase_name = "Normalization"
        job.step = "normalizing"
        job.log("Normalizing (left-align, split multiallelics)...")
        on_progress(job)

        rc, stdout, stderr = await run_cmd(
            f"{BCFTOOLS} norm -m -any -f {job.reference} {merged_path} -Oz -o {norm_path} 2>&1",
            job,
        )
        if rc != 0:
            raise RuntimeError(f"Normalize failed: {stderr}")
        for line in (stdout + stderr).strip().split("\n"):
            if line.strip():
                job.log(f"  norm: {line.strip()}")
        await run_cmd(f"{BCFTOOLS} index -t {norm_path}", job)
        job.progress = (job.total_chroms + 2) / total_steps
        on_progress(job)
        resume_from = "filtering"  # fall through

    if resume_from == "filtering":
        # --- Filter ---
        job.step = "filtering"
        job.log(f"Filtering (QUAL>={job.qual_filter}, DP {job.min_dp}-{job.max_dp})...")
        on_progress(job)

        filter_expr = f"QUAL<{job.qual_filter}"
        if job.min_dp > 0 or job.max_dp < 50000:
            filter_expr += f" || INFO/DP<{job.min_dp} || INFO/DP>{job.max_dp}"

        rc, _, stderr = await run_cmd(
            f"{BCFTOOLS} filter -e '{filter_expr}' -s 'LowQual' {norm_path} | "
            f"{BCFTOOLS} view -f 'PASS' -Oz -o {final_path}",
            job,
        )
        if rc != 0:
            raise RuntimeError(f"Filter failed: {stderr}")
        await run_cmd(f"{BCFTOOLS} index -t {final_path}", job)
        job.progress = (job.total_chroms + 3) / total_steps
        on_progress(job)
        resume_from = "qc"  # fall through

    if resume_from == "qc":
        # --- QC Validation ---
        job.phase = 4
        job.phase_name = "QC Validation"
        job.step = "qc"
        job.log("Running QC validation...")
        on_progress(job)

        qc = await run_qc_validation(job, final_path, multi_sample=is_multi)
        job.qc_result = qc
        job.progress = (job.total_chroms + 4) / total_steps
        on_progress(job)

    # --- Cleanup ---
    job.step = "cleanup"
    job.log("Cleaning up temporary files...")
    for c in job.chromosomes:
        for ext in [".vcf.gz", ".vcf.gz.tbi"]:
            try:
                os.remove(os.path.join(job.output_dir, f"chr{c}{ext}"))
            except OSError:
                pass
    for p in [merged_path, norm_path]:
        for ext in ["", ".tbi"]:
            try:
                os.remove(p + ext)
            except OSError:
                pass

    # --- Final stats ---
    stats_cmd = f"{BCFTOOLS} stats {final_path} | grep '^SN'"
    rc, stdout, _ = await run_cmd(stats_cmd, job)
    if rc == 0:
        for line in stdout.strip().split("\n"):
            if line.strip():
                job.log(f"  {line.strip()}")

    # --- Done ---
    final_size = os.path.getsize(final_path) / (1024 * 1024)
    job.output_vcf = final_path
    job.status = "completed"
    job.completed_at = time.time()
    job.step = "done"
    job.phase = 5
    job.phase_name = "Complete"
    elapsed = job.completed_at - (job.started_at or time.time())
    job.log(f"Pipeline complete! {final_size:.1f} MB")

    if job.qc_result and not job.qc_result.overall_pass:
        job.log(f"WARNING: QC validation had failures — review before using this VCF")

    on_progress(job)
