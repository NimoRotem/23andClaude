# bam-converter — `/convert/` (port 8720)

A standalone FastAPI service that converts BAM/CRAM alignments into
normalized VCF files. Primary use is genome-wide variant calling for
users who only have alignment files and want a VCF to feed into
`simple-genomics`. Optional GPU acceleration via DeepVariant.

- **URL**: `https://23andclaude.com/convert/`
- **Port**: 8720
- **Service**: supervisor `bam-converter`
- **Code**: `/home/nimrod_rotem/bam-converter/`
- **Jobs**: `/home/nimrod_rotem/bam-converter/jobs/`
- **Entry**: `app.py` (FastAPI) + `pipeline.py` (engine)
- **Runtime**: `/home/nimo/miniconda3/envs/genomics/bin/python`

## 1. What it does

Given a BAM or CRAM aligned to GRCh38, produce a bgzipped, indexed,
normalized VCF (`*.vcf.gz` + `.tbi`) suitable for any downstream
genomics tool. Two calling backends:

| Mode         | Caller(s)                       | Best for |
| ------------ | ------------------------------- | --- |
| `bcftools`   | bcftools mpileup + call per chr | Single-sample fast turnaround |
| `deepvariant`| DeepVariant per sample + GLnexus joint | Family / trio / batch (joint genotyping) |

The bcftools mode is the daily-use path (~15–30 min for 30× WGS).
DeepVariant mode is the high-quality/joint path (~hours for WGS on
GPU; days on CPU).

## 2. Pipeline (`pipeline.py`)

### 2.1 Reference selection

Reads the input alignment header. Chooses one of:

| Image / binary                   | Used in mode |
| -------------------------------- | --- |
| `/home/nimo/miniconda3/envs/genomics/bin/bcftools` | bcftools |
| `/home/nimo/miniconda3/envs/genomics/bin/samtools` | both |
| `/data/containers/deepvariant_1.6.1-gpu.sif`       | deepvariant (GPU) |
| `/data/pgs2/containers/deepvariant.sif`            | deepvariant (CPU) |
| `docker://google/deepvariant:1.6.1-gpu`            | deepvariant fallback |
| `/scratch/tmp/glnexus_v1.4.1.sif`                  | deepvariant (joint) |
| `docker://ghcr.io/dnanexus-rnd/glnexus:v1.4.1`     | deepvariant fallback |

GPU presence is detected at runtime via `nvidia-smi --query-gpu=name`
(`detect_gpu()` in `pipeline.py`). On the genom-beast-gpu host (T4
GPU) DeepVariant defaults to the GPU image.

### 2.2 bcftools mode

22 autosomes + X/Y/M in parallel (12-way default):

```bash
samtools view -b -T <ref> <bam> chrN | \
  bcftools mpileup -f <ref> --max-depth 250 -q 20 -Q 20 \
                   -a FORMAT/AD,FORMAT/DP -Ou - | \
  bcftools call -mv -Oz -o per_chr/chrN.vcf.gz

# concat
bcftools concat -a per_chr/chr*.vcf.gz -Oz -o final.vcf.gz
bcftools index -t final.vcf.gz
```

Identical parameters to `simple-genomics/scripts/cram_to_vcf.sh` —
the two pipelines share the same calling profile so a VCF produced by
the converter behaves the same as one produced ad-hoc inside
simple-genomics.

### 2.3 DeepVariant mode

```bash
apptainer exec --nv /data/containers/deepvariant_1.6.1-gpu.sif \
    /opt/deepvariant/bin/run_deepvariant \
    --model_type=WGS \
    --ref=<ref> \
    --reads=<bam> \
    --output_vcf=<sample>.vcf.gz \
    --output_gvcf=<sample>.g.vcf.gz \
    --num_shards=$(nproc)
```

For multi-sample input (e.g. a trio), GLnexus joint-genotypes the
gVCFs:

```bash
apptainer exec /scratch/tmp/glnexus_v1.4.1.sif \
    glnexus_cli --config DeepVariant \
                --threads $(nproc) \
                --bed <regions.bed> \
                sample1.g.vcf.gz sample2.g.vcf.gz sample3.g.vcf.gz \
    > family.bcf
bcftools view family.bcf | bgzip > family.vcf.gz
bcftools index -t family.vcf.gz
```

Output naming:

- `<sample>.vcf.gz` — variant-only sample VCF
- `<sample>.g.vcf.gz` — gVCF (DeepVariant only)
- `family.vcf.gz` — joint-genotyped, multi-sample VCF (DeepVariant
  with multiple inputs)

### 2.4 QC validation

Before declaring a conversion done, the pipeline runs a QC pass
(implemented in `pipeline.py::qc_validate`):

| Check                          | Source                  | Pass gate |
| ------------------------------ | ----------------------- | --- |
| Total variant count            | bcftools stats          | ≥ 3 M (WGS) / ≥ 50K (exome) |
| Ts/Tv ratio                    | bcftools stats          | 2.0 – 2.2 (WGS) / 3.0 – 3.5 (exome) |
| Hom/het ratio                  | bcftools stats          | 1.5 – 2.5 |
| Singleton rate (DP=1 only)     | bcftools stats          | < 30 % |
| Build sanity (3-SNP spot)      | matches simple-genomics | ≥ 2 of 3 SNPs at expected coords |

QC verdict is emitted as `qc.json` alongside the VCF and downloadable
via `/api/download/{job_id}/qc`.

## 3. API

```
GET  /                                       UI HTML (single-page app, static/)
GET  /api/gpu-status                         is the GPU live? (nvidia-smi result)
GET  /api/browse?path=/data/genom-nimo       directory listing for the path picker
POST /api/convert                            kick off a job
GET  /api/jobs                               list user's jobs
GET  /api/jobs/{job_id}                      job status JSON
GET  /api/jobs/{job_id}/stream               SSE — per-step progress events
GET  /api/download/{job_id}                  download the resulting VCF
GET  /api/download/{job_id}/qc               download qc.json
GET  /api/jobs/{job_id}/resume-check         is this job resumable?
POST /api/jobs/{job_id}/resume               resume a partially-failed job
```

### POST /api/convert payload

```json
{
  "input_path": "/data/genom-nimo/SZ7A76M9LNU/SZ7A76M9LNU.cram",
  "mode": "bcftools",          // or "deepvariant"
  "out_dir": "/data/vcfs/converted",
  "sample_name": "SZ7A76M9LNU",
  "ref_build": "GRCh38",
  "options": {
    "threads": 12,
    "joint_with": ["family_mom.cram", "family_dad.cram"],  // deepvariant only
    "regions_bed": null                                     // optional restriction
  }
}
```

Returns `{"job_id": "...", "status": "queued"}`. The SSE stream at
`/api/jobs/{job_id}/stream` then carries `{"step": "...", "pct":
..., "stderr_tail": "..."}` events.

## 4. Job state

`jobs/<job_id>/`:

```
jobs/<job_id>/
├── job.json              status, started_at, completed_at, input, options
├── stdout.log            per-step stdout (multi-megabyte for WGS)
├── stderr.log            per-step stderr
├── progress.jsonl        SSE event log (replayable)
├── out/                  the actual VCF outputs
│   ├── sample.vcf.gz
│   ├── sample.vcf.gz.tbi
│   └── qc.json
└── _resume_state.json    last-completed-step pointer
```

`_resume_state.json` lets a failed job pick up where it died —
crucial for DeepVariant runs that take 6+ hours on GPU. The
resume-check endpoint examines this and tells the UI whether
restart-from-scratch is needed (e.g. input file moved) or whether
the job can fast-forward.

## 5. Path-picker safety

`/api/browse?path=` is the directory listing the UI uses. It is
**not** chrooted — any directory readable to the `nimrod_rotem` user
is listable. The intended audience is the operator running the
service, not random web users. Auth is enforced by nginx-side basic
auth or by being on the same private network. The reviewer should
flag this if it's escalated to a public-facing tier.

## 6. Caveats and gotchas

- **CRAM reference**: the converter assumes a matching reference is
  on disk (no automatic download). If the CRAM was aligned to a
  weird build, the bcftools `samtools view -T` call fails fast.
- **bcftools call -mv**: variant-only by default. To produce a
  gVCF, switch to deepvariant mode — bcftools call -m (without -v)
  would emit hom-ref records but those don't carry the FORMAT/GVCF
  block tagging the rest of the platform expects.
- **DeepVariant model**: the WGS model is hardcoded. Exome and PacBio
  models exist (`run_deepvariant --model_type=WES` / `PACBIO`) but
  aren't currently selectable from the UI. Open follow-up.
- **GPU memory**: DeepVariant GPU requires ~6 GB free; on a busy host
  the runtime detects OOM and falls back to CPU automatically (slow).

## 7. Open questions

- Should the QC gate be a **hard** refusal (don't emit a VCF that
  fails Ts/Tv) or a **warning** (emit + flag)? Today it's a warning,
  which means a low-quality alignment can still produce a VCF that
  downstream tools won't catch as suspect.
- Should we add per-chromosome QC (coverage distribution, ROH
  density) rather than only genome-wide aggregates?
- DeepVariant + GLnexus is gold-standard for joint genotyping; is
  it worth the GPU cost for our use cases, or should we steer all
  users to bcftools and only invoke DeepVariant for genuinely-
  family analyses?
