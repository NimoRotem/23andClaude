# File Conversion Pipelines

## BAM to VCF via nimog (Web UI)

nimog is embedded in the main app at `http://localhost:8600/nimog/`.
Output: `/scratch/nimog_output/<run_id>/dv/`

## BAM to VCF via DeepVariant (GPU — direct command)

```bash
INPUT_BAM="/data/aligned_bams/Sample1.bam"
SAMPLE="Sample1"
OUTPUT_DIR="/scratch/nimog_output/manual_dv"
mkdir -p "$OUTPUT_DIR"

apptainer run --nv \
  -B /data:/data \
  -B /scratch:/scratch \
  -B "$HOME":"$HOME" \
  /data/containers/deepvariant_1.6.1-gpu.sif \
  /opt/deepvariant/bin/run_deepvariant \
    --model_type=WGS \
    --ref=/data/reference/GRCh38.fa \
    --reads="$INPUT_BAM" \
    --output_vcf="$OUTPUT_DIR/${SAMPLE}.vcf.gz" \
    --output_gvcf="$OUTPUT_DIR/${SAMPLE}.g.vcf.gz" \
    --num_shards=20 \
    --intermediate_results_dir="$OUTPUT_DIR/tmp_${SAMPLE}"
```

**Key:** Use `apptainer` (not singularity), `--nv` for GPU, bind mounts with `-B`.

## FASTQ to BAM

```bash
conda activate genomics
SAMPLE="SampleName"
REF="/data/reference/GRCh38.fa"

bwa mem -t 32 -R "@RG\tID:${SAMPLE}\tSM:${SAMPLE}\tPL:ILLUMINA" \
  "$REF" R1.fastq.gz R2.fastq.gz \
| samtools sort -@ 16 -o /scratch/alignments/${SAMPLE}.bam

samtools index /scratch/alignments/${SAMPLE}.bam
```

Or via API:
```bash
curl -X POST http://localhost:8600/api/files/convert/fastq-to-bam \
  -H "Content-Type: application/json" \
  -d '{"fastq_r1": "R1.fq.gz", "fastq_r2": "R2.fq.gz", "sample_name": "Sample", "aligner": "bwa", "threads": 32}'
```

## VCF Filtering

```bash
bcftools view -f PASS input.vcf.gz -Oz -o filtered.vcf.gz
bcftools view -r chr1:1000000-2000000 input.vcf.gz -Oz -o region.vcf.gz
bcftools view -i 'QUAL>30' input.vcf.gz -Oz -o highqual.vcf.gz
bcftools index -t filtered.vcf.gz   # Always index output
```

## File Inspection

```bash
# BAM
samtools quickcheck file.bam && echo OK
samtools flagstat file.bam
samtools idxstats file.bam

# VCF
bcftools stats file.vcf.gz
bcftools query -l file.vcf.gz          # List samples
bcftools view -H file.vcf.gz | wc -l   # Count variants

# Via API
curl -X POST http://localhost:8600/api/files/inspect -H "Content-Type: application/json" \
  -d '{"path": "/data/aligned_bams/Sample1.bam"}'
```
