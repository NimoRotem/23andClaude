#!/usr/bin/env bash
# Generate a genome-wide VCF from a CRAM file by parallelizing per-chromosome.
# Usage: bash cram_to_vcf.sh /data/aligned_bams/SZ7A76M9LNU.cram /data/vcfs/SZ7A76M9LNU.vcf.gz
#
# Uses the same pipeline as runners.py: samtools view (ignore_md5) → mpileup → call.
# Runs up to MAX_PARALLEL chromosomes concurrently.

set -euo pipefail

CRAM="${1:?Usage: $0 <cram_path> <output_vcf_gz>}"
OUT="${2:?Usage: $0 <cram_path> <output_vcf_gz>}"

SAMTOOLS=/home/nimrod_rotem/conda/envs/genomics/bin/samtools
BCFTOOLS=/home/nimrod_rotem/conda/envs/genomics/bin/bcftools
REF=/data/genom-nimo/reference_chr.fa
MAX_PARALLEL=12
TMPDIR=$(mktemp -d /scratch/simple-genomics/cram2vcf.XXXXXX)

trap 'rm -rf "$TMPDIR"' EXIT

CHROMS=(chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10
        chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20
        chr21 chr22 chrX chrY chrM)

echo "[$(date +%T)] Starting genome-wide variant calling from $(basename "$CRAM")"
echo "[$(date +%T)] Output: $OUT"
echo "[$(date +%T)] Temp dir: $TMPDIR"
echo "[$(date +%T)] Parallelism: $MAX_PARALLEL chromosomes"

call_chrom() {
    local chrom=$1
    local slice="$TMPDIR/${chrom}.bam"
    local vcf="$TMPDIR/${chrom}.vcf.gz"

    echo "[$(date +%T)] Starting $chrom..."

    # Extract reads for this chromosome
    $SAMTOOLS view --input-fmt-option ignore_md5=1 \
        -T "$REF" -b -o "$slice" "$CRAM" "$chrom" 2>/dev/null

    if [[ ! -s "$slice" ]]; then
        echo "[$(date +%T)] $chrom: empty slice, skipping"
        return 0
    fi

    $SAMTOOLS index "$slice" 2>/dev/null

    # mpileup → call (piped to avoid intermediate BCF)
    $BCFTOOLS mpileup -f "$REF" \
        --max-depth 250 -q 20 -Q 20 \
        -a FORMAT/AD,FORMAT/DP \
        -Ou "$slice" 2>/dev/null | \
    $BCFTOOLS call -mv -Oz -o "$vcf" 2>/dev/null

    $BCFTOOLS index -t "$vcf" 2>/dev/null

    # Clean up slice BAM to save disk
    rm -f "$slice" "${slice}.bai"

    local n=$($BCFTOOLS view -H "$vcf" 2>/dev/null | wc -l)
    echo "[$(date +%T)] $chrom done: $n variants"
}

export -f call_chrom
export SAMTOOLS BCFTOOLS REF CRAM TMPDIR

# Run chromosomes in parallel
printf '%s\n' "${CHROMS[@]}" | xargs -P "$MAX_PARALLEL" -I{} bash -c 'call_chrom "$@"' _ {}

echo "[$(date +%T)] All chromosomes done. Concatenating..."

# Collect per-chrom VCFs in order
VCF_LIST=()
for chrom in "${CHROMS[@]}"; do
    vcf="$TMPDIR/${chrom}.vcf.gz"
    if [[ -s "$vcf" ]]; then
        VCF_LIST+=("$vcf")
    fi
done

mkdir -p "$(dirname "$OUT")"
$BCFTOOLS concat -a -Oz -o "$OUT" "${VCF_LIST[@]}"
$BCFTOOLS index -t "$OUT"

echo "[$(date +%T)] Done! Output: $OUT ($(du -h "$OUT" | cut -f1))"
echo "[$(date +%T)] Variants: $($BCFTOOLS view -H "$OUT" | wc -l)"
