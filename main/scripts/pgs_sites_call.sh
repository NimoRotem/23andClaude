#!/usr/bin/env bash
# Call variants (including hom-ref sites) at PGS positions from a CRAM.
# This fills in the hom-ref gaps that bcftools call -mv drops from the main
# genome-wide VCF, bringing PGS match rates from ~50% up to ~99%.
#
# Usage: bash pgs_sites_call.sh <cram> <positions_tsv> <output_vcf_gz>
# positions_tsv format: chr<tab>pos (1-based, chr-prefixed)

set -euo pipefail

CRAM="${1:?usage: $0 <cram> <positions_tsv> <output_vcf_gz>}"
POSITIONS="${2:?usage: $0 <cram> <positions_tsv> <output_vcf_gz>}"
OUT="${3:?usage: $0 <cram> <positions_tsv> <output_vcf_gz>}"

SAMTOOLS=/home/nimrod_rotem/conda/envs/genomics/bin/samtools
BCFTOOLS=/home/nimrod_rotem/conda/envs/genomics/bin/bcftools
REF=/data/genom-nimo/reference_chr.fa
MAX_PARALLEL=12
TMPDIR=$(mktemp -d /scratch/simple-genomics/pgscall.XXXXXX)

trap 'rm -rf "$TMPDIR"' EXIT

CHROMS=(chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10
        chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20
        chr21 chr22 chrX chrY chrM)

echo "[$(date +%T)] PGS-sites calling from $(basename "$CRAM")"
echo "[$(date +%T)] Input positions: $(wc -l <"$POSITIONS")"
echo "[$(date +%T)] Output: $OUT"

# Split positions by chromosome
mkdir -p "$TMPDIR/pos"
for c in "${CHROMS[@]}"; do
    awk -v c="$c" '$1 == c {print $1"\t"$2}' "$POSITIONS" > "$TMPDIR/pos/${c}.tsv"
done

call_chrom() {
    local chrom=$1
    local pos_tsv="$TMPDIR/pos/${chrom}.tsv"
    local slice="$TMPDIR/${chrom}.bam"
    local vcf="$TMPDIR/${chrom}.vcf.gz"

    if [[ ! -s "$pos_tsv" ]]; then
        echo "[$(date +%T)] $chrom: no positions, skipping"
        return 0
    fi

    local n=$(wc -l <"$pos_tsv")
    echo "[$(date +%T)] Starting $chrom ($n positions)..."

    $SAMTOOLS view --input-fmt-option ignore_md5=1 \
        -T "$REF" -b -o "$slice" "$CRAM" "$chrom" 2>/dev/null

    if [[ ! -s "$slice" ]]; then
        echo "[$(date +%T)] $chrom: empty slice"
        return 0
    fi

    $SAMTOOLS index "$slice" 2>/dev/null

    # Note: bcftools mpileup -R requires bgzipped+tabixed region file, but a
    # simple TSV also works when sorted. Use -T instead for plain TSV.
    # Use bcftools call -m WITHOUT -v so hom-ref sites are emitted.
    $BCFTOOLS mpileup -f "$REF" \
        -T "$pos_tsv" \
        --max-depth 250 -q 20 -Q 20 \
        -a FORMAT/AD,FORMAT/DP \
        -Ou "$slice" 2>/dev/null | \
    $BCFTOOLS call -m -Oz -o "$vcf" 2>/dev/null

    $BCFTOOLS index -t "$vcf" 2>/dev/null

    rm -f "$slice" "${slice}.bai"

    local nv=$($BCFTOOLS view -H "$vcf" 2>/dev/null | wc -l)
    echo "[$(date +%T)] $chrom done: $nv records"
}

export -f call_chrom
export SAMTOOLS BCFTOOLS REF CRAM TMPDIR

printf '%s\n' "${CHROMS[@]}" | xargs -P "$MAX_PARALLEL" -I{} bash -c 'call_chrom "$@"' _ {}

echo "[$(date +%T)] Concatenating..."

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
echo "[$(date +%T)] Records: $($BCFTOOLS view -H "$OUT" | wc -l)"
