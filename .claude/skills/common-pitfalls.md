# Common Pitfalls

## gVCF Block Records
Block records have REF allele only at block start. To get the actual ref base at any position within a block:
```bash
samtools faidx /data/refs/GRCh38.fa chr1:12345-12345
```

## Palindromic Variants
A/T, T/A, C/G, G/C pairs are **excluded** from Pipeline E+ (BAM) scoring — they're strand-ambiguous. This is expected.

## Genome Build Detection
Detect by contig lengths, NOT "chr" prefix:
- GRCh38: chr1 = 248,956,422 bp
- GRCh37: chr1 = 249,250,621 bp

## DeepVariant with Apptainer
Use `apptainer` (not `singularity`). Add `--nv` for GPU. Explicit bind mounts required:
```bash
apptainer run --nv -B /data:/data -B /scratch:/scratch -B $HOME:$HOME \
  /data/containers/deepvariant_1.6.1-gpu.sif ...
```

## DeepVariant 3-Stage Pipeline
1. **make_examples** (CPU-only) — creates tf.Examples from BAM
2. **call_variants** (GPU) — neural network inference
3. **postprocess_variants** (CPU) — produces final VCF

GPU utilization is 0% during stages 1 and 3 — this is normal.

## Match Rate Issues
- Low (<50%): genome build mismatch between VCF and PGS file
- 0%: PGS file might be GRCh37 while VCF is GRCh38
- ~100% only with gVCF (includes all positions, not just variants)

## Extreme Percentiles
If getting 0% or 100% with absurd Z-scores:
- Check frequency source — fallback (0.5) produces unreliable Z-scores
- Use `freq_source: "1kg_plink2"` for real population frequencies

## Stuck/Orphaned Runs
If a run shows "scoring" after server restart, it was orphaned. The app auto-cleans on startup.

## Chromosome Naming
Both `chr1` and `1` formats are handled automatically.

## PGS File Format
Trailing empty columns are valid. Header lines start with `#`.
