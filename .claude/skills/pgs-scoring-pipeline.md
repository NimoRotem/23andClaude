# PGS Scoring Pipeline

## Quick Start (API)

### 1. Search for PGS scores
```bash
curl "http://localhost:8600/api/pgs/search?q=type+2+diabetes&limit=10"
```

### 2. Download scoring files
```bash
curl -X POST http://localhost:8600/api/pgs/download \
  -H "Content-Type: application/json" \
  -d '{"pgs_ids": ["PGS000025"], "build": "GRCh38"}'
```

### 3. Create a scoring run
```bash
curl -X POST http://localhost:8600/api/runs/ \
  -H "Content-Type: application/json" \
  -d '{
    "source_files": [{"type": "bam", "path": "/data/aligned_bams/SampleA.bam"}],
    "pgs_ids": ["PGS000025"],
    "engine": "auto",
    "ref_population": "EUR"
  }'
```

Source file types:
- `{"type": "bam", "path": "/absolute/path.bam"}` — BAM file
- `{"type": "vcf", "vcf_id": "<uuid>"}` — registered VCF
- `{"type": "gvcf", "vcf_id": "<uuid>"}` — registered gVCF

### 4. Monitor and get results
```bash
curl http://localhost:8600/api/runs/{run_id}
curl http://localhost:8600/api/runs/{run_id}/results
```

## Recipes

**Score all family for height:**
```bash
curl -X POST http://localhost:8600/api/runs/ -H "Content-Type: application/json" \
  -d '{"source_files": [
    {"type": "bam", "path": "/data/aligned_bams/SampleA.bam"},
    {"type": "bam", "path": "/data/aligned_bams/Efi.bam"},
    {"type": "bam", "path": "/data/aligned_bams/Mina.bam"},
    {"type": "bam", "path": "/data/aligned_bams/SampleB.bam"},
    {"type": "bam", "path": "/data/aligned_bams/B2XH.bam"},
    {"type": "bam", "path": "/data/aligned_bams/B3XH.bam"}
  ], "pgs_ids": ["PGS000297"]}'
```

## Scoring Architecture

**Pipeline E+** (`pipeline_e_plus.py`): BAM-direct scoring via pysam pileup. Reads BAM at each variant position. Excludes palindromic variants (A/T, C/G). Dosage thresholds: <0.15=hom-ref, 0.15-0.85=het, >0.85=hom-alt. Min depth: 10 reads.

**VCF-based** (`engine.py`): Extracts genotypes via bcftools. Handles gVCF blocks. Mean imputation for missing positions.

**Frequency priority (auto):** PGS file → 1000G (plink2) → VCF AF → fallback.

**Per-file reference population:** Each source file can override `ref_population`. Available: EUR, EAS, AFR, SAS, AMR, MULTI.

## Manual plink2 Scoring

```bash
conda activate genomics
plink2 \
  --pfile /data/pgs2/ref_panel/GRCh38_1000G_ALL \
  --score /data/pgs_cache/PGS000025/PGS000025_hmPOS_GRCh38.txt.gz \
    header-read cols=+scoresums \
  --score-col-nums 1 \
  --out /scratch/tmp/pgs_output
```
