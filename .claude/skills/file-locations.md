# File Locations & Storage

## BAM/CRAM Files — `/data/aligned_bams/`

Whole-genome sequencing alignment files (GRCh38). Scan with:
```bash
ls -lh /data/aligned_bams/*.{bam,cram} 2>/dev/null
```

Or via API: `curl -s http://localhost:8600/api/files/scan | python3 -m json.tool`

## Reference Genomes

The reference genome is auto-detected from common paths:
- `/data/refs/GRCh38.fa` (standard location after setup)
- `/data/refs/GRCh38.fa` (legacy)
- Must have `.fai` index (from `samtools faidx`)
- BWA index (`.bwt`, `.pac`, `.sa`) needed for FASTQ alignment

Check: `python3 -c "from backend.config import DEFAULT_REFERENCE_GRCH38; print(DEFAULT_REFERENCE_GRCH38)"`

## VCF/gVCF Output — `/scratch/nimog_output/`

DeepVariant and bcftools outputs organized by nimog run ID:
```
/scratch/nimog_output/<run_id>/dv/<Sample>.vcf.gz
/scratch/nimog_output/<run_id>/dv/<Sample>.g.vcf.gz
/scratch/nimog_output/<run_id>/final.vcf.gz
```

## 1000 Genomes Reference Panel — `/data/pgs2/ref_panel/`

```
GRCh38: /data/pgs2/ref_panel/GRCh38_1000G_ALL (.pgen/.psam/.pvar.zst)
GRCh37: /data/pgs2/ref_panel/GRCh37_1000G_ALL (.pgen/.psam/.pvar.zst)
```
3,202 samples. Used for ancestry estimation during PGS scoring.

## PGS Cache — `/data/pgs_cache/{PGS_ID}/`

Downloaded harmonized scoring files from PGS Catalog. See `pgs-data-inventory.md`.

## Storage Layout

```
/data/                          # Persistent storage (configured via GENOMICS_DATA_DIR)
  aligned_bams/                 # WGS BAM/CRAM files
  refs/                         # Reference genomes (GRCh38.fa, GRCh37.fa)
  pgs_cache/                    # PGS scoring files
  pgs2/ref_panel/               # 1000G reference panel
  runs/                         # Scoring run results
  vcfs/                         # Registered VCFs
  uploads/                      # User uploads
  app/db.sqlite                 # SQLite database
  app/server_config.json        # Auto-detected hardware config
  containers/                   # Apptainer SIF images

/scratch/                       # Fast ephemeral storage (configured via GENOMICS_SCRATCH_DIR)
  nimog_output/                 # DeepVariant pipeline outputs
  runs/                         # Fast-tier scoring results
  pipeline/                     # Pipeline intermediates
  alignments/                   # FASTQ alignment output
  tmp/                          # Temporary files
```

## Database

SQLite at `/data/app/db.sqlite`. Tables: ScoringRun, RunResult, VCF, PGSCacheEntry, GenomicFile, User.
