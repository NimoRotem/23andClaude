# simple-genomics

Genomic analysis server providing polygenic risk scores (PGS), monogenic variant screening, pharmacogenomics, ancestry analysis, and sample QC.

## First-Time Install

### System Prerequisites

```bash
# On Debian/Ubuntu (run as root or with sudo)
apt-get update
apt-get install -y build-essential git wget unzip default-jre-headless \
    libcurl4-openssl-dev libbz2-dev liblzma-dev zlib1g-dev libdeflate-dev
```

### Python Environment

```bash
# Create conda environment (if not already present)
conda create -n genomics python=3.11 -y
conda activate genomics

# Install Python dependencies
pip install -r requirements.txt

# Additional: pyliftover (for haplogroup data build)
pip install pyliftover
```

### Bioinformatics Tools

```bash
# Install via conda (recommended)
conda install -c bioconda bcftools=1.22 samtools=1.19 plink2=2.00a6 plink=1.90 -y
```

### Data Dependencies

```bash
# Install all external data (ClinVar, haplogroup refs, T1K, HaploGrep3)
bash scripts/setup_data.sh --all
```

### Manual Prerequisites (must be set up separately)

These large files must be present before starting:

1. **Reference FASTA**: `/data/refs/hs38DH.fa` (GRCh38, chr-prefixed) + `.fai` index
2. **1000G reference panel** (plink2 format):
   - `/data/pgs2/ref_panel/GRCh38_1000G_ALL.pgen`
   - `/data/pgs2/ref_panel/GRCh38_1000G_ALL.psam`
   - `/data/pgs2/ref_panel/GRCh38_1000G_ALL.pvar.zst`
3. **Precomputed PGS reference stats**: `/data/pgs2/ref_panel_stats/*.json`
   - Build with: `python scripts/build_ref_panel_stats.py`

### Start the Server

```bash
sudo supervisorctl restart simple-genomics
```

## Quick Start (existing install)

```bash
# Install/update data dependencies
bash scripts/setup_data.sh --all

# Restart server
sudo supervisorctl restart simple-genomics
```

## Architecture

- **Server**: FastAPI (Python), single-file `app.py` + `runners.py` scoring engine
- **Port**: 8800 (proxied by nginx at 23andclaude.com root `/`)
- **Process**: supervisor program `simple-genomics`
- **Python**: `/home/nimo/miniconda3/envs/genomics/bin/python`

## File Preparation (pgen Cache)

Before running Polygenic Risk Score (PGS) or PCA tests, input files must be converted to plink2's binary format (pgen). This "preparation" step builds a variant index that dramatically speeds up all subsequent scoring.

### Accepted Input Formats

| Format | Extension | Preparation Time | Notes |
|--------|-----------|-----------------|-------|
| **gVCF** | `.g.vcf.gz` | 5–15 minutes | Recommended. Contains reference blocks — normalized per-chromosome during preparation. Highest accuracy for PGS scoring. |
| **VCF** | `.vcf.gz` | 5–30 seconds | Fast to prepare. Contains only variant sites (no ref blocks). Good for quick scoring. |
| **BAM** | `.bam` | No prep needed | Variant calling done per-test. Slower per-test (~1 min) but no upfront preparation. |
| **CRAM** | `.cram` | No prep needed | Same as BAM. Requires reference FASTA for decoding. |

### How Preparation Works

1. **VCF files**: Directly imported to pgen via `plink2 --make-pgen` (~5 seconds).
2. **gVCF files**: Reference blocks (`<NON_REF>` / `<*>` ALT alleles) are expanded at all positions needed by PGS scoring files and PCA. This normalization rewrites placeholder ALTs to actual alleles using a precomputed allele map (277 MB, covering all PGS + PCA positions). The 22 autosomes are processed in parallel (~16 workers), then merged and imported to pgen.
3. **BAM/CRAM files**: No preparation needed — variant calling is performed on-demand per test using `bcftools mpileup` at target positions.

### When Preparation Happens

- **Automatic**: Triggered immediately after file upload or registration. Runs in a background thread.
- **Manual**: Click the "Prepare" button in **My Data** for any file showing "Needs prep" status.
- **Status badges**:
  - Green "Ready" — file is prepared and available for scoring
  - Yellow "Preparing..." — build is in progress (pulsing animation)
  - Red "Needs prep" — not yet prepared; click Prepare to start

### File Visibility

- **Test dropdown** (header): Only shows files that are **Ready** or don't need prep (BAM/CRAM). If no files are ready, a link to "My Data" is shown.
- **My Data view**: Shows **all** registered files with their preparation status and a Prepare button where needed.

### Accuracy and Speed Trade-offs

| Input Type | PGS Test Speed | Accuracy | Best For |
|-----------|---------------|----------|----------|
| gVCF | ~5 sec/test (after prep) | Highest — ref-block positions correctly handled as hom-ref | Production scoring |
| VCF | ~5 sec/test (after prep) | Good — variant-only sites matched | Quick results |
| BAM/CRAM | ~60 sec/test (no prep) | Good — variant calling per-test at PGS positions | When VCF not available |

### Converting BAM to gVCF

If you have BAM/CRAM files and want the highest accuracy + fastest per-test speed:

1. Run DeepVariant (recommended) or GATK HaplotypeCaller to produce a gVCF:
   ```bash
   # DeepVariant (via Docker or local install)
   run_deepvariant \
     --model_type=WGS \
     --ref=/data/refs/hs38DH.fa \
     --reads=sample.bam \
     --output_vcf=sample.vcf.gz \
     --output_gvcf=sample.g.vcf.gz \
     --num_shards=16
   ```
2. Register the `.g.vcf.gz` in the app (upload or add path).
3. Preparation will auto-trigger. Once status shows "Ready", the file appears in the test dropdown.

**Time estimates for BAM→gVCF conversion**: ~30–60 minutes for a 30x WGS BAM on 16 cores (DeepVariant). This is a one-time cost that pays off in much faster and more accurate PGS scoring afterward.

### Cache Location

Prepared pgen files are stored at `/data/pgen_cache/sg/<hash>/sample.{pgen,pvar,psam}`. The cache is keyed by the file's realpath and a schema version, so renaming or moving the source file invalidates the cache. Cache files are permanent and survive server restarts.

## Genome Build Validation

Before running plink2 scoring, the pipeline validates that the input VCF's genome build matches the reference panel (GRCh38). This prevents silent coordinate misalignment that would corrupt PGS results without any obvious error.

### Validation Steps

1. **Header metadata extraction** — Parses `##reference` and `##contig` lines from the VCF header looking for explicit build declarations (GRCh38, GRCh37, hg19, hg38, etc.).

2. **Cross-check against reference panel** — If the VCF declares a build that doesn't match the PGS scoring file's build (e.g., VCF is hg19 but scoring file expects GRCh38), the pipeline **FAILs immediately** with a clear error message. If the build is undeclared, a **WARN** is issued.

3. **Spot-check variant validation** — Uses rs7412 (APOE e2 SNP, chr19) as a sentinel:
   - GRCh38 expected position: `chr19:44908822`
   - GRCh37 expected position: `chr19:45412079`
   - If the variant is found at the wrong build's coordinate, the pipeline **FAILs**.
   - If the variant is absent (e.g., targeted panel data), validation passes with a note.

### Outcomes

| Status | Meaning | Action |
|--------|---------|--------|
| **PASS** | Build confirmed compatible | Scoring proceeds |
| **WARN** | Build undeclared, spot-check inconclusive | Scoring proceeds with caution |
| **FAIL** | Build mismatch detected | Scoring **blocked** — returns error |

### Audit Log

Every validation result is logged to `/scratch/simple-genomics/build_validation.log` as newline-delimited JSON with:
- Timestamp, VCF path, detected build, reference build
- Spot-check result (found position vs expected)
- PASS/WARN/FAIL status and message

### Design Principle

**Fail loudly rather than silently.** A coordinate mismatch will silently score wrong variants, corrupting results without obvious error signals. The pipeline blocks scoring when a mismatch is detected rather than producing misleading results.

## Test Categories

### Polygenic Risk Scores (PGS)
- **Runner**: `run_pgs_score()` in `runners.py`
- **Method**: plink2 `--score` against PGS Catalog harmonized scoring files
- **Data**: `/data/pgs_cache/` (scoring files), `/data/pgen_cache/sg/` (VCF→pgen cache)
- **Reference panel**: `/data/pgs2/ref_panel/GRCh38_1000G_ALL` (1000 Genomes Phase 3)
- **Percentile stats**: `/data/pgs2/ref_panel_stats/` (precomputed EUR distribution)
- **Fast path**: For gVCF + small PGS (≤500 variants), bypasses full pgen build (~5s vs ~15min)
- **Percentile method**: Precomputed stats preferred (reliable); dynamic scoring used as validation/fallback
- **Sanity gates**: |z|>6 fails, |z|>4 warns, std collapse detection, percentile capped at [0.5, 99.5]
- **Input**: VCF, gVCF, BAM, CRAM

### Monogenic (ClinVar Screening)
- **Runner**: `run_clinvar_screen()` in `runners.py`
- **Method**: bcftools annotate with ClinVar VCF → query for Pathogenic/Likely_pathogenic
- **Data**: `/data/clinvar/clinvar.vcf.gz` (bare chrom), `/data/clinvar/clinvar_chr.vcf.gz` (chr-prefixed)
- **Cache**: `/data/pgen_cache/clinvar_annotated/` (annotated VCF cache)
- **Panels**: ACMG SF v3.3 — Cancer predisposition, Cardiovascular, Metabolism, Misc
- **Input**: VCF, gVCF (auto-annotated on first run)

### Pharmacogenomics (PGx)
- **Runner**: `run_variant_lookup()` (most genes) or `run_specialized(method='pgx')` (star alleles)
- **Method**: bcftools query for specific rsIDs with position fallback
- **Genes**: CYP2D6, CYP2C19, CYP2C9, VKORC1, DPYD, TPMT, NUDT15, SLCO1B1, HLA-B, UGT1A1, G6PD, etc.
- **Data**: Built-in `rs_positions.py` (curated GRCh38 coordinates, no external files)
- **Note**: Star allele calling (CYP2D6 *3/*4/*5 etc.) requires PharmCAT + BAM — currently returns warning
- **Input**: VCF, gVCF

### Ancestry
- **PCA**: `_run_pca_1000g()` — projects sample onto 1000G PC space
  - Data: `/data/pgs_cache/pca_1000g/ref.eigenvec.allele` (106K pruned sites)
  - For BAM/CRAM: derives VCF at PCA positions on demand (cached)
- **ADMIXTURE**: `_run_admixture_from_pca()` — K=5 super-population estimates from PCA
- **Y-DNA haplogroup**: `_run_y_haplogroup()` — ISOGG SNP panel
  - Data: `/data/haplogroup_data/ydna_snps_grch38.json`
- **mtDNA haplogroup**: `_run_mt_haplogroup()` — HaploGrep3 classification
  - Data: `/data/haplogroup_data/mtdna_snps.json`
  - Tool: `/home/nimrod_rotem/tools/haplogrep3/haplogrep3` (requires Java 11+)
- **Neanderthal %**: `_run_neanderthal()` — population-based estimate from PCA
  - Data: `/data/haplogroup_data/neanderthal_snps_grch38.json`
- **ROH**: `_run_roh()` — plink `--homozyg` for consanguinity estimate
- **HLA typing**: `_run_hla_typing()` — T1K genotyper
  - Tool: `/home/nimo/miniconda3/envs/genomics/bin/run-t1k`
  - Data: `/data/t1k_ref/hla/{hla_dna_seq.fa, hla_dna_coord.fa}`
  - Input: BAM/CRAM only (extracts MHC region reads)

### Sample QC
- **Runner**: `run_vcf_stats()` in `runners.py`
- **Tests**: Ti/Tv ratio, Het/Hom ratio, SNP count, Indel count
- **Method**: `bcftools stats` parsing
- **Input**: VCF, gVCF, BAM, CRAM

### Sex Check
- **Tests**: Y-chromosome reads, SRY gene, X:Y ratio, Het rate chrX, chrY variant count
- **Method**: samtools idxstats + bcftools query
- **Input**: BAM, CRAM (some tests also work on VCF)

## External Data Dependencies

| Component | Path | Source | Install |
|-----------|------|--------|---------|
| ClinVar VCF | `/data/clinvar/clinvar.vcf.gz` | NCBI FTP | `setup_data.sh --clinvar` |
| ClinVar VCF (chr) | `/data/clinvar/clinvar_chr.vcf.gz` | Generated from above | `setup_data.sh --clinvar` |
| Y-DNA SNPs | `/data/haplogroup_data/ydna_snps_grch38.json` | ISOGG 2016 + liftover | `setup_data.sh --haplogroups` |
| mtDNA markers | `/data/haplogroup_data/mtdna_snps.json` | PhyloTree Build 17 | `setup_data.sh --haplogroups` |
| Neanderthal SNVs | `/data/haplogroup_data/neanderthal_snps_grch38.json` | Curated panel | `setup_data.sh --haplogroups` |
| T1K binary | `/home/nimo/miniconda3/envs/genomics/bin/run-t1k` | GitHub source | `setup_data.sh --t1k` |
| HLA reference | `/data/t1k_ref/hla/hla_dna_{seq,coord}.fa` | IPD-IMGT/HLA + GENCODE | `setup_data.sh --t1k` |
| HaploGrep3 | `/home/nimrod_rotem/tools/haplogrep3/haplogrep3` | GitHub release | `setup_data.sh --haplogrep3` |
| Reference FASTA | `/data/refs/GRCh38.fa` → `hs38DH.fa` | Prerequisite | Manual |
| 1000G ref panel | `/data/pgs2/ref_panel/GRCh38_1000G_ALL.{pgen,psam,pvar.zst}` | Prerequisite | Manual |
| Ref panel stats | `/data/pgs2/ref_panel_stats/{PGS*}_EUR_GRCh38.json` | `scripts/build_ref_panel_stats.py` | Manual |
| PGS scoring files | `/data/pgs_cache/PGS*/` | PGS Catalog | Auto-downloaded on demand |

## Tools Required

| Tool | Path | Version |
|------|------|---------|
| bcftools | `/home/nimo/miniconda3/envs/genomics/bin/bcftools` | 1.22+ |
| samtools | `/home/nimo/miniconda3/envs/genomics/bin/samtools` | 1.19+ |
| plink2 | `/home/nimo/miniconda3/envs/genomics/bin/plink2` | 2.00+ |
| plink | `/home/nimo/miniconda3/envs/genomics/bin/plink` | 1.90+ |
| Java | system | 11+ (for HaploGrep3) |

## Updating ClinVar

ClinVar is updated weekly. To refresh:

```bash
cd /data/clinvar
rm -f clinvar.vcf.gz clinvar_chr.vcf.gz clinvar.vcf.gz.tbi clinvar_chr.vcf.gz.tbi
bash /home/nimrod_rotem/simple-genomics/scripts/setup_data.sh --clinvar
# Clear the annotation cache so files get re-annotated
rm -rf /data/pgen_cache/clinvar_annotated/*
sudo supervisorctl restart simple-genomics
```

## File Layout

```
/home/nimrod_rotem/simple-genomics/
├── app.py                  # FastAPI server
├── runners.py              # All scoring/analysis logic (~5000 lines)
├── test_registry.py        # Test definitions (IDs, categories, params)
├── rs_positions.py         # Curated rsID → GRCh38 position map
├── rsid_list_pgs.py        # PGS-associated rsID positions
├── rsid_list_positions.py  # Extended rsID position database
├── chat.py                 # AI chat integration
├── requirements.txt        # Python dependencies
├── scripts/
│   ├── setup_data.sh       # Data dependency installer
│   ├── build_haplogroup_data.py
│   └── build_ref_panel_stats.py
├── cram_vcf_cache/         # On-demand CRAM→VCF conversions
└── ref_cache/              # Reference file caches
```

## PGx Star-Allele Callers

### Cyrius (CYP2D6)

Cyrius is used for CYP2D6 star-allele calling from BAM/CRAM input. It handles
the CYP2D7 pseudogene complexity, structural variants (*5 deletion, *13/*68
hybrids), and gene duplications (*2xN) that generic variant callers miss.

**Installation:**
```bash
sudo git clone https://github.com/Illumina/Cyrius.git /opt/cyrius
pip3 install --break-system-packages pysam scipy statsmodels
```

**Dependencies:** pysam, scipy, statsmodels, numpy, pandas

**Integration:** When a CYP2D6 test runs on BAM input, the system attempts
Cyrius first. If Cyrius is unavailable or fails, it falls back to Pipeline E+
pileup genotyping with allele verification and impossible-diplotype detection.

### Allele Verification

All variant_lookup tests now verify REF/ALT alleles match the expected
star-allele-defining change. A position-only match without matching alleles
is reported as `locus_mismatch` (not as a positive star-allele call).


### ExpansionHunter (Repeat Expansions)

ExpansionHunter v5.0.0 is used to call trinucleotide repeat expansions
(FMR1/Fragile X, HTT/Huntington's, DMPK/Myotonic Dystrophy) from BAM/CRAM.
These expansions are not detectable from VCF — they require read-level analysis.

**Installation:**
```bash
wget https://github.com/Illumina/ExpansionHunter/releases/download/v5.0.0/ExpansionHunter-v5.0.0-linux_x86_64.tar.gz
tar xzf ExpansionHunter-v5.0.0-linux_x86_64.tar.gz
sudo cp ExpansionHunter-v5.0.0-linux_x86_64/bin/ExpansionHunter /usr/local/bin/
sudo mkdir -p /opt/expansion-hunter
sudo cp -r ExpansionHunter-v5.0.0-linux_x86_64/variant_catalog /opt/expansion-hunter/
```

**Supported loci:** FMR1 (CGG), HTT (CAG), DMPK (CTG) — extensible via
the standard Illumina variant catalog at `/opt/expansion-hunter/variant_catalog/`.

**Integration:** When `carrier_fragx` (or any `repeat_expansion` method) runs
on BAM input, ExpansionHunter is invoked with a single-locus catalog.
The result includes per-allele repeat counts and clinical classification
(Normal / Intermediate / Premutation / Full mutation).

## gVCF Reference Block Handling

When querying a gVCF, reference blocks (ALT = `<*>` or `<NON_REF>`, GT = 0/0)
are correctly recognized as homozygous reference. Previously, the symbolic ALT
allele was compared against the expected variant allele and reported as
"inconclusive" (locus_mismatch). The fix:

- Symbolic ALTs (`<*>`, `<NON_REF>`) with homref GT → return 0/0 (ref/ref)
- Position-within-block detection: record POS ≤ query ≤ INFO/END → ref/ref
- Real variant records with non-matching alleles still correctly reported as locus_mismatch


### ExpansionHunter (Repeat Expansions)

ExpansionHunter v5.0.0 is used to call trinucleotide repeat expansions
(FMR1/Fragile X, HTT/Huntington's, DMPK/Myotonic Dystrophy) from BAM/CRAM.
These expansions are not detectable from VCF -- they require read-level analysis.

**Installation:**
```bash
wget https://github.com/Illumina/ExpansionHunter/releases/download/v5.0.0/ExpansionHunter-v5.0.0-linux_x86_64.tar.gz
tar xzf ExpansionHunter-v5.0.0-linux_x86_64.tar.gz
sudo cp ExpansionHunter-v5.0.0-linux_x86_64/bin/ExpansionHunter /usr/local/bin/
sudo mkdir -p /opt/expansion-hunter
sudo cp -r ExpansionHunter-v5.0.0-linux_x86_64/variant_catalog /opt/expansion-hunter/
```

**Supported loci:** FMR1 (CGG), HTT (CAG), DMPK (CTG) -- extensible via
the standard Illumina variant catalog at `/opt/expansion-hunter/variant_catalog/`.

**Integration:** When `carrier_fragx` (or any `repeat_expansion` method) runs
on BAM input, ExpansionHunter is invoked with a single-locus catalog.
The result includes per-allele repeat counts and clinical classification
(Normal / Intermediate / Premutation / Full mutation).

## gVCF Reference Block Handling

When querying a gVCF, reference blocks (ALT = `<*>` or `<NON_REF>`, GT = 0/0)
are correctly recognized as homozygous reference. Previously, the symbolic ALT
allele was compared against the expected variant allele and reported as
"inconclusive" (locus_mismatch). The fix:

- Symbolic ALTs (`<*>`, `<NON_REF>`) with homref GT -> return 0/0 (ref/ref)
- Position-within-block detection: record POS <= query <= INFO/END -> ref/ref
- Real variant records with non-matching alleles still correctly reported as locus_mismatch
