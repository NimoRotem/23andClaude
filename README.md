# simple-genomics

**A genomics analysis server that turns raw DNA files into something Claude can actually read.**

---

## What this is, and why it exists

Your genome lives in files like **BAM**, **CRAM**, **VCF**, and **gVCF** — formats that are enormous, semi-binary, and completely opaque to AI assistants. A whole-genome BAM is 50–100 GB of compressed read alignments. A gVCF packs tens of millions of variant calls into a layout optimized for downstream bioinformatics tools, not human (or LLM) readers. Claude, ChatGPT, and every other chat-based AI simply cannot open these files, parse them, or reason about their contents directly.

**simple-genomics closes that gap.** It runs a full stack of battle-tested bioinformatics pipelines — plink2, bcftools, samtools, Cyrius, ExpansionHunter, HaploGrep3, T1K, DeepVariant — on your genomic files, and distills the results into clean, structured markdown that Claude *can* read. You get the scientific rigor of the real tools with the conversational power of an LLM on top.

Think of it as a **genomics interpreter for your AI**: the pipelines do the heavy lifting, and Claude does the reasoning, explaining, and cross-referencing against the literature.

## Why this approach beats the alternatives

**Generic LLMs hallucinate about variants they can't see.** Ask Claude "what does my sequencing file say about my APOE status?" with no pipeline underneath, and you'll get confident-sounding nonsense. There is no way for a language model to scan a 20 GB BAM and know whether rs429358 is C/C, C/T, or T/T.

**Consumer genomics services are black boxes.** 23andMe, Promethease, and similar platforms give you fixed reports. You can't ask follow-up questions, you can't inspect the confidence of a call, and you can't drill into edge cases like star-allele ambiguity or structural variants.

**Raw bioinformatics tools are unforgiving, and small mistakes corrupt results silently.** plink2, bcftools, and friends will happily run on the wrong inputs and produce clean-looking, totally wrong numbers. This isn't a skill problem — it's a reliability problem. Both humans and LLM agents make the same classes of subtle mistakes when driving these tools directly:

- Scoring a **GRCh37** VCF against a **GRCh38** scoring file — coordinates don't line up, nothing errors out, the number is meaningless.
- **Lifting coordinates between builds** incorrectly — strand flips on ambiguous SNPs, variants that silently drop out, positions that land on the wrong allele.
- Running PGS without a **matched reference population** — a raw score with no percentile is nearly useless, because the meaningful question is always "where does this sit in the distribution?"
- **Skipping allele verification** at star-allele positions — a position hit with the wrong REF/ALT gets counted as a positive call when it shouldn't.
- Treating a **gVCF reference block** (`<NON_REF>`) as a missing call instead of hom-ref — quietly dropping every hom-ref variant the scoring file needs.
- Calling CYP2D6 with a **generic variant caller** — missing `*5` deletions, `*13`/`*68` hybrids, and `*2xN` duplications because the CYP2D7 pseudogene confounds naive callers.
- Running the wrong chromosome set for a sex-specific test, forgetting to normalize multiallelic sites, feeding hg19-style `1` contigs into a `chr1`-prefixed pipeline.

When an agent tries to orchestrate these tools directly, any one of these mistakes produces confident, plausible-looking output that is quietly wrong — with no error for the agent to latch onto and self-correct from.

**Some of these pipelines are also heavy, and resource-aware orchestration matters.** gVCF reference-block expansion across every PGS and PCA position is expensive — done naively on one core it runs for half an hour. simple-genomics shards the work across chromosomes and runs the autosomes in parallel workers, cutting wall-clock time by roughly 20x on a 16-core machine. The pgen cache, precomputed reference-panel percentile stats, on-demand BAM variant calling at target loci, and the fast path for small PGS on gVCF input all exist for the same reason: do the expensive, deterministic step once, correctly, and never repeat it.

**This is the split simple-genomics enforces.** The efficient, accurate, deterministic part — pipeline orchestration, build validation, liftover, parallel sharding, QC gates, sanity checks, caching, resource-aware scheduling — belongs in this library. The interpretation part — explaining what a z-score of 2.1 for coronary artery disease actually means for *you*, comparing two variants, contextualizing a haplogroup against population history, reasoning about a pharmacogenomic combination — belongs in the AI. Each side does what it's actually good at, and the handoff between them is structured markdown rather than a 50 GB binary file.

![Clinical Genomics Master Summary — 47 underlying pipeline reports synthesized by Claude into a single narrative. The AI pulls together the MUTYH compound-heterozygous finding, CYP2D6 poor-metabolizer status, protective APOE ε2/ε3, BRCA1-negative screen, and PGS percentiles, and surfaces what's clinically actionable. None of this synthesis is possible directly from the raw BAM — it only works because the deterministic pipelines produced structured, AI-readable results first.](docs/screenshots/7.png)

## How it works

1. **You upload or register a file.** VCF, gVCF, BAM, or CRAM — all supported.
2. **The server prepares the file.** VCFs and gVCFs are converted to plink2's pgen binary format (once, cached forever). gVCF reference blocks are expanded into explicit calls at every position a downstream test might need. BAM/CRAM skip preparation — variants are called on demand at the relevant loci.
3. **You pick a test.** Polygenic risk score? ACMG monogenic screen? Ancestry and haplogroup? Star-allele pharmacogenomics? HLA typing? Repeat expansions?
4. **The right pipeline runs.** Each test category has a dedicated runner invoking the appropriate validated tool — plink2 for PGS, bcftools + ClinVar for monogenic screening, Cyrius for CYP2D6, ExpansionHunter for trinucleotide repeats, HaploGrep3 for mtDNA, T1K for HLA, and so on.
5. **Results come out as structured markdown.** Claude reads them natively and can explain, compare, and put them in context.

The server **fails loudly on things that silent pipelines would miss** — genome-build mismatches (GRCh37 vs GRCh38 is a coordinate disaster), allele mismatches at star-allele positions, and PGS z-score outliers are all explicitly flagged rather than buried in a number.

![The main test runner. 38 curated tests, 284 polygenic scores, 51 monogenic panels, 19 pharmacogenomic tests, and 16 validation checks — all organized by category and grouped by file. The header bar shows live CPU / memory / load / GPU / queue metrics so you can see when the box has room for a heavy run. Each test shows which of your registered files (BAM / VCF / gVCF) it can run against.](docs/screenshots/1.png)

![The Reports view. Every run produces a permanent record with category, score, percentile, match quality, and the source file it was computed from. This is also the structured-markdown surface that Claude reads when you ask it to interpret or compare results.](docs/screenshots/2.png)

## What it can do

### Polygenic Risk Scores (PGS)
Scores you against any PGS Catalog harmonized scoring file using plink2 against a 1000 Genomes reference panel. Percentiles are precomputed against the EUR distribution for reliability, with dynamic scoring available as a validation fallback. Sanity gates catch bad runs: `|z|>6` fails, `|z|>4` warns, standard-deviation collapse is detected, and percentiles are capped at [0.5, 99.5]. A fast path bypasses full pgen builds for gVCF + small scoring files (~5s instead of ~15min).

### Monogenic Screening (ClinVar)
Annotates your variants with the current ClinVar release and pulls out everything classified Pathogenic or Likely_pathogenic. Panels include ACMG SF v3.3 — Cancer predisposition, Cardiovascular, Metabolism, and Misc. Annotated VCFs are cached so the slow step only runs once per file.

### Pharmacogenomics (PGx)
Looks up curated star-allele-defining positions for CYP2D6, CYP2C19, CYP2C9, VKORC1, DPYD, TPMT, NUDT15, SLCO1B1, HLA-B, UGT1A1, G6PD and more. All lookups verify that REF/ALT match the expected star-allele change — a position-only hit without matching alleles is reported as `locus_mismatch`, not a false positive.

For **CYP2D6** specifically, the server uses [Cyrius](https://github.com/Illumina/Cyrius) when BAM is available. Cyrius handles the CYP2D7 pseudogene, structural variants (`*5` deletion, `*13`/`*68` hybrids), and gene duplications (`*2xN`) that generic callers miss entirely. A Pipeline E+ pileup genotyper with impossible-diplotype detection serves as fallback.

### Repeat Expansions
**ExpansionHunter v5** calls trinucleotide repeat expansions — FMR1/Fragile X (CGG), HTT/Huntington's (CAG), DMPK/Myotonic Dystrophy (CTG) — from BAM/CRAM. These are invisible to VCF: they need read-level analysis. Each result includes per-allele repeat counts and a clinical bucket (Normal / Intermediate / Premutation / Full mutation).

### Ancestry
- **PCA**: projects your sample onto 1000 Genomes PC space using ~106K pruned sites.
- **ADMIXTURE**: K=5 super-population estimates derived from the PCA projection.
- **Y-DNA haplogroup**: ISOGG SNP panel classification.
- **mtDNA haplogroup**: HaploGrep3 classification against PhyloTree Build 17.
- **Neanderthal %**: population-based estimate from PCA coordinates.
- **ROH**: runs of homozygosity via plink `--homozyg` for consanguinity estimates.
- **HLA typing**: T1K genotyper from BAM/CRAM (extracts MHC-region reads).

![Ancestry composition result — 63.4% Middle Eastern, 31.0% South European, 5.6% Central/Khoisan African. The ancestry-signatures panel pattern-matches the composition against known founder populations (Ashkenazi Jewish, Sephardic Jewish) and reports a confidence level rather than forcing a single label.](docs/screenshots/4.png)

![Inbreeding coefficient from runs of homozygosity: F_ROH = 0.22%, classified as outbred. The view breaks out total ROH (Mb), number of segments, average segment size, and whether a bottleneck signal is present, with reference bands for outbred / founder-population / endogamous / close-consanguinity profiles.](docs/screenshots/5.png)

### Sample QC and Sex Check
Ti/Tv ratio, Het/Hom ratio, SNP and indel counts from `bcftools stats`. Sex verification via Y-chromosome read counts, SRY coverage, X:Y ratio, chrX het rate, and chrY variant count.

---

## Architecture

- **Server**: FastAPI (Python). Single-file `app.py`, with `runners.py` as the ~5000-line scoring engine.
- **Port**: 8800 (proxied by nginx at `23andclaude.com`).
- **Process**: supervisor program `simple-genomics`.
- **Python**: `/home/nimo/miniconda3/envs/genomics/bin/python`.

### File layout

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

---

## Installation

### System prerequisites (Debian/Ubuntu)

```bash
apt-get update
apt-get install -y build-essential git wget unzip default-jre-headless \
    libcurl4-openssl-dev libbz2-dev liblzma-dev zlib1g-dev libdeflate-dev
```

### Python environment

```bash
conda create -n genomics python=3.11 -y
conda activate genomics
pip install -r requirements.txt
pip install pyliftover  # needed for haplogroup data build
```

### Bioinformatics tools

```bash
conda install -c bioconda bcftools=1.22 samtools=1.19 plink2=2.00a6 plink=1.90 -y
```

### Data dependencies

```bash
bash scripts/setup_data.sh --all
```

This installs ClinVar, haplogroup references, T1K, and HaploGrep3.

### Manual prerequisites

These large files must be in place before the server will start cleanly:

- **Reference FASTA**: `/data/refs/hs38DH.fa` (GRCh38, chr-prefixed) + `.fai` index
- **1000G reference panel** (plink2 format):
  - `/data/pgs2/ref_panel/GRCh38_1000G_ALL.pgen`
  - `/data/pgs2/ref_panel/GRCh38_1000G_ALL.psam`
  - `/data/pgs2/ref_panel/GRCh38_1000G_ALL.pvar.zst`
- **Precomputed PGS reference stats**: `/data/pgs2/ref_panel_stats/*.json` — build with `python scripts/build_ref_panel_stats.py`

### Start the server

```bash
sudo supervisorctl restart simple-genomics
```

### Quick start on an existing install

```bash
bash scripts/setup_data.sh --all
sudo supervisorctl restart simple-genomics
```

---

## File Preparation (the pgen cache)

Before PGS or PCA tests can run, input files are converted to plink2's binary `pgen` format. This "preparation" step builds a variant index that dramatically speeds up every subsequent scoring run.

### Accepted input formats

| Format | Extension | Preparation time | Notes |
|--------|-----------|------------------|-------|
| **gVCF** | `.g.vcf.gz` | 5–15 minutes | **Recommended.** Contains reference blocks, normalized per-chromosome during prep. Highest PGS accuracy. |
| **VCF**  | `.vcf.gz`  | 5–30 seconds   | Fast to prepare. Variant sites only (no ref blocks). Good for quick scoring. |
| **BAM**  | `.bam`     | No prep needed | Variant calling done per test (~1 min/test), no upfront cost. |
| **CRAM** | `.cram`    | No prep needed | Same as BAM. Requires the reference FASTA for decoding. |

### How preparation works

- **VCF files**: imported directly to pgen via `plink2 --make-pgen` (~5 seconds).
- **gVCF files**: reference blocks (`<NON_REF>` / `<*>` ALT alleles) are expanded at every position needed by PGS scoring files and PCA. This normalization rewrites placeholder ALTs to actual alleles using a precomputed allele map (277 MB, covering all PGS + PCA positions). The 22 autosomes are processed in parallel (~16 workers), merged, then imported to pgen.
- **BAM/CRAM**: no prep — variant calling is performed on-demand per test using `bcftools mpileup` at target positions.

### When preparation runs

- **Automatic**: triggered immediately after upload or registration, in a background thread.
- **Manual**: the "Prepare" button in My Data, for any file showing `Needs prep`.
- **Status badges**:
  - 🟢 **Ready** — prepared and available for scoring
  - 🟡 **Preparing…** — build in progress (pulsing animation)
  - 🔴 **Needs prep** — not yet prepared; click Prepare to start

### File visibility

- **Test dropdown** (header): only `Ready` files or BAM/CRAM (which skip prep) appear. If nothing is ready, a link to "My Data" is shown instead.
- **My Data view**: shows all registered files with prep status and a Prepare button.

![The My Data view. Each registered file shows format (VCF / GVCF), detected genome build (GRCh38), contig style (`chr1-style` vs `1-style`), variant count, upstream caller (e.g. DeepVariant 1.6.0), and prep status as a colored badge. Files can be added from upload, local path, or a remote URL.](docs/screenshots/3.png)

### Accuracy vs speed

| Input type | PGS test speed | Accuracy | Best for |
|------------|---------------|----------|----------|
| **gVCF**      | ~5 sec/test (after prep) | Highest — ref-block positions correctly handled as hom-ref | Production scoring |
| **VCF**       | ~5 sec/test (after prep) | Good — variant-only sites matched | Quick results |
| **BAM/CRAM**  | ~60 sec/test (no prep)   | Good — per-test calling at PGS positions | When no VCF is available |

### Converting BAM to gVCF

If you have BAM/CRAM and want the highest accuracy plus the fastest per-test speed, run DeepVariant (recommended) or GATK HaplotypeCaller to produce a gVCF. simple-genomics ships a built-in UI for this — pick a mode, pick your BAMs, and it handles the rest — or you can run DeepVariant directly on the command line.

![The in-app BAM-to-VCF pipeline picker. Quick mode runs bcftools mpileup per chromosome for fast results. Full mode runs DeepVariant + GLnexus joint genotyping — the gold-standard choice for production scoring and family studies. The picker detects available GPUs (here, a Tesla T4 with 15360 MiB) and sizes the run accordingly.](docs/screenshots/6.png)

For the command-line path:

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

Then register the `.g.vcf.gz` in the app. Preparation auto-triggers, and once it shows `Ready`, the file is usable in the test dropdown.

Rough time budget: a 30x WGS BAM takes about 30–60 minutes on 16 cores with DeepVariant. A one-time cost that pays off enormously in later scoring speed and accuracy.

### Cache location

Prepared pgen files live at `/data/pgen_cache/sg/<hash>/sample.{pgen,pvar,psam}`. The cache is keyed by the file's realpath plus a schema version — renaming or moving the source file invalidates the cache. Cache entries are permanent and survive server restarts.

---

## Genome Build Validation

Before plink2 scoring runs, the pipeline validates that the input VCF's genome build matches the reference panel (GRCh38). This prevents **silent coordinate misalignment**, which would corrupt PGS results without any obvious error.

### Validation steps

1. **Header metadata extraction** — parses `##reference` and `##contig` lines for explicit build declarations (GRCh38, GRCh37, hg19, hg38, etc.).
2. **Cross-check against reference panel** — if the VCF declares a build that doesn't match the scoring file's expected build, the pipeline **fails immediately** with a clear error. If the build is undeclared, a `WARN` is issued.
3. **Spot-check variant validation** — uses rs7412 (APOE e2 SNP, chr19) as a sentinel:
   - GRCh38 expected position: `chr19:44908822`
   - GRCh37 expected position: `chr19:45412079`
   - If the variant is found at the wrong build's coordinate → `FAIL`.
   - If absent (e.g., targeted panel) → validation passes with a note.

### Outcomes

| Status | Meaning | Action |
|--------|---------|--------|
| **PASS** | Build confirmed compatible | Scoring proceeds |
| **WARN** | Build undeclared, spot-check inconclusive | Scoring proceeds with caution |
| **FAIL** | Build mismatch detected | Scoring blocked — returns error |

### Audit log

Every validation result is logged to `/scratch/simple-genomics/build_validation.log` as newline-delimited JSON:

- Timestamp, VCF path, detected build, reference build
- Spot-check result (found position vs expected)
- PASS/WARN/FAIL status and message

### Design principle

**Fail loudly rather than silently.** A coordinate mismatch silently scores the wrong variants, corrupting results with no obvious error. The pipeline blocks scoring when a mismatch is detected rather than producing misleading numbers.

---

## Test Categories in Depth

### Polygenic Risk Scores (PGS)
- **Runner**: `run_pgs_score()` in `runners.py`
- **Method**: `plink2 --score` against PGS Catalog harmonized scoring files
- **Data**: `/data/pgs_cache/` (scoring files), `/data/pgen_cache/sg/` (VCF→pgen cache)
- **Reference panel**: `/data/pgs2/ref_panel/GRCh38_1000G_ALL` (1000 Genomes Phase 3)
- **Percentile stats**: `/data/pgs2/ref_panel_stats/` (precomputed EUR distribution)
- **Fast path**: gVCF + small PGS (≤500 variants) bypasses full pgen build (~5s vs ~15min)
- **Percentile method**: precomputed stats preferred (reliable); dynamic scoring used as validation/fallback
- **Sanity gates**: `|z|>6` fails, `|z|>4` warns, std-collapse detection, percentile capped at [0.5, 99.5]
- **Input**: VCF, gVCF, BAM, CRAM

### Monogenic (ClinVar Screening)
- **Runner**: `run_clinvar_screen()` in `runners.py`
- **Method**: `bcftools annotate` with ClinVar VCF, then query for Pathogenic/Likely_pathogenic
- **Data**: `/data/clinvar/clinvar.vcf.gz` (bare chrom), `/data/clinvar/clinvar_chr.vcf.gz` (chr-prefixed)
- **Cache**: `/data/pgen_cache/clinvar_annotated/` (annotated VCF cache)
- **Panels**: ACMG SF v3.3 — Cancer predisposition, Cardiovascular, Metabolism, Misc
- **Input**: VCF, gVCF (auto-annotated on first run)

### Pharmacogenomics (PGx)
- **Runner**: `run_variant_lookup()` (most genes) or `run_specialized(method='pgx')` (star alleles)
- **Method**: `bcftools query` for specific rsIDs with position fallback
- **Genes**: CYP2D6, CYP2C19, CYP2C9, VKORC1, DPYD, TPMT, NUDT15, SLCO1B1, HLA-B, UGT1A1, G6PD, etc.
- **Data**: built-in `rs_positions.py` (curated GRCh38 coordinates, no external files)
- **Note**: star-allele calling from VCF for some genes requires PharmCAT + BAM and currently returns a warning
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
- **ROH**: `_run_roh()` — `plink --homozyg` for consanguinity estimate
- **HLA typing**: `_run_hla_typing()` — T1K genotyper
  - Tool: `/home/nimo/miniconda3/envs/genomics/bin/run-t1k`
  - Data: `/data/t1k_ref/hla/{hla_dna_seq.fa, hla_dna_coord.fa}`
  - Input: BAM/CRAM only (extracts MHC-region reads)

### Sample QC
- **Runner**: `run_vcf_stats()` in `runners.py`
- **Tests**: Ti/Tv ratio, Het/Hom ratio, SNP count, indel count
- **Method**: `bcftools stats` parsing
- **Input**: VCF, gVCF, BAM, CRAM

### Sex Check
- **Tests**: Y-chromosome reads, SRY gene, X:Y ratio, chrX het rate, chrY variant count
- **Method**: `samtools idxstats` + `bcftools query`
- **Input**: BAM, CRAM (some tests also work on VCF)

---

## PGx Star-Allele Callers

### Cyrius (CYP2D6)

Cyrius handles CYP2D6 star-allele calling from BAM/CRAM. It correctly resolves the CYP2D7 pseudogene complexity, structural variants (`*5` deletion, `*13`/`*68` hybrids), and gene duplications (`*2xN`) that generic variant callers miss.

**Installation**:
```bash
sudo git clone https://github.com/Illumina/Cyrius.git /opt/cyrius
pip3 install --break-system-packages pysam scipy statsmodels
```

**Dependencies**: pysam, scipy, statsmodels, numpy, pandas.

**Integration**: when a CYP2D6 test runs on BAM input, Cyrius is tried first. If it's unavailable or fails, the system falls back to Pipeline E+ pileup genotyping with allele verification and impossible-diplotype detection.

### Allele verification

All `variant_lookup` tests verify that REF/ALT match the expected star-allele-defining change. A position-only match without matching alleles is reported as `locus_mismatch` — **not** as a positive star-allele call.

---

## ExpansionHunter (Repeat Expansions)

ExpansionHunter v5.0.0 calls trinucleotide repeat expansions — FMR1/Fragile X, HTT/Huntington's, DMPK/Myotonic Dystrophy — from BAM/CRAM. These expansions are **not detectable from VCF**: they require read-level analysis.

**Installation**:
```bash
wget https://github.com/Illumina/ExpansionHunter/releases/download/v5.0.0/ExpansionHunter-v5.0.0-linux_x86_64.tar.gz
tar xzf ExpansionHunter-v5.0.0-linux_x86_64.tar.gz
sudo cp ExpansionHunter-v5.0.0-linux_x86_64/bin/ExpansionHunter /usr/local/bin/
sudo mkdir -p /opt/expansion-hunter
sudo cp -r ExpansionHunter-v5.0.0-linux_x86_64/variant_catalog /opt/expansion-hunter/
```

**Supported loci**: FMR1 (CGG), HTT (CAG), DMPK (CTG). Extensible via the standard Illumina variant catalog at `/opt/expansion-hunter/variant_catalog/`.

**Integration**: when `carrier_fragx` (or any repeat-expansion method) runs on BAM input, ExpansionHunter is invoked with a single-locus catalog. The result includes per-allele repeat counts and a clinical classification (Normal / Intermediate / Premutation / Full mutation).

---

## gVCF Reference Block Handling

When querying a gVCF, reference blocks (ALT = `<*>` or `<NON_REF>`, GT = `0/0`) are correctly recognized as homozygous reference. Previously the symbolic ALT was compared against the expected variant allele and reported as "inconclusive" (`locus_mismatch`). The current behavior:

- Symbolic ALTs (`<*>`, `<NON_REF>`) with hom-ref GT → return `0/0` (ref/ref)
- Position-within-block detection: record POS ≤ query ≤ `INFO/END` → ref/ref
- Real variant records with non-matching alleles are still correctly reported as `locus_mismatch`

---

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
| Reference FASTA | `/data/refs/GRCh38.fa → hs38DH.fa` | Prerequisite | Manual |
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

---

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
