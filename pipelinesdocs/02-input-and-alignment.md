# 02 — Input, Alignment, and Variant Calling

The system never re-aligns FASTQ on demand: the pipeline starts from
already-aligned BAM / CRAM, or from a user-supplied VCF / gVCF / 23andMe
text file. This document describes how each input is normalized to the
form that the rest of the pipeline expects.

## 2.1 Input classification

`runners.py::_detect_file_type(path)` decides which branch to take, purely
by file extension:

| Extension                                                | Class | Branch |
| -------------------------------------------------------- | ----- | --- |
| `.vcf`, `.vcf.gz`, `.vcf.bgz`, `.bcf`, `.gvcf`, `.gvcf.gz`, `.g.vcf.gz` | `vcf` | indexed → scored |
| `.bam`                                                   | `bam` | per-test on-demand calling |
| `.cram`                                                  | `cram` | per-test on-demand calling, with `-T <ref>` decoding |
| 23andMe / AncestryDNA `.txt` / `.zip`                    | (converter) | external `bam-converter` builds a VCF first |

If the user submits a test that requires variant data but provides BAM/CRAM,
`_CRAM_OK_METHODS` (in `run_specialized`) decides whether the test knows
how to call its own variants. Tests not in that set return an explicit
"requires VCF input" error rather than silently mis-processing.

## 2.2 BAM / CRAM handling

### 2.2.1 Reference fasta selection

CRAM decoding needs a fasta whose `@SQ` names exactly match the CRAM
header. `runners.py::_pick_reference_for(aln_path)` iterates candidates
and picks the first whose `.fai` chr-prefix flag matches the CRAM's
chrom naming:

```python
_REF_CANDIDATES = [
    REF_FASTA,                                # from env
    "/data/genom-nimo/reference_chr.fa",     # GRCh38, chr-prefixed
    "/data/genom-nimo/reference.fasta",      # GRCh38, bare-chrom
    "/data/refs/GRCh38.fa",
]
```

If no match is found, the function falls back to `REF_FASTA` and the
caller sees a clear samtools error.

### 2.2.2 Indexing

`_ensure_alignment_indexed` creates a `.bai` / `.crai` if missing:

```
samtools index <file>           # BAM
samtools index -@ 4 <file>      # CRAM
```

### 2.2.3 Targeted variant calling

Most tests do not need a genome-wide VCF — they only need genotypes at
the specific positions used by that test. The pipeline implements two
narrow on-demand callers:

**A. PCA-site calling** (`runners.py::_derive_pca_vcf_from_cram`)

Calls the ~106K LD-pruned 1000G PCA positions from the alignment, caches
the result, and returns its path. Used by `pca_1000g`, `admixture`,
`neanderthal`, `roh` (and indirectly anything that delegates to PCA).

```bash
# 1. Read PCA-allele weights (chrom:pos:ref:alt format) → BED + targets TSV
# 2. Restrict to autosomes 1..22 so contigs like chrEBV don't break decode
samtools view --input-fmt-option ignore_md5=1 -T <ref> \
    -b -L positions.bed -o slice.bam <CRAM> chr1 chr2 ... chr22
samtools index slice.bam
# 3. Pileup + call at the exact positions; emit ALL genotypes (-m, not -mv)
bcftools mpileup -f <ref> -R positions.tsv \
    --max-depth 250 -q 20 -Q 20 -a FORMAT/AD,FORMAT/DP \
    -Ou slice.bam | bcftools call -m -Oz -o pca.vcf.gz
bcftools index -t pca.vcf.gz
# 4. Cache at cram_vcf_cache/<sha(realpath)>/pca.vcf.gz
```

**B. PGS-sites + hom-ref calling** (`scripts/pgs_sites_call.sh`)

Same idea but for the union of PGS positions. Calls with `-m` (not `-mv`)
so hom-ref sites are emitted, which is essential for PGS scoring to count
those positions toward the match rate. Parallelized by chromosome.

```bash
bash scripts/pgs_sites_call.sh <cram> <positions_tsv> <out.vcf.gz>
# Per chrom in 12-way parallel:
#   samtools view ... -b -o slice.bam <CRAM> chrN
#   bcftools mpileup ... -T <positions> | bcftools call -m  (no -v!)
#   → emits hom-ref rows. Without these, match rate is ~50%.
```

### 2.2.4 Genome-wide variant calling (CLI tool)

For users who want a full genome VCF:

```bash
bash scripts/cram_to_vcf.sh <cram_path> <out.vcf.gz>
```

Implementation: 22 autosomes + chrX/Y/M called in 12-way parallel, each
via `samtools view → bcftools mpileup → bcftools call -mv` (variant
sites only). Per-chrom VCFs concatenated with `bcftools concat -a`.

Parameters:

| Parameter            | Value                                       |
| -------------------- | ------------------------------------------- |
| Min MAPQ             | 20                                          |
| Min base quality     | 20                                          |
| Max pileup depth     | 250                                         |
| FORMAT fields added  | `AD`, `DP`                                  |
| Calling mode         | `bcftools call -mv` (var-only, multiallelic) |
| Output               | bgzipped, tabix-indexed                     |
| Parallelism          | 12 chromosomes concurrent                   |

### 2.2.5 HLA typing (BAM/CRAM only)

`_run_hla_typing` invokes T1K against the alignment. If T1K fails and a
sibling VCF is cached, the pipeline falls back to proxy-SNP HLA typing
(specific tag SNP per allele). When only VCF is available we go straight
to proxy-SNP.

### 2.2.6 Repeat expansion (BAM/CRAM only)

`_run_expansion_hunter` invokes ExpansionHunter per gene per disease.
Inputs are BAM/CRAM only — repeats are not callable from a standard VCF,
so VCF inputs return a clear warning.

## 2.3 VCF / gVCF handling

### 2.3.1 Indexing & format normalization

`_ensure_indexed` bgzips and tabix-indexes any plain VCF (`.vcf` →
`.vcf.gz` + `.tbi`). Existing indexed files pass through.

### 2.3.2 gVCF detection

`_is_gvcf(vcf_path)` reads the header for `##GVCF`, `END=` records, or
`<NON_REF>` / `<*>` ALTs. The full pipeline branch then runs
gVCF-aware expansion (next section); the variant-only branch uses the
file directly.

### 2.3.3 gVCF block expansion (`_normalize_gvcf`)

PGS scoring needs hom-ref genotypes at PGS positions to count as "0-dose
matched", not as "missing". A naive `bcftools view --exclude '<*>'`
drops every reference block and collapses the match rate to ~50%. The
pipeline therefore expands gVCF blocks at a **union of PGS + PCA
positions** (~7.34M sites):

```
# 1. Build / refresh union positions file
#    /data/pgs_cache/_all_pgs_pca_positions_{chr,bare}.tsv
#    Rebuilt automatically if any PGS scoring file (or PCA eigenvec) is
#    newer than the cache. This is the bug that masked PGS002753 at 16% match.

# 2. Per chromosome (parallel, up to 22 workers):
bcftools convert --gvcf2vcf -f <ref> \
    -T <union_positions> --targets-overlap 1 \
    -r chrN -Oz -o per_chr/expanded_chrN.vcf.gz <gvcf>
#   --targets-overlap 1 is essential: a gVCF block (POS..END) that
#   spans a target position but doesn't start at one would be dropped
#   under the default --targets-overlap 0.

# 3. Python pass: rewrite leftover <*> / <NON_REF> ALTs to the PGS/PCA
#    panel's expected ALT allele (looked up from an allele_map built from
#    all scoring files + the PCA eigenvec). Variant records are passed
#    through unchanged.

# 4. Concat per-chr outputs in order with bcftools concat --naive.
#    ALL 22 chroms must succeed — partial outputs are refused.
#    Otherwise an inflated AVG silently masks dropped hom-ref positions
#    (this is the bug behind the PGS001229 cohort_sanity flag).
```

**Failure semantics**: if even one chromosome's convert step fails, the
function raises and refuses to write `out_path`. Atomic write via a
`.tmp.<pid>` sidecar + `os.replace` so a crash mid-concat does not leave
a half-baked cached normalized VCF.

**Caching**: the normalized output is stored at
`cram_vcf_cache/<sha>/gvcf_normalized.v5.vcf.gz`. The `v5` suffix is
`PGEN_CACHE_SCHEMA` — bumping that constant invalidates all old cached
files automatically. Versions to date:

| Schema | Change |
| ------ | ------ |
| v1     | original — PGS-positions only |
| v2     | `--gvcf2vcf` normalization with PGS positions only |
| v3     | added PCA panel positions + concat with genome-wide variants — **broken** (`bcftools concat -D` dropped hom-ref → prostate match collapsed 94→45%) |
| v4     | reverted variant concat; genome-wide tests read raw gVCF directly |
| v5     | also strip `<*>`/`<NON_REF>` placeholder from multi-allelic ALTs (`C,<*>` → `C`) so plink2 `--score variance-standardize` doesn't barf on PCA records |

### 2.3.4 Why the normalized output is PGS+PCA-only

History: an earlier version tried to concat the expanded VCF with a
variant-only genome-wide VCF so all downstream tests could share one
file. `bcftools concat -D` had a deduplication bug that quietly dropped
most hom-ref records when both inputs contained the same positions. The
prostate-cancer PGS regressed from 94% → 45% match.

Current contract: the normalized output is used **only** for
plink2-based tests (PGS scoring + PCA). Genome-wide tests (ROH, sex,
ClinVar, Y/mt haplogroup, variant lookup) read the original gVCF
through `bcftools query` / `bcftools view` directly. The bifurcation is
intentional.

## 2.4 Build detection and validation

Every PGS run validates the VCF's genome build against the scoring file's
positions. `_validate_genome_build(vcf_path, reference_build)`:

1. **Header parse** — reads `##reference`, `##contig`, and common
   metadata to extract a declared build (`GRCh37` / `GRCh38` / `hg19` /
   `hg38`).
2. **Spot-check panel** — queries 3 well-characterized SNPs at both
   builds' coordinates:

   | rsID       | Notes                              | GRCh38            | GRCh37            |
   | ---------- | ---------------------------------- | ----------------- | ----------------- |
   | rs7412     | APOE — present in most WGS         | chr19:44908822    | chr19:45412079    |
   | rs429358   | APOE — adjacent to rs7412          | chr19:44908684    | chr19:45411941    |
   | rs1801133  | MTHFR C677T — in nearly all panels | chr1:11796321     | chr1:11856378     |

3. **Decision**:

   | Header declared        | Spot-check result                  | Status | Action |
   | ---------------------- | ---------------------------------- | ------ | --- |
   | matches reference      | ≥2 SNPs at expected coords         | PASS   | proceed |
   | matches reference      | 1 SNP confirmed                    | WARN   | proceed, downgrade confidence |
   | matches reference      | 0 SNPs present (targeted data)     | PASS   | proceed (small VCF, can't validate) |
   | undeclared             | ≥2 SNPs at expected coords         | PASS   | proceed |
   | undeclared             | 1 SNP confirmed                    | WARN   | proceed cautiously |
   | undeclared             | 0 SNPs present                     | WARN   | proceed with explicit warning |
   | mismatched / wrong-build hits | matches_wrong > matches_expected | FAIL → liftover scoring file or fall through to match-rate gate |

4. **Auto-liftover**: when the VCF is GRCh37 but the scoring file is
   GRCh38 (or vice versa), `_liftover_pgs_scoring(plink2_scoring,
   from_build, to_build, tmpdir)` rewrites the **scoring file** to the
   VCF's build using UCSC `liftOver` and the chain files in
   `/data/ancestry_reference/hg19ToHg38.over.chain.gz` and
   `simple-genomics/liftover/hg38ToHg19.over.chain.gz`. Lifting the
   scoring file is cheap (~thousands of lines); lifting the user's VCF
   is wasteful (millions of records, half of them irrelevant).

5. **Audit log**: every decision is appended to
   `<SCRATCH>/build_validation.log` as a JSON line for retroactive review.

## 2.5 23andMe / AncestryDNA text files

These are not aligned data — they are pre-genotyped chip outputs. The
external `bam-converter` project (separate repo, `~/bam-converter/`)
parses the rsID / chrom / pos / genotype tsv and produces a synthetic
VCF whose REF/ALT come from a dbSNP lookup. The VCF then enters the
pipeline at step 2.3.

Caveats:
- chip files are sparse (~600K SNPs); match rates against large PGS
  (1M+ variants) are intrinsically low. The match-rate gate (<60%) will
  reject most modern PGS for chip-only inputs.
- ambiguity in REF/ALT for indels and on rsID-only lines is resolved via
  dbSNP; rare miscalls are possible.

## 2.6 chrX / chrY / chrM handling

- **chrX**: plink2's `--split-par b38` is required because PAR regions
  follow autosomal inheritance; we always pass this. A sidecar sex file
  is written with all samples set to `SEX=0` (unknown) so plink2 keeps
  chrX as diploid (we don't trust user-declared sex at this step).
- **chrY**: only used for `y_haplogroup`. Filtered to chrY before
  haplogrouping; women's samples emit a clear "no Y reads" warning.
- **chrM**: HaploGrep3 is run against an extracted chrM VCF. Sample
  with 0 chrM coverage returns a clear unable-to-call result.

## 2.7 Variant ID convention

Inside the pgen, variant IDs follow `--set-all-var-ids chr@:#` (e.g.
`chr1:12345`). The PCA cache uses `@:#:$r:$a` to match the 1000G
reference panel's IDs (`1:12345:A:G`). Multi-allelic / overlapping
records that collide on ID are dropped with `--rm-dup force-first`
(plink2 fails the score otherwise). These are silent drops; if a PGS
relies on alternate alt alleles at a multi-allelic site, our scoring
will underestimate the contribution. We don't currently detect this
explicitly — it's on the list (see `known-issues.md`).
