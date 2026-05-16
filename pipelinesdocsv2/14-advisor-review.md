# 14 — External Advisor Review

Captured 2026-05-14, in response to the brief in chapter 13 and the
EAS-failure exhibit. Preserved verbatim for the audit trail; the
concrete action plan derived from this review lives in
[15-action-plan.md](15-action-plan.md).

---

## Bottom line

> The engineering direction is good, but the system's main weakness
> is **calibration governance**. Fix ancestry-aware reference
> statistics, validation metadata, and deterministic reporting
> before expanding traits or presenting percentiles as meaningful
> across populations. Treat current PGS outputs as
> research/educational unless each trait has ancestry-specific
> validation.

## Highest-priority issues

1. **EAS / non-EUR percentile failure is real and systemic.** The
   loader rejects existing EAS stats because required schema fields
   are missing; EUR users escape through a legacy fallback. Fix by
   rebuilding/refactoring the reference-stat registry, not by
   loosening validation blindly.
2. **Do not score non-EUR users against EUR percentiles.** EUR-
   derived PGSes commonly lose accuracy outside European ancestry.
   This is a known PRS limitation, not just a software bug.
3. **Separate "raw PGS score" from "interpretable percentile."** Raw
   plink2 output can be produced, but percentile/risk should only be
   emitted when ref pop, build, variant set, scoring direction, and
   calibration metadata match.
4. **One authoritative ref-stat store.** Two stores + a registry is
   a drift risk. One canonical registry should own: PGS ID, genome
   build, population, sample count, score distribution, variant
   hash, scoring method, imputation policy, generation timestamp,
   source panel, and validation status.
5. **`/compare` can silently bias comparisons.** EAS samples drop
   because of the ref-stats incident; cohort comparisons become
   misleading. Add visible "excluded from comparison" reasons per
   sample/trait.
6. **Ancestry inference is fragmented.** The main pipeline uses
   1000G PCA; the standalone ancestry app uses gnomAD HGDP+1kGP +
   Rye + ROH. Pick one engine as source of truth, or clearly
   document which drives PGS ref selection.
7. **Middle Eastern / North African / admixed samples need
   explicit handling.** The standalone app resolves MID; main
   pipeline doesn't. Don't force these samples into EUR/SAS/AMR
   buckets without uncertainty reporting.
8. **Variant-calling validation is under-specified.** Add
   GIAB/hap.py stratified benchmarking by SNV/indel, coverage, GC,
   repeats, low-complexity, ancestry-relevant regions.
9. **BAM/CRAM → VCF → PGS can introduce genotype missingness
   artifacts.** Every report should show: matched variant count,
   expected variant count, missingness, allele-flip count, excluded
   variants, build, liftover status, imputation/default policy.
10. **Consumer-chip inputs should not be treated like WGS.** Block
    or downgrade low-overlap scores, not "simply computed."
11. **LLM interpretation is a risk layer.** Genomic result
    interpretation should be deterministic from structured flags,
    not generated freely. Use fixed templates fed by
    machine-readable fields.
12. **PGS traits like "intelligence" are especially risky.** Either
    remove them or put them behind strict disclaimers, lower-
    confidence labels, and no deterministic "ability" language.
13. **Structural-variant scanner needs independent validation.**
    Truth sets, orthogonal confirmation rules, "research only"
    language unless clinically validated.
14. **Old scanner endpoints should be hidden or clearly
    deprecated.** Especially the v2 endpoint pointing at the
    terminated VM.
15. **Avoid clinical-grade claims.** Use "research prioritization
    tiering" unless the pipeline has CLIA/CAP-style controls and
    confirmation workflows.

---

## P0 — Correctness blockers

1. **Fix the EAS/non-EUR percentile failure by regenerating
   canonical stats, not by weakening validation.** Regenerate every
   (PGS × pop × build) stats file with the current hard contract:
   `schema_version, variant_ids_sha256, scoring_method,
   imputation_policy, generated_at, n_variants`. Do not add a
   lenient loader except as a quarantine-only migration tool.
2. **Make ECDF the primary percentile method.** Parametric Φ is
   fallback only. Store per-pop reference score arrays / compact
   quantile sketches; compute percentile by ECDF/rank interpolation.
3. **Compute matched-subset reference stats.** For every user
   score, compute percentile against the reference-panel scores
   using the **exact same matched variant set**. Store
   `matched_variant_ids_sha256, n_matched, n_total, coverage_pct,
   reference_pop_n`. Single biggest improvement for chip, gVCF,
   BAM/CRAM, low-coverage consistency.
4. **Require all three scoring paths to agree.** Full-pgen, fast-
   path, Pipeline E+ must produce identical scores on truth samples
   for the same PGS. Pipeline E+ restricted to biallelic SNVs
   unless explicit validation exists for indels/multiallelics/
   repeats/low-confidence pileup.
5. **Stop using LLM-generated root-cause messages for pipeline
   failures.** Emit deterministic reason codes:
   `ref_stats_schema_invalid, population_stats_missing,
   variant_hash_mismatch, match_rate_below_threshold,
   z_score_extreme, build_mismatch, unsupported_ancestry_panel`,
   etc.
6. **Audit the 1000G panel claim.** Phase 3 is 2,504 GRCh37
   samples; the 3,202-sample set is the later high-coverage GRCh38
   re-release including additional related samples. Rename,
   document relatedness filtering, publish final per-pop counts.
7. **Remove or disable MID until a real Middle Eastern reference
   panel exists.** Show only ancestry-inference uncertainty or "no
   validated reference distribution."

---

## P1 — PGS scoring and percentile accuracy

- Use PGS Catalog harmonized GRCh38 files first; liftover author-
  reported GRCh37 only when no harmonized GRCh38 file exists.
- Reject unsupported weight types unless converted. Log-scale
  OR/HR before linear scoring or reject. Store `weight_transform`.
- Canonical variant IDs are `CHR:POS:REF:ALT` after normalization.
  rsIDs are annotation only. Never hash rsIDs alone.
- Hash the actual scored allele vector, not just the variant list
  (chrom, pos, REF, ALT, effect allele, weight, normalization
  version, liftover version, scoring method).
- Use `plink2 --score cols=+scoresums` and store raw sums.
- Always persist `--score list-variants` output (`.sscore.vars`)
  and hash it into the result bundle.
- Make missingness policy explicit per PGS.
  `no-mean-imputation` is good for transparency, but the
  percentile reference must follow the same rule.
- Report **confidence intervals for percentiles**. Bootstrap over
  reference samples and over matched variants. Output `percentile,
  ci_low, ci_high, n_ref, n_matched, match_rate`.
- Add a **PGS eligibility matrix**: allowed ancestries, validated
  ancestries, original GWAS ancestry, evaluation AUC/R², weight
  type, variant count, supported input types, sex restriction,
  percentile eligibility.
- Do not present cross-ancestry percentiles as equivalent.
- Separate "score computed" from "score interpretable" as
  distinct statuses.
- Use sex-stratified stats where trait or chromosome demands it.
- Store LD/variant redundancy diagnostics (effective n_variants).
- Add per-PGS distribution sanity checks: collapsed sigma,
  multimodal distributions, extreme skew, missingness, outlier
  rates.

## P1 — Ancestry inference and reference selection

- Replace nearest-centroid PC1–PC4 with Mahalanobis/density-based
  assignment, uncertainty, and an admixed/unknown state.
- Don't force every sample into one super-pop. Posteriors +
  "insufficient reference match."
- Continuous ancestry adjustment for PGS: regress reference score
  distributions on ancestry PCs and compute ancestry-adjusted
  residual percentiles.
- Build admixed reference distributions by PC-nearest neighbors
  or local ancestry where available.
- Validate PCA projection scaling — plink2's projected PCs shrink
  toward zero in out-of-reference samples; bake tests for this.
- Anchor PCA QC with HG00096 + NA12878/HG001/HG002 + one
  representative per super-pop. Check PCs, assignment, score,
  percentile, report text.
- Publish reference-pop sample counts on every percentile.
- Expand beyond 1000G for non-EUR calibration (HGDP/SGDP,
  GenomeAsia, H3Africa-like, UKB/All of Us subject to access).

## P1 — Input handling, build validation, normalization

- Use exact sequence dictionaries, not ad-hoc `chr` prefixing.
  UCSC / NCBI / EBI naming must be explicit in a manifest.
- Strengthen build detection beyond 3 SNPs — larger sentinel
  panel across autosomes, chrX, MT.
- Normalize every VCF before scoring: left-norm, multiallelic
  split/atomize, REF check, canonical ID assignment.
- Never use `bcftools norm --check-ref s` as a strand fix. Use
  fixref/AF checks or reject.
- Treat palindromic SNPs separately. A/T or C/G without clear
  resolution → drop or AF-resolve.
- Rework gVCF `<*>`/`<NON_REF>` rewriting. Placeholder ALT
  rewrite only after confirming reference base, block coverage,
  GQ, depth, allele semantics. Otherwise set genotype missing.
- Use targeted VCF calling over direct pileup for non-SNVs.
  Pipeline E+ pileup is OK for high-confidence biallelic SNVs;
  use genotype-likelihood calling for indels, multiallelics,
  STR-adjacent loci, low-depth sites.
- For CRAM, require matching reference FASTA MD5 (not "a
  GRCh38").
- Use GRCh38 no-alt analysis set unless aligner/caller is
  ALT-aware. `hs38DH` is not recommended unless the pipeline
  handles ALT-aware correctly.
- Reference FASTA provenance must be immutable: URI, source, MD5/
  SHA256, `.fai`, `.dict`, contig names, alt/decoy status, patch.
- Use `-Ou` in bcftools pipes to avoid round-trip overhead.

## P1 — Liftover

- Prefer native GRCh38 scoring files over liftover.
- Use allele-aware liftover (`bcftools +liftover`) for VCFs over
  coordinate-only UCSC `liftOver` for indels/multiallelics.
- Always keep a reject file with reasons.
- Normalize after liftover (REF check, left-norm, multiallelic
  split, hash recomputation).
- Do not liftover user VCFs unless unavoidable.

## P1 — ClinVar VCF and clinical variant data

- ClinVar VCF is not complete ClinVar. VCF is limited to variants
  with precise locations and summary-level data; XML is complete.
- Use VCF for simple precise alleles; XML/TSV for complete
  interpretation. ClinVar VCF excludes many CNVs, cytogenetic
  variants, imprecise variants, and >10 kb variants.
- Pin ClinVar release date and checksums.
- Handle VCF vs HGVS coordinate differences (left- vs right-
  shifted indels in repeats).
- For chr-prefixed ClinVar, use a tested contig map. Validate
  MT/chrM, PAR, alt contigs, accession-style contigs.
- Track ClinVar error files (`.error.txt`); fail if severe.
- Separate ClinVar classification from medical assertion. Store
  assertion criteria, review status, submitter count, condition,
  date, evidence links. Don't collapse all "Pathogenic" labels
  into a single deterministic health claim.

## P1 — Specialized tools

- Pin htslib/samtools/bcftools 1.23.1 trio across the pipeline.
- Use plink2 as primary; keep plink 1.9 (b.7.11) only for
  compatibility.
- Pin Java runtime per tool, not globally. Containerize per
  Java tool.
- Pin HaploGrep3 3.2.2 and tree version; output must include
  tree name/version, distance function, heteroplasmy threshold,
  reference convention.
- Don't classify mtDNA from sparse SNPs as if it were full mtDNA.
  Array data → reduced confidence, missing defining mutations,
  upstream-only haplogroup.
- Pin ExpansionHunter v5 + catalog hash. Store EH version,
  catalog version/hash, reference FASTA hash, read depth, per-
  locus QC.
- STR-specific QC per locus: spanning/in-repeat counts, depth,
  CI, off-target, sex/ploidy, pathogenic threshold source,
  input-validated flag.
- Use T1K v1.0.9 with versioned IPD-IMGT/HLA + IPD-KIR builds.
- Validate T1K by input type. Low-depth/array-derived HLA →
  "unsupported" or "low confidence."
- Constrain Cyrius to WGS ≥ 30×. Below that, no-call. Preserve
  ambiguity states (`More_than_one_possible_genotype`,
  `Not_assigned_to_haplotypes`, no-call) — don't flatten.
- For Neanderthal SNPs, use versioned introgression resources
  (genome-wide GRCh38 archaic VCF/BED). Label as ancestry /
  research / recreational, not health.
- UCSC ancient hominid VCFs are source-specific annotations, not
  definitive individual ancestry estimates.

## P2 — Reliability, deployment, monitoring

- Build a complete **availability matrix**: PGS × ancestry × build
  × method nightly; alert on drops below expected coverage.
- Ref-stats registry with **atomic promotion**: staging → validate
  → hash → registry pointer. No partial writes.
- Schema migrations: every stats JSON has `schema_version`;
  loaders support exact versions with migrations, not implicit
  compatibility.
- Blue/green data deployments. Atomic switch + rollback for PGS
  catalog, ClinVar, ref stats, ancestry panel, ExpansionHunter
  catalog, T1K reference, FASTA.
- Reproducible data refreshes — manifest per refresh: source,
  release date, checksum, command, tool versions, input hashes,
  output hashes, row counts, diff from previous release.
- End-to-end synthetic tests: VCF, gVCF, BAM, CRAM, 23andMe text,
  chr / no-chr, NCBI-accession VCF, GRCh37 input, GRCh38 input,
  palindromic SNPs, multiallelics, indels, missing genotypes,
  symbolic ALTs.
- Golden biological controls: HG001/NA12878, HG002, one per
  super-pop, plus synthetic edge cases.
- Monitor live output distributions — KS-vs-uniform per population,
  z-inflation, percentile clipping spikes, match-rate drops,
  schema-refusal spikes.
- Log skipped variants by cause: missing site, allele mismatch,
  REF mismatch, duplicate ID, palindromic unresolved, liftover
  reject, low depth, low GQ, symbolic ALT, unsupported contig,
  unsupported variant type.
- Match-rate thresholds PGS-specific. Small high-impact PGSes
  and very large PGSes need different confidence logic.
- Score reproducibility tests after every tool upgrade.
- Containerize each tool with checksums. No mutable `latest`
  tags. Record path, version output, package build, container
  digest.
- SBOM + vulnerability scans (Java, Python, nginx, bioinformatics
  binaries).
- File locks + idempotent cache writes: temp file → fsync →
  validate → atomic rename.
- Move concurrent audit/report state out of SQLite if traffic
  grows; use Postgres.
- Keep docs versioned. `/pipelinesdocsv2/` immutable after review;
  future changes → `/pipelinesdocsv3/`. Disable nginx directory
  listing; stable cache headers.

## P2 — Reporting and UX accuracy

- Show **calculation provenance** on every report: build, input
  type, reference FASTA, scoring file ID/version, PGS Catalog
  release, tool versions, matched variants, skipped variants,
  ancestry reference, ref-stats hash, percentile method.
- "Why unavailable" instead of blank percentiles. E.g. "No
  percentile: EAS_GRCh38 stats failed schema validation." Avoid
  vague "no ancestral benchmark" when data exists but validation
  failed.
- Show raw score separately from percentile.
- Deterministic interpretation templates. LLMs may rewrite
  explanations but never decide correctness, severity, medical
  meaning, or failure cause.
- Separate clinical / pharmacogenomic / ancestry / STR / mtDNA /
  archaic / recreational modules. Distinct evidence standards
  and warnings per module.
- ClinVar results: show review status and condition specificity.
  "Pathogenic" without condition + review status is misleading.
- STRs: show locus-validation status. Don't display like ordinary
  SNP genotypes.
- HLA/KIR/CYP2D6: surface no-call/ambiguity explicitly.
- Ancestry: probabilities + uncertainty, not deterministic
  continental labels when PCs are intermediate.

---

## Advisor's implementation order

1. Regenerate canonical ref stats for all populations and block
   lenient fallback.
2. Add matched-subset ECDF percentiles.
3. Add full provenance bundle and deterministic failure reason
   codes.
4. Audit 1000G panel naming, relatedness filtering, population
   counts.
5. Harden normalization/build/gVCF logic.
6. Validate full-pgen vs fast-path vs Pipeline E+ on truth samples.
7. Replace centroid ancestry assignment with calibrated uncertainty-
   aware ancestry selection.
8. Pin and containerize all tool/data versions.
9. Add ClinVar XML/TSV support beside VCF.
10. Add continuous monitoring for percentile availability, schema
    failures, match-rate drift, population-specific failures.
