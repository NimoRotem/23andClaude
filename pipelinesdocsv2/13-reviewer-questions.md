# 13 — Questions for the Reviewer

Decisions and trade-offs we'd value an outside opinion on. Organized
by chapter; pointers back where useful.

## 13.1 Ancestry inference (chapter 05)

1. **PCA + inverse-distance vs ADMIXTURE/RFMix**. Today we estimate
   admixture proportions by inverse-distance weighting of PC1..PC4
   centroid distances. It is smooth-but-shallow — we can't
   confidently distinguish 70%EUR/30%AMR from 60%EUR/40%AMR. For
   ref-stats selection this is mostly fine (top posterior ≥ 0.80
   routes 95% of users to a single-population panel), but for the
   AMR / Latino edge cases we lose resolution. Worth the engineering
   cost to wire in a supervised classifier (RF on the eigenvec) or
   true ADMIXTURE? If yes — should it run synchronously per request
   or as a once-per-sample post-upload job?

2. **Anchor anchor tests as hard gates**. `pca_projection_validation`
   has HG00096 anchor + cohort-scatter checks but they don't block
   scoring today. Should they? The trade-off is that legitimate
   distinct ancestry samples (e.g. a Han Chinese sample being scored
   on a panel where HG00096 is the anchor) might trip cohort-scatter.

3. **MID / Pacific / Indigenous Australasian**. We currently route
   these to `UNSUPPORTED` and emit per-pop sensitivity arrays. Are
   we right to refuse a primary percentile, or should we pick the
   AF-best-fit pop and surface that with a strong caveat?

## 13.2 Reference panel and ref-stats (chapter 06)

4. **Two-store consolidation**. The legacy
   `/data/pgs2/ref_panel_stats/` (with registry, full schema, 57
   PGSes) coexists with the new `/data/ref_stats/` (362 PGSes,
   schema-incomplete). Chapter 09 proposes fixes A–D. Which path
   would you pick? Specifically:
   - Is **schema backfill** (Fix A — stamp the missing fields in
     place) reckless given we have to compute
     `variant_ids_sha256` live, or pragmatic?
   - Is **ECDF-from-NPY** (Fix B — use the raw arrays, skip the JSON
     schema) the right long-term direction?
   - Is **full recompute** (Fix C — 8 hours of plink2) the
     conservative answer worth waiting for?
   - Is **lenient loader** (Fix D — accept schema-incomplete but
     numerically-plausible files) the right granularity, or does it
     erode the post-PGS000334 contract too much?

5. **Sex-stratified stats**. Module exists; not wired into live path.
   PGS001229 cohort-sanity flag is the worked example. Worth the
   build cost (~2× the ref-stats files) for the PGSes flagged as
   sex-dimorphic, or is the warning ribbon sufficient?

6. **MIX population definition**. `MIX = 50% EUR + 50% EAS` is a
   blunt instrument. True admixed reference panels would need
   individual-level admixture proportions. Should we (a) keep MIX
   as a fallback for the simple admixed case, (b) drop MIX entirely
   in favor of the per-pop sensitivity array (chapter 05 §5.6 Phase
   1.5), or (c) build proper admixed panels?

## 13.3 Percentile and stats (chapter 07)

7. **Parametric vs ECDF as primary**. The parametric path makes a
   normality assumption that holds for the majority of PGSes but
   fails for heavy-tailed traits. Should ECDF be the live default,
   with parametric as a "consistency check" fallback?

8. **Scale reconciliation heuristic**. We swap `raw_score` for
   `score_sum` when `|raw| < 0.001 × |mean|`. Conservative but
   heuristic. Worth tagging every ref-stats file with its scale
   explicitly and requiring exact match?

9. **Schema contract granularity**. The post-PGS000334 contract
   refuses any ref-stats file with a missing field. Should we split
   it into:
   - **drift contract** (catalog sha mismatch) → hard fail, never
     fall back
   - **metadata contract** (missing schema_version,
     scoring_method, etc.) → soft fail with a confidence demotion
     and a warning?

## 13.4 QA and validation (chapter 08)

10. **Match-rate thresholds**. 60% / 85% are empirical. For
    consumer-chip data (sparse, ~600K SNPs) these are catastrophic
    for modern 1M-variant PGSes. Should we have input-class-aware
    thresholds (e.g. 30% for chip data)?

11. **Pipeline E+ build validation**. Pileup paths skip
    `_validate_genome_build` (BAM coordinates are aligner-implicit).
    Should we add a fallback 3-SNP pileup check just to confirm
    nothing pathological?

12. **Cohort sanity preemption**. Cohort sanity flags PGSes after
    they've shipped wrong percentiles. Should the flag short-circuit
    future runs for the same PGS?

## 13.5 PGS handling (chapter 03–04)

13. **PRS-CSx / multi-population PGS construction**. We score every
    user against the EUR-trained PRS. PRS-CSx would let us combine
    EAS-trained + EUR-trained weights per-variant. Worth the build?
    What's the right scope to start with — the top-23 "Common PGS"
    list, or the full ~250 curated set?

14. **Portability list (`pipeline/portability_warnings.py`)**.
    Currently one entry (PGS000898). What's a defensible source for
    expanding this — manual literature review per PGS, automated
    extraction from PGS Catalog evaluation tables, or a separate
    "ancestry-portability calibration" pass we'd run periodically?

15. **Eligibility ancestry gate**. The `ancestry_mismatch` gate
    refuses to emit a percentile when the user's pop isn't in the
    PGS's `evaluation_ancestry`. Strict but correct. Should we
    distinguish "eval includes our pop" (full validation), "eval
    includes a related pop" (e.g. user is EAS, PGS eval is mixed
    EAS+EUR — degrade confidence), and "eval is single distant pop"
    (current behavior — refuse)?

## 13.6 LLM interpretation (chapter 04 §4.8, chapter 09 §9.1)

16. **Prose phrasing**. The LLM synthesizes the user-facing message
    from `cross_ancestry_warning` + `percentile_details.description`
    + `confidence_reasons`. The catastrophizing tone the user
    experiences seems to come from the LLM conflating "schema
    failure" with "no ancestry data". Would you reword the
    `cross_ancestry_warning` prose to steer the LLM toward more
    measured language? Specifically, should the warning text
    explicitly mention that the percentile being absent is a
    pipeline issue, not a biology issue?

17. **`risk_language_allowed` gate**. The
    `result_guards.filter_risk_language` substitutes hedged language
    when ancestry/sex aren't validated. Are we being too restrictive
    (e.g. wrapping every non-EUR percentile in "this is
    exploratory") or too permissive (allowing directional claims
    when only PCA, not PRS-CSx, was applied)?

## 13.7 Reference panel updates

18. **Phase 4 / NYGC 30× re-release**. Allele frequencies shift
    slightly between Phase 3 1000G and the 30× re-release; PCA
    centroids would shift correspondingly. Is the migration worth
    quantifying, and how would you design the side-by-side
    validation?

19. **HGDP, SGDP, EGA datasets**. Adding any of these expands the
    representation of currently-`UNSUPPORTED` ancestries (Middle
    Eastern especially). What's a sensible starting point and
    layering strategy — augment Phase 3, or build a separate
    "extended" panel and route under-represented users to it?

## 13.8 Open architectural questions

20. **Per-sample matched-subset stats**. `pipeline/matched_subset_stats.py`
    proposes computing μ/σ over **only** the variants the user
    matched, not the full PGS. This removes the missing-variants-as-
    zero bias in `no-mean-imputation`. Worth the per-request compute
    cost? Or does it open the door to over-fitting μ/σ to the
    user's exact match set?

21. **Local ancestry painting (RFMix / Loter)**. For genuinely
    admixed users (top posterior < 0.80), chromosome-level ancestry
    painting would let us route per-chrom-region variants to per-
    region appropriate ref-stats. Currently a `MULTI` user gets the
    sensitivity-array treatment. Long-term right answer, or
    over-engineering for the ~3% of users that fall here?

22. **Strictness vs availability**. Across many places in this
    packet, the team has chosen strictness (refuse rather than
    guess) over availability (always emit something with a caveat).
    The EAS incident (chapter 09) is a stark example of the
    operational cost of that choice. Is the bias right, or should
    parts of the pipeline (specifically the ref-stats contract)
    move toward "always emit, demote confidence"?

## 13.9 Specific code reviews welcome

If the reviewer wants to dig into specific files, these would be the
highest-yield to read end-to-end:

- `pipeline/scoring.py` (614 lines) — `select_reference`,
  `compute_percentile_multipop`, `_compute_single_percentile`,
  `_rs_validate`.
- `pipeline/eligibility_gates.py` (182 lines) — the six gates that
  decide whether risk language is allowed.
- `runners.py:3470–4400` — `run_pgs_score`, fast path, percentile
  wrapper, `_compute_confidence`, `_postprocess_pgs_result`.
- `runners.py:7864–8270` — `_run_pgs_score_pileup` Pipeline E+ path.
- `runners.py:1002–1300` — `_normalize_gvcf` (the v5-cache logic).
- `pipeline/ecdf_percentile.py` (140 lines) — the Phase 2.1 ECDF
  implementation.
- `pipeline/live_percentile.py` — the read-time overlay.
- `scripts/recompute_ref_stats.py` — the ref-stats builder.

We'll set up read-only SSH access to `genom-beast-gpu` for the
reviewer on request.
