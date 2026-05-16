# 15 — Action Plan (derived from chapter 14)

This chapter turns the advisor's review (chapter 14) into concrete,
sequenced engineering work tied to the actual files in this
codebase. The advisor's implementation order is the spine; each
item below lists the **code touchpoints**, the **decision** we'll
make, and the **verification step** that confirms it landed.

Where the advisor recommends new architectural pieces (e.g. ECDF as
primary, matched-subset stats), we keep two parallel options — the
"land it cleanly" plan and a "minimal-viable" fallback — so we can
ship correctness improvements without blocking on the larger
rewrites.

## Status convention

`[ ]` not started — `[~]` in progress — `[x]` done — `[!]` blocked
on external dependency.

---

## Wave 0 — Revised ordering (advisor's correction, 2026-05-14)

**The correct first move is the gate, not the rebuild.** The advisor's
core warning is that raw scores and interpretable percentiles are being
conflated. Ship the deterministic gate first so unsafe outputs stop
rendering; rebuild the stats afterwards under atomic promotion.

Sequence:

1. **Ship the gate** — W0.3, W0.4, W0.5, W0.7, and read-time part of
   W1.5 (✅ landed 2026-05-14, commit `d60100a` on
   `genom-beast-gpu:/home/nimrod_rotem/simple-genomics`).
2. **Disable unsafe public outputs** — MID, sensitive PGSes, scanner v2
   → 410, LLM failure messages, /compare excluded_samples (✅ same
   commit).
3. **Capture availability matrix** — read-only inventory before W0.1
   begins (✅ snapshot at
   `logs/availability_matrix.before-rebuild.jsonl`; 1,924 (PGS×pop)
   entries blocked by `REF_STATS_SCHEMA_INVALID` — that is the W0.1
   queue).
4. **W0.1 staging rebuild with atomic promotion** — write to
   `staging/`, validate, replay anchors, manifest, then atomic
   promote to `current/`. **100% of public curated (PGS × supported
   pop × build) must pass; partial is not acceptable.**
5. **Remove EUR fallback (W0.2)** the moment W0.1 promotion completes.
6. **W1.1 + W1.2 as a single calibration release** — ECDF + matched-
   subset stats wired together, not as independent features. ECDF is
   blocked on low/medium-match samples until matched-subset is ready.
7. **Freeze docs (W4.6)** only after the Wave 0 evidence above exists.

## Wave 5 — Second-advisor recommendations (Items 1-5)

Captured 2026-05-14 from a second advisor's review (corpus-grounded
pgsc_calc / fraposa_pgsc / Nat Genet 2024 method). Five items, all
in flight or shipped:

| Item | Status | Notes |
| --- | --- | --- |
| **Item 1 — per-sample panel score dumps** | ✅ shipped (commit 37a93dc) | `recompute_ref_stats.py` now writes `_scores/<PGS>/<POP>_scores.npy` + `sample_ids.txt`. `dump_panel_scores.py` back-fills already-done PGSes. Smoke verified: PGS002746 EAS_scores.npy matches blessed JSON μ/σ exactly. |
| **Item 2 — `--chr 1-22,X,Y,XY` + pgen cache v6** | ✅ shipped (commit 37a93dc) | Restricts user VCF→pgen to autosomes + sex chroms (excludes chrM/EBV/alt contigs). `PGEN_CACHE_SCHEMA = "v6"` invalidates old caches. Service restarted, all endpoints OK. |
| **Item 3 — pgsc_calc sanity-check harness** | ✅ **shipped + QA'd** (2026-05-15 00:47 UTC) | Nextflow v24.10.5, pgsc_calc workflow, HG002 + `pgsc_1000G_v1.tar.zst` (7.4GB). Ran successfully in 47m 56s with `--min_overlap 0.30` (default 0.75 was too strict for HG002's coverage). FRAPOSA_PCA + FRAPOSA_PROJECT confirmed; PC projection placed HG002 firmly in EUR (RF_P_EUR=0.88). **The headline finding**: pgsc_calc's PC-normalized percentile for HG002+PGS000004 is **78.1** (z=0.84), our pipeline's discrete-EUR-bucket gives **99.5** (z=3.27, clamped). A 21pp gap. This is exactly the gap that Item 4 (continuous PC-regression normalization) closes — our module gets HG002 into the same range pgsc_calc does. Also: pgsc_calc's default match-rate gate is 75 % vs our 60 %, so they're stricter than us by 15pp. |
| **Item 4 — continuous PC-regression normalization** | ✅ shipped (commits 37a93dc, 0e4bb61) | `pipeline/pc_normalization.py` + `scripts/fit_pc_normalization.py` (OLS with mean+log-variance fits). Wired into `_compute_percentile_multipop_wrapper` — augments result with `pc_normalized_percentile_{mean_var,empirical}` when user PCs flow through. `auto_fit_pcnorm.sh` keeps coeffs current as the bulk rebuild progresses. Verified: PGS002746 R²=0.379, EAS-centroid empirical pct=2.0 vs discrete pct=1.2 (within 1pp). |
| **Item 5 — HGDP+1kGP unified reference panel** | ✅ **panel staged** (2026-05-14 22:54 UTC) | `pgsc_HGDP+1kGP_v1.tar.zst` 16GB downloaded + unpacked to `/data/pgsc_refs/pgsc_HGDP+1kGP_v1/`. Both GRCh37 and GRCh38 panels included; GRCh38 pgen 13GB. **Sample inventory**: 3,942 samples (3,123 gnomAD_1kG + 819 gnomAD_HGDP). **Super-pop coverage: AFR 891 / EAS 812 / EUR 770 / CSA 766 / AMR 545 / MID 158** — resolves the previously-`UNSUPPORTED` Middle Eastern bucket with 158 real samples. Fine-grain populations include GWD/CEU/YRI/CHS/IBS/etc. The actual pipeline swap (PCA-cache regen + ref-stats rebuild against new panel) is a separate ~24h operator decision; data is ready to consume. |

Plus an inflight auto-process layer: `auto_fit_pcnorm.sh` runs every
10 min while rebuild is going so PC-norm coefficients track the
growing set of `_scores/*` directories.

### What the second advisor said we already had (no action needed)

- Refuse to default to EUR for ancestry-unresolved samples ✓ (Phase 1.5)
- pfile conversion before scoring ✓ (`_get_or_build_pgen`)
- `--split-par b38` ✓
- Cross-ancestry caveat as deterministic prose ✓ (W0.4 gate template)
- §1.5 refusal "is the right call" ✓ (current behaviour)
- 3-SNP build-validation spot check ✓ (chapter 02 §2.4)
- 1000G as PGS reference ✓ (chapter 06)

---

## Wave 0 + Wave 1 evidence captured 2026-05-14

**Two commits on `genom-beast-gpu:/home/nimrod_rotem/simple-genomics`:**
- `d60100a` — Wave 0 safety gate (7 files, 951 line diff)
- `87dcdb3` — Wave 1 foundations (6 files, 930 line diff)

### Wave 0 — gate is live in production

| Check | Result |
| ----- | --- |
| `pipeline/reason_codes.py` + `result_gate.py` deployed | ✅ commit `d60100a` |
| Gate unit tests (5 cases: sensitive / MID / ADHD-schema-fail / clean / extreme-z) | ✅ all PASS |
| **PGS002746 ADHD exhibit replay (after blessed rebuild)** | ✅ **`INTERPRETABLE`, percentile 19.1 (live), z=-0.873, stats=blessed n=510922** — exact case from the user's exhibit now produces a real number with proper provenance |
| PGS000898 EAS Alzheimer (still-unrebuilt) | `REF_STATS_SCHEMA_INVALID`, percentile blanked, templated prose: *"This is a pipeline data issue, not a biological finding."* (will resolve when bulk rebuild reaches PGS000898) |
| PGS002598 hair-color extreme-z replay | ✅ `EXTREME_Z`, percentile blanked, templated prose explains scale/allele-encoding causes |
| PGS003724 IQ + PGS002012 EduAttain replay | ✅ `TRAIT_HIDDEN_BY_POLICY`, percentile blanked |
| `cross_ancestry_warning` removed from refused results | ✅ on every refused case |
| Scanner v2 → 410 Gone | ✅ `/v2/`, `/v2/api/...`, `/v2` all return 410 |
| Availability matrix snapshot (W0.1 queue) | ✅ **1,924 (PGS×pop) schema-invalid + 39 missing** in `logs/availability_matrix.before-rebuild.jsonl` |
| nginx config valid + reloaded | ✅ `nginx -t` PASS, service running |
| simple-genomics service healthy | ✅ HTTP 200 on `/`, traffic flowing |

### Wave 1 foundations — additive, no production impact

| Check | Result |
| ----- | --- |
| **W1.4 eligibility table** — `pipeline/eligibility_matrix.py` + bootstrap | ✅ 386 PGSes bootstrapped, 7 sensitive flagged `status=hidden`, indexed by status + risk_tier |
| **W1.10 PCA anchor scaffold** — `pipeline/pca_anchors.py` | ✅ 7-anchor registry (HG002 on disk; HG00096/NA12878/HG001/super-pop reps placeholder pending data) |
| **W2.6 CRAM MD5 enforcement** — extension to `pipeline/cram_reference_selection.py` | ✅ `cram_reference_md5_check()` reads @SQ M5, validates against FASTA contig MD5 (cached at `.md5_index`), returns `CRAM_REFERENCE_MD5_MISMATCH` on disagree |
| **W4.5 daily monitor** — `scripts/monitor_daily.sh` + crontab | ✅ availability snapshot + diff vs yesterday + regression alerts at 04:15 UTC |

### W0.1 ref-stats rebuild — running in background

| Check | Result |
| ----- | --- |
| Driver tooling | ✅ `scripts/rebuild_driver.py` — per-PGS atomic (validate all pops → bless all or none) |
| Single-PGS smoke | ✅ PGS002746 rebuild ran in 2 min 5 sec for all 5 working pops; all blessed into registry |
| **Bulk rebuild started 2026-05-14 20:05 UTC** (detached, supervisor-independent) | running; logs to `logs/rebuild_bulk.log` + `logs/rebuild_progress.jsonl` |
| Expected duration | ~12 h for ~362 PGSes at 2 min/PGS |
| MIX pop note | recompute script doesn't currently produce MIX files; tracked as known gap. Not blocking (MIX wasn't user-visible by default anyway) |
| Post-rebuild verification | **gated**: rebuild_summary.json must show 100% ok before W0.2 fallback removal lands |

## Wave 0 — Stop the bleeding (this week)

### W0.1 Regenerate canonical ref-stats for all populations `[~]` (running 2026-05-14 20:09 UTC; 372 PGSes in queue, ~12h ETA)

Resumable run. Status visible via:

- `logs/rebuild_progress.jsonl` — append-only per-PGS record
- `logs/rebuild_summary.json` — final counts
- `logs/rebuild_bulk2.log` — driver stdout
- registry growth: `python3 -c "import json; print(len(json.load(open('/data/pgs2/ref_panel_stats/registry.json'))['entries']))"`

Resume command: `python3 scripts/rebuild_from_matrix.py --resume`

The advisor's P0 #1. Do not weaken validation; rebuild. **Staging →
validate → manifest → atomic promote** (no direct writes to live
registry).

- **Touch**: `scripts/recompute_ref_stats.py`,
  `scripts/ref_stats_registry.py`,
  `pipeline/scoring.py::_rs_validate`,
  `pipeline/scoring.py::_load_stats`,
  `pipeline/config.py::REF_STATS_DIR`,
  `pipeline/registry.py::REGISTRY_PATH`.
- **Decision**: Single canonical store at
  `/data/pgs2/ref_panel_stats/`. Decommission `/data/ref_stats/`
  after migration. The new-store μ/σ are **not** trusted as-is —
  we **rebuild** rather than backfill metadata onto unverified
  values, per advisor's "do not loosen validation blindly."
- **Sequencing**:
  1. Inventory ✅ done — `scripts/availability_matrix.py` snapshot at
     `logs/availability_matrix.before-rebuild.jsonl` is the exact
     queue: 1,924 (PGS × pop) entries blocked by
     `REF_STATS_SCHEMA_INVALID`, 39 by `POPULATION_STATS_MISSING`.
  2. Stage all rebuild output to
     `/data/pgs2/ref_panel_stats/staging/`. Do NOT touch the live
     `/current/` symlink during the run.
  3. Per (PGS, pop, build): run
     `recompute_ref_stats.py --pgs <id> --pop ALL --build GRCh38
     --apply --out-dir <staging>` to compute μ/σ live with
     `plink2-nomi` / `no-mean-imputation`. Also persist the
     `<pop>_scores.npy` array for W1.1 ECDF and the
     `.sscore.vars` list for W1.2 matched-subset stats.
  4. Per file: validate against `_rs_validate`, hash, then write a
     row into `staging/manifest.jsonl` capturing
     `pgs_id, pop, build, n_ref, n_variants, scoring_method,
     imputation_policy, scoring_file_sha256, variant_ids_sha256,
     tool_versions, generated_at`.
  5. Replay anchor samples (HG00096 plus NA12878/HG002 once added)
     against staged stats; bail out if any anchor drifts > 2 pp.
  6. **Atomic promote**: `ref_stats_registry.py promote --from staging
     --to current` swaps the symlink in one transaction.
  7. Delete `/data/ref_stats/` and remove the path from
     `pipeline/config.py` (separate commit, post-promotion).
- **Verification (hard, no <100% allowed)**:
  - HG00096 anchor: regenerated stats give EUR percentiles within
    ±2 pp of pre-incident reports.
  - Nightly `ref_stats_selftest.py` passes for **100%** of public
    curated (PGS × supported pop × build); anything below 100% is
    explicitly blocked with a visible reason code.
  - EAS user replay of any PGS in the rebuild set produces a real
    percentile, NOT `REF_STATS_SCHEMA_INVALID`.
  - Provenance bundle on every new file has `scoring_file_sha256`,
    `variant_ids_sha256`, `tool_versions`, `generated_at`.
- **Budget**: ~8 hours of plink2 wall time on 32-core host; can
  shard by PGS chunks to overlap with normal traffic.

### W0.2 Remove EUR-only fallback asymmetry `[ ]` (immediately after W0.1)

- **Touch**: `pipeline/scoring.py::_load_stats._candidate_stats`.
- **Decision**: Once W0.1 completes, **delete** the EUR-only legacy
  fallback in `_load_stats`. All populations must go through the
  registry. Keeping it is the asymmetry the advisor calls out.
- **Verification**: grep for `_load_legacy_stats` returns zero
  references in the live path; smoke-test confirms EUR percentiles
  still resolve (via registry, not via fallback).

### W0.3 Disable MID percentile output `[x]` (2026-05-14, commit d60100a)

- **Touch**: `pipeline/config.py::POPULATIONS["MID"]`,
  `pipeline/scoring.py::select_reference`, the UI dropdown in
  `app.py`.
- **Decision**: MID stays as an **inferred** ancestry label
  (surfaced from simple-ancestry where applicable) but **never** as
  a percentile reference. When a user PCA-classifies as MID, emit
  `status=ancestry_unsupported` with the per-pop sensitivity array,
  not an EUR fallback.
- **Verification**: no report can contain `selected_ref="MID"`.

### W0.4 Deterministic reason codes `[x]` (2026-05-14, commit d60100a)

Replace the LLM-paraphrased failure messages with structured
machine-readable codes; LLM only renders prose from templates.

- **Touch**: `pipeline/scoring.py::_compute_single_percentile`
  (already emits `method` + `reason` fields), `runners.py`
  `_postprocess_pgs_result`, `app.py::_interpret_result`,
  `pipeline/result_guards.py`.
- **Decision**: Final reason-code enum (extend as needed):
  ```
  REF_STATS_SCHEMA_INVALID        — missing required schema fields
  REF_STATS_VARIANT_HASH_MISMATCH — catalog drift detected
  POPULATION_STATS_MISSING        — no stats file for this pop
  MATCH_RATE_BELOW_THRESHOLD      — match_rate < 60
  Z_SCORE_EXTREME                 — |z| > 6 after computation
  DISTRIBUTION_COLLAPSED          — ref_std < 0.1 × expected
  BUILD_MISMATCH_UNLIFTED         — build mismatch + liftover failed
  UNSUPPORTED_ANCESTRY_PANEL      — MID, OCE, etc.
  ELIGIBILITY_ANCESTRY_MISMATCH   — PGS eval_ancestry doesn't include user pop
  ELIGIBILITY_WEIGHT_TYPE_UNKNOWN — weight_type not in {beta, log_or, log_hr}
  ELIGIBILITY_DIRECTION_UNKNOWN
  ELIGIBILITY_PERFORMANCE_INSUFFICIENT
  CHIP_INPUT_COVERAGE_LOW         — chip + <60% match (block, not just warn)
  ```
- **Template emission**: `app.py::_interpret_result` first looks up
  a deterministic template per reason code; only when the
  outcome is "interpretable" does the LLM get to write free prose.
  The LLM prompt is now downstream of the gate, not upstream of it.
- **Verification**: every result has `failure_reason_code` (or
  null) and `failure_reason_human` (templated string). Negative
  test: no production report can produce the catastrophizing "no
  precomputed ancestral benchmarks" line anywhere.

### W0.5 `/compare` exclude-and-explain `[x]` (2026-05-14, commit d60100a)

- **Touch**: `app.py::_compare_build_for_user`,
  `app.py:13500-` HTML.
- **Decision**: Filtered-out reports surface as an
  **excluded_samples** list per trait, with the reason code
  (`ref_stats_schema_invalid`, `match_rate_below_threshold`,
  `z_score_extreme`, …). Render in the UI as a collapsed footer
  per trait card.
- **Verification**: a user with mixed EAS/EUR samples on a PGS
  where only EUR has registry coverage sees: ranked card for the
  EUR sample, **plus** a row "EAS sample excluded — ref_stats_
  schema_invalid (pending W0.1 backfill)".

### W0.6 Scanner v2 → 410 Gone `[x]` (2026-05-14)

- **Touch**: `/etc/nginx/sites-enabled/23andclaude.com`
  (scanner-v2 location block).
- **Decision**: 410 Gone with explanatory body pointing to v3/v4
  (advisor's "do not repoint stale tools" advice).
- **Verification** ✅: all three v2 paths return 410:
  - `/translocation-scanner-v2/api/health` → 410
  - `/translocation-scanner-v2/` → 410
  - `/translocation-scanner-v2` → 410

### W0.7 Remove sensitive PGSes from public picker `[x]` (2026-05-14, commit d60100a)

- **Touch**: `pgs_curated_list.md`,
  `app.py::_interpret_result` template registry,
  `pipeline/result_guards.py::filter_risk_language`.
- **Decision (advisor's correction)**: not disclaimer — **remove
  from the public UI listings** for now. The PGSes are still
  scorable by explicit ID, but the gate flags them
  `TRAIT_HIDDEN_BY_POLICY` and blanks the percentile. Revisit after
  external ethics review.
- **Verification** ✅: PGS003724 (IQ) and PGS002012 (Educational
  attainment) replay → `TRAIT_HIDDEN_BY_POLICY`, percentile blanked,
  templated prose. Sensitive tests excluded from `/api/tests/tabs`
  counts and the `curated` / `common` tabs.

---

## Wave 1 — Calibration governance (next 4–6 weeks)

### W1.1 ECDF as primary percentile `[ ]`

- **Touch**: `pipeline/ecdf_percentile.py` (already implemented;
  parity-tested), `pipeline/scoring.py::_compute_single_percentile`,
  result schema (`percentile_method`).
- **Decision**: Per-pop reference score arrays
  (`/data/ref_stats/<pgs>/<pop>_scores.npy`) — or equivalent
  quantile sketches — become the primary percentile source. Move
  these into the canonical `/data/pgs2/ref_panel_stats/` store
  alongside the rebuilt JSONs (W0.1). Parametric Φ is fallback for
  PGSes where the NPY array is absent.
- **Report fields**: `percentile, percentile_ci_low,
  percentile_ci_high, percentile_method ∈ {ecdf, parametric_phi},
  n_ref, n_matched`.
- **Verification**: side-by-side parametric vs ECDF on 50 known
  controls; reports flag samples where the two disagree by > 5 pp
  (`ecdf_phi_disagreement` confidence reason).

### W1.2 Matched-subset reference stats `[ ]`

The advisor's "single biggest improvement." Where a user matches
only 65 % of a PGS, recompute the panel's μ/σ on the **same**
variant subset before z'ing.

- **Touch**: New module
  `pipeline/matched_subset_stats.py` (skeleton exists);
  `pipeline/scoring.py::compute_percentile_multipop`;
  `pipeline/config.py` for the panel score NPYs.
- **Decision**: Two paths:
  - **Path A** (correct, slower): for every user score, run
    plink2 `--score` against the panel's pgen restricted to the
    user's matched variant set. Cache by
    `(pgs, pop, matched_variant_ids_sha256)`. First-run cost: a
    few seconds; subsequent runs hit cache.
  - **Path B** (approximate, faster): pre-compute the panel's
    raw per-variant dose×weight contributions and sum on the
    fly for the user's subset. Same cache key.
- **Report fields**: `matched_variant_ids_sha256, n_matched,
  n_total, coverage_pct`, plus
  `reference_method: matched_subset | full_pgs`.
- **Verification**:
  - PGS002598 hair-color exhibit (the z=−18 case at 65 % match)
    re-runs with matched-subset stats and produces a sane z.
  - HG00096 anchor at 100 % match: matched-subset and full
    differ < 0.1 pp.

### W1.3 Three-path scoring agreement test `[ ]`

- **Touch**: New `tests/test_three_path_agreement.py`. CI gate.
- **Decision**: For a curated panel of 10 truth samples × 10
  PGSes (representative of size/density/build), compute scores
  via full-pgen, fast-path, and Pipeline E+. Require agreement
  within `1e-6` for raw_score, `1e-3` for score_sum (floating
  rounding only).
- **Pipeline E+ scope shrinks**: restrict to biallelic SNVs until
  indel/multiallelic/STR-adjacent validation lands. Tag
  `pileup_supported=biallelic_snv_only` in the report.
- **Verification**: CI green; one curated indel PGS in the test
  set must currently fail Pipeline E+ (with explicit
  unsupported-variant reason).

### W1.4 PGS eligibility matrix `[x]` (2026-05-14, commit 87dcdb3)

- **Touch**: `pipeline/eligibility_gates.py` (extend), new
  `/api/pgs/{pgs_id}/eligibility` endpoint,
  `pgs_pipeline.db.pgs_eligibility` table.
- **Decision**: Per-PGS row capturing: allowed_ancestries,
  validated_ancestries, original_gwas_ancestry, evaluation_auc,
  evaluation_r2, weight_type, weight_transform, variant_count,
  supported_input_types, sex_restriction, percentile_eligibility,
  effective_n_variants (LD-collapsed), trait_class, social_risk_tier.
- **Verification**: every PGS in the curated list has a row; the
  UI's PGS picker shows eligibility status before run, not just
  after.

### W1.5 Provenance bundle on every report `[ ]`

- **Touch**: `pipeline/score_provenance.py` (exists, expand),
  `runners.py::_postprocess_pgs_result` to attach.
- **Decision**: Provenance dict on every result:
  ```
  pipeline_version, plink2_version, bcftools_version,
  samtools_version, scoring_method, imputation_policy,
  weight_transform, ref_panel_path, ref_panel_md5,
  ref_stats_path, ref_stats_sha256, pgs_catalog_url,
  pgs_catalog_release_date, scoring_file_sha256,
  matched_variant_ids_sha256, ancestry_method,
  ancestry_reference_panel, build_validation_summary,
  scoring_started, scoring_completed, host,
  failure_reason_code (nullable)
  ```
- **Verification**: every JSON in `users/<u>/reports/` carries
  the bundle; old reports get it on next read via
  `live_percentile.apply_live_overlay`.

### W1.6 1000G panel audit `[ ]`

The advisor's P0 #6.

- **Touch**: `pipeline/config.py::POPULATIONS`,
  `pipelinesdocsv2/06-reference-panel.md`, every report's
  `pipeline_info.reference_panel` string.
- **Decision**: Rename "1000 Genomes Phase 3" →
  "1000G NYGC high-coverage GRCh38 re-release" where the file we
  use is the 3,202-sample one. Document KING relatedness filter
  (`king.cutoff.out`). Publish per-pop sample counts post-filter
  in the report.
- **Verification**: every report shows the corrected name + n_ref;
  chapter 06 of this packet updated.

### W1.7 Visible per-result confidence header `[ ]`

- **Touch**: UI templates in `app.py`, plus the API result schema.
- **Decision**: Every report's header shows: ancestry inference
  (label + confidence), matched variants (n / total / %),
  reference panel + n, percentile source (ecdf / parametric /
  unavailable), calibration status (validated_for_ancestry /
  transfer / unsupported). Per advisor's P1 reporting accuracy.
- **Verification**: visual inspection across 20 reports
  representing every combination of ancestry × failure mode.

### W1.8 Match-rate thresholds PGS-specific `[ ]`

- **Touch**: `pipeline/eligibility_gates.py`, per-PGS row in the
  eligibility matrix (W1.4).
- **Decision**: Replace the global 60 / 85 with per-PGS thresholds
  derived from the panel's own match-rate distribution. PGSes with
  panel median match < 95 % get a wider tolerance band; ultra-dense
  PGSes (>500K variants) tighten. Document the rule.
- **Verification**: PGS-specific thresholds visible in the
  eligibility matrix; reports show
  `match_rate_threshold_source: per_pgs | default`.

### W1.9 Continuous-ancestry residual percentile (research item) `[~]`

- **Touch**: new `pipeline/ancestry_residual.py`. Research, not
  shipped to the live percentile path.
- **Decision**: Regress panel score on ancestry PCs (PC1..PC4),
  compute `residual = score − Ê[score | PCs]`. Residual
  percentile is ancestry-independent under the linear-additive
  assumption. Build alongside ECDF, compare for the curated 50
  PGSes, and decide post-evaluation.
- **Verification**: research notebook in `simple-genomics/docs/`
  with a side-by-side residual-vs-bucketed-vs-ECDF comparison on
  truth samples.

### W1.10 PCA QC anchors `[~]` (scaffold 2026-05-14, commit 87dcdb3; data pinning pending)

- **Touch**: `pipeline/pca_projection_validation.py` (HG00096
  exists), extend to NA12878, HG002, and one rep per super-pop.
- **Decision**: Each anchor sample has pinned PC1..PC4, pinned
  super-pop assignment, pinned admixture proportions; CI fails
  on any drift > ε. Anchor list becomes a hard pre-score gate,
  not just CI.
- **Verification**: CI green; manual `plink2 --pca` rebuild gives
  matching coordinates.

---

## Wave 2 — Input handling and normalization (in parallel with W1)

### W2.1 Strengthen build detection `[ ]`

- **Touch**: `runners.py::_validate_genome_build`.
- **Decision**: Expand the 3-SNP panel to ~50 well-characterized
  SNPs spanning autosomes + chrX + MT. Three SNPs is too narrow
  for low-coverage data.
- **Verification**: synthetic VCFs that hide the original 3 SNPs
  must still classify correctly with the expanded panel.

### W2.2 Normalize-before-score on every VCF `[ ]`

- **Touch**: `runners.py::_get_or_build_pgen` (insert pre-step),
  new `pipeline/normalize.py`.
- **Decision**: `bcftools norm -f <ref> -m - -c x` (or `-c w` per
  CRAM origin), then atomize multiallelics, then assign canonical
  IDs (`CHR:POS:REF:ALT`). REF check on every record. Reject
  records that fail REF check beyond a small fraction.
- **Verification**: a sample with a known REF-mismatched VCF
  (deliberately mis-built) is rejected with
  `BUILD_MISMATCH_UNLIFTED`, not silently scored.

### W2.3 Palindromic SNP policy `[ ]`

- **Touch**: `runners.py::_recover_strand_flips` (extend),
  `pipeline/match_logic.py`.
- **Decision**: A/T and C/G SNPs are dropped unless allele
  frequency disambiguates (panel AF agreement within 0.05). Count
  surfaced as `palindromic_dropped` in `scoring_diagnostics`.
- **Verification**: a PGS with N palindromic SNPs reports the
  drop count; manual spot-check confirms drops are correct calls.

### W2.4 Activate strand-flip recovery `[ ]`

The PGS002746 ADHD exhibit had `strand_flip_recoverable: 3561`,
`strand_flip_applied: false`. That's a known leak.

- **Touch**: `runners.py::_recover_strand_flips` (lift the
  inert flag).
- **Decision**: Enable when `skipped_due_to_mismatching_allele_code
  >= 1% of total variants` (per advisor) AND `non_palindromic
  recoverable count >= threshold`.
- **Verification**: PGS002746 ADHD replay shows
  `strand_flip_applied: true`, recovered count > 0, match-rate
  unchanged or improved.

### W2.5 gVCF placeholder ALT rewrite hardening `[ ]`

- **Touch**: `pipeline/gvcf_ref_aware_rewrite.py`,
  `runners.py::_normalize_gvcf`.
- **Decision**: `<*>`/`<NON_REF>` rewrite only when (a) the REF
  base matches the panel REF, (b) the gVCF block has DP ≥ 8 and
  GQ ≥ 20 at the position, (c) the catalog's effect allele is
  unambiguous. Otherwise set genotype to missing — better to lose
  the variant than to false-call.
- **Verification**: new test in `test_gvcf_refblock.py` for
  low-GQ / shallow blocks (must set missing); high-GQ blocks
  produce the rewritten ALT.

### W2.6 CRAM reference MD5 enforcement `[x]` (2026-05-14, commit 87dcdb3)

- **Touch**: `pipeline/cram_reference_selection.py`,
  `runners.py::_pick_reference_for`.
- **Decision**: Read the CRAM header's `@SQ M5:` tags; require
  that the chosen reference FASTA's per-contig MD5 matches.
  Otherwise refuse to decode. (`samtools view --reference` with
  a non-matching MD5 silently misreads — don't allow that.)
- **Verification**: a CRAM whose header MD5 doesn't match any
  on-disk FASTA is rejected with
  `CRAM_REFERENCE_MD5_MISMATCH`.

### W2.7 `hs38DH` vs no-alt analysis set `[ ]`

- **Touch**: `pipeline/cram_reference_selection.py`,
  `pipeline/config.py`.
- **Decision**: Default reference becomes the **no-alt GRCh38
  analysis set** for PGS scoring (per advisor). `hs38DH` is
  permitted only when the aligner/caller is explicitly ALT-aware
  AND the PGS doesn't include alt/decoy contigs.
- **Verification**: `pipeline_info.reference_fasta` notes
  `assembly_variant: no_alt | hs38dh_alt_aware` per report.

---

## Wave 3 — Specialized tools and clinical layer (sequenced after W1)

### W3.1 Pin tool versions in containers `[ ]`

- **Touch**: new `docker/` directory (or `containers/`), pinned
  `Dockerfile`s per tool, `Makefile` build targets,
  supervisor configs to invoke containers.
- **Decision**: One pinned trio of htslib/samtools/bcftools (1.23.1).
  Per-Java-tool containers (HaploGrep3, etc.). plink2 + plink 1.9
  with their respective stable tags. Record image digest in
  provenance bundle.
- **Verification**: every tool invocation logs container digest;
  digests appear in W1.5 provenance.

### W3.2 ClinVar XML + VCF dual ingestion `[ ]`

- **Touch**: `runners.py::run_clinvar_screen`, new
  `pipeline/clinvar_xml.py`.
- **Decision**: VCF for fast precise-allele intersection; XML
  for assertion details (review status, condition specificity,
  submitter, date). Display surfaces both — review status + condition
  are required to render a "Pathogenic" label.
- **Verification**: a ClinVar 1-star vs 3-star pathogenic variant
  renders differently; CNV variants present only in XML are
  flagged (or excluded by design with a note).

### W3.3 STR / HLA / Cyrius input-aware behavior `[ ]`

- **Touch**: `runners.py::_run_expansion_hunter`,
  `runners.py::_run_hla_typing`,
  `runners.py::_run_cyrius_star_caller`.
- **Decision**:
  - ExpansionHunter: emit `locus_validated_for_input` per locus;
    chip / low-coverage inputs get a hard refuse for non-validated
    loci.
  - T1K: low-depth / array → "low_confidence" or "unsupported."
  - Cyrius: refuse below 30× WGS; preserve ambiguity states.
- **Verification**: chip input running ExpansionHunter on a
  validated locus is OK, on a non-validated locus returns
  `UNSUPPORTED_FOR_INPUT_TYPE`.

### W3.4 SV scanner truth-set benchmarking `[ ]`

- **Touch**: `translocation-scanner-v4/tests/`, new
  `benchmarks/` directory.
- **Decision**: Run v4 against the GIAB HG002 SV truth set; report
  precision/recall per SV class. Publish at
  `/translocation-scanner-v4/benchmarks/` for transparency.
- **Verification**: numbers on the dashboard. Where v4 falls
  short, the report header tier-classifies "research only."

### W3.5 Retire scanner v2 / v3 from the public surface `[ ]`

- **Touch**: nginx config; UI links.
- **Decision**: v4 is the default. v2 and v3 endpoints either
  removed or behind an explicit `/legacy/` prefix that loads with
  a deprecation banner. (W0.6 is the urgent fix; this is the
  graceful retirement.)

---

## Wave 4 — Reliability and ops (continuous)

### W4.1 Availability matrix + alerting `[ ]`

- **Touch**: new `scripts/availability_matrix.py`, cron entry,
  `logs/availability_matrix.jsonl`.
- **Decision**: Nightly: for every (PGS, ancestry, build, method)
  combination, record `available: true | false` with the reason.
  Alert on drops or schema-refusal spikes.

### W4.2 Atomic ref-stats promotion `[ ]`

- **Touch**: `scripts/ref_stats_registry.py::bless`.
- **Decision**: New file written to staging dir → validated →
  `os.replace`'d into the registered location → registry pointer
  updated in a single SQLite transaction. No partial writes.

### W4.3 Blue/green data deployments `[ ]`

- **Touch**: new `data_bundles/` layout under `/data/`,
  per-bundle symlinks the live process resolves.
- **Decision**: PGS catalog, ClinVar, ref stats, ancestry panel,
  ExpansionHunter catalog, T1K reference, FASTA each have a
  `current/` symlink. Refresh writes to `next/`; promotion is an
  atomic symlink swap; rollback is the inverse.

### W4.4 Synthetic + golden end-to-end tests `[ ]`

- **Touch**: `tests/e2e/`, fixture genomes in
  `tests/fixtures/`.
- **Decision**: VCF, gVCF, BAM, CRAM, 23andMe text inputs ×
  multiple builds × representative PGSes. HG001/NA12878, HG002,
  one rep per super-pop. Synthetic edge cases: palindromic SNPs,
  multiallelics, indels, missing genotypes, symbolic ALTs,
  chr-prefixed vs bare, accession-style contigs.

### W4.5 Live distribution monitoring `[x]` (2026-05-14, commit 87dcdb3)

- **Touch**: extend `scripts/cron_cohort_sanity.py`.
- **Decision**: Per-population KS-vs-uniform, z-inflation rate,
  percentile-clamp rate, match-rate drops, schema-refusal spikes,
  population-coverage gaps.

### W4.6 Versioned docs `[ ]`

- **Touch**: this packet.
- **Decision**: Once chapters 14-15 are reviewed, freeze
  `/pipelinesdocsv2/` in nginx (Cache-Control immutable, no
  directory listing). Future revisions go to `/pipelinesdocsv3/`.

---

## Cross-wave: the LLM interpretation contract

The advisor flags LLM-paraphrased messages as risk. The contract
we're committing to:

1. **LLM never decides** correctness, severity, medical meaning,
   or failure cause. Failure cause is a reason code (W0.4).
2. **Deterministic template per reason code.** When percentile is
   absent, the user-facing string is generated from a string
   template, not from the LLM. Example:
   ```
   No percentile available — reason: REF_STATS_SCHEMA_INVALID.
   Reference statistics for {pop} on {pgs_id} failed our schema
   validation contract (missing required metadata fields). Raw
   score was computed (raw={raw_score}) but cannot be interpreted
   relative to a population without validated reference statistics.
   This is a pipeline issue, not a biology issue; we are
   regenerating the affected statistics (W0.1 in the action plan).
   ```
3. **LLM-generated prose only when interpretable.** When a
   percentile renders, the LLM may write explanatory text but the
   directional-language filter (`pipeline/result_guards.py::
   filter_risk_language`) gates it post-hoc against the
   eligibility verdict.
4. **Audit log every LLM call** — model, prompt, response,
   user-visible field — so we can replay any decision.

---

## Open questions for the advisor (round 2)

A few items we deliberately want to come back on after Wave 0:

- **Continuous ancestry adjustment (W1.9)** vs admixed reference
  distributions. Which gives more honest percentiles for genuine
  AMR / mixed-ancestry users — residual approach or PC-nearest-
  neighbor sub-panels?
- **PRS-CSx for the curated ~250 PGSes**: scope, cohort
  requirements, validation rubric.
- **Removing socially sensitive PGSes (W0.7)** vs putting them
  behind disclaimers: would the advisor write the disclaimer text
  themselves, or recommend external ethics review first?
- **Replacing 1000G with HGDP+1kGP throughout** (today only
  simple-ancestry uses it): worth the panel rebuild + ref-stats
  recompute, or layer HGDP on top of the existing panel?
