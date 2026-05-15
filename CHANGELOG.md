# Changelog

All material changes to the 23andclaude PGS pipeline. Each section
describes the user-facing or operator-facing impact, not the bare
diff — see `git log` for line-level history.

## 2026-05-14 / 2026-05-15 — Wave 0 + Wave 1 + Items 1-5 (advisor-driven rebuild)

Single, intensive session driven by two successive bioinformatics
advisor reviews. Every change below is committed to `main` of this
repo; supporting documentation lives at
<https://23andclaude.com/pipelinesdocsv2/>.

### Wave 0 — deterministic interpretability gate (commit `d60100a`)

The first advisor flagged that the pipeline was conflating
"raw score computed" with "interpretable percentile" and that
failure messages were LLM-paraphrased rather than deterministic.
Wave 0 fixes that.

**Why**: EAS / non-EUR users were silently dropped from `/compare`
results and the LLM was generating doomy prose like *"no precomputed
ancestral benchmarks"* — which doesn't actually map to a structured
failure reason in our code.

**What changed**:
- New `pipeline/reason_codes.py` — 19-enum failure-reason taxonomy
  (`REF_STATS_SCHEMA_INVALID`, `Z_SCORE_EXTREME`, `MATCH_RATE_BELOW_THRESHOLD`,
  `UNSUPPORTED_ANCESTRY_PANEL`, `TRAIT_HIDDEN_BY_POLICY`, etc.) plus a
  deterministic templated-prose registry. The LLM is no longer
  authorised to invent failure copy.
- New `pipeline/result_gate.py` — `apply_gate(result)` is the single
  chokepoint every PGS result passes through (fresh scores and
  read-time overlay on old reports). When the gate refuses,
  `percentile` is blanked, `interpretability_status` + `failure_reason_code`
  + `failure_reason_human` are set, and the catastrophizing
  `cross_ancestry_warning` is removed in favour of the templated reason.
- `runners.py::_postprocess_pgs_result` — final-step gate invocation
  before any UI sees the result.
- `pipeline/live_percentile.py` — try/finally wrap so the gate runs
  on EVERY return path including the schema-fail early return.
- `app.py::_interpret_result` — LLM short-circuits to templated prose
  when `interpretability_status != "INTERPRETABLE"`. The LLM is only
  allowed to elaborate on interpretable results.
- `app.py::_compare_build_for_user` — every silently-dropped sample
  becomes a row in `excluded_samples[]` with a reason code. No more
  silent biased rankings.
- `app.py::_api_tests` — sensitive PGSes (intelligence, IQ, cognitive,
  educational attainment, income) filtered from the curated/common
  test pickers. Still scorable by explicit ID, but the gate flags
  them `TRAIT_HIDDEN_BY_POLICY` and blanks the percentile.
- `pipeline/scoring.py::select_reference` — MID with ≥40 % posterior
  share returns `primary="MID"` so the gate maps to
  `UNSUPPORTED_ANCESTRY_PANEL` rather than fall back to EUR/MIX.
- `/etc/nginx/sites-enabled/23andclaude.com` — `/translocation-scanner-v2/*`
  returns HTTP 410 Gone with a body pointing at v3/v4. Old endpoint
  was upstream-dead on a terminated VM.
- New `scripts/availability_matrix.py` — read-only inventory of
  every (PGS × population × build) combination + the reason it's
  blocked. Pre-rebuild snapshot logged: **1,924 (PGS × pop) entries
  blocked by `REF_STATS_SCHEMA_INVALID`, 39 missing**.

**Live verification (production, 2026-05-14)**:
- ADHD exhibit replay (PGS002746 EAS) → `REF_STATS_SCHEMA_INVALID`,
  percentile blanked, templated prose: *"This is a pipeline data
  issue, not a biological finding."*
- Hair-color PGS002598 z=−18.26 → `EXTREME_Z`, percentile blanked.
- PGS003724 IQ + PGS002012 EduAttain → `TRAIT_HIDDEN_BY_POLICY`,
  percentile blanked.
- `/translocation-scanner-v2/api/health` → 410.
- 386 eligibility rows bootstrapped; 7 sensitive flagged hidden.

### Wave 1 — foundations (commit `87dcdb3`)

Additive infrastructure that the rebuild + future work depends on.

- New `pipeline/eligibility_matrix.py` + `pgs_pipeline.db.pgs_eligibility`
  table — per-PGS policy + calibration metadata: `trait_class`,
  `social_risk_tier`, `allowed_ancestries`, `validated_ancestries`,
  `weight_type`, `percentile_eligible`, `status`. Bootstrapped 386
  rows from `/data/pgs_cache/<PGS>/meta.json`.
- New `pipeline/pca_anchors.py` — 7-anchor registry for cross-pipeline
  PCA QC. HG002 fixture present on disk; HG00096 / NA12878 / one-per-
  super-pop placeholders pending data.
- Extended `pipeline/cram_reference_selection.py` — `cram_reference_md5_check()`
  reads CRAM `@SQ M5:` tags, validates against per-contig FASTA MD5
  (cached at `.md5_index`), returns `CRAM_REFERENCE_MD5_MISMATCH` on
  disagree.
- New `scripts/rebuild_driver.py` — atomic per-PGS rebuild driver.
  Per-PGS-atomic: if any pop fails validation, registry isn't updated.
- New `scripts/rebuild_from_matrix.py` — fast queue source from the
  availability-matrix snapshot (avoids the O(n × pops) live SHA
  re-compute that made full enumeration prohibitively slow).
- New `scripts/monitor_daily.sh` — 04:15 UTC cron, captures the
  availability matrix and diffs against yesterday. Alerts on
  regressions.

### Wave 5 — second-advisor recommendations (Items 1-5)

Commits `37a93dc`, `0e4bb61`, `dbec33c`.

**Item 1 — per-sample panel-score dumps (commit `37a93dc`)**

  - `scripts/recompute_ref_stats.py` now writes
    `_scores/<PGS>/<POP>_scores.npy` (structured `[avg, sum, ct]` dtype)
    + `<POP>_sample_ids.txt` alongside the canonical JSON.
  - `scripts/dump_panel_scores.py` back-fills already-blessed PGSes.
  - Smoke verified: PGS002746 EAS_scores.npy n=585, μ=−0.001441 σ=0.000153
    matches the blessed JSON exactly.

**Item 2 — `--chr 1-22,X,Y,XY` filter + pgen cache schema v6 (commit `37a93dc`)**

  - `runners._get_or_build_pgen` and the fast-path pgen build now
    restrict to autosomes + X/Y/PAR per the pgsc_calc recipe.
    Excludes chrM, EBV, alt contigs that pollute PCA.
  - `PGEN_CACHE_SCHEMA` bumped `"v5"` → `"v6"`; old caches invalidate
    automatically.

**Item 3 — pgsc_calc sanity-check harness (commit `0e4bb61`)**

  - Nextflow v24.10.5 installed at `~/tools/nextflow`.
  - `pgsc_1000G_v1.tar.zst` (7.4 GB) downloaded to `/data/pgsc_refs/`.
  - `scripts/auto_run_pgsc_calc.sh` runs the full pgsc_calc workflow
    (`pgscatalog/pgsc_calc`) on HG002 against PGS000004 + PGS000007
    via Docker, `--run_ancestry pgsc_1000G_v1.tar.zst`.
  - **Headline finding (2026-05-15 00:47 UTC)**: pgsc_calc completed
    in 47 m 56 s, 23 stages ✔. PC-normalized percentile for
    HG002 + PGS000004 = **78.1** (z=0.84). Our discrete-EUR-bucket
    pipeline gives the same sample **99.5** (z=3.27, clamped from
    higher). **A 21 pp gap**, attributable entirely to PC-based
    normalization vs discrete bucket μ/σ. Both pipelines agree
    HG002 is EUR (RF_P_EUR=0.88).
  - pgsc_calc's default match-rate gate is **75 %** vs our 60 %.
    They are stricter than us by 15 pp.
  - Audit artifacts at `logs/pgsc_calc_HG002/`: `HG002_pgs.txt.gz`,
    `pop_summary.csv`, `HG002_summary.csv`,
    `HG002_popsimilarity.txt.gz`.

**Item 4 — continuous PC-regression normalization (commits `37a93dc`, `0e4bb61`)**

  - New `pipeline/pc_normalization.py` — per-PGS regression coefficients
    `score ~ β₀ + β·PCs` (mean) and `log(var) ~ γ₀ + γ·PCs` (heteroskedastic
    variance fit). Exposes `mean | mean+var | empirical` percentile methods.
    Closes the methodological gap the second advisor flagged
    (pgsc_calc's `--normalization_method`).
  - New `scripts/fit_pc_normalization.py` — OLS fitter against the
    panel's per-PGS raw scores (item 1's NPYs) plus
    `pca_1000g/ref.eigenvec`. With `--resume` (default) skips already-fit.
  - `runners._compute_percentile_multipop_wrapper` — emits
    `pc_normalized_percentile_mean_var`, `pc_normalized_percentile_empirical`,
    `pc_normalized_status` alongside the discrete-bucket percentile so
    the UI / LLM / reviewer can see both numbers and any disagreement.
  - New `scripts/auto_fit_pcnorm.sh` — 10-min loop that keeps PC-norm
    coefficients in sync as the bulk rebuild grows the `_scores/`
    inventory.
  - Verified: PGS002746 R²=0.379. EAS-centroid → PC-norm empirical
    percentile **2.0** vs discrete EAS **1.2** (1 pp).

**Item 5 — HGDP+1kGP unified reference panel (commit `dbec33c`)**

  - `pgsc_HGDP+1kGP_v1.tar.zst` (16 GB) downloaded + unpacked to
    `/data/pgsc_refs/pgsc_HGDP+1kGP_v1/`. Includes BOTH GRCh37 and
    GRCh38 panels; GRCh38 pgen 13 GB.
  - New `scripts/auto_setup_hgdp.sh` — polls for download completion,
    unpacks, inventories.
  - **Panel inventory**: 3,942 samples (3,123 gnomAD_1kG + 819
    gnomAD_HGDP). Super-pop coverage: AFR 891 / EAS 812 / EUR 770 /
    CSA 766 / AMR 545 / **MID 158**. Resolves the previously-
    `UNSUPPORTED` Middle Eastern bucket with real samples for the
    first time.
  - The actual pipeline swap (PCA-cache regen + ref-stats rebuild
    against the new panel) is a separate operator decision — ~24 h
    of plink2 work, not yet executed.

### W0.1 / W0.2 — canonical ref-stats rebuild (in flight)

  - `scripts/rebuild_from_matrix.py --resume` running detached.
    Reads the pre-rebuild availability snapshot for the queue source.
    Per-PGS: invokes `recompute_ref_stats.py --pop ALL`, validates
    each output via the strict contract, then blesses into
    `/data/pgs2/ref_panel_stats/registry.json`.
  - Status at commit time: ~100 / 372 PGSes done, registry up from
    57 unique PGSes to ~140+. ETA ~12 h.
  - W0.2 (EUR-fallback removal in `_load_stats`) is gated on this
    completing 100 %, and is a one-line change once it does.

### Operational

- Daily monitor cron at 04:15 UTC (`scripts/monitor_daily.sh`)
  captures availability matrix, diffs against yesterday, alerts on
  regressions.
- The `pipelinesdocsv2/` bioinformatics review packet is at
  <https://23andclaude.com/pipelinesdocsv2/>. Chapters 14 (advisor
  reviews) and 15 (action plan) are the audit trail.
