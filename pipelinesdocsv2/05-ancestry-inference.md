# 05 — Ancestry Inference

Ancestry inference is the upstream input to reference-population
selection. It is **not** used to alter PGS weights — the scoring file is
the same regardless of ancestry — but it decides which (PGS × pop) μ/σ
JSON the percentile path loads.

## 5.1 The big-picture pipeline

```
user VCF / gVCF / BAM / CRAM
        │
        ▼
    PCA-site genotypes
    (~106K LD-pruned SNPs at /data/pgs_cache/pca_1000g/)
        │
        ▼
    plink2 --score ref.eigenvec.allele
       no-mean-imputation variance-standardize
       --read-freq ref.afreq
        │
        ▼
    user PC1..PC10
        │
        ├──► nearest super-pop centroid
        │    (EUR, EAS, AFR, SAS, AMR over PC1..PC4)
        │       │
        │       ▼   confidence high / moderate / low
        │   user.super_pop = "EAS"
        │
        ▼
    inverse-distance weighting → admixture proportions
    {EUR: 0.05, EAS: 0.83, AFR: 0.04, SAS: 0.05, AMR: 0.03}
        │
        ▼
    pipeline/scoring.select_reference()
       top ≥ 0.80 → primary = top
       else → primary = "MULTI"  (Phase 1.5)
        │
        ▼
    primary, secondary = chosen ref pop + neighbors
```

## 5.2 PCA reference cache (`runners._build_pca_reference_cache`)

Built once at first run, stored at `/data/pgs_cache/pca_1000g/`:

```
pca_1000g/
├── ref.eigenvec.allele     per-variant PC weights (plink2 projector)
├── ref.afreq               read-freq for projection
├── ref.eigenvec            per-sample PC coordinates (centroid source)
├── ref.eigenval            eigenvalues (we report PC1..PC5)
└── ref.psam                super_pop labels per 1000G sample
```

Two-stage build:

```bash
# Stage 1 — LD-prune the panel (autosomes only, MAF ≥ 5%, missing ≤ 2%, biallelic SNPs)
plink2 --pfile GRCh38_1000G_ALL vzs \
    --allow-extra-chr \
    --chr 1-22 --maf 0.05 --geno 0.02 --snps-only --max-alleles 2 \
    --rm-dup force-first \
    --indep-pairwise 1000 50 0.1 \
    --threads 16 --memory 48000 \
    --out ref_pruned_ld
# → ref_pruned_ld.prune.in : ~106K LD-pruned variant IDs

# Stage 2 — PCA + freqs on the pruned set
plink2 --pfile GRCh38_1000G_ALL vzs \
    --extract ref_pruned_ld.prune.in \
    --rm-dup force-first \
    --freq \
    --pca 10 approx allele-wts \
    --threads 16 --memory 48000 \
    --out ref
```

`--pca ... approx allele-wts` produces `eigenvec.allele` directly so
we never need to project against the dense pgen at score time.

## 5.3 User projection (`_run_pca_1000g`)

```bash
plink2 --pfile <user> \
    --read-freq pca_1000g/ref.afreq \
    --score pca_1000g/ref.eigenvec.allele 2 5 header-read \
        no-mean-imputation variance-standardize \
    --score-col-nums 6-15 \
    --allow-extra-chr \
    --out projected
```

`variance-standardize` is **required**. Without it the projected
coordinates do not live in the same space as `ref.eigenvec` (the
centroids) and super-pop assignment is unreliable. The 2026-02 PCA
misclassification incident traced back to a missing
`variance-standardize`; the regression is now covered by the
`pipeline/pca_projection_validation.py` anchor-fixture test which
re-projects HG00096 and checks PC1..PC4 against pinned coordinates.

## 5.4 Super-population assignment

`_compute_pca_centroids(ref.eigenvec, ref.psam)` averages per-super-pop
PCs:

```python
centroids = {
    "EUR": (mean_PC1_EUR, mean_PC2_EUR, ..., mean_PC4_EUR),
    "EAS": (mean_PC1_EAS, mean_PC2_EAS, ..., mean_PC4_EAS),
    "AFR": (...), "SAS": (...), "AMR": (...),
}
```

Assignment is by **min Euclidean distance over PC1..PC4** (PC5..PC10
are reported but not used for the centroid decision — they're typically
noise for super-pop scale). Confidence:

- `high` — 2nd-closest super-pop is ≥30% farther than closest
- `moderate` — gap is 10–30%
- `low` — gap is <10%

## 5.5 Admixture estimation (`_run_admixture_from_pca`)

We do **not** run ADMIXTURE / RFMix today. We approximate admixture
proportions from PCA distances by inverse-distance weighting against
the 5 super-pop centroids over PC1..PC4:

```python
def admixture_from_pca(user_pc, centroids):
    dists = {pop: euclid(user_pc, c) for pop, c in centroids.items()}
    inv = {pop: 1.0 / (d + epsilon) for pop, d in dists.items()}
    total = sum(inv.values())
    return {pop: v/total for pop, v in inv.items()}
```

Output:

```json
{
  "EUR": 0.612,
  "EAS": 0.108,
  "AFR": 0.075,
  "SAS": 0.082,
  "AMR": 0.123,
  "reason": "pc_distance_weighting"
}
```

**Properties** the reviewer should know:

- This is a smooth function of PC distance; it never returns a hard 0
  for any population.
- It cannot resolve, e.g., 70%EUR/30%AMR vs 60%EUR/40%AMR with
  confidence. The reviewer's call: is this good enough for selecting
  the ref-stats population, or do we need supervised projection (RF /
  SVM on the eigenvec) or true ADMIXTURE?
- "Ancestry not in 1000G" (Middle Eastern, Pacific Islander,
  Indigenous Australasian) produces an admixture vector that's a
  smooth blend of the 5 super-pops but doesn't flag itself as such.
  Phase 1.5 plans for an `unsupported: True` flag from a separate
  classifier; currently absent.

## 5.6 Reference selection (`pipeline/scoring.select_reference`)

```python
def select_reference(ancestry_result, pgs_id, genome_build="GRCh38"):
    POPS_5 = ["EUR", "EAS", "AFR", "SAS", "AMR"]
    if not ancestry_result:
        return RefSelection(
            primary="UNRESOLVED",
            secondary=POPS_5,
            reason="no_ancestry_data — multi-pop array required (§1.5)",
        )
    if ancestry_result.get("unsupported"):
        return RefSelection(primary="UNSUPPORTED", ...)

    proportions = ancestry_result.get("proportions") or \
                  ancestry_result.get("admixture") or {}
    if not proportions:
        return RefSelection(primary="UNRESOLVED", ...)

    sorted_pops = sorted(proportions.items(), key=lambda x: -x[1])
    top_pop, top_prop = sorted_pops[0]
    if top_prop >= 0.80:
        secondaries = [p for p, _ in sorted_pops[1:3] if p != top_pop]
        return RefSelection(
            primary=top_pop,
            secondary=secondaries,
            reason=f"single_cluster ({top_pop}={top_prop:.0%})",
        )
    # Posterior < 0.80 → multi-pop array; no fixed MIX (Phase 1.5).
    return RefSelection(
        primary="MULTI",
        secondary=POPS_5,
        reason=f"admixed (top={top_pop}={top_prop:.0%} < 0.80)",
    )
```

Phase 1.5 (2026-05-14) removed:

- The legacy "default EUR" for `UNRESOLVED` cases. We now emit a
  multi-pop array and an explicit `status="ancestry_unresolved"`. The
  user sees a sensitivity grid instead of a silent EUR percentile.
- The fixed `MIX = 50% EUR + 50% EAS` for admixed cases. `MIX` is still
  a buildable population for backfilled stats but is no longer the
  automatic answer for "top posterior < 0.80".

## 5.7 Manual override

The UI lets the user pin a population per-test (and per-PGS). The API
endpoints:

- `GET /api/pgs/{pgs_id}/refs` — list available ref pops for a PGS.
- `POST /api/pgs/{pgs_id}/recompute?ref_pop=EAS` — recompute the
  percentile against a different pop, no re-scoring needed (just
  reload μ/σ).

On a manual override, `_ancestry_hint = {ref_pop: 1.0}` is passed
through and `ancestry_model="user_pinned:EAS"` is logged.

## 5.8 PCA QC (`pipeline/pca_projection_validation.py`)

Two checks run on every PCA projection:

1. **Anchor fixture**: HG00096 (a well-known British 1000G EUR sample)
   re-projected through the live pipeline must land within ε of pinned
   PC1..PC4 coordinates. Any drift > 0.01 in any PC fails the test.
2. **Within-cohort scatter**: when scoring a family / batch, all
   members should cluster (median pairwise PC distance < panel median
   between unrelated samples). Excessive scatter implies a contig-
   naming mismatch or a partial pgen.

These checks are CI-runnable
(`tests/test_pca_projection.py`) but not blocking in production yet.

## 5.9 What ancestry inference does not do

- **No PRS-CSx**: we do not produce ancestry-specific PGS weights. The
  same EUR-trained scoring file is applied to every user regardless of
  detected ancestry.
- **No fine-scale admixture**: no chromosomal-painting, no local
  ancestry. We have super-pop labels only.
- **No HGDP, no SGDP, no 1000G phase 4 / NYGC 30× re-release**: we
  pin to 1000G phase 3 GRCh38 for the panel and centroids. Adding
  HGDP would improve coverage of underrepresented populations
  (Middle Eastern, Pacific) — open question for the reviewer.

## 5.10 The signal handed to the rest of the pipeline

The `ancestry_result` dict consumed by `select_reference` always has
the same shape:

```json
{
  "primary_super_pop":   "EAS",
  "primary_confidence":  "high",
  "proportions": {
    "EUR": 0.05, "EAS": 0.83, "AFR": 0.04, "SAS": 0.05, "AMR": 0.03
  },
  "method":              "pca_inverse_distance",
  "n_pca_variants":      102876,
  "pc1_through_pc5":     [-0.041, 0.082, 0.013, -0.005, 0.004],
  "unsupported":         false
}
```

This is the single source of truth — every downstream scoring
invocation, every override path, and every audit log resolves back
to this object.
