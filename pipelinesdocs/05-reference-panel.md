# 05 — Reference Panel and Ancestry-Aware Reference Selection

The pipeline's percentile estimates and ancestry assignments are all
anchored to a single 1000 Genomes Phase 3 (GRCh38) panel. This document
covers what's in that panel, how the population subsets are defined,
how the PCA reference is built, and how the runtime ancestry call feeds
back into ref-stats lookup.

## 5.1 The panel files

```
/data/pgs2/ref_panel/
├── GRCh38_1000G_ALL.pgen    # 3,202 unrelated 1000G samples, GRCh38, all chromosomes
├── GRCh38_1000G_ALL.pvar.zst (compressed)
├── GRCh38_1000G_ALL.psam    # sample metadata: super_pop, pop, sex
├── GRCh38_1000G.king.cutoff.out.id  # kept (unrelated) sample IDs (KING 2nd-degree cutoff)
├── pop_samples/
│   ├── EUR.txt              # 633 IIDs
│   ├── EAS.txt              # 585 IIDs
│   ├── AFR.txt              # 893 IIDs
│   ├── SAS.txt              # 601 IIDs
│   └── AMR.txt              # 490 IIDs
└── ancestry_ref.{bed,bim,fam}  # legacy plink1 reference for the ancestry app
```

A duplicate GRCh37 version (`GRCh37_1000G_ALL.*`) is kept for
auto-liftover-into-GRCh37 edge cases but is not used by the live PGS
pipeline.

`.pvar.zst` is read on demand via `zstdcat` (no full decompression).
This is the bottleneck for ref-stats recomputation; everything else
is plink2-fast.

## 5.2 Population definitions

`pipeline/config.py::POPULATIONS` (canonical):

| Code  | Label              | Sample file              | Min n |
| ----- | ------------------ | ------------------------ | ----- |
| EUR   | European           | `pop_samples/EUR.txt`    | 633   |
| EAS   | East Asian         | `pop_samples/EAS.txt`    | 585   |
| AFR   | African            | `pop_samples/AFR.txt`    | 893   |
| SAS   | South Asian        | `pop_samples/SAS.txt`    | 601   |
| AMR   | Admixed American   | `pop_samples/AMR.txt`    | 490   |
| MIX   | Mixed (EUR+EAS)    | dynamic, seed=42         | 1170  |
| MID   | Middle Eastern     | placeholder              | 0     |

`MIX` is built on the fly inside `recompute_ref_stats.py`: equal counts
from `EUR.txt` and `EAS.txt` (currently `min(len(EUR), len(EAS)) = 585`
each, total ~1170), random seed 42 for reproducibility. The intent is
to give "mostly admixed but not assignable" users a less biased
reference than blanket EUR. We do not currently build a true admixed
ref panel from individual-level admixture proportions — this is on the
list (see `README.md` open questions).

`MID` has no panel because the 1000G phase 3 super-pops don't include
Middle Eastern. Reports for samples that admixture-classify as MID
fall back to MIX or EUR with `reason="ancestry_data_unparseable"`-ish
messaging.

`BUILDABLE_POPULATIONS = ["EUR", "EAS", "AFR", "SAS", "AMR", "MIX"]`
controls which ref-stats files are created by
`recompute_ref_stats.py --pop ALL`.

`UI_POPULATIONS = ["EUR", "EAS", "MIX"]` are the first-class options
shown in the report UI's "compare to" dropdown.

## 5.3 PCA cache

`runners._build_pca_reference_cache(cache_dir)` builds, once,
`/data/pgs_cache/pca_1000g/`:

```
pca_1000g/
├── ref.eigenvec.allele   # per-variant PC weights (the projector)
├── ref.afreq             # allele frequencies (read-freq for projection)
├── ref.eigenvec          # per-sample PC coordinates (for centroids)
├── ref.eigenval          # eigenvalues (we report only PC1..PC5)
└── ref.psam              # super_pop labels per sample (for centroids)
```

Built in two stages:

```
# Stage 1 — LD-prune the panel (autosomes only, MAF ≥ 5%, missing ≤ 2%, biallelic SNPs)
plink2 --pfile GRCh38_1000G_ALL vzs
    --allow-extra-chr
    --chr 1-22 --maf 0.05 --geno 0.02 --snps-only --max-alleles 2
    --rm-dup force-first
    --indep-pairwise 1000 50 0.1
    --threads 16 --memory 48000
    --out ref_pruned_ld
# → ref_pruned_ld.prune.in : the ~106K LD-pruned variant IDs

# Stage 2 — PCA + freqs on the pruned set
plink2 --pfile GRCh38_1000G_ALL vzs
    --extract ref_pruned_ld.prune.in
    --rm-dup force-first
    --freq
    --pca 10 approx allele-wts
    --threads 16 --memory 48000
    --out ref
```

`--pca ... approx allele-wts` produces `eigenvec.allele` directly so we
never need to project against the dense pgen at score time.

User projection (`_run_pca_1000g`):

```
plink2 --pfile <user>
    --read-freq pca_1000g/ref.afreq
    --score pca_1000g/ref.eigenvec.allele 2 5 header-read
        no-mean-imputation variance-standardize
    --score-col-nums 6-15            (PC1..PC10)
    --allow-extra-chr
    --out projected
```

`variance-standardize` is required so the projected coordinates live in
the same space as `ref.eigenvec` (the centroids). The earlier version
without that flag silently misclassified samples.

Super-population centroids are mean-PC over each `super_pop` group in
`ref.psam`. We assign by minimum Euclidean distance over the first 4
PCs. Confidence is `high` when the 2nd-closest super-pop is ≥30%
farther than the closest, else `moderate`.

## 5.4 Admixture proportions

`_run_admixture_from_pca` does **not** call ADMIXTURE/RFMix. We
approximate admixture from the PCA distances by inverse-distance
weighting against the 5 super-pop centroids (over PC1..PC4). Output:

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

These proportions feed `select_reference_population(ancestry_result)`
(in `pipeline/scoring.py`) at PGS percentile time.

## 5.5 Ancestry-aware reference selection

`pipeline/scoring.py::select_reference_population(ancestry_result)`:

```
proportions = ancestry_result["admixture"]   # or top-level pop dict

# Top population by share
top_pop, top_prop = max(proportions.items(), key=share)

if top_prop >= 0.80:
    primary   = top_pop
    secondary = next two pops by share
    reason    = "single_cluster (TOP=80%)"
else:
    primary   = "MIX"
    secondary = [top_pop, second_pop]
    reason    = "admixed (top={top_pop}={top_prop:.0%})"
```

The selected `primary` decides which ref-stats file is loaded. The
`secondary` percentiles are computed alongside and reported as
`secondary_percentiles: {EAS: 67.3, EUR: 71.0}` so the UI can show a
"compare against X" toggle without re-running.

If the user has not yet run PCA, `ancestry_result` is `None` and we
default to `primary=EUR` (with `reason="ancestry_data_unparseable
(default EUR)"`) — this is logged in the report so a reviewer can see
which results were defaulted.

## 5.6 Manual reference override

The UI lets the user pin a population per-test. The API path is
`/api/pgs/<pgs_id>/refs` (GET available refs) and a `ref_pop` param on
the run request. When provided, `_ancestry_hint = {ref_pop: 1.0}` and
the selection logic chooses that as primary. We log
`ancestry_model="user_pinned:EAS"` in the report.

## 5.7 What the panel does NOT cover

- **Phase 4 / NYGC 30× rerelease**: we use phase 3. Some Y/mt analyses
  would benefit from the phase-4 sample set; not done yet.
- **Non-1000G ancestries**: Middle Eastern, indigenous Australasian,
  some Pacific populations are absent. The MIX fallback is the best
  we currently do.
- **Family panels (trio / pedigree)**: the panel is unrelated samples
  by design (`king.cutoff.out` applied). Family-aware PGS calibration
  (e.g. within-family PGS) is out of scope.
- **Sex-stratified references**: we compute one ref-stats file per
  (PGS, pop), not per (PGS, pop, sex). For traits with strong sex
  effects (e.g. CAD) this means percentiles are sex-pooled. A
  reviewer-flagged extension would be welcome.
