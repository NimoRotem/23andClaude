# simple-ancestry — `/ancestry/` (port 8710)

A standalone ancestry-inference FastAPI app. Differs from the
`simple-genomics` PCA-1000G test (chapter 05) in three ways:

1. Reference panel is **gnomAD HGDP+1kGP** (much broader than 1000G
   Phase 3 alone — includes HGDP populations).
2. Ancestry decomposition uses **Rye** (a regression-based admixture
   estimator), not inverse-distance PCA.
3. Output includes **runs of homozygosity (ROH)** analysis and a
   curated **population-signatures** layer.

The two apps coexist deliberately: simple-genomics uses 1000G-only
PCA for ref-stats compatibility (μ/σ are computed over 1000G subsets),
while simple-ancestry produces the population labels users care about.

- **URL**: `https://23andclaude.com/ancestry/`
- **Port**: 8710 (loopback, behind nginx)
- **Service**: supervisor `simple-ancestry`
- **Code**: `/home/nimrod_rotem/simple-ancestry/`
- **Data root**: `/data/ancestry_app/` (shared with the legacy app on
  port 8700; see [legacy-ancestry.md](legacy-ancestry.md))
- **Reference panel**: `/data/ancestry_app/reference/` (gnomAD HGDP+
  1kGP merged, LD-pruned)
- **Entry**: `backend/main.py` (FastAPI) + `backend/pipeline.py`

## 1. The reference panel

`/data/ancestry_app/reference/`:

```
ref_pruned.{bed,bim,fam,pgen,pvar,psam}   gnomAD HGDP+1kGP merged, LD-pruned to ~125K SNPs
pop2group.txt                              26 1KG pops + ~50 HGDP pops → 5 super-groups
pop2group_ea_detail.txt                    East-Asian sub-population detail (Han/Korean/Japanese/Yakut/...)
signatures.yaml                            curated population signatures (Ashkenazi, Yamato, etc.)
```

Population grouping is two-level: the top-level groups (`pop2group.txt`)
are the standard EUR/EAS/AFR/SAS/AMR + a sixth "OCE" (Oceania) and a
"MID" (Middle East — *actually* covered here, unlike in 1000G alone).
The detail file (`pop2group_ea_detail.txt`) splits East Asian into
specific populations for fine-resolution reporting.

## 2. Pipeline (`backend/pipeline.py`)

### 2.1 Input detection

`detect_input_type(path)` returns one of: `vcf`, `gvcf`, `bam`,
`cram`, `txt23andme`. Routing differs slightly from simple-genomics —
the 23andMe text path is in-process here (not delegated to
bam-converter).

### 2.2 Per-input steps (typical durations from `_TYPICAL_DURATIONS`)

**VCF input** (~4M variants, WGS):

```
[1]  Normalize VCF variants                     ~120 s    bcftools norm -f <ref> -m -
[2]  Index normalized VCF                       ~10 s     bcftools index -t
[3]  Convert VCF to PLINK binary                ~20 s     plink2 --vcf ... --make-pgen
[4]  Compute variant overlap with reference     ~15 s     plink2 --extract-intersect
[5]  Align overlapping variants                 ~15 s     strand resolution against ref AF
[6]  Subset reference to overlapping variants   ~10 s     plink2 --extract
[7]  Merge sample with reference                ~30 s     plink2 --pmerge-list
[8]  Clean merged dataset                       ~10 s     plink2 --maf 0.005 --geno 0.05
[9]  PCA (20 components)                        ~60 s     plink2 --pca 20
[10] Rye ancestry decomposition                 ~300 s    R or Python Rye implementation
[11] Runs of Homozygosity (ROH)                 ~30 s     bcftools roh --AF-tag AF
[12] Population signatures                      ~2 s      lookup against signatures.yaml
[13] Finalize results                           ~2 s      assemble JSON
```

**BAM/CRAM input** is the same flow plus a leading variant-calling
step (steps 1-2 replaced by ~600 s of bcftools mpileup+call at the
reference panel positions).

### 2.3 Rye (the ancestry estimator)

[Rye](https://github.com/healthystudent/rye) is a least-squares
ancestry estimator that operates on PCA coordinates. Given the
panel's PCs and labeled super-group assignments, it solves for the
user's super-group proportions that best reproduce their PCs.

Two implementations:

| Backend | Script | Performance |
| ------- | ------ | --- |
| R       | `/data/ancestry_app/tools/rye/rye.R`  | reference, slow |
| Python  | `/data/ancestry_app/tools/rye/rye.py` | port, much faster (default) |

Toggled by `RYE_USE_PYTHON` env var. The Python port has been
spot-checked against the R reference for parity within 1pp on the
HGDP samples; the speedup is roughly 5–10×.

### 2.4 Population signatures (`signatures.yaml`)

A hand-curated list of population signatures — specific PCA-region +
ROH-pattern combinations that flag known populations underrepresented
in the panel's flat super-group labels. Example entries (paraphrased):

```yaml
- name: Ashkenazi Jewish
  match:
    super_group: EUR
    pca_region: { PC4: [0.05, 0.12], PC5: [-0.02, 0.04] }
    long_roh_count_min: 25
  description: "Mediterranean-EUR cluster with elevated long-run ROH"

- name: Yamato (Mainland Japanese)
  match:
    super_group: EAS
    sub_population_match: ["JPT", "Yamato"]
  description: "Distinct from Han Chinese on PC3"
```

When a signature matches, it's surfaced in the result JSON as
`population_signatures: [{"name": ..., "confidence": ...}, ...]`. The
UI renders these as a separate "Population signatures" panel.

### 2.5 Output schema

```json
{
  "job_id": "...",
  "sample_id": "SZ7A76M9LNU",
  "ancestry": {
    "proportions": {
      "EUR": 0.02, "EAS": 0.94, "AFR": 0.01, "SAS": 0.02,
      "AMR": 0.005, "MID": 0.003, "OCE": 0.002
    },
    "method": "rye",
    "primary": "EAS",
    "primary_confidence": "high",
    "sub_population": {
      "primary": "CHB",        // Han Chinese, Beijing
      "second": "CHS"
    }
  },
  "pca": {
    "PC1": -0.0412, "PC2": 0.0823, ..., "PC10": -0.0021,
    "panel_pcs_url": "/api/jobs/<job_id>/pca-plot.csv"
  },
  "roh": {
    "total_length_mb": 23.4,
    "n_segments": 41,
    "long_segments_n": 12,        // ≥ 5 Mb
    "longest_mb": 18.2,
    "froh": 0.0078,              // total_roh / autosome_length
    "consanguinity_flag": false   // FROH > 0.01 → suggestive
  },
  "signatures": [
    {"name": "Yamato", "confidence": 0.91, "description": "..."}
  ],
  "interpretation": "...",        // LLM-generated, cached
  "completed_at": "..."
}
```

## 3. API

```
GET  /api/health                                 service health
GET  /api/reference/status                       reference panel availability + version
GET  /api/reference/detail                       per-population panel statistics
GET  /api/server-files                           file picker — lists configured SAMPLE_DIRS
POST /api/analyze                                kick off one job
POST /api/analyze/batch                          kick off many (one per file)
GET  /api/jobs                                   list jobs
GET  /api/jobs/{job_id}                          one job
GET  /api/jobs/{job_id}/stream                   SSE — per-step progress
GET  /api/jobs/compare                           multi-job side-by-side ancestry comparison
GET  /api/jobs/{job_id}/csv                      raw PCA + Rye coefficients as CSV
GET  /api/export/all-csv                         all completed jobs as one CSV (for plotting)
DELETE /api/jobs/{job_id}                        remove a job
GET  /api/settings                               user settings
PUT  /api/settings                               update
POST /api/settings/reset                         reset to defaults
GET  /api/databases                              list installed reference DBs (e.g. gnomAD)
POST /api/databases/{db_id}/download             one-off download
GET  /api/databases/{db_id}/status               download progress
DELETE /api/databases/{db_id}                    remove
```

## 4. Auth and cross-auth

`simple-ancestry` accepts two cookies:

| Cookie name        | Set by             | Used by simple-ancestry |
| ------------------ | ------------------ | --- |
| `ancestry_session` | this app itself    | full access |
| `sg_session`       | simple-genomics    | full access, cross-app SSO |

Cross-auth is implemented by reading `simple-genomics/sessions.json`
(env `SIMPLE_GENOMICS_DATA_ROOT`) and validating the HMAC. So a user
logged into the main app at `/` is automatically logged into
`/ancestry/`.

## 5. Storage

```
/data/ancestry_app/
├── reference/                  panel (gnomAD HGDP+1kGP, LD-pruned)
├── uploads/                    user-uploaded files
├── jobs/<job_id>/              per-job working dirs
│   ├── job.json                status + timings
│   ├── stages/<step>/          intermediate plink/vcf files
│   ├── pca.eigenvec            user + ref-panel PCs
│   ├── rye_proportions.tsv     Rye output
│   ├── roh.txt                 bcftools roh output
│   └── result.json             final JSON (the schema in §2.5)
└── users.json + sessions.json  auth state (this app's only)
```

`SAMPLE_DIRS` env var lists allowed source directories the file
picker may list (currently `/data/aligned_bams`,
`/data/ancestry_app/uploads`, `/data/vcfs`).

## 6. /api/jobs/compare — the cross-sample view

```
GET /api/jobs/compare?ids=<jid1>,<jid2>,<jid3>
```

Returns a JSON with all selected jobs' ancestry proportions side-by-
side, intended for the UI to render as a stacked-bar chart. The
endpoint enforces that all jobs belong to the same user.

This is the ancestry-side counterpart to `simple-genomics`'s
`/compare` (which compares PGS results across samples, not ancestry).

## 7. Reference-DB management

The "Databases" UI tab is for **operators**, not end users. It lists
the reference DBs the platform needs (gnomAD HGDP+1kGP source files,
ClinVar VCFs, Ensembl annotations) and shows whether each is
installed. Operators can trigger downloads from these endpoints —
useful when bringing up a fresh host.

## 8. Caveats

- **Rye is unsupervised on the panel labels we feed it**. If the
  `pop2group.txt` labels are misjoined (e.g. a 1000G sample labeled
  HGDP), Rye's least-squares solution can give plausible-looking
  but wrong proportions. We hand-checked these joins at panel-build
  time; the integrity check is in `pipeline.py::_verify_pop2group`.
- **MID is real here**, unlike in simple-genomics. The HGDP panel
  includes Mozabite, Bedouin, Druze, Palestinian — so a Middle
  Eastern user gets actual MID proportions, not `UNSUPPORTED`.
  The reviewer's call: should the simple-genomics ref-stats path
  also adopt the gnomAD HGDP+1kGP panel? (We currently don't,
  because the panel is denser and our ref-stats build would 4–6×
  longer.)
- **ROH is computed but not gated**. `consanguinity_flag = FROH >
  0.01` is surfaced but doesn't block any downstream logic. The
  PGS pipeline doesn't know about ROH at all.
- **gVCF input**: supported but slow because we don't carry the
  PGS+PCA union-positions targeted-expansion logic from
  simple-genomics here. Open follow-up.

## 9. The relationship to chapter 05

Chapter 05 documents the ancestry inference that runs **inside**
`simple-genomics` (the `pca_1000g` test). That's the fast,
embedded ancestry — used at PGS-score time to pick the ref-stats
population.

`simple-ancestry` is the **standalone**, gnomAD-HGDP-augmented
ancestry app — used when the user wants a full ancestry report
(sub-populations, ROH, signatures) as a deliverable. It can run
without ever touching simple-genomics.

The two share no code today; the user uploads the same file into
both and gets two different reports. Whether the platforms should
share an ancestry layer is an open architectural question.
