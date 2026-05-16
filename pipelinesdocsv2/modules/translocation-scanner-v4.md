# translocation-scanner-v4 — `/translocation-scanner-v4/` (port 8780)

The current production scanner. 8-stage pipeline with explicit
adjudication, ensemble of external callers (DELLY + optional
GRIDSS2), and per-call enrichment from clinical databases.

- **URL**: `https://23andclaude.com/translocation-scanner-v4/`
- **Port**: 8780 (loopback)
- **Service**: supervisor `translocation-scanner-v4`
- **Code**: `/home/nimrod_rotem/translocation-scanner-v4/`
- **Frontend**: `frontend/dist/` served via nginx alias
- **Health**: `{"status":"ok","version":"v4","runs":<N>}`
- **Pipeline orchestrator**: `backend/pipeline/orchestrator.py`

## 1. The 8 stages

```
[0] Preflight    Verify BAM/CRAM index, reference availability,
                 disk space, run-id allocation, BAM header parse
                 (chrom naming, build).
    │
    ▼
[1] Extraction   Per-chrom worker pool reads:
                 - discordant pairs (mate cross-chrom or far)
                 - split reads (SA tag)
                 - soft-clipped reads (high clip ratio at a position)
                 Output: per-chrom Parquet (~24 workers by default)
                 MIN_MAPQ=20, MIN_CLIP_LEN=20, BIN_SIZE=100kb
    │
    ▼
[2] Clustering   HDBSCAN on the discordant+split records (per chrom-pair)
                 HDBSCAN_MIN_CLUSTER_SIZE=4, MIN_UNIQUE_FRAGMENTS=4
                 Output: Candidate records (chromA, posA, chromB, posB,
                 read_evidence, MAPQ distribution, soft-clip distribution)
    │
    ▼
[3] Enrichment   Per-candidate: local background depth, repeat-region
                 overlap (RepeatMasker), segdup overlap (UCSC segdups),
                 gene overlap (Ensembl GTF). Computes a local-noise
                 prior to weight the evidence against background.
    │
    ▼
[4] Adjudication In-pipeline confirmation:
                 - Cluster SA positions (window=5bp) to find junction
                 - Re-pileup at the candidate breakpoint
                 - Compute soft-clip evidence count, alignment quality
                 Output: AdjudicationResult { CONFIRMED | WEAK | FALSE,
                          evidence_summary, mapq_summary }
    │
    ▼
[5] External     DELLY for second-opinion BND calls.
                 Optional: GRIDSS2 for assembly-based confirmation.
                 (DELLY_TIMEOUT default 4 hours; OMP_NUM_THREADS configurable)
                 External calls intersected with internal candidates.
    │
    ▼
[6] Tiering      Per-call tier assignment (same schema as v3):
                 Tier 1 (clinical + high-conf), Tier 2, Tier 3, VUS.
                 Plus: known-fusion check against Mitelman/COSMIC.
    │
    ▼
[7] Report       Final VCF, BEDPE, JSON, HTML report.
                 Filters: tier ≥ threshold, gene-overlap optional.
```

## 2. Source layout

```
backend/
├── main.py                            FastAPI entry (~400 lines, routes only)
├── config.py                          all constants (paths, thresholds, defaults)
├── models.py                          dataclasses: Run, Candidate, AdjudicationResult, etc.
├── run_manager.py                     run-id allocation, state persistence
├── aggregator.py                      cross-stage state aggregation
├── dev_subbam.py                      dev-mode helper: subset a BAM to chr3+chr12 for tests
├── lib/
│   ├── bam_utils.py                   BAM read helpers (mate fetch, SA parsing)
│   ├── chrom_utils.py                 chrom name normalization (chr1 ↔ 1)
│   ├── mask_loader.py                 RepeatMasker / segdup BED loader
│   └── vcf_writer.py                  emit conforming BND VCF
└── pipeline/
    ├── orchestrator.py                run all 8 stages in sequence
    ├── stage_preflight.py             Stage 0
    ├── stage_extraction.py            Stage 1
    ├── stage_clustering.py            Stage 2 (HDBSCAN)
    ├── stage_enrichment.py            Stage 3
    ├── stage_adjudication.py          Stage 4
    ├── stage_external.py              Stage 5 (DELLY, GRIDSS2)
    ├── stage_tiering.py               Stage 6
    └── stage_report.py                Stage 7
```

## 3. API

```
GET  /api/health                              {"status":"ok","version":"v4","runs":...}
GET  /api/server-files                        list BAMs in SAMPLE_DIRS
POST /api/runs                                kick off a scan
GET  /api/runs                                list runs
GET  /api/runs/{run_id}                       one run's status
POST /api/runs/{run_id}/cancel                cancel
GET  /api/runs/{run_id}/stream                SSE — per-stage events
GET  /api/runs/{run_id}/calls                 list calls (paginated)
GET  /api/runs/{run_id}/calls/{call_id}       one call detail (including read evidence)
GET  /api/runs/{run_id}/report                full report bundle
GET  /api/dev-fixtures                        list dev-mode sub-BAM fixtures
GET  /{path:path}                             SPA fallback
```

### POST /api/runs payload

```json
{
  "file_path": "/data/aligned_bams/SAMPLE.bam",
  "reference_path": "/data/refs/hs38DH.fa",
  "reference_build": "GRCh38",
  "settings": {
    "dev_mode": false,
    "dev_chroms": ["3", "12"],         // if dev_mode=true, restrict to these
    "min_mapq": 20,
    "min_unique_fragments": 4,
    "run_external_callers": true,
    "run_assembly": false,             // GRIDSS2 — slow
    "tier_threshold": 3                // minimum tier to include in report
  }
}
```

Returns `{"run_id": "...", "status": "queued"}`. Subscribe to
`/api/runs/<id>/stream` for live progress.

## 4. Configuration (`backend/config.py`)

```
SAMPLE_DIRS:               /data/aligned_bams, /data/ancestry_app/uploads, /scratch
REFERENCE_PATHS:
   GRCh38:                 /data/refs/hs38DH.fa
   GRCh38_numeric:         /data/genom-nimo/reference.fasta

# Extraction
MIN_MAPQ:                  20
MIN_CLIP_LEN:              20      bp
MIN_SPLIT_ALIGNED:         30      bp
CLIP_PILEUP_RADIUS:        5       bp
CLIP_PILEUP_MIN_DEPTH:     4
BIN_SIZE:                  100_000 bp
BAD_FLAGS:                 0xF00   secondary | supplementary | duplicate
INSERT_SIZE_SAMPLE_CAP:    10_000_000

# Workers
DEFAULT_NUM_WORKERS:       24
DEV_EXTRACTION_WORKERS:    4
DEV_PYSAM_THREADS:         4

# Clustering
HDBSCAN_MIN_CLUSTER_SIZE:  4
HDBSCAN_MIN_SAMPLES:       4
MIN_UNIQUE_FRAGMENTS:      4
LOCAL_BACKGROUND_WINDOW:   100_000 bp

# Stage 5 — DELLY
DELLY_BIN:                 /home/nimo/miniconda3/envs/genomics/bin/delly
DELLY_OMP_THREADS:         configurable env
DELLY_TIMEOUT:             14400 s (4 h)

# Telemetry
TELEMETRY_INTERVAL_SEC:    0.25
READ_NAME_HASH_LEN:        12
```

## 5. Dev mode

`dev_mode: true` restricts extraction to a configured chrom subset
(`dev_chroms`, default `["3", "12"]`). This is for CI / local
testing — a fresh 30× WGS pipeline run in dev mode completes in
under 5 minutes vs. 1+ hour for a full run. Dev mode also uses
fewer extraction workers (`DEV_EXTRACTION_WORKERS=4`) to stay
within laptop CPU budgets.

The dev fixtures themselves are sub-BAMs pre-built by
`dev_subbam.py` — these are committed to the repo (under
`backend/data/dev_fixtures/`) so any developer can replay a known
scan deterministically.

## 6. Adjudication detail (Stage 4)

The clearest delta from v3. v4's adjudication is in-process and
combines:

1. **SA-position clustering** (`_cluster_sa_positions`): group split
   reads whose supplementary alignments land within 5 bp of each
   other. A junction is the modal SA position.
2. **Pileup at the candidate breakpoint**: count soft-clipped reads
   on the breakpoint side and reads spanning the junction.
3. **MAPQ distribution**: if all supporting reads have MAPQ < 30,
   the cluster is in a repetitive region and downgraded.
4. **Evidence aggregation**: split-read count + discordant-pair
   count + soft-clip support → AdjudicationResult enum.

The output is consumed by Stage 6 (tiering) as one of the model
features. This is more transparent than v3's logistic regression —
each call carries the per-feature evidence directly.

## 7. Stage 5 — External callers

DELLY is the default second opinion. Run with `-t BND` to restrict
to translocations:

```bash
delly call -t BND -g <ref> -o <out>.bcf <bam>
```

Outputs are intersected with internal candidates by breakpoint
proximity (within 50 bp). Concordant calls upgrade confidence;
internal-only calls remain at their internal tier; DELLY-only calls
are reported as a separate list.

GRIDSS2 (assembly-based) is an optional further confirmation. When
enabled (`run_assembly: true`), each candidate gets a local assembly
attempt; if a contig spans the junction, the call gets a strong
upgrade.

## 8. Storage

```
translocation-scanner-v4/results/<run_id>/
├── run.json                       status + timings + settings
├── stage_0/                       preflight cache
├── stage_1/                       per-chrom Parquet
├── stage_2/                       candidates.parquet
├── stage_3/                       enrichment.parquet
├── stage_4/                       adjudication.parquet
├── stage_5/                       delly.bcf (+ gridss/ if used)
├── stage_6/                       tier_assignments.json
├── stage_7/
│   ├── calls.vcf.gz + .tbi        BND VCF
│   ├── calls.bedpe                BEDPE for visualization
│   ├── calls.json                 structured call list
│   ├── report.html                rendered HTML
│   └── tier_summary.json          counts per tier
├── events.jsonl                   SSE event replay log
└── logs/
    ├── stdout.log
    └── stderr.log
```

## 9. v4 vs v3 deltas

| Aspect                  | v3                       | v4 |
| ----------------------- | ------------------------ | --- |
| Pipeline stages         | 6                        | 8 |
| Preflight stage         | implicit                 | explicit (stage 0) |
| Enrichment (repeats/segdup/genes) | post-tier annotation | dedicated stage 3 (before tiering) |
| Adjudication            | logistic regression model | explicit, feature-by-feature |
| External callers        | DELLY in clinical mode   | DELLY in all modes (configurable off) |
| Run-state persistence   | per-job dirs             | `run_manager.py` with explicit state machine |
| Dev mode                | no                        | yes (chr-subset fixtures) |
| Frontend                | static HTML               | proper SPA bundle (`frontend/dist/`) |

## 10. Caveats and open questions

- **HDBSCAN parameters**. `min_cluster_size=4` and
  `min_samples=4` were calibrated on internal data. The reviewer
  may want to opine on whether they generalize across coverage
  depths (10× vs 30× vs 60×).
- **DELLY timeout = 4 hours**. On large genomes with many candidate
  regions, DELLY can hit this. We log the timeout and continue;
  the call is reported as "internal-only" with no DELLY
  confirmation. Should we increase the budget or shard DELLY by
  chrom?
- **Reference path normalization**. The `GRCh38_numeric` reference
  (`/data/genom-nimo/reference.fasta`) is the bare-chrom variant.
  v4 auto-selects based on BAM header; misaligned BAMs (e.g. a
  bare-chrom BAM with a chr-prefixed reference path passed in
  manually) currently fail at stage 0 with a clear error.
- **Tier-1 clinical-gene list** is hand-curated. As with v3 — open
  follow-up to integrate a more dynamic source (DGV, OncoKB API,
  COSMIC Fusion).
- **No GPU acceleration** today. Assembly-based confirmation
  (GRIDSS2) and DELLY are CPU-bound. If the host accumulates more
  scanner runs concurrently, we may want to consider scheduling
  the heavy stages on a dedicated worker.

## 11. Reviewer asks specific to v4

- The 8-stage decomposition was meant to make each step
  individually re-runnable. Does the granularity look right, or
  are there stages we should merge / split?
- Adjudication (stage 4) vs external callers (stage 5) overlap
  conceptually. Is that healthy redundancy or wasted compute?
- The tiering rubric is shared with v3. Is it the right schema
  for the platform's clinical-adjacent use cases, or do we need
  separate tiers for research-grade vs return-of-results?
