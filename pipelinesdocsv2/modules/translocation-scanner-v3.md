# translocation-scanner-v3 — `/translocation-scanner-v3/` (port 8770)

A clean rewrite of the v2 scanner with a formal 6-stage pipeline,
optional assembly-based verification, and clinical-grade tiering.

- **URL**: `https://23andclaude.com/translocation-scanner-v3/`
- **Port**: 8770 (loopback)
- **Service**: supervisor `translocation-scanner-v3`
- **Code**: `/home/nimrod_rotem/translocation-scanner-v3/`
- **Frontend**: `frontend/dist/` served via nginx alias
- **Health**: `{"status":"ok","version":"3.0.0","pipeline":"6-stage"}`

## 1. The 6 stages

```
[0] Ingest      BAM/CRAM → CRAM-converted + Parquet mate-pair index
                Pre-extracts discordant + split reads to a queryable Parquet
                so subsequent stages don't re-read the BAM.
    │
    ▼
[1] Candidates  HDBSCAN clustering on the Parquet index → candidate BEDPE
                Same cluster definition as v2 but in Parquet, with
                density estimation for cluster-level confidence.
    │
    ▼
[2] Assembly    (optional) GRIDSS2 targeted verification per candidate
                Local de-novo assembly at each candidate breakpoint to
                produce a contig that spans the junction. Confirms the
                join sequence directly (gold-standard).
                Enabled when run mode includes 'gridss'.
    │
    ▼
[3] Rerank      Feature scoring → calibrated probability
                Per candidate, features: read evidence, MAPQ distribution,
                soft-clip distribution, gnomAD overlap, GRIDSS2 status,
                repeat-region overlap. Logistic regression model
                produces a calibrated p(true_translocation).
    │
    ▼
[4] Ensemble    Multi-caller validation (optional, clinical mode)
                Cross-check against second-opinion callers (e.g. DELLY)
                where available. If two callers agree, confidence
                upgrades.
    │
    ▼
[5] Tier        Classification → VCF/JSON output
                Tier 1: high-evidence + clinical region (gene-disrupting)
                Tier 2: high-evidence, non-clinical region
                Tier 3: medium-evidence
                VUS:    weak evidence, conservative reporting
                Plus per-call: gene overlap (Ensembl GTF), known-fusion
                check (Mitelman DB / COSMIC fusion list).
```

Entry: `backend/main.py` (FastAPI on 8770) + `backend/pipeline.py`
(`PipelineV3` orchestrator) + per-stage modules in `backend/`:
`ingest.py`, `candidates.py`, `assembly.py`, `rerank.py`,
`ensemble.py`.

## 2. API

```
GET  /api/health                              {"status":"ok","version":"3.0.0","pipeline":"6-stage"}
GET  /api/server-files                        list BAM/CRAM files in configured dirs
POST /api/scan                                kick off a scan
GET  /api/jobs/{job_id}                       job status
GET  /api/jobs/{job_id}/stream                SSE — per-stage progress events
GET  /api/jobs/{job_id}/results               structured calls
GET  /api/jobs/{job_id}/download/{filename}   download an artifact (VCF, BEDPE, JSON, assembly FASTA)
GET  /api/jobs/{job_id}/report                full report
POST /api/jobs/{job_id}/cancel                cancel mid-run
GET  /api/previous-runs                       jobs the user has run before
```

### POST /api/scan payload

```json
{
  "input_path": "/data/aligned_bams/SAMPLE.bam",
  "mode": "fast" | "standard" | "clinical",
  "regions_bed": null,                              // optional
  "options": {
    "min_reads": 4,
    "assembly": true,                                 // enable GRIDSS2 (Stage 2)
    "ensemble": true,                                 // enable Stage 4
    "tier_clinical_genes_only": false
  }
}
```

The three modes are presets:

| Mode      | Stages run            | Typical time on 30× WGS |
| --------- | --------------------- | --- |
| fast      | 0,1,3,5               | ~5–10 min |
| standard  | 0,1,2,3,5             | ~30–60 min |
| clinical  | 0,1,2,3,4,5           | ~1–2 hr |

## 3. The candidate Parquet index

Stage 0 transforms a BAM into a Parquet table of every read that
could support a translocation:

```
column           dtype       description
─────────────────────────────────────────────────────────────────
read_name        string      original read name
chrom1           string      primary alignment chrom
pos1             int32       primary alignment pos
chrom2           string      mate or SA chrom
pos2             int32       mate or SA pos
evidence_type    string      "discordant" | "split" | "soft_clip"
mapq1            int8        primary MAPQ
mapq2            int8        mate / SA MAPQ
soft_clip_len    int16       0 if discordant; >0 if split/soft_clip
orientation      string      FR | FF | RR | RF (mate-pair orientation)
insert_size      int32       0 for inter-chrom or splits
```

This index is the persistent intermediate — stages 1+ query it
(rather than re-reading the BAM). It enables fast re-runs at
different stage parameters without paying the BAM-decode cost
again.

## 4. Tiering

The terminal stage assigns a tier per call:

| Tier  | Criteria |
| ----- | --- |
| **1** | Calibrated p ≥ 0.85 AND ≥ 1 partner gene in clinical list (COSMIC, OncoKB, etc.) AND breakpoint disrupts a CDS exon |
| **2** | Calibrated p ≥ 0.85, no clinical partner gene |
| **3** | 0.6 ≤ p < 0.85 OR p ≥ 0.85 with gnomAD AF > 0.001 (likely common variant) |
| VUS   | p < 0.6 but cluster size ≥ min_reads — surfaced for completeness |

The clinical-gene list lives in `backend/data/clinical_genes.tsv`
(hand-curated; reviewer's input on the list welcome).

## 5. Output

```
results/<job_id>/
├── job.json                       status + timings + tier counts
├── stage_0/                       Parquet index + CRAM
│   └── reads.parquet
├── stage_1/                       candidates.bedpe
├── stage_2/                       assembly outputs (per-candidate FASTA + GRIDSS2 VCF)
├── stage_3/                       per-call feature TSV + model scores
├── stage_4/                       (clinical only) DELLY VCF + ensemble verdict
├── stage_5/                       final tiered outputs:
│   ├── calls.vcf.gz + .tbi        BND VCF (tier annotated)
│   ├── calls.bedpe                BEDPE for IGV / breakpoint visualizer
│   ├── calls.json                 structured per-call detail
│   ├── tier1_calls.json           filtered to Tier 1 only
│   └── report.html                browsable HTML report
└── logs/
```

## 6. v3 vs v2 deltas

| Aspect                    | v2                       | v3 |
| ------------------------- | ------------------------ | --- |
| Persistent intermediate   | per-chrom Parquet        | unified Parquet index |
| Local assembly            | none                     | GRIDSS2 (optional) |
| Probability calibration   | none                     | logistic regression |
| Second-opinion caller     | none                     | DELLY (clinical mode) |
| Tiering                   | CONFIRMED / WEAK / FALSE | Tier 1/2/3/VUS |
| Clinical-gene annotation  | none                     | yes |

## 7. Caveats

- **GRIDSS2 is heavy**. ~10–30 min per candidate breakpoint on
  3-core machines. For a sample with 50 candidates this is the
  bulk of standard mode's runtime.
- **The calibration model was trained on a small in-house truth
  set**. We don't have a publicly-recognized benchmark
  (HG002 SV truth set, COLO-829, etc.) wired in. Open follow-up.
- **gnomAD-SV indexing is the same `gnomad_sv_v4.1.bnd.vcf.gz`
  used by v2**. If that file is missing, gnomAD annotation is
  silently skipped (no failure).
- **Reference is `/data/refs/GRCh38.fa`**. CRAM with non-matching
  contig names fails fast at stage 0.

## 8. Useful endpoints during development

The SSE stream emits stage-level events:

```json
{"stage": 0, "event": "ingest.started", "timestamp": ...}
{"stage": 0, "event": "ingest.parquet_written", "rows": 12345678, ...}
{"stage": 1, "event": "candidates.completed", "n_candidates": 47}
{"stage": 2, "event": "assembly.candidate.started", "candidate_id": "..."}
{"stage": 2, "event": "assembly.candidate.completed", "result": "contig_assembled"}
{"stage": 5, "event": "scan.completed", "tier1_count": 2, "tier2_count": 11, ...}
```

These are persisted to disk in `results/<job_id>/events.jsonl` and
replayable on reconnect.
