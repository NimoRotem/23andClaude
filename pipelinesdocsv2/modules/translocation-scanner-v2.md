# translocation-scanner-v2 — `/translocation-scanner-v2/` (port 8760)

The first production version of the translocation scanner. Detects
chromosomal translocations (BND-class structural variants) from
BAM/CRAM input using discordant-read pair clustering.

- **URL**: `https://23andclaude.com/translocation-scanner-v2/`
- **Port**: 8760 (loopback)
- **Service**: supervisor `translocation-scanner-v2`
- **Code**: `/home/nimrod_rotem/translocation-scanner-v2/`
- **Reference**: `/data/refs/GRCh38.fa`
- **gnomAD index**: `/data/refs/gnomad_sv_v4.1.bnd.vcf.gz`

Newer versions (v3, v4) supersede this one for clinical work. v2
remains useful as a fast first-pass scanner — its single-stage
clustering returns BND candidates faster than v4's full pipeline.

## 1. What it detects

BND = "breakend" — the half-call class that variant callers use for
translocations and complex SVs. A BND record describes one side of a
breakpoint joining two non-adjacent genomic positions. v2 hunts
specifically for **inter-chromosomal** BNDs (i.e. translocations:
chr A → chr B) and **inversions** within a chromosome.

Out of scope for v2: insertions, deletions, duplications,
unbalanced rearrangements, mobile-element insertions.

## 2. Architecture

```
BAM/CRAM input
    │
    ▼
[1] Extraction       Per-chrom worker reads:
                     - discordant pairs (mate on a different chrom, or far away)
                     - split reads (SA tag — primary aligns in one place, supplementary in another)
                     Output: per-chrom Parquet of mate-pair records
    │
    ▼
[2] Clustering       HDBSCAN-ish dense-region detection in 2D (chromA × chromB)
                     Output: cluster definitions (chromA, posA, chromB, posB, n_reads, mapq distribution)
    │
    ▼
[3] Adjudication     For each cluster:
                     - Re-pileup at the candidate breakpoint
                     - Compute soft-clip support, MAPQ distribution, repeat-region overlap
                     - Verdict: CONFIRMED / WEAK / FALSE
                     Parallel pool, default 16 workers (V2_ADJUDICATE_THREADS)
    │
    ▼
[4] Annotation       gnomAD-SV BND index lookup
                     Output: known_in_gnomad flag + AF if present
    │
    ▼
[5] Report           VCF (BND records) + JSON per call + summary table
```

Core entry: `backend/main.py` (FastAPI) + `backend/pipeline.py` +
`backend/adjudicate.py`.

## 3. API

```
GET  /api/health                              service health
GET  /api/bam-files                           list configured sample dirs
GET  /api/jobs                                jobs
POST /api/scan                                kick off a scan
DELETE /api/jobs/{job_id}                     delete
POST /api/jobs/{job_id}/cancel                cancel
GET  /api/jobs/{job_id}                       job status
GET  /api/jobs/{job_id}/events                SSE — per-step progress
GET  /api/jobs/{job_id}/status                short-form status
GET  /api/jobs/{job_id}/report                JSON report (top-level summary + candidate list)
GET  /api/jobs/{job_id}/reads?call_id=<id>    reads supporting one call (for viewer)
GET  /api/jobs/{job_id}/export/json          one big JSON
GET  /api/jobs/{job_id}/export/vcf           VCF (BND records)
```

## 4. Key parameters

| Constant                  | Default | Source |
| ------------------------- | ------- | --- |
| Reference                 | `/data/refs/GRCh38.fa` | env `REF_FASTA` |
| gnomAD-SV BND VCF         | `/data/refs/gnomad_sv_v4.1.bnd.vcf.gz` | env `GNOMAD_BND_VCF` |
| Adjudication threads      | 16      | env `V2_ADJUDICATE_THREADS` |
| Min MAPQ                  | 20      | constant in `pipeline.py` |
| Min clip length (split)   | 20 bp   | constant |
| Min supplementary aligned | 30 bp   | constant |
| Cluster min size          | ≥ 4 reads | HDBSCAN min_cluster_size |
| Discordant insert threshold | 1000 bp | beyond this, the pair is considered discordant |

## 5. Output: BND VCF

```
##fileformat=VCFv4.2
##ALT=<ID=BND,Description="Breakend">
##INFO=<ID=SVTYPE,Number=1,Type=String,Description="SV type">
##INFO=<ID=MATEID,Number=1,Type=String,Description="ID of mate breakend">
##INFO=<ID=READS,Number=1,Type=Integer,Description="Supporting reads">
##INFO=<ID=KNOWN,Number=0,Type=Flag,Description="In gnomAD-SV">
##INFO=<ID=GNOMAD_AF,Number=1,Type=Float,Description="gnomAD-SV AF">
##INFO=<ID=ADJUDICATION,Number=1,Type=String,Description="CONFIRMED|WEAK|FALSE">

#CHROM POS    ID         REF ALT             QUAL FILTER INFO
chr3   12345  bnd_001_a  N   N[chr12:67890[  .    .      SVTYPE=BND;MATEID=bnd_001_b;READS=24;...
chr12  67890  bnd_001_b  N   ]chr3:12345]N   .    .      SVTYPE=BND;MATEID=bnd_001_a;READS=24;...
```

Plus a per-call JSON with full evidence inventory (every supporting
read's MAPQ, position, soft-clip length, etc.) for the UI's
read-viewer.

## 6. Job storage

```
translocation-scanner-v2/<job_id>/
├── job.json                       status + timings + input + options
├── extraction/<chrom>.parquet     per-chrom discordant + split reads
├── clusters.parquet               cluster definitions
├── adjudication.parquet           per-cluster verdicts
├── annotations.parquet            gnomAD overlap results
├── report.json                    final structured output
├── calls.vcf.gz + .tbi            BND VCF
└── logs/
    ├── stdout.log
    └── stderr.log
```

## 7. Caveats

- **No assembly verification**. A cluster is a CONFIRMED call based
  on read-evidence statistics alone; no local breakpoint assembly is
  performed. This is what v3/v4 added.
- **No deletion/inversion specificity**. Inversions are detected as
  paired BNDs in the same chromosome with opposite orientations;
  the user has to interpret the pair as an inversion. v4 emits
  explicit `SVTYPE=INV` records.
- **gnomAD-SV v4.1** is the annotation source; calls absent from
  gnomAD aren't necessarily novel — they may be technical artifacts
  for centromere-near, repeat-rich regions.
- **No clinical-grade tiering**. v2 emits CONFIRMED / WEAK / FALSE
  per call; it doesn't bucket into a "Tier 1 / Tier 2 / VUS" schema.
  v3 added tiering.

## 8. nginx note

The nginx config proxies to `10.128.0.65:8760` (the old
`scanner-v2` GCE VM). That VM is currently TERMINATED. A local
copy of v2 runs on this host at port 8760 — but `/translocation-
scanner-v2/` requests hit the dead remote and time out.

**Open follow-up**: update nginx to point at `127.0.0.1:8760` or
retire the public URL until the remote is rehydrated.

## 9. When to prefer v2 over v3/v4

- Fast triage on small (chr-restricted) regions.
- When v3/v4 are unavailable.
- For comparing scanner-output schemas — v2's BND VCF is the
  simplest case to read.

For anything clinical, jump to [v4](translocation-scanner-v4.md).
