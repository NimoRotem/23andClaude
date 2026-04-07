# API Reference

**Base URL**: `http://localhost:8600`

## Files — `/api/files/`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/files/scan` | Scan for all genomic files |
| POST | `/api/files/inspect` | Deep file inspection |
| POST | `/api/files/validate` | Validate file integrity |
| POST | `/api/files/upload` | Upload file (multipart) |
| POST | `/api/files/convert/fastq-to-bam` | FASTQ to BAM conversion |
| GET | `/api/files/convert/status/{job_id}` | Conversion status |

## PGS Catalog — `/api/pgs/`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/pgs/search?q=...&limit=20` | Search PGS Catalog |
| GET | `/api/pgs/autocomplete?q=...` | Autocomplete |
| POST | `/api/pgs/download` | Download scoring files |
| GET | `/api/pgs/cache` | List cached files |
| DELETE | `/api/pgs/cache/{pgs_id}` | Remove cached file |

## Scoring Runs — `/api/runs/`

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/runs/` | Create and start scoring run |
| POST | `/api/runs/estimate` | Estimate duration |
| GET | `/api/runs/` | List runs |
| GET | `/api/runs/{id}` | Run details |
| GET | `/api/runs/{id}/results` | Scoring results |
| GET | `/api/runs/{id}/results/detail/{pgs_id}` | Per-variant detail |
| POST | `/api/runs/{id}/rerun` | Rerun same config |
| DELETE | `/api/runs/{id}` | Delete run |
| WS | `/api/runs/{id}/progress` | WebSocket live progress |

### Create run request
```json
{
  "source_files": [
    {"type": "bam", "path": "/data/aligned_bams/Sample1.bam"},
    {"type": "vcf", "vcf_id": "uuid-here", "ref_population": "AFR"}
  ],
  "pgs_ids": ["PGS000025"],
  "engine": "auto",
  "ref_population": "EUR",
  "freq_source": "auto"
}
```

## nimog Pipeline — `/nimog/`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/nimog/` | Web UI |
| GET | `/nimog/api/browse` | File browser |
| POST | `/nimog/api/convert` | Start BAM→VCF job |
| GET | `/nimog/api/jobs` | List jobs |
| GET | `/nimog/api/jobs/{id}` | Job details |
| GET | `/nimog/api/jobs/{id}/stream` | SSE progress stream |
| GET | `/nimog/api/download/{id}` | Download VCF |
| POST | `/nimog/api/jobs/{id}/resume` | Resume failed job |

## System — `/api/system/`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/system/stats` | CPU, RAM, GPU, disk, processes |

## Storage — `/api/storage/`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/storage/status` | Disk usage for /data and /scratch |
