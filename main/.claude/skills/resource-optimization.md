# Resource Optimization

## Check Resources First

```bash
curl -s http://localhost:8600/api/system/stats | python3 -c "
import sys, json; s = json.load(sys.stdin)
print(f'CPU: {s[\"cpu\"][\"threads\"]} threads, {s[\"cpu\"][\"usage_pct\"]}% used')
print(f'RAM: {s[\"memory\"][\"used_gb\"]:.1f}/{s[\"memory\"][\"total_gb\"]:.1f} GB free')
g = s.get('gpu',{})
if g.get('available'):
    for d in g.get('devices',[]):
        print(f'GPU: {d[\"name\"]} {d[\"utilization_pct\"]}% util, {d[\"memory_used_mb\"]/1024:.1f}/{d[\"memory_total_mb\"]/1024:.0f} GB VRAM')
"
```

## CPU Guidelines (32 threads total)

| Availability | BAM Scoring | DeepVariant | FASTQ→BAM |
|---|---|---|---|
| High (load < 8) | 4-6 parallel | 20 shards | `-t 32` |
| Medium (load 8-20) | 2-3 parallel | 12-16 shards | `-t 16` |
| Low (load > 20) | 1 at a time | defer or 8 shards | `-t 8` |

## GPU Usage

- Tesla T4: 15 GB VRAM
- DeepVariant `call_variants` stage uses GPU automatically with GPU container
- GPU is idle during `make_examples` and `postprocess_variants` stages
- Only one DeepVariant job should use the GPU at a time

## Memory (117 GB)

- BAM scoring (Pipeline E+): ~4 GB per concurrent file
- plink2 reference panel: ~8 GB (keep 20 GB free for plink2)
- DeepVariant: ~20-30 GB for WGS; each shard ~1 GB
- Don't run multiple DeepVariant jobs simultaneously

## Storage Tiers

| Tier | Size | Use For |
|------|------|---------|
| /data (SSD) | 2.9 TB | BAMs, references, final results, PGS cache |
| /scratch (NVMe) | 1.5 TB | Pipeline intermediates, temp files, active runs |

Run pipelines writing to /scratch, copy final results to /data.

## Threading Rules (MANDATORY)

**ALWAYS use multi-threading. This machine has 32 CPUs — never run single-threaded.**

- **plink2**: ALWAYS pass `--threads 16` (or CPU_COUNT from config)
- **bcftools view/norm/stats**: ALWAYS pass `--threads 16`
- **samtools flagstat/sort/index**: ALWAYS pass `--threads 16` or `-@ 16`
- **Per-chromosome parallelism**: For operations on VCF/BAM, split by chromosome and run in parallel using ThreadPoolExecutor. Example: bcftools query across 22 chromosomes simultaneously.
- **Independent checks**: Run independent analyses (e.g., sex checks, QC metrics) in parallel with ThreadPoolExecutor, not sequentially.
- **Config reference**: `from backend.config import CPU_COUNT` — use `min(CPU_COUNT, 16)` for tool threads.
- **Template commands**: Use `{threads}` placeholder in COMMAND_REGISTRY, substituted at runtime.

## Concurrent Pipeline Rules

- **DeepVariant + scoring**: NOT recommended simultaneously. Run DV first, then score.
- **Multiple BAM scoring**: Up to 6 parallel (auto-detected by engine).
- **FASTQ alignment + scoring**: Split cores — 16 threads for alignment, rest for scoring.

## Monitoring

```bash
curl -s http://localhost:8600/api/system/stats | python3 -c "
import sys,json; s=json.load(sys.stdin)
print(f'CPU: {s[\"cpu\"][\"usage_pct\"]}% | RAM: {s[\"memory\"][\"used_gb\"]:.1f}/{s[\"memory\"][\"total_gb\"]:.1f} GB | Load: {s[\"load_avg\"]}')"
```
