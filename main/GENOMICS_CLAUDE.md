# Genomics App — Claude Code Instructions

You are the AI assistant for a genomics dashboard. The server hardware is auto-detected — check capabilities with `curl -s http://localhost:8600/api/system/capabilities` or read `/data/app/server_config.json`.

## Skill Files

Detailed instructions are in `.claude/skills/`:

| File | Contents |
|------|----------|
| `server-environment.md` | Hardware detection, tool paths, conda env, services |
| `file-locations.md` | BAM/VCF paths, reference genomes, storage layout |
| `pgs-scoring-pipeline.md` | Scoring via API, manual plink2, architecture |
| `file-conversion.md` | BAM/CRAM→VCF (DeepVariant), FASTQ→BAM, VCF filtering |
| `pgs-data-inventory.md` | Cached PGS scores with traits and variant counts |
| `api-reference.md` | Full API endpoint reference |
| `common-pitfalls.md` | Known issues: gVCF blocks, palindromic variants, build detection |
| `results-reporting.md` | Score interpretation, percentiles, confidence levels |
| `resource-optimization.md` | CPU/GPU/memory scaling, concurrent pipeline rules |

Read the relevant skill file before performing any task. All tools are in `conda activate genomics`.

## Key Defaults

- Genome build: **GRCh38**
- Reference population: **EUR** (configurable)
- API base: `http://localhost:8600`
- Tools auto-detected from conda env or PATH
- GPU auto-detected (if available, DeepVariant uses `--nv` flag)
- Container runtime: `apptainer` (not singularity)

## Hardware Adaptation

The system adapts to available hardware:
- **CPU cores** → thread count for alignment, DeepVariant shards, parallel scoring
- **GPU** → DeepVariant GPU mode (if NVIDIA GPU detected), otherwise CPU-only
- **RAM** → affects bcftools max depth, plink2 memory allocation
- **Storage** → two-tier: persistent `/data/` + fast `/scratch/` (both configurable via env vars)
