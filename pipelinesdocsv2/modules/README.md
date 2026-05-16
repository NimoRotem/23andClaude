# Modules of the 23andclaude.com platform

The platform is **not** a single app. `https://23andclaude.com/` is the
public face of the genomics pipeline (the `simple-genomics` FastAPI
service documented in chapters 01–13), but a half-dozen sibling apps
are reverse-proxied under the same hostname. Many features the user
reaches by URL aren't surfaced from the home navigation, so they're
easy to overlook.

This packet documents each one independently.

## Inventory (as of 2026-05-14)

| URL                            | Port | Code dir                                       | Supervisor service       | Purpose |
| ------------------------------ | ---- | ---------------------------------------------- | ------------------------ | --- |
| `/`                            | 8800 | `/home/nimrod_rotem/simple-genomics/`          | `simple-genomics`        | Main genomics pipeline + UI (chapters 01–13) |
| `/compare`                     | 8800 | same — route in `simple-genomics/app.py:13500` | (same)                   | Multi-sample × per-trait comparison aggregator |
| `/chat`, `/ws`                 | 8800 | same — `chat.py`                               | (same)                   | LLM-backed Q&A over a user's results |
| `/convert/`                    | 8720 | `/home/nimrod_rotem/bam-converter/`            | `bam-converter`          | BAM/CRAM → normalized VCF converter |
| `/ancestry/`                   | 8710 | `/home/nimrod_rotem/simple-ancestry/`          | `simple-ancestry`        | Ancestry inference (gnomAD HGDP+1kGP + Rye), current |
| `/ancestry2/`, `/v1/`          | 8700 | `/data/ancestry_app/app/backend/`              | `ancestry`               | Older ancestry app, kept for backup |
| `/translocation-scanner-v2/`   | 8760 | `/home/nimrod_rotem/translocation-scanner-v2/` | `translocation-scanner-v2` | BND/translocation detection — discordant-read clustering |
| `/translocation-scanner-v3/`   | 8770 | `/home/nimrod_rotem/translocation-scanner-v3/` | `translocation-scanner-v3` | 6-stage pipeline (HDBSCAN + GRIDSS2 + ensemble) |
| `/translocation-scanner-v4/`   | 8780 | `/home/nimrod_rotem/translocation-scanner-v4/` | `translocation-scanner-v4` | 8-stage pipeline with DELLY + adjudication |
| `/LLM`, `/LLM/`                | redirect | `llm.23andclaude.com` (Open WebUI)         | (separate VM/container)  | Open WebUI in front of local LLM stack |
| `/pipelinesdocs/`              | static | `simple-genomics/pipelinesdocs/`             | (nginx alias)            | v1 of this packet |
| `/pipelinesdocsv2/`            | static | `simple-genomics/pipelinesdocsv2/`           | (nginx alias)            | This packet |

The non-genomics sub-apps `/openzl/`, `/progzl/`, `/runresults`,
`/runresultsV3` are compression-research labs that share the hostname
but are out of scope for the genomics review.

## Module reports

| File | Module |
| ---- | --- |
| [simple-genomics-internals.md](simple-genomics-internals.md) | `/compare`, profiles, file manager, chat, master-summary, settings, auth — features in the main app that aren't on the home page |
| [bam-converter.md](bam-converter.md) | `/convert/` — BAM/CRAM → VCF converter (8720) |
| [simple-ancestry.md](simple-ancestry.md) | `/ancestry/` — current ancestry inference app (8710) |
| [legacy-ancestry.md](legacy-ancestry.md) | `/ancestry2/`, `/v1/` — older ancestry app, kept as backup (8700) |
| [translocation-scanner-v2.md](translocation-scanner-v2.md) | `/translocation-scanner-v2/` — discordant-read BND scanner (8760) |
| [translocation-scanner-v3.md](translocation-scanner-v3.md) | `/translocation-scanner-v3/` — HDBSCAN + GRIDSS2 + ensemble (8770) |
| [translocation-scanner-v4.md](translocation-scanner-v4.md) | `/translocation-scanner-v4/` — 8-stage pipeline with DELLY + adjudication (8780) |
| [llm-webui.md](llm-webui.md) | `/LLM` → `llm.23andclaude.com` — Open WebUI |

## How they relate

```
                ┌──────────────────────────────────────────────┐
                │  nginx :443  https://23andclaude.com/        │
                └──────────────────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┬───────────────┬───────────────┐
            ▼                           ▼               ▼               ▼
  simple-genomics :8800        bam-converter :8720   simple-ancestry  translocation-
  (the "main" app)              (BAM/CRAM→VCF)       :8710             scanner v2/v3/v4
        │                              │              │               :8760/8770/8780
        │ uses output (synthetic       │              │
        │ VCF from 23andMe TSV)        │              │
        ├──────────────────────────────┘              │
        │                                             │
        │ users can also drop into /ancestry/         │
        │ directly (BAM/CRAM/VCF input)               │
        │                                             │
        ▼                                             │
  cross-auth (sg_session cookie)  ←──────────────────┘
        │
        ▼
  /compare aggregates across the user's reports
```

Each sub-app has its own storage, its own UI, its own auth where it
runs auth at all. Cross-app links happen at the nginx layer; data
flow is by users uploading the output of one app into another.

## Reading order for a reviewer

If the reviewer's goal is to understand the PGS pipeline, chapters
01–13 are sufficient. If the reviewer wants to assess every feature
the platform actually exposes, start with
[simple-genomics-internals.md](simple-genomics-internals.md) (it covers
features inside the main app that the home UI doesn't surface), then
work down the list in inventory order.
