# simple-genomics internals — features the home page doesn't surface

`simple-genomics` is the FastAPI app at `https://23andclaude.com/`
(uvicorn on port 8800). It is a single Python file (`app.py`,
~14K lines) with sidecar modules. The home page exposes file upload
and "run tests"; this doc catalogs the features that exist alongside
but aren't on that page.

Code paths are `simple-genomics/app.py:LINE` unless otherwise noted.

## 1. `/compare` — multi-sample × per-trait comparison

The reviewer's top request. Auto-aggregating side-by-side comparison
of every PGS report across every file the logged-in user owns.

### What it does

For every (trait × sample) pair the user has scored, render:

- A trait card with: category, description, list of PGS IDs that
  scored this trait, and the average match-rate.
- Per-sample mini-cards inside, sorted by mean z-score
  (lower = "lowest risk", higher = "highest risk"). The top-ranked
  sample is highlighted as `compare-best`.
- Risk labels are bucketed by sample count:
  | n samples | Labels |
  | --- | --- |
  | 2 | Lowest / Highest |
  | 3 | Lowest / Average / Highest |
  | 4 | Lowest / Below Avg / Above Avg / Highest |
  | 5 | Lowest / Below Avg / Average / Above Avg / Highest |
  | 6+ | Lowest / Below Avg / (Average) × (n − 5) / Above Avg / High / Highest |

### Aggregation logic (`app.py:13247–13475`)

For each (sample, trait):

1. Walk `users/<u>/reports/<file_id>/pgs_*.json` for every file the
   user owns.
2. Apply the live-percentile overlay
   (`pipeline.live_percentile.apply_live_overlay`) before reading
   values — so old reports get current μ/σ.
3. Filter by:
   - `status` ∈ {passed, ok, success, completed, ""}
   - `match_rate_value ≥ min_match` (default 60)
   - `|z_score| ≤ max_abs_z` (default 6)
   - `confidence == "high"` (optional; off by default)
4. Each surviving report contributes a `z_score`,
   `percentile`, `match_rate`, `ancestry`, and confidence.
5. Per (trait, sample): mean `z_score`, mean `percentile`, mean
   `match_rate`, joined `ancestry` set.
6. Per trait: rank samples by `mean_z` (or fallback to
   `(mean_pct − 50) / 25`), assign risk labels.
7. Compute `spread_z = max(mean_z) − min(mean_z)` per trait.
8. Sort traits by category, then trait name.

### Cache

`_compare_data_cached` keys on `(username, min_match, max_abs_z,
high_conf_only)`. TTL `_COMPARE_TTL_S` (~60s). Bounded at 32 entries
(LRU eviction by ts).

### API

```
GET /api/compare?min_match=60&max_abs_z=6&high_conf=0
→ JSON: { traits: [...], facets: {categories, samples}, filters, generated_at }
```

The HTML page at `/compare` is a self-contained template (rendered
in `app.py:13500–14000`) that fetches `/api/compare` and renders
trait cards client-side.

### Reviewer-relevant details

- **Risk labels are within-cohort**: "Lowest" means "lowest of the
  samples you uploaded", not "lowest in the general population".
  Two users with identical scores would see different labels
  depending on who else's data is in their account.
- **Z-score is the primary rank key**, not percentile. This is
  intentional — percentiles are bounded [0.5, 99.5] (chapter 07),
  z-scores capture true distance from the mean. But it means a
  sample with EAS μ/σ and a sample with EUR μ/σ can be compared on
  z directly even when their percentiles look very different.
- **Missing ref-stats → trait is dropped** for that sample (z is
  None, contributes nothing). For EAS users hit by the chapter 09
  incident, /compare shows fewer traits than for the equivalent EUR
  user. This is silent.
- **Family relatedness is ignored**: /compare doesn't know whether
  two samples are siblings vs unrelated, so trait spreads carry no
  family-inference signal.

## 2. Profile system

Profiles let a user group multiple files under a named identity
("Mom", "Dad", etc.) so reports aggregate sensibly.

### API

```
GET  /api/profiles
POST /api/profiles
GET  /api/profiles/{prof_id}/reports
```

### Storage

`users/<u>/profiles.json`:

```json
{
  "profiles": {
    "<prof_id>": {
      "id": "...",
      "name": "Mom",
      "sex": "F",
      "year_of_birth": 1965,
      "file_ids": ["<fid_1>", "<fid_2>"],
      "created_at": "..."
    }
  }
}
```

A file can belong to multiple profiles; this is OK because the
reports are stored per-file, not per-profile, and the profile is
just a presentation grouping.

`/api/profiles/{prof_id}/reports` walks every file_id in the
profile and merges the per-file `reports/` directories — so a
profile-level view shows everything that's been computed for any
of the profile's files.

## 3. Chat — `/chat` and `/ws`

LLM-backed Q&A scoped to a user's results. Implementation in
`simple-genomics/chat.py`.

### Storage

`simple-genomics/trees/<user>/<conversation_id>/` — JSON-line files
per conversation, including the LLM responses and any tool calls
the chat invoked.

### Mechanics

- A WebSocket endpoint at `/ws` streams tokens as the LLM generates.
- The chat backend can pull the user's reports into context on
  request ("what's my T2D score?"), trim/aggregate them, and ask
  the configured LLM (Gemini / Claude / OpenAI per user settings).
- Same `_get_provider_key(user, provider)` resolves which model the
  user wants.

### Tool surface

The chat can call into:

- `_list_user_reports(username, file_id=None)` — list available
  PGS results.
- `_summarize_pgs(report_id)` — pull one report into context.
- `_run_compare(min_match, max_abs_z)` — invoke the /compare
  aggregator and surface the trait table inline.

These tools are defined in `chat.py` and registered with the
provider's tool-use protocol (Anthropic tool_use, Gemini function
calling, OpenAI function calling).

## 4. File manager

The "Files" tab — file upload, paste-by-path, paste-by-URL, rename,
prepare, clear-results, download.

### Routes

```
GET  /api/files                         list user's files
POST /api/files/upload                  multipart upload
POST /api/files/add-path                add by absolute path (server-side file)
POST /api/files/add-url                 download by URL
POST /api/files/{fid}/select            mark as the "active" file
POST /api/files/{fid}/prepare           pre-build pgen cache, normalize gVCF, etc.
POST /api/files/{fid}/clear-results     wipe reports/ for this file
POST /api/files/{fid}/rename            rename
GET  /api/files/{fid}/download          download original
```

### Storage

`users/<u>/files.json`:

```json
{
  "files": {
    "<fid>": {
      "id": "...",
      "name": "MomGenome.vcf.gz",
      "path": "/data/genom-nimo/.../mom.vcf.gz",
      "file_type": "gvcf",
      "size_bytes": 1234567890,
      "build": "GRCh38",
      "sample_name": "Mom",
      "uploaded_at": "...",
      "prepared": true,
      "pgen_cache_key": "<sha>",
      "qc": { ... }
    }
  }
}
```

### "Prepare"

Pre-runs the expensive cache builds so first-test latency is small:

- VCF → pgen (`_get_or_build_pgen`)
- gVCF → normalized + expanded VCF (`_normalize_gvcf`, chapter 02
  §2.3.3)
- CRAM → PCA VCF (`_derive_pca_vcf_from_cram`)
- Optionally: trigger HLA typing, Y/mt haplogroups, ROH in the
  background

A "prepare" task fans out these into a tracked queue so the user
sees per-step progress.

## 5. Test catalogue and dispatch

### Routes

```
GET  /api/tests                         list available tests (curated UI list)
GET  /api/tests/tabs                    grouped by category for the UI
GET  /api/tests/markdown                hand-curated markdown descriptions for each test
POST /api/run/{test_id}                 run a single test against the active file
POST /api/run-category/{category}       run all tests in a category
POST /api/run-all                       run all curated tests against the active file
GET  /api/status                        currently-running tasks
POST /api/clear-queue                   abort pending tasks
POST /api/task/{task_id}/stop           cancel one task
```

### Storage

`pgs_pipeline.db.tasks` (SQLite) — per-task row with status, queue
position, started_at, completed_at, result_json. The "Queue" UI
polls `/api/status` for live updates.

`users/<u>/reports/<file_id>/<test_id>_<task_id>.json` — per-run
result snapshot (so multiple runs of the same test on the same file
preserve history).

## 6. Master summary

A bird's-eye one-page summary across **every** test the user has
run, with categorical color-coding and trend lines.

### Routes

```
GET  /api/master-summary               cached summary JSON
POST /api/master-summary/generate       force regeneration
```

### Mechanics

`_master_summary_build` (in `app.py`) walks `users/<u>/reports/`,
extracts headline + risk-class + percentile for each result, groups
by category, and emits a stable JSON ordering for the UI to render.
The regenerate endpoint just clears the cache + rebuilds; the cache
is mtime-aware so most reads are free.

## 7. PGS browser / refresher

The "PGS Catalog" tab — search for a PGS by trait/keyword, add a
PGS that isn't in our curated list, force-refresh a PGS scoring
file from the catalog.

### Routes

```
GET  /api/pgs/search?q=diabetes        catalog text search (proxies PGS Catalog API)
GET  /api/pgs/custom                    user's added-not-in-curated PGS list
POST /api/pgs/add                       add a PGS (downloads + parses + caches)
POST /api/pgs/refresh/{category}        re-download + re-parse all PGSes in a category
GET  /api/pgs/refresh/{category}/status job status for the above
GET  /api/pgs/{pgs_id}/refs             list available reference populations for a PGS
GET  /api/pgs/refs-bulk                 bulk-list (used by the /compare loader)
GET  /api/pgs/{pgs_id}/percentile       recompute percentile against a different ref pop
```

`GET /api/pgs/{pgs_id}/percentile?file_id=<fid>&ref_pop=EAS` is the
endpoint the UI uses for the "compare against X" dropdown — it
loads the user's raw_score from the cached report and recomputes
the percentile against a different μ/σ without re-running plink2.

## 8. Settings and auth

### Settings

```
GET  /api/settings
POST /api/settings/show-vcf              persist UI preferences
POST /api/settings/interp-model          pick gemini / openai / claude for LLM interpretation
POST /api/settings/provider-key          set the user's own API key for that provider
GET  /api/settings/deps                  status of system deps (plink2, bcftools, samtools)
POST /api/settings/install-dep           install a missing dep (where supported)
GET  /api/settings/install-dep/{job_id}  install job status
```

### Auth

```
POST /api/auth/signup                    email + password
POST /api/auth/login                     sets HMAC session cookie (sg_session)
POST /api/auth/logout
GET  /api/auth/me                        current user
POST /api/auth/api-key                   issue a programmatic API key
GET  /api/auth/api-key                   list issued keys
```

The session cookie format is HMAC(secret, user_id || expiry || nonce);
`simple-ancestry` reads this same cookie for cross-auth (see
[simple-ancestry.md](simple-ancestry.md)).

## 9. CLAUDE.md viewer/editor

A bizarre but real feature: the in-browser
viewer/editor for `~/CLAUDE.md` files, scoped to the user's home
directory with path-traversal protection.

### Routes

```
GET  /api/claude-md                    list claude.md files in user's home
POST /api/claude-md/rebuild            regenerate (run any project's CLAUDE.md generator)
```

This is for users who run their own scripts on the host machine
alongside the platform — not directly relevant to the genomics
review.

## 10. Chat OAuth — Anthropic subscription auth

```
POST /api/chat/oauth-start              start an OAuth dance with Anthropic
```

For users who pay for Claude Pro and want the chat to consume their
subscription instead of an API key. Implementation lives in
`chat.py`.

## 11. Errors panel

```
GET  /api/errors                        recent server-side errors
POST /api/clear-errors                  reset
```

A simple per-user error log. Errors include "plink2 returned
non-zero", "build validation failed", etc. — the kind of operational
detail the LLM interpretation pass strips out but a power user wants
to see.

## 12. Reports archive

```
GET  /api/reports                       list every report the user has
GET  /api/reports/download              ZIP of every report
GET  /api/report/{task_id}              one report
GET  /api/report/{task_id}/download     one report (downloadable)
```

The ZIP archive is the bulk export the user needs to share with a
clinician, or to upload into the chat for follow-up questions.
