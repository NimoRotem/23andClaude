# Security & Documentation Sanitization Policy

This repo's published docs (`pipelinesdocs/`) and any user-facing
artifact (web UI, API responses, downloaded reports) MUST NOT contain:

- Internal hostnames (e.g. the production host's GCE instance name)
- Absolute home/data/log paths (`/home/<user>`, `/data/...`, `/scratch/...`,
  `/var/log/...`)
- Service ports (8xxx ports), internal IPs (10.x.x.x), or production
  external IPs
- Linux usernames or operator emails
- API keys, credentials, OAuth tokens (these belong in `~/CLAUDE_API_KEYS.md`,
  outside the repo)

## Replacement placeholders

Use these in published docs and example commands:

| Real | Sanitized placeholder |
|---|---|
| `/home/<user>` | `<USER_HOME>` |
| `/data/pgs2`, `/data/refs`, `/data/aligned_bams`, … | `<DATA_ROOT>/…` |
| `/data/pgs_cache`, `/data/ref_stats` | `<CACHE_ROOT>/…` |
| `/scratch/…` | `<SCRATCH_ROOT>` |
| `/var/log/...` | `<LOG_ROOT>` |
| `/etc/supervisor` | `<ETC_ROOT>/supervisor` |
| GCE instance name | `<PIPELINE_HOST>` |
| `23andclaude.com`, `grabo.cc`, `rotem.ai` | `<APP_DOMAIN>` |
| `127.0.0.1` | `<LOOPBACK>` |
| `10.x.x.x` | `<INTERNAL_IP>` |
| production external IP | `<EXTERNAL_IP>` |
| Service port `:8xxx` | `:<PORT>` |
| Linux username | `<USER>` |
| `user@domain` | `<USER>@<EMAIL_DOMAIN>` |

## Enforcement

- **Local tool:** `scripts/sanitize_docs.py --docs pipelinesdocs --apply`
  rewrites violations in place; `--lint-only` exits non-zero if any
  pattern below still matches.
- **CI gate:** the same script runs in `--lint-only` mode on every PR;
  a regex hit fails the build. Patterns checked: `/home/[A-Za-z0-9_-]`,
  `/data/[a-z]`, `/scratch/[A-Za-z0-9]`, the GCE instance name,
  `:8\d{3}`, `nimrod_rotem`, `NimoRotem`, `10.128.*` (GCE internal),
  the pinned external IP.

## Operator-only material

Anything that legitimately needs absolute paths (cron files, ops runbooks,
supervisor configs) stays out of `pipelinesdocs/` and out of any
user-facing artifact. Put it under `docs/operator/` (gitignored or
permission-restricted) or in `~/CLAUDE.md` / `~/CLAUDE_API_KEYS.md`.

## Out of scope for this policy

Public Git history may still contain pre-sanitization commits; for
strictly secret leaks (real API keys, real CRAM/BAM sample identifiers,
human subject identifiers), use `git filter-repo` to rewrite. The
sanitization policy above is for *non-secret operational leakage*
(paths, hostnames) — secret leaks need a separate rotation + history
rewrite playbook.

## Audit history

- 2026-05-14: Initial sanitization pass replaced 88 occurrences across
  `pipelinesdocs/*.md` and `pipelinesdocs/index.html`.
- 2026-05-14: `SECURITY.md` introduced. `scripts/sanitize_docs.py` added
  with `--lint-only` mode.
