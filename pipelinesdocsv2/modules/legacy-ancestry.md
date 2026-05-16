# legacy ancestry app — `/ancestry2/` and `/v1/` (port 8700)

The earlier version of the ancestry app, kept running as a fallback.
Same data layer as [simple-ancestry.md](simple-ancestry.md)
(`/data/ancestry_app/`), but a different backend codebase and UI.

- **URLs**:
  - `/v1/` — the original public path
  - `/ancestry2/` — restored as a backup target after the
    `simple-ancestry` rewrite became the default at `/ancestry/`
- **Port**: 8700
- **Service**: supervisor `ancestry`
- **Code**: `/data/ancestry_app/app/backend/` (note: lives under
  the data directory, not under the user's home)
- **Run as**: user `nimo` (not `nimrod_rotem`)

## Why is it still here?

Three reasons:

1. **Diff target during the rewrite**. When `simple-ancestry`
   landed at port 8710, we wanted users to compare results without
   re-running pipelines, so the old service stayed up.
2. **Auth migration period**. The old app's session cookies don't
   interop with simple-genomics' `sg_session`; users who logged in
   under the old auth needed somewhere to land while we backfilled
   accounts.
3. **Insurance for the gnomAD-HGDP panel transition**. The legacy
   service uses a slightly different reference panel build; if a
   regression in the new panel surfaces, the legacy app is the
   sanity check.

The expectation is that `/ancestry2/` and `/v1/` will be retired
once we've fully decoupled and the new app has six months of
clean operation.

## What's different from simple-ancestry

| Aspect              | Legacy (`/v1/`, `/ancestry2/`) | New (`/ancestry/`) |
| ------------------- | ------------------------------ | --- |
| Backend root        | `/data/ancestry_app/app/backend/` | `/home/nimrod_rotem/simple-ancestry/backend/` |
| User                | `nimo`                          | `nimrod_rotem` |
| Port                | 8700                           | 8710 |
| Auth                | session token (own scheme)     | cross-auth with simple-genomics |
| Rye implementation  | R script only                  | Python port available (faster) |
| Sub-population detail | basic                         | `pop2group_ea_detail.txt` (East-Asian split) |
| ROH                 | not surfaced in UI             | surfaced + consanguinity flag |
| Signatures.yaml     | absent                         | present |
| Multi-sample compare | not exposed                   | `/api/jobs/compare` |

The pipelines themselves are similar (plink2 + Rye against the same
reference panel under `/data/ancestry_app/reference/`). The major
delta is UX surface and auth.

## API

Largely a subset of the simple-ancestry API; the same shape of
endpoints exists, but several are absent or implemented
differently. The reviewer can audit by hitting:

```
GET https://23andclaude.com/v1/api/health
GET https://23andclaude.com/v1/api/reference/status
GET https://23andclaude.com/v1/api/jobs
```

For day-to-day use, treat `/v1/` and `/ancestry2/` as read-only
historical paths: new analyses should go through `/ancestry/`.

## Nginx subtlety — the `sub_filter` rewrite

The `/ancestry2/` location block has a curious detail:

```nginx
location /ancestry2/assets/ {
    proxy_pass http://127.0.0.1:8700/assets/;
    proxy_set_header Accept-Encoding "";
    sub_filter '/ancestry/' '/ancestry2/';
    sub_filter_once off;
    sub_filter_types application/javascript text/javascript;
}
```

The old app was built with hardcoded `/ancestry/` paths in its
JavaScript bundle. To serve the same bundle under `/ancestry2/`,
nginx rewrites every literal `/ancestry/` → `/ancestry2/` on the
wire, with `Accept-Encoding: ""` to disable gzip (so `sub_filter`
can see the bytes). This is a hack — the proper fix would be to
rebuild the frontend with a configurable base path, but the cost-
to-benefit ratio doesn't justify it for a backup service.

The reviewer should know this exists because it makes the
`/ancestry2/` payload slightly larger and uncached at the
backend.

## When to look here

- **Cross-validating simple-ancestry output**: same input file
  through both ports should produce ancestry proportions within
  1–2pp on each super-pop.
- **Diagnosing reference-panel regressions**: if a new sample
  classifies oddly in `/ancestry/`, run it through `/v1/` to
  confirm the panel build (not the Rye code) is at fault.
- **Recovery**: if `simple-ancestry` is down, point users at
  `/v1/` as a fallback.

The legacy app is otherwise out of scope for the genomics review;
nothing it produces feeds into the PGS pipeline.
