# LLM Web UI — `/LLM` → `llm.23andclaude.com`

Not a genomics module per se, but a sibling service users discover
through the `/LLM` shortcut at `23andclaude.com/LLM`. Documented
here for completeness.

- **Public URL**: `https://llm.23andclaude.com/`
- **Reached from**: `https://23andclaude.com/LLM` (301 redirect)
- **Software**: [Open WebUI](https://github.com/open-webui/open-webui) — a
  multi-provider chat UI for local and remote LLMs.
- **Code**: not in `simple-genomics`; deployed as a separate
  service on its own subdomain.

## What it is

Open WebUI is a self-hosted chat frontend supporting:

- Local LLMs via Ollama (Llama 3, Mistral, etc. — on-host inference)
- Anthropic Claude (API key)
- OpenAI / Azure (API key)
- Google Gemini (API key)
- Any OpenAI-compatible endpoint

The redirect at `/LLM` exists so a user already on
`23andclaude.com` can jump to the chat UI without remembering the
subdomain. The redirect chain is:

```
/LLM        → 301 → https://llm.23andclaude.com/
/LLM/       → 301 → same
/LLM/<path> → 301 → same (path is dropped)
```

## How it relates to the genomics platform

It doesn't share storage with `simple-genomics`. A user wanting to
discuss their genomics report in Open WebUI must:

1. Download the report bundle from `/api/reports/download`.
2. Upload it as an attachment in Open WebUI.
3. Ask questions.

The `simple-genomics` built-in chat (covered in
[simple-genomics-internals.md](simple-genomics-internals.md) §3) is
the one that reads the user's reports directly. Open WebUI is for
freer-form LLM use that doesn't need to touch the platform's
storage.

## Configuration (operator-only)

The certificate and deployment are managed outside the genomics
codebase. The DNS for `llm.23andclaude.com` is in our Namecheap
account, pointing at the host running Open WebUI (currently the
same `genom-beast-gpu` VM, behind nginx on its own `server` block
in `/etc/nginx/sites-enabled/llm.23andclaude.com`).

## Reviewer note

Out of scope for the bioinformatics review. Listed here purely so
the reviewer isn't surprised by an `/LLM` link in the genomics UI
that bounces them off the platform.
