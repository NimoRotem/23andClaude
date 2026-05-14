#!/usr/bin/env python3
"""Replace internal paths/hostnames/ports/usernames in pipelinesdocs/ with
sanitized placeholders per REMEDIATION_PLAN §0.3.

Mappings:
    /home/nimrod_rotem               → <USER_HOME>
    /home/nimo                       → <USER_HOME>
    /home/<other>                    → <USER_HOME>   (any leftover /home/X path)
    /data/pgs_cache                  → <CACHE_ROOT>/pgs_cache
    /data/pgs2                       → <DATA_ROOT>/pgs2
    /data/refs                       → <DATA_ROOT>/refs
    /data/aligned_bams               → <DATA_ROOT>/aligned_bams
    /data/fastq_backup               → <DATA_ROOT>/fastq_backup
    /data/uploads                    → <DATA_ROOT>/uploads
    /data/containers                 → <DATA_ROOT>/containers
    /data/                           → <DATA_ROOT>/                (catch-all)
    /scratch/                        → <SCRATCH_ROOT>/
    /var/log/supervisor              → <LOG_ROOT>
    /var/log/                        → <LOG_ROOT>/
    /etc/supervisor                  → <ETC_ROOT>/supervisor
    genom-beast-gpu                  → <PIPELINE_HOST>
    23andclaude.com                  → <APP_DOMAIN>
    grabo.cc                         → <APP_DOMAIN>
    rotem.ai                         → <APP_DOMAIN>
    nimrod_rotem                     → <USER>
    NimoRotem                        → <USER>
    nimo@rotem.ai                    → <USER>@<EMAIL_DOMAIN>
    port (\d{4,5})                   → port <PORT>
    :8\d{3}                          → :<PORT>
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


DOCS_ROOT_DEFAULT = Path("pipelinesdocs")

# Order matters: longer/more-specific replacements first so we don't
# clobber substrings of paths.
_REPLACEMENTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"/home/nimrod_rotem"), "<USER_HOME>"),
    (re.compile(r"/home/nimo(?![a-z])"), "<USER_HOME>"),
    (re.compile(r"/home/[A-Za-z0-9_-]+"), "<USER_HOME>"),
    (re.compile(r"/data/pgs_cache"), "<CACHE_ROOT>/pgs_cache"),
    (re.compile(r"/data/pgs2"), "<DATA_ROOT>/pgs2"),
    (re.compile(r"/data/refs"), "<DATA_ROOT>/refs"),
    (re.compile(r"/data/aligned_bams"), "<DATA_ROOT>/aligned_bams"),
    (re.compile(r"/data/fastq_backup"), "<DATA_ROOT>/fastq_backup"),
    (re.compile(r"/data/uploads"), "<DATA_ROOT>/uploads"),
    (re.compile(r"/data/containers"), "<DATA_ROOT>/containers"),
    (re.compile(r"/data/ref_stats"), "<CACHE_ROOT>/ref_stats"),
    (re.compile(r"/data/(?![A-Z<])"), "<DATA_ROOT>/"),
    (re.compile(r"/scratch/[A-Za-z0-9_/\.\-]*"), "<SCRATCH_ROOT>"),
    (re.compile(r"/scratch(?![A-Za-z0-9_/])"), "<SCRATCH_ROOT>"),
    (re.compile(r"/var/log/supervisor"), "<LOG_ROOT>/supervisor"),
    (re.compile(r"/var/log(?![a-z])"), "<LOG_ROOT>"),
    (re.compile(r"/etc/supervisor"), "<ETC_ROOT>/supervisor"),
    (re.compile(r"genom-beast-gpu"), "<PIPELINE_HOST>"),
    (re.compile(r"\b23andclaude\.com\b"), "<APP_DOMAIN>"),
    (re.compile(r"\bgrabo\.cc\b"), "<APP_DOMAIN>"),
    (re.compile(r"\brotem\.ai\b"), "<APP_DOMAIN>"),
    (re.compile(r"\bnimrod_rotem\b"), "<USER>"),
    (re.compile(r"\bNimoRotem\b"), "<USER>"),
    (re.compile(r"nimo@rotem\.ai"), "<USER>@<EMAIL_DOMAIN>"),
    (re.compile(r"\bport\s+(8\d{3})\b"), "port <PORT>"),
    (re.compile(r":(8\d{3})(?=\b|\D)"), ":<PORT>"),
    (re.compile(r"127\.0\.0\.1"), "<LOOPBACK>"),
    (re.compile(r"\b10\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "<INTERNAL_IP>"),
    (re.compile(r"\b104\.197\.40\.181\b"), "<EXTERNAL_IP>"),
    (re.compile(r"\b34\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "<EXTERNAL_IP>"),
]

# Regex used by the lint to flag remaining leaks.
LINT_PATTERNS = [
    re.compile(r"/home/[A-Za-z0-9_-]"),
    re.compile(r"/data/[a-z]"),
    re.compile(r"/scratch/[A-Za-z0-9]"),
    re.compile(r"genom-beast-gpu"),
    re.compile(r":8\d{3}"),
    re.compile(r"\bport\s+8\d{3}\b"),
    re.compile(r"\bnimrod_rotem\b"),
    re.compile(r"\bNimoRotem\b"),
    re.compile(r"\b10\.128\."),  # GCE internal
    re.compile(r"\b104\.197\.40\.181\b"),
]


def sanitize_text(text: str) -> tuple[str, int]:
    n = 0
    for pat, repl in _REPLACEMENTS:
        new, k = pat.subn(repl, text)
        n += k
        text = new
    return text, n


def lint_text(text: str) -> list[str]:
    hits: list[str] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        for pat in LINT_PATTERNS:
            if pat.search(line):
                hits.append(f"  line {line_no}: {line.strip()[:200]}")
                break
    return hits


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", default=str(DOCS_ROOT_DEFAULT))
    ap.add_argument("--apply", action="store_true",
                    help="Write changes; otherwise dry-run.")
    ap.add_argument("--lint-only", action="store_true",
                    help="Skip rewriting; exit 1 if any leak pattern matches.")
    args = ap.parse_args(argv)

    docs = Path(args.docs)
    if not docs.exists():
        print(f"ERROR: docs dir not found: {docs}", file=sys.stderr)
        return 2

    files = sorted(docs.glob("*.md")) + sorted(docs.glob("*.html"))
    total_rewrites = 0
    any_leaks = False
    for f in files:
        text = f.read_text()
        if args.lint_only:
            hits = lint_text(text)
            if hits:
                any_leaks = True
                print(f"{f}: {len(hits)} leak pattern hit(s):")
                for h in hits[:10]:
                    print(h)
            continue
        sanitized, n = sanitize_text(text)
        total_rewrites += n
        if args.apply and sanitized != text:
            f.write_text(sanitized)
            print(f"{f}: {n} replacements")
        elif not args.apply and sanitized != text:
            print(f"{f}: would rewrite {n} occurrence(s) (dry-run; pass --apply)")
        # Post-sanitization lint pass
        hits_after = lint_text(sanitized)
        if hits_after:
            any_leaks = True
            print(f"{f}: WARNING — {len(hits_after)} leak pattern(s) remain after sanitization:")
            for h in hits_after[:10]:
                print(h)

    if args.lint_only:
        return 1 if any_leaks else 0
    print(f"\nTotal replacements: {total_rewrites}")
    return 1 if any_leaks else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
