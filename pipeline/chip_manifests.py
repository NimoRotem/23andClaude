"""Phase 2.7 — Chip vendor manifest ingestion.

Per REMEDIATION_PLAN §2.7:

  - Replace dbSNP-lookup conversion with vendor- and chip-version-specific
    manifests: 23andMe v3/v4/v5, AncestryDNA v1/v2, MyHeritage, FTDNA.
    Manifest provides build, strand, REF/ALT, chip variant ID.
  - Detect chip version from file header signature; if undetectable,
    require user-supplied chip version (block ingestion otherwise).
  - Exclude ambiguous A/T and C/G sites unless REF orientation is
    confirmed by the manifest.
  - Build chip-specific matched-subset reference distributions per
    (chip_version, population). One-time cache.

This module provides:
  - chip-version detection from file signatures
  - manifest schema + loader
  - per-manifest variant lookup with strand-aware REF/ALT
  - integration helper that produces a normalized (chrom, pos, ref, alt,
    gt) iterator suitable for downstream pgen / scoring.
"""
from __future__ import annotations

import csv
import gzip
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional


log = logging.getLogger("pgs-pipeline.chip_manifests")


MANIFEST_DIR = Path("/data/pgs2/chip_manifests")


_VENDOR_SIGNATURES = (
    # (regex, vendor, default_version)
    (re.compile(r"^#\s*23andMe", re.IGNORECASE), "23andMe", None),
    (re.compile(r"AncestryDNA", re.IGNORECASE), "AncestryDNA", None),
    (re.compile(r"MyHeritage", re.IGNORECASE), "MyHeritage", None),
    (re.compile(r"FTDNA|familytreedna|Family\s*Tree\s*DNA", re.IGNORECASE), "FTDNA", None),
)


_VERSION_TAGS = (
    # (regex, version string)
    (re.compile(r"v\s*5(?:\D|$)", re.IGNORECASE), "v5"),
    (re.compile(r"v\s*4(?:\D|$)", re.IGNORECASE), "v4"),
    (re.compile(r"v\s*3(?:\D|$)", re.IGNORECASE), "v3"),
    (re.compile(r"v\s*2(?:\D|$)", re.IGNORECASE), "v2"),
    (re.compile(r"v\s*1(?:\D|$)", re.IGNORECASE), "v1"),
)


@dataclass
class ChipDetection:
    vendor: Optional[str]
    version: Optional[str]
    confidence: str       # "high" | "header_signature_only" | "user_supplied" | "unknown"
    header_snippet: str = ""


def detect_chip_version(path: str | Path, *, max_header_lines: int = 50) -> ChipDetection:
    """Sniff a chip text-export file's first ~50 lines for vendor + version."""
    p = Path(path)
    opener = gzip.open if str(p).endswith(".gz") else open
    header_lines: list[str] = []
    try:
        with opener(p, "rt", errors="ignore") as f:
            for i, line in enumerate(f):
                if i >= max_header_lines:
                    break
                header_lines.append(line.rstrip("\n"))
    except OSError:
        pass
    blob = "\n".join(header_lines)
    vendor = None
    for pat, v, _ in _VENDOR_SIGNATURES:
        if pat.search(blob):
            vendor = v
            break
    version = None
    for pat, v in _VERSION_TAGS:
        if pat.search(blob):
            version = v
            break
    if vendor and version:
        return ChipDetection(vendor, version, "high", blob[:400])
    if vendor:
        return ChipDetection(vendor, None, "header_signature_only", blob[:400])
    return ChipDetection(None, None, "unknown", blob[:400])


@dataclass
class ManifestEntry:
    chip_variant_id: str
    chrom: str
    pos1: int
    ref: str
    alt: str
    strand: str           # "+" | "-"
    build: str            # "GRCh38" (post-build-normalization)
    rsid: str = ""


def load_manifest(vendor: str, version: str,
                  manifest_dir: str | Path = MANIFEST_DIR) -> dict[str, ManifestEntry]:
    """Load manifest TSV at `<MANIFEST_DIR>/<vendor>_<version>.tsv`.

    Schema: chip_variant_id, chrom, pos1, ref, alt, strand, build, rsid.
    """
    p = Path(manifest_dir) / f"{vendor}_{version}.tsv"
    out: dict[str, ManifestEntry] = {}
    if not p.exists():
        log.warning("manifest missing for %s/%s at %s", vendor, version, p)
        return out
    with p.open() as f:
        reader = csv.DictReader(
            (line for line in f if not line.startswith("#")),
            delimiter="\t",
        )
        for row in reader:
            try:
                out[row["chip_variant_id"]] = ManifestEntry(
                    chip_variant_id=row["chip_variant_id"],
                    chrom=row["chrom"],
                    pos1=int(row["pos1"]),
                    ref=row["ref"].upper(),
                    alt=row["alt"].upper(),
                    strand=row.get("strand", "+"),
                    build=row.get("build", "GRCh38"),
                    rsid=row.get("rsid", ""),
                )
            except (KeyError, ValueError):
                continue
    return out


_COMPLEMENT = {"A": "T", "T": "A", "C": "G", "G": "C"}


def _is_ambiguous(ref: str, alt: str) -> bool:
    """A/T or C/G — orientation cannot be resolved without AF check."""
    return len(ref) == 1 and len(alt) == 1 and _COMPLEMENT.get(ref) == alt


def normalize_chip_call(
    chip_variant_id: str,
    raw_gt: str,
    manifest: dict[str, ManifestEntry],
    *,
    drop_at_cg: bool = True,
) -> Optional[tuple[str, int, str, str, str]]:
    """Map a raw chip call to a normalized (chrom, pos1, ref, alt, gt).

    `raw_gt` is the vendor's two-letter / single-letter genotype call
    (e.g. "AG", "GG", "TT", "DD" for indels). Returns None if the variant
    isn't in the manifest, is ambiguous A/T or C/G without orientation
    confirmation, or is uninterpretable.

    Strand-flipping: when manifest.strand == "-", complement the call
    before comparing to REF/ALT.
    """
    entry = manifest.get(chip_variant_id)
    if entry is None:
        return None
    raw_gt = raw_gt.strip().upper()
    if entry.strand == "-":
        raw_gt = "".join(_COMPLEMENT.get(c, c) for c in raw_gt)
    if drop_at_cg and _is_ambiguous(entry.ref, entry.alt):
        return None
    # Determine dosage of ALT
    n_alt = sum(1 for c in raw_gt if c == entry.alt)
    n_ref = sum(1 for c in raw_gt if c == entry.ref)
    if n_alt + n_ref != 2:
        return None
    if n_alt == 0:
        gt = "0/0"
    elif n_alt == 1:
        gt = "0/1"
    else:
        gt = "1/1"
    return entry.chrom, entry.pos1, entry.ref, entry.alt, gt


def iter_normalized_chip_file(
    path: str | Path,
    vendor: str,
    version: str,
    *,
    chip_id_col: str = "rsid",
    gt_col: str = "genotype",
    manifest_dir: str | Path = MANIFEST_DIR,
) -> Iterator[tuple[str, int, str, str, str]]:
    """Stream a chip TSV → (chrom, pos1, ref, alt, gt) tuples using the
    vendor/version manifest. Skips lines whose chip_variant_id is not in
    the manifest or whose calls fail normalization (ambiguous, malformed)."""
    manifest = load_manifest(vendor, version, manifest_dir)
    if not manifest:
        return
    p = Path(path)
    opener = gzip.open if str(p).endswith(".gz") else open
    with opener(p, "rt", errors="ignore") as f:
        header: list[str] | None = None
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            if header is None:
                header = [c.strip().lower() for c in line.rstrip("\n").split("\t")]
                continue
            parts = line.rstrip("\n").split("\t")
            row = dict(zip(header, parts))
            chip_id = row.get(chip_id_col, "")
            gt = row.get(gt_col, "")
            if not chip_id or not gt:
                continue
            tup = normalize_chip_call(chip_id, gt, manifest)
            if tup:
                yield tup
