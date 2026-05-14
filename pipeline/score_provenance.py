"""Phase 0.2 — per-PGS `score_provenance` block.

Captures exactly which variants the scoring step actually used so a
reviewer can compute what was vs. what was advertised:

    score_stat_used         "SUM" | "AVG"
    n_matched               int  - variants in .sscore.vars
    n_total                 int  - variants in mask
    weighted_coverage       float in [0, 1]
                              = Σ_matched 2p(1−p)β² / Σ_all 2p(1−p)β²
                              uses reference-panel allele frequencies
    allele_skip_count       int  - plink2 "mismatching allele code" warnings
    multiallelic_handled    int  - records that survived bcftools norm -m-any
    multiallelic_excluded   int  - records dropped at split (rare)
    dup_aggregated          int  - duplicates collapsed in scoring
    liftover_unmapped_frac        float - fraction of records liftOver dropped
    liftover_ambiguous_frac       float - fraction of records that multi-mapped
    liftover_ref_mismatch_frac    float - fraction where post-lift REF disagrees
                                          with target FASTA (excluded)
"""
from __future__ import annotations

import gzip
import logging
import os
import re
import statistics
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Optional


log = logging.getLogger("pgs-pipeline.score_provenance")


@dataclass
class ScoreProvenance:
    score_stat_used: str = "AVG"     # historical default; spec §1.1 flips to SUM
    n_matched: int = 0
    n_total: int = 0
    weighted_coverage: float = 0.0
    allele_skip_count: int = 0
    multiallelic_handled: int = 0
    multiallelic_excluded: int = 0
    dup_aggregated: int = 0
    liftover_unmapped_frac: float = 0.0
    liftover_ambiguous_frac: float = 0.0
    liftover_ref_mismatch_frac: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


def parse_sscore_vars(path: str | Path) -> set[str]:
    """plink2 emits one variant ID per line that contributed to the score."""
    out: set[str] = set()
    try:
        with open(path) as f:
            for line in f:
                v = line.strip()
                if v:
                    out.add(v)
    except OSError:
        pass
    return out


def parse_plink2_stderr_for_allele_skips(stderr: str) -> int:
    """plink2 logs e.g. `Warning: 27 entries in <file> were skipped due to
    mismatching allele codes`. Sum the integers across all such lines."""
    if not stderr:
        return 0
    total = 0
    for m in re.finditer(
        r"(\d+)\s+entries[^\.]*?(skipped|missing)[^\.]*?(allele|variant)",
        stderr,
    ):
        try:
            total += int(m.group(1))
        except ValueError:
            continue
    return total


def compute_weighted_coverage(
    matched_ids: set[str],
    all_mask_rows: list[tuple[str, float]],
    refpanel_af: dict[str, float],
) -> float:
    """weighted_coverage = Σ_matched 2p(1−p)β² / Σ_all 2p(1−p)β²

    `all_mask_rows` is a list of (variant_id, weight) from the final
    plink2 scoring TSV.

    `refpanel_af` maps variant_id → allele frequency p in the reference
    panel for the variant's effect allele. Variants missing from
    refpanel_af are assumed to have unknown AF and contribute 0 to both
    numerator and denominator (conservative — they don't dilute the
    coverage but also don't help).
    """
    num = 0.0
    den = 0.0
    for vid, w in all_mask_rows:
        p = refpanel_af.get(vid)
        if p is None or not 0 < p < 1:
            continue
        info = 2.0 * p * (1.0 - p) * (w * w)
        den += info
        if vid in matched_ids:
            num += info
    if den == 0:
        return 0.0
    return num / den


def load_mask_rows(plink2_scoring_tsv: str | Path) -> list[tuple[str, float]]:
    """Reads `ID<TAB>A1<TAB>WEIGHT` lines → (variant_id, weight). Skips header."""
    rows: list[tuple[str, float]] = []
    try:
        with open(plink2_scoring_tsv) as f:
            for i, line in enumerate(f):
                if i == 0 or not line.strip():
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 3:
                    continue
                try:
                    rows.append((parts[0], float(parts[2])))
                except ValueError:
                    continue
    except OSError:
        pass
    return rows


def load_refpanel_af_for_ids(
    pvar_zst_path: str | Path,
    ids: Iterable[str],
) -> dict[str, float]:
    """Stream the panel pvar, return AF map for the requested variant IDs.

    The 1000G pvar has INFO/AF on each line; we parse the leftmost AF
    value when present. Missing AF → omit from map.
    """
    import subprocess
    need = set(ids)
    if not need:
        return {}
    found: dict[str, float] = {}
    proc = subprocess.Popen(
        ["zstdcat", str(pvar_zst_path)],
        stdout=subprocess.PIPE, text=True,
    )
    assert proc.stdout is not None
    af_re = re.compile(r"AF=([0-9.]+)")
    try:
        for line in proc.stdout:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t", 7)
            if len(parts) < 3:
                continue
            vid = parts[2]
            if vid not in need:
                continue
            info = parts[7] if len(parts) >= 8 else ""
            m = af_re.search(info)
            if m:
                try:
                    found[vid] = float(m.group(1))
                except ValueError:
                    continue
            if len(found) == len(need):
                break
    finally:
        proc.stdout.close()
        proc.wait()
    return found


def build_score_provenance(
    *,
    pgs_id: str,
    matched_ids: set[str],
    mask_tsv_path: str | Path,
    refpanel_pvar_zst: Optional[str | Path] = None,
    plink2_stderr: str = "",
    score_stat_used: str = "SUM",
    multiallelic_handled: int = 0,
    multiallelic_excluded: int = 0,
    dup_aggregated: int = 0,
    liftover_unmapped_frac: float = 0.0,
    liftover_ambiguous_frac: float = 0.0,
    liftover_ref_mismatch_frac: float = 0.0,
) -> ScoreProvenance:
    """Assemble a complete ScoreProvenance from the scoring run's artifacts."""
    mask_rows = load_mask_rows(mask_tsv_path)
    n_total = len(mask_rows)
    n_matched = len(matched_ids)
    allele_skip = parse_plink2_stderr_for_allele_skips(plink2_stderr)
    af_map: dict[str, float] = {}
    if refpanel_pvar_zst and Path(refpanel_pvar_zst).exists():
        try:
            af_map = load_refpanel_af_for_ids(
                refpanel_pvar_zst, (vid for vid, _ in mask_rows),
            )
        except Exception as e:
            log.warning("score_provenance: AF lookup failed: %s", e)
    wc = compute_weighted_coverage(matched_ids, mask_rows, af_map)
    return ScoreProvenance(
        score_stat_used=score_stat_used,
        n_matched=n_matched,
        n_total=n_total,
        weighted_coverage=wc,
        allele_skip_count=allele_skip,
        multiallelic_handled=multiallelic_handled,
        multiallelic_excluded=multiallelic_excluded,
        dup_aggregated=dup_aggregated,
        liftover_unmapped_frac=liftover_unmapped_frac,
        liftover_ambiguous_frac=liftover_ambiguous_frac,
        liftover_ref_mismatch_frac=liftover_ref_mismatch_frac,
    )


def attach_score_provenance(
    report: dict,
    provenance: ScoreProvenance,
) -> dict:
    """Add the `score_provenance` block to the report dict. Idempotent."""
    result = report.get("result") if isinstance(report.get("result"), dict) else report
    result["score_provenance"] = provenance.to_dict()
    return report
