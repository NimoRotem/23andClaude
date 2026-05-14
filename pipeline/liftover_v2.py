"""Phase 2.3 — Stricter liftover gates + REF re-validation.

Per REMEDIATION_PLAN §2.3:

  - Reject input if `weighted_coverage_post_liftover < 0.95`.
    Remove the 50% raw-variant-count threshold.
  - After liftover, validate every lifted record's REF against the
    target FASTA. Mismatches excluded, counted in `liftover_ref_mismatch`.
  - Report `liftover_unmapped_frac`, `liftover_ambiguous_frac`
    (multi-mapping), `liftover_ref_mismatch_frac`,
    `weighted_coverage_post_liftover` per PGS.
"""
from __future__ import annotations

import logging
import subprocess
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np


log = logging.getLogger("pgs-pipeline.liftover_v2")


WC_REJECT_THRESHOLD = 0.95   # spec §2.3


@dataclass
class LiftoverGateReport:
    n_input_variants: int
    n_unmapped: int
    n_ambiguous: int
    n_ref_mismatch: int
    n_passed: int
    weighted_coverage_post_liftover: float
    liftover_unmapped_frac: float
    liftover_ambiguous_frac: float
    liftover_ref_mismatch_frac: float
    decision: str             # "PASS" | "REJECT"
    reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _ref_at(fasta_path: str, chrom: str, pos1: int) -> Optional[str]:
    try:
        import pysam
        with pysam.FastaFile(fasta_path) as fa:
            for c in (chrom, chrom[3:] if chrom.startswith("chr") else f"chr{chrom}"):
                try:
                    b = fa.fetch(c, pos1 - 1, pos1).upper()
                except (KeyError, ValueError):
                    continue
                if len(b) == 1 and b in "ACGT":
                    return b
    except Exception:
        pass
    return None


def run_liftover_with_gates(
    input_variants: list[tuple[str, int, str, str, float]],
    *,
    chain_file: str,
    target_fasta: str,
    liftover_bin: str = "liftOver",
) -> tuple[list[tuple[str, int, str, str, float]], LiftoverGateReport]:
    """`input_variants` = (chrom, pos1, ref, alt, weight).

    Runs UCSC liftOver, post-validates each lifted REF against `target_fasta`,
    and computes the weighted coverage (Σ|w_kept| / Σ|w_input|).

    Returns (kept_lifted_variants, report). The report's decision = REJECT
    when weighted_coverage_post_liftover < 0.95 — caller MUST refuse to
    score on REJECT.
    """
    n_in = len(input_variants)
    if n_in == 0:
        rep = LiftoverGateReport(
            n_input_variants=0, n_unmapped=0, n_ambiguous=0, n_ref_mismatch=0,
            n_passed=0, weighted_coverage_post_liftover=0.0,
            liftover_unmapped_frac=0.0, liftover_ambiguous_frac=0.0,
            liftover_ref_mismatch_frac=0.0,
            decision="REJECT", reason="empty input",
        )
        return [], rep

    weight_total = sum(abs(w) for *_, w in input_variants)

    with tempfile.TemporaryDirectory(prefix="liftover_v2_") as tmpdir_s:
        tmp = Path(tmpdir_s)
        bed_in = tmp / "in.bed"
        bed_out = tmp / "out.bed"
        bed_unmap = tmp / "unmap.bed"
        with bed_in.open("w") as f:
            for i, (chrom, pos1, ref, alt, w) in enumerate(input_variants):
                # BED is 0-based half-open; record index in name field
                c = chrom if chrom.startswith("chr") else f"chr{chrom}"
                f.write(f"{c}\t{pos1 - 1}\t{pos1}\t{i}|{ref}|{alt}|{w:.6g}\n")
        proc = subprocess.run(
            [liftover_bin, str(bed_in), chain_file,
             str(bed_out), str(bed_unmap)],
            capture_output=True, text=True, timeout=600,
        )
        if proc.returncode != 0:
            rep = LiftoverGateReport(
                n_input_variants=n_in, n_unmapped=n_in,
                n_ambiguous=0, n_ref_mismatch=0, n_passed=0,
                weighted_coverage_post_liftover=0.0,
                liftover_unmapped_frac=1.0,
                liftover_ambiguous_frac=0.0,
                liftover_ref_mismatch_frac=0.0,
                decision="REJECT",
                reason=f"liftOver failed: {proc.stderr[-200:]}",
            )
            return [], rep
        # Count ambiguous = lines with same input id mapping multiple times
        unmapped_indices: set[int] = set()
        for line in bed_unmap.read_text().splitlines():
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) >= 4:
                try:
                    unmapped_indices.add(int(parts[3].split("|", 1)[0]))
                except ValueError:
                    continue
        seen: dict[int, int] = {}
        lifted_rows: list[tuple[int, str, int, str, str, float]] = []
        for line in bed_out.read_text().splitlines():
            if not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                continue
            c2 = parts[0]
            try:
                p2 = int(parts[2])
            except ValueError:
                continue
            name_parts = parts[3].split("|")
            try:
                idx = int(name_parts[0])
                ref = name_parts[1]
                alt = name_parts[2]
                w = float(name_parts[3])
            except (ValueError, IndexError):
                continue
            seen[idx] = seen.get(idx, 0) + 1
            lifted_rows.append((idx, c2, p2, ref, alt, w))
        ambiguous_indices = {i for i, n in seen.items() if n > 1}
        keep_rows: list[tuple[str, int, str, str, float]] = []
        n_ref_mismatch = 0
        weight_kept = 0.0
        seen_indices: set[int] = set()
        for idx, c, p, ref, alt, w in lifted_rows:
            if idx in ambiguous_indices:
                continue
            if idx in seen_indices:
                continue
            seen_indices.add(idx)
            actual_ref = _ref_at(target_fasta, c, p)
            if actual_ref is None or actual_ref != ref.upper():
                n_ref_mismatch += 1
                continue
            keep_rows.append((c, p, ref, alt, w))
            weight_kept += abs(w)

    n_unmapped = len(unmapped_indices)
    n_ambiguous = len(ambiguous_indices)
    n_passed = len(keep_rows)
    wc = (weight_kept / weight_total) if weight_total > 0 else 0.0
    rep = LiftoverGateReport(
        n_input_variants=n_in,
        n_unmapped=n_unmapped,
        n_ambiguous=n_ambiguous,
        n_ref_mismatch=n_ref_mismatch,
        n_passed=n_passed,
        weighted_coverage_post_liftover=wc,
        liftover_unmapped_frac=n_unmapped / n_in,
        liftover_ambiguous_frac=n_ambiguous / n_in,
        liftover_ref_mismatch_frac=n_ref_mismatch / n_in,
        decision="PASS" if wc >= WC_REJECT_THRESHOLD else "REJECT",
        reason=(f"weighted_coverage_post_liftover={wc:.3f} "
                f"{'≥' if wc >= WC_REJECT_THRESHOLD else '<'} {WC_REJECT_THRESHOLD}"),
    )
    if rep.decision == "REJECT":
        log.warning("liftover REJECT: %s (n_passed=%d/%d)", rep.reason, n_passed, n_in)
    return keep_rows, rep
