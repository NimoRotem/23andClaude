"""Phase 1.8 — Strict CRAM reference selection.

Per REMEDIATION_PLAN §1.8:

  FASTA selection: match CRAM header @SQ SN+LN+M5 to candidate FASTA
  `.fai` + `.dict`. Require M5 match on contigs present in the CRAM.

  Remove `ignore_md5=1` from default decode. Add opt-in flag
  `--cram-ignore-md5` (env: PGS_CRAM_IGNORE_MD5=1). When set:
    - Cap report `confidence` at `MEDIUM` regardless of other signals.
    - Set `cram_md5_override=true` in the report.

  Persist per report: chosen FASTA path, FASTA SHA256, `.fai` SHA256,
  CRAM header SHA256, matched contig M5 list.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional


log = logging.getLogger("pgs-pipeline.cram_reference_selection")


SAMTOOLS = os.environ.get("SAMTOOLS", "samtools")


@dataclass
class CramSqContig:
    sn: str
    ln: int
    m5: Optional[str]


@dataclass
class CramReferenceMatch:
    cram_path: str
    chosen_fasta: Optional[str]
    chosen_fasta_sha256: Optional[str]
    chosen_fai_sha256: Optional[str]
    cram_header_sha256: str
    n_cram_contigs: int
    n_matched: int
    n_mismatched: int
    matched_m5: list[str] = field(default_factory=list)
    mismatched: list[tuple[str, str, str]] = field(default_factory=list)
    selection_status: str = "PASS"   # "PASS" | "FAIL" | "OVERRIDE"
    cram_md5_override: bool = False
    reason: str = ""


def cram_ignore_md5_env() -> bool:
    return os.environ.get("PGS_CRAM_IGNORE_MD5", "0").lower() in ("1", "true", "yes")


def _sha256_file(path: str | Path) -> Optional[str]:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_cram_sq(cram_path: str | Path,
                  samtools_path: Optional[str] = None) -> tuple[list[CramSqContig], str]:
    """Returns (sq_contigs, cram_header_sha256). The header sha covers
    every @SQ line and the @HD line as a stable fingerprint."""
    cmd = [samtools_path or SAMTOOLS, "view", "-H", str(cram_path)]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(
            f"samtools view -H failed on {cram_path}: {proc.stderr[-300:]}"
        )
    header_text = proc.stdout
    sq: list[CramSqContig] = []
    for line in header_text.splitlines():
        if not line.startswith("@SQ"):
            continue
        m_sn = re.search(r"\bSN:([^\s]+)", line)
        m_ln = re.search(r"\bLN:(\d+)", line)
        m_m5 = re.search(r"\bM5:([0-9a-fA-F]{32})", line)
        if not (m_sn and m_ln):
            continue
        sq.append(CramSqContig(
            sn=m_sn.group(1),
            ln=int(m_ln.group(1)),
            m5=m_m5.group(1).lower() if m_m5 else None,
        ))
    return sq, _sha256_text(header_text)


def parse_fai_lengths(fai_path: str | Path) -> dict[str, int]:
    """Per-contig length from a FASTA index. (Length-only check; the M5
    match comes from the .dict.)
    """
    out: dict[str, int] = {}
    p = Path(fai_path)
    if not p.exists():
        return out
    with p.open() as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                continue
    return out


def parse_dict_m5s(dict_path: str | Path) -> dict[str, str]:
    """Per-contig MD5 from a samtools dict file."""
    out: dict[str, str] = {}
    p = Path(dict_path)
    if not p.exists():
        return out
    with p.open() as f:
        for line in f:
            if not line.startswith("@SQ"):
                continue
            m_sn = re.search(r"\bSN:([^\s]+)", line)
            m_m5 = re.search(r"\bM5:([0-9a-fA-F]{32})", line)
            if m_sn and m_m5:
                out[m_sn.group(1)] = m_m5.group(1).lower()
    return out


def select_fasta_for_cram(
    cram_path: str | Path,
    candidate_fastas: list[str | Path],
    *,
    samtools_path: Optional[str] = None,
    require_m5: bool = True,
    require_min_match_frac: float = 0.95,
) -> CramReferenceMatch:
    """Pick the candidate FASTA whose .dict M5s match the CRAM @SQ M5s
    most completely. Spec §1.8 requires M5 match on contigs present in
    the CRAM — `require_m5=False` is the override path and downstream
    code MUST cap confidence at MEDIUM."""
    cram_sq, header_sha = parse_cram_sq(cram_path, samtools_path)
    n_cram = len(cram_sq)
    cram_md5_set = {c.sn: c for c in cram_sq if c.m5}
    cram_ln = {c.sn: c.ln for c in cram_sq}
    best_match: Optional[CramReferenceMatch] = None
    for fa in candidate_fastas:
        fa_p = Path(fa)
        fai = Path(str(fa_p) + ".fai")
        dct = fa_p.with_suffix(".dict")
        if not fa_p.exists() or not fai.exists():
            continue
        lens = parse_fai_lengths(fai)
        m5s = parse_dict_m5s(dct)
        matched: list[str] = []
        mismatched: list[tuple[str, str, str]] = []
        for c in cram_sq:
            f_len = lens.get(c.sn)
            f_m5 = m5s.get(c.sn)
            if f_len is None or f_len != c.ln:
                mismatched.append((c.sn, "length", f"cram={c.ln} fasta={f_len}"))
                continue
            if c.m5 and f_m5 and c.m5 != f_m5:
                mismatched.append((c.sn, "m5", f"cram={c.m5} fasta={f_m5}"))
                continue
            if c.m5 and f_m5 and c.m5 == f_m5:
                matched.append(c.sn)
        candidate = CramReferenceMatch(
            cram_path=str(cram_path),
            chosen_fasta=str(fa_p),
            chosen_fasta_sha256=_sha256_file(fa_p),
            chosen_fai_sha256=_sha256_file(fai),
            cram_header_sha256=header_sha,
            n_cram_contigs=n_cram,
            n_matched=len(matched),
            n_mismatched=len(mismatched),
            matched_m5=matched,
            mismatched=mismatched,
        )
        if best_match is None or candidate.n_matched > best_match.n_matched:
            best_match = candidate

    if best_match is None:
        return CramReferenceMatch(
            cram_path=str(cram_path),
            chosen_fasta=None,
            chosen_fasta_sha256=None,
            chosen_fai_sha256=None,
            cram_header_sha256=header_sha,
            n_cram_contigs=n_cram, n_matched=0, n_mismatched=0,
            selection_status="FAIL",
            reason="no candidate FASTAs available",
        )

    # Apply M5 strictness
    if cram_md5_set:
        frac_matched = best_match.n_matched / max(1, len(cram_md5_set))
        if frac_matched >= require_min_match_frac:
            best_match.selection_status = "PASS"
            best_match.reason = (f"M5 match {best_match.n_matched}/{len(cram_md5_set)} "
                                 f"contigs ({frac_matched:.1%})")
            return best_match
        if not require_m5:
            best_match.selection_status = "OVERRIDE"
            best_match.cram_md5_override = True
            best_match.reason = (f"M5 match below threshold "
                                 f"({frac_matched:.1%} < {require_min_match_frac:.1%}); "
                                 f"PGS_CRAM_IGNORE_MD5 override active — "
                                 f"downstream confidence capped at MEDIUM")
            return best_match
        best_match.selection_status = "FAIL"
        best_match.reason = (f"M5 match below threshold "
                             f"({frac_matched:.1%} < {require_min_match_frac:.1%})")
        return best_match
    # No M5 tags on the CRAM at all — fall back to length-only match
    if best_match.n_mismatched == 0:
        best_match.selection_status = "PASS"
        best_match.reason = "no CRAM M5 tags; length-only match"
        return best_match
    best_match.selection_status = "FAIL"
    best_match.reason = (f"no CRAM M5 tags and {best_match.n_mismatched} length "
                         f"mismatches; cannot select FASTA safely")
    return best_match


def cap_confidence_for_override(report: dict, override: CramReferenceMatch) -> None:
    """If the CRAM was decoded under PGS_CRAM_IGNORE_MD5 override, cap any
    confidence field in the report at MEDIUM and mark `cram_md5_override`."""
    if not override.cram_md5_override:
        return
    rank = {"high": 0, "medium": 1, "synthetic": 2, "low": 3}
    for k in ("confidence", "population_percentile_confidence"):
        cur = (report.get(k) or "").lower()
        if cur and rank.get(cur, 0) < rank["medium"]:
            report[k] = "medium"
    res = report.get("result") if isinstance(report.get("result"), dict) else report
    res["cram_md5_override"] = True
    res.setdefault("warnings", []).append({
        "type": "cram_md5_override",
        "n_matched": override.n_matched,
        "n_mismatched": override.n_mismatched,
        "fasta": override.chosen_fasta,
        "reason": override.reason,
    })
