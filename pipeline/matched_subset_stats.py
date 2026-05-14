"""Phase 1.1 — Matched-subset reference distribution recompute.

When the user's matched variant set differs from the cached ref-stats'
variant set (chip, targeted, WES, or any case where `weighted_coverage`
< 0.99), the cached pooled stats are no longer the right null
distribution. This module:

  1. Computes `weighted_coverage = Σ_matched 2p(1−p)β² / Σ_all 2p(1−p)β²`
     using reference panel AFs.

  2. If `weighted_coverage < 0.99`: re-scores the reference panel
     restricted to the user's `.sscore.vars` matched set, caches the
     resulting `(sum_mean, sum_std, sum_scores.npy)` per matched-set
     SHA, and percentiles the user against that distribution instead.

  3. Hard gate: `weighted_coverage < 0.80` → percentile refused, status =
     `coverage_insufficient`.

The cache key is the SHA256 of the sorted matched variant ID list. Per
spec §1.1.

The output is a SUM-based distribution (spec §1.1 default).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np


log = logging.getLogger("pgs-pipeline.matched_subset_stats")


PLINK2 = os.environ.get("PLINK2", "/home/nimo/miniconda3/envs/genomics/bin/plink2")
REF_PANEL_PFILE = Path(os.environ.get(
    "REF_PANEL_PFILE", "/data/pgs2/ref_panel/GRCh38_1000G_ALL",
))
REF_PANEL_PVAR_ZST = Path(str(REF_PANEL_PFILE) + ".pvar.zst")
POP_DIR = Path(os.environ.get(
    "POP_DIR", "/data/pgs2/ref_panel/pop_samples",
))
DYNAMIC_CACHE_DIR = Path(os.environ.get(
    "MATCHED_SUBSET_CACHE_DIR", "/data/pgs2/ref_panel_stats/dynamic",
))


WC_DYNAMIC_TRIGGER = 0.99
WC_HARD_REFUSE = 0.80


@dataclass
class MatchedSubsetStats:
    pgs_id: str
    population: str
    matched_set_sha256: str
    n_variants: int
    n_samples: int
    sum_mean: float
    sum_std: float
    sum_scores_npy_path: str
    cached_json_path: str
    generated_at: str = ""

    def percentile(self, target_sum: float) -> tuple[float, float]:
        """Return (z, ecdf_percentile_0_100). ECDF is the spec §2.1 primary."""
        z = ((target_sum - self.sum_mean) / self.sum_std) if self.sum_std > 0 else 0.0
        try:
            arr = np.load(self.sum_scores_npy_path)
        except (OSError, ValueError):
            return z, _phi_pct(z)
        if arr.size == 0:
            return z, _phi_pct(z)
        ecdf = 100.0 * float(np.searchsorted(np.sort(arr), target_sum, side="left")) / arr.size
        return z, ecdf


def _phi_pct(z: float) -> float:
    import math
    return 100.0 * 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def matched_set_sha256(variant_ids: list[str]) -> str:
    h = hashlib.sha256()
    for v in sorted(variant_ids):
        h.update(v.encode())
        h.update(b"\n")
    return h.hexdigest()


def cache_path(pgs_id: str, population: str, matched_sha: str) -> Path:
    """Where the dynamic stats live on disk. Versioned by matched-set sha
    so any change in the user's variant subset gets its own cache entry."""
    return DYNAMIC_CACHE_DIR / f"{pgs_id}_{population}_GRCh38_matched-{matched_sha[:12]}.json"


def load_cached_matched_stats(
    pgs_id: str, population: str, matched_sha: str,
) -> Optional[MatchedSubsetStats]:
    p = cache_path(pgs_id, population, matched_sha)
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    npy = d.get("sum_scores_npy_path") or ""
    if not npy or not Path(npy).exists():
        return None
    return MatchedSubsetStats(
        pgs_id=d["pgs_id"],
        population=d["population"],
        matched_set_sha256=d["matched_set_sha256"],
        n_variants=int(d["n_variants"]),
        n_samples=int(d["n_samples"]),
        sum_mean=float(d["sum_mean"]),
        sum_std=float(d["sum_std"]),
        sum_scores_npy_path=npy,
        cached_json_path=str(p),
        generated_at=d.get("generated_at", ""),
    )


def _population_keep_file(population: str, tmpdir: Path) -> Path:
    src = POP_DIR / f"{population}.txt"
    if not src.exists():
        raise FileNotFoundError(f"population keep file missing: {src}")
    keep = tmpdir / f"{population}.keep"
    with src.open() as fin, keep.open("w") as fout:
        fout.write("#IID\n")
        for line in fin:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            fout.write(line + "\n")
    return keep


def _extract_subset_to_temp_score_file(
    mask_path: str | Path, matched_ids: set[str], out_path: Path,
) -> int:
    """Write a temp plink2 score file containing only the matched_ids
    rows from the original mask. plink2's --extract is at the variant
    level, not score-file level — so we have to filter the score file
    ourselves to avoid plink2 scoring unmatched variants and
    re-introducing the pooled-pop dilution we're trying to fix."""
    n = 0
    with open(mask_path) as fin, out_path.open("w") as fout:
        header = fin.readline()
        fout.write(header)
        for line in fin:
            vid = line.split("\t", 1)[0]
            if vid in matched_ids:
                fout.write(line)
                n += 1
    return n


def recompute_matched_subset_stats(
    pgs_id: str,
    population: str,
    matched_ids: set[str],
    mask_path: str | Path,
    *,
    plink2_path: Optional[str] = None,
) -> Optional[MatchedSubsetStats]:
    """Re-score the reference panel restricted to `matched_ids` and cache
    the resulting SUM distribution. Returns the new stats record, or None
    if scoring failed.
    """
    plink2 = plink2_path or PLINK2
    sha = matched_set_sha256(sorted(matched_ids))
    cached = load_cached_matched_stats(pgs_id, population, sha)
    if cached is not None:
        return cached

    DYNAMIC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f"matched_subset_{pgs_id}_{population}_", dir=DYNAMIC_CACHE_DIR,
    ) as tmpdir_s:
        tmpdir = Path(tmpdir_s)
        subset_mask = tmpdir / "subset_mask.tsv"
        n_kept = _extract_subset_to_temp_score_file(mask_path, matched_ids, subset_mask)
        if n_kept == 0:
            log.warning("matched-subset recompute %s/%s: no variants survived filter", pgs_id, population)
            return None
        keep = _population_keep_file(population, tmpdir)
        out_prefix = tmpdir / "ref_subset_score"
        cmd = [
            plink2,
            "--pfile", str(REF_PANEL_PFILE), "vzs",
            "--keep", str(keep),
            "--score", str(subset_mask), "header-read", "1", "2", "3",
            "cols=+scoresums,+denom",
            "no-mean-imputation",
            "list-variants",
            "--threads", os.environ.get("PLINK_THREADS", "4"),
            "--memory", os.environ.get("PLINK_MEMORY_MB", "8000"),
            "--out", str(out_prefix),
        ]
        log.info("matched-subset recompute %s/%s: scoring %d variants", pgs_id, population, n_kept)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60 * 30)
        if proc.returncode != 0:
            log.warning("matched-subset recompute %s/%s plink2 failed: %s",
                        pgs_id, population, proc.stderr[:500])
            return None
        sscore = Path(str(out_prefix) + ".sscore")
        if not sscore.exists():
            return None
        sums: list[float] = []
        n_samples = 0
        with sscore.open() as f:
            header = f.readline().lstrip("#").rstrip("\n").split("\t")
            col = {n.strip(): i for i, n in enumerate(header)}
            si = col.get("SCORE1_SUM", col.get("WEIGHT_SUM", -1))
            for line in f:
                p = line.rstrip("\n").split("\t")
                if not p[0] or si < 0 or si >= len(p):
                    continue
                try:
                    sums.append(float(p[si]))
                    n_samples += 1
                except ValueError:
                    continue
        if n_samples < 50:
            log.warning("matched-subset recompute %s/%s: only %d samples scored",
                        pgs_id, population, n_samples)
        arr = np.array(sums, dtype=np.float64)
        npy_path = DYNAMIC_CACHE_DIR / f"{pgs_id}_{population}_GRCh38_matched-{sha[:12]}.sum_scores.npy"
        np.save(npy_path, arr)
        stats = MatchedSubsetStats(
            pgs_id=pgs_id, population=population,
            matched_set_sha256=sha, n_variants=n_kept, n_samples=n_samples,
            sum_mean=float(arr.mean()) if arr.size else 0.0,
            sum_std=float(arr.std(ddof=1)) if arr.size > 1 else 0.0,
            sum_scores_npy_path=str(npy_path),
            cached_json_path=str(cache_path(pgs_id, population, sha)),
            generated_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        )
        cache_path(pgs_id, population, sha).write_text(json.dumps({
            "pgs_id": stats.pgs_id,
            "population": stats.population,
            "matched_set_sha256": stats.matched_set_sha256,
            "n_variants": stats.n_variants,
            "n_samples": stats.n_samples,
            "sum_mean": stats.sum_mean,
            "sum_std": stats.sum_std,
            "sum_scores_npy_path": stats.sum_scores_npy_path,
            "generated_at": stats.generated_at,
        }, indent=2))
        return stats


def should_use_dynamic_subset(
    weighted_coverage: float,
    allele_skip_count: int,
    input_class: str = "wgs",
) -> bool:
    """Spec §1.1 trigger: dynamic recompute when wc<0.99 OR allele skips
    OR sparse input class (chip/targeted/WES)."""
    if weighted_coverage < WC_DYNAMIC_TRIGGER:
        return True
    if allele_skip_count > 0:
        return True
    if input_class.lower() in {"chip", "targeted", "wes", "sparse-wes"}:
        return True
    return False


def coverage_gate_status(weighted_coverage: float) -> Optional[str]:
    """Spec §1.1 hard refusal: wc<0.80 → coverage_insufficient."""
    if weighted_coverage < WC_HARD_REFUSE:
        return "coverage_insufficient"
    return None
