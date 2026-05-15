"""Phase 0.1 — pipeline_fingerprint block.

Every PGS report carries a `pipeline_fingerprint` dict capturing every
binary, reference, container, and configuration sha that materially
affects the percentile. A percentile is REFUSED if any required field
resolves to None, unless `--accept-fingerprint-drift` is passed (then we
emit but cap confidence at MEDIUM and write a structured
`fingerprint_drift` warning).

Required fields (per REMEDIATION_PLAN §0.1):
    plink2_version            - full build string from `plink2 --version`
    bcftools_version          - first line of `bcftools --version`
    samtools_version          - first line of `samtools --version`
    liftover_version          - first line of `liftOver` (-version unsupported)
    chain_files               - dict {basename: sha256}
    fasta_path                - canonical FASTA path used for REF lookups
    fasta_sha256              - sha256 of FASTA bytes
    fai_sha256                - sha256 of FASTA index
    pgs_catalog_release       - release tag from PGS Catalog (or "unknown")
    scoring_file_url          - URL the harmonized scoring file came from
    scoring_file_sha256       - sha256 of the harmonized scoring file
    container_digest          - container image SHA (or "host" if no container)
    python_lockfile_sha256    - sha256 of the requirements.txt / lock file
"""
from __future__ import annotations

import hashlib
import os
import re
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parent.parent
PGS_CACHE_DIR = Path("/data/pgs_cache")
FASTA_DEFAULT = Path(os.environ.get("FAMILY_PGS_FASTA", "/data/refs/hs38DH.fa"))
CHAINS_DEFAULT = Path("/home/nimrod_rotem/simple-genomics/liftover")
PGS_CATALOG_RELEASE_FILE = Path("/data/pgs_cache/_catalog_release.txt")
LOCKFILE_CANDIDATES = (
    REPO_ROOT / "requirements.txt",
    REPO_ROOT / "pyproject.toml",
)
REFUSAL_REASON_FIELD = "fingerprint_drift_reasons"


REQUIRED_FIELDS = {
    "plink2_version", "bcftools_version", "samtools_version",
    "liftover_version", "chain_files",
    "fasta_path", "fasta_sha256", "fai_sha256",
    "pgs_catalog_release", "scoring_file_url", "scoring_file_sha256",
    "container_digest", "python_lockfile_sha256",
}


def _sha256_file(path: str | Path) -> Optional[str]:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def _run_for_version(cmd: list[str]) -> Optional[str]:
    """Run a tool with --version and return the first non-empty line.
    Returns None on failure so the caller treats this as a missing
    fingerprint field (refuses by default)."""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    out = (proc.stdout or proc.stderr or "").strip().splitlines()
    return out[0] if out else None


# # ABS_PATH_FIX: resolve tool binaries by absolute path (the supervisor
# service runs without the conda env on PATH, so bare names fail).
_GENOMICS_BIN = "/home/nimo/miniconda3/envs/genomics/bin"
_PLINK2_BIN   = os.getenv("PLINK2",   f"{_GENOMICS_BIN}/plink2")
_BCFTOOLS_BIN = os.getenv("BCFTOOLS", f"{_GENOMICS_BIN}/bcftools")
_SAMTOOLS_BIN = os.getenv("SAMTOOLS", f"{_GENOMICS_BIN}/samtools")
_LIFTOVER_BIN = os.getenv("LIFTOVER_BIN",
                           "/home/nimrod_rotem/simple-genomics/liftover/liftOver")


@lru_cache(maxsize=1)
def plink2_version() -> Optional[str]:
    return _run_for_version([_PLINK2_BIN, "--version"])


@lru_cache(maxsize=1)
def bcftools_version() -> Optional[str]:
    return _run_for_version([_BCFTOOLS_BIN, "--version"])


@lru_cache(maxsize=1)
def samtools_version() -> Optional[str]:
    return _run_for_version([_SAMTOOLS_BIN, "--version"])


@lru_cache(maxsize=1)
def liftover_version() -> Optional[str]:
    """liftOver doesn't accept --version; the help text first line carries
    the build date. Capture it as version proxy."""
    try:
        proc = subprocess.run([_LIFTOVER_BIN], capture_output=True, text=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    out = (proc.stdout or proc.stderr or "").strip().splitlines()
    return out[0] if out else None


def chain_files_sha256(chain_dir: str | Path = CHAINS_DEFAULT) -> dict[str, str]:
    """Hash every `.chain` / `.chain.gz` file under chain_dir."""
    d = Path(chain_dir)
    out: dict[str, str] = {}
    if not d.exists():
        return out
    for child in d.iterdir():
        if child.is_file() and (child.suffix in {".chain", ".gz"} or "chain" in child.name):
            sha = _sha256_file(child)
            if sha:
                out[child.name] = sha
    return out


@lru_cache(maxsize=4)
def fasta_sha256(path: str | Path = FASTA_DEFAULT) -> Optional[str]:
    return _sha256_file(path)


@lru_cache(maxsize=4)
def fai_sha256(path: str | Path = FASTA_DEFAULT) -> Optional[str]:
    """sha256 of the .fai index next to the FASTA."""
    fai = str(path) + ".fai"
    return _sha256_file(fai)


@lru_cache(maxsize=1)
def pgs_catalog_release() -> str:
    """Optional pinned PGS Catalog release tag. The pipeline doesn't
    download the catalog index proactively; if the operator has stamped
    a release tag at PGS_CATALOG_RELEASE_FILE, we surface it here.
    Otherwise return "unknown" — the field is non-null but not pinned."""
    try:
        return PGS_CATALOG_RELEASE_FILE.read_text().strip() or "unknown"
    except OSError:
        return "unknown"


def scoring_file_url(pgs_id: str) -> str:
    """Canonical PGS Catalog URL for the harmonized GRCh38 file."""
    return (
        f"https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/{pgs_id}"
        f"/ScoringFiles/Harmonized/{pgs_id}_hmPOS_GRCh38.txt.gz"
    )


def scoring_file_sha256(pgs_id: str) -> Optional[str]:
    p = PGS_CACHE_DIR / pgs_id / f"{pgs_id}_hmPOS_GRCh38.txt.gz"
    return _sha256_file(p)


@lru_cache(maxsize=1)
def container_digest() -> str:
    """Container image SHA. Reads `/etc/container-image-sha` if present
    (operator-stamped at build time); otherwise "host" — the service is
    running directly on the host, not in a container."""
    for cand in ("/etc/container-image-sha",
                 "/home/nimrod_rotem/.container-image-sha"):
        try:
            v = Path(cand).read_text().strip()
            if v:
                return v
        except OSError:
            continue
    return "host"


@lru_cache(maxsize=1)
def python_lockfile_sha256() -> Optional[str]:
    for cand in LOCKFILE_CANDIDATES:
        sha = _sha256_file(cand)
        if sha:
            return sha
    return None


def build_pipeline_fingerprint(
    pgs_id: Optional[str] = None,
    *,
    chain_dir: Optional[str | Path] = None,
    fasta: Optional[str | Path] = None,
) -> dict:
    """Assemble the full `pipeline_fingerprint` block. Each value is
    either a real string or None — refusal logic in
    `refuse_or_downgrade_if_missing` decides what to do with missing fields."""
    return {
        "plink2_version": plink2_version(),
        "bcftools_version": bcftools_version(),
        "samtools_version": samtools_version(),
        "liftover_version": liftover_version(),
        "chain_files": chain_files_sha256(chain_dir or CHAINS_DEFAULT),
        "fasta_path": str(fasta or FASTA_DEFAULT),
        "fasta_sha256": fasta_sha256(fasta or FASTA_DEFAULT),
        "fai_sha256": fai_sha256(fasta or FASTA_DEFAULT),
        "pgs_catalog_release": pgs_catalog_release(),
        "scoring_file_url": scoring_file_url(pgs_id) if pgs_id else None,
        "scoring_file_sha256": scoring_file_sha256(pgs_id) if pgs_id else None,
        "container_digest": container_digest(),
        "python_lockfile_sha256": python_lockfile_sha256(),
    }


def fingerprint_missing_fields(fingerprint: dict) -> list[str]:
    missing: list[str] = []
    for key in REQUIRED_FIELDS:
        v = fingerprint.get(key)
        if v is None or v == "":
            missing.append(key)
        elif key == "chain_files" and not isinstance(v, dict):
            missing.append(key)
        elif key == "chain_files" and isinstance(v, dict) and not v:
            missing.append(key)
    return sorted(missing)


def refuse_or_downgrade_if_missing(
    report: dict,
    fingerprint: dict,
    *,
    accept_drift: bool = False,
) -> tuple[bool, list[str]]:
    """Apply the §0.1 rule: missing required fields → refuse percentile,
    UNLESS accept_drift=True in which case emit but cap confidence at
    MEDIUM and write `fingerprint_drift` warning into the report.

    Returns (suppress_percentile, missing_fields). Mutates `report` in
    place (sets confidence cap / drift warning).
    """
    missing = fingerprint_missing_fields(fingerprint)
    if not missing:
        return False, []
    if accept_drift:
        # Emit with a downgraded confidence
        report.setdefault("warnings", []).append({
            "type": "fingerprint_drift",
            "missing": missing,
            "accept_drift_flag": True,
            "confidence_cap": "MEDIUM",
        })
        # Cap confidence if a confidence field exists
        for k in ("confidence", "population_percentile_confidence"):
            cur = report.get(k)
            rank = {"high": 0, "medium": 1, "synthetic": 2, "low": 3}
            if cur and rank.get(cur.lower(), 0) < rank.get("medium", 1):
                report[k] = "medium"
        return False, missing
    # Default: refuse percentile
    report.setdefault("warnings", []).append({
        "type": "fingerprint_drift",
        "missing": missing,
        "accept_drift_flag": False,
        "action": "percentile_refused",
    })
    report[REFUSAL_REASON_FIELD] = missing
    return True, missing


def attach_pipeline_fingerprint(
    report: dict,
    pgs_id: Optional[str] = None,
    *,
    accept_drift: bool = False,
) -> dict:
    """Idempotent: add `pipeline_fingerprint` block and apply refusal
    logic. Returns the (possibly mutated) report."""
    fp = build_pipeline_fingerprint(pgs_id=pgs_id)
    report["pipeline_fingerprint"] = fp
    suppress, missing = refuse_or_downgrade_if_missing(
        report, fp, accept_drift=accept_drift,
    )
    if suppress:
        # Wipe any computed percentile from the result body
        res = report.get("result") or report
        for k in ("percentile", "z_score", "population_percentile",
                  "percentile_under_eur", "percentile_under_eas",
                  "percentile_under_afr", "percentile_under_sas",
                  "percentile_under_amr"):
            if k in res:
                res[k] = None
        res["status"] = "fingerprint_drift_refused"
    return report
