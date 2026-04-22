"""PGS ingestion pipeline: download, parse, normalize, liftover, QC.

Usage:
    from pipeline.ingest_pgs import ingest_pgs
    result = ingest_pgs("PGS000005")
"""
import gzip
import json
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Optional, Dict, List

from .config import (PGS_CACHE, LIFTOVER_BIN, CHAIN_FILES)
from .match_logic import parse_pgs_scoring_file, to_user_plink2_format, to_refpanel_plink2_format

logger = logging.getLogger("pgs-pipeline")

VALID_CHROMS = {str(i) for i in range(1, 23)} | {"X", "Y", "MT", "M"}
VALID_ALLELES = {"A", "C", "G", "T"}


@dataclass
class IngestResult:
    """Result of ingesting a PGS scoring file."""
    pgs_id: str
    success: bool
    cache_dir: str
    metadata: Dict = field(default_factory=dict)
    scoring_file: Optional[str] = None
    scoring_clean: Optional[str] = None
    scoring_plink2: Optional[str] = None
    eligibility: Dict = field(default_factory=dict)
    error: Optional[str] = None


def ingest_pgs(pgs_id: str, force: bool = False) -> IngestResult:
    """Idempotent 5-step ingestion pipeline for a PGS scoring file.

    Steps:
      1. Download from PGS Catalog (or use cache)
      2. Parse metadata from header comments
      3. Normalize via parse_pgs_scoring_file -> scoring_clean.tsv.gz
      4. Liftover if GRCh37 -> GRCh38
      5. QC checks -> eligibility.json

    Output files in /data/pgs_cache/{PGS_ID}/:
      - metadata.json
      - scoring_original.txt.gz (symlink to downloaded file)
      - scoring_clean.tsv.gz
      - scoring_plink2.tsv (user-format, chr-prefixed)
      - scoring_refpanel.tsv (ref panel format, bare chroms, both orientations)
      - eligibility.json
    """
    cache_dir = os.path.join(PGS_CACHE, pgs_id)
    os.makedirs(cache_dir, exist_ok=True)

    result = IngestResult(pgs_id=pgs_id, success=False, cache_dir=cache_dir)

    # Check if already ingested (idempotent)
    eligibility_path = os.path.join(cache_dir, "eligibility.json")
    metadata_path = os.path.join(cache_dir, "metadata.json")
    clean_path = os.path.join(cache_dir, "scoring_clean.tsv.gz")
    plink2_path = os.path.join(cache_dir, "scoring_plink2.tsv")
    refpanel_path = os.path.join(cache_dir, "scoring_refpanel.tsv")

    if not force and all(os.path.exists(p) for p in [eligibility_path, metadata_path, clean_path, plink2_path, refpanel_path]):
        try:
            with open(metadata_path) as f:
                result.metadata = json.load(f)
            with open(eligibility_path) as f:
                result.eligibility = json.load(f)
            result.scoring_clean = clean_path
            result.scoring_plink2 = plink2_path
            result.success = result.eligibility.get("status") != "rejected"
            logger.info(f"{pgs_id}: already ingested, skipping")
            return result
        except (json.JSONDecodeError, KeyError):
            pass  # Re-ingest on corrupted files

    # Step 1: Download
    logger.info(f"{pgs_id}: step 1/5 — downloading")
    scoring_file = _download(pgs_id)
    if not scoring_file:
        result.error = "Download failed"
        return result
    result.scoring_file = scoring_file

    # Step 2 + 3: Parse metadata and normalize
    logger.info(f"{pgs_id}: step 2-3/5 — parsing and normalizing")
    try:
        metadata, variants = parse_pgs_scoring_file(scoring_file)
    except Exception as e:
        result.error = f"Parse failed: {e}"
        logger.error(f"{pgs_id}: parse failed: {e}")
        return result

    result.metadata = metadata

    if not variants:
        result.error = "No usable variants in scoring file"
        result.eligibility = {"status": "rejected", "reasons": ["zero_variants"]}
        _save_json(eligibility_path, result.eligibility)
        _save_json(metadata_path, metadata)
        return result

    # Write normalized clean file
    _write_clean_tsv(variants, clean_path)
    result.scoring_clean = clean_path

    # Step 4: Liftover if needed
    positions_build = metadata.get("positions_build", "GRCh38")
    if positions_build == "GRCh37":
        logger.info(f"{pgs_id}: step 4/5 — lifting over GRCh37 → GRCh38")
        variants = _liftover_variants(variants, "GRCh37", "GRCh38")
        if not variants:
            result.error = "Liftover failed (too few variants mapped)"
            result.eligibility = {"status": "rejected", "reasons": ["liftover_failed"]}
            _save_json(eligibility_path, result.eligibility)
            _save_json(metadata_path, metadata)
            return result
        metadata["liftover"] = "GRCh37→GRCh38"
        metadata["positions_build"] = "GRCh38"
        # Re-write clean file with lifted positions
        _write_clean_tsv(variants, clean_path)
    else:
        logger.info(f"{pgs_id}: step 4/5 — no liftover needed (build={positions_build})")

    # Write plink2 scoring files (both formats)
    n_user = to_user_plink2_format(variants, plink2_path, chr_prefix=True)
    n_ref = to_refpanel_plink2_format(variants, refpanel_path)
    result.scoring_plink2 = plink2_path
    metadata["variant_count"] = n_user
    logger.info(f"{pgs_id}: wrote {n_user} user-format variants, {n_ref} ref-panel lines")

    # Step 5: QC
    logger.info(f"{pgs_id}: step 5/5 — QC checks")
    eligibility = _run_qc(variants, metadata)
    result.eligibility = eligibility

    # Save outputs
    _save_json(metadata_path, metadata)
    _save_json(eligibility_path, eligibility)

    result.success = eligibility.get("status") != "rejected"
    logger.info(f"{pgs_id}: ingestion complete — status={eligibility['status']}, "
                f"variants={len(variants)}")
    return result


def _download(pgs_id: str) -> Optional[str]:
    """Download harmonized scoring file from PGS Catalog. Returns cached path."""
    cache_dir = os.path.join(PGS_CACHE, pgs_id)
    os.makedirs(cache_dir, exist_ok=True)

    # Check if already cached
    for suffix in ["_hmPOS_GRCh38.txt.gz", "_hmPOS_GRCh37.txt.gz", ".txt.gz"]:
        cached = os.path.join(cache_dir, f"{pgs_id}{suffix}")
        if os.path.exists(cached) and os.path.getsize(cached) > 100:
            logger.info(f"{pgs_id}: using cached scoring file: {cached}")
            return cached

    # Download GRCh38 harmonized version
    url = (f"https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/{pgs_id}/"
           f"ScoringFiles/Harmonized/{pgs_id}_hmPOS_GRCh38.txt.gz")
    dest = os.path.join(cache_dir, f"{pgs_id}_hmPOS_GRCh38.txt.gz")

    logger.info(f"{pgs_id}: downloading from PGS Catalog...")
    try:
        rc = subprocess.run(["wget", "-q", "-O", dest, url],
                            timeout=300, capture_output=True).returncode
        if rc == 0 and os.path.exists(dest) and os.path.getsize(dest) > 100:
            return dest
    except subprocess.TimeoutExpired:
        pass

    # Try without harmonization
    url2 = (f"https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/{pgs_id}/"
            f"ScoringFiles/{pgs_id}.txt.gz")
    dest2 = os.path.join(cache_dir, f"{pgs_id}.txt.gz")
    try:
        rc = subprocess.run(["wget", "-q", "-O", dest2, url2],
                            timeout=300, capture_output=True).returncode
        if rc == 0 and os.path.exists(dest2) and os.path.getsize(dest2) > 100:
            return dest2
    except subprocess.TimeoutExpired:
        pass

    logger.error(f"{pgs_id}: failed to download scoring file")
    return None


def _write_clean_tsv(variants, output_path: str):
    """Write normalized variants to a gzipped TSV."""
    with gzip.open(output_path, 'wt') as f:
        f.write("chrom\tpos\teffect_allele\tother_allele\tweight\n")
        for v in variants:
            f.write(f"{v.chrom}\t{v.pos}\t{v.effect_allele}\t{v.other_allele}\t{v.weight}\n")


def _liftover_variants(variants, from_build: str, to_build: str):
    """Lift over variant positions using UCSC liftOver. Returns new variant list or None."""
    chain = CHAIN_FILES.get((from_build, to_build))
    if not chain or not os.path.exists(chain) or not os.path.exists(LIFTOVER_BIN):
        logger.warning(f"Liftover {from_build}→{to_build}: missing binary or chain file")
        return None

    with tempfile.TemporaryDirectory(prefix="pgs_liftover_") as tmpdir:
        bed_in = os.path.join(tmpdir, "in.bed")
        bed_out = os.path.join(tmpdir, "out.bed")
        bed_unmap = os.path.join(tmpdir, "unmapped.bed")

        # Write BED (0-based)
        with open(bed_in, "w") as f:
            for i, v in enumerate(variants):
                try:
                    pos = int(v.pos)
                except ValueError:
                    continue
                f.write(f"chr{v.chrom}\t{pos - 1}\t{pos}\t{i}\n")

        result = subprocess.run([LIFTOVER_BIN, bed_in, chain, bed_out, bed_unmap],
                                capture_output=True, timeout=60)
        if result.returncode != 0:
            logger.warning(f"liftOver failed: {result.stderr.decode()[:200]}")
            return None

        # Parse lifted positions
        lifted_map = {}
        if os.path.exists(bed_out):
            with open(bed_out) as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 4:
                        new_pos = int(parts[2])  # end = 1-based
                        orig_idx = int(parts[3])
                        lifted_map[orig_idx] = str(new_pos)

        lift_rate = len(lifted_map) / len(variants) if variants else 0
        if lift_rate < 0.5:
            logger.warning(f"Liftover only mapped {lift_rate:.0%} — aborting")
            return None

        # Build new variant list with lifted positions
        from .match_logic import ScoringVariant
        lifted = []
        for i, v in enumerate(variants):
            if i in lifted_map:
                lifted.append(ScoringVariant(
                    chrom=v.chrom, pos=lifted_map[i],
                    effect_allele=v.effect_allele, other_allele=v.other_allele,
                    weight=v.weight,
                ))

        logger.info(f"Liftover {from_build}→{to_build}: "
                    f"{len(lifted)}/{len(variants)} variants ({lift_rate:.0%})")
        return lifted


def _run_qc(variants, metadata: Dict) -> Dict:
    """Run QC checks on parsed variants. Returns eligibility dict."""
    reasons = []
    n = len(variants)

    # Check 1: Variant count
    if n < 5:
        reasons.append(f"too_few_variants ({n})")

    # Check 2: Valid chromosomes
    invalid_chroms = set()
    for v in variants:
        if v.chrom not in VALID_CHROMS:
            invalid_chroms.add(v.chrom)
    if invalid_chroms:
        reasons.append(f"invalid_chroms: {sorted(invalid_chroms)}")

    # Check 3: Valid alleles
    invalid_alleles = 0
    for v in variants:
        if v.effect_allele.upper() not in VALID_ALLELES:
            invalid_alleles += 1
    if invalid_alleles > n * 0.1:  # >10% invalid
        reasons.append(f"invalid_alleles: {invalid_alleles}/{n} ({invalid_alleles/n:.0%})")

    # Check 4: Weight values
    bad_weights = 0
    for v in variants:
        try:
            w = float(v.weight)
            if w != w:  # NaN
                bad_weights += 1
        except ValueError:
            bad_weights += 1
    if bad_weights > 0:
        reasons.append(f"bad_weights: {bad_weights}")

    # Determine status
    if any("too_few" in r for r in reasons):
        status = "rejected"
    elif reasons:
        status = "flagged"
    else:
        status = "ok"

    return {
        "status": status,
        "reasons": reasons,
        "variant_count": n,
        "genome_build": metadata.get("positions_build", "unknown"),
    }


def _save_json(path: str, data: Dict):
    """Write JSON to file."""
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
