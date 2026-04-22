#!/usr/bin/env python3
"""
Generate precomputed EUR GRCh38 reference stats for PGS IDs that lack them.

Scores the full 1000 Genomes Phase 3 reference panel with each PGS,
extracts EUR sample AVG scores, and saves mean/std/n_samples.
"""
import os
import sys
import json
import gzip
import statistics
import subprocess
import tempfile
import shutil
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from runners import _download_pgs_scoring_file, PLINK2, REF_PANEL, REF_PANEL_STATS

TARGET_PGS_IDS = [
    "PGS002012", "PGS002231", "PGS003573", "PGS002319", "PGS002391",
    "PGS002440", "PGS002489", "PGS002538", "PGS002587", "PGS002636",
    "PGS002685", "PGS001931", "PGS002148", "PGS003516",
]


def prepare_ref_panel_scoring(scoring_file, output_path):
    """Convert PGS scoring file to plink2 format matching ref panel IDs.

    Ref panel variant IDs are chr:pos:ref:alt (e.g. '1:751133:C:CGT').
    We emit both allele orientations so plink2 can match either one.
    """
    opener = gzip.open if scoring_file.endswith(".gz") else open
    cols = None
    score_lines = []

    with opener(scoring_file, "rt") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if cols is None:
                cols = parts
                continue

            def col(name):
                try:
                    idx = cols.index(name)
                    return parts[idx] if idx < len(parts) else ""
                except ValueError:
                    return ""

            chrom = col("hm_chr") or col("chr_name")
            pos = col("hm_pos") or col("chr_position")
            ea = col("effect_allele")
            oa = col("other_allele") or col("hm_inferOtherAllele")
            w = col("effect_weight")

            if not chrom or not pos or chrom == "NA" or pos == "NA":
                continue
            if chrom.startswith("chr"):
                chrom = chrom[3:]
            if chrom not in [str(i) for i in range(1, 23)] + ["X", "Y"]:
                continue
            if not ea or not w:
                continue
            try:
                float(w)
            except ValueError:
                continue

            # Emit both allele orientations to match ref panel chr:pos:ref:alt IDs
            if oa:
                score_lines.append(f"{chrom}:{pos}:{oa}:{ea}\t{ea}\t{w}")
                score_lines.append(f"{chrom}:{pos}:{ea}:{oa}\t{ea}\t{w}")
            else:
                score_lines.append(f"{chrom}:{pos}:N:{ea}\t{ea}\t{w}")

    if not score_lines:
        return None, 0

    # Deduplicate by (variant_id, allele)
    seen = set()
    unique = []
    for line in score_lines:
        parts = line.split("\t")
        key = (parts[0], parts[1])
        if key not in seen:
            seen.add(key)
            unique.append(line)

    with open(output_path, "w") as f:
        f.write("ID\tA1\tWEIGHT\n")
        for line in unique:
            f.write(line + "\n")

    return output_path, len(unique)


def load_eur_samples():
    """Load EUR sample IDs from reference panel .psam file."""
    psam = REF_PANEL + ".psam"
    eur_iids = []
    with open(psam) as f:
        header = f.readline().strip().split("\t")
        iid_idx = header.index("IID") if "IID" in header else 0
        pop_idx = header.index("SuperPop") if "SuperPop" in header else -1
        for line in f:
            parts = line.strip().split("\t")
            if pop_idx >= 0 and parts[pop_idx] == "EUR":
                eur_iids.append(parts[iid_idx])
    return set(eur_iids)


def score_ref_panel(pgs_id, plink2_scoring, tmpdir):
    """Score the full 1000G panel, return EUR SCORE1_AVG values."""
    out_prefix = os.path.join(tmpdir, f"{pgs_id}_ref")

    cmd = [
        PLINK2,
        "--pfile", REF_PANEL, "vzs",
        "--score", plink2_scoring,
        "header-read",
        "cols=+scoresums",
        "no-mean-imputation",
        "--score-col-nums", "3",
        "--allow-extra-chr",
        "--out", out_prefix,
        "--threads", "8",
        "--memory", "32000",
    ]

    logger.info(f"Running plink2 --score...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

    # Log warnings even on success
    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            if "Warning" in line or "Error" in line or "variants" in line.lower():
                logger.info(f"  plink2: {line.strip()}")

    if result.returncode not in (0, 3):  # rc=3 is warnings-only
        logger.error(f"{pgs_id}: plink2 --score failed (rc={result.returncode})")
        logger.error(result.stderr[:1000])
        return None

    sscore_file = out_prefix + ".sscore"
    if not os.path.exists(sscore_file):
        logger.error(f"{pgs_id}: no .sscore output")
        return None

    eur_samples = load_eur_samples()
    eur_avgs = []

    with open(sscore_file) as f:
        header = f.readline().strip().replace("#", "").split("\t")
        iid_idx = next((i for i, h in enumerate(header) if h.strip() == "IID"), 0)
        avg_idx = next((i for i, h in enumerate(header) if "AVG" in h and "ALLELE" not in h), None)
        sum_idx = next((i for i, h in enumerate(header) if "SUM" in h and "ALLELE" not in h and "DOSAGE" not in h and "MISSING" not in h), None)

        if avg_idx is None and sum_idx is None:
            logger.error(f"{pgs_id}: no AVG or SUM column in sscore: {header}")
            return None

        for line in f:
            parts = line.strip().split("\t")
            iid = parts[iid_idx].strip()
            if iid in eur_samples:
                try:
                    val = float(parts[avg_idx]) if avg_idx is not None else float(parts[sum_idx])
                    eur_avgs.append(val)
                except (ValueError, IndexError):
                    pass

    logger.info(f"{pgs_id}: {len(eur_avgs)} EUR samples scored")
    return eur_avgs


def generate_stats(pgs_id):
    """Generate precomputed stats for one PGS ID."""
    output_file = os.path.join(REF_PANEL_STATS, f"{pgs_id}_EUR_GRCh38.json")
    if os.path.exists(output_file):
        logger.info(f"{pgs_id}: stats already exist, skipping")
        return True

    tmpdir = tempfile.mkdtemp(prefix=f"pgs_stats_{pgs_id}_")
    try:
        # Download scoring file
        scoring_file = _download_pgs_scoring_file(pgs_id, tmpdir)
        if not scoring_file:
            logger.error(f"{pgs_id}: could not download scoring file")
            return False

        # Prepare ref-panel-format scoring file
        plink2_scoring = os.path.join(tmpdir, f"{pgs_id}_ref_score.tsv")
        plink2_scoring, n_variants = prepare_ref_panel_scoring(scoring_file, plink2_scoring)
        if not plink2_scoring:
            logger.error(f"{pgs_id}: could not prepare scoring file (0 variants)")
            return False
        logger.info(f"{pgs_id}: prepared {n_variants} variant entries for ref panel scoring")

        # Score
        eur_avgs = score_ref_panel(pgs_id, plink2_scoring, tmpdir)
        if not eur_avgs or len(eur_avgs) < 50:
            logger.error(f"{pgs_id}: insufficient EUR scores ({len(eur_avgs) if eur_avgs else 0})")
            return False

        mean_val = statistics.mean(eur_avgs)
        std_val = statistics.stdev(eur_avgs)
        median_val = statistics.median(eur_avgs)

        stats = {
            "pgs_id": pgs_id,
            "population": "EUR",
            "genome_build": "GRCh38",
            "mean": mean_val,
            "std": std_val,
            "median": median_val,
            "n_samples": len(eur_avgs),
            "min": min(eur_avgs),
            "max": max(eur_avgs),
        }

        with open(output_file, "w") as f:
            json.dump(stats, f, indent=2)
        logger.info(f"{pgs_id}: SAVED — mean={mean_val:.6g}, std={std_val:.6g}, n={len(eur_avgs)}")
        return True

    except Exception as e:
        logger.exception(f"{pgs_id}: failed — {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    pgs_ids = TARGET_PGS_IDS
    if len(sys.argv) > 1:
        pgs_ids = sys.argv[1:]

    logger.info(f"Generating precomputed stats for {len(pgs_ids)} PGS IDs")
    ok, fail = 0, 0
    for pgs_id in pgs_ids:
        if generate_stats(pgs_id):
            ok += 1
        else:
            fail += 1
        logger.info(f"Progress: {ok} ok, {fail} failed, {len(pgs_ids) - ok - fail} remaining")

    logger.info(f"Done: {ok}/{len(pgs_ids)} succeeded, {fail} failed")


if __name__ == "__main__":
    main()
