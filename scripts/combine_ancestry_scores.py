#!/usr/bin/env python3
"""Combine PRS-CSx ancestry-specific scores using sample ancestry proportions.

For each sample, computes a weighted linear combination of population-specific
PGS scores based on the sample's ancestry proportions. Also computes percentiles
against ancestry-matched reference distributions.

Usage:
    python scripts/combine_ancestry_scores.py --trait CAD --out-dir /data/prs_csx_output/CAD --pops EUR,EAS
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.config import DATA_DIR
from backend.database import SessionLocal
from backend.models.schemas import SampleAncestry, AncestryPGSResult


def load_scores(out_dir: str, trait: str, pop: str) -> dict:
    """Load plink2 .sscore file for a population."""
    sscore_file = Path(out_dir) / f"{trait}_{pop}_scores.sscore"
    if not sscore_file.exists():
        return {}

    df = pd.read_csv(sscore_file, sep="\t")
    # plink2 sscore columns: #FID, IID, ALLELE_CT, NAMED_ALLELE_DOSAGE_SUM, SCORE1_AVG
    score_col = [c for c in df.columns if "SCORE" in c.upper() and "AVG" in c.upper()]
    if not score_col:
        score_col = [c for c in df.columns if "SCORE" in c.upper()]
    if not score_col:
        return {}

    id_col = "IID" if "IID" in df.columns else df.columns[1]
    return dict(zip(df[id_col].astype(str), df[score_col[0]].astype(float)))


def compute_combined_pgs(
    sample_id: str,
    ancestry_props: dict,
    pop_scores: dict,
    available_pops: list,
) -> dict:
    """Compute ancestry-weighted combined PGS for a sample."""
    # Individual population scores for this sample
    components = {}
    for pop in available_pops:
        if pop in pop_scores and sample_id in pop_scores[pop]:
            components[pop] = pop_scores[pop][sample_id]

    if not components:
        return None

    # Weighted linear combination
    combined = 0.0
    covered = 0.0
    for pop, score in components.items():
        prop_key = pop.upper()
        weight = ancestry_props.get(prop_key, 0.0)
        combined += weight * score
        covered += weight

    # Uncovered fraction
    uncovered = 1.0 - covered

    # Confidence tier
    primary_pop = max(ancestry_props.items(), key=lambda x: x[1])[0]
    is_admixed = max(ancestry_props.values()) < 0.85

    if primary_pop in components and not is_admixed:
        confidence = "high"
    elif covered >= 0.80:
        confidence = "moderate"
    else:
        confidence = "low"

    # Ancestry warnings
    warnings = []
    if uncovered > 0.15:
        uncov_pops = [p for p in ["EUR", "EAS", "AFR", "SAS", "AMR"]
                      if p not in components and ancestry_props.get(p, 0) > 0.05]
        if uncov_pops:
            warnings.append(
                f"{', '.join(uncov_pops)} ancestry ({uncovered*100:.0f}%) not covered by available GWAS"
            )
    if is_admixed:
        warnings.append("Admixed sample — combined score uses linear approximation")

    return {
        "combined_score": combined,
        "components": components,
        "covered_fraction": covered,
        "confidence": confidence,
        "warnings": warnings,
        "primary_ancestry": primary_pop,
        "is_admixed": is_admixed,
    }


def compute_percentile(
    score: float,
    primary_ancestry: str,
    reference_scores: dict,
) -> dict:
    """Place score against ancestry-matched reference distribution."""
    if primary_ancestry in reference_scores and len(reference_scores[primary_ancestry]) > 0:
        ref = np.array(reference_scores[primary_ancestry])
        pct = (np.sum(ref < score) / len(ref)) * 100
        return {"percentile": round(pct, 1), "reference_population": primary_ancestry, "reference_n": len(ref)}

    # Fallback to EUR
    if "EUR" in reference_scores and len(reference_scores["EUR"]) > 0:
        ref = np.array(reference_scores["EUR"])
        pct = (np.sum(ref < score) / len(ref)) * 100
        return {"percentile": round(pct, 1), "reference_population": "EUR (fallback)", "reference_n": len(ref)}

    return {"percentile": None, "reference_population": None, "reference_n": 0}


def load_reference_distributions(out_dir: str, trait: str, pops: list) -> dict:
    """Load reference score distributions from 1KG scored samples."""
    ref_dir = Path(DATA_DIR) / "ancestry" / "reference_score_distributions"
    distributions = {}

    for pop in pops:
        ref_file = ref_dir / pop / f"{trait}.npy"
        if ref_file.exists():
            distributions[pop] = np.load(str(ref_file)).tolist()
        else:
            # Try to extract from sscore file if 1KG samples were scored
            sscore = Path(out_dir) / f"{trait}_{pop}_scores.sscore"
            if sscore.exists():
                df = pd.read_csv(sscore, sep="\t")
                score_col = [c for c in df.columns if "SCORE" in c.upper()]
                if score_col:
                    distributions[pop] = df[score_col[0]].dropna().tolist()

    return distributions


def combine_for_trait(trait: str, out_dir: str, pops: list):
    """Main function: combine scores for all samples for a given trait."""
    print(f"\n  Combining scores for {trait} (pops: {', '.join(pops)})")

    # Load population-specific scores
    pop_scores = {}
    for pop in pops:
        scores = load_scores(out_dir, trait, pop)
        if scores:
            pop_scores[pop] = scores
            print(f"    {pop}: {len(scores)} samples scored")

    if not pop_scores:
        print("    ERROR: No scores loaded")
        return

    # Load ancestry data
    db = SessionLocal()
    try:
        ancestries = db.query(SampleAncestry).all()
        if not ancestries:
            print("    ERROR: No ancestry data in database. Run ancestry inference first.")
            return

        # Load reference distributions for percentiles
        ref_dists = load_reference_distributions(out_dir, trait, pops)

        results = []
        for anc in ancestries:
            props = {
                "EUR": anc.eur_proportion or 0,
                "EAS": anc.eas_proportion or 0,
                "AFR": anc.afr_proportion or 0,
                "SAS": anc.sas_proportion or 0,
                "AMR": anc.amr_proportion or 0,
            }

            combined = compute_combined_pgs(anc.sample_id, props, pop_scores, pops)
            if not combined:
                continue

            # Percentile for combined score
            pct_info = compute_percentile(
                combined["combined_score"],
                combined["primary_ancestry"],
                ref_dists,
            )

            # Store combined result
            result = AncestryPGSResult(
                sample_id=anc.sample_id,
                trait=trait,
                pgs_id=f"PRSCSx_{trait}",
                scoring_method="PRSCSx_combined",
                raw_score=combined["combined_score"],
                combined_score=combined["combined_score"],
                eur_component=combined["components"].get("EUR"),
                eas_component=combined["components"].get("EAS"),
                percentile=pct_info["percentile"],
                reference_population=pct_info["reference_population"],
                reference_n=pct_info["reference_n"],
                confidence=combined["confidence"],
                covered_fraction=combined["covered_fraction"],
                ancestry_warnings=combined["warnings"],
                pgs_training_pop="Multi",
                pgs_training_pop_match=combined["primary_ancestry"] in pops,
            )

            # Check for existing
            existing = db.query(AncestryPGSResult).filter(
                AncestryPGSResult.sample_id == anc.sample_id,
                AncestryPGSResult.pgs_id == f"PRSCSx_{trait}",
                AncestryPGSResult.scoring_method == "PRSCSx_combined",
            ).first()

            if existing:
                for col in ["raw_score", "combined_score", "eur_component", "eas_component",
                            "percentile", "reference_population", "reference_n",
                            "confidence", "covered_fraction", "ancestry_warnings",
                            "pgs_training_pop", "pgs_training_pop_match"]:
                    setattr(existing, col, getattr(result, col))
            else:
                db.add(result)

            results.append(combined)
            print(f"    {anc.sample_id}: combined={combined['combined_score']:.6f}, "
                  f"confidence={combined['confidence']}, covered={combined['covered_fraction']:.0%}")

            # Also store individual population component scores
            for pop, score in combined["components"].items():
                pop_pct = compute_percentile(score, pop, ref_dists)

                pop_result = AncestryPGSResult(
                    sample_id=anc.sample_id,
                    trait=trait,
                    pgs_id=f"PRSCSx_{trait}",
                    scoring_method=f"PRSCSx_{pop}",
                    raw_score=score,
                    percentile=pop_pct["percentile"],
                    reference_population=pop_pct["reference_population"],
                    reference_n=pop_pct["reference_n"],
                    confidence="high" if pop == combined["primary_ancestry"] else "moderate",
                    covered_fraction=1.0,
                    ancestry_warnings=[],
                    pgs_training_pop=pop,
                    pgs_training_pop_match=(pop == combined["primary_ancestry"]),
                )

                existing_pop = db.query(AncestryPGSResult).filter(
                    AncestryPGSResult.sample_id == anc.sample_id,
                    AncestryPGSResult.pgs_id == f"PRSCSx_{trait}",
                    AncestryPGSResult.scoring_method == f"PRSCSx_{pop}",
                ).first()

                if existing_pop:
                    for col in ["raw_score", "percentile", "reference_population", "reference_n",
                                "confidence", "covered_fraction", "ancestry_warnings",
                                "pgs_training_pop", "pgs_training_pop_match"]:
                        setattr(existing_pop, col, getattr(pop_result, col))
                else:
                    db.add(pop_result)

        db.commit()
        print(f"\n    Stored {len(results)} combined results in database")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine PRS-CSx scores")
    parser.add_argument("--trait", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--pops", required=True, help="Comma-separated populations")
    args = parser.parse_args()

    combine_for_trait(args.trait, args.out_dir, args.pops.split(","))
