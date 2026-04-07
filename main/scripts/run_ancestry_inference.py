#!/usr/bin/env python3
"""Ancestry Inference Pipeline — PCA + Random Forest classifier.

Runs on our WGS samples merged with 1000 Genomes reference panel.
Outputs ancestry proportions and PCA coordinates for each sample.

Usage:
    cd <repo-root>
    python scripts/run_ancestry_inference.py

Prerequisites:
    - 1KG reference panel at /data/reference/1kg/
    - Our samples in plink2 format at /data/ancestry/our_samples.*
    - plink2, bcftools in PATH
    - scikit-learn, numpy, pandas, joblib installed
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.config import DATA_DIR
from backend.database import SessionLocal
from backend.models.schemas import SampleAncestry

# ── Configuration ─────────────────────────────────────────────

ANCESTRY_DIR = Path(DATA_DIR) / "ancestry"
REFERENCE_DIR = Path(DATA_DIR) / "reference" / "1kg"
PANEL_FILE = REFERENCE_DIR / "integrated_call_samples_v3.20130502.ALL.panel"
SUPERPOPS = ["EUR", "EAS", "AFR", "SAS", "AMR"]
N_PCS = 10  # Number of PCs for classification
N_PCS_COMPUTE = 20  # Number of PCs to compute

ANCESTRY_DIR.mkdir(parents=True, exist_ok=True)


def run_cmd(cmd, desc=""):
    """Run a shell command, print output on failure."""
    print(f"  [{desc}] {' '.join(cmd[:5])}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr[:500]}")
        raise RuntimeError(f"{desc} failed: {result.stderr[:200]}")
    return result


# ── Step 1: Extract overlapping SNPs ─────────────────────────

def step1_extract_snps():
    """Extract high-quality, LD-pruned autosomal SNPs from our samples."""
    print("\n=== Step 1: Extract & LD-prune SNPs ===")

    our_pfile = ANCESTRY_DIR / "our_samples"
    if not our_pfile.with_suffix(".pgen").exists():
        print("  ERROR: our_samples.pgen not found. Please prepare plink2 files first.")
        print(f"  Expected at: {our_pfile}.pgen")
        sys.exit(1)

    # LD pruning
    run_cmd([
        "plink2",
        "--pfile", str(our_pfile),
        "--autosome",
        "--maf", "0.01",
        "--geno", "0.02",
        "--snps-only",
        "--max-alleles", "2",
        "--indep-pairwise", "1000", "50", "0.2",
        "--out", str(ANCESTRY_DIR / "pruned_snps"),
    ], "LD pruning")

    prune_in = ANCESTRY_DIR / "pruned_snps.prune.in"
    n_snps = sum(1 for _ in open(prune_in))
    print(f"  Retained {n_snps:,} LD-pruned SNPs")

    # Extract pruned SNPs from our samples
    run_cmd([
        "plink2",
        "--pfile", str(our_pfile),
        "--extract", str(prune_in),
        "--make-pgen",
        "--out", str(ANCESTRY_DIR / "our_pruned"),
    ], "Extract pruned")

    return prune_in


# ── Step 2: Merge with 1KG + PCA ─────────────────────────────

def step2_merge_and_pca(prune_in):
    """Merge our samples with 1KG reference and run PCA."""
    print("\n=== Step 2: Merge with 1KG & PCA ===")

    ref_pfile = REFERENCE_DIR / "1kg_all"
    if not ref_pfile.with_suffix(".pgen").exists():
        print("  ERROR: 1KG plink2 files not found.")
        print(f"  Expected at: {ref_pfile}.pgen")
        print("  Please convert 1KG VCFs to plink2 format first.")
        sys.exit(1)

    # Extract same SNPs from 1KG
    run_cmd([
        "plink2",
        "--pfile", str(ref_pfile),
        "--extract", str(prune_in),
        "--make-pgen",
        "--out", str(ANCESTRY_DIR / "ref_pruned"),
    ], "Extract ref SNPs")

    # Remove ambiguous A/T and C/G SNPs
    our_bim = pd.read_csv(ANCESTRY_DIR / "our_pruned.pvar", sep="\t", comment="#",
                           names=["CHROM", "POS", "ID", "REF", "ALT"], usecols=[0, 1, 2, 3, 4])
    ambiguous = our_bim[
        ((our_bim["REF"] == "A") & (our_bim["ALT"] == "T")) |
        ((our_bim["REF"] == "T") & (our_bim["ALT"] == "A")) |
        ((our_bim["REF"] == "C") & (our_bim["ALT"] == "G")) |
        ((our_bim["REF"] == "G") & (our_bim["ALT"] == "C"))
    ]
    if len(ambiguous) > 0:
        exclude_file = ANCESTRY_DIR / "ambiguous_snps.txt"
        ambiguous["ID"].to_csv(exclude_file, index=False, header=False)
        print(f"  Removing {len(ambiguous)} ambiguous A/T C/G SNPs")

        for prefix in ["our_pruned", "ref_pruned"]:
            run_cmd([
                "plink2",
                "--pfile", str(ANCESTRY_DIR / prefix),
                "--exclude", str(exclude_file),
                "--make-pgen",
                "--out", str(ANCESTRY_DIR / f"{prefix}_clean"),
            ], f"Remove ambiguous from {prefix}")
    else:
        # Symlink clean files
        for prefix in ["our_pruned", "ref_pruned"]:
            for ext in [".pgen", ".pvar", ".psam"]:
                src = ANCESTRY_DIR / f"{prefix}{ext}"
                dst = ANCESTRY_DIR / f"{prefix}_clean{ext}"
                if not dst.exists():
                    os.symlink(src, dst)

    # Merge
    merge_list = ANCESTRY_DIR / "merge_list.txt"
    merge_list.write_text(str(ANCESTRY_DIR / "ref_pruned_clean") + "\n")

    run_cmd([
        "plink2",
        "--pfile", str(ANCESTRY_DIR / "our_pruned_clean"),
        "--pmerge-list", str(merge_list),
        "--make-pgen",
        "--out", str(ANCESTRY_DIR / "merged"),
    ], "Merge samples")

    # PCA
    run_cmd([
        "plink2",
        "--pfile", str(ANCESTRY_DIR / "merged"),
        "--pca", str(N_PCS_COMPUTE),
        "--out", str(ANCESTRY_DIR / "ancestry_pca"),
    ], "PCA")

    print(f"  PCA complete: {ANCESTRY_DIR / 'ancestry_pca.eigenvec'}")


# ── Step 3: Train classifier & predict ───────────────────────

def step3_classify():
    """Train RF classifier on 1KG reference, predict our samples."""
    print("\n=== Step 3: Ancestry classification ===")

    # Load PCA results
    eigenvec = pd.read_csv(
        ANCESTRY_DIR / "ancestry_pca.eigenvec", sep="\t",
        comment="#",
    )
    # Handle both plink1 and plink2 eigenvec formats
    if "#FID" in eigenvec.columns:
        eigenvec = eigenvec.rename(columns={"#FID": "FID"})
    if "IID" in eigenvec.columns:
        sample_col = "IID"
    elif "#IID" in eigenvec.columns:
        sample_col = "#IID"
        eigenvec = eigenvec.rename(columns={"#IID": "IID"})
        sample_col = "IID"
    else:
        sample_col = eigenvec.columns[0]

    pc_cols = [c for c in eigenvec.columns if c.startswith("PC")][:N_PCS]

    # Load 1KG panel info
    panel = pd.read_csv(PANEL_FILE, sep="\t")
    ref_samples = set(panel["sample"].values)

    # Split reference vs our samples
    ref_mask = eigenvec[sample_col].isin(ref_samples)
    X_ref = eigenvec.loc[ref_mask, pc_cols].values
    y_ref = panel.set_index("sample").loc[
        eigenvec.loc[ref_mask, sample_col].values, "super_pop"
    ].values

    X_our = eigenvec.loc[~ref_mask, pc_cols].values
    our_ids = eigenvec.loc[~ref_mask, sample_col].values

    print(f"  Reference samples: {len(X_ref)}, Our samples: {len(X_our)}")
    print(f"  PCs used: {len(pc_cols)}")

    # Train classifier
    clf = RandomForestClassifier(n_estimators=500, random_state=42, n_jobs=-1)
    clf.fit(X_ref, y_ref)

    # Cross-validation accuracy
    from sklearn.model_selection import cross_val_score
    cv_scores = cross_val_score(clf, X_ref, y_ref, cv=5, n_jobs=-1)
    print(f"  CV accuracy: {cv_scores.mean():.4f} +/- {cv_scores.std():.4f}")

    # Save classifier
    joblib.dump(clf, str(ANCESTRY_DIR / "ancestry_classifier.joblib"))

    # Predict probabilities for our samples
    probs = clf.predict_proba(X_our)
    labels = clf.classes_

    print("\n  Ancestry predictions:")
    results = []
    for i, sample in enumerate(our_ids):
        props = dict(zip(labels, probs[i].round(4)))
        primary, is_admixed, desc = tag_ancestry(props)
        pcs = eigenvec.loc[eigenvec[sample_col] == sample, pc_cols].values[0]

        print(f"    {sample}: {desc} ({props})")
        results.append({
            "sample_id": sample,
            "proportions": props,
            "primary": primary,
            "is_admixed": is_admixed,
            "description": desc,
            "pcs": pcs.tolist(),
        })

    # Also save reference PCA points for plotting
    ref_pca_points = []
    for idx in eigenvec.loc[ref_mask].index:
        row = eigenvec.loc[idx]
        sample_name = row[sample_col]
        pop = panel.set_index("sample").loc[sample_name, "super_pop"]
        ref_pca_points.append({
            "sample_id": sample_name,
            "pc1": float(row[pc_cols[0]]),
            "pc2": float(row[pc_cols[1]]),
            "pc3": float(row[pc_cols[2]]) if len(pc_cols) > 2 else 0,
            "population": pop,
        })

    ref_pca_file = ANCESTRY_DIR / "reference_pca_points.json"
    ref_pca_file.write_text(json.dumps(ref_pca_points))
    print(f"  Saved {len(ref_pca_points)} reference PCA points")

    return results


def tag_ancestry(proportions: dict) -> tuple:
    """Returns (primary_ancestry, is_admixed, description)."""
    sorted_pops = sorted(proportions.items(), key=lambda x: -x[1])
    primary = sorted_pops[0][0]
    primary_frac = sorted_pops[0][1]

    if primary_frac >= 0.85:
        return primary, False, primary

    components = [(pop, frac) for pop, frac in sorted_pops if frac >= 0.10]
    desc = "/".join(pop for pop, _ in components)
    return primary, True, f"{desc} admixed"


# ── Step 4: Store in database ─────────────────────────────────

def step4_store(results):
    """Store ancestry results in the database."""
    print("\n=== Step 4: Storing results ===")

    db = SessionLocal()
    try:
        for r in results:
            existing = db.query(SampleAncestry).filter(
                SampleAncestry.sample_id == r["sample_id"]
            ).first()

            data = {
                "sample_id": r["sample_id"],
                "eur_proportion": r["proportions"].get("EUR", 0),
                "eas_proportion": r["proportions"].get("EAS", 0),
                "afr_proportion": r["proportions"].get("AFR", 0),
                "sas_proportion": r["proportions"].get("SAS", 0),
                "amr_proportion": r["proportions"].get("AMR", 0),
                "primary_ancestry": r["primary"],
                "is_admixed": r["is_admixed"],
                "admixture_description": r["description"],
                "inference_method": "RF_classifier",
            }
            # Add PC coordinates
            for j, pc in enumerate(r["pcs"][:10], 1):
                data[f"pc{j}"] = float(pc)

            if existing:
                for k, v in data.items():
                    setattr(existing, k, v)
            else:
                db.add(SampleAncestry(**data))

        db.commit()
        print(f"  Stored ancestry for {len(results)} samples")
    finally:
        db.close()


# ── Main ──────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Ancestry Inference Pipeline")
    print("=" * 60)

    prune_in = step1_extract_snps()
    step2_merge_and_pca(prune_in)
    results = step3_classify()
    step4_store(results)

    print("\n" + "=" * 60)
    print("  Pipeline complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
