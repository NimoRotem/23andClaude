#!/usr/bin/env python3
"""Bootstrap the pgs_eligibility table from existing catalog metadata.

Reads:
    /data/pgs_cache/<PGS>/meta.json      — header metadata from ingest
    /data/pgs_cache/<PGS>/<PGS>_hmPOS_GRCh38.txt.gz  — for catalog n_variants
    pgs_reorganized.md                    — curated trait names
    pipeline/portability_warnings.py      — known low-portability
    pipeline/result_gate.HIDDEN_PGS_IDS   — sensitive traits

Writes one row per PGS to pgs_pipeline.db pgs_eligibility.

Re-runnable. Existing rows are UPDATED (not replaced) when fields change.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline import eligibility_matrix as elig
from pipeline.result_gate import HIDDEN_PGS_IDS, HIDDEN_TRAIT_KEYWORDS

PGS_CACHE = Path("/data/pgs_cache")


def _trait_class_for(trait: str) -> str:
    """Crude classification — extend over time."""
    t = (trait or "").lower()
    if any(k in t for k in HIDDEN_TRAIT_KEYWORDS):
        return "sensitive"
    if any(k in t for k in ("cancer", "tumor", "carcinoma", "neoplasm")):
        return "oncology"
    if any(k in t for k in ("heart", "cad", "atrial", "stroke", "cardiac",
                             "ldl", "hdl", "cholesterol", "triglyceride",
                             "blood pressure", "hypertension")):
        return "cardiometabolic"
    if any(k in t for k in ("diabetes", "glycemic", "insulin", "obesity",
                             "bmi", "body mass")):
        return "metabolic"
    if any(k in t for k in ("depression", "anxiety", "schizo", "bipolar",
                             "alzheimer", "parkinson", "adhd", "autism",
                             "neuro", "cognitive")):
        return "neuro_psych"
    if any(k in t for k in ("autoimmune", "lupus", "crohn", "ulcerative",
                             "rheumatoid", "celiac", "psoriasis")):
        return "immune"
    if any(k in t for k in ("height", "weight", "bmi")):
        return "anthropometric"
    return "other"


def _social_risk_tier(pgs_id: str, trait: str) -> int:
    """0 = none, 1 = caveated, 2 = hidden (sensitive)."""
    if pgs_id in HIDDEN_PGS_IDS:
        return 2
    t = (trait or "").lower()
    if any(k in t for k in HIDDEN_TRAIT_KEYWORDS):
        return 2
    return 0


def _read_meta(pgs_id: str) -> dict:
    p = PGS_CACHE / pgs_id / "meta.json"
    if not p.exists():
        return {}
    try:
        return json.load(open(p))
    except (json.JSONDecodeError, OSError):
        return {}


def _detect_pgs_ids() -> list[str]:
    """Find every PGS we've downloaded."""
    out = []
    if PGS_CACHE.exists():
        for d in PGS_CACHE.iterdir():
            if d.is_dir() and d.name.startswith("PGS"):
                out.append(d.name)
    return sorted(out)


def _parse_dev_eval_ancestries(meta: dict) -> tuple[list[str], list[str]]:
    """Pull dev_ancestry / eval_ancestry from header strings.

    Catalog headers look like: development_ancestry={'EUR'}
                              evaluation_ancestry={'EUR', 'EAS'}
    """
    def _parse(field: str) -> list[str]:
        v = meta.get(field, "") or ""
        if isinstance(v, list):
            return [a.upper() for a in v]
        if not v:
            return []
        # Extract uppercase tokens that look like population codes
        tokens = re.findall(r"\b([A-Z]{2,5})\b", str(v))
        return list({t for t in tokens if t in ("EUR", "EAS", "AFR", "SAS",
                                                 "AMR", "MID", "OCE")})

    return _parse("development_ancestry"), _parse("evaluation_ancestry")


def bootstrap(pgs_id: str) -> bool:
    """Bootstrap one PGS row. Returns True if a row was written."""
    meta = _read_meta(pgs_id)
    trait = (meta.get("trait_reported") or meta.get("trait_efo")
             or meta.get("pgs_id") or pgs_id)
    weight_type = (meta.get("weight_type") or "").lower() or None
    weight_transform = {
        "beta": "identity",
        "log_or": "log_or",
        "log_hr": "log_hr",
    }.get(weight_type)
    dev, evl = _parse_dev_eval_ancestries(meta)
    catalog_n = meta.get("variants_number") or meta.get("variant_count")
    try:
        catalog_n = int(catalog_n) if catalog_n is not None else None
    except (TypeError, ValueError):
        catalog_n = None
    tier = _social_risk_tier(pgs_id, trait)
    status = "hidden" if tier >= 2 else "active"
    percentile_eligible = (tier < 2) and (weight_type in (None, "beta", "log_or", "log_hr"))

    elig.upsert(
        pgs_id,
        trait_name=trait,
        trait_class=_trait_class_for(trait),
        social_risk_tier=tier,
        allowed_ancestries=evl or dev,
        validated_ancestries=evl,
        weight_type=weight_type,
        weight_transform=weight_transform,
        supported_input_types=["vcf", "gvcf", "bam", "cram"],
        percentile_eligible=int(percentile_eligible),
        min_match_rate_default=60.0,
        requires_sex_stratification=0,
        status=status,
        catalog_n_variants=catalog_n,
        effective_n_variants=catalog_n,
        notes=f"Bootstrapped from /data/pgs_cache/{pgs_id}/meta.json",
    )
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pgs", help="Single PGS ID")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    elig.init_db()

    if args.pgs:
        pgs_ids = [args.pgs]
    elif args.all:
        pgs_ids = _detect_pgs_ids()
    else:
        ap.error("provide --pgs or --all")

    n_ok = 0
    for pgs in pgs_ids:
        try:
            if bootstrap(pgs):
                n_ok += 1
        except Exception as e:
            print(f"  ! {pgs}: {e}")
    print(f"Bootstrapped {n_ok}/{len(pgs_ids)} eligibility rows.")


if __name__ == "__main__":
    main()
