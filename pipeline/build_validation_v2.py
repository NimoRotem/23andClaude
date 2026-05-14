"""Phase 1.7 — Build validation with ≥100 anchors + explicit UNVALIDATED state.

Per REMEDIATION_PLAN §1.7:

  Curate a 500-SNP autosomal anchor panel (~50/chromosome) with both
  GRCh37 and GRCh38 coordinates and REF/ALT pairs. Source: gnomAD common
  variants with AF ∈ [0.05, 0.95] in ≥3 populations.

  Decision rule:
    ≥50 anchors observed AND ≥95% REF/ALT-concordant with one build
      → that build, build_status="PASS"
    ≥50 anchors observed but concordance ambiguous
      → build_status="FAIL", return error
    <50 anchors observed
      → build_status="UNVALIDATED"; downstream consumers must supply
        build explicitly or reject the input

The legacy 3-SNP anchor set (rs7412, rs429358, rs1801133) is removed —
those positions alone cannot distinguish builds reliably and are
deprecated.

When PGS Catalog harmonized file exists in the input's build, use it
directly. Liftover only when target-build harmonized file is
unavailable.
"""
from __future__ import annotations

import csv
import json
import logging
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional


log = logging.getLogger("pgs-pipeline.build_validation_v2")


ANCHOR_FIXTURE_PATH = Path(__file__).resolve().parent.parent / \
    "data" / "build_anchors_500snp.tsv"
N_ANCHORS_TARGET = 500
N_ANCHORS_MIN = 50
CONCORDANCE_MIN = 0.95


@dataclass
class BuildAnchor:
    rsid: str
    chrom: str
    pos_grch37: int
    pos_grch38: int
    ref: str
    alt: str
    af_max_pop: float = 0.0


@dataclass
class BuildValidationResult:
    """Per-build concordance + decision."""
    build_status: str        # "PASS" | "FAIL" | "UNVALIDATED"
    chosen_build: Optional[str]
    n_anchors_observed: int
    concordance_grch37: float
    concordance_grch38: float
    n_anchors_concordant_grch37: int
    n_anchors_concordant_grch38: int
    reason: str = ""


def load_anchors(path: str | Path = ANCHOR_FIXTURE_PATH) -> list[BuildAnchor]:
    p = Path(path)
    anchors: list[BuildAnchor] = []
    if not p.exists():
        log.warning("anchor fixture missing at %s", p)
        return anchors
    with p.open() as f:
        reader = csv.DictReader(
            (line for line in f if not line.startswith("#")),
            delimiter="\t",
        )
        for row in reader:
            try:
                anchors.append(BuildAnchor(
                    rsid=row["rsid"],
                    chrom=row["chrom"],
                    pos_grch37=int(row["pos_grch37"]),
                    pos_grch38=int(row["pos_grch38"]),
                    ref=row["ref"].upper(),
                    alt=row["alt"].upper(),
                    af_max_pop=float(row.get("af_max_pop", "0") or 0),
                ))
            except (KeyError, ValueError):
                continue
    return anchors


def validate_vcf_build(
    vcf_path: str | Path,
    anchors: Optional[list[BuildAnchor]] = None,
    *,
    bcftools_path: str = "bcftools",
    min_anchors: int = N_ANCHORS_MIN,
    concordance_threshold: float = CONCORDANCE_MIN,
) -> BuildValidationResult:
    """Streams the VCF over both candidate position sets in one bcftools
    invocation per build. Compares REF allele at each anchor to the
    expected REF for both builds.
    """
    anchors = anchors or load_anchors()
    if not anchors:
        return BuildValidationResult(
            build_status="UNVALIDATED",
            chosen_build=None,
            n_anchors_observed=0,
            concordance_grch37=0.0, concordance_grch38=0.0,
            n_anchors_concordant_grch37=0, n_anchors_concordant_grch38=0,
            reason="anchor fixture missing (build_anchors_500snp.tsv)",
        )

    # Build region files for both builds
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir_s:
        tmpdir = Path(tmpdir_s)
        regions_37 = tmpdir / "anchors_grch37.tsv"
        regions_38 = tmpdir / "anchors_grch38.tsv"
        with regions_37.open("w") as f37, regions_38.open("w") as f38:
            for a in anchors:
                f37.write(f"{a.chrom}\t{a.pos_grch37}\t{a.pos_grch37}\n")
                f38.write(f"{a.chrom}\t{a.pos_grch38}\t{a.pos_grch38}\n")

        def _scan(regions_file: Path, pos_field: str) -> dict[tuple[str, int], str]:
            proc = subprocess.run(
                [bcftools_path, "view", "-R", str(regions_file),
                 "-O", "v", "--no-version", str(vcf_path)],
                capture_output=True, text=True, timeout=180,
            )
            observed: dict[tuple[str, int], str] = {}
            for line in proc.stdout.splitlines():
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 5:
                    continue
                try:
                    observed[(parts[0], int(parts[1]))] = parts[3].upper()
                except ValueError:
                    continue
            return observed

        obs_37 = _scan(regions_37, "pos_grch37")
        obs_38 = _scan(regions_38, "pos_grch38")

    n_obs_any = 0
    n_concordant_37 = 0
    n_concordant_38 = 0
    for a in anchors:
        ref37 = obs_37.get((a.chrom, a.pos_grch37)) or obs_37.get(
            (a.chrom[3:] if a.chrom.startswith("chr") else f"chr{a.chrom}",
             a.pos_grch37)
        )
        ref38 = obs_38.get((a.chrom, a.pos_grch38)) or obs_38.get(
            (a.chrom[3:] if a.chrom.startswith("chr") else f"chr{a.chrom}",
             a.pos_grch38)
        )
        if ref37 is None and ref38 is None:
            continue
        n_obs_any += 1
        if ref37 == a.ref:
            n_concordant_37 += 1
        if ref38 == a.ref:
            n_concordant_38 += 1

    if n_obs_any < min_anchors:
        return BuildValidationResult(
            build_status="UNVALIDATED",
            chosen_build=None,
            n_anchors_observed=n_obs_any,
            concordance_grch37=0.0, concordance_grch38=0.0,
            n_anchors_concordant_grch37=n_concordant_37,
            n_anchors_concordant_grch38=n_concordant_38,
            reason=(f"only {n_obs_any} of {len(anchors)} anchor positions observed "
                    f"(min {min_anchors}); caller must supply build explicitly"),
        )

    c37 = n_concordant_37 / n_obs_any
    c38 = n_concordant_38 / n_obs_any
    if c37 >= concordance_threshold and c38 < concordance_threshold:
        return BuildValidationResult(
            build_status="PASS", chosen_build="GRCh37",
            n_anchors_observed=n_obs_any,
            concordance_grch37=c37, concordance_grch38=c38,
            n_anchors_concordant_grch37=n_concordant_37,
            n_anchors_concordant_grch38=n_concordant_38,
            reason=f"GRCh37 concordance {c37:.3f} ≥ {concordance_threshold}",
        )
    if c38 >= concordance_threshold and c37 < concordance_threshold:
        return BuildValidationResult(
            build_status="PASS", chosen_build="GRCh38",
            n_anchors_observed=n_obs_any,
            concordance_grch37=c37, concordance_grch38=c38,
            n_anchors_concordant_grch37=n_concordant_37,
            n_anchors_concordant_grch38=n_concordant_38,
            reason=f"GRCh38 concordance {c38:.3f} ≥ {concordance_threshold}",
        )
    # Both ≥ threshold (very unusual — almost certainly indicates an
    # error in the anchor fixture or a chimeric VCF) → FAIL
    return BuildValidationResult(
        build_status="FAIL", chosen_build=None,
        n_anchors_observed=n_obs_any,
        concordance_grch37=c37, concordance_grch38=c38,
        n_anchors_concordant_grch37=n_concordant_37,
        n_anchors_concordant_grch38=n_concordant_38,
        reason=(f"ambiguous: GRCh37={c37:.3f}, GRCh38={c38:.3f}, both "
                f"below or above the {concordance_threshold} threshold"),
    )


def write_anchor_fixture_from_gnomad(
    gnomad_vcf: str | Path,
    out_path: str | Path,
    *,
    n_per_chrom: int = 23,   # 22 autosomes × ~23 ≈ 500 (we aim for ~50/chr in spec; tune)
    bcftools_path: str = "bcftools",
) -> int:
    """Helper to BUILD the anchor fixture from a gnomAD common-variants
    VCF. Selects biallelic SNPs with AF ∈ [0.05, 0.95] in ≥3 populations,
    samples evenly across each autosome, and stores both build coords.

    Not invoked by the runtime; this is a one-time fixture builder.
    Requires gnomAD's per-pop INFO/AF_<pop> fields.
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    candidates_by_chrom: dict[str, list[BuildAnchor]] = {}
    cmd = [
        bcftools_path, "query",
        "-f", "%CHROM\t%POS\t%REF\t%ALT\t%INFO/rsid\t%INFO/AF_nfe\t%INFO/AF_afr\t%INFO/AF_eas\t%INFO/AF_sas\t%INFO/AF_amr\n",
        str(gnomad_vcf),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    assert proc.stdout is not None
    for line in proc.stdout:
        cols = line.rstrip("\n").split("\t")
        if len(cols) < 10:
            continue
        chrom, pos_s, ref, alt, rsid = cols[:5]
        if len(ref) != 1 or len(alt) != 1:
            continue
        if ref not in "ACGT" or alt not in "ACGT":
            continue
        try:
            pos = int(pos_s)
            af_pops = [float(a) if a not in ("", ".") else 0.0 for a in cols[5:]]
        except ValueError:
            continue
        n_in_band = sum(1 for af in af_pops if 0.05 <= af <= 0.95)
        if n_in_band < 3:
            continue
        chrom_norm = chrom if chrom.startswith("chr") else f"chr{chrom}"
        # NOTE: this writer assumes positions are GRCh38; the GRCh37
        # coordinate must be filled in via a liftOver pass before the
        # fixture is used.
        candidates_by_chrom.setdefault(chrom_norm, []).append(BuildAnchor(
            rsid=rsid or ".",
            chrom=chrom_norm,
            pos_grch37=0,         # filled in by separate liftOver pass
            pos_grch38=pos,
            ref=ref,
            alt=alt,
            af_max_pop=max(af_pops),
        ))
    proc.wait()
    selected: list[BuildAnchor] = []
    for chrom, lst in candidates_by_chrom.items():
        if not lst:
            continue
        step = max(1, len(lst) // n_per_chrom)
        for a in lst[::step][:n_per_chrom]:
            selected.append(a)
    with out.open("w") as f:
        f.write("# 500-SNP autosomal build-validation anchor fixture\n")
        f.write("# REMEDIATION_PLAN §1.7: gnomAD common variants AF ∈ [0.05, 0.95] in ≥3 pops\n")
        f.write("# pos_grch37 column is filled by a separate liftOver pass after this file is written\n")
        f.write("rsid\tchrom\tpos_grch37\tpos_grch38\tref\talt\taf_max_pop\n")
        for a in selected:
            f.write(f"{a.rsid}\t{a.chrom}\t{a.pos_grch37}\t{a.pos_grch38}\t"
                    f"{a.ref}\t{a.alt}\t{a.af_max_pop:.3f}\n")
    return len(selected)
