"""W1.10 — PCA anchor registry (scaffold).

Per advisor's recommendation, every PCA projection must validate against
known anchor samples spanning all five super-populations plus GIAB
truth-set samples. This module declares the anchor list and the pinned
expected PC coordinates; the actual verification runs against
`pipeline/pca_projection_validation.py`.

Status today:
    - HG002 sample data exists on disk (Ashkenazi Jewish trio child, GIAB)
    - HG00096 / NA12878 / one-per-super-pop NOT YET ON DISK; placeholders below

Once each sample's PCA projection is verified against the live pipeline,
fill in the `expected_pcs` field from the run. The test in
`tests/test_pca_anchors.py` (TODO) will then assert that the live
projection stays within `pc_tolerance` of the pinned value.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PcaAnchor:
    sample_id: str
    super_pop: str              # 'EUR' | 'EAS' | 'AFR' | 'SAS' | 'AMR'
    source: str                 # '1000G' | 'GIAB' | 'HGDP' | etc.
    notes: str = ""
    input_path: Optional[str] = None
    expected_pcs: list[float] = field(default_factory=list)   # PC1..PC4
    pc_tolerance: float = 0.01
    enabled: bool = False       # only enable once expected_pcs is pinned


ANCHORS: list[PcaAnchor] = [
    # GIAB Ashkenazi Jewish child (NA24385 / HG002) — exists on disk
    PcaAnchor(
        sample_id="HG002",
        super_pop="EUR",
        source="GIAB",
        notes="GIAB AJ trio child; PC4–PC5 should reflect AJ sub-cluster",
        input_path="/data/ancestry_app/reference/giab_asj/HG002.vcf.gz",
        expected_pcs=[],  # TODO: pin after first verified projection
        enabled=False,
    ),
    # 1000G Yoruba (AFR anchor) — placeholder
    PcaAnchor(sample_id="NA19238", super_pop="AFR", source="1000G",
              notes="Yoruba in Ibadan; canonical AFR anchor",
              input_path=None, enabled=False),
    # 1000G Han Chinese (EAS anchor) — placeholder
    PcaAnchor(sample_id="NA18525", super_pop="EAS", source="1000G",
              notes="Han Chinese in Beijing; canonical EAS anchor",
              input_path=None, enabled=False),
    # 1000G British (EUR anchor) — used by existing tests in spirit
    PcaAnchor(sample_id="HG00096", super_pop="EUR", source="1000G",
              notes="British in England and Scotland",
              input_path=None, enabled=False),
    # 1000G Punjabi (SAS anchor) — placeholder
    PcaAnchor(sample_id="HG03642", super_pop="SAS", source="1000G",
              notes="Punjabi from Lahore, Pakistan",
              input_path=None, enabled=False),
    # 1000G Mexican Ancestry (AMR anchor) — placeholder
    PcaAnchor(sample_id="NA19649", super_pop="AMR", source="1000G",
              notes="Mexican Ancestry in LA",
              input_path=None, enabled=False),
    # GIAB Caucasian (NA12878 / HG001) — placeholder
    PcaAnchor(sample_id="NA12878", super_pop="EUR", source="GIAB",
              notes="GIAB CEPH Utah; gold-standard EUR",
              input_path=None, enabled=False),
]


def enabled_anchors() -> list[PcaAnchor]:
    return [a for a in ANCHORS if a.enabled and a.expected_pcs]


def pin_anchor(sample_id: str, pcs: list[float], tol: float = 0.01) -> None:
    """Set expected_pcs and enable an anchor after first verified projection.

    Intended to be called once from a one-shot verification script after
    a human has inspected the projection result.
    """
    for a in ANCHORS:
        if a.sample_id == sample_id:
            a.expected_pcs = list(pcs[:4])
            a.pc_tolerance = tol
            a.enabled = True
            return
    raise KeyError(f"unknown anchor: {sample_id}")
