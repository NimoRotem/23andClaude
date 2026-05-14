"""Known PGS portability issues — PGS that are documented to transfer
poorly across ancestries, or where this codebase has empirically observed
systematic bias.

Surfaced in the live overlay BEFORE cron_cohort_sanity has enough samples
to flag a PGS itself. cron_cohort_sanity needs n>=4 and is flat-text-log
based; this list catches known-bad scores at first read.
"""
from __future__ import annotations

# Hardcoded list — extend when new portability issues are confirmed.
# Source notes are in the value so the warning text can cite them.
KNOWN_LOW_PORTABILITY_PGS = {
    "PGS001229": (
        "Standing height (Tanigawa Y et al. PLoS Genet 2022). Trained on "
        "UK Biobank white-British. Empirically biased >5x for non-British "
        "EUR and most non-EUR ancestries (cohort_sanity KS p<0.001)."
    ),
    "PGS000327": (
        "Autism spectrum (Grove J et al.). Trained primarily on EUR; "
        "transfers poorly to non-EUR (cohort_sanity-flagged)."
    ),
    "PGS000334": (
        "Alzheimer's disease, n=22 variants. APOE-dominated; non-EUR LD "
        "patterns can shift the percentile substantially even when the "
        "APOE genotype itself is correctly called."
    ),
}


def portability_warning(pgs_id: str) -> str:
    """Return a portability warning string for known-bad PGSes, else empty."""
    note = KNOWN_LOW_PORTABILITY_PGS.get(pgs_id)
    if not note:
        return ""
    return (
        f"{pgs_id}: known low cross-ancestry portability. {note} "
        "Treat the percentile as indicative, not diagnostic."
    )


def is_known_low_portability(pgs_id: str) -> bool:
    return pgs_id in KNOWN_LOW_PORTABILITY_PGS
