#!/usr/bin/env python3
"""Prewarm the plink2 pgen cache for a VCF/gVCF in both chr-naming variants.

Invoked by bam-converter as a final pipeline step after a gVCF is written,
so a user's first PGS or PCA test on the new profile runs against a warm
cache instead of paying the multi-minute VCF→pgen conversion.

Builds two cache variants — the same two runners.py builds on demand:
  * PGS-style:  var_id_template="chr@:#",      output_chr=None
  * PCA-style:  var_id_template="@:#:$r:$a",   output_chr="26"
"""
import argparse
import logging
import os
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] prewarm: %(message)s",
    datefmt="%H:%M:%S",
)

# Make simple-genomics' runners.py importable.
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from runners import _get_or_build_pgen  # noqa: E402


VARIANTS = [
    ("PGS-style", {"var_id_template": "chr@:#", "output_chr": None}),
    ("PCA-style", {"var_id_template": "@:#:$r:$a", "output_chr": "26"}),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("vcf", help="VCF or gVCF path to prewarm")
    args = ap.parse_args()

    if not os.path.exists(args.vcf):
        logging.error("vcf not found: %s", args.vcf)
        sys.exit(2)

    failures = 0
    for label, kwargs in VARIANTS:
        logging.info("building %s cache for %s", label, args.vcf)
        t0 = time.time()
        try:
            prefix = _get_or_build_pgen(args.vcf, **kwargs)
            logging.info("  %s done in %.1fs -> %s", label, time.time() - t0, prefix)
        except Exception as e:
            failures += 1
            logging.error("  %s failed: %s", label, e)

    # Non-zero exit only if BOTH variants failed (caller treats prewarm as
    # best-effort; one missing variant just means that test pays normal
    # build cost the first time it runs).
    sys.exit(2 if failures == len(VARIANTS) else 0)


if __name__ == "__main__":
    main()
