#!/usr/bin/env python3
"""Build a reference-panel stats JSON for a PGS by scoring the 1000G panel.

Usage:
    python build_stats.py PGS000662

Produces: /data/pgs2/ref_panel_stats/{pgs_id}_EUR_GRCh38.json

Approach
--------
1. Read the cached harmonized scoring file (GRCh38) from /data/pgs_cache.
2. Emit a plink2 --score file using 1000G ref-panel variant IDs
   (`chrom:pos:ref:alt`, bare chromosomes). For each variant we emit both
   allele orientations — only the one matching the pvar will actually hit.
3. Run plink2 --score against the 1000G pgen to produce per-sample SUMs.
4. Join with the psam on IID, subset to SuperPop == EUR, compute
   mean/std/median/n/min/max and write the JSON.
"""
import gzip
import json
import os
import statistics
import subprocess
import sys
import tempfile

PGS_CACHE = "/data/pgs_cache"
REF_PFILE = "/data/pgs2/ref_panel/GRCh38_1000G_ALL"
REF_PSAM = REF_PFILE + ".psam"
STATS_DIR = "/data/pgs2/ref_panel_stats"
PLINK2 = "/home/nimrod_rotem/conda/envs/pgs2/bin/plink2"


def load_scoring(pgs_id):
    path = os.path.join(PGS_CACHE, pgs_id, f"{pgs_id}_hmPOS_GRCh38.txt.gz")
    if not os.path.exists(path):
        raise FileNotFoundError(f"cached scoring file not found: {path}")

    cols = None
    rows = []
    with gzip.open(path, "rt") as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if cols is None:
                cols = parts
                continue

            def col(name):
                return parts[cols.index(name)] if name in cols else ""

            chrom = col("hm_chr") or col("chr_name")
            pos = col("hm_pos") or col("chr_position")
            ea = col("effect_allele")
            oa = col("other_allele") or col("hm_inferOtherAllele")
            w = col("effect_weight")

            if not chrom or not pos or chrom == "NA" or pos == "NA":
                continue
            if not ea or not w:
                continue
            # Strip any leading "chr"
            if chrom.startswith("chr"):
                chrom = chrom[3:]
            try:
                float(w)
            except ValueError:
                continue

            rows.append((chrom, pos, ea, oa, w))
    return rows


def write_plink_score(rows, out_path):
    """Write a plink2 --score file using 1000G-panel IDs (bare chrom:pos:ref:alt).

    For each variant we emit up to two candidate IDs (one per allele
    orientation); only the orientation present in the pvar will match.
    """
    with open(out_path, "w") as f:
        f.write("ID\tA1\tWEIGHT\n")
        for chrom, pos, ea, oa, w in rows:
            if oa:
                f.write(f"{chrom}:{pos}:{oa}:{ea}\t{ea}\t{w}\n")
                f.write(f"{chrom}:{pos}:{ea}:{oa}\t{ea}\t{w}\n")
            else:
                # No other-allele info — just the "effect is ALT" guess.
                f.write(f"{chrom}:{pos}:N:{ea}\t{ea}\t{w}\n")


def run(cmd):
    print("+", " ".join(cmd), flush=True)
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(r.stdout)
        sys.stderr.write(r.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")
    return r


def score_ref_panel(plink_score, tmpdir):
    out_prefix = os.path.join(tmpdir, "ref_score")
    cmd = [
        PLINK2,
        "--pfile", REF_PFILE, "vzs",
        "--score", plink_score, "header-read", "1", "2", "3",
        "cols=+scoresums",
        "no-mean-imputation",
        "list-variants",
        "--threads", "8",
        "--memory", "16000",
        "--out", out_prefix,
    ]
    run(cmd)
    return out_prefix + ".sscore", out_prefix + ".sscore.vars"


def load_superpop_map():
    """Map IID -> SuperPop from the 1000G psam."""
    m = {}
    with open(REF_PSAM) as f:
        header = f.readline().lstrip("#").strip().split("\t")
        iid_i = header.index("IID")
        sp_i = header.index("SuperPop")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            m[parts[iid_i]] = parts[sp_i]
    return m


def parse_sscore(sscore_path, superpop, want="EUR"):
    with open(sscore_path) as f:
        header = f.readline().lstrip("#").strip().split("\t")
        print(f"sscore header: {header}")
        iid_i = header.index("IID")

        def find(keys):
            for k in keys:
                if k in header:
                    return header.index(k)
            return None

        sum_i = find(["SCORE1_SUM", "WEIGHT_SUM", "SCORE_SUM"])
        avg_i = find(["SCORE1_AVG", "WEIGHT_AVG", "SCORE_AVG"])
        if sum_i is None and avg_i is None:
            raise RuntimeError(f"sscore has no score columns: {header}")

        sums = []
        avgs = []
        for line in f:
            parts = line.rstrip("\n").split("\t")
            iid = parts[iid_i]
            if superpop.get(iid) != want:
                continue
            try:
                if sum_i is not None:
                    sums.append(float(parts[sum_i]))
                if avg_i is not None:
                    avgs.append(float(parts[avg_i]))
            except ValueError:
                continue
    return sums, avgs


def summarise(values):
    return {
        "mean": statistics.mean(values),
        "std": statistics.stdev(values) if len(values) > 1 else 0.0,
        "median": statistics.median(values),
        "n_samples": len(values),
        "min": min(values),
        "max": max(values),
    }


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: build_stats.py PGSxxxxxxx")
    pgs_id = sys.argv[1]

    rows = load_scoring(pgs_id)
    print(f"parsed {len(rows)} variants from {pgs_id} scoring file")

    with tempfile.TemporaryDirectory(prefix=f"refstats_{pgs_id}_") as tmpdir:
        plink_score = os.path.join(tmpdir, f"{pgs_id}.plink2.tsv")
        write_plink_score(rows, plink_score)

        sscore_path, vars_path = score_ref_panel(plink_score, tmpdir)

        matched = 0
        if os.path.exists(vars_path):
            with open(vars_path) as f:
                matched = sum(1 for _ in f)
        print(f"plink2 matched {matched} variants against 1000G")

        superpop = load_superpop_map()
        sums, avgs = parse_sscore(sscore_path, superpop, "EUR")
        n_scored = len(avgs) if avgs else len(sums)
        if n_scored == 0:
            raise SystemExit("no EUR samples scored — aborting")
        print(f"scored {n_scored} EUR samples")

        # runners.py reads SCORE1_AVG into raw_score, so mean/std must be
        # computed over AVGs to make z-scores comparable.
        s = summarise(avgs) if avgs else summarise(sums)
        s_sum = summarise(sums) if sums else s
        out = {
            "pgs_id": pgs_id,
            "population": "EUR",
            "genome_build": "GRCh38",
            "mean": s["mean"],
            "std": s["std"],
            "median": s["median"],
            "n_samples": s["n_samples"],
            "min": s["min"],
            "max": s["max"],
            "matched_variants": matched,
            "total_variants": len(rows),
            "score_sum_mean": s_sum["mean"],
            "score_sum_std": s_sum["std"],
        }

        out_path = os.path.join(STATS_DIR, f"{pgs_id}_EUR_GRCh38.json")
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2)
        print(f"wrote {out_path}")
        print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
