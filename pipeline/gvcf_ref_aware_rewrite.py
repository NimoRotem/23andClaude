"""Phase 1.3 — REF-aware gVCF placeholder ALT rewriting.

Replaces the legacy `_rewrite_gvcf_placeholder_alts` in `runners.py`
with strict per-site logic per REMEDIATION_PLAN §1.3:

  ref_base = fasta.fetch(chrom, pos)   # 1-based, single base
  if effect_allele == ref_base:
      synthetic_alt = other_allele
      score_allele = "REF"             # dosage flips
  elif other_allele == ref_base:
      synthetic_alt = effect_allele
      score_allele = "ALT"
  else:
      exclude(site, reason="effect_other_ref_mismatch")

Threshold for hom-ref synthetic emission to count as matched:
DP ≥ 10, GQ ≥ 20. Sites below threshold emit '<*>'/'<NON_REF>' rather
than synthetic — they become missing, NOT hom-ref.

Per-run audit `gvcf_rewrite_audit.tsv` lists every decision with
(chrom, pos, ref, effect, other, decision, dp, gq).
"""
from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


log = logging.getLogger("pgs-pipeline.gvcf_ref_aware_rewrite")


PLACEHOLDERS = ("<*>", "<NON_REF>")


@dataclass
class RewriteStats:
    n_total: int = 0
    n_rewrite_ref_eq_effect: int = 0   # ALT becomes other_allele; dosage flips
    n_rewrite_ref_eq_other: int = 0    # ALT becomes effect_allele
    n_excluded_mismatch: int = 0       # neither effect_allele nor other_allele == REF
    n_below_threshold: int = 0         # DP/GQ too low for confident hom-ref
    n_missing_in_allele_map: int = 0   # site not in scoring file
    n_unknown_ref_base: int = 0        # FASTA returned N or non-ACGT
    n_kept_variant: int = 0            # actual variant record (not a placeholder)
    audit_path: str = ""


@dataclass
class _AlleleMapEntry:
    effect_allele: str
    other_allele: str
    pgs_id: str = ""


class FastaRef:
    """Thin wrapper over pysam.FastaFile with a memoized fetch."""

    def __init__(self, fasta_path: str):
        try:
            import pysam
        except ImportError as e:
            raise RuntimeError(
                "pysam required for REF-aware rewrite; "
                "install with `pip install pysam`"
            ) from e
        self._fa = pysam.FastaFile(fasta_path)
        self.path = fasta_path

    def fetch_ref(self, chrom: str, pos1: int) -> Optional[str]:
        """Return the FASTA base at 1-based `pos1` on `chrom`. Tries both
        chr-prefixed and bare-chrom names. Returns None on lookup failure
        or non-ACGT."""
        candidates = (chrom, chrom[3:] if chrom.startswith("chr") else f"chr{chrom}")
        for c in candidates:
            try:
                base = self._fa.fetch(c, pos1 - 1, pos1).upper()
            except (KeyError, ValueError):
                continue
            if len(base) == 1 and base in "ACGT":
                return base
        return None


def _gt_dp_gq(fmt: str, sample: str) -> tuple[str, Optional[int], Optional[int]]:
    """Extract GT, DP, GQ from a single VCF FORMAT/sample pair."""
    f_keys = fmt.split(":")
    s_vals = sample.split(":")
    g: dict[str, str] = {}
    for i, k in enumerate(f_keys):
        if i < len(s_vals):
            g[k] = s_vals[i]
    gt = g.get("GT", "")
    dp_s = g.get("DP", g.get("MIN_DP", ""))
    gq_s = g.get("GQ", g.get("MIN_GQ", ""))
    try:
        dp: Optional[int] = int(dp_s) if dp_s and dp_s != "." else None
    except ValueError:
        dp = None
    try:
        gq: Optional[int] = int(gq_s) if gq_s and gq_s != "." else None
    except ValueError:
        gq = None
    return gt, dp, gq


def rewrite_gvcf_ref_aware(
    in_vcf: str | Path,
    out_vcf: str | Path,
    *,
    fasta_path: str,
    allele_map: dict[tuple[str, int], _AlleleMapEntry],
    audit_tsv_path: str | Path,
    min_dp: int = 10,
    min_gq: int = 20,
    bcftools_path: str = "bcftools",
) -> RewriteStats:
    """Rewrite placeholder ALTs in a gVCF using per-site REF lookup.

    `allele_map` is `(chrom, pos_1based) → _AlleleMapEntry(effect_allele,
    other_allele, pgs_id)`. Positions absent from the map are dropped
    (their REF-block dosage isn't scoreable anyway).

    Emits the rewritten VCF at `out_vcf` and a `gvcf_rewrite_audit.tsv`
    at `audit_tsv_path` capturing every decision.

    Returns RewriteStats with per-decision counts.
    """
    fa = FastaRef(fasta_path)
    stats = RewriteStats(audit_path=str(audit_tsv_path))
    Path(audit_tsv_path).parent.mkdir(parents=True, exist_ok=True)

    reader = subprocess.Popen(
        [bcftools_path, "view", str(in_vcf)],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    writer = subprocess.Popen(
        [bcftools_path, "view", "-Oz", "-o", str(out_vcf), "-"],
        stdin=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )

    audit_f = open(audit_tsv_path, "w")
    audit_f.write("chrom\tpos\tref\teffect_allele\tother_allele\tdecision\tdp\tgq\tpgs_id\n")

    try:
        for line in reader.stdout:
            if line.startswith("#"):
                writer.stdin.write(line)
                continue
            stats.n_total += 1
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 10:
                continue
            chrom, pos_s, vid, ref, alt = parts[0], parts[1], parts[2], parts[3], parts[4]
            try:
                pos1 = int(pos_s)
            except ValueError:
                continue

            # Non-placeholder records: pass through after stripping any
            # trailing ',<*>'/',<NON_REF>' tokens.
            if not any(p in alt for p in PLACEHOLDERS):
                stats.n_kept_variant += 1
                writer.stdin.write(line)
                continue
            if alt not in PLACEHOLDERS:
                cleaned = ",".join(a for a in alt.split(",") if a not in PLACEHOLDERS)
                if cleaned and cleaned != alt:
                    parts[4] = cleaned
                    writer.stdin.write("\t".join(parts) + "\n")
                else:
                    writer.stdin.write(line)
                stats.n_kept_variant += 1
                continue

            # Now alt is purely a placeholder. Look up REF and the PGS row.
            map_entry = (
                allele_map.get((chrom, pos1))
                or (allele_map.get(("chr" + chrom, pos1))
                    if not chrom.startswith("chr") else None)
                or (allele_map.get((chrom.lstrip("chr") or chrom, pos1))
                    if chrom.startswith("chr") else None)
            )
            if map_entry is None:
                stats.n_missing_in_allele_map += 1
                audit_f.write(
                    f"{chrom}\t{pos1}\t{ref}\t-\t-\tdrop_not_in_scoring_file\t-\t-\t-\n"
                )
                continue

            ref_base = fa.fetch_ref(chrom, pos1)
            if ref_base is None:
                stats.n_unknown_ref_base += 1
                audit_f.write(
                    f"{chrom}\t{pos1}\t{ref}\t{map_entry.effect_allele}\t"
                    f"{map_entry.other_allele}\tdrop_fasta_ref_unknown\t-\t-\t"
                    f"{map_entry.pgs_id}\n"
                )
                continue

            effect = map_entry.effect_allele
            other = map_entry.other_allele
            if effect == ref_base:
                synthetic_alt = other
                decision = "rewrite_ref_eq_effect"
                stats.n_rewrite_ref_eq_effect += 1
            elif other == ref_base:
                synthetic_alt = effect
                decision = "rewrite_ref_eq_other"
                stats.n_rewrite_ref_eq_other += 1
            else:
                stats.n_excluded_mismatch += 1
                audit_f.write(
                    f"{chrom}\t{pos1}\t{ref_base}\t{effect}\t{other}\t"
                    f"effect_other_ref_mismatch\t-\t-\t{map_entry.pgs_id}\n"
                )
                continue

            # DP/GQ threshold for hom-ref synthetic emission
            fmt = parts[8] if len(parts) > 8 else ""
            sample = parts[9] if len(parts) > 9 else ""
            _gt, dp, gq = _gt_dp_gq(fmt, sample)
            dp_ok = (dp is not None) and (dp >= min_dp)
            gq_ok = (gq is not None) and (gq >= min_gq)
            if not (dp_ok and gq_ok):
                stats.n_below_threshold += 1
                audit_f.write(
                    f"{chrom}\t{pos1}\t{ref_base}\t{effect}\t{other}\t"
                    f"drop_below_dp_gq_threshold\t{dp if dp is not None else '-'}\t"
                    f"{gq if gq is not None else '-'}\t{map_entry.pgs_id}\n"
                )
                continue

            parts[3] = ref_base
            parts[4] = synthetic_alt
            writer.stdin.write("\t".join(parts) + "\n")
            audit_f.write(
                f"{chrom}\t{pos1}\t{ref_base}\t{effect}\t{other}\t"
                f"{decision}\t{dp}\t{gq}\t{map_entry.pgs_id}\n"
            )
    finally:
        try:
            writer.stdin.close()
        except Exception:
            pass
        reader.stdout.close()
        reader.wait()
        writer.wait()
        audit_f.close()
    return stats


def build_allele_map_from_scoring_files(
    scoring_files: list[str | Path],
) -> dict[tuple[str, int], _AlleleMapEntry]:
    """Construct the allele map by reading PGS Catalog harmonized scoring
    files. Uses the rstrip-newline-only parser (the strip() bug fix from
    pipeline/match_logic.py).
    """
    import gzip
    out: dict[tuple[str, int], _AlleleMapEntry] = {}
    for path in scoring_files:
        opener = gzip.open if str(path).endswith(".gz") else open
        pgs_id = ""
        col_names = None
        with opener(path, "rt") as f:
            for raw in f:
                if raw.startswith("#"):
                    if raw.startswith("#pgs_id="):
                        pgs_id = raw[len("#pgs_id="):].strip()
                    continue
                parts = raw.rstrip("\n").rstrip("\r").split("\t")
                if col_names is None:
                    col_names = parts
                    continue
                if len(parts) < len(col_names):
                    parts = parts + [""] * (len(col_names) - len(parts))
                ci = {n: i for i, n in enumerate(col_names)}
                try:
                    chrom = parts[ci["hm_chr"]] if "hm_chr" in ci else parts[ci["chr_name"]]
                    pos_s = parts[ci["hm_pos"]] if "hm_pos" in ci else parts[ci["chr_position"]]
                    ea = parts[ci["effect_allele"]]
                except (KeyError, IndexError):
                    continue
                if not chrom or not pos_s or chrom == "NA" or pos_s == "NA":
                    continue
                oa = ""
                for k in ("other_allele", "hm_inferOtherAllele"):
                    if k in ci and ci[k] < len(parts):
                        c = parts[ci[k]].strip()
                        if c and c != "NA":
                            oa = c
                            break
                try:
                    pos1 = int(pos_s)
                except ValueError:
                    continue
                chrom_norm = chrom if chrom.startswith("chr") else f"chr{chrom}"
                out[(chrom_norm, pos1)] = _AlleleMapEntry(
                    effect_allele=ea, other_allele=oa, pgs_id=pgs_id,
                )
    return out
