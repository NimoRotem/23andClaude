#!/bin/bash
# ── PRS-CSx Scoring Pipeline ────────────────────────────────
# Usage: ./run_prscsx.sh <trait_name>
#
# Runs PRS-CSx for a given trait using EUR + EAS GWAS summary stats,
# then computes per-sample scores with plink2.
#
# Prerequisites:
#   - PRS-CSx installed at ./tools/PRScsx/
#   - LD reference panels downloaded
#   - GWAS sumstats at /data/gwas_sumstats/{trait}/{POP}.txt.gz
#   - Target samples BIM at /data/ancestry/all_samples.bim

set -euo pipefail

TRAIT=${1:?Usage: $0 <trait_name>}

# Paths
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PRSCSX="${SCRIPT_DIR}/../tools/PRScsx/PRScsx.py"
REF_DIR="${SCRIPT_DIR}/../tools/PRScsx"
SUMSTATS_DIR=/data/gwas_sumstats
OUT_DIR=/data/prs_csx_output/${TRAIT}
BIM_PREFIX=/data/ancestry/all_samples
DATA_DIR=/data

echo "=================================================="
echo "  PRS-CSx Pipeline: ${TRAIT}"
echo "=================================================="

# Check prerequisites
if [ ! -f "${PRSCSX}" ]; then
    echo "ERROR: PRS-CSx not found at ${PRSCSX}"
    echo "Install: cd $(pwd)/tools && git clone https://github.com/getian107/PRScsx.git"
    exit 1
fi

if [ ! -f "${BIM_PREFIX}.bim" ]; then
    echo "ERROR: Target BIM file not found at ${BIM_PREFIX}.bim"
    exit 1
fi

# Detect available populations
POPS=""
SST_FILES=""
N_GWAS=""
for POP in EUR EAS AFR SAS AMR; do
    if [ -f "${SUMSTATS_DIR}/${TRAIT}/${POP}.txt.gz" ] || [ -f "${SUMSTATS_DIR}/${TRAIT}/${POP}.txt" ]; then
        if [ -n "${POPS}" ]; then
            POPS="${POPS},${POP}"
            SST_FILES="${SST_FILES},${SUMSTATS_DIR}/${TRAIT}/${POP}.txt.gz"
            # Default sample sizes — should be overridden per-trait
            N_GWAS="${N_GWAS},200000"
        else
            POPS="${POP}"
            SST_FILES="${SUMSTATS_DIR}/${TRAIT}/${POP}.txt.gz"
            N_GWAS="400000"
        fi
    fi
done

if [ -z "${POPS}" ]; then
    echo "ERROR: No GWAS summary stats found for ${TRAIT}"
    echo "Expected at: ${SUMSTATS_DIR}/${TRAIT}/{EUR,EAS,...}.txt.gz"
    exit 1
fi

echo "Populations: ${POPS}"
echo "Summary stats: ${SST_FILES}"

mkdir -p ${OUT_DIR}

# Check if N_GWAS override file exists
if [ -f "${SUMSTATS_DIR}/${TRAIT}/n_gwas.txt" ]; then
    N_GWAS=$(cat "${SUMSTATS_DIR}/${TRAIT}/n_gwas.txt")
    echo "Using custom N_GWAS: ${N_GWAS}"
fi

# ── Step 1: Run PRS-CSx ──────────────────────────────────────

echo ""
echo "--- Step 1: Running PRS-CSx ---"

python ${PRSCSX} \
  --ref_dir=${REF_DIR} \
  --bim_prefix=${BIM_PREFIX} \
  --sst_file=${SST_FILES} \
  --n_gwas=${N_GWAS} \
  --pop=${POPS} \
  --out_dir=${OUT_DIR} \
  --out_name=${TRAIT} \
  --chrom=1-22 \
  --phi=1e-2

echo "PRS-CSx complete."

# ── Step 2: Concatenate per-chromosome weights ────────────────

echo ""
echo "--- Step 2: Concatenating weights ---"

IFS=',' read -ra POP_ARRAY <<< "${POPS}"
for POP in "${POP_ARRAY[@]}"; do
    WEIGHTS_FILE="${OUT_DIR}/${TRAIT}_${POP}_weights.txt"
    cat ${OUT_DIR}/${TRAIT}_${POP}_pst_eff_a1_b0.5_phi1e-02_chr*.txt > ${WEIGHTS_FILE}
    N_VARIANTS=$(wc -l < ${WEIGHTS_FILE})
    echo "  ${POP}: ${N_VARIANTS} variants"
done

# ── Step 3: Score with plink2 ─────────────────────────────────

echo ""
echo "--- Step 3: Scoring with plink2 ---"

PFILE=/data/ancestry/all_samples
if [ ! -f "${PFILE}.pgen" ]; then
    echo "  Using BED format..."
    PFILE_FLAG="--bfile ${BIM_PREFIX}"
else
    PFILE_FLAG="--pfile ${PFILE}"
fi

for POP in "${POP_ARRAY[@]}"; do
    WEIGHTS_FILE="${OUT_DIR}/${TRAIT}_${POP}_weights.txt"
    echo "  Scoring with ${POP} weights..."

    plink2 \
        ${PFILE_FLAG} \
        --score ${WEIGHTS_FILE} 2 4 6 cols=+scoresums \
        --out ${OUT_DIR}/${TRAIT}_${POP}_scores

    echo "  ${POP} scoring complete: ${OUT_DIR}/${TRAIT}_${POP}_scores.sscore"
done

# ── Step 4: Combine scores using ancestry proportions ─────────

echo ""
echo "--- Step 4: Combining ancestry-weighted scores ---"

python -c "
import sys
import os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.combine_ancestry_scores import combine_for_trait
combine_for_trait('${TRAIT}', '${OUT_DIR}', '${POPS}'.split(','))
"

echo ""
echo "=================================================="
echo "  ${TRAIT} scoring complete!"
echo "  Results at: ${OUT_DIR}/"
echo "=================================================="
