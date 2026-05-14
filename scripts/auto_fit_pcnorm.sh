#!/bin/bash
# Auto-fit PC-normalization coefficients as new .npy files appear.
# Runs in a loop with a 10-minute interval; exits when no new .npys arrive
# for 30 minutes (a heuristic that the bulk rebuild is finished or paused).

set -euo pipefail

cd /home/nimrod_rotem/simple-genomics
mkdir -p logs

LOG="logs/auto_fit_pcnorm.log"
NPY_DIR="/data/pgs2/ref_panel_stats/_scores"
PCNORM_DIR="/data/pgs2/ref_panel_stats/_pcnorm"

idle_ticks=0
while true; do
  # Count PGSes with .npy vs PC-norm coeffs
  n_npy=$(ls "$NPY_DIR" 2>/dev/null | wc -l)
  n_coef=$(ls "$PCNORM_DIR" 2>/dev/null | wc -l)
  delta=$((n_npy - n_coef))

  if [ "$delta" -gt 0 ]; then
    echo "[$(date -u +%H:%M:%S)] auto_fit_pcnorm: npy=$n_npy coef=$n_coef delta=$delta — fitting" | tee -a "$LOG"
    python3 scripts/fit_pc_normalization.py --all 2>&1 \
      | grep -E "loading|done|^\s+\[" \
      | tee -a "$LOG"
    idle_ticks=0
  else
    idle_ticks=$((idle_ticks + 1))
    echo "[$(date -u +%H:%M:%S)] auto_fit_pcnorm: nothing new (idle=$idle_ticks)" | tee -a "$LOG"
  fi

  # Exit after 3 idle ticks (30 minutes) if a rebuild process is no longer
  # running. This lets the loop wind itself down gracefully.
  if [ "$idle_ticks" -ge 3 ]; then
    if ! pgrep -f rebuild_from_matrix > /dev/null; then
      echo "[$(date -u +%H:%M:%S)] auto_fit_pcnorm: rebuild done, exiting" | tee -a "$LOG"
      break
    fi
  fi

  sleep 600
done
