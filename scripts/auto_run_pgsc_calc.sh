#!/bin/bash
# Wait for pgsc_1000G_v1 download to complete, then unpack + run pgsc_calc
# on HG002 against several PGSes and compare against our pipeline.

set -euo pipefail
exec > /home/nimrod_rotem/simple-genomics/logs/auto_pgsc_calc.log 2>&1

cd /data/pgsc_refs
EXPECTED_1000G=7434464202

# 1. Wait for download
while true; do
  size=$(stat -c%s pgsc_1000G_v1.tar.zst 2>/dev/null || echo 0)
  if [ "$size" -ge "$EXPECTED_1000G" ]; then
    echo "[$(date -u +%H:%M:%S)] pgsc_1000G_v1.tar.zst download complete ($size bytes)"
    break
  fi
  pct=$((size * 100 / EXPECTED_1000G))
  echo "[$(date -u +%H:%M:%S)] pgsc_1000G_v1 download $pct% ($size/$EXPECTED_1000G)"
  sleep 120
done

# 2. Unpack if not already
if [ ! -d /data/pgsc_refs/pgsc_1000G_v1 ]; then
  echo "[$(date -u +%H:%M:%S)] unpacking pgsc_1000G_v1..."
  zstd -d --stdout pgsc_1000G_v1.tar.zst | tar xf -
  echo "[$(date -u +%H:%M:%S)] unpack done"
fi
ls /data/pgsc_refs/pgsc_1000G_v1/ | head -10

# 3. Build sample sheet (already done in /home/nimrod_rotem/pgsc_runs/HG002)
cd /home/nimrod_rotem/pgsc_runs/HG002
cat samplesheet.csv

# 4. Run pgsc_calc on HG002 against a handful of PGSes
NEXTFLOW=/home/nimrod_rotem/tools/nextflow
mkdir -p pgsc_runs_out
cd pgsc_runs_out

# Detect a reference panel file inside the unpacked dir
REF_PANEL_PATH=$(find /data/pgsc_refs/pgsc_1000G_v1 -maxdepth 2 -name "reference.tar.zst" 2>/dev/null | head -1)
if [ -z "$REF_PANEL_PATH" ]; then
  # pgsc_calc accepts the original .tar.zst directly
  REF_PANEL_PATH=/data/pgsc_refs/pgsc_1000G_v1.tar.zst
fi
echo "ref panel: $REF_PANEL_PATH"

echo "[$(date -u +%H:%M:%S)] launching pgsc_calc on HG002 + PGS000004,PGS000007"
$NEXTFLOW run pgscatalog/pgsc_calc -profile docker \
    --input /home/nimrod_rotem/pgsc_runs/HG002/samplesheet.csv \
    --target_build GRCh38 \
    --pgs_id PGS000004,PGS000007 \
    --run_ancestry "$REF_PANEL_PATH" \
    --outdir /home/nimrod_rotem/pgsc_runs/HG002/results 2>&1 | tail -200
echo "[$(date -u +%H:%M:%S)] pgsc_calc finished"
ls /home/nimrod_rotem/pgsc_runs/HG002/results/ | head -10
