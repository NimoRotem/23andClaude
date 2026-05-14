#!/bin/bash
# Wait for pgsc_HGDP+1kGP download to complete, then unpack and prep
# pipeline/config.py for the alternative panel path. Does NOT trigger the
# full ref-stats rebuild — that's a multi-hour operator decision.

set -euo pipefail
exec > /home/nimrod_rotem/simple-genomics/logs/auto_setup_hgdp.log 2>&1

EXPECTED=15953388718
PANEL=/data/pgsc_refs/pgsc_HGDP+1kGP_v1.tar.zst

# 1. Wait for download
while true; do
  size=$(stat -c%s "$PANEL" 2>/dev/null || echo 0)
  if [ "$size" -ge "$EXPECTED" ]; then
    echo "[$(date -u +%H:%M:%S)] HGDP+1kGP download complete ($size bytes)"
    break
  fi
  pct=$((size * 100 / EXPECTED))
  echo "[$(date -u +%H:%M:%S)] HGDP+1kGP download $pct% ($size/$EXPECTED)"
  sleep 300
done

# 2. Unpack
mkdir -p /data/pgsc_refs/pgsc_HGDP+1kGP_v1
cd /data/pgsc_refs
if [ -z "$(ls pgsc_HGDP+1kGP_v1/ 2>/dev/null | head -1)" ]; then
  echo "[$(date -u +%H:%M:%S)] unpacking…"
  zstd -d --stdout pgsc_HGDP+1kGP_v1.tar.zst | tar xf -C pgsc_HGDP+1kGP_v1
  echo "[$(date -u +%H:%M:%S)] unpack done"
fi
ls /data/pgsc_refs/pgsc_HGDP+1kGP_v1/ 2>&1 | head -20

# 3. Inventory the panel contents so we know what's there
echo
echo "=== panel inventory ==="
find /data/pgsc_refs/pgsc_HGDP+1kGP_v1 -maxdepth 3 -name "*.pgen" -o -name "*.pvar*" -o -name "*.psam" 2>&1 | head -30
echo
echo "=== sample count ==="
find /data/pgsc_refs/pgsc_HGDP+1kGP_v1 -name "*.psam" 2>&1 | head -5
PSAM=$(find /data/pgsc_refs/pgsc_HGDP+1kGP_v1 -name "*.psam" 2>&1 | head -1)
if [ -n "$PSAM" ] && [ -f "$PSAM" ]; then
  echo "first PSAM:"
  head -3 "$PSAM"
  echo "n samples:"
  wc -l "$PSAM"
fi

echo
echo "[$(date -u +%H:%M:%S)] HGDP+1kGP staged. Full ref-stats rebuild is a"
echo "separate ~12h operation triggered manually:"
echo "  HGDP_PANEL=/data/pgsc_refs/pgsc_HGDP+1kGP_v1/<panel-prefix> \\"
echo "    python3 scripts/rebuild_from_matrix.py --force"
