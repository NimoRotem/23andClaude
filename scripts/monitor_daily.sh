#!/bin/bash
# W4.5 — daily availability & drift monitoring.
# Runs the availability matrix, diffs against yesterday's snapshot,
# writes an alert file if any (pop x reason_code) counts changed
# significantly. Designed to be cron-runnable.

set -euo pipefail

cd /home/nimrod_rotem/simple-genomics
mkdir -p logs/monitor

TODAY=$(date -u +%Y%m%d)
YESTERDAY=$(date -u -d "yesterday" +%Y%m%d 2>/dev/null || date -u -v-1d +%Y%m%d)
TODAY_FILE="logs/monitor/avail_${TODAY}.jsonl"
YESTERDAY_FILE="logs/monitor/avail_${YESTERDAY}.jsonl"
ALERT_FILE="logs/monitor/alerts_${TODAY}.txt"

# 1. Capture today's matrix
python3 scripts/availability_matrix.py \
    --pops EUR,EAS,AFR,SAS,AMR,MIX --build GRCh38 \
    --json > "${TODAY_FILE}.tmp"
mv "${TODAY_FILE}.tmp" "$TODAY_FILE"

# 2. Summary counts
python3 - <<PYEND > "logs/monitor/summary_${TODAY}.json"
import json, collections
rows = [json.loads(l) for l in open("$TODAY_FILE")]
by_pop = collections.defaultdict(lambda: collections.Counter())
by_reason = collections.Counter()
for r in rows:
    pop = r["population"]
    by_pop[pop]["total"] += 1
    if r["percentile_allowed"]:
        by_pop[pop]["allowed"] += 1
    else:
        by_pop[pop]["blocked"] += 1
        by_reason[r.get("block_reason") or "UNKNOWN"] += 1
print(json.dumps({"by_pop": {p: dict(c) for p, c in by_pop.items()},
                   "by_reason": dict(by_reason)}, indent=2))
PYEND

# 3. Diff vs yesterday if available
if [ -f "$YESTERDAY_FILE" ]; then
  python3 - "$YESTERDAY_FILE" "$TODAY_FILE" > "logs/monitor/diff_${TODAY}.json" <<'PYEND'
import json, sys, collections
ya, tx = sys.argv[1], sys.argv[2]
def load(f):
    out = {}
    for line in open(f):
        r = json.loads(line)
        out[(r["pgs_id"], r["population"], r["build"])] = r
    return out
y = load(ya); t = load(tx)
added = set(t) - set(y)
removed = set(y) - set(t)
flipped = []  # blocked → allowed or vice versa
for k in set(t) & set(y):
    if t[k]["percentile_allowed"] != y[k]["percentile_allowed"]:
        flipped.append((k, y[k]["percentile_allowed"], t[k]["percentile_allowed"]))
print(json.dumps({
    "added": [list(k) for k in sorted(added)][:20],
    "removed": [list(k) for k in sorted(removed)][:20],
    "flipped_blocked_to_allowed": [list(k[0])+[True] for k in flipped if k[2]][:20],
    "flipped_allowed_to_blocked": [list(k[0])+[False] for k in flipped if not k[2]][:20],
    "n_added": len(added), "n_removed": len(removed),
    "n_flipped_to_allowed": sum(1 for k in flipped if k[2]),
    "n_flipped_to_blocked": sum(1 for k in flipped if not k[2]),
}, indent=2))
PYEND

  # 4. Alert if regressions
  REGRESSIONS=$(python3 -c "
import json
d = json.load(open('logs/monitor/diff_${TODAY}.json'))
print(d.get('n_flipped_to_blocked', 0))
")
  if [ "$REGRESSIONS" -gt 0 ]; then
    {
      echo "ALERT: $REGRESSIONS (PGS x pop) entries regressed (allowed→blocked)"
      echo "Date: $(date -u)"
      echo "See logs/monitor/diff_${TODAY}.json for details"
    } > "$ALERT_FILE"
  fi
fi

# 5. Prune old monitor files (keep 30 days)
find logs/monitor/ -type f -mtime +30 -delete 2>/dev/null || true

echo "monitor_daily done: $TODAY_FILE"
