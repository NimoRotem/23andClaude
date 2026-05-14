#!/bin/bash
# Watchdog for simple-genomics: ensures the service is healthy.
# Runs via cron every 2 minutes. Checks:
#   1. Is supervisor process RUNNING?
#   2. Does the app respond to HTTP within 10s?
# If not, kills stale port holders and restarts.

LOG="/var/log/supervisor/simple-genomics-watchdog.log"
PORT=8800
SERVICE="simple-genomics"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [watchdog] $1" >> "$LOG"; }

# Check supervisor status
STATUS=$(supervisorctl status "$SERVICE" 2>/dev/null | awk '{print $2}')

if [ "$STATUS" != "RUNNING" ]; then
    log "Service not RUNNING (status=$STATUS). Killing stale port holders and restarting."
    fuser -k "$PORT/tcp" 2>/dev/null
    sleep 2
    supervisorctl start "$SERVICE" 2>/dev/null
    log "Restart issued."
    exit 0
fi

# Check HTTP health (GET /app should return 200 within 10s)
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "http://127.0.0.1:$PORT/app" 2>/dev/null)

if [ "$HTTP_CODE" != "200" ]; then
    log "Health check failed (HTTP $HTTP_CODE). Restarting service."
    supervisorctl stop "$SERVICE" 2>/dev/null
    sleep 2
    # Kill anything still holding the port
    fuser -k "$PORT/tcp" 2>/dev/null
    sleep 1
    supervisorctl start "$SERVICE" 2>/dev/null
    log "Restart complete."
    exit 0
fi

# All good — no output unless there's a problem
