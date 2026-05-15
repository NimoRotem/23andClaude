#!/bin/bash
# Watchdog for simple-genomics: ensures the service is healthy.
# Runs via cron every 2 minutes. Hardened (2026-05-15) to avoid
# false-positive restarts under heavy load (bulk ref-stats rebuilds,
# concurrent scoring runs). The previous version used a single curl
# with --max-time 10 against /app, which restarted the service every
# 2 min during sustained load — causing intermittent 502s for users.
#
# Current rules:
#   1. Service must be RUNNING in supervisor.
#   2. The lightweight /api/init endpoint must respond 200 within 30s.
#   3. If a single check fails, retry once after 15s before restarting.
#      Two consecutive failures => the service is genuinely sick.

LOG="/var/log/supervisor/simple-genomics-watchdog.log"
PORT=8800
SERVICE="simple-genomics"
HEALTH_URL="http://127.0.0.1:$PORT/sign-in"
HEALTH_TIMEOUT=30
GRACE_PERIOD=15   # seconds between first failure and re-check

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

# HTTP health check (require 200 within HEALTH_TIMEOUT)
check_health() {
    curl -s -o /dev/null -w '%{http_code}' --max-time "$HEALTH_TIMEOUT" "$HEALTH_URL" 2>/dev/null
}

HTTP_CODE=$(check_health)

if [ "$HTTP_CODE" != "200" ]; then
    log "First health check failed (HTTP $HTTP_CODE). Waiting ${GRACE_PERIOD}s before retrying."
    sleep "$GRACE_PERIOD"
    HTTP_CODE=$(check_health)
    if [ "$HTTP_CODE" != "200" ]; then
        log "Second health check also failed (HTTP $HTTP_CODE). Service is genuinely sick — restarting."
        supervisorctl stop "$SERVICE" 2>/dev/null
        sleep 2
        fuser -k "$PORT/tcp" 2>/dev/null
        sleep 1
        supervisorctl start "$SERVICE" 2>/dev/null
        log "Restart complete."
    else
        log "Second health check recovered (HTTP 200) — no restart needed."
    fi
fi

# All good — no log entry on the happy path keeps the log small.
