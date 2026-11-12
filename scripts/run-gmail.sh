#!/usr/bin/env bash
# Classifies emails from a real Gmail inbox (read-only).  Needs gmail.env (see gmail.env.example).
#   scripts/run-gmail.sh                 # latest 10 new emails
#   scripts/run-gmail.sh --limit 25
#   scripts/run-gmail.sh --watch 30      # keep checking for new mail every 30 s
. "$(dirname "$0")/common.sh"
cd "$PROJECT_DIR"
exec "$BIG/venv/bin/python" -W ignore producer/gmail_fetcher.py --kafka "$KAFKA_BOOTSTRAP" "$@"
