#!/usr/bin/env bash
# Starts the FastAPI backend, which also serves the built React dashboard.
#   Open http://<MASTER_IP>:8000
. "$(dirname "$0")/common.sh"
cd "$PROJECT_DIR"
echo "Dashboard: http://$MASTER_IP:8000    API docs: http://$MASTER_IP:8000/docs"
exec "$BIG/venv/bin/uvicorn" backend.app:app --host 0.0.0.0 --port 8000
