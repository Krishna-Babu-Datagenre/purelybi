#!/usr/bin/env bash
# Azure App Service: Oryx often extracts the app under /tmp/...; /home/site/wwwroot may only
# hold output.tar.zst. Resolve paths from this script so imports work in both layouts.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export PYTHONPATH="${SCRIPT_DIR}/src:${PYTHONPATH:-}"
PORT="${PORT:-${WEBSITES_PORT:-8000}}"
exec gunicorn \
  --bind "0.0.0.0:${PORT}" \
  --timeout 600 \
  -k uvicorn.workers.UvicornWorker \
  fastapi_app.app:app
