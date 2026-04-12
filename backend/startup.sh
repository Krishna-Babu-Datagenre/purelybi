#!/usr/bin/env bash
# Azure App Service runs dependencies from Oryx's antenv; application code lives under src/
# without a pip install of the project. Put src on PYTHONPATH so `fastapi_app` and `ai` import.
set -euo pipefail
cd /home/site/wwwroot
export PYTHONPATH="/home/site/wwwroot/src:${PYTHONPATH:-}"
# App Service usually sets PORT for the process to bind to.
PORT="${PORT:-${WEBSITES_PORT:-8000}}"
exec gunicorn \
  --bind "0.0.0.0:${PORT}" \
  --timeout 600 \
  -k uvicorn.workers.UvicornWorker \
  fastapi_app.app:app
