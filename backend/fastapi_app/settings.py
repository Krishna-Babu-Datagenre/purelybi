"""
Central place for non-secret configuration read from the environment.

Secrets (keys, connection strings) must only be loaded via ``os.environ`` here
or in dedicated client modules — never hard-code credentials.

See ``backend/.env-example`` for variable names and documentation.
"""

from __future__ import annotations

import os


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v.strip() if v else default


# --- Azure / blob (data plane; section 5) ---
AZURE_STORAGE_ACCOUNT_URL: str = _env("AZURE_STORAGE_ACCOUNT_URL")
AZURE_STORAGE_CONTAINER: str = _env(
    "AZURE_STORAGE_CONTAINER",
    _env("BLOB_CONTAINER_NAME", "raw"),
)
AZURE_STORAGE_CONNECTION_STRING: str = _env("AZURE_STORAGE_CONNECTION_STRING")

# Prefix inside the container for per-user Parquet layout, e.g. "tenants" → "tenants/{user_id}/..."
USER_DATA_BLOB_PREFIX: str = _env("USER_DATA_BLOB_PREFIX", "users")

# --- SQL agent ---
# duckdb = LangGraph analyst over user-scoped Parquet in Azure Blob (see streamchat/duckdb_sandbox.py).
SQL_AGENT_BACKEND: str = _env("SQL_AGENT_BACKEND", "duckdb").lower()

# --- Observability ---
LOG_LEVEL: str = _env("LOG_LEVEL", "INFO").upper()

# --- Guided connector onboarding (Phase 5) ---
# Public URL of this API — used as OAuth redirect_uri (must match provider app settings).
API_PUBLIC_BASE_URL: str = _env("API_PUBLIC_BASE_URL", "http://127.0.0.1:8000")
# Browser URL to send users after OAuth callback (e.g. SPA /data/connect).
ONBOARDING_FRONTEND_REDIRECT: str = _env(
    "ONBOARDING_FRONTEND_REDIRECT", "http://localhost:5173/data/connect"
)
# Set to 1 to allow Docker-based check/discover in onboarding tools (local dev only).
ONBOARDING_DOCKER_ENABLED: bool = _env("ONBOARDING_DOCKER_ENABLED", "0").lower() in (
    "1",
    "true",
    "yes",
)
# `run_sync` Docker read probe: timeout (seconds) and max streams to include in configured catalog.
ONBOARDING_DOCKER_READ_TIMEOUT: int = int(_env("ONBOARDING_DOCKER_READ_TIMEOUT", "300") or "300")
ONBOARDING_DOCKER_READ_STREAM_CAP: int = int(_env("ONBOARDING_DOCKER_READ_STREAM_CAP", "3") or "3")
ONBOARDING_RATE_LIMIT_PER_MIN: int = int(_env("ONBOARDING_RATE_LIMIT_PER_MIN", "40") or "40")
