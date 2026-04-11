# Purely BI

## Overview

Web app for integrating data from multiple platforms, running scheduled syncs into cloud storage, and exploring that data with natural language. Users connect sources through a guided flow, ask questions, generate chart and KPI widgets for dashboards and reports, and arrange or export content from the UI.

---

## Repository layout

Monorepo, main pieces:

| Path | Role |
|------|------|
| `backend/` | FastAPI (`src/fastapi_app/`), agents (`src/ai/`), Python deps via **uv** (`pyproject.toml`) |
| `frontend/` | Vite + React SPA |
| `azure-function-schema-updater/`, `azure-function-sync-orchestrator/` | Azure Functions (timers) |
| `docker-image/` | Container Apps Job image for sync workers |

---

## Local development

- **API** (from `backend/`): `uv run python -m uvicorn fastapi_app.app:app --reload --host 127.0.0.1 --port 8000`
- **Frontend** (from `frontend/`): `npm install` then `npm run dev` (default Vite port 5173)
- **Env**: copy `backend/.env-example` → `backend/.env` and fill values (see below)

Interactive API docs: `http://127.0.0.1:8000/docs` when the server is running.

---

## Configuration

Authoritative variable names and comments live in **`backend/.env-example`**. Highlights:

- **Supabase**: `SUPABASE_URL`, `SUPABASE_KEY` (anon), `SUPABASE_SERVICE_ROLE_KEY` (server-only)
- **Data plane (blob)**: `AZURE_STORAGE_ACCOUNT_URL`, `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_CONTAINER` / `BLOB_CONTAINER_NAME`, `USER_DATA_BLOB_PREFIX` (see [blob layout note](#data-storage-layout-blob) below)
- **Onboarding**: `API_PUBLIC_BASE_URL`, `ONBOARDING_FRONTEND_REDIRECT`, `ONBOARDING_DOCKER_ENABLED`, `ONBOARDING_DOCKER_EXECUTION_MODE` (`local` vs `azure_job`), optional ACA settings for remote Docker jobs
- **CORS**: `CORS_EXTRA_ORIGINS` (comma-separated) for deployed frontends

**LLM (required for chat and onboarding agents):** `AZURE_LLM_ENDPOINT`, `AZURE_LLM_API_KEY`, `AZURE_LLM_NAME` (used by `backend/src/ai/llms.py` for onboarding and SQL agents).

---

## API surface

FastAPI routers cover more than streaming agents: **auth** (Supabase-backed), **templates**, **dashboards**, **connectors** (catalog + user configs), **chat** (SSE), **onboarding** (SSE + `ui_block`), and **agent** (capabilities / backend flags). See OpenAPI at `/docs` or `fastapi_app/app.py` for tag descriptions.

---

## Tech Stack

### backend

FastAPI (`backend/src/fastapi_app`), LangChain / LangGraph agents in `backend/src/ai/`, DuckDB (per-tenant sandboxes over synced Parquet), Supabase (catalog, user connector configs, secrets). Python 3.12+, packaged with **uv** (`backend/pyproject.toml`).

### frontend

Vite, React 19, TypeScript (strict), Tailwind CSS, Zustand, Apache ECharts (`echarts-for-react`), `react-grid-layout`, Motion. Entry and scripts in `frontend/package.json`.

### cloud & data plane

**Azure** — Blob storage for Parquet, Azure Functions (schema registry + sync orchestration), Container Apps Jobs for PyAirbyte extraction workers, ACR for worker images. **Supabase** — Postgres + APIs for app metadata and connector state.

---

## Data Onboarding

### Purpose

Walk a user from “pick a connector” to a saved, test-validated sync configuration: resolve auth variants, collect credentials via structured UI (not raw chat), test the connection, choose streams, handle OAuth in the browser, persist config, and run a minimal sync probe so `sync_validated` can be set when Docker-based validation is enabled.

### Data Connectors

Airbyte OSS–style connectors: connection specs and Docker images come from the product catalog; a **schema updater** Function refreshes connector definitions into Supabase (`connector_schemas`). Runtime extraction uses **PyAirbyte** in the worker container; paths and orchestration are documented under [Azure Data Sync](#azure-data-sync---quick-ops-doc) below.

### Onboarding Agent

LangChain `create_agent` in `backend/src/ai/agents/onboarding/agent.py`, streamed over SSE (`/api/onboarding` — same event shape as chat plus `ui_block`). Tools combine **UI** (`render_auth_options`, `render_input_fields`, `render_stream_selector`, `start_oauth_flow`), **connector ops** (`get_connector_spec`, `test_connection`, `discover_streams`, `run_sync`), and **persistence** (`save_config` → Supabase). Optional Docker-backed discover/read when `ONBOARDING_DOCKER_ENABLED` is set.

---

## SQL Agent

### Purpose

Answer ad hoc questions against the user’s **DuckDB** view of synced data: discover schema, run read-only SQL, optionally emit ECharts and KPI widget payloads the React app can place on dashboards. Served from `/api/chat` (SSE); session-scoped agent state in `backend/src/fastapi_app/services/chat_service.py`. Primary implementation: `AnalystAgent` in `backend/src/ai/agents/sql/agent.py`.

### Tools

- **DuckDB**: `sql_db_list_tables`, `sql_db_schema`, `sql_db_query` (read-only; built in `backend/src/ai/tools/sql/duckdb_tools.py`).
- **Helpers**: `calculate`, `get_current_time`.
- **Widgets**: `create_react_chart`, `create_react_kpi` (bind to latest query result for the session).

---

## Azure Data Sync - Quick Ops Doc

### Purpose

Production flow for:
- Connector schema refresh (Airbyte registry -> Supabase)
- Scheduled user sync orchestration (Supabase configs -> Container Apps Job)
- Worker execution (PyAirbyte -> Parquet in Blob)

### End-to-end flow (UI -> scheduled sync)

- User connects a source in the product UI (onboarding flow) and config is saved to `user_connector_configs` in Supabase.
- If onboarding validation passes, row is marked `sync_validated=true`.
- `func-purelybi-sync-orchestrator-dev-ci` runs on its timer schedule (see Function responsibilities).
- It selects eligible connector configs and starts `caj-purelybi-data-sync-dev-ci` executions.
- Worker container reads config from Supabase, runs extraction, writes Parquet to blob, and updates sync status.

### Azure resources (dev baseline)

- Resource group: `rg-purelybi-dev-ci`
- Function App (orchestrator): `func-purelybi-sync-orchestrator-dev-ci`
  - Plan: Flex Consumption
  - Runtime: Python 3.12
- Function App (schema updater): `func-purelybi-schema-updater-dev-ci`
  - Plan: Flex Consumption
  - Runtime: Python 3.12
- ACR: `acrpurelybidevci` (`acrpurelybidevci.azurecr.io`)
- Storage account (data lake): `sapurelybidatalakedevci`
- Container Apps environment: `caenv-purelybi-dev-ci`
- Container Apps Job: `caj-purelybi-data-sync-dev-ci`

### Function responsibilities

- `func-purelybi-schema-updater-dev-ci`
  - Timer: daily (03:00 UTC)
  - Fetches Airbyte OSS registry
  - Upserts source schemas into Supabase `connector_schemas`

- `func-purelybi-sync-orchestrator-dev-ci`
  - Timer: every 2 hours
  - Finds eligible `user_connector_configs`
  - Starts Container Apps Job execution with per-run env (`SYNC_CONFIG_ID`, `SYNC_USER_ID`, `SYNC_CONNECTOR_NAME`)
  - Uses managed identity to call `Microsoft.App/jobs/start/action`

### Orchestrator start conditions

A config is eligible when all are true:
- `is_active = true`
- `sync_validated = true`
- `last_sync_status` is not `queued`
- `last_sync_status` is not `running`
- `last_sync_status` is not `reauth_required`
- `last_sync_at` is null, or elapsed minutes >= `sync_frequency_minutes`

### Sync status lifecycle

- `queued`: orchestrator accepted/start call succeeded
- `running`: worker process started and marked row running
- `success`: worker finished and stored output
- `failed`: worker/runtime failure or start failure handling
- `reauth_required`: token refresh requires user re-auth

### Data storage layout (blob)

**Path convention:** objects live under `{container}/{prefix}/{user_id}/{connector_name}/{stream_name}/{YYYY-MM}.parquet`, where `container` and `USER_DATA_BLOB_PREFIX` come from env (defaults in `fastapi_app/settings.py` are not identical to every deployment). The example below shows one production-style layout; adjust mentally if your env uses a different container or prefix.

Parquet path format (illustrative):

`raw/user-data/{user_id}/{connector_name}/{stream_name}/{YYYY-MM}.parquet`

Example:

`raw/user-data/c5efc103-bb7f-42dd-ae10-527612b146d4/source-facebook-marketing/ads_insights/2026-04.parquet`

Monthly behavior:
- If month file exists: download + append new rows + overwrite same monthly file
- If month file does not exist: create it

---

## Required app settings (minimum)

- Orchestrator function app:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `AZURE_SUBSCRIPTION_ID`
  - `AZURE_RESOURCE_GROUP`
  - `ACA_JOB_NAME`
  - `ACA_JOB_CONTAINER_NAME`

- Schema updater function app:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`

- Container Apps Job container:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY` (secret ref preferred)
  - `AZURE_STORAGE_CONNECTION_STRING` (secret ref preferred)
  - `BLOB_CONTAINER_NAME`
  - `AIRBYTE_ENABLE_UNSAFE_CODE=true`

### Deployment notes

- **FastAPI (Azure App Service):** `.github/workflows/deploy-azure-app-service.yml`
- **Frontend (Azure Static Web Apps):** `.github/workflows/deploy-azure-static-web-apps.yml`
- **Functions:**
  - `.github/workflows/deploy-azure-function-schema-updater.yml`
  - `.github/workflows/deploy-azure-function-sync-orchestrator.yml`
- **Sync worker image** (build / push / redeploy): `docker-image/README.md`
