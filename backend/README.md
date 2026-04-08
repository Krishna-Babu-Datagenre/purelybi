# BI Agent Backend

Backend API for a BI-style app: natural-language analytics (LangChain agents over SQL), dashboard templates, and chat streaming via **FastAPI**. A **Streamlit** app under `sql-agent/` exists for local experimentation with the same agents.

---

## Install (developers)

```bash
uv sync
```

Copy `.env-example` to `.env` (PowerShell: `Copy-Item .env-example .env`).

Edit `.env` with your keys (e.g. Azure OpenAI). Paths are relative to the repo root.

---

## Run locally

**API (main backend):**

```bash
uv run python -m uvicorn fastapi_app.app:app --reload --host 127.0.0.1 --port 8000
```

Use `python -m uvicorn` (not `uv run uvicorn`). On Windows, `uv run` can fail with **Failed to canonicalize script path** when launching the `uvicorn` console script; invoking the module avoids that. With a venv activated, `python -m uvicorn …` is equivalent.

- OpenAPI docs: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/health`

**Streamlit (optional, SQL agent playground):**

```bash
uv run python -m streamlit run sql-agent/app/app.py
```

---

## CI/CD (GitHub Actions)

Workflow: `.github/workflows/deploy-azure.yml`

- **Trigger:** push to `main`, or manual **workflow_dispatch**
- **Build:** `uv pip compile` → install deps into `.python_packages/lib/site-packages` → artifact zip
- **Deploy:** `azure/webapps-deploy` to the App Service named in the workflow (`AZURE_WEBAPP_NAME`); requires repo secret **`AZURE_WEBAPP_PUBLISH_PROFILE`**

Adjust `AZURE_WEBAPP_NAME` if you deploy to a different app.

---

## Azure resources (non-prod / CI)

| Name | Value |
|------|--------|
| Subscription ID | `3892ad52-b508-4b32-8a93-ac5a9c1712e4` |
| Region | `centralindia` |
| Resource group | `rg-pocs-np-ci` |
| App Service plan | `asp-dravya-bi-backend-np-ci` |
| App Service (API) | `app-dravya-bi-backend-np-ci` |
| Public base URL | `https://app-dravya-bi-backend-np-ci.azurewebsites.net` |
| Storage account (blob / ADLS Gen2) | `adlsgen2dravya01` |
| Analytics data (Parquet) | container per `BLOB_CONTAINER_NAME` / `AZURE_STORAGE_CONTAINER` (e.g. `raw`), prefix `USER_DATA_BLOB_PREFIX/{user_id}/` |

---

## Analytics data (DuckDB + Parquet)

The API does **not** bundle a local analytics database file. The chat agent and dashboard widget hydration read **user-scoped Parquet** in Azure Blob through an ephemeral **DuckDB** connection (`AZURE_STORAGE_CONNECTION_STRING`, `streamchat/duckdb_sandbox.py`). Sync jobs (Azure Functions / Container Apps workers) populate that layout; see `MULTI_TENANT_TASK_CHECKLIST.md` Phase 6.

---

## Backend health

**Liveness / quick check:**

```powershell
Invoke-RestMethod -Uri "https://app-dravya-bi-backend-np-ci.azurewebsites.net/health" -Method GET
```

Use **`GET /health`** for status.

---

## Repo layout (short)

| Path | Role |
|------|------|
| `fastapi_app/` | FastAPI app, routers, services |
| `sql-agent/streamchat/` | LangChain agents and tools |
| `sql-agent/app/` | Streamlit demo |
| `supabase/` | SQL migrations / queries (templates, etc.) |

See `AGENTS.md` for project context and folder conventions.
