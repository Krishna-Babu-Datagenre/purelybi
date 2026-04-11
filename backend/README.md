# BI Agent Backend

FastAPI service for the Purely BI app: **auth**, **dashboards**, **templates**, **connectors**, **streaming chat** over user data (DuckDB over Parquet), and **guided onboarding** for new sources.

Python **3.12+**, dependencies managed with **[uv](https://github.com/astral-sh/uv)**. Application code lives under **`src/`** as installable packages (`fastapi_app`, `ai`).

---

## Quick start

```bash
uv sync
```

Copy `.env-example` to `.env` and set API keys and URLs (see comments in `.env-example`).

**Run the API** (from this `backend/` directory):

```bash
uv run python -m uvicorn fastapi_app.app:app --reload --host 127.0.0.1 --port 8000
```

On Windows, prefer `python -m uvicorn` over the `uvicorn` script to avoid path issues with `uv run`.

- Docs: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/health`

---

## Layout

| Path | Purpose |
|------|---------|
| `src/fastapi_app/` | FastAPI app: `app.py`, routers, services, Pydantic models, middleware |
| `src/ai/` | `llms.py`, agents (`agents/onboarding/`, `agents/sql/`), optional shared `tools/` |
| `tests/` | Unit tests |
| `supabase/` | SQL snippets and schema assets used with Supabase |
| `docs/` | Extra API notes (OpenAPI remains the source of truth for routes) |

Agent code is **not** embedded under `fastapi_app/`; HTTP layers import from `ai`.

---

## Chat and analytics

`POST /api/chat` streams SSE responses from the **analyst** agent (`agent_type`: `"analyst"`). It runs **read-only SQL** against a tenant-scoped **DuckDB** connection built over **Parquet** in Azure Blob (`src/ai/agents/sql/duckdb_sandbox.py`). Widget hydration for dashboards uses the same sandbox pattern in `fastapi_app/services/widget_data_service.py`.

---

## Deployment

CI/CD is defined under the repo’s `.github/workflows/` (e.g. App Service deploy). The build installs dependencies and the local package so `fastapi_app` and `ai` resolve from `src/` in the deployment artifact.

---

## More context

See **`AGENTS.md`** in this folder for Cursor/agent-oriented notes on the backend.
