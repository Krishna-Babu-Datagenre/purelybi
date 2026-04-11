# Project Context
Purely BI
I am building a web application that allows users to easily integrate data from multiple platforms, setup automatic sync and then interact with their data using natural language powered by Generative AI. Once the data has been setup, the users can ask questions about their data, create visuals, build reports (including mixed visuals, KPI blocks, and more), rearrange visuals within a report, and export their reports seamlessly.

## Backend layout (`backend/`)

- **`pyproject.toml`**, **`uv.lock`** — Python deps; app code lives under **`src/`** (import as `fastapi_app`, `ai`).
- **`main.py`** — CLI entry; **`fastapi_app.app:app`** is the ASGI app.
- **`src/fastapi_app/`** — FastAPI surface area:
  - **`app.py`** — router registration, middleware
  - **`routers/`** — HTTP routes (`chat`, `auth`, `dashboards`, `connectors`, `templates`, `onboarding`, `agent`, …)
  - **`services/`** — business logic (`chat_service`, `widget_data_service`, `connector_service`, `dashboard_service`, …)
  - **`models/`** — Pydantic / API models
  - **`middleware/`**, **`utils/`** (`auth_dep`, `supabase_client`), **`settings.py`**
- **`src/ai/`** — LangChain / agents and tools:
  - **`agents/sql/`** — analyst agent, **`duckdb_sandbox.py`**, prompts, streaming
  - **`agents/onboarding/`** — onboarding agent; **`infra/`** (Azure, Docker, OAuth, stores)
  - **`tools/sql/`** — DuckDB tools, **`charts.py`** (widgets / ECharts)
  - **`tools/common/`** — shared tools (calculator, calendar, weather)
  - **`llms.py`** — model construction
- **`docs/`** — backend API notes (`docs/fast_api/*.md`); **`tests/`** — pytest; **`supabase/`** — SQL and schema helpers.