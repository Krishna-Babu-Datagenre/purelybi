# Project Context
Purely BI
I am building a web application that allows users to easily integrate data from multiple platforms, setup automatic sync and then interact with their data using natural language powered by Generative AI. Once the data has been setup, the users can ask questions about their data, create visuals, build reports (including mixed visuals, KPI blocks, and more), rearrange visuals within a report, and export their reports seamlessly.

## Backend layout (`backend/`)

- **`pyproject.toml`**, **`uv.lock`** — Python deps; app code lives under **`src/`** (import as `fastapi_app`, `ai`).
- **`main.py`** — CLI entry; **`fastapi_app.app:app`** is the ASGI app.
- **`src/fastapi_app/`** — FastAPI surface area:
  - **`app.py`** — router registration, middleware
  - **`routers/`** — HTTP routes (`chat`, `auth`, `dashboards`, `connectors`, `alerts`, `metadata`, `onboarding`, `templates`, `agent`)
  - **`services/`** — core business logic
  - **`models/`** — Pydantic / API models
  - **`middleware/`**, **`utils/`**, **`settings.py`** — cross-cutting concerns and configuration
- **`src/ai/`** — LangChain / agents and tools:
  - **`agents/`** — specific agents (`sql`, `onboarding`, `alerts`, `dashboard`), along with their prompts, infra, and streaming handlers
  - **`tools/`** — specific and shared tools (`sql` with duckdb tools, `onboarding`, `dashboard_tools.py`, `common`)
  - **`llms.py`** — foundational LLM construction
- **`docs/`** — API and project docs; **`tests/`** — pytest suite; **`supabase/`** — SQL and schema helpers.

## Instructions

To ensure maximum efficiency, quality, and alignment during development, strictly adhere to the following principles:

1. **Clarify, Don't Assume:** Never guess intent or missing requirements. If ambiguous, stop and ask.
2. **Validate & Elevate:** Don't blindly implement my ideas. If my approach isn't production-grade or best-practice, push back and propose the optimal alternative.
3. **Step-by-Step Execution:** Always outline a sub-task plan first. Get my approval, then execute *one step at a time*.
4. **Incremental Verification:** Pause after completing each sub-task so I can test it. Never dump massive code blocks and wait until the end for verification.
5. **Production Quality by Default:** Write DRY code with robust error handling, edge-case checks, and strict type safety. No quick hacks.