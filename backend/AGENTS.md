# Project Context
I am building a web application that enables users to interact with their data using natural language powered by Generative AI. Users can ask questions about their data, create visuals, build reports (including mixed visuals, KPI blocks, and more), rearrange visuals within a report, and export their reports seamlessly.

# Role
Act as an python expert, developing backend for React and TypeScript Frontend Architecture.

# Tech Stack
- **AI Agent Framework**: Langchain (Python)
- **Web Framework**: FastAPI (Python)

# Project Folder Structure

/src/ai                # LLMs (`llms.py`), agents (`agents/onboarding`, `agents/sql`), tools
/src/fastapi_app       # FastAPI application (routers, services, models, utils)
/supabase              # SQL migrations and queries (templates, etc.)