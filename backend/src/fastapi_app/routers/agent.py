"""
SQL agent and backend capability flags (evolves with checklist §6 DuckDB + Parquet).

The interactive chat stream remains under ``/api/chat``; this router exposes
read-only status for clients and operators.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from fastapi import APIRouter

from fastapi_app.settings import SQL_AGENT_BACKEND

router = APIRouter(prefix="/api/agent", tags=["agent"])


class SqlBackendStatus(BaseModel):
    sql_agent_backend: str = Field(
        ...,
        description="duckdb — controlled by SQL_AGENT_BACKEND env.",
    )
    duckdb_parquet_ready: bool = Field(
        True,
        description="True when user-scoped Parquet read path is enabled.",
    )
    notes: str | None = None


@router.get("/sql-backend", response_model=SqlBackendStatus)
async def sql_backend_status():
    """Report which analytical SQL backend the server is configured to use."""
    backend = SQL_AGENT_BACKEND
    ready = backend in ("duckdb", "parquet")
    return SqlBackendStatus(
        sql_agent_backend=backend,
        duckdb_parquet_ready=ready,
        notes=(
            None
            if ready
            else "Set SQL_AGENT_BACKEND=duckdb to enable the production SQL agent backend."
        ),
    )
