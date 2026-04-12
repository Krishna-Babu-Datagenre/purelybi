"""Dashboard builder agent — SQL + chart tools + Supabase dashboard CRUD."""

from __future__ import annotations

import logging
from typing import Literal

import duckdb
from langchain.agents import create_agent
from langchain.agents.middleware import SummarizationMiddleware

from ai.agents.dashboard.prompts import (
    DASHBOARD_GUIDED_SYSTEM_PROMPT,
    DASHBOARD_MAGIC_SYSTEM_PROMPT,
)
from ai.llms import get_analyst_llm
from ai.tools.common import get_current_time
from ai.tools.dashboard_tools import ALL_DASHBOARD_TOOLS
from ai.tools.sql import create_react_chart, create_react_kpi
from ai.tools.sql.duckdb_tools import build_duckdb_tools

logger = logging.getLogger(__name__)


class DashboardBuilderAgent:
    """Builds dashboards using DuckDB analytics tools and persistence tools."""

    def __init__(
        self,
        llm=None,
        checkpointer=None,
        database: str = "DuckDB",
        conn: duckdb.DuckDBPyConnection | None = None,
        *,
        mode: Literal["magic", "guided"] = "guided",
    ):
        if database.lower() != "duckdb":
            raise ValueError("Only DuckDB is supported.")
        if conn is None:
            raise ValueError("DuckDB connection is required.")
        model = (
            get_analyst_llm() if llm is None or isinstance(llm, str) else llm
        )
        database_tools = build_duckdb_tools(conn, model)

        system_prompt = (
            DASHBOARD_MAGIC_SYSTEM_PROMPT
            if mode == "magic"
            else DASHBOARD_GUIDED_SYSTEM_PROMPT
        )

        self.agent = create_agent(
            model=model,
            tools=database_tools
            + [
                get_current_time,
                create_react_chart,
                create_react_kpi,
            ]
            + ALL_DASHBOARD_TOOLS,
            middleware=[
                SummarizationMiddleware(
                    model=model,
                    trigger=("tokens", 4000),
                    keep=("messages", 14),
                )
            ],
            checkpointer=checkpointer,
            system_prompt=system_prompt,
        )

    def get_agent(self):
        return self.agent
