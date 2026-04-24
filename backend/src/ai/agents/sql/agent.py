"""SQL / analytics agent (LangChain graph over DuckDB)."""

from __future__ import annotations

import duckdb
from langchain.agents import create_agent
from langchain.agents.middleware import SummarizationMiddleware

from ai.agents.sql.prompts import ANALYST_SYSTEM_PROMPT
from ai.tools.common import calculate, get_current_time
from ai.tools.dashboard_tools import ANALYST_DASHBOARD_TOOLS
from ai.tools.sql import create_react_chart, create_react_kpi
from ai.tools.sql.duckdb_tools import build_duckdb_tools
from ai.llms import get_analyst_llm


class AnalystAgent:
    """Agent designed to interact with a SQL database for data analysis tasks."""

    def __init__(
        self,
        llm="gpt-4.1-mini",
        checkpointer=None,
        database: str = "DuckDB",
        conn: duckdb.DuckDBPyConnection | None = None,
        user_id: str | None = None,
    ):
        if database.lower() != "duckdb":
            raise ValueError("Only DuckDB is supported by AnalystAgent.")
        if conn is None:
            raise ValueError("DuckDB connection is required for AnalystAgent.")
        model = get_analyst_llm() if llm is None or isinstance(llm, str) else llm
        database_tools = build_duckdb_tools(conn, model, user_id=user_id)

        self.agent = create_agent(
            model=model,
            tools=database_tools
            + [
                calculate,
                get_current_time,
                create_react_chart,
                create_react_kpi,
            ]
            + ANALYST_DASHBOARD_TOOLS,
            middleware=[
                SummarizationMiddleware(
                    model=model,
                    trigger=("tokens", 4000),
                    keep=("messages", 12),
                )
            ],
            checkpointer=checkpointer,
            system_prompt=ANALYST_SYSTEM_PROMPT,
        )

        self.starter_prompts = {
            ":blue[:material/database:] How many journeys were there in 2025?": (
                "How many journeys were there in 2025?"
            ),
            ":blue[:material/database:] YoY growth in journeys": (
                "What is the year-over-year growth in journeys between 2024 and 2025?"
            ),
        }

    def get_agent(self):
        return self.agent

    def get_starter_prompts(self):
        return self.starter_prompts


__all__ = ["AnalystAgent"]
