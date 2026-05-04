"""Alert builder agent — NL → structured AlertDefinition via LangGraph."""

from __future__ import annotations

import logging

import duckdb
from langchain.agents import create_agent
from langchain.agents.middleware import SummarizationMiddleware

from ai.agents.alerts.prompts import ALERT_BUILDER_SYSTEM_PROMPT
from ai.agents.alerts.tools import (
    inspect_columns,
    list_user_tables,
    propose_alert,
    validate_metric_sql,
)
from ai.llms import get_analyst_llm

logger = logging.getLogger(__name__)


class AlertBuilderAgent:
    """Turns natural language into a structured AlertDefinition."""

    def __init__(
        self,
        llm=None,
        checkpointer=None,
        conn: duckdb.DuckDBPyConnection | None = None,
        *,
        user_id: str | None = None,
    ):
        if conn is None:
            raise ValueError("DuckDB connection is required.")

        model = (
            get_analyst_llm() if llm is None or isinstance(llm, str) else llm
        )

        self.agent = create_agent(
            model=model,
            tools=[
                list_user_tables,
                inspect_columns,
                validate_metric_sql,
                propose_alert,
            ],
            middleware=[
                SummarizationMiddleware(
                    model=model,
                    trigger=("tokens", 4000),
                    keep=("messages", 10),
                )
            ],
            checkpointer=checkpointer,
            system_prompt=ALERT_BUILDER_SYSTEM_PROMPT,
        )

    def get_agent(self):
        return self.agent
