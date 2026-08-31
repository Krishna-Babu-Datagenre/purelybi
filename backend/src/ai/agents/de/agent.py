"""DE pipeline builder agent."""

from __future__ import annotations

import logging

from langchain.agents import create_agent
from langchain.agents.middleware import SummarizationMiddleware

from ai.agents.de.prompts import DE_SYSTEM_PROMPT
from ai.agents.de.tools import build_de_tools
from ai.llms import get_analyst_llm

logger = logging.getLogger(__name__)


class DEAgent:
    """Agent that helps users build and manage DE pipelines via natural language."""

    def __init__(
        self,
        llm=None,
        checkpointer=None,
        database: str = "DuckDB",  # accepted for interface compatibility, not used
        conn=None,                  # accepted for interface compatibility, not used
        user_id: str | None = None,
        pipeline_id: str | None = None,
        connector_config_id: str | None = None,
        connector_name: str = "your connector",
    ):
        if not user_id:
            raise ValueError("user_id is required for DEAgent.")
        if not pipeline_id:
            raise ValueError("pipeline_id is required for DEAgent.")

        model = get_analyst_llm() if llm is None or isinstance(llm, str) else llm

        system_prompt = DE_SYSTEM_PROMPT.format(
            pipeline_id=pipeline_id,
            connector_name=connector_name,
        )

        self.agent = create_agent(
            model=model,
            tools=build_de_tools(
                user_id=user_id,
                pipeline_id=pipeline_id,
                connector_config_id=connector_config_id,
            ),
            middleware=[
                SummarizationMiddleware(
                    model=model,
                    trigger=("tokens", 3000),
                    keep=("messages", 10),
                )
            ],
            checkpointer=checkpointer,
            system_prompt=system_prompt,
        )

    def get_agent(self):
        return self.agent
