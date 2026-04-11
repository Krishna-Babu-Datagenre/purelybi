"""LangChain onboarding agent (create_agent + tool middleware)."""

from __future__ import annotations

import json
import logging

from langchain.agents import create_agent
from langchain.agents.middleware import SummarizationMiddleware, wrap_tool_call
from langchain_core.messages import ToolMessage
from langgraph.checkpoint.memory import MemorySaver

from ai.agents.onboarding.prompts import ONBOARDING_SYSTEM_PROMPT
from ai.agents.onboarding.infra.stores import resolve_secrets
from ai.agents.onboarding.tools import ALL_TOOLS
from ai.llms import get_onboarding_llm

logger = logging.getLogger(__name__)

_TOOL_MAP = {t.name: t for t in ALL_TOOLS}


@wrap_tool_call
def _direct_tool_executor(request, _handler):
    """Execute tools via .func(**kwargs) with secret resolution."""
    tc = request.tool_call
    name = tc["name"]
    args = tc["args"]
    tool = _TOOL_MAP.get(name)

    if tool is None:
        return ToolMessage(
            content=json.dumps({"error": f"Unknown tool: {name}"}),
            tool_call_id=tc["id"],
            name=name,
        )

    logger.info("Onboarding tool %s", name)
    try:
        resolved = resolve_secrets(args)
        result = tool.func(**resolved)
        return ToolMessage(
            content=str(result),
            tool_call_id=tc["id"],
            name=name,
        )
    except Exception as e:
        logger.exception("Tool %s failed", name)
        return ToolMessage(
            content=json.dumps({"error": f"Tool '{name}' failed: {e}"}),
            tool_call_id=tc["id"],
            name=name,
        )


def create_onboarding_agent(checkpointer=None):
    llm = get_onboarding_llm()
    if checkpointer is None:
        checkpointer = MemorySaver()

    return create_agent(
        model=llm,
        tools=ALL_TOOLS,
        system_prompt=ONBOARDING_SYSTEM_PROMPT,
        middleware=[
            _direct_tool_executor,
            SummarizationMiddleware(
                model=llm,
                trigger=("tokens", 4000),
                keep=("messages", 20),
            ),
        ],
        checkpointer=checkpointer,
    ).with_config({"recursion_limit": 50})
