"""LangChain onboarding agent (create_agent + tool middleware)."""

from __future__ import annotations

import json
import logging

from langchain.agents import create_agent
from langchain.agents.middleware import SummarizationMiddleware, wrap_tool_call
from langchain_core.messages import ToolMessage
from langgraph.checkpoint.memory import MemorySaver

from fastapi_app.onboarding.llm import get_onboarding_llm
from fastapi_app.onboarding.stores import resolve_secrets
from fastapi_app.onboarding.tools import ALL_TOOLS

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are an AI agent that guides users through connecting a data source (Airbyte-style connectors) in a web app.

## Workflow

1. **Analyse** the connection specification you receive for auth methods (oneOf / credentials), required fields, and OAuth.
2. **Auth choice** — If multiple auth variants exist, call `render_auth_options` and wait for the user's choice.
3. **Credentials** — Call `render_input_fields` with clear labels and descriptions. Never ask users to paste secrets in freeform chat; use the form.
4. **Connection test** — Build nested config per schema (including discriminators like `auth_type`). Call `test_connection` and fix issues until it succeeds.
5. **Streams** — Call `discover_streams` when Docker discover is enabled; otherwise use `render_stream_selector` with streams the user should sync (or ask them to pick).
6. **OAuth** — For OAuth, after collecting client id/secret (and shop if Shopify), call `start_oauth_flow`. The user completes consent in the browser; tokens arrive in a follow-up message.
7. **Save** — Call `save_config` with the full working `config` (include `__oauth_meta__` when applicable) and `selected_streams` when known.
8. **Test sync (required)** — After `save_config` succeeds, you **must** call `run_sync` with the same `connector_name` and the user-selected stream names (or the streams you saved). When the server has `ONBOARDING_DOCKER_ENABLED=1`, this runs a real Docker `discover` + `read` on the connector image (minimal streams) and only then sets `sync_validated`. Be honest: if Docker is disabled, `run_sync` does **not** prove extraction — tell the user to enable Docker locally and re-run for a real end-to-end check.

## Rules

- After any `render_*` tool, stop and wait for the user's next message.
- Preserve schema nesting when building `config` (e.g. credentials oneOf).
- Secret fields may appear as `__SECRET_REF__:field_key` — pass them through unchanged in config dicts.
- Use ISO 8601 dates where required.
- Be concise; explain what you're doing and quote connector error text when diagnosing failures.
"""


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
        system_prompt=SYSTEM_PROMPT,
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
