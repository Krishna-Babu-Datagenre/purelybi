"""
Chat service – manages agent lifecycle, session memory, and streaming.

Exposes a pure-Python async generator suitable for FastAPI's
``StreamingResponse`` / SSE.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator

from langgraph.checkpoint.memory import InMemorySaver
from ai.agents.dashboard import DashboardBuilderAgent
from ai.agents.dashboard.user_proxy import run_user_proxy_decision
from ai.agents.dashboard.context import (
    reset_dashboard_tool_context,
    set_dashboard_tool_context,
)
from ai.agents.sql import AnalystAgent
from ai.agents.sql.duckdb_sandbox import create_tenant_sandbox
from ai.tools.sql.charts import (
    clear_query_result,
    set_discovered_tables,
    set_session_conn,
    set_session_context,
)

logger = logging.getLogger(__name__)

_MAX_MAGIC_PROXY_ROUNDS = 12

# ---------------------------------------------------------------------------
# Agent registry
# ---------------------------------------------------------------------------

AGENT_CLASSES: dict[str, type] = {
    "analyst": AnalystAgent,
    "dashboard": DashboardBuilderAgent,
}

# ---------------------------------------------------------------------------
# In-memory session store
#
# Each entry keeps a (checkpointer, agent, settings_tuple) so we can
# detect when the caller changes model / db and transparently rebuild.
# ---------------------------------------------------------------------------
_sessions: dict[str, dict[str, Any]] = {}


def _get_or_create_agent(
    session_id: str,
    tenant_id: str,
    agent_type: str,
    llm: str,
    database: str,
    *,
    dashboard_mode: str = "guided",
    dashboard_datasets_key: tuple[str, ...] | None = None,
):
    """
    Return the LangGraph agent for *session_id*, creating or recreating it
    when settings change.
    """
    if agent_type == "dashboard":
        desired = (
            tenant_id,
            agent_type,
            llm,
            database,
            dashboard_mode,
            dashboard_datasets_key,
        )
    else:
        desired = (tenant_id, agent_type, llm, database)
    entry = _sessions.get(session_id)

    if entry is not None and entry["settings"] == desired:
        return entry["agent"]
    if entry is not None:
        conn = entry.get("conn")
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.exception("Failed closing stale DuckDB connection.")

    agent_cls = AGENT_CLASSES.get(agent_type)
    if agent_cls is None:
        raise ValueError(
            f"Unknown agent_type '{agent_type}'. "
            f"Valid options: {list(AGENT_CLASSES.keys())}"
        )

    checkpointer = InMemorySaver()
    agent_kwargs: dict[str, Any] = {
        "llm": llm,
        "checkpointer": checkpointer,
        "database": database,
    }
    conn = None
    discovered_tables: frozenset[str] = frozenset()
    if database.lower() == "duckdb":
        views_filter: frozenset[str] | None = None
        if agent_type == "dashboard" and dashboard_datasets_key is not None:
            views_filter = frozenset(dashboard_datasets_key)
        conn, discovered_tables = create_tenant_sandbox(
            tenant_id=tenant_id,
            views_filter=views_filter,
        )
        agent_kwargs["conn"] = conn
        if agent_type == "dashboard":
            agent_kwargs["mode"] = dashboard_mode

    agent = agent_cls(**agent_kwargs).get_agent()

    _sessions[session_id] = {
        "checkpointer": checkpointer,
        "agent": agent,
        "settings": desired,
        "conn": conn,
        "discovered_tables": discovered_tables,
        "agent_type": agent_type,
    }
    return agent


# ---------------------------------------------------------------------------
# Helpers ported from visual_utils.py
# ---------------------------------------------------------------------------


def _try_extract_chart(tool_result: str) -> dict[str, Any] | None:
    """If *tool_result* is successful ``create_react_chart`` or ``create_react_kpi`` JSON
    (``success`` and ``chartConfig``), return the parsed dict; otherwise ``None``."""
    try:
        result = json.loads(tool_result)
        if (
            isinstance(result, dict)
            and result.get("success")
            and "chartConfig" in result
        ):
            return result
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _stringify_model_content(content: Any) -> str:
    """Normalize provider-specific message blocks into plain text for SSE tokens."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                if "text" in block:
                    parts.append(str(block.get("text", "")))
                elif block.get("type") == "text" and "text" in block:
                    parts.append(str(block.get("text", "")))
            else:
                parts.append(str(block))
        return "".join(parts)
    return str(content)


def _stringify_tool_output(content: Any) -> str:
    """Serialize tool message bodies so SSE payload stays consistently string-based."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    try:
        return json.dumps(content, default=str)
    except TypeError:
        return str(content)


# ---------------------------------------------------------------------------
# Single graph turn (one user message → streamed model/tools)
# ---------------------------------------------------------------------------


async def _stream_one_graph_turn(
    agent: Any,
    user_message: str,
    config: dict[str, Any],
    _sse: Any,
    _next_item: Any,
    _DONE: Any,
) -> AsyncGenerator[str, None]:
    active_tool_calls: dict[str, str] = {}
    tool_call_args: dict[str, str] = {}
    stream = agent.stream(
        {"messages": [{"role": "user", "content": user_message}]},
        config=config,
        stream_mode="messages",
    )
    stream_iter = iter(stream)

    while True:
        result = await asyncio.to_thread(_next_item, stream_iter)
        if result is _DONE:
            break
        message_chunk, metadata = result

        node = metadata.get("langgraph_node", "")

        if node == "model":
            if message_chunk.tool_calls:
                for tc in message_chunk.tool_calls:
                    name = tc.get("name", "")
                    tid = tc.get("id")
                    if name and tid and tid not in active_tool_calls:
                        active_tool_calls[tid] = name
                        tool_call_args[tid] = ""
                        yield _sse(
                            "tool_call_start",
                            {
                                "tool_call_id": tid,
                                "tool_name": name,
                            },
                        )
                        await asyncio.sleep(0.01)

            if (
                hasattr(message_chunk, "tool_call_chunks")
                and message_chunk.tool_call_chunks
            ):
                for chunk in message_chunk.tool_call_chunks:
                    raw_args = chunk.get("args", "")
                    if isinstance(raw_args, str):
                        chunk_args = raw_args
                    elif raw_args in (None, "", {}):
                        chunk_args = ""
                    else:
                        chunk_args = json.dumps(raw_args, default=str)
                    chunk_id = chunk.get("id")
                    tid = chunk_id or (
                        list(active_tool_calls.keys())[-1]
                        if active_tool_calls
                        else None
                    )
                    if tid and chunk_args:
                        tool_call_args[tid] = (
                            tool_call_args.get(tid, "") + chunk_args
                        )
                        yield _sse(
                            "tool_call_args",
                            {
                                "tool_call_id": tid,
                                "args_chunk": chunk_args,
                            },
                        )
                        await asyncio.sleep(0.01)

            if message_chunk.content and not active_tool_calls:
                text = _stringify_model_content(message_chunk.content)
                if text:
                    yield _sse("token", {"content": text})
                    await asyncio.sleep(0.01)

        elif node == "tools" and hasattr(message_chunk, "tool_call_id"):
            tool_call_id = message_chunk.tool_call_id
            tool_name = active_tool_calls.pop(
                tool_call_id, message_chunk.name
            )
            args = tool_call_args.pop(tool_call_id, "{}")

            result_text = _stringify_tool_output(
                getattr(message_chunk, "content", None)
            )
            chart_data = (
                _try_extract_chart(result_text)
                if tool_name in ("create_react_chart", "create_react_kpi")
                else None
            )

            if chart_data is not None:
                chart_payload: dict[str, Any] = {
                    "tool_call_id": tool_call_id,
                    "chart_type": chart_data.get(
                        "chart_type", "chart"
                    ),
                    "chartConfig": chart_data["chartConfig"],
                }
                if chart_data.get("title") is not None:
                    chart_payload["title"] = chart_data["title"]
                if chart_data.get("dataConfig") is not None:
                    chart_payload["dataConfig"] = chart_data["dataConfig"]
                yield _sse("chart", chart_payload)
            else:
                yield _sse(
                    "tool_result",
                    {
                        "tool_call_id": tool_call_id,
                        "tool_name": tool_name,
                        "args": args,
                        "result": result_text,
                    },
                )
            await asyncio.sleep(0.01)

        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# Core streaming generator
# ---------------------------------------------------------------------------


async def stream_agent_response(
    message: str,
    tenant_id: str,
    session_id: str = "default",
    agent_type: str = "analyst",
    llm: str = "gpt-4.1",
    database: str = "DuckDB",
    *,
    dashboard_mode: str = "guided",
    selected_datasets: list[str] | None = None,
    magic_dashboard_name: str | None = None,
    magic_goal: str | None = None,
) -> AsyncGenerator[str, None]:
    """
    Async generator that yields **Server-Sent Event** (SSE) formatted
    strings while the LangGraph agent processes *message*.

    SSE event types:

    * ``token``           – partial text content from the model
    * ``tool_call_start`` – a tool invocation begins
    * ``tool_call_args``  – argument fragment for the active tool call
    * ``tool_result``     – tool finished, here is the result
    * ``chart``           – Plotly figure ready to render
    * ``segment_end``     – Magic Mode: one assistant turn finished; client flushes buffers
    * ``proxy_reply``     – Magic Mode: synthetic user message content (display optional)
    * ``end``             – stream finished
    * ``error``           – something went wrong
    """
    def _sse(event: str, data: Any) -> str:
        """Format a single SSE frame."""
        payload = json.dumps(data, default=str)
        return f"event: {event}\ndata: {payload}\n\n"

    ds_key: tuple[str, ...] | None = None
    if agent_type == "dashboard" and selected_datasets is not None:
        ds_key = tuple(sorted(selected_datasets))

    try:
        agent = _get_or_create_agent(
            session_id=session_id,
            tenant_id=tenant_id,
            agent_type=agent_type,
            llm=llm,
            database=database,
            dashboard_mode=dashboard_mode,
            dashboard_datasets_key=ds_key,
        )
    except ValueError as exc:
        yield _sse("start", {"status": "streaming"})
        yield _sse("error", {"detail": str(exc)})
        return

    config = {"configurable": {"thread_id": session_id}}

    # Set session context so chart tools can locate stored DataFrames and tenant views
    set_session_context(session_id)
    dash_ctx_token = None
    if agent_type == "dashboard":
        dash_ctx_token = set_dashboard_tool_context(tenant_id)
    entry = _sessions.get(session_id) or {}
    set_discovered_tables(
        session_id, entry.get("discovered_tables", frozenset())
    )
    set_session_conn(session_id, entry.get("conn"))

    # Yield "start" immediately so the client receives headers and can show
    # "Agent is thinking" / thought section without waiting for the first agent chunk.
    yield _sse("start", {"status": "streaming"})
    await asyncio.sleep(
        0.01
    )  # Force flush: StreamingResponse buffers otherwise

    # -- helpers to bridge sync → async ----------------------------------
    # agent.stream() is a *synchronous* iterator.  If we consume it
    # directly inside this async generator the for-loop blocks the
    # event-loop and FastAPI cannot flush individual SSE frames – the
    # client receives everything in one burst.
    #
    # Solution: pull one item at a time from the sync iterator inside a
    # thread (via asyncio.to_thread) so the event loop stays free to
    # write each yielded chunk to the socket immediately.

    _DONE = object()  # sentinel – StopIteration cannot cross thread boundary

    def _next_item(it):
        """Return the next (chunk, metadata) or _DONE sentinel."""
        try:
            return next(it)
        except StopIteration:
            return _DONE

    try:
        use_magic_proxy = (
            agent_type == "dashboard" and dashboard_mode == "magic"
        )
        current_user_message = message
        round_idx = 0

        while True:
            round_idx += 1
            async for frame in _stream_one_graph_turn(
                agent,
                current_user_message,
                config,
                _sse,
                _next_item,
                _DONE,
            ):
                yield frame

            if not use_magic_proxy or round_idx >= _MAX_MAGIC_PROXY_ROUNDS:
                break

            state = agent.get_state(config)
            thread_messages = (state.values or {}).get("messages") or []
            decision = await run_user_proxy_decision(
                magic_dashboard_name=magic_dashboard_name,
                magic_goal=magic_goal,
                selected_datasets=selected_datasets,
                thread_messages=thread_messages,
            )
            if not decision.continue_automation:
                break

            yield _sse("segment_end", {})
            await asyncio.sleep(0.01)
            yield _sse("proxy_reply", {"content": decision.user_message})
            await asyncio.sleep(0.01)
            current_user_message = decision.user_message

        yield _sse("end", {})
        await asyncio.sleep(0.01)

    except Exception as exc:
        logger.exception(
            "Error during agent stream for session %s", session_id
        )
        yield _sse("error", {"detail": str(exc)})
    finally:
        if dash_ctx_token is not None:
            reset_dashboard_tool_context(dash_ctx_token)


# ---------------------------------------------------------------------------
# Conversation history
# ---------------------------------------------------------------------------


def get_conversation_history(
    session_id: str,
    agent_type: str = "analyst",
    llm: str = "gpt-4.1",
    database: str = "DuckDB",
) -> list[dict[str, Any]]:
    """
    Return the full conversation for *session_id* as a JSON-serialisable
    list of message dicts.

    Message shape follows ``ChatMessage`` from ``models/chat.py``.
    """
    entry = _sessions.get(session_id)
    if entry is None:
        return []

    agent = entry["agent"]
    config = {"configurable": {"thread_id": session_id}}

    state = agent.get_state(config)
    messages = state.values.get("messages")
    if not messages:
        return []

    result: list[dict[str, Any]] = []
    for msg in messages:
        role = msg.type  # "human", "ai", "tool"
        if role == "human":
            result.append({"role": "user", "content": msg.content})
        elif role == "ai":
            entry_dict: dict[str, Any] = {
                "role": "assistant",
                "content": msg.content or None,
            }
            tool_calls = getattr(msg, "tool_calls", None)
            if tool_calls:
                entry_dict["tool_calls"] = [
                    {
                        "id": tc.get("id"),
                        "name": tc.get("name"),
                        "args": tc.get("args"),
                    }
                    for tc in tool_calls
                ]
            result.append(entry_dict)
        elif role == "tool":
            tool_entry: dict[str, Any] = {
                "role": "tool",
                "content": msg.content,
                "tool_call_id": getattr(msg, "tool_call_id", None),
                "tool_name": getattr(msg, "name", None),
            }
            # Attach chart if applicable
            chart_data = _try_extract_chart(msg.content)
            if chart_data:
                chart_entry: dict[str, Any] = {
                    "tool_call_id": getattr(msg, "tool_call_id", None),
                    "chart_type": chart_data.get("chart_type", "chart"),
                }
                chart_entry["chartConfig"] = chart_data["chartConfig"]
                if chart_data.get("title") is not None:
                    chart_entry["title"] = chart_data["title"]
                if chart_data.get("dataConfig") is not None:
                    chart_entry["dataConfig"] = chart_data["dataConfig"]
                tool_entry["chart"] = chart_entry
            result.append(tool_entry)

    return result
