"""
Chat service – manages agent lifecycle, session memory, and streaming.

Mirrors the behaviour of the Streamlit ``agent_chat.py`` +
``visual_utils.py`` but exposes a pure-Python async generator suitable
for FastAPI's ``StreamingResponse`` / SSE.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator

from langgraph.checkpoint.memory import InMemorySaver
from streamchat.agents import AnalystAgent, SupervisorAgent
from streamchat.duckdb_sandbox import create_tenant_sandbox
from streamchat.tools.charts import clear_query_result, set_session_context

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Agent registry
# ---------------------------------------------------------------------------

AGENT_CLASSES: dict[str, type] = {
    "analyst": AnalystAgent,
    "supervisor": SupervisorAgent,
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
):
    """
    Return the LangGraph agent for *session_id*, creating or recreating it
    when settings change.
    """
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
    }
    # SupervisorAgent doesn't accept a `database` kwarg
    conn = None
    if agent_type != "supervisor":
        agent_kwargs["database"] = database
        if database.lower() == "duckdb":
            conn = create_tenant_sandbox(tenant_id=tenant_id)
            agent_kwargs["conn"] = conn

    agent = agent_cls(**agent_kwargs).get_agent()

    _sessions[session_id] = {
        "checkpointer": checkpointer,
        "agent": agent,
        "settings": desired,
        "conn": conn,
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
# Core streaming generator
# ---------------------------------------------------------------------------


async def stream_agent_response(
    message: str,
    tenant_id: str,
    session_id: str = "default",
    agent_type: str = "analyst",
    llm: str = "gpt-4.1",
    database: str = "DuckDB",
) -> AsyncGenerator[str, None]:
    """
    Async generator that yields **Server-Sent Event** (SSE) formatted
    strings while the LangGraph agent processes *message*.

    The event vocabulary mirrors the Streamlit streaming loop in
    ``display_streaming_response`` (visual_utils.py):

    * ``token``           – partial text content from the model
    * ``tool_call_start`` – a tool invocation begins
    * ``tool_call_args``  – argument fragment for the active tool call
    * ``tool_result``     – tool finished, here is the result
    * ``chart``           – Plotly figure ready to render
    * ``end``             – stream finished
    * ``error``           – something went wrong
    """
    agent = _get_or_create_agent(
        session_id=session_id,
        tenant_id=tenant_id,
        agent_type=agent_type,
        llm=llm,
        database=database,
    )
    config = {"configurable": {"thread_id": session_id}}

    # Set session context so chart tools can locate stored DataFrames
    set_session_context(session_id)

    # Track in-flight tool calls (mirrors active_tool_calls in visual_utils)
    active_tool_calls: dict[str, str] = {}  # tool_call_id → tool_name
    tool_call_args: dict[str, str] = {}  # tool_call_id → accumulated args

    def _sse(event: str, data: Any) -> str:
        """Format a single SSE frame."""
        payload = json.dumps(data, default=str)
        return f"event: {event}\ndata: {payload}\n\n"

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
        stream = agent.stream(
            {"messages": [{"role": "user", "content": message}]},
            config=config,
            stream_mode="messages",
        )

        stream_iter = iter(stream)

        while True:
            # Run the blocking next() call in a worker thread so the
            # event loop can flush previous SSE frames in the meantime.
            result = await asyncio.to_thread(_next_item, stream_iter)
            if result is _DONE:
                break
            message_chunk, metadata = result

            node = metadata.get("langgraph_node", "")

            # ----- model node: AI tokens & tool-call bookkeeping -----
            if node == "model":
                # New tool call detected
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

                # Accumulate streamed tool-call argument chunks
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

                # Regular text content
                if message_chunk.content and not active_tool_calls:
                    text = _stringify_model_content(message_chunk.content)
                    if text:
                        yield _sse("token", {"content": text})
                        await asyncio.sleep(0.01)

            # ----- tools node: results -----
            elif node == "tools" and hasattr(message_chunk, "tool_call_id"):
                tool_call_id = message_chunk.tool_call_id
                tool_name = active_tool_calls.pop(
                    tool_call_id, message_chunk.name
                )
                args = tool_call_args.pop(tool_call_id, "{}")

                result_text = _stringify_tool_output(
                    getattr(message_chunk, "content", None)
                )
                # Check for chart / KPI widget payload (same JSON shape: success + chartConfig)
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

            # Yield control so FastAPI/uvicorn flush each SSE frame to the client.
            await asyncio.sleep(0.01)

        yield _sse("end", {})
        await asyncio.sleep(0.01)

    except Exception as exc:
        logger.exception(
            "Error during agent stream for session %s", session_id
        )
        yield _sse("error", {"detail": str(exc)})


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
