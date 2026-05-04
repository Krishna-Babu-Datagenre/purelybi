"""
Alert builder streaming service — SSE generator for the alert builder agent.

Modeled on ``chat_service.py`` but specialised for the alert builder flow.
Emits ``alert_preview`` events when the agent calls ``propose_alert``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator

from langgraph.checkpoint.memory import InMemorySaver

from ai.agents.alerts.agent import AlertBuilderAgent
from ai.agents.alerts.tools import get_alert_proposal, set_alert_tool_context
from ai.agents.sql.duckdb_sandbox import create_tenant_sandbox

logger = logging.getLogger(__name__)

# In-memory sessions for alert builder conversations
_alert_sessions: dict[str, dict[str, Any]] = {}


def _stringify_content(content: Any) -> str:
    """Normalize message content to plain text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and "text" in block:
                parts.append(str(block["text"]))
            else:
                parts.append(str(block))
        return "".join(parts)
    return str(content)


def _stringify_tool_output(content: Any) -> str:
    """Serialize tool output for SSE."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    try:
        return json.dumps(content, default=str)
    except TypeError:
        return str(content)


async def stream_alert_builder(
    message: str,
    tenant_id: str,
    session_id: str,
) -> AsyncGenerator[str, None]:
    """Async generator yielding SSE frames for the alert builder agent.

    SSE event types:
    - ``token``:            partial text from the model
    - ``tool_call_start``:  tool invocation begins
    - ``tool_call_args``:   argument fragment
    - ``tool_result``:      tool finished
    - ``alert_preview``:    structured AlertDefinition preview
    - ``end``:              stream finished
    - ``error``:            something went wrong
    """

    def _sse(event: str, data: Any) -> str:
        payload = json.dumps(data, default=str)
        return f"event: {event}\ndata: {payload}\n\n"

    # Get or create agent
    conn = None
    try:
        scoped_id = f"alert:{tenant_id}:{session_id}"
        entry = _alert_sessions.get(scoped_id)

        if entry is None:
            checkpointer = InMemorySaver()
            conn, _views = create_tenant_sandbox(tenant_id=tenant_id)
            set_alert_tool_context(conn, tenant_id)

            agent_instance = AlertBuilderAgent(
                checkpointer=checkpointer,
                conn=conn,
                user_id=tenant_id,
            )
            agent = agent_instance.get_agent()
            _alert_sessions[scoped_id] = {
                "agent": agent,
                "checkpointer": checkpointer,
                "conn": conn,
            }
        else:
            agent = entry["agent"]
            conn = entry["conn"]
            set_alert_tool_context(conn, tenant_id)

    except Exception as exc:
        logger.exception("Error creating alert builder agent for session %s", session_id)
        yield _sse("error", {"detail": str(exc)})
        return

    config = {
        "configurable": {"thread_id": scoped_id},
        "recursion_limit": 50,
    }

    yield _sse("start", {"status": "streaming"})
    await asyncio.sleep(0.01)

    _DONE = object()

    def _next_item(it):
        try:
            return next(it)
        except StopIteration:
            return _DONE

    try:
        active_tool_calls: dict[str, str] = {}
        tool_call_args_buf: dict[str, str] = {}

        stream = agent.stream(
            {"messages": [{"role": "user", "content": message}]},
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
                # Tool call starts
                if message_chunk.tool_calls:
                    for tc in message_chunk.tool_calls:
                        name = tc.get("name", "")
                        tid = tc.get("id")
                        if name and tid and tid not in active_tool_calls:
                            active_tool_calls[tid] = name
                            tool_call_args_buf[tid] = ""
                            yield _sse("tool_call_start", {
                                "tool_call_id": tid,
                                "tool_name": name,
                            })
                            await asyncio.sleep(0.01)

                # Tool call args chunks
                if hasattr(message_chunk, "tool_call_chunks") and message_chunk.tool_call_chunks:
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
                            list(active_tool_calls.keys())[-1] if active_tool_calls else None
                        )
                        if tid and chunk_args:
                            tool_call_args_buf[tid] = tool_call_args_buf.get(tid, "") + chunk_args
                            yield _sse("tool_call_args", {
                                "tool_call_id": tid,
                                "args_chunk": chunk_args,
                            })
                            await asyncio.sleep(0.01)

                # Text tokens
                if message_chunk.content and not active_tool_calls:
                    text = _stringify_content(message_chunk.content)
                    if text:
                        yield _sse("token", {"content": text})
                        await asyncio.sleep(0.01)

            elif node == "tools" and hasattr(message_chunk, "tool_call_id"):
                tool_call_id = message_chunk.tool_call_id
                tool_name = active_tool_calls.pop(tool_call_id, message_chunk.name)
                args = tool_call_args_buf.pop(tool_call_id, "{}")

                result_text = _stringify_tool_output(
                    getattr(message_chunk, "content", None)
                )

                yield _sse("tool_result", {
                    "tool_call_id": tool_call_id,
                    "tool_name": tool_name,
                    "args": args,
                    "result": result_text,
                })
                await asyncio.sleep(0.01)

                # Check if propose_alert was called successfully — emit alert_preview
                if tool_name == "propose_alert" and not result_text.startswith("Error:"):
                    try:
                        proposal = json.loads(args)
                        yield _sse("alert_preview", proposal)
                    except Exception:
                        pass
                await asyncio.sleep(0.01)

            await asyncio.sleep(0.01)

        yield _sse("end", {})
        await asyncio.sleep(0.01)

    except Exception as exc:
        logger.exception("Error in alert builder stream for session %s", session_id)
        yield _sse("error", {"detail": str(exc), "message": str(exc)})
