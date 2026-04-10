"""SSE streaming for the onboarding agent (same event vocabulary as /api/chat + ui_block)."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator

from langchain_core.messages import HumanMessage

from fastapi_app.onboarding.agent_factory import create_onboarding_agent
from fastapi_app.onboarding.context import OnboardingContext, clear_onboarding_context, set_onboarding_context
from fastapi_app.onboarding.stores import clear_pending_ui, peek_pending_ui, prune_expired_oauth_states
from fastapi_app.onboarding.tools import UI_TOOL_NAMES

logger = logging.getLogger(__name__)

_DONE = object()

_sessions: dict[str, dict[str, Any]] = {}

# Remember catalog row per user + client thread so follow-up chat turns have context.
_catalog_by_thread: dict[str, dict[str, Any]] = {}


def remember_catalog(user_id: str, thread_id: str, catalog: dict[str, Any]) -> None:
    _catalog_by_thread[_session_key(user_id, thread_id)] = catalog


def get_remembered_catalog(user_id: str, thread_id: str) -> dict[str, Any]:
    return dict(_catalog_by_thread.get(_session_key(user_id, thread_id), {}))


def thread_id_for_graph(ctx: OnboardingContext) -> str:
    """Scoped thread id for the LangGraph checkpointer."""
    return f"onb-{ctx.user_id}-{ctx.thread_id}"


def _session_key(user_id: str, thread_id: str) -> str:
    return f"{user_id}:{thread_id}"


def get_or_create_onboarding_agent(user_id: str, thread_id: str):
    """Return a cached agent + checkpointer per user + thread."""
    key = _session_key(user_id, thread_id)
    entry = _sessions.get(key)
    if entry is not None:
        return entry["agent"]

    agent = create_onboarding_agent()
    _sessions[key] = {"agent": agent}
    return agent


def _sse(event: str, data: Any) -> str:
    payload = json.dumps(data, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def _stringify_model_content(content: Any) -> str:
    """Turn LangChain / provider message content into plain text for SSE ``token`` frames."""
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
    """Serialize tool message bodies for ``tool_result`` (always JSON-safe string)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    try:
        return json.dumps(content, default=str)
    except TypeError:
        return str(content)


def _next_item(it):
    try:
        return next(it)
    except StopIteration:
        return _DONE


async def stream_onboarding(
    *,
    ctx: OnboardingContext,
    messages: list[HumanMessage],
) -> AsyncGenerator[str, None]:
    """
    Stream LangGraph messages as SSE.

    Events: ``start``, ``token``, ``tool_call_start``, ``tool_call_args``,
    ``tool_result``, ``ui_block`` (dynamic UI payload), ``end``, ``error``.
    """
    prune_expired_oauth_states()
    set_onboarding_context(ctx)
    agent = get_or_create_onboarding_agent(ctx.user_id, ctx.thread_id)
    config = {
        "configurable": {"thread_id": thread_id_for_graph(ctx)},
        "recursion_limit": 50,
    }

    yield _sse("start", {"status": "streaming"})
    await asyncio.sleep(0.01)

    active_tool_calls: dict[str, str] = {}
    tool_call_args: dict[str, str] = {}

    try:
        stream = agent.stream(
            {"messages": messages},
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
                                {"tool_call_id": tid, "tool_name": name},
                            )
                            await asyncio.sleep(0.01)

                if (
                    hasattr(message_chunk, "tool_call_chunks")
                    and message_chunk.tool_call_chunks
                ):
                    for chunk in message_chunk.tool_call_chunks:
                        raw_args = chunk.get("args", "")
                        if isinstance(raw_args, str):
                            chunk_s = raw_args
                        elif raw_args in (None, "", {}):
                            chunk_s = ""
                        else:
                            chunk_s = json.dumps(raw_args, default=str)
                        chunk_id = chunk.get("id")
                        tid = chunk_id or (
                            list(active_tool_calls.keys())[-1]
                            if active_tool_calls
                            else None
                        )
                        if tid and chunk_s:
                            tool_call_args[tid] = tool_call_args.get(tid, "") + chunk_s
                            yield _sse(
                                "tool_call_args",
                                {
                                    "tool_call_id": tid,
                                    "args_chunk": chunk_s,
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

                ui = peek_pending_ui(user_id=ctx.user_id, thread_id=ctx.thread_id)
                if ui and (
                    tool_name in UI_TOOL_NAMES
                    or ui.get("type")
                    in (
                        "auth_options",
                        "input_fields",
                        "stream_selector",
                        "oauth_button",
                    )
                ):
                    # Let the client paint tool_result before the next UI paint (progressive UX).
                    await asyncio.sleep(0.02)
                    yield _sse("ui_block", {"ui": ui})
                    await asyncio.sleep(0.01)
                    clear_pending_ui(user_id=ctx.user_id, thread_id=ctx.thread_id)

            await asyncio.sleep(0.01)

        yield _sse("end", {})
        await asyncio.sleep(0.01)

    except Exception as exc:
        logger.exception("Onboarding stream failed")
        yield _sse("error", {"detail": str(exc)})
    finally:
        clear_onboarding_context()
