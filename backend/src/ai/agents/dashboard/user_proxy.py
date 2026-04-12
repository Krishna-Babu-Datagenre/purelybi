"""
User Proxy AI — Magic Mode only.

When the dashboard builder pauses for confirmation, a lightweight LLM call
decides whether to inject a synthetic user reply so the run can continue
without manual input.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from ai.llms import get_user_proxy_llm

logger = logging.getLogger(__name__)

USER_PROXY_SYSTEM_PROMPT = """\
You are a **user representative** for Magic Mode dashboard building. You speak and decide **on behalf of the real user** who is not available to type.

## Your job
- Read the assistant’s latest message and the conversation context.
- Decide whether the dashboard-building run should **continue** with an automatic user reply, or **stop** because the task is complete.
- When continuing, output a **short, decisive user message** (1–4 sentences) that unblocks the assistant: confirmations, approvals, or concrete choices—aligned with the user’s stated goal.

## Magic Mode objective
The BI assistant must autonomously build a useful dashboard from the tenant’s synced data (DuckDB views over Parquet): explore tables, run SQL, create KPI/chart widgets (at most 4 KPIs and 6 charts), save them to a named dashboard, then summarize. Speed and minimal friction matter more than exhaustive explanations.

## What the assistant can do (high level)
- Inspect schema and run read-only SQL (`sql_db_list_tables`, `sql_db_schema`, `sql_db_query`, checker tools).
- Create visuals (`create_react_chart`, `create_react_kpi`).
- Persist dashboards (`dashboard_create`, `dashboard_add_widget`, and related dashboard tools).

## Rules
- **Prioritize progress**: Prefer choices that use **available data now** over asking the human to sync more systems, unless syncing is clearly required to meet the user’s goal.
- **No meta commentary**: Your user_message must sound like the real user—no “As the proxy…” or system explanations.
- **Stop when done**: If the assistant has already created the dashboard with widgets and is wrapping up, set continue_automation to false.
- **Stop if blocked without a path**: If the assistant only asks for credentials or external actions the user cannot do inside this product, set continue_automation to false.
- **JSON only**: Reply with a single JSON object exactly in the schema below—no markdown fences, no extra text.

## Output schema
{"continue_automation": boolean, "user_message": string}

- If continue_automation is true, user_message must be non-empty.
- If continue_automation is false, user_message must be an empty string.
"""


@dataclass(frozen=True)
class UserProxyDecision:
    continue_automation: bool
    user_message: str


def _truncate(s: str, max_len: int = 1200) -> str:
    s = s.strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _format_thread_for_proxy(messages: list[Any]) -> str:
    """Compact transcript for the proxy (human + AI text; tools summarized)."""
    lines: list[str] = []
    for msg in messages[-24:]:
        role = getattr(msg, "type", None) or ""
        if role == "human":
            lines.append(f"User: {_truncate(_stringify_content(getattr(msg, 'content', '')))}")
        elif role == "ai":
            text = _stringify_content(getattr(msg, "content", ""))
            if text:
                lines.append(f"Assistant: {_truncate(text)}")
        elif role == "tool":
            name = getattr(msg, "name", "tool") or "tool"
            body = _stringify_content(getattr(msg, "content", ""))
            lines.append(f"Tool [{name}]: {_truncate(body, 400)}")
    return "\n".join(lines) if lines else "(no messages)"


def _stringify_content(content: Any) -> str:
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


def _parse_proxy_json(text: str) -> dict[str, Any] | None:
    raw = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
    if fence:
        raw = fence.group(1).strip()
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", raw)
    if m:
        try:
            obj = json.loads(m.group(0))
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
    return None


async def run_user_proxy_decision(
    *,
    magic_dashboard_name: str | None,
    magic_goal: str | None,
    selected_datasets: list[str] | None,
    thread_messages: list[Any],
) -> UserProxyDecision:
    """
    Decide whether to continue Magic Mode with a synthetic user message.

    thread_messages: LangChain messages from agent state (same session).
    """
    transcript = _format_thread_for_proxy(thread_messages)
    scope = (
        "all available datasets"
        if not selected_datasets
        else ", ".join(selected_datasets)
    )
    meta_lines = [
        "## User intent (fixed)",
        f"- Dashboard name: {magic_dashboard_name or '(not specified)'}",
        f"- Dataset scope: {scope}",
        f"- High-level goal: {magic_goal or '(not specified)'}",
        "",
        "## Conversation",
        transcript,
    ]
    human_block = "\n".join(meta_lines)

    llm = get_user_proxy_llm()
    try:
        resp = await llm.ainvoke(
            [
                SystemMessage(content=USER_PROXY_SYSTEM_PROMPT),
                HumanMessage(content=human_block),
            ]
        )
        text = _stringify_content(getattr(resp, "content", "") or "")
        parsed = _parse_proxy_json(text)
        if not parsed:
            logger.warning("User proxy returned unparseable JSON: %s", text[:500])
            return UserProxyDecision(continue_automation=False, user_message="")

        cont = bool(parsed.get("continue_automation"))
        um = str(parsed.get("user_message") or "").strip()
        if cont and not um:
            logger.warning("User proxy wanted to continue but gave empty message.")
            return UserProxyDecision(continue_automation=False, user_message="")
        if not cont:
            return UserProxyDecision(continue_automation=False, user_message="")
        return UserProxyDecision(continue_automation=True, user_message=um)
    except Exception:
        logger.exception("User proxy LLM call failed; stopping Magic automation.")
        return UserProxyDecision(continue_automation=False, user_message="")

