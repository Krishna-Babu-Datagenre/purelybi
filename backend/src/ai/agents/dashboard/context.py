"""Request-scoped context for dashboard builder tools (user id from JWT)."""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass


@dataclass(frozen=True)
class DashboardToolContext:
    user_id: str


_ctx: ContextVar[DashboardToolContext | None] = ContextVar(
    "dashboard_tool_ctx", default=None
)


def set_dashboard_tool_context(user_id: str) -> Token:
    return _ctx.set(DashboardToolContext(user_id=user_id))


def reset_dashboard_tool_context(token: Token) -> None:
    _ctx.reset(token)


def get_dashboard_tool_context() -> DashboardToolContext:
    c = _ctx.get()
    if c is None:
        raise RuntimeError("Dashboard tool context is not set")
    return c
