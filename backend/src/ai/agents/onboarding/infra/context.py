"""Per-request onboarding context (user id, connector catalog row, OAuth redirect)."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

_ctx: ContextVar["OnboardingContext | None"] = ContextVar(
    "onboarding_context", default=None
)


@dataclass
class OnboardingContext:
    user_id: str
    thread_id: str
    oauth_redirect_uri: str
    frontend_base: str
    catalog: dict[str, Any]


def set_onboarding_context(ctx: OnboardingContext) -> None:
    _ctx.set(ctx)


def get_onboarding_context() -> OnboardingContext | None:
    return _ctx.get()


def clear_onboarding_context() -> None:
    _ctx.set(None)
