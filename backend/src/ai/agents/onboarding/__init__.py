"""Guided data-source onboarding agent (LangChain + SSE)."""

__all__ = ["create_onboarding_agent"]


def __getattr__(name: str):
    if name == "create_onboarding_agent":
        from ai.agents.onboarding.agent import create_onboarding_agent

        return create_onboarding_agent
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
