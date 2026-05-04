"""Dashboard builder agent package.

Re-exports ``DashboardBuilderAgent`` lazily so that importing sub-modules
(e.g. ``ai.agents.dashboard.context``) does NOT trigger a circular import
through ``ai.tools.dashboard_tools``.
"""

from __future__ import annotations


def __getattr__(name: str):
    if name == "DashboardBuilderAgent":
        from ai.agents.dashboard.agent import DashboardBuilderAgent

        return DashboardBuilderAgent
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["DashboardBuilderAgent"]
