"""Shared LangChain tools for all agents.

Subpackages
-----------
``common`` — Reusable helpers (calculator, timezone time, weather API).
``sql`` — DuckDB/SQL execution, Plotly/React chart tools bound to query sessions.
``onboarding`` — Guided connector setup (UI render, Docker/OAuth, Supabase).

Example: ``from ai.tools.common import calculate`` and ``from ai.tools.sql import create_react_chart``.
"""
