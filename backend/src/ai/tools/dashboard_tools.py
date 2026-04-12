"""LangChain tools for dashboard CRUD (scoped to authenticated user via context)."""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.tools import tool

from ai.agents.dashboard.context import get_dashboard_tool_context
from fastapi_app.services.dashboard_service import (
    add_widget_to_dashboard,
    create_dashboard,
    delete_dashboard,
    delete_widget,
    get_user_dashboard,
    list_user_dashboards,
    update_dashboard,
)

logger = logging.getLogger(__name__)


def _uid() -> str:
    return get_dashboard_tool_context().user_id


@tool
def dashboard_list_my_dashboards() -> str:
    """List this user's dashboards (id, name, description, tags). Use before creating duplicates."""
    try:
        rows = list_user_dashboards(_uid())
        slim = [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "description": r.get("description"),
                "tags": r.get("tags"),
            }
            for r in rows
        ]
        return json.dumps({"dashboards": slim}, default=str)
    except Exception as e:
        logger.exception("dashboard_list_my_dashboards")
        return json.dumps({"error": str(e)})


@tool
def dashboard_create(name: str, description: str = "") -> str:
    """Create a new empty dashboard. Returns id and metadata as JSON."""
    try:
        row = create_dashboard(_uid(), name=name, description=description or None)
        return json.dumps(
            {
                "ok": True,
                "dashboard_id": row.get("id"),
                "name": row.get("name"),
                "description": row.get("description"),
            },
            default=str,
        )
    except Exception as e:
        logger.exception("dashboard_create")
        return json.dumps({"ok": False, "error": str(e)})


@tool
def dashboard_update_metadata(
    dashboard_id: str,
    name: str = "",
    description: str = "",
) -> str:
    """Rename or update description for a user-owned dashboard. Pass only fields to change."""
    try:
        kw: dict[str, Any] = {}
        if name.strip():
            kw["name"] = name.strip()
        if description is not None:
            kw["description"] = description
        if not kw:
            return json.dumps(
                {"ok": False, "error": "Provide name and/or description to update."}
            )
        row = update_dashboard(_uid(), dashboard_id, **kw)
        if row is None:
            return json.dumps({"ok": False, "error": "Dashboard not found."})
        return json.dumps({"ok": True, "dashboard": row}, default=str)
    except Exception as e:
        logger.exception("dashboard_update_metadata")
        return json.dumps({"ok": False, "error": str(e)})


@tool
def dashboard_get_summary(dashboard_id: str) -> str:
    """Return widget titles, types, and ids for a dashboard (for planning edits)."""
    try:
        dash = get_user_dashboard(_uid(), dashboard_id)
        if dash is None:
            return json.dumps({"error": "Dashboard not found."})
        widgets = dash.get("widgets") or []
        summary = [
            {
                "id": w.get("id"),
                "title": w.get("title"),
                "type": w.get("type"),
            }
            for w in widgets
        ]
        return json.dumps(
            {
                "dashboard_id": dash.get("id"),
                "name": dash.get("name"),
                "widgets": summary,
            },
            default=str,
        )
    except Exception as e:
        logger.exception("dashboard_get_summary")
        return json.dumps({"error": str(e)})


@tool
def dashboard_add_widget(
    dashboard_id: str,
    title: str,
    widget_type: str,
    chart_config_json: str,
    data_config_json: str = "{}",
) -> str:
    """Add a chart or KPI widget. chart_config_json: ECharts option or KPI config. data_config_json: optional SQL hydration config as JSON object."""
    try:
        chart_config = json.loads(chart_config_json)
        data_config = json.loads(data_config_json) if data_config_json.strip() else None
        row = add_widget_to_dashboard(
            _uid(),
            dashboard_id,
            title=title,
            widget_type=widget_type,
            chart_config=chart_config,
            data_config=data_config,
        )
        return json.dumps({"ok": True, "widget": row}, default=str)
    except json.JSONDecodeError as e:
        return json.dumps({"ok": False, "error": f"Invalid JSON: {e}"})
    except Exception as e:
        logger.exception("dashboard_add_widget")
        return json.dumps({"ok": False, "error": str(e)})


@tool
def dashboard_delete(dashboard_id: str) -> str:
    """Permanently delete a user-owned dashboard and all its widgets."""
    try:
        ok = delete_dashboard(_uid(), dashboard_id)
        return json.dumps({"ok": ok})
    except Exception as e:
        logger.exception("dashboard_delete")
        return json.dumps({"ok": False, "error": str(e)})


@tool
def dashboard_remove_widget(dashboard_id: str, widget_id: str) -> str:
    """Remove one widget from a dashboard."""
    try:
        ok = delete_widget(_uid(), dashboard_id, widget_id)
        return json.dumps({"ok": ok})
    except Exception as e:
        logger.exception("dashboard_remove_widget")
        return json.dumps({"ok": False, "error": str(e)})


ALL_DASHBOARD_TOOLS = [
    dashboard_list_my_dashboards,
    dashboard_create,
    dashboard_update_metadata,
    dashboard_get_summary,
    dashboard_add_widget,
    dashboard_delete,
    dashboard_remove_widget,
]
