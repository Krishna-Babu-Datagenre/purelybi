"""LangChain tools for dashboard CRUD (scoped to authenticated user via context)."""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.tools import tool

from ai.agents.dashboard.context import get_dashboard_tool_context
from ai.tools.sql.charts import (
    clear_last_widget,
    get_last_widget,
    get_session_context,
)

# NOTE: dashboard_service imports are done lazily inside each tool to avoid a
# circular import: fastapi_app.services.dashboard_service → widget_data_service
# → ai.agents.sql.agent → ai.tools.dashboard_tools.

logger = logging.getLogger(__name__)


def _uid() -> str:
    return get_dashboard_tool_context().user_id


def _svc():
    """Lazy accessor for dashboard_service to avoid circular imports at module load."""
    from fastapi_app.services import dashboard_service as _ds

    return _ds


@tool
def dashboard_list_my_dashboards() -> str:
    """List this user's dashboards (id, name, description, tags). Use before creating duplicates."""
    try:
        rows = _svc().list_user_dashboards(_uid())
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
        row = _svc().create_dashboard(_uid(), name=name, description=description or None)
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
        row = _svc().update_dashboard(_uid(), dashboard_id, **kw)
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
        dash = _svc().get_user_dashboard(_uid(), dashboard_id)
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
    title: str = "",
    widget_type: str = "",
    chart_config_json: str = "",
    data_config_json: str = "",
    force: bool = False,
) -> str:
    """Add the most recently generated widget (chart or KPI) to a dashboard.

    By default this tool **reuses the widget cached from the last call** to
    `create_react_chart` or `create_react_kpi` — you do NOT need to resend
    `chart_config_json` or `data_config_json`. Only pass overrides:

    - ``title``: override the cached title (optional).
    - ``widget_type``: override the cached type (rare — only if you know better).
    - ``chart_config_json`` / ``data_config_json``: pass explicit JSON only when
      adding a widget that was NOT produced by the most recent create_react_* call.
    - ``force``: set True to add even if server-side validation flagged the
      widget as empty. Use only as a last resort.

    Returns the created widget row as JSON.
    """
    try:
        session_id = get_session_context()
        cached = get_last_widget(session_id) if session_id else None

        # Resolve chart_config
        chart_config: dict[str, Any] | None = None
        if chart_config_json.strip():
            try:
                chart_config = json.loads(chart_config_json)
            except json.JSONDecodeError as e:
                return json.dumps(
                    {"ok": False, "error": f"Invalid chart_config_json: {e}"}
                )
        elif cached is not None:
            chart_config = cached.chart_config

        if not chart_config:
            return json.dumps(
                {
                    "ok": False,
                    "error": (
                        "No cached widget found and no chart_config_json provided. "
                        "Call create_react_chart or create_react_kpi first."
                    ),
                }
            )

        # Resolve data_config
        data_config: dict[str, Any] | None = None
        if data_config_json.strip():
            try:
                data_config = json.loads(data_config_json)
            except json.JSONDecodeError as e:
                return json.dumps(
                    {"ok": False, "error": f"Invalid data_config_json: {e}"}
                )
        elif cached is not None and not chart_config_json.strip():
            data_config = cached.data_config

        # Resolve widget_type / title
        resolved_type = (widget_type or (cached.chart_type if cached else "")).strip()
        if not resolved_type:
            return json.dumps(
                {
                    "ok": False,
                    "error": "widget_type is required when no cached widget is available.",
                }
            )
        resolved_title = (title or (cached.title if cached else "")).strip()
        if not resolved_title:
            return json.dumps(
                {
                    "ok": False,
                    "error": "title is required when no cached widget is available.",
                }
            )

        # Gatekeep on validation of the cached widget (only when we're reusing it).
        if (
            cached is not None
            and not chart_config_json.strip()
            and not force
            and not cached.validation.get("ok", True)
        ):
            return json.dumps(
                {
                    "ok": False,
                    "error": (
                        "Cached widget failed data validation and was NOT added: "
                        f"{cached.validation.get('reason', 'empty result')}. "
                        "Refine the SQL query and regenerate the widget, or pass force=True to add anyway."
                    ),
                    "validation": cached.validation,
                }
            )

        row = _svc().add_widget_to_dashboard(
            _uid(),
            dashboard_id,
            title=resolved_title,
            widget_type=resolved_type,
            chart_config=chart_config,
            data_config=data_config,
        )

        # Invalidate cache so a future add call can't silently re-add the same widget.
        if cached is not None and session_id and not chart_config_json.strip():
            clear_last_widget(session_id)

        return json.dumps(
            {
                "ok": True,
                "widget": {
                    "id": row.get("id"),
                    "title": row.get("title"),
                    "type": row.get("type"),
                },
            },
            default=str,
        )
    except Exception as e:
        logger.exception("dashboard_add_widget")
        return json.dumps({"ok": False, "error": str(e)})


@tool
def dashboard_delete(dashboard_id: str) -> str:
    """Permanently delete a user-owned dashboard and all its widgets."""
    try:
        ok = _svc().delete_dashboard(_uid(), dashboard_id)
        return json.dumps({"ok": ok})
    except Exception as e:
        logger.exception("dashboard_delete")
        return json.dumps({"ok": False, "error": str(e)})


@tool
def dashboard_remove_widget(dashboard_id: str, widget_id: str) -> str:
    """Remove one widget from a dashboard."""
    try:
        ok = _svc().delete_widget(_uid(), dashboard_id, widget_id)
        return json.dumps({"ok": ok})
    except Exception as e:
        logger.exception("dashboard_remove_widget")
        return json.dumps({"ok": False, "error": str(e)})


@tool
def dashboard_get_widget_detail(dashboard_id: str, widget_id: str) -> str:
    """Return full details (title, type, chart_config, data_config) for a widget.

    Use this before `dashboard_update_widget` so you can see the current
    configuration before deciding what to change.
    """
    try:
        dash = _svc().get_user_dashboard(_uid(), dashboard_id, hydrate=False)
        if dash is None:
            return json.dumps({"error": "Dashboard not found."})
        widgets = dash.get("widgets") or []
        for w in widgets:
            if w.get("id") == widget_id:
                return json.dumps(
                    {
                        "id": w.get("id"),
                        "title": w.get("title"),
                        "type": w.get("type"),
                        "chart_config": w.get("chart_config"),
                        "data_config": w.get("data_config"),
                    },
                    default=str,
                )
        return json.dumps({"error": "Widget not found on this dashboard."})
    except Exception as e:
        logger.exception("dashboard_get_widget_detail")
        return json.dumps({"error": str(e)})


@tool
def dashboard_update_widget(
    dashboard_id: str,
    widget_id: str,
    title: str = "",
    widget_type: str = "",
    chart_config_json: str = "",
    data_config_json: str = "",
    use_cached_widget: bool = False,
) -> str:
    """Update a widget's title, type, and/or chart/data config on a dashboard.

    Provide only the fields you want to change. To replace the visualization
    entirely, first call `create_react_chart` or `create_react_kpi` to build
    the new widget, then pass ``use_cached_widget=True`` here (this uses the
    last cached widget's chart_config/data_config/type/title as the update
    source). Otherwise pass explicit JSON via ``chart_config_json`` /
    ``data_config_json``.

    Returns the updated widget row as JSON, or an error message.
    """
    try:
        chart_config: dict[str, Any] | None = None
        data_config: dict[str, Any] | None = None
        resolved_type = widget_type.strip() or None
        resolved_title = title.strip() or None

        if use_cached_widget:
            session_id = get_session_context()
            cached = get_last_widget(session_id) if session_id else None
            if cached is None:
                return json.dumps(
                    {
                        "ok": False,
                        "error": (
                            "use_cached_widget=True but no cached widget is "
                            "available. Call create_react_chart or "
                            "create_react_kpi first."
                        ),
                    }
                )
            if not cached.validation.get("ok", True):
                return json.dumps(
                    {
                        "ok": False,
                        "error": (
                            "Cached widget failed data validation: "
                            f"{cached.validation.get('reason', 'empty result')}. "
                            "Refine the query and regenerate."
                        ),
                        "validation": cached.validation,
                    }
                )
            chart_config = cached.chart_config
            data_config = cached.data_config
            if resolved_type is None:
                resolved_type = cached.chart_type
            if resolved_title is None:
                resolved_title = cached.title

        if chart_config_json.strip():
            try:
                chart_config = json.loads(chart_config_json)
            except json.JSONDecodeError as e:
                return json.dumps(
                    {"ok": False, "error": f"Invalid chart_config_json: {e}"}
                )
        if data_config_json.strip():
            try:
                data_config = json.loads(data_config_json)
            except json.JSONDecodeError as e:
                return json.dumps(
                    {"ok": False, "error": f"Invalid data_config_json: {e}"}
                )

        if (
            resolved_title is None
            and resolved_type is None
            and chart_config is None
            and data_config is None
        ):
            return json.dumps(
                {
                    "ok": False,
                    "error": (
                        "Nothing to update. Provide title, widget_type, "
                        "chart_config_json / data_config_json, or set "
                        "use_cached_widget=True."
                    ),
                }
            )

        row = _svc().update_widget_in_dashboard(
            _uid(),
            dashboard_id,
            widget_id,
            title=resolved_title,
            widget_type=resolved_type,
            chart_config=chart_config,
            data_config=data_config,
        )
        if row is None:
            return json.dumps(
                {"ok": False, "error": "Dashboard or widget not found."}
            )

        # Invalidate cached widget so a follow-up call can't re-apply stale state.
        if use_cached_widget:
            session_id = get_session_context()
            if session_id:
                clear_last_widget(session_id)

        return json.dumps(
            {
                "ok": True,
                "widget": {
                    "id": row.get("id"),
                    "title": row.get("title"),
                    "type": row.get("type"),
                },
            },
            default=str,
        )
    except Exception as e:
        logger.exception("dashboard_update_widget")
        return json.dumps({"ok": False, "error": str(e)})


ALL_DASHBOARD_TOOLS = [
    dashboard_list_my_dashboards,
    dashboard_create,
    dashboard_update_metadata,
    dashboard_get_summary,
    dashboard_get_widget_detail,
    dashboard_add_widget,
    dashboard_update_widget,
    dashboard_delete,
    dashboard_remove_widget,
]


# Read-only + widget-delete subset used by the analyst agent when a dashboard
# is attached to the chat. The analyst may NOT update widgets in place or add
# widgets — only the user can add widgets via the dashboard UI. "Update"
# flows are expressed as: read widget → delete widget → regenerate widget
# (user adds the new one themselves).
ANALYST_DASHBOARD_TOOLS = [
    dashboard_get_summary,
    dashboard_get_widget_detail,
    dashboard_remove_widget,
]
