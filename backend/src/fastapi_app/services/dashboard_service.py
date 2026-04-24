"""
Dashboard service – user-owned dashboard CRUD.

Handles creating dashboards (from template or blank), adding widgets,
listing dashboards, and fetching a single dashboard with widgets.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status

from fastapi_app.services.template_service import (
    get_template_by_id,
    get_template_by_slug,
)
from fastapi_app.services.widget_data_service import hydrate_widgets
from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)


def _load_relationships_for_tenant(
    tenant_id: str | None,
) -> list[dict[str, Any]] | None:
    """Load tenant relationship edges once per request for filter BFS.

    Returns ``None`` when no tenant is provided or metadata is unavailable,
    which the filter engine treats as an empty graph (direct-only filters).
    """
    if not tenant_id:
        return None
    try:
        from fastapi_app.services import metadata_service  # local import (cycle safety)
        rows = metadata_service.list_relationships(user_id=tenant_id)
    except Exception:
        logger.exception("Failed to load tenant relationships for %s", tenant_id)
        return None
    return [
        {
            "from_table": r.from_table,
            "from_column": r.from_column,
            "to_table": r.to_table,
            "to_column": r.to_column,
            "kind": r.kind.value if hasattr(r.kind, "value") else str(r.kind),
        }
        for r in rows
    ]


def _widgets_for_hydration_from_template(
    template_widgets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Shape ``widget_templates`` rows like ``widgets`` for hydration."""
    out: list[dict[str, Any]] = []
    for wt in template_widgets:
        out.append(
            {
                "id": wt.get("id"),
                "title": wt.get("title"),
                "type": wt.get("type"),
                "layout": wt.get("layout") or {},
                "chart_config": wt.get("chart_config") or {},
                "data_config": wt.get("data_config"),
                "sort_order": wt.get("sort_order", 0),
            }
        )
    return out


def _rects_overlap(a: dict[str, int], b: dict[str, int]) -> bool:
    return (
        a["x"] < b["x"] + b["w"]
        and a["x"] + a["w"] > b["x"]
        and a["y"] < b["y"] + b["h"]
        and a["y"] + a["h"] > b["y"]
    )


def _can_place(
    candidate: dict[str, int], occupied: list[dict[str, int]], cols: int = 12
) -> bool:
    if candidate["x"] < 0 or candidate["y"] < 0:
        return False
    if candidate["w"] < 1 or candidate["h"] < 1:
        return False
    if candidate["x"] + candidate["w"] > cols:
        return False
    return not any(_rects_overlap(candidate, r) for r in occupied)


MAX_KPIS_PER_ROW = 4
KPI_HEIGHT = 2
_SECTION_GAP = 1


def _kpis_per_row(count: int, cols: int) -> int:
    cap = min(count, MAX_KPIS_PER_ROW)
    for n in range(cap, 0, -1):
        if cols % n == 0 and cols // n >= 2:
            return n
    return min(cap, max(1, cols // 2))


def _split_kpi_rows(count: int, cols: int) -> list[int]:
    """Prefer even distribution; fall back to filling rows from the top."""
    if count == 0:
        return []
    max_per_row = min(count, MAX_KPIS_PER_ROW)
    min_rows = -(-count // max_per_row)  # ceil division

    for per_row in range(max_per_row, 0, -1):
        if count % per_row != 0:
            continue
        if cols % per_row != 0:
            continue
        if cols // per_row < 2:
            continue
        num_rows = count // per_row
        if num_rows > min_rows:
            continue
        return [per_row] * num_rows

    per_row = _kpis_per_row(count, cols)
    rows: list[int] = []
    remaining = count
    while remaining > 0:
        rows.append(min(remaining, per_row))
        remaining -= per_row
    return rows


def _compute_kpi_layouts(
    kpi_ids: list[str], cols: int = 12
) -> dict[str, dict[str, int]]:
    """Return balanced {id: {x, y, w, h}} for a list of KPI IDs."""
    result: dict[str, dict[str, int]] = {}
    if not kpi_ids:
        return result
    rows = _split_kpi_rows(len(kpi_ids), cols)
    kpi_index = 0
    current_y = 0
    for row_count in rows:
        item_width = cols // row_count
        for col in range(row_count):
            kid = kpi_ids[kpi_index]
            result[kid] = {
                "x": col * item_width,
                "y": current_y,
                "w": item_width,
                "h": KPI_HEIGHT,
            }
            kpi_index += 1
        current_y += KPI_HEIGHT
    return result


def _is_auto_balanced_kpi_layout(
    kpi_widgets: list[dict[str, Any]], cols: int = 12
) -> bool:
    """True when stored KPI layouts match what auto-balance would produce."""
    if not kpi_widgets:
        return True
    with_layouts = [
        k
        for k in kpi_widgets
        if isinstance(k.get("layout"), dict) and k["layout"]
    ]
    if len(with_layouts) != len(kpi_widgets):
        return True  # missing layouts → treat as auto-balanced

    sorted_kpis = sorted(
        with_layouts,
        key=lambda k: (
            int(k["layout"].get("y", 0)),
            int(k["layout"].get("x", 0)),
        ),
    )
    expected = _compute_kpi_layouts(
        [str(i) for i in range(len(sorted_kpis))], cols
    )
    expected_arr = list(expected.values())
    for i, kpi in enumerate(sorted_kpis):
        actual = kpi["layout"]
        exp = expected_arr[i]
        if (
            int(actual.get("x", 0)) != exp["x"]
            or int(actual.get("y", 0)) != exp["y"]
            or int(actual.get("w", 1)) != exp["w"]
            or int(actual.get("h", 1)) != exp["h"]
        ):
            return False
    return True


def _compute_default_new_widget_layout(
    existing_widgets: list[dict[str, Any]], widget_type: str
) -> dict[str, int]:
    occupied: list[dict[str, int]] = []
    for row in existing_widgets:
        layout = row.get("layout")
        if not isinstance(layout, dict):
            continue
        try:
            occupied.append(
                {
                    "x": int(layout.get("x", 0)),
                    "y": int(layout.get("y", 0)),
                    "w": int(layout.get("w", 1)),
                    "h": int(layout.get("h", 1)),
                }
            )
        except (TypeError, ValueError):
            continue

    max_bottom = max((r["y"] + r["h"] for r in occupied), default=0)

    if widget_type == "kpi":
        kpi_layouts = []
        for w in existing_widgets:
            if w.get("type") != "kpi":
                continue
            lo = w.get("layout")
            if isinstance(lo, dict):
                try:
                    kpi_layouts.append(
                        {"x": int(lo.get("x", 0)), "y": int(lo.get("y", 0)),
                         "w": int(lo.get("w", 1)), "h": int(lo.get("h", 1))}
                    )
                except (TypeError, ValueError):
                    pass
        kpi_bottom = max((r["y"] + r["h"] for r in kpi_layouts), default=0)
        num_kpi_rows = max(1, -(-kpi_bottom // 2))  # ceil division by KPI_H=2
        for scan_row in range(num_kpi_rows + 1):
            for scan_col in range(4):
                candidate = {"x": scan_col * 3, "y": scan_row * 2, "w": 3, "h": 2}
                if _can_place(candidate, occupied):
                    return candidate
        return {"x": 0, "y": max_bottom, "w": 3, "h": 2}

    chart_layouts: list[dict[str, int]] = []
    for row in existing_widgets:
        if row.get("type") == "kpi":
            continue
        layout = row.get("layout")
        if not isinstance(layout, dict):
            continue
        try:
            chart_layouts.append(
                {
                    "x": int(layout.get("x", 0)),
                    "y": int(layout.get("y", 0)),
                    "w": int(layout.get("w", 1)),
                    "h": int(layout.get("h", 1)),
                }
            )
        except (TypeError, ValueError):
            continue
    if chart_layouts:
        last = sorted(chart_layouts, key=lambda l: (l["y"], l["x"]))[-1]
        side_by_side = {"x": 6, "y": last["y"], "w": 6, "h": 8}
        if last["x"] < 6 and _can_place(side_by_side, occupied):
            return side_by_side

    return {"x": 0, "y": max_bottom, "w": 6, "h": 8}


def build_live_template_dashboard(
    tmpl: dict[str, Any],
    *,
    tenant_id: str | None = None,
    filters: list[dict[str, Any]] | None = None,
    force_refresh: bool = False,
    filters_from_preset: str | None = None,
    filter_spec: Any | None = None,
    relationships: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Hydrate a dashboard template for direct viewing (no ``widgets`` table rows)."""
    widgets_in = _widgets_for_hydration_from_template(tmpl.get("widgets") or [])
    hydrated = hydrate_widgets(
        widgets_in,
        tenant_id=tenant_id,
        force_refresh=force_refresh,
        filters=filters,
        filters_from_preset=filters_from_preset,
        filter_spec=filter_spec,
        relationships=relationships,
    )
    return {
        "id": tmpl["id"],
        "slug": tmpl.get("slug"),
        "name": tmpl["name"],
        "description": tmpl.get("description") or "",
        "tags": tmpl.get("tags") or [],
        "platforms": tmpl.get("platforms") or [],
        "source": "template",
        "template_id": tmpl["id"],
        "user_id": None,
        "connection_id": None,
        "preview_image": tmpl.get("preview_image"),
        "created_at": tmpl.get("created_at"),
        "updated_at": tmpl.get("updated_at"),
        "widgets": hydrated,
        "live_from_template": True,
    }


# ---------------------------------------------------------------------------
# Create a blank user dashboard
# ---------------------------------------------------------------------------


def update_dashboard(
    user_id: str,
    dashboard_id: str,
    *,
    name: str | None = None,
    description: str | None = None,
) -> dict[str, Any] | None:
    """Update name and/or description for a user-owned dashboard. Returns row or None."""
    if name is None and description is None:
        return None
    client = get_supabase_admin_client()
    rows = (
        client.table("dashboards")
        .select("id")
        .eq("id", dashboard_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data
    if not rows:
        return None
    payload: dict[str, Any] = {}
    if name is not None:
        payload["name"] = name
    if description is not None:
        payload["description"] = description
    if not payload:
        return None
    updated = (
        client.table("dashboards")
        .update(payload)
        .eq("id", dashboard_id)
        .eq("user_id", user_id)
        .execute()
    ).data
    return updated[0] if updated else None


def create_dashboard(
    user_id: str,
    name: str,
    description: str | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Create a new blank (manual) dashboard for the user."""
    client = get_supabase_admin_client()

    dashboard_row = (
        client.table("dashboards")
        .insert(
            {
                "user_id": user_id,
                "name": name,
                "description": description or "",
                "tags": tags or [],
                "source": "manual",
            }
        )
        .execute()
    ).data[0]

    dashboard_row["widgets"] = []
    return dashboard_row


# ---------------------------------------------------------------------------
# Add a widget to an existing dashboard
# ---------------------------------------------------------------------------


def add_widget_to_dashboard(
    user_id: str,
    dashboard_id: str,
    title: str,
    widget_type: str,
    chart_config: dict[str, Any],
    layout: dict[str, Any] | None = None,
    data_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert a new widget into a user-owned dashboard.

    Returns the created widget row.
    """
    client = get_supabase_admin_client()

    # Verify ownership
    rows = (
        client.table("dashboards")
        .select("id")
        .eq("id", dashboard_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )

    # Determine sort_order and read existing widgets for default placement.
    existing_widgets = (
        client.table("widgets")
        .select("id, type, layout, sort_order")
        .eq("dashboard_id", dashboard_id)
        .order("sort_order")
        .execute()
    ).data or []
    last_rows = sorted(
        existing_widgets,
        key=lambda r: r.get("sort_order", 0),
        reverse=True,
    )
    next_sort = (last_rows[0]["sort_order"] + 1) if last_rows else 0

    # Default layout if not provided: append to end without overlapping existing.
    rebalance_updates: list[dict[str, Any]] = []

    if layout is None:
        if widget_type == "kpi":
            existing_kpis = [
                w for w in existing_widgets if w.get("type") == "kpi"
            ]
            if _is_auto_balanced_kpi_layout(existing_kpis):
                kpi_ids = [k["id"] for k in existing_kpis] + ["__pending__"]
                balanced = _compute_kpi_layouts(kpi_ids)
                layout = balanced.get(
                    "__pending__", {"x": 0, "y": 0, "w": 12, "h": KPI_HEIGHT}
                )

                # Rebalance existing KPIs
                for kpi in existing_kpis:
                    if kpi["id"] in balanced:
                        rebalance_updates.append(
                            {"id": kpi["id"], "layout": balanced[kpi["id"]]}
                        )

                # Shift charts down if KPI section grew
                old_kpi_bottom = max(
                    (
                        int(k["layout"].get("y", 0))
                        + int(k["layout"].get("h", KPI_HEIGHT))
                        for k in existing_kpis
                        if isinstance(k.get("layout"), dict)
                    ),
                    default=0,
                )
                new_kpi_bottom = max(
                    (r["y"] + r["h"] for r in balanced.values()), default=0
                )
                had_kpis = len(existing_kpis) > 0
                y_shift = (
                    (new_kpi_bottom - old_kpi_bottom)
                    if had_kpis
                    else (new_kpi_bottom + _SECTION_GAP)
                )

                if y_shift > 0:
                    for w in existing_widgets:
                        if w.get("type") == "kpi":
                            continue
                        wl = w.get("layout")
                        if not isinstance(wl, dict):
                            continue
                        rebalance_updates.append(
                            {
                                "id": w["id"],
                                "layout": {
                                    "x": int(wl.get("x", 0)),
                                    "y": int(wl.get("y", 0)) + y_shift,
                                    "w": int(wl.get("w", 1)),
                                    "h": int(wl.get("h", 1)),
                                },
                            }
                        )
            else:
                layout = _compute_default_new_widget_layout(
                    existing_widgets, widget_type
                )
        else:
            layout = _compute_default_new_widget_layout(
                existing_widgets, widget_type
            )

    widget_row = (
        client.table("widgets")
        .insert(
            {
                "dashboard_id": dashboard_id,
                "title": title,
                "type": widget_type,
                "layout": layout,
                "chart_config": chart_config,
                "data_config": data_config,
                "sort_order": next_sort,
            }
        )
        .execute()
    ).data[0]

    # Apply rebalance updates for existing widgets
    for update in rebalance_updates:
        try:
            (
                client.table("widgets")
                .update({"layout": update["layout"]})
                .eq("id", update["id"])
                .eq("dashboard_id", dashboard_id)
                .execute()
            )
        except Exception:
            logger.warning(
                "Failed to rebalance widget %s", update["id"], exc_info=True
            )

    return widget_row


# ---------------------------------------------------------------------------
# Instantiate a template → user-owned dashboard
# ---------------------------------------------------------------------------


def instantiate_template(
    user_id: str,
    template_slug: str,
) -> dict[str, Any]:
    """Return a live, hydrated view of a dashboard template.

    Does **not** insert rows into ``dashboards`` or ``widgets``. Template
    updates in Supabase are always reflected immediately.

    Widget data is loaded from blob storage for *user_id* (tenant).
    """
    template = get_template_by_slug(template_slug)
    if not template:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Template '{template_slug}' not found.",
        )
    return build_live_template_dashboard(template, tenant_id=user_id)


def get_live_template_by_slug(
    template_slug: str,
    *,
    tenant_id: str,
    filters: list[dict[str, Any]] | None = None,
    force_refresh: bool = False,
    filters_from_preset: str | None = None,
) -> dict[str, Any]:
    """Load and hydrate a template by slug (public helper for routes)."""
    template = get_template_by_slug(template_slug)
    if not template:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Template '{template_slug}' not found.",
        )
    return build_live_template_dashboard(
        template,
        tenant_id=tenant_id,
        filters=filters,
        force_refresh=force_refresh,
        filters_from_preset=filters_from_preset,
    )


# ---------------------------------------------------------------------------
# Duplicate a dashboard
# ---------------------------------------------------------------------------


def duplicate_dashboard(
    user_id: str,
    dashboard_id: str,
    new_name: str | None = None,
) -> dict[str, Any]:
    """Create a copy of a dashboard (user-owned or template) under the user's profile.

    All widgets are deep-copied to the new dashboard. The caller can optionally
    specify *new_name*; otherwise a ``" (Copy)"`` suffix is appended.

    Returns the newly created dashboard dict (with ``widgets`` populated).
    """
    client = get_supabase_admin_client()

    # ── Resolve source dashboard + widgets ──────────────────────────────
    source_dash: dict[str, Any] | None = None
    source_widgets: list[dict[str, Any]] = []

    # Try user-owned dashboard first
    rows = (
        client.table("dashboards")
        .select("*")
        .eq("id", dashboard_id)
        .limit(1)
        .execute()
    ).data
    if rows:
        source_dash = rows[0]
        source_widgets = (
            client.table("widgets")
            .select("*")
            .eq("dashboard_id", dashboard_id)
            .order("sort_order")
            .execute()
        ).data or []
    else:
        # Fall back to template
        tmpl = get_template_by_id(dashboard_id)
        if tmpl:
            source_dash = {
                "name": tmpl["name"],
                "description": tmpl.get("description") or "",
                "tags": tmpl.get("tags") or [],
            }
            source_widgets = _widgets_for_hydration_from_template(
                tmpl.get("widgets") or []
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dashboard '{dashboard_id}' not found.",
            )

    # ── Create new dashboard row ────────────────────────────────────────
    original_name = source_dash.get("name", "Dashboard")
    name = new_name or f"{original_name} (Copy)"

    new_dash = (
        client.table("dashboards")
        .insert(
            {
                "user_id": user_id,
                "name": name,
                "description": source_dash.get("description", ""),
                "tags": source_dash.get("tags") or [],
                "source": "manual",
            }
        )
        .execute()
    ).data[0]

    new_dashboard_id = new_dash["id"]

    # ── Copy widgets ────────────────────────────────────────────────────
    copied_widgets: list[dict[str, Any]] = []
    for idx, w in enumerate(source_widgets):
        widget_row = (
            client.table("widgets")
            .insert(
                {
                    "dashboard_id": new_dashboard_id,
                    "title": w.get("title", ""),
                    "type": w.get("type", "chart"),
                    "layout": w.get("layout") or {},
                    "chart_config": w.get("chart_config") or {},
                    "data_config": w.get("data_config"),
                    "sort_order": w.get("sort_order", idx),
                }
            )
            .execute()
        ).data[0]
        copied_widgets.append(widget_row)

    new_dash["widgets"] = copied_widgets
    return new_dash


# ---------------------------------------------------------------------------
# List user dashboards
# ---------------------------------------------------------------------------


def list_user_dashboards(user_id: str) -> list[dict[str, Any]]:
    """Return all dashboards owned by the given user (metadata only)."""
    client = get_supabase_admin_client()
    rows = (
        client.table("dashboards")
        .select(
            "id, name, description, tags, source, template_id, "
            "connection_id, created_at, updated_at"
        )
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    ).data or []
    # Legacy: hide any old per-user copies of built-in templates (migration 005).
    return [r for r in rows if r.get("source") != "template"]


def get_dashboard_builder_readiness(user_id: str) -> dict[str, Any]:
    """UI state for AI dashboard builder: sync status, dataset view names, messaging."""
    from fastapi_app.services.connector_service import list_user_connectors
    from ai.agents.sql.duckdb_sandbox import list_tenant_dataset_view_names

    connectors = list_user_connectors(user_id)
    datasets = list_tenant_dataset_view_names(user_id)
    has_data = len(datasets) > 0
    has_connector = len(connectors) > 0

    if has_data:
        status = "ready"
        message = "Your synced data is ready. You can generate dashboards with AI."
    elif has_connector:
        status = "waiting_sync"
        message = (
            "A data source is connected. Wait for the first sync to finish, "
            "then come back here or refresh."
        )
    else:
        status = "no_connector"
        message = "Connect a data source under Data to enable AI-generated dashboards."

    return {
        "status": status,
        "message": message,
        "datasets": datasets,
        "has_connector": has_connector,
        "has_synced_data": has_data,
    }


# ---------------------------------------------------------------------------
# Get a single dashboard with widgets
# ---------------------------------------------------------------------------


def get_user_dashboard(
    user_id: str,
    dashboard_id: str,
    filters: list[dict[str, Any]] | None = None,
    filters_from_preset: str | None = None,
    hydrate: bool = True,
    filter_spec: Any | None = None,
) -> dict[str, Any] | None:
    """Return a dashboard with its widgets, or ``None`` if not found / not owned.

    When *hydrate* is ``False``, widgets are returned as stored in the database
    (``chart_config`` from DB, no DuckDB queries). This is instant and useful
    for a first render; the caller can request ``hydrate=True`` in a follow-up
    to get live data.

    *filter_spec* (native dashboard filtering, Group D) is applied to every
    widget via the filter engine. Relationships are loaded once per request
    so the filter engine can reroute filters through foreign keys.
    """
    client = get_supabase_admin_client()

    rows = (
        client.table("dashboards")
        .select("*")
        .eq("id", dashboard_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data

    if not rows:
        # Resolve built-in template UUID → live hydrated template (no user row).
        tmpl = get_template_by_id(dashboard_id)
        if tmpl:
            relationships = (
                _load_relationships_for_tenant(user_id) if hydrate else None
            )
            return build_live_template_dashboard(
                tmpl,
                tenant_id=user_id,
                filters=filters if hydrate else None,
                filters_from_preset=filters_from_preset if hydrate else None,
                filter_spec=filter_spec if hydrate else None,
                relationships=relationships,
            )
        return None

    dashboard = rows[0]

    widgets = (
        client.table("widgets")
        .select("*")
        .eq("dashboard_id", dashboard_id)
        .order("sort_order")
        .execute()
    ).data or []

    if not hydrate:
        dashboard["widgets"] = widgets
        return dashboard

    # Hydrate widgets with live DuckDB data
    relationships = _load_relationships_for_tenant(user_id)
    hydrated = hydrate_widgets(
        widgets,
        tenant_id=user_id,
        filters=filters,
        filters_from_preset=filters_from_preset,
        filter_spec=filter_spec,
        relationships=relationships,
    )
    dashboard["widgets"] = hydrated
    return dashboard


# ---------------------------------------------------------------------------
# Refresh dashboard data
# ---------------------------------------------------------------------------


def refresh_dashboard(
    user_id: str,
    dashboard_id: str,
    filters: list[dict[str, Any]] | None = None,
    filters_from_preset: str | None = None,
    filter_spec: Any | None = None,
) -> dict[str, Any] | None:
    """Force-refresh all widget data for a dashboard, ignoring cache."""
    client = get_supabase_admin_client()

    rows = (
        client.table("dashboards")
        .select("*")
        .eq("id", dashboard_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data

    if not rows:
        tmpl = get_template_by_id(dashboard_id)
        if tmpl:
            relationships = _load_relationships_for_tenant(user_id)
            return build_live_template_dashboard(
                tmpl,
                tenant_id=user_id,
                filters=filters,
                force_refresh=True,
                filters_from_preset=filters_from_preset,
                filter_spec=filter_spec,
                relationships=relationships,
            )
        return None

    dashboard = rows[0]

    widgets = (
        client.table("widgets")
        .select("*")
        .eq("dashboard_id", dashboard_id)
        .order("sort_order")
        .execute()
    ).data or []

    relationships = _load_relationships_for_tenant(user_id)
    hydrated = hydrate_widgets(
        widgets,
        tenant_id=user_id,
        force_refresh=True,
        filters=filters,
        filters_from_preset=filters_from_preset,
        filter_spec=filter_spec,
        relationships=relationships,
    )
    dashboard["widgets"] = hydrated
    return dashboard


# ---------------------------------------------------------------------------
# Delete a dashboard
# ---------------------------------------------------------------------------


def delete_dashboard(user_id: str, dashboard_id: str) -> bool:
    """Delete a user-owned dashboard and all its widgets.

    Returns True if deleted, False if not found / not owned.
    Widgets are cascade-deleted by the DB foreign key.
    """
    client = get_supabase_admin_client()

    rows = (
        client.table("dashboards")
        .select("id")
        .eq("id", dashboard_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data

    if not rows:
        return False

    client.table("dashboards").delete().eq("id", dashboard_id).execute()
    return True


# ---------------------------------------------------------------------------
# Delete a widget
# ---------------------------------------------------------------------------


def delete_widget(user_id: str, dashboard_id: str, widget_id: str) -> bool:
    """Delete a single widget from a user-owned dashboard.

    Returns True if deleted, False if not found / not owned.
    """
    client = get_supabase_admin_client()

    # Verify dashboard ownership
    dash_rows = (
        client.table("dashboards")
        .select("id")
        .eq("id", dashboard_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data

    if not dash_rows:
        return False

    # Verify widget belongs to this dashboard
    widget_rows = (
        client.table("widgets")
        .select("id")
        .eq("id", widget_id)
        .eq("dashboard_id", dashboard_id)
        .limit(1)
        .execute()
    ).data

    if not widget_rows:
        return False

    client.table("widgets").delete().eq("id", widget_id).execute()
    return True


# ---------------------------------------------------------------------------
# Persist widget layouts
# ---------------------------------------------------------------------------


def persist_widget_layouts(
    user_id: str,
    dashboard_id: str,
    layouts: list[dict[str, Any]],
) -> bool:
    """Persist widget layout changes for a user-owned dashboard.

    Returns ``True`` when ownership is valid and updates were applied,
    ``False`` when dashboard does not exist or is not owned by the user.
    """
    client = get_supabase_admin_client()

    # Verify dashboard ownership
    dash_rows = (
        client.table("dashboards")
        .select("id")
        .eq("id", dashboard_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data
    if not dash_rows:
        return False

    if not layouts:
        return True

    # Validate all target widgets belong to this dashboard before updating.
    layout_ids = [str(item.get("id", "")) for item in layouts if item.get("id")]
    if not layout_ids:
        return True

    widget_rows = (
        client.table("widgets")
        .select("id")
        .eq("dashboard_id", dashboard_id)
        .in_("id", layout_ids)
        .execute()
    ).data or []
    found_ids = {row.get("id") for row in widget_rows}
    missing = [wid for wid in layout_ids if wid not in found_ids]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "One or more widgets were not found on this dashboard: "
                + ", ".join(missing)
            ),
        )

    for item in layouts:
        wid = str(item["id"])
        layout_payload = {
            "x": int(item["x"]),
            "y": int(item["y"]),
            "w": int(item["w"]),
            "h": int(item["h"]),
        }
        client.table("widgets").update({"layout": layout_payload}).eq(
            "id", wid
        ).eq("dashboard_id", dashboard_id).execute()

    return True



