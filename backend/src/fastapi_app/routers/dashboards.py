"""
API routes for user-owned dashboards.

Endpoints
---------
POST   /api/dashboards/create                          – create a blank user dashboard
POST   /api/dashboards                                 – hydrated template by slug (no DB copy)
GET    /api/dashboards                                 – list dashboards for the authenticated user
GET    /api/dashboards/{dashboard_id}                  – get a dashboard (optional date filter params; hydrate=false returns shell instantly)
POST   /api/dashboards/{dashboard_id}/duplicate        – duplicate a dashboard (user or template)
POST   /api/dashboards/{dashboard_id}/widgets          – add a widget to a dashboard
POST   /api/dashboards/{dashboard_id}/refresh          – force-refresh widget data (optional date params)
POST   /api/dashboards/{dashboard_id}/filtered         – get dashboard with arbitrary filters
DELETE /api/dashboards/{dashboard_id}                  – delete a dashboard
DELETE /api/dashboards/{dashboard_id}/widgets/{widget_id} – delete a widget
PUT    /api/dashboards/{dashboard_id}/widgets/{widget_id} – update a widget's config and title
POST   /api/dashboards/preview-widget                  – run a widget's SQL and return hydrated data
PUT    /api/dashboards/{dashboard_id}/widgets/layouts  – persist widget layout changes
GET    /api/dashboards/builder/readiness               – AI builder data status + dataset views
PUT    /api/dashboards/{dashboard_id}/metadata          – update dashboard name/description
POST   /api/dashboards/prewarm                         – trigger DuckDB sandbox initialisation in background
"""

from __future__ import annotations

import threading
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from fastapi_app.models.auth import UserProfile
from fastapi_app.models.filters import FilterSpec
from fastapi_app.services.dashboard_service import (
    add_widget_to_dashboard,
    create_dashboard,
    delete_dashboard,
    delete_widget,
    duplicate_dashboard,
    get_dashboard_builder_readiness,
    get_user_dashboard,
    instantiate_template,
    list_user_dashboards,
    persist_widget_layouts,
    refresh_dashboard,
    update_dashboard,
    update_widget_config,
)
from fastapi_app.services.widget_data_service import (
    build_date_filters_from_params,
    get_max_data_date_iso,
    hydrate_widgets,
)
from fastapi_app.utils.auth_dep import get_current_user_dep

router = APIRouter(prefix="/api/dashboards", tags=["dashboards"])


# ---------------------------------------------------------------------------
# Background DuckDB pre-warm helper (used by sign-in and the prewarm endpoint)
# ---------------------------------------------------------------------------

def _prewarm_duckdb(user_id: str) -> None:
    """Materialise the tenant DuckDB sandbox in a daemon thread (best-effort)."""
    try:
        from ai.agents.sql.duckdb_sandbox import get_tenant_sandbox  # noqa: PLC0415
        get_tenant_sandbox(user_id)
    except Exception:  # noqa: BLE001
        pass  # pre-warm failure must never surface to the caller


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class InstantiateDashboardRequest(BaseModel):
    """Body for creating a new dashboard from a template."""

    template_slug: str


class CreateDashboardRequest(BaseModel):
    """Body for creating a new blank dashboard."""

    name: str = Field(..., min_length=1)
    description: str | None = None
    tags: list[str] | None = None


class UpdateDashboardRequest(BaseModel):
    """Body for updating dashboard metadata."""

    name: str | None = Field(default=None, min_length=1)
    description: str | None = None


class DuplicateDashboardRequest(BaseModel):
    """Body for duplicating a dashboard."""

    name: str | None = Field(
        default=None,
        description="Optional custom name for the copy. Defaults to '<original> (Copy)'.",
    )


class AddWidgetRequest(BaseModel):
    """Body for adding a widget (e.g. an agent-generated chart) to a dashboard."""

    title: str = Field(..., min_length=1)
    type: str = Field(
        ..., description="Widget type: 'bar', 'line', 'pie', 'kpi', etc."
    )
    chart_config: dict[str, Any] = Field(
        ..., description="ECharts option object (or KpiConfig for type='kpi')"
    )
    layout: dict[str, Any] | None = Field(
        default=None,
        description="Grid position { x, y, w, h }. Auto-assigned if omitted.",
    )
    data_config: dict[str, Any] | None = Field(
        default=None,
        description="SQL query + mappings for server-side re-hydration (enables date filtering).",
    )


class UpdateWidgetRequest(BaseModel):
    """Body for updating an existing widget."""

    title: str | None = Field(None, min_length=1)
    chart_config: dict[str, Any] | None = Field(None)
    data_config: dict[str, Any] | None = Field(None)


class PreviewWidgetRequest(BaseModel):
    """Body for previewing a widget (hydrating its data without saving)."""

    widget: dict[str, Any] = Field(..., description="Widget definition to hydrate")


class DashboardFilterRequest(BaseModel):
    """Body for fetching a dashboard with arbitrary data filters.

    Backward compatible: legacy callers pass only ``filters`` (list of
    ``{column, op, value}`` dicts). Group D callers add ``filter_spec``
    for native dashboard filtering, plus optional ``preset`` /
    ``start_date`` / ``end_date`` so the same endpoint can apply both
    date ranges and cross-table predicates in a single request.
    """

    filters: list[dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Filter objects: {column, op, value}. "
            "Ops: eq, neq, in, not_in, gt, gte, lt, lte, between."
        ),
    )
    filter_spec: FilterSpec | None = Field(
        default=None,
        description=(
            "Native dashboard FilterSpec (time + categorical + numeric). "
            "Applied via the filter engine in widget_data_service."
        ),
    )
    preset: str | None = Field(
        default=None,
        description="Optional date preset, e.g. 'last_7_days'.",
    )
    start_date: str | None = Field(
        default=None, description="Optional custom start date (ISO)."
    )
    end_date: str | None = Field(
        default=None, description="Optional custom end date (ISO, exclusive)."
    )
    force_refresh: bool = Field(
        default=False,
        description="Ignore the preset-filter cache when True.",
    )


class WidgetLayoutUpdateItem(BaseModel):
    """One widget layout update payload."""

    id: str = Field(..., min_length=1)
    x: int = Field(..., ge=0)
    y: int = Field(..., ge=0)
    w: int = Field(..., ge=1, le=12)
    h: int = Field(..., ge=1)


class PersistWidgetLayoutsRequest(BaseModel):
    """Body for persisting dashboard widget layout changes."""

    layouts: list[WidgetLayoutUpdateItem] = Field(default_factory=list)


class MaxDataDateResponse(BaseModel):
    """Latest calendar date present in analytics data (data boundary)."""

    max_date: str = Field(..., description="ISO date YYYY-MM-DD")


class DashboardBuilderReadinessResponse(BaseModel):
    """Whether the user can run the dashboard builder agent."""

    status: str = Field(
        ...,
        description="ready | waiting_sync | no_connector",
    )
    message: str
    datasets: list[str] = Field(
        default_factory=list,
        description="DuckDB view names available for this tenant (from synced Parquet).",
    )
    has_connector: bool
    has_synced_data: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/create", status_code=201)
def create_blank_dashboard(
    body: CreateDashboardRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Create a new empty dashboard owned by the authenticated user."""
    return create_dashboard(
        user_id=user.id,
        name=body.name,
        description=body.description,
        tags=body.tags,
    )


@router.post("")
def open_template_dashboard(
    body: InstantiateDashboardRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Return a live, hydrated dashboard for a template slug.

    Does not create rows in ``dashboards`` or ``widgets``; the response
    always reflects the current template definition in Supabase.
    """
    return instantiate_template(
        user_id=user.id,
        template_slug=body.template_slug,
    )


@router.get("")
def list_dashboards(
    user: UserProfile = Depends(get_current_user_dep),
):
    """Return all dashboards owned by the authenticated user."""
    return list_user_dashboards(user_id=user.id)


@router.get("/data/max-date", response_model=MaxDataDateResponse)
def get_max_data_date(
    user: UserProfile = Depends(get_current_user_dep),
):
    """Latest calendar date present in the analytics database (dataset boundary)."""
    return MaxDataDateResponse(max_date=get_max_data_date_iso(user.id))


@router.get("/builder/readiness", response_model=DashboardBuilderReadinessResponse)
def dashboard_builder_readiness(
    user: UserProfile = Depends(get_current_user_dep),
):
    """Data availability and dataset view names for the AI dashboard builder."""
    return DashboardBuilderReadinessResponse(
        **get_dashboard_builder_readiness(user_id=user.id)
    )


@router.post("/prewarm", status_code=202)
def prewarm_dashboard(user: UserProfile = Depends(get_current_user_dep)):
    """Trigger DuckDB sandbox initialisation in the background.

    Returns immediately (HTTP 202) — the tenant's Parquet files start
    materialising into DuckDB in a daemon thread.  Call this as soon as the
    user lands on the dashboard list so the sandbox is ready by the time they
    open a specific dashboard.  Safe to call multiple times; subsequent calls
    while materialisation is in progress are no-ops (the per-tenant lock in
    get_tenant_sandbox serialises them).
    """
    threading.Thread(
        target=_prewarm_duckdb, args=(user.id,), daemon=True
    ).start()
    return {"status": "warming"}


@router.get("/{dashboard_id}")
def get_dashboard(
    dashboard_id: str,
    user: UserProfile = Depends(get_current_user_dep),
    preset: str | None = Query(
        None,
        description="Date preset: last_7_days, last_14_days, last_30_days",
    ),
    start_date: str | None = Query(
        None,
        description="Start date (ISO format, inclusive)",
    ),
    end_date: str | None = Query(
        None,
        description="End date (ISO format, exclusive)",
    ),
    hydrate: bool = Query(
        True,
        description=(
            "When false, return the dashboard shell (layout + chart_config from DB) "
            "immediately without running DuckDB queries. Use this for a fast initial "
            "render; follow up with hydrate=true (or omit) to get live data."
        ),
    ),
):
    """Return a single dashboard with all its widgets.

    Optional date filtering via *preset* (e.g. ``last_7_days``) or
    explicit *start_date* / *end_date* range.

    Pass ``hydrate=false`` for an instant shell response (< 200 ms) that
    skips DuckDB queries — useful for progressive loading where the
    frontend shows the layout first and fetches live data in a second call.
    """
    try:
        filters, filters_from_preset = build_date_filters_from_params(
            preset, start_date, end_date, tenant_id=user.id
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    dashboard = get_user_dashboard(
        user_id=user.id,
        dashboard_id=dashboard_id,
        filters=filters if hydrate else None,
        filters_from_preset=filters_from_preset if hydrate else None,
        hydrate=hydrate,
    )
    if dashboard is None:
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )
    return dashboard


@router.put("/{dashboard_id}/metadata")
def update_dashboard_metadata(
    dashboard_id: str,
    body: UpdateDashboardRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Update dashboard name and/or description."""
    if body.name is None and body.description is None:
        raise HTTPException(
            status_code=400,
            detail="Provide name and/or description.",
        )
    row = update_dashboard(
        user_id=user.id,
        dashboard_id=dashboard_id,
        name=body.name,
        description=body.description,
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )
    return row


@router.post("/{dashboard_id}/duplicate", status_code=201)
def duplicate_dashboard_endpoint(
    dashboard_id: str,
    body: DuplicateDashboardRequest | None = None,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Duplicate a dashboard (user-owned or template) under the authenticated user's profile."""
    return duplicate_dashboard(
        user_id=user.id,
        dashboard_id=dashboard_id,
        new_name=body.name if body else None,
    )


@router.post("/{dashboard_id}/widgets", status_code=201)
def add_widget(
    dashboard_id: str,
    body: AddWidgetRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Add a widget to an existing dashboard.

    Use this to save an agent-generated chart to a dashboard.
    """
    return add_widget_to_dashboard(
        user_id=user.id,
        dashboard_id=dashboard_id,
        title=body.title,
        widget_type=body.type,
        chart_config=body.chart_config,
        layout=body.layout,
        data_config=body.data_config,
    )


@router.put("/{dashboard_id}/widgets/layouts", status_code=204)
def persist_dashboard_widget_layouts(
    dashboard_id: str,
    body: PersistWidgetLayoutsRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Persist drag/resize layout changes for widgets on a dashboard."""
    if not persist_widget_layouts(
        user_id=user.id,
        dashboard_id=dashboard_id,
        layouts=[item.model_dump() for item in body.layouts],
    ):
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )


@router.put("/{dashboard_id}/widgets/{widget_id}")
def update_widget(
    dashboard_id: str,
    widget_id: str,
    body: UpdateWidgetRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Update a widget's config (title, chart_config, data_config)."""
    row = update_widget_config(
        user_id=user.id,
        dashboard_id=dashboard_id,
        widget_id=widget_id,
        title=body.title,
        chart_config=body.chart_config,
        data_config=body.data_config,
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Widget '{widget_id}' not found on dashboard '{dashboard_id}'.",
        )
    return row


@router.post("/preview-widget")
def preview_widget(
    body: PreviewWidgetRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Hydrate a single widget definition with live data without saving it."""
    # hydrate_widgets takes a list of widgets and modifies them in place
    widgets = [body.widget]
    hydrated = hydrate_widgets(
        widgets,
        tenant_id=user.id,
        force_refresh=True,
    )
    if not hydrated:
        raise HTTPException(status_code=400, detail="Failed to hydrate widget.")
    return hydrated[0]


@router.post("/{dashboard_id}/refresh")
def refresh_dashboard_data(
    dashboard_id: str,
    user: UserProfile = Depends(get_current_user_dep),
    preset: str | None = Query(
        None,
        description="Date preset: last_7_days, last_14_days, last_30_days",
    ),
    start_date: str | None = Query(
        None,
        description="Start date (ISO format, inclusive)",
    ),
    end_date: str | None = Query(
        None,
        description="End date (ISO format, exclusive)",
    ),
):
    """Force-refresh all widget data for a dashboard, ignoring cache."""
    try:
        filters, filters_from_preset = build_date_filters_from_params(
            preset, start_date, end_date, tenant_id=user.id
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    dashboard = refresh_dashboard(
        user_id=user.id,
        dashboard_id=dashboard_id,
        filters=filters,
        filters_from_preset=filters_from_preset,
    )
    if dashboard is None:
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )
    return dashboard


@router.post("/{dashboard_id}/filtered")
def get_filtered_dashboard(
    dashboard_id: str,
    body: DashboardFilterRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Return a dashboard with arbitrary data filters applied.

    Accepts either the legacy list of ``{column, op, value}`` filter dicts
    (date ranges, single-table categorical) **and/or** a native
    ``filter_spec`` (Group D). When ``preset`` / ``start_date`` /
    ``end_date`` are provided in the body, they produce date filters the
    same way the ``GET /{dashboard_id}`` endpoint does via query params.

    Widgets whose source table does not contain a filtered column are
    returned unfiltered (skipped filters are logged per widget).
    """
    try:
        preset_filters, preset_key = build_date_filters_from_params(
            body.preset, body.start_date, body.end_date, tenant_id=user.id
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Merge legacy body.filters (already column/op/value dicts) with any
    # preset-generated ones; preset filters take precedence for `between`.
    merged_filters: list[dict[str, Any]] | None = None
    if body.filters and preset_filters:
        merged_filters = [*body.filters, *preset_filters]
    else:
        merged_filters = body.filters or preset_filters or None

    if body.force_refresh:
        dashboard = refresh_dashboard(
            user_id=user.id,
            dashboard_id=dashboard_id,
            filters=merged_filters,
            filters_from_preset=preset_key,
            filter_spec=body.filter_spec,
        )
    else:
        dashboard = get_user_dashboard(
            user_id=user.id,
            dashboard_id=dashboard_id,
            filters=merged_filters,
            filters_from_preset=preset_key,
            filter_spec=body.filter_spec,
        )
    if dashboard is None:
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )
    return dashboard


@router.delete("/{dashboard_id}", status_code=204)
def remove_dashboard(
    dashboard_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Delete a dashboard and all its widgets."""
    if not delete_dashboard(user_id=user.id, dashboard_id=dashboard_id):
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )


@router.delete("/{dashboard_id}/widgets/{widget_id}", status_code=204)
def remove_widget(
    dashboard_id: str,
    widget_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Delete a single widget from a dashboard."""
    if not delete_widget(
        user_id=user.id, dashboard_id=dashboard_id, widget_id=widget_id
    ):
        raise HTTPException(
            status_code=404,
            detail=f"Widget '{widget_id}' not found.",
        )
