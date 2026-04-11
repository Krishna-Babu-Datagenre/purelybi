"""
API routes for user-owned dashboards.

Endpoints
---------
POST   /api/dashboards/create                          – create a blank user dashboard
POST   /api/dashboards                                 – hydrated template by slug (no DB copy)
GET    /api/dashboards                                 – list dashboards for the authenticated user
GET    /api/dashboards/{dashboard_id}                  – get a dashboard (optional date filter params)
POST   /api/dashboards/{dashboard_id}/duplicate        – duplicate a dashboard (user or template)
POST   /api/dashboards/{dashboard_id}/widgets          – add a widget to a dashboard
POST   /api/dashboards/{dashboard_id}/refresh          – force-refresh widget data (optional date params)
POST   /api/dashboards/{dashboard_id}/filtered         – get dashboard with arbitrary filters
DELETE /api/dashboards/{dashboard_id}                  – delete a dashboard
DELETE /api/dashboards/{dashboard_id}/widgets/{widget_id} – delete a widget
PUT    /api/dashboards/{dashboard_id}/widgets/layouts  – persist widget layout changes
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from fastapi_app.models.auth import UserProfile
from fastapi_app.services.dashboard_service import (
    add_widget_to_dashboard,
    create_dashboard,
    delete_dashboard,
    delete_widget,
    duplicate_dashboard,
    get_user_dashboard,
    instantiate_template,
    list_user_dashboards,
    persist_widget_layouts,
    refresh_dashboard,
)
from fastapi_app.services.widget_data_service import (
    build_date_filters_from_params,
    get_max_data_date_iso,
)
from fastapi_app.utils.auth_dep import get_current_user_dep

router = APIRouter(prefix="/api/dashboards", tags=["dashboards"])


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


class DashboardFilterRequest(BaseModel):
    """Body for fetching a dashboard with arbitrary data filters."""

    filters: list[dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Filter objects: {column, op, value}. "
            "Ops: eq, neq, in, not_in, gt, gte, lt, lte, between."
        ),
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/create", status_code=201)
async def create_blank_dashboard(
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
async def open_template_dashboard(
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
async def list_dashboards(
    user: UserProfile = Depends(get_current_user_dep),
):
    """Return all dashboards owned by the authenticated user."""
    return list_user_dashboards(user_id=user.id)


@router.get("/data/max-date", response_model=MaxDataDateResponse)
async def get_max_data_date(
    user: UserProfile = Depends(get_current_user_dep),
):
    """Latest calendar date present in the analytics database (dataset boundary)."""
    return MaxDataDateResponse(max_date=get_max_data_date_iso(user.id))


@router.get("/{dashboard_id}")
async def get_dashboard(
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
    """Return a single dashboard with all its widgets.

    Optional date filtering via *preset* (e.g. ``last_7_days``) or
    explicit *start_date* / *end_date* range.
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
        filters=filters,
        filters_from_preset=filters_from_preset,
    )
    if dashboard is None:
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )
    return dashboard


@router.post("/{dashboard_id}/duplicate", status_code=201)
async def duplicate_dashboard_endpoint(
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
async def add_widget(
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


@router.post("/{dashboard_id}/refresh")
async def refresh_dashboard_data(
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
async def get_filtered_dashboard(
    dashboard_id: str,
    body: DashboardFilterRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Return a dashboard with arbitrary data filters applied.

    Accepts a list of filter objects in the request body for category,
    numeric, and date filtering.  Widgets whose source table does not
    contain the filtered column are returned unfiltered.
    """
    dashboard = get_user_dashboard(
        user_id=user.id,
        dashboard_id=dashboard_id,
        filters=body.filters or None,
    )
    if dashboard is None:
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard '{dashboard_id}' not found.",
        )
    return dashboard


@router.delete("/{dashboard_id}", status_code=204)
async def remove_dashboard(
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
async def remove_widget(
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


@router.put("/{dashboard_id}/widgets/layouts", status_code=204)
async def persist_dashboard_widget_layouts(
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
