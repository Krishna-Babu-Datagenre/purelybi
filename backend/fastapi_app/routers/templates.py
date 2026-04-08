"""
API routes for dashboard templates.

Endpoints
---------
GET  /api/templates              – list all available templates (optionally filter by platforms)
GET  /api/templates/{slug}/live – hydrated template dashboard (live data)
GET  /api/templates/{slug}       – get a single template with its widget blueprints
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from fastapi_app.models.auth import UserProfile
from fastapi_app.services.dashboard_service import get_live_template_by_slug
from fastapi_app.services.template_service import (
    get_template_by_slug,
    list_templates,
)
from fastapi_app.services.widget_data_service import build_date_filters_from_params
from fastapi_app.utils.auth_dep import get_current_user_dep

router = APIRouter(prefix="/api/templates", tags=["templates"])


@router.get("")
async def list_all_templates(
    platforms: str | None = Query(
        default=None,
        description="Comma-separated platform filter, e.g. 'shopify,meta_ads'",
    ),
):
    """Return metadata for every active dashboard template.

    Optionally filter by ``?platforms=shopify,meta_ads`` to only return
    templates whose supported platforms overlap with the given list.
    """
    platform_list = (
        [p.strip() for p in platforms.split(",") if p.strip()]
        if platforms
        else None
    )
    return list_templates(platforms=platform_list)


@router.get("/{slug}/live")
async def get_template_live(
    slug: str,
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
    """Return a template with widgets hydrated from the analytics database."""
    try:
        filters, filters_from_preset = build_date_filters_from_params(
            preset, start_date, end_date, tenant_id=user.id
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return get_live_template_by_slug(
        slug,
        tenant_id=user.id,
        filters=filters,
        filters_from_preset=filters_from_preset,
    )


@router.get("/{slug}")
async def get_template(slug: str):
    """Return a single template with its widget blueprints."""
    tmpl = get_template_by_slug(slug)
    if tmpl is None:
        raise HTTPException(
            status_code=404, detail=f"Template '{slug}' not found"
        )
    return tmpl
