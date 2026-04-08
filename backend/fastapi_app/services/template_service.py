"""
Dashboard template service – reads from Supabase.

Queries ``dashboard_templates`` and ``widget_templates`` tables.
Supports filtering by platform so the frontend can recommend
templates that match the user's connected data sources.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status

from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def list_templates(
    platforms: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return all active dashboard templates.

    If *platforms* is provided, only templates whose ``platforms`` array
    overlaps with the given list are returned (uses the ``&&`` array
    overlap operator via an RPC or post-filter).
    """
    try:
        client = get_supabase_admin_client()
        query = (
            client.table("dashboard_templates")
            .select(
                "id, slug, name, description, platforms, tags, preview_image"
            )
            .eq("is_active", True)
            .order("created_at")
        )
        rows = query.execute().data or []
    except Exception:
        logger.exception("Failed to list templates from Supabase")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to fetch templates. Please try again later.",
        )

    if platforms:
        platform_set = set(platforms)
        rows = [
            r for r in rows if set(r.get("platforms") or []) & platform_set
        ]

    return rows


def get_template_by_slug(slug: str) -> dict[str, Any] | None:
    """Return a single template by slug, including its widget templates."""
    try:
        client = get_supabase_admin_client()

        rows = (
            client.table("dashboard_templates")
            .select("*")
            .eq("slug", slug)
            .eq("is_active", True)
            .limit(1)
            .execute()
        ).data
        tmpl = rows[0] if rows else None

        if not tmpl:
            return None

        widgets = (
            client.table("widget_templates")
            .select("*")
            .eq("template_id", tmpl["id"])
            .order("sort_order")
            .execute()
        ).data or []
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to fetch template '%s' from Supabase", slug)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to fetch template. Please try again later.",
        )

    tmpl["widgets"] = widgets
    return tmpl


def get_template_by_id(template_id: str) -> dict[str, Any] | None:
    """Return a single template by UUID, including its widget templates."""
    try:
        client = get_supabase_admin_client()

        rows = (
            client.table("dashboard_templates")
            .select("*")
            .eq("id", template_id)
            .eq("is_active", True)
            .limit(1)
            .execute()
        ).data
        tmpl = rows[0] if rows else None

        if not tmpl:
            return None

        widgets = (
            client.table("widget_templates")
            .select("*")
            .eq("template_id", tmpl["id"])
            .order("sort_order")
            .execute()
        ).data or []
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Failed to fetch template id='%s' from Supabase", template_id
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to fetch template. Please try again later.",
        )

    tmpl["widgets"] = widgets
    return tmpl
