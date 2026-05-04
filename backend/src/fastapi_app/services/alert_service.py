"""
Alert service – CRUD, scheduling math, and run-history queries.

Mirrors ``dashboard_service.py``: every function takes ``user_id``
and uses ``get_supabase_admin_client()`` with explicit
``.eq("user_id", user_id)`` filters.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import HTTPException, status

from fastapi_app.models.alerts import (
    AlertCreate,
    AlertDefinition,
    AlertOut,
    AlertRunOut,
    AlertUpdate,
)
from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_alert_out(row: dict[str, Any]) -> AlertOut:
    """Convert a Supabase row dict to an AlertOut model."""
    return AlertOut(
        id=row["id"],
        name=row["name"],
        description=row.get("description"),
        definition=AlertDefinition(**row["definition"]),
        sql_query=row["sql_query"],
        comparator=row["comparator"],
        threshold=float(row["threshold"]),
        frequency=row["frequency"],
        notification_channel=row.get("notification_channel", "email"),
        notification_target=row.get("notification_target"),
        enabled=row.get("enabled", True),
        last_evaluated_at=row.get("last_evaluated_at"),
        last_fired_at=row.get("last_fired_at"),
        last_state=row.get("last_state"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_alert_run_out(row: dict[str, Any]) -> AlertRunOut:
    """Convert a Supabase row dict to an AlertRunOut model."""
    return AlertRunOut(
        id=row["id"],
        evaluated_at=row["evaluated_at"],
        status=row["status"],
        observed_value=float(row["observed_value"]) if row.get("observed_value") is not None else None,
        error_message=row.get("error_message"),
    )


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def create_alert(user_id: str, payload: AlertCreate) -> AlertOut:
    """Persist a new alert and return the created row."""
    client = get_supabase_admin_client()
    defn = payload.definition

    insert_data: dict[str, Any] = {
        "user_id": user_id,
        "name": payload.name,
        "description": payload.description or "",
        "definition": defn.model_dump(mode="json"),
        "sql_query": defn.metric_sql,
        "comparator": defn.comparator.value,
        "threshold": defn.threshold,
        "frequency": defn.frequency.value,
        "notification_channel": defn.notification_channel,
        "notification_target": defn.notification_target,
        "enabled": True,
    }

    result = client.table("alerts").insert(insert_data).execute()
    if not result.data:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create alert.",
        )
    return _row_to_alert_out(result.data[0])


def list_alerts(user_id: str) -> list[AlertOut]:
    """Return all alerts owned by the user, newest first."""
    client = get_supabase_admin_client()
    rows = (
        client.table("alerts")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    ).data or []
    return [_row_to_alert_out(r) for r in rows]


def get_alert(user_id: str, alert_id: str) -> AlertOut | None:
    """Return a single alert or None."""
    client = get_supabase_admin_client()
    rows = (
        client.table("alerts")
        .select("*")
        .eq("id", alert_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data
    if not rows:
        return None
    return _row_to_alert_out(rows[0])


def update_alert(user_id: str, alert_id: str, patch: AlertUpdate) -> AlertOut | None:
    """Update mutable fields on an alert."""
    client = get_supabase_admin_client()

    # Verify ownership
    existing = (
        client.table("alerts")
        .select("id")
        .eq("id", alert_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data
    if not existing:
        return None

    payload: dict[str, Any] = {}
    if patch.name is not None:
        payload["name"] = patch.name
    if patch.frequency is not None:
        payload["frequency"] = patch.frequency.value
    if patch.enabled is not None:
        payload["enabled"] = patch.enabled
    if patch.notification_target is not None:
        payload["notification_target"] = patch.notification_target

    if not payload:
        return get_alert(user_id, alert_id)

    result = (
        client.table("alerts")
        .update(payload)
        .eq("id", alert_id)
        .eq("user_id", user_id)
        .execute()
    )
    if not result.data:
        return None
    return _row_to_alert_out(result.data[0])


def delete_alert(user_id: str, alert_id: str) -> bool:
    """Delete an alert. Returns True if found and deleted."""
    client = get_supabase_admin_client()
    existing = (
        client.table("alerts")
        .select("id")
        .eq("id", alert_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data
    if not existing:
        return False
    client.table("alerts").delete().eq("id", alert_id).eq("user_id", user_id).execute()
    return True


def toggle_alert_enabled(user_id: str, alert_id: str, enabled: bool) -> AlertOut | None:
    """Toggle the enabled state of an alert."""
    return update_alert(user_id, alert_id, AlertUpdate(enabled=enabled))


# ---------------------------------------------------------------------------
# Run history
# ---------------------------------------------------------------------------

def list_alert_runs(user_id: str, alert_id: str, limit: int = 50) -> list[AlertRunOut]:
    """Return the most recent runs for an alert."""
    client = get_supabase_admin_client()

    # Verify ownership
    existing = (
        client.table("alerts")
        .select("id")
        .eq("id", alert_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data
    if not existing:
        return []

    rows = (
        client.table("alert_runs")
        .select("*")
        .eq("alert_id", alert_id)
        .eq("user_id", user_id)
        .order("evaluated_at", desc=True)
        .limit(limit)
        .execute()
    ).data or []
    return [_row_to_alert_run_out(r) for r in rows]


# ---------------------------------------------------------------------------
# Test evaluation (synchronous one-off run)
# ---------------------------------------------------------------------------

def test_evaluate_alert(user_id: str, alert_id: str) -> AlertRunOut:
    """Run a single synchronous evaluation of an alert and return the result.

    This does NOT send notifications — it only records the run.
    """
    alert = get_alert(user_id, alert_id)
    if alert is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert '{alert_id}' not found.",
        )

    import duckdb
    from ai.agents.sql.duckdb_sandbox import get_tenant_sandbox

    observed_value: float | None = None
    run_status = "ok"
    error_message: str | None = None

    try:
        conn, _views = get_tenant_sandbox(user_id)
        result = conn.execute(alert.sql_query).fetchone()
        if result is None or result[0] is None:
            run_status = "error"
            error_message = "Query returned no rows or NULL."
        else:
            observed_value = float(result[0])
            # Apply comparator
            fired = _compare(observed_value, alert.comparator.value, float(alert.threshold))
            run_status = "firing" if fired else "ok"
    except Exception as exc:
        run_status = "error"
        error_message = str(exc)[:500]
        logger.exception("Test evaluation failed for alert %s", alert_id)

    # Record the run
    client = get_supabase_admin_client()
    run_row = (
        client.table("alert_runs")
        .insert({
            "alert_id": alert_id,
            "user_id": user_id,
            "status": run_status,
            "observed_value": observed_value,
            "error_message": error_message,
        })
        .execute()
    ).data[0]

    return _row_to_alert_run_out(run_row)


def _compare(observed: float, comparator: str, threshold: float) -> bool:
    """Apply a comparison operator."""
    if comparator == "gt":
        return observed > threshold
    elif comparator == "gte":
        return observed >= threshold
    elif comparator == "lt":
        return observed < threshold
    elif comparator == "lte":
        return observed <= threshold
    elif comparator == "eq":
        return observed == threshold
    elif comparator == "neq":
        return observed != threshold
    # pct_change variants need a baseline — not supported in test mode
    return False
