"""
API routes for the Alerts feature.

Endpoints
---------
POST   /api/alerts                    – create a new alert
GET    /api/alerts                    – list user's alerts
GET    /api/alerts/{id}               – get a single alert
PATCH  /api/alerts/{id}               – update name, frequency, enabled, target
DELETE /api/alerts/{id}               – delete an alert (cascades to runs & notifications)
GET    /api/alerts/{id}/runs          – run history (last N runs)
POST   /api/alerts/{id}/test          – synchronous one-off evaluation (no notification)
POST   /api/alerts/builder/stream     – NL → AlertDefinition via agent (SSE) [Phase 2]

All endpoints require ``Authorization: Bearer <token>``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from fastapi_app.models.alerts import (
    AlertCreate,
    AlertOut,
    AlertRunOut,
    AlertUpdate,
)
from fastapi_app.models.auth import UserProfile
from fastapi_app.services.alert_service import (
    create_alert,
    delete_alert,
    get_alert,
    list_alert_runs,
    list_alerts,
    test_evaluate_alert,
    update_alert,
)
from fastapi_app.services.alert_builder_service import stream_alert_builder
from fastapi_app.utils.auth_dep import get_current_user_dep

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class AlertBuilderStreamRequest(BaseModel):
    """Body for the alert builder SSE stream."""

    session_id: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/builder/stream")
async def alert_builder_stream(
    body: AlertBuilderStreamRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Stream the alert builder agent's response as SSE.

    The frontend should consume this with a fetch() reader.
    SSE events: token, tool_call_start, tool_result, alert_preview, end, error.
    """
    scoped_session = f"{user.id}:{body.session_id}"
    return StreamingResponse(
        stream_alert_builder(
            message=body.message,
            tenant_id=user.id,
            session_id=scoped_session,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("", status_code=201, response_model=AlertOut)
def create_alert_endpoint(
    body: AlertCreate,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Persist a finalised alert."""
    return create_alert(user_id=user.id, payload=body)


@router.get("", response_model=list[AlertOut])
def list_alerts_endpoint(
    user: UserProfile = Depends(get_current_user_dep),
):
    """List all alerts for the authenticated user."""
    return list_alerts(user_id=user.id)


@router.get("/{alert_id}", response_model=AlertOut)
def get_alert_endpoint(
    alert_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Get a single alert by ID."""
    alert = get_alert(user_id=user.id, alert_id=alert_id)
    if alert is None:
        raise HTTPException(
            status_code=404,
            detail=f"Alert '{alert_id}' not found.",
        )
    return alert


@router.patch("/{alert_id}", response_model=AlertOut)
def update_alert_endpoint(
    alert_id: str,
    body: AlertUpdate,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Update alert name, frequency, enabled, or notification target."""
    result = update_alert(user_id=user.id, alert_id=alert_id, patch=body)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Alert '{alert_id}' not found.",
        )
    return result


@router.delete("/{alert_id}", status_code=204)
def delete_alert_endpoint(
    alert_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Delete an alert and all associated runs and notifications."""
    if not delete_alert(user_id=user.id, alert_id=alert_id):
        raise HTTPException(
            status_code=404,
            detail=f"Alert '{alert_id}' not found.",
        )


@router.get("/{alert_id}/runs", response_model=list[AlertRunOut])
def get_alert_runs_endpoint(
    alert_id: str,
    user: UserProfile = Depends(get_current_user_dep),
    limit: int = Query(50, ge=1, le=200),
):
    """Return the most recent runs for an alert."""
    return list_alert_runs(user_id=user.id, alert_id=alert_id, limit=limit)


@router.post("/{alert_id}/test", response_model=AlertRunOut)
def test_alert_endpoint(
    alert_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Synchronous one-off evaluation — runs the SQL, records a run, no notifications."""
    return test_evaluate_alert(user_id=user.id, alert_id=alert_id)
