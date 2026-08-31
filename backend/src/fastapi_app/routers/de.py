"""
Data Engineering pipeline API endpoints.

All routes require ``Authorization: Bearer <access_token>``.

Endpoints
---------
GET    /api/de/recipes                              – list prebuilt recipes
POST   /api/de/pipelines                            – create pipeline
GET    /api/de/pipelines/{pipeline_id}              – fetch pipeline + steps
PATCH  /api/de/pipelines/{pipeline_id}              – update name / active flag
DELETE /api/de/pipelines/{pipeline_id}              – delete pipeline + conditional cleanup
POST   /api/de/pipelines/{pipeline_id}/steps        – upsert ordered step
POST   /api/de/pipelines/{pipeline_id}/validate     – sample validation preview
GET    /api/de/pipelines/{pipeline_id}/runs         – list run history
POST   /api/de/pipelines/{pipeline_id}/runs         – trigger manual run
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from fastapi_app.models.auth import UserProfile
from fastapi_app.models.de import (
    DEPipeline,
    DEPipelineCreate,
    DEPipelineDeleteResponse,
    DEPipelineDetail,
    DEPipelinePatch,
    DEPipelineRun,
    DEPipelineStep,
    DEPipelineStepUpsert,
    DEValidationRequest,
    DEValidationResponse,
    RecipeDefinition,
)
from fastapi_app.services import de_service
from fastapi_app.utils.auth_dep import get_current_user_dep

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/de", tags=["data-engineering"])

_UUID_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


def _validate_uuid(value: str, label: str) -> None:
    import re

    if not re.fullmatch(_UUID_RE, value, re.IGNORECASE):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid {label}: must be a UUID.",
        )


def _validate_uuid_list(values: list[str], label: str) -> None:
    for idx, value in enumerate(values):
        _validate_uuid(value, f"{label}[{idx}]")


# ---------------------------------------------------------------------------
# Recipes catalog
# ---------------------------------------------------------------------------


@router.get("/recipes", response_model=list[RecipeDefinition])
def list_recipes(user: UserProfile = Depends(get_current_user_dep)):
    """Return the catalog of prebuilt DE recipe types."""
    return de_service.PREBUILT_RECIPES


# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------


@router.post("/pipelines", response_model=DEPipeline, status_code=status.HTTP_201_CREATED)
def create_pipeline(
    body: DEPipelineCreate,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Create a new DE pipeline for a connector config."""
    if body.connector_config_id:
        _validate_uuid(body.connector_config_id, "connector_config_id")
    if body.source_connector_ids:
        _validate_uuid_list(body.source_connector_ids, "source_connector_ids")
    try:
        return de_service.create_pipeline(user_id=user.id, body=body)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))


@router.get("/pipelines/all", response_model=list[DEPipeline])
def list_all_pipelines(
    connector_config_id: str | None = Query(default=None),
    user: UserProfile = Depends(get_current_user_dep),
):
    """List pipelines for the current user, optionally filtered by source connector id."""
    if connector_config_id:
        _validate_uuid(connector_config_id, "connector_config_id")
    return de_service.list_pipelines(user_id=user.id, connector_config_id=connector_config_id)


@router.get("/pipelines", response_model=DEPipelineDetail | None)
def get_pipeline_by_connector(
    connector_config_id: str = Query(...),
    user: UserProfile = Depends(get_current_user_dep),
):
    """Fetch the newest pipeline (with steps) for a connector config, if one exists."""
    _validate_uuid(connector_config_id, "connector_config_id")
    pipelines = de_service.list_pipelines(
        user_id=user.id,
        connector_config_id=connector_config_id,
    )
    if not pipelines:
        return None
    return de_service.get_pipeline_detail(user_id=user.id, pipeline_id=pipelines[0].id)


@router.get("/pipelines/{pipeline_id}", response_model=DEPipelineDetail)
def get_pipeline(
    pipeline_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Fetch a pipeline and its ordered steps."""
    _validate_uuid(pipeline_id, "pipeline_id")
    result = de_service.get_pipeline_detail(user_id=user.id, pipeline_id=pipeline_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found.")
    return result


@router.patch("/pipelines/{pipeline_id}", response_model=DEPipeline)
def patch_pipeline(
    pipeline_id: str,
    body: DEPipelinePatch,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Update pipeline name and/or active flag."""
    _validate_uuid(pipeline_id, "pipeline_id")
    if body.source_connector_ids is not None:
        _validate_uuid_list(body.source_connector_ids, "source_connector_ids")
    try:
        result = de_service.patch_pipeline(user_id=user.id, pipeline_id=pipeline_id, body=body)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found.")
    return result


@router.delete("/pipelines/{pipeline_id}", response_model=DEPipelineDeleteResponse)
def delete_pipeline(
    pipeline_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Delete pipeline and clean transformed output when sources are no longer shared."""
    _validate_uuid(pipeline_id, "pipeline_id")
    try:
        result = de_service.delete_pipeline(user_id=user.id, pipeline_id=pipeline_id)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found.")
    return result


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@router.post(
    "/pipelines/{pipeline_id}/steps",
    response_model=DEPipelineStep,
    status_code=status.HTTP_201_CREATED,
)
def upsert_step(
    pipeline_id: str,
    body: DEPipelineStepUpsert,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Add or replace a step at the given step_order within a pipeline."""
    _validate_uuid(pipeline_id, "pipeline_id")
    try:
        return de_service.upsert_step(user_id=user.id, pipeline_id=pipeline_id, body=body)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))


@router.delete(
    "/pipelines/{pipeline_id}/steps/{step_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_step(
    pipeline_id: str,
    step_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Remove a step from a pipeline by its UUID."""
    _validate_uuid(pipeline_id, "pipeline_id")
    _validate_uuid(step_id, "step_id")
    deleted = de_service.delete_step(user_id=user.id, pipeline_id=pipeline_id, step_id=step_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Step not found.")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@router.post("/pipelines/{pipeline_id}/validate", response_model=DEValidationResponse)
def validate_pipeline(
    pipeline_id: str,
    body: DEValidationRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Run the pipeline's enabled steps against sample rows and return a preview."""
    _validate_uuid(pipeline_id, "pipeline_id")
    # Check pipeline exists before running potentially expensive validation.
    pipe = de_service.get_pipeline(user_id=user.id, pipeline_id=pipeline_id)
    if pipe is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found.")
    return de_service.validate_pipeline(
        user_id=user.id,
        pipeline_id=pipeline_id,
        request=body,
    )


# ---------------------------------------------------------------------------
# Run history
# ---------------------------------------------------------------------------


@router.get("/pipelines/{pipeline_id}/runs", response_model=list[DEPipelineRun])
def list_runs(
    pipeline_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    user: UserProfile = Depends(get_current_user_dep),
):
    """Return recent run history for a pipeline (newest first)."""
    _validate_uuid(pipeline_id, "pipeline_id")
    return de_service.list_runs(user_id=user.id, pipeline_id=pipeline_id, limit=limit)


@router.post(
    "/pipelines/{pipeline_id}/runs",
    response_model=DEPipelineRun,
    status_code=status.HTTP_202_ACCEPTED,
)
def trigger_run(
    pipeline_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Queue and trigger a manual DE pipeline run for the selected pipeline."""
    _validate_uuid(pipeline_id, "pipeline_id")
    try:
        run = de_service.trigger_pipeline_run(user_id=user.id, pipeline_id=pipeline_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found.")
    return run
