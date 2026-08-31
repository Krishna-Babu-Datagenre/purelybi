"""
Pydantic models for the Data Engineering pipeline layer.

Mirrors the Supabase tables defined in 11-de-pipeline-tables.sql.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enums (mirror Postgres enums)
# ---------------------------------------------------------------------------


class DEPipelineRunStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    failed_to_start = "failed_to_start"


class DEMaterializationStatus(str, Enum):
    none = "none"
    ready = "ready"
    failed = "failed"
    stale = "stale"


# ---------------------------------------------------------------------------
# Recipes catalog
# ---------------------------------------------------------------------------


class RecipeDefinition(BaseModel):
    """Describes a prebuilt recipe that users can add as a pipeline step."""

    recipe_type: str
    label: str
    description: str
    config_schema: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class DEPipeline(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    connector_config_id: str
    source_connector_ids: list[str] = Field(default_factory=list)
    name: str
    is_active: bool = False
    version: int = 1
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DEPipelineCreate(BaseModel):
    connector_config_id: str | None = None
    source_connector_ids: list[str] = Field(default_factory=list)
    name: str = Field(..., min_length=1, max_length=255)


class DEPipelinePatch(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=255)
    is_active: bool | None = None
    source_connector_ids: list[str] | None = None


# ---------------------------------------------------------------------------
# Pipeline step
# ---------------------------------------------------------------------------


class DEPipelineStep(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    pipeline_id: str
    step_order: int
    recipe_type: str
    config_json: dict[str, Any] = Field(default_factory=dict)
    is_enabled: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DEPipelineStepUpsert(BaseModel):
    """Create or replace a single step (identified by step_order within the pipeline)."""

    step_order: int = Field(..., ge=1)
    recipe_type: str = Field(..., min_length=1)
    config_json: dict[str, Any] = Field(default_factory=dict)
    is_enabled: bool = True


# ---------------------------------------------------------------------------
# Pipeline with steps (rich response)
# ---------------------------------------------------------------------------


class DEPipelineDetail(DEPipeline):
    steps: list[DEPipelineStep] = Field(default_factory=list)


class DEPipelineDeleteResponse(BaseModel):
    pipeline_id: str
    cleaned_source_connector_ids: list[str] = Field(default_factory=list)
    retained_source_connector_ids: list[str] = Field(default_factory=list)
    deleted_output_prefixes: list[str] = Field(default_factory=list)
    deleted_blob_count: int = 0


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class DEValidationRequest(BaseModel):
    """Request body for the /validate endpoint."""

    sample_rows: list[dict[str, Any]] = Field(
        ...,
        description="Sample input rows to run through the pipeline steps.",
        min_length=1,
    )


class DEStepValidationResult(BaseModel):
    step_order: int
    recipe_type: str
    ok: bool
    error: str | None = None
    output_columns: list[str] = Field(default_factory=list)


class DEValidationResponse(BaseModel):
    ok: bool
    step_results: list[DEStepValidationResult]
    output_sample: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None


# ---------------------------------------------------------------------------
# Pipeline run
# ---------------------------------------------------------------------------


class DEPipelineRun(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    pipeline_id: str
    user_id: str
    connector_config_id: str
    trigger_source: str
    status: DEPipelineRunStatus
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error: str | None = None
    sync_work_id: str | None = None
    aca_execution_name: str | None = None
    failed_step_order: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


# ---------------------------------------------------------------------------
# Materialization (internal; exposed for transparency)
# ---------------------------------------------------------------------------


class DEDatasetMaterialization(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    connector_config_id: str
    pipeline_id: str
    last_success_run_id: str | None = None
    status: DEMaterializationStatus
    output_prefix: str | None = None
    updated_at: datetime | None = None
    created_at: datetime | None = None
