"""
Pydantic models for the dashboard metadata layer.

These describe the LLM-generated, user-editable semantic schema that drives
native dashboard filtering. See ``docs/native_dashboard_filtering.md`` §3.

The codebase treats ``user_id`` as the tenant id (user = tenant).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enums (mirror the Postgres enums in 6-metadata-tables.sql)
# ---------------------------------------------------------------------------


class SemanticType(str, Enum):
    categorical = "categorical"
    numeric = "numeric"
    temporal = "temporal"
    identifier = "identifier"
    measure = "measure"
    unknown = "unknown"


class RelationshipKind(str, Enum):
    many_to_one = "many_to_one"
    one_to_one = "one_to_one"
    many_to_many = "many_to_many"


class MetadataJobStatus(str, Enum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


# ---------------------------------------------------------------------------
# Table metadata
# ---------------------------------------------------------------------------


class TableMetadata(BaseModel):
    """One row in ``tenant_table_metadata``."""

    model_config = ConfigDict(from_attributes=True)

    user_id: str
    table_name: str
    description: str | None = None
    primary_date_column: str | None = None
    grain: str | None = None
    generated_at: datetime | None = None
    edited_by_user: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


class TableMetadataPatch(BaseModel):
    """Partial update payload for a single table metadata row."""

    description: str | None = None
    primary_date_column: str | None = None
    grain: str | None = None


# ---------------------------------------------------------------------------
# Column metadata
# ---------------------------------------------------------------------------


class ColumnMetadata(BaseModel):
    """One row in ``tenant_column_metadata``."""

    model_config = ConfigDict(from_attributes=True)

    user_id: str
    table_name: str
    column_name: str
    data_type: str
    semantic_type: SemanticType = SemanticType.unknown
    description: str | None = None
    is_filterable: bool = True
    cardinality: int | None = None
    sample_values: list[Any] | None = None
    generated_at: datetime | None = None
    edited_by_user: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ColumnMetadataPatch(BaseModel):
    """Partial update payload for a single column metadata row."""

    semantic_type: SemanticType | None = None
    description: str | None = None
    is_filterable: bool | None = None


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------


class Relationship(BaseModel):
    """One row in ``tenant_table_relationships``."""

    model_config = ConfigDict(from_attributes=True)

    user_id: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    kind: RelationshipKind
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    edited_by_user: bool = False
    generated_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class RelationshipCreate(BaseModel):
    """User-supplied edge to insert into ``tenant_table_relationships``."""

    from_table: str = Field(..., min_length=1)
    from_column: str = Field(..., min_length=1)
    to_table: str = Field(..., min_length=1)
    to_column: str = Field(..., min_length=1)
    kind: RelationshipKind
    confidence: float | None = Field(default=1.0, ge=0.0, le=1.0)


class RelationshipPatch(BaseModel):
    """Partial update for an existing edge (only ``kind`` is mutable today)."""

    kind: RelationshipKind | None = None


# ---------------------------------------------------------------------------
# Generation job
# ---------------------------------------------------------------------------


class MetadataJob(BaseModel):
    """One row in ``tenant_metadata_jobs``."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    status: MetadataJobStatus
    progress: float = 0.0
    message: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    aca_execution_name: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class MetadataGenerationResponse(BaseModel):
    """Response returned by ``POST /api/metadata/generate``."""

    job: MetadataJob
