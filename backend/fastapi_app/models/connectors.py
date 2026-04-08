"""Pydantic models for connector catalog and ``user_connector_configs`` APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ConnectorCatalogListItem(BaseModel):
    """Light catalog row for grid/search — excludes large JSON blobs."""

    id: str
    docker_repository: str
    name: str
    docker_image_tag: str = "latest"
    icon_url: str | None = None
    documentation_url: str | None = None
    is_active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ConnectorCatalogDetail(BaseModel):
    """Full ``connector_schemas`` row (detail endpoint / onboarding)."""

    id: str
    docker_repository: str
    name: str
    docker_image_tag: str = "latest"
    icon_url: str | None = None
    documentation_url: str | None = None
    config_schema: dict[str, Any] | None = None
    oauth_config: dict[str, Any] | None = None
    is_active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


# Backwards-compatible alias (same shape as detail)
ConnectorCatalogItem = ConnectorCatalogDetail


class UserConnectorConfig(BaseModel):
    """Full row returned to the owner for Manage UI."""

    id: str
    user_id: str
    connector_name: str
    docker_repository: str
    docker_image: str
    config: dict[str, Any]
    oauth_meta: dict[str, Any] | None = None
    selected_streams: list[str] | None = None
    sync_frequency_minutes: int = 360
    is_active: bool = True
    sync_validated: bool = False
    last_sync_at: datetime | None = None
    last_sync_status: str = "pending"
    last_sync_error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class UserConnectorConfigCreate(BaseModel):
    """Create body — never includes ``user_id`` (taken from JWT)."""

    connector_name: str = Field(..., min_length=1)
    docker_repository: str = Field(..., min_length=1)
    docker_image: str = Field(..., min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)
    oauth_meta: dict[str, Any] | None = None
    selected_streams: list[str] | None = None
    sync_frequency_minutes: int = Field(default=360, ge=1)
    is_active: bool = True


class UserConnectorConfigUpdate(BaseModel):
    """Partial update for pause/resume, streams, validation flags, etc."""

    connector_name: str | None = None
    docker_image: str | None = None
    config: dict[str, Any] | None = None
    oauth_meta: dict[str, Any] | None = None
    selected_streams: list[str] | None = None
    sync_frequency_minutes: int | None = Field(default=None, ge=1)
    is_active: bool | None = None
    sync_validated: bool | None = None


class SyncedMonthFile(BaseModel):
    """One monthly Parquet object under a stream prefix."""

    month: str = Field(..., description="Partition month YYYY-MM.")
    size_bytes: int | None = None


class StreamInventoryItem(BaseModel):
    """Stream (Airbyte table) with Parquet months visible in the requested date window."""

    stream: str
    months: list[SyncedMonthFile] = Field(default_factory=list)


class RawTablePreview(BaseModel):
    """Paginated JSON rows from Parquet preview (read-only)."""

    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    limit: int = Field(..., ge=1, description="Requested page size.")
    offset: int = Field(..., ge=0, description="Requested offset.")
    has_more: bool = Field(
        False,
        description="True when more rows exist after this page.",
    )
    months_included: list[str] = Field(
        default_factory=list,
        description="YYYY-MM Parquet files included in this preview.",
    )


class SyncedTableInfo(BaseModel):
    """Sync metadata for View raw tables — paths, status, optional per-stream inventory."""

    connector_config_id: str
    docker_repository: str
    connector_name: str
    last_sync_at: datetime | None = None
    last_sync_status: str = "pending"
    last_sync_error: str | None = None
    data_prefix_hint: str = Field(
        ...,
        description="Expected blob prefix for this user's Parquet files (layout TBD).",
    )
    synced_tables: list[str] = Field(
        default_factory=list,
        description="Table names once sync metadata is available.",
    )
    stream_inventory: list[StreamInventoryItem] | None = Field(
        None,
        description="Populated when synced-tables is called with start_date and end_date.",
    )
