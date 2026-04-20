"""
Data connector APIs — catalog (``connector_schemas``) and per-user configs
(``user_connector_configs``).

All routes require ``Authorization: Bearer <access_token>``. Resource access is
always scoped to ``UserProfile.id`` from the JWT; path ids must belong to the
caller or the handler returns 404.
"""

from __future__ import annotations

from datetime import date
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from fastapi_app.models.auth import UserProfile
from fastapi_app.models.connectors import (
    ConnectorCatalogDetail,
    ConnectorCatalogListItem,
    RawTablePreview,
    SyncedTableInfo,
    UserConnectorConfig,
    UserConnectorConfigCreate,
    UserConnectorConfigUpdate,
)
from fastapi_app.services.connector_service import (
    build_stream_parquet_zip,
    preview_raw_stream_table,
    create_user_connector,
    delete_user_connector,
    get_connector_catalog_detail,
    get_user_connector,
    list_connector_catalog,
    list_synced_tables_metadata,
    list_user_connectors,
    update_user_connector,
)
from fastapi_app.utils.auth_dep import get_current_user_dep

router = APIRouter(prefix="/api/connectors", tags=["connectors"])


@router.get("/catalog", response_model=list[ConnectorCatalogListItem])
async def get_connector_catalog(
    user: UserProfile = Depends(get_current_user_dep),
    q: str | None = Query(
        None,
        description="Case-insensitive filter on connector name or docker_repository.",
    ),
    active_only: bool = Query(True, description="Only include active connectors."),
):
    """Read-only catalog for “Connect a new source” (Airbyte registry sync)."""
    del user  # auth only
    rows = list_connector_catalog(search=q, active_only=active_only)
    return rows


@router.get("/catalog/{identifier:path}", response_model=ConnectorCatalogDetail)
async def get_connector_catalog_row(
    identifier: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Full catalog row: ``identifier`` is a catalog UUID or ``docker_repository`` (e.g. ``airbyte/source-github``)."""
    del user
    key = unquote(identifier.strip())
    if not key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector not found in catalog.",
        )
    row = get_connector_catalog_detail(identifier=key)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector not found in catalog.",
        )
    return row


@router.get("/synced-tables", response_model=list[SyncedTableInfo])
def list_synced_tables(
    user: UserProfile = Depends(get_current_user_dep),
    start_date: date | None = Query(
        None,
        description="With end_date, includes per-stream Parquet months in this inclusive range.",
    ),
    end_date: date | None = Query(
        None,
        description="With start_date, includes per-stream Parquet months in this inclusive range.",
    ),
):
    """Read-only sync metadata for “View raw tables” (paths + status + optional inventory)."""
    if (start_date is None) ^ (end_date is None):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide both start_date and end_date, or neither.",
        )
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date must be on or before end_date.",
        )
    return list_synced_tables_metadata(
        user_id=user.id, start_date=start_date, end_date=end_date
    )


@router.get("/{config_id}/streams/{stream_name:path}/download")
def download_raw_stream_zip(
    config_id: str,
    stream_name: str,
    user: UserProfile = Depends(get_current_user_dep),
    start_date: date = Query(..., description="Inclusive range start (ISO date)."),
    end_date: date = Query(..., description="Inclusive range end (ISO date)."),
):
    """Download monthly Parquet files for a stream as a single ZIP (read-only)."""
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date must be on or before end_date.",
        )
    result = build_stream_parquet_zip(
        user_id=user.id,
        config_id=config_id,
        stream_name=stream_name,
        start=start_date,
        end=end_date,
    )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No parquet files or stream not found for this range.",
        )
    body, filename = result
    return StreamingResponse(
        iter([body]),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/{config_id}/streams/{stream_name:path}/preview", response_model=RawTablePreview)
def preview_raw_stream(
    config_id: str,
    stream_name: str,
    user: UserProfile = Depends(get_current_user_dep),
    start_date: date = Query(..., description="Inclusive range start (ISO date)."),
    end_date: date = Query(..., description="Inclusive range end (ISO date)."),
    limit: int = Query(50, ge=1, le=200, description="Rows per page."),
    offset: int = Query(0, ge=0, description="Row offset for pagination."),
):
    """Return JSON rows from Parquet for in-app table preview (read-only)."""
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date must be on or before end_date.",
        )
    data = preview_raw_stream_table(
        user_id=user.id,
        config_id=config_id,
        stream_name=stream_name,
        start=start_date,
        end=end_date,
        limit=limit,
        offset=offset,
    )
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Preview not available (no data in range or connection not found).",
        )
    return data


@router.get("", response_model=list[UserConnectorConfig])
def list_my_connectors(
    user: UserProfile = Depends(get_current_user_dep),
):
    """List the current user’s saved connector configurations (Manage UI)."""
    return list_user_connectors(user_id=user.id)


@router.post("", response_model=UserConnectorConfig, status_code=status.HTTP_201_CREATED)
def create_my_connector(
    body: UserConnectorConfigCreate,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Create a connector configuration; ``user_id`` is taken from the JWT only."""
    return create_user_connector(user_id=user.id, body=body)


@router.get("/{config_id}", response_model=UserConnectorConfig)
def get_my_connector(
    config_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    row = get_user_connector(user_id=user.id, config_id=config_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector configuration not found.",
        )
    return row


@router.patch("/{config_id}", response_model=UserConnectorConfig)
def update_my_connector(
    config_id: str,
    body: UserConnectorConfigUpdate,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Update fields (pause via ``is_active: false``, streams, sync flags, etc.)."""
    return update_user_connector(
        user_id=user.id, config_id=config_id, body=body
    )


@router.delete("/{config_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_my_connector(
    config_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Delete a connector configuration permanently."""
    if not delete_user_connector(user_id=user.id, config_id=config_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector configuration not found.",
        )
