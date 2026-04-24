"""
Routes for the dashboard-filter metadata layer.

All routes require ``Authorization: Bearer <access_token>`` and are scoped
to the authenticated user (= tenant). See
``docs/native_dashboard_filtering.md`` §4.

Endpoints
---------
GET    /api/metadata/tables                              – list tables
PATCH  /api/metadata/tables/{table_name}                 – update one table
GET    /api/metadata/columns                             – list columns (?table=)
PATCH  /api/metadata/columns/{table_name}/{column_name}  – update one column
GET    /api/metadata/values                              – distinct column values
GET    /api/metadata/relationships                       – list relationship edges
POST   /api/metadata/relationships                       – create / upsert an edge
PATCH  /api/metadata/relationships/{f_t}/{f_c}/{t_t}/{t_c} – update an edge
DELETE /api/metadata/relationships/{f_t}/{f_c}/{t_t}/{t_c} – remove an edge
GET    /api/metadata/jobs/latest                         – latest generation job
GET    /api/metadata/jobs/{job_id}                       – one job (poll)
POST   /api/metadata/generate                            – trigger generation
"""

from __future__ import annotations

import logging
import re

from fastapi import APIRouter, Depends, HTTPException, Query, status

from fastapi_app.models.auth import UserProfile
from fastapi_app.models.metadata import (
    ColumnMetadata,
    ColumnMetadataPatch,
    MetadataGenerationResponse,
    MetadataJob,
    Relationship,
    RelationshipCreate,
    RelationshipPatch,
    TableMetadata,
    TableMetadataPatch,
)
from fastapi_app.services import metadata_service
from fastapi_app.utils.auth_dep import get_current_user_dep

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/metadata", tags=["metadata"])


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


@router.get("/tables", response_model=list[TableMetadata])
def get_tables(user: UserProfile = Depends(get_current_user_dep)):
    """List all per-table metadata rows for the authenticated user."""
    return metadata_service.list_table_metadata(user_id=user.id)


@router.patch("/tables/{table_name}", response_model=TableMetadata)
def patch_table(
    table_name: str,
    body: TableMetadataPatch,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Update a single table metadata row (flags it as user-edited)."""
    row = metadata_service.patch_table_metadata(
        user_id=user.id, table_name=table_name, patch=body
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No metadata row for table '{table_name}'.",
        )
    return row


# ---------------------------------------------------------------------------
# Columns
# ---------------------------------------------------------------------------


@router.get("/columns", response_model=list[ColumnMetadata])
def get_columns(
    user: UserProfile = Depends(get_current_user_dep),
    table: str | None = Query(
        default=None,
        description="Optional table name filter; returns all columns if omitted.",
    ),
):
    """List per-column metadata rows; optionally scoped to a single table."""
    return metadata_service.list_column_metadata(user_id=user.id, table_name=table)


@router.patch(
    "/columns/{table_name}/{column_name}",
    response_model=ColumnMetadata,
)
def patch_column(
    table_name: str,
    column_name: str,
    body: ColumnMetadataPatch,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Update a single column metadata row (flags it as user-edited)."""
    row = metadata_service.patch_column_metadata(
        user_id=user.id,
        table_name=table_name,
        column_name=column_name,
        patch=body,
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No metadata row for column '{table_name}.{column_name}'.",
        )
    return row


# ---------------------------------------------------------------------------
# Column distinct values (for categorical filter dropdowns)
# ---------------------------------------------------------------------------

# Only allow safe DuckDB identifiers (alphanumeric + underscore).
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_DISTINCT_VALUES = 500


@router.get("/values")
def get_column_values(
    table: str = Query(..., min_length=1, description="Table name"),
    column: str = Query(..., min_length=1, description="Column name"),
    limit: int = Query(
        _MAX_DISTINCT_VALUES,
        ge=1,
        le=_MAX_DISTINCT_VALUES,
        description=f"Max distinct values to return (cap {_MAX_DISTINCT_VALUES}).",
    ),
    user: UserProfile = Depends(get_current_user_dep),
):
    """Return distinct non-null values for a column, capped at *limit*.

    Used by the categorical filter multi-select dropdown in the frontend
    filter pane. Values are sorted ascending for stable ordering.
    """
    if not _SAFE_IDENT.match(table) or not _SAFE_IDENT.match(column):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid table or column identifier.",
        )

    from ai.agents.sql.duckdb_sandbox import get_tenant_sandbox  # noqa: PLC0415

    try:
        conn, _ = get_tenant_sandbox(user.id)
    except Exception:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Data sandbox not available. Ensure at least one data source has synced.",
        )

    try:
        sql = (
            f'SELECT DISTINCT "{column}" AS v '
            f'FROM "{table}" '
            f'WHERE "{column}" IS NOT NULL '
            f"ORDER BY v "
            f"LIMIT {limit}"
        )
        rows = conn.execute(sql).fetchall()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to fetch distinct values for %s.%s (user %s): %s",
            table,
            column,
            user.id,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Column '{table}.{column}' not found or not queryable.",
        ) from exc

    return {"table": table, "column": column, "values": [r[0] for r in rows]}


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------


@router.get("/relationships", response_model=list[Relationship])
def get_relationships(user: UserProfile = Depends(get_current_user_dep)):
    """List all relationship edges for the authenticated user."""
    return metadata_service.list_relationships(user_id=user.id)


@router.post(
    "/relationships",
    response_model=Relationship,
    status_code=status.HTTP_201_CREATED,
)
def create_relationship(
    body: RelationshipCreate,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Create or upsert a user-defined relationship edge."""
    if body.from_table == body.to_table and body.from_column == body.to_column:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A relationship cannot point to itself.",
        )
    return metadata_service.create_relationship(user_id=user.id, body=body)


@router.patch(
    "/relationships/{from_table}/{from_column}/{to_table}/{to_column}",
    response_model=Relationship,
)
def patch_relationship(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    body: RelationshipPatch,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Update an existing relationship edge."""
    row = metadata_service.patch_relationship(
        user_id=user.id,
        from_table=from_table,
        from_column=from_column,
        to_table=to_table,
        to_column=to_column,
        patch=body,
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Relationship edge not found.",
        )
    return row


@router.delete(
    "/relationships/{from_table}/{from_column}/{to_table}/{to_column}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_relationship(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Delete a relationship edge. 204 even if the row was already absent."""
    metadata_service.delete_relationship(
        user_id=user.id,
        from_table=from_table,
        from_column=from_column,
        to_table=to_table,
        to_column=to_column,
    )
    return None


# ---------------------------------------------------------------------------
# Generation jobs
# ---------------------------------------------------------------------------


@router.get("/jobs/latest", response_model=MetadataJob | None)
def get_latest_generation_job(user: UserProfile = Depends(get_current_user_dep)):
    """Most recently created metadata-generation job for the user (or null)."""
    return metadata_service.get_latest_job(user_id=user.id)


@router.get("/jobs/{job_id}", response_model=MetadataJob)
def get_generation_job(
    job_id: str,
    user: UserProfile = Depends(get_current_user_dep),
):
    """Poll a single generation job by id."""
    job = metadata_service.get_job(user_id=user.id, job_id=job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Generation job not found.",
        )
    return job


@router.post(
    "/generate",
    response_model=MetadataGenerationResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def trigger_generation(user: UserProfile = Depends(get_current_user_dep)):
    """Enqueue a metadata-generation run for the authenticated user.

    Creates a ``pending`` job row immediately, then best-effort starts the
    ACA execution. The container patches the row with progress + final
    status. ACA failures are logged but do not surface to the caller — the
    pending row remains useful for retries.
    """
    job = metadata_service.create_job(user_id=user.id)

    try:
        from fastapi_app.services import metadata_job_trigger  # noqa: PLC0415

        metadata_job_trigger.start_job(user_id=user.id, job_id=job.id)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to start ACA metadata job for user %s", user.id)

    # Re-fetch so the response reflects any updates the trigger made
    # (aca_execution_name, message, etc.).
    refreshed = metadata_service.get_job(user_id=user.id, job_id=job.id) or job
    return MetadataGenerationResponse(job=refreshed)
