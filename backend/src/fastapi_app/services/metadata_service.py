"""
CRUD for the dashboard-filter metadata layer (Supabase-backed).

All reads/writes are scoped by ``user_id`` (= tenant_id) using the admin
client; never trust a client-supplied user id — pass the authenticated
``UserProfile.id`` from the JWT.

Patch operations preserve LLM-generated rows from being overwritten by
later regeneration runs by setting ``edited_by_user = TRUE``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi_app.models.metadata import (
    ColumnMetadata,
    ColumnMetadataPatch,
    MetadataJob,
    MetadataJobStatus,
    Relationship,
    RelationshipCreate,
    RelationshipKind,
    RelationshipPatch,
    SemanticType,
    TableMetadata,
    TableMetadataPatch,
)
from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)

_TABLES_TBL = "tenant_table_metadata"
_COLUMNS_TBL = "tenant_column_metadata"
_RELATIONSHIPS_TBL = "tenant_table_relationships"
_JOBS_TBL = "tenant_metadata_jobs"

# ---------------------------------------------------------------------------
# Per-tenant derived maps (TTL cache)
#
# Dashboard hydration calls look up filterable columns and primary date
# columns on every request. The rows themselves are small but fetching them
# from Supabase on every widget hit is wasteful, so we cache the derived
# maps per-tenant for a short TTL. Regeneration of metadata is infrequent
# (user-triggered ACA job), so a 60s TTL is a safe balance.
# ---------------------------------------------------------------------------

_DERIVED_CACHE_TTL_SECONDS = 60.0
_FILTERABLE_COLUMNS_CACHE: dict[str, tuple[dict[str, frozenset[str]], float]] = {}
_DATE_COLUMNS_CACHE: dict[str, tuple[dict[str, str], float]] = {}


def _now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


def invalidate_tenant_derived_cache(user_id: str) -> None:
    """Drop cached filterable/date maps for *user_id*.

    Call this after metadata regeneration or user edits so the next
    hydration sees the updated allowlist immediately.
    """
    _FILTERABLE_COLUMNS_CACHE.pop(user_id, None)
    _DATE_COLUMNS_CACHE.pop(user_id, None)


def get_filterable_columns_map(
    *,
    user_id: str,
    use_cache: bool = True,
) -> dict[str, frozenset[str]]:
    """Return ``{table_name: frozenset[column_name]}`` of filterable columns.

    A column is considered filterable when ``is_filterable = TRUE`` **and**
    its ``semantic_type`` is one of ``categorical``, ``numeric``, ``temporal``,
    or ``measure`` (identifier / unknown are excluded).

    Returns an empty dict when the tenant has no metadata yet — callers are
    expected to fall back to their legacy allowlist in that case.
    """
    if use_cache:
        cached = _FILTERABLE_COLUMNS_CACHE.get(user_id)
        if cached is not None:
            value, ts = cached
            if _now_epoch() - ts < _DERIVED_CACHE_TTL_SECONDS:
                return value

    client = get_supabase_admin_client()
    res = (
        client.table(_COLUMNS_TBL)
        .select("table_name,column_name,semantic_type,is_filterable")
        .eq("user_id", user_id)
        .eq("is_filterable", True)
        .execute()
    )
    allowed_sem = {
        SemanticType.categorical.value,
        SemanticType.numeric.value,
        SemanticType.temporal.value,
        SemanticType.measure.value,
    }
    buckets: dict[str, set[str]] = {}
    for row in res.data or []:
        if row.get("semantic_type") not in allowed_sem:
            continue
        buckets.setdefault(row["table_name"], set()).add(row["column_name"])
    result = {t: frozenset(cols) for t, cols in buckets.items()}
    _FILTERABLE_COLUMNS_CACHE[user_id] = (result, _now_epoch())
    return result


def get_date_columns_map(
    *,
    user_id: str,
    use_cache: bool = True,
) -> dict[str, str]:
    """Return ``{table_name: primary_date_column}`` for tables that have one.

    Prefers ``tenant_table_metadata.primary_date_column`` when set. For
    tables without a curated primary, falls back to the first column with
    ``semantic_type = 'temporal'``.
    """
    if use_cache:
        cached = _DATE_COLUMNS_CACHE.get(user_id)
        if cached is not None:
            value, ts = cached
            if _now_epoch() - ts < _DERIVED_CACHE_TTL_SECONDS:
                return value

    client = get_supabase_admin_client()
    tables_res = (
        client.table(_TABLES_TBL)
        .select("table_name,primary_date_column")
        .eq("user_id", user_id)
        .execute()
    )
    result: dict[str, str] = {}
    missing_tables: list[str] = []
    for row in tables_res.data or []:
        col = row.get("primary_date_column")
        if col:
            result[row["table_name"]] = col
        else:
            missing_tables.append(row["table_name"])

    if missing_tables:
        cols_res = (
            client.table(_COLUMNS_TBL)
            .select("table_name,column_name,semantic_type")
            .eq("user_id", user_id)
            .eq("semantic_type", SemanticType.temporal.value)
            .in_("table_name", missing_tables)
            .order("table_name")
            .order("column_name")
            .execute()
        )
        for row in cols_res.data or []:
            t = row["table_name"]
            if t not in result:
                result[t] = row["column_name"]

    _DATE_COLUMNS_CACHE[user_id] = (result, _now_epoch())
    return result


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


def list_table_metadata(*, user_id: str) -> list[TableMetadata]:
    """All ``tenant_table_metadata`` rows for *user_id*, ordered by table_name."""
    client = get_supabase_admin_client()
    res = (
        client.table(_TABLES_TBL)
        .select("*")
        .eq("user_id", user_id)
        .order("table_name")
        .execute()
    )
    return [TableMetadata(**row) for row in (res.data or [])]


def patch_table_metadata(
    *,
    user_id: str,
    table_name: str,
    patch: TableMetadataPatch,
) -> TableMetadata | None:
    """Update a single table metadata row; flags it as user-edited."""
    payload = patch.model_dump(exclude_unset=True)
    if not payload:
        rows = (
            get_supabase_admin_client()
            .table(_TABLES_TBL)
            .select("*")
            .eq("user_id", user_id)
            .eq("table_name", table_name)
            .limit(1)
            .execute()
            .data
        )
        return TableMetadata(**rows[0]) if rows else None

    payload["edited_by_user"] = True
    client = get_supabase_admin_client()
    res = (
        client.table(_TABLES_TBL)
        .update(payload)
        .eq("user_id", user_id)
        .eq("table_name", table_name)
        .execute()
    )
    rows = res.data or []
    if rows:
        invalidate_tenant_derived_cache(user_id)
    return TableMetadata(**rows[0]) if rows else None


# ---------------------------------------------------------------------------
# Columns
# ---------------------------------------------------------------------------


def list_column_metadata(
    *,
    user_id: str,
    table_name: str | None = None,
) -> list[ColumnMetadata]:
    """All ``tenant_column_metadata`` rows for *user_id* (optionally one table)."""
    client = get_supabase_admin_client()
    q = client.table(_COLUMNS_TBL).select("*").eq("user_id", user_id)
    if table_name is not None:
        q = q.eq("table_name", table_name)
    res = q.order("table_name").order("column_name").execute()
    return [ColumnMetadata(**row) for row in (res.data or [])]


def patch_column_metadata(
    *,
    user_id: str,
    table_name: str,
    column_name: str,
    patch: ColumnMetadataPatch,
) -> ColumnMetadata | None:
    """Update a single column metadata row; flags it as user-edited."""
    payload = patch.model_dump(exclude_unset=True)
    if "semantic_type" in payload and isinstance(payload["semantic_type"], SemanticType):
        payload["semantic_type"] = payload["semantic_type"].value
    if not payload:
        rows = (
            get_supabase_admin_client()
            .table(_COLUMNS_TBL)
            .select("*")
            .eq("user_id", user_id)
            .eq("table_name", table_name)
            .eq("column_name", column_name)
            .limit(1)
            .execute()
            .data
        )
        return ColumnMetadata(**rows[0]) if rows else None

    payload["edited_by_user"] = True
    client = get_supabase_admin_client()
    res = (
        client.table(_COLUMNS_TBL)
        .update(payload)
        .eq("user_id", user_id)
        .eq("table_name", table_name)
        .eq("column_name", column_name)
        .execute()
    )
    rows = res.data or []
    if rows:
        invalidate_tenant_derived_cache(user_id)
    return ColumnMetadata(**rows[0]) if rows else None


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------


def list_relationships(*, user_id: str) -> list[Relationship]:
    """All relationship edges for *user_id*."""
    client = get_supabase_admin_client()
    res = (
        client.table(_RELATIONSHIPS_TBL)
        .select("*")
        .eq("user_id", user_id)
        .order("from_table")
        .order("from_column")
        .execute()
    )
    return [Relationship(**row) for row in (res.data or [])]


def create_relationship(
    *,
    user_id: str,
    body: RelationshipCreate,
) -> Relationship:
    """Insert (or overwrite) a user-defined relationship edge."""
    payload = body.model_dump()
    payload["kind"] = body.kind.value
    payload["user_id"] = user_id
    payload["edited_by_user"] = True
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()

    client = get_supabase_admin_client()
    res = (
        client.table(_RELATIONSHIPS_TBL)
        .upsert(
            payload,
            on_conflict="user_id,from_table,from_column,to_table,to_column",
        )
        .execute()
    )
    rows = res.data or []
    if not rows:
        # Some Supabase versions don't return rows on upsert without RETURNING; re-fetch.
        rows = (
            client.table(_RELATIONSHIPS_TBL)
            .select("*")
            .eq("user_id", user_id)
            .eq("from_table", body.from_table)
            .eq("from_column", body.from_column)
            .eq("to_table", body.to_table)
            .eq("to_column", body.to_column)
            .limit(1)
            .execute()
            .data
            or []
        )
    return Relationship(**rows[0])


def patch_relationship(
    *,
    user_id: str,
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    patch: RelationshipPatch,
) -> Relationship | None:
    """Update an existing relationship edge; flags it as user-edited."""
    payload = patch.model_dump(exclude_unset=True)
    if "kind" in payload and isinstance(payload["kind"], RelationshipKind):
        payload["kind"] = payload["kind"].value
    if not payload:
        return None
    payload["edited_by_user"] = True

    client = get_supabase_admin_client()
    res = (
        client.table(_RELATIONSHIPS_TBL)
        .update(payload)
        .eq("user_id", user_id)
        .eq("from_table", from_table)
        .eq("from_column", from_column)
        .eq("to_table", to_table)
        .eq("to_column", to_column)
        .execute()
    )
    rows = res.data or []
    return Relationship(**rows[0]) if rows else None


def delete_relationship(
    *,
    user_id: str,
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
) -> bool:
    """Delete an edge. Returns True iff a row was removed."""
    client = get_supabase_admin_client()
    res = (
        client.table(_RELATIONSHIPS_TBL)
        .delete()
        .eq("user_id", user_id)
        .eq("from_table", from_table)
        .eq("from_column", from_column)
        .eq("to_table", to_table)
        .eq("to_column", to_column)
        .execute()
    )
    return bool(res.data)


# ---------------------------------------------------------------------------
# Generation jobs
# ---------------------------------------------------------------------------


def get_latest_job(*, user_id: str) -> MetadataJob | None:
    """Return the most recently created job row for *user_id* (or None)."""
    client = get_supabase_admin_client()
    res = (
        client.table(_JOBS_TBL)
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return MetadataJob(**rows[0]) if rows else None


def get_job(*, user_id: str, job_id: str) -> MetadataJob | None:
    """Return a single job row scoped to *user_id*."""
    client = get_supabase_admin_client()
    res = (
        client.table(_JOBS_TBL)
        .select("*")
        .eq("user_id", user_id)
        .eq("id", job_id)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return MetadataJob(**rows[0]) if rows else None


def create_job(
    *,
    user_id: str,
    aca_execution_name: str | None = None,
) -> MetadataJob:
    """Insert a new ``pending`` job row and return it."""
    payload: dict[str, Any] = {
        "user_id": user_id,
        "status": MetadataJobStatus.pending.value,
        "progress": 0,
    }
    if aca_execution_name is not None:
        payload["aca_execution_name"] = aca_execution_name

    client = get_supabase_admin_client()
    res = client.table(_JOBS_TBL).insert(payload).execute()
    rows = res.data or []
    if not rows:
        raise RuntimeError("Failed to create metadata generation job row.")
    job = MetadataJob(**rows[0])
    logger.info(
        "event=metadata_job_created job_id=%s user_id=%s",
        job.id, user_id,
    )
    return job


def update_job(
    *,
    user_id: str,
    job_id: str,
    status: MetadataJobStatus | None = None,
    progress: float | None = None,
    message: str | None = None,
    error: str | None = None,
    aca_execution_name: str | None = None,
    mark_started: bool = False,
    mark_finished: bool = False,
) -> MetadataJob | None:
    """Patch a job row. Service-role only (RLS bypass)."""
    payload: dict[str, Any] = {}
    if status is not None:
        payload["status"] = status.value
    if progress is not None:
        payload["progress"] = progress
    if message is not None:
        payload["message"] = message
    if error is not None:
        payload["error"] = error
    if aca_execution_name is not None:
        payload["aca_execution_name"] = aca_execution_name
    now_iso = datetime.now(timezone.utc).isoformat()
    if mark_started:
        payload["started_at"] = now_iso
    if mark_finished:
        payload["finished_at"] = now_iso

    if not payload:
        return get_job(user_id=user_id, job_id=job_id)

    client = get_supabase_admin_client()
    res = (
        client.table(_JOBS_TBL)
        .update(payload)
        .eq("user_id", user_id)
        .eq("id", job_id)
        .execute()
    )
    rows = res.data or []
    if not rows:
        return None
    job = MetadataJob(**rows[0])
    if status is not None:
        logger.info(
            "event=metadata_job_status_change job_id=%s status=%s progress=%.2f user_id=%s",
            job_id, status.value, job.progress, user_id,
        )
    return job
