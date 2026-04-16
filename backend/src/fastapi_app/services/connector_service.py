"""
Connector catalog + per-user connector configuration (Supabase).

All mutations are scoped with ``user_id`` from the validated JWT. The service
uses the **admin** client and explicit ``user_id`` filters — never trust a
client-supplied user id.
"""

from __future__ import annotations

import io
import logging
import re
import zipfile
import math
import os
import tempfile
from datetime import date, datetime
from typing import Literal
from decimal import Decimal
from typing import Any

import duckdb

from azure.storage.blob import BlobServiceClient
from fastapi import HTTPException, status

from fastapi_app.models.connectors import (
    UserConnectorConfigCreate,
    UserConnectorConfigUpdate,
)
from fastapi_app.settings import (
    AZURE_STORAGE_CONNECTION_STRING,
    AZURE_STORAGE_CONTAINER,
    USER_DATA_BLOB_PREFIX,
)
from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)

_TABLE = "user_connector_configs"
_CATALOG = "connector_schemas"

# List endpoint columns only — keeps payloads small (no config_schema / oauth_config).
_CATALOG_LIST_COLUMNS = (
    "id,name,docker_repository,docker_image_tag,icon_url,documentation_url,"
    "is_active,created_at,updated_at"
)

_CATALOG_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_PARQUET_MONTH_RE = re.compile(r"^\d{4}-\d{2}\.parquet$", re.IGNORECASE)

# Cap long strings in preview JSON (keeps payloads bounded).
_PREVIEW_CELL_MAX_CHARS = 4000


def _preview_json_cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, str):
        if len(value) <= _PREVIEW_CELL_MAX_CHARS:
            return value
        return value[:_PREVIEW_CELL_MAX_CHARS] + "…"
    if isinstance(value, bytes):
        s = value.decode("utf-8", errors="replace")
        if len(s) <= _PREVIEW_CELL_MAX_CHARS:
            return s
        return s[:_PREVIEW_CELL_MAX_CHARS] + "…"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    s = str(value)
    if len(s) <= _PREVIEW_CELL_MAX_CHARS:
        return s
    return s[:_PREVIEW_CELL_MAX_CHARS] + "…"


def preview_raw_stream_table(
    user_id: str,
    config_id: str,
    stream_name: str,
    start: date,
    end: date,
    *,
    limit: int,
    offset: int,
) -> dict[str, Any] | None:
    """Read Parquet month files in range and return paginated rows as JSON-safe lists."""
    row = get_user_connector(user_id, config_id)
    if not row:
        return None
    streams, blob_prefix = _stream_names_for_row(user_id, row)
    if stream_name not in set(streams):
        return None
    if not AZURE_STORAGE_CONNECTION_STRING:
        return None
    try:
        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(AZURE_STORAGE_CONTAINER)
    except Exception:
        logger.exception("Could not initialize Azure Blob client for preview")
        return None

    months = _list_stream_months_in_range(
        container, blob_prefix, stream_name, start, end
    )
    if not months:
        return {
            "columns": [],
            "rows": [],
            "limit": limit,
            "offset": offset,
            "has_more": False,
            "months_included": [],
        }

    with tempfile.TemporaryDirectory() as td:
        paths: list[str] = []
        month_labels: list[str] = []
        for m in months:
            month = m["month"]
            blob_name = f"{blob_prefix}/{stream_name}/{month}.parquet"
            data = container.download_blob(blob_name).readall()
            path = os.path.join(td, f"{month}.parquet")
            with open(path, "wb") as fh:
                fh.write(data)
            paths.append(path.replace("\\", "/"))
            month_labels.append(month)

        # DuckDB reads a list of parquet files as a combined table.
        con = duckdb.connect(database=":memory:")
        try:
            lim = max(1, min(limit + 1, 501))
            off = max(0, offset)
            result = con.execute(
                "SELECT * FROM read_parquet(?) LIMIT ? OFFSET ?",
                [paths, lim, off],
            )
        except Exception as exc:
            logger.exception("DuckDB read_parquet failed")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Could not read Parquet data for preview.",
            ) from exc

        desc = result.description
        columns = [d[0] for d in desc] if desc else []
        raw_rows = result.fetchall()
        has_more = len(raw_rows) > limit
        page = raw_rows[:limit]
        rows_out: list[list[Any]] = []
        for tup in page:
            rows_out.append([_preview_json_cell(c) for c in tup])

    return {
        "columns": columns,
        "rows": rows_out,
        "limit": limit,
        "offset": offset,
        "has_more": has_more,
        "months_included": month_labels,
    }


def _extract_connector_folder(row: dict[str, Any]) -> str:
    """Match sync worker folder naming (source extracted from docker image/repo)."""
    docker_image = str(row.get("docker_image") or "").strip()
    if docker_image:
        repo = docker_image.split(":", 1)[0]
        last = repo.rsplit("/", 1)[-1].strip()
        if last:
            return last
    docker_repository = str(row.get("docker_repository") or "").strip()
    if docker_repository:
        last = docker_repository.rsplit("/", 1)[-1].strip()
        if last:
            return last
    fallback = str(row.get("connector_name") or "connector").strip().lower()
    fallback = re.sub(r"[^a-z0-9._-]+", "-", fallback).strip("-")
    return fallback or "connector"


def _candidate_prefixes(user_id: str, connector_folder: str) -> list[str]:
    base = USER_DATA_BLOB_PREFIX.strip("/")
    candidates = [f"{base}/{user_id}/{connector_folder}"] if base else []
    # Backward/ops-safe fallback to the documented worker path.
    fallback = f"user-data/{user_id}/{connector_folder}"
    if fallback not in candidates:
        candidates.append(fallback)
    return candidates


def _discover_stream_names(prefixes: list[str]) -> tuple[list[str], str | None]:
    if not AZURE_STORAGE_CONNECTION_STRING:
        return [], None
    try:
        service = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
        container = service.get_container_client(AZURE_STORAGE_CONTAINER)
    except Exception:
        logger.exception("Could not initialize Azure Blob client for synced tables")
        return [], None

    seen: set[str] = set()
    matched_prefix: str | None = None
    for prefix in prefixes:
        try:
            for blob in container.list_blobs(name_starts_with=f"{prefix}/"):
                name = str(blob.name or "")
                rel = name[len(prefix) + 1 :]
                parts = rel.split("/")
                if len(parts) != 2:
                    continue
                stream_name, month_file = parts
                if not stream_name or not _PARQUET_MONTH_RE.match(month_file):
                    continue
                seen.add(stream_name)
                matched_prefix = prefix
        except Exception:
            logger.exception(
                "Failed blob list for prefix '%s' in container '%s'",
                prefix,
                AZURE_STORAGE_CONTAINER,
            )
    return sorted(seen), matched_prefix


def _iter_months_in_range(start: date, end: date) -> list[str]:
    """Chronological YYYY-MM strings for calendar months overlapping [start, end]."""
    out: list[str] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(f"{y:04d}-{m:02d}")
        if m == 12:
            m = 1
            y += 1
        else:
            m += 1
    return out


def _list_stream_months_in_range(
    container: Any,
    blob_prefix: str,
    stream: str,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    wanted = set(_iter_months_in_range(start, end))
    prefix_path = f"{blob_prefix}/{stream}/"
    out: list[dict[str, Any]] = []
    try:
        for blob in container.list_blobs(name_starts_with=prefix_path):
            name = str(blob.name or "")
            if not name.startswith(prefix_path):
                continue
            rel = name[len(prefix_path) :]
            if "/" in rel:
                continue
            if not _PARQUET_MONTH_RE.match(rel):
                continue
            month = rel[:7]
            if month not in wanted:
                continue
            sz = getattr(blob, "size", None)
            out.append(
                {
                    "month": month,
                    "size_bytes": int(sz) if sz is not None else None,
                }
            )
    except Exception:
        logger.exception("Failed listing blobs for stream %s", stream)
    out.sort(key=lambda x: x["month"])
    return out


def _stream_names_for_row(user_id: str, row: dict[str, Any]) -> tuple[list[str], str]:
    """Return (stream names, blob prefix used for listing) for a connector row."""
    connector_folder = _extract_connector_folder(row)
    prefixes = _candidate_prefixes(user_id, connector_folder)
    discovered, matched_prefix = _discover_stream_names(prefixes)
    selected = row.get("selected_streams") or []
    synced_tables = discovered or [s for s in selected if isinstance(s, str)]
    prefix = matched_prefix or prefixes[0]
    return synced_tables, prefix


def build_stream_parquet_zip(
    user_id: str,
    config_id: str,
    stream_name: str,
    start: date,
    end: date,
) -> tuple[bytes, str] | None:
    """Zip Parquet month files for a stream in [start, end]; None if nothing to export."""
    row = get_user_connector(user_id, config_id)
    if not row:
        return None
    streams, blob_prefix = _stream_names_for_row(user_id, row)
    if stream_name not in set(streams):
        return None
    if not AZURE_STORAGE_CONNECTION_STRING:
        return None
    try:
        service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container = service.get_container_client(AZURE_STORAGE_CONTAINER)
    except Exception:
        logger.exception("Could not initialize Azure Blob client for download")
        return None

    months = _list_stream_months_in_range(
        container, blob_prefix, stream_name, start, end
    )
    if not months:
        return None

    buf = io.BytesIO()
    safe_stream = re.sub(r"[^a-zA-Z0-9._-]+", "_", stream_name).strip("_")[:80] or "stream"
    arc_prefix = f"{safe_stream}_{start.isoformat()}_{end.isoformat()}"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for m in months:
            month = m["month"]
            blob_name = f"{blob_prefix}/{stream_name}/{month}.parquet"
            data = container.download_blob(blob_name).readall()
            zf.writestr(f"{arc_prefix}/{month}.parquet", data)
    buf.seek(0)
    filename = f"{arc_prefix}.zip"
    return buf.read(), filename


def list_connector_catalog(
    *,
    search: str | None = None,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    """Return connector definitions from ``connector_schemas`` (read-only, slim rows)."""
    client = get_supabase_admin_client()
    q = client.table(_CATALOG).select(_CATALOG_LIST_COLUMNS)
    if active_only:
        q = q.eq("is_active", True)
    rows = (q.order("name").execute()).data or []
    if search:
        s = search.strip().lower()
        rows = [
            r
            for r in rows
            if s in (r.get("name") or "").lower()
            or s in (r.get("docker_repository") or "").lower()
        ]
    return rows


def get_connector_catalog_detail(*, identifier: str) -> dict[str, Any] | None:
    """Fetch one catalog row by UUID ``id`` or exact ``docker_repository`` match."""
    ident = identifier.strip()
    if not ident:
        return None
    client = get_supabase_admin_client()
    if _CATALOG_UUID.match(ident):
        rows = (
            client.table(_CATALOG)
            .select("*")
            .eq("id", ident)
            .limit(1)
            .execute()
        ).data
    else:
        rows = (
            client.table(_CATALOG)
            .select("*")
            .eq("docker_repository", ident)
            .limit(1)
            .execute()
        ).data
    return rows[0] if rows else None


def list_user_connectors(user_id: str) -> list[dict[str, Any]]:
    """List all connector configs for the authenticated user."""
    client = get_supabase_admin_client()
    return (
        client.table(_TABLE)
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    ).data or []


def get_user_connector(user_id: str, config_id: str) -> dict[str, Any] | None:
    client = get_supabase_admin_client()
    rows = (
        client.table(_TABLE)
        .select("*")
        .eq("id", config_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    ).data
    return rows[0] if rows else None


def create_user_connector(
    user_id: str, body: UserConnectorConfigCreate
) -> dict[str, Any]:
    client = get_supabase_admin_client()
    payload = body.model_dump(exclude_none=True)
    payload["user_id"] = user_id
    try:
        res = client.table(_TABLE).insert(payload).execute()
    except Exception as exc:
        logger.exception("Insert user_connector_configs failed")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create connector configuration.",
        ) from exc
    data = res.data
    if not data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create connector configuration.",
        )
    return data[0]


def update_user_connector(
    user_id: str, config_id: str, body: UserConnectorConfigUpdate
) -> dict[str, Any]:
    patch = body.model_dump(exclude_none=True)
    if not patch:
        existing = get_user_connector(user_id, config_id)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Connector configuration not found.",
            )
        return existing

    client = get_supabase_admin_client()
    try:
        res = (
            client.table(_TABLE)
            .update(patch)
            .eq("id", config_id)
            .eq("user_id", user_id)
            .execute()
        )
    except Exception as exc:
        logger.exception("Update user_connector_configs failed")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not update connector configuration.",
        ) from exc

    data = res.data
    if not data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connector configuration not found.",
        )
    return data[0]


def get_active_user_connector_by_repository(
    user_id: str, docker_repository: str
) -> dict[str, Any] | None:
    """Return the active config row for this user and connector repo, if any."""
    client = get_supabase_admin_client()
    rows = (
        client.table(_TABLE)
        .select("*")
        .eq("user_id", user_id)
        .eq("docker_repository", docker_repository)
        .eq("is_active", True)
        .limit(1)
        .execute()
    ).data
    return rows[0] if rows else None


def upsert_user_connector_onboarding(
    user_id: str,
    *,
    connector_name: str,
    docker_repository: str,
    docker_image: str,
    config: dict[str, Any],
    oauth_meta: dict[str, Any] | None = None,
    selected_streams: list[str] | None = None,
    discovered_catalog: dict[str, Any] | None = None,
    sync_mode: Literal["one_off", "recurring"] = "recurring",
    sync_frequency_minutes: int = 360,
    sync_validated: bool = False,
    is_active: bool = True,
    incremental_enabled: bool = False,
) -> dict[str, Any]:
    """Create or update the **active** row for (user_id, docker_repository)."""
    existing = get_active_user_connector_by_repository(user_id, docker_repository)
    if existing:
        patch = UserConnectorConfigUpdate(
            connector_name=connector_name,
            docker_image=docker_image,
            config=config,
            oauth_meta=oauth_meta,
            selected_streams=selected_streams,
            sync_mode=sync_mode,
            sync_frequency_minutes=sync_frequency_minutes,
            sync_validated=sync_validated,
            is_active=is_active,
            incremental_enabled=incremental_enabled,
        )
        result = update_user_connector(user_id, existing["id"], patch)
        # Save discovered catalog separately (not in the Pydantic model)
        if discovered_catalog:
            client = get_supabase_admin_client()
            client.table(_TABLE).update(
                {"discovered_catalog": discovered_catalog}
            ).eq("id", existing["id"]).execute()
        return result

    body = UserConnectorConfigCreate(
        connector_name=connector_name,
        docker_repository=docker_repository,
        docker_image=docker_image,
        config=config,
        oauth_meta=oauth_meta,
        selected_streams=selected_streams,
        sync_mode=sync_mode,
        sync_frequency_minutes=sync_frequency_minutes,
        is_active=is_active,
        incremental_enabled=incremental_enabled,
    )
    result = create_user_connector(user_id, body)
    # Save discovered catalog separately (not in the Pydantic model)
    if discovered_catalog and result.get("id"):
        client = get_supabase_admin_client()
        client.table(_TABLE).update(
            {"discovered_catalog": discovered_catalog}
        ).eq("id", result["id"]).execute()
    return result


def delete_user_connector(user_id: str, config_id: str) -> bool:
    client = get_supabase_admin_client()
    res = (
        client.table(_TABLE)
        .delete()
        .eq("id", config_id)
        .eq("user_id", user_id)
        .execute()
    )
    return bool(res.data)


def list_synced_tables_metadata(
    user_id: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict[str, Any]]:
    """Derive sync metadata for View-raw-tables from connector rows + blob paths."""
    rows = list_user_connectors(user_id)
    out: list[dict[str, Any]] = []
    want_inventory = start_date is not None and end_date is not None
    container = None
    if want_inventory and AZURE_STORAGE_CONNECTION_STRING:
        try:
            service = BlobServiceClient.from_connection_string(
                AZURE_STORAGE_CONNECTION_STRING
            )
            container = service.get_container_client(AZURE_STORAGE_CONTAINER)
        except Exception:
            logger.exception("Could not initialize Azure Blob for stream inventory")
            container = None

    for r in rows:
        connector_folder = _extract_connector_folder(r)
        prefixes = _candidate_prefixes(user_id, connector_folder)
        discovered, matched_prefix = _discover_stream_names(prefixes)
        selected = r.get("selected_streams") or []
        synced_tables = discovered or [s for s in selected if isinstance(s, str)]
        prefix = matched_prefix or prefixes[0]
        entry: dict[str, Any] = {
            "connector_config_id": r["id"],
            "docker_repository": r.get("docker_repository", ""),
            "connector_name": r.get("connector_name", ""),
            "last_sync_at": r.get("last_sync_at"),
            "last_sync_status": r.get("last_sync_status", "pending"),
            "last_sync_error": r.get("last_sync_error"),
            "data_prefix_hint": f"{AZURE_STORAGE_CONTAINER}/{prefix}",
            "synced_tables": synced_tables,
        }
        if want_inventory and start_date and end_date:
            inv: list[dict[str, Any]] = []
            if container is not None:
                for stream in synced_tables:
                    months = _list_stream_months_in_range(
                        container, prefix, stream, start_date, end_date
                    )
                    inv.append({"stream": stream, "months": months})
            else:
                inv = [{"stream": s, "months": []} for s in synced_tables]
            entry["stream_inventory"] = inv
        out.append(entry)
    return out
