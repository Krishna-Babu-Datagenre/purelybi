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
import threading
import time
import zipfile
import math
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Literal
from decimal import Decimal
from typing import Any

import duckdb

from azure.storage.blob import BlobServiceClient
from fastapi import HTTPException, status, UploadFile

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

# Matches Hive partition directory segments like "year=2026" or "month=04".
_HIVE_YEAR_RE = re.compile(r"year=(\d{4})")
_HIVE_MONTH_RE = re.compile(r"month=(\d{1,2})")

# Cap long strings in preview JSON (keeps payloads bounded).
_PREVIEW_CELL_MAX_CHARS = 4000

# ---------------------------------------------------------------------------
# Blob client singleton + stream name cache
# ---------------------------------------------------------------------------
_blob_container_client: Any = None
_blob_container_lock = threading.Lock()

_STREAM_CACHE: dict[str, tuple[list[str], str | None, float]] = {}
_STREAM_CACHE_TTL = 300.0  # 5 min


def _get_blob_container() -> Any:
    """Lazily-created module-level ContainerClient (avoids per-request overhead)."""
    global _blob_container_client
    if _blob_container_client is not None:
        return _blob_container_client
    if not AZURE_STORAGE_CONNECTION_STRING:
        return None
    with _blob_container_lock:
        if _blob_container_client is not None:
            return _blob_container_client
        try:
            service = BlobServiceClient.from_connection_string(
                AZURE_STORAGE_CONNECTION_STRING
            )
            _blob_container_client = service.get_container_client(
                AZURE_STORAGE_CONTAINER
            )
        except Exception:
            logger.exception("Could not initialize Azure Blob client singleton")
            return None
        return _blob_container_client


def invalidate_stream_cache() -> None:
    """Clear cached stream names (call after sync completes)."""
    _STREAM_CACHE.clear()


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
    container = _get_blob_container()
    if container is None:
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
        for i, m in enumerate(months):
            month = m["month"]
            blob_name = m["blob_name"]
            data = container.download_blob(blob_name).readall()
            path = os.path.join(td, f"{i}_{month}.parquet")
            with open(path, "wb") as fh:
                fh.write(data)
            paths.append(path.replace("\\", "/"))
            if month not in month_labels:
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
    docker_repository = str(row.get("docker_repository") or "").strip()
    if docker_repository == "local/file-upload":
        fallback = str(row.get("connector_name") or "local-file").strip().lower()
        fallback = re.sub(r"[^a-z0-9._-]+", "-", fallback).strip("-")
        return fallback or "local-file"

    docker_image = str(row.get("docker_image") or "").strip()
    if docker_image:
        repo = docker_image.split(":", 1)[0]
        last = repo.rsplit("/", 1)[-1].strip()
        if last:
            return last
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
    cache_key = prefixes[0] if prefixes else ""
    if cache_key:
        cached = _STREAM_CACHE.get(cache_key)
        if cached is not None:
            streams, matched, ts = cached
            if time.monotonic() - ts < _STREAM_CACHE_TTL:
                return list(streams), matched

    container = _get_blob_container()
    if container is None:
        return [], None

    seen: set[str] = set()
    matched_prefix: str | None = None
    for prefix in prefixes:
        try:
            for blob in container.list_blobs(name_starts_with=f"{prefix}/"):
                name = str(blob.name or "")
                if not name.endswith(".parquet"):
                    continue
                rel = name[len(prefix) + 1 :]
                parts = rel.split("/")
                stream_name = parts[0] if parts else ""
                if not stream_name:
                    continue
                seen.add(stream_name)
                matched_prefix = prefix
        except Exception:
            logger.exception(
                "Failed blob list for prefix '%s' in container '%s'",
                prefix,
                AZURE_STORAGE_CONTAINER,
            )
    result = sorted(seen)
    if cache_key:
        _STREAM_CACHE[cache_key] = (result, matched_prefix, time.monotonic())
    return result, matched_prefix


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
    """List parquet files for a stream, filtered by a date range.

    Handles three blob layouts:
      1. Full-refresh: ``{prefix}/{stream}/part{N}.parquet``  (no date → always included)
      2. Hive:         ``{prefix}/{stream}/year=YYYY/month=MM/*.parquet``

    Returns a list of dicts with ``blob_name``, ``month`` (``YYYY-MM`` or
    ``"unpartitioned"``), and ``size_bytes``.
    """
    wanted = set(_iter_months_in_range(start, end))
    prefix_path = f"{blob_prefix}/{stream}/"
    out: list[dict[str, Any]] = []
    try:
        for blob in container.list_blobs(name_starts_with=prefix_path):
            name = str(blob.name or "")
            if not name.endswith(".parquet"):
                continue
            rel = name[len(prefix_path):]
            sz = getattr(blob, "size", None)

            # 1. Hive-partitioned: year=YYYY/month=MM/...
            ym = _HIVE_YEAR_RE.search(rel)
            mm = _HIVE_MONTH_RE.search(rel)
            if ym and mm:
                month = f"{ym.group(1)}-{int(mm.group(1)):02d}"
                if month not in wanted:
                    continue
                out.append({
                    "month": month,
                    "blob_name": name,
                    "size_bytes": int(sz) if sz is not None else None,
                })
                continue

            # 2. Full-refresh part files (no date info) — always include
            if rel.endswith(".parquet"):
                out.append({
                    "month": "unpartitioned",
                    "blob_name": name,
                    "size_bytes": int(sz) if sz is not None else None,
                })

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
    container = _get_blob_container()
    if container is None:
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
        for i, m in enumerate(months):
            month = m["month"]
            blob_name = m["blob_name"]
            data = container.download_blob(blob_name).readall()
            # Use index prefix to avoid name collisions across part files
            arc_name = blob_name.rsplit("/", 1)[-1]
            zf.writestr(f"{arc_prefix}/{i:04d}_{arc_name}", data)
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


def _build_connector_metadata_entry(
    r: dict[str, Any],
    user_id: str,
    want_inventory: bool,
    start_date: date | None,
    end_date: date | None,
    container: Any,
) -> dict[str, Any]:
    """Build the metadata dict for a single connector row (designed for thread-pool use)."""
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
    return entry


def list_synced_tables_metadata(
    user_id: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict[str, Any]]:
    """Derive sync metadata for View-raw-tables from connector rows + blob paths.

    Each connector's blob discovery runs in a thread-pool so that multiple
    connectors do not serialize their Azure Blob list() round-trips.
    """
    rows = list_user_connectors(user_id)
    if not rows:
        return []

    want_inventory = start_date is not None and end_date is not None
    container = _get_blob_container() if want_inventory else None

    # Fan out one task per connector; preserve original ordering via submitted
    # order rather than as_completed so the UI list is stable.
    max_workers = min(len(rows), 8)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _build_connector_metadata_entry,
                r, user_id, want_inventory, start_date, end_date, container,
            ): idx
            for idx, r in enumerate(rows)
        }
        results: list[dict[str, Any]] = [{}] * len(rows)
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception:
                logger.exception(
                    "Failed building metadata for connector row idx=%d", idx
                )
    # Filter out any empty placeholders from failures
    return [r for r in results if r]


async def preview_local_file(file: UploadFile) -> dict[str, Any] | None:
    """Read a local file upload and return the first 50 rows as JSON-safe arrays."""
    if not file.filename:
        return None
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
        tmp.write(await file.read())
        temp_path = tmp.name
        
    try:
        columns = []
        rows = []
        if file.filename.lower().endswith(('.xlsx', '.xls')):
            import pandas as pd
            df = pd.read_excel(temp_path, nrows=50)
            columns = list(df.columns)
            # handle NaNs gracefully
            df = df.where(pd.notnull(df), None)
            raw_rows = df.values.tolist()
            rows = [[_preview_json_cell(c) for c in r] for r in raw_rows]
        else:
            con = duckdb.connect(database=":memory:")
            # Use appropriate read function
            if file.filename.lower().endswith('.json'):
                query = f"SELECT * FROM read_json_auto('{temp_path}') LIMIT 50"
            elif file.filename.lower().endswith('.parquet'):
                query = f"SELECT * FROM read_parquet('{temp_path}') LIMIT 50"
            else:
                query = f"SELECT * FROM read_csv_auto('{temp_path}') LIMIT 50"
                
            result = con.execute(query)
            desc = result.description
            columns = [d[0] for d in desc] if desc else []
            raw_rows = result.fetchall()
            rows = [[_preview_json_cell(c) for c in r] for r in raw_rows]
            
        return {
            "columns": columns,
            "rows": rows,
            "limit": 50,
            "offset": 0,
            "has_more": False,
            "months_included": ["unpartitioned"],
        }
    except Exception as exc:
        logger.exception("Failed to preview local file %s", file.filename)
        return None
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


async def process_local_file_upload(
    user_id: str, files: list[UploadFile], source_name: str, config_id: str | None
) -> dict[str, Any]:
    slug = re.sub(r"[^a-z0-9._-]+", "-", source_name.lower()).strip("-") or "local-file"
    prefix = f"{USER_DATA_BLOB_PREFIX.strip('/')}/{user_id}/{slug}"
    container = _get_blob_container()
    if container is None:
        raise HTTPException(status_code=500, detail="Azure Blob Storage not configured")

    streams_added = []
    
    for file in files:
        if not file.filename:
            continue
        filename_no_ext = os.path.splitext(file.filename)[0]
        stream_name = re.sub(r"[^a-zA-Z0-9_]+", "_", filename_no_ext).strip("_") or "stream"
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
            tmp.write(await file.read())
            temp_path = tmp.name
            
        try:
            parquet_path = temp_path + ".parquet"
            if file.filename.lower().endswith(('.xlsx', '.xls')):
                import pandas as pd
                df = pd.read_excel(temp_path)
                df.columns = [str(c) for c in df.columns]  # ensure string columns
                df.to_parquet(parquet_path, engine="pyarrow")
            else:
                con = duckdb.connect(database=":memory:")
                if file.filename.lower().endswith('.json'):
                    con.execute(f"COPY (SELECT * FROM read_json_auto('{temp_path}')) TO '{parquet_path}' (FORMAT PARQUET)")
                elif file.filename.lower().endswith('.parquet'):
                    import shutil
                    shutil.copy(temp_path, parquet_path)
                else:
                    con.execute(f"COPY (SELECT * FROM read_csv_auto('{temp_path}')) TO '{parquet_path}' (FORMAT PARQUET)")
            
            safe_filename = re.sub(r"[^a-zA-Z0-9_.-]+", "_", file.filename)
            blob_name = f"{prefix}/{stream_name}/{safe_filename}.parquet"
            with open(parquet_path, "rb") as fh:
                container.upload_blob(name=blob_name, data=fh, overwrite=True)
            if stream_name not in streams_added:
                streams_added.append(stream_name)
        except Exception as exc:
            logger.exception("Failed to process local file upload: %s", file.filename)
            raise HTTPException(status_code=400, detail=f"Failed to process {file.filename}: {exc}") from exc
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            if 'parquet_path' in locals() and os.path.exists(parquet_path):
                os.unlink(parquet_path)

    invalidate_stream_cache()

    if config_id:
        existing = get_user_connector(user_id, config_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Connector configuration not found.")
        selected_streams = existing.get("selected_streams") or []
        for s in streams_added:
            if s not in selected_streams:
                selected_streams.append(s)
        patch = UserConnectorConfigUpdate(selected_streams=selected_streams)
        result = update_user_connector(user_id, config_id, patch)
    else:
        body = UserConnectorConfigCreate(
            connector_name=source_name,
            docker_repository="local/file-upload",
            docker_image="local",
            selected_streams=streams_added,
            sync_mode="one_off",
            is_active=False,
        )
        result = create_user_connector(user_id, body)

    # Immediately mark the upload as successfully synced so the UI doesn't show "Pending"
    client = get_supabase_admin_client()
    now_iso = datetime.utcnow().isoformat()
    client.table(_TABLE).update({
        "last_sync_status": "success",
        "last_sync_at": now_iso
    }).eq("id", result["id"]).execute()
    
    result["last_sync_status"] = "success"
    result["last_sync_at"] = now_iso
    return result
