"""Supabase REST writes for the metadata generator.

Uses ``httpx`` directly against PostgREST so the container does not need to
ship the heavy ``supabase-py`` dependency. The service-role key is required
because the container writes on behalf of the tenant.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx

logger = logging.getLogger(__name__)

_TABLES_TBL = "tenant_table_metadata"
_COLUMNS_TBL = "tenant_column_metadata"
_RELATIONSHIPS_TBL = "tenant_table_relationships"
_JOBS_TBL = "tenant_metadata_jobs"
_CONNECTOR_CONFIGS_TBL = "user_connector_configs"


def fetch_connector_map(
    client: httpx.Client,
    *,
    user_id: str,
) -> dict[str, str]:
    """Return ``{stream_name: connector_name}`` for *user_id*'s active connectors.

    A stream that appears in multiple active connectors resolves to the first
    one returned by Supabase; this is a reasonable compromise because the
    metadata prompt only needs a reader-friendly source label.
    """
    try:
        r = client.get(
            f"/{_CONNECTOR_CONFIGS_TBL}",
            params={
                "select": "connector_name,selected_streams",
                "user_id": f"eq.{user_id}",
                "is_active": "eq.true",
            },
        )
        if r.status_code >= 400:
            logger.error(
                "fetch_connector_map failed (%d): %s", r.status_code, r.text
            )
            return {}
        data = r.json() or []
    except Exception:
        logger.exception("fetch_connector_map request failed")
        return {}
    out: dict[str, str] = {}
    for row in data:
        name = row.get("connector_name")
        streams = row.get("selected_streams") or []
        if not name or not isinstance(streams, list):
            continue
        for s in streams:
            if isinstance(s, str) and s not in out:
                out[s] = name
    return out


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


def _client() -> httpx.Client:
    url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set."
        )
    return httpx.Client(
        base_url=f"{url}/rest/v1",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
        timeout=30.0,
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Job lifecycle
# ---------------------------------------------------------------------------


def update_job(
    client: httpx.Client,
    *,
    user_id: str,
    job_id: str,
    status: str | None = None,
    progress: float | None = None,
    message: str | None = None,
    error: str | None = None,
    mark_started: bool = False,
    mark_finished: bool = False,
) -> None:
    """PATCH a row in ``tenant_metadata_jobs`` (service role)."""
    payload: dict[str, Any] = {}
    if status is not None:
        payload["status"] = status
    if progress is not None:
        payload["progress"] = progress
    if message is not None:
        payload["message"] = message
    if error is not None:
        payload["error"] = error
    if mark_started:
        payload["started_at"] = _now_iso()
    if mark_finished:
        payload["finished_at"] = _now_iso()
    if not payload:
        return
    r = client.patch(
        f"/{_JOBS_TBL}",
        params={"id": f"eq.{job_id}", "user_id": f"eq.{user_id}"},
        content=json.dumps(payload),
    )
    if r.status_code >= 400:
        logger.error("update_job failed (%d): %s", r.status_code, r.text)


# ---------------------------------------------------------------------------
# Edited-row preservation
# ---------------------------------------------------------------------------


def _fetch_edited_keys(
    client: httpx.Client,
    table: str,
    *,
    user_id: str,
    key_columns: list[str],
) -> set[tuple[str, ...]]:
    """Return the set of composite keys that have ``edited_by_user = TRUE``."""
    select_cols = ",".join(key_columns)
    r = client.get(
        f"/{table}",
        params={
            "select": select_cols,
            "user_id": f"eq.{user_id}",
            "edited_by_user": "eq.true",
        },
    )
    if r.status_code >= 400:
        logger.error("fetch_edited failed (%d): %s", r.status_code, r.text)
        return set()
    out: set[tuple[str, ...]] = set()
    for row in r.json() or []:
        out.add(tuple(str(row.get(k, "")) for k in key_columns))
    return out


def _filter_unedited(
    rows: list[dict[str, Any]],
    *,
    edited: set[tuple[str, ...]],
    key_columns: list[str],
) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if tuple(str(row.get(k, "")) for k in key_columns) not in edited
    ]


def _bulk_upsert(
    client: httpx.Client,
    table: str,
    rows: list[dict[str, Any]],
    *,
    on_conflict: str,
) -> None:
    if not rows:
        return
    r = client.post(
        f"/{table}",
        params={"on_conflict": on_conflict},
        headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        content=json.dumps(rows),
    )
    if r.status_code >= 400:
        logger.error("upsert into %s failed (%d): %s", table, r.status_code, r.text)
        raise RuntimeError(f"Supabase upsert into {table} failed: {r.text[:300]}")


# ---------------------------------------------------------------------------
# Public writers
# ---------------------------------------------------------------------------


def upsert_table_metadata(
    client: httpx.Client,
    *,
    user_id: str,
    payloads: Iterable[dict[str, Any]],
) -> int:
    """Upsert per-table descriptions; preserves user-edited rows."""
    edited = _fetch_edited_keys(
        client, _TABLES_TBL, user_id=user_id, key_columns=["table_name"]
    )
    now = _now_iso()
    rows = [
        {
            "user_id": user_id,
            "table_name": p["table_name"],
            "description": p.get("description"),
            "primary_date_column": p.get("primary_date_column"),
            "grain": p.get("grain"),
            "generated_at": now,
            "edited_by_user": False,
        }
        for p in payloads
    ]
    rows = _filter_unedited(rows, edited=edited, key_columns=["table_name"])
    _bulk_upsert(
        client, _TABLES_TBL, rows, on_conflict="user_id,table_name"
    )
    return len(rows)


def upsert_column_metadata(
    client: httpx.Client,
    *,
    user_id: str,
    payloads: Iterable[dict[str, Any]],
) -> int:
    """Upsert per-column metadata; preserves user-edited rows."""
    edited = _fetch_edited_keys(
        client,
        _COLUMNS_TBL,
        user_id=user_id,
        key_columns=["table_name", "column_name"],
    )
    now = _now_iso()
    rows = [
        {
            "user_id": user_id,
            "table_name": p["table_name"],
            "column_name": p["column_name"],
            "data_type": p["data_type"],
            "semantic_type": p.get("semantic_type") or "unknown",
            "description": p.get("description"),
            "is_filterable": bool(p.get("is_filterable", True)),
            "cardinality": p.get("cardinality"),
            "sample_values": p.get("sample_values"),
            "generated_at": now,
            "edited_by_user": False,
        }
        for p in payloads
    ]
    rows = _filter_unedited(
        rows, edited=edited, key_columns=["table_name", "column_name"]
    )
    _bulk_upsert(
        client,
        _COLUMNS_TBL,
        rows,
        on_conflict="user_id,table_name,column_name",
    )
    return len(rows)


def upsert_relationships(
    client: httpx.Client,
    *,
    user_id: str,
    edges: Iterable[dict[str, Any]],
) -> int:
    """Upsert relationship edges; preserves user-edited rows."""
    edited = _fetch_edited_keys(
        client,
        _RELATIONSHIPS_TBL,
        user_id=user_id,
        key_columns=[
            "from_table",
            "from_column",
            "to_table",
            "to_column",
        ],
    )
    now = _now_iso()
    rows = [
        {
            "user_id": user_id,
            "from_table": e["from_table"],
            "from_column": e["from_column"],
            "to_table": e["to_table"],
            "to_column": e["to_column"],
            "kind": e["kind"],
            "confidence": e.get("confidence"),
            "generated_at": now,
            "edited_by_user": False,
        }
        for e in edges
    ]
    rows = _filter_unedited(
        rows,
        edited=edited,
        key_columns=["from_table", "from_column", "to_table", "to_column"],
    )
    _bulk_upsert(
        client,
        _RELATIONSHIPS_TBL,
        rows,
        on_conflict="user_id,from_table,from_column,to_table,to_column",
    )
    return len(rows)
