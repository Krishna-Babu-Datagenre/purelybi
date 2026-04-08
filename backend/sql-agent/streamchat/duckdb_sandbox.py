from __future__ import annotations

import logging
import os
import re
from collections import defaultdict

import duckdb
from azure.storage.blob import ContainerClient

logger = logging.getLogger(__name__)

_SAFE_TENANT_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _container_name() -> str:
    return (
        os.environ.get("BLOB_CONTAINER_NAME")
        or os.environ.get("AZURE_STORAGE_CONTAINER")
        or "raw"
    )


def _connection_string() -> str:
    cs = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not cs:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING environment variable is required for DuckDB parquet reads."
        )
    return cs


def _tenant_prefix(tenant_id: str) -> str:
    if not _SAFE_TENANT_ID_RE.match(tenant_id):
        raise ValueError("Invalid tenant id format.")
    prefix_root = (
        os.environ.get("USER_DATA_BLOB_PREFIX", "user-data").strip("/")
        or "user-data"
    )
    return f"{prefix_root}/{tenant_id}/"


def _tenant_prefix_candidates(tenant_id: str) -> list[str]:
    if not _SAFE_TENANT_ID_RE.match(tenant_id):
        raise ValueError("Invalid tenant id format.")
    roots = [
        r.strip().strip("/")
        for r in os.environ.get("USER_DATA_BLOB_PREFIX", "user-data,users").split(",")
        if r.strip().strip("/")
    ]
    candidates = [f"{root}/{tenant_id}/" for root in roots]
    # Backward-compat path variants seen across prototypes/deploys.
    candidates.append(f"tenant-{tenant_id}/")
    candidates.append(f"{tenant_id}/")
    # Preserve order while deduplicating.
    out: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _view_name(path: str) -> str:
    return path.replace("/", "_").replace("-", "_")


def discover_tenant_views(tenant_id: str) -> dict[str, str]:
    container_name = _container_name()
    container = ContainerClient.from_connection_string(
        _connection_string(), container_name=container_name
    )
    selected_prefix = ""
    for prefix in _tenant_prefix_candidates(tenant_id):
        has_any = any(
            blob.name.endswith(".parquet")
            for blob in container.list_blobs(name_starts_with=prefix)
        )
        if has_any:
            selected_prefix = prefix
            break
    if not selected_prefix:
        # Keep deterministic fallback for clear diagnostics.
        selected_prefix = _tenant_prefix(tenant_id)
    base_url = f"azure://{container_name}/{selected_prefix}"

    dir_files: dict[str, list[str]] = defaultdict(list)
    for blob in container.list_blobs(name_starts_with=selected_prefix):
        if not blob.name.endswith(".parquet"):
            continue
        relative = blob.name[len(selected_prefix) :]
        parts = relative.split("/")
        parent = "/".join(parts[:-1])
        filename = parts[-1]
        if parent:
            dir_files[parent].append(filename)
    if dir_files:
        logger.info(
            "Discovered %d parquet directories for tenant %s at prefix %s",
            len(dir_files),
            tenant_id,
            selected_prefix,
        )
    else:
        logger.warning(
            "No parquet data found for tenant %s. Tried prefixes: %s",
            tenant_id,
            ", ".join(_tenant_prefix_candidates(tenant_id)),
        )

    all_dirs = set(dir_files.keys())
    parent_dirs: set[str] = set()
    for d in all_dirs:
        for other in all_dirs:
            if other != d and other.startswith(d + "/"):
                parent_dirs.add(d)
                break

    views: dict[str, str] = {}
    for dir_path in sorted(dir_files):
        files = dir_files[dir_path]
        if dir_path in parent_dirs:
            for filename in sorted(files):
                stem = filename.rsplit(".", 1)[0]
                view = _view_name(f"{dir_path}/{stem}")
                views[view] = f"{base_url}{dir_path}/{filename}"
        else:
            view = _view_name(dir_path)
            views[view] = f"{base_url}{dir_path}/*.parquet"
    return views


def create_tenant_sandbox(tenant_id: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL azure; LOAD azure;")
    conn.execute("SET threads=2")

    safe_cs = _connection_string().replace("'", "''")
    conn.execute(
        f"CREATE SECRET azure_creds (TYPE AZURE, CONNECTION_STRING '{safe_cs}');"
    )

    for view_name, blob_path in discover_tenant_views(tenant_id).items():
        try:
            conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS "
                f"SELECT * FROM read_parquet('{blob_path}')"
            )
        except Exception:
            logger.exception("Failed mounting view %s from %s", view_name, blob_path)

    return conn
