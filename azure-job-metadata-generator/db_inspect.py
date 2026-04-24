"""DuckDB schema inspection for the metadata generator.

Builds a per-tenant DuckDB sandbox over the tenant's Parquet data in Azure
Blob Storage, then collects the raw inputs the LLM steps need:

- per-table column list with DuckDB types
- a small sample of rows per table
- approximate distinct count per column
- top-N sample values for likely-categorical columns

The logic mirrors ``ai/agents/sql/duckdb_sandbox.py`` but is intentionally
self-contained — this container must not import from the FastAPI backend.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import duckdb

try:
    import certifi
except ImportError:  # pragma: no cover
    certifi = None  # type: ignore[assignment]

from azure.storage.blob import ContainerClient

logger = logging.getLogger(__name__)

_SAFE_TENANT_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# Tunables
SAMPLE_ROWS = int(os.environ.get("METADATA_SAMPLE_ROWS", "1000"))
SAMPLE_VALUES_PER_COLUMN = int(os.environ.get("METADATA_SAMPLE_VALUES", "20"))
CATEGORICAL_CARDINALITY_MAX = int(
    os.environ.get("METADATA_CATEGORICAL_MAX", "200")
)
# Minimum number of distinct rows a table must have to be worth describing.
# Tables with fewer rows are skipped (insufficient data for a useful LLM
# description and likely either empty or just a header row).
MIN_TABLE_ROWS = int(os.environ.get("METADATA_MIN_TABLE_ROWS", "3"))


class InsufficientDataError(RuntimeError):
    """Raised when a table has fewer than ``MIN_TABLE_ROWS`` rows."""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ColumnSnapshot:
    name: str
    data_type: str
    cardinality: int | None = None
    sample_values: list[Any] = field(default_factory=list)


@dataclass
class TableSnapshot:
    name: str
    columns: list[ColumnSnapshot]
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    # Human-friendly identifiers for the LLM prompt. ``stream_name`` is the
    # original blob/folder name (pre-view-mangling); ``source_name`` is the
    # connector that produced it (e.g. "Shopify", "Google Analytics").
    stream_name: str | None = None
    source_name: str | None = None


# ---------------------------------------------------------------------------
# Sandbox setup
# ---------------------------------------------------------------------------


def _ensure_tls_ca() -> None:
    """Point libcurl at a readable CA bundle for DuckDB's Azure HTTPS reads."""
    if os.environ.get("CURL_CA_INFO") or os.environ.get("SSL_CERT_FILE"):
        return
    candidates: list[str] = []
    if certifi is not None:
        try:
            candidates.append(certifi.where())
        except Exception:
            pass
    candidates.extend(
        (
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/tls/certs/ca-bundle.crt",
        )
    )
    for path in candidates:
        if path and os.path.isfile(path) and os.access(path, os.R_OK):
            os.environ["CURL_CA_INFO"] = path
            os.environ.setdefault("SSL_CERT_FILE", path)
            os.environ.setdefault("REQUESTS_CA_BUNDLE", path)
            break


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
            "AZURE_STORAGE_CONNECTION_STRING environment variable is required."
        )
    return cs


def _tenant_prefix_candidates(tenant_id: str) -> list[str]:
    if not _SAFE_TENANT_ID_RE.match(tenant_id):
        raise ValueError("Invalid tenant id format.")
    roots = [
        r.strip().strip("/")
        for r in os.environ.get("USER_DATA_BLOB_PREFIX", "user-data,users").split(",")
        if r.strip().strip("/")
    ]
    out: list[str] = []
    seen: set[str] = set()
    for root in roots + [""]:
        prefix = f"{root}/{tenant_id}/" if root else f"{tenant_id}/"
        if prefix not in seen:
            seen.add(prefix)
            out.append(prefix)
    return out


def _view_name(path: str) -> str:
    return path.replace("/", "_").replace("-", "_")


_HIVE_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.+")


def _discover_views(tenant_id: str) -> dict[str, dict[str, str]]:
    """Return ``{view_name: {"glob": blob_glob, "stream": stream_name}}``.

    Mirrors the backend's ``duckdb_sandbox.discover_tenant_views`` so the
    metadata generator sees the same dataset names the runtime uses.
    Handles Hive-partitioned layouts like::

        user-data/<tenant>/<source>/<stream>/year=YYYY/month=MM/*.parquet

    by walking back to the last non-Hive segment (``<source>/<stream>``)
    and exposing it as one view over ``**/*.parquet``.
    """
    container_name = _container_name()
    container = ContainerClient.from_connection_string(
        _connection_string(), container_name=container_name
    )
    selected_prefix = ""
    dir_files: set[str] = set()
    for prefix in _tenant_prefix_candidates(tenant_id):
        for blob in container.list_blobs(name_starts_with=prefix):
            if not blob.name.endswith(".parquet"):
                continue
            if not selected_prefix:
                selected_prefix = prefix
            relative = blob.name[len(prefix):]
            parts = relative.split("/")
            parent = "/".join(parts[:-1])
            if parent:
                dir_files.add(parent)
        if selected_prefix:
            break

    if not selected_prefix:
        return {}

    # Detect Hive-partitioned roots.
    hive_roots: set[str] = set()
    hive_dirs: set[str] = set()
    for dir_path in dir_files:
        parts = dir_path.split("/")
        for i, part in enumerate(parts):
            if _HIVE_SEGMENT_RE.match(part):
                root = "/".join(parts[:i])
                if root:
                    hive_roots.add(root)
                    hive_dirs.add(dir_path)
                break

    out: dict[str, dict[str, str]] = {}

    # Hive roots: one view per <source>/<stream> covering all partitions.
    for root in sorted(hive_roots):
        glob_path = f"azure://{container_name}/{selected_prefix}{root}/**/*.parquet"
        out[_view_name(root)] = {"glob": glob_path, "stream": root}

    # Non-Hive leaf directories.
    for dir_path in sorted(dir_files):
        if dir_path in hive_dirs:
            continue
        if any(dir_path.startswith(r + "/") for r in hive_roots):
            continue
        if dir_path in hive_roots:
            continue
        glob_path = f"azure://{container_name}/{selected_prefix}{dir_path}/*.parquet"
        out[_view_name(dir_path)] = {"glob": glob_path, "stream": dir_path}
    return out


def open_sandbox(
    tenant_id: str,
) -> tuple[duckdb.DuckDBPyConnection, list[dict[str, str]]]:
    """Open a fresh DuckDB connection with one view per Parquet dataset.

    Returns ``(conn, views)`` where each ``views`` entry is
    ``{"view": <duckdb_view_name>, "stream": <original_stream_name>}``.
    The list is empty when the tenant has no synced data yet.
    """
    _ensure_tls_ca()
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL azure; LOAD azure;")
    conn.execute("SET threads=2")
    conn.execute(
        f"SET memory_limit='{os.environ.get('DUCKDB_MEMORY_LIMIT', '512MB')}'"
    )
    if sys.platform == "linux":
        conn.execute("SET azure_transport_option_type = 'curl';")
    safe_cs = _connection_string().replace("'", "''")
    conn.execute(
        f"CREATE SECRET azure_creds (TYPE AZURE, CONNECTION_STRING '{safe_cs}');"
    )

    views = _discover_views(tenant_id)
    created: list[dict[str, str]] = []
    for view_name, info in views.items():
        blob_path = info["glob"]
        try:
            hive_opt = ", hive_partitioning=true" if "**/" in blob_path else ""
            conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS "
                f"SELECT * FROM read_parquet('{blob_path}', union_by_name=true{hive_opt})"
            )
            created.append({"view": view_name, "stream": info["stream"]})
        except Exception:
            logger.exception("Failed to create view for %s", view_name)
    return conn, created


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------


def _safe_ident(name: str) -> bool:
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name))


def describe_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
) -> list[ColumnSnapshot]:
    """Return one ``ColumnSnapshot`` per column in *table* (types only)."""
    if not _safe_ident(table):
        raise ValueError(f"Unsafe table identifier: {table!r}")
    rows = conn.execute(f"DESCRIBE {table}").fetchall()
    return [
        ColumnSnapshot(name=str(r[0]), data_type=str(r[1])) for r in rows
    ]


def sample_rows(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    limit: int = SAMPLE_ROWS,
) -> list[dict[str, Any]]:
    """Return up to *limit* random distinct rows from *table*.

    Uses DuckDB ``USING SAMPLE`` over a ``SELECT DISTINCT *`` subquery so
    the returned rows are guaranteed to be unique. Falls back to a plain
    ``LIMIT`` when sampling raises (typically on tiny tables).
    """
    if not _safe_ident(table):
        raise ValueError(f"Unsafe table identifier: {table!r}")
    try:
        df = conn.execute(
            f"SELECT * FROM (SELECT DISTINCT * FROM {table}) "
            f"USING SAMPLE {int(limit)} ROWS"
        ).fetchdf()
    except Exception:
        logger.exception("Sampling failed for %s; falling back to LIMIT", table)
        df = conn.execute(
            f"SELECT DISTINCT * FROM {table} LIMIT {int(limit)}"
        ).fetchdf()
    return df.to_dict(orient="records")


def count_distinct_rows(
    conn: duckdb.DuckDBPyConnection,
    table: str,
) -> int:
    """Return the count of distinct rows in *table* (0 on failure)."""
    if not _safe_ident(table):
        return 0
    try:
        row = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table})"
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        logger.exception("Distinct row count failed for %s", table)
        return 0


def column_cardinality(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
) -> int | None:
    """Return ``approx_count_distinct(column)`` or ``None`` on failure."""
    if not (_safe_ident(table) and _safe_ident(column)):
        return None
    try:
        row = conn.execute(
            f"SELECT approx_count_distinct({column}) FROM {table}"
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def sample_column_values(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    limit: int = SAMPLE_VALUES_PER_COLUMN,
) -> list[Any]:
    """Return up to *limit* most-frequent non-null values for *column*."""
    if not (_safe_ident(table) and _safe_ident(column)):
        return []
    try:
        rows = conn.execute(
            f"SELECT {column} AS v, COUNT(*) AS c FROM {table} "
            f"WHERE {column} IS NOT NULL "
            f"GROUP BY 1 ORDER BY c DESC LIMIT {int(limit)}"
        ).fetchall()
    except Exception:
        return []
    return [r[0] for r in rows]


def snapshot_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    *,
    stream_name: str | None = None,
    source_name: str | None = None,
) -> TableSnapshot:
    """Build a complete ``TableSnapshot`` (schema + samples + cardinality).

    Raises :class:`InsufficientDataError` when the table has fewer than
    ``MIN_TABLE_ROWS`` distinct rows.
    """
    cols = describe_table(conn, table)
    distinct_count = count_distinct_rows(conn, table)
    if distinct_count < MIN_TABLE_ROWS:
        raise InsufficientDataError(
            f"{table}: only {distinct_count} distinct row(s) "
            f"(minimum {MIN_TABLE_ROWS})."
        )
    rows = sample_rows(conn, table)
    for col in cols:
        col.cardinality = column_cardinality(conn, table, col.name)
        if (
            col.cardinality is not None
            and col.cardinality <= CATEGORICAL_CARDINALITY_MAX
        ):
            col.sample_values = sample_column_values(conn, table, col.name)
    return TableSnapshot(
        name=table,
        columns=cols,
        sample_rows=rows,
        stream_name=stream_name or table,
        source_name=source_name,
    )


def snapshot_all(
    conn: duckdb.DuckDBPyConnection,
    tables: list[str] | list[dict[str, str]],
    *,
    source_map: dict[str, str] | None = None,
) -> list[TableSnapshot]:
    """Build snapshots for every table; failures are logged and skipped.

    *tables* may be a list of view names (strings) OR a list of dicts shaped
    ``{"view": <view>, "stream": <stream>}`` as returned by ``open_sandbox``.
    *source_map* maps stream name → connector name so the LLM prompt can
    include the data source.
    """
    out: list[TableSnapshot] = []
    source_map = source_map or {}
    for entry in tables:
        if isinstance(entry, dict):
            view = entry["view"]
            stream = entry.get("stream") or view
        else:
            view = entry
            stream = entry
        try:
            out.append(
                snapshot_table(
                    conn,
                    view,
                    stream_name=stream,
                    source_name=source_map.get(stream),
                )
            )
        except InsufficientDataError as exc:
            logger.warning("Skipping %s: %s", view, exc)
        except Exception:
            logger.exception("Failed to snapshot table %s", view)
    return out
