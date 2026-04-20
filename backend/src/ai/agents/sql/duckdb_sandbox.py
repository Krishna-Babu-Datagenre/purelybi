from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field

import duckdb

# requests depends on certifi; bundle path is reliable on Azure App Service zip/Oryx
# where system CA paths may be missing or invisible to DuckDB's default Azure HTTP stack.
try:
    import certifi
except ImportError:  # pragma: no cover
    certifi = None  # type: ignore[assignment]
from azure.storage.blob import ContainerClient

logger = logging.getLogger(__name__)

_SAFE_TENANT_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_HIVE_SEGMENT_RE = re.compile(r"^[^=]+=.")

# ---------------------------------------------------------------------------
# Caches — avoid repeated Azure Blob listing on every request.
# ---------------------------------------------------------------------------
# View discovery: tenant_id → (views_dict, monotonic_ts)
_VIEWS_CACHE: dict[str, tuple[dict[str, str], float]] = {}
_VIEWS_CACHE_LOCK = threading.Lock()
_VIEWS_CACHE_TTL = 300.0  # 5 min

# Resolved blob prefix: tenant_id → (prefix, monotonic_ts)
# Longer TTL because the correct prefix for a tenant almost never changes.
_PREFIX_CACHE: dict[str, tuple[str, float]] = {}
_PREFIX_CACHE_TTL = 1800.0  # 30 min

# Module-level ContainerClient — reused across calls, created lazily.
_container_client: ContainerClient | None = None
_container_client_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Tenant sandbox pool — materialized DuckDB connections reused across requests.
# ---------------------------------------------------------------------------
_POOL_MAX = int(os.environ.get("DUCKDB_POOL_MAX_TENANTS", "50"))
_POOL_TTL = float(os.environ.get("DUCKDB_POOL_TTL_SECONDS", "3600"))  # 60 min — keep materialized data warm to avoid cold-start re-download


@dataclass
class _TenantPoolEntry:
    conn: duckdb.DuckDBPyConnection
    views: frozenset[str]
    created_at: float
    last_used: float
    lock: threading.RLock = field(default_factory=threading.RLock)


_POOL: dict[str, _TenantPoolEntry] = {}
_POOL_LOCK = threading.Lock()
# Per-tenant creation locks prevent duplicate materialisation by concurrent requests.
_TENANT_CREATE_LOCKS: dict[str, threading.Lock] = {}
_TENANT_CREATE_LOCKS_LOCK = threading.Lock()


def _ensure_tls_ca_for_duckdb_azure() -> None:
    """Point OpenSSL/libcurl at a readable CA bundle (fixes Azure Blob HTTPS in App Service).

    DuckDB's Azure extension may fail with "Problem with the SSL CA cert" when the
    default transport cannot resolve CA paths. Setting CURL_CA_INFO and using the
    curl transport honors this on Linux; certifi provides a known-good bundle path.
    See: https://duckdb.org/docs/stable/core_extensions/azure.html (Configuration)
    """
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
            "AZURE_STORAGE_CONNECTION_STRING environment variable is required for DuckDB parquet reads."
        )
    return cs


def _get_container_client() -> ContainerClient:
    """Lazily-created module-level ContainerClient (avoids per-request overhead)."""
    global _container_client
    if _container_client is not None:
        return _container_client
    with _container_client_lock:
        if _container_client is not None:
            return _container_client
        _container_client = ContainerClient.from_connection_string(
            _connection_string(), container_name=_container_name()
        )
        return _container_client


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
    # Fast path: return cached result if still fresh.
    now = time.monotonic()
    with _VIEWS_CACHE_LOCK:
        cached = _VIEWS_CACHE.get(tenant_id)
        if cached is not None:
            views_cached, ts = cached
            if now - ts < _VIEWS_CACHE_TTL:
                return dict(views_cached)

    container_name = _container_name()
    container = _get_container_client()

    # Use cached prefix to skip candidate probing when possible.
    prefix_entry = _PREFIX_CACHE.get(tenant_id)
    if prefix_entry is not None and now - prefix_entry[1] < _PREFIX_CACHE_TTL:
        candidates = [prefix_entry[0]]
    else:
        candidates = _tenant_prefix_candidates(tenant_id)

    selected_prefix = ""
    dir_files: dict[str, list[str]] = defaultdict(list)

    # Single-pass: enumerate blobs for each candidate prefix and keep
    # the first one that contains parquet files.  Eliminates the old
    # two-phase probe-then-enumerate pattern (saves 1-3 HTTP round trips).
    for prefix in candidates:
        for blob in container.list_blobs(name_starts_with=prefix):
            if not blob.name.endswith(".parquet"):
                continue
            if not selected_prefix:
                selected_prefix = prefix
            relative = blob.name[len(prefix):]
            parts = relative.split("/")
            parent = "/".join(parts[:-1])
            filename = parts[-1]
            if parent:
                dir_files[parent].append(filename)
        if selected_prefix:
            _PREFIX_CACHE[tenant_id] = (selected_prefix, time.monotonic())
            break

    if not selected_prefix:
        selected_prefix = _tenant_prefix(tenant_id)
    base_url = f"azure://{container_name}/{selected_prefix}"

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
            ", ".join(candidates),
        )

    all_dirs = set(dir_files.keys())
    parent_dirs: set[str] = set()
    for d in all_dirs:
        for other in all_dirs:
            if other != d and other.startswith(d + "/"):
                parent_dirs.add(d)
                break

    # ── Detect Hive-partitioned directory trees ───────────────────
    hive_roots: set[str] = set()
    hive_dirs: set[str] = set()
    for dir_path in all_dirs:
        parts = dir_path.split("/")
        for i, part in enumerate(parts):
            if _HIVE_SEGMENT_RE.match(part):
                root = "/".join(parts[:i])
                if root:
                    hive_roots.add(root)
                    hive_dirs.add(dir_path)
                break

    views: dict[str, str] = {}

    for root in sorted(hive_roots):
        view = _view_name(root)
        views[view] = f"{base_url}{root}/**/*.parquet"

    for dir_path in sorted(dir_files):
        if dir_path in hive_dirs:
            continue
        if any(dir_path.startswith(r + "/") for r in hive_roots):
            continue
        if dir_path in hive_roots:
            continue

        files = dir_files[dir_path]
        if dir_path in parent_dirs:
            for filename in sorted(files):
                stem = filename.rsplit(".", 1)[0]
                view = _view_name(f"{dir_path}/{stem}")
                views[view] = f"{base_url}{dir_path}/{filename}"
        else:
            view = _view_name(dir_path)
            views[view] = f"{base_url}{dir_path}/*.parquet"

    # Store in cache for subsequent requests.
    with _VIEWS_CACHE_LOCK:
        _VIEWS_CACHE[tenant_id] = (dict(views), time.monotonic())
    return views


def list_tenant_dataset_view_names(tenant_id: str) -> list[str]:
    """Return sorted DuckDB view names for the tenant's synced Parquet data (no DB connection)."""
    views = discover_tenant_views(tenant_id)
    return sorted(views.keys())


def invalidate_tenant_cache(tenant_id: str) -> None:
    """Remove cached blob discovery and pooled connection for *tenant_id* (call after sync completes)."""
    with _VIEWS_CACHE_LOCK:
        _VIEWS_CACHE.pop(tenant_id, None)
    _PREFIX_CACHE.pop(tenant_id, None)
    with _POOL_LOCK:
        entry = _POOL.pop(tenant_id, None)
    if entry is not None:
        try:
            entry.conn.close()
        except Exception:
            pass


def invalidate_all_tenant_caches() -> None:
    """Clear all tenant discovery caches and pooled connections."""
    with _VIEWS_CACHE_LOCK:
        _VIEWS_CACHE.clear()
    _PREFIX_CACHE.clear()
    with _POOL_LOCK:
        old_entries = list(_POOL.values())
        _POOL.clear()
    for entry in old_entries:
        try:
            entry.conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tenant sandbox pool — "import mode" like Power BI / Tableau.
# ---------------------------------------------------------------------------


def _evict_expired_locked() -> None:
    """Remove expired pool entries.  Must hold ``_POOL_LOCK``."""
    now = time.monotonic()
    expired = [k for k, v in _POOL.items() if now - v.created_at > _POOL_TTL]
    for k in expired:
        entry = _POOL.pop(k)
        try:
            entry.conn.close()
        except Exception:
            pass


def _evict_lru_locked() -> None:
    """Remove the least-recently-used pool entry.  Must hold ``_POOL_LOCK``."""
    if not _POOL:
        return
    lru_key = min(_POOL, key=lambda k: _POOL[k].last_used)
    entry = _POOL.pop(lru_key)
    logger.info("Evicted pooled sandbox for tenant %s (LRU)", lru_key)
    try:
        entry.conn.close()
    except Exception:
        pass


def _get_tenant_create_lock(tenant_id: str) -> threading.Lock:
    """Get or create a per-tenant lock to avoid duplicate materialisation."""
    with _TENANT_CREATE_LOCKS_LOCK:
        lock = _TENANT_CREATE_LOCKS.get(tenant_id)
        if lock is None:
            lock = threading.Lock()
            _TENANT_CREATE_LOCKS[tenant_id] = lock
        return lock


def _create_materialized_sandbox(
    tenant_id: str,
) -> tuple[duckdb.DuckDBPyConnection, frozenset[str]]:
    """Create a DuckDB connection with data materialised into in-memory TABLEs.

    Unlike ``create_tenant_sandbox`` (which uses VIEWs that re-download from
    Azure on every query), this loads each table's Parquet once into DuckDB
    memory.  Subsequent queries against these tables are local and instant —
    the "import mode" pattern used by Power BI and Tableau.
    """
    _ensure_tls_ca_for_duckdb_azure()
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

    views = discover_tenant_views(tenant_id)
    succeeded: dict[str, str] = {}
    for view_name, blob_path in views.items():
        try:
            hive_opt = ", hive_partitioning=true" if "**/" in blob_path else ""
            conn.execute(
                f"CREATE TABLE {view_name} AS "
                f"SELECT * FROM read_parquet('{blob_path}', union_by_name=true{hive_opt})"
            )
            succeeded[view_name] = blob_path
        except Exception:
            logger.exception(
                "Failed materialising table %s from %s", view_name, blob_path
            )

    logger.info(
        "Materialised %d/%d tables for tenant %s",
        len(succeeded),
        len(views),
        tenant_id,
    )
    return conn, frozenset(succeeded.keys())


def get_tenant_sandbox(
    tenant_id: str,
) -> tuple[duckdb.DuckDBPyConnection, frozenset[str]]:
    """Return a pooled DuckDB connection with materialised tables.

    The connection is **managed by the pool** — callers must NOT close it.
    Data lives in DuckDB memory, so repeated queries are local and fast.
    """
    now = time.monotonic()

    # Fast path: check pool.
    with _POOL_LOCK:
        entry = _POOL.get(tenant_id)
        if entry is not None and now - entry.created_at < _POOL_TTL:
            entry.last_used = now
            return entry.conn, entry.views

    # Slow path: materialise tables.  Per-tenant lock prevents two
    # concurrent requests from both downloading the same data.
    create_lock = _get_tenant_create_lock(tenant_id)
    with create_lock:
        # Re-check after acquiring per-tenant lock (another thread may have created it).
        with _POOL_LOCK:
            entry = _POOL.get(tenant_id)
            if entry is not None and now - entry.created_at < _POOL_TTL:
                entry.last_used = now
                return entry.conn, entry.views
            # Evict stale entry if present.
            old = _POOL.pop(tenant_id, None)

        if old is not None:
            try:
                old.conn.close()
            except Exception:
                pass

        t0 = time.monotonic()
        conn, views = _create_materialized_sandbox(tenant_id)
        elapsed = time.monotonic() - t0
        logger.info(
            "Created materialised sandbox for tenant %s in %.1fs (%d tables)",
            tenant_id,
            elapsed,
            len(views),
        )

        new_entry = _TenantPoolEntry(
            conn=conn,
            views=views,
            created_at=time.monotonic(),
            last_used=time.monotonic(),
        )
        with _POOL_LOCK:
            _evict_expired_locked()
            while len(_POOL) >= _POOL_MAX:
                _evict_lru_locked()
            _POOL[tenant_id] = new_entry
        return conn, views


def create_tenant_sandbox(
    tenant_id: str,
    *,
    views_filter: frozenset[str] | None = None,
) -> tuple[duckdb.DuckDBPyConnection, frozenset[str]]:
    _ensure_tls_ca_for_duckdb_azure()
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL azure; LOAD azure;")
    conn.execute("SET threads=2")
    conn.execute(
        f"SET memory_limit='{os.environ.get('DUCKDB_MEMORY_LIMIT', '512MB')}'"
    )
    # libcurl honors CURL_CA_INFO on Linux; default Azure SDK transport often lacks CA path on App Service.
    if sys.platform == "linux":
        conn.execute("SET azure_transport_option_type = 'curl';")

    safe_cs = _connection_string().replace("'", "''")
    conn.execute(
        f"CREATE SECRET azure_creds (TYPE AZURE, CONNECTION_STRING '{safe_cs}');"
    )

    views = discover_tenant_views(tenant_id)
    if views_filter is not None:
        filtered = {k: v for k, v in views.items() if k in views_filter}
        if not filtered:
            known = ", ".join(sorted(views.keys())[:40])
            suffix = f" Known: {known}." if known else ""
            raise ValueError(
                "No datasets match the current selection. "
                "Pick at least one dataset that exists for this account."
                + suffix
            )
        views = filtered

    for view_name, blob_path in views.items():
        try:
            hive_opt = ", hive_partitioning=true" if "**/" in blob_path else ""
            conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS "
                f"SELECT * FROM read_parquet('{blob_path}', union_by_name=true{hive_opt})"
            )
        except Exception:
            logger.exception("Failed mounting view %s from %s", view_name, blob_path)

    return conn, frozenset(views.keys())
