from __future__ import annotations

import logging
import os
import re
import sys
from collections import defaultdict

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

    # ── Detect Hive-partitioned directory trees ───────────────────
    # Directories whose path contains segments like "year=2026" or
    # "month=04" belong to a Hive partition tree.  We find the root
    # (deepest ancestor *without* a key=value segment) and create a
    # single view using ``**/*.parquet`` with ``hive_partitioning=true``.
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

    # Hive root views (one view per root, reads all partitions)
    for root in sorted(hive_roots):
        view = _view_name(root)
        views[view] = f"{base_url}{root}/**/*.parquet"

    # Non-Hive directories (flat layouts, backward-compat)
    for dir_path in sorted(dir_files):
        if dir_path in hive_dirs:
            continue
        # Skip dirs that sit under a Hive root
        if any(dir_path.startswith(r + "/") for r in hive_roots):
            continue
        # Skip dirs that ARE a Hive root (already handled above)
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
    return views


def list_tenant_dataset_view_names(tenant_id: str) -> list[str]:
    """Return sorted DuckDB view names for the tenant's synced Parquet data (no DB connection)."""
    views = discover_tenant_views(tenant_id)
    return sorted(views.keys())


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
                f"SELECT * FROM read_parquet('{blob_path}'{hive_opt})"
            )
        except Exception:
            logger.exception("Failed mounting view %s from %s", view_name, blob_path)

    return conn, frozenset(views.keys())
