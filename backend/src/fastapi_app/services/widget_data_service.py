"""
Widget data hydration service.

Executes SQL queries defined in each widget's ``data_config`` against the
user's analytics data in **DuckDB** (views over Parquet in Azure Blob),
and maps the results into the ECharts ``chart_config`` structure.

Widget results are cached purely in process (``_PRESET_FILTER_CACHE``) for
preset date ranges; no widget data is stored in Supabase.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from typing import Any

import duckdb

from ai.agents.sql.duckdb_sandbox import create_tenant_sandbox, get_tenant_sandbox
from fastapi_app.models.filters import FilterSpec
from fastapi_app.services import metadata_service
from fastapi_app.services.filter_engine import apply_filters

logger = logging.getLogger(__name__)

# Legacy widget SQL was authored for SQLite (e.g. ``DATE(col)``). DuckDB uses ``cast``.
_LEGACY_DATE_CALL = re.compile(r"\bDATE\s*\(\s*([^)]+?)\s*\)", re.IGNORECASE)

# TTL for in-process preset-filter cache (preset key → chart_config dict).
CACHE_TTL_SECONDS = 300  # 5 minutes

# In-memory cache for preset-filtered widget hydration (key: widget id + preset + filter_spec hash).
# Custom date ranges never use this cache.
_PRESET_FILTER_CACHE: dict[tuple[str, str, str], tuple[dict[str, Any], datetime]] = {}
_PRESET_CACHE_MAX_ENTRIES = 4000


def _hash_filter_spec(filter_spec: FilterSpec | None) -> str:
    """Stable 16-char digest of *filter_spec* for use in cache keys.

    Empty / ``None`` specs collapse to the sentinel ``"none"``. Dict
    serialisation uses ``sort_keys`` so equivalent specs share a key.
    """
    if filter_spec is None or filter_spec.is_empty():
        return "none"
    try:
        payload = filter_spec.model_dump(mode="json", exclude_none=True)
        raw = json.dumps(payload, sort_keys=True, default=str)
    except Exception:
        raw = repr(filter_spec)
    return hashlib.blake2b(raw.encode("utf-8"), digest_size=8).hexdigest()

# Tables that are allowed in auto-generated KPI SQL (not free-form queries).
_ALLOWED_TABLES = frozenset(
    {
        "shopify_orders",
        "meta_campaign_insights",
        "meta_daily_insights",
        "meta_ad_insights",
        "meta_adset_insights",
    }
)

# ---------------------------------------------------------------------------
# Legacy hardcoded allowlists (Group C6 compatibility shim)
#
# These used to be the single source of truth for which columns a dashboard
# could filter on. They now serve as a FALLBACK for tenants that have not
# yet run the metadata-generator job. When ``tenant_column_metadata`` has
# any rows for a tenant, those drive ``_resolve_allowed_filters`` and this
# constant is ignored. Safe to delete once all tenants have metadata.
# ---------------------------------------------------------------------------

# Default date column per table (only tables with a time-series axis).
_DATE_COLUMNS: dict[str, str] = {
    "shopify_orders": "created_at",
    "meta_daily_insights": "date",
}

# Columns that may appear in user-supplied filters, keyed by table.
_ALLOWED_FILTERS: dict[str, frozenset[str]] = {
    "shopify_orders": frozenset(
        {
            "created_at",
            "billing_country",
            "shipping_country",
            "payment_gateways",
            "financial_status",
            "fulfillment_status",
            "currency",
            "total_price",
            "net_sales",
            "order_margin_base",
            "cancelled_at",
        }
    ),
    "meta_daily_insights": frozenset(
        {
            "date",
            "spend",
            "roas",
        }
    ),
    "meta_campaign_insights": frozenset(
        {
            "campaign_name",
            "spend",
            "roas",
        }
    ),
    "meta_ad_insights": frozenset(
        {
            "campaign_name",
            "ad_name",
            "spend",
            "revenue",
        }
    ),
    "meta_adset_insights": frozenset(
        {
            "campaign_name",
            "adset_name",
            "spend",
            "revenue",
        }
    ),
}

_FILTER_OPS = frozenset(
    {
        "eq",
        "neq",
        "in",
        "not_in",
        "gt",
        "gte",
        "lt",
        "lte",
        "between",
    }
)


# ---------------------------------------------------------------------------
# Metadata-backed allowlist resolvers (Group C6)
# ---------------------------------------------------------------------------


def _resolve_allowed_filters(
    tenant_id: str | None,
    table: str,
) -> frozenset[str]:
    """Return the set of filterable columns for *table*.

    Prefers the tenant's ``tenant_column_metadata`` (``is_filterable = TRUE``
    on non-identifier semantic types). Falls back to the hardcoded legacy
    allowlist when the tenant has no metadata yet, or when a metadata lookup
    fails (logged, not raised — we never want dashboard hydration to hard-
    fail because Supabase is transiently unreachable).
    """
    if tenant_id:
        try:
            tenant_map = metadata_service.get_filterable_columns_map(
                user_id=tenant_id
            )
        except Exception:
            logger.exception(
                "Failed to load filterable columns for tenant=%s table=%s; "
                "falling back to legacy allowlist.",
                tenant_id,
                table,
            )
            tenant_map = {}
        if tenant_map:
            return tenant_map.get(table, frozenset())
    return _ALLOWED_FILTERS.get(table, frozenset())


def _resolve_source_is_filterable(
    tenant_id: str | None,
    source: str,
) -> bool:
    """Return True if *source* has any filterable columns for this tenant."""
    return bool(_resolve_allowed_filters(tenant_id, source))


def _resolve_date_columns(tenant_id: str | None) -> dict[str, str]:
    """Return ``{table: primary_date_column}`` for the tenant (metadata-driven).

    Falls back to :data:`_DATE_COLUMNS` when no metadata is present.
    """
    if tenant_id:
        try:
            tenant_map = metadata_service.get_date_columns_map(user_id=tenant_id)
        except Exception:
            logger.exception(
                "Failed to load date columns for tenant=%s; "
                "falling back to legacy _DATE_COLUMNS.",
                tenant_id,
            )
            tenant_map = {}
        if tenant_map:
            return tenant_map
    return dict(_DATE_COLUMNS)

# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------


def _normalize_legacy_sql(query: str) -> str:
    """Map SQLite-style ``DATE(expr)`` to DuckDB ``cast(expr as date)``."""
    return _LEGACY_DATE_CALL.sub(r"cast(\1 as date)", query)


def _query_db(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    params: tuple | None = None,
) -> list[dict[str, Any]]:
    """Execute a read-only SQL query against the tenant DuckDB sandbox."""
    query = _normalize_legacy_sql(query)
    params_list = list(params or ())
    try:
        df = conn.execute(query, params_list).fetchdf()
    except Exception as exc:
        logger.error("DuckDB query failed: %s | params=%s", query[:500], params_list)
        raise
    rows = df.to_dict(orient="records")
    # Clean up string values that look like Python list repr: "['Foo', 'Bar']"
    for row in rows:
        for k, v in list(row.items()):
            if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
                row[k] = v.strip("[]").replace("'", "").strip()
    return rows


def _apply_native_filter_spec(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    params: tuple | list,
    filter_spec: FilterSpec | None,
    relationships: list[dict] | None,
    *,
    widget_id: Any = None,
) -> tuple[str, tuple]:
    """Wrap *query* with filters from *filter_spec* (Group C path).

    Returns ``(rewritten_query, params_tuple)``. When *filter_spec* is
    ``None`` or empty, the original ``(query, params)`` are returned
    untouched. Skipped filters are logged for observability.
    """
    if filter_spec is None or filter_spec.is_empty():
        return query, tuple(params or ())
    t0 = time.perf_counter()
    try:
        new_sql, new_params, application = apply_filters(
            query,
            spec=filter_spec,
            conn=conn,
            relationships=relationships,
            existing_params=params,
        )
    except Exception:
        logger.exception(
            "Filter injection failed for widget %s; running original SQL.",
            widget_id,
        )
        return query, tuple(params or ())
    elapsed_ms = (time.perf_counter() - t0) * 1000
    if application is not None:
        applied_tables = sorted({p.table for p in application.plans})
        skipped = application.skipped or []
        logger.info(
            "event=filter_apply widget=%s tables_applied=%s skipped=%s skip_count=%d latency_ms=%.1f",
            widget_id,
            applied_tables,
            skipped,
            len(skipped),
            elapsed_ms,
        )
    return new_sql, tuple(new_params)


def _is_ts_within_ttl(ts: datetime) -> bool:
    try:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds() < CACHE_TTL_SECONDS
    except (TypeError, ValueError):
        return False


def _clear_preset_cache_for_widget(widget_id: str) -> None:
    keys = [k for k in _PRESET_FILTER_CACHE if k[0] == widget_id]
    for k in keys:
        del _PRESET_FILTER_CACHE[k]


def _maybe_prune_preset_cache() -> None:
    if len(_PRESET_FILTER_CACHE) > _PRESET_CACHE_MAX_ENTRIES:
        _PRESET_FILTER_CACHE.clear()


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------


def _get_latest_data_date(
    conn: duckdb.DuckDBPyConnection,
    *,
    tenant_id: str | None = None,
):
    """Return the most recent date found across all time-series tables.

    Falls back to ``date.today()`` if the database cannot be queried. Uses
    the metadata-driven date-column map when *tenant_id* is supplied so new
    tenant tables are picked up automatically; otherwise falls back to the
    legacy hardcoded list.
    """
    date_map = _resolve_date_columns(tenant_id)
    latest = None
    for table, col in date_map.items():
        if not (_is_safe_identifier(table) and _is_safe_identifier(col)):
            continue
        try:
            rows = _query_db(conn, f"SELECT MAX({col}) AS max_date FROM {table}")
            if rows and rows[0].get("max_date"):
                d = date.fromisoformat(str(rows[0]["max_date"])[:10])
                if latest is None or d > latest:
                    latest = d
        except Exception:
            pass
    return latest or date.today()


# Short TTL so the max-date endpoint does not hit blob-backed DuckDB on every refresh.
_MAX_DATA_DATE_ISO_CACHE: dict[str, tuple[str, datetime]] = {}
_MAX_DATA_DATE_CACHE_TTL_SECONDS = 600.0


def get_max_data_date_iso(tenant_id: str) -> str:
    """Latest calendar date (YYYY-MM-DD) across time-series analytics tables for *tenant_id*."""
    now = datetime.now(timezone.utc)
    cached = _MAX_DATA_DATE_ISO_CACHE.get(tenant_id)
    if cached is not None:
        value, cached_at = cached
        age = (now - cached_at).total_seconds()
        if age >= 0 and age < _MAX_DATA_DATE_CACHE_TTL_SECONDS:
            return value
    conn, _ = get_tenant_sandbox(tenant_id)
    iso = _get_latest_data_date(conn, tenant_id=tenant_id).isoformat()
    _MAX_DATA_DATE_ISO_CACHE[tenant_id] = (iso, now)
    return iso


def build_date_filters_from_params(
    preset: str | None,
    start_date: str | None,
    end_date: str | None,
    tenant_id: str | None = None,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """Build date filter dicts for dashboard hydration.

    Returns ``(filters, preset_cache_key)``. *preset_cache_key* is the preset
    id (e.g. ``last_7_days``) when the range came from a preset query param;
    it is ``None`` for explicit ``start_date`` / ``end_date`` (custom range)
    or when no date filter is requested.

    *tenant_id* scopes preset ranges to the latest date in that user's data;
    if omitted, presets anchor to today's date (degraded behaviour).
    """
    date_cols = set(_resolve_date_columns(tenant_id).values())
    if preset:
        start, end = resolve_date_preset(preset, tenant_id=tenant_id)
        flist = [
            {"column": col, "op": "between", "value": [start, end]}
            for col in date_cols
        ]
        return flist, preset
    if start_date and end_date:
        flist = [
            {"column": col, "op": "between", "value": [start_date, end_date]}
            for col in date_cols
        ]
        return flist, None
    return None, None


def resolve_date_preset(
    preset: str, *, tenant_id: str | None = None
) -> tuple[str, str]:
    """Convert a preset like ``last_7_days`` to *(start, end)* ISO dates.

    The range is relative to the **latest date available in the database**,
    not today's wall-clock date.  This ensures presets always return results
    even when the dataset is historical (e.g. data ends weeks before today).

    The end date is exclusive: ``start <= date < end``.

    Without *tenant_id*, the reference date is **today** (no live DB probe).
    """
    from datetime import timedelta

    days_map = {
        "last_7_days": 7,
        "last_14_days": 14,
        "last_30_days": 30,
        "last_60_days": 60,
        "last_90_days": 90,
    }
    days = days_map.get(preset)
    if days is None:
        raise ValueError(f"Unknown date preset: {preset!r}")
    if tenant_id:
        reference = date.fromisoformat(get_max_data_date_iso(tenant_id))
    else:
        reference = date.today()
    end = reference + timedelta(days=1)  # exclusive upper bound
    start = reference - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


def _build_filter_clause(
    filters: list[dict[str, Any]],
    table: str,
    *,
    tenant_id: str | None = None,
) -> tuple[str, list]:
    """Build ``(clause, params)`` from a list of generic filter dicts.

    Only columns in the tenant's metadata-driven allowlist (see
    :func:`_resolve_allowed_filters`) are accepted; unknown columns or
    operators are silently skipped. The returned *clause* does **not**
    include the ``WHERE`` keyword.
    """
    allowed = _resolve_allowed_filters(tenant_id, table)
    parts: list[str] = []
    params: list[Any] = []

    for f in filters:
        col = f.get("column", "")
        op = f.get("op", "")
        val = f.get("value")

        if col not in allowed or op not in _FILTER_OPS:
            continue
        if not _is_safe_identifier(col):
            continue

        if op == "eq":
            parts.append(f"{col} = ?")
            params.append(val)
        elif op == "neq":
            parts.append(f"{col} != ?")
            params.append(val)
        elif op == "gt":
            parts.append(f"{col} > ?")
            params.append(val)
        elif op == "gte":
            parts.append(f"{col} >= ?")
            params.append(val)
        elif op == "lt":
            parts.append(f"{col} < ?")
            params.append(val)
        elif op == "lte":
            parts.append(f"{col} <= ?")
            params.append(val)
        elif op == "between":
            if isinstance(val, (list, tuple)) and len(val) == 2:
                parts.append(f"{col} >= ? AND {col} < ?")
                params.extend(val)
        elif op == "in":
            if isinstance(val, (list, tuple)) and val:
                placeholders = ", ".join("?" for _ in val)
                parts.append(f"{col} IN ({placeholders})")
                params.extend(val)
        elif op == "not_in":
            if isinstance(val, (list, tuple)) and val:
                placeholders = ", ".join("?" for _ in val)
                parts.append(f"{col} NOT IN ({placeholders})")
                params.extend(val)

    return (" AND ".join(parts), params) if parts else ("", [])


def _inject_where(
    query: str,
    clause: str,
    params: list,
) -> tuple[str, list]:
    """Inject a ``WHERE`` clause into an existing SQL query.

    Tracks parenthesis depth so that keywords inside subqueries are
    ignored — only top-level ``WHERE`` / ``GROUP BY`` / ``ORDER BY`` /
    ``LIMIT`` are considered.
    """
    if not clause:
        return query, []

    upper = query.upper()

    # 1. Look for a top-level WHERE (depth == 0)
    #    Match WHERE preceded by any whitespace (space, newline, tab, etc.)
    depth = 0
    top_where = -1
    for i, ch in enumerate(query):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and ch in (" ", "\n", "\r", "\t") and upper[i + 1 : i + 7] == "WHERE ":
            top_where = i
            break

    if top_where != -1:
        insert_at = top_where + 7  # skip past "<ws>WHERE "
        return (
            query[:insert_at] + clause + " AND " + query[insert_at:],
            params,
        )

    # 2. No top-level WHERE → insert before first top-level keyword
    depth = 0
    for i, ch in enumerate(query):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0:
            for kw in ("GROUP BY", "ORDER BY", "LIMIT"):
                if upper[i:].startswith(kw):
                    return (
                        query[:i] + "WHERE " + clause + " " + query[i:],
                        params,
                    )

    return query.rstrip() + " WHERE " + clause, params


# ---------------------------------------------------------------------------
# KPI hydration
# ---------------------------------------------------------------------------

# Simple identifier pattern – only alphanumeric + underscore
_IDENT_CHARS = set("abcdefghijklmnopqrstuvwxyz_0123456789")


def _is_safe_identifier(name: str) -> bool:
    return bool(name) and set(name.lower()) <= _IDENT_CHARS


def _execute_kpi_aggregation(
    conn: duckdb.DuckDBPyConnection,
    data_config: dict,
    filters: list[dict[str, Any]] | None = None,
    *,
    filter_spec: FilterSpec | None = None,
    relationships: list[dict] | None = None,
    widget_id: Any = None,
    tenant_id: str | None = None,
) -> float | None:
    """Run a single-table KPI aggregation and return the scalar, or None."""
    source = data_config.get("source", "")
    agg = data_config.get("aggregation", "")
    field = data_config.get("field", "")

    if source not in _ALLOWED_TABLES:
        return None

    if agg == "custom":
        formula = data_config.get("formula", "0")
        sql = f"SELECT ({formula}) AS value FROM {source}"
    elif agg == "count_distinct":
        if not _is_safe_identifier(field):
            return None
        sql = f"SELECT COUNT(DISTINCT {field}) AS value FROM {source}"
    elif agg in ("sum", "count", "avg"):
        if not _is_safe_identifier(field):
            return None
        sql = f"SELECT {agg.upper()}({field}) AS value FROM {source}"
    else:
        return None

    params: tuple = ()
    if filters:
        clause, p = _build_filter_clause(filters, source, tenant_id=tenant_id)
        if clause:
            sql += f" WHERE {clause}"
            params = tuple(p)

    sql, params = _apply_native_filter_spec(
        conn, sql, params, filter_spec, relationships, widget_id=widget_id
    )

    rows = _query_db(conn, sql, params)
    if not rows:
        return None
    val = rows[0].get("value")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _hydrate_kpi_components(
    conn: duckdb.DuckDBPyConnection,
    chart_config: dict,
    data_config: dict,
    filters: list[dict[str, Any]] | None = None,
    *,
    filter_spec: FilterSpec | None = None,
    relationships: list[dict] | None = None,
    widget_id: Any = None,
    tenant_id: str | None = None,
) -> dict:
    """Combine multiple table-level KPI parts (set, subtract, divide)."""
    components: list[dict[str, Any]] = data_config.get("components") or []
    if not components:
        return chart_config

    value: float | None = None
    for comp in components:
        op = (comp.get("op") or "set").lower()
        part = _execute_kpi_aggregation(
            conn,
            comp,
            filters,
            filter_spec=filter_spec,
            relationships=relationships,
            widget_id=widget_id,
            tenant_id=tenant_id,
        )
        if part is None:
            part = 0.0

        if op == "set":
            value = part
        elif op == "add":
            value = (value or 0.0) + part
        elif op == "subtract":
            value = (value or 0.0) - part
        elif op == "divide":
            if value is None:
                value = 0.0
            else:
                value = value / part if part else 0.0
        else:
            continue

    if value is not None:
        chart_config["value"] = round(float(value), 2)
    return chart_config


def _hydrate_kpi_from_query(
    conn: duckdb.DuckDBPyConnection,
    chart_config: dict,
    data_config: dict,
    filters: list[dict[str, Any]] | None = None,
    *,
    filter_spec: FilterSpec | None = None,
    relationships: list[dict] | None = None,
    widget_id: Any = None,
    tenant_id: str | None = None,
) -> dict:
    """Run an arbitrary SQL query for a KPI widget and update chart_config['value'].

    Used for agent-generated KPIs that carry a raw ``query`` + ``kpi_value_column``
    rather than the structured ``source``/``aggregation``/``field`` format.
    """
    query = data_config.get("query", "")
    value_col = data_config.get("kpi_value_column", "")
    if not query or not value_col:
        return chart_config

    query = query.rstrip().rstrip(";").rstrip()

    params: tuple = ()
    if filters:
        source = data_config.get("source", "")
        if source and _resolve_source_is_filterable(tenant_id, source):
            clause, p = _build_filter_clause(filters, source, tenant_id=tenant_id)
            if clause:
                query, p = _inject_where(query, clause, p)
                params = tuple(p)
        elif not source and data_config.get("sources"):
            between_val = next(
                (
                    f["value"]
                    for f in filters
                    if f.get("op") == "between"
                    and isinstance(f.get("value"), list)
                    and len(f["value"]) == 2
                ),
                None,
            )
            if between_val:
                query = (
                    f"SELECT * FROM ({query}) AS _src"
                    f" WHERE {value_col} IS NOT NULL"
                )
                # Wrap and apply generic date filter on the outer query
                for col_name in set(_resolve_date_columns(tenant_id).values()):
                    if _is_safe_identifier(col_name):
                        query += f" AND {col_name} >= ? AND {col_name} < ?"
                        params = params + tuple(between_val)

    query, params = _apply_native_filter_spec(
        conn, query, params, filter_spec, relationships, widget_id=widget_id
    )

    try:
        rows = _query_db(conn, query, params)
    except Exception:
        logger.exception("KPI query-based hydration failed")
        return chart_config

    if rows and value_col in rows[0]:
        val = rows[0][value_col]
        try:
            chart_config["value"] = round(float(val), 2)
        except (TypeError, ValueError):
            pass

    return chart_config


def _hydrate_kpi(
    conn: duckdb.DuckDBPyConnection,
    chart_config: dict,
    data_config: dict,
    filters: list[dict[str, Any]] | None = None,
    *,
    filter_spec: FilterSpec | None = None,
    relationships: list[dict] | None = None,
    widget_id: Any = None,
    tenant_id: str | None = None,
) -> dict:
    """Build and run a KPI aggregation query, set chart_config['value']."""
    if data_config.get("components"):
        return _hydrate_kpi_components(
            conn,
            chart_config,
            data_config,
            filters,
            filter_spec=filter_spec,
            relationships=relationships,
            widget_id=widget_id,
            tenant_id=tenant_id,
        )

    # Agent-generated KPIs carry a raw SQL query instead of structured aggregation
    if data_config.get("query") and data_config.get("kpi_value_column"):
        return _hydrate_kpi_from_query(
            conn,
            chart_config,
            data_config,
            filters,
            filter_spec=filter_spec,
            relationships=relationships,
            widget_id=widget_id,
            tenant_id=tenant_id,
        )

    val = _execute_kpi_aggregation(
        conn,
        data_config,
        filters,
        filter_spec=filter_spec,
        relationships=relationships,
        widget_id=widget_id,
        tenant_id=tenant_id,
    )
    chart_config["value"] = round(val, 2) if val is not None else 0

    return chart_config


# ---------------------------------------------------------------------------
# Chart hydration
# ---------------------------------------------------------------------------


def _hydrate_chart(
    conn: duckdb.DuckDBPyConnection,
    chart_config: dict,
    data_config: dict,
    filters: list[dict[str, Any]] | None = None,
    *,
    filter_spec: FilterSpec | None = None,
    relationships: list[dict] | None = None,
    widget_id: Any = None,
    tenant_id: str | None = None,
) -> dict:
    """Execute the chart query and map results into chart_config."""
    query = data_config.get("query")
    if not query:
        return chart_config

    # Strip trailing semicolons — agent-generated SQL often includes them
    # and they can interfere with query wrapping / injection.
    query = query.rstrip().rstrip(";").rstrip()

    # Fix known column-name typos from legacy seed data
    query = query.replace(
        "SELECT product_name,", "SELECT product_names AS product_name,"
    )

    params: tuple = ()
    if filters:
        source = data_config.get("source", "")
        if source and _resolve_source_is_filterable(tenant_id, source):
            # Single-source query: inject WHERE directly.
            clause, p = _build_filter_clause(filters, source, tenant_id=tenant_id)
            if clause:
                query, p = _inject_where(query, clause, p)
                params = tuple(p)
        elif not source and data_config.get("sources"):
            # Multi-source (JOIN) query: column references may be ambiguous, so
            # wrap the whole query and filter on the projected xAxis date alias.
            xaxis_field = data_config.get("mappings", {}).get("xAxis", "")
            if xaxis_field and _is_safe_identifier(xaxis_field):
                between_val = next(
                    (
                        f["value"]
                        for f in filters
                        if f.get("op") == "between"
                        and isinstance(f.get("value"), list)
                        and len(f["value"]) == 2
                    ),
                    None,
                )
                if between_val:
                    query = (
                        f"SELECT * FROM ({query}) AS _src"
                        f" WHERE {xaxis_field} >= ? AND {xaxis_field} < ?"
                    )
                    params = tuple(between_val)

    query, params = _apply_native_filter_spec(
        conn, query, params, filter_spec, relationships, widget_id=widget_id
    )

    logger.debug("_hydrate_chart final query: %s | params: %s", query, params)
    rows = _query_db(conn, query, params)
    if not rows:
        return chart_config

    mappings = data_config.get("mappings", {})
    series = chart_config.get("series", [])

    # ---- xAxis / yAxis category data ----
    for axis_key in ("xAxis", "yAxis"):
        axis_field = mappings.get(axis_key)
        if not axis_field:
            continue
        values = [r.get(axis_field) for r in rows]
        axis_cfg = chart_config.get(axis_key)
        if isinstance(axis_cfg, dict):
            axis_cfg["data"] = values
        elif isinstance(axis_cfg, list):
            # Multi-axis (e.g. dual-axis): fill the category axis
            for ax in axis_cfg:
                if ax.get("type") == "category":
                    ax["data"] = values

    # ---- series mappings ----
    series_maps = mappings.get("series", [])
    if series_maps:
        first = series_maps[0]

        if "nameField" in first and "valueField" in first:
            # Pie / donut
            nf, vf = first["nameField"], first["valueField"]
            data = [
                {"name": str(r.get(nf, "")), "value": r.get(vf, 0)}
                for r in rows
            ]
            if series:
                series[0]["data"] = data

        elif "xField" in first:
            # Scatter
            xf = first["xField"]
            yf = first["yField"]
            sf = first.get("sizeField")
            nf = first.get("nameField")
            data = []
            for r in rows:
                pt: dict[str, Any] = {"value": [r.get(xf, 0), r.get(yf, 0)]}
                if sf:
                    pt["value"].append(r.get(sf, 0))
                if nf:
                    pt["name"] = str(r.get(nf, ""))[:35]
                data.append(pt)
            if series:
                series[0]["data"] = data

        else:
            # Standard series (line / bar) — one mapping per series
            for i, sm in enumerate(series_maps):
                fld = sm.get("field")
                if fld and i < len(series):
                    series[i]["data"] = [r.get(fld, 0) for r in rows]

    # ---- funnel mapping ----
    funnel_fields = mappings.get("funnel")
    if funnel_fields and series and series[0].get("type") == "funnel":
        agg_row = rows[0] if rows else {}
        for item in series[0].get("data", []):
            key = item.get("name", "").lower().replace(" ", "_")
            if key in funnel_fields:
                item["value"] = agg_row.get(key, 0)

    return chart_config


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def hydrate_widget(
    widget: dict,
    *,
    tenant_id: str | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
    force_refresh: bool = False,
    filters: list[dict[str, Any]] | None = None,
    filters_from_preset: str | None = None,
    filter_spec: FilterSpec | None = None,
    relationships: list[dict] | None = None,
) -> dict:
    """Populate a widget's ``chart_config`` with live data.

    *tenant_id* selects the user's Parquet prefix in blob storage. When
    omitted, live queries are skipped and the widget is returned unchanged.

    When *conn* is passed (e.g. from :func:`hydrate_widgets`), it is used for
    SQL execution; otherwise a sandbox is opened per call.

    When *filters* come from a date *preset* (*filters_from_preset* set),
    results are served from an in-process TTL cache. Custom date ranges
    always run a fresh query.

    *filter_spec* (Group C native filtering) and *relationships* are passed
    through to the inner hydrators so widget SQL is rewritten with the
    appropriate per-table predicates before execution.
    """
    data_config = widget.get("data_config")
    if not data_config:
        return widget

    wid = widget.get("id")
    wid_str = str(wid) if wid is not None else None

    if force_refresh and wid_str:
        _clear_preset_cache_for_widget(wid_str)

    has_native_spec = filter_spec is not None and not filter_spec.is_empty()
    spec_hash = _hash_filter_spec(filter_spec)

    # ---- preset filter: in-memory TTL cache (not used for custom ranges) ----
    # The preset cache key is (widget_id, preset, filter_spec_hash) so that
    # distinct filter_spec payloads cache independently; absent specs share
    # the ``"none"`` bucket.
    if (
        filters
        and filters_from_preset
        and wid_str
        and not force_refresh
    ):
        key = (wid_str, filters_from_preset, spec_hash)
        entry = _PRESET_FILTER_CACHE.get(key)
        if entry:
            cached_chart, ts = entry
            if _is_ts_within_ttl(ts):
                widget["chart_config"] = copy.deepcopy(cached_chart)
                return widget

    # ---- fresh query path ----
    if tenant_id is None:
        logger.warning(
            "Skipping widget hydration (no tenant_id) widget_id=%s",
            widget.get("id"),
        )
        return widget

    chart_config = copy.deepcopy(widget.get("chart_config") or {})

    def _run(c: duckdb.DuckDBPyConnection) -> dict:
        if widget.get("type") == "kpi":
            return _hydrate_kpi(
                c,
                chart_config,
                data_config,
                filters,
                filter_spec=filter_spec,
                relationships=relationships,
                widget_id=wid,
                tenant_id=tenant_id,
            )
        return _hydrate_chart(
            c,
            chart_config,
            data_config,
            filters,
            filter_spec=filter_spec,
            relationships=relationships,
            widget_id=wid,
            tenant_id=tenant_id,
        )

    try:
        if conn is not None:
            chart_config = _run(conn)
        else:
            c, _ = get_tenant_sandbox(tenant_id)
            chart_config = _run(c)
    except Exception:
        logger.exception("Failed to hydrate widget %s", widget.get("id"))
        return widget

    widget["chart_config"] = chart_config

    if (
        filters
        and filters_from_preset
        and wid_str
    ):
        key = (wid_str, filters_from_preset, spec_hash)
        _PRESET_FILTER_CACHE[key] = (copy.deepcopy(chart_config), datetime.now(timezone.utc))
        _maybe_prune_preset_cache()

    return widget


def hydrate_widgets(
    widgets: list[dict[str, Any]],
    *,
    tenant_id: str | None = None,
    force_refresh: bool = False,
    filters: list[dict[str, Any]] | None = None,
    filters_from_preset: str | None = None,
    filter_spec: FilterSpec | None = None,
    relationships: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """Hydrate every widget in a list using one DuckDB connection when *tenant_id* is set.

    Each widget is hydrated on its own DuckDB cursor (new connection to the same
    in-memory database) inside a thread-pool so independent SQL queries run
    concurrently — DuckDB 1.x supports parallel reads across connections to the
    same database. A single widget falls back to the simple serial path.
    """
    if not tenant_id:
        return [
            hydrate_widget(
                w,
                tenant_id=None,
                force_refresh=force_refresh,
                filters=filters,
                filters_from_preset=filters_from_preset,
                filter_spec=filter_spec,
                relationships=relationships,
            )
            for w in widgets
        ]

    conn, _ = get_tenant_sandbox(tenant_id)

    if len(widgets) <= 1:
        # Serial path for a single widget — no threading overhead.
        return [
            hydrate_widget(
                w,
                tenant_id=tenant_id,
                conn=conn,
                force_refresh=force_refresh,
                filters=filters,
                filters_from_preset=filters_from_preset,
                filter_spec=filter_spec,
                relationships=relationships,
            )
            for w in widgets
        ]

    # Parallel path: each widget gets its own cursor (independent DuckDB connection
    # to the same in-memory database) so queries don't serialise.
    def _hydrate_one(w: dict[str, Any]) -> dict[str, Any]:
        cursor = conn.cursor()  # new connection, same DB — thread-safe read
        return hydrate_widget(
            w,
            tenant_id=tenant_id,
            conn=cursor,
            force_refresh=force_refresh,
            filters=filters,
            filters_from_preset=filters_from_preset,
            filter_spec=filter_spec,
            relationships=relationships,
        )

    max_workers = min(len(widgets), 8)
    # Preserve original widget order — use index-keyed futures.
    results: list[dict[str, Any]] = [{}] * len(widgets)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_hydrate_one, w): i for i, w in enumerate(widgets)}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception:
                logger.exception("Failed to hydrate widget at index %d", idx)
                results[idx] = widgets[idx]  # return widget unchanged on failure
    return results
