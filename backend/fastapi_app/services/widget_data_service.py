"""
Widget data hydration service.

Executes SQL queries defined in each widget's ``data_config`` against the
user's analytics data in **DuckDB** (views over Parquet in Azure Blob),
maps the results into the ECharts ``chart_config`` structure, and caches
the hydrated config in ``data_snapshot`` so subsequent reads are instant.

Follows the same pattern as ``bi_templates/shopify_metaads.py`` but
works generically for any widget whose ``data_config`` describes a
query + mappings.
"""

from __future__ import annotations

import copy
import logging
import re
from datetime import date, datetime, timezone
from typing import Any

import duckdb

from streamchat.duckdb_sandbox import create_tenant_sandbox

logger = logging.getLogger(__name__)

# Legacy widget SQL was authored for SQLite (e.g. ``DATE(col)``). DuckDB uses ``cast``.
_LEGACY_DATE_CALL = re.compile(r"\bDATE\s*\(\s*([^)]+?)\s*\)", re.IGNORECASE)

# Widgets with a data_snapshot younger than this are considered fresh.
CACHE_TTL_SECONDS = 300  # 5 minutes

# In-memory cache for preset-filtered widget hydration (key: widget id + preset).
# Custom date ranges never use this cache.
_PRESET_FILTER_CACHE: dict[tuple[str, str], tuple[dict[str, Any], datetime]] = {}
_PRESET_CACHE_MAX_ENTRIES = 4000

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


def _is_cache_fresh(widget: dict) -> bool:
    """Return True when data_snapshot exists and is younger than CACHE_TTL."""
    refreshed = widget.get("data_refreshed_at")
    if not refreshed or not widget.get("data_snapshot"):
        return False
    try:
        if isinstance(refreshed, str):
            dt = datetime.fromisoformat(refreshed.replace("Z", "+00:00"))
        else:
            dt = refreshed
        return (
            datetime.now(timezone.utc) - dt
        ).total_seconds() < CACHE_TTL_SECONDS
    except (ValueError, TypeError):
        return False


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


def _get_latest_data_date(conn: duckdb.DuckDBPyConnection):
    """Return the most recent date found across all time-series tables.

    Falls back to ``date.today()`` if the database cannot be queried.
    """
    _DATE_QUERIES = [
        "SELECT MAX(created_at) AS max_date FROM shopify_orders",
        "SELECT MAX(date) AS max_date FROM meta_daily_insights",
    ]
    latest = None
    for q in _DATE_QUERIES:
        try:
            rows = _query_db(conn, q)
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
    conn = create_tenant_sandbox(tenant_id)
    try:
        iso = _get_latest_data_date(conn).isoformat()
    finally:
        conn.close()
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
    if preset:
        start, end = resolve_date_preset(preset, tenant_id=tenant_id)
        flist = [
            {"column": col, "op": "between", "value": [start, end]}
            for col in set(_DATE_COLUMNS.values())
        ]
        return flist, preset
    if start_date and end_date:
        flist = [
            {"column": col, "op": "between", "value": [start_date, end_date]}
            for col in set(_DATE_COLUMNS.values())
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
        conn = create_tenant_sandbox(tenant_id)
        try:
            reference = _get_latest_data_date(conn)
        finally:
            conn.close()
    else:
        reference = date.today()
    end = reference + timedelta(days=1)  # exclusive upper bound
    start = reference - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


def _build_filter_clause(
    filters: list[dict[str, Any]],
    table: str,
) -> tuple[str, list]:
    """Build ``(clause, params)`` from a list of generic filter dicts.

    Only columns in ``_ALLOWED_FILTERS[table]`` are accepted; unknown
    columns or operators are silently skipped.  The returned *clause*
    does **not** include the ``WHERE`` keyword.
    """
    allowed = _ALLOWED_FILTERS.get(table, frozenset())
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
        clause, p = _build_filter_clause(filters, source)
        if clause:
            sql += f" WHERE {clause}"
            params = tuple(p)

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
) -> dict:
    """Combine multiple table-level KPI parts (set, subtract, divide)."""
    components: list[dict[str, Any]] = data_config.get("components") or []
    if not components:
        return chart_config

    value: float | None = None
    for comp in components:
        op = (comp.get("op") or "set").lower()
        part = _execute_kpi_aggregation(conn, comp, filters)
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
        if source and source in _ALLOWED_FILTERS:
            clause, p = _build_filter_clause(filters, source)
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
                for col_name in set(_DATE_COLUMNS.values()):
                    if _is_safe_identifier(col_name):
                        query += f" AND {col_name} >= ? AND {col_name} < ?"
                        params = params + tuple(between_val)

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
) -> dict:
    """Build and run a KPI aggregation query, set chart_config['value']."""
    if data_config.get("components"):
        return _hydrate_kpi_components(conn, chart_config, data_config, filters)

    # Agent-generated KPIs carry a raw SQL query instead of structured aggregation
    if data_config.get("query") and data_config.get("kpi_value_column"):
        return _hydrate_kpi_from_query(conn, chart_config, data_config, filters)

    val = _execute_kpi_aggregation(conn, data_config, filters)
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
        if source and source in _ALLOWED_FILTERS:
            # Single-source query: inject WHERE directly.
            clause, p = _build_filter_clause(filters, source)
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
    persist_cache: bool = True,
) -> dict:
    """Populate a widget's ``chart_config`` with live data.

    *tenant_id* selects the user's Parquet prefix in blob storage. When
    omitted, live queries are skipped and the widget is returned unchanged.

    When *conn* is passed (e.g. from :func:`hydrate_widgets`), it is used for
    SQL execution; otherwise a sandbox is opened per call.

    When *force_refresh* is False, no *filters* are active, and a fresh
    cache exists, the cached ``data_snapshot`` is returned directly.

    When *filters* come from a date *preset* (*filters_from_preset* set),
    results are also served from an in-process TTL cache (same window as
    ``CACHE_TTL_SECONDS``). Custom date ranges (*filters* without preset)
    always run a fresh query.

    After a fresh query, ``data_snapshot`` and ``data_refreshed_at`` are
    set on the widget dict for **unfiltered** loads only, and a
    ``_cache_dirty`` flag is added so the caller can persist the cache
    (unless *persist_cache* is False, e.g. for live template views).
    """
    data_config = widget.get("data_config")
    if not data_config:
        return widget

    wid = widget.get("id")
    wid_str = str(wid) if wid is not None else None

    if force_refresh and wid_str:
        _clear_preset_cache_for_widget(wid_str)

    # ---- cached path (only when no filters are active) ----
    if not filters and not force_refresh and _is_cache_fresh(widget):
        widget["chart_config"] = widget["data_snapshot"]
        return widget

    # ---- preset filter: in-memory TTL cache (not used for custom ranges) ----
    if (
        filters
        and filters_from_preset
        and wid_str
        and not force_refresh
    ):
        key = (wid_str, filters_from_preset)
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
            return _hydrate_kpi(c, chart_config, data_config, filters)
        return _hydrate_chart(c, chart_config, data_config, filters)

    try:
        if conn is not None:
            chart_config = _run(conn)
        else:
            c = create_tenant_sandbox(tenant_id)
            try:
                chart_config = _run(c)
            finally:
                c.close()
    except Exception:
        logger.exception("Failed to hydrate widget %s", widget.get("id"))
        return widget

    now = datetime.now(timezone.utc).isoformat()
    widget["chart_config"] = chart_config

    # Only update the persisted cache for unfiltered queries (user widgets table)
    if persist_cache and not filters:
        widget["data_snapshot"] = chart_config
        widget["data_refreshed_at"] = now
        widget["_cache_dirty"] = True

    if filters and filters_from_preset and wid_str:
        key = (wid_str, filters_from_preset)
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
    persist_cache: bool = True,
) -> list[dict[str, Any]]:
    """Hydrate every widget in a list using one DuckDB connection when *tenant_id* is set."""
    if not tenant_id:
        return [
            hydrate_widget(
                w,
                tenant_id=None,
                force_refresh=force_refresh,
                filters=filters,
                filters_from_preset=filters_from_preset,
                persist_cache=persist_cache,
            )
            for w in widgets
        ]
    conn = create_tenant_sandbox(tenant_id)
    try:
        return [
            hydrate_widget(
                w,
                tenant_id=tenant_id,
                conn=conn,
                force_refresh=force_refresh,
                filters=filters,
                filters_from_preset=filters_from_preset,
                persist_cache=persist_cache,
            )
            for w in widgets
        ]
    finally:
        conn.close()
