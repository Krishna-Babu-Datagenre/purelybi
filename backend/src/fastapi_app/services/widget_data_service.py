"""
Widget data hydration service.

Executes SQL queries defined in each widget's ``data_config`` against the
user's analytics data in **DuckDB** (views over Parquet in Azure Blob),
and maps the results into the ECharts ``chart_config`` structure.

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

from ai.agents.sql.duckdb_sandbox import create_tenant_sandbox

logger = logging.getLogger(__name__)

# Legacy widget SQL was authored for SQLite (e.g. ``DATE(col)``). DuckDB uses ``cast``.
_LEGACY_DATE_CALL = re.compile(r"\bDATE\s*\(\s*([^)]+?)\s*\)", re.IGNORECASE)

# TTL for in-memory preset-filter query cache.
CACHE_TTL_SECONDS = 300  # 5 minutes

# In-memory TTL cache for unfiltered widget hydration (key: widget id).
# Populated on first dashboard open; subsequent requests within TTL skip DuckDB.
_WIDGET_CACHE: dict[str, tuple[dict, datetime]] = {}
_WIDGET_CACHE_MAX_ENTRIES = 2000

# In-memory cache for preset-filtered widget hydration (key: widget id + preset).
# Custom date ranges never use this cache.
_PRESET_FILTER_CACHE: dict[tuple[str, str], tuple[dict[str, Any], datetime]] = {}
_PRESET_CACHE_MAX_ENTRIES = 4000


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


def _is_ts_within_ttl(ts: datetime) -> bool:
    try:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds() < CACHE_TTL_SECONDS
    except (TypeError, ValueError):
        return False


def _clear_widget_cache(widget_id: str) -> None:
    """Remove all cached entries (unfiltered + preset) for a widget."""
    _WIDGET_CACHE.pop(widget_id, None)
    keys = [k for k in _PRESET_FILTER_CACHE if k[0] == widget_id]
    for k in keys:
        del _PRESET_FILTER_CACHE[k]


# Keep old name as alias so callers that already use it continue to work.
_clear_preset_cache_for_widget = _clear_widget_cache


def _maybe_prune_preset_cache() -> None:
    if len(_PRESET_FILTER_CACHE) > _PRESET_CACHE_MAX_ENTRIES:
        _PRESET_FILTER_CACHE.clear()
    if len(_WIDGET_CACHE) > _WIDGET_CACHE_MAX_ENTRIES:
        _WIDGET_CACHE.clear()


def strip_chart_data(chart_config: dict) -> dict:
    """Remove all data arrays / values from a chart_config.

    Returns a structure-only skeleton (series types, axis config, grid,
    tooltip, etc.) with empty data arrays.  Used both when persisting
    widgets (so the DB never stores stale query results) and when a
    hydration query returns no rows or fails.
    """
    for s in chart_config.get("series", []):
        s["data"] = []
    for axis_key in ("xAxis", "yAxis"):
        ax = chart_config.get(axis_key)
        if isinstance(ax, dict) and "data" in ax:
            ax["data"] = []
        elif isinstance(ax, list):
            for ax_item in ax:
                if isinstance(ax_item, dict) and "data" in ax_item:
                    ax_item["data"] = []
    chart_config.pop("value", None)
    chart_config.pop("sparkline", None)
    chart_config.pop("change", None)
    return chart_config


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

# In-process cache for discovered date columns (keyed by table name).
# Empty string means "probed, no temporal column found".
_DATE_COLUMN_DISCOVERY_CACHE: dict[str, str] = {}


def _discover_date_column(
    conn: duckdb.DuckDBPyConnection, table: str
) -> str | None:
    """Return the best date/timestamp column in *table* via DuckDB introspection.

    Queries ``information_schema.columns`` for any DATE / TIMESTAMP column.
    Results are cached in-process.  Used only by ``_get_latest_data_date`` to
    scan all tables in the sandbox — widget-level hydration uses the explicit
    ``data_config["date_column"]`` instead.
    """
    cached = _DATE_COLUMN_DISCOVERY_CACHE.get(table)
    if cached is not None:
        return cached or None  # empty string → not found

    try:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ?",
            [table],
        ).fetchall()
    except Exception:
        _DATE_COLUMN_DISCOVERY_CACHE[table] = ""
        return None

    date_cols: list[str] = []
    for col_name, data_type in rows:
        upper_type = (data_type or "").upper()
        if any(t in upper_type for t in ("DATE", "TIMESTAMP")):
            date_cols.append(col_name)

    if not date_cols:
        _DATE_COLUMN_DISCOVERY_CACHE[table] = ""
        return None

    # Prefer conventional names
    _PREFERRED = (
        "date", "created_at", "updated_at", "order_date",
        "timestamp", "datetime", "time",
    )
    for pref in _PREFERRED:
        for col in date_cols:
            if col.lower() == pref:
                _DATE_COLUMN_DISCOVERY_CACHE[table] = col
                return col

    # Substring match (e.g. "order_created_at", "event_date")
    for hint in ("date", "created", "time"):
        for col in date_cols:
            if hint in col.lower():
                _DATE_COLUMN_DISCOVERY_CACHE[table] = col
                return col

    # Fall back to the first temporal column
    _DATE_COLUMN_DISCOVERY_CACHE[table] = date_cols[0]
    return date_cols[0]


def _extract_date_range(filters: list[dict[str, Any]]) -> list[str] | None:
    """Return ``[start, end]`` from the first ``between`` filter, or None."""
    for f in filters:
        if (
            f.get("op") == "between"
            and isinstance(f.get("value"), list)
            and len(f["value"]) == 2
        ):
            return f["value"]
    return None


def _get_latest_data_date(conn: duckdb.DuckDBPyConnection):
    """Return the most recent date found across all tables in the sandbox.

    Probes every table/view for a temporal column and takes the global
    MAX.  Falls back to ``date.today()`` if nothing can be queried.
    """
    latest = None

    try:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        ]
    except Exception:
        tables = []

    for table in tables:
        if not _is_safe_identifier(table):
            continue
        date_col = _discover_date_column(conn, table)
        if not date_col or not _is_safe_identifier(date_col):
            continue
        try:
            rows = _query_db(conn, f"SELECT MAX({date_col}) AS max_date FROM {table}")
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
    conn, _ = create_tenant_sandbox(tenant_id)
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
    """Build a column-agnostic date-range filter for dashboard hydration.

    Returns ``(filters, preset_cache_key)``.  The filter list contains at
    most one ``{"op": "between", "value": [start, end]}`` dict — the
    hydration functions combine it with each widget's
    ``data_config["date_column"]`` to inject the actual WHERE clause.

    *preset_cache_key* is the preset id (e.g. ``last_7_days``) when the
    range came from a preset query param; it is ``None`` for explicit
    ``start_date`` / ``end_date`` (custom range) or when no date filter
    is requested.
    """
    if preset:
        start, end = resolve_date_preset(preset, tenant_id=tenant_id)
        return [{"op": "between", "value": [start, end]}], preset
    if start_date and end_date:
        return [{"op": "between", "value": [start_date, end_date]}], None
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
        conn, _ = create_tenant_sandbox(tenant_id)
        try:
            reference = _get_latest_data_date(conn)
        finally:
            conn.close()
    else:
        reference = date.today()
    end = reference + timedelta(days=1)  # exclusive upper bound
    start = reference - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


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

    if not _is_safe_identifier(source):
        return None
    try:
        exists = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [source],
        ).fetchone()
    except Exception:
        return None
    if not exists:
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
        date_col = data_config.get("date_column", "")
        if date_col and _is_safe_identifier(date_col):
            date_range = _extract_date_range(filters)
            if date_range:
                sql += f" WHERE {date_col} >= ? AND {date_col} < ?"
                params = tuple(date_range)

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
        date_col = data_config.get("date_column", "")
        if date_col and _is_safe_identifier(date_col):
            date_range = _extract_date_range(filters)
            if date_range:
                clause = f"{date_col} >= ? AND {date_col} < ?"
                query, p = _inject_where(query, clause, list(date_range))
                params = tuple(p)

    rows = _query_db(conn, query, params)

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
        date_col = data_config.get("date_column", "")
        if date_col and _is_safe_identifier(date_col):
            date_range = _extract_date_range(filters)
            if date_range:
                clause = f"{date_col} >= ? AND {date_col} < ?"
                query, p = _inject_where(query, clause, list(date_range))
                params = tuple(p)

    logger.debug("_hydrate_chart final query: %s | params: %s", query, params)
    rows = _query_db(conn, query, params)
    if not rows:
        return strip_chart_data(chart_config)

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
) -> dict:
    """Populate a widget's ``chart_config`` by executing a live DuckDB query.

    *tenant_id* selects the user's Parquet prefix in blob storage. When
    omitted, live queries are skipped and the widget is returned unchanged.

    When *conn* is passed (e.g. from :func:`hydrate_widgets`), it is used for
    SQL execution; otherwise a sandbox is opened per call.

    When *filters* come from a date *preset* (*filters_from_preset* set),
    results are served from an in-process TTL cache keyed by widget id and
    preset name. Custom date ranges (*filters* without preset) always run a
    fresh query.
    """
    data_config = widget.get("data_config")
    if not data_config:
        # No query to execute — ensure chart_config has no stale data.
        widget["chart_config"] = strip_chart_data(
            copy.deepcopy(widget.get("chart_config") or {})
        )
        return widget

    wid = widget.get("id")
    wid_str = str(wid) if wid is not None else None

    if force_refresh and wid_str:
        _clear_widget_cache(wid_str)

    # ---- unfiltered: in-memory TTL cache (populated on first dashboard open) ----
    if not filters and not force_refresh and wid_str:
        entry = _WIDGET_CACHE.get(wid_str)
        if entry:
            cached_chart, ts = entry
            if _is_ts_within_ttl(ts):
                widget["chart_config"] = copy.deepcopy(cached_chart)
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
            c, _ = create_tenant_sandbox(tenant_id)
            try:
                chart_config = _run(c)
            finally:
                c.close()
    except Exception:
        logger.exception("Failed to hydrate widget %s", widget.get("id"))
        # Clear any stale data baked into the Supabase-stored chart_config so the
        # frontend shows an empty chart rather than phantom data points.
        widget["chart_config"] = strip_chart_data(chart_config)
        return widget

    widget["chart_config"] = chart_config

    # Populate the appropriate cache tier.
    if not filters and wid_str:
        _WIDGET_CACHE[wid_str] = (copy.deepcopy(chart_config), datetime.now(timezone.utc))
        _maybe_prune_preset_cache()
    elif filters and filters_from_preset and wid_str:
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
            )
            for w in widgets
        ]
    conn, _ = create_tenant_sandbox(tenant_id)
    try:
        return [
            hydrate_widget(
                w,
                tenant_id=tenant_id,
                conn=conn,
                force_refresh=force_refresh,
                filters=filters,
                filters_from_preset=filters_from_preset,
            )
            for w in widgets
        ]
    finally:
        conn.close()
