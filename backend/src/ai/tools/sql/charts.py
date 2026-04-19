"""
Chart creation tools for the Database Agent.

This module provides functions to create Plotly charts from data,
exposed as LangChain tools for the agent to use.

Data is stored in a module-level variable because LangChain tools may run
in background threads outside the HTTP request context.
"""

import json
import re
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Literal

import duckdb
import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure

# ============================================================================
# Session Context (for multi-user deployments)
# ============================================================================

# Context variable for session ID - set by frontend before agent invocation
_session_id_var: ContextVar[str | None] = ContextVar(
    "session_id", default=None
)


def set_session_context(session_id: str) -> None:
    """
    Set the session context for the current execution.

    Call this before invoking the agent to ensure chart tools use the correct session.

    Args:
        session_id: Unique identifier for the user session.
    """
    _session_id_var.set(session_id)


def get_session_context() -> str | None:
    """
    Get the current session ID from context.

    Returns:
        The session ID, or None if not set.
    """
    return _session_id_var.get()


def set_discovered_tables(session_id: str, tables: frozenset[str]) -> None:
    """
    Store DuckDB view names discovered for this chat session (from blob layout).

    Used to populate ``data_config`` sources without hardcoding integration names.
    """
    _session_discovered_tables[session_id] = tables


def set_session_conn(session_id: str, conn: duckdb.DuckDBPyConnection | None) -> None:
    """Store the DuckDB connection for *session_id* so schema probing works in data_config builders."""
    if conn is not None:
        _session_conn[session_id] = conn
    else:
        _session_conn.pop(session_id, None)


# ============================================================================
# Module-Level Data Storage (Session-Keyed)
# ============================================================================


@dataclass
class _QuerySnapshot:
    """Pairs a SQL query with the DataFrame it produced so they stay in sync."""

    query: str = ""
    df: pd.DataFrame = field(default_factory=pd.DataFrame)


# Single store keyed by session_id — query and DataFrame travel together.
_session_store: dict[str, _QuerySnapshot] = {}

# View/table names discovered for the tenant (same set as DuckDB sandbox), keyed by session_id.
_session_discovered_tables: dict[str, frozenset[str]] = {}

# DuckDB connection keyed by session_id — used for schema probing in data_config builder.
_session_conn: dict[str, duckdb.DuckDBPyConnection] = {}


def store_query_snapshot(session_id: str, query: str, df: pd.DataFrame) -> None:
    """Atomically store both the SQL query and the DataFrame it produced."""
    _session_store[session_id] = _QuerySnapshot(query=query, df=df)


def store_query_result(session_id: str, data: pd.DataFrame) -> None:
    """
    Store a query result DataFrame for later use by chart tools.

    Backward-compat wrapper — prefers ``store_query_snapshot`` which pairs
    the DataFrame with its originating SQL query.
    """
    existing = _session_store.get(session_id)
    if existing:
        existing.df = data
    else:
        _session_store[session_id] = _QuerySnapshot(query="", df=data)


def store_last_query(session_id: str, query: str) -> None:
    """Backward-compat wrapper — prefers ``store_query_snapshot``."""
    existing = _session_store.get(session_id)
    if existing:
        existing.query = query
    else:
        _session_store[session_id] = _QuerySnapshot(query=query)


def get_last_query(session_id: str) -> str | None:
    """Retrieve the SQL query that produced the current stored DataFrame."""
    snap = _session_store.get(session_id)
    return snap.query if snap and snap.query else None


def get_query_result(session_id: str) -> pd.DataFrame | None:
    """
    Retrieve the last query result DataFrame for a session.

    Returns:
        The stored DataFrame, or None if no data is stored for this session.
    """
    snap = _session_store.get(session_id)
    if snap is not None and not snap.df.empty:
        return snap.df
    return None


def clear_query_result(session_id: str) -> None:
    """Clear the stored query snapshot for a session."""
    _session_store.pop(session_id, None)
    _session_discovered_tables.pop(session_id, None)
    _session_conn.pop(session_id, None)


# ============================================================================
# Chart Creation Functions
# ============================================================================


def create_bar_chart(
    data: list[dict[str, Any]],
    x: str,
    y: str,
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    color: str | None = None,
    orientation: Literal["v", "h"] = "v",
    barmode: Literal["group", "stack", "relative", "overlay"] = "relative",
) -> Figure:
    """
    Create a bar chart from the given data.

    Args:
        data: List of dictionaries containing the data.
        x: Column name for x-axis values.
        y: Column name for y-axis values.
        title: Chart title (optional).
        x_label: Label for x-axis (optional, defaults to column name).
        y_label: Label for y-axis (optional, defaults to column name).
        color: Column name for color grouping (optional).
        orientation: 'v' for vertical bars, 'h' for horizontal bars.
        barmode: Bar mode for grouped data. Options:
            - 'group': Clustered bars side by side.
            - 'stack': Stacked bars on top of each other.
            - 'relative': Stacked bars with negative values below zero.
            - 'overlay': Bars overlaid on top of each other.

    Returns:
        A Plotly Figure object.
    """
    fig = px.bar(
        data,
        x=x,
        y=y,
        title=title,
        labels={x: x_label or x, y: y_label or y},
        color=color,
        orientation=orientation,
        barmode=barmode,
    )
    fig.update_layout(
        template="plotly_white",
        title_font_size=16,
        showlegend=color is not None,
    )
    return fig


def create_line_chart(
    data: list[dict[str, Any]],
    x: str,
    y: str,
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    color: str | None = None,
    markers: bool = True,
) -> Figure:
    """
    Create a line chart from the given data.

    Args:
        data: List of dictionaries containing the data.
        x: Column name for x-axis values.
        y: Column name for y-axis values.
        title: Chart title (optional).
        x_label: Label for x-axis (optional, defaults to column name).
        y_label: Label for y-axis (optional, defaults to column name).
        color: Column name for line grouping/coloring (optional).
        markers: Whether to show markers on the line.

    Returns:
        A Plotly Figure object.
    """
    fig = px.line(
        data,
        x=x,
        y=y,
        title=title,
        labels={x: x_label or x, y: y_label or y},
        color=color,
        markers=markers,
    )
    fig.update_layout(
        template="plotly_white",
        title_font_size=16,
        showlegend=color is not None,
    )
    return fig


def create_pie_chart(
    data: list[dict[str, Any]],
    names: str,
    values: str,
    title: str | None = None,
    hole: float = 0,
) -> Figure:
    """
    Create a pie chart from the given data.

    Args:
        data: List of dictionaries containing the data.
        names: Column name for slice labels.
        values: Column name for slice values.
        title: Chart title (optional).
        hole: Size of the hole in the middle (0-1). Use > 0 for donut chart.

    Returns:
        A Plotly Figure object.
    """
    fig = px.pie(
        data,
        names=names,
        values=values,
        title=title,
        hole=hole,
    )
    fig.update_layout(
        template="plotly_white",
        title_font_size=16,
    )
    return fig


def create_scatter_chart(
    data: list[dict[str, Any]],
    x: str,
    y: str,
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    color: str | None = None,
    size: str | None = None,
) -> Figure:
    """
    Create a scatter plot from the given data.

    Args:
        data: List of dictionaries containing the data.
        x: Column name for x-axis values.
        y: Column name for y-axis values.
        title: Chart title (optional).
        x_label: Label for x-axis (optional, defaults to column name).
        y_label: Label for y-axis (optional, defaults to column name).
        color: Column name for point coloring (optional).
        size: Column name for point sizing (optional).

    Returns:
        A Plotly Figure object.
    """
    fig = px.scatter(
        data,
        x=x,
        y=y,
        title=title,
        labels={x: x_label or x, y: y_label or y},
        color=color,
        size=size,
    )
    fig.update_layout(
        template="plotly_white",
        title_font_size=16,
    )
    return fig


# ============================================================================
# Chart Type Registry
# ============================================================================


CHART_CREATORS = {
    "bar": create_bar_chart,
    "line": create_line_chart,
    "pie": create_pie_chart,
    "scatter": create_scatter_chart,
}


# ============================================================================
# Proxy Tool Function for Agent
# ============================================================================


def create_plotly_chart(
    chart_type: Literal["bar", "line", "pie", "scatter"],
    x: str | None = None,
    y: str | None = None,
    names: str | None = None,
    values: str | None = None,
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    color: str | None = None,
    orientation: Literal["v", "h"] = "v",
    markers: bool = True,
    hole: float = 0,
    barmode: Literal["group", "stack", "relative", "overlay"] = "relative",
) -> str:
    """
    Create a Plotly chart from the most recent SQL query results.

    Prefer ``create_react_chart`` for the web app (JSON payloads for the React
    frontend). This tool emits a Plotly figure for other consumers.

    The data is automatically retrieved from the last sql_db_query execution.
    You do NOT need to pass the data - just specify the chart type and column mappings.

    Args:
        chart_type: The type of chart to create. Options: "bar", "line", "pie", "scatter".
            - Use "bar" for comparing quantities across categories.
            - Use "line" for showing trends over time or continuous data.
            - Use "pie" for showing proportions of a whole.
            - Use "scatter" for showing relationships between two variables.
        x: Column name for x-axis (required for bar, line, scatter charts).
        y: Column name for y-axis (required for bar, line, scatter charts).
        names: Column name for slice labels (required for pie charts).
        values: Column name for slice values (required for pie charts).
        title: Chart title (optional).
        x_label: Custom label for x-axis (optional, defaults to column name).
        y_label: Custom label for y-axis (optional, defaults to column name).
        color: Column name for color grouping (optional). Required for clustered/stacked charts.
        orientation: For bar charts - 'v' for vertical, 'h' for horizontal (default: 'v').
        markers: For line charts - whether to show markers (default: True).
        hole: For pie charts - size of hole in middle, 0-1 (default: 0, use >0 for donut).
        barmode: For bar charts - how to display grouped bars (default: 'relative').
            - 'group': Clustered bars side by side (use with color parameter).
            - 'stack': Stacked bars on top of each other (use with color parameter).
            - 'relative': Stacked with negative values below zero.
            - 'overlay': Bars overlaid on top of each other.

    Returns:
        A JSON string containing the serialized Plotly figure that can be rendered.
    """
    try:
        # Get session_id from context (set by frontend before agent invocation)
        session_id = get_session_context()
        if session_id is None:
            return json.dumps(
                {
                    "error": "Session context not set. Call set_session_context() before invoking the agent."
                }
            )

        # Retrieve data from session-keyed storage
        df = get_query_result(session_id)
        if df is None or df.empty:
            return json.dumps(
                {
                    "error": "No query results available. Please run a SQL query first using sql_db_query."
                }
            )

        # Convert DataFrame to list of dicts for chart functions
        data = df.to_dict(orient="records")

        # Validate chart type
        if chart_type not in CHART_CREATORS:
            return json.dumps(
                {
                    "error": f"Unknown chart type: {chart_type}. Valid options: {list(CHART_CREATORS.keys())}"
                }
            )

        # Build config based on chart type
        if chart_type == "pie":
            if not names or not values:
                return json.dumps(
                    {
                        "error": "Pie charts require 'names' and 'values' parameters."
                    }
                )
            config = {
                "names": names,
                "values": values,
                "title": title,
                "hole": hole,
            }
        else:  # bar, line, scatter
            if not x or not y:
                return json.dumps(
                    {
                        "error": f"{chart_type.title()} charts require 'x' and 'y' parameters."
                    }
                )
            config = {
                "x": x,
                "y": y,
                "title": title,
                "x_label": x_label,
                "y_label": y_label,
                "color": color,
            }
            if chart_type == "bar":
                config["orientation"] = orientation
                config["barmode"] = barmode
            elif chart_type == "line":
                config["markers"] = markers

        # Get the chart creator function
        chart_creator = CHART_CREATORS[chart_type]

        # Create the chart
        fig = chart_creator(data=data, **config)

        # Return the figure as JSON (can be deserialized with plotly.io.from_json)
        return json.dumps(
            {
                "success": True,
                "chart_type": chart_type,
                "figure": fig.to_json(),
            }
        )

    except TypeError as e:
        return json.dumps(
            {
                "error": f"Invalid configuration for {chart_type} chart: {str(e)}"
            }
        )
    except Exception as e:
        return json.dumps({"error": f"Error creating chart: {str(e)}"})


# ============================================================================
# ECharts Configuration Builders (for React frontend)
# ============================================================================


def _build_echarts_bar(
    data: list[dict[str, Any]],
    x: str,
    y: str,
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    color: str | None = None,
    orientation: Literal["v", "h"] = "v",
    barmode: Literal["group", "stack"] = "group",
) -> dict[str, Any]:
    """Build an Apache ECharts option dict for a bar chart."""
    if color:
        # Grouped / stacked: one series per unique value in `color`
        groups: dict[str, list] = {}
        categories: list[str] = []
        for row in data:
            cat = str(row.get(x, ""))
            if cat not in categories:
                categories.append(cat)
            grp = str(row.get(color, ""))
            groups.setdefault(grp, {})[cat] = row.get(y, 0)

        series = []
        for grp_name, cat_vals in groups.items():
            series_data = [cat_vals.get(c, 0) for c in categories]
            s: dict[str, Any] = {
                "name": grp_name,
                "type": "bar",
                "data": series_data,
            }
            if barmode == "stack":
                s["stack"] = "total"
            series.append(s)

        legend = {"data": list(groups.keys())}
    else:
        categories = [str(row.get(x, "")) for row in data]
        values = [row.get(y, 0) for row in data]
        series = [{"type": "bar", "data": values}]
        legend = {}

    if orientation == "h":
        config: dict[str, Any] = {
            "tooltip": {"trigger": "axis"},
            "legend": legend,
            "grid": {
                "left": "3%",
                "right": "4%",
                "bottom": "3%",
                "containLabel": True,
            },
            "yAxis": {
                "type": "category",
                "data": categories,
                "name": x_label or x,
            },
            "xAxis": {"type": "value", "name": y_label or y},
            "series": series,
        }
    else:
        config = {
            "tooltip": {"trigger": "axis"},
            "legend": legend,
            "grid": {
                "left": "3%",
                "right": "4%",
                "bottom": "3%",
                "containLabel": True,
            },
            "xAxis": {
                "type": "category",
                "data": categories,
                "name": x_label or x,
            },
            "yAxis": {"type": "value", "name": y_label or y},
            "series": series,
        }

    if title:
        config["title"] = {"text": title}

    return config


def _build_echarts_line(
    data: list[dict[str, Any]],
    x: str,
    y: str,
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    color: str | None = None,
    area: bool = False,
) -> dict[str, Any]:
    """Build an Apache ECharts option dict for a line (or area) chart."""
    if color:
        groups: dict[str, list] = {}
        categories: list[str] = []
        for row in data:
            cat = str(row.get(x, ""))
            if cat not in categories:
                categories.append(cat)
            grp = str(row.get(color, ""))
            groups.setdefault(grp, {})[cat] = row.get(y, 0)

        series = []
        for grp_name, cat_vals in groups.items():
            s: dict[str, Any] = {
                "name": grp_name,
                "type": "line",
                "smooth": True,
                "data": [cat_vals.get(c, 0) for c in categories],
            }
            if area:
                s["areaStyle"] = {"opacity": 0.15}
            series.append(s)
        legend = {"data": list(groups.keys())}
    else:
        categories = [str(row.get(x, "")) for row in data]
        values = [row.get(y, 0) for row in data]
        s = {"type": "line", "smooth": True, "data": values}
        if area:
            s["areaStyle"] = {"opacity": 0.15}
        series = [s]
        legend = {}

    config: dict[str, Any] = {
        "tooltip": {"trigger": "axis"},
        "legend": legend,
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "3%",
            "containLabel": True,
        },
        "xAxis": {
            "type": "category",
            "boundaryGap": False,
            "data": categories,
            "name": x_label or x,
        },
        "yAxis": {"type": "value", "name": y_label or y},
        "series": series,
    }
    if title:
        config["title"] = {"text": title}

    return config


def _build_echarts_pie(
    data: list[dict[str, Any]],
    names: str,
    values: str,
    title: str | None = None,
    hole: float = 0,
) -> dict[str, Any]:
    """Build an Apache ECharts option dict for a pie / donut chart."""
    pie_data = [
        {"value": row.get(values, 0), "name": str(row.get(names, ""))}
        for row in data
    ]
    radius = ["40%", "70%"] if hole > 0 else "70%"

    config: dict[str, Any] = {
        "tooltip": {"trigger": "item"},
        "legend": {"orient": "vertical", "left": "left"},
        "series": [
            {
                "type": "pie",
                "radius": radius,
                "data": pie_data,
                "emphasis": {
                    "itemStyle": {
                        "shadowBlur": 10,
                        "shadowOffsetX": 0,
                        "shadowColor": "rgba(0, 0, 0, 0.5)",
                    }
                },
            }
        ],
    }
    if title:
        config["title"] = {"text": title}

    return config


def _build_echarts_scatter(
    data: list[dict[str, Any]],
    x: str,
    y: str,
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    color: str | None = None,
) -> dict[str, Any]:
    """Build an Apache ECharts option dict for a scatter chart."""
    if color:
        groups: dict[str, list] = {}
        for row in data:
            grp = str(row.get(color, ""))
            groups.setdefault(grp, []).append([row.get(x, 0), row.get(y, 0)])

        series = [
            {"name": grp_name, "type": "scatter", "data": pts}
            for grp_name, pts in groups.items()
        ]
        legend = {"data": list(groups.keys())}
    else:
        scatter_data = [[row.get(x, 0), row.get(y, 0)] for row in data]
        series = [{"type": "scatter", "data": scatter_data}]
        legend = {}

    config: dict[str, Any] = {
        "tooltip": {"trigger": "item"},
        "legend": legend,
        "xAxis": {"type": "value", "name": x_label or x},
        "yAxis": {"type": "value", "name": y_label or y},
        "series": series,
    }
    if title:
        config["title"] = {"text": title}

    return config


# Map chart type string to the corresponding ECharts builder
ECHARTS_BUILDERS = {
    "bar": _build_echarts_bar,
    "line": _build_echarts_line,
    "area": _build_echarts_line,
    "pie": _build_echarts_pie,
    "scatter": _build_echarts_scatter,
}


# ============================================================================
# data_config builder — enables date filtering after adding to a dashboard
# ============================================================================


def _sql_references_identifier(sql: str, name: str) -> bool:
    """True if *name* appears as a whole SQL identifier (case-insensitive)."""
    if not name:
        return False
    esc = re.escape(name)
    if re.search(rf"\b{esc}\b", sql, re.IGNORECASE):
        return True
    # Quoted identifiers (DuckDB / SQL standard)
    return re.search(rf'"{esc}"', sql) is not None


def _detect_source_tables(sql: str) -> list[str]:
    """Return tenant view names from the session allowlist that appear in *sql*."""
    session_id = get_session_context()
    if not session_id:
        return []
    known = _session_discovered_tables.get(session_id)
    if not known:
        return []
    matched = [t for t in known if _sql_references_identifier(sql, t)]
    return sorted(matched)


def _looks_like_iso_date(val: str) -> bool:
    """Return True if *val* looks like an ISO date or datetime string."""
    import re as _re
    return bool(_re.match(r"^\d{4}-\d{2}-\d{2}", val))


def _detect_date_column(df: pd.DataFrame, col: str | None) -> str | None:
    """If *col* holds date/datetime values in *df*, return it; else None."""
    if not col or col not in df.columns:
        return None
    if pd.api.types.is_datetime64_any_dtype(df[col]):
        return col
    if pd.api.types.is_object_dtype(df[col]):
        sample = df[col].dropna().head(1).tolist()
        if sample and _looks_like_iso_date(str(sample[0])):
            return col
    return None


# Ordered list of column-name prefixes/exact names we prefer as the primary date column.
_PREFERRED_DATE_NAMES = (
    "date", "created_at", "updated_at", "timestamp", "time",
    "released", "published_at", "event_date", "order_date",
)


def _probe_date_column(table: str) -> str | None:
    """Query ``information_schema.columns`` for *table* and return the best date/timestamp column.

    Priority: explicit preferred names first (see ``_PREFERRED_DATE_NAMES``), then first found.
    Returns ``None`` if no date column exists or the connection is unavailable.
    """
    session_id = get_session_context()
    if not session_id:
        return None
    conn = _session_conn.get(session_id)
    if conn is None:
        return None
    try:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? AND table_schema = 'main' "
            "ORDER BY ordinal_position",
            [table],
        ).fetchall()
    except Exception:
        return None

    date_cols = [
        r[0]
        for r in rows
        if r[1].upper().startswith(("DATE", "TIMESTAMP"))
    ]
    if not date_cols:
        return None

    lower_map = {c.lower(): c for c in date_cols}
    for pref in _PREFERRED_DATE_NAMES:
        if pref in lower_map:
            return lower_map[pref]
    return date_cols[0]


def _build_chart_data_config(
    sql: str,
    chart_type: str,
    df: pd.DataFrame,
    *,
    x: str | None = None,
    y: str | None = None,
    names: str | None = None,
    values: str | None = None,
    color: str | None = None,
    date_column: str | None = None,
) -> dict[str, Any] | None:
    """Build a ``data_config`` dict so ``hydrate_widget`` can re-run the query with date filters.

    ``date_column`` is resolved in order:
    1. Explicit *date_column* arg (agent knows best).
    2. Auto-detected from the ``x`` column dtype in *df* (works for time-series charts).
    """
    if not sql:
        return None

    tables = _detect_source_tables(sql)
    if not tables:
        return None

    dc: dict[str, Any] = {"query": sql}

    if len(tables) == 1:
        dc["source"] = tables[0]
    else:
        dc["sources"] = tables

    mappings: dict[str, Any] = {}
    if chart_type == "pie":
        if names and values:
            mappings["series"] = [{"nameField": names, "valueField": values}]
    elif chart_type == "scatter":
        if x and y:
            mappings["xAxis"] = x
            mappings["series"] = [{"xField": x, "yField": y}]
    else:
        if x:
            mappings["xAxis"] = x
        series_list: list[dict[str, str]] = []
        if y:
            series_list.append({"field": y})
        if color:
            series_list.append({"field": color})
        if series_list:
            mappings["series"] = series_list

    if mappings:
        dc["mappings"] = mappings

    # Resolve date_column in priority order:
    # 1. Explicit arg (agent knows best)
    # 2. Auto-detect from x column dtype in the result df (works for time-series on x)
    # 3. Schema probe: query information_schema for the source table's date/timestamp cols
    resolved_date_col = (
        date_column
        or _detect_date_column(df, x)
        or (len(tables) == 1 and _probe_date_column(tables[0]) or None)
    )
    if resolved_date_col:
        dc["date_column"] = resolved_date_col

    return dc


def _build_kpi_data_config(
    sql: str,
    value_column: str,
    date_column: str | None = None,
) -> dict[str, Any] | None:
    """Build a ``data_config`` for a KPI widget so date filtering works.

    *date_column* is the column in the underlying source table that the
    dashboard date-range filter should target.  When set, it is stored as
    ``data_config["date_column"]``.
    """
    if not sql:
        return None

    tables = _detect_source_tables(sql)
    if not tables:
        return None

    dc: dict[str, Any] = {
        "query": sql,
        "kpi_value_column": value_column,
    }
    if len(tables) == 1:
        dc["source"] = tables[0]
    else:
        dc["sources"] = tables

    # Resolve date_column in priority order:
    # 1. Explicit arg (agent knows best)
    # 2. Schema probe: query information_schema for the source table's date/timestamp cols
    resolved_date_col = date_column or (len(tables) == 1 and _probe_date_column(tables[0]) or None)
    if resolved_date_col:
        dc["date_column"] = resolved_date_col

    return dc


# ============================================================================
# React Chart Proxy Tool Function for Agent
# ============================================================================


def create_react_chart(
    chart_type: Literal["bar", "line", "area", "pie", "scatter"],
    x: str | None = None,
    y: str | None = None,
    names: str | None = None,
    values: str | None = None,
    title: str | None = None,
    x_label: str | None = None,
    y_label: str | None = None,
    color: str | None = None,
    orientation: Literal["v", "h"] = "v",
    hole: float = 0,
    barmode: Literal["group", "stack"] = "group",
    area: bool = False,
    date_column: str | None = None,
) -> str:
    """
    Create an Apache ECharts configuration from the most recent SQL query results.

    Use this tool to create charts for the React frontend. The output is an ECharts
    option object that can be rendered directly by the frontend's ECharts renderer.

    The data is automatically retrieved from the last sql_db_query execution.
    You do NOT need to pass the data - just specify the chart type and column mappings.

    Args:
        chart_type: The type of chart to create. Options: "bar", "line", "area", "pie", "scatter".
            - Use "bar" for comparing quantities across categories.
            - Use "line" for showing trends over time or continuous data.
            - Use "area" for filled area charts (same as line but with filled region).
            - Use "pie" for showing proportions of a whole.
            - Use "scatter" for showing relationships between two variables.
        x: Column name for x-axis (required for bar, line, area, scatter charts).
        y: Column name for y-axis (required for bar, line, area, scatter charts).
        names: Column name for slice labels (required for pie charts).
        values: Column name for slice values (required for pie charts).
        title: Chart title (optional).
        x_label: Custom label for x-axis (optional, defaults to column name).
        y_label: Custom label for y-axis (optional, defaults to column name).
        color: Column name for color grouping (optional). Use for multi-series or clustered/stacked charts.
        orientation: For bar charts - 'v' for vertical, 'h' for horizontal (default: 'v').
        hole: For pie charts - size of hole in middle, 0-1 (default: 0, use >0 for donut).
        barmode: For bar charts - how to display grouped bars (default: 'group').
            - 'group': Clustered bars side by side (use with color parameter).
            - 'stack': Stacked bars on top of each other (use with color parameter).
        area: For line charts - whether to fill the area under the line (default: False).
        date_column: Name of the date/timestamp column in the source table for dashboard
            date-range filtering. Always pass this when the source table has a date column.
            Auto-detected only when ``x`` itself is temporal; for non-date x-axes (bar
            rankings, pie breakdowns, etc.) you **must** pass it explicitly.

    Returns:
        A JSON string containing the ECharts configuration and widget metadata
        for the React frontend to render.
    """
    try:
        # Get session_id from context (set by frontend before agent invocation)
        session_id = get_session_context()
        if session_id is None:
            return json.dumps(
                {
                    "error": "Session context not set. Call set_session_context() before invoking the agent."
                }
            )

        # Retrieve data from session-keyed storage
        df = get_query_result(session_id)
        if df is None or df.empty:
            return json.dumps(
                {
                    "error": "No query results available. Please run a SQL query first using sql_db_query."
                }
            )

        # Convert DataFrame to list of dicts for chart functions
        data = df.to_dict(orient="records")

        # Determine the effective chart type for the builder
        effective_type = chart_type
        if chart_type == "area":
            effective_type = "area"

        # Validate chart type
        if effective_type not in ECHARTS_BUILDERS:
            return json.dumps(
                {
                    "error": f"Unknown chart type: {chart_type}. Valid options: {list(ECHARTS_BUILDERS.keys())}"
                }
            )

        # Build ECharts config based on chart type
        if chart_type == "pie":
            if not names or not values:
                return json.dumps(
                    {
                        "error": "Pie charts require 'names' and 'values' parameters."
                    }
                )
            echarts_config = _build_echarts_pie(
                data=data, names=names, values=values, title=title, hole=hole
            )
        elif chart_type in ("line", "area"):
            if not x or not y:
                return json.dumps(
                    {
                        "error": f"{chart_type.title()} charts require 'x' and 'y' parameters."
                    }
                )
            use_area = chart_type == "area" or area
            echarts_config = _build_echarts_line(
                data=data,
                x=x,
                y=y,
                title=title,
                x_label=x_label,
                y_label=y_label,
                color=color,
                area=use_area,
            )
        elif chart_type == "scatter":
            if not x or not y:
                return json.dumps(
                    {"error": "Scatter charts require 'x' and 'y' parameters."}
                )
            echarts_config = _build_echarts_scatter(
                data=data,
                x=x,
                y=y,
                title=title,
                x_label=x_label,
                y_label=y_label,
                color=color,
            )
        else:  # bar
            if not x or not y:
                return json.dumps(
                    {"error": "Bar charts require 'x' and 'y' parameters."}
                )
            echarts_config = _build_echarts_bar(
                data=data,
                x=x,
                y=y,
                title=title,
                x_label=x_label,
                y_label=y_label,
                color=color,
                orientation=orientation,
                barmode=barmode,
            )

        # Map chart_type to the WidgetType expected by the frontend
        widget_type = "area" if chart_type == "area" else chart_type

        result: dict[str, Any] = {
            "success": True,
            "chart_type": widget_type,
            "chartConfig": echarts_config,
        }

        last_sql = get_last_query(session_id)
        if last_sql:
            data_cfg = _build_chart_data_config(
                last_sql,
                chart_type,
                df,
                x=x,
                y=y,
                names=names,
                values=values,
                color=color,
                date_column=date_column,
            )
            if data_cfg:
                result["dataConfig"] = data_cfg

        return json.dumps(result)

    except TypeError as e:
        return json.dumps(
            {
                "error": f"Invalid configuration for {chart_type} chart: {str(e)}"
            }
        )
    except Exception as e:
        return json.dumps({"error": f"Error creating chart: {str(e)}"})


def _scalar_to_float(val: Any) -> float:
    """Coerce a single cell value to float for KPI / sparkline values."""
    import math

    if val is None:
        raise ValueError("value is null")
    if isinstance(val, float) and (math.isnan(val) or pd.isna(val)):
        raise ValueError("value is NaN")
    if isinstance(val, (int, float)):
        return float(val)
    if hasattr(val, "dtype") and pd.isna(val):
        raise ValueError("value is NaN")
    from decimal import Decimal

    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, str):
        s = val.strip().replace(",", "")
        if not s:
            raise ValueError("empty value")
        return float(s)
    return float(val)


def create_react_kpi(
    value_column: str,
    title: str,
    prefix: str | None = None,
    suffix: str | None = None,
    change_column: str | None = None,
    change_label: str | None = None,
    icon: Literal["revenue", "orders", "aov", "customers", "generic"] | None = None,
    sparkline_value_column: str | None = None,
    sparkline_max_points: int = 32,
    date_column: str | None = None,
) -> str:
    """
    Build a KPI card config from the most recent ``sql_db_query`` result for the React dashboard.

    Use after a query that returns at least one row. For a single headline number, the query
    should typically return one row (e.g. ``SELECT SUM(amount) AS total``). For a sparkline,
    return multiple rows ordered by time or sequence and pass ``sparkline_value_column``.

    Data is read from the same session-scoped store as ``create_react_chart`` (last query only).

    Args:
        value_column: Column name for the main KPI value (read from the first row).
        title: Short label for the KPI (e.g. "Total revenue") — shown when adding to a dashboard.
        prefix: Optional display prefix (e.g. "$").
        suffix: Optional display suffix (e.g. "%" or " orders").
        change_column: Optional column on the first row with a numeric delta or percent change.
        change_label: Optional label for the change (e.g. "vs last month").
        icon: Optional icon hint for the UI: revenue, orders, aov, customers, or generic.
        sparkline_value_column: If set and the result has multiple rows, build a sparkline from
            this column (most recent ``sparkline_max_points`` values, in row order).
        sparkline_max_points: Cap on sparkline length when many rows are returned.
        date_column: Name of the date/timestamp column in the source table for dashboard
            date-range filtering. Always pass this when the source table has a date column
            (e.g. ``"created_at"`` for orders, ``"date"`` for comments). Omit only for
            KPIs that are truly time-agnostic (e.g. total product count).

    Returns:
        JSON string with ``success``, ``chart_type`` ``"kpi"``, ``chartConfig`` (KpiConfig-shaped
        dict for the frontend), and ``title``.
    """
    try:
        session_id = get_session_context()
        if session_id is None:
            return json.dumps(
                {
                    "error": "Session context not set. Call set_session_context() before invoking the agent."
                }
            )

        df = get_query_result(session_id)
        if df is None or df.empty:
            return json.dumps(
                {
                    "error": "No query results available. Run sql_db_query first with a query that returns at least one row."
                }
            )

        if value_column not in df.columns:
            return json.dumps(
                {
                    "error": f"Column '{value_column}' not in query result. Available: {list(df.columns)}"
                }
            )

        first = df.iloc[0]
        try:
            value = _scalar_to_float(first[value_column])
        except (ValueError, TypeError) as e:
            return json.dumps(
                {"error": f"Could not parse KPI value from '{value_column}': {e}"}
            )

        kpi: dict[str, Any] = {
            "value": value,
            "prefix": prefix,
            "suffix": suffix,
        }

        if change_column:
            if change_column not in df.columns:
                return json.dumps(
                    {
                        "error": f"change_column '{change_column}' not in query result columns: {list(df.columns)}"
                    }
                )
            try:
                kpi["change"] = _scalar_to_float(first[change_column])
            except (ValueError, TypeError) as e:
                return json.dumps(
                    {"error": f"Could not parse change from '{change_column}': {e}"}
                )
        if change_label is not None:
            kpi["changeLabel"] = change_label

        if icon is not None:
            kpi["icon"] = icon

        if sparkline_value_column:
            if sparkline_value_column not in df.columns:
                return json.dumps(
                    {
                        "error": f"sparkline_value_column '{sparkline_value_column}' not in columns: {list(df.columns)}"
                    }
                )
            raw_series = df[sparkline_value_column].tolist()
            sparkline: list[float] = []
            for cell in raw_series[-sparkline_max_points:]:
                try:
                    sparkline.append(_scalar_to_float(cell))
                except (ValueError, TypeError):
                    continue
            if sparkline:
                kpi["sparkline"] = sparkline

        kpi = {k: v for k, v in kpi.items() if v is not None}

        result: dict[str, Any] = {
            "success": True,
            "chart_type": "kpi",
            "title": title,
            "chartConfig": kpi,
        }

        last_sql = get_last_query(session_id)
        if last_sql:
            data_cfg = _build_kpi_data_config(last_sql, value_column, date_column=date_column)
            if data_cfg:
                result["dataConfig"] = data_cfg

        return json.dumps(result)
    except Exception as e:
        return json.dumps({"error": f"Error creating KPI: {str(e)}"})
