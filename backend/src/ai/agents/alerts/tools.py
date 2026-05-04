"""
Tools for the Alert Builder agent.

- list_user_tables: enumerate available DuckDB views/tables
- inspect_columns: show schema + sample rows for a table
- validate_metric_sql: run SQL in read-only sandbox; reject non-scalar results
- propose_alert: emit a structured AlertDefinition for frontend preview
"""

from __future__ import annotations

import json
import logging
import threading
from contextvars import ContextVar
from typing import Any

import duckdb
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

# Context var set by the router before each SSE stream
_alert_conn_var: ContextVar[duckdb.DuckDBPyConnection | None] = ContextVar(
    "_alert_conn_var", default=None
)
_alert_user_id_var: ContextVar[str | None] = ContextVar(
    "_alert_user_id_var", default=None
)

# Stores the latest proposed alert for the frontend to pick up
_alert_proposal_var: ContextVar[dict[str, Any] | None] = ContextVar(
    "_alert_proposal_var", default=None
)

_tenant_locks: dict[str, threading.Lock] = {}

def _get_tenant_lock() -> threading.Lock:
    user_id = _alert_user_id_var.get(None)
    if not user_id:
        return threading.Lock()
    if user_id not in _tenant_locks:
        _tenant_locks[user_id] = threading.Lock()
    return _tenant_locks[user_id]


def set_alert_tool_context(
    conn: duckdb.DuckDBPyConnection, user_id: str
) -> None:
    """Set context for alert tools (called before streaming)."""
    _alert_conn_var.set(conn)
    _alert_user_id_var.set(user_id)
    _alert_proposal_var.set(None)


def get_alert_proposal() -> dict[str, Any] | None:
    """Retrieve the latest proposed alert (for SSE emission)."""
    return _alert_proposal_var.get(None)


def _get_conn() -> duckdb.DuckDBPyConnection:
    conn = _alert_conn_var.get(None)
    if conn is None:
        raise RuntimeError("Alert tool context not set — no DuckDB connection.")
    return conn


@tool
def list_user_tables() -> str:
    """List all available data tables/views for the user's analytics data."""
    conn = _get_conn()
    try:
        with _get_tenant_lock():
            tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables]
        if not table_names:
            return "No tables found. The user may need to sync data first."
        return "Available tables:\n" + "\n".join(f"  - {t}" for t in sorted(table_names))
    except Exception as e:
        return f"Error listing tables: {e}"


@tool
def inspect_columns(table_name: str) -> str:
    """Show the column names, types, and a few sample rows for a table.

    Args:
        table_name: Name of the table to inspect.
    """
    conn = _get_conn()
    try:
        with _get_tenant_lock():
            # Schema
            cols = conn.execute(f"DESCRIBE {table_name}").fetchall()
            
            # Sample rows (limit 5)
            sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()

        schema_lines = [f"  {c[0]}: {c[1]}" for c in cols]
        sample_str = sample.to_string(index=False, max_cols=10)

        return (
            f"Schema for '{table_name}':\n"
            + "\n".join(schema_lines)
            + f"\n\nSample rows (first 5):\n{sample_str}"
        )
    except Exception as e:
        return f"Error inspecting table '{table_name}': {e}"


@tool
def validate_metric_sql(sql: str) -> str:
    """Run a metric SQL query in read-only mode and validate it returns exactly one numeric scalar.

    Args:
        sql: The SQL SELECT statement to validate. Must return exactly one row with one numeric column.
    """
    conn = _get_conn()

    # Basic safety checks
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT"):
        return "Error: Only SELECT statements are allowed."
    for forbidden in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"):
        if forbidden in sql_upper.split():
            return f"Error: {forbidden} statements are not allowed."

    try:
        with _get_tenant_lock():
            result = conn.execute(sql).fetchall()
        if len(result) == 0:
            return "Error: Query returned 0 rows. The metric SQL must return exactly 1 row."
        if len(result) > 1:
            return f"Error: Query returned {len(result)} rows. The metric SQL must return exactly 1 row with 1 numeric column."
        row = result[0]
        if len(row) != 1:
            return f"Error: Query returned {len(row)} columns. Expected exactly 1 numeric column."
        value = row[0]
        if value is None:
            return "Warning: Query returned NULL. This may indicate no matching data for the time window."
        try:
            float(value)
        except (TypeError, ValueError):
            return f"Error: Query returned non-numeric value: {value!r}. Expected a number."
        return f"✓ Valid. Query returned: {value}"
    except Exception as e:
        return f"Error executing SQL: {e}"


@tool
def propose_alert(
    name: str,
    metric_description: str,
    table: str,
    metric_sql: str,
    comparator: str,
    threshold: float,
    time_window: str,
    notification_target: str,
    notification_channel: str = "email",
) -> str:
    """Propose a complete alert definition for user confirmation.

    This emits a structured alert preview that the frontend displays.
    Call this once you have validated the SQL and gathered all parameters.

    Args:
        name: Human-readable alert name.
        metric_description: What the metric measures.
        table: Source table name.
        metric_sql: Validated SQL returning one numeric scalar.
        comparator: One of gt, gte, lt, lte, eq, neq, pct_change_gt, pct_change_lt.
        threshold: The threshold value for the comparison.
        time_window: Time window description (e.g. "yesterday", "last_7_days").
        notification_channel: Notification channel (email for v1).
        notification_target: Email address to send the notification to.
    """
    valid_comparators = {"gt", "gte", "lt", "lte", "eq", "neq", "pct_change_gt", "pct_change_lt"}

    if comparator not in valid_comparators:
        return f"Error: Invalid comparator '{comparator}'. Must be one of: {', '.join(sorted(valid_comparators))}"

    proposal = {
        "name": name,
        "metric_description": metric_description,
        "table": table,
        "metric_sql": metric_sql,
        "comparator": comparator,
        "threshold": threshold,
        "time_window": time_window,
        "notification_channel": notification_channel,
        "notification_target": notification_target,
    }
    _alert_proposal_var.set(proposal)

    # Build a human-readable summary
    comp_symbols = {
        "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
        "eq": "==", "neq": "!=",
        "pct_change_gt": "% change >", "pct_change_lt": "% change <",
    }
    symbol = comp_symbols.get(comparator, comparator)

    return (
        f"Alert proposed successfully!\n\n"
        f"  Name: {name}\n"
        f"  Metric: {metric_description}\n"
        f"  Condition: value {symbol} {threshold}\n"
        f"  Time window: {time_window}\n"
        f"  Target: {notification_target}\n\n"
        f"Instruct the user to review the details in the preview card, make any desired edits, and click the 'Save Alert' button directly. You cannot save the alert yourself."
    )
