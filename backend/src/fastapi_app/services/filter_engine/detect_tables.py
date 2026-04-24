"""Detect the base tables referenced by a widget SQL statement.

Primary path: DuckDB ``EXPLAIN (FORMAT JSON) <sql>`` and walk the plan tree
collecting ``SEQ_SCAN`` / ``READ_PARQUET`` nodes. This is the most accurate
because DuckDB resolves CTEs, view chains, and ``USING`` joins for us.

Fallback: ``sqlglot`` AST walk. Used when the connection is unavailable or
``EXPLAIN`` fails (e.g. the widget SQL references a view that does not exist
in this connection \u2014 mostly relevant in tests).
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import duckdb
import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

# Plan node types that DuckDB emits for a base-table read.
_SCAN_NODE_TYPES = frozenset(
    {
        "SEQ_SCAN",
        "READ_PARQUET",
        "READ_CSV",
        "TABLE_SCAN",
    }
)

# Strip trailing semicolons / whitespace from widget SQL before EXPLAIN.
_TRAILING_SEMI_RE = re.compile(r";\s*$")


def detect_referenced_tables(
    sql: str,
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> set[str]:
    """Return the set of base-table names referenced by *sql*.

    CTE names, subquery aliases, and derived tables are excluded.
    """
    sql_clean = _TRAILING_SEMI_RE.sub("", sql.strip())
    if not sql_clean:
        return set()

    if conn is not None:
        try:
            return _detect_via_explain(conn, sql_clean)
        except Exception:
            logger.debug(
                "EXPLAIN-based table detection failed; falling back to sqlglot",
                exc_info=True,
            )

    return _detect_via_sqlglot(sql_clean)


# ---------------------------------------------------------------------------
# DuckDB EXPLAIN path
# ---------------------------------------------------------------------------


def _detect_via_explain(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
) -> set[str]:
    rows = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchall()
    if not rows:
        return set()
    # DuckDB returns one or two rows of (kind, plan_json). Concatenate any
    # JSON payloads we find.
    plans: list[Any] = []
    for row in rows:
        for cell in row:
            if not isinstance(cell, str):
                continue
            cell_stripped = cell.strip()
            if not cell_stripped.startswith(("{", "[")):
                continue
            try:
                plans.append(json.loads(cell_stripped))
            except json.JSONDecodeError:
                continue
    if not plans:
        return set()

    found: set[str] = set()
    for plan in plans:
        _walk_plan(plan, found)
    return found


def _walk_plan(node: Any, out: set[str]) -> None:
    if isinstance(node, list):
        for child in node:
            _walk_plan(child, out)
        return
    if not isinstance(node, dict):
        return

    name = node.get("name") or node.get("operator_type") or node.get("node_type")
    if isinstance(name, str) and name.strip().upper() in _SCAN_NODE_TYPES:
        table = _extract_scan_table(node)
        if table:
            out.add(table)

    for value in node.values():
        if isinstance(value, (dict, list)):
            _walk_plan(value, out)


def _extract_scan_table(node: dict[str, Any]) -> str | None:
    """Pull the relation name from a DuckDB scan node."""
    for key in ("table_name", "relation", "Table"):
        value = node.get(key)
        if isinstance(value, str) and value:
            return value.split(".")[-1]
    extra = node.get("extra_info")
    if isinstance(extra, dict):
        for key in ("Table", "table_name", "Relation"):
            value = extra.get(key)
            if isinstance(value, str) and value:
                return value.split(".")[-1]
    elif isinstance(extra, str):
        # Older DuckDB versions render ``extra_info`` as a free-form string
        # like ``"shopify_orders\\n[INFO ...]"``. Take the first token.
        first = extra.splitlines()[0].strip() if extra else ""
        if first and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", first):
            return first
    return None


# ---------------------------------------------------------------------------
# sqlglot fallback
# ---------------------------------------------------------------------------


def _detect_via_sqlglot(sql: str) -> set[str]:
    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        logger.debug("sqlglot parse failed for SQL: %s", sql[:200], exc_info=True)
        return set()
    if tree is None:
        return set()

    cte_names = {
        cte.alias_or_name.lower()
        for cte in tree.find_all(exp.CTE)
        if cte.alias_or_name
    }

    out: set[str] = set()
    for table in tree.find_all(exp.Table):
        name = table.name
        if not name:
            continue
        if name.lower() in cte_names:
            continue
        out.add(name)
    return out
