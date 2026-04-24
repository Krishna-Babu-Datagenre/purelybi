"""Per-table filter \"view plans\" and SQL rewriting.

The plan (``docs/native_dashboard_filtering.md`` \u00a75) describes installing
``CREATE OR REPLACE TEMP VIEW`` shadows. Our DuckDB sandbox today exposes
each base dataset as a regular view (no ``_raw.*`` schema), and the
hydration layer reuses connection cursors across widgets, so live shadowing
of those view names would mutate shared state.

We achieve the same outcome \u2014 widget SQL stays untouched semantically \u2014 by
rewriting each base-table reference into a derived subquery with the
filter's predicates baked in:

    SELECT ... FROM shopify_orders   ->   SELECT ... FROM (
        SELECT * FROM shopify_orders WHERE created_at BETWEEN ? AND ?
    ) AS shopify_orders

This is what Superset does internally and matches the plan's *intent*
(\"DuckDB resolves the view inside CTEs, subqueries, joins, and unions\")
without requiring a sandbox refactor.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable

import sqlglot
from sqlglot import exp

from fastapi_app.models.filters import (
    CategoricalFilter,
    Filter,
    FilterSpec,
    NumericFilter,
    TimeFilter,
)

from .relationships import PathStep, RelationshipGraph, find_filter_path

logger = logging.getLogger(__name__)

# Identifier hygiene: only allow conservative SQL identifiers in column /
# table names so the rewritten SQL cannot smuggle in injection.
import re

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe(name: str) -> bool:
    return bool(_SAFE_IDENT.match(name or ""))


# ---------------------------------------------------------------------------
# Plan dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TablePredicate:
    """A single ``WHERE`` fragment for one base table."""

    sql: str
    params: list[Any] = field(default_factory=list)


@dataclass
class ViewPlan:
    """All predicates that must be ANDed onto reads of *table*."""

    table: str
    predicates: list[TablePredicate] = field(default_factory=list)

    def render_subquery_sql(self) -> tuple[str, list[Any]]:
        """Return the ``SELECT * FROM <table> WHERE ...`` body and bound params."""
        parts: list[str] = []
        params: list[Any] = []
        for p in self.predicates:
            parts.append(f"({p.sql})")
            params.extend(p.params)
        where = " AND ".join(parts)
        body = f"SELECT * FROM {self.table}"
        if where:
            body = f"{body} WHERE {where}"
        return body, params


@dataclass
class FilterApplication:
    """Result of planning a :class:`FilterSpec` against a widget."""

    plans: list[ViewPlan]
    skipped: list[dict[str, str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Predicate builders \u2014 one per filter kind
# ---------------------------------------------------------------------------


def _direct_predicate(filt: Filter | TimeFilter, *, column: str) -> TablePredicate | None:
    if not _safe(column):
        return None
    if isinstance(filt, TimeFilter):
        if filt.range is None:
            return None  # presets are resolved upstream into a TimeRange
        return TablePredicate(
            sql=f"{column} >= ? AND {column} <= ?",
            params=[filt.range.from_.isoformat(), filt.range.to.isoformat()],
        )
    if isinstance(filt, CategoricalFilter):
        if not filt.values:
            return None
        placeholders = ", ".join("?" for _ in filt.values)
        op = "IN" if filt.op == "in" else "NOT IN"
        return TablePredicate(
            sql=f"{column} {op} ({placeholders})",
            params=list(filt.values),
        )
    if isinstance(filt, NumericFilter):
        parts: list[str] = []
        params: list[Any] = []
        if filt.min is not None:
            parts.append(f"{column} >= ?")
            params.append(filt.min)
        if filt.max is not None:
            parts.append(f"{column} <= ?")
            params.append(filt.max)
        if not parts:
            return None
        return TablePredicate(sql=" AND ".join(parts), params=params)
    return None


def _semi_join_predicate(
    *,
    path: list[PathStep],
    target_column: str,
    inner_predicate: TablePredicate,
) -> TablePredicate | None:
    """Wrap *inner_predicate* in a chained ``EXISTS`` subquery along *path*."""
    if not path or not _safe(target_column):
        return None
    for step in path:
        if not (
            _safe(step.scan_table)
            and _safe(step.scan_column)
            and _safe(step.next_table)
            and _safe(step.next_column)
        ):
            return None

    # Build innermost-out:
    #   EXISTS (SELECT 1 FROM next_n WHERE next_n.col = scan_n.col AND <inner>)
    # then for each previous hop wrap another EXISTS.
    last = path[-1]
    inner_sql = (
        f"EXISTS (SELECT 1 FROM {last.next_table} "
        f"WHERE {last.next_table}.{last.next_column} = "
        f"{last.scan_table}.{last.scan_column} "
        f"AND ({inner_predicate.sql.replace(target_column, f'{last.next_table}.{target_column}')}))"
    )
    params: list[Any] = list(inner_predicate.params)

    for step in reversed(path[:-1]):
        inner_sql = (
            f"EXISTS (SELECT 1 FROM {step.next_table} "
            f"WHERE {step.next_table}.{step.next_column} = "
            f"{step.scan_table}.{step.scan_column} "
            f"AND {inner_sql})"
        )

    return TablePredicate(sql=inner_sql, params=params)


# ---------------------------------------------------------------------------
# Plan builder
# ---------------------------------------------------------------------------


def build_view_plans(
    spec: FilterSpec,
    *,
    scanning_tables: set[str],
    graph: RelationshipGraph | None = None,
) -> FilterApplication:
    """Translate *spec* into one :class:`ViewPlan` per scanning table.

    Filters whose target table is not scanned (and cannot be reached via the
    relationship *graph*) are recorded in :attr:`FilterApplication.skipped`.
    """
    plans_by_table: dict[str, ViewPlan] = {
        t: ViewPlan(table=t) for t in scanning_tables if _safe(t)
    }
    skipped: list[dict[str, str]] = []

    def _apply_one(filt: Filter | TimeFilter) -> None:
        target_table = filt.column_ref.table
        target_column = filt.column_ref.column
        direct = _direct_predicate(filt, column=target_column)
        if direct is None:
            skipped.append(
                {
                    "table": target_table,
                    "column": target_column,
                    "reason": "invalid_predicate",
                }
            )
            return

        if target_table in plans_by_table:
            plans_by_table[target_table].predicates.append(direct)
            return

        # Target table is not directly scanned \u2014 try to route via the graph.
        if graph is None:
            skipped.append(
                {
                    "table": target_table,
                    "column": target_column,
                    "reason": "no_relationship_graph",
                }
            )
            return
        path = find_filter_path(
            graph,
            scanning_tables=set(plans_by_table.keys()),
            target_table=target_table,
        )
        if not path:
            skipped.append(
                {
                    "table": target_table,
                    "column": target_column,
                    "reason": "no_safe_path",
                }
            )
            return
        wrapped = _semi_join_predicate(
            path=path,
            target_column=target_column,
            inner_predicate=direct,
        )
        if wrapped is None:
            skipped.append(
                {
                    "table": target_table,
                    "column": target_column,
                    "reason": "invalid_path",
                }
            )
            return
        plans_by_table[path[0].scan_table].predicates.append(wrapped)

    if spec.time is not None:
        if spec.time.range is None:
            skipped.append(
                {
                    "table": spec.time.column_ref.table,
                    "column": spec.time.column_ref.column,
                    "reason": "preset_unresolved",
                }
            )
        else:
            _apply_one(spec.time)
    for f in spec.filters:
        _apply_one(f)

    plans = [p for p in plans_by_table.values() if p.predicates]
    return FilterApplication(plans=plans, skipped=skipped)


# ---------------------------------------------------------------------------
# SQL rewriting
# ---------------------------------------------------------------------------


def rewrite_sql(
    sql: str,
    plans: Iterable[ViewPlan],
    *,
    existing_params: tuple | list | None = None,
) -> tuple[str, list[Any]]:
    """Wrap each base-table reference in *sql* matched by a plan.

    Returns ``(rewritten_sql, params)``. Params from filter predicates are
    appended **after** *existing_params* in left-to-right table-occurrence
    order, matching DuckDB's positional ``?`` binding.
    """
    plans_map = {p.table: p for p in plans if p.predicates}
    base_params: list[Any] = list(existing_params or ())

    if not plans_map:
        return sql, base_params

    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        logger.warning(
            "sqlglot could not parse widget SQL; skipping filter injection: %s",
            sql[:200],
        )
        return sql, base_params
    if tree is None:
        return sql, base_params

    cte_names = {
        cte.alias_or_name.lower()
        for cte in tree.find_all(exp.CTE)
        if cte.alias_or_name
    }

    appended_params: list[Any] = []

    for table_node in tree.find_all(exp.Table):
        name = table_node.name
        if not name or name.lower() in cte_names:
            continue
        plan = plans_map.get(name)
        if plan is None:
            continue
        body, params = plan.render_subquery_sql()
        # Preserve the original alias \u2014 if absent, alias the subquery to the
        # base table name so downstream column references keep resolving.
        alias = table_node.alias or name
        replacement = sqlglot.parse_one(
            f"(SELECT * FROM ({body}) AS _f) AS {alias}", read="duckdb"
        )
        # ``parse_one`` of a single FROM expression returns a Select; we want
        # the inner Subquery node.
        if isinstance(replacement, exp.Select):
            replacement = replacement.args["from"].expressions[0]
        table_node.replace(replacement)
        appended_params.extend(params)

    rewritten = tree.sql(dialect="duckdb")
    return rewritten, base_params + appended_params
