"""Unit + integration tests for the dashboard filter engine.

Covers:
- FilterSpec model validation.
- DuckDB EXPLAIN-based table detection (CTE, subquery, join, UNION).
- Relationship-graph BFS direction safety.
- SQL rewriting (single source, JOIN, CTE).
- End-to-end apply_filters against a live in-memory DuckDB.
"""

from __future__ import annotations

import unittest

import duckdb
import pytest
from pydantic import ValidationError

from fastapi_app.models.filters import (
    CategoricalFilter,
    ColumnRef,
    FilterSpec,
    NumericFilter,
    TimeFilter,
    TimeRange,
)
from fastapi_app.services.filter_engine import (
    apply_filters,
    build_view_plans,
    detect_referenced_tables,
)
from fastapi_app.services.filter_engine.relationships import (
    RelationshipGraph,
    find_filter_path,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _seed_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE customers (id BIGINT, country VARCHAR);
        INSERT INTO customers VALUES
            (1, 'US'), (2, 'CA'), (3, 'US'), (4, 'GB');

        CREATE TABLE orders (
            id BIGINT,
            customer_id BIGINT,
            total_price DOUBLE,
            created_at DATE
        );
        INSERT INTO orders VALUES
            (10, 1, 50, DATE '2026-01-01'),
            (11, 1, 30, DATE '2026-01-15'),
            (12, 2, 75, DATE '2026-02-01'),
            (13, 3, 12, DATE '2026-03-01'),
            (14, 4, 99, DATE '2026-03-15');
        """
    )
    return conn


_RELS = [
    {
        "from_table": "orders",
        "from_column": "customer_id",
        "to_table": "customers",
        "to_column": "id",
        "kind": "many_to_one",
    }
]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class FilterSpecModelTests(unittest.TestCase):
    def test_time_filter_requires_preset_or_range(self):
        with self.assertRaises(ValidationError):
            TimeFilter(column_ref=ColumnRef(table="orders", column="created_at"))

    def test_time_filter_rejects_both(self):
        with self.assertRaises(ValidationError):
            TimeFilter(
                column_ref=ColumnRef(table="orders", column="created_at"),
                preset="last_7_days",
                range=TimeRange.model_validate({"from": "2026-01-01", "to": "2026-02-01"}),
            )

    def test_time_range_requires_to_after_from(self):
        with self.assertRaises(ValidationError):
            TimeRange.model_validate({"from": "2026-02-01", "to": "2026-01-01"})

    def test_numeric_filter_requires_a_bound(self):
        with self.assertRaises(ValidationError):
            NumericFilter(column_ref=ColumnRef(table="orders", column="total_price"))

    def test_categorical_filter_rejects_empty_values(self):
        with self.assertRaises(ValidationError):
            CategoricalFilter(
                column_ref=ColumnRef(table="customers", column="country"),
                values=[],
            )

    def test_filter_spec_is_empty_when_default(self):
        self.assertTrue(FilterSpec().is_empty())


# ---------------------------------------------------------------------------
# Table detection
# ---------------------------------------------------------------------------


class TableDetectionTests(unittest.TestCase):
    def setUp(self):
        self.conn = _seed_conn()

    def test_simple_select(self):
        tables = detect_referenced_tables(
            "SELECT * FROM orders", conn=self.conn
        )
        self.assertEqual(tables, {"orders"})

    def test_join(self):
        tables = detect_referenced_tables(
            "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id",
            conn=self.conn,
        )
        self.assertEqual(tables, {"orders", "customers"})

    def test_cte_excludes_cte_name(self):
        sql = (
            "WITH recent AS (SELECT * FROM orders WHERE created_at >= DATE '2026-02-01') "
            "SELECT * FROM recent"
        )
        tables = detect_referenced_tables(sql, conn=self.conn)
        # CTE name must NOT be reported as a base table.
        self.assertNotIn("recent", tables)
        self.assertIn("orders", tables)

    def test_union(self):
        sql = "SELECT id FROM orders UNION ALL SELECT id FROM customers"
        tables = detect_referenced_tables(sql, conn=self.conn)
        self.assertEqual(tables, {"orders", "customers"})

    def test_subquery(self):
        sql = (
            "SELECT * FROM (SELECT customer_id FROM orders WHERE total_price > 20) AS hi"
        )
        tables = detect_referenced_tables(sql, conn=self.conn)
        self.assertEqual(tables, {"orders"})

    def test_sqlglot_fallback_when_no_conn(self):
        tables = detect_referenced_tables(
            "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        )
        self.assertEqual(tables, {"orders", "customers"})


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------


class RelationshipGraphTests(unittest.TestCase):
    def test_many_to_one_forward_only(self):
        graph = RelationshipGraph(_RELS)
        # orders -> customers is allowed (FK side -> PK side)
        path = find_filter_path(
            graph, scanning_tables={"orders"}, target_table="customers"
        )
        self.assertIsNotNone(path)
        self.assertEqual(len(path), 1)
        self.assertEqual(path[0].next_table, "customers")

    def test_many_to_one_reverse_blocked(self):
        graph = RelationshipGraph(_RELS)
        # customers -> orders would fan rows out; must not be reachable.
        path = find_filter_path(
            graph, scanning_tables={"customers"}, target_table="orders"
        )
        self.assertIsNone(path)

    def test_many_to_many_excluded(self):
        graph = RelationshipGraph(
            [
                {
                    "from_table": "a",
                    "from_column": "x",
                    "to_table": "b",
                    "to_column": "y",
                    "kind": "many_to_many",
                }
            ]
        )
        self.assertIsNone(
            find_filter_path(graph, scanning_tables={"a"}, target_table="b")
        )

    def test_one_to_one_traversable_both_ways(self):
        graph = RelationshipGraph(
            [
                {
                    "from_table": "a",
                    "from_column": "id",
                    "to_table": "b",
                    "to_column": "id",
                    "kind": "one_to_one",
                }
            ]
        )
        self.assertIsNotNone(
            find_filter_path(graph, scanning_tables={"a"}, target_table="b")
        )
        self.assertIsNotNone(
            find_filter_path(graph, scanning_tables={"b"}, target_table="a")
        )

    def test_target_in_scanning_returns_empty_path(self):
        graph = RelationshipGraph(_RELS)
        self.assertEqual(
            find_filter_path(
                graph, scanning_tables={"orders"}, target_table="orders"
            ),
            [],
        )


# ---------------------------------------------------------------------------
# Plan builder
# ---------------------------------------------------------------------------


class PlanBuilderTests(unittest.TestCase):
    def test_direct_filter_attaches_to_scanned_table(self):
        spec = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(table="orders", column="customer_id"),
                    values=[1, 2],
                ),
            ]
        )
        app = build_view_plans(spec, scanning_tables={"orders"})
        self.assertEqual(len(app.plans), 1)
        self.assertEqual(app.plans[0].table, "orders")
        self.assertEqual(app.skipped, [])

    def test_filter_on_unrelated_table_is_skipped(self):
        spec = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(table="customers", column="country"),
                    values=["US"],
                ),
            ]
        )
        app = build_view_plans(spec, scanning_tables={"orders"})
        # No graph supplied \u2192 cannot be routed.
        self.assertEqual(app.plans, [])
        self.assertEqual(len(app.skipped), 1)
        self.assertEqual(app.skipped[0]["reason"], "no_relationship_graph")

    def test_filter_routed_via_relationship(self):
        graph = RelationshipGraph(_RELS)
        spec = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(table="customers", column="country"),
                    values=["US"],
                ),
            ]
        )
        app = build_view_plans(spec, scanning_tables={"orders"}, graph=graph)
        self.assertEqual(len(app.plans), 1)
        self.assertEqual(app.plans[0].table, "orders")
        # Predicate should be wrapped in EXISTS.
        body, _params = app.plans[0].render_subquery_sql()
        self.assertIn("EXISTS", body)
        self.assertIn("customers", body)


# ---------------------------------------------------------------------------
# End-to-end against real DuckDB
# ---------------------------------------------------------------------------


class ApplyFiltersIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.conn = _seed_conn()

    def _run(self, sql: str, params: list) -> list[dict]:
        return self.conn.execute(sql, params).fetchdf().to_dict(orient="records")

    def test_no_filters_passthrough(self):
        sql, params, app = apply_filters(
            "SELECT COUNT(*) AS c FROM orders",
            spec=None,
            conn=self.conn,
        )
        self.assertEqual(sql, "SELECT COUNT(*) AS c FROM orders")
        self.assertEqual(params, [])
        self.assertIsNone(app)

    def test_direct_categorical_filter(self):
        spec = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(table="orders", column="customer_id"),
                    values=[1, 2],
                )
            ]
        )
        sql, params, app = apply_filters(
            "SELECT COUNT(*) AS c FROM orders",
            spec=spec,
            conn=self.conn,
        )
        rows = self._run(sql, params)
        self.assertEqual(rows[0]["c"], 3)  # orders 10, 11, 12

    def test_time_range_filter(self):
        spec = FilterSpec(
            time=TimeFilter(
                column_ref=ColumnRef(table="orders", column="created_at"),
                range=TimeRange.model_validate(
                    {"from": "2026-02-01", "to": "2026-03-31"}
                ),
            )
        )
        sql, params, _ = apply_filters(
            "SELECT COUNT(*) AS c FROM orders",
            spec=spec,
            conn=self.conn,
        )
        rows = self._run(sql, params)
        self.assertEqual(rows[0]["c"], 3)  # orders 12, 13, 14

    def test_filter_via_related_table(self):
        spec = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(table="customers", column="country"),
                    values=["US"],
                )
            ]
        )
        sql, params, app = apply_filters(
            "SELECT COUNT(*) AS c FROM orders",
            spec=spec,
            conn=self.conn,
            relationships=_RELS,
        )
        self.assertIsNotNone(app)
        self.assertEqual(len(app.plans), 1)
        rows = self._run(sql, params)
        # US customers are ids 1 and 3 -> orders 10, 11, 13
        self.assertEqual(rows[0]["c"], 3)

    def test_cte_widget_filtered(self):
        spec = FilterSpec(
            filters=[
                NumericFilter(
                    column_ref=ColumnRef(table="orders", column="total_price"),
                    min=20,
                )
            ]
        )
        sql, params, _ = apply_filters(
            "WITH t AS (SELECT * FROM orders) SELECT COUNT(*) AS c FROM t",
            spec=spec,
            conn=self.conn,
        )
        rows = self._run(sql, params)
        # total_price >= 20 -> orders 10, 11, 12, 14 (drops 13 with 12)
        self.assertEqual(rows[0]["c"], 4)

    def test_join_widget_filters_both_tables(self):
        spec = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(table="customers", column="country"),
                    values=["US", "CA"],
                ),
                NumericFilter(
                    column_ref=ColumnRef(table="orders", column="total_price"),
                    min=30,
                ),
            ]
        )
        sql, params, _ = apply_filters(
            "SELECT COUNT(*) AS c FROM orders o JOIN customers c ON o.customer_id = c.id",
            spec=spec,
            conn=self.conn,
        )
        rows = self._run(sql, params)
        # US/CA customers (1, 2, 3) AND total_price >= 30
        # \u2192 orders 10 (50), 11 (30), 12 (75) \u2192 3 rows
        self.assertEqual(rows[0]["c"], 3)

    def test_skipped_filter_is_reported(self):
        spec = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(table="unrelated", column="x"),
                    values=["a"],
                )
            ]
        )
        _sql, _params, app = apply_filters(
            "SELECT COUNT(*) FROM orders",
            spec=spec,
            conn=self.conn,
        )
        self.assertIsNotNone(app)
        self.assertEqual(len(app.skipped), 1)


if __name__ == "__main__":
    unittest.main()
