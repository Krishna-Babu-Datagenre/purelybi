"""Regression tests for template widget hydration (DuckDB in-memory seed)."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SQL_AGENT = _REPO / "sql-agent"
if str(_SQL_AGENT) not in sys.path:
    sys.path.insert(0, str(_SQL_AGENT))

import unittest
from unittest.mock import patch

import duckdb

from fastapi_app.services.widget_data_service import (
    _DATE_COLUMNS,
    hydrate_widget,
    resolve_date_preset,
)


def _seed_analytics_conn() -> duckdb.DuckDBPyConnection:
    """Minimal DuckDB schema + rows matching legacy SQLite dashboard tests."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE shopify_orders (
            created_at TIMESTAMP,
            payment_gateways VARCHAR,
            financial_status VARCHAR,
            order_margin_base DOUBLE,
            total_price DOUBLE
        );
        INSERT INTO shopify_orders VALUES
            (TIMESTAMP '2025-01-02 10:00:00', 'stripe', 'paid', 50.0, 100.0),
            (TIMESTAMP '2025-01-03 11:00:00', 'paypal', 'paid', 30.0, 80.0),
            (TIMESTAMP '2025-01-08 12:00:00', 'stripe', 'pending', 0.0, 40.0);
        """
    )
    conn.execute(
        """
        CREATE TABLE meta_daily_insights (
            date DATE,
            spend DOUBLE,
            roas DOUBLE
        );
        INSERT INTO meta_daily_insights VALUES
            (DATE '2024-06-01', 100.0, 1.0),
            (DATE '2025-01-02', 20.0, 2.0),
            (DATE '2025-01-03', 15.0, 2.5),
            (DATE '2025-01-08', 10.0, 3.0);
        """
    )
    return conn


def _date_filters() -> list[dict]:
    start, end = resolve_date_preset("last_7_days", tenant_id="test-tenant")
    return [
        {"column": col, "op": "between", "value": [start, end]}
        for col in set(_DATE_COLUMNS.values())
    ]


@patch(
    "fastapi_app.services.widget_data_service.create_tenant_sandbox",
    side_effect=lambda _tid: _seed_analytics_conn(),
)
class TemplateWidgetHydrationTests(unittest.TestCase):
    def test_payment_method_pie_has_data(self, _mock: object) -> None:
        w = {
            "id": "t1",
            "type": "pie",
            "chart_config": {"series": [{"type": "pie", "data": []}]},
            "data_config": {
                "source": "shopify_orders",
                "query": "SELECT payment_gateways AS gateway, COUNT(*) AS orders FROM shopify_orders GROUP BY payment_gateways ORDER BY orders DESC",
                "mappings": {
                    "series": [{"nameField": "gateway", "valueField": "orders"}]
                },
            },
        }
        out = hydrate_widget(
            dict(w), tenant_id="test-tenant", filters=None, persist_cache=False
        )
        data = out["chart_config"]["series"][0].get("data") or []
        self.assertTrue(len(data) >= 1)
        self.assertTrue(all("name" in d and "value" in d for d in data))

    def test_daily_order_count_line_hydrates_axis_and_series(self, _mock: object) -> None:
        w = {
            "id": "doc1",
            "type": "line",
            "chart_config": {
                "xAxis": {"type": "category", "name": "Date", "data": []},
                "yAxis": {"type": "value", "name": "Orders"},
                "series": [{"name": "Orders", "type": "line", "data": []}],
            },
            "data_config": {
                "source": "shopify_orders",
                "query": (
                    "SELECT DATE(created_at) AS dt, COUNT(*) AS order_count "
                    "FROM shopify_orders GROUP BY dt ORDER BY dt"
                ),
                "mappings": {
                    "xAxis": "dt",
                    "series": [{"field": "order_count", "name": "Orders"}],
                },
            },
        }
        out = hydrate_widget(
            dict(w), tenant_id="test-tenant", filters=None, persist_cache=False
        )
        xs = out["chart_config"]["xAxis"]["data"]
        ys = out["chart_config"]["series"][0].get("data") or []
        self.assertTrue(len(xs) >= 1)
        self.assertEqual(len(xs), len(ys))

    def test_net_profit_components_respects_filters(self, _mock: object) -> None:
        dc = {
            "components": [
                {
                    "op": "set",
                    "source": "shopify_orders",
                    "aggregation": "custom",
                    "formula": "SUM(CASE WHEN financial_status = 'paid' THEN order_margin_base ELSE 0 END)",
                },
                {
                    "op": "subtract",
                    "source": "meta_daily_insights",
                    "aggregation": "sum",
                    "field": "spend",
                },
            ]
        }
        w0 = {
            "id": "k1",
            "type": "kpi",
            "chart_config": {"value": 0},
            "data_config": dc,
        }
        f = _date_filters()
        a = hydrate_widget(
            dict(w0), tenant_id="test-tenant", filters=f, persist_cache=False
        )["chart_config"]["value"]
        b = hydrate_widget(
            dict(w0), tenant_id="test-tenant", filters=None, persist_cache=False
        )["chart_config"]["value"]
        self.assertIsInstance(a, (int, float))
        self.assertIsInstance(b, (int, float))

    def test_paid_revenue_vs_spend_line_has_points_with_filter(self, _mock: object) -> None:
        w = {
            "id": "c1",
            "type": "line",
            "chart_config": {
                "xAxis": {"type": "category", "data": []},
                "series": [
                    {"name": "Paid revenue (Shopify)", "data": []},
                    {"name": "Meta ad spend", "data": []},
                ],
            },
            "data_config": {
                "sources": ["shopify_orders", "meta_daily_insights"],
                "query": (
                    "WITH days AS ( SELECT DISTINCT date AS d FROM meta_daily_insights "
                    "UNION SELECT DISTINCT DATE(created_at) AS d FROM shopify_orders ) "
                    "SELECT days.d AS dt, COALESCE(s.revenue,0) AS shopify_revenue, COALESCE(d.spend,0) AS meta_spend "
                    "FROM days "
                    "LEFT JOIN meta_daily_insights d ON d.date = days.d "
                    "LEFT JOIN ( SELECT DATE(created_at) AS date, "
                    "SUM(CASE WHEN financial_status = 'paid' THEN total_price ELSE 0 END) AS revenue "
                    "FROM shopify_orders GROUP BY DATE(created_at) ) s ON s.date = days.d "
                    "ORDER BY dt"
                ),
                "mappings": {
                    "xAxis": "dt",
                    "series": [
                        {"field": "shopify_revenue", "name": "Paid revenue (Shopify)"},
                        {"field": "meta_spend", "name": "Meta ad spend"},
                    ],
                },
            },
        }
        out = hydrate_widget(
            dict(w),
            tenant_id="test-tenant",
            filters=_date_filters(),
            persist_cache=False,
        )
        xs = out["chart_config"]["xAxis"]["data"]
        self.assertTrue(len(xs) >= 1)

    def test_daily_roas_line_has_points_with_filter(self, _mock: object) -> None:
        w = {
            "id": "c2",
            "type": "line",
            "chart_config": {
                "xAxis": {"type": "category", "data": []},
                "series": [{"name": "ROAS", "data": []}],
            },
            "data_config": {
                "sources": ["shopify_orders", "meta_daily_insights"],
                "query": (
                    "WITH days AS ( SELECT DISTINCT date AS d FROM meta_daily_insights "
                    "UNION SELECT DISTINCT DATE(created_at) AS d FROM shopify_orders ) "
                    "SELECT days.d AS dt, COALESCE(m.roas, 0) AS roas FROM days "
                    "LEFT JOIN meta_daily_insights m ON m.date = days.d ORDER BY dt"
                ),
                "mappings": {"xAxis": "dt", "series": [{"field": "roas", "name": "ROAS"}]},
            },
        }
        out = hydrate_widget(
            dict(w),
            tenant_id="test-tenant",
            filters=_date_filters(),
            persist_cache=False,
        )
        xs = out["chart_config"]["xAxis"]["data"]
        self.assertTrue(len(xs) >= 1)

    def test_total_ad_spend_kpi_meta_daily(self, _mock: object) -> None:
        w = {
            "id": "k2",
            "type": "kpi",
            "chart_config": {"value": 0},
            "data_config": {
                "source": "meta_daily_insights",
                "aggregation": "sum",
                "field": "spend",
            },
        }
        a = hydrate_widget(
            dict(w), tenant_id="test-tenant", filters=_date_filters(), persist_cache=False
        )["chart_config"]["value"]
        b = hydrate_widget(
            dict(w), tenant_id="test-tenant", filters=None, persist_cache=False
        )["chart_config"]["value"]
        self.assertNotEqual(a, b, "filtered and unfiltered spend should usually differ")


if __name__ == "__main__":
    unittest.main()
