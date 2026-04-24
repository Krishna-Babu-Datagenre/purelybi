"""End-to-end test: hydrate_widget with a native FilterSpec.

Confirms that widget SQL passed through the existing template-style
hydration path is correctly rewritten by the filter engine.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import duckdb

from fastapi_app.models.filters import (
    CategoricalFilter,
    ColumnRef,
    FilterSpec,
    NumericFilter,
    TimeFilter,
    TimeRange,
)
from fastapi_app.services.widget_data_service import (
    _build_filter_clause,
    _hash_filter_spec,
    _PRESET_FILTER_CACHE,
    _resolve_allowed_filters,
    hydrate_widget,
)


def _seed_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE shopify_orders (
            created_at TIMESTAMP,
            payment_gateways VARCHAR,
            financial_status VARCHAR,
            total_price DOUBLE
        );
        INSERT INTO shopify_orders VALUES
            (TIMESTAMP '2026-01-02 10:00:00', 'stripe', 'paid', 100.0),
            (TIMESTAMP '2026-01-03 11:00:00', 'paypal', 'paid', 80.0),
            (TIMESTAMP '2026-01-08 12:00:00', 'stripe', 'pending', 40.0),
            (TIMESTAMP '2026-02-01 09:00:00', 'stripe', 'paid', 120.0);
        """
    )
    return conn


@patch(
    "fastapi_app.services.widget_data_service.get_tenant_sandbox",
    side_effect=lambda _tid: (_seed_conn(), frozenset({"shopify_orders"})),
)
class HydrateWidgetWithFilterSpecTests(unittest.TestCase):
    def _chart_widget(self) -> dict:
        return {
            "id": "w1",
            "type": "line",
            "chart_config": {
                "xAxis": {"type": "category", "data": []},
                "series": [{"type": "line", "data": []}],
            },
            "data_config": {
                "source": "shopify_orders",
                "query": (
                    "SELECT cast(created_at as date) AS day, COUNT(*) AS orders "
                    "FROM shopify_orders GROUP BY 1 ORDER BY 1"
                ),
                "mappings": {
                    "xAxis": "day",
                    "series": [{"field": "orders"}],
                },
            },
        }

    def test_categorical_filter_narrows_series(self, _mock):
        spec = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(
                        table="shopify_orders", column="payment_gateways"
                    ),
                    values=["stripe"],
                )
            ]
        )
        out = hydrate_widget(
            self._chart_widget(), tenant_id="t1", filter_spec=spec
        )
        # Only 3 stripe orders across 3 distinct days.
        days = out["chart_config"]["xAxis"]["data"]
        self.assertEqual(len(days), 3)
        totals = out["chart_config"]["series"][0]["data"]
        self.assertEqual(sum(totals), 3)

    def test_time_range_narrows_series(self, _mock):
        spec = FilterSpec(
            time=TimeFilter(
                column_ref=ColumnRef(
                    table="shopify_orders", column="created_at"
                ),
                range=TimeRange.model_validate(
                    {"from": "2026-01-01", "to": "2026-01-31"}
                ),
            )
        )
        out = hydrate_widget(
            self._chart_widget(), tenant_id="t1", filter_spec=spec
        )
        days = out["chart_config"]["xAxis"]["data"]
        self.assertEqual(len(days), 3)  # Jan 2, 3, 8

    def test_numeric_filter_on_kpi(self, _mock):
        kpi = {
            "id": "k1",
            "type": "kpi",
            "chart_config": {"value": 0},
            "data_config": {
                "source": "shopify_orders",
                "aggregation": "sum",
                "field": "total_price",
            },
        }
        spec = FilterSpec(
            filters=[
                NumericFilter(
                    column_ref=ColumnRef(
                        table="shopify_orders", column="total_price"
                    ),
                    min=80,
                )
            ]
        )
        out = hydrate_widget(kpi, tenant_id="t1", filter_spec=spec)
        # 100 + 80 + 120 = 300
        self.assertEqual(out["chart_config"]["value"], 300.0)

    def test_no_filter_spec_is_passthrough(self, _mock):
        out = hydrate_widget(self._chart_widget(), tenant_id="t1")
        days = out["chart_config"]["xAxis"]["data"]
        self.assertEqual(len(days), 4)  # all rows


class AllowedFilterResolutionTests(unittest.TestCase):

    def test_falls_back_to_legacy_when_no_metadata(self):
        with patch(
            "fastapi_app.services.widget_data_service.metadata_service."
            "get_filterable_columns_map",
            return_value={},
        ):
            cols = _resolve_allowed_filters("t1", "shopify_orders")
        # Legacy shim includes billing_country
        self.assertIn("billing_country", cols)

    def test_metadata_overrides_legacy(self):
        # Metadata says only `custom_col` is filterable; billing_country must
        # be rejected even though the legacy shim allows it.
        override = {"shopify_orders": frozenset({"custom_col"})}
        with patch(
            "fastapi_app.services.widget_data_service.metadata_service."
            "get_filterable_columns_map",
            return_value=override,
        ):
            cols = _resolve_allowed_filters("t1", "shopify_orders")
            self.assertEqual(cols, frozenset({"custom_col"}))

            clause, params = _build_filter_clause(
                [
                    {"column": "billing_country", "op": "eq", "value": "US"},
                    {"column": "custom_col", "op": "eq", "value": "x"},
                ],
                "shopify_orders",
                tenant_id="t1",
            )
        self.assertIn("custom_col", clause)
        self.assertNotIn("billing_country", clause)
        self.assertEqual(params, ["x"])

    def test_no_tenant_uses_legacy_shim(self):
        # Without tenant_id, no metadata call is made.
        with patch(
            "fastapi_app.services.widget_data_service.metadata_service."
            "get_filterable_columns_map",
        ) as mocked:
            cols = _resolve_allowed_filters(None, "shopify_orders")
            mocked.assert_not_called()
        self.assertIn("billing_country", cols)


class FilterSpecCacheKeyTests(unittest.TestCase):
    """Group D3: preset cache must partition entries by filter_spec hash."""

    def test_hash_distinguishes_specs(self):
        spec_a = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(
                        table="shopify_orders", column="payment_gateways"
                    ),
                    values=["stripe"],
                )
            ]
        )
        spec_b = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(
                        table="shopify_orders", column="payment_gateways"
                    ),
                    values=["paypal"],
                )
            ]
        )
        h_none = _hash_filter_spec(None)
        h_empty = _hash_filter_spec(FilterSpec())
        self.assertEqual(h_none, "none")
        self.assertEqual(h_empty, "none")
        self.assertNotEqual(_hash_filter_spec(spec_a), _hash_filter_spec(spec_b))
        # Stable: same spec hashes to the same value twice.
        self.assertEqual(
            _hash_filter_spec(spec_a), _hash_filter_spec(spec_a)
        )

    @patch(
        "fastapi_app.services.widget_data_service.get_tenant_sandbox",
        side_effect=lambda _tid: (_seed_conn(), frozenset({"shopify_orders"})),
    )
    def test_preset_cache_partitions_by_filter_spec(self, _mock):
        _PRESET_FILTER_CACHE.clear()

        w = {
            "id": "wcache",
            "type": "line",
            "chart_config": {
                "xAxis": {"type": "category", "data": []},
                "series": [{"type": "line", "data": []}],
            },
            "data_config": {
                "source": "shopify_orders",
                "query": (
                    "SELECT cast(created_at as date) AS day, COUNT(*) AS orders "
                    "FROM shopify_orders GROUP BY 1 ORDER BY 1"
                ),
                "mappings": {
                    "xAxis": "day",
                    "series": [{"field": "orders"}],
                },
            },
        }
        # A preset filter must be present to exercise the cache path.
        date_filters = [
            {
                "column": "created_at",
                "op": "between",
                "value": ["2026-01-01", "2026-03-01"],
            }
        ]
        spec_stripe = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(
                        table="shopify_orders", column="payment_gateways"
                    ),
                    values=["stripe"],
                )
            ]
        )
        spec_paypal = FilterSpec(
            filters=[
                CategoricalFilter(
                    column_ref=ColumnRef(
                        table="shopify_orders", column="payment_gateways"
                    ),
                    values=["paypal"],
                )
            ]
        )

        hydrate_widget(
            dict(w),
            tenant_id="t1",
            filters=date_filters,
            filters_from_preset="last_30_days",
            filter_spec=spec_stripe,
        )
        hydrate_widget(
            dict(w),
            tenant_id="t1",
            filters=date_filters,
            filters_from_preset="last_30_days",
            filter_spec=spec_paypal,
        )
        hydrate_widget(
            dict(w),
            tenant_id="t1",
            filters=date_filters,
            filters_from_preset="last_30_days",
            filter_spec=None,
        )
        # 3 distinct filter_spec hashes → 3 distinct cache entries.
        keys_for_widget = [k for k in _PRESET_FILTER_CACHE if k[0] == "wcache"]
        hashes = {k[2] for k in keys_for_widget}
        self.assertEqual(len(hashes), 3)
        self.assertIn("none", hashes)


if __name__ == "__main__":
    unittest.main()
