"""Unit tests for metadata_service CRUD using a mocked Supabase admin client.

These tests verify request shape (table, filters, payload) — not Supabase
network behaviour. RLS is enforced server-side by Supabase; we assert that
all reads/writes are scoped by ``user_id``.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi_app.models.metadata import (
    ColumnMetadataPatch,
    MetadataJobStatus,
    RelationshipCreate,
    RelationshipKind,
    SemanticType,
    TableMetadataPatch,
)
from fastapi_app.services import metadata_service


def _builder(return_data):
    """Return a chainable mock that mirrors the supabase-py builder API."""
    b = MagicMock()
    b.select.return_value = b
    b.eq.return_value = b
    b.order.return_value = b
    b.limit.return_value = b
    b.update.return_value = b
    b.insert.return_value = b
    b.delete.return_value = b
    b.upsert.return_value = b
    b.in_.return_value = b
    b.execute.return_value = SimpleNamespace(data=return_data)
    return b


def _client(table_name_to_builder):
    c = MagicMock()
    c.table.side_effect = lambda name: table_name_to_builder[name]
    return c


class TableMetadataTests(unittest.TestCase):
    def test_list_table_metadata_scopes_by_user(self):
        rows = [
            {"user_id": "u1", "table_name": "shopify_orders", "edited_by_user": False},
        ]
        b = _builder(rows)
        client = _client({"tenant_table_metadata": b})

        with patch.object(metadata_service, "get_supabase_admin_client", return_value=client):
            result = metadata_service.list_table_metadata(user_id="u1")

        b.eq.assert_any_call("user_id", "u1")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].table_name, "shopify_orders")

    def test_patch_table_metadata_marks_edited(self):
        b = _builder([
            {
                "user_id": "u1",
                "table_name": "shopify_orders",
                "description": "Orders fact table",
                "edited_by_user": True,
            }
        ])
        client = _client({"tenant_table_metadata": b})

        with patch.object(metadata_service, "get_supabase_admin_client", return_value=client):
            row = metadata_service.patch_table_metadata(
                user_id="u1",
                table_name="shopify_orders",
                patch=TableMetadataPatch(description="Orders fact table"),
            )

        sent_payload = b.update.call_args[0][0]
        self.assertEqual(sent_payload["description"], "Orders fact table")
        self.assertTrue(sent_payload["edited_by_user"])
        self.assertIsNotNone(row)
        self.assertTrue(row.edited_by_user)


class ColumnMetadataTests(unittest.TestCase):
    def test_patch_column_serialises_enum(self):
        b = _builder([
            {
                "user_id": "u1",
                "table_name": "shopify_orders",
                "column_name": "billing_country",
                "data_type": "VARCHAR",
                "semantic_type": "categorical",
                "edited_by_user": True,
            }
        ])
        client = _client({"tenant_column_metadata": b})

        with patch.object(metadata_service, "get_supabase_admin_client", return_value=client):
            row = metadata_service.patch_column_metadata(
                user_id="u1",
                table_name="shopify_orders",
                column_name="billing_country",
                patch=ColumnMetadataPatch(semantic_type=SemanticType.categorical),
            )

        payload = b.update.call_args[0][0]
        # Must be serialised to its string value, not an Enum instance.
        self.assertEqual(payload["semantic_type"], "categorical")
        self.assertTrue(payload["edited_by_user"])
        self.assertEqual(row.semantic_type, SemanticType.categorical)


class RelationshipTests(unittest.TestCase):
    def test_create_relationship_upserts_with_user_scope(self):
        returned = {
            "user_id": "u1",
            "from_table": "shopify_orders",
            "from_column": "customer_id",
            "to_table": "shopify_customers",
            "to_column": "id",
            "kind": "many_to_one",
            "confidence": 0.95,
            "edited_by_user": True,
        }
        b = _builder([returned])
        client = _client({"tenant_table_relationships": b})

        body = RelationshipCreate(
            from_table="shopify_orders",
            from_column="customer_id",
            to_table="shopify_customers",
            to_column="id",
            kind=RelationshipKind.many_to_one,
            confidence=0.95,
        )

        with patch.object(metadata_service, "get_supabase_admin_client", return_value=client):
            row = metadata_service.create_relationship(user_id="u1", body=body)

        upsert_payload = b.upsert.call_args[0][0]
        self.assertEqual(upsert_payload["user_id"], "u1")
        self.assertEqual(upsert_payload["kind"], "many_to_one")
        self.assertTrue(upsert_payload["edited_by_user"])
        self.assertEqual(row.kind, RelationshipKind.many_to_one)


class JobLifecycleTests(unittest.TestCase):
    def test_create_job_inserts_pending_row(self):
        b = _builder([
            {
                "id": "job-1",
                "user_id": "u1",
                "status": "pending",
                "progress": 0,
            }
        ])
        client = _client({"tenant_metadata_jobs": b})

        with patch.object(metadata_service, "get_supabase_admin_client", return_value=client):
            job = metadata_service.create_job(user_id="u1")

        payload = b.insert.call_args[0][0]
        self.assertEqual(payload["user_id"], "u1")
        self.assertEqual(payload["status"], MetadataJobStatus.pending.value)
        self.assertEqual(job.status, MetadataJobStatus.pending)


class DerivedMapsTests(unittest.TestCase):
    """Tests for the filterable-columns / date-columns resolvers (Group C6)."""

    def setUp(self):
        # Ensure no stale cache carries over between tests.
        metadata_service.invalidate_tenant_derived_cache("u1")

    def test_filterable_columns_filters_out_identifiers_and_unknown(self):
        col_rows = [
            {
                "table_name": "shopify_orders",
                "column_name": "billing_country",
                "semantic_type": "categorical",
                "is_filterable": True,
            },
            {
                "table_name": "shopify_orders",
                "column_name": "total_price",
                "semantic_type": "measure",
                "is_filterable": True,
            },
            # identifier row should be excluded even though is_filterable=True
            {
                "table_name": "shopify_orders",
                "column_name": "id",
                "semantic_type": "identifier",
                "is_filterable": True,
            },
        ]
        b = _builder(col_rows)
        client = _client({"tenant_column_metadata": b})

        with patch.object(metadata_service, "get_supabase_admin_client", return_value=client):
            m = metadata_service.get_filterable_columns_map(
                user_id="u1", use_cache=False
            )

        b.eq.assert_any_call("user_id", "u1")
        b.eq.assert_any_call("is_filterable", True)
        self.assertEqual(
            m, {"shopify_orders": frozenset({"billing_country", "total_price"})}
        )

    def test_filterable_columns_cache_hit_avoids_client(self):
        # Prime cache
        b = _builder([
            {
                "table_name": "t",
                "column_name": "c",
                "semantic_type": "categorical",
                "is_filterable": True,
            }
        ])
        client = _client({"tenant_column_metadata": b})
        with patch.object(metadata_service, "get_supabase_admin_client", return_value=client):
            metadata_service.get_filterable_columns_map(user_id="u1")

        # Second call must not hit the client.
        with patch.object(metadata_service, "get_supabase_admin_client") as mocked:
            out = metadata_service.get_filterable_columns_map(user_id="u1")
            mocked.assert_not_called()
        self.assertEqual(out, {"t": frozenset({"c"})})

    def test_date_columns_prefers_primary_date_column(self):
        tables_builder = _builder([
            {"table_name": "shopify_orders", "primary_date_column": "created_at"},
            {"table_name": "meta_daily_insights", "primary_date_column": None},
        ])
        cols_builder = _builder([
            {
                "table_name": "meta_daily_insights",
                "column_name": "date",
                "semantic_type": "temporal",
            }
        ])
        client = _client(
            {
                "tenant_table_metadata": tables_builder,
                "tenant_column_metadata": cols_builder,
            }
        )

        with patch.object(metadata_service, "get_supabase_admin_client", return_value=client):
            m = metadata_service.get_date_columns_map(user_id="u1", use_cache=False)

        self.assertEqual(
            m,
            {"shopify_orders": "created_at", "meta_daily_insights": "date"},
        )

    def test_invalidate_clears_both_caches(self):
        _FILT = metadata_service._FILTERABLE_COLUMNS_CACHE
        _DATE = metadata_service._DATE_COLUMNS_CACHE
        _FILT["u1"] = ({}, 0.0)
        _DATE["u1"] = ({}, 0.0)
        metadata_service.invalidate_tenant_derived_cache("u1")
        self.assertNotIn("u1", _FILT)
        self.assertNotIn("u1", _DATE)


if __name__ == "__main__":
    unittest.main()
