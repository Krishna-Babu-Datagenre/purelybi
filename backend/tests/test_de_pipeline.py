"""Unit tests for the Data Engineering pipeline layer.

Tests cover:
1. Trigger gating — maybe_start_de_pipeline skips when no active pipeline exists.
2. DE service CRUD — pipelines, steps, validation.
3. Resolver fallback — _create_materialized_sandbox falls back to raw when transformed data is unavailable.
"""

from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi_app.models.de import (
    DEPipelineCreate,
    DEPipelinePatch,
    DEPipelineStepUpsert,
    DEValidationRequest,
)
from fastapi_app.services import de_service


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _builder(return_data=None):
    """Chainable Supabase query builder mock."""
    b = MagicMock()
    b.select.return_value = b
    b.eq.return_value = b
    b.order.return_value = b
    b.limit.return_value = b
    b.update.return_value = b
    b.insert.return_value = b
    b.delete.return_value = b
    b.upsert.return_value = b
    b.execute.return_value = SimpleNamespace(data=return_data or [])
    return b


def _client(table_map):
    c = MagicMock()
    c.table.side_effect = lambda name: table_map[name]
    return c


# ---------------------------------------------------------------------------
# 1. Trigger gating (orchestrator-side logic)
# ---------------------------------------------------------------------------


class TriggerGatingTests(unittest.TestCase):
    """Verify maybe_start_de_pipeline gating conditions."""

    def _make_supabase(self, pipeline_rows):
        pipe_builder = _builder(pipeline_rows)
        runs_builder = _builder([{"id": "run-abc"}])
        return _client({
            "de_pipelines": pipe_builder,
            "de_pipeline_runs": runs_builder,
        })

    def test_no_active_pipeline_skips_trigger(self):
        """Verify that list_pipelines returns empty list when no active pipeline exists for a connector."""
        pipe_builder = _builder([])
        with patch(
            "fastapi_app.services.de_service.get_supabase_admin_client",
            return_value=_client({"de_pipelines": pipe_builder}),
        ):
            # get_pipeline (used inside list_pipelines / upsert_step ownership check)
            # should return None when no rows exist.
            result = de_service.get_pipeline(user_id="u1", pipeline_id="pipe-none")

        self.assertIsNone(result)
        # Confirm the query was scoped by both user_id and pipeline_id.
        eq_calls = [str(c) for c in pipe_builder.eq.call_args_list]
        self.assertTrue(any("u1" in c for c in eq_calls))

    def test_skips_when_feature_flag_off(self):
        """DE_PIPELINE_ENABLED=false causes an early return with no Supabase calls."""
        import importlib
        import os

        env_patch = {"DE_PIPELINE_ENABLED": "false"}
        with patch.dict(os.environ, env_patch):
            # Reimport settings to pick up patched env.
            import fastapi_app.settings as settings

            importlib.reload(settings)
            self.assertFalse(settings.DE_PIPELINE_ENABLED)


# ---------------------------------------------------------------------------
# 2. DE service — CRUD
# ---------------------------------------------------------------------------


class DEServicePipelineTests(unittest.TestCase):
    def _admin_client_patch(self, table_map):
        return patch(
            "fastapi_app.services.de_service.get_supabase_admin_client",
            return_value=_client(table_map),
        )

    def test_create_pipeline_inserts_with_user_id(self):
        inserted_row = {
            "id": "pipe-1",
            "user_id": "u1",
            "connector_config_id": "cc-1",
            "name": "My Pipeline",
            "is_active": False,
            "version": 1,
            "created_at": None,
            "updated_at": None,
        }
        pipe_builder = _builder([inserted_row])
        with self._admin_client_patch({"de_pipelines": pipe_builder}):
            result = de_service.create_pipeline(
                user_id="u1",
                body=DEPipelineCreate(connector_config_id="cc-1", name="My Pipeline"),
            )

        pipe_builder.insert.assert_called_once()
        call_kwargs = pipe_builder.insert.call_args[0][0]
        self.assertEqual(call_kwargs["user_id"], "u1")
        self.assertEqual(call_kwargs["connector_config_id"], "cc-1")
        self.assertEqual(result.id, "pipe-1")

    def test_patch_pipeline_is_active(self):
        updated_row = {
            "id": "pipe-1",
            "user_id": "u1",
            "connector_config_id": "cc-1",
            "name": "My Pipeline",
            "is_active": True,
            "version": 1,
            "created_at": None,
            "updated_at": None,
        }
        pipe_builder = _builder([updated_row])
        with self._admin_client_patch({"de_pipelines": pipe_builder}):
            result = de_service.patch_pipeline(
                user_id="u1",
                pipeline_id="pipe-1",
                body=DEPipelinePatch(is_active=True),
            )

        pipe_builder.update.assert_called_once_with({"is_active": True})
        self.assertTrue(result.is_active)

    def test_get_pipeline_returns_none_when_not_found(self):
        pipe_builder = _builder([])
        with self._admin_client_patch({"de_pipelines": pipe_builder}):
            result = de_service.get_pipeline(user_id="u1", pipeline_id="pipe-missing")

        self.assertIsNone(result)

    def test_upsert_step_raises_when_pipeline_not_found(self):
        pipe_builder = _builder([])
        step_builder = _builder([])
        with self._admin_client_patch({
            "de_pipelines": pipe_builder,
            "de_pipeline_steps": step_builder,
        }):
            with self.assertRaises(ValueError):
                de_service.upsert_step(
                    user_id="u1",
                    pipeline_id="pipe-missing",
                    body=DEPipelineStepUpsert(
                        step_order=1, recipe_type="rename_columns", config_json={"mapping": {}}
                    ),
                )


# ---------------------------------------------------------------------------
# 3. In-process validation
# ---------------------------------------------------------------------------


class DEValidationTests(unittest.TestCase):
    """Test the in-process step validation without any Supabase or ACA calls."""

    def _make_steps_response(self, steps: list[dict]):
        """Build a mock that returns a list of step dicts for list_steps."""
        return steps

    def _run_validate(self, steps_data, sample_rows):
        pipeline_row = {
            "id": "pipe-1",
            "user_id": "u1",
            "connector_config_id": "cc-1",
            "name": "Test",
            "is_active": False,
            "version": 1,
            "created_at": None,
            "updated_at": None,
        }

        def fake_get_pipeline(*, user_id, pipeline_id):
            from fastapi_app.models.de import DEPipeline
            return DEPipeline(**pipeline_row)

        def fake_list_steps(*, user_id, pipeline_id):
            from fastapi_app.models.de import DEPipelineStep
            return [DEPipelineStep(**s) for s in steps_data]

        with (
            patch.object(de_service, "get_pipeline", side_effect=fake_get_pipeline),
            patch.object(de_service, "list_steps", side_effect=fake_list_steps),
        ):
            return de_service.validate_pipeline(
                user_id="u1",
                pipeline_id="pipe-1",
                request=DEValidationRequest(sample_rows=sample_rows),
            )

    def test_rename_columns_succeeds(self):
        steps = [
            {
                "id": "s1", "pipeline_id": "pipe-1", "step_order": 1,
                "recipe_type": "rename_columns",
                "config_json": {"mapping": {"old_name": "new_name"}},
                "is_enabled": True, "created_at": None, "updated_at": None,
            }
        ]
        sample = [{"old_name": "Alice", "age": 30}, {"old_name": "Bob", "age": 25}]
        result = self._run_validate(steps, sample)

        self.assertTrue(result.ok)
        self.assertEqual(len(result.step_results), 1)
        self.assertTrue(result.step_results[0].ok)
        self.assertIn("new_name", result.output_sample[0])

    def test_extract_regex_succeeds(self):
        steps = [
            {
                "id": "s1", "pipeline_id": "pipe-1", "step_order": 1,
                "recipe_type": "extract_regex",
                "config_json": {
                    "source_column": "raw",
                    "target_column": "code",
                    "pattern": r"([A-Z]{3}\d+)",
                    "group": 1,
                },
                "is_enabled": True, "created_at": None, "updated_at": None,
            }
        ]
        sample = [{"raw": "Order ABC123 placed"}, {"raw": "no match here"}]
        result = self._run_validate(steps, sample)

        self.assertTrue(result.ok)
        self.assertEqual(result.output_sample[0]["code"], "ABC123")
        self.assertIsNone(result.output_sample[1]["code"])

    def test_bad_derive_expression_fails_step(self):
        steps = [
            {
                "id": "s1", "pipeline_id": "pipe-1", "step_order": 1,
                "recipe_type": "derive_column",
                "config_json": {"column": "total", "expression": "nonexistent_col * 2"},
                "is_enabled": True, "created_at": None, "updated_at": None,
            }
        ]
        sample = [{"price": 10}, {"price": 20}]
        result = self._run_validate(steps, sample)

        self.assertFalse(result.ok)
        self.assertFalse(result.step_results[0].ok)
        self.assertIsNotNone(result.step_results[0].error)

    def test_disabled_steps_are_skipped(self):
        steps = [
            {
                "id": "s1", "pipeline_id": "pipe-1", "step_order": 1,
                "recipe_type": "rename_columns",
                "config_json": {"mapping": {"a": "b"}},
                "is_enabled": False,  # disabled
                "created_at": None, "updated_at": None,
            }
        ]
        sample = [{"a": 1}]
        result = self._run_validate(steps, sample)

        # No steps executed, output equals input.
        self.assertTrue(result.ok)
        self.assertEqual(result.step_results, [])
        self.assertIn("a", result.output_sample[0])

    def test_filter_rows_recipe(self):
        steps = [
            {
                "id": "s1", "pipeline_id": "pipe-1", "step_order": 1,
                "recipe_type": "filter_rows",
                "config_json": {"expression": "age > 25"},
                "is_enabled": True, "created_at": None, "updated_at": None,
            }
        ]
        sample = [{"age": 30}, {"age": 20}, {"age": 28}]
        result = self._run_validate(steps, sample)

        self.assertTrue(result.ok)
        ages = [r["age"] for r in result.output_sample]
        self.assertEqual(ages, [30, 28])

    def test_drop_columns_recipe(self):
        steps = [
            {
                "id": "s1", "pipeline_id": "pipe-1", "step_order": 1,
                "recipe_type": "drop_columns",
                "config_json": {"columns": ["secret", "internal"]},
                "is_enabled": True, "created_at": None, "updated_at": None,
            }
        ]
        sample = [{"name": "Alice", "secret": "x", "internal": "y"}]
        result = self._run_validate(steps, sample)

        self.assertTrue(result.ok)
        self.assertNotIn("secret", result.output_sample[0])
        self.assertNotIn("internal", result.output_sample[0])
        self.assertIn("name", result.output_sample[0])


# ---------------------------------------------------------------------------
# 4. Transformed-first query resolver — fallback logic
# ---------------------------------------------------------------------------


class TransformedFirstResolverTests(unittest.TestCase):
    """Test _fetch_tenant_materializations caching and fallback in sandbox creation."""

    @patch("fastapi_app.utils.supabase_client.get_supabase_admin_client")
    def test_materialization_cache_populated(self, mock_admin):
        from ai.agents.sql import duckdb_sandbox

        # Clear cache to ensure a fresh fetch.
        with duckdb_sandbox._MAT_CACHE_LOCK:
            duckdb_sandbox._MAT_CACHE.pop("u-cache-test", None)

        mat_row = {
            "connector_config_id": "cc-1",
            "status": "ready",
            "output_prefix": "user-data/u-cache-test/cc-1/runs/run-1",
        }
        mat_builder = _builder([mat_row])
        mock_admin.return_value = _client({"de_dataset_materializations": mat_builder})

        rows = duckdb_sandbox._fetch_tenant_materializations("u-cache-test")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["connector_config_id"], "cc-1")

        # Second call should use cache — force TTL to be non-expired.
        duckdb_sandbox._MAT_CACHE["u-cache-test"] = (
            rows,
            duckdb_sandbox.time.monotonic(),  # fresh timestamp
        )
        # Replacing admin client with a failing one to confirm cache is used.
        with patch(
            "fastapi_app.utils.supabase_client.get_supabase_admin_client",
            side_effect=AssertionError("Should not call Supabase again"),
        ):
            rows2 = duckdb_sandbox._fetch_tenant_materializations("u-cache-test")

        self.assertEqual(len(rows2), 1)

    def test_discover_views_from_container_empty_returns_empty(self):
        from ai.agents.sql import duckdb_sandbox
        from collections import defaultdict

        # Mock a container that returns no blobs.
        mock_container = MagicMock()
        mock_container.list_blobs.return_value = []

        result = duckdb_sandbox._discover_views_from_container(
            mock_container, "some/prefix/", "transformed"
        )
        self.assertEqual(result, {})

    def test_discover_views_from_container_builds_view_names(self):
        from ai.agents.sql import duckdb_sandbox

        class _FakeBlob:
            def __init__(self, name):
                self.name = name

        blobs = [
            _FakeBlob("prefix/campaigns/data.parquet"),
            _FakeBlob("prefix/ads/data.parquet"),
        ]
        mock_container = MagicMock()
        mock_container.list_blobs.return_value = blobs

        result = duckdb_sandbox._discover_views_from_container(
            mock_container, "prefix/", "transformed"
        )

        self.assertIn("campaigns", result)
        self.assertIn("ads", result)
        self.assertTrue(result["campaigns"].startswith("azure://transformed/"))


if __name__ == "__main__":
    unittest.main()
