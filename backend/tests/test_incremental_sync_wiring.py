"""Tests for the incremental_enabled wiring across the onboarding pipeline.

Covers:
  - Catalog introspection (_catalog_has_incremental_streams)
  - Schedule form field generation (_sync_schedule_form_fields)
  - Router-level schedule parsing (_parse_sync_schedule)
  - Tool-level schedule resolution (_resolve_sync_schedule)
  - Pydantic model acceptance (UserConnectorConfigCreate / Update)
  - upsert_user_connector_onboarding passes incremental_enabled through
  - save_config reads incremental_enabled from the schedule form
  - run_sync preserves incremental_enabled from the existing row
"""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Helpers under test
# ---------------------------------------------------------------------------
from ai.tools.onboarding import (
    _catalog_has_incremental_streams,
    _sync_schedule_form_fields,
    _resolve_sync_schedule,
)
from fastapi_app.models.connectors import (
    UserConnectorConfigCreate,
    UserConnectorConfigUpdate,
)
from fastapi_app.routers.onboarding import _parse_sync_schedule


# ── Fixtures ──────────────────────────────────────────────────────────────


def _make_catalog(streams: list[dict]) -> dict:
    """Build a minimal Airbyte catalog dict."""
    return {"streams": streams}


def _fb_catalog() -> dict:
    """Realistic Facebook Marketing catalog snippet with mixed sync modes."""
    return _make_catalog(
        [
            {
                "name": "ads",
                "supported_sync_modes": ["full_refresh", "incremental"],
                "default_cursor_field": ["updated_time"],
            },
            {
                "name": "campaigns",
                "supported_sync_modes": ["full_refresh", "incremental"],
                "default_cursor_field": ["updated_time"],
            },
            {
                "name": "ad_account",
                "supported_sync_modes": ["full_refresh"],
            },
        ]
    )


def _full_refresh_only_catalog() -> dict:
    return _make_catalog(
        [
            {"name": "users", "supported_sync_modes": ["full_refresh"]},
            {"name": "events", "supported_sync_modes": ["full_refresh"]},
        ]
    )


# ── _catalog_has_incremental_streams ──────────────────────────────────────


class TestCatalogHasIncrementalStreams(unittest.TestCase):
    def test_returns_true_when_streams_support_incremental(self) -> None:
        self.assertTrue(_catalog_has_incremental_streams(_fb_catalog()))

    def test_returns_false_when_only_full_refresh(self) -> None:
        self.assertFalse(
            _catalog_has_incremental_streams(_full_refresh_only_catalog())
        )

    def test_returns_false_for_none_catalog(self) -> None:
        self.assertFalse(_catalog_has_incremental_streams(None))

    def test_returns_false_for_empty_catalog(self) -> None:
        self.assertFalse(_catalog_has_incremental_streams({}))
        self.assertFalse(_catalog_has_incremental_streams({"streams": []}))

    def test_filters_by_selected_streams(self) -> None:
        # Only selecting ad_account (full_refresh only) → False
        self.assertFalse(
            _catalog_has_incremental_streams(_fb_catalog(), ["ad_account"])
        )
        # Selecting ads (incremental) → True
        self.assertTrue(
            _catalog_has_incremental_streams(_fb_catalog(), ["ads"])
        )

    def test_selected_streams_not_in_catalog(self) -> None:
        self.assertFalse(
            _catalog_has_incremental_streams(_fb_catalog(), ["nonexistent"])
        )

    def test_mixed_selected_one_incremental(self) -> None:
        self.assertTrue(
            _catalog_has_incremental_streams(_fb_catalog(), ["ad_account", "campaigns"])
        )

    def test_handles_missing_supported_sync_modes(self) -> None:
        cat = _make_catalog([{"name": "x"}])
        self.assertFalse(_catalog_has_incremental_streams(cat))

    def test_handles_non_dict_stream_entries(self) -> None:
        cat = {"streams": [None, "bad", 42, {"name": "a", "supported_sync_modes": ["incremental"]}]}
        self.assertTrue(_catalog_has_incremental_streams(cat))


# ── _sync_schedule_form_fields ────────────────────────────────────────────


class TestSyncScheduleFormFields(unittest.TestCase):
    def test_base_fields_always_present(self) -> None:
        fields = _sync_schedule_form_fields()
        keys = [f["key"] for f in fields]
        self.assertIn("sync_mode", keys)
        self.assertIn("interval_value", keys)
        self.assertIn("interval_unit", keys)
        self.assertIn("start_date", keys)

    def test_no_incremental_toggle_by_default(self) -> None:
        fields = _sync_schedule_form_fields()
        keys = [f["key"] for f in fields]
        self.assertNotIn("incremental_enabled", keys)

    def test_no_incremental_toggle_when_false(self) -> None:
        fields = _sync_schedule_form_fields(has_incremental_streams=False)
        keys = [f["key"] for f in fields]
        self.assertNotIn("incremental_enabled", keys)

    def test_incremental_toggle_present_when_true(self) -> None:
        fields = _sync_schedule_form_fields(has_incremental_streams=True)
        keys = [f["key"] for f in fields]
        self.assertIn("incremental_enabled", keys)

    def test_incremental_field_shape(self) -> None:
        fields = _sync_schedule_form_fields(has_incremental_streams=True)
        inc = next(f for f in fields if f["key"] == "incremental_enabled")
        self.assertEqual(inc["type"], "boolean")
        self.assertTrue(inc["default"])
        self.assertIn("description", inc)
        self.assertIn("incremental", inc["description"].lower())


# ── _parse_sync_schedule (router) ─────────────────────────────────────────


class TestParseSyncSchedule(unittest.TestCase):
    def test_recurring_with_incremental_enabled(self) -> None:
        result = _parse_sync_schedule(
            {
                "sync_mode": "recurring",
                "interval_value": 6,
                "interval_unit": "hours",
                "start_date": "2025-01-01",
                "incremental_enabled": True,
            }
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["mode"], "recurring")
        self.assertEqual(result["frequency_minutes"], 360)
        self.assertEqual(result["start_date"], "2025-01-01")
        self.assertTrue(result["incremental_enabled"])

    def test_recurring_with_incremental_disabled(self) -> None:
        result = _parse_sync_schedule(
            {
                "sync_mode": "recurring",
                "interval_value": 1,
                "interval_unit": "days",
                "incremental_enabled": False,
            }
        )
        self.assertIsNotNone(result)
        self.assertFalse(result["incremental_enabled"])

    def test_recurring_without_incremental_field(self) -> None:
        result = _parse_sync_schedule(
            {
                "sync_mode": "recurring",
                "interval_value": 30,
                "interval_unit": "minutes",
            }
        )
        self.assertIsNotNone(result)
        self.assertFalse(result["incremental_enabled"])

    def test_one_off_always_false(self) -> None:
        result = _parse_sync_schedule(
            {"sync_mode": "one_off", "incremental_enabled": True}
        )
        self.assertIsNotNone(result)
        self.assertFalse(result["incremental_enabled"])

    def test_incremental_string_true(self) -> None:
        result = _parse_sync_schedule(
            {
                "sync_mode": "recurring",
                "interval_value": 6,
                "interval_unit": "hours",
                "incremental_enabled": "true",
            }
        )
        self.assertIsNotNone(result)
        self.assertTrue(result["incremental_enabled"])

    def test_incremental_string_false(self) -> None:
        result = _parse_sync_schedule(
            {
                "sync_mode": "recurring",
                "interval_value": 6,
                "interval_unit": "hours",
                "incremental_enabled": "false",
            }
        )
        self.assertIsNotNone(result)
        self.assertFalse(result["incremental_enabled"])


# ── _resolve_sync_schedule (tool internal) ────────────────────────────────


class TestResolveSyncSchedule(unittest.TestCase):
    """Ensure _resolve_sync_schedule still works (returns sync_mode, freq)."""

    def test_recurring_resolves(self) -> None:
        result = _resolve_sync_schedule(
            {"mode": "recurring", "frequency_minutes": 360}
        )
        self.assertEqual(result, ("recurring", 360))

    def test_one_off_resolves(self) -> None:
        result = _resolve_sync_schedule({"mode": "one_off"})
        self.assertEqual(result, ("one_off", 1))

    def test_missing_data_returns_none(self) -> None:
        self.assertIsNone(_resolve_sync_schedule(None))
        self.assertIsNone(_resolve_sync_schedule({}))
        self.assertIsNone(_resolve_sync_schedule("garbage"))


# ── Pydantic models ──────────────────────────────────────────────────────


class TestPydanticModels(unittest.TestCase):
    def test_create_model_accepts_incremental_enabled(self) -> None:
        body = UserConnectorConfigCreate(
            connector_name="FB Marketing",
            docker_repository="airbyte/source-facebook-marketing",
            docker_image="airbyte/source-facebook-marketing:5.2.6",
            incremental_enabled=True,
        )
        self.assertTrue(body.incremental_enabled)

    def test_create_model_defaults_incremental_to_false(self) -> None:
        body = UserConnectorConfigCreate(
            connector_name="FB Marketing",
            docker_repository="airbyte/source-facebook-marketing",
            docker_image="airbyte/source-facebook-marketing:5.2.6",
        )
        self.assertFalse(body.incremental_enabled)

    def test_update_model_accepts_incremental_enabled(self) -> None:
        patch = UserConnectorConfigUpdate(incremental_enabled=True)
        self.assertTrue(patch.incremental_enabled)
        dumped = patch.model_dump(exclude_none=True)
        self.assertIn("incremental_enabled", dumped)

    def test_update_model_omits_incremental_when_none(self) -> None:
        patch = UserConnectorConfigUpdate()
        dumped = patch.model_dump(exclude_none=True)
        self.assertNotIn("incremental_enabled", dumped)

    def test_create_model_serialises_incremental(self) -> None:
        body = UserConnectorConfigCreate(
            connector_name="x",
            docker_repository="repo",
            docker_image="img",
            incremental_enabled=True,
        )
        dumped = body.model_dump(exclude_none=True)
        self.assertTrue(dumped["incremental_enabled"])


# ── upsert_user_connector_onboarding ─────────────────────────────────────


class TestUpsertPassesIncremental(unittest.TestCase):
    """Mock Supabase to verify incremental_enabled reaches the DB layer."""

    def _mock_supabase(self) -> MagicMock:
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.insert.return_value.execute.return_value = MagicMock(
            data=[{"id": "new-id", "incremental_enabled": True}]
        )
        mock_table.update.return_value.eq.return_value.eq.return_value.execute.return_value = (
            MagicMock(data=[{"id": "existing-id", "incremental_enabled": True}])
        )
        mock_table.select.return_value.eq.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = (
            MagicMock(data=[])
        )
        return mock_client

    @patch("fastapi_app.services.connector_service.get_supabase_admin_client")
    def test_create_path_includes_incremental(self, mock_get_client: MagicMock) -> None:
        mock_client = self._mock_supabase()
        mock_get_client.return_value = mock_client

        from fastapi_app.services.connector_service import upsert_user_connector_onboarding

        upsert_user_connector_onboarding(
            "user-1",
            connector_name="test",
            docker_repository="test/repo",
            docker_image="test/repo:latest",
            config={"key": "val"},
            incremental_enabled=True,
        )

        # The insert call should include incremental_enabled
        insert_call = mock_client.table.return_value.insert
        self.assertTrue(insert_call.called)
        payload = insert_call.call_args[0][0]
        self.assertIn("incremental_enabled", payload)
        self.assertTrue(payload["incremental_enabled"])

    @patch("fastapi_app.services.connector_service.get_supabase_admin_client")
    @patch("fastapi_app.services.connector_service.get_active_user_connector_by_repository")
    def test_update_path_includes_incremental(
        self, mock_get_existing: MagicMock, mock_get_client: MagicMock
    ) -> None:
        mock_get_existing.return_value = {
            "id": "existing-id",
            "incremental_enabled": False,
        }
        mock_client = self._mock_supabase()
        mock_get_client.return_value = mock_client

        from fastapi_app.services.connector_service import upsert_user_connector_onboarding

        upsert_user_connector_onboarding(
            "user-1",
            connector_name="test",
            docker_repository="test/repo",
            docker_image="test/repo:latest",
            config={"key": "val"},
            incremental_enabled=True,
        )

        update_call = mock_client.table.return_value.update
        self.assertTrue(update_call.called)
        patch = update_call.call_args[0][0]
        self.assertIn("incremental_enabled", patch)
        self.assertTrue(patch["incremental_enabled"])


# ── save_config integration ───────────────────────────────────────────────


def _onboarding_ctx(user_id: str = "u1", thread_id: str = "t1") -> MagicMock:
    ctx = MagicMock()
    ctx.user_id = user_id
    ctx.thread_id = thread_id
    ctx.catalog = {
        "docker_repository": "airbyte/source-facebook-marketing",
        "docker_image_tag": "5.2.6",
    }
    return ctx


class TestSaveConfigIncremental(unittest.TestCase):
    """Verify save_config passes incremental_enabled from the schedule form to upsert."""

    @patch("ai.tools.onboarding.upsert_user_connector_onboarding")
    @patch("ai.tools.onboarding.stores")
    @patch("ai.tools.onboarding.get_onboarding_context")
    def test_save_config_passes_incremental_true(
        self,
        mock_ctx: MagicMock,
        mock_stores: MagicMock,
        mock_upsert: MagicMock,
    ) -> None:
        mock_ctx.return_value = _onboarding_ctx()
        mock_upsert.return_value = {"id": "row-1"}

        def _kv_side_effect(key: str, *a, **kw):
            return {
                "sync_schedule": {
                    "mode": "recurring",
                    "frequency_minutes": 360,
                    "start_date": None,
                    "incremental_enabled": True,
                },
                "oauth_meta": None,
                "discovered_catalog": _fb_catalog(),
            }.get(key)

        mock_stores.get_tool_kv.side_effect = _kv_side_effect

        from ai.tools.onboarding import save_config

        # Call the underlying function directly (LangChain's .invoke reserves
        # the 'config' keyword, which clashes with the tool's 'config' arg).
        result = json.loads(
            save_config.func(
                connector_name="FB Marketing",
                docker_image="airbyte/source-facebook-marketing:5.2.6",
                config={"access_token": "tok123"},
                selected_streams=["ads", "campaigns"],
            )
        )

        self.assertTrue(result["success"])
        mock_upsert.assert_called_once()
        _, kwargs = mock_upsert.call_args
        self.assertTrue(kwargs["incremental_enabled"])

    @patch("ai.tools.onboarding.upsert_user_connector_onboarding")
    @patch("ai.tools.onboarding.stores")
    @patch("ai.tools.onboarding.get_onboarding_context")
    def test_save_config_defaults_incremental_false_without_toggle(
        self,
        mock_ctx: MagicMock,
        mock_stores: MagicMock,
        mock_upsert: MagicMock,
    ) -> None:
        mock_ctx.return_value = _onboarding_ctx()
        mock_upsert.return_value = {"id": "row-1"}

        def _kv_side_effect(key: str, *a, **kw):
            return {
                "sync_schedule": {
                    "mode": "recurring",
                    "frequency_minutes": 360,
                    # no incremental_enabled key — toggle was not shown
                },
                "oauth_meta": None,
                "discovered_catalog": _full_refresh_only_catalog(),
            }.get(key)

        mock_stores.get_tool_kv.side_effect = _kv_side_effect

        from ai.tools.onboarding import save_config

        result = json.loads(
            save_config.func(
                connector_name="My Source",
                docker_image="airbyte/source-test:1.0",
                config={},
            )
        )

        self.assertTrue(result["success"])
        _, kwargs = mock_upsert.call_args
        self.assertFalse(kwargs["incremental_enabled"])

    @patch("ai.tools.onboarding.stores")
    @patch("ai.tools.onboarding.get_onboarding_context")
    def test_save_config_shows_incremental_toggle_when_catalog_has_incremental(
        self,
        mock_ctx: MagicMock,
        mock_stores: MagicMock,
    ) -> None:
        """When schedule is not yet submitted, the form should include the toggle."""
        mock_ctx.return_value = _onboarding_ctx()

        captured_ui: dict = {}

        def _kv_side_effect(key: str, *a, **kw):
            return {
                "sync_schedule": None,  # not submitted yet
                "oauth_meta": None,
                "discovered_catalog": _fb_catalog(),
            }.get(key)

        def _set_kv(key: str, value, *a, **kw):
            captured_ui[key] = value

        mock_stores.get_tool_kv.side_effect = _kv_side_effect
        mock_stores.set_tool_kv.side_effect = _set_kv

        from ai.tools.onboarding import save_config

        result = json.loads(
            save_config.func(
                connector_name="FB Marketing",
                docker_image="airbyte/source-facebook-marketing:5.2.6",
                config={},
            )
        )

        self.assertFalse(result["success"])
        self.assertTrue(result.get("needs_sync_schedule"))
        # The rendered form should include the incremental toggle
        ui = captured_ui.get("pending_ui", {})
        field_keys = [f["key"] for f in ui.get("fields", [])]
        self.assertIn("incremental_enabled", field_keys)


# ── run_sync integration ─────────────────────────────────────────────────


class TestRunSyncPreservesIncremental(unittest.TestCase):
    @patch("ai.tools.onboarding.upsert_user_connector_onboarding")
    @patch("ai.tools.onboarding.get_active_user_connector_by_repository")
    @patch("ai.tools.onboarding.get_onboarding_context")
    def test_run_sync_preserves_incremental_from_existing(
        self,
        mock_ctx: MagicMock,
        mock_get_existing: MagicMock,
        mock_upsert: MagicMock,
    ) -> None:
        ctx = _onboarding_ctx()
        mock_ctx.return_value = ctx
        mock_get_existing.return_value = {
            "id": "row-1",
            "docker_image": "airbyte/source-facebook-marketing:5.2.6",
            "config": {"access_token": "tok"},
            "oauth_meta": None,
            "selected_streams": ["ads"],
            "sync_mode": "recurring",
            "sync_frequency_minutes": 360,
            "incremental_enabled": True,
        }
        mock_upsert.return_value = {"id": "row-1"}

        from ai.tools.onboarding import run_sync

        # With Docker disabled, run_sync will skip the docker probe
        with patch("ai.tools.onboarding.ONBOARDING_DOCKER_ENABLED", False):
            result = json.loads(
                run_sync.invoke(
                    {"connector_name": "FB Marketing", "streams": ["ads"]}
                )
            )

        mock_upsert.assert_called_once()
        _, kwargs = mock_upsert.call_args
        self.assertTrue(kwargs["incremental_enabled"])

    @patch("ai.tools.onboarding.upsert_user_connector_onboarding")
    @patch("ai.tools.onboarding.get_active_user_connector_by_repository")
    @patch("ai.tools.onboarding.get_onboarding_context")
    def test_run_sync_defaults_false_when_not_in_existing(
        self,
        mock_ctx: MagicMock,
        mock_get_existing: MagicMock,
        mock_upsert: MagicMock,
    ) -> None:
        ctx = _onboarding_ctx()
        mock_ctx.return_value = ctx
        mock_get_existing.return_value = {
            "id": "row-1",
            "docker_image": "airbyte/source-test:1.0",
            "config": {},
            "oauth_meta": None,
            "selected_streams": [],
            "sync_mode": "recurring",
            "sync_frequency_minutes": 360,
            # incremental_enabled absent — old row before migration
        }
        mock_upsert.return_value = {"id": "row-1"}

        from ai.tools.onboarding import run_sync

        with patch("ai.tools.onboarding.ONBOARDING_DOCKER_ENABLED", False):
            run_sync.invoke({"connector_name": "Test", "streams": None})

        _, kwargs = mock_upsert.call_args
        self.assertFalse(kwargs["incremental_enabled"])


if __name__ == "__main__":
    unittest.main()
