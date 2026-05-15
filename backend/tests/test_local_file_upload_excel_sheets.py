"""Regression tests for Excel multi-sheet upload handling."""

from __future__ import annotations

import asyncio
import io
import types
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import UploadFile

from fastapi_app.services import connector_service


def _upload(filename: str, payload: bytes) -> UploadFile:
    return UploadFile(filename=filename, file=io.BytesIO(payload))


class ExcelMultiSheetUploadTests(unittest.TestCase):
    def test_all_sheets_mode_uploads_each_sheet_with_filename_sheetname_streams(self) -> None:
        class _FakeDataFrame:
            columns = ["employee_id", "name"]

            def to_parquet(self, path: str, engine: str | None = None) -> None:
                del engine
                with open(path, "wb") as fh:
                    fh.write(b"parquet-bytes")

        fake_pd = types.ModuleType("pandas")
        fake_pd.ExcelFile = lambda *_args, **_kwargs: SimpleNamespace(
            sheet_names=["Summary", "People Data"]
        )
        fake_pd.read_excel = lambda *_args, **_kwargs: _FakeDataFrame()

        mock_container = MagicMock()

        update_builder = MagicMock()
        update_builder.update.return_value = update_builder
        update_builder.eq.return_value = update_builder
        update_builder.execute.return_value = SimpleNamespace(data=[{"id": "cfg-1"}])

        mock_client = MagicMock()
        mock_client.table.return_value = update_builder

        captured_selected_streams: list[str] = []

        def _fake_create_user_connector(user_id: str, body):
            del user_id
            captured_selected_streams[:] = list(body.selected_streams)
            return {
                "id": "cfg-1",
                "selected_streams": list(body.selected_streams),
            }

        with patch.dict("sys.modules", {"pandas": fake_pd}):
            with patch.object(
                connector_service,
                "_get_blob_container",
                return_value=mock_container,
            ), patch.object(
                connector_service,
                "create_user_connector",
                side_effect=_fake_create_user_connector,
            ), patch.object(
                connector_service,
                "get_supabase_admin_client",
                return_value=mock_client,
            ), patch.object(
                connector_service,
                "invalidate_stream_cache",
                return_value=None,
            ):
                result = asyncio.run(
                    connector_service.process_local_file_upload(
                        user_id="user-1",
                        files=[_upload("HumanResources.xlsx", b"dummy")],
                        source_name="HR Upload",
                        config_id=None,
                        excel_sheet_selections=[{"mode": "all"}],
                    )
                )

        self.assertIn("HumanResources_Summary", captured_selected_streams)
        self.assertIn("HumanResources_People_Data", captured_selected_streams)
        self.assertIn("selected_streams", result)
        self.assertIn("HumanResources_Summary", result["selected_streams"])
        self.assertIn("HumanResources_People_Data", result["selected_streams"])

        self.assertEqual(mock_container.upload_blob.call_count, 2)
        uploaded_blob_names = [call.kwargs["name"] for call in mock_container.upload_blob.call_args_list]
        self.assertTrue(any("/HumanResources_Summary/" in name for name in uploaded_blob_names))
        self.assertTrue(any("/HumanResources_People_Data/" in name for name in uploaded_blob_names))
