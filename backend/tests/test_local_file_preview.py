"""Regression tests for local file preview validation and error messaging."""

from __future__ import annotations

import asyncio
import io
import types
import unittest
from unittest.mock import patch

from fastapi import HTTPException, UploadFile

from fastapi_app.services import connector_service


def _upload(filename: str, payload: bytes) -> UploadFile:
    return UploadFile(filename=filename, file=io.BytesIO(payload))


class LocalFilePreviewTests(unittest.TestCase):
    def test_preview_rejects_unsupported_extension(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(connector_service.preview_local_file(_upload("notes.txt", b"hello")))

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("Unsupported file type", str(ctx.exception.detail))
        self.assertIn(".txt", str(ctx.exception.detail))

    def test_preview_rejects_empty_payload(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(connector_service.preview_local_file(_upload("data.csv", b"")))

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("is empty", str(ctx.exception.detail))

    def test_preview_reports_missing_xlsx_engine_actionably(self) -> None:
        fake_pd = types.ModuleType("pandas")
        fake_pd.ExcelFile = lambda *_args, **_kwargs: types.SimpleNamespace(sheet_names=["Sheet1"])

        def _raise_missing_openpyxl(*_args, **_kwargs):
            raise ImportError("Missing optional dependency 'openpyxl'.")

        fake_pd.read_excel = _raise_missing_openpyxl
        fake_pd.notnull = lambda value: value is not None

        with patch.dict("sys.modules", {"pandas": fake_pd}):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(
                    connector_service.preview_local_file(
                        _upload("workbook.xlsx", b"not-a-real-xlsx")
                    )
                )

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn(".xlsx' support is unavailable", str(ctx.exception.detail))

    def test_preview_reports_invalid_excel_file_actionably(self) -> None:
        fake_pd = types.ModuleType("pandas")
        fake_pd.ExcelFile = lambda *_args, **_kwargs: types.SimpleNamespace(sheet_names=["Sheet1"])

        def _raise_invalid_excel(*_args, **_kwargs):
            raise ValueError("Excel file format cannot be determined")

        fake_pd.read_excel = _raise_invalid_excel
        fake_pd.notnull = lambda value: value is not None

        with patch.dict("sys.modules", {"pandas": fake_pd}):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(
                    connector_service.preview_local_file(
                        _upload("broken.xlsx", b"not-a-real-xlsx")
                    )
                )

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("not a valid Excel file", str(ctx.exception.detail))

    def test_preview_excel_includes_sheet_metadata(self) -> None:
        class _FakeValues:
            def tolist(self):
                return [[1, "Alex"]]

        class _FakeDataFrame:
            columns = ["id", "name"]

            def where(self, *_args, **_kwargs):
                return self

            @property
            def values(self):
                return _FakeValues()

        fake_pd = types.ModuleType("pandas")
        fake_pd.ExcelFile = lambda *_args, **_kwargs: types.SimpleNamespace(sheet_names=["Overview", "Employees"])
        fake_pd.read_excel = lambda *_args, **_kwargs: _FakeDataFrame()
        fake_pd.notnull = lambda value: value is not None

        with patch.dict("sys.modules", {"pandas": fake_pd}):
            preview = asyncio.run(
                connector_service.preview_local_file(
                    _upload("human_resources.xlsx", b"fake-bytes"),
                    sheet_name="Employees",
                    all_sheets=True,
                )
            )

        self.assertEqual(preview["sheet_name"], "Employees")
        self.assertEqual(preview["available_sheets"], ["Overview", "Employees"])
        self.assertEqual(preview["columns"], ["id", "name"])
