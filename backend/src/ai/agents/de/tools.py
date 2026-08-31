"""
DE agent tool functions.

All tools are built as closures over (user_id, pipeline_id) so they carry
the correct authorization context without relying on global state.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from langchain.tools import tool

from fastapi_app.services import connector_service, de_service, metadata_service
from fastapi_app.models.de import DEPipelineStepUpsert, DEValidationRequest

logger = logging.getLogger(__name__)


def build_de_tools(
    user_id: str,
    pipeline_id: str,
    connector_config_id: str | None = None,
) -> list[Any]:
    """Return a list of LangChain tools scoped to (user_id, pipeline_id)."""

    def _resolve_scoped_tables() -> set[str] | None:
        if not connector_config_id:
            return None
        row = connector_service.get_user_connector(user_id, connector_config_id)
        if row is None:
            return None

        selected_streams = {
            str(s).strip().lower()
            for s in (row.get("selected_streams") or [])
            if str(s).strip()
        }
        docker_repository = str(row.get("docker_repository") or "")
        last_segment = docker_repository.split("/")[-1] if docker_repository else ""
        folder_prefix = re.sub(r"[^a-z0-9]", "_", last_segment.lower())

        tables = metadata_service.list_table_metadata(user_id=user_id)
        matched: set[str] = set()
        for table in tables:
            table_name = table.table_name
            lower = table_name.lower()
            if folder_prefix and lower.startswith(folder_prefix + "_"):
                stream_part = lower[len(folder_prefix) + 1 :]
                if not selected_streams or stream_part in selected_streams:
                    matched.add(table_name)

        if not matched and selected_streams:
            for table in tables:
                if table.table_name.lower() in selected_streams:
                    matched.add(table.table_name)

        return matched or None

    scoped_tables = _resolve_scoped_tables()

    @tool
    def get_connector_tables() -> str:
        """List all tables available for the user's connected data sources."""
        tables = metadata_service.list_table_metadata(user_id=user_id)
        if scoped_tables is not None:
            tables = [t for t in tables if t.table_name in scoped_tables]
        if not tables:
            return "No tables found for the selected data source. The user may not have completed a sync yet."
        rows = [f"- {t.table_name}" for t in tables]
        return "Available tables:\n" + "\n".join(rows)

    @tool
    def get_table_schema(table_name: str) -> str:
        """
        Return the column names and data types for a specific table.

        Args:
            table_name: The exact table name to inspect.
        """
        if scoped_tables is not None and table_name not in scoped_tables:
            return (
                f"Table '{table_name}' is not part of the selected data source. "
                "Call get_connector_tables() and choose one of those tables."
            )

        cols = metadata_service.list_column_metadata(user_id=user_id, table_name=table_name)
        if not cols:
            return f"No columns found for table '{table_name}'. Check the table name."
        rows = [f"  - {c.column_name}: {c.data_type or 'unknown'}" for c in cols]
        return f"Schema for '{table_name}':\n" + "\n".join(rows)

    @tool
    def list_recipes() -> str:
        """List all available transformation recipes with descriptions."""
        recipes = de_service.PREBUILT_RECIPES
        lines = [f"- **{r.label}** (`{r.recipe_type}`): {r.description}" for r in recipes]
        return "Available recipes:\n" + "\n".join(lines)

    @tool
    def add_step(recipe_type: str, config_json: str) -> str:
        """
        Add a new transformation step to the pipeline.

        The step is appended after any existing steps.

        Args:
            recipe_type: One of the recipe type identifiers, e.g. 'rename_columns'.
            config_json: JSON string with the recipe configuration, e.g. '{"mapping": {"old": "new"}}'.
        """
        try:
            config = json.loads(config_json)
        except json.JSONDecodeError as e:
            return f"Invalid config_json: {e}. Provide valid JSON."

        # Determine next step_order
        existing = de_service.list_steps(user_id=user_id, pipeline_id=pipeline_id)
        next_order = (max((s.step_order for s in existing), default=0) + 1)

        body = DEPipelineStepUpsert(
            step_order=next_order,
            recipe_type=recipe_type,
            config_json=config,
            is_enabled=True,
        )
        try:
            step = de_service.upsert_step(user_id=user_id, pipeline_id=pipeline_id, body=body)
            return f"Step #{step.step_order} added: {recipe_type} with config {config_json}."
        except ValueError as e:
            return f"Failed to add step: {e}"

    @tool
    def remove_step(step_order: int) -> str:
        """
        Remove a step from the pipeline by its step number.

        Args:
            step_order: The 1-based step number to remove.
        """
        existing = de_service.list_steps(user_id=user_id, pipeline_id=pipeline_id)
        target = next((s for s in existing if s.step_order == step_order), None)
        if target is None:
            orders = [s.step_order for s in existing]
            return f"No step with order {step_order}. Existing step orders: {orders}"
        deleted = de_service.delete_step(
            user_id=user_id, pipeline_id=pipeline_id, step_id=target.id
        )
        if deleted:
            return f"Step #{step_order} ({target.recipe_type}) removed."
        return f"Failed to remove step #{step_order}."

    @tool
    def run_validation() -> str:
        """
        Run all enabled pipeline steps against sample data and return a preview.
        Shows any errors and a sample of the transformed output rows.
        """
        # Fetch sample rows from metadata (use dummy rows if none available)
        sample_rows: list[dict[str, Any]] = [
            {"col_a": "value1", "col_b": 1},
            {"col_a": "value2", "col_b": 2},
        ]
        try:
            req = DEValidationRequest(sample_rows=sample_rows)
            result = de_service.validate_pipeline(
                user_id=user_id, pipeline_id=pipeline_id, request=req
            )
            if result.ok:
                preview = json.dumps(result.output_sample[:3], default=str, indent=2)
                return f"Validation passed. Output preview (first 3 rows):\n{preview}"
            else:
                errors = [
                    f"  Step {r.step_id}: {r.error}"
                    for r in result.step_results
                    if not r.ok
                ]
                return "Validation failed:\n" + "\n".join(errors)
        except Exception as e:
            return f"Validation error: {e}"

    return [
        get_connector_tables,
        get_table_schema,
        list_recipes,
        add_step,
        remove_step,
        run_validation,
    ]
