"""
CRUD and business logic for the Data Engineering pipeline layer.

All Supabase writes use the admin (service_role) client.
All queries are scoped by user_id — never trust a client-supplied user id.
"""

from __future__ import annotations

import io
import logging
import re
from datetime import datetime, timezone
from typing import Any

from fastapi_app.models.de import (
    DEDatasetMaterialization,
    DEMaterializationStatus,
    DEPipelineDeleteResponse,
    DEPipeline,
    DEPipelineCreate,
    DEPipelineDetail,
    DEPipelinePatch,
    DEPipelineRun,
    DEPipelineStep,
    DEPipelineStepUpsert,
    DEStepValidationResult,
    DEValidationRequest,
    DEValidationResponse,
    RecipeDefinition,
)
from fastapi_app.services import connector_service
from fastapi_app.services.de_pipeline_job_trigger import start_run as start_de_pipeline_run
from fastapi_app.settings import (
    AZURE_STORAGE_CONNECTION_STRING,
    DE_TRANSFORMED_CONTAINER,
    DE_TRANSFORMED_PREFIX,
    USER_DATA_BLOB_PREFIX,
)
from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)

_PIPELINES_TBL = "de_pipelines"
_STEPS_TBL = "de_pipeline_steps"
_RUNS_TBL = "de_pipeline_runs"
_MATS_TBL = "de_dataset_materializations"
_CONNECTORS_TBL = "user_connector_configs"

# ---------------------------------------------------------------------------
# Prebuilt recipe catalog
# ---------------------------------------------------------------------------

PREBUILT_RECIPES: list[RecipeDefinition] = [
    RecipeDefinition(
        recipe_type="rename_columns",
        label="Rename Columns",
        description="Rename one or more columns using a mapping.",
        config_schema={
            "type": "object",
            "properties": {
                "mapping": {
                    "type": "object",
                    "description": "Keys are current column names, values are new names.",
                    "additionalProperties": {"type": "string"},
                }
            },
            "required": ["mapping"],
        },
    ),
    RecipeDefinition(
        recipe_type="replace_values",
        label="Replace Values",
        description="Replace a specific value in a column with another value.",
        config_schema={
            "type": "object",
            "properties": {
                "column": {"type": "string", "description": "Target column name."},
                "from": {"description": "Value to replace (any scalar)."},
                "to": {"description": "Replacement value (any scalar)."},
            },
            "required": ["column", "from", "to"],
        },
    ),
    RecipeDefinition(
        recipe_type="derive_column",
        label="Derive Column",
        description="Create or overwrite a column using a pandas-style expression.",
        config_schema={
            "type": "object",
            "properties": {
                "column": {"type": "string", "description": "Output column name."},
                "expression": {
                    "type": "string",
                    "description": "DataFrame.eval()-compatible expression, e.g. 'price * quantity'.",
                },
            },
            "required": ["column", "expression"],
        },
    ),
    RecipeDefinition(
        recipe_type="extract_regex",
        label="Extract via Regex",
        description="Extract a capture group from a source column into a new column.",
        config_schema={
            "type": "object",
            "properties": {
                "source_column": {"type": "string"},
                "target_column": {"type": "string"},
                "pattern": {"type": "string", "description": "Python regex pattern."},
                "group": {
                    "type": "integer",
                    "default": 1,
                    "description": "Capture group index (1-based).",
                },
            },
            "required": ["source_column", "target_column", "pattern"],
        },
    ),
    RecipeDefinition(
        recipe_type="filter_rows",
        label="Filter Rows",
        description="Keep only rows that satisfy a pandas-style boolean expression.",
        config_schema={
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string",
                    "description": "DataFrame.query()-compatible expression, e.g. 'status == \"active\"'.",
                },
            },
            "required": ["expression"],
        },
    ),
    RecipeDefinition(
        recipe_type="drop_columns",
        label="Drop Columns",
        description="Remove one or more columns from the dataset.",
        config_schema={
            "type": "object",
            "properties": {
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Column names to drop.",
                }
            },
            "required": ["columns"],
        },
    ),
]

_RECIPE_TYPES: set[str] = {r.recipe_type for r in PREBUILT_RECIPES}


def _is_missing_source_connector_ids_column_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "pgrst204" in text
        and "source_connector_ids" in text
        and "de_pipelines" in text
    )


def _normalize_source_connector_ids(
    source_connector_ids: list[str] | None,
    connector_config_id: str | None,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for value in source_connector_ids or []:
        sid = str(value or "").strip()
        if not sid or sid in seen:
            continue
        seen.add(sid)
        out.append(sid)

    primary = str(connector_config_id or "").strip()
    if primary and primary not in seen:
        out.insert(0, primary)

    return out


def _dataset_prefix_for_source(*, user_id: str, connector_config_id: str) -> str:
    resolved_raw_prefix = connector_service.resolve_connector_blob_prefix(
        user_id,
        connector_config_id,
    )
    if resolved_raw_prefix:
        root = DE_TRANSFORMED_PREFIX.strip("/")
        return f"{root}/{resolved_raw_prefix}" if root else resolved_raw_prefix

    root = DE_TRANSFORMED_PREFIX.strip("/")
    base = f"{root}/" if root else ""
    return f"{base}{USER_DATA_BLOB_PREFIX}/{user_id}/{connector_config_id}"


def _delete_transformed_blobs(*, prefixes: list[str]) -> int:
    normalized: list[str] = []
    seen: set[str] = set()
    for prefix in prefixes:
        clean = str(prefix or "").strip().strip("/")
        if not clean or clean in seen:
            continue
        seen.add(clean)
        normalized.append(clean)

    if not normalized:
        return 0

    if not AZURE_STORAGE_CONNECTION_STRING:
        raise RuntimeError("Cannot delete transformed data: missing AZURE_STORAGE_CONNECTION_STRING.")

    try:
        from azure.storage.blob import BlobServiceClient
    except Exception as exc:  # pragma: no cover - import-level environment issue
        raise RuntimeError("Cannot delete transformed data: azure-storage-blob dependency unavailable.") from exc

    service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container = service.get_container_client(DE_TRANSFORMED_CONTAINER)

    deleted = 0
    for prefix in normalized:
        starts_with = f"{prefix}/"
        for blob in container.list_blobs(name_starts_with=starts_with):
            container.delete_blob(blob.name)
            deleted += 1
    return deleted


def _pipeline_from_row(row: dict[str, Any]) -> DEPipeline:
    source_ids = _normalize_source_connector_ids(
        row.get("source_connector_ids"),
        row.get("connector_config_id"),
    )
    payload = dict(row)
    payload["source_connector_ids"] = source_ids
    if source_ids:
        payload["connector_config_id"] = source_ids[0]
    return DEPipeline(**payload)


# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------


def list_pipelines(*, user_id: str, connector_config_id: str | None = None) -> list[DEPipeline]:
    sb = get_supabase_admin_client()
    q = sb.table(_PIPELINES_TBL).select("*").eq("user_id", user_id)
    res = q.order("created_at", desc=True).execute()
    out = [_pipeline_from_row(row) for row in (res.data or [])]
    if connector_config_id:
        out = [p for p in out if connector_config_id in p.source_connector_ids]
    return out


def get_pipeline(*, user_id: str, pipeline_id: str) -> DEPipeline | None:
    sb = get_supabase_admin_client()
    res = (
        sb.table(_PIPELINES_TBL)
        .select("*")
        .eq("id", pipeline_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return _pipeline_from_row(rows[0]) if rows else None


def get_pipeline_detail(*, user_id: str, pipeline_id: str) -> DEPipelineDetail | None:
    pipeline = get_pipeline(user_id=user_id, pipeline_id=pipeline_id)
    if pipeline is None:
        return None
    steps = list_steps(user_id=user_id, pipeline_id=pipeline_id)
    return DEPipelineDetail(**pipeline.model_dump(), steps=steps)


def create_pipeline(*, user_id: str, body: DEPipelineCreate) -> DEPipeline:
    source_ids = _normalize_source_connector_ids(
        body.source_connector_ids,
        body.connector_config_id,
    )
    if not source_ids:
        raise ValueError("At least one source connector must be selected.")

    sb = get_supabase_admin_client()
    insert_payload = {
        "user_id": user_id,
        "connector_config_id": source_ids[0],
        "source_connector_ids": source_ids,
        "name": body.name,
    }
    try:
        res = (
            sb.table(_PIPELINES_TBL)
            .insert(insert_payload)
            .execute()
        )
    except Exception as exc:
        # Backward compatibility while migration 12 is not yet applied.
        if not _is_missing_source_connector_ids_column_error(exc):
            raise
        logger.warning(
            "de_pipelines.source_connector_ids missing; falling back to legacy single-source create",
        )
        res = (
            sb.table(_PIPELINES_TBL)
            .insert(
                {
                    "user_id": user_id,
                    "connector_config_id": source_ids[0],
                    "name": body.name,
                }
            )
            .execute()
        )
    return _pipeline_from_row((res.data or [{}])[0])


def patch_pipeline(*, user_id: str, pipeline_id: str, body: DEPipelinePatch) -> DEPipeline | None:
    sb = get_supabase_admin_client()
    fields: dict[str, Any] = {}
    if body.name is not None:
        fields["name"] = body.name
    if body.is_active is not None:
        fields["is_active"] = body.is_active
    if body.source_connector_ids is not None:
        source_ids = _normalize_source_connector_ids(body.source_connector_ids, None)
        if not source_ids:
            raise ValueError("At least one source connector must be selected.")
        fields["source_connector_ids"] = source_ids
        fields["connector_config_id"] = source_ids[0]
    if not fields:
        return get_pipeline(user_id=user_id, pipeline_id=pipeline_id)

    try:
        res = (
            sb.table(_PIPELINES_TBL)
            .update(fields)
            .eq("id", pipeline_id)
            .eq("user_id", user_id)
            .execute()
        )
    except Exception as exc:
        # Backward compatibility while migration 12 is not yet applied.
        if not (
            "source_connector_ids" in fields
            and _is_missing_source_connector_ids_column_error(exc)
        ):
            raise
        logger.warning(
            "de_pipelines.source_connector_ids missing; falling back to legacy single-source patch",
        )
        legacy_fields = dict(fields)
        legacy_fields.pop("source_connector_ids", None)
        if not legacy_fields:
            return get_pipeline(user_id=user_id, pipeline_id=pipeline_id)
        res = (
            sb.table(_PIPELINES_TBL)
            .update(legacy_fields)
            .eq("id", pipeline_id)
            .eq("user_id", user_id)
            .execute()
        )
    rows = res.data or []
    return _pipeline_from_row(rows[0]) if rows else None


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def list_steps(*, user_id: str, pipeline_id: str) -> list[DEPipelineStep]:
    """Return enabled + disabled steps ordered by step_order."""
    sb = get_supabase_admin_client()
    # Verify ownership via pipeline
    pipe = get_pipeline(user_id=user_id, pipeline_id=pipeline_id)
    if pipe is None:
        return []
    res = (
        sb.table(_STEPS_TBL)
        .select("*")
        .eq("pipeline_id", pipeline_id)
        .order("step_order", desc=False)
        .execute()
    )
    return [DEPipelineStep(**row) for row in (res.data or [])]


def delete_step(*, user_id: str, pipeline_id: str, step_id: str) -> bool:
    """Delete a single step by ID. Returns True if deleted, False if not found."""
    pipe = get_pipeline(user_id=user_id, pipeline_id=pipeline_id)
    if pipe is None:
        return False
    sb = get_supabase_admin_client()
    res = (
        sb.table(_STEPS_TBL)
        .delete()
        .eq("id", step_id)
        .eq("pipeline_id", pipeline_id)
        .execute()
    )
    return bool(res.data)


def upsert_step(*, user_id: str, pipeline_id: str, body: DEPipelineStepUpsert) -> DEPipelineStep:
    """Create or replace a step at the given step_order."""
    # Verify ownership
    pipe = get_pipeline(user_id=user_id, pipeline_id=pipeline_id)
    if pipe is None:
        raise ValueError("Pipeline not found or access denied.")

    sb = get_supabase_admin_client()
    payload = {
        "pipeline_id": pipeline_id,
        "step_order": body.step_order,
        "recipe_type": body.recipe_type,
        "config_json": body.config_json,
        "is_enabled": body.is_enabled,
    }
    res = (
        sb.table(_STEPS_TBL)
        .upsert(payload, on_conflict="pipeline_id,step_order")
        .execute()
    )
    return DEPipelineStep(**(res.data or [{}])[0])


# ---------------------------------------------------------------------------
# Validation (in-process, no ACA)
# ---------------------------------------------------------------------------


def _apply_step_in_memory(df: Any, step: DEPipelineStepUpsert | DEPipelineStep) -> Any:
    """Apply a single pipeline step to a pandas DataFrame. Mirrors runner logic."""
    import pandas as pd

    recipe_type = step.recipe_type.strip().lower()
    config = step.config_json if isinstance(step, (DEPipelineStepUpsert, DEPipelineStep)) else {}

    if recipe_type == "rename_columns":
        mapping = config.get("mapping") or {}
        return df.rename(columns={str(k): str(v) for k, v in mapping.items()})

    if recipe_type == "replace_values":
        col = str(config.get("column") or "")
        if col and col in df.columns:
            df = df.copy()
            df[col] = df[col].replace(config.get("from"), config.get("to"))
        return df

    if recipe_type == "derive_column":
        col = str(config.get("column") or "")
        expr = str(config.get("expression") or "").strip()
        if col and expr:
            df = df.copy()
            df[col] = df.eval(expr)
        return df

    if recipe_type == "extract_regex":
        src = str(config.get("source_column") or "")
        tgt = str(config.get("target_column") or "")
        pattern = str(config.get("pattern") or "")
        group_index = int(config.get("group", 1) or 1)
        if src and tgt and pattern and src in df.columns:
            compiled = re.compile(pattern)

            def _extract(val: Any) -> Any:
                if val is None:
                    return None
                m = compiled.search(str(val))
                if not m:
                    return None
                try:
                    return m.group(group_index)
                except IndexError:
                    return None

            df = df.copy()
            df[tgt] = df[src].map(_extract)
        return df

    if recipe_type == "filter_rows":
        expr = str(config.get("expression") or "").strip()
        if expr:
            df = df.query(expr)
        return df

    if recipe_type == "drop_columns":
        cols = [str(c) for c in (config.get("columns") or [])]
        existing = [c for c in cols if c in df.columns]
        if existing:
            df = df.drop(columns=existing)
        return df

    # Unknown recipe — no-op.
    return df


def validate_pipeline(
    *,
    user_id: str,
    pipeline_id: str,
    request: DEValidationRequest,
) -> DEValidationResponse:
    import pandas as pd

    steps = list_steps(user_id=user_id, pipeline_id=pipeline_id)
    enabled_steps = [s for s in steps if s.is_enabled]

    if not enabled_steps:
        return DEValidationResponse(
            ok=True,
            step_results=[],
            output_sample=request.sample_rows,
            error=None,
        )

    try:
        df = pd.DataFrame(request.sample_rows)
    except Exception as exc:
        return DEValidationResponse(ok=False, step_results=[], error=f"Invalid sample_rows: {exc}")

    step_results: list[DEStepValidationResult] = []
    all_ok = True

    for step in enabled_steps:
        try:
            df = _apply_step_in_memory(df, step)
            step_results.append(
                DEStepValidationResult(
                    step_order=step.step_order,
                    recipe_type=step.recipe_type,
                    ok=True,
                    output_columns=list(df.columns),
                )
            )
        except Exception as exc:
            all_ok = False
            step_results.append(
                DEStepValidationResult(
                    step_order=step.step_order,
                    recipe_type=step.recipe_type,
                    ok=False,
                    error=str(exc),
                    output_columns=list(df.columns),
                )
            )
            break  # Stop on first failure — downstream steps are meaningless.

    output_sample: list[dict[str, Any]] = []
    try:
        output_sample = df.head(50).to_dict(orient="records")
    except Exception:
        pass

    return DEValidationResponse(
        ok=all_ok,
        step_results=step_results,
        output_sample=output_sample,
    )


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


def list_runs(
    *,
    user_id: str,
    pipeline_id: str,
    limit: int = 20,
) -> list[DEPipelineRun]:
    # Verify ownership
    pipe = get_pipeline(user_id=user_id, pipeline_id=pipeline_id)
    if pipe is None:
        return []
    sb = get_supabase_admin_client()
    res = (
        sb.table(_RUNS_TBL)
        .select("*")
        .eq("pipeline_id", pipeline_id)
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return [DEPipelineRun(**row) for row in (res.data or [])]


def trigger_pipeline_run(*, user_id: str, pipeline_id: str) -> DEPipelineRun | None:
    """Create a manual run row and attempt to launch the DE runner."""
    pipeline = get_pipeline(user_id=user_id, pipeline_id=pipeline_id)
    if pipeline is None:
        return None

    source_ids = _normalize_source_connector_ids(
        pipeline.source_connector_ids,
        pipeline.connector_config_id,
    )
    if not source_ids:
        raise ValueError("Pipeline must have at least one source connector.")

    primary_source_id = source_ids[0]
    sb = get_supabase_admin_client()

    connector_row = (
        sb.table(_CONNECTORS_TBL)
        .select("docker_image")
        .eq("id", primary_source_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    )
    docker_image = str((connector_row.data or [{}])[0].get("docker_image") or "")
    raw_blob_prefix = connector_service.resolve_connector_blob_prefix(
        user_id,
        primary_source_id,
    ) or ""

    run_row = (
        sb.table(_RUNS_TBL)
        .insert(
            {
                "pipeline_id": pipeline_id,
                "user_id": user_id,
                "connector_config_id": primary_source_id,
                "trigger_source": "manual",
                "status": "queued",
                "sync_work_id": "",
            }
        )
        .execute()
    )
    run = (run_row.data or [{}])[0]
    run_id = str(run.get("id") or "").strip()
    if not run_id:
        raise RuntimeError("Failed to create DE run record.")

    failure_update = {
        "status": "failed_to_start",
        "ended_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        execution_name = start_de_pipeline_run(
            user_id=user_id,
            connector_config_id=primary_source_id,
            pipeline_id=pipeline_id,
            run_id=run_id,
            sync_work_id="",
            docker_image=docker_image,
            raw_blob_prefix=raw_blob_prefix,
        )
        if execution_name:
            updated = (
                sb.table(_RUNS_TBL)
                .update({"aca_execution_name": execution_name})
                .eq("id", run_id)
                .eq("user_id", user_id)
                .execute()
            )
            rows = updated.data or []
            return DEPipelineRun(**(rows[0] if rows else {**run, "aca_execution_name": execution_name}))

        failure_update["error"] = "DE pipeline runner is not configured."
    except Exception as exc:
        logger.exception("Failed to start manual DE pipeline run")
        failure_update["error"] = str(exc)[:2000]

    failed = (
        sb.table(_RUNS_TBL)
        .update(failure_update)
        .eq("id", run_id)
        .eq("user_id", user_id)
        .execute()
    )
    rows = failed.data or []
    return DEPipelineRun(**(rows[0] if rows else {**run, **failure_update}))


def delete_pipeline(*, user_id: str, pipeline_id: str) -> DEPipelineDeleteResponse | None:
    """Delete a pipeline and clean transformed artifacts for non-shared sources."""
    pipeline = get_pipeline(user_id=user_id, pipeline_id=pipeline_id)
    if pipeline is None:
        return None

    source_ids = _normalize_source_connector_ids(
        pipeline.source_connector_ids,
        pipeline.connector_config_id,
    )

    all_pipelines = list_pipelines(user_id=user_id)
    other_pipelines = [p for p in all_pipelines if p.id != pipeline_id]

    retained_source_ids: list[str] = []
    cleaned_source_ids: list[str] = []
    replacement_pipeline_by_source: dict[str, str] = {}

    for source_id in source_ids:
        replacement = next(
            (p.id for p in other_pipelines if source_id in p.source_connector_ids),
            None,
        )
        if replacement:
            retained_source_ids.append(source_id)
            replacement_pipeline_by_source[source_id] = replacement
        else:
            cleaned_source_ids.append(source_id)

    sb = get_supabase_admin_client()
    mats_res = (
        sb.table(_MATS_TBL)
        .select("id,pipeline_id,connector_config_id,output_prefix")
        .eq("user_id", user_id)
        .execute()
    )
    mat_rows = mats_res.data or []

    # Re-point materialization ownership for shared sources so cascade delete
    # of the removed pipeline does not drop still-relevant rows.
    for row in mat_rows:
        source_id = str(row.get("connector_config_id") or "")
        if source_id not in replacement_pipeline_by_source:
            continue
        replacement_pipeline = replacement_pipeline_by_source[source_id]
        if str(row.get("pipeline_id") or "") == replacement_pipeline:
            continue
        mat_id = str(row.get("id") or "")
        if not mat_id:
            continue
        (
            sb.table(_MATS_TBL)
            .update({"pipeline_id": replacement_pipeline})
            .eq("id", mat_id)
            .eq("user_id", user_id)
            .execute()
        )

    prefixes_to_delete: list[str] = []
    cleaned_source_set = set(cleaned_source_ids)

    for row in mat_rows:
        source_id = str(row.get("connector_config_id") or "")
        if source_id not in cleaned_source_set:
            continue
        output_prefix = str(row.get("output_prefix") or "").strip().strip("/")
        if output_prefix:
            prefixes_to_delete.append(output_prefix)
        mat_id = str(row.get("id") or "")
        if mat_id:
            (
                sb.table(_MATS_TBL)
                .delete()
                .eq("id", mat_id)
                .eq("user_id", user_id)
                .execute()
            )

    for source_id in cleaned_source_ids:
        prefixes_to_delete.append(
            _dataset_prefix_for_source(user_id=user_id, connector_config_id=source_id)
        )

    deleted_blob_count = _delete_transformed_blobs(prefixes=prefixes_to_delete)

    deleted = (
        sb.table(_PIPELINES_TBL)
        .delete()
        .eq("id", pipeline_id)
        .eq("user_id", user_id)
        .execute()
    )
    if not (deleted.data or []):
        return None

    return DEPipelineDeleteResponse(
        pipeline_id=pipeline_id,
        cleaned_source_connector_ids=cleaned_source_ids,
        retained_source_connector_ids=retained_source_ids,
        deleted_output_prefixes=prefixes_to_delete,
        deleted_blob_count=deleted_blob_count,
    )


# ---------------------------------------------------------------------------
# Materialization (internal helper used by query resolver)
# ---------------------------------------------------------------------------


def get_materialization(
    *,
    user_id: str,
    connector_config_id: str,
) -> DEDatasetMaterialization | None:
    sb = get_supabase_admin_client()
    res = (
        sb.table(_MATS_TBL)
        .select("*")
        .eq("user_id", user_id)
        .eq("connector_config_id", connector_config_id)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return DEDatasetMaterialization(**rows[0]) if rows else None
