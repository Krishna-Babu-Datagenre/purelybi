"""Entrypoint for the DE pipeline runner container.

The runner is triggered post-upload by the sync orchestrator when an active
pipeline exists for a connector config. It reads raw Parquet blobs, applies
ordered pipeline steps, writes transformed Parquet blobs, and updates Supabase
run/materialization metadata.
"""

from __future__ import annotations

import json
import os
import re
import sys
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

import pandas as pd
from azure.storage.blob import BlobServiceClient


def _env(name: str, default: str = "") -> str:
    value = os.environ.get(name, default)
    return value.strip() if isinstance(value, str) else default


USER_ID = _env("USER_ID")
CONNECTOR_CONFIG_ID = _env("CONNECTOR_CONFIG_ID")
DE_PIPELINE_ID = _env("DE_PIPELINE_ID")
DE_RUN_ID = _env("DE_RUN_ID")
SYNC_WORK_ID = _env("SYNC_WORK_ID")
CONNECTOR_DOCKER_IMAGE = _env("CONNECTOR_DOCKER_IMAGE")
RAW_BLOB_PREFIX = _env("RAW_BLOB_PREFIX")

AZURE_STORAGE_CONNECTION_STRING = _env("AZURE_STORAGE_CONNECTION_STRING")
RAW_CONTAINER_NAME = _env("RAW_CONTAINER_NAME", "raw")
TRANSFORMED_CONTAINER_NAME = _env("TRANSFORMED_CONTAINER_NAME", "transformed")
USER_DATA_BLOB_PREFIX = _env("USER_DATA_BLOB_PREFIX", "user-data")
TRANSFORMED_PREFIX = _env("TRANSFORMED_PREFIX", "")

SUPABASE_URL = _env("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = _env("SUPABASE_SERVICE_ROLE_KEY")


def _headers() -> dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _supabase_get(path: str) -> list[dict[str, Any]]:
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/{path}",
        method="GET",
        headers=_headers(),
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        payload = response.read().decode("utf-8")
        data = json.loads(payload or "[]")
        return data if isinstance(data, list) else []


def _supabase_patch(path: str, fields: dict[str, Any]) -> None:
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/{path}",
        data=json.dumps(fields, default=str).encode("utf-8"),
        method="PATCH",
        headers=_headers(),
    )
    with urllib.request.urlopen(req, timeout=20):
        return


def _supabase_post(path: str, payload: dict[str, Any], prefer_minimal: bool = False) -> list[dict[str, Any]]:
    headers = _headers()
    if prefer_minimal:
        headers["Prefer"] = "return=minimal"
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/{path}",
        data=json.dumps(payload, default=str).encode("utf-8"),
        method="POST",
        headers=headers,
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        if prefer_minimal:
            return []
        payload_text = response.read().decode("utf-8")
        data = json.loads(payload_text or "[]")
        return data if isinstance(data, list) else []


def _update_run(*, status: str, error: str | None = None, started: bool | None = None, ended: bool = False) -> None:
    fields: dict[str, Any] = {"status": status}
    now_iso = datetime.now(timezone.utc).isoformat()
    if started is True:
        fields["started_at"] = now_iso
    if ended:
        fields["ended_at"] = now_iso
    if error:
        fields["error"] = error[:8000]
    _supabase_patch(f"de_pipeline_runs?id=eq.{DE_RUN_ID}", fields)


def _source_slug() -> str:
    image = CONNECTOR_DOCKER_IMAGE.split(":", 1)[0]
    tail = image.split("/")[-1] if image else "unknown"
    return tail.strip() or "unknown"


def _raw_prefix() -> str:
    explicit = RAW_BLOB_PREFIX.strip("/")
    if explicit:
        return f"{explicit}/"
    return f"{USER_DATA_BLOB_PREFIX}/{USER_ID}/{_source_slug()}/"


def _dataset_prefix() -> str:
    raw_base = RAW_BLOB_PREFIX.strip("/")
    if raw_base:
        root = TRANSFORMED_PREFIX.strip("/")
        return f"{root}/{raw_base}" if root else raw_base

    root = TRANSFORMED_PREFIX.strip("/")
    base = f"{root}/" if root else ""
    return f"{base}{USER_DATA_BLOB_PREFIX}/{USER_ID}/{CONNECTOR_CONFIG_ID}"


def _run_prefix() -> str:
    return f"{_dataset_prefix()}/runs/{DE_RUN_ID}"


def _load_steps() -> list[dict[str, Any]]:
    rows = _supabase_get(
        "de_pipeline_steps"
        f"?pipeline_id=eq.{DE_PIPELINE_ID}"
        "&is_enabled=eq.true"
        "&order=step_order.asc"
    )
    return rows


def _apply_step(df: pd.DataFrame, step: dict[str, Any]) -> pd.DataFrame:
    recipe_type = str(step.get("recipe_type") or "").strip().lower()
    config = step.get("config_json") or {}
    if not isinstance(config, dict):
        config = {}

    if recipe_type == "rename_columns":
        mapping = config.get("mapping") or {}
        if isinstance(mapping, dict):
            rename_map = {str(k): str(v) for k, v in mapping.items()}
            return df.rename(columns=rename_map)
        return df

    if recipe_type == "replace_values":
        col = str(config.get("column") or "")
        from_value = config.get("from")
        to_value = config.get("to")
        if col and col in df.columns:
            df[col] = df[col].replace(from_value, to_value)
        return df

    if recipe_type == "derive_column":
        col = str(config.get("column") or "")
        expression = str(config.get("expression") or "").strip()
        if col and expression:
            # DataFrame.eval keeps expressions constrained to dataframe context.
            df[col] = df.eval(expression)
        return df

    if recipe_type == "extract_regex":
        source_column = str(config.get("source_column") or "")
        target_column = str(config.get("target_column") or "")
        pattern = str(config.get("pattern") or "")
        group_index = int(config.get("group", 1) or 1)
        if source_column and target_column and pattern and source_column in df.columns:
            compiled = re.compile(pattern)

            def _extract(value: Any) -> Any:
                if value is None:
                    return None
                match = compiled.search(str(value))
                if not match:
                    return None
                try:
                    return match.group(group_index)
                except IndexError:
                    return None

            df[target_column] = df[source_column].map(_extract)
        return df

    # Unknown steps are no-ops in v1 to keep runs resilient.
    return df


def _load_parquet_blobs(blob_service: BlobServiceClient, prefix: str) -> list[tuple[str, pd.DataFrame]]:
    container = blob_service.get_container_client(RAW_CONTAINER_NAME)
    out: list[tuple[str, pd.DataFrame]] = []
    for blob in container.list_blobs(name_starts_with=prefix):
        name = str(blob.name)
        if not name.endswith(".parquet"):
            continue
        payload = container.download_blob(name).readall()
        df = pd.read_parquet(BytesIO(payload))
        out.append((name, df))
    return out


def _write_parquet_blobs(
    blob_service: BlobServiceClient,
    transformed_rows: list[tuple[str, pd.DataFrame]],
    *,
    raw_prefix: str,
    run_prefix: str,
) -> int:
    container = blob_service.get_container_client(TRANSFORMED_CONTAINER_NAME)
    count = 0
    for raw_blob_name, df in transformed_rows:
        relative = raw_blob_name[len(raw_prefix):] if raw_blob_name.startswith(raw_prefix) else raw_blob_name.split("/", 3)[-1]
        target_name = f"{run_prefix}/{relative}".replace("//", "/")
        stream = BytesIO()
        df.to_parquet(stream, index=False)
        stream.seek(0)
        container.upload_blob(target_name, stream, overwrite=True)
        count += 1
    return count


def _upsert_materialization(output_prefix: str) -> None:
    rows = _supabase_get(
        "de_dataset_materializations"
        f"?user_id=eq.{USER_ID}&connector_config_id=eq.{CONNECTOR_CONFIG_ID}"
        "&select=id"
    )
    payload = {
        "user_id": USER_ID,
        "connector_config_id": CONNECTOR_CONFIG_ID,
        "pipeline_id": DE_PIPELINE_ID,
        "last_success_run_id": DE_RUN_ID,
        "status": "ready",
        "output_prefix": output_prefix,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if rows:
        mat_id = rows[0].get("id")
        _supabase_patch(f"de_dataset_materializations?id=eq.{mat_id}", payload)
    else:
        _supabase_post("de_dataset_materializations", payload, prefer_minimal=True)


def run() -> int:
    missing = [
        name
        for name, value in (
            ("USER_ID", USER_ID),
            ("CONNECTOR_CONFIG_ID", CONNECTOR_CONFIG_ID),
            ("DE_PIPELINE_ID", DE_PIPELINE_ID),
            ("DE_RUN_ID", DE_RUN_ID),
            ("AZURE_STORAGE_CONNECTION_STRING", AZURE_STORAGE_CONNECTION_STRING),
            ("SUPABASE_URL", SUPABASE_URL),
            ("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY),
        )
        if not value
    ]
    if missing:
        raise RuntimeError("Missing required env vars: " + ", ".join(missing))

    _update_run(status="running", started=True)

    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    raw_prefix = _raw_prefix()
    run_prefix = _run_prefix()

    steps = _load_steps()
    blobs = _load_parquet_blobs(blob_service, raw_prefix)
    if not blobs:
        raise RuntimeError(f"No raw parquet files found under prefix: {raw_prefix}")

    transformed_rows: list[tuple[str, pd.DataFrame]] = []
    for raw_blob_name, df in blobs:
        current = df
        for step in steps:
            try:
                current = _apply_step(current, step)
            except Exception as exc:
                order = step.get("step_order")
                raise RuntimeError(
                    f"Step failed (order={order}, type={step.get('recipe_type')}): {exc}"
                ) from exc
        transformed_rows.append((raw_blob_name, current))

    files_written = _write_parquet_blobs(
        blob_service,
        transformed_rows,
        raw_prefix=raw_prefix,
        run_prefix=run_prefix,
    )

    _upsert_materialization(run_prefix)
    _update_run(status="succeeded", ended=True, error=None)
    print(
        "de runner succeeded",
        json.dumps(
            {
                "user_id": USER_ID,
                "connector_config_id": CONNECTOR_CONFIG_ID,
                "pipeline_id": DE_PIPELINE_ID,
                "run_id": DE_RUN_ID,
                "sync_work_id": SYNC_WORK_ID,
                "files_written": files_written,
                "output_prefix": run_prefix,
            }
        ),
    )
    return 0


def main() -> int:
    try:
        return run()
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[:2000]}"
        try:
            _update_run(status="failed", ended=True, error=error_text)
        except Exception:
            print("failed to update run status", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
        print(error_text, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())