"""Shared ACA Job + File Share helpers for connector execution (Sync V2).

Used by both the FastAPI backend (onboarding) and the sync orchestrator.
All connectors — regardless of language — run as official Airbyte Docker
images on a single ACA Job with image override.

Usage:
    from ai.agents.onboarding.infra.connector_runner import (
        start_connector_execution,
        poll_execution_status,
        wait_for_execution,
        write_to_fileshare,
        read_from_fileshare,
        cleanup_fileshare,
    )
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.storage.fileshare import ShareDirectoryClient, ShareFileClient

from fastapi_app.settings import (
    ACA_JOB_CONTAINER_NAME_V2,
    ACA_JOB_NAME_V2,
    ACA_RESOURCE_GROUP_V2,
    ACA_SUBSCRIPTION_ID_V2,
    AZURE_FILE_SHARE_CONN_STR,
    AZURE_FILE_SHARE_NAME_V2,
)

logger = logging.getLogger(__name__)

# ── ACA Job execution ────────────────────────────────────────────────


def start_connector_execution(
    docker_image: str,
    airbyte_command: str,
    work_id: str,
    *,
    extra_args: str = "",
) -> str:
    """Start a connector image on the single ACA Job. Returns execution_name.

    Args:
        docker_image: Full image reference, e.g. ``airbyte/source-shopify:3.2.3``
        airbyte_command: Airbyte CLI verb — ``check``, ``discover``, or ``read``
        work_id: Unique directory name on the File Share for this execution
        extra_args: Additional CLI args (e.g. ``--catalog /data/{work_id}/catalog.json``)
    """
    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, ACA_SUBSCRIPTION_ID_V2)

    shell_script = (
        f"echo \"EP=$AIRBYTE_ENTRYPOINT\" > /data/{work_id}/debug.log; "
        f"$AIRBYTE_ENTRYPOINT {airbyte_command} "
        f"--config /data/{work_id}/config.json "
        f"{extra_args} "
        f"> /data/{work_id}/output.jsonl "
        f"2>/data/{work_id}/stderr.log; "
        f"RC=$?; "
        f"echo \"RC=$RC\" >> /data/{work_id}/debug.log; "
        f"echo \"OUT=$(wc -c < /data/{work_id}/output.jsonl 2>/dev/null)\" >> /data/{work_id}/debug.log; "
        f"echo \"ERR=$(wc -c < /data/{work_id}/stderr.log 2>/dev/null)\" >> /data/{work_id}/debug.log; "
        f"exit 0"
    )

    container_override = {
        "name": ACA_JOB_CONTAINER_NAME_V2,
        "image": docker_image,
        "command": ["/bin/sh"],
        "args": ["-c", shell_script],
        "env": [{"name": "AIRBYTE_ENABLE_UNSAFE_CODE", "value": "true"}],
    }

    logger.info(
        "Starting ACA execution: job=%s image=%s cmd=%s work_id=%s",
        ACA_JOB_NAME_V2,
        docker_image,
        airbyte_command,
        work_id,
    )

    result = client.jobs.begin_start(
        resource_group_name=ACA_RESOURCE_GROUP_V2,
        job_name=ACA_JOB_NAME_V2,
        template={"containers": [container_override]},
    ).result()

    execution_name = str(getattr(result, "name", "") or "")
    logger.info("ACA execution started: %s", execution_name)
    return execution_name


def start_uploader_execution(
    uploader_image: str,
    work_id: str,
    *,
    env_vars: dict[str, str],
) -> str:
    """Start the sync-uploader image on the same ACA Job. Returns execution_name.

    The uploader reads JSONL from the File Share, converts to Parquet,
    and uploads to Blob Storage.
    """
    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, ACA_SUBSCRIPTION_ID_V2)

    env_list = [{"name": k, "value": v} for k, v in env_vars.items()]

    container_override = {
        "name": ACA_JOB_CONTAINER_NAME_V2,
        "image": uploader_image,
        "env": env_list,
    }

    logger.info(
        "Starting uploader execution: job=%s image=%s work_id=%s",
        ACA_JOB_NAME_V2,
        uploader_image,
        work_id,
    )

    result = client.jobs.begin_start(
        resource_group_name=ACA_RESOURCE_GROUP_V2,
        job_name=ACA_JOB_NAME_V2,
        template={"containers": [container_override]},
    ).result()

    execution_name = str(getattr(result, "name", "") or "")
    logger.info("Uploader execution started: %s", execution_name)
    return execution_name


# ── Execution polling ─────────────────────────────────────────────────


def poll_execution_status(execution_name: str) -> str:
    """Check ACA Job execution status.

    Returns ``'running'``, ``'succeeded'``, or ``'failed'``.
    """
    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, ACA_SUBSCRIPTION_ID_V2)

    try:
        job_exec_fn = getattr(client, "job_execution", None)
        if callable(job_exec_fn):
            execution = job_exec_fn(
                resource_group_name=ACA_RESOURCE_GROUP_V2,
                job_name=ACA_JOB_NAME_V2,
                job_execution_name=execution_name,
            )
        else:
            # Fallback for older SDK versions
            for attr_name in ("job_executions", "jobs_executions"):
                ops = getattr(client, attr_name, None)
                getter = getattr(ops, "get", None) if ops else None
                if callable(getter):
                    execution = getter(
                        resource_group_name=ACA_RESOURCE_GROUP_V2,
                        job_name=ACA_JOB_NAME_V2,
                        job_execution_name=execution_name,
                    )
                    break
            else:
                raise RuntimeError("No supported ACA job execution getter found")

        # Extract status from SDK model or dict
        status = ""
        if isinstance(execution, dict):
            status = (execution.get("properties") or {}).get("status", "")
        else:
            status = str(
                getattr(getattr(execution, "properties", None), "status", None)
                or getattr(execution, "status", "")
                or ""
            )

        status = status.strip().lower()
        if status in ("succeeded", "completed", "success"):
            return "succeeded"
        if status in ("failed", "canceled", "cancelled", "stopped", "error"):
            return "failed"
        return "running"

    except Exception as exc:
        logger.warning("poll_error: execution=%s error=%s", execution_name, exc)
        return "running"


def wait_for_execution(execution_name: str, timeout: int = 300, poll_interval: int = 5) -> bool:
    """Block until execution completes. Used by onboarding (user is waiting).

    Returns True on success, False on failure/timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = poll_execution_status(execution_name)
        if status == "succeeded":
            return True
        if status == "failed":
            return False
        time.sleep(poll_interval)
    return False


# ── File Share I/O ────────────────────────────────────────────────────


def write_to_fileshare(work_id: str, filename: str, content: str) -> None:
    """Write a file to ``/{work_id}/{filename}`` on the Azure File Share."""
    dir_client = ShareDirectoryClient.from_connection_string(
        conn_str=AZURE_FILE_SHARE_CONN_STR,
        share_name=AZURE_FILE_SHARE_NAME_V2,
        directory_path=work_id,
    )
    try:
        dir_client.create_directory()
    except ResourceExistsError:
        pass

    file_client = ShareFileClient.from_connection_string(
        conn_str=AZURE_FILE_SHARE_CONN_STR,
        share_name=AZURE_FILE_SHARE_NAME_V2,
        file_path=f"{work_id}/{filename}",
    )
    file_client.upload_file(content.encode("utf-8"))


def read_from_fileshare(work_id: str, filename: str) -> str:
    """Read a file from ``/{work_id}/{filename}`` on the Azure File Share.

    Returns empty string if file doesn't exist or read fails.
    """
    try:
        file_client = ShareFileClient.from_connection_string(
            conn_str=AZURE_FILE_SHARE_CONN_STR,
            share_name=AZURE_FILE_SHARE_NAME_V2,
            file_path=f"{work_id}/{filename}",
        )
        return file_client.download_file().readall().decode("utf-8", errors="replace")
    except (ResourceNotFoundError, Exception) as exc:
        logger.warning("Failed to read %s/%s: %s", work_id, filename, exc)
        return ""


def cleanup_fileshare(work_id: str) -> None:
    """Delete ``/{work_id}/`` and all files in it from the File Share."""
    try:
        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=AZURE_FILE_SHARE_CONN_STR,
            share_name=AZURE_FILE_SHARE_NAME_V2,
            directory_path=work_id,
        )
        for item in dir_client.list_directories_and_files():
            dir_client.delete_file(item["name"])
        dir_client.delete_directory()
    except Exception as exc:
        logger.warning("Cleanup failed for %s: %s", work_id, exc)


# ── Airbyte output parsing ───────────────────────────────────────────


def parse_connection_status(jsonl: str) -> tuple[bool, str]:
    """Parse CONNECTION_STATUS from Airbyte JSONL output."""
    for line in jsonl.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            if msg.get("type") == "CONNECTION_STATUS":
                status = msg.get("connectionStatus", {})
                ok = status.get("status", "").upper() == "SUCCEEDED"
                return ok, status.get("message", "")
        except json.JSONDecodeError:
            continue
    return False, "No CONNECTION_STATUS message in output"


def parse_catalog(jsonl: str) -> tuple[list[str], dict[str, Any] | None]:
    """Parse CATALOG from Airbyte JSONL. Returns ``(stream_names, raw_catalog)``."""
    for line in jsonl.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            if msg.get("type") == "CATALOG":
                catalog = msg.get("catalog", {})
                names: list[str] = []
                for stream_obj in catalog.get("streams", []):
                    name = stream_obj.get("name") or (
                        stream_obj.get("stream", {}).get("name")
                    )
                    if name:
                        names.append(str(name))
                return sorted(names), catalog
        except json.JSONDecodeError:
            continue
    return [], None


def build_configured_catalog(
    raw_catalog: dict[str, Any],
    selected_streams: list[str] | None = None,
) -> dict[str, Any]:
    """Build a ConfiguredAirbyteCatalog from a raw discover catalog.

    If ``selected_streams`` is None or empty, all streams are included.
    """
    name_set = set(selected_streams) if selected_streams else None
    streams_out: list[dict[str, Any]] = []

    for stream_obj in raw_catalog.get("streams", []) or []:
        if not isinstance(stream_obj, dict):
            continue
        name = stream_obj.get("name")
        if name_set and name not in name_set:
            continue

        modes = stream_obj.get("supported_sync_modes") or ["full_refresh"]
        sync_mode = "full_refresh" if "full_refresh" in modes else str(modes[0])
        cursor_field: list[str] = []
        if sync_mode != "full_refresh":
            dc = stream_obj.get("default_cursor_field")
            if isinstance(dc, list):
                cursor_field = [str(x) for x in dc]
        streams_out.append(
            {
                "stream": stream_obj,
                "sync_mode": sync_mode,
                "destination_sync_mode": "overwrite",
                "cursor_field": cursor_field,
            }
        )
    return {"streams": streams_out}


def count_records(jsonl: str, max_records: int = 0) -> int:
    """Count RECORD messages in Airbyte JSONL output."""
    count = 0
    for line in jsonl.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            if msg.get("type") == "RECORD":
                count += 1
                if max_records > 0 and count >= max_records:
                    return count
        except json.JSONDecodeError:
            continue
    return count
