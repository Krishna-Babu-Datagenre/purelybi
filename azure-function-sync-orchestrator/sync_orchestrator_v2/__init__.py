"""Sync Orchestrator V2 — Azure Function (Timer Trigger, every 5 minutes).

State-machine orchestrator for the simplified sync architecture:
  - Single ACA Job runs both connector images and the sync-uploader
  - No language routing — all connectors use official Docker images
  - No sync-worker middleman — connectors run directly on ACA

State transitions per timer tick:
  1. uploading → check uploader execution → success / failed
  2. reading   → check connector execution → start uploader → uploading
  3. eligible  → write config to File Share → start connector → reading
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from uuid import uuid4

import azure.functions as func
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.storage.fileshare import ShareDirectoryClient, ShareFileClient
from supabase import Client, create_client

logger = logging.getLogger(__name__)

# ── Environment variables ─────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

AZURE_SUBSCRIPTION_ID = os.environ["AZURE_SUBSCRIPTION_ID"]
AZURE_RESOURCE_GROUP = os.environ["AZURE_RESOURCE_GROUP"]
ACA_JOB_NAME = os.environ["ACA_JOB_NAME"]
ACA_JOB_CONTAINER_NAME = os.environ.get("ACA_JOB_CONTAINER_NAME", "connector")

FILESHARE_CONN_STR = os.environ["AZURE_FILE_SHARE_CONN_STR"]
FILESHARE_NAME = os.environ["AZURE_FILE_SHARE_NAME"]

BLOB_CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "raw")

SYNC_UPLOADER_IMAGE = os.environ["SYNC_UPLOADER_IMAGE"]

# Circuit breaker: skip configs that have failed this many times consecutively
MAX_CONSECUTIVE_FAILURES = 5


def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ── File Share I/O ────────────────────────────────────────────────────


def write_to_fileshare(work_id: str, filename: str, content: str) -> None:
    dir_client = ShareDirectoryClient.from_connection_string(
        conn_str=FILESHARE_CONN_STR,
        share_name=FILESHARE_NAME,
        directory_path=work_id,
    )
    try:
        dir_client.create_directory()
    except ResourceExistsError:
        pass
    file_client = ShareFileClient.from_connection_string(
        conn_str=FILESHARE_CONN_STR,
        share_name=FILESHARE_NAME,
        file_path=f"{work_id}/{filename}",
    )
    file_client.upload_file(content.encode("utf-8"))


def read_from_fileshare(work_id: str, filename: str) -> str:
    try:
        file_client = ShareFileClient.from_connection_string(
            conn_str=FILESHARE_CONN_STR,
            share_name=FILESHARE_NAME,
            file_path=f"{work_id}/{filename}",
        )
        return file_client.download_file().readall().decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning("Failed to read %s/%s: %s", work_id, filename, exc)
        return ""


def cleanup_fileshare(work_id: str) -> None:
    try:
        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=FILESHARE_CONN_STR,
            share_name=FILESHARE_NAME,
            directory_path=work_id,
        )
        for item in dir_client.list_directories_and_files():
            dir_client.delete_file(item["name"])
        dir_client.delete_directory()
    except Exception as exc:
        logger.warning("Cleanup failed for %s: %s", work_id, exc)


# ── ACA Job execution ────────────────────────────────────────────────


def start_connector_execution(
    docker_image: str,
    airbyte_command: str,
    work_id: str,
    *,
    extra_args: str = "",
    credential: DefaultAzureCredential | None = None,
) -> str:
    """Start a connector image on the ACA Job. Returns execution_name."""
    if credential is None:
        credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, AZURE_SUBSCRIPTION_ID)

    shell_script = (
        f"$AIRBYTE_ENTRYPOINT {airbyte_command} "
        f"--config /data/{work_id}/config.json "
        f"{extra_args} "
        f"> /data/{work_id}/output.jsonl "
        f"2>/data/{work_id}/stderr.log "
        f"|| true"
    )

    container_override = {
        "name": ACA_JOB_CONTAINER_NAME,
        "image": docker_image,
        "command": ["/bin/sh"],
        "args": ["-c", shell_script],
        "env": [{"name": "AIRBYTE_ENABLE_UNSAFE_CODE", "value": "true"}],
    }

    result = client.jobs.begin_start(
        resource_group_name=AZURE_RESOURCE_GROUP,
        job_name=ACA_JOB_NAME,
        template={"containers": [container_override]},
    ).result()

    return str(getattr(result, "name", "") or "")


def start_uploader_execution(
    work_id: str,
    user_id: str,
    docker_image: str,
    *,
    credential: DefaultAzureCredential | None = None,
) -> str:
    """Start the sync-uploader image on the ACA Job. Returns execution_name."""
    if credential is None:
        credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, AZURE_SUBSCRIPTION_ID)

    env_list = [
        {"name": "WORK_ID", "value": work_id},
        {"name": "USER_ID", "value": user_id},
        {"name": "DOCKER_IMAGE", "value": docker_image},
        {"name": "AZURE_FILE_SHARE_CONN_STR", "value": FILESHARE_CONN_STR},
        {"name": "AZURE_FILE_SHARE_NAME", "value": FILESHARE_NAME},
        {"name": "AZURE_STORAGE_CONNECTION_STRING", "value": BLOB_CONN_STR},
        {"name": "BLOB_CONTAINER_NAME", "value": BLOB_CONTAINER},
    ]

    container_override = {
        "name": ACA_JOB_CONTAINER_NAME,
        "image": SYNC_UPLOADER_IMAGE,
        "env": env_list,
    }

    result = client.jobs.begin_start(
        resource_group_name=AZURE_RESOURCE_GROUP,
        job_name=ACA_JOB_NAME,
        template={"containers": [container_override]},
    ).result()

    return str(getattr(result, "name", "") or "")


def poll_execution_status(
    execution_name: str,
    *,
    credential: DefaultAzureCredential | None = None,
) -> str:
    """Returns 'running', 'succeeded', or 'failed'."""
    if credential is None:
        credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, AZURE_SUBSCRIPTION_ID)

    try:
        job_exec_fn = getattr(client, "job_execution", None)
        if callable(job_exec_fn):
            execution = job_exec_fn(
                resource_group_name=AZURE_RESOURCE_GROUP,
                job_name=ACA_JOB_NAME,
                job_execution_name=execution_name,
            )
        else:
            for attr_name in ("job_executions", "jobs_executions"):
                ops = getattr(client, attr_name, None)
                getter = getattr(ops, "get", None) if ops else None
                if callable(getter):
                    execution = getter(
                        resource_group_name=AZURE_RESOURCE_GROUP,
                        job_name=ACA_JOB_NAME,
                        job_execution_name=execution_name,
                    )
                    break
            else:
                raise RuntimeError("No supported ACA job execution getter")

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
        logger.warning("poll_error: %s — %s", execution_name, exc)
        return "running"


# ── Credential refresh (inlined from credential_refresh.py) ──────────


def refresh_credentials_if_needed(
    raw_config: dict, oauth_meta: dict
) -> tuple[dict, bool]:
    """Refresh OAuth tokens if expired. Returns (config, was_refreshed)."""
    import sys
    # Import from the shared credential_refresh module
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
    try:
        from credential_refresh import (
            ReauthRequired,
            TokenRefreshError,
            ensure_fresh_credentials,
        )
        return ensure_fresh_credentials(raw_config, oauth_meta)
    except ReauthRequired:
        raise
    except TokenRefreshError as exc:
        logger.warning("Token refresh failed: %s — continuing with current credentials", exc)
        return raw_config, False
    except ImportError:
        logger.warning("credential_refresh module not found — skipping token refresh")
        return raw_config, False
    finally:
        sys.path.pop(0)


# ── Airbyte JSONL helpers ─────────────────────────────────────────────


def extract_last_airbyte_state(jsonl: str) -> dict[str, Any] | None:
    """Extract the last Airbyte STATE message from JSONL output.

    Connectors emit STATE messages as checkpoints during a read.  The last one
    represents the cursor position at the end of the sync.  Persisting it and
    passing it back via ``--state`` on the next run enables incremental sync.
    """
    last_state: dict[str, Any] | None = None
    for line in jsonl.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            if msg.get("type") == "STATE":
                last_state = msg
        except json.JSONDecodeError:
            continue
    return last_state


# ── Catalog helpers ───────────────────────────────────────────────────


def build_configured_catalog(
    raw_catalog: dict,
    selected_streams: list[str] | None = None,
    *,
    prefer_incremental: bool = False,
) -> dict:
    """Build ConfiguredAirbyteCatalog from raw discover catalog.

    When *prefer_incremental* is True and a stream supports ``incremental``
    sync mode, the catalog entry uses ``incremental`` + ``append`` so the
    connector can resume from the last persisted Airbyte STATE.
    """
    name_set = set(selected_streams) if selected_streams else None
    streams_out = []
    for stream_obj in raw_catalog.get("streams", []) or []:
        if not isinstance(stream_obj, dict):
            continue
        name = stream_obj.get("name")
        if name_set and name not in name_set:
            continue
        modes = stream_obj.get("supported_sync_modes") or ["full_refresh"]

        if prefer_incremental and "incremental" in modes:
            sync_mode = "incremental"
            dest_mode = "append"
        else:
            sync_mode = "full_refresh" if "full_refresh" in modes else str(modes[0])
            dest_mode = "overwrite"

        cursor_field: list[str] = []
        if sync_mode == "incremental":
            dc = stream_obj.get("default_cursor_field")
            if isinstance(dc, list):
                cursor_field = [str(x) for x in dc]
        streams_out.append({
            "stream": stream_obj,
            "sync_mode": sync_mode,
            "destination_sync_mode": dest_mode,
            "cursor_field": cursor_field,
        })
    return {"streams": streams_out}


# ── Supabase queries ─────────────────────────────────────────────────


def get_configs_by_status(supabase: Client, status: str) -> list[dict]:
    """Get all user_connector_configs with the given last_sync_status."""
    resp = (
        supabase.table("user_connector_configs")
        .select("*")
        .eq("last_sync_status", status)
        .execute()
    )
    return resp.data or []


def get_eligible_configs(supabase: Client) -> list[dict]:
    """Query configs due for a new sync.

    Eligibility rules:
      * ``is_active`` and ``sync_validated`` must be True.
      * Not currently in-flight (``queued``, ``reading``, ``uploading``,
        ``reauth_required``).
      * Circuit breaker: ≥ MAX_CONSECUTIVE_FAILURES consecutive failures → skip.
      * **one_off**: eligible only when ``last_sync_at IS NULL``.
      * **recurring**: eligible when ``last_sync_at IS NULL`` OR elapsed
        minutes since last sync ≥ ``sync_frequency_minutes``.

    Syncing starts immediately once a config row is created (no scheduling
    gate). The connector-level ``start_date`` (data-range) is inside the
    config JSONB and passed directly to the Airbyte connector.
    """
    now = datetime.now(timezone.utc)
    response = (
        supabase.table("user_connector_configs")
        .select("*")
        .eq("is_active", True)
        .eq("sync_validated", True)
        .neq("last_sync_status", "queued")
        .neq("last_sync_status", "reading")
        .neq("last_sync_status", "uploading")
        .neq("last_sync_status", "reauth_required")
        .execute()
    )

    eligible = []
    for config in response.data or []:
        # Circuit breaker
        if (config.get("consecutive_failures") or 0) >= MAX_CONSECUTIVE_FAILURES:
            continue

        sync_mode = str(config.get("sync_mode") or "recurring").strip().lower()
        freq_minutes = config.get("sync_frequency_minutes", 360)
        last_sync = config.get("last_sync_at")

        if sync_mode == "one_off":
            if last_sync is not None:
                continue
            eligible.append(config)
            continue

        # Recurring: first run or enough time elapsed
        if last_sync is None:
            eligible.append(config)
            continue

        last_sync_dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
        elapsed_minutes = (now - last_sync_dt).total_seconds() / 60
        if elapsed_minutes >= int(freq_minutes or 360):
            eligible.append(config)

    return eligible


def mark_status(supabase: Client, config_id: str, **fields) -> None:
    supabase.table("user_connector_configs").update(fields).eq("id", config_id).execute()


# ── State machine phases ─────────────────────────────────────────────


def phase_check_uploading(supabase: Client, credential: DefaultAzureCredential) -> int:
    """Phase 1: Check configs where the uploader is running."""
    configs = get_configs_by_status(supabase, "uploading")
    completed = 0

    for config in configs:
        config_id = config["id"]
        execution_name = config.get("aca_execution_name", "")
        work_id = config.get("aca_work_id", "")

        if not execution_name:
            mark_status(supabase, config_id,
                        last_sync_status="failed",
                        last_sync_error="No uploader execution name tracked")
            continue

        status = poll_execution_status(execution_name, credential=credential)

        if status == "running":
            continue

        if status == "succeeded":
            # Capture the last Airbyte STATE message for incremental sync
            last_state = None
            if work_id:
                try:
                    jsonl = read_from_fileshare(work_id, "output.jsonl")
                    last_state = extract_last_airbyte_state(jsonl)
                except Exception as exc:
                    logger.warning("Failed to extract state for config=%s: %s", config_id, exc)

            now = datetime.now(timezone.utc).isoformat()
            update_fields: dict[str, Any] = {
                "last_sync_at": now,
                "last_sync_status": "success",
                "last_sync_error": None,
                "aca_execution_name": None,
                "aca_work_id": None,
                "consecutive_failures": 0,
            }
            if last_state is not None:
                update_fields["last_airbyte_state"] = last_state
            mark_status(supabase, config_id, **update_fields)
            cleanup_fileshare(work_id)
            completed += 1
            logger.info("Upload succeeded: config=%s state_captured=%s",
                        config_id, last_state is not None)
        else:
            stderr = read_from_fileshare(work_id, "stderr.log") if work_id else ""
            failures = (config.get("consecutive_failures") or 0) + 1
            mark_status(supabase, config_id,
                        last_sync_status="failed",
                        last_sync_error=f"Uploader failed: {stderr[:2000]}" if stderr else "Uploader failed",
                        aca_execution_name=None,
                        aca_work_id=None,
                        consecutive_failures=failures)
            cleanup_fileshare(work_id)
            logger.warning("Upload failed: config=%s", config_id)

    return completed


def phase_check_reading(supabase: Client, credential: DefaultAzureCredential) -> int:
    """Phase 2: Check configs where the connector is running. Start uploader if done."""
    configs = get_configs_by_status(supabase, "reading")
    transitioned = 0

    for config in configs:
        config_id = config["id"]
        execution_name = config.get("aca_execution_name", "")
        work_id = config.get("aca_work_id", "")

        if not execution_name:
            mark_status(supabase, config_id,
                        last_sync_status="failed",
                        last_sync_error="No connector execution name tracked")
            continue

        status = poll_execution_status(execution_name, credential=credential)

        if status == "running":
            continue

        if status == "succeeded":
            # Start the uploader on the same ACA Job
            try:
                uploader_exec = start_uploader_execution(
                    work_id=work_id,
                    user_id=config["user_id"],
                    docker_image=config["docker_image"],
                    credential=credential,
                )
                mark_status(supabase, config_id,
                            last_sync_status="uploading",
                            aca_execution_name=uploader_exec)
                transitioned += 1
                logger.info("Connector done, uploader started: config=%s exec=%s",
                            config_id, uploader_exec)
            except Exception as exc:
                failures = (config.get("consecutive_failures") or 0) + 1
                mark_status(supabase, config_id,
                            last_sync_status="failed",
                            last_sync_error=f"Failed to start uploader: {exc}"[:2000],
                            aca_execution_name=None,
                            consecutive_failures=failures)
                logger.exception("Failed to start uploader: config=%s", config_id)
        else:
            stderr = read_from_fileshare(work_id, "stderr.log") if work_id else ""
            failures = (config.get("consecutive_failures") or 0) + 1
            mark_status(supabase, config_id,
                        last_sync_status="failed",
                        last_sync_error=f"Connector failed: {stderr[:2000]}" if stderr else "Connector failed",
                        aca_execution_name=None,
                        aca_work_id=None,
                        consecutive_failures=failures)
            cleanup_fileshare(work_id)
            logger.warning("Connector failed: config=%s", config_id)

    return transitioned


def phase_start_new_syncs(supabase: Client, credential: DefaultAzureCredential) -> int:
    """Phase 3: Start new syncs for eligible configs."""
    eligible = get_eligible_configs(supabase)
    started = 0

    for config in eligible:
        config_id = config["id"]
        correlation_id = uuid4().hex[:8]
        work_id = f"sync-{config_id[:8]}-{correlation_id}"

        try:
            raw_config = config["config"]
            if isinstance(raw_config, str):
                raw_config = json.loads(raw_config)
            oauth_meta = config.get("oauth_meta") or raw_config.get("__oauth_meta__", {})

            # Refresh credentials if needed
            if oauth_meta:
                try:
                    raw_config, was_refreshed = refresh_credentials_if_needed(raw_config, oauth_meta)
                    if was_refreshed:
                        supabase.table("user_connector_configs").update(
                            {"config": raw_config, "oauth_meta": oauth_meta}
                        ).eq("id", config_id).execute()
                except Exception as exc:
                    if "reauth" in str(exc).lower():
                        mark_status(supabase, config_id,
                                    last_sync_status="reauth_required",
                                    last_sync_error=str(exc)[:2000])
                        continue
                    logger.warning("Credential refresh failed for %s: %s", config_id, exc)

            user_config = {k: v for k, v in raw_config.items() if not str(k).startswith("__")}

            # Get catalog: prefer cached from onboarding, otherwise we'd need to discover
            catalog = config.get("discovered_catalog")
            if not catalog:
                logger.warning("No discovered_catalog for config %s — skipping (run discover first)", config_id)
                continue

            # Build configured catalog for selected streams.
            # When incremental_enabled, prefer incremental sync mode per-stream
            # so the connector can resume from the last Airbyte STATE.
            incremental = bool(config.get("incremental_enabled", False))
            configured = build_configured_catalog(
                catalog,
                config.get("selected_streams"),
                prefer_incremental=incremental,
            )
            if not configured.get("streams"):
                mark_status(supabase, config_id,
                            last_sync_status="failed",
                            last_sync_error="No streams in configured catalog")
                continue

            # Write config + catalog to File Share
            write_to_fileshare(work_id, "config.json", json.dumps(user_config, default=str))
            write_to_fileshare(work_id, "catalog.json", json.dumps(configured, default=str))

            # If we have persisted Airbyte state from a previous sync and
            # incremental is enabled, write it so the connector can resume.
            extra_args = f"--catalog /data/{work_id}/catalog.json"
            last_state = config.get("last_airbyte_state")
            if last_state and incremental:
                write_to_fileshare(work_id, "state.json", json.dumps(last_state, default=str))
                extra_args += f" --state /data/{work_id}/state.json"

            # Start the connector on the ACA Job
            execution_name = start_connector_execution(
                docker_image=config["docker_image"],
                airbyte_command="read",
                work_id=work_id,
                extra_args=extra_args,
                credential=credential,
            )

            # Mark as reading with tracking info
            mark_status(supabase, config_id,
                        last_sync_status="reading",
                        last_sync_error=None,
                        aca_execution_name=execution_name,
                        aca_work_id=work_id)
            started += 1
            logger.info("Sync started: config=%s exec=%s work_id=%s",
                        config_id, execution_name, work_id)

        except Exception as exc:
            failures = (config.get("consecutive_failures") or 0) + 1
            mark_status(supabase, config_id,
                        last_sync_status="failed",
                        last_sync_error=f"Failed to start sync: {exc}"[:2000],
                        consecutive_failures=failures)
            logger.exception("Failed to start sync: config=%s", config_id)

    return started


# ── Entry point ───────────────────────────────────────────────────────


def main(timer: func.TimerRequest) -> None:
    """Timer trigger entry point — runs every 5 minutes."""
    if timer.past_due:
        logger.warning("Timer trigger is past due — running catch-up")

    supabase = get_supabase()
    credential = DefaultAzureCredential()

    # Phase 1: Check uploaders (uploading → success/failed)
    uploads_completed = phase_check_uploading(supabase, credential)

    # Phase 2: Check connectors (reading → uploading/failed)
    connectors_done = phase_check_reading(supabase, credential)

    # Phase 3: Start new syncs (eligible → reading)
    new_started = phase_start_new_syncs(supabase, credential)

    logger.info(
        "Orchestrator V2 tick: uploads_completed=%d connectors_done=%d new_started=%d",
        uploads_completed, connectors_done, new_started,
    )
