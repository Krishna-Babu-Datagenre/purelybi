"""Azure Container Apps Job runner for onboarding connector checks."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from azure.identity import DefaultAzureCredential

from fastapi_app.settings import (
    ONBOARDING_ACA_JOB_CONTAINER_NAME,
    ONBOARDING_ACA_JOB_NAME,
    ONBOARDING_ACA_DOCKER_JOB_NAME,
    ONBOARDING_ACA_DOCKER_JOB_CONTAINER_NAME,
    ONBOARDING_ACA_POLL_INTERVAL_SECONDS,
    ONBOARDING_ACA_RESOURCE_GROUP,
    ONBOARDING_ACA_SUBSCRIPTION_ID,
    ONBOARDING_ACA_WAIT_TIMEOUT_SECONDS,
    DOCKER_IMAGE_LANGUAGES,
)

logger = logging.getLogger(__name__)


def _as_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "as_dict"):
        try:
            value = obj.as_dict()
            if isinstance(value, dict):
                return value
        except Exception:
            pass
    return {}


def _extract_execution_name(result: Any) -> str:
    if isinstance(result, dict):
        return str(result.get("name") or "")
    return str(getattr(result, "name", "") or "")


def _extract_execution_status(execution: Any) -> str:
    data = _as_dict(execution)
    if data:
        properties = data.get("properties") or {}
        status = properties.get("status") or data.get("status")
        if status:
            return str(status)
    return str(
        getattr(getattr(execution, "properties", None), "status", None)
        or getattr(execution, "status", "")
        or ""
    )


def _env_with_overrides(base_env: list[dict[str, Any]], overrides: dict[str, str]) -> list[dict[str, Any]]:
    env_by_name: dict[str, dict[str, Any]] = {
        entry["name"]: dict(entry) for entry in base_env if isinstance(entry, dict) and entry.get("name")
    }
    for k, v in overrides.items():
        env_by_name[k] = {"name": k, "value": v}
    return list(env_by_name.values())


def _resolve_job_base_env(client: Any) -> tuple[str, str | None, list[dict[str, Any]]]:
    job = client.jobs.get(
        resource_group_name=ONBOARDING_ACA_RESOURCE_GROUP,
        job_name=ONBOARDING_ACA_JOB_NAME,
    )
    containers = ((job.template or {}).get("containers") if isinstance(job, dict) else None)
    if not containers:
        containers = getattr(getattr(job, "template", None), "containers", None)

    if not containers:
        return ONBOARDING_ACA_JOB_CONTAINER_NAME, None, []

    first = containers[0]
    name = (
        first.get("name")
        if isinstance(first, dict)
        else getattr(first, "name", None)
    )
    image = (
        first.get("image")
        if isinstance(first, dict)
        else getattr(first, "image", None)
    )
    raw_env = (
        first.get("env")
        if isinstance(first, dict)
        else getattr(first, "env", None)
    ) or []

    base_env: list[dict[str, Any]] = []
    for item in raw_env:
        env_name = (
            item.get("name")
            if isinstance(item, dict)
            else getattr(item, "name", None)
        )
        if not env_name:
            continue
        env_entry: dict[str, Any] = {"name": str(env_name)}
        if isinstance(item, dict):
            if item.get("value") is not None:
                env_entry["value"] = item["value"]
            if item.get("secretRef") is not None:
                env_entry["secretRef"] = item["secretRef"]
        else:
            item_value = getattr(item, "value", None)
            item_secret_ref = getattr(item, "secret_ref", None)
            if item_value is not None:
                env_entry["value"] = item_value
            if item_secret_ref is not None:
                env_entry["secretRef"] = item_secret_ref
        base_env.append(env_entry)

    container_name = str(name or ONBOARDING_ACA_JOB_CONTAINER_NAME)
    return container_name, (str(image) if image else None), base_env


def _get_execution(client: Any, execution_name: str) -> Any:
    # azure-mgmt-appcontainers 3.x exposes GET execution as a method on the client
    # (`ContainerAppsAPIClient.job_execution`, not `client.job_execution.get`).
    job_exec_fn = getattr(client, "job_execution", None)
    if callable(job_exec_fn):
        return job_exec_fn(
            resource_group_name=ONBOARDING_ACA_RESOURCE_GROUP,
            job_name=ONBOARDING_ACA_JOB_NAME,
            job_execution_name=execution_name,
        )
    # Older SDKs: operations object with .get (names vary).
    for attr in ("job_executions", "jobs_executions"):
        ops = getattr(client, attr, None)
        getter = getattr(ops, "get", None) if ops is not None else None
        if callable(getter):
            return getter(
                resource_group_name=ONBOARDING_ACA_RESOURCE_GROUP,
                job_name=ONBOARDING_ACA_JOB_NAME,
                job_execution_name=execution_name,
            )
    raise RuntimeError(
        "Container Apps SDK has no supported job execution getter "
        "(expected client.job_execution(...) or jobs_executions/job_executions.get)."
    )


def run_onboarding_aca_job(
    *,
    action: str,
    docker_image: str,
    config: dict[str, Any],
    streams: list[str] | None = None,
    max_streams: int | None = None,
    read_timeout: int | None = None,
) -> tuple[bool, str]:
    """Start onboarding ACA job execution and wait for terminal status."""
    if not ONBOARDING_ACA_SUBSCRIPTION_ID or not ONBOARDING_ACA_RESOURCE_GROUP or not ONBOARDING_ACA_JOB_NAME:
        return (
            False,
            "Azure job mode is selected, but onboarding ACA settings are incomplete "
            "(need ONBOARDING_ACA_SUBSCRIPTION_ID, ONBOARDING_ACA_RESOURCE_GROUP, ONBOARDING_ACA_JOB_NAME).",
        )

    try:
        from azure.mgmt.appcontainers import ContainerAppsAPIClient
    except Exception:
        return (
            False,
            "azure-mgmt-appcontainers is required for ONBOARDING_DOCKER_EXECUTION_MODE=azure_job.",
        )

    try:
        credential = DefaultAzureCredential()
        client = ContainerAppsAPIClient(credential, ONBOARDING_ACA_SUBSCRIPTION_ID)
        container_name, container_image, base_env = _resolve_job_base_env(client)

        payload = {
            "action": action,
            "docker_image": docker_image,
            "config": config,
            "streams": streams or [],
            "max_streams": max_streams,
            "read_timeout": read_timeout,
        }
        env_overrides = {
            "ONBOARDING_JOB_MODE": "onboarding_connector_probe",
            "ONBOARDING_JOB_PAYLOAD_JSON": json.dumps(payload, default=str),
        }

        container_override: dict[str, Any] = {
            "name": container_name,
            "env": _env_with_overrides(base_env, env_overrides),
        }
        if container_image:
            container_override["image"] = container_image

        logger.info(
            "Starting onboarding ACA job '%s' (container=%s, action=%s)",
            ONBOARDING_ACA_JOB_NAME,
            container_name,
            action,
        )
        result = client.jobs.begin_start(
            resource_group_name=ONBOARDING_ACA_RESOURCE_GROUP,
            job_name=ONBOARDING_ACA_JOB_NAME,
            template={"containers": [container_override]},
        ).result()
        execution_name = _extract_execution_name(result)
        if not execution_name:
            return False, "ACA job start returned no execution name."

        deadline = time.time() + max(10, ONBOARDING_ACA_WAIT_TIMEOUT_SECONDS)
        terminal_success = {"succeeded", "completed", "success"}
        terminal_failed = {"failed", "canceled", "cancelled", "stopped", "error"}

        while time.time() < deadline:
            execution = _get_execution(client, execution_name)
            status = _extract_execution_status(execution).strip().lower()
            if status in terminal_success:
                return True, f"ACA onboarding job execution succeeded ({execution_name})."
            if status in terminal_failed:
                return (
                    False,
                    f"ACA onboarding job execution failed with status '{status}' ({execution_name}).",
                )
            time.sleep(max(1, ONBOARDING_ACA_POLL_INTERVAL_SECONDS))

        return (
            False,
            f"ACA onboarding job did not complete within {ONBOARDING_ACA_WAIT_TIMEOUT_SECONDS}s "
            f"(execution={execution_name}).",
        )
    except Exception as exc:
        logger.exception("Onboarding ACA job execution failed")
        return False, f"ACA onboarding job invocation failed: {type(exc).__name__}: {exc}"


# ── Docker-native onboarding (official Airbyte images) ────────────────


def _get_docker_job_execution(client: Any, execution_name: str) -> Any:
    """Poll execution status on the Docker connector ACA Job."""
    job_exec_fn = getattr(client, "job_execution", None)
    if callable(job_exec_fn):
        return job_exec_fn(
            resource_group_name=ONBOARDING_ACA_RESOURCE_GROUP,
            job_name=ONBOARDING_ACA_DOCKER_JOB_NAME,
            job_execution_name=execution_name,
        )
    for attr in ("job_executions", "jobs_executions"):
        ops = getattr(client, attr, None)
        getter = getattr(ops, "get", None) if ops else None
        if callable(getter):
            return getter(
                resource_group_name=ONBOARDING_ACA_RESOURCE_GROUP,
                job_name=ONBOARDING_ACA_DOCKER_JOB_NAME,
                job_execution_name=execution_name,
            )
    raise RuntimeError("No supported ACA job execution getter found")


def run_onboarding_docker_native_job(
    *,
    action: str,
    docker_image: str,
    config: dict[str, Any],
    streams: list[str] | None = None,
    max_streams: int | None = None,
    read_timeout: int | None = None,
) -> tuple[bool, str]:
    """Run onboarding check/discover/read using the official Airbyte Docker image.

    Instead of PyAirbyte (which can't install Java connectors and is slow for
    Python connectors), this launches the official connector image on the
    Docker-native ACA Job and reads the Airbyte-protocol output from the
    shared Azure File Share.

    Supported actions: ``check``, ``discover``, ``discover_catalog``, ``read_probe``.
    For ``read_probe``, a minimal configured catalog is built from the provided
    stream names (the onboarding flow always discovers streams before the test sync).
    """
    if not ONBOARDING_ACA_DOCKER_JOB_NAME:
        return (
            False,
            "ONBOARDING_ACA_DOCKER_JOB_NAME (or ACA_DOCKER_JOB_NAME) is not set.",
        )

    valid_actions = ("check", "discover", "discover_catalog", "read_probe")
    if action not in valid_actions:
        return False, f"Unsupported Docker-native onboarding action: {action}"

    if action == "read_probe" and not streams:
        return (
            False,
            "Docker-native read_probe requires stream names. "
            "Run discover first so the user can select streams.",
        )

    try:
        from azure.mgmt.appcontainers import ContainerAppsAPIClient
    except Exception:
        return False, "azure-mgmt-appcontainers is required."

    # Map action → Airbyte CLI command.
    # NOTE: `read` is a shell built-in; use `env read` to force PATH lookup
    # so /airbyte/bin/read (the connector binary) is executed instead.
    if action == "read_probe":
        cli_command = "env read"
    elif action in ("discover", "discover_catalog"):
        cli_command = "discover"
    else:
        cli_command = "check"

    # Unique work dir on the shared File Share
    job_id = uuid.uuid4().hex[:12]
    work_dir = f"onboarding-{job_id}"

    # Clean config (strip internal __ keys)
    clean_config = {k: v for k, v in config.items() if not str(k).startswith("__")}

    try:
        credential = DefaultAzureCredential()
        client = ContainerAppsAPIClient(credential, ONBOARDING_ACA_SUBSCRIPTION_ID)

        config_json_escaped = json.dumps(json.dumps(clean_config, default=str))

        if action == "read_probe":
            # Build a minimal ConfiguredAirbyteCatalog from the known stream names.
            # By this point in the onboarding flow the user has already discovered
            # streams, so we have the names. An empty json_schema is accepted by
            # most connectors — the schema is only used for output validation.
            effective_max = max_streams or 3
            selected_streams = (streams or [])[:effective_max]
            configured_catalog = {
                "streams": [
                    {
                        "stream": {
                            "name": s,
                            "json_schema": {},
                            "supported_sync_modes": ["full_refresh"],
                        },
                        "sync_mode": "full_refresh",
                        "destination_sync_mode": "overwrite",
                    }
                    for s in selected_streams
                ]
            }
            catalog_json_escaped = json.dumps(
                json.dumps(configured_catalog, default=str)
            )
            shell_script = (
                f"mkdir -p /data/{work_dir} && "
                f"echo {config_json_escaped} > /data/{work_dir}/config.json && "
                f"echo {catalog_json_escaped} > /data/{work_dir}/catalog.json && "
                f"{cli_command} --config /data/{work_dir}/config.json "
                f"--catalog /data/{work_dir}/catalog.json "
                f"> /data/{work_dir}/output.jsonl 2>/data/{work_dir}/stderr.log ; "
                f"echo $? > /data/{work_dir}/exit_code"
            )
        else:
            # check / discover — only config.json needed
            shell_script = (
                f"mkdir -p /data/{work_dir} && "
                f"echo {config_json_escaped} > /data/{work_dir}/config.json && "
                f"{cli_command} --config /data/{work_dir}/config.json "
                f"> /data/{work_dir}/output.jsonl 2>/data/{work_dir}/stderr.log ; "
                f"echo $? > /data/{work_dir}/exit_code"
            )

        container_override = {
            "name": ONBOARDING_ACA_DOCKER_JOB_CONTAINER_NAME,
            "image": docker_image,
            "command": ["/bin/sh"],
            "args": ["-c", shell_script],
            "env": [
                {"name": "AIRBYTE_ENABLE_UNSAFE_CODE", "value": "true"},
            ],
        }

        logger.info(
            "Starting Docker-native onboarding job '%s' (image=%s, action=%s, work_dir=%s)",
            ONBOARDING_ACA_DOCKER_JOB_NAME,
            docker_image,
            action,
            work_dir,
        )

        result = client.jobs.begin_start(
            resource_group_name=ONBOARDING_ACA_RESOURCE_GROUP,
            job_name=ONBOARDING_ACA_DOCKER_JOB_NAME,
            template={"containers": [container_override]},
        ).result()

        execution_name = _extract_execution_name(result)
        if not execution_name:
            return False, "Docker-native ACA job start returned no execution name."

        # Poll until terminal status
        deadline = time.time() + max(10, ONBOARDING_ACA_WAIT_TIMEOUT_SECONDS)
        terminal_success = {"succeeded", "completed", "success"}
        terminal_failed = {"failed", "canceled", "cancelled", "stopped", "error"}

        while time.time() < deadline:
            execution = _get_docker_job_execution(client, execution_name)
            status = _extract_execution_status(execution).strip().lower()
            if status in terminal_success:
                return (
                    True,
                    f"Docker-native onboarding {action} succeeded ({execution_name}).",
                )
            if status in terminal_failed:
                return (
                    False,
                    f"Docker-native onboarding {action} failed with status '{status}' ({execution_name}).",
                )
            time.sleep(max(1, ONBOARDING_ACA_POLL_INTERVAL_SECONDS))

        return (
            False,
            f"Docker-native onboarding job timed out after {ONBOARDING_ACA_WAIT_TIMEOUT_SECONDS}s "
            f"(execution={execution_name}).",
        )

    except Exception as exc:
        logger.exception("Docker-native onboarding job failed")
        return False, f"Docker-native onboarding failed: {type(exc).__name__}: {exc}"
