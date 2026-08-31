"""Start an ACA Job execution that runs the DE pipeline runner image.

Called from the DE router's manual-trigger path and, internally, by the
sync orchestrator post-upload hook. Returns the ACA execution name on
success, or None when the job is not configured.

The trigger is best-effort — run row remains useful for re-runs even when
the ACA call fails.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient

from fastapi_app.settings import (
    ACA_RESOURCE_GROUP_V2,
    ACA_SUBSCRIPTION_ID_V2,
    AZURE_STORAGE_CONTAINER,
    DE_PIPELINE_ACA_CONTAINER_NAME,
    DE_PIPELINE_ACA_JOB_NAME,
    DE_PIPELINE_IMAGE,
    DE_TRANSFORMED_CONTAINER,
    DE_TRANSFORMED_PREFIX,
)

logger = logging.getLogger(__name__)

# Env vars forwarded into the DE runner container.
_FORWARD_ENV = (
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "AZURE_STORAGE_CONNECTION_STRING",
    "BLOB_CONTAINER_NAME",
    "AZURE_STORAGE_CONTAINER",
    "USER_DATA_BLOB_PREFIX",
    "DUCKDB_MEMORY_LIMIT",
)


def _forwarded_env(
    *,
    user_id: str,
    connector_config_id: str,
    pipeline_id: str,
    run_id: str,
    sync_work_id: str,
    docker_image: str,
    raw_blob_prefix: str = "",
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = [
        {"name": "USER_ID", "value": user_id},
        {"name": "CONNECTOR_CONFIG_ID", "value": connector_config_id},
        {"name": "DE_PIPELINE_ID", "value": pipeline_id},
        {"name": "DE_RUN_ID", "value": run_id},
        {"name": "SYNC_WORK_ID", "value": sync_work_id},
        {"name": "CONNECTOR_DOCKER_IMAGE", "value": docker_image},
        {"name": "RAW_CONTAINER_NAME", "value": AZURE_STORAGE_CONTAINER},
        {"name": "TRANSFORMED_CONTAINER_NAME", "value": DE_TRANSFORMED_CONTAINER},
        {"name": "TRANSFORMED_PREFIX", "value": DE_TRANSFORMED_PREFIX},
    ]
    clean_raw_prefix = str(raw_blob_prefix or "").strip().strip("/")
    if clean_raw_prefix:
        out.append({"name": "RAW_BLOB_PREFIX", "value": clean_raw_prefix})
    for key in _FORWARD_ENV:
        val = os.environ.get(key)
        if val is not None:
            out.append({"name": key, "value": val})
    return out


def _config_or_raise() -> tuple[str, str, str, str]:
    missing = [
        name
        for name, val in (
            ("ACA_SUBSCRIPTION_ID", ACA_SUBSCRIPTION_ID_V2),
            ("ACA_RESOURCE_GROUP", ACA_RESOURCE_GROUP_V2),
            ("DE_PIPELINE_ACA_JOB_NAME", DE_PIPELINE_ACA_JOB_NAME),
            ("DE_PIPELINE_IMAGE", DE_PIPELINE_IMAGE),
        )
        if not val
    ]
    if missing:
        raise RuntimeError(
            "Cannot start DE pipeline runner — missing settings: "
            + ", ".join(missing)
        )
    return (
        ACA_SUBSCRIPTION_ID_V2,
        ACA_RESOURCE_GROUP_V2,
        DE_PIPELINE_ACA_JOB_NAME,
        DE_PIPELINE_IMAGE,
    )


def start_run(
    *,
    user_id: str,
    connector_config_id: str,
    pipeline_id: str,
    run_id: str,
    sync_work_id: str = "",
    docker_image: str = "",
    raw_blob_prefix: str = "",
) -> str | None:
    """Launch the DE runner container and return the ACA execution name.

    Returns None when the job is not configured (settings missing). Raises
    on actual ACA API failures so the caller can mark the run accordingly.
    """
    try:
        sub_id, rg, job_name, image = _config_or_raise()
    except RuntimeError as exc:
        logger.warning("DE pipeline runner not configured: %s", exc)
        return None

    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, sub_id)

    env_vars = _forwarded_env(
        user_id=user_id,
        connector_config_id=connector_config_id,
        pipeline_id=pipeline_id,
        run_id=run_id,
        sync_work_id=sync_work_id,
        docker_image=docker_image,
        raw_blob_prefix=raw_blob_prefix,
    )

    container_override: dict[str, Any] = {
        "name": DE_PIPELINE_ACA_CONTAINER_NAME,
        "image": image,
        "env": env_vars,
    }

    logger.info(
        "de_trigger_started: job=%s image=%s user=%s pipeline=%s run=%s",
        job_name,
        image,
        user_id,
        pipeline_id,
        run_id,
    )

    result = client.jobs.begin_start(
        resource_group_name=rg,
        job_name=job_name,
        template={"containers": [container_override]},
    ).result()

    return str(getattr(result, "name", "") or "")
