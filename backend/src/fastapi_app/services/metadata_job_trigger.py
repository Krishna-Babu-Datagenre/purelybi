"""Start an ACA Job execution that runs the metadata-generator image.

Wired into ``POST /api/metadata/generate``: that endpoint creates a
``pending`` row in ``tenant_metadata_jobs`` and then calls
:func:`start_job` here to launch the container. The container itself
patches the row with progress updates and final status.

The trigger is intentionally best-effort — the job row remains useful for
manual re-runs even if the ACA call fails.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient

from fastapi_app.services import metadata_service
from fastapi_app.settings import (
    ACA_RESOURCE_GROUP_V2,
    ACA_SUBSCRIPTION_ID_V2,
    METADATA_GENERATOR_ACA_CONTAINER_NAME,
    METADATA_GENERATOR_ACA_JOB_NAME,
    METADATA_GENERATOR_IMAGE,
)

logger = logging.getLogger(__name__)


# Env vars forwarded into the container. Only forward what the container
# needs — keep secrets scoped.
_FORWARD_ENV = (
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "AZURE_STORAGE_CONNECTION_STRING",
    "BLOB_CONTAINER_NAME",
    "AZURE_STORAGE_CONTAINER",
    "USER_DATA_BLOB_PREFIX",
    "DUCKDB_MEMORY_LIMIT",
    "METADATA_SAMPLE_ROWS",
    "METADATA_SAMPLE_VALUES",
    "METADATA_CATEGORICAL_MAX",
    "METADATA_RELATIONSHIP_MIN_OVERLAP",
    "METADATA_RELATIONSHIP_MAX_EDGES",
)

# LLM env vars: the metadata generator may want a different model than the
# backend's default (e.g. backend on Claude, generator on Azure OpenAI). If
# ``AZURE_LLM_*_METADATA`` is set it takes precedence; otherwise we fall back
# to the backend's ``AZURE_LLM_*`` vars.
_LLM_ENV_KEYS = (
    "AZURE_LLM_ENDPOINT",
    "AZURE_LLM_API_KEY",
    "AZURE_LLM_API_VERSION",
    "AZURE_LLM_NAME",
)


def _forwarded_env(*, user_id: str, job_id: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = [
        {"name": "USER_ID", "value": user_id},
        {"name": "JOB_ID", "value": job_id},
    ]
    for key in _FORWARD_ENV:
        val = os.environ.get(key)
        if val is not None:
            out.append({"name": key, "value": val})
    for key in _LLM_ENV_KEYS:
        val = os.environ.get(f"{key}_METADATA") or os.environ.get(key)
        if val is not None:
            out.append({"name": key, "value": val})
    return out


def _config_or_raise() -> tuple[str, str, str, str]:
    missing = [
        name
        for name, val in (
            ("ACA_SUBSCRIPTION_ID", ACA_SUBSCRIPTION_ID_V2),
            ("ACA_RESOURCE_GROUP", ACA_RESOURCE_GROUP_V2),
            ("METADATA_GENERATOR_ACA_JOB_NAME", METADATA_GENERATOR_ACA_JOB_NAME),
            ("METADATA_GENERATOR_IMAGE", METADATA_GENERATOR_IMAGE),
        )
        if not val
    ]
    if missing:
        raise RuntimeError(
            "Cannot start metadata generator — missing settings: "
            + ", ".join(missing)
        )
    return (
        ACA_SUBSCRIPTION_ID_V2,
        ACA_RESOURCE_GROUP_V2,
        METADATA_GENERATOR_ACA_JOB_NAME,
        METADATA_GENERATOR_IMAGE,
    )


def start_job(*, user_id: str, job_id: str) -> str | None:
    """Launch the metadata-generator container and record the execution name.

    Returns the ACA execution name on success, or ``None`` when the job is
    not configured (callers can keep the pending row for manual runs).
    """
    try:
        sub_id, rg, job_name, image = _config_or_raise()
    except RuntimeError as exc:
        logger.warning("metadata generator not configured: %s", exc)
        metadata_service.update_job(
            user_id=user_id,
            job_id=job_id,
            message=(
                "Metadata generator container is not configured for this "
                "deployment. The job row was created but no execution started."
            ),
        )
        return None

    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, sub_id)

    container_override: dict[str, Any] = {
        "name": METADATA_GENERATOR_ACA_CONTAINER_NAME,
        "image": image,
        "env": _forwarded_env(user_id=user_id, job_id=job_id),
    }

    logger.info(
        "Starting metadata generator: job=%s image=%s user=%s job_row=%s",
        job_name,
        image,
        user_id,
        job_id,
    )

    try:
        result = client.jobs.begin_start(
            resource_group_name=rg,
            job_name=job_name,
            template={"containers": [container_override]},
        ).result()
    except Exception as exc:
        logger.exception("Failed to start ACA execution for metadata generator")
        metadata_service.update_job(
            user_id=user_id,
            job_id=job_id,
            error=f"Failed to start ACA job: {type(exc).__name__}: {exc}",
        )
        raise

    execution_name = str(getattr(result, "name", "") or "")
    metadata_service.update_job(
        user_id=user_id,
        job_id=job_id,
        aca_execution_name=execution_name,
        message=f"ACA execution started: {execution_name}",
    )
    return execution_name
