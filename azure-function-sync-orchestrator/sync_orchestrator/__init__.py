"""Sync Orchestrator — Azure Function (Timer Trigger, every 5 minutes).

Checks each user's connector configs in Supabase. For any that are due
for a sync (based on last_sync_at and sync_frequency_minutes), it queues
an Azure Container Apps job to run the actual data extraction.

Two execution paths depending on the connector language
(stored in ``connector_schemas.language``):

  Python / manifest-only connectors
    → Single-phase: run the ``sync-worker`` image, execute via PyAirbyte.
  Java connectors (e.g. source-mongodb-v2)
    → Phase 1: run the **official Airbyte Docker image** directly as an ACA job.
               The entrypoint runs ``read --config --catalog`` and dumps
               Airbyte protocol JSONL to an Azure File Share (``/output/``).
    → Phase 2: run ``sync-worker`` in ``DOCKER_PHASE2`` mode to parse
               the JSONL output, convert to Parquet, and upload to Blob.

Flow:
  Timer fires → Query eligible configs → Detect language → Start ACA job → Update status
"""

import logging
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from supabase import Client, create_client

logger = logging.getLogger(__name__)

# ── Environment variables (set in Azure Function App Settings) ────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
AZURE_SUBSCRIPTION_ID = os.environ["AZURE_SUBSCRIPTION_ID"]
AZURE_RESOURCE_GROUP = os.environ["AZURE_RESOURCE_GROUP"]
ACA_JOB_NAME = os.environ[
    "ACA_JOB_NAME"
]  # Name of the Container Apps Job resource

# Must match the container *name* in the Job template (portal often uses the job name).
# If this does not match, begin_start env overrides are ignored and SYNC_* vars are missing.
ACA_JOB_CONTAINER_NAME = os.environ.get("ACA_JOB_CONTAINER_NAME", "sync-worker")

# Separate ACA Job for running official Airbyte Docker images (Java connectors).
# Template must allow image override and mount the shared Azure File Share at /output.
ACA_DOCKER_JOB_NAME = os.environ.get("ACA_DOCKER_JOB_NAME", "")
ACA_DOCKER_JOB_CONTAINER_NAME = os.environ.get(
    "ACA_DOCKER_JOB_CONTAINER_NAME", "connector"
)

# Languages that use the official Docker image instead of PyAirbyte pip install.
# - java: no PyPI package exists, Docker image is the only option
# - python: PyPI package exists but install takes 30-90s per job; Docker image is instant
# manifest-only connectors are excluded: they just download a YAML file (~1-2s).
DOCKER_IMAGE_LANGUAGES = {"java", "python"}


def get_supabase() -> Client:
    """Create a Supabase client using the service-role key (bypasses RLS)."""
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def resolve_job_container(
    client: ContainerAppsAPIClient,
) -> tuple[str, str | None, list[dict]]:
    """Resolve the actual job container name/image/env from Azure when possible.

    This avoids mismatches between portal-generated names and local assumptions.
    """
    try:
        job = client.jobs.get(
            resource_group_name=AZURE_RESOURCE_GROUP,
            job_name=ACA_JOB_NAME,
        )
        containers = ((job.template or {}).get("containers") if isinstance(job, dict) else None)
        if not containers:
            # SDK model path
            containers = getattr(getattr(job, "template", None), "containers", None)
        if containers:
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
            if name:
                raw_env = (
                    first.get("env")
                    if isinstance(first, dict)
                    else getattr(first, "env", None)
                ) or []
                env_list: list[dict] = []
                for item in raw_env:
                    env_name = (
                        item.get("name")
                        if isinstance(item, dict)
                        else getattr(item, "name", None)
                    )
                    if not env_name:
                        continue
                    env_entry: dict = {"name": str(env_name)}
                    if isinstance(item, dict):
                        if "value" in item and item.get("value") is not None:
                            env_entry["value"] = item["value"]
                        if (
                            "secretRef" in item
                            and item.get("secretRef") is not None
                        ):
                            env_entry["secretRef"] = item["secretRef"]
                    else:
                        item_value = getattr(item, "value", None)
                        item_secret_ref = getattr(item, "secret_ref", None)
                        if item_value is not None:
                            env_entry["value"] = item_value
                        if item_secret_ref is not None:
                            env_entry["secretRef"] = item_secret_ref
                    env_list.append(env_entry)
                return str(name), (str(image) if image else None), env_list
    except Exception:
        logger.warning(
            "Could not auto-resolve ACA job container name; using ACA_JOB_CONTAINER_NAME=%s",
            ACA_JOB_CONTAINER_NAME,
        )
    return ACA_JOB_CONTAINER_NAME, None, []


def get_eligible_configs(supabase: Client) -> list[dict]:
    """Query user_connector_configs for integrations that are due for sync.

    An integration is eligible when:
      1. is_active = TRUE
      2. sync_validated = TRUE (passed a test sync during onboarding)
      3. last_sync_status is NOT 'queued'/'running' (avoid double-runs)
      4. last_sync_status is NOT 'reauth_required' (needs human intervention)
      5. Enough time has passed since last_sync_at based on sync_frequency_minutes
    """
    now = datetime.now(timezone.utc)

    # Fetch all active, validated configs that aren't currently running
    response = (
        supabase.table("user_connector_configs")
        .select("*")
        .eq("is_active", True)
        .eq("sync_validated", True)
        .neq("last_sync_status", "queued")
        .neq("last_sync_status", "running")
        .neq("last_sync_status", "reauth_required")
        .execute()
    )

    # Build lookup: docker_repository → language from connector_schemas
    repos = list({c["docker_repository"] for c in response.data if c.get("docker_repository")})
    language_map: dict[str, str] = {}
    if repos:
        schema_rows = (
            supabase.table("connector_schemas")
            .select("docker_repository, language")
            .in_("docker_repository", repos)
            .execute()
        ).data or []
        language_map = {r["docker_repository"]: r.get("language", "unknown") for r in schema_rows}

    eligible = []
    for config in response.data:
        freq_minutes = config.get("sync_frequency_minutes", 360)
        last_sync = config.get("last_sync_at")

        # Attach language for routing decisions downstream
        config["_language"] = language_map.get(config.get("docker_repository", ""), "unknown")

        if last_sync is None:
            # Never synced — always eligible
            eligible.append(config)
            continue

        # Parse the timestamp and check if enough time has elapsed
        last_sync_dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
        elapsed_minutes = (now - last_sync_dt).total_seconds() / 60

        if elapsed_minutes >= freq_minutes:
            eligible.append(config)

    logger.info(
        "Found %d eligible configs out of %d total active",
        len(eligible),
        len(response.data),
    )
    return eligible


def start_container_job(
    config: dict,
    credential: DefaultAzureCredential,
) -> str | None:
    """Start an Azure Container Apps job execution for a single sync.

    Routes based on connector language:
      - ``java`` → Docker-native two-phase via ``ACA_DOCKER_JOB_NAME``
      - everything else → existing ``sync-worker`` via ``ACA_JOB_NAME``

    Returns the execution name if started, None on failure.
    """
    language = config.get("_language", "unknown")

    if language in DOCKER_IMAGE_LANGUAGES:
        return _start_docker_native_job(config, credential)
    return _start_pyairbyte_job(config, credential)


def _start_pyairbyte_job(
    config: dict,
    credential: DefaultAzureCredential,
) -> str | None:
    """Start the sync-worker ACA job (PyAirbyte — Python / manifest connectors)."""
    client = ContainerAppsAPIClient(credential, AZURE_SUBSCRIPTION_ID)
    container_name, container_image, base_env = resolve_job_container(client)

    user_id = config["user_id"]
    connector_name = config["connector_name"]
    config_id = config["id"]

    env_by_name = {entry["name"]: entry for entry in base_env if "name" in entry}
    env_by_name["SYNC_CONFIG_ID"] = {"name": "SYNC_CONFIG_ID", "value": config_id}
    env_by_name["SYNC_USER_ID"] = {"name": "SYNC_USER_ID", "value": user_id}
    env_by_name["SYNC_CONNECTOR_NAME"] = {
        "name": "SYNC_CONNECTOR_NAME",
        "value": connector_name,
    }
    env_by_name["SUPABASE_URL"] = {"name": "SUPABASE_URL", "value": SUPABASE_URL}
    env_by_name["SUPABASE_SERVICE_ROLE_KEY"] = {
        "name": "SUPABASE_SERVICE_ROLE_KEY",
        "value": SUPABASE_SERVICE_KEY,
    }
    env_by_name["AIRBYTE_ENABLE_UNSAFE_CODE"] = {
        "name": "AIRBYTE_ENABLE_UNSAFE_CODE",
        "value": "true",
    }

    container_override = {
        "name": container_name,
        "env": list(env_by_name.values()),
    }
    if container_image:
        container_override["image"] = container_image

    job_execution_template = {"containers": [container_override]}

    try:
        logger.info(
            "Starting PyAirbyte ACA job=%s container=%s image=%s SYNC_CONFIG_ID=%s",
            ACA_JOB_NAME,
            container_name,
            container_image or "<unchanged>",
            config_id,
        )
        result = client.jobs.begin_start(
            resource_group_name=AZURE_RESOURCE_GROUP,
            job_name=ACA_JOB_NAME,
            template=job_execution_template,
        ).result()

        execution_name = getattr(result, "name", "unknown")
        logger.info(
            "Started PyAirbyte job for user=%s connector=%s execution=%s",
            user_id,
            connector_name,
            execution_name,
        )
        return execution_name
    except Exception:
        logger.exception(
            "Failed to start PyAirbyte job for user=%s connector=%s",
            user_id,
            connector_name,
        )
        return None


def _start_docker_native_job(
    config: dict,
    credential: DefaultAzureCredential,
) -> str | None:
    """Start a two-phase ACA job for Docker-only (Java/Python) connectors.

    This runs the **sync-worker** image on the standard sync ACA job
    (``ACA_JOB_NAME``) with ``SYNC_PHASE=docker_read``.  The sync-worker then
    internally launches the official Airbyte connector image on the Docker
    connector ACA job (``ACA_DOCKER_JOB_NAME``) and orchestrates the whole
    discover → read → Parquet → Blob pipeline.
    """
    if not ACA_DOCKER_JOB_NAME:
        logger.error(
            "ACA_DOCKER_JOB_NAME is not set — cannot run Docker-native job for %s",
            config.get("docker_image"),
        )
        return None

    # We run the sync-worker on the standard sync job (same image, same volume
    # mounts, same secrets).  Only SYNC_PHASE + SYNC_DOCKER_IMAGE differentiate.
    client = ContainerAppsAPIClient(credential, AZURE_SUBSCRIPTION_ID)
    container_name, container_image, base_env = resolve_job_container(client)

    user_id = config["user_id"]
    connector_name = config["connector_name"]
    config_id = config["id"]
    docker_image = config["docker_image"]  # e.g. "airbyte/source-mongodb-v2:6.6.4"

    env_by_name = {entry["name"]: entry for entry in base_env if "name" in entry}
    env_by_name["SYNC_CONFIG_ID"] = {"name": "SYNC_CONFIG_ID", "value": config_id}
    env_by_name["SYNC_USER_ID"] = {"name": "SYNC_USER_ID", "value": user_id}
    env_by_name["SYNC_CONNECTOR_NAME"] = {
        "name": "SYNC_CONNECTOR_NAME",
        "value": connector_name,
    }
    env_by_name["SYNC_PHASE"] = {"name": "SYNC_PHASE", "value": "docker_read"}
    env_by_name["SYNC_DOCKER_IMAGE"] = {"name": "SYNC_DOCKER_IMAGE", "value": docker_image}
    env_by_name["SUPABASE_URL"] = {"name": "SUPABASE_URL", "value": SUPABASE_URL}
    env_by_name["SUPABASE_SERVICE_ROLE_KEY"] = {
        "name": "SUPABASE_SERVICE_ROLE_KEY",
        "value": SUPABASE_SERVICE_KEY,
    }
    env_by_name["AIRBYTE_ENABLE_UNSAFE_CODE"] = {
        "name": "AIRBYTE_ENABLE_UNSAFE_CODE",
        "value": "true",
    }

    container_override = {
        "name": container_name,
        "env": list(env_by_name.values()),
    }
    if container_image:
        container_override["image"] = container_image

    try:
        logger.info(
            "Starting Docker-native sync: job=%s container=%s image=%s "
            "SYNC_PHASE=docker_read connector_image=%s config_id=%s",
            ACA_JOB_NAME,
            container_name,
            container_image or "<unchanged>",
            docker_image,
            config_id,
        )
        result = client.jobs.begin_start(
            resource_group_name=AZURE_RESOURCE_GROUP,
            job_name=ACA_JOB_NAME,
            template={"containers": [container_override]},
        ).result()

        execution_name = getattr(result, "name", "unknown")
        logger.info(
            "Started Docker-native job for user=%s connector=%s execution=%s",
            user_id,
            connector_name,
            execution_name,
        )
        return execution_name
    except Exception:
        logger.exception(
            "Failed to start Docker-native job for user=%s connector=%s",
            user_id,
            connector_name,
        )
        return None


def mark_queued(supabase: Client, config_id: str) -> None:
    """Mark a config as queued once ACA accepts the start request."""
    supabase.table("user_connector_configs").update(
        {"last_sync_status": "queued", "last_sync_error": None}
    ).eq("id", config_id).execute()


def main(timer: func.TimerRequest) -> None:
    """Entry point — triggered every 5 minutes by Azure Functions timer."""
    if timer.past_due:
        logger.warning("Timer trigger is past due — running catch-up")

    supabase = get_supabase()
    eligible = get_eligible_configs(supabase)

    if not eligible:
        logger.info("No eligible syncs at this time")
        return

    credential = DefaultAzureCredential()
    started = 0
    failed = 0

    for config in eligible:
        config_id = config["id"]

        execution_name = start_container_job(config, credential)
        if execution_name:
            mark_queued(supabase, config_id)
            started += 1
        else:
            # Revert status on failure so it's retried next cycle
            supabase.table("user_connector_configs").update(
                {
                    "last_sync_status": "failed",
                    "last_sync_error": "Failed to start Azure Container Apps job",
                }
            ).eq("id", config_id).execute()
            failed += 1

    logger.info(
        "Orchestrator complete: %d started, %d failed out of %d eligible",
        started,
        failed,
        len(eligible),
    )
