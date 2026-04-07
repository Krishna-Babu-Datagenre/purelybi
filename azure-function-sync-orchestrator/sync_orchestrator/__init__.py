"""Sync Orchestrator — Azure Function (Timer Trigger, every 5 minutes).

Checks each user's connector configs in Supabase. For any that are due
for a sync (based on last_sync_at and sync_frequency_minutes), it queues
an Azure Container Apps job to run the actual data extraction.

Flow:
  Timer fires → Query eligible configs → Start ACA job per config → Update status
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


def get_supabase() -> Client:
    """Create a Supabase client using the service-role key (bypasses RLS)."""
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_eligible_configs(supabase: Client) -> list[dict]:
    """Query user_connector_configs for integrations that are due for sync.

    An integration is eligible when:
      1. is_active = TRUE
      2. sync_validated = TRUE (passed a test sync during onboarding)
      3. last_sync_status is NOT 'running' (avoid double-runs)
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
        .neq("last_sync_status", "running")
        .neq("last_sync_status", "reauth_required")
        .execute()
    )

    eligible = []
    for config in response.data:
        freq_minutes = config.get("sync_frequency_minutes", 360)
        last_sync = config.get("last_sync_at")

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

    The ACA job runs the sync_worker Docker image with the user's config.
    Environment variables pass the Supabase credentials so the worker can
    read/write configs and upload data.

    Returns the execution name if started, None on failure.
    """
    client = ContainerAppsAPIClient(credential, AZURE_SUBSCRIPTION_ID)

    user_id = config["user_id"]
    connector_name = config["connector_name"]
    config_id = config["id"]

    # The job execution template overrides env vars for this specific run
    job_execution = {
        "template": {
            "containers": [
                {
                    "name": "sync-worker",
                    "env": [
                        {"name": "SYNC_CONFIG_ID", "value": config_id},
                        {"name": "SYNC_USER_ID", "value": user_id},
                        {
                            "name": "SYNC_CONNECTOR_NAME",
                            "value": connector_name,
                        },
                        {"name": "SUPABASE_URL", "value": SUPABASE_URL},
                        {
                            "name": "SUPABASE_SERVICE_ROLE_KEY",
                            "value": SUPABASE_SERVICE_KEY,
                        },
                        {
                            "name": "AIRBYTE_ENABLE_UNSAFE_CODE",
                            "value": "true",
                        },
                    ],
                }
            ]
        }
    }

    try:
        result = client.jobs.begin_start(
            resource_group_name=AZURE_RESOURCE_GROUP,
            job_name=ACA_JOB_NAME,
            template=job_execution,
        ).result()

        execution_name = getattr(result, "name", "unknown")
        logger.info(
            "Started ACA job for user=%s connector=%s execution=%s",
            user_id,
            connector_name,
            execution_name,
        )
        return execution_name
    except Exception:
        logger.exception(
            "Failed to start ACA job for user=%s connector=%s",
            user_id,
            connector_name,
        )
        return None


def mark_running(supabase: Client, config_id: str) -> None:
    """Mark a config as 'running' to prevent duplicate job starts."""
    supabase.table("user_connector_configs").update(
        {"last_sync_status": "running"}
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

        # Mark as running BEFORE starting the job to prevent race conditions
        mark_running(supabase, config_id)

        execution_name = start_container_job(config, credential)
        if execution_name:
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
