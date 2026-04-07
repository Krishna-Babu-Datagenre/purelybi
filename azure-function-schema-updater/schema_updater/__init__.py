"""Schema Updater — Azure Function (Timer Trigger, daily at 3:00 AM UTC).

Downloads the latest Airbyte OSS connector registry and upserts all
source connector schemas into the Supabase ``connector_schemas`` table.

Implementation lives in ``shared.connector_registry_sync`` so the same logic
can be run once locally (see ``prototypes/data-onboarding/scripts/run_connector_registry_sync.py``).
"""

from __future__ import annotations

import logging

import azure.functions as func

from shared.connector_registry_sync import run_sync

logger = logging.getLogger(__name__)


def main(timer: func.TimerRequest) -> None:
    """Entry point — triggered daily at 3:00 AM UTC."""
    if timer.past_due:
        logger.warning("Timer trigger is past due — running catch-up")

    try:
        run_sync()
    except Exception:
        logger.exception("Schema sync failed")
