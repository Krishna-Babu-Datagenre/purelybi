"""
Fetch the Airbyte OSS connector registry and upsert rows into Supabase
``connector_schemas``.

Used by:
  - ``schema_updater`` Azure Function (timer)
  - ``prototypes/data-onboarding/scripts/run_connector_registry_sync.py`` (one-off local run)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests
from supabase import Client, create_client

logger = logging.getLogger(__name__)

AIRBYTE_REGISTRY_URL = (
    "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
)


def get_supabase_from_env() -> Client:
    """Create a Supabase client using ``SUPABASE_SERVICE_ROLE_KEY``."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in the environment "
            "(e.g. backend/.env)."
        )
    return create_client(url, key)


def fetch_registry() -> list[dict[str, Any]]:
    """Download the Airbyte OSS registry and return source connectors."""
    logger.info("Fetching Airbyte OSS connector registry from %s", AIRBYTE_REGISTRY_URL)
    resp = requests.get(AIRBYTE_REGISTRY_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    sources = data.get("sources", [])
    logger.info("Registry contains %d source connectors", len(sources))
    return sources


def filter_sources(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only official airbyte/source-* connectors."""
    filtered = [
        s
        for s in sources
        if (s.get("dockerRepository") or "").startswith("airbyte/source-")
    ]
    logger.info("Filtered to %d airbyte/source-* connectors", len(filtered))
    return filtered


def upsert_to_supabase(supabase: Client, sources: list[dict[str, Any]]) -> dict[str, int]:
    """Upsert connector metadata into ``connector_schemas``.

    Returns counts: upserted, failed, skipped.
    """
    stats = {"upserted": 0, "failed": 0, "skipped": 0}

    for src in sources:
        docker_repo = src.get("dockerRepository", "")
        name = src.get("name", docker_repo)
        tag = src.get("dockerImageTag", "latest")
        icon_url = src.get("iconUrl", "")
        doc_url = src.get("documentationUrl", "")

        spec = src.get("spec") or {}
        config_schema = spec.get("connectionSpecification")
        oauth_config = spec.get("advanced_auth")

        if not docker_repo:
            stats["skipped"] += 1
            continue

        row = {
            "docker_repository": docker_repo,
            "name": name,
            "docker_image_tag": tag,
            "language": src.get("language") or "unknown",
            "icon_url": icon_url or None,
            "documentation_url": doc_url or None,
            "config_schema": config_schema,
            "oauth_config": oauth_config,
            "is_active": True,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            supabase.table("connector_schemas").upsert(
                row, on_conflict="docker_repository"
            ).execute()
            stats["upserted"] += 1
        except Exception:
            logger.exception("Failed to upsert %s", docker_repo)
            stats["failed"] += 1

    return stats


def run_sync() -> dict[str, int]:
    """Fetch registry, filter sources, upsert to Supabase. Raises on HTTP errors."""
    sources = fetch_registry()
    sources = filter_sources(sources)
    supabase = get_supabase_from_env()
    stats = upsert_to_supabase(supabase, sources)
    logger.info(
        "Schema update complete: %d upserted, %d failed, %d skipped",
        stats["upserted"],
        stats["failed"],
        stats["skipped"],
    )
    return stats
