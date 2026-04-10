"""Production Sync Worker — Azure Container Apps Job.

Reads the connector config from Supabase (not DuckDB), refreshes
credentials if needed, runs PyAirbyte, writes Parquet to Azure Blob
Storage, and updates the sync status in Supabase.

Environment variables (set by the sync_orchestrator Azure Function):
    SYNC_CONFIG_ID            — UUID of the user_connector_configs row
    SYNC_USER_ID              — UUID of the user
    SYNC_CONNECTOR_NAME       — Display name of the connector

Onboarding probe (set by FastAPI when ``ONBOARDING_DOCKER_EXECUTION_MODE=azure_job``):
    ONBOARDING_JOB_MODE           — ``onboarding_connector_probe`` to run connector check/discover/read via PyAirbyte
    ONBOARDING_JOB_PAYLOAD_JSON   — JSON with action, docker_image, config, optional streams / max_streams / read_timeout

Shared (job template secrets):
    SUPABASE_URL              — Supabase project URL
    SUPABASE_SERVICE_ROLE_KEY — Service role key (bypasses RLS)
    AZURE_STORAGE_CONNECTION_STRING — Blob Storage connection string
    BLOB_CONTAINER_NAME       — Container name for Parquet output (default: "sync-output")
"""

import json
import os
import sys
import tempfile
from io import BytesIO
from datetime import datetime, timezone
from pathlib import Path

import airbyte as ab
import pandas as pd
from supabase import create_client

from credential_refresh import (
    ReauthRequired,
    TokenRefreshError,
    ensure_fresh_credentials,
)

# ── Supabase client ──────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Config loading ────────────────────────────────────────────────────


def load_config(config_id: str) -> dict:
    """Load a single connector config row from Supabase."""
    supabase = get_supabase()
    resp = (
        supabase.table("user_connector_configs")
        .select("*")
        .eq("id", config_id)
        .single()
        .execute()
    )
    return resp.data


def update_status(config_id: str, **fields) -> None:
    """Update sync status fields on the config row."""
    supabase = get_supabase()
    supabase.table("user_connector_configs").update(fields).eq(
        "id", config_id
    ).execute()


def save_refreshed_config(
    config_id: str, config: dict, oauth_meta: dict
) -> None:
    """Persist refreshed tokens back to Supabase."""
    supabase = get_supabase()
    supabase.table("user_connector_configs").update(
        {"config": config, "oauth_meta": oauth_meta}
    ).eq("id", config_id).execute()


# ── Blob upload ───────────────────────────────────────────────────────


def upload_to_blob(
    local_dir: Path, user_id: str, docker_image: str
) -> list[str]:
    """Upload all Parquet files in local_dir to Azure Blob Storage.

    Returns list of uploaded blob paths.
    """
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.environ.get("BLOB_CONTAINER_NAME", "sync-output")

    if not conn_str:
        print(
            "WARNING: No AZURE_STORAGE_CONNECTION_STRING set, skipping upload"
        )
        return []

    from azure.storage.blob import BlobServiceClient

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = blob_service.get_container_client(container_name)

    month_prefix = datetime.now(timezone.utc).strftime("%Y-%m")
    uploaded = []
    source_name = extract_source_name(docker_image)

    for parquet_file in local_dir.rglob("*.parquet"):
        # Stream name is the file stem (e.g. "orders.parquet" → "orders")
        stream_name = parquet_file.stem
        # Build: raw/user-data/{user_id}/{connector_name}/{stream_name}/{YYYY-MM}.parquet
        blob_path = (
            f"user-data/{user_id}/{source_name}/{stream_name}/{month_prefix}.parquet"
        )

        new_df = pd.read_parquet(parquet_file)
        blob_client = container.get_blob_client(blob_path)
        if blob_client.exists():
            existing_bytes = blob_client.download_blob().readall()
            existing_df = pd.read_parquet(BytesIO(existing_bytes))
            merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            merged_df = new_df

        out = BytesIO()
        merged_df.to_parquet(out, index=False)
        out.seek(0)
        container.upload_blob(blob_path, out, overwrite=True)
        uploaded.append(blob_path)
        print(f"  Uploaded: {blob_path}")

    return uploaded


# ── Clean config ──────────────────────────────────────────────────────


def clean_config(config: dict) -> dict:
    """Remove internal __ keys from config before passing to PyAirbyte."""
    return {k: v for k, v in config.items() if not k.startswith("__")}


def extract_source_name(docker_image: str) -> str:
    """Extract source name: 'airbyte/source-shopify:3.2.3' -> 'source-shopify'."""
    repo = docker_image.split(":")[0]
    return repo.split("/")[-1]


# ── Main sync pipeline ───────────────────────────────────────────────


def run_sync(config_id: str) -> None:
    """Execute the full sync pipeline for a single user connector config."""

    # Mark running only once the worker process actually starts.
    update_status(config_id, last_sync_status="running", last_sync_error=None)

    # 1. Load config from Supabase
    print(f"[1/5] Loading config {config_id} from Supabase...")
    row = load_config(config_id)
    if not row:
        print(f"ERROR: Config {config_id} not found in Supabase")
        sys.exit(1)

    user_id = row["user_id"]
    connector_name = row["connector_name"]
    docker_image = row["docker_image"]
    raw_config = row["config"]
    oauth_meta = row.get("oauth_meta") or raw_config.get("__oauth_meta__", {})
    selected_streams = row.get("selected_streams")
    source_name = extract_source_name(docker_image)

    if isinstance(raw_config, str):
        raw_config = json.loads(raw_config)

    print(f"  User: {user_id}")
    print(f"  Connector: {connector_name} ({source_name})")
    print(f"  Streams: {selected_streams or 'all'}")

    # 2. Refresh credentials if needed
    if oauth_meta:
        print("[2/5] Checking token freshness...")
        try:
            raw_config, was_refreshed = ensure_fresh_credentials(
                raw_config, oauth_meta
            )
            if was_refreshed:
                print("  Credentials refreshed — persisting to Supabase")
                save_refreshed_config(config_id, raw_config, oauth_meta)
            else:
                print("  Token still valid")
        except ReauthRequired as e:
            print(f"  ERROR: {e}")
            update_status(
                config_id,
                last_sync_status="reauth_required",
                last_sync_error=str(e),
            )
            sys.exit(2)
        except TokenRefreshError as e:
            print(f"  WARNING: Token refresh failed: {e}")
            print("  Attempting sync with existing credentials...")
    else:
        print("[2/5] No OAuth metadata — skipping token refresh")

    user_config = clean_config(raw_config)

    # 3. Run PyAirbyte
    print(f"[3/5] Initializing PyAirbyte source: {source_name}...")
    source = ab.get_source(source_name, config=user_config)

    print("  Running connection check...")
    source.check()
    print("  Connection OK!")

    available_streams = source.get_available_streams()
    if selected_streams:
        valid = [s for s in selected_streams if s in available_streams]
        if not valid:
            msg = "None of the selected streams are available"
            update_status(
                config_id, last_sync_status="failed", last_sync_error=msg
            )
            print(f"ERROR: {msg}")
            sys.exit(1)
        source.select_streams(valid)
        print(f"  Selecting {len(valid)} stream(s)")
    else:
        source.select_all_streams()
        print(f"  Selecting all {len(available_streams)} stream(s)")

    print("[4/5] Reading data...")
    cache = ab.get_default_cache()
    result = source.read(cache=cache)

    # 4. Export to local Parquet, then upload to Blob Storage
    print("[5/5] Exporting and uploading...")
    output_dir = Path(tempfile.mkdtemp())
    stream_names = list(result.streams.keys()) if result else []

    rows_total = 0
    for stream_name in stream_names:
        try:
            df = cache.get_pandas_dataframe(stream_name)
        except Exception:
            print(f"  Skipping {stream_name} (not in cache)")
            continue
        if df.empty:
            continue
        parquet_path = output_dir / f"{stream_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        rows_total += len(df)
        print(f"  {stream_name}: {len(df)} rows")

    uploaded = upload_to_blob(output_dir, user_id, docker_image)

    # 5. Update sync status
    now = datetime.now(timezone.utc).isoformat()
    update_status(
        config_id,
        last_sync_at=now,
        last_sync_status="success",
        last_sync_error=None,
    )
    print(
        f"\nSync complete! {len(uploaded)} files uploaded, {rows_total} total rows."
    )


def _mark_failed(config_id: str, message: str) -> None:
    """Best-effort status update when the job fails."""
    try:
        update_status(config_id, last_sync_status="failed", last_sync_error=message[:2000])
    except Exception as exc:
        print(f"WARNING: Could not persist failed status: {exc}")


def _pick_streams_for_probe(
    catalog: dict,
    requested: list | None,
    *,
    max_streams: int,
) -> list[str]:
    """Match backend ``docker_ops._pick_streams_for_probe`` (subset + cap)."""
    all_names: list[str] = []
    for stream_obj in catalog.get("streams", []) or []:
        if not isinstance(stream_obj, dict):
            continue
        name = stream_obj.get("name")
        if name:
            all_names.append(str(name))
    if not all_names:
        return []
    if not requested:
        return all_names[:max_streams]
    by_lower = {n.lower(): n for n in all_names}
    picked: list[str] = []
    for req in requested:
        r = str(req).strip()
        if not r:
            continue
        if r in all_names:
            picked.append(r)
        elif r.lower() in by_lower:
            picked.append(by_lower[r.lower()])
    if not picked:
        return all_names[:max_streams]
    return picked[:max_streams]


def run_onboarding_connector_probe(payload: dict) -> None:
    """Run check/discover/read steps for guided onboarding (PyAirbyte; no Docker CLI).

    Env: ``ONBOARDING_JOB_PAYLOAD_JSON`` from ``azure_job_runner.run_onboarding_aca_job``.
    """
    action = str(payload.get("action") or "check")
    docker_image = str(payload.get("docker_image") or "")
    raw_config = payload.get("config")
    if not isinstance(raw_config, dict):
        print("ERROR: payload.config must be an object")
        sys.exit(1)
    if not docker_image:
        print("ERROR: payload.docker_image is required")
        sys.exit(1)

    streams_req = payload.get("streams")
    if streams_req is not None and not isinstance(streams_req, list):
        streams_req = None

    max_streams = int(payload.get("max_streams") or 3)
    read_timeout = int(payload.get("read_timeout") or 300)

    raw_config = dict(raw_config)
    oauth_meta = raw_config.get("__oauth_meta__") or {}

    if oauth_meta:
        print("[onboarding] Checking OAuth token freshness...")
        try:
            raw_config, _was = ensure_fresh_credentials(raw_config, oauth_meta)
        except ReauthRequired as e:
            print(f"ERROR: {e}")
            sys.exit(2)
        except TokenRefreshError as e:
            print(f"WARNING: Token refresh failed: {e}")

    user_config = clean_config(raw_config)
    source_name = extract_source_name(docker_image)
    print(f"[onboarding] action={action} source={source_name} image={docker_image}")

    source = ab.get_source(source_name, config=user_config)

    if action == "check":
        source.check()
        print("Onboarding connection check: succeeded.")
        return

    if action == "discover":
        source.check()
        names = [str(x) for x in source.get_available_streams()]
        print(f"Onboarding discover: {len(names)} stream(s).")
        return

    if action == "discover_catalog":
        source.check()
        names = [str(x) for x in source.get_available_streams()]
        catalog = {"streams": [{"name": n, "supported_sync_modes": ["full_refresh"]} for n in names]}
        print(f"Onboarding discover_catalog: {len(catalog['streams'])} stream(s).")
        return

    if action == "read_probe":
        source.check()
        available = [str(x) for x in source.get_available_streams()]
        catalog = {
            "streams": [
                {"name": n, "supported_sync_modes": ["full_refresh"]} for n in available
            ]
        }
        picked = _pick_streams_for_probe(
            catalog, streams_req, max_streams=max_streams
        )
        if not picked:
            print("ERROR: No streams available for test read.")
            sys.exit(1)
        source.select_streams(picked)
        print(f"[onboarding] read_probe streams={picked!r} (timeout={read_timeout}s)")
        cache = ab.get_default_cache()
        source.read(cache=cache)
        print("Onboarding read probe: succeeded.")
        return

    print(f"ERROR: Unknown onboarding action: {action}")
    sys.exit(1)


if __name__ == "__main__":
    if os.environ.get("ONBOARDING_JOB_MODE") == "onboarding_connector_probe":
        raw = os.environ.get("ONBOARDING_JOB_PAYLOAD_JSON")
        if not raw:
            print(
                "ERROR: ONBOARDING_JOB_PAYLOAD_JSON is required when "
                "ONBOARDING_JOB_MODE=onboarding_connector_probe"
            )
            sys.exit(1)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid ONBOARDING_JOB_PAYLOAD_JSON: {e}")
            sys.exit(1)
        try:
            run_onboarding_connector_probe(payload)
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            print(f"ERROR: {err}")
            sys.exit(1)
        sys.exit(0)

    config_id = os.environ.get("SYNC_CONFIG_ID")
    if not config_id:
        print("ERROR: SYNC_CONFIG_ID environment variable is required")
        sys.exit(1)
    try:
        run_sync(config_id)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        print(f"ERROR: {err}")
        _mark_failed(config_id, err)
        sys.exit(1)
