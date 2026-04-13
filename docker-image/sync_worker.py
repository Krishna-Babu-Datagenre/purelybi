"""Production Sync Worker — Azure Container Apps Job.

Reads the connector config from Supabase (not DuckDB), refreshes
credentials if needed, runs PyAirbyte, writes Parquet to Azure Blob
Storage, and updates the sync status in Supabase.

Execution modes (determined by environment variables):

  Default (PyAirbyte) — SYNC_CONFIG_ID is set, SYNC_PHASE is absent:
    Uses PyAirbyte to pip-install and run the connector in-process.
    Works for manifest-only and Python connectors.

  Docker-native — SYNC_PHASE=docker_read:
    For Java (Docker-only) connectors:
      1. Loads config from Supabase, writes config.json + catalog.json to /output/<config_id>/
      2. Launches a *second* ACA job using the official Airbyte Docker image
         with the files bind-mounted via the shared Azure File Share
      3. Waits for that job to complete
      4. Parses Airbyte-protocol JSONL from /output/<config_id>/output.jsonl
      5. Converts records to Parquet and uploads to Blob Storage

  Connector runner — SYNC_PHASE=connector_run:
    The official Airbyte image is launched by the Docker-native phase with this
    env var. This mode is *not* handled by sync_worker.py — it's the native
    connector's own entrypoint.

  Onboarding probe — ONBOARDING_JOB_MODE=onboarding_connector_probe:
    Runs connector check/discover/read for guided onboarding.

Environment variables (set by the sync_orchestrator Azure Function):
    SYNC_CONFIG_ID            — UUID of the user_connector_configs row
    SYNC_USER_ID              — UUID of the user
    SYNC_CONNECTOR_NAME       — Display name of the connector
    SYNC_PHASE                — Optional: "docker_read" for Docker-native path
    SYNC_DOCKER_IMAGE         — Official Airbyte image (e.g. airbyte/source-mongodb-v2:6.6.4)

Onboarding probe (set by FastAPI when ``ONBOARDING_DOCKER_EXECUTION_MODE=azure_job``):
    ONBOARDING_JOB_MODE           — ``onboarding_connector_probe`` to run connector check/discover/read via PyAirbyte
    ONBOARDING_JOB_PAYLOAD_JSON   — JSON with action, docker_image, config, optional streams / max_streams / read_timeout

Shared (job template secrets):
    SUPABASE_URL              — Supabase project URL
    SUPABASE_SERVICE_ROLE_KEY — Service role key (bypasses RLS)
    AZURE_STORAGE_CONNECTION_STRING — Blob Storage connection string
    BLOB_CONTAINER_NAME       — Container name for Parquet output (default: "sync-output")

Docker-native additional:
    ACA_DOCKER_JOB_NAME       — ACA Job resource name for running official images
    AZURE_SUBSCRIPTION_ID     — Azure subscription ID (for managing ACA jobs)
    AZURE_RESOURCE_GROUP      — Azure resource group
"""

import json
import os
import sys
import tempfile
import time
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


# ── Docker-native sync pipeline (Java connectors) ────────────────────

# Shared volume for exchanging config/output between Phase 1 (this worker)
# and Phase 2 (official Airbyte connector image).  Azure File Share mounted here.
DOCKER_OUTPUT_BASE = Path(os.environ.get("DOCKER_OUTPUT_DIR", "/output"))


def _parse_discover_output(discover_path: Path) -> dict | None:
    """Parse Airbyte discover JSONL and return the raw catalog dict.

    Looks for a ``{"type": "CATALOG", "catalog": {...}}`` message.
    """
    if not discover_path.exists():
        return None
    with open(discover_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "CATALOG":
                return msg.get("catalog")
    return None


def _launch_connector_aca_job(
    docker_image: str,
    work_dir: Path,
    config_id: str,
    *,
    airbyte_command: str = "read",
    output_file: str = "output.jsonl",
) -> str:
    """Launch the official Airbyte connector image as an ACA job.

    *airbyte_command* is the Airbyte CLI verb (``read``, ``discover``, ``check``).
    For ``read`` the catalog flag is included automatically.

    Returns the execution name.
    """
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.appcontainers import ContainerAppsAPIClient

    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group = os.environ["AZURE_RESOURCE_GROUP"]
    job_name = os.environ["ACA_DOCKER_JOB_NAME"]
    container_name = os.environ.get("ACA_DOCKER_CONNECTOR_CONTAINER_NAME", "connector")

    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, subscription_id)

    rel_dir = work_dir.relative_to(DOCKER_OUTPUT_BASE)

    # Build the shell command using $AIRBYTE_ENTRYPOINT (set in every
    # official Airbyte image to point at the actual binary).
    cmd_parts = [
        f"$AIRBYTE_ENTRYPOINT {airbyte_command}",
        f"--config /data/{rel_dir}/config.json",
    ]
    if airbyte_command == "read":
        cmd_parts.append(f"--catalog /data/{rel_dir}/catalog.json")
    cmd_parts.append(
        f"> /data/{rel_dir}/{output_file} 2>/data/{rel_dir}/stderr.log || true"
    )
    shell_script = " ".join(cmd_parts)

    container_override = {
        "name": container_name,
        "image": docker_image,
        "command": ["/bin/sh"],
        "args": ["-c", shell_script],
        "env": [
            {"name": "AIRBYTE_ENABLE_UNSAFE_CODE", "value": "true"},
        ],
    }

    print(f"  Launching connector ACA job: {job_name} image={docker_image} cmd={airbyte_command}")
    result = client.jobs.begin_start(
        resource_group_name=resource_group,
        job_name=job_name,
        template={"containers": [container_override]},
    ).result()

    execution_name = getattr(result, "name", "unknown")
    print(f"  Connector job execution: {execution_name}")
    return str(execution_name)


def _wait_for_connector_job(execution_name: str, timeout: int = 900) -> bool:
    """Poll the connector ACA job until it reaches a terminal status.

    Returns True on success, False on failure/timeout.
    """
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.appcontainers import ContainerAppsAPIClient

    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group = os.environ["AZURE_RESOURCE_GROUP"]
    job_name = os.environ["ACA_DOCKER_JOB_NAME"]

    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, subscription_id)
    poll_interval = int(os.environ.get("DOCKER_JOB_POLL_INTERVAL", "10"))

    terminal_success = {"succeeded", "completed", "success"}
    terminal_failed = {"failed", "canceled", "cancelled", "stopped", "error"}

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            # SDK 3.x: client.job_execution(...)
            job_exec_fn = getattr(client, "job_execution", None)
            if callable(job_exec_fn):
                execution = job_exec_fn(
                    resource_group_name=resource_group,
                    job_name=job_name,
                    job_execution_name=execution_name,
                )
            else:
                for attr in ("job_executions", "jobs_executions"):
                    ops = getattr(client, attr, None)
                    getter = getattr(ops, "get", None) if ops else None
                    if callable(getter):
                        execution = getter(
                            resource_group_name=resource_group,
                            job_name=job_name,
                            job_execution_name=execution_name,
                        )
                        break
                else:
                    raise RuntimeError("No supported ACA job execution getter found")

            # Extract status
            if isinstance(execution, dict):
                status = (execution.get("properties") or {}).get("status", "")
            else:
                status = str(
                    getattr(getattr(execution, "properties", None), "status", None)
                    or getattr(execution, "status", "")
                    or ""
                )
            status = status.strip().lower()

            if status in terminal_success:
                print(f"  Connector job {execution_name} succeeded.")
                return True
            if status in terminal_failed:
                print(f"  Connector job {execution_name} failed with status: {status}")
                return False

            print(f"  Connector job status: {status} — waiting...")
        except Exception as exc:
            print(f"  WARNING: Error polling connector job: {exc}")

        time.sleep(poll_interval)

    print(f"  Connector job {execution_name} timed out after {timeout}s")
    return False


def _parse_airbyte_jsonl(jsonl_path: Path) -> dict[str, list[dict]]:
    """Parse Airbyte-protocol JSONL and return records grouped by stream name."""
    streams: dict[str, list[dict]] = {}
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "RECORD":
                record = msg.get("record", {})
                stream_name = record.get("stream", "unknown")
                data = record.get("data", {})
                if data:
                    streams.setdefault(stream_name, []).append(data)
    return streams


def run_docker_native_sync(config_id: str) -> None:
    """Execute a full sync for a Docker-only (Java) connector.

    This is the ``SYNC_PHASE=docker_read`` entrypoint.
    """
    update_status(config_id, last_sync_status="running", last_sync_error=None)

    docker_image = os.environ.get("SYNC_DOCKER_IMAGE", "")
    if not docker_image:
        msg = "SYNC_DOCKER_IMAGE is required for SYNC_PHASE=docker_read"
        print(f"ERROR: {msg}")
        _mark_failed(config_id, msg)
        sys.exit(1)

    # 1. Load config from Supabase
    print(f"[1/7] Loading config {config_id} from Supabase...")
    row = load_config(config_id)
    if not row:
        print(f"ERROR: Config {config_id} not found")
        sys.exit(1)

    user_id = row["user_id"]
    connector_name = row["connector_name"]
    raw_config = row["config"]
    oauth_meta = row.get("oauth_meta") or raw_config.get("__oauth_meta__", {})
    selected_streams = row.get("selected_streams")

    if isinstance(raw_config, str):
        raw_config = json.loads(raw_config)

    print(f"  User: {user_id}")
    print(f"  Connector: {connector_name} ({docker_image})")
    print(f"  Streams: {selected_streams or 'all'}")

    # 2. Refresh credentials
    if oauth_meta:
        print("[2/7] Checking token freshness...")
        try:
            raw_config, was_refreshed = ensure_fresh_credentials(raw_config, oauth_meta)
            if was_refreshed:
                print("  Credentials refreshed — persisting to Supabase")
                save_refreshed_config(config_id, raw_config, oauth_meta)
            else:
                print("  Token still valid")
        except ReauthRequired as e:
            print(f"  ERROR: {e}")
            update_status(config_id, last_sync_status="reauth_required", last_sync_error=str(e))
            sys.exit(2)
        except TokenRefreshError as e:
            print(f"  WARNING: Token refresh failed: {e}")
            print("  Attempting sync with existing credentials...")
    else:
        print("[2/7] No OAuth metadata — skipping token refresh")

    user_config = clean_config(raw_config)

    # 3. Write config + catalog to shared volume
    work_dir = DOCKER_OUTPUT_BASE / config_id
    work_dir.mkdir(parents=True, exist_ok=True)

    config_path = work_dir / "config.json"
    config_path.write_text(json.dumps(user_config, default=str))
    print(f"[3/7] Wrote config to {config_path}")

    # ── Phase 1: Discover via the official connector image ──────────
    print(f"[4/7] Discovering streams via connector image: {docker_image}...")
    try:
        discover_exec = _launch_connector_aca_job(
            docker_image, work_dir, config_id,
            airbyte_command="discover",
            output_file="discover_output.jsonl",
        )
    except Exception as exc:
        msg = f"Failed to launch discover job: {type(exc).__name__}: {exc}"
        print(f"ERROR: {msg}")
        _mark_failed(config_id, msg)
        sys.exit(1)

    discover_timeout = int(os.environ.get("DOCKER_JOB_TIMEOUT", "900"))
    if not _wait_for_connector_job(discover_exec, timeout=discover_timeout):
        stderr_path = work_dir / "stderr.log"
        stderr_tail = ""
        if stderr_path.exists():
            stderr_tail = stderr_path.read_text(encoding="utf-8", errors="replace")[-2000:]
        msg = f"Discover job failed. stderr: {stderr_tail}"
        print(f"ERROR: {msg}")
        _mark_failed(config_id, msg)
        sys.exit(1)

    # Parse the CATALOG message from discover output
    discover_path = work_dir / "discover_output.jsonl"
    raw_catalog = _parse_discover_output(discover_path)
    if not raw_catalog or not raw_catalog.get("streams"):
        msg = "Discover produced no catalog / zero streams"
        print(f"ERROR: {msg}")
        _mark_failed(config_id, msg)
        sys.exit(1)

    all_stream_names = [
        s["name"] for s in raw_catalog["streams"] if isinstance(s, dict) and s.get("name")
    ]
    print(f"  Discovered {len(all_stream_names)} stream(s)")

    # Determine which streams to sync
    if selected_streams:
        streams_to_sync = [s for s in selected_streams if s in all_stream_names]
        if not streams_to_sync:
            streams_to_sync = list(selected_streams)
    else:
        streams_to_sync = all_stream_names

    # Build ConfiguredAirbyteCatalog from the real discover output
    stream_lookup = {s["name"]: s for s in raw_catalog["streams"] if isinstance(s, dict) and s.get("name")}
    configured_streams = []
    for stream_name in streams_to_sync:
        real_stream = stream_lookup.get(stream_name, {"name": stream_name, "json_schema": {}, "supported_sync_modes": ["full_refresh"]})
        configured_streams.append({
            "stream": real_stream,
            "sync_mode": "full_refresh",
            "destination_sync_mode": "overwrite",
        })

    catalog = {"streams": configured_streams}
    catalog_path = work_dir / "catalog.json"
    catalog_path.write_text(json.dumps(catalog, default=str))
    print(f"  Wrote configured catalog ({len(configured_streams)} streams) to {catalog_path}")

    # ── Phase 2: Read via the official connector image ──────────────
    print(f"[5/7] Launching connector read: {docker_image}...")
    try:
        execution_name = _launch_connector_aca_job(
            docker_image, work_dir, config_id,
            airbyte_command="read",
            output_file="output.jsonl",
        )
    except Exception as exc:
        msg = f"Failed to launch connector job: {type(exc).__name__}: {exc}"
        print(f"ERROR: {msg}")
        _mark_failed(config_id, msg)
        sys.exit(1)

    # 5. Wait for connector job to complete
    print(f"[6/7] Waiting for connector job {execution_name}...")
    timeout = int(os.environ.get("DOCKER_JOB_TIMEOUT", "900"))
    success = _wait_for_connector_job(execution_name, timeout=timeout)

    if not success:
        stderr_path = work_dir / "stderr.log"
        stderr_tail = ""
        if stderr_path.exists():
            stderr_tail = stderr_path.read_text(encoding="utf-8", errors="replace")[-2000:]
        msg = f"Connector job failed. stderr: {stderr_tail}"
        print(f"ERROR: {msg}")
        _mark_failed(config_id, msg)
        sys.exit(1)

    # 6. Parse JSONL output → Parquet → Blob
    print("[7/7] Parsing connector output and uploading...")
    jsonl_path = work_dir / "output.jsonl"
    if not jsonl_path.exists():
        msg = "Connector job produced no output.jsonl"
        print(f"ERROR: {msg}")
        _mark_failed(config_id, msg)
        sys.exit(1)

    stream_records = _parse_airbyte_jsonl(jsonl_path)
    if not stream_records:
        msg = "No RECORD messages found in connector output"
        print(f"ERROR: {msg}")
        _mark_failed(config_id, msg)
        sys.exit(1)

    output_dir = Path(tempfile.mkdtemp())
    rows_total = 0
    for stream_name, records in stream_records.items():
        df = pd.DataFrame(records)
        if df.empty:
            continue
        parquet_path = output_dir / f"{stream_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        rows_total += len(df)
        print(f"  {stream_name}: {len(df)} rows")

    uploaded = upload_to_blob(output_dir, user_id, docker_image)

    # Clean up shared volume
    try:
        import shutil
        shutil.rmtree(work_dir, ignore_errors=True)
    except Exception:
        pass

    now = datetime.now(timezone.utc).isoformat()
    update_status(
        config_id,
        last_sync_at=now,
        last_sync_status="success",
        last_sync_error=None,
    )
    print(f"\nDocker-native sync complete! {len(uploaded)} files uploaded, {rows_total} total rows.")


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

    sync_phase = os.environ.get("SYNC_PHASE", "")
    config_id = os.environ.get("SYNC_CONFIG_ID")
    if not config_id:
        print("ERROR: SYNC_CONFIG_ID environment variable is required")
        sys.exit(1)

    if sync_phase == "docker_read":
        # Docker-native path for Java connectors
        try:
            run_docker_native_sync(config_id)
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            print(f"ERROR: {err}")
            _mark_failed(config_id, err)
            sys.exit(1)
    else:
        # Default PyAirbyte path for Python / manifest connectors
        try:
            run_sync(config_id)
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            print(f"ERROR: {err}")
            _mark_failed(config_id, err)
            sys.exit(1)
