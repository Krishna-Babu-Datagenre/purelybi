"""Sync Uploader — Slim container for JSONL → Parquet → Blob.

Reads Airbyte JSONL from the mounted File Share, converts RECORD messages
to Parquet (one file per stream), and uploads to Azure Blob Storage.

Architecture: streaming single-pass with batch flushing.
  - JSONL is streamed line-by-line from the File Share (never fully loaded).
  - Records are buffered per-stream in memory.
  - When a buffer hits BATCH_SIZE rows, the batch is flushed to a
    part-file in Blob Storage and the buffer is released.
  - Airbyte STATE messages are captured inline during the same pass.

Blob path layout:
    user-data/{user_id}/{source}/{stream}/{sync_ts}_part{N}.parquet

  Each sync produces one or more part-files per stream.  DuckDB reads
  ``*.parquet`` globs on leaf directories, so all parts are unioned
  automatically — no merge or download-concat-reupload needed.

  For full_refresh syncs, old part-files are deleted before uploading so
  stale data doesn't accumulate.  For incremental syncs, parts accumulate
  and DuckDB unions them.  Deduplication (if needed) happens at query
  time via the _airbyte_ab_id column.

On completion the uploader writes the sync outcome (success/failed,
last_airbyte_state, last_sync_at, etc.) directly to Supabase so the UI
reflects the result immediately — no need to wait for the next
orchestrator tick.

Environment variables (set by the orchestrator via ACA Job image override):
    WORK_ID                         — File Share directory with output.jsonl
    USER_ID                         — Supabase user UUID
    DOCKER_IMAGE                    — e.g. airbyte/source-shopify:3.2.3 (for blob path)
    CONFIG_ID                       — user_connector_configs.id (for status callback)
    INCREMENTAL_ENABLED             — "true" if incremental sync; else full_refresh
    AZURE_FILE_SHARE_CONN_STR       — File Share connection string
    AZURE_FILE_SHARE_NAME           — File Share name
    AZURE_STORAGE_CONNECTION_STRING  — Blob Storage connection string
    BLOB_CONTAINER_NAME             — Blob container (default: raw)
    SUPABASE_URL                    — Supabase project URL (for status callback)
    SUPABASE_SERVICE_ROLE_KEY       — Supabase service-role key (for status callback)
"""

import gc
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO

import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.storage.fileshare import ShareFileClient

# ── Config from env ───────────────────────────────────────────────────

WORK_ID = os.environ["WORK_ID"]
USER_ID = os.environ["USER_ID"]
DOCKER_IMAGE = os.environ["DOCKER_IMAGE"]
CONFIG_ID = os.environ.get("CONFIG_ID", "")
INCREMENTAL = os.environ.get("INCREMENTAL_ENABLED", "").lower() == "true"
FILESHARE_CONN = os.environ["AZURE_FILE_SHARE_CONN_STR"]
FILESHARE_NAME = os.environ["AZURE_FILE_SHARE_NAME"]
BLOB_CONN = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "raw")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# Flush a stream's buffer to a Parquet part-file once it hits this size.
# 50 000 rows ≈ 5–30 MB of Parquet depending on schema width.
BATCH_SIZE = 50_000


# ── Helpers ───────────────────────────────────────────────────────────


def extract_source_name(docker_image: str) -> str:
    """'airbyte/source-shopify:3.2.3' → 'source-shopify'"""
    return docker_image.split(":")[0].split("/")[-1]


def sanitize_df_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Make a DataFrame safely writable to Parquet.

    JSON-serialises complex types (dict/list/ndarray) in object columns
    and forces them to str. DuckDB can parse JSON strings at query time.
    """
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().head(50)
            needs_json = sample.apply(
                lambda v: isinstance(v, (dict, list, np.ndarray, set, tuple))
            ).any()
            if needs_json:
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, default=str)
                    if isinstance(v, (dict, list, np.ndarray, set, tuple))
                    else v
                )
            df[col] = df[col].astype(str)
    return df


def read_fileshare_file(path: str) -> bytes:
    """Read raw bytes from the File Share."""
    file_client = ShareFileClient.from_connection_string(
        conn_str=FILESHARE_CONN,
        share_name=FILESHARE_NAME,
        file_path=path,
    )
    return file_client.download_file().readall()


def stream_fileshare_lines(path: str):
    """Yield decoded lines from a File Share file without loading it all.

    Uses chunked download so peak memory is O(chunk_size) rather than
    O(file_size).  The Azure SDK default chunk is 4 MB.
    """
    file_client = ShareFileClient.from_connection_string(
        conn_str=FILESHARE_CONN,
        share_name=FILESHARE_NAME,
        file_path=path,
    )
    stream = file_client.download_file()
    buf = b""
    for chunk in stream.chunks():
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            yield line.decode("utf-8", errors="replace")
    # Yield trailing content without newline (last line)
    if buf:
        yield buf.decode("utf-8", errors="replace")


def cleanup_fileshare() -> None:
    """Remove the work directory from the File Share after a successful upload."""
    from azure.storage.fileshare import ShareDirectoryClient
    try:
        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=FILESHARE_CONN,
            share_name=FILESHARE_NAME,
            directory_path=WORK_ID,
        )
        for item in dir_client.list_directories_and_files():
            dir_client.delete_file(item["name"])
        dir_client.delete_directory()
        print(f"  Cleaned up file share: {WORK_ID}")
    except Exception as exc:
        print(f"  WARNING: File share cleanup failed (non-fatal): {exc}")


# ── Blob helpers ──────────────────────────────────────────────────────


def _blob_prefix_for_stream(source_name: str, stream_name: str) -> str:
    """Return the blob prefix (folder) for a given stream."""
    return f"user-data/{USER_ID}/{source_name}/{stream_name}/"


def delete_old_parts(container, prefix: str) -> int:
    """Delete all existing Parquet part-files under a blob prefix.

    Used for full_refresh syncs to prevent stale data accumulation.
    Returns the number of blobs deleted.
    """
    deleted = 0
    for blob in container.list_blobs(name_starts_with=prefix):
        if blob.name.endswith(".parquet"):
            container.delete_blob(blob.name)
            deleted += 1
    return deleted


def flush_batch(
    container,
    records: list[dict],
    stream_name: str,
    source_name: str,
    sync_ts: str,
    part_num: int,
) -> str:
    """Convert a batch of records to Parquet and upload as a part-file.

    Returns the blob path written.
    """
    df = pd.DataFrame(records)
    df = sanitize_df_for_parquet(df)

    prefix = _blob_prefix_for_stream(source_name, stream_name)
    blob_path = f"{prefix}{sync_ts}_part{part_num}.parquet"

    out = BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)

    container.upload_blob(blob_path, out, overwrite=True)
    print(f"  {stream_name}: flushed {len(records)} rows → {blob_path}")

    # Explicitly free the DataFrame and buffer
    del df, out
    gc.collect()

    return blob_path


def _update_supabase(fields: dict) -> bool:
    """Best-effort status callback to Supabase.

    Uses urllib so we don't add ``supabase-py``/``httpx`` to the slim image.
    Returns True on success. Failures are logged but never fatal — the
    orchestrator will reconcile.
    """
    if not (SUPABASE_URL and SUPABASE_KEY and CONFIG_ID):
        return False
    import urllib.request
    import urllib.error

    url = f"{SUPABASE_URL}/rest/v1/user_connector_configs?id=eq.{CONFIG_ID}"
    data = json.dumps(fields, default=str).encode()
    req = urllib.request.Request(
        url,
        data=data,
        method="PATCH",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print(f"  Supabase status updated: {fields.get('last_sync_status', '?')}")
        return True
    except (urllib.error.URLError, OSError) as exc:
        print(f"  WARNING: Supabase callback failed (orchestrator will reconcile): {exc}")
        return False


def _read_supabase_field(field: str):
    """Read a single field from the current config row. Returns None on failure."""
    if not (SUPABASE_URL and SUPABASE_KEY and CONFIG_ID):
        return None
    import urllib.request
    import urllib.error

    url = (
        f"{SUPABASE_URL}/rest/v1/user_connector_configs"
        f"?id=eq.{CONFIG_ID}&select={field}"
    )
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
        },
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        rows = json.loads(resp.read().decode())
        if rows:
            return rows[0].get(field)
    except Exception as exc:
        print(f"  WARNING: Failed to read {field} from Supabase: {exc}")
    return None


def _append_error_and_fail(error_detail: str, phase: str = "uploader") -> None:
    """Append a detailed error entry to sync_error_log and mark the config as failed.

    Reads the existing sync_error_log array, appends the new entry (capped at 5),
    increments consecutive_failures, then writes everything back.
    """
    MAX_ENTRIES = 5

    # Read current state
    existing_log = _read_supabase_field("sync_error_log") or []
    consecutive = _read_supabase_field("consecutive_failures") or 0

    if not isinstance(existing_log, list):
        existing_log = []
    if not isinstance(consecutive, int):
        consecutive = 0

    consecutive += 1

    error_entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "phase": phase,
        "error": error_detail[:8000],
    }
    existing_log.append(error_entry)
    if len(existing_log) > MAX_ENTRIES:
        existing_log = existing_log[-MAX_ENTRIES:]

    _update_supabase({
        "last_sync_status": "failed",
        "last_sync_error": f"Uploader failed: {error_detail[:2000]}",
        "aca_execution_name": None,
        "aca_work_id": None,
        "consecutive_failures": consecutive,
        "sync_error_log": existing_log,
    })


# ── Main ──────────────────────────────────────────────────────────────


def main() -> None:
    print(f"Sync Uploader starting: work_id={WORK_ID} user={USER_ID} image={DOCKER_IMAGE}")
    print(f"  mode={'incremental' if INCREMENTAL else 'full_refresh'}"
          f"  batch_size={BATCH_SIZE}")

    jsonl_path = f"{WORK_ID}/output.jsonl"
    source_name = extract_source_name(DOCKER_IMAGE)
    sync_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN)
    container = blob_service.get_container_client(BLOB_CONTAINER)

    # ── Single-pass streaming ─────────────────────────────────────────
    #
    # We stream the JSONL line-by-line from the File Share, buffering
    # RECORD data per-stream.  When any buffer reaches BATCH_SIZE, the
    # batch is flushed to a Parquet part-file and the buffer is freed.
    # STATE messages are captured inline so we only traverse the file once.

    print(f"[1/2] Streaming {jsonl_path} → Parquet part-files...")

    stream_buffers: dict[str, list[dict]] = defaultdict(list)
    stream_part_counts: dict[str, int] = defaultdict(int)
    last_state: dict | None = None
    total_records = 0
    uploaded: list[str] = []
    streams_seen: set[str] = set()

    # For full_refresh we need to delete old parts, but only once we
    # know which streams are present in the new data.  We track which
    # streams have been cleaned so we only delete once per stream.
    cleaned_streams: set[str] = set()

    for line in stream_fileshare_lines(jsonl_path):
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue

        msg_type = msg.get("type")

        if msg_type == "RECORD":
            record = msg.get("record", {})
            stream_name = record.get("stream", "unknown")
            data = record.get("data", {})
            if not data:
                continue

            streams_seen.add(stream_name)
            stream_buffers[stream_name].append(data)
            total_records += 1

            # For full_refresh: delete old parts the first time we see
            # a stream so stale data doesn't accumulate.
            if not INCREMENTAL and stream_name not in cleaned_streams:
                prefix = _blob_prefix_for_stream(source_name, stream_name)
                deleted = delete_old_parts(container, prefix)
                if deleted:
                    print(f"  {stream_name}: deleted {deleted} old part-file(s) (full_refresh)")
                cleaned_streams.add(stream_name)

            # Flush when the buffer hits the batch threshold
            if len(stream_buffers[stream_name]) >= BATCH_SIZE:
                part_num = stream_part_counts[stream_name]
                blob_path = flush_batch(
                    container,
                    stream_buffers[stream_name],
                    stream_name,
                    source_name,
                    sync_ts,
                    part_num,
                )
                uploaded.append(blob_path)
                stream_part_counts[stream_name] += 1
                stream_buffers[stream_name] = []

        elif msg_type == "STATE":
            last_state = msg

    # ── Flush remaining buffers ───────────────────────────────────────

    for stream_name, records in stream_buffers.items():
        if not records:
            continue
        part_num = stream_part_counts[stream_name]
        blob_path = flush_batch(
            container,
            records,
            stream_name,
            source_name,
            sync_ts,
            part_num,
        )
        uploaded.append(blob_path)

    # Free buffers
    del stream_buffers
    gc.collect()

    # ── Report ────────────────────────────────────────────────────────

    if not uploaded:
        print("WARNING: No RECORD messages in output — nothing to upload")

    print(f"[2/2] Reporting sync result to Supabase...")
    print(f"  {total_records} rows across {len(streams_seen)} stream(s)"
          f" → {len(uploaded)} part-file(s)")

    update_fields: dict = {
        "last_sync_at": datetime.now(timezone.utc).isoformat(),
        "last_sync_status": "success",
        "last_sync_error": None,
        "aca_execution_name": None,
        "aca_work_id": None,
        "consecutive_failures": 0,
        "sync_error_log": [],
    }
    if last_state is not None:
        update_fields["last_airbyte_state"] = last_state

    ok = _update_supabase(update_fields)
    if ok:
        cleanup_fileshare()

    print(f"\nUpload complete: {len(uploaded)} part-files uploaded"
          f" | state_captured={last_state is not None}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        error_detail = traceback.format_exc()
        print(f"FATAL ERROR in sync uploader:\n{error_detail}", file=sys.stderr)
        _append_error_and_fail(error_detail, phase="uploader")
        sys.exit(1)
