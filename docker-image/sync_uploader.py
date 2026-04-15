"""Sync Uploader — Slim container for JSONL → Parquet → Blob.

Reads Airbyte JSONL from the mounted File Share, converts RECORD messages
to Parquet (one file per stream), and uploads to Azure Blob Storage with
monthly merge.

This replaces the 900-line sync_worker for the Parquet conversion step.

Environment variables (set by the orchestrator via ACA Job image override):
    WORK_ID                         — File Share directory with output.jsonl
    USER_ID                         — Supabase user UUID
    DOCKER_IMAGE                    — e.g. airbyte/source-shopify:3.2.3 (for blob path)
    AZURE_FILE_SHARE_CONN_STR       — File Share connection string
    AZURE_FILE_SHARE_NAME           — File Share name
    AZURE_STORAGE_CONNECTION_STRING  — Blob Storage connection string
    BLOB_CONTAINER_NAME             — Blob container (default: raw)
"""

import json
import os
import sys
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
FILESHARE_CONN = os.environ["AZURE_FILE_SHARE_CONN_STR"]
FILESHARE_NAME = os.environ["AZURE_FILE_SHARE_NAME"]
BLOB_CONN = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "raw")


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


def parse_airbyte_jsonl(content: str) -> dict[str, list[dict]]:
    """Parse Airbyte JSONL and return records grouped by stream name."""
    streams: dict[str, list[dict]] = {}
    for line in content.splitlines():
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


def upload_to_blob(stream_records: dict[str, list[dict]]) -> list[str]:
    """Convert records to Parquet and upload to Blob Storage with monthly merge."""
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN)
    container = blob_service.get_container_client(BLOB_CONTAINER)

    month_prefix = datetime.now(timezone.utc).strftime("%Y-%m")
    source_name = extract_source_name(DOCKER_IMAGE)
    uploaded: list[str] = []

    for stream_name, records in stream_records.items():
        df = pd.DataFrame(records)
        if df.empty:
            continue

        df = sanitize_df_for_parquet(df)
        blob_path = f"user-data/{USER_ID}/{source_name}/{stream_name}/{month_prefix}.parquet"
        blob_client = container.get_blob_client(blob_path)

        # Merge with existing monthly Parquet if present
        if blob_client.exists():
            existing_bytes = blob_client.download_blob().readall()
            existing_df = pd.read_parquet(BytesIO(existing_bytes))
            merged = pd.concat([existing_df, df], ignore_index=True)
            # Deduplicate by _airbyte_ab_id when available (incremental append)
            if "_airbyte_ab_id" in merged.columns:
                merged = merged.drop_duplicates(subset=["_airbyte_ab_id"], keep="last")
            elif "_ab_id" in merged.columns:
                merged = merged.drop_duplicates(subset=["_ab_id"], keep="last")
            df = merged

        out = BytesIO()
        df.to_parquet(out, index=False)
        out.seek(0)
        container.upload_blob(blob_path, out, overwrite=True)
        uploaded.append(blob_path)
        print(f"  {stream_name}: {len(records)} new rows → {blob_path}")

    return uploaded


# ── Main ──────────────────────────────────────────────────────────────


def main() -> None:
    print(f"Sync Uploader starting: work_id={WORK_ID} user={USER_ID} image={DOCKER_IMAGE}")

    # 1. Read JSONL from File Share
    jsonl_path = f"{WORK_ID}/output.jsonl"
    print(f"[1/3] Reading {jsonl_path} from File Share...")
    content = read_fileshare_file(jsonl_path).decode("utf-8", errors="replace")

    # 2. Parse records
    print("[2/3] Parsing Airbyte JSONL...")
    stream_records = parse_airbyte_jsonl(content)
    if not stream_records:
        print("WARNING: No RECORD messages in output — nothing to upload")
        sys.exit(0)

    total = sum(len(r) for r in stream_records.values())
    print(f"  Found {total} records across {len(stream_records)} stream(s)")

    # 3. Upload to Blob
    print("[3/3] Converting to Parquet and uploading...")
    uploaded = upload_to_blob(stream_records)

    print(f"\nUpload complete: {len(uploaded)} files uploaded")


if __name__ == "__main__":
    main()
