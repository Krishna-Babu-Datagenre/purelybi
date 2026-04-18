# Docker Image Runbook (Sync Uploader)

This folder contains the sync-uploader container image used by the Azure Container Apps Job during scheduled syncs.

- Job: `caj-purelybi-connector-v2-dev-ci`
- ACR: `acrpurelybiv2devci.azurecr.io`
- Repository: `sync-uploader`

The uploader reads Airbyte JSONL output from the Azure File Share, converts it to Parquet, and uploads it to Blob Storage. It runs as a second ACA Job execution after the connector image completes its `read` phase.

Use this runbook whenever you change `sync_uploader.py`, `credential_refresh.py`, or `Dockerfile.uploader`.

## 1) Build and push image

Run from this folder (`docker-image/`):

```powershell
az acr login --name acrpurelybiv2devci

docker build --no-cache -t sync-uploader:v2 -f Dockerfile.uploader .
docker tag sync-uploader:v2 acrpurelybiv2devci.azurecr.io/sync-uploader:v2
docker push acrpurelybiv2devci.azurecr.io/sync-uploader:v2
```

`--no-cache` is always used to ensure the latest code is picked up.

If you are not logged in to ACR yet:

```powershell
az acr login --name acrpurelybiv2devci
```

## 2) The ACA Job pulls the image on next execution

No manual image update needed — the job references `sync-uploader:v2` and pulls on each execution. The `SYNC_UPLOADER_IMAGE` env var in the orchestrator Function App controls which image is used.

## 3) Quick test flow

1. Ensure one row in `user_connector_configs` is eligible:
   - `is_active = true`
   - `sync_validated = true`
   - `last_sync_status` is not `queued`, `running`, or `reauth_required`
2. Trigger `func-purelybi-sync-orchestrator-v2-dev-ci` manually (or wait for timer).
3. Verify:
   - Function log shows connector execution started
   - Execution appears under `caj-purelybi-connector-v2-dev-ci` executions
   - After connector completes, uploader execution starts
   - Parquet appears in Blob Storage

## 4) Required app settings

### In Function App (`func-purelybi-sync-orchestrator-v2-dev-ci`)

- `ACA_SUBSCRIPTION_ID`
- `ACA_RESOURCE_GROUP`
- `ACA_JOB_NAME=caj-purelybi-connector-v2-dev-ci`
- `ACA_JOB_CONTAINER_NAME=connector`
- `AZURE_FILE_SHARE_CONN_STR` (Azure File Share connection string)
- `AZURE_FILE_SHARE_NAME=connector-data-v2`
- `SYNC_UPLOADER_IMAGE=acrpurelybiv2devci.azurecr.io/sync-uploader:v2`
- `AZURE_STORAGE_CONNECTION_STRING` (Blob Storage)
- `BLOB_CONTAINER_NAME=raw`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

### Env vars injected per uploader execution (by the orchestrator)

- `WORK_ID` — File Share directory for this sync run
- `AZURE_FILE_SHARE_CONN_STR`
- `AZURE_FILE_SHARE_NAME`
- `AZURE_STORAGE_CONNECTION_STRING`
- `BLOB_CONTAINER_NAME`
- `USER_ID`, `CONNECTOR_NAME`, `USER_DATA_BLOB_PREFIX`
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SYNC_CONFIG_ID`

## Storage layout

Uploader writes to blob:

`{container}/{prefix}/{user_id}/{connector_name}/{stream_name}/{YYYY-MM}.parquet`

Example:

`raw/user-data/c5efc103-bb7f-42dd-ae10-527612b146d4/source-facebook-marketing/ads_insights/2026-04.parquet`

Monthly behavior:
- If monthly blob exists: download + append new rows + overwrite
- If not: create new file
