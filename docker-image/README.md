# Docker Image Runbook (Sync Worker)

This folder contains the container image source used by the Azure Container Apps Job:

- Job: `caj-purelybi-data-sync-dev-ci`
- ACR: `acrpurelybidevci.azurecr.io`
- Repository: `purelybi/data-sync-worker`

Use this runbook whenever you change `sync_worker.py`, `credential_refresh.py`, or `Dockerfile.worker`.

## 1) Build and push image

Run from this folder (`docker-image/`):

```powershell
docker build -t data-sync-worker:latest -f Dockerfile.worker .
docker tag data-sync-worker:latest acrpurelybidevci.azurecr.io/purelybi/data-sync-worker:latest
docker push acrpurelybidevci.azurecr.io/purelybi/data-sync-worker:latest
```

If you are not logged in to ACR yet:

```powershell
docker login acrpurelybidevci.azurecr.io
```

## 2) Update Container Apps Job to pick the new image

Portal path:

1. Open `caj-purelybi-data-sync-dev-ci`
2. Go to the container/template edit screen
3. Keep image as:
   - `acrpurelybidevci.azurecr.io/purelybi/data-sync-worker`
   - tag: `latest`
4. Save/Deploy a new revision/execution template

## 3) Quick test flow

1. Ensure one row in `user_connector_configs` is eligible:
   - `is_active = true`
   - `sync_validated = true`
   - `last_sync_status` is not `queued`, `running`, or `reauth_required`
2. Trigger `func-purelybi-sync-orchestrator-dev-ci` manually (or wait for timer).
3. Verify:
   - Function log shows `Started ACA job ...`
   - Job execution appears under `caj-purelybi-data-sync-dev-ci` executions
   - Worker logs no longer show stale startup errors

## 4) Required app settings / env references

### In Function App (`func-purelybi-sync-orchestrator-dev-ci`)

- `AZURE_SUBSCRIPTION_ID`
- `AZURE_RESOURCE_GROUP`
- `ACA_JOB_NAME=caj-purelybi-data-sync-dev-ci`
- `ACA_JOB_CONTAINER_NAME` (must match the container name in the ACA Job template)
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

### In Container Apps Job (`caj-purelybi-data-sync-dev-ci`)

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY` (secret reference)
- `AZURE_STORAGE_CONNECTION_STRING` (secret reference)
- `BLOB_CONTAINER_NAME` (for example: `sync-output`)
- `AIRBYTE_ENABLE_UNSAFE_CODE=true`

Per-run values (`SYNC_CONFIG_ID`, `SYNC_USER_ID`, `SYNC_CONNECTOR_NAME`) are injected by the orchestrator on job start.

## Storage layout

Worker uploads to blob using:

`raw/user-data/{user_id}/{connector_name}/{stream_name}/{YYYY-MM}.parquet`

Example:

`raw/user-data/c5efc103-bb7f-42dd-ae10-527612b146d4/source-facebook-marketing/ads_insights/2026-04.parquet`

Current behavior for the same month file:

- If the monthly blob exists, worker downloads it, appends new rows, and writes it back.
- If it does not exist, worker creates it.

Status semantics used by current orchestration:

- `queued`: job start accepted by ACA API
- `running`: worker process started and marked itself running
- `success` / `failed` / `reauth_required`: terminal outcomes from worker/runtime

## 5) Recommended tagging (optional but safer)

Using only `latest` works, but version tags make rollbacks easier:

```powershell
$tag = "2026-04-07-1"
docker build -t data-sync-worker:$tag -f Dockerfile.worker .
docker tag data-sync-worker:$tag acrpurelybidevci.azurecr.io/purelybi/data-sync-worker:$tag
docker push acrpurelybidevci.azurecr.io/purelybi/data-sync-worker:$tag
```

Then update the job to use that exact tag.
