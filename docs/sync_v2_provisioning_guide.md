# Sync V2 — Provisioning & Setup Guide

## What changed and why

**V1 pain points:** Two separate ACA Jobs (one for Docker-native connectors, one for PyAirbyte), language-based routing logic (`_should_use_docker_native`), an unreliable PyAirbyte dependency, and a heavyweight sync-worker container that handled both extraction _and_ Parquet conversion in one shot.

**V2 approach — single ACA Job, two-phase execution:**

1. **Phase 1 — Extract (connector image).** The orchestrator (or the local backend during onboarding) starts the ACA Job with an image override pointing to the official Airbyte connector image (e.g. `airbyte/source-shopify:latest`). The connector runs `read --config --catalog`, writing JSONL to an Azure File Share mounted at `/data/{work_id}/`.
2. **Phase 2 — Upload (uploader image).** Once the connector execution succeeds, a second execution starts on the _same_ ACA Job using a slim custom image (`sync-uploader`). It reads the JSONL from the File Share, converts each stream to Parquet, and uploads to Blob Storage with monthly-partition merge.

**What this eliminates:**
- PyAirbyte entirely — all connectors use their official Docker images.
- Language routing — no more `DOCKER_IMAGE_LANGUAGES` or manifest/Python/Java branching.
- The second ACA Job — one job (`caj-purelybi-connector-v2-dev-ci`) with `parallelism=5` handles everything.
- Docker Desktop dependency — the backend invokes ACA directly via the Azure SDK (`DefaultAzureCredential` from `az login`).

**All resources are brand-new** — nothing reused from the existing V1 setup.

---

## File Map

| File | Role |
|------|------|
| **`backend/src/ai/agents/onboarding/infra/connector_runner.py`** | Shared helpers for starting ACA Job executions (connector & uploader), polling status, and File Share I/O. Used by both the backend onboarding flow and conceptually mirrored in the orchestrator. |
| **`backend/src/ai/agents/onboarding/infra/docker_ops.py`** | Public API for onboarding connector operations (`check`, `discover`, `read_probe`). Routes to ACA-direct helpers or local Docker subprocess based on `ONBOARDING_DOCKER_EXECUTION_MODE`. |
| **`backend/src/ai/tools/onboarding/__init__.py`** | LangChain tools (`discover_streams`, `save_config`, etc.) that the onboarding agent calls. `discover_streams` caches the full Airbyte catalog in KV; `save_config` persists it to Supabase via `discovered_catalog`. |
| **`backend/src/fastapi_app/services/connector_service.py`** | `upsert_user_connector_onboarding()` — creates/updates a `user_connector_configs` row including the `discovered_catalog` JSONB column so the orchestrator can skip rediscovery. |
| **`backend/src/fastapi_app/settings.py`** | V2 settings block (`ACA_SUBSCRIPTION_ID_V2`, `ACA_JOB_NAME_V2`, `AZURE_FILE_SHARE_CONN_STR`, `SYNC_UPLOADER_IMAGE`, etc.). |
| **`docker-image/sync_uploader.py`** | Slim container script: reads JSONL from the File Share, converts each stream to Parquet, uploads to Blob Storage with monthly-partition merge. |
| **`docker-image/Dockerfile.uploader`** | Dockerfile for the sync-uploader image (Python 3.11-slim, pandas, pyarrow, azure-storage-blob/file-share). |
| **`azure-function-sync-orchestrator/sync_orchestrator_v2/__init__.py`** | Timer-triggered Azure Function (every 5 min). Runs a 3-phase state machine: poll `uploading` → poll `reading` → start new eligible syncs. Includes credential refresh and a circuit breaker (`MAX_CONSECUTIVE_FAILURES = 5`). |
| **`azure-function-sync-orchestrator/shared/credential_refresh.py`** | OAuth token refresh logic imported by the orchestrator. Copied from `docker-image/credential_refresh.py`. |
| **`backend/supabase/queries/3-sync-v2-columns.sql`** | Migration SQL: adds `discovered_catalog`, `aca_execution_name`, `aca_work_id`, `consecutive_failures` columns and an index on `last_sync_status`. |

---

## Architecture Overview

```
                          ┌───────────────────────────────────────┐
 Local Backend ──────────►│  caj-purelybi-connector-v2-dev-ci     │◄── Orchestrator
 (or App Service)         │  (Single ACA Job)                     │    (Azure Function)
                          │                                       │
                          │  Execution A: connector               │
                          │    airbyte/source-shopify              │
                          │    read --config --catalog             │
                          │                                       │
                          │  Execution B: uploader                │
                          │    acrpurelybiv2devci.azurecr.io/     │
                          │    sync-uploader:latest                │
                          │    JSONL → Parquet → Blob              │
                          └──────────────────┬────────────────────┘
                                             │
                                   Azure File Share
                                   connector-data-v2
                                   /data/{work_id}/
                                     config.json
                                     catalog.json
                                     output.jsonl
                                             │
                                             ▼
                                   Azure Blob Storage
                                   Container: raw
                                   user-data/{uid}/{src}/{stream}/{YYYY-MM}.parquet
```

**State machine (orchestrator timer tick):**

1. Check `uploading` configs → poll uploader execution → mark `success` or `failed`
2. Check `reading` configs → poll connector execution → if done: start uploader, mark `uploading`
3. Start new syncs for eligible configs → write config/catalog to File Share → start connector → mark `reading`

---

## Prerequisites

- Azure CLI installed (`az --version`)
- Logged in (`az login`)
- Subscription selected: `az account set --subscription <YOUR_SUBSCRIPTION_ID>`

Set these shell variables for the rest of the guide:

```powershell
# ── Edit these ───────────────────────────────────────────────────────
$SUBSCRIPTION_ID   = "3892ad52-b508-4b32-8a93-ac5a9c1712e4"
$LOCATION          = "centralindia"                          # or your preferred region
$SUPABASE_URL      = "https://mcscwwsexvdqlmulqgjd.supabase.co"
$SUPABASE_KEY      = # i will manually set in the terminal you open
$BLOB_CONN_STR     = # i will manually set in the terminal you open  # for Parquet output
$BLOB_CONTAINER    = "raw"                             # existing blob container name

# ── Fixed names (new resources) ──────────────────────────────────────
$RG                = "rg-purelybi-sync-v2-dev-ci"
$LAW               = "law-purelybi-sync-v2-dev-ci"
$STORAGE_ACCT      = "sapurelybisyncv2devci"                # globally unique, lowercase
$FILE_SHARE        = "connector-data-v2"
$CAE               = "caenv-purelybi-sync-v2-dev-ci"
$CAJ               = "caj-purelybi-connector-v2-dev-ci"
$CAJ_CONTAINER     = "connector"
$ACR               = "acrpurelybiv2devci"
```

---

## Step 1 — Resource Group

```powershell
az group create `
  --name $RG `
  --location $LOCATION
```

---

## Step 2 — Storage Account + File Share

The File Share is the I/O bridge between your code and the ACA Job container.

```powershell
# Create the storage account
az storage account create `
  --name $STORAGE_ACCT `
  --resource-group $RG `
  --location $LOCATION `
  --sku Standard_LRS `
  --kind StorageV2

# Get the storage account key
$STORAGE_KEY = (az storage account keys list `
  --account-name $STORAGE_ACCT `
  --resource-group $RG `
  --query "[0].value" -o tsv)

# Get the connection string (needed by backend and uploader)
$FILESHARE_CONN_STR = (az storage account show-connection-string `
  --name $STORAGE_ACCT `
  --resource-group $RG `
  --query "connectionString" -o tsv)

# Create the file share
az storage share-rm create `
  --storage-account $STORAGE_ACCT `
  --resource-group $RG `
  --name $FILE_SHARE `
  --quota 10
```

---

## Step 3 — Log Analytics Workspace

Required by the Container Apps Environment.

```powershell
az monitor log-analytics workspace create `
  --resource-group $RG `
  --workspace-name $LAW `
  --location $LOCATION

$LAW_CLIENT_ID = (az monitor log-analytics workspace show `
  --resource-group $RG `
  --workspace-name $LAW `
  --query "customerId" -o tsv)

$LAW_KEY = (az monitor log-analytics workspace get-shared-keys `
  --resource-group $RG `
  --workspace-name $LAW `
  --query "primarySharedKey" -o tsv)
```

---

## Step 4 — Container Apps Environment (with File Share mount)

```powershell
# Create the environment
az containerapp env create `
  --name $CAE `
  --resource-group $RG `
  --location $LOCATION `
  --logs-workspace-id $LAW_CLIENT_ID `
  --logs-workspace-key $LAW_KEY

# Add the File Share as a storage mount
az containerapp env storage set `
  --name $CAE `
  --resource-group $RG `
  --storage-name "fileshare" `
  --azure-file-account-name $STORAGE_ACCT `
  --azure-file-account-key $STORAGE_KEY `
  --azure-file-share-name $FILE_SHARE `
  --access-mode ReadWrite
```

---

## Step 5 — Container Apps Job

This single ACA Job runs both connector images (Airbyte) and the uploader image.
Every execution overrides the image via `begin_start()`.

```powershell
az containerapp job create `
  --name $CAJ `
  --resource-group $RG `
  --environment $CAE `
  --trigger-type Manual `
  --replica-timeout 1800 `
  --replica-retry-limit 0 `
  --parallelism 5 `
  --replica-completion-count 1 `
  --image "mcr.microsoft.com/azurelinux/base/core:3.0" `
  --cpu "1.0" `
  --memory "2.0Gi" `
  --command "/bin/sh" "-c" "echo placeholder" `
  --env-vars "AIRBYTE_ENABLE_UNSAFE_CODE=true"
```

> **Note:** The base image is a placeholder. Every execution overrides the image
> to the actual Airbyte connector or the uploader. `parallelism=5` allows up to
> 5 concurrent executions (onboarding checks don't queue behind long reads).

Now add the File Share volume mount to the job:

```powershell
# Export current YAML, add volume mount, re-apply
az containerapp job show `
  --name $CAJ `
  --resource-group $RG `
  -o yaml > caj-connector-v2.yaml
```

Edit `caj-connector-v2.yaml` to add the volume mount under `template`:

```yaml
template:
  containers:
    - name: connector
      # ... existing fields ...
      volumeMounts:
        - volumeName: fileshare
          mountPath: /data
  volumes:
    - name: fileshare
      storageName: fileshare
      storageType: AzureFile
```

Apply the updated YAML:

```powershell
az containerapp job update `
  --name $CAJ `
  --resource-group $RG `
  --yaml caj-connector-v2.yaml

# Clean up
Remove-Item caj-connector-v2.yaml
```

**Verify:** the job should show the volume mount:

```powershell
az containerapp job show `
  --name $CAJ `
  --resource-group $RG `
  --query "template.{containers:containers[0].volumeMounts, volumes:volumes}" `
  -o json
```

---

## Step 6 — Container Registry (for uploader image)

```powershell
az acr create `
  --name $ACR `
  --resource-group $RG `
  --sku Basic `
  --admin-enabled true
```

Grant the Container Apps Environment pull access:

```powershell
$ACR_SERVER   = "$ACR.azurecr.io"
$ACR_USERNAME = (az acr credential show --name $ACR --query "username" -o tsv)
$ACR_PASSWORD = (az acr credential show --name $ACR --query "passwords[0].value" -o tsv)

# Register ACR credentials with the Container Apps Environment
az containerapp env registry set `
  --name $CAE `
  --resource-group $RG `
  --server $ACR_SERVER `
  --username $ACR_USERNAME `
  --password $ACR_PASSWORD

# Also register ACR credentials on the ACA Job itself
# (required for the job to pull images from ACR)
az containerapp job registry set `
  --name $CAJ `
  --resource-group $RG `
  --server $ACR_SERVER `
  --username $ACR_USERNAME `
  --password $ACR_PASSWORD
```

---

## Step 7 — Build & Push the Sync Uploader Image

From the repo root:

```powershell
cd docker-image

# Build locally
docker build -t sync-uploader:latest -f Dockerfile.uploader .

# Tag for ACR
docker tag sync-uploader:latest "$ACR_SERVER/sync-uploader:latest"

# Login to ACR
az acr login --name $ACR

# Push
docker push "$ACR_SERVER/sync-uploader:latest"

cd ..
```

Alternatively, build directly in ACR (no local Docker needed):

```powershell
az acr build `
  --registry $ACR `
  --image sync-uploader:latest `
  --file docker-image/Dockerfile.uploader `
  docker-image/
```

---

## Step 8 — Supabase Schema Migration

Run this SQL in the Supabase SQL Editor to add the tracking columns:

```sql
-- Sync V2: tracking columns for the state-machine orchestrator
ALTER TABLE public.user_connector_configs
  ADD COLUMN IF NOT EXISTS discovered_catalog      JSONB,
  ADD COLUMN IF NOT EXISTS aca_execution_name       TEXT,
  ADD COLUMN IF NOT EXISTS aca_work_id              TEXT,
  ADD COLUMN IF NOT EXISTS consecutive_failures     INTEGER NOT NULL DEFAULT 0;

-- Index for the orchestrator to find configs in 'reading' or 'uploading' state
CREATE INDEX IF NOT EXISTS idx_ucc_sync_status
  ON public.user_connector_configs (last_sync_status)
  WHERE last_sync_status IN ('reading', 'uploading');
```

---

## Step 9 — Configure Your Local Backend

Add these to your `backend/.env` (or export as environment variables):

```bash
# ── Sync V2: ACA Job settings ────────────────────────────
ONBOARDING_DOCKER_ENABLED=1
ONBOARDING_DOCKER_EXECUTION_MODE=azure_job

# Single ACA Job (used by both onboarding and sync)
ACA_SUBSCRIPTION_ID=<your-subscription-id>
ACA_RESOURCE_GROUP=rg-purelybi-sync-v2-dev-ci
ACA_JOB_NAME=caj-purelybi-connector-v2-dev-ci
ACA_JOB_CONTAINER_NAME=connector

# File Share (for config/output exchange)
AZURE_FILE_SHARE_CONN_STR=<output of $FILESHARE_CONN_STR from Step 2>
AZURE_FILE_SHARE_NAME=connector-data-v2

# Blob Storage (existing — for Parquet output)
AZURE_STORAGE_CONNECTION_STRING=<your-blob-storage-connection-string>
AZURE_STORAGE_CONTAINER=raw

# Sync uploader image (ACR)
SYNC_UPLOADER_IMAGE=acrpurelybiv2devci.azurecr.io/sync-uploader:latest
```

**Azure authentication for local dev:**

```powershell
# Ensure you're logged in — DefaultAzureCredential uses this
az login

# Verify access
az containerapp job show `
  --name caj-purelybi-connector-v2-dev-ci `
  --resource-group rg-purelybi-sync-v2-dev-ci `
  --query "name" -o tsv
```

This should print `caj-purelybi-connector-v2-dev-ci`. If it does, your local backend can invoke ACA Jobs directly — no Docker Desktop needed.

---

## Step 10 — Configure the Orchestrator Azure Function

Create a new Function App (or add settings to an existing one):

```powershell
# If creating a new Function App:
$FUNC_STORAGE = "sapurelybifuncv2devci"
$FUNC_APP     = "func-purelybi-sync-orchestrator-v2-dev-ci"

az storage account create `
  --name $FUNC_STORAGE `
  --resource-group $RG `
  --location $LOCATION `
  --sku Standard_LRS

az functionapp create `
  --name $FUNC_APP `
  --resource-group $RG `
  --storage-account $FUNC_STORAGE `
  --consumption-plan-location $LOCATION `
  --runtime python `
  --runtime-version 3.11 `
  --functions-version 4 `
  --os-type Linux
```

Set the Function App settings:

```powershell
az functionapp config appsettings set `
  --name $FUNC_APP `
  --resource-group $RG `
  --settings `
    "SUPABASE_URL=$SUPABASE_URL" `
    "SUPABASE_SERVICE_ROLE_KEY=$SUPABASE_KEY" `
    "AZURE_SUBSCRIPTION_ID=$SUBSCRIPTION_ID" `
    "AZURE_RESOURCE_GROUP=rg-purelybi-sync-v2-dev-ci" `
    "ACA_JOB_NAME=caj-purelybi-connector-v2-dev-ci" `
    "ACA_JOB_CONTAINER_NAME=connector" `
    "AZURE_FILE_SHARE_CONN_STR=$FILESHARE_CONN_STR" `
    "AZURE_FILE_SHARE_NAME=connector-data-v2" `
    "AZURE_STORAGE_CONNECTION_STRING=$BLOB_CONN_STR" `
    "BLOB_CONTAINER_NAME=raw" `
    "SYNC_UPLOADER_IMAGE=acrpurelybiv2devci.azurecr.io/sync-uploader:latest"
```

Enable system-assigned managed identity and grant it the **Contributor** role
on the ACA Job (so it can call `begin_start()`):

```powershell
az functionapp identity assign `
  --name $FUNC_APP `
  --resource-group $RG

$FUNC_PRINCIPAL_ID = (az functionapp identity show `
  --name $FUNC_APP `
  --resource-group $RG `
  --query "principalId" -o tsv)

$CAJ_RESOURCE_ID = (az containerapp job show `
  --name $CAJ `
  --resource-group $RG `
  --query "id" -o tsv)

az role assignment create `
  --assignee $FUNC_PRINCIPAL_ID `
  --role "Contributor" `
  --scope $CAJ_RESOURCE_ID
```

Deploy the orchestrator:

```powershell
cd azure-function-sync-orchestrator
func azure functionapp publish $FUNC_APP --python
cd ..
```

---

## Step 11 — Smoke Test (Local)

### Test 1: Start a connector check via ACA from your local machine

```python
# test_aca_check.py — run from the backend venv
import json, os, time
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.storage.fileshare import ShareDirectoryClient, ShareFileClient
from azure.core.exceptions import ResourceExistsError

SUB_ID = os.environ["ACA_SUBSCRIPTION_ID"]
RG     = os.environ["ACA_RESOURCE_GROUP"]
JOB    = os.environ["ACA_JOB_NAME"]
CONTAINER = os.environ["ACA_JOB_CONTAINER_NAME"]

SHARE_CONN = os.environ["AZURE_FILE_SHARE_CONN_STR"]
SHARE_NAME = os.environ["AZURE_FILE_SHARE_NAME"]

# 1. Write config to File Share
work_id = "smoke-test-001"
config = {"api_key": "test"}  # replace with real config

dir_client = ShareDirectoryClient.from_connection_string(
    conn_str=SHARE_CONN, share_name=SHARE_NAME, directory_path=work_id
)
try:
    dir_client.create_directory()
except ResourceExistsError:
    pass

file_client = ShareFileClient.from_connection_string(
    conn_str=SHARE_CONN, share_name=SHARE_NAME, file_path=f"{work_id}/config.json"
)
file_client.upload_file(json.dumps(config).encode())

# 2. Start ACA Job with connector image
cred = DefaultAzureCredential()
client = ContainerAppsAPIClient(cred, SUB_ID)

shell = f"$AIRBYTE_ENTRYPOINT check --config /data/{work_id}/config.json > /data/{work_id}/output.jsonl 2>/data/{work_id}/stderr.log || true"

result = client.jobs.begin_start(
    resource_group_name=RG,
    job_name=JOB,
    template={"containers": [{
        "name": CONTAINER,
        "image": "airbyte/source-faker:latest",  # lightweight test connector
        "command": ["/bin/sh"],
        "args": ["-c", shell],
        "env": [{"name": "AIRBYTE_ENABLE_UNSAFE_CODE", "value": "true"}],
    }]},
).result()

exec_name = getattr(result, "name", "")
print(f"Execution started: {exec_name}")

# 3. Poll until done
for _ in range(60):
    time.sleep(5)
    ex = client.job_execution(resource_group_name=RG, job_name=JOB, job_execution_name=exec_name)
    status = getattr(getattr(ex, "properties", None), "status", "") or ""
    print(f"  Status: {status}")
    if status.lower() in ("succeeded", "failed"):
        break

# 4. Read output
out_client = ShareFileClient.from_connection_string(
    conn_str=SHARE_CONN, share_name=SHARE_NAME, file_path=f"{work_id}/output.jsonl"
)
print("\n--- output.jsonl ---")
print(out_client.download_file().readall().decode())
```

```powershell
cd backend
python test_aca_check.py
```

If you see `CONNECTION_STATUS` with `SUCCEEDED` in the output, everything is wired correctly.

---

## Resource Summary

| Resource | Name | Purpose |
|----------|------|---------|
| Resource Group | `rg-purelybi-sync-v2-dev-ci` | Contains all V2 sync resources |
| Storage Account | `sapurelybisyncv2devci` | Hosts the File Share for ACA I/O |
| File Share | `connector-data-v2` | Config/output exchange with ACA containers |
| Log Analytics | `law-purelybi-sync-v2-dev-ci` | Container Apps Environment logs |
| CA Environment | `caenv-purelybi-sync-v2-dev-ci` | Hosts the ACA Job |
| CA Job | `caj-purelybi-connector-v2-dev-ci` | Runs connector + uploader images |
| Container Registry | `acrpurelybiv2devci` | Stores the sync-uploader image |
| Function App | `func-purelybi-sync-orchestrator-v2-dev-ci` | Orchestrator (timer, state machine) |
| Function Storage | `sapurelybifuncv2devci` | Required by Azure Functions runtime |

---

## Environment Variables Reference

| Variable | Used By | Description |
|----------|---------|-------------|
| `ACA_SUBSCRIPTION_ID` | Backend, Orchestrator | Azure subscription ID |
| `ACA_RESOURCE_GROUP` | Backend, Orchestrator | `rg-purelybi-sync-v2-dev-ci` |
| `ACA_JOB_NAME` | Backend, Orchestrator | `caj-purelybi-connector-v2-dev-ci` |
| `ACA_JOB_CONTAINER_NAME` | Backend, Orchestrator | `connector` |
| `AZURE_FILE_SHARE_CONN_STR` | Backend, Orchestrator, Uploader | File Share connection string |
| `AZURE_FILE_SHARE_NAME` | Backend, Orchestrator, Uploader | `connector-data-v2` |
| `AZURE_STORAGE_CONNECTION_STRING` | Orchestrator, Uploader | Blob Storage connection string |
| `BLOB_CONTAINER_NAME` | Orchestrator, Uploader | `raw` |
| `SYNC_UPLOADER_IMAGE` | Orchestrator | `acrpurelybiv2devci.azurecr.io/sync-uploader:latest` |
| `SUPABASE_URL` | Orchestrator | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Orchestrator | Service role key |
