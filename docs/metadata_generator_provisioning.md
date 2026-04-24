# Metadata Generator — Provisioning Guide

The metadata generator is a slim Python container that produces the LLM
semantic-metadata layer driving native dashboard filtering. It runs as an
Azure Container Apps Job, one execution per generation request.

See [docs/native_dashboard_filtering.md](./native_dashboard_filtering.md) §6.

## 1. Build & push the image

```pwsh
$ACR = "<your-acr>.azurecr.io"
docker build -t $ACR/metadata-generator:latest -f azure-job-metadata-generator/Dockerfile azure-job-metadata-generator
docker push $ACR/metadata-generator:latest
```

## 2. Provision the ACA Job

The generator can run on the **shared sync ACA Job** (recommended for
v1 — same RG, same managed identity, same Container Apps Environment as
`sync-uploader`) by overriding the image at execution time. The backend
defaults to that pattern when `METADATA_GENERATOR_ACA_JOB_NAME` is unset.

If you prefer a **dedicated job** (different RBAC, different scaling
defaults), create one:

```pwsh
$RG       = "<resource-group>"
$ENV      = "<container-apps-environment>"
$JOB_NAME = "metadata-generator-job"
$IMAGE    = "$ACR/metadata-generator:latest"

az containerapp job create `
  --name $JOB_NAME `
  --resource-group $RG `
  --environment $ENV `
  --trigger-type Manual `
  --replica-timeout 1800 `
  --replica-retry-limit 0 `
  --replica-completion-count 1 `
  --parallelism 1 `
  --image $IMAGE `
  --cpu 1 --memory 2Gi `
  --registry-server $ACR `
  --mi-system-assigned
```

Grant the job's managed identity:

- `Storage Blob Data Reader` on the user-data blob container
  (Parquet reads via DuckDB).
- AcrPull on the registry.

## 3. Configure the backend

Set these env vars on the FastAPI App Service:

| Name | Required | Notes |
| --- | --- | --- |
| `METADATA_GENERATOR_IMAGE` | yes | Full image ref, e.g. `myacr.azurecr.io/metadata-generator:latest` |
| `METADATA_GENERATOR_ACA_JOB_NAME` | optional | Defaults to `ACA_JOB_NAME` (shared sync job) |
| `METADATA_GENERATOR_ACA_CONTAINER_NAME` | optional | Defaults to `ACA_JOB_CONTAINER_NAME` (`connector`) |
| `ACA_SUBSCRIPTION_ID`, `ACA_RESOURCE_GROUP` | yes | Already set for sync v2 |

The trigger forwards these env vars into the container execution
(`backend/src/fastapi_app/services/metadata_job_trigger.py`):

- `USER_ID`, `JOB_ID` — assigned per execution
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` — Supabase REST writes
- `AZURE_STORAGE_CONNECTION_STRING`, `BLOB_CONTAINER_NAME` — DuckDB Parquet reads
- `AZURE_LLM_ENDPOINT`, `AZURE_LLM_API_KEY`, `AZURE_LLM_NAME` — LLM
- Optional tuning: `METADATA_SAMPLE_ROWS`, `METADATA_SAMPLE_VALUES`,
  `METADATA_CATEGORICAL_MAX`, `METADATA_RELATIONSHIP_MIN_OVERLAP`,
  `METADATA_RELATIONSHIP_MAX_EDGES`, `DUCKDB_MEMORY_LIMIT`.

## 4. Trigger a run

```http
POST /api/metadata/generate
Authorization: Bearer <user JWT>
```

Returns `202 Accepted` with the job row. Poll
`GET /api/metadata/jobs/{job_id}` for progress.

The container patches the job row with `running` → `succeeded`/`failed`
and progress percentages. Edited rows
(`edited_by_user = TRUE`) are preserved across re-runs.

## 5. Local manual run

For development you can run the container against a real tenant:

```pwsh
$env:USER_ID                       = "<uuid>"
$env:JOB_ID                        = "<uuid of pending job row>"
$env:SUPABASE_URL                  = "https://<project>.supabase.co"
$env:SUPABASE_SERVICE_ROLE_KEY     = "<service role key>"
$env:AZURE_STORAGE_CONNECTION_STRING = "<connection string>"
$env:AZURE_LLM_ENDPOINT            = "<endpoint>"
$env:AZURE_LLM_API_KEY             = "<key>"
$env:AZURE_LLM_NAME                = "<model name>"

cd azure-job-metadata-generator
pip install -r requirements.txt
python -m main
```
