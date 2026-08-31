# DE Pipeline Runner

Container Apps Job image that executes a Data Engineering pipeline after a
successful sync upload.

## What it does

1. Loads ordered, enabled steps from `de_pipeline_steps`.
2. Reads raw Parquet files from the source prefix in blob storage.
3. Applies step transforms per file.
4. Writes transformed Parquet to a run-scoped output prefix.
5. Updates `de_pipeline_runs` and `de_dataset_materializations` in Supabase.

## Required env vars

- `USER_ID`
- `CONNECTOR_CONFIG_ID`
- `DE_PIPELINE_ID`
- `DE_RUN_ID`
- `AZURE_STORAGE_CONNECTION_STRING`
- `RAW_CONTAINER_NAME` (default `raw`)
- `TRANSFORMED_CONTAINER_NAME` (default `transformed`)
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Optional:

- `SYNC_WORK_ID`
- `CONNECTOR_DOCKER_IMAGE`
- `USER_DATA_BLOB_PREFIX` (default `user-data`)
- `TRANSFORMED_PREFIX`

## Build locally and push to ACR

```powershell
$REGISTRY_NAME = "acrpurelybiv2devci"
$IMAGE_NAME = "de-pipeline-runner"
$IMAGE_TAG = "latest"
$LOGIN_SERVER = "$REGISTRY_NAME.azurecr.io"

# 1) Authenticate Docker to your Azure Container Registry
az acr login --name $REGISTRY_NAME

# 2) Build image locally from repo root.
# IMPORTANT: use azure-job-de-pipeline-runner as the build context,
# because Dockerfile copies ./main.py and ./requirements.txt.
docker build --no-cache -f azure-job-de-pipeline-runner/Dockerfile -t "${IMAGE_NAME}:${IMAGE_TAG}" azure-job-de-pipeline-runner

# 3) Tag image for ACR
docker tag "${IMAGE_NAME}:${IMAGE_TAG}" "${LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}"

# 4) Push to ACR
docker push "${LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}"

# 5) Optional: verify tag exists in ACR
az acr repository show-tags --name $REGISTRY_NAME --repository $IMAGE_NAME --top 10 --orderby time_desc
```

## Supported step types (v1)

- `rename_columns` with `config_json.mapping`
- `replace_values` with `config_json.column`, `from`, `to`
- `derive_column` with `config_json.column`, `expression`
- `extract_regex` with `source_column`, `target_column`, `pattern`, `group`