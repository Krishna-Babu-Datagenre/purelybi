# Metadata Generator

Container Apps Job that produces the semantic metadata layer (table descriptions, column types, relationships) for native dashboard filtering.

Runs as a per-request execution on **Azure Container Apps Jobs**.

## Infrastructure

| Resource              | Value                                                      |
| --------------------- | ---------------------------------------------------------- |
| ACR                   | `acrpurelybiv2devci.azurecr.io`                            |
| Image                 | `acrpurelybiv2devci.azurecr.io/metadata-generator`         |
| Container Apps Job    | `metadata-generator-job`                                   |
| Resource Group        | `rg-purelybi-sync-v2-dev-ci`                               |
| Region                | Central India                                              |

## How Deployment Works

The backend web app (`app-purelybi-backend-dev-ci`) needs these app settings
to target the **dedicated** metadata-generator CAJ (without them, it falls
back to the shared sync job `caj-purelybi-connector-v2-dev-ci`):

```
METADATA_GENERATOR_IMAGE              = acrpurelybiv2devci.azurecr.io/metadata-generator:latest
METADATA_GENERATOR_ACA_JOB_NAME       = metadata-generator-job
METADATA_GENERATOR_ACA_CONTAINER_NAME = metadata-generator-job
```

When a user triggers metadata generation, the backend passes this image as a
**container override** to the Container Apps Job execution. This means the
image tag in the app setting is what actually controls which image runs — not
whatever is configured on the CAJ resource directly.

## Build, Push & Deploy

To ensure ACA pulls a fresh image when using the `:latest` tag, **delete the
old tag from ACR first**, then push. This forces a new digest lookup.

```powershell
# 1. Build
docker build --no-cache `
  -t acrpurelybiv2devci.azurecr.io/metadata-generator:latest `
  -f azure-job-metadata-generator/Dockerfile `
  azure-job-metadata-generator

# 2. Delete the old :latest tag from ACR (forces fresh pull)
az acr repository delete `
  --name acrpurelybiv2devci `
  --image metadata-generator:latest `
  --yes

# 3. Push the new :latest
docker push acrpurelybiv2devci.azurecr.io/metadata-generator:latest
```

No need to update any app settings or the CAJ — the next job execution will
pull the new image automatically.

> **Why delete first?** ACA caches pulled images by tag+digest. Simply
> overwriting `:latest` in ACR does not invalidate the cache. Deleting the
> tag ensures the old digest is gone, so ACA is forced to resolve and pull
> the new one.

## Environment Variables

The job expects these env vars (configured on the Container Apps Job):

| Variable                          | Required | Description                                      |
| --------------------------------- | -------- | ------------------------------------------------ |
| `USER_ID`                         | Yes      | Tenant / user ID for the generation run           |
| `JOB_ID`                          | Yes      | Job row ID for progress tracking                  |
| `AZURE_STORAGE_CONNECTION_STRING` | Yes      | Blob storage connection string (Parquet data)     |
| `SUPABASE_URL`                    | Yes      | Supabase REST endpoint                            |
| `SUPABASE_SERVICE_KEY`            | Yes      | Supabase service-role key                         |
| `AZURE_LLM_API_KEY`              | Yes      | Azure OpenAI API key                              |
| `AZURE_LLM_ENDPOINT`             | Yes      | Azure OpenAI endpoint URL                         |
| `AZURE_LLM_NAME`                 | No       | Deployment name (default: `gpt-4.1`)              |
| `AZURE_LLM_API_VERSION`          | No       | API version (default: `2024-12-01-preview`)       |
| `BLOB_CONTAINER_NAME`            | No       | Blob container (default: `raw`)                   |
| `DUCKDB_MEMORY_LIMIT`            | No       | DuckDB memory cap (default: `512MB`)              |

## Tunable Thresholds

These env vars control the hybrid heuristic + LLM relationship engine:

| Variable                                  | Default | Description                                         |
| ----------------------------------------- | ------- | --------------------------------------------------- |
| `METADATA_RELATIONSHIP_MIN_OVERLAP`       | `0.5`   | Minimum data overlap to accept a heuristic edge     |
| `METADATA_RELATIONSHIP_LLM_OVERLAP`       | `0.9`   | Stricter overlap for LLM-proposed edges             |
| `METADATA_RELATIONSHIP_MAX_EDGES`         | `40`    | Hard cap on total relationship edges returned       |
| `METADATA_RELATIONSHIP_AUTO_SCORE`        | `0.80`  | Composite score threshold for auto-approve          |
| `METADATA_RELATIONSHIP_NEAR_SCORE`        | `0.40`  | Composite score threshold for near-miss → LLM       |
| `METADATA_RELATIONSHIP_PK_TOLERANCE`      | `0.98`  | Distinct/rowcount ratio to infer PK candidate       |
| `METADATA_RELATIONSHIP_MIN_FK_CARD`       | `5`     | Minimum cardinality for FK candidate columns        |
| `METADATA_RELATIONSHIP_FUZZY_THRESHOLD`   | `0.88`  | Jaro-Winkler threshold for fuzzy name matching      |
| `METADATA_RELATIONSHIP_LLM_CATALOG_PARENTS` | `15` | Max parent tables per LLM catalog prompt            |
| `METADATA_SAMPLE_ROWS`                    | `1000`  | Rows sampled per table for inspection               |
| `METADATA_LLM_SAMPLE_ROWS`               | `25`    | Sample rows included in the LLM prompt              |
| `METADATA_LLM_COLUMN_BATCH`              | `20`    | Columns per LLM describe batch                      |
| `METADATA_MIN_TABLE_ROWS`                | `3`     | Tables with fewer distinct rows are skipped         |

## Running Tests

Tests live in `backend/tests/test_metadata_generator.py`:

```bash
cd backend
.\.venv\Scripts\python.exe -m pytest tests/test_metadata_generator.py -v
```
