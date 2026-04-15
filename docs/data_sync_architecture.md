# Data Sync Architecture

How user-connected data sources get synced into Azure Blob Storage as Parquet.

---

## High-level flow

```
┌──────────────┐     ┌──────────────────────┐    ┌─────────────────────────────┐
│  Schema      │     │  Sync Orchestrator    │    │  Single ACA Job             │
│  Updater     │     │  (Function, 5 min)    │    │  (image overridden per      │
│  (Function)  │     │                       │    │   execution)                │
│              │     │  State machine:       │    │                             │
│  Airbyte OSS │     │  1. uploading →       │    │  Execution A:               │
│  registry    │──→  │     check uploader    │    │  airbyte/source-shopify     │
│  → Supabase  │     │  2. reading →         │──→ │  $AIRBYTE_ENTRYPOINT read   │
│  connector_  │     │     check connector   │    │                             │
│  schemas     │     │     → start uploader  │    │  Execution B:               │
└──────────────┘     │  3. eligible →        │    │  sync-uploader:latest       │
                     │     write config to   │    │  JSONL → Parquet → Blob     │
                     │     File Share →      │    │                             │
                     │     start connector   │    └──────────────┬──────────────┘
                     └──────────────────────┘                   │
                                                        Azure File Share
                                                        /data/{work_id}/
                                                          config.json
                                                          catalog.json
                                                          output.jsonl
                                                          stderr.log
                                                                │
                                                                ▼
                                                       Azure Blob Storage
                                                       (monthly Parquet)
```

---

## Execution model

All connectors — regardless of language (manifest, Python, Java) — run as official Airbyte Docker images on a **single ACA Job**. The job's base image is overridden per execution via `begin_start()` with container overrides.

There is no sync-worker middleman, no PyAirbyte, and no language-based routing.

### Two-phase sync

1. **Connector phase** — The orchestrator writes config + catalog to the File Share, starts the official Airbyte image with `$AIRBYTE_ENTRYPOINT read`. Output goes to `/data/{work_id}/output.jsonl`.
2. **Uploader phase** — When the connector succeeds, the orchestrator starts the sync-uploader image on the same ACA Job. The uploader reads JSONL from the File Share, converts to Parquet, and uploads to Blob Storage.

### Onboarding

The FastAPI backend uses the **same ACA Job** for onboarding operations (check, discover, read-probe), sharing `connector_runner.py` helpers. The only difference: onboarding polls inline (user is waiting) via `wait_for_execution()`, while scheduled syncs use the timer-driven state machine.

Local dev uses `docker run` subprocess for onboarding operations when `ONBOARDING_DOCKER_EXECUTION_MODE=local`.

---

## Components

### 1. Schema Updater (`azure-function-schema-updater/`)

Timer-triggered Azure Function (daily, 03:00 UTC). Fetches the Airbyte OSS connector registry and upserts source connector metadata into the Supabase `connector_schemas` table.

**Code:** `shared/connector_registry_sync.py`

| Env var | Required | Notes |
|---------|:--------:|-------|
| `SUPABASE_URL` | ✓ | |
| `SUPABASE_SERVICE_ROLE_KEY` | ✓ | |

### 2. Sync Orchestrator (`azure-function-sync-orchestrator/`)

Timer-triggered Azure Function (every 5 minutes). Runs a three-phase state machine on each tick:

| Phase | Checks status | On completion | Transitions to |
|-------|--------------|---------------|----------------|
| 1 | `uploading` | Uploader done → mark success, cleanup File Share | `success` or `failed` |
| 2 | `reading` | Connector done → start uploader | `uploading` or `failed` |
| 3 | eligible configs | Write config → start connector | `reading` |

**Code:** `sync_orchestrator_v2/__init__.py`

| Env var | Required | Default | Notes |
|---------|:--------:|---------|-------|
| `SUPABASE_URL` | ✓ | | |
| `SUPABASE_SERVICE_ROLE_KEY` | ✓ | | |
| `AZURE_SUBSCRIPTION_ID` | ✓ | | ACA management |
| `AZURE_RESOURCE_GROUP` | ✓ | | ACA management |
| `ACA_JOB_NAME` | ✓ | | Single ACA Job |
| `ACA_JOB_CONTAINER_NAME` | | `connector` | Must match ACA Job template |
| `AZURE_FILE_SHARE_CONN_STR` | ✓ | | File Share connection string |
| `AZURE_FILE_SHARE_NAME` | ✓ | | File Share name |
| `SYNC_UPLOADER_IMAGE` | ✓ | | ACR image for uploader |
| `AZURE_STORAGE_CONNECTION_STRING` | ✓ | | Blob Storage |
| `BLOB_CONTAINER_NAME` | | `raw` | Blob container |

**Eligibility logic (start sync vs skip):**

A config is eligible when:
- `is_active = true`
- `sync_validated = true`
- `last_sync_status` not in (`queued`, `reading`, `uploading`, `reauth_required`)

Then per config:
- `sync_mode = 'one_off'`: eligible only when `last_sync_at IS NULL` (and past `sync_start_at` if set)
- `sync_mode = 'recurring'`: eligible when `last_sync_at IS NULL` or elapsed minutes ≥ `sync_frequency_minutes`

**Circuit breaker:** configs with ≥ 5 consecutive failures are skipped.

**IAM:** Managed identity needs `Microsoft.App/jobs/start/action` on the ACA Job.

### 3. Sync Uploader (`docker-image/`)

Container image (`sync-uploader`) running on the same ACA Job as connectors. Started by the orchestrator after the connector phase completes.

Reads Airbyte JSONL from the File Share → converts to Parquet (pandas/pyarrow) → uploads to Blob Storage.

**Code:** `sync_uploader.py`, `Dockerfile.uploader`

**Docker image:** `acrpurelybiv2devci.azurecr.io/sync-uploader:latest`

Env vars are injected per execution by the orchestrator:

| Env var | Notes |
|---------|-------|
| `WORK_ID` | File Share directory for this sync run |
| `USER_ID` | Blob path prefix |
| `DOCKER_IMAGE` | Source connector name for Blob path |
| `AZURE_FILE_SHARE_CONN_STR` | File Share connection |
| `AZURE_FILE_SHARE_NAME` | File Share name |
| `AZURE_STORAGE_CONNECTION_STRING` | Blob upload |
| `BLOB_CONTAINER_NAME` | Blob container |

### 4. Backend / Onboarding (`backend/src/ai/agents/onboarding/`)

The onboarding agent uses the same ACA Job for check/discover/read-probe of connectors. Shared helpers in `connector_runner.py` provide:

- `start_connector_execution()` — start any Airbyte image on the ACA Job
- `start_uploader_execution()` — start the sync-uploader
- `poll_execution_status()` / `wait_for_execution()` — check/wait for completion
- `write_to_fileshare()` / `read_from_fileshare()` / `cleanup_fileshare()` — File Share I/O
- `parse_connection_status()` / `parse_catalog()` — Airbyte JSONL parsing

Routing in `docker_ops.py`:
- `ONBOARDING_DOCKER_EXECUTION_MODE=local` → `docker run` subprocess (dev)
- `ONBOARDING_DOCKER_EXECUTION_MODE=azure_job` → ACA Job via `connector_runner.py` (cloud)

Key settings in `backend/src/fastapi_app/settings.py`:

| Setting | Default | Notes |
|---------|---------|-------|
| `ONBOARDING_DOCKER_ENABLED` | `false` | Enable Docker check/discover |
| `ONBOARDING_DOCKER_EXECUTION_MODE` | `local` | `local` or `azure_job` |
| `ACA_SUBSCRIPTION_ID_V2` | falls back to `ONBOARDING_ACA_SUBSCRIPTION_ID` → `AZURE_SUBSCRIPTION_ID` | |
| `ACA_RESOURCE_GROUP_V2` | falls back to `ONBOARDING_ACA_RESOURCE_GROUP` → `AZURE_RESOURCE_GROUP` | |
| `ACA_JOB_NAME_V2` | falls back to `ONBOARDING_ACA_JOB_NAME` | |
| `ACA_JOB_CONTAINER_NAME_V2` | `connector` | |
| `AZURE_FILE_SHARE_CONN_STR` | falls back to `AZURE_STORAGE_CONNECTION_STRING` | |
| `AZURE_FILE_SHARE_NAME_V2` | `connector-data-v2` | |
| `SYNC_UPLOADER_IMAGE` | — | ACR image for uploader |

---

## Azure resources (dev)

| Resource | Type | Name |
|----------|------|------|
| Resource group | — | `rg-purelybi-sync-v2-dev-ci` |
| Schema updater | Function App | `func-purelybi-schema-updater-dev-ci` |
| Sync orchestrator | Function App | `func-purelybi-sync-orchestrator-v2-dev-ci` |
| ACA Job | Container Apps Job | `caj-purelybi-connector-v2-dev-ci` |
| Container registry | ACR | `acrpurelybiv2devci.azurecr.io` |
| Storage account | Blob + File Share | `sapurelybisyncv2devci` |
| File Share | Azure Files | `connector-data-v2` |
| CA environment | Container Apps Env | `caenv-purelybi-sync-v2-dev-ci` |

ACA Job config: `parallelism=5`, `replicaTimeout=1800`, volume mount `/data` → Azure File Share.

---

## Data storage layout (blob)

```
{container}/user-data/{user_id}/{connector_name}/{stream_name}/{YYYY-MM}.parquet
```

Monthly append: if the month file exists, download → append → overwrite. Otherwise create.

---

## Sync status lifecycle

| Status | Meaning |
|--------|---------|
| `reading` | Connector image running on ACA Job |
| `uploading` | Sync-uploader converting JSONL → Parquet → Blob |
| `success` | Parquet written to Blob |
| `failed` | Connector, uploader, or runtime failure |
| `reauth_required` | OAuth token refresh needs user re-auth |

---

## Environment variables by resource

### Function App — Sync Orchestrator (`func-purelybi-sync-orchestrator-v2-dev-ci`)

Set as App Settings in the Azure portal or via `az functionapp config appsettings set`.

| Env var | Required | Default | Notes |
|---------|:--------:|---------|-------|
| `SUPABASE_URL` | ✓ | | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | ✓ | | Server-only key |
| `AZURE_SUBSCRIPTION_ID` | ✓ | | For ACA management SDK |
| `AZURE_RESOURCE_GROUP` | ✓ | | Resource group containing the ACA Job |
| `ACA_JOB_NAME` | ✓ | | e.g. `caj-purelybi-connector-v2-dev-ci` |
| `ACA_JOB_CONTAINER_NAME` | | `connector` | Must match the ACA Job template |
| `AZURE_FILE_SHARE_CONN_STR` | ✓ | | File Share connection string |
| `AZURE_FILE_SHARE_NAME` | ✓ | | e.g. `connector-data-v2` |
| `SYNC_UPLOADER_IMAGE` | ✓ | | ACR image for uploader, e.g. `acrpurelybiv2devci.azurecr.io/sync-uploader:latest` |
| `AZURE_STORAGE_CONNECTION_STRING` | ✓ | | Blob Storage (passed to uploader) |
| `BLOB_CONTAINER_NAME` | | `raw` | Blob container (passed to uploader) |

### Function App — Schema Updater (`func-purelybi-schema-updater-dev-ci`)

| Env var | Required | Notes |
|---------|:--------:|-------|
| `SUPABASE_URL` | ✓ | |
| `SUPABASE_SERVICE_ROLE_KEY` | ✓ | |

### App Service — Backend / FastAPI

Configured via `backend/.env` locally or App Settings in Azure. Full reference: `backend/.env-example`.

| Env var | Required | Default | Notes |
|---------|:--------:|---------|-------|
| `AZURE_LLM_ENDPOINT` | ✓ | | Azure AI / Anthropic-compatible URL |
| `AZURE_LLM_API_KEY` | ✓ | | |
| `AZURE_LLM_NAME` | ✓ | | Deployment name |
| `AZURE_LLM_API_VERSION` | | | |
| `SUPABASE_URL` | ✓ | | |
| `SUPABASE_KEY` | ✓ | | Anon / public key |
| `SUPABASE_SERVICE_ROLE_KEY` | ✓ | | Server-only key |
| `AZURE_STORAGE_CONNECTION_STRING` | | | Blob + fallback for File Share |
| `BLOB_CONTAINER_NAME` | | `raw` | |
| `USER_DATA_BLOB_PREFIX` | | `users` | |
| `API_PUBLIC_BASE_URL` | | `http://127.0.0.1:8000` | OAuth redirect URI base |
| `ONBOARDING_FRONTEND_REDIRECT` | | `http://localhost:5173/data/connect` | Post-OAuth browser redirect |
| `ONBOARDING_DOCKER_ENABLED` | | `0` | Set `1` to enable Docker ops |
| `ONBOARDING_DOCKER_EXECUTION_MODE` | | `local` | `local` or `azure_job` |
| `ACA_SUBSCRIPTION_ID` | ★ | | Required when `azure_job` mode |
| `ACA_RESOURCE_GROUP` | ★ | | Required when `azure_job` mode |
| `ACA_JOB_NAME` | ★ | | Required when `azure_job` mode |
| `ACA_JOB_CONTAINER_NAME` | | `connector` | |
| `AZURE_FILE_SHARE_CONN_STR` | ★ | | Required when `azure_job` mode |
| `AZURE_FILE_SHARE_NAME` | | `connector-data-v2` | |
| `SYNC_UPLOADER_IMAGE` | | | Only needed if backend triggers syncs |
| `LOG_LEVEL` | | `INFO` | |
| `CORS_EXTRA_ORIGINS` | | | Comma-separated origins |

★ = required only when `ONBOARDING_DOCKER_EXECUTION_MODE=azure_job`

### Sync Uploader container (per-execution, injected by orchestrator)

These are **not** set in any App Settings — the orchestrator injects them as container env overrides on each ACA Job execution.

| Env var | Notes |
|---------|-------|
| `WORK_ID` | File Share directory for this sync run |
| `USER_ID` | Supabase user UUID (for Blob path) |
| `DOCKER_IMAGE` | Source connector image name (for Blob path) |
| `AZURE_FILE_SHARE_CONN_STR` | File Share connection string |
| `AZURE_FILE_SHARE_NAME` | File Share name |
| `AZURE_STORAGE_CONNECTION_STRING` | Blob Storage connection string |
| `BLOB_CONTAINER_NAME` | Blob container |

### Container Apps Job (`caj-purelybi-connector-v2-dev-ci`)

The ACA Job itself has **no static app settings**. All env vars are injected per execution via container overrides in `begin_start()`. The job only needs:

- Volume mount: `/data` → Azure File Share (`connector-data-v2`)
- Registry: ACR credentials for pulling `sync-uploader` image
- Managed identity: `Microsoft.App/jobs/start/action` granted to orchestrator + backend

---

## Deployment

| Component | Method |
|-----------|--------|
| Sync uploader image | `docker build -f Dockerfile.uploader .` → push to ACR → ACA Job pulls on next execution |
| Orchestrator | `.github/workflows/deploy-azure-function-sync-orchestrator.yml` |
| Schema updater | `.github/workflows/deploy-azure-function-schema-updater.yml` |
| Backend (FastAPI) | `.github/workflows/deploy-azure-app-service.yml` |

Provisioning guide for setting up all Azure resources from scratch: **[`docs/sync_v2_provisioning_guide.md`](sync_v2_provisioning_guide.md)**
