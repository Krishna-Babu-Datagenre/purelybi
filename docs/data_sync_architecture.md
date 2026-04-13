# Data Sync Architecture

How user-connected data sources get synced into Azure Blob Storage as Parquet.

---

## High-level flow

```
┌──────────────┐     ┌──────────────────┐    ┌─────────────────────────┐
│  Schema      │     │  Sync            │    │  Container Apps Jobs    │
│  Updater     │     │  Orchestrator    │    │                         │
│  (Function)  │     │  (Function)      │    │  ┌───────────────────┐  │
│              │     │                  │    │  │  sync-worker      │  │
│  Airbyte OSS │     │  Supabase query  │--->│  │  (PyAirbyte)      │  │
│  registry    │---> |  → start ACA Job │    │  └───────────────────┘  │
│  → Supabase  │     │                  │    │                         │
│  connector_  │     │  Routes by       │    │  ┌───────────────────┐  │
│  schemas     │     │  language:       │    │  │  sync-worker      │  │
└──────────────┘     │  manifest → PyAi │    │  │  (docker_read)    │  │
                     │  java/py → Docker│--->│  │    ▼              │  │
                     └──────────────────┘    │  │  launches ------->│-----┐
                                             │  └───────────────────┘  │  │
                                             │                         │  │
                                             │  ┌───────────────────┐  │  │
                                             │  │ Official Airbyte  │◀---┘
                                             │  │ Docker image      │  │
                                             │  │ (File Share I/O)  │  │
                                             │  └───────────────────┘  │
                                             └─────────────────────────┘
                                                       │
                                                       ▼
                                              Azure Blob Storage
                                              (monthly Parquet)
```

---

## Connector language routing

The `connector_schemas` table has a `language` column populated by the schema updater from the Airbyte OSS registry.

| Language | Count (approx) | Execution path |
|----------|---------------:|----------------|
| `manifest_only` | ~490 | PyAirbyte (in-process, YAML manifest) |
| `python` | ~63 | Docker-native (official image) |
| `java` | ~24 | Docker-native (official image) |

Routing constant: `DOCKER_IMAGE_LANGUAGES = {"java", "python"}` (in orchestrator and backend settings).

**Why Docker-native?** PyAirbyte can only run manifest-only connectors natively. Python/Java connectors would need `pip install` or a JVM at runtime — instead we run the official pre-built Airbyte Docker image directly.

---

## Components

### 1. Schema Updater (`azure-function-schema-updater/`)

Timer-triggered Azure Function (daily, 03:00 UTC). Fetches the Airbyte OSS connector registry and upserts source connector metadata (including `language`) into the Supabase `connector_schemas` table.

**Code:** `shared/connector_registry_sync.py`

| Env var | Required | Notes |
|---------|:--------:|-------|
| `SUPABASE_URL` | ✓ | |
| `SUPABASE_SERVICE_ROLE_KEY` | ✓ | |

### 2. Sync Orchestrator (`azure-function-sync-orchestrator/`)

Timer-triggered Azure Function (every 2 hours). Queries Supabase for eligible configs, joins `connector_schemas.language`, and starts the appropriate ACA Job execution.

**Code:** `sync_orchestrator/__init__.py`

| Env var | Required | Default | Notes |
|---------|:--------:|---------|-------|
| `SUPABASE_URL` | ✓ | | |
| `SUPABASE_SERVICE_ROLE_KEY` | ✓ | | |
| `AZURE_SUBSCRIPTION_ID` | ✓ | | |
| `AZURE_RESOURCE_GROUP` | ✓ | | |
| `ACA_JOB_NAME` | ✓ | | Sync-worker ACA Job |
| `ACA_JOB_CONTAINER_NAME` | | `sync-worker` | |
| `ACA_DOCKER_JOB_NAME` | | `""` | Docker connector ACA Job |
| `ACA_DOCKER_JOB_CONTAINER_NAME` | | `connector` | |

**Routing logic:**
- If `language ∈ {"java", "python"}` and `ACA_DOCKER_JOB_NAME` is set → starts sync-worker with `SYNC_PHASE=docker_read` + `SYNC_DOCKER_IMAGE`
- Otherwise → starts sync-worker with default PyAirbyte path

**Eligibility logic (start sync vs skip):**

The orchestrator first filters to rows where:
- `is_active = true`
- `sync_validated = true`
- `last_sync_status NOT IN ('queued', 'running', 'reauth_required')`

Then per config:
- `sync_mode = 'one_off'`
  - eligible only when `last_sync_at IS NULL` (never synced yet)
  - if `sync_start_at` is set, only eligible when `now >= sync_start_at`
- `sync_mode = 'recurring'` (or missing/unknown; treated as recurring)
  - if `last_sync_at IS NULL`: eligible immediately (or waits for `sync_start_at` if set)
  - else: eligible when elapsed minutes since `last_sync_at` >= `sync_frequency_minutes`

This keeps one-off connectors from re-running forever, and recurring connectors on cadence.

**IAM:** Managed identity needs `Microsoft.App/jobs/start/action` on the sync-worker ACA Job (and the Docker connector ACA Job if used).

### 3. Sync Worker (`docker-image/`)

Container image (`data-sync-worker`) running in the sync-worker ACA Job. Two execution modes:

#### PyAirbyte path (default)

Runs when `SYNC_PHASE` is absent. Uses the `airbyte` Python package to install and run the connector in-process. Works for manifest-only connectors.

#### Docker-native path (`SYNC_PHASE=docker_read`)

Runs when orchestrator sets `SYNC_PHASE=docker_read`. Two-phase pipeline:

1. **Discover** — launches the official Airbyte image on the Docker connector ACA Job with `$AIRBYTE_ENTRYPOINT discover`. Reads the real catalog from the File Share output.
2. **Read** — builds a `ConfiguredAirbyteCatalog` from the real discover output (with full `json_schema`), then launches the image again with `$AIRBYTE_ENTRYPOINT read`. Parses JSONL output → Parquet → Blob.

Config and output are exchanged via an **Azure File Share** mounted on both jobs.

| Env var | Path | Required | Default | Notes |
|---------|------|:--------:|---------|-------|
| `SUPABASE_URL` | both | ✓ | | |
| `SUPABASE_SERVICE_ROLE_KEY` | both | ✓ | | |
| `AZURE_STORAGE_CONNECTION_STRING` | both | ✓ | | Blob upload |
| `BLOB_CONTAINER_NAME` | both | | `sync-output` | |
| `AIRBYTE_ENABLE_UNSAFE_CODE` | both | | `true` (in Dockerfile) | |
| `SYNC_CONFIG_ID` | both | ✓ | | Set by orchestrator |
| `SYNC_PHASE` | docker | ✓ | | `docker_read` |
| `SYNC_DOCKER_IMAGE` | docker | ✓ | | e.g. `airbyte/source-mongodb-v2:2.0.7` |
| `ACA_DOCKER_JOB_NAME` | docker | ✓ | | Docker connector ACA Job |
| `ACA_DOCKER_CONNECTOR_CONTAINER_NAME` | docker | | `connector` | |
| `AZURE_SUBSCRIPTION_ID` | docker | ✓ | | ACA management |
| `AZURE_RESOURCE_GROUP` | docker | ✓ | | ACA management |
| `DOCKER_OUTPUT_DIR` | docker | | `/output` | File Share mount point inside sync-worker |
| `DOCKER_JOB_TIMEOUT` | docker | | `900` | Max seconds per connector job |
| `DOCKER_JOB_POLL_INTERVAL` | docker | | `10` | Poll interval (seconds) |

**Docker image:** `acrpurelybidevci.azurecr.io/purelybi/data-sync-worker:latest`
**Dockerfile:** `docker-image/Dockerfile.worker` (Python 3.11, pip: `airbyte supabase azure-storage-blob requests azure-identity azure-mgmt-appcontainers`)

### 4. Docker Connector ACA Job

A separate ACA Job resource whose image is **overridden at runtime** with the official Airbyte connector image. The sync-worker launches it via the Azure Management SDK (`jobs.begin_start()` with container overrides).

- Runs `$AIRBYTE_ENTRYPOINT` (set inside every official Airbyte image)
- Config/catalog read from `/data/{config_id}/config.json` and `/data/{config_id}/catalog.json`
- Output written to `/data/{config_id}/output.jsonl`
- Azure File Share mounted at `/data`

No application env vars needed — only `AIRBYTE_ENABLE_UNSAFE_CODE=true` is injected per execution.

**IAM:** The sync-worker's managed identity needs Contributor on this job.

### 5. Backend / Onboarding (`backend/src/ai/agents/onboarding/`)

The onboarding agent also uses the Docker-native path for check/discover/read-probe of Java/Python connectors. Same two-phase approach but communicates via Azure File Share SDK instead of shared volume mount.

Key settings in `backend/src/fastapi_app/settings.py`:

| Setting | Default | Notes |
|---------|---------|-------|
| `ONBOARDING_DOCKER_ENABLED` | `false` | Enable Docker check/discover |
| `ONBOARDING_DOCKER_EXECUTION_MODE` | `local` | `local` or `azure_job` |
| `ONBOARDING_ACA_DOCKER_JOB_NAME` | falls back to `ACA_DOCKER_JOB_NAME` | |
| `AZURE_FILE_SHARE_NAME` | `connector-data` | For reading connector output |
| `DOCKER_IMAGE_LANGUAGES` | `{"java", "python"}` | Hardcoded set |

---

## Azure resources (dev)

| Resource | Type | Name |
|----------|------|------|
| Resource group | — | `rg-purelybi-dev-ci` |
| Schema updater | Function App | `func-purelybi-schema-updater-dev-ci` |
| Sync orchestrator | Function App | `func-purelybi-sync-orchestrator-dev-ci` |
| Sync-worker job | Container Apps Job | `caj-purelybi-data-sync-dev-ci` |
| Docker connector job | Container Apps Job | `caj-pbi-docker-connector-dev-ci` |
| Container registry | ACR | `acrpurelybidevci.azurecr.io` |
| Storage account | Blob + File Share | `sapurelybidatalakedevci` |
| File Share | Azure Files | `connector-data` |
| CA environment | Container Apps Env | `caenv-purelybi-dev-ci` |

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
| `queued` | Orchestrator accepted, ACA Job start succeeded |
| `running` | Worker started processing |
| `success` | Parquet written to blob |
| `failed` | Worker or runtime failure |
| `reauth_required` | OAuth token refresh needs user re-auth |

---

## Deployment

| Component | Method |
|-----------|--------|
| Sync worker image | `docker build -f Dockerfile.worker .` → push to ACR → ACA Job pulls on next execution |
| Orchestrator | `.github/workflows/deploy-azure-function-sync-orchestrator.yml` |
| Schema updater | `.github/workflows/deploy-azure-function-schema-updater.yml` |
| Backend (FastAPI) | `.github/workflows/deploy-azure-app-service.yml` |
