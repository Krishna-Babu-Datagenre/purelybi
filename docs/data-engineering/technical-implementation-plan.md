# Data Engineering Pipeline - Technical Implementation Plan

## 1. API Contracts

### Backend endpoints (new)
- `GET /api/de/recipes` -> list prebuilt recipes.
- `POST /api/de/pipelines` -> create pipeline for dataset/config.
- `GET /api/de/pipelines/{pipeline_id}` -> fetch pipeline + steps.
- `PATCH /api/de/pipelines/{pipeline_id}` -> update active flag/name.
- `POST /api/de/pipelines/{pipeline_id}/steps` -> add/update ordered step.
- `POST /api/de/pipelines/{pipeline_id}/validate` -> sample validation preview.
- `GET /api/de/pipelines/{pipeline_id}/runs` -> list run history.

### Trigger contract (internal)
- Orchestrator -> DE trigger service:
  - input: `user_id`, `connector_config_id`, `sync_work_id`, `docker_image`.
  - behavior: start ACA job only if active pipeline exists.

## 2. Database Schema Changes (Supabase)

Add tables:
- `de_pipelines`
  - `id`, `user_id`, `connector_config_id`, `name`, `is_active`, `version`, timestamps.
- `de_pipeline_steps`
  - `id`, `pipeline_id`, `step_order`, `recipe_type`, `config_json`, `is_enabled`, timestamps.
- `de_pipeline_runs`
  - `id`, `pipeline_id`, `user_id`, `connector_config_id`, `trigger_source`, `status`, `started_at`, `ended_at`, `error`, `sync_work_id`.
- `de_dataset_materializations`
  - `id`, `user_id`, `connector_config_id`, `pipeline_id`, `last_success_run_id`, `status`, `output_prefix`, `updated_at`.

Indexes:
- `de_pipelines(user_id, connector_config_id, is_active)`
- `de_pipeline_runs(pipeline_id, started_at desc)`
- `de_dataset_materializations(user_id, connector_config_id)`

RLS:
- Tenant rows visible only to owner.
- Service-role bypass for orchestrator/runner writes.

## 3. Backend Implementation Details

### 3.1 Orchestrator integration
- In upload success path (same phase where sync status becomes success), call `maybe_start_de_pipeline(...)`.
- `maybe_start_de_pipeline` query:
  - find active pipeline for `(user_id, connector_config_id)`.
  - if absent -> skip.
  - if present -> create run row (`queued`) and start DE ACA job.

### 3.2 DE trigger service
- Pattern match `metadata_job_trigger.py`:
  - `de_pipeline_job_trigger.py` with image override and forwarded env vars.
- Required env vars:
  - `DE_PIPELINE_IMAGE`
  - `DE_PIPELINE_ACA_JOB_NAME`
  - `DE_PIPELINE_ACA_CONTAINER_NAME` (default `connector` pattern)
  - existing `ACA_SUBSCRIPTION_ID`, `ACA_RESOURCE_GROUP`

### 3.3 DE runner job (new folder)
- Suggested path: `azure-job-de-pipeline-runner/`.
- Responsibilities:
  - load active pipeline + steps.
  - read raw parquet from sync layout.
  - apply steps in order.
  - run data checks.
  - write transformed parquet to transformed prefix.
  - update run + materialization tables.

### 3.4 Query data source resolver
- Update dataset resolution in DuckDB sandbox discovery path.
- Resolver algorithm:
  1. read `de_dataset_materializations` for tenant/config (cache TTL, e.g. 60s).
  2. if status is ready -> probe transformed prefix first.
  3. if transformed missing/unreadable -> fallback raw and log `de_query_fallback_raw`.
  4. if no materialization row -> raw directly.

## 4. Frontend Implementation Details
- New `Data Engineering` page under data source management.
- Components:
  - recipe catalog panel.
  - pipeline step editor (order + enable/disable).
  - sample validation preview.
  - run history table.
- Keep interactions async; never block sync onboarding flow.

## 5. Agent and Workflow Logic
- Prebuilt recipes: strongly typed configs only.
- Custom DE agent:
  - generate recipe step config (not free-form code by default).
  - require validation pass before activation.
- Optional guarded code-mode (future): sandboxed runtime + static checks.

## 6. Error Handling and Retries
- Trigger failures: mark run `failed_to_start` with explicit reason.
- Runner step failures: mark run `failed`, include `failed_step_order` and short error.
- Retry policy:
  - orchestrator trigger: 1 retry max.
  - runner internal retries: per-step retry only for transient IO.
- Publish policy: atomic publish only on full success.

## 7. Feature Flags and Config
- `DE_PIPELINE_ENABLED` (global kill switch).
- `DE_QUERY_PREFERS_TRANSFORMED` (default true).
- `DE_VALIDATION_REQUIRED_FOR_ACTIVATION` (default true).

## 8. Testing Approach

### Unit
- pipeline existence check + trigger gating.
- step ordering and config validation.
- resolver fallback logic.

### Integration
- orchestrator upload-success -> conditional DE trigger.
- runner write path + materialization metadata updates.
- chat/dashboard transformed-first then raw fallback.

### E2E
- user creates pipeline, validates, activates.
- next successful sync triggers DE run.
- dashboard reflects transformed data.

## 9. Deployment Considerations
- Provision separate ACA job for DE runner (same provisioning style as metadata generator).
- Add backend app settings for DE trigger service.
- Add migration SQL under existing Supabase migration pattern.

### 9.1 Azure resources (used vs created)

Use existing resources (dev baseline names):
- Resource group: `rg-purelybi-sync-v2-dev-ci`
- Container Apps environment: `caenv-purelybi-sync-v2-dev-ci`
- Sync orchestrator Function App: `func-purelybi-sync-orchestrator-v2-dev-ci`
- Backend App Service (existing backend app)
- Storage account: `sapurelybisyncv2devci`
- Existing raw container: `raw`
- Existing ACR: `acrpurelybiv2devci.azurecr.io`

Create for this feature:
- Separate ACA Job: `caj-purelybi-de-pipeline-v1-dev-ci`
- DE runner image repo/tag in existing ACR:
  - `acrpurelybiv2devci.azurecr.io/de-pipeline-runner:latest`
- Transformed container (or dedicated top-level prefix):
  - preferred container: `transformed`
  - alternative prefix in `raw`: `transformed-data/`

Notes:
- Keep the same naming convention per environment (`-dev-ci`, `-stg`, `-prod`).
- If a dedicated backend app is used for DE APIs later, follow current backend naming pattern and keep it in the same resource group.
- Rollout sequence:
  1. schema + backend trigger (flag off)
  2. runner deployment
  3. query resolver fallback
  4. frontend UI
  5. enable flag for internal tenants
