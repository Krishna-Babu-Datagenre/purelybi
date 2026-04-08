# Azure Data Sync (Phase 6) - Quick Ops Doc

## Purpose

Production flow for:
- Connector schema refresh (Airbyte registry -> Supabase)
- Scheduled user sync orchestration (Supabase configs -> Container Apps Job)
- Worker execution (PyAirbyte -> Parquet in Blob)

## End-to-end flow (UI -> scheduled sync)

- User connects a source in the product UI (onboarding flow) and config is saved to `user_connector_configs` in Supabase.
- If onboarding validation passes, row is marked `sync_validated=true`.
- `func-purelybi-sync-orchestrator-dev-ci` runs every 5 minutes.
- It selects eligible connector configs and starts `caj-purelybi-data-sync-dev-ci` executions.
- Worker container reads config from Supabase, runs extraction, writes Parquet to blob, and updates sync status.

## Azure resources (dev baseline)

- Resource group: `rg-purelybi-dev-ci`
- Function App (orchestrator): `func-purelybi-sync-orchestrator-dev-ci`
  - Plan: Flex Consumption
  - Runtime: Python 3.12
- Function App (schema updater): `func-purelybi-schema-updater-dev-ci`
  - Plan: Flex Consumption
  - Runtime: Python 3.12
- ACR: `acrpurelybidevci` (`acrpurelybidevci.azurecr.io`)
- Storage account (data lake): `sapurelybidatalakedevci`
- Container Apps environment: `caenv-purelybi-dev-ci`
- Container Apps Job: `caj-purelybi-data-sync-dev-ci`

## Function responsibilities

- `func-purelybi-schema-updater-dev-ci`
  - Timer: daily (03:00 UTC)
  - Fetches Airbyte OSS registry
  - Upserts source schemas into Supabase `connector_schemas`

- `func-purelybi-sync-orchestrator-dev-ci`
  - Timer: every 2 hours
  - Finds eligible `user_connector_configs`
  - Starts Container Apps Job execution with per-run env (`SYNC_CONFIG_ID`, `SYNC_USER_ID`, `SYNC_CONNECTOR_NAME`)
  - Uses managed identity to call `Microsoft.App/jobs/start/action`

## Orchestrator start conditions

A config is eligible when all are true:
- `is_active = true`
- `sync_validated = true`
- `last_sync_status` is not `queued`
- `last_sync_status` is not `running`
- `last_sync_status` is not `reauth_required`
- `last_sync_at` is null, or elapsed minutes >= `sync_frequency_minutes`

## Sync status lifecycle

- `queued`: orchestrator accepted/start call succeeded
- `running`: worker process started and marked row running
- `success`: worker finished and stored output
- `failed`: worker/runtime failure or start failure handling
- `reauth_required`: token refresh requires user re-auth

## Data storage layout (blob)

Parquet path format:

`raw/user-data/{user_id}/{connector_name}/{stream_name}/{YYYY-MM}.parquet`

Example:

`raw/user-data/c5efc103-bb7f-42dd-ae10-527612b146d4/source-facebook-marketing/ads_insights/2026-04.parquet`

Monthly behavior:
- If month file exists: download + append new rows + overwrite same monthly file
- If month file does not exist: create it

## Required app settings (minimum)

- Orchestrator function app:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `AZURE_SUBSCRIPTION_ID`
  - `AZURE_RESOURCE_GROUP`
  - `ACA_JOB_NAME`
  - `ACA_JOB_CONTAINER_NAME`

- Schema updater function app:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`

- Container Apps Job container:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY` (secret ref preferred)
  - `AZURE_STORAGE_CONNECTION_STRING` (secret ref preferred)
  - `BLOB_CONTAINER_NAME`
  - `AIRBYTE_ENABLE_UNSAFE_CODE=true`

## Deployment notes

- Functions deploy via:
  - `.github/workflows/deploy-azure-function-schema-updater.yml`
  - `.github/workflows/deploy-azure-function-sync-orchestrator.yml`
- Worker image build/push/redeploy steps:
  - `docker-image/README.md`
