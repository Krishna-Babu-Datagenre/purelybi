# Data Engineering Progress

Last updated: 2026-05-16

## Completed

- Reviewed and aligned on scope from:
  - product-flow-spec.md
  - system-design.md
  - technical-implementation-plan.md
- Added Supabase DE schema migration:
  - backend/supabase/queries/11-de-pipeline-tables.sql
  - Includes tables: de_pipelines, de_pipeline_steps, de_pipeline_runs, de_dataset_materializations
  - Added indexes, RLS policies, and service_role write policies
- Integrated DE trigger in sync orchestrator upload-success flow:
  - azure-function-sync-orchestrator/sync_orchestrator_v2/__init__.py
  - Added feature-gated maybe_start_de_pipeline(...)
  - Creates de_pipeline_runs row and attempts ACA DE job start
  - Keeps sync success resilient (best-effort trigger)
- Added DE env settings in local example:
  - azure-function-sync-orchestrator/local.settings.json.example
- Created DE runner job scaffold:
  - azure-job-de-pipeline-runner/main.py
  - azure-job-de-pipeline-runner/requirements.txt
  - azure-job-de-pipeline-runner/Dockerfile
  - azure-job-de-pipeline-runner/README.md
- Built image locally and pushed to ACR:
  - acrpurelybiv2devci.azurecr.io/de-pipeline-runner:latest

## Azure Resource Status (dev)

- Subscription: sub-nonprod (3892ad52-b508-4b32-8a93-ac5a9c1712e4)
- Resource group: rg-purelybi-sync-v2-dev-ci
- Storage:
  - transformed container exists in sapurelybisyncv2devci
- Container Apps Job:
  - Name: caj-pbi-de-pipe-v1-dev-ci
  - Single container: connector
  - Image: acrpurelybiv2devci.azurecr.io/de-pipeline-runner:latest
  - Provisioning state: Succeeded
  - Registry configured with system identity + AcrPull role
- Sync orchestrator function app settings:
  - DE_PIPELINE_ACA_JOB_NAME=caj-pbi-de-pipe-v1-dev-ci
  - DE_PIPELINE_ACA_CONTAINER_NAME=connector
  - DE_PIPELINE_IMAGE=acrpurelybiv2devci.azurecr.io/de-pipeline-runner:latest
  - DE_TRANSFORMED_CONTAINER=transformed
  - DE_PIPELINE_ENABLED=false (intentional safe rollout)

## Remaining

- Apply the new Supabase SQL migration in the target DB environment.
- Implement DE API endpoints in backend (recipes/pipelines/steps/validate/runs).
- Implement transformed-first query resolver with raw fallback.
- Add unit/integration tests for trigger gating and runner behavior.
- Enable DE_PIPELINE_ENABLED=true after smoke validation.

## Notes

- Python workflow preference honored: uv run / uv add / uv remove.