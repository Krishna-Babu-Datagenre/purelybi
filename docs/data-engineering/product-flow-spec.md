# Data Engineering Pipeline - Product and Flow Spec

## 1. Problem Statement
Users can sync source data, but cannot easily define reliable transform/extract/create workflows before analysis.
Current options are manual SQL or source-specific logic, which is slow and error-prone.

## 2. Goals and Non-Goals

### Goals
- Provide a recipe-style DE experience for non-technical users.
- Support prebuilt recipes (rename, replace, derived column, basic extraction).
- Support custom recipes through a dedicated DE agent when prebuilt recipes are insufficient.
- Validate quickly on sample data before execution.
- Execute DE as a separate ACA job only after upload success and only if the user has an active DE pipeline.
- Query transformed data by default, with safe fallback to raw data.

### Non-Goals (v1)
- Full dbt-style project management.
- Arbitrary user-supplied code execution without guardrails.
- Real-time streaming transforms.

## 3. Functional Requirements
- User can create one or more DE pipelines per connector dataset.
- User can add, reorder, enable/disable, and delete recipe steps.
- User can run sample validation and view output preview + errors.
- System runs DE pipeline asynchronously after successful upload completion.
- System skips DE trigger if no active pipeline exists in Supabase.
- Chat and dashboard read transformed data first; fallback to raw when transformed data is unavailable.
- Full run history is visible (status, started_at, ended_at, error summary).

## 4. User Journeys

### Journey A - Configure Prebuilt Pipeline
1. User opens Data Engineering page for a connected dataset.
2. User selects prebuilt recipe.
3. User chooses target columns and recipe parameters.
4. User adds step to pipeline and saves.
5. User runs sample validation and reviews preview.
6. User activates pipeline.

### Journey B - Use Custom Recipe (DE Agent)
1. User describes required transformation in natural language.
2. DE agent proposes step logic and generated recipe config.
3. User reviews and confirms.
4. User runs sample validation.
5. User saves and activates.

### Journey C - Automatic Post-Sync Execution
1. Connector upload phase completes successfully.
2. Orchestrator checks Supabase for active DE pipeline for that dataset.
3. If active pipeline exists, trigger separate DE ACA job.
4. Materialized transformed output is written to transformed container/prefix.
5. Run status is persisted and visible in UI.

## 5. UI Behavior (Concise)
- Pipeline Builder: step list + reorder + enable/disable.
- Recipe Catalog: prebuilt + custom(DE agent).
- Validation Panel: input sample, output sample, warnings, errors.
- Run History: latest status and recent runs.
- Clear state labels: Draft, Active, Running, Succeeded, Failed.

## 6. Edge Cases and Failure Scenarios
- Pipeline exists but invalid schema after source changes: mark run failed with actionable step error.
- Transformed output missing for a dataset: auto fallback to raw query path.
- DE job timeout: mark failed, do not block raw-data analytics.
- Partial step failure: atomic publish rule (do not publish partial transformed output).

## 7. Roles and Permissions
- Tenant user: manage only own pipelines and runs.
- System jobs (orchestrator/runner): service-role access scoped by tenant metadata.
- No cross-tenant pipeline visibility.

## 8. Acceptance Criteria
- A successful upload triggers DE job only when active pipeline exists.
- If no active pipeline exists, no DE job is started.
- Pipeline validation runs on sample rows and reports per-step errors.
- Chat/dashboard defaults to transformed data and falls back to raw in under one request cycle.
- Every DE run is traceable in Supabase with status and error details.
