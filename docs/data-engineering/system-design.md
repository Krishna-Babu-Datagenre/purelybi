# Data Engineering Pipeline - System Design

## 1. Architecture Overview

New components follow existing patterns used by sync orchestration and metadata-generator jobs:

- Sync Orchestrator Function: detects successful upload completion.
- Supabase DE Metadata: stores pipeline config, active state, runs, and materialization pointers.
- DE Runner ACA Job (separate job): executes transform pipeline and writes curated output.
- Transformed Data Container/Prefix: curated parquet output per tenant/dataset.
- Query Resolver Layer (backend): routes chat/dashboard to transformed-first, raw-fallback.

## 2. End-to-End Data Flow

1. Existing sync flow runs connector read -> uploader -> status `success`.
2. On uploader success, orchestrator checks Supabase for active DE pipeline for the config/dataset.
3. If active pipeline exists, orchestrator starts DE Runner ACA job (image override pattern).
4. DE Runner loads raw parquet, applies recipe steps, validates schema/quality, writes transformed parquet.
5. DE Runner updates Supabase run + materialization metadata.
6. Chat/dashboard query resolver prefers transformed location when metadata indicates ready; otherwise raw.

## 3. Triggering Model (Required Behavior)
- Trigger point: post-upload success only.
- Trigger condition: active pipeline exists in Supabase.
- Trigger action: start separate DE ACA job.
- Trigger skip: no active pipeline, disabled pipeline, or pipeline has blocking validation status.

## 4. Query Routing Decision (Latency-Oriented)

Chosen model: metadata-first routing, storage fallback.

- First check Supabase materialization metadata (indexed, low payload, cacheable).
- If metadata says transformed is ready, use transformed prefix/container.
- If transformed discovery fails at runtime, immediately fallback to raw and record a warning event.

Why this model:
- Lower request latency than repeated blob existence probes.
- Better control plane visibility (explicit status vs implicit file existence).
- Works with existing in-process caching already used in sandbox discovery.

## 5. External Integrations
- Azure Container Apps Jobs API: start DE runner execution.
- Azure Blob Storage: read raw and write transformed parquet.
- Supabase: pipeline metadata, runs, materialization state.

## 6. Security and Access Boundaries
- Tenant isolation by user_id in metadata and data prefixes.
- Service-role writes only from trusted backend/functions/jobs.
- DE runner receives minimal env vars (tenant/run ids + storage/supabase creds).

## 7. Scalability Considerations
- Separate DE job avoids contention with sync uploader workloads.
- Concurrency controls per ACA job (`parallelism`, timeout, retries).
- Idempotency key per run to avoid duplicate materialization for same sync event.
- Partitioned transformed outputs for efficient DuckDB reads.

## 8. Observability
- Structured events:
  - `de_trigger_checked`
  - `de_trigger_started`
  - `de_run_succeeded`
  - `de_run_failed`
  - `de_query_fallback_raw`
- Metrics:
  - trigger rate, start skip rate, run success rate, p95 run duration, fallback rate.
- Trace correlation:
  - sync config id + work_id + de_run_id.
