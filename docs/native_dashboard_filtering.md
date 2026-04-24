# Native Dashboard Filtering — Implementation Plan

## 1. Context & Goal

Purely BI already renders dashboards whose widgets are backed by SQL queries
executed against a tenant's DuckDB sandbox (views over Parquet in Azure Blob).
Today, filtering is limited to a hardcoded allow-list in
`widget_data_service.py` (`_ALLOWED_FILTERS`, `_DATE_COLUMNS`) and a
best-effort `_inject_where` text splice.

We want **Apache Superset-style native dashboard filtering**:

- A filter pane on each dashboard.
- Filters are translated into DuckDB SQL clauses and applied to **every**
  widget on the dashboard automatically, without re-authoring widget SQL.
- Filter options are driven by a **rich metadata layer** (table / column
  descriptions, data types, semantic types, relationships) which is
  LLM-generated and user-editable.
- Metadata is produced on demand by an **Azure Container Apps (ACA) job**
  triggered from the UI after at least one data source has synced.

### Supported filter types (v1)

| Kind | UI | SQL effect |
|---|---|---|
| Time | preset (7/14/30d), or custom range | `col BETWEEN $from AND $to` |
| Categorical | table → column → multi-select of unique values | `col IN (...)` |
| Numeric range | table → column → min/max | `col BETWEEN $min AND $max` |

### Non-goals (v1)

- Cross-filtering via chart clicks.
- Hierarchical / cascading filters.
- Per-widget filter overrides (all filters apply to all applicable widgets).
- Saved filter sets per dashboard.

---

## 2. Architecture Overview

```
┌─────────────────────────┐      ┌────────────────────────────┐
│ Frontend Dashboard UI   │──┐   │ Metadata Review UI         │
│  - Filter pane          │  │   │  - Edit table/col/rel rows │
└─────────────────────────┘  │   └────────────┬───────────────┘
          │ filter_spec      │                │
          ▼                  │                ▼
┌─────────────────────────┐  │   ┌────────────────────────────┐
│ /widgets/hydrate (POST) │  │   │ /metadata/* CRUD endpoints │
│  widget_data_service    │  │   └────────────┬───────────────┘
│   1. resolve metadata   │  │                │
│   2. build_filter_views │  │                ▼
│   3. run widget SQL     │  │   ┌────────────────────────────┐
└────────────┬────────────┘  │   │ Supabase metadata tables   │
             │               │   │  tenant_table_metadata     │
             ▼               │   │  tenant_column_metadata    │
        DuckDB sandbox       │   │  tenant_table_relationships│
             ▲               │   └────────────┬───────────────┘
             │               │                ▲
             │               └── trigger ─────┤
                                              │
                                 ┌────────────┴──────────────┐
                                 │ ACA job: metadata_generator│
                                 │  - inspect DuckDB schema   │
                                 │  - LLM describe + relate   │
                                 │  - upsert metadata rows    │
                                 └───────────────────────────┘
```

### Core design decision — filtered views, not WHERE splicing

Widget SQL is left **untouched**. For each hydration request we register
per-request `TEMP VIEW`s that shadow the base tables, embedding the filters.
DuckDB resolves the view inside CTEs, subqueries, joins, and unions,
guaranteeing correctness regardless of widget SQL complexity.

```sql
CREATE OR REPLACE TEMP VIEW shopify_orders AS
SELECT * FROM _raw.shopify_orders
WHERE created_at BETWEEN $from AND $to
  AND billing_country IN ('US','CA');
```

Widget SQL referencing `shopify_orders` is automatically filtered. For
filters on related (but not directly referenced) tables, we rewrite the view
with a semi-join using the relationship graph.

---

## 3. Metadata Model

### 3.1 Supabase tables

```sql
create table tenant_table_metadata (
  tenant_id        uuid not null,
  table_name       text not null,
  description      text,
  primary_date_column text,
  grain            text,          -- free-form, e.g. "row = order"
  generated_at     timestamptz,
  edited_by_user   boolean default false,
  primary key (tenant_id, table_name)
);

create table tenant_column_metadata (
  tenant_id        uuid not null,
  table_name       text not null,
  column_name      text not null,
  data_type        text not null, -- from DuckDB DESCRIBE
  semantic_type    text not null, -- categorical|numeric|temporal|identifier|measure|unknown
  description      text,
  is_filterable    boolean default true,
  cardinality      int,           -- cached distinct count
  sample_values    jsonb,         -- small array for categorical hints
  generated_at     timestamptz,
  edited_by_user   boolean default false,
  primary key (tenant_id, table_name, column_name)
);

create table tenant_table_relationships (
  tenant_id        uuid not null,
  from_table       text not null,
  from_column      text not null,
  to_table         text not null,
  to_column        text not null,
  kind             text not null, -- many_to_one | one_to_one | many_to_many
  confidence       numeric,       -- 0..1 from LLM
  edited_by_user   boolean default false,
  generated_at     timestamptz,
  primary key (tenant_id, from_table, from_column, to_table, to_column)
);
```

RLS: scope every row to `tenant_id = auth.uid()` (or the org equivalent
already in use).

### 3.2 Semantic types

- `categorical` — drives "IN (...)" filters; requires `cardinality` check.
- `numeric` — drives range filters.
- `temporal` — drives time filters; one per table may be elected as
  `primary_date_column`.
- `identifier` — FK/PK candidates; used only for relationship edges.
- `measure` — numeric but aggregated (e.g. `total_price`); still filterable.
- `unknown` — shown in review UI but hidden from filter pane.

---

## 4. Filter Spec (API Contract)

```jsonc
{
  "time": {
    "column_ref": { "table": "shopify_orders", "column": "created_at" },
    // one of:
    "preset": "last_30_days",        // last_7|last_14|last_30|last_90|ytd|mtd
    "range":  { "from": "2026-01-01", "to": "2026-04-01" }
  },
  "filters": [
    { "kind": "categorical",
      "column_ref": { "table": "shopify_orders", "column": "billing_country" },
      "op": "in",
      "values": ["US","CA"] },
    { "kind": "numeric",
      "column_ref": { "table": "meta_daily_insights", "column": "spend" },
      "op": "between",
      "min": 10, "max": 500 }
  ]
}
```

Every filter is explicitly anchored to `(table, column)` — no heuristics.

---

## 5. Filter-to-SQL Translation

Given a `FilterSpec` and the set of tables a widget scans:

1. **Detect widget's base tables** via `EXPLAIN (FORMAT JSON) <widget_sql>`
   (DuckDB), extracting `SCAN` / `SEQ_SCAN` relation names. Fallback:
   `sqlglot` AST walk.
2. For each filter on `(T_f, col_f)`:
   - If `T_f ∈ tables_used` → applies directly to `T_f`.
   - Else, BFS the `tenant_table_relationships` graph for a safe path from
     any `T ∈ tables_used` to `T_f`. Only traverse `many_to_one`/`one_to_one`
     edges in the direction from the scanning table toward `T_f` (prevents
     row fan-out). If path exists, rewrite as semi-join; else skip filter
     for this widget and log.
3. Collect per-table predicates and build one `TEMP VIEW` per affected
   base table:

   ```sql
   CREATE OR REPLACE TEMP VIEW {table} AS
   SELECT * FROM _raw.{table}
   WHERE {collected predicates, parameterized};
   ```

4. Execute widget SQL unchanged with the same connection / parameters.
5. Drop views at end of request (or rely on connection scope).

All values go through DuckDB parameter binding (`$name`) — never string
concatenation.

---

## 6. Metadata Generation Job

**Runtime:** ACA Job, Python, same base image as existing sync uploader.
**Trigger:** Frontend button → `POST /metadata/generate` → backend starts
ACA job via management SDK (mirror `azure-function-schema-updater` pattern).

**Job steps per tenant:**

1. Attach to tenant's DuckDB sandbox (read-only).
2. For each table:
   - `DESCRIBE {table}` → data types.
   - Sample up to N rows (`USING SAMPLE 1000 ROWS`).
   - `approx_count_distinct` per column for cardinality.
3. One LLM call per table producing: table description, per-column
   `{semantic_type, description}`, candidate `primary_date_column`.
4. One LLM call across tables producing relationship edges (name + sample
   overlap hints). Validate each edge with a cheap DuckDB join probe
   (`SELECT COUNT(*) FROM a JOIN b USING(col)`) to reject false positives.
5. Upsert into the three Supabase tables. `edited_by_user = true` rows are
   preserved (do not overwrite).

Progress written to `tenant_metadata_jobs` table for UI polling.

---

## 7. Task Groups

Each task is intended to be a reviewable PR-sized unit. Order within a group
matters; groups can overlap where noted.

### Group A — Metadata storage & API (backend)

- [x] **A1** — Supabase migration adding `tenant_table_metadata`, `tenant_column_metadata`, `tenant_table_relationships`, `tenant_metadata_jobs` under `backend/supabase/queries/`, including RLS policies.
- [x] **A2** — Pydantic models `TableMetadata`, `ColumnMetadata`, `Relationship`, `MetadataJob` in `fastapi_app/models/metadata.py`.
- [x] **A3** — `metadata_service.py` with CRUD against Supabase that preserves `edited_by_user` on upsert.
- [x] **A4** — Router `routers/metadata.py` exposing `GET/PATCH /metadata/tables`, `/metadata/columns`, `/metadata/relationships`, and `POST /metadata/generate`, using existing `auth_dep`.
- [x] **A5** — Unit tests for CRUD behavior and RLS scoping.

### Group B — Metadata generation job (ACA)

- [x] **B1** — Scaffold `azure-job-metadata-generator/` (Dockerfile, requirements, entrypoint) mirroring `docker-image/`.
- [x] **B2** — `inspect.py`: DuckDB schema + row samples + per-column cardinality.
- [x] **B3** — `llm_describe.py`: per-table description and per-column semantic type via `ai.llms` factory at temperature 0.
- [x] **B4** — `llm_relationships.py`: propose relationship edges and validate each with a DuckDB join probe.
- [x] **B5** — `upsert.py`: Supabase writes that respect `edited_by_user` and update `tenant_metadata_jobs` status.
- [x] **B6** — ACA job definition / `az containerapp job` provisioning script (documented in `docs/`), deployed to the sync-uploader resource group.
- [x] **B7** — Backend trigger: `POST /metadata/generate` starts the ACA job via SDK and returns `job_id`, mirroring the schema-updater pattern.
- [x] **B8** — Integration test running the job against a seeded tenant fixture.

### Group C — Filter engine (backend)

- [x] **C1** — `FilterSpec` and sub-models in `models/filters.py` with strict validation.
- [x] **C2** — `filter_engine/detect_tables.py`: DuckDB `EXPLAIN (FORMAT JSON)` parser with `sqlglot` AST fallback, returning `set[str]`; unit tests over CTE, subquery, join, UNION samples.
- [x] **C3** — `filter_engine/relationships.py`: BFS over the relationship graph with direction safety rules.
- [x] **C4** — `filter_engine/build_views.py`: emit `CREATE OR REPLACE TEMP VIEW` statements with parameterized predicates.  *(Implementation deviation: rewrites widget SQL via sqlglot to wrap each base-table reference in a filtered subquery — semantically equivalent to view shadowing but does not require mutating the shared sandbox or a `_raw.*` schema. Documented in module docstring.)*
- [x] **C5** — Integrate into `widget_data_service.hydrate_widget` so it accepts `filter_spec`, calls `build_views`, and executes widget SQL unchanged.
- [x] **C6** — Remove hardcoded `_ALLOWED_FILTERS` / `_DATE_COLUMNS`, replacing them with metadata lookups (keep a compatibility shim until the frontend migrates).
- [x] **C7** — Unit + integration tests against real DuckDB: direct filter, related-table filter, skipped filter, CTE widget.
- [x] **C8** — Perf check extending `backend/tests/perf_benchmark.py` to confirm view creation is negligible vs. query time.  *(Added `_bench_filter_engine` and `--filter-engine-only` flag. Offline 50k-row DuckDB microbenchmark: rewrite-only median ≈3 ms, end-to-end (rewrite + execute) ≈7 ms vs. 3 ms baseline — ≈3 ms absolute overhead, negligible against typical blob-backed widget query times. Report at `docs/benchmark_results/benchmark_filter_engine.md`.)*

### Group D — Dashboard hydration wiring

- [x] **D1** — Extend the dashboard hydrate endpoint payload to accept `filter_spec` (absent = no filters; backward compatible). *(`DashboardFilterRequest` in `routers/dashboards.py` now carries `filter_spec`, `preset`, `start_date`, `end_date`, `force_refresh`. `POST /{dashboard_id}/filtered` applies both legacy column-dict filters and native `FilterSpec` in one call; existing callers passing only `filters` keep working unchanged.)*
- [x] **D2** — Plumb `filter_spec` through `dashboard_service` → `widget_data_service`, building views once per request and reusing across widgets. *(`get_user_dashboard` and `refresh_dashboard` now accept `filter_spec`; `_load_relationships_for_tenant` loads `tenant_table_relationships` once per request and feeds `hydrate_widgets` — the same relationship list is reused across every widget inside the thread-pool.)*
- [x] **D3** — Include a hash of `filter_spec` in the `_PRESET_FILTER_CACHE` key (custom ranges continue to skip cache). *(Key is now `(widget_id, preset, filter_spec_hash)` with `_hash_filter_spec()` producing a stable BLAKE2b-8 digest; `None`/empty specs collapse to `"none"`. Custom ranges still bypass the cache because `filters_from_preset` is `None`.)*
- [x] **D4** — Telemetry: log applied and skipped filters per widget. *(`_apply_native_filter_spec` now emits `widget=<id> filters_applied=[tables] filters_skipped=[...]` at INFO on every hydration, regardless of success.)*

### Group E — Frontend: filter pane

- [x] **E1** — `services/metadataApi.ts`: typed client for metadata endpoints.
- [x] **E2** — `store/filterStore.ts`: store holding `FilterSpec` per dashboard.
- [x] **E3** — `components/FilterPane/TimeFilter.tsx`: presets + custom range picker.
- [x] **E4** — `components/FilterPane/CategoricalFilter.tsx`: table → column → multi-select; unique values fetched on demand via new `GET /metadata/values` endpoint (backed by DuckDB `SELECT DISTINCT ... LIMIT N`).
- [x] **E5** — `components/FilterPane/NumericFilter.tsx`: min/max inputs, optional slider for high-cardinality columns.
- [x] **E6** — `components/FilterPane/FilterPane.tsx`: orchestrator; "Apply" triggers dashboard re-hydrate with `filter_spec`.
- [ ] **E7** — E2E test applying time, categorical, and numeric filters across a multi-widget dashboard.

### Group F — Frontend: metadata review UI

- [x] **F1** — "Generate metadata" button on the data sources page, disabled until at least one source has a successful sync.
- [x] **F2** — Job status toast / panel polling `tenant_metadata_jobs`.
- [x] **F3** — `MetadataReview` page: tree view (Tables → Columns) with inline edits for description, semantic_type, and is_filterable.
- [x] **F4** — Relationship editor: table-pair list with confidence plus edit/delete/add.
- [x] **F5** — "Edited by user" badge; regeneration confirms it will overwrite unedited rows only.

### Group G — Observability & docs

- [x] **G1** — Structured log events for filter apply/skip and job lifecycle.
- [x] **G2** — Metrics for job duration, filter-apply latency, and widget-skip count.
- [x] **G3** — Update `backend/docs/fast_api/` with the new endpoints.
- [x] **G4** — Update `frontend/docs/ui_flows.md` with the filter and metadata flows.

---

## 8. Rollout Plan

1. Ship Groups A + B behind a `feature.metadata_v1` flag; seed metadata for
   internal tenants; validate LLM quality.
2. Ship Group C with `filter_spec=None` default (no behavior change).
3. Ship Groups D + E together behind `feature.dashboard_filters`.
4. Ship Group F; enable flags for all tenants.
5. Remove legacy `_ALLOWED_FILTERS` / `_DATE_COLUMNS` path after one release.

---

## 9. Open Questions / Risks

- **LLM quality for relationships.** Mitigated by join-probe validation +
  user edits, but could produce noise on wide schemas. Consider limiting
  relationship inference to identifier-typed columns with matching names.
- **View-swap collisions.** If a widget's SQL already references a table
  name that collides with a CTE, DuckDB resolves CTE first. Verify with
  tests; document as known limitation.
- **Cardinality-heavy categorical filters.** Cap distinct-value fetches at
  e.g. 500; fall back to type-ahead search endpoint for high-cardinality
  columns.
- **Multi-tenant sandbox lifecycle.** Temp views are connection-scoped —
  confirm the existing sandbox reuses connections per request (if not,
  build views at the start of each hydrate call).
- **Analyst agent alignment.** The SQL agent should read the same metadata
  to improve generated SQL. Out of scope for v1, but the metadata tables
  are shaped to support it.
