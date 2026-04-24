# Metadata API

Endpoints for managing the dashboard-filter metadata layer: table/column
metadata, relationships, column values, and generation jobs.

All endpoints require `Authorization: Bearer <access_token>`.

---

## Tables

### `GET /api/metadata/tables`

List all table metadata rows for the authenticated user.

**Response:** `TableMetadata[]`

```json
[
  {
    "user_id": "uuid",
    "table_name": "shopify_orders",
    "description": "All Shopify orders",
    "primary_date_column": "created_at",
    "grain": "row = order",
    "generated_at": "2026-04-20T12:00:00Z",
    "edited_by_user": false
  }
]
```

### `PATCH /api/metadata/tables/{table_name}`

Update a table metadata row. Flags it as user-edited (preserved on regeneration).

**Body:**
```json
{
  "description": "Updated description",
  "primary_date_column": "order_date",
  "grain": "row = order line item"
}
```

---

## Columns

### `GET /api/metadata/columns`

List column metadata. Optional `?table=shopify_orders` filter.

**Response:** `ColumnMetadata[]`

```json
[
  {
    "user_id": "uuid",
    "table_name": "shopify_orders",
    "column_name": "billing_country",
    "data_type": "VARCHAR",
    "semantic_type": "categorical",
    "description": "Customer billing country",
    "is_filterable": true,
    "cardinality": 42,
    "sample_values": ["US", "CA", "GB"],
    "edited_by_user": false
  }
]
```

### `PATCH /api/metadata/columns/{table_name}/{column_name}`

Update a column. Flags as user-edited.

**Body:**
```json
{
  "semantic_type": "numeric",
  "description": "Total order value",
  "is_filterable": true
}
```

---

## Column Values (for Filter Dropdowns)

### `GET /api/metadata/values?table=...&column=...&limit=500`

Return distinct non-null values for a column, capped at `limit` (max 500).
Used by the categorical filter multi-select in the filter pane.

**Response:**
```json
{
  "table": "shopify_orders",
  "column": "billing_country",
  "values": ["AU", "CA", "DE", "GB", "US"]
}
```

---

## Relationships

### `GET /api/metadata/relationships`

List all relationship edges for the user.

**Response:** `Relationship[]`

```json
[
  {
    "from_table": "shopify_orders",
    "from_column": "customer_id",
    "to_table": "shopify_customers",
    "to_column": "id",
    "kind": "many_to_one",
    "confidence": 0.95,
    "edited_by_user": false
  }
]
```

### `POST /api/metadata/relationships`

Create or upsert a relationship edge.

**Body:**
```json
{
  "from_table": "shopify_orders",
  "from_column": "customer_id",
  "to_table": "shopify_customers",
  "to_column": "id",
  "kind": "many_to_one",
  "confidence": 1.0
}
```

### `PATCH /api/metadata/relationships/{from_table}/{from_column}/{to_table}/{to_column}`

Update a relationship kind.

### `DELETE /api/metadata/relationships/{from_table}/{from_column}/{to_table}/{to_column}`

Delete a relationship edge. Returns 204.

---

## Generation Jobs

### `GET /api/metadata/jobs/latest`

Most recently created metadata-generation job (or null).

### `GET /api/metadata/jobs/{job_id}`

Poll a single job by ID.

**Response:**
```json
{
  "id": "uuid",
  "status": "running",
  "progress": 0.5,
  "message": "Describing table 3/6…",
  "error": null,
  "started_at": "2026-04-20T12:00:00Z"
}
```

### `POST /api/metadata/generate` (202 Accepted)

Trigger a metadata generation run. Creates a pending job and starts the
ACA container job. Poll `/jobs/{job_id}` for progress.

**Response:**
```json
{
  "job": { "id": "uuid", "status": "pending", "progress": 0 }
}
```

---

## Native Dashboard Filtering (via FilterSpec)

### `POST /api/dashboards/{id}/filtered`

Apply native filters to a dashboard. Accepts both legacy column-dict
filters and the new `filter_spec`.

**Body:**
```json
{
  "filter_spec": {
    "time": {
      "column_ref": { "table": "shopify_orders", "column": "created_at" },
      "preset": "last_30_days"
    },
    "filters": [
      {
        "kind": "categorical",
        "column_ref": { "table": "shopify_orders", "column": "billing_country" },
        "op": "in",
        "values": ["US", "CA"]
      },
      {
        "kind": "numeric",
        "column_ref": { "table": "meta_daily_insights", "column": "spend" },
        "op": "between",
        "min": 10,
        "max": 500
      }
    ]
  }
}
```

Filters are applied per-widget via the filter engine. Widgets whose tables
don't contain a filtered column are returned unfiltered (skipped filters
logged in telemetry).
