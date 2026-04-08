# Dashboards API

Base prefix: `/api/dashboards`

All endpoints require `Authorization: Bearer <access_token>`.

---

## POST `/api/dashboards/create`

Create a new blank dashboard.

**Request**
```json
{
  "name": "My Custom Dashboard",
  "description": "Optional description",
  "tags": ["sales", "custom"]
}
```
- `name` is required (min 1 char)
- `description` and `tags` are optional

**Response `201 Created`**
```json
{
  "id": "e3a1b2c3-...",
  "user_id": "4173f84e-...",
  "name": "My Custom Dashboard",
  "description": "Optional description",
  "tags": ["sales", "custom"],
  "source": "manual",
  "template_id": null,
  "connection_id": null,
  "created_at": "2026-03-14T10:00:00.000000+00:00",
  "updated_at": "2026-03-14T10:00:00.000000+00:00",
  "widgets": []
}
```

---

## POST `/api/dashboards`

Create a user dashboard from a template.

**Request**
```json
{ "template_slug": "shopify-orders" }
```

**Response `201 Created`** — new dashboard with widgets
**Response `200 OK`** — if the user already has a dashboard from this template, the existing one is returned

```json
{
  "id": "12980f6d-363c-45c8-9f63-08fb6afe28fe",
  "user_id": "4173f84e-...",
  "name": "Shopify Orders Overview",
  "description": "...",
  "tags": ["shopify", "orders", "e-commerce", "template"],
  "source": "template",
  "template_id": "a1000000-...",
  "connection_id": null,
  "created_at": "2026-03-12T01:22:23.107229+00:00",
  "updated_at": "2026-03-12T01:22:23.107229+00:00",
  "widgets": [
    {
      "id": "192bcdb2-...",
      "dashboard_id": "12980f6d-...",
      "title": "Total Revenue",
      "type": "kpi",
      "layout": { "x": 0, "y": 0, "w": 3, "h": 2 },
      "chart_config": { "value": 0, "prefix": "₹", "icon": "revenue" },
      "data_config": { "source": "shopify_orders", "aggregation": "sum", "field": "total_price" },
      "sort_order": 1
    }
  ]
}
```

**Error `404`** — template slug not found.

---

## GET `/api/dashboards`

List all dashboards for the authenticated user (metadata only, no widgets).

**Response `200 OK`**
```json
[
  {
    "id": "12980f6d-...",
    "name": "Shopify Orders Overview",
    "description": "...",
    "tags": ["shopify", "orders", "e-commerce", "template"],
    "source": "template",
    "template_id": "a1000000-...",
    "connection_id": null,
    "created_at": "2026-03-12T01:22:23.107229+00:00",
    "updated_at": "2026-03-12T01:22:23.107229+00:00"
  }
]
```

---

## GET `/api/dashboards/{dashboard_id}`

Get a single dashboard with all its widgets.

**Query Parameters (optional — date filtering)**

| Param | Type | Description |
|---|---|---|
| `preset` | string | Quick-select: `last_7_days`, `last_14_days`, `last_30_days`, `last_60_days`, `last_90_days` |
| `start_date` | string | Start date (ISO format, inclusive). Must be paired with `end_date`. |
| `end_date` | string | End date (ISO format, exclusive). Must be paired with `start_date`. |

If both `preset` and `start_date`/`end_date` are provided, `preset` takes priority.
When no filter params are provided, all data is returned (current behavior).

**Examples**
```
GET /api/dashboards/12980f6d-...?preset=last_7_days
GET /api/dashboards/12980f6d-...?start_date=2024-01-01&end_date=2024-02-01
GET /api/dashboards/12980f6d-...                        ← no filter (all data)
```

**Response `200 OK`**
```json
{
  "id": "12980f6d-...",
  "user_id": "4173f84e-...",
  "name": "Shopify Orders Overview",
  "widgets": [
    {
      "id": "192bcdb2-...",
      "title": "Total Revenue",
      "type": "kpi",
      "layout": { "x": 0, "y": 0, "w": 3, "h": 2 },
      "chart_config": { ... },
      "data_config": { ... },
      "data_snapshot": null,
      "data_refreshed_at": null,
      "sort_order": 1
    }
  ]
}
```

**Error `404`** — dashboard not found or not owned by the user.

---

## POST `/api/dashboards/{dashboard_id}/widgets`

Add a widget (e.g. an agent-generated chart) to an existing dashboard.

**Request**
```json
{
  "title": "Monthly Revenue",
  "type": "bar",
  "chart_config": {
    "tooltip": { "trigger": "axis" },
    "xAxis": { "type": "category", "data": ["Jan", "Feb", "Mar"] },
    "yAxis": { "type": "value" },
    "series": [{ "type": "bar", "data": [15000, 22000, 18000] }]
  },
  "layout": { "x": 0, "y": 0, "w": 6, "h": 4 }
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `title` | string | yes | Display title for the widget |
| `type` | string | yes | `"bar"`, `"line"`, `"pie"`, `"kpi"`, etc. |
| `chart_config` | object | yes | ECharts option object (or `KpiConfig` for `type="kpi"`) |
| `layout` | object | no | `{ x, y, w, h }` grid position. Defaults to `{ x:0, y:0, w:6, h:4 }` |

**Response `201 Created`** — the created widget
```json
{
  "id": "a7b8c9d0-...",
  "dashboard_id": "e3a1b2c3-...",
  "title": "Monthly Revenue",
  "type": "bar",
  "layout": { "x": 0, "y": 0, "w": 6, "h": 4 },
  "chart_config": { ... },
  "data_config": null,
  "data_snapshot": null,
  "data_refreshed_at": null,
  "sort_order": 0
}
```

**Error `404`** — dashboard not found or not owned by the user.

---

## POST `/api/dashboards/{dashboard_id}/refresh`

Force-refresh widget data for a dashboard (bypasses the 5-minute cache).

**Query Parameters (optional — date filtering)**

Same as `GET /api/dashboards/{dashboard_id}` — accepts `preset`, `start_date`, and `end_date`.

**Examples**
```
POST /api/dashboards/12980f6d-.../refresh?preset=last_30_days
POST /api/dashboards/12980f6d-.../refresh?start_date=2024-01-01&end_date=2024-02-01
POST /api/dashboards/12980f6d-.../refresh                   ← no filter (all data)
```

**Response `200 OK`** — same shape as `GET /api/dashboards/{dashboard_id}`, with freshly hydrated `data_snapshot` on each widget.

**Error `404`** — dashboard not found or not owned by the user.

---

## POST `/api/dashboards/{dashboard_id}/filtered`

Get a dashboard with arbitrary data filters applied. Supports date, category, numeric, and cross-filtering.

**Request**
```json
{
  "filters": [
    { "column": "created_at", "op": "between", "value": ["2024-01-01", "2024-02-01"] },
    { "column": "billing_country", "op": "in", "value": ["India", "US"] },
    { "column": "total_price", "op": "gte", "value": 500 },
    { "column": "gateway", "op": "eq", "value": "Razorpay" }
  ]
}
```

**Filter Object**

| Field | Type | Description |
|---|---|---|
| `column` | string | Column name to filter (must be in the allowlist for the widget's source table) |
| `op` | string | Operator: `eq`, `neq`, `in`, `not_in`, `gt`, `gte`, `lt`, `lte`, `between` |
| `value` | any | Filter value — single value for `eq`/`gt`/etc., array for `in`/`not_in`/`between` |

**Supported Operators**

| Operator | SQL Equivalent | Value Type | Example |
|---|---|---|---|
| `eq` | `=` | single | `{"column": "gateway", "op": "eq", "value": "Razorpay"}` |
| `neq` | `!=` | single | `{"column": "currency", "op": "neq", "value": "INR"}` |
| `gt` | `>` | number | `{"column": "total_price", "op": "gt", "value": 1000}` |
| `gte` | `>=` | number | `{"column": "total_price", "op": "gte", "value": 500}` |
| `lt` | `<` | number | `{"column": "spend", "op": "lt", "value": 100}` |
| `lte` | `<=` | number | `{"column": "spend", "op": "lte", "value": 50}` |
| `between` | `>= AND <` | [start, end] | `{"column": "created_at", "op": "between", "value": ["2024-01-01", "2024-02-01"]}` |
| `in` | `IN (...)` | array | `{"column": "billing_country", "op": "in", "value": ["India", "US"]}` |
| `not_in` | `NOT IN (...)` | array | `{"column": "financial_status", "op": "not_in", "value": ["refunded"]}` |

**Filter Behaviour**

- Filters are applied per-widget based on the widget's source table.
- If a filter column does not exist in a widget's source table, that filter is silently skipped for that widget.
- Example: a filter on `billing_country` applies to `shopify_orders` widgets but is ignored by `meta_campaign_insights` widgets.
- Filtered queries always bypass the cache and run fresh SQL.
- All filter values use parameterized queries (`?` placeholders) to prevent SQL injection.

**Response `200 OK`** — same shape as `GET /api/dashboards/{dashboard_id}`.

**Error `404`** — dashboard not found or not owned by the user.

**Error `400`** — invalid preset value.

---

## DELETE `/api/dashboards/{dashboard_id}`

Delete a dashboard and all its widgets.

**Response `204 No Content`** — dashboard deleted.

**Error `404`** — dashboard not found or not owned by the user.

---

## DELETE `/api/dashboards/{dashboard_id}/widgets/{widget_id}`

Delete a single widget from a dashboard.

**Response `204 No Content`** — widget deleted.

**Error `404`** — widget or dashboard not found / not owned by the user.

---

## Widget Model

| Field | Type | Notes |
|---|---|---|
| `id` | string (UUID) | Widget identifier |
| `title` | string | Display title |
| `type` | string | `"kpi"`, `"bar"`, `"line"`, `"area"`, `"pie"`, `"scatter"`, etc. |
| `layout` | object | `{ x, y, w, h }` — 12-column grid |
| `chart_config` | object | `KpiConfig` for `type="kpi"`, otherwise an ECharts option object |
| `data_config` | object | Describes the data source (backend use; not needed for rendering) |
| `data_snapshot` | object \| null | Live data merged into `chart_config` after refresh |
| `data_refreshed_at` | string \| null | ISO-8601 timestamp of last refresh |
| `sort_order` | number | Render order |

### KPI `chart_config` shape (`type = "kpi"`)
```json
{
  "value": 42000,
  "prefix": "₹",
  "suffix": null,
  "change": 12.5,
  "changeLabel": "vs last month",
  "icon": "revenue",
  "sparkline": [100, 120, 115, 130]
}
```
`icon` values: `"revenue"`, `"orders"`, `"aov"`, `"customers"`, `"generic"`

### Chart `chart_config` shape (`type != "kpi"`)
A raw [Apache ECharts option object](https://echarts.apache.org/en/option.html) — pass directly to `echartsInstance.setOption()`.

---

## Dashboard Sources

The `source` field on a dashboard indicates how it was created:

| Value | Meaning |
|---|---|
| `"template"` | Created from a dashboard template |
| `"manual"` | Created by the user from the UI |
| `"agent"` | Created by the AI agent |

---

## Adding Agent Charts to Dashboards — Flow

```
1.  User asks a question in chat → agent returns a chart (SSE `chart` event)
2.  UI shows "Add to Dashboard" with two options:
    a.  Existing dashboard → pick from GET /api/dashboards
    b.  New dashboard      → POST /api/dashboards/create { name }
3.  POST /api/dashboards/{id}/widgets { title, type, chart_config }
4.  Widget saved → user can view it in the dashboard
```

---

## Data Filtering

Dashboards support date-based and general-purpose data filtering at query time. Filters are injected into each widget's SQL query during hydration — no schema changes required.

### Quick Date Filters (GET query params)

Add `preset` or `start_date`+`end_date` to `GET /api/dashboards/{id}` or `POST .../refresh`:

```
GET /api/dashboards/{id}?preset=last_7_days
GET /api/dashboards/{id}?preset=last_30_days
GET /api/dashboards/{id}?start_date=2024-01-01&end_date=2024-02-01
```

Available presets: `last_7_days`, `last_14_days`, `last_30_days`, `last_60_days`, `last_90_days`.

### Advanced Filters (POST body)

For category, numeric, or cross-visual filtering, use `POST /api/dashboards/{id}/filtered`:

```json
{
  "filters": [
    { "column": "created_at", "op": "between", "value": ["2024-01-01", "2024-02-01"] },
    { "column": "billing_country", "op": "in", "value": ["India", "US"] }
  ]
}
```

### Filterable Columns

| Source Table | Filterable Columns |
|---|---|
| `shopify_orders` | `created_at`, `billing_country`, `shipping_country`, `gateway`, `financial_status`, `fulfillment_status`, `currency`, `total_price`, `net_sales` |
| `meta_daily_insights` | `date`, `spend`, `roas` |
| `meta_campaign_insights` | `campaign_name`, `spend`, `roas` |
| `meta_ad_insights` | `campaign_name`, `ad_name`, `spend`, `revenue` |
| `meta_adset_insights` | `campaign_name`, `adset_name`, `spend`, `revenue` |

### Date Column Mapping

Date-based filtering auto-resolves the correct column per table:

| Source Table | Date Column |
|---|---|
| `shopify_orders` | `created_at` |
| `meta_daily_insights` | `date` |
| `meta_campaign_insights` | *(no date column — date filters skipped)* |
| `meta_ad_insights` | *(no date column — date filters skipped)* |

### Caching Behaviour

- **No filters** → cache is used (5-minute TTL) and persisted to `data_snapshot`.
- **Filters active** → cache is bypassed, fresh SQL executes every time. Results are **not** written to `data_snapshot` (preserving the unfiltered baseline).

### Cross-Filtering

Cross-filtering (clicking a pie slice to filter the whole dashboard) is a frontend concern:

```
1.  User clicks "India" slice on "Orders by Country" pie chart
2.  Frontend reads the clicked dimension: billing_country = "India"
3.  Frontend adds to active filter set and calls:
    POST /api/dashboards/{id}/filtered
    { "filters": [{ "column": "billing_country", "op": "eq", "value": "India" }] }
4.  Backend re-hydrates all widgets with the filter applied
5.  Shopify widgets are filtered; Meta Ads widgets are unaffected
```
