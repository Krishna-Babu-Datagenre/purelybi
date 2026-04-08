# Templates API

Base prefix: `/api/templates`

No authentication required.

---

## GET `/api/templates`

List all available dashboard templates.

**Query param** (optional): `platforms=shopify,meta_ads`  
Filter to templates whose supported platforms overlap with the given comma-separated list.

**Response `200 OK`**
```json
[
  {
    "id": "a1000000-0000-0000-0000-000000000001",
    "slug": "shopify-orders",
    "name": "Shopify Orders Overview",
    "description": "Comprehensive view of Shopify order metrics ...",
    "platforms": ["shopify"],
    "tags": ["shopify", "orders", "e-commerce", "template"],
    "preview_image": null
  },
  {
    "id": "a2000000-0000-0000-0000-000000000002",
    "slug": "shopify-metaads",
    "name": "Shopify + Meta Ads Overview",
    "description": "A unified view of your Shopify store and Meta Ads ...",
    "platforms": ["shopify", "meta_ads"],
    "tags": ["shopify", "meta-ads", "e-commerce", "template"],
    "preview_image": null
  }
]
```

---

## GET `/api/templates/{slug}`

Get a single template with its full widget blueprints.

**Path param:** `slug` — e.g. `shopify-orders`

**Response `200 OK`**
```json
{
  "id": "a1000000-0000-0000-0000-000000000001",
  "slug": "shopify-orders",
  "name": "Shopify Orders Overview",
  "description": "...",
  "platforms": ["shopify"],
  "tags": ["shopify", "orders", "e-commerce", "template"],
  "preview_image": null,
  "is_active": true,
  "widgets": [
    {
      "id": "...",
      "template_id": "a1000000-...",
      "title": "Total Revenue",
      "type": "kpi",
      "layout": { "x": 0, "y": 0, "w": 3, "h": 2 },
      "chart_config": { "value": 0, "prefix": "₹", "icon": "revenue", "changeLabel": "all-time" },
      "data_config": { "source": "shopify_orders", "aggregation": "sum", "field": "total_price" },
      "sort_order": 1
    }
  ]
}
```

**Error `404`** — slug not found.

---

## Usage Flow

```
1. GET /api/templates?platforms=shopify,meta_ads   → show matching templates to user
2. User picks one → GET /api/templates/{slug}      → preview widget layout
3. User confirms  → POST /api/dashboards { template_slug }  → create user dashboard
```
