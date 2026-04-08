# UI Flows

---

## Authentication

```
1.  POST /api/auth/signin        → get access_token + user profile
2.  Store access_token in memory (or short-lived cookie)
3.  Send as header on all authenticated requests:
      Authorization: Bearer <access_token>
4.  GET /api/auth/me             → restore session on page reload
```

---

## Dashboard from Template

```
1.  POST /api/auth/signin                        → get access_token
2.  GET /api/templates?platforms=shopify,meta_ads → recommend matching templates
3.  User selects "shopify-orders"
4.  POST /api/dashboards  { template_slug: "shopify-orders" }
5.  Receive user-owned dashboard + widgets (201)
6.  GET /api/dashboards                           → list all user dashboards
7.  GET /api/dashboards/{id}                      → open a specific dashboard
8.  React renders the dashboard from widgets array
```

---

## Dashboard Date Filtering

```
1.  Dashboard is open → user sees filter bar in top-right corner
2.  Quick-select buttons: [Last 7 days] [Last 14 days] [Last 30 days]
3.  User clicks "Last 7 days":
    GET /api/dashboards/{id}?preset=last_7_days
4.  Backend re-runs all widget SQL with date WHERE clause injected
5.  Widgets with a date column are filtered; others return all data
6.  Custom date range via date picker:
    GET /api/dashboards/{id}?start_date=2024-01-01&end_date=2024-02-01
7.  Clear filter → GET /api/dashboards/{id} (no params → all data)
```

---

## Dashboard Cross-Filtering (Advanced)

```
1.  User clicks a data point on a chart (e.g. "India" on pie chart)
2.  Frontend reads the clicked dimension: { column: "billing_country", value: "India" }
3.  POST /api/dashboards/{id}/filtered
    { "filters": [{ "column": "billing_country", "op": "eq", "value": "India" }] }
4.  All widgets whose source table has "billing_country" are filtered
5.  Other widgets (e.g. Meta Ads) are returned unfiltered
6.  User clicks again to clear → POST with empty filters array
```

---

## Adding Agent Charts to Dashboards

```
1.  User asks a question in chat → agent generates a chart (SSE `chart` event)
2.  UI shows "Add to Dashboard" with two options:
    a.  Add to existing dashboard → select from GET /api/dashboards
    b.  Create new dashboard      → POST /api/dashboards/create { name }
3.  POST /api/dashboards/{id}/widgets  { title, type, chart_config }
4.  Widget is saved → user can view it in the dashboard
```

---

## Chat (Streaming)

```
1.  POST /api/chat  { message: "...", session_id: "s1" }
2.  Consume SSE stream → render tokens, tool calls, charts in real time
3.  GET /api/chat/history/s1     → restore conversation on page reload
4.  DELETE /api/chat/history/s1  → start fresh
```

### SSE Consumption (JavaScript)

```js
async function streamChat(message, sessionId) {
  const res = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, session_id: sessionId }),
  });

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop(); // keep incomplete line in buffer

    let eventType = '';
    for (const line of lines) {
      if (line.startsWith('event: ')) eventType = line.slice(7);
      else if (line.startsWith('data: ')) {
        const data = JSON.parse(line.slice(6));
        handleEvent(eventType, data);
      }
    }
  }
}
```

### SSE Event Handling Guide

| Event | What to do |
|---|---|
| `start` | Show "Agent is thinking" / loading indicator |
| `token` | Append `data.content` to the assistant message bubble |
| `tool_call_start` | Show tool name badge (e.g. "Running sql_db_query…") |
| `tool_call_args` | Optionally stream the tool arguments for a "thought process" UI |
| `tool_result` | Display collapsible tool result; hide the "running" badge |
| `chart` | Render ECharts widget from `data.chartConfig` — show "Add to Dashboard" button |
| `end` | Hide loading indicator; mark message as complete |
| `error` | Show error toast / inline error from `data.detail` |
