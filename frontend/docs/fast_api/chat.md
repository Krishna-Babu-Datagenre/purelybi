# Chat API

Base prefix: `/api/chat`

All responses from `POST /api/chat` are **Server-Sent Events (SSE)** streams.

---

## POST `/api/chat`

Send a message to the AI agent and receive a streamed response.

**Request**
```json
{
  "message": "What were total sales last month?",
  "session_id": "user-123",
  "agent_type": "analyst",
  "llm": "gpt-4.1",
  "database": "DuckDB"
}
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `message` | string | required | The user's question |
| `session_id` | string | `"default"` | Reuse to continue a conversation |
| `agent_type` | string | `"analyst"` | `"analyst"` or `"supervisor"` |
| `llm` | string | `"gpt-4.1"` | LLM model name |
| `database` | string | `"DuckDB"` | Must be `"DuckDB"` |

**Response** — `Content-Type: text/event-stream`

Each frame:
```
event: <event_type>
data: <json>
```

### SSE Event Types

| Event | Data Shape | When |
|---|---|---|
| `start` | `{ "status": "streaming" }` | Stream begins — show loading state |
| `token` | `{ "content": "..." }` | A text chunk from the model |
| `tool_call_start` | `{ "tool_call_id": "...", "tool_name": "..." }` | Agent invoked a tool |
| `tool_call_args` | `{ "tool_call_id": "...", "args_chunk": "..." }` | Streaming tool arguments |
| `tool_result` | `{ "tool_call_id": "...", "tool_name": "...", "args": "...", "result": "..." }` | Tool finished |
| `chart` | `{ "tool_call_id": "...", "chart_type": "...", "chartConfig": { ... } }` | Render an ECharts chart |
| `end` | `{}` | Stream finished |
| `error` | `{ "detail": "..." }` | An error occurred |

### `chart` Event — ECharts Config

When the agent generates a chart, emit a `chart` SSE event. `chartConfig` is a raw [Apache ECharts option object](https://echarts.apache.org/en/option.html) — pass it directly to `echartsInstance.setOption()`.

```json
{
  "tool_call_id": "call_def",
  "chart_type": "bar",
  "chartConfig": {
    "tooltip": { "trigger": "axis" },
    "xAxis": { "type": "category", "data": ["Jan", "Feb", "Mar"] },
    "yAxis": { "type": "value" },
    "series": [{ "type": "bar", "data": [15000, 22000, 18000] }]
  }
}
```

### JS Consumption Pattern

```js
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
  buffer = lines.pop();

  let eventType = '';
  for (const line of lines) {
    if (line.startsWith('event: ')) eventType = line.slice(7);
    else if (line.startsWith('data: ')) {
      const data = JSON.parse(line.slice(6));
      handleEvent(eventType, data);
    }
  }
}
```

---

## GET `/api/chat/history/{session_id}`

Retrieve full conversation history for a session (use on page load to restore chat).

**Query params** (all optional, must match values used during the session):
- `agent_type` (default: `"analyst"`)
- `llm` (default: `"gpt-4.1"`)
- `database` (default: `"DuckDB"`)

**Response `200 OK`** — array of messages
```json
[
  { "role": "user", "content": "How many orders?" },
  {
    "role": "assistant",
    "content": null,
    "tool_calls": [
      { "id": "call_1", "name": "sql_db_query", "args": { "query": "SELECT COUNT(*) ..." } }
    ]
  },
  {
    "role": "tool",
    "content": "[{\"count\": 1234}]",
    "tool_call_id": "call_1",
    "tool_name": "sql_db_query"
  },
  { "role": "assistant", "content": "There are 1,234 orders." }
]
```

When a chart was generated, the tool message includes a `chart` field:
```json
{
  "role": "tool",
  "tool_call_id": "call_2",
  "tool_name": "create_react_chart",
  "chart": {
    "tool_call_id": "call_2",
    "chart_type": "bar",
    "chartConfig": { ... }
  }
}
```

---

## DELETE `/api/chat/history/{session_id}`

Clear a conversation session.

**Response `200 OK`**
```json
{ "status": "deleted", "session_id": "user-123" }
```

**Error `404`** — session not found.
