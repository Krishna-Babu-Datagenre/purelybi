# Alerts Feature — Implementation Plan

> **Status:** Draft — awaiting approval before implementation.
> **Owner:** Purely BI team
> **Last updated:** 2026-05-01

## 1. Overview

Introduce a **data-driven alerting** capability so users can describe an alert in natural language (e.g. *"Email me when yesterday's Facebook ad spend exceeds $500"*), have it parsed into a structured definition by an LLM agent, persisted in Supabase, evaluated on a recurring schedule by a dedicated Azure Function, and delivered via email when the condition is met.

The feature mirrors three existing patterns already proven in the codebase:

| Existing pattern                                     | Reused for Alerts                                                |
| ---------------------------------------------------- | ---------------------------------------------------------------- |
| Dashboard builder agent (NL → structured definition) | Alert builder agent (NL → structured alert)                      |
| `dashboards` / `widgets` Supabase tables + RLS       | `alerts` / `alert_runs` / `alert_notifications` tables + RLS     |
| `azure-function-sync-orchestrator` (timer-driven)    | `azure-function-alert-evaluator` (timer-driven evaluation loop)  |
| Zustand store + service module + layout (Dashboards) | `useAlertsStore` + `alertsApi` + `AlertsLayout` with two tabs    |

## 2. Goals & Non-Goals

### Goals (v1)

1. Users can **create an alert** by chatting in natural language; the agent confirms a structured definition before saving.
2. Users can **manage alerts** (list, view, enable/disable, edit name & schedule, delete).
3. Alerts are **evaluated on a schedule** (default every 15 minutes; user-selectable: 15min / hourly / daily) by a dedicated Azure Function.
4. When an alert fires, the user receives an **email** with the alert name, the triggering value, and a deep-link back to a relevant dashboard or query result.
5. Alert run history is recorded for audit and to drive de-duplication ("don't notify me again until the condition resets").

### Non-Goals (v1, deferred)

- Slack / Teams / SMS / webhook channels (architecture leaves room — see §10).
- Multi-recipient distribution lists.
- Anomaly-detection alerts (statistical baselines). v1 covers **threshold / comparison** alerts only.
- Per-user custom evaluation cadence below 15 minutes.
- In-app push notifications (out of scope; possible follow-up using the existing SSE plumbing).

## 3. User Experience

### 3.1 Navigation

Add an **Alerts** entry to the existing `Sidebar` (between *Dashboards* and *Connectors*). Route: `/alerts`.

### 3.2 Layout — `AlertsLayout`

The page renders a tab strip with two tabs (matches Tailwind / Lucide style already used by the app):

1. **Create Alert** — default tab.
2. **Manage Alerts**.

#### Tab 1 — Create Alert

Conversational builder, modeled on the existing **Dashboard Builder** experience.

- Left column: chat panel that streams from the **Alert Builder agent** (SSE, same plumbing as `chat` and `onboarding` routers).
- Right column: live **Alert Preview Card** that materialises as the agent fills in fields (`name`, `metric`, `comparator`, `threshold`, `time window`, `frequency`, `notification channel`).
- A **"Save alert"** call-to-action becomes enabled once the agent emits a complete, validated `AlertDefinition` and the user confirms.

Example dialogue:

> **User:** Tell me when my daily Google Ads spend goes above 1,000 USD.
> **Agent:** *(after one or two clarifying questions)* Here is your alert: name "Google Ads daily spend > $1k", checked every hour, comparing yesterday's `cost` from `google_ads_campaigns`. Save it?

#### Tab 2 — Manage Alerts

- Table / card list of all alerts owned by the user.
- Columns: Name, Status (Enabled/Disabled), Frequency, Last evaluated, Last fired, Actions (Edit name & schedule, Toggle enabled, Delete, View runs).
- Drawer / dialog for **Run history** (last 50 runs with timestamp, status, observed value, error message if any).

## 4. Architecture

```
┌──────────────┐  SSE   ┌─────────────────────────┐  CRUD   ┌──────────────┐
│  Frontend    │◀──────▶│  FastAPI /api/alerts    │◀───────▶│  Supabase    │
│  AlertsLayout│  REST  │  + alert builder agent  │         │  alerts /    │
└──────────────┘        └─────────────────────────┘         │  alert_runs  │
                                                            └──────▲───────┘
                                                                   │ poll
                                          ┌────────────────────────┴────────────┐
                                          │  Azure Function:                     │
                                          │  azure-function-alert-evaluator      │
                                          │  (timer-trigger, every 5 min)        │
                                          │  • read enabled alerts due to run    │
                                          │  • run SQL against user's DuckDB /   │
                                          │    parquet store                     │
                                          │  • compare result to threshold       │
                                          │  • write alert_runs row              │
                                          │  • on transition → send email via    │
                                          │    Azure Communication Services      │
                                          └──────────────────────────────────────┘
```

### 4.1 Component Inventory

| Layer        | New file / module                                                      | Purpose                                                                          |
| ------------ | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Supabase     | `backend/supabase/queries/N-create-alerts-tables.sql`                  | DDL for `alerts`, `alert_runs`, `alert_notifications`, RLS policies, indexes     |
| Backend      | `backend/src/fastapi_app/routers/alerts.py`                            | REST + SSE endpoints                                                              |
| Backend      | `backend/src/fastapi_app/services/alert_service.py`                    | CRUD, scheduling math, run-history queries                                        |
| Backend      | `backend/src/fastapi_app/models/alerts.py`                             | Pydantic models (`AlertDefinition`, `AlertCondition`, `AlertRun`, …)              |
| Backend      | `backend/src/ai/agents/alerts/agent.py`                                | LangGraph agent that turns NL into `AlertDefinition`                              |
| Backend      | `backend/src/ai/agents/alerts/prompts.py`                              | System prompt + few-shot examples                                                 |
| Backend      | `backend/src/ai/agents/alerts/tools.py`                                | Tools: `list_user_tables`, `inspect_columns`, `propose_alert`, `validate_sql`     |
| Azure        | `azure-function-alert-evaluator/host.json`                             | Standard Functions v2 host config                                                 |
| Azure        | `azure-function-alert-evaluator/alert_evaluator/__init__.py`           | Timer entry point — orchestrates one evaluation tick                              |
| Azure        | `azure-function-alert-evaluator/alert_evaluator/function.json`         | Timer binding, schedule `0 */5 * * * *`                                           |
| Azure        | `azure-function-alert-evaluator/shared/evaluator.py`                   | Pure evaluation logic (testable without Functions runtime)                        |
| Azure        | `azure-function-alert-evaluator/shared/notifier.py`                    | Email sender (Azure Communication Services / SendGrid)                             |
| Azure        | `azure-function-alert-evaluator/shared/data_access.py`                 | Reads parquet/DuckDB for user                                                     |
| Azure        | `azure-function-alert-evaluator/requirements.txt`                      | `azure-functions`, `supabase`, `duckdb`, `azure-communication-email`              |
| Azure        | `azure-function-alert-evaluator/local.settings.json.example`           | Documented env vars                                                                |
| Frontend     | `frontend/src/layouts/AlertsLayout.tsx`                                | Tabbed page                                                                        |
| Frontend     | `frontend/src/components/alerts/AlertBuilderChat.tsx`                  | Chat panel + preview card                                                          |
| Frontend     | `frontend/src/components/alerts/AlertList.tsx`                         | Manage tab table                                                                   |
| Frontend     | `frontend/src/components/alerts/AlertEditDialog.tsx`                   | Edit name / schedule / enabled                                                    |
| Frontend     | `frontend/src/components/alerts/AlertRunHistoryDrawer.tsx`             | Run history viewer                                                                 |
| Frontend     | `frontend/src/services/alertsApi.ts`                                   | HTTP client (uses `backendClient`)                                                |
| Frontend     | `frontend/src/store/useAlertsStore.ts`                                 | Zustand store                                                                      |
| Frontend     | `frontend/src/types/alerts.ts`                                         | TS types matching backend models                                                   |

## 5. Data Model (Supabase)

All tables scoped by `user_id` with RLS `auth.uid() = user_id`, mirroring `dashboards` / `widgets` conventions.

### `alerts`

| Column                | Type           | Notes                                                                                        |
| --------------------- | -------------- | -------------------------------------------------------------------------------------------- |
| `id`                  | `uuid` PK      | `gen_random_uuid()`                                                                          |
| `user_id`             | `uuid` FK      | `auth.users(id)`                                                                             |
| `name`                | `text`         | Human-readable                                                                               |
| `description`         | `text`         | Optional, agent-generated summary                                                            |
| `definition`          | `jsonb`        | Structured `AlertDefinition` (see §6)                                                        |
| `sql_query`           | `text`         | The validated SQL that produces a single scalar value at evaluation time                     |
| `comparator`          | `text`         | `gt` / `gte` / `lt` / `lte` / `eq` / `neq` / `pct_change_gt` / `pct_change_lt`               |
| `threshold`           | `numeric`      | The right-hand-side of the comparison                                                        |
| `frequency`           | `text`         | `every_15_min` / `hourly` / `daily` (drives next-run cadence)                                |
| `notification_channel`| `text`         | `email` (v1)                                                                                 |
| `notification_target` | `text`         | Email address (defaults to `profiles.email`)                                                 |
| `enabled`             | `boolean`      | Default `true`                                                                               |
| `last_evaluated_at`   | `timestamptz`  | Set by evaluator                                                                             |
| `last_fired_at`       | `timestamptz`  | Set by evaluator on transition `false → true`                                                |
| `last_state`          | `text`         | `ok` / `firing` / `error` — used for de-duplication                                          |
| `created_at`          | `timestamptz`  | `now()`                                                                                      |
| `updated_at`          | `timestamptz`  | trigger-maintained                                                                           |

Indexes: `(user_id)`, `(enabled, last_evaluated_at)` — used by the evaluator's "due-now" query.

### `alert_runs`

| Column            | Type           | Notes                                                  |
| ----------------- | -------------- | ------------------------------------------------------ |
| `id`              | `uuid` PK      |                                                        |
| `alert_id`        | `uuid` FK      | `alerts(id) ON DELETE CASCADE`                         |
| `user_id`         | `uuid` FK      | denormalised for RLS                                   |
| `evaluated_at`    | `timestamptz`  |                                                        |
| `status`          | `text`         | `ok` / `firing` / `error`                              |
| `observed_value`  | `numeric`      | Result of the SQL                                      |
| `error_message`   | `text`         | Populated when `status = 'error'`                      |
| `notification_id` | `uuid` FK NULL | links to `alert_notifications` if a message was sent   |

Indexes: `(alert_id, evaluated_at desc)`.

### `alert_notifications`

| Column          | Type           | Notes                                                  |
| --------------- | -------------- | ------------------------------------------------------ |
| `id`            | `uuid` PK      |                                                        |
| `alert_id`      | `uuid` FK      |                                                        |
| `user_id`       | `uuid` FK      |                                                        |
| `channel`       | `text`         | `email`                                                |
| `target`        | `text`         | recipient                                              |
| `payload`       | `jsonb`        | rendered subject + body                                |
| `provider_id`   | `text`         | message id returned by Azure Communication Services    |
| `delivered_at`  | `timestamptz`  | nullable                                               |
| `error_message` | `text`         | nullable                                               |

### RLS

```sql
alter table alerts enable row level security;
create policy "alerts_owner" on alerts
  for all using (auth.uid() = user_id) with check (auth.uid() = user_id);
-- repeat for alert_runs, alert_notifications
```

The Azure Function uses the **service-role key** and bypasses RLS, but always filters explicitly by `user_id` defensively.

## 6. Pydantic Models

```python
# backend/src/fastapi_app/models/alerts.py
class Comparator(str, Enum):
    gt = "gt"; gte = "gte"; lt = "lt"; lte = "lte"
    eq = "eq"; neq = "neq"
    pct_change_gt = "pct_change_gt"; pct_change_lt = "pct_change_lt"

class Frequency(str, Enum):
    every_15_min = "every_15_min"
    hourly = "hourly"
    daily = "daily"

class AlertDefinition(BaseModel):
    metric_description: str            # human summary of what is measured
    table: str                         # source table in user's DuckDB / parquet
    metric_sql: str                    # SELECT producing one scalar
    comparator: Comparator
    threshold: float
    time_window: str                   # e.g. "yesterday", "last_7_days"
    frequency: Frequency
    notification_channel: Literal["email"] = "email"
    notification_target: EmailStr | None = None

class AlertCreate(BaseModel):
    name: str
    description: str | None = None
    definition: AlertDefinition

class AlertOut(AlertCreate):
    id: UUID
    enabled: bool
    last_evaluated_at: datetime | None
    last_fired_at: datetime | None
    last_state: Literal["ok", "firing", "error"] | None
    created_at: datetime
    updated_at: datetime

class AlertRunOut(BaseModel):
    id: UUID
    evaluated_at: datetime
    status: Literal["ok", "firing", "error"]
    observed_value: float | None
    error_message: str | None
```

## 7. REST API

All endpoints under `/api/alerts`, gated by `Depends(get_current_user_dep)`.

| Method | Path                                  | Body / Query                  | Response             | Notes                                |
| ------ | ------------------------------------- | ----------------------------- | -------------------- | ------------------------------------ |
| POST   | `/api/alerts/builder/stream`          | `{ session_id, message }`     | `text/event-stream`  | NL → `AlertDefinition` via agent     |
| POST   | `/api/alerts`                         | `AlertCreate`                 | `AlertOut`           | Persists a finalised alert           |
| GET    | `/api/alerts`                         | —                             | `list[AlertOut]`     | List user's alerts                   |
| GET    | `/api/alerts/{id}`                    | —                             | `AlertOut`           |                                      |
| PATCH  | `/api/alerts/{id}`                    | partial fields                | `AlertOut`           | name, frequency, enabled, target     |
| DELETE | `/api/alerts/{id}`                    | —                             | `204`                | Cascades to runs & notifications     |
| GET    | `/api/alerts/{id}/runs`               | `?limit=50`                   | `list[AlertRunOut]`  | History for the drawer               |
| POST   | `/api/alerts/{id}/test`               | —                             | `AlertRunOut`        | Synchronous one-off evaluation       |

`alert_service.py` mirrors `dashboard_service.py`: every function takes `user_id` and uses `get_supabase_admin_client()` with explicit `.eq("user_id", user_id)` filters.

## 8. Alert Builder Agent

Lives at `backend/src/ai/agents/alerts/`. Built with `create_agent` (LangGraph), same scaffolding as the Dashboard Builder (`backend/src/ai/agents/dashboard/`).

- **LLM:** reuse `get_user_proxy_llm()` for cheap clarifications, `get_analyst_llm()` for SQL drafting.
- **Tools:**
  - `list_user_tables(user_id)` — wraps existing DuckDB introspection used by the analyst agent.
  - `inspect_columns(table)` — sample rows + dtypes for grounding.
  - `validate_metric_sql(sql)` — runs SQL inside the read-only DuckDB sandbox; rejects multi-row / non-numeric results.
  - `propose_alert(...)` — emits a structured `AlertDefinition` event over SSE so the frontend preview card can update.
- **Termination:** agent ends when the user confirms; the frontend then calls `POST /api/alerts` with the final payload.
- **Streaming:** identical SSE event taxonomy as `chat` (`token`, `tool_call_start`, `alert_preview`, `end`, `error`).
- **Prompts:** in `prompts.py`. Constraints include: must produce SQL with exactly one numeric column, must pick a `time_window` consistent with the chosen `frequency`, must avoid joins not present in the user's metadata.

## 9. Azure Function — `azure-function-alert-evaluator`

Modelled on `azure-function-sync-orchestrator`.

### 9.1 Trigger & Cadence

- Timer trigger, schedule `0 */5 * * * *` (every 5 minutes).
- Each tick selects alerts where `enabled = true AND (last_evaluated_at IS NULL OR last_evaluated_at + frequency_interval <= now())`.
- A short advisory lock (`alert_id` + `evaluated_at` minute bucket) prevents double evaluation if a tick overlaps.

### 9.2 Evaluation Loop (per alert)

1. Resolve user → load latest parquet snapshot path / DuckDB attach instructions (reuse helpers from `backend/src/ai/agents/sql/duckdb_sandbox.py` or a slimmed copy in `shared/data_access.py`).
2. Run `definition.metric_sql` read-only with a tight statement timeout (e.g. 10s).
3. Compute new state by applying `comparator` to (`observed_value`, `threshold`).
4. Insert an `alert_runs` row.
5. **Notify only on transition** `last_state in {ok, error, null} → firing`. This avoids notification spam for an alert that stays firing across many ticks. Re-arming happens automatically once the alert returns to `ok`.
6. Update `alerts.last_evaluated_at`, `last_state`, `last_fired_at`.

### 9.3 Email Delivery

- Provider: **Azure Communication Services Email** (consistent with the existing Azure-first stack); abstract behind `Notifier` so SendGrid / Resend can be swapped in.
- Template: subject `[Purely BI] Alert "<name>" is firing`, body contains observed value, threshold, time window, link `https://purelybi.com/alerts/<id>`.
- Failures are recorded in `alert_notifications.error_message`; the run remains `firing`, so the next tick will retry until either delivery succeeds or the condition resets.

### 9.4 Configuration (`local.settings.json.example`)

```json
{
  "SUPABASE_URL": "",
  "SUPABASE_SERVICE_ROLE_KEY": "",
  "AZURE_STORAGE_CONNECTION_STRING": "",
  "PARQUET_CONTAINER_NAME": "raw",
  "ACS_CONNECTION_STRING": "",
  "ACS_SENDER_ADDRESS": "alerts@purelybi.com",
  "ALERT_SQL_TIMEOUT_SECONDS": "10",
  "APP_BASE_URL": "https://purelybi.com"
}
```

### 9.5 Provisioning

- Create a new dedicated Function App `func-purelybi-alert-evaluator`.

## 10. Frontend Implementation

### 10.1 Routing

- Add an `/alerts` route in `App.tsx` rendering `<AlertsLayout />`. Internal tab state via `useState`; tab persisted in URL hash for deep-linking (`/alerts#manage`).

### 10.2 State

`useAlertsStore` (Zustand):

```ts
interface AlertsState {
  alerts: AlertOut[];
  loading: boolean;
  error: string | null;
  fetchAlerts(): Promise<void>;
  createAlert(payload: AlertCreate): Promise<AlertOut>;
  updateAlert(id: string, patch: Partial<AlertOut>): Promise<void>;
  deleteAlert(id: string): Promise<void>;
  toggleEnabled(id: string, enabled: boolean): Promise<void>;
  fetchRuns(id: string): Promise<AlertRunOut[]>;
}
```

### 10.3 Service

`alertsApi.ts` uses `backendClient.request` for REST and `fetchEventStream` for the builder SSE (the same helper used by the chat client).

### 10.4 Components

- `AlertsLayout` — shell with the two tabs.
- `AlertBuilderChat` — reuses `ChatMessageList` styling; right-hand `AlertPreviewCard` reflects the current draft `AlertDefinition`.
- `AlertList` — table with status pills, frequency chip, last-fired timestamp.
- `AlertEditDialog` — edit name / frequency / target / enabled.
- `AlertRunHistoryDrawer` — last 50 runs with status colour + observed value.

### 10.5 Notifications UX

When the user lands on `/alerts` and an alert has fired since their last visit, show a soft toast / banner derived from `last_fired_at > last_seen_at` (tracked in `localStorage`). Real-time push is out of scope for v1.

## 11. Security & Compliance

- All routes enforce `auth.uid() = user_id` via Supabase RLS **and** explicit service-side filtering.
- The alert builder agent runs SQL in the existing read-only DuckDB sandbox — no DDL / DML allowed; statement timeout enforced.
- The evaluator never executes user-authored SQL outside the sandbox; injection cannot escape into the host.
- Emails contain no row-level data beyond the observed scalar and the alert name; deep-links require login.
- Service-role key for the function is held only in App Settings; rotated alongside other secrets.

## 12. Observability

- Reuse `request-id` middleware for all API calls.
- Function logs include `alert_id`, `user_id`, `tick_id`, `duration_ms`, `outcome`.
- Application Insights dashboard cards: alerts evaluated/min, error rate, p95 evaluation latency, notifications sent, notification failures.
- Surface `last_state = error` prominently in the Manage tab so users can self-diagnose broken alerts.

## 13. Testing Strategy

- **Backend unit tests** (`backend/tests/`):
  - `test_alert_service.py` — CRUD, RLS-equivalent filtering, transitions.
  - `test_alerts_router.py` — endpoint contracts.
  - `test_alert_builder_agent.py` — golden-prompt snapshot tests.
- **Function tests** (`azure-function-alert-evaluator/tests/`):
  - `test_evaluator.py` — pure logic with synthetic `AlertDefinition`s & DuckDB fixtures.
  - `test_notifier.py` — mocked ACS client.
- **Frontend**: Vitest + React Testing Library for the store actions and `AlertList` rendering.
- **End-to-end smoke**: a script under `backend/tests/e2e_alerts.py` that creates an alert, seeds parquet to make it fire, invokes the evaluator's `main(timer)` directly, and asserts a notification row.

## 14. Rollout

1. Land Supabase migration behind a feature flag (`ALERTS_ENABLED` env var).
2. Deploy backend + agent — UI tab hidden unless flag is on.
3. Deploy Azure Function in **dry-run mode** (`ALERTS_DRY_RUN=true`): writes runs but does not send emails. Run for 24–48 hours.
4. Enable email delivery for an internal pilot user.
5. Gradually flip the flag for all users.

## 15. Task Breakdown

> Each item below is intended as a single PR.

### Phase 1 — Foundations

1. **Supabase migration** — add `alerts`, `alert_runs`, `alert_notifications` tables, indexes, RLS, `updated_at` trigger. Update `backend/supabase/SCHEMA.md`.
2. **Pydantic models** — `backend/src/fastapi_app/models/alerts.py`.
3. **Service + router (CRUD only)** — `alert_service.py`, `routers/alerts.py`, register in `app.py`. No agent yet; accepts a hand-crafted `AlertDefinition`.

### Phase 2 — Builder Agent

4. **Alert builder agent** — `ai/agents/alerts/{agent,prompts,tools}.py`, integrated with the existing DuckDB sandbox tooling.
5. **`/api/alerts/builder/stream`** SSE endpoint with `alert_preview` events.
6. **Backend unit + agent tests**.

### Phase 3 — Evaluator Function

7. **Scaffold `azure-function-alert-evaluator`** (host.json, function.json, requirements, local.settings example).
8. **Shared evaluation logic** in `shared/evaluator.py` with full unit tests.
9. **Notifier** (`shared/notifier.py`) wrapping Azure Communication Services Email.
10. **Wire timer entry point** + state-transition logic + run/notification persistence.
11. **Provisioning notes** added under `docs/alerts_provisioning.md` (separate doc, follows the style of `docs/sync_v2_provisioning_guide.md`).

### Phase 4 — Frontend

12. **Types + service + store** (`types/alerts.ts`, `services/alertsApi.ts`, `store/useAlertsStore.ts`).
13. **`AlertsLayout` + sidebar entry** with empty Manage table and skeleton Create tab.
14. **`AlertBuilderChat`** — wired to SSE endpoint, preview card, save flow.
15. **`AlertList` + `AlertEditDialog` + `AlertRunHistoryDrawer`**.
16. **Frontend tests** for the store and key components.

### Phase 5 — Hardening & Rollout

17. End-to-end smoke test script.
18. Application Insights dashboard + alert on evaluator failure rate.
19. Feature-flag rollout per §14.

## 16. Open Questions

1. **Email provider** — confirm Azure Communication Services vs. an existing transactional provider already in use (SendGrid / Postmark)?
2. **Per-alert recipients** — v1 defaults to the owner's `profiles.email`; do we want to allow a typed override now (still scoped to the owner) or defer entirely?
3. **Timezone for "yesterday" / daily windows** — store a per-user timezone on the profile, or accept the tenant's first-detected timezone from the browser at alert-creation time?
4. **Re-arm semantics** — auto re-arm when the condition clears (proposed default), or require manual acknowledgement before the next firing? Default proposed: auto re-arm; acknowledgement is a v2 enhancement.
5. **Free-tier limits** — do we cap the number of active alerts per user (e.g. tied to `subscription_plan`)? Recommended cap: 5 on free, unlimited on paid.

---

**Awaiting approval on this plan before starting Phase 1.** Reviewers, please leave comments on §16 and the §15 phasing.
