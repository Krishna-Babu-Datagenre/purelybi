# Sync V2 — Robustness & Failure Handling

> Companion to [`simplified_sync_architecture_proposal.md`](simplified_sync_architecture_proposal.md).
> Last updated: 2026-04-16.

---

## State Machine

```
pending ──→ reading ──→ uploading ──→ success
               │            │
               ▼            ▼
            failed       failed
               ▲            ▲
               │            │
          reauth_required   │
                            │
            (circuit breaker after 5 consecutive failures)
```

**Valid statuses:** `pending`, `reading`, `uploading`, `success`, `failed`, `reauth_required`.

The orchestrator (Azure Function, timer trigger every 5 min) runs three phases per tick:

1. **Phase 1 — Check uploaders** (`uploading` → `success` / `failed`)
2. **Phase 2 — Check connectors** (`reading` → `uploading` / `failed`)
3. **Phase 3 — Start new syncs** (`eligible` → `reading`)

---

## Eligibility (who gets picked for a new sync)

| Rule | How |
|------|-----|
| Active + validated | `is_active = true AND sync_validated = true` |
| **Allowlist** | `last_sync_status IN ('pending', 'success', 'failed')` — only terminal/initial statuses qualify. Unexpected values (e.g. leftover `"running"` from a retired system) are **never** picked up. |
| Circuit breaker | `consecutive_failures < 5` |
| One-off | Only when `last_sync_at IS NULL` |
| Recurring | `last_sync_at IS NULL` OR elapsed ≥ `sync_frequency_minutes` |

### Why allowlist, not denylist

A denylist (`neq reading, neq uploading, …`) lets unknown statuses slip through — this caused a production incident where a v1 orchestrator left `"running"` in the DB, and v2 kept re-picking it every tick. An allowlist is closed by default.

---

## Error Visibility

Every failure path writes to **both** `last_sync_error` (human-readable, up to 2000 chars) and increments `consecutive_failures`.

| What | Where the error comes from |
|------|---------------------------|
| Connector failed | `stderr.log` from the File Share (the connector's stderr) |
| Uploader failed | `stderr.log` from the uploader container |
| Uploader crashed before callback | Orchestrator reads `stderr.log` on next tick |
| Azure API unreachable | `"Azure API unreachable for N min"` |
| No catalog / no streams | Static message |
| OAuth expired | `reauth_required` status + error from refresh |
| Failed to start ACA job | Python exception message |

**Error preservation:** When a new sync starts (`reading`), the previous `last_sync_error` is **not** cleared. It stays visible until the outcome of the current sync overwrites it (success clears it, failure replaces it).

---

## Fast Status Updates (Uploader → Supabase Direct)

```
Before:  reading ──[5 min tick]──→ uploading ──[5 min tick]──→ success
After:   reading ──[5 min tick]──→ uploading ──→ success (seconds)
```

The sync-uploader writes success directly to Supabase (REST API via `urllib`, no extra deps). This eliminates the second 5-minute wait.

**The orchestrator's `phase_check_uploading` remains as a safety net** for:
- Uploader crashed before its callback
- Supabase callback failed (network/timeout)
- ACA execution stuck

**File share cleanup order:**
- Callback succeeded → uploader cleans up files immediately.
- Callback failed → files are **preserved** so the orchestrator safety net can still read `output.jsonl` for state extraction and `stderr.log` for error capture.

---

## Failure Scenarios

### 1. Connector takes longer than sync interval (e.g. 6-hour MongoDB sync, 6-hour frequency)

| | |
|-|-|
| **Status during run** | `reading` |
| **Is it re-picked?** | No — `reading` is not in the allowlist `[pending, success, failed]` |
| **Poll result** | `running` → orchestrator does nothing, waits until next tick |
| **Timeout?** | None. Legitimately running executions are never force-killed. |
| **When it finishes** | Next tick sees `succeeded` → starts uploader → `uploading` → success |

### 2. Azure API unreachable (poll_error)

| | |
|-|-|
| **Poll result** | `poll_error` (exception in Azure SDK call) |
| **Immediate action** | None — skip for this tick |
| **After 120 min of consecutive poll_error** | Force-fail with descriptive error, increment `consecutive_failures` |
| **Why 120 min?** | At 5-min ticks, that's 24 consecutive API failures — enough confidence the execution is orphaned. |

### 3. Uploader succeeds but Supabase callback fails

| | |
|-|-|
| **Status** | Still `uploading` |
| **File share** | Preserved (cleanup only runs on successful callback) |
| **Next tick** | Orchestrator polls ACA → `succeeded` → reads state from file share → writes success → cleans up |
| **Data loss?** | None. Parquet files are already in Blob Storage. |

### 4. Uploader crashes mid-Parquet

| | |
|-|-|
| **Status** | Still `uploading` |
| **Supabase callback** | Never reached |
| **Next tick** | Orchestrator polls ACA → `failed` → reads stderr → writes failure + increments counter |
| **Partial data?** | Possible if some streams uploaded before crash. Next sync will do full_refresh (or incremental re-read) to correct. |

### 5. OAuth token expires between syncs

| | |
|-|-|
| **Where caught** | `phase_start_new_syncs` → `refresh_credentials_if_needed()` |
| **Refresh succeeds** | Updated token saved to DB, sync proceeds |
| **Refresh fails (reauth needed)** | Status → `reauth_required`, error stored. Not re-picked (not in allowlist). User must re-authenticate via UI. |

### 6. Connector image doesn't exist / ACA job fails to start

| | |
|-|-|
| **Where caught** | `start_connector_execution()` raises exception |
| **Handled by** | Outer `try/except` in `phase_start_new_syncs` → `failed` + error + increment counter |
| **Retried?** | Yes, on next eligible tick (if under circuit breaker limit) |

### 7. Old system wrote an unexpected status

| | |
|-|-|
| **Example** | `last_sync_status = "running"` from v1 orchestrator |
| **Eligible?** | No — `"running"` is not in `[pending, success, failed]` |
| **Fix** | Manual DB update to `pending` + reset `consecutive_failures` |

### 8. Two orchestrator instances running simultaneously

| | |
|-|-|
| **Possible?** | Only if Azure Functions scales out (unlikely for timer triggers, but possible) |
| **Risk** | Same config picked by both → two ACA jobs for one config |
| **Mitigation** | Status transitions are immediate (`pending` → `reading` before next config is processed). Race window is small. Not a correctness issue — worst case is a duplicate sync, not data loss. |

---

## Circuit Breaker

After **5 consecutive failures**, the config is skipped by eligibility. Resets to 0 on any success.

To manually unblock:
```sql
UPDATE user_connector_configs
SET consecutive_failures = 0,
    last_sync_status = 'pending',
    last_sync_error = NULL,
    aca_execution_name = NULL,
    aca_work_id = NULL
WHERE id = '<config_id>';
```

---

## Incremental Sync

When `incremental_enabled = true`:
1. Catalog is built with `incremental` + `append` (instead of `full_refresh` + `overwrite`)
2. Last Airbyte STATE message is persisted in `last_airbyte_state` (JSONB column)
3. STATE is written to file share as `state.json` and passed via `--state` flag on next run
4. Uploader deduplicates by `_airbyte_ab_id` during Parquet merge

STATE is captured in **two places** for redundancy:
- By the sync-uploader (during its Supabase callback)
- By the orchestrator safety net (if uploader callback failed)

---

## File Share Naming

```
sync-source-mongodb-v2-01e6529c-20260416T143022   ← recurring sync
onb-check-source-shopify-20260416T120000           ← onboarding: connection check
onb-discover-source-shopify-20260416T120030        ← onboarding: stream discovery
onb-probe-source-shopify-20260416T120100           ← onboarding: read probe
```

Pattern: `{phase}-{connector-slug}-{config_id_prefix (sync only)}-{UTC timestamp}`

Each directory contains: `config.json`, `catalog.json` (if read), `output.jsonl`, `stderr.log`, optionally `state.json`.

---

## Environment Variables (Orchestrator Azure Function)

| Variable | Purpose |
|----------|---------|
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Service-role key (bypasses RLS) |
| `AZURE_SUBSCRIPTION_ID` | Azure sub for ACA API calls |
| `AZURE_RESOURCE_GROUP` | Resource group containing the ACA Job |
| `ACA_JOB_NAME` | ACA Job name (e.g. `caj-purelybi-connector-v2-dev-ci`) |
| `ACA_JOB_CONTAINER_NAME` | Container name in the job template (default: `connector`) |
| `AZURE_FILE_SHARE_CONN_STR` | Azure Files connection string |
| `AZURE_FILE_SHARE_NAME` | File share name (e.g. `connector-data-v2`) |
| `AZURE_STORAGE_CONNECTION_STRING` | Blob Storage connection string |
| `BLOB_CONTAINER_NAME` | Blob container (default: `raw`) |
| `SYNC_UPLOADER_IMAGE` | Uploader image URI |

The orchestrator passes `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and `CONFIG_ID` to the uploader container automatically — no separate configuration needed.
