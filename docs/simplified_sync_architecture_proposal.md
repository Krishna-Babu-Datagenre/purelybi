# Simplified Sync Architecture Proposal

> **Status: IMPLEMENTED** — This proposal has been built and deployed as Sync V2.
> See [`data_sync_architecture.md`](data_sync_architecture.md) for the live architecture reference
> and [`sync_v2_provisioning_guide.md`](sync_v2_provisioning_guide.md) for the Azure provisioning guide.
> This document is kept as historical context for design decisions.

> **Constraints:**
> - Docker images for ALL connectors (no PyAirbyte). PyAirbyte is unreliable — missing connector libraries, slow dependency installation, inconsistent behavior.
> - Serverless compute (no VMs). Onboarding operations (check, discover, probe) must respond quickly — can't tolerate 3-4 min VM cold starts.
> - Minimize Azure resources. Target: one orchestrator function, one ACA Job, one File Share.

## Problem Statement

The current sync architecture has too many moving parts, making it hard to debug, trace, and operate reliably:

| Concern | Current State |
|---------|---------------|
| **Complexity** | 6+ Azure resources (2 Functions, 2 ACA Jobs, File Share, Blob, ACR), 3 execution modes, 2-phase Docker-native pipeline |
| **Debugging** | Logs scattered across Azure Functions, ACA Job executions, and Supabase; no correlation ID; `print()` instead of structured logging |
| **Local/cloud parity** | Local uses `docker run` CLI (`docker_ops.py`); cloud uses ACA Jobs + File Share + SDK polling (`azure_job_runner.py`); completely different code paths |
| **Reliability** | No retry/backoff, no circuit breaker, no deduplication, brittle Azure SDK version handling (3 different code paths for `job_execution`), silent failures |

---

## Root Cause Analysis

The complexity comes from three compounding decisions:

1. **Two execution runtimes** — PyAirbyte (in-process) for manifest/Python connectors AND Docker images (ACA Job) for Java/Python connectors. Two fundamentally different code paths that share nothing.

2. **Two ACA Jobs** — since ACA can't run Docker-in-Docker, the sync-worker (Job 1) needs to launch the connector image on a second ACA Job (Job 2) and coordinate via File Share. The sync-worker sits idle polling while the connector runs — paying for two containers.

3. **Onboarding duplicates everything** — local dev uses `subprocess.run("docker run ...")`, cloud uses ACA Jobs + File Share + polling. The same operation (check, discover, read) has 4 different implementations depending on `ONBOARDING_DOCKER_EXECUTION_MODE` × language routing.

The result: 3 execution modes in the worker, 2 language-routing paths in the orchestrator, 2 Docker execution paths in onboarding (local CLI vs ACA+FileShare), 650+ lines of Azure SDK glue code, and the sync-worker container existing only to orchestrate another container.

---

## Proposed Architecture: Single ACA Job, Direct Connector Execution

### Core Idea

**Eliminate the sync-worker container entirely.** Instead of a "worker that launches another container," run the official Airbyte connector image *directly* as the ACA Job execution with image override. The orchestrator and backend become the orchestrators — they write config to the File Share, start the connector, and process the output when it completes.

This is already how `begin_start()` with image overrides works. The current architecture just wraps it in an unnecessary intermediate container.

### What changes

| Current | Proposed |
|---------|----------|
| Orchestrator → starts sync-worker → sync-worker starts connector → sync-worker polls → sync-worker uploads | Orchestrator → starts connector directly → orchestrator processes output |
| 2 ACA Jobs (sync-worker + connector) | 1 ACA Job (connector only) |
| sync-worker image exists to orchestrate other containers | No sync-worker image |
| Language routing (PyAirbyte vs Docker) | No routing — all connectors use Docker images |
| Onboarding: 4 code paths (local Docker / ACA PyAirbyte / ACA Docker-native / disabled) | Onboarding: 1 code path (start ACA Job, poll, read output) |

---

## Architecture Diagram

```
┌──────────────────────────┐
│  FastAPI Backend          │
│  (App Service)            │
│                           │
│  Onboarding Agent         │──────── SSE ────────→ Frontend
│  - test_connection()      │
│  - discover_streams()     │
│  - run_sync_probe()       │
│                           │
│  1. Write config to       │
│     File Share            │
│  2. Start ACA Job         │
│     (image = connector)   │
│  3. Poll execution        │
│  4. Read output from      │
│     File Share            │
│  5. Parse + return result │
└──────────┬────────────────┘
           │
           │ same ACA Job
           │
┌──────────┴───────────────┐     ┌──────────────────────────┐
│  Sync Orchestrator        │     │  Schema Updater           │
│  (Azure Function, timer)  │     │  (Azure Function, daily)  │
│                           │     │                           │
│  Timer tick:              │     │  Airbyte registry →       │
│  1. Check 'reading' →    │     │  Supabase connector_      │
│     poll ACA executions   │     │  schemas                  │
│     → if done: download   │     │                           │
│     JSONL, upload Parquet │     │  (unchanged)              │
│     → update Supabase     │     │                           │
│                           │     │                           │
│  2. Check eligible →      │     │                           │
│     write config to       │     │                           │
│     File Share, start     │     │                           │
│     ACA Job, mark         │     │                           │
│     'reading'             │     │                           │
└──────────┬────────────────┘     └──────────────────────────┘
           │
           ▼
┌───────────────────────────────────────────────────────┐
│  Single ACA Job                                       │
│  (image overridden per execution)                     │
│                                                       │
│  Execution A: airbyte/source-shopify:3.2.3            │
│    $AIRBYTE_ENTRYPOINT check --config /data/.../      │
│                                                       │
│  Execution B: airbyte/source-mongodb-v2:6.6.4         │
│    $AIRBYTE_ENTRYPOINT discover --config /data/.../   │
│                                                       │
│  Execution C: airbyte/source-facebook-marketing:3.0   │
│    $AIRBYTE_ENTRYPOINT read --config ... --catalog .. │
│                                                       │
│  Each execution:                                      │
│  - Image overridden to official Airbyte connector     │
│  - Reads config/catalog from File Share               │
│  - Writes JSONL output to File Share                  │
│  - ACA handles container lifecycle                    │
│                                                       │
│  No sync-worker. No PyAirbyte. No language routing.   │
└───────────────────────────┬───────────────────────────┘
                            │
                    Azure File Share
                    /data/{work_id}/
                      config.json
                      catalog.json
                      output.jsonl
                      stderr.log
                            │
                            │ (orchestrator reads output,
                            │  converts to Parquet)
                            ▼
                     Azure Blob Storage
                     (monthly Parquet)
```

---

## What Gets Eliminated

| Component | Current | Proposed | Impact |
|-----------|---------|----------|--------|
| ACA Jobs | 2 (`caj-data-sync` + `caj-docker-connector`) | **1** (connector only) | -1 ACA Job resource |
| Container Registry | `acrpurelybidevci.azurecr.io` for sync-worker image | Not needed (connectors pulled from Docker Hub) | -1 resource (optional) |
| sync-worker image | `Dockerfile.worker` + `sync_worker.py` (900 lines) + `credential_refresh.py` | **Gone** — orchestrator handles post-processing | ~1,000 lines removed |
| Worker execution branches | 3 modes (`run_sync`, `run_docker_native_sync`, `run_onboarding_connector_probe`) | **0** — ACA Job just runs the connector | No branching in worker |
| Orchestrator routing | Language-based (`PyAirbyte` vs `Docker-native`) | **None** — all connectors are Docker images | `DOCKER_IMAGE_LANGUAGES` eliminated |
| Onboarding infra | `docker_ops.py` (500+ lines) + `azure_job_runner.py` (600+ lines), 4 code paths | **1 code path** (start ACA, poll, read File Share) | ~900 lines removed |
| SDK version workarounds | 3 different SDK paths for `job_execution` polling | **1** (simplified, one ACA Job) | Eliminates brittle compat code |
| ACA job template resolve | `resolve_job_container()` — 60 lines parsing templates | **Gone** — no need to introspect worker template | Eliminates fragile SDK introspection |
| Environment variables | 20+ across 3 components | **~8** total | Much simpler configuration |
| PyAirbyte dependency | `airbyte` package in worker image | **Gone** — connectors run as Docker images | No pip install at runtime |

---

## Detailed Design

### 1. ACA Job — Direct Connector Execution

The single ACA Job is a "generic execution slot." Its base image doesn't matter — every execution overrides it with the actual connector image via `begin_start()`.

```python
# Shared helper — used by BOTH orchestrator and backend

def start_connector_execution(
    docker_image: str,
    airbyte_command: str,    # "check", "discover", "read"
    work_id: str,            # unique ID for this execution's File Share directory
    *,
    extra_args: str = "",    # e.g. "--catalog /data/{work_id}/catalog.json"
) -> str:
    """Start a connector image on the single ACA Job. Returns execution_name."""
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.appcontainers import ContainerAppsAPIClient

    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, ACA_SUBSCRIPTION_ID)

    shell_script = (
        f"$AIRBYTE_ENTRYPOINT {airbyte_command} "
        f"--config /data/{work_id}/config.json "
        f"{extra_args} "
        f"> /data/{work_id}/output.jsonl "
        f"2>/data/{work_id}/stderr.log "
        f"|| true"
    )

    container_override = {
        "name": ACA_JOB_CONTAINER_NAME,
        "image": docker_image,
        "command": ["/bin/sh"],
        "args": ["-c", shell_script],
        "env": [{"name": "AIRBYTE_ENABLE_UNSAFE_CODE", "value": "true"}],
    }

    result = client.jobs.begin_start(
        resource_group_name=ACA_RESOURCE_GROUP,
        job_name=ACA_JOB_NAME,
        template={"containers": [container_override]},
    ).result()

    return str(getattr(result, "name", ""))
```

**That's it.** One function, ~30 lines. Replaces `_start_pyairbyte_job()`, `_start_docker_native_job()`, `_launch_connector_aca_job()`, `run_onboarding_aca_job()`, `run_onboarding_docker_native_job()`, and `_launch_docker_job_and_wait()` — hundreds of lines collapsed into one.

### 2. File Share I/O — Simple Read/Write Helpers

```python
# Shared File Share helpers — used by orchestrator and backend

def write_to_fileshare(work_id: str, filename: str, content: str) -> None:
    """Write a file to /data/{work_id}/{filename} on the Azure File Share."""
    from azure.storage.fileshare import ShareDirectoryClient, ShareFileClient
    from azure.core.exceptions import ResourceExistsError

    dir_client = ShareDirectoryClient.from_connection_string(
        conn_str=AZURE_STORAGE_CONNECTION_STRING,
        share_name=FILE_SHARE_NAME,
        directory_path=work_id,
    )
    try:
        dir_client.create_directory()
    except ResourceExistsError:
        pass

    file_client = ShareFileClient.from_connection_string(
        conn_str=AZURE_STORAGE_CONNECTION_STRING,
        share_name=FILE_SHARE_NAME,
        file_path=f"{work_id}/{filename}",
    )
    file_client.upload_file(content.encode("utf-8"))


def read_from_fileshare(work_id: str, filename: str) -> str:
    """Read a file from /data/{work_id}/{filename} on the Azure File Share."""
    from azure.storage.fileshare import ShareFileClient

    file_client = ShareFileClient.from_connection_string(
        conn_str=AZURE_STORAGE_CONNECTION_STRING,
        share_name=FILE_SHARE_NAME,
        file_path=f"{work_id}/{filename}",
    )
    return file_client.download_file().readall().decode("utf-8", errors="replace")


def cleanup_fileshare(work_id: str) -> None:
    """Delete /data/{work_id}/ from the File Share."""
    from azure.storage.fileshare import ShareDirectoryClient
    dir_client = ShareDirectoryClient.from_connection_string(
        conn_str=AZURE_STORAGE_CONNECTION_STRING,
        share_name=FILE_SHARE_NAME,
        directory_path=work_id,
    )
    # delete all files in dir, then delete dir
    for item in dir_client.list_directories_and_files():
        dir_client.delete_file(item["name"])
    dir_client.delete_directory()
```

### 3. Orchestrator — State Machine on Timer

The orchestrator becomes a simple two-phase state machine that runs on each timer tick:

**Phase A:** Check completed connector executions → convert output → upload Parquet
**Phase B:** Start new connector executions for eligible configs

```python
# Simplified sync_orchestrator/__init__.py

def main(timer: func.TimerRequest) -> None:
    supabase = get_supabase()

    # ── Phase A: Process completed connector runs ─────────────────
    reading_configs = get_configs_by_status(supabase, "reading")
    for config in reading_configs:
        execution_name = config["aca_execution_name"]
        work_id = config["aca_work_id"]
        config_id = config["id"]

        status = poll_execution_status(execution_name)

        if status == "running":
            continue  # still going, check next tick

        if status == "succeeded":
            try:
                # Read connector JSONL output from File Share
                jsonl = read_from_fileshare(work_id, "output.jsonl")
                records_by_stream = parse_airbyte_records(jsonl)

                if not records_by_stream:
                    mark_failed(supabase, config_id, "No records in connector output")
                    continue

                # Convert to Parquet + upload to Blob
                uploaded = convert_and_upload(
                    records_by_stream,
                    user_id=config["user_id"],
                    docker_image=config["docker_image"],
                )
                mark_success(supabase, config_id, files=len(uploaded))
                cleanup_fileshare(work_id)

            except Exception as exc:
                mark_failed(supabase, config_id, str(exc))
        else:
            # Failed or timed out
            stderr = read_from_fileshare(work_id, "stderr.log") or "Unknown error"
            mark_failed(supabase, config_id, stderr[:2000])
            cleanup_fileshare(work_id)

    # ── Phase B: Start new syncs for eligible configs ─────────────
    eligible = get_eligible_configs(supabase)  # same eligibility logic as today
    for config in eligible:
        config_id = config["id"]
        correlation_id = str(uuid4())
        work_id = f"sync-{config_id}-{correlation_id[:8]}"

        try:
            # Refresh credentials if needed
            user_config = refresh_credentials_if_needed(config)

            # Get catalog (cached from onboarding, or run discover)
            catalog = config.get("discovered_catalog")
            if not catalog:
                catalog = run_discover_and_cache(config, work_id)

            # Build configured catalog for selected streams
            configured = build_configured_catalog(
                catalog, config.get("selected_streams")
            )

            # Write config + catalog to File Share
            write_to_fileshare(work_id, "config.json", json.dumps(user_config))
            write_to_fileshare(work_id, "catalog.json", json.dumps(configured))

            # Start connector directly on the ACA Job
            execution_name = start_connector_execution(
                docker_image=config["docker_image"],
                airbyte_command="read",
                work_id=work_id,
                extra_args=f"--catalog /data/{work_id}/catalog.json",
            )

            # Store execution tracking info
            mark_reading(supabase, config_id, execution_name, work_id, correlation_id)

        except Exception as exc:
            mark_failed(supabase, config_id, str(exc))

    log.info("orchestrator_tick",
        reading_checked=len(reading_configs),
        new_started=len(eligible))
```

**What's fundamentally different:**
- **No sync-worker container.** The orchestrator writes files, starts the connector, and processes the output itself.
- **No language routing.** ALL connectors run the same way — official Docker image on the ACA Job.
- **No long-running poll loops.** The timer fires every 5 min; each tick checks statuses and moves the state machine forward. No `while True: poll; sleep(10)` inside a container.
- **Parquet conversion happens in the Function.** For typical sync volumes (thousands of records, <50 MB JSONL), this takes seconds and fits in Function memory (1.5 GB on Consumption plan).

### 4. Onboarding — Same ACA Job, Inline Poll

For onboarding, the backend calls the SAME ACA Job and polls inline (the user is waiting):

```python
# Simplified docker_ops.py (cloud path)

def docker_check_connection(docker_image: str, config: dict) -> tuple[bool, str]:
    """Test connection via the single ACA Job."""
    work_id = f"onb-check-{uuid4().hex[:12]}"
    clean = {k: v for k, v in config.items() if not k.startswith("__")}

    write_to_fileshare(work_id, "config.json", json.dumps(clean, default=str))

    execution_name = start_connector_execution(
        docker_image=docker_image,
        airbyte_command="check",
        work_id=work_id,
    )

    success = wait_for_execution(execution_name, timeout=120)

    if success:
        jsonl = read_from_fileshare(work_id, "output.jsonl")
        ok, message = parse_connection_status(jsonl)
    else:
        stderr = read_from_fileshare(work_id, "stderr.log")
        ok, message = False, f"Check failed: {stderr[:500]}"

    cleanup_fileshare(work_id)
    return ok, message


def docker_discover_streams(docker_image: str, config: dict) -> tuple[bool, list[str], str]:
    """Discover streams via the single ACA Job."""
    work_id = f"onb-discover-{uuid4().hex[:12]}"
    clean = {k: v for k, v in config.items() if not k.startswith("__")}

    write_to_fileshare(work_id, "config.json", json.dumps(clean, default=str))

    execution_name = start_connector_execution(
        docker_image=docker_image,
        airbyte_command="discover",
        work_id=work_id,
    )

    success = wait_for_execution(execution_name, timeout=180)

    if success:
        jsonl = read_from_fileshare(work_id, "output.jsonl")
        streams, catalog = parse_catalog(jsonl)
        cleanup_fileshare(work_id)
        return True, streams, f"Discovered {len(streams)} streams."
    else:
        stderr = read_from_fileshare(work_id, "stderr.log")
        cleanup_fileshare(work_id)
        return False, [], f"Discover failed: {stderr[:500]}"


def docker_read_probe(docker_image, config, stream_names, **kwargs) -> tuple[bool, int, str, str]:
    """Bounded read test via the single ACA Job (2-phase: discover → read)."""
    work_id = f"onb-probe-{uuid4().hex[:12]}"
    clean = {k: v for k, v in config.items() if not k.startswith("__")}
    max_records = kwargs.get("max_records", 50)

    write_to_fileshare(work_id, "config.json", json.dumps(clean, default=str))

    # Phase 1: Discover to get real catalog (with json_schema)
    exec1 = start_connector_execution(docker_image, "discover", work_id)
    if not wait_for_execution(exec1, timeout=180):
        cleanup_fileshare(work_id)
        return False, 0, "Discover phase failed", ""

    jsonl = read_from_fileshare(work_id, "output.jsonl")
    _, catalog = parse_catalog(jsonl)
    if not catalog:
        cleanup_fileshare(work_id)
        return False, 0, "No catalog found", ""

    # Phase 2: Read with bounded catalog
    selected = (stream_names or list_stream_names(catalog))[:1]
    configured = build_configured_catalog(catalog, selected)
    write_to_fileshare(work_id, "catalog.json", json.dumps(configured, default=str))

    # Pipe through head to cap output
    exec2 = start_connector_execution(
        docker_image, "read", work_id,
        extra_args=f"--catalog /data/{work_id}/catalog.json",
    )
    if not wait_for_execution(exec2, timeout=kwargs.get("read_timeout", 300)):
        cleanup_fileshare(work_id)
        return False, 0, "Read phase failed", ""

    jsonl = read_from_fileshare(work_id, "output.jsonl")
    record_count = count_records(jsonl, max_records)
    cleanup_fileshare(work_id)
    return True, record_count, f"Read probe succeeded ({record_count} records)", ""
```

**Key properties:**
- **Same ACA Job** as scheduled syncs — one resource for everything
- **Same File Share** — config in, output out
- **Same `start_connector_execution()`** helper — no code duplication
- **Inline polling** for onboarding (user is waiting) vs. timer-driven polling for scheduled syncs
- Local dev still uses `docker run` subprocess as before (kept for development convenience)

### 5. Execution Polling — Simplified to One ACA Job

```python
# Shared polling helper — ONE ACA Job, ONE polling function

def poll_execution_status(execution_name: str) -> str:
    """Check ACA Job execution status. Returns 'running', 'succeeded', or 'failed'."""
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.appcontainers import ContainerAppsAPIClient

    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, ACA_SUBSCRIPTION_ID)

    try:
        execution = client.job_execution(
            resource_group_name=ACA_RESOURCE_GROUP,
            job_name=ACA_JOB_NAME,
            job_execution_name=execution_name,
        )
        status = (
            getattr(getattr(execution, "properties", None), "status", None)
            or getattr(execution, "status", "")
            or ""
        ).strip().lower()

        if status in ("succeeded", "completed", "success"):
            return "succeeded"
        if status in ("failed", "canceled", "cancelled", "stopped", "error"):
            return "failed"
        return "running"

    except Exception as exc:
        log.warning("poll_error", execution=execution_name, error=str(exc))
        return "running"  # assume still going if we can't check


def wait_for_execution(execution_name: str, timeout: int = 300) -> bool:
    """Block until execution completes. Used by onboarding (user is waiting)."""
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = poll_execution_status(execution_name)
        if status == "succeeded":
            return True
        if status == "failed":
            return False
        time.sleep(5)
    return False  # timed out
```

No more `_get_execution()` vs `_get_docker_job_execution()` — one function, one ACA Job.

### 6. Catalog Caching — Skip Discover During Scheduled Syncs

Currently the Docker-native path runs `discover` before every `read`, adding a full container startup per sync.

**Fix:** Cache the discovered catalog in Supabase during onboarding:

```sql
ALTER TABLE user_connector_configs
ADD COLUMN discovered_catalog JSONB;
ADD COLUMN aca_execution_name TEXT;
ADD COLUMN aca_work_id TEXT;
ADD COLUMN consecutive_failures INT DEFAULT 0;
```

During onboarding `discover_streams()`, save the catalog. During scheduled sync, load it and skip discover entirely. This eliminates one ACA execution per scheduled sync.

### 7. Structured Logging & Correlation IDs

```python
import structlog

log = structlog.get_logger()

# Orchestrator
log.info("sync_dispatched", config_id=config_id, execution=execution_name,
         correlation_id=correlation_id)
log.info("sync_output_processed", config_id=config_id, streams=len(records),
         rows=total_rows, correlation_id=correlation_id)

# Onboarding
log.info("onboarding_check", image=docker_image, execution=execution_name,
         work_id=work_id)
log.info("onboarding_discover", streams=len(streams), work_id=work_id)
```

**All logs converge to two places:** Azure Functions logs (orchestrator) and App Service logs (backend/onboarding). No ACA container logs to hunt for.

### 8. Reliability Improvements

#### a. Circuit Breaker in Orchestrator

```python
# Skip configs that have failed too many times in a row
if config.get("consecutive_failures", 0) >= 5:
    log.warning("circuit_breaker_open", config_id=config_id)
    continue
```

#### b. Idempotent Blob Writes

```python
# Deduplicate on merge by _ab_id if available
if "_ab_id" in new_df.columns and "_ab_id" in existing_df.columns:
    merged = pd.concat([existing_df, new_df]).drop_duplicates(
        subset=["_ab_id"], keep="last"
    )
else:
    merged = pd.concat([existing_df, new_df])
```

#### c. File Share Cleanup on Every Terminal Status

```python
# Always clean up, whether success or failure
finally:
    cleanup_fileshare(work_id)
```

---

## Environment Variables — Before vs. After

### Before (20+ variables across 3 components)

```
# Orchestrator Azure Function
SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP
ACA_JOB_NAME, ACA_JOB_CONTAINER_NAME
ACA_DOCKER_JOB_NAME, ACA_DOCKER_JOB_CONTAINER_NAME

# Worker (shared)
SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME
SYNC_CONFIG_ID, SYNC_USER_ID, SYNC_CONNECTOR_NAME

# Worker (Docker-native path)
SYNC_PHASE, SYNC_DOCKER_IMAGE
ACA_DOCKER_JOB_NAME, AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP
ACA_DOCKER_CONNECTOR_CONTAINER_NAME
DOCKER_OUTPUT_DIR, DOCKER_JOB_TIMEOUT, DOCKER_JOB_POLL_INTERVAL

# Backend (onboarding)
ONBOARDING_DOCKER_ENABLED, ONBOARDING_DOCKER_EXECUTION_MODE
ONBOARDING_ACA_SUBSCRIPTION_ID, ONBOARDING_ACA_RESOURCE_GROUP
ONBOARDING_ACA_JOB_NAME, ONBOARDING_ACA_JOB_CONTAINER_NAME
ONBOARDING_ACA_DOCKER_JOB_NAME, ONBOARDING_ACA_DOCKER_JOB_CONTAINER_NAME
ONBOARDING_ACA_WAIT_TIMEOUT_SECONDS, ONBOARDING_ACA_POLL_INTERVAL_SECONDS
AZURE_FILE_SHARE_NAME
```

### After (~8 variables, shared across orchestrator + backend)

```
# Shared ACA + File Share config
ACA_SUBSCRIPTION_ID
ACA_RESOURCE_GROUP
ACA_JOB_NAME
ACA_JOB_CONTAINER_NAME
AZURE_STORAGE_CONNECTION_STRING
FILE_SHARE_NAME

# Orchestrator + backend (data plane)
SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
BLOB_CONTAINER_NAME
```

---

## Azure Resources — Before vs. After

### Before (8 resources)

| Resource | Type | Purpose |
|----------|------|---------|
| `func-purelybi-sync-orchestrator-dev-ci` | Function App | Timer → start ACA Jobs |
| `func-purelybi-schema-updater-dev-ci` | Function App | Daily registry sync |
| `caj-purelybi-data-sync-dev-ci` | Container Apps Job | Runs sync-worker image |
| `caj-pbi-docker-connector-dev-ci` | Container Apps Job | Runs official Airbyte images |
| `caenv-purelybi-dev-ci` | Container Apps Environment | Hosts both ACA Jobs |
| `connector-data` | Azure File Share | Docker I/O bridge |
| `acrpurelybidevci.azurecr.io` | Container Registry | Stores sync-worker image |
| `sapurelybidatalakedevci` | Storage Account (Blob) | Parquet output |

### After (5 resources)

| Resource | Type | Change |
|----------|------|--------|
| `func-purelybi-sync-orchestrator-dev-ci` | Function App | Simplified (state machine) |
| `func-purelybi-schema-updater-dev-ci` | Function App | Unchanged |
| `caj-purelybi-connector-dev-ci` | Container Apps Job | **Renamed/reused** — runs connector images directly |
| `connector-data` | Azure File Share | **Reused** — same purpose, simpler usage |
| `sapurelybidatalakedevci` | Storage Account (Blob) | Unchanged |
| ~~`caj-purelybi-data-sync-dev-ci`~~ | ~~Container Apps Job~~ | **Deleted** (sync-worker ACA Job) |
| ~~`caenv-purelybi-dev-ci`~~ | Container Apps Environment | **Kept** (still needed for the one ACA Job) |
| ~~`acrpurelybidevci.azurecr.io`~~ | ~~Container Registry~~ | **Deleted** (no custom image to store) |

**Net:** -2 resources (sync-worker ACA Job + ACR), CA Environment kept

---

## Cost at Scale

**Per-sync ACA cost** (connector only, no sync-worker sitting idle):

| | Current (2 containers) | Proposed (1 container) |
|--|------------------------|----------------------|
| Connector runs ~2 min | $0.0065 | $0.0065 |
| Sync-worker polling ~5 min | $0.016 | **$0** (eliminated) |
| **Total per sync** | **$0.023** | **$0.0065** |

**65% cost reduction per sync** by eliminating the idle sync-worker container.

| Users | Syncs/day | Current cost/month | Proposed cost/month | Savings |
|------:|----------:|-------------------:|--------------------:|--------:|
| 10 | 80 | ~$0 (free tier) | ~$0 (free tier) | — |
| 100 | 800 | ~$552 | ~$156 | **72%** |
| 1,000 | 8,000 | ~$5,520 | ~$1,560 | **72%** |
| 10,000 | 80,000 | ~$55,200 | ~$15,600 | **72%** |

**Serverless scaling with no idle cost.** Zero compute charges when no syncs are running.

---

## How Large Syncs Are Handled

**Concern:** The orchestrator Function now parses JSONL and converts to Parquet. Can Azure Functions handle this?

| Connector output | JSONL size | Parquet time | Memory | Functions OK? |
|-----------------|-----------|-------------|--------|--------------|
| Shopify orders (1K rows) | ~2 MB | <1s | ~50 MB | Yes |
| Facebook ads (10K rows) | ~20 MB | ~2s | ~200 MB | Yes |
| Large MongoDB export (100K rows) | ~200 MB | ~10s | ~1 GB | Marginal (1.5 GB limit) |
| Huge export (1M+ rows) | ~2 GB | ~60s+ | ~4 GB+ | **No** |

**For the vast majority of syncs** (sub-100K rows), the Function handles it fine. For the rare large export:

**Escape hatch:** Keep a lightweight "uploader" ACA execution (same single ACA Job, but image overridden to a small Python image with pandas/azure-storage-blob) that reads JSONL from File Share and uploads to Blob. The orchestrator detects large output files and delegates to this uploader instead of processing in-Function.

This is an optimization you add later if needed — most connectors produce manageable output.

---

## Local Development

For local dev, `docker_ops.py` keeps the `docker run` subprocess path:

```python
def docker_check_connection(docker_image: str, config: dict) -> tuple[bool, str]:
    if _use_azure_job_mode():
        return _aca_check_connection(docker_image, config)  # cloud path
    return _local_docker_check(docker_image, config)  # existing docker run subprocess
```

The `_local_docker_check` is the existing code from `docker_ops.py` — it already works. No change needed for local dev.

---

## Migration Path

### Phase 1: Add tracking columns to Supabase
```sql
ALTER TABLE user_connector_configs
ADD COLUMN discovered_catalog JSONB,
ADD COLUMN aca_execution_name TEXT,
ADD COLUMN aca_work_id TEXT,
ADD COLUMN consecutive_failures INT DEFAULT 0;
```

### Phase 2: Extract shared helpers
Create a shared `connector_runner.py` module with `start_connector_execution()`, `poll_execution_status()`, `wait_for_execution()`, File Share I/O helpers. These are used by both the orchestrator and the backend.

### Phase 3: Simplify onboarding
1. Replace `azure_job_runner.py` with calls to the shared helpers + single ACA Job
2. Simplify `docker_ops.py` cloud path to use shared helpers
3. Populate `discovered_catalog` during onboarding discover
4. Test onboarding check/discover/probe end-to-end

### Phase 4: Rewrite orchestrator
1. Implement the two-phase state machine (check completed → start new)
2. Add JSONL parsing + Parquet conversion + Blob upload in the Function
3. Remove language routing, `resolve_job_container()`, PyAirbyte references
4. Test scheduled sync end-to-end

### Phase 5: Delete old resources
1. Delete sync-worker ACA Job (`caj-purelybi-data-sync-dev-ci`)
2. Delete ACR (or stop pushing sync-worker images)
3. Remove `sync_worker.py`, `Dockerfile.worker`, `credential_refresh.py` (move cred refresh to shared module)
4. Remove all 20+ eliminated env vars
5. Add structured logging + correlation IDs

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|-----------|
| Functions memory limit (1.5 GB) too small for large exports | Low (most syncs <100K rows) | Stream JSONL parsing; or delegate to uploader ACA execution for large files |
| Functions timeout (10 min on Consumption) | Low (output processing is fast) | Use Premium plan if needed ($100/month); or offload large processing |
| ACA Job cold start for onboarding | Medium (~10-30s for first execution) | Keep CA Environment always on (minimal idle cost); most connector images are cached after first pull |
| Connector image pull time (first use) | Medium (large images like MongoDB ~1-2 min) | Pre-pull popular images via a scheduled job; subsequent pulls are cached |
| File Share becomes a bottleneck at high concurrency | Very low (Azure Files scales well) | Monitor; upgrade Share tier if needed |
| Orchestrator timer interval (5 min) adds latency to sync completion | Low (acceptable for batch syncs) | Reduce timer to 2 min if needed |

---

## What Stays the Same

- **Schema Updater Function** — unchanged
- **Orchestrator eligibility logic** — unchanged (same Supabase query, same sync_mode/frequency rules)
- **Credential refresh logic** — moves from sync_worker to a shared module, same code
- **Blob storage layout** — unchanged (`user-data/{user_id}/{source}/{stream}/{YYYY-MM}.parquet`)
- **Airbyte protocol parsing** — same JSONL parsing (already exists in `_parse_airbyte_jsonl`, `_parse_discover_output`)
- **Onboarding agent flow** — unchanged from user perspective
- **File Share** — reused (same resource, simpler usage pattern)
- **Container Apps Environment** — kept (hosts the single ACA Job)

---

## Summary

| Concern | Current | Proposed |
|---------|---------|----------|
| **Azure resources** | 8 (2 Functions, 2 ACA Jobs, CA Env, File Share, ACR, Blob) | 5 (2 Functions, 1 ACA Job, File Share, Blob) |
| **Containers per sync** | 2 (sync-worker + connector) | 1 (connector only) |
| **Code paths for "run connector"** | 4 (PyAirbyte, Docker ACA, local Docker, onboarding ACA probe) | 1 cloud path + 1 local dev path |
| **Orchestrator logic** | Language routing → start worker → worker starts connector | Start connector directly → process output on completion |
| **Onboarding infrastructure** | `docker_ops.py` (500 LOC) + `azure_job_runner.py` (600 LOC) | ~100 lines calling shared helpers |
| **Where to find logs** | Functions + ACA sync-worker + ACA connector | Functions + App Service only |
| **Cost per sync** | ~$0.023 (2 containers, one idle) | ~$0.0065 (1 container) |
| **Idle cost** | $0 (serverless) | $0 (serverless) |
| **Scale** | Serverless, auto-scales | Serverless, auto-scales |

The fundamental simplification: **the orchestrator and backend talk to the ACA Job directly instead of through an intermediary sync-worker container.** The sync-worker was just a middleman — remove it, and the architecture collapses from 4 code paths to 1.
