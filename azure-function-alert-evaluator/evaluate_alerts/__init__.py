"""
Azure Function — Alert Evaluator (Timer Trigger)

Runs every 5 minutes. For each enabled alert:
  1. Spins up a DuckDB sandbox with the tenant's Parquet data.
  2. Runs the alert's ``sql_query``.
  3. Applies the comparator + threshold.
  4. Records an ``alert_runs`` row.
  5. If firing, inserts an ``alert_notifications`` row and sends via ACS Email.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timezone

import azure.functions as func
import duckdb
from azure.storage.blob import ContainerClient
from supabase import Client, create_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
ACS_CONN_STR = os.environ.get("ACS_CONNECTION_STRING", "")
EMAIL_SENDER = os.environ.get("ALERT_EMAIL_SENDER", "alerts@purelybi.com")
SQL_TIMEOUT = int(os.environ.get("ALERT_SQL_TIMEOUT_SECONDS", "30"))
STORAGE_CONN_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "raw")
PREFIX_ROOT = os.environ.get("USER_DATA_BLOB_PREFIX", "user-data").strip("/") or "user-data"
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "256MB")

_SAFE_TENANT_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_HIVE_SEGMENT_RE = re.compile(r"^[^=]+=.")


# ---------------------------------------------------------------------------
# Supabase client (service-role — bypasses RLS)
# ---------------------------------------------------------------------------
def _supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# DuckDB sandbox (lightweight copy of backend duckdb_sandbox.py)
# ---------------------------------------------------------------------------
def _discover_views(tenant_id: str) -> dict[str, str]:
    """Discover parquet views for a tenant (simplified from duckdb_sandbox)."""
    if not _SAFE_TENANT_ID_RE.match(tenant_id):
        raise ValueError("Invalid tenant id format.")

    container = ContainerClient.from_connection_string(
        STORAGE_CONN_STR, container_name=CONTAINER
    )
    prefix = f"{PREFIX_ROOT}/{tenant_id}/"
    dir_files: dict[str, list[str]] = {}

    for blob in container.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith(".parquet"):
            continue
        relative = blob.name[len(prefix):]
        parts = relative.split("/")
        parent = "/".join(parts[:-1])
        if parent:
            dir_files.setdefault(parent, []).append(parts[-1])

    # Detect Hive-partitioned directory trees
    hive_roots = set()
    hive_dirs = set()
    for dir_path in list(dir_files.keys()):
        parts = dir_path.split("/")
        for i, part in enumerate(parts):
            if _HIVE_SEGMENT_RE.match(part):
                root = "/".join(parts[:i])
                if root:
                    hive_roots.add(root)
                    hive_dirs.add(dir_path)
                break

    base_url = f"azure://{CONTAINER}/{prefix}"
    views: dict[str, str] = {}

    for root in sorted(hive_roots):
        view_name = root.replace("/", "_").replace("-", "_")
        views[view_name] = f"{base_url}{root}/**/*.parquet"

    for dir_path in sorted(dir_files):
        if dir_path in hive_dirs or any(dir_path.startswith(r + "/") for r in hive_roots):
            continue
        view_name = dir_path.replace("/", "_").replace("-", "_")
        views[view_name] = f"{base_url}{dir_path}/*.parquet"

    return views


def _create_sandbox(tenant_id: str) -> tuple[duckdb.DuckDBPyConnection, list[str]]:
    """Create an in-memory DuckDB with materialised tables for the tenant."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL azure; LOAD azure;")
    conn.execute("SET threads=2")
    conn.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    if sys.platform == "linux":
        conn.execute("SET azure_transport_option_type = 'curl';")
        # CA bundle fix
        try:
            import certifi
            ca = certifi.where()
            if os.path.isfile(ca):
                os.environ.setdefault("CURL_CA_INFO", ca)
        except ImportError:
            pass

    safe_cs = STORAGE_CONN_STR.replace("'", "''")
    conn.execute(f"CREATE SECRET azure_creds (TYPE AZURE, CONNECTION_STRING '{safe_cs}');")

    views = _discover_views(tenant_id)
    succeeded = []
    for view_name, blob_path in views.items():
        try:
            hive_opt = ", hive_partitioning=true" if "**/" in blob_path else ""
            conn.execute(
                f"CREATE TABLE {view_name} AS "
                f"SELECT * FROM read_parquet('{blob_path}', union_by_name=true{hive_opt})"
            )
            succeeded.append(view_name)
        except Exception:
            logger.exception("Failed materialising table %s", view_name)

    return conn, succeeded


# ---------------------------------------------------------------------------
# Comparators
# ---------------------------------------------------------------------------
def _compare(observed: float, comparator: str, threshold: float) -> bool:
    """Apply a comparison operator."""
    if comparator == "gt":
        return observed > threshold
    elif comparator == "gte":
        return observed >= threshold
    elif comparator == "lt":
        return observed < threshold
    elif comparator == "lte":
        return observed <= threshold
    elif comparator == "eq":
        return observed == threshold
    elif comparator == "neq":
        return observed != threshold
    return False


# ---------------------------------------------------------------------------
# Email notification via Azure Communication Services
# ---------------------------------------------------------------------------
def _send_email(to: str, subject: str, body_html: str) -> str | None:
    """Send an email via ACS. Returns the provider message ID or None on failure."""
    if not ACS_CONN_STR:
        logger.warning("ACS_CONNECTION_STRING not set — skipping email.")
        return None
    try:
        from azure.communication.email import EmailClient

        # Fix missing '=' padding on the base64 accesskey which causes binascii.Error
        conn_str = ACS_CONN_STR
        parts = conn_str.split(";")
        fixed_parts = []
        for part in parts:
            if part.lower().startswith("accesskey="):
                key = part.split("=", 1)[1]
                missing_padding = len(key) % 4
                if missing_padding:
                    key += "=" * (4 - missing_padding)
                part = f"accesskey={key}"
            fixed_parts.append(part)
        conn_str = ";".join(fixed_parts)

        client = EmailClient.from_connection_string(conn_str)
        message = {
            "senderAddress": EMAIL_SENDER,
            "recipients": {"to": [{"address": to}]},
            "content": {"subject": subject, "html": body_html},
        }
        poller = client.begin_send(message)
        result = poller.result()
        return result.message_id
    except Exception:
        logger.exception("Failed sending alert email to %s", to)
        return None


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def main(timer: func.TimerRequest) -> None:
    """Evaluate all due alerts."""
    utc_now = datetime.now(timezone.utc)
    logger.info("Alert evaluator fired at %s", utc_now.isoformat())

    sb = _supabase()

    # Fetch all enabled alerts
    rows = (
        sb.table("alerts")
        .select("*")
        .eq("enabled", True)
        .execute()
    ).data or []

    logger.info("Found %d enabled alerts", len(rows))

    for alert_row in rows:
        alert_id = alert_row["id"]
        user_id = alert_row["user_id"]

        logger.info("Evaluating alert %s for user %s", alert_id, user_id)

        sql_query = alert_row["sql_query"]
        comparator = alert_row["comparator"]
        threshold = float(alert_row["threshold"])
        channel = alert_row.get("notification_channel", "email")
        target = alert_row.get("notification_target")
        definition = alert_row.get("definition", {})

        observed_value: float | None = None
        run_status = "ok"
        error_message: str | None = None

        conn: duckdb.DuckDBPyConnection | None = None
        try:
            conn, _tables = _create_sandbox(user_id)
            result = conn.execute(sql_query).fetchone()
            if result is None or result[0] is None:
                run_status = "error"
                error_message = "Query returned no rows or NULL."
            else:
                observed_value = float(result[0])
                fired = _compare(observed_value, comparator, threshold)
                run_status = "firing" if fired else "ok"
        except Exception as exc:
            run_status = "error"
            error_message = str(exc)[:500]
            logger.exception("Evaluation failed for alert %s", alert_id)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

        # Record the run
        notification_id = None
        if run_status == "firing" and target:
            # Send notification
            alert_name = alert_row.get("name", "Alert")
            metric_desc = definition.get("metric_description", "metric")
            subject = f"🚨 Alert: {alert_name}"
            body = (
                f"<h2>{alert_name}</h2>"
                f"<p>Your alert <strong>{alert_name}</strong> has triggered.</p>"
                f"<p><strong>Metric:</strong> {metric_desc}</p>"
                f"<p><strong>Observed value:</strong> {observed_value}</p>"
                f"<p><strong>Condition:</strong> {comparator} {threshold}</p>"
                f"<p><em>Evaluated at {utc_now.strftime('%Y-%m-%d %H:%M UTC')}</em></p>"
            )
            provider_id = _send_email(target, subject, body)

            # Record notification
            notif_row = (
                sb.table("alert_notifications")
                .insert({
                    "alert_id": alert_id,
                    "user_id": user_id,
                    "channel": channel,
                    "target": target,
                    "payload": {
                        "subject": subject,
                        "body_html": body,
                    },
                    "provider_id": provider_id,
                    "delivered_at": utc_now.isoformat() if provider_id else None,
                    "error_message": None if provider_id else "Email send failed",
                })
                .execute()
            ).data
            if notif_row:
                notification_id = notif_row[0]["id"]

        # Insert alert_runs row
        sb.table("alert_runs").insert({
            "alert_id": alert_id,
            "user_id": user_id,
            "status": run_status,
            "observed_value": observed_value,
            "error_message": error_message,
            "notification_id": notification_id,
        }).execute()

        # Update alert's last_evaluated_at and last_state
        update_payload: dict = {
            "last_evaluated_at": utc_now.isoformat(),
            "last_state": run_status,
        }
        if run_status == "firing":
            update_payload["last_fired_at"] = utc_now.isoformat()
        sb.table("alerts").update(update_payload).eq("id", alert_id).execute()

        logger.info(
            "Alert %s: status=%s, value=%s",
            alert_id, run_status, observed_value,
        )

    logger.info("Alert evaluator completed. Processed %d alerts.", len(rows))
