"""Entrypoint for the metadata-generator container.

Reads ``USER_ID`` and ``JOB_ID`` from the environment, walks the tenant's
DuckDB sandbox, runs the LLM describe + relationship steps, and writes the
results to Supabase.

Job lifecycle:
  pending → running → succeeded | failed
"""

from __future__ import annotations

import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

from db_inspect import open_sandbox, snapshot_all
from llm_describe import describe_table
from llm_relationships import discover_relationships
from upsert import (
    _client,
    delete_generated_metadata,
    fetch_connector_map,
    update_job,
    upsert_column_metadata,
    upsert_relationships,
    upsert_table_metadata,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("metadata-generator")


def _to_jsonable(value: Any) -> Any:
    """Coerce DuckDB / pyarrow scalars into JSON-serialisable primitives."""
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _column_payloads(snapshot, llm_payload) -> list[dict[str, Any]]:
    by_name = {c["name"]: c for c in llm_payload.get("columns", [])}
    out: list[dict[str, Any]] = []
    for col in snapshot.columns:
        info = by_name.get(col.name, {})
        sample_values = (
            [_to_jsonable(v) for v in (col.sample_values or [])]
            if col.sample_values
            else None
        )
        out.append(
            {
                "table_name": snapshot.name,
                "column_name": col.name,
                "data_type": col.data_type,
                "semantic_type": info.get("semantic_type") or "unknown",
                "description": info.get("description"),
                "is_filterable": True,
                "cardinality": col.cardinality,
                "sample_values": sample_values,
            }
        )
    return out


def run(user_id: str, job_id: str) -> int:
    """Run one full metadata generation pass. Returns process exit code."""
    client = _client()
    try:
        update_job(
            client,
            user_id=user_id,
            job_id=job_id,
            status="running",
            mark_started=True,
            message="Clearing stale metadata.",
            progress=2,
        )

        # Wipe auto-generated rows from previous runs so only fresh results
        # are visible.  User-edited rows are preserved.
        delete_generated_metadata(client, user_id=user_id)

        update_job(
            client,
            user_id=user_id,
            job_id=job_id,
            message="Opening DuckDB sandbox.",
            progress=5,
        )
        conn, tables = open_sandbox(user_id)

        if not tables:
            update_job(
                client,
                user_id=user_id,
                job_id=job_id,
                status="failed",
                mark_finished=True,
                error="No synced datasets found for this user.",
                progress=100,
            )
            return 0

        update_job(
            client,
            user_id=user_id,
            job_id=job_id,
            message=f"Inspecting {len(tables)} tables.",
            progress=10,
        )
        try:
            source_map = fetch_connector_map(client, user_id=user_id)
        except Exception:
            logger.exception("fetch_connector_map failed; continuing without source labels")
            source_map = {}
        snapshots = snapshot_all(conn, tables, source_map=source_map)

        # ── Per-table describe (LLM) ───────────────────────────────────
        table_payloads: list[dict[str, Any]] = []
        column_payloads: list[dict[str, Any]] = []
        total = max(1, len(snapshots))
        for idx, snap in enumerate(snapshots):
            update_job(
                client,
                user_id=user_id,
                job_id=job_id,
                message=f"Describing {snap.name} ({idx + 1}/{total}).",
                progress=10 + 60 * (idx / total),
            )
            llm_payload = describe_table(snap)
            table_payloads.append(
                {
                    "table_name": snap.name,
                    "description": llm_payload.get("description"),
                    "primary_date_column": llm_payload.get("primary_date_column"),
                    "grain": llm_payload.get("grain"),
                }
            )
            column_payloads.extend(_column_payloads(snap, llm_payload))

            # Write LLM semantic types back onto the snapshot so the
            # relationship engine can leverage them (e.g. "identifier").
            by_name = {c["name"]: c for c in llm_payload.get("columns", [])}
            for col in snap.columns:
                st = by_name.get(col.name, {}).get("semantic_type")
                if st:
                    col.semantic_type = st

        update_job(
            client,
            user_id=user_id,
            job_id=job_id,
            message="Persisting table and column metadata.",
            progress=72,
        )
        upsert_table_metadata(client, user_id=user_id, payloads=table_payloads)
        upsert_column_metadata(client, user_id=user_id, payloads=column_payloads)

        # ── Relationships (LLM proposal + DuckDB validation) ───────────
        update_job(
            client,
            user_id=user_id,
            job_id=job_id,
            message="Discovering and validating relationships.",
            progress=80,
        )
        edges = discover_relationships(conn, snapshots)
        upsert_relationships(client, user_id=user_id, edges=edges)

        update_job(
            client,
            user_id=user_id,
            job_id=job_id,
            status="succeeded",
            mark_finished=True,
            message=(
                f"Generated metadata for {len(snapshots)} tables and "
                f"{len(edges)} relationship edges."
            ),
            progress=100,
        )
        return 0

    except Exception as exc:
        logger.exception("metadata generation failed")
        update_job(
            client,
            user_id=user_id,
            job_id=job_id,
            status="failed",
            mark_finished=True,
            error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[:1500]}",
            progress=100,
        )
        return 1
    finally:
        client.close()


def main() -> int:
    user_id = os.environ.get("USER_ID", "").strip()
    job_id = os.environ.get("JOB_ID", "").strip()
    if not user_id or not job_id:
        logger.error("USER_ID and JOB_ID env vars are required.")
        return 2
    return run(user_id, job_id)


if __name__ == "__main__":
    sys.exit(main())
