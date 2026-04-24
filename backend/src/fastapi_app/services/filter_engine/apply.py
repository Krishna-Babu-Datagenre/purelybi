"""High-level orchestration: take widget SQL + a :class:`FilterSpec` and
return SQL with filters baked in.

This module is the only thing widget_data_service needs to call.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import duckdb

from fastapi_app.models.filters import FilterSpec

from .build_views import FilterApplication, build_view_plans, rewrite_sql
from .detect_tables import detect_referenced_tables
from .relationships import RelationshipGraph

logger = logging.getLogger(__name__)


def apply_filters(
    sql: str,
    *,
    spec: FilterSpec | None,
    conn: duckdb.DuckDBPyConnection | None = None,
    relationships: list[dict] | None = None,
    existing_params: tuple | list | None = None,
) -> tuple[str, list[Any], FilterApplication | None]:
    """Return ``(rewritten_sql, params, application)``.

    *spec* may be ``None`` or empty \u2014 in that case the SQL is returned
    unchanged. *application* exposes the planned per-table predicates and
    any filters that had to be skipped (for telemetry).
    """
    base_params: list[Any] = list(existing_params or ())
    if spec is None or spec.is_empty():
        return sql, base_params, None

    scanning = detect_referenced_tables(sql, conn=conn)
    if not scanning:
        logger.info("No base tables detected in widget SQL; skipping filter injection.")
        return sql, base_params, None

    graph = RelationshipGraph(relationships or [])
    application = build_view_plans(spec, scanning_tables=scanning, graph=graph)
    if not application.plans:
        logger.info(
            "event=filter_no_plans tables_scanned=%s skipped=%s",
            sorted(scanning),
            application.skipped or [],
        )
        return sql, base_params, application

    t0 = time.perf_counter()
    rewritten, params = rewrite_sql(
        sql, application.plans, existing_params=base_params
    )
    rewrite_ms = (time.perf_counter() - t0) * 1000

    applied_tables = sorted({p.table for p in application.plans})
    logger.info(
        "event=filter_applied tables=%s skipped=%s rewrite_ms=%.1f filter_count=%d",
        applied_tables,
        application.skipped or [],
        rewrite_ms,
        len(spec.filters or []) + (1 if spec.time else 0),
    )
    return rewritten, params, application


# The TEMP-VIEW shadow approach described in the plan would be installed and
# dropped via these helpers. We keep stubs so the public API is forward-
# compatible if the sandbox grows a ``_raw.*`` schema later. They are no-ops
# today.


def install_filter_views(*args, **kwargs) -> None:  # pragma: no cover - stub
    return None


def drop_filter_views(*args, **kwargs) -> None:  # pragma: no cover - stub
    return None
