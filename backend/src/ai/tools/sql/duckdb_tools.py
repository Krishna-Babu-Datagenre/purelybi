from __future__ import annotations

import json
import math
import re
from typing import Any

import duckdb
import pandas as pd
from langchain_core.language_models import BaseLanguageModel
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import tool

from ai.tools.sql.charts import get_session_context, store_query_snapshot

QUERY_CHECKER_PROMPT = PromptTemplate(
    input_variables=["dialect", "query"],
    template=(
        "{query}\n"
        "Double check the {dialect} query above for common mistakes, including:\n"
        "- Using NOT IN with NULL values\n"
        "- Using UNION when UNION ALL should have been used\n"
        "- Using BETWEEN for exclusive ranges\n"
        "- Data type mismatch in predicates\n"
        "- Properly quoting identifiers\n"
        "- Using the correct number of arguments for functions\n"
        "- Casting to the correct data type\n"
        "- Using the proper columns for joins\n\n"
        "If there are any of the above mistakes, rewrite the query. "
        "If there are no mistakes, just reproduce the original query.\n\n"
        "Output the final SQL query only.\n\nSQL Query: "
    ),
)

_DISALLOWED_SQL_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|COPY|EXPORT|IMPORT|INSTALL|LOAD|PRAGMA|CALL)\b",
    re.IGNORECASE,
)


def _is_readonly_sql(query: str) -> bool:
    q = query.strip().lstrip("(").upper()
    if not (q.startswith("SELECT") or q.startswith("WITH")):
        return False
    return _DISALLOWED_SQL_RE.search(query) is None


def _safe_number(val: Any) -> float | None:
    """Convert a numeric value to a JSON-safe float, or None if not finite."""
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _summarize_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    """Lightweight stats so the agent can decide whether the result is chartable.

    Returns ``{row_count, columns, numeric_stats}`` where ``numeric_stats`` is
    a dict of ``column -> {null_count, zero_count, min, max}`` for numeric
    columns only.
    """
    summary: dict[str, Any] = {
        "row_count": int(len(df)),
        "columns": [str(c) for c in df.columns],
    }

    if df.empty:
        return summary

    numeric_stats: dict[str, dict[str, Any]] = {}
    for col in df.columns:
        series = pd.to_numeric(df[col], errors="coerce")
        non_null = series.notna()
        if not non_null.any():
            continue  # not a numeric column (or all nulls)
        numeric_stats[str(col)] = {
            "null_count": int((~non_null).sum()),
            "zero_count": int((series.fillna(0) == 0).sum()),
            "min": _safe_number(series.min()),
            "max": _safe_number(series.max()),
        }
    if numeric_stats:
        summary["numeric_stats"] = numeric_stats
    return summary


def _load_tenant_metadata(
    user_id: str,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, dict[str, Any]]],
    list[dict[str, Any]],
] | None:
    """Best-effort fetch of tenant metadata from Supabase.

    Returns ``(tables_by_name, columns_by_table, relationships)`` or ``None``
    when the tenant has no generated metadata yet (or the lookup fails).
    ``tables_by_name`` is keyed by ``table_name``; ``columns_by_table`` is a
    nested ``{table_name: {column_name: column_row}}``; relationships is a
    flat list of edge rows.
    """
    try:
        from fastapi_app.services import metadata_service  # lazy: avoid import at module load
        from fastapi_app.utils.supabase_client import get_supabase_admin_client
    except Exception:  # pragma: no cover - defensive
        logger = __import__("logging").getLogger(__name__)
        logger.exception("Could not import metadata service for enhanced SQL tools.")
        return None

    try:
        tables = [t.model_dump() for t in metadata_service.list_table_metadata(user_id=user_id)]
        if not tables:
            return None
        columns = [c.model_dump() for c in metadata_service.list_column_metadata(user_id=user_id)]
        rels = [r.model_dump() for r in metadata_service.list_relationships(user_id=user_id)]
    except Exception:
        __import__("logging").getLogger(__name__).exception(
            "Failed loading tenant metadata; falling back to basic SQL tools."
        )
        return None

    tables_by_name = {t["table_name"]: t for t in tables}
    columns_by_table: dict[str, dict[str, dict[str, Any]]] = {}
    for col in columns:
        columns_by_table.setdefault(col["table_name"], {})[col["column_name"]] = col
    # get_supabase_admin_client imported only to keep import graph explicit for tests.
    _ = get_supabase_admin_client  # noqa: F841
    return tables_by_name, columns_by_table, rels


def _format_sample_values(raw: Any, limit: int = 5) -> str:
    if not raw:
        return ""
    if not isinstance(raw, list):
        return str(raw)
    clipped = raw[:limit]
    return ", ".join(repr(v) for v in clipped)


def build_duckdb_tools(
    conn: duckdb.DuckDBPyConnection,
    llm: BaseLanguageModel,
    user_id: str | None = None,
) -> list:
    """Build the DuckDB agent toolset.

    When ``user_id`` is provided and the tenant has metadata in Supabase
    (``tenant_table_metadata`` and friends), ``list_tables`` / ``get_schema``
    are replaced by metadata-enriched versions and a new
    ``sql_db_list_relationships`` tool is exposed. ``sql_db_query_checker``
    and ``sql_db_query`` are unchanged in either mode.
    """
    metadata = _load_tenant_metadata(user_id) if user_id else None

    @tool("sql_db_list_tables")
    def list_tables(tool_input: str = "") -> str:
        """Input is an empty string, output is a list of tables available in the database.

        When tenant metadata is available, each table is annotated with its
        curated description, grain, and primary date column. Otherwise a
        plain comma-separated list of table names is returned.
        """
        try:
            rows = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
            names = [row[0] for row in rows]
        except Exception as e:
            return f"Error listing tables: {e}"

        if metadata is None:
            return ", ".join(names)

        tables_by_name, _cols, _rels = metadata
        lines: list[str] = []
        for name in names:
            meta = tables_by_name.get(name)
            if meta is None:
                lines.append(f"- {name}")
                continue
            bits: list[str] = []
            if meta.get("description"):
                bits.append(str(meta["description"]).strip())
            extras: list[str] = []
            if meta.get("grain"):
                extras.append(f"grain={meta['grain']}")
            if meta.get("primary_date_column"):
                extras.append(f"primary_date_column={meta['primary_date_column']}")
            if extras:
                bits.append(f"({'; '.join(extras)})")
            suffix = f" — {' '.join(bits)}" if bits else ""
            lines.append(f"- {name}{suffix}")
        return "\n".join(lines)

    @tool("sql_db_schema")
    def get_schema(table_names: str) -> str:
        """Input is a comma-separated list of tables, output is the schema (and sample rows) for those tables.

        When tenant metadata is available, each column line includes its
        semantic type and description, and a short ``Filterable columns``
        summary is appended. Otherwise the raw DuckDB DESCRIBE output is
        used.
        """
        tables = [t.strip() for t in table_names.split(",") if t.strip()]
        parts: list[str] = []
        for table in tables:
            try:
                cols = conn.execute(f"DESCRIBE {table}").fetchall()
                col_lines: list[str] = []

                table_meta: dict[str, Any] | None = None
                col_meta_map: dict[str, dict[str, Any]] = {}
                if metadata is not None:
                    tables_by_name, columns_by_table, _rels = metadata
                    table_meta = tables_by_name.get(table)
                    col_meta_map = columns_by_table.get(table, {})

                for col in cols:
                    col_name, col_type = col[0], col[1]
                    meta_col = col_meta_map.get(col_name)
                    if meta_col is None:
                        col_lines.append(f"  {col_name} {col_type}")
                        continue
                    annotations: list[str] = []
                    sem = meta_col.get("semantic_type")
                    if sem and sem != "unknown":
                        annotations.append(str(sem))
                    if meta_col.get("cardinality") is not None:
                        annotations.append(f"cardinality={meta_col['cardinality']}")
                    samples = _format_sample_values(meta_col.get("sample_values"))
                    if samples:
                        annotations.append(f"samples=[{samples}]")
                    desc = meta_col.get("description")
                    trailing = f" -- {'; '.join(annotations)}" if annotations else ""
                    if desc:
                        trailing = f"{trailing} | {desc}" if trailing else f" -- {desc}"
                    col_lines.append(f"  {col_name} {col_type}{trailing}")

                header = f"CREATE TABLE {table} (\n" + ",\n".join(col_lines) + "\n)"

                extras: list[str] = []
                if table_meta is not None:
                    if table_meta.get("description"):
                        extras.append(f"Description: {table_meta['description']}")
                    if table_meta.get("grain"):
                        extras.append(f"Grain: {table_meta['grain']}")
                    if table_meta.get("primary_date_column"):
                        extras.append(
                            f"Primary date column: {table_meta['primary_date_column']}"
                        )
                if col_meta_map:
                    filterable = [
                        n
                        for n, c in col_meta_map.items()
                        if c.get("is_filterable")
                        and c.get("semantic_type") in {"categorical", "numeric", "temporal", "measure"}
                    ]
                    if filterable:
                        extras.append("Filterable columns: " + ", ".join(sorted(filterable)))

                sample = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()
                section = header
                if extras:
                    section += "\n\n" + "\n".join(extras)
                section += (
                    f"\n\n/*\n3 rows from {table} table:\n"
                    f"{sample.to_string(index=False)}\n*/"
                )
                parts.append(section)
            except Exception as e:
                parts.append(f"Error getting schema for {table}: {e}")
        return "\n\n".join(parts)

    @tool("sql_db_list_relationships")
    def list_relationships(tool_input: str = "") -> str:
        """List known join relationships (foreign-key-like edges) between tables.

        Input is an empty string. Output is one edge per line in the form
        ``from_table.from_column -> to_table.to_column (kind, confidence)``.
        Use these edges to build joins instead of guessing keys.
        """
        if metadata is None:
            return "No relationship metadata available for this tenant (Yet to be generated)."
        _tables, _cols, rels = metadata
        if not rels:
            return "No relationships recorded for this tenant."
        lines: list[str] = []
        for r in rels:
            conf = r.get("confidence")
            conf_str = f", confidence={conf:.2f}" if isinstance(conf, (int, float)) else ""
            lines.append(
                f"{r['from_table']}.{r['from_column']} -> "
                f"{r['to_table']}.{r['to_column']} "
                f"({r['kind']}{conf_str})"
            )
        return "\n".join(lines)

    @tool("sql_db_query_checker")
    def sql_checker(query: str) -> str:
        """Use this tool to double check if your query is correct before executing it."""
        prompt_value = QUERY_CHECKER_PROMPT.format(dialect="DuckDB", query=query)
        response = llm.invoke(prompt_value)
        return response.content if hasattr(response, "content") else str(response)

    @tool("sql_db_query")
    def execute_sql(query: str) -> str:
        """Execute a read-only SQL query against DuckDB and return the result.

        Output is a JSON object with:
          - ``summary``: ``{row_count, columns, numeric_stats}`` — use ``row_count``
            and ``numeric_stats[col].null_count / zero_count`` to decide if the
            result is worth charting BEFORE calling create_react_chart / create_react_kpi.
            If row_count == 0 or every numeric column is all-null/all-zero,
            don't build a widget — fix the query first.
          - ``rows``: up to 200 records as a list of objects.
          - ``truncated``: true when more rows existed than were returned.
        """
        if not _is_readonly_sql(query):
            return json.dumps(
                {"error": "Only read-only SELECT/WITH queries are allowed."}
            )
        try:
            df = conn.execute(query).fetchdf()
            session_id = get_session_context()
            if session_id and not df.empty:
                store_query_snapshot(session_id, query, df)

            summary = _summarize_dataframe(df)
            row_cap = 200
            truncated = len(df) > row_cap
            rows = df.head(row_cap).to_dict(orient="records")
            payload: dict[str, Any] = {
                "summary": summary,
                "rows": rows,
                "truncated": truncated,
            }
            return json.dumps(payload, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    tools: list = [execute_sql, get_schema, list_tables, sql_checker]
    if metadata is not None:
        tools.append(list_relationships)
    return tools
