"""Discover relationship edges between tables via LLM proposal + DuckDB join probe.

Uses LangChain's ``with_structured_output()`` so the proposal step returns a
Pydantic-validated list of edges — no JSON parsing.

Process:
1. Build a compact catalog of every column flagged as ``identifier`` (or named
   like one) across all tables.
2. Ask the LLM for plausible foreign-key edges (structured output).
3. Validate each proposed edge with a cheap DuckDB join probe.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Literal

import duckdb
from langchain_openai import AzureChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from db_inspect import TableSnapshot

logger = logging.getLogger(__name__)

RelationshipKind = Literal["many_to_one", "one_to_one", "many_to_many"]

_MIN_OVERLAP_RATIO = float(os.environ.get("METADATA_RELATIONSHIP_MIN_OVERLAP", "0.5"))
_MAX_EDGES = int(os.environ.get("METADATA_RELATIONSHIP_MAX_EDGES", "40"))


# ---------------------------------------------------------------------------
# Structured-output schema
# ---------------------------------------------------------------------------


class _ProposedEdge(BaseModel):
    from_table: str = Field(..., description="Table containing the foreign key column.")
    from_column: str = Field(..., description="Foreign key column name.")
    to_table: str = Field(..., description="Referenced table.")
    to_column: str = Field(..., description="Referenced column (usually the PK).")
    kind: RelationshipKind = Field(
        ...,
        description=(
            "many_to_one: typical FK (orders.customer_id -> customers.id). "
            "one_to_one: 1:1 join. many_to_many: bridge tables."
        ),
    )
    confidence: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="0..1 — how confident the model is in this edge.",
    )


class _RelationshipProposal(BaseModel):
    edges: list[_ProposedEdge] = Field(
        default_factory=list,
        description="Proposed foreign-key edges between the given tables.",
    )


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = (
    "You are a database modeller proposing foreign-key relationships between "
    "tables. Be conservative — only propose edges where column names and "
    "samples strongly suggest a real foreign key."
)

_USER_TEMPLATE = """Tables (one per line, with identifier-like columns and small samples):
{catalog_block}

Rules:
- Only propose edges between columns whose values likely overlap.
- Prefer matching identifier-like names (e.g. customer_id -> customers.id).
- Use kind 'many_to_one' for the common FK case.
- Do not invent column names; use only those present in the catalog above.
- Hard cap: at most {max_edges} edges total.
"""


def _llm() -> AzureChatOpenAI:
    return AzureChatOpenAI(
        azure_deployment=os.getenv("AZURE_LLM_NAME", "gpt-4.1"),
        api_key=os.getenv("AZURE_LLM_API_KEY"),
        azure_endpoint=os.getenv("AZURE_LLM_ENDPOINT"),
        api_version=os.getenv("AZURE_LLM_API_VERSION", "2024-12-01-preview"),
        temperature=0,
    )


# ---------------------------------------------------------------------------
# Catalog building (identifier-like columns only; keeps prompt size bounded)
# ---------------------------------------------------------------------------


_IDENT_NAME_RE = re.compile(r"(^|_)id($|_)|_key$|_uuid$", re.IGNORECASE)


def _is_identifier_like(col_name: str, data_type: str) -> bool:
    if _IDENT_NAME_RE.search(col_name or ""):
        return True
    dt = (data_type or "").upper()
    return any(k in dt for k in ("UUID", "CHAR", "VARCHAR", "TEXT", "BIGINT", "INT"))


def build_catalog(snapshots: list[TableSnapshot]) -> dict[str, list[dict[str, Any]]]:
    """``{table_name: [{name, data_type, samples}]}`` for ID-like columns only."""
    catalog: dict[str, list[dict[str, Any]]] = {}
    for snap in snapshots:
        cols: list[dict[str, Any]] = []
        for col in snap.columns:
            if not _is_identifier_like(col.name, col.data_type):
                continue
            cols.append(
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "samples": [
                        str(v)[:40] for v in (col.sample_values or [])[:5]
                    ],
                }
            )
        if cols:
            catalog[snap.name] = cols
    return catalog


def _catalog_block(catalog: dict[str, list[dict[str, Any]]]) -> str:
    lines: list[str] = []
    for table, cols in catalog.items():
        lines.append(f"{table}:")
        for col in cols:
            samples = ", ".join(col["samples"])
            lines.append(f"  - {col['name']} ({col['data_type']}) [{samples}]")
    return "\n".join(lines) or "(no identifier-like columns found)"


# ---------------------------------------------------------------------------
# LLM proposal (structured output)
# ---------------------------------------------------------------------------


def propose_edges(snapshots: list[TableSnapshot]) -> list[dict[str, Any]]:
    """Return raw (un-validated) edges as proposed by the LLM.

    The proposals are still filtered against the input catalog so the model
    cannot reference invented tables or columns even if the structured-
    output schema is satisfied.
    """
    catalog = build_catalog(snapshots)
    if not catalog:
        return []

    prompt = _USER_TEMPLATE.format(
        catalog_block=_catalog_block(catalog),
        max_edges=_MAX_EDGES,
    )

    parsed: _RelationshipProposal | None = None
    try:
        chain = _llm().with_structured_output(_RelationshipProposal)
        result = chain.invoke(
            [
                SystemMessage(content=_SYSTEM_PROMPT),
                HumanMessage(content=prompt),
            ]
        )
        parsed = result if isinstance(result, _RelationshipProposal) else None
    except Exception:
        logger.exception("LLM relationships call failed")
        return []

    if parsed is None:
        return []

    table_cols = {t: {c["name"] for c in cols} for t, cols in catalog.items()}
    out: list[dict[str, Any]] = []
    for edge in parsed.edges[:_MAX_EDGES]:
        if edge.from_table not in table_cols or edge.to_table not in table_cols:
            continue
        if (
            edge.from_column not in table_cols[edge.from_table]
            or edge.to_column not in table_cols[edge.to_table]
        ):
            continue
        if edge.from_table == edge.to_table and edge.from_column == edge.to_column:
            continue
        out.append(
            {
                "from_table": edge.from_table,
                "from_column": edge.from_column,
                "to_table": edge.to_table,
                "to_column": edge.to_column,
                "kind": edge.kind,
                "confidence": float(edge.confidence),
            }
        )
    return out


# ---------------------------------------------------------------------------
# DuckDB join-probe validation
# ---------------------------------------------------------------------------


_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe(name: str) -> bool:
    return bool(_SAFE_IDENT_RE.match(name))


def _probe_overlap(
    conn: duckdb.DuckDBPyConnection,
    edge: dict[str, Any],
) -> tuple[int, int]:
    """Return ``(left_distinct, joined_distinct)`` for the FK direction."""
    ft, fc = edge["from_table"], edge["from_column"]
    tt, tc = edge["to_table"], edge["to_column"]
    if not all(_safe(x) for x in (ft, fc, tt, tc)):
        return (0, 0)

    left = conn.execute(
        f"SELECT COUNT(DISTINCT {fc}) FROM {ft} WHERE {fc} IS NOT NULL"
    ).fetchone()
    joined = conn.execute(
        f"SELECT COUNT(DISTINCT a.{fc}) FROM {ft} a "
        f"JOIN {tt} b ON a.{fc} = b.{tc} WHERE a.{fc} IS NOT NULL"
    ).fetchone()
    return (
        int(left[0] or 0) if left else 0,
        int(joined[0] or 0) if joined else 0,
    )


def validate_edges(
    conn: duckdb.DuckDBPyConnection,
    edges: list[dict[str, Any]],
    min_overlap: float = _MIN_OVERLAP_RATIO,
) -> list[dict[str, Any]]:
    """Drop edges where the join overlap ratio is below *min_overlap*.

    Confidence is rescaled by the observed overlap ratio so downstream
    consumers can prioritise stronger edges.
    """
    accepted: list[dict[str, Any]] = []
    for edge in edges:
        try:
            left, joined = _probe_overlap(conn, edge)
        except Exception:
            logger.warning(
                "Probe failed for %s.%s -> %s.%s",
                edge["from_table"],
                edge["from_column"],
                edge["to_table"],
                edge["to_column"],
            )
            continue
        if left == 0:
            continue
        ratio = joined / left
        if ratio < min_overlap:
            logger.info(
                "Rejected edge %s.%s -> %s.%s (overlap=%.2f)",
                edge["from_table"],
                edge["from_column"],
                edge["to_table"],
                edge["to_column"],
                ratio,
            )
            continue
        edge = dict(edge)
        edge["confidence"] = round(min(1.0, edge["confidence"] * ratio + 0.001), 3)
        accepted.append(edge)
    return accepted


def discover_relationships(
    conn: duckdb.DuckDBPyConnection,
    snapshots: list[TableSnapshot],
) -> list[dict[str, Any]]:
    """LLM-propose edges (structured output) then DuckDB-validate each one."""
    proposed = propose_edges(snapshots)
    if not proposed:
        return []
    return validate_edges(conn, proposed)
