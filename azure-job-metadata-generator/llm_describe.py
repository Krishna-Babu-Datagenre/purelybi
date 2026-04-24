"""LLM-driven semantic typing and table descriptions.

Uses LangChain's ``with_structured_output()`` so the model is constrained to
return a Pydantic-validated payload — no JSON parsing or ``coerce`` step.

The prompt carries the connector source name and stream name so the LLM
has real-world context (e.g. "Shopify / orders") in addition to the raw
DuckDB view name. When a table has many columns we batch the describe call
so the model does not drop any.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Literal

from langchain_openai import AzureChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from db_inspect import ColumnSnapshot, TableSnapshot

logger = logging.getLogger(__name__)

# Number of rows from the snapshot to include in the LLM prompt. The
# tenant-side sampling is much larger (see ``db_inspect.SAMPLE_ROWS``);
# this only bounds what we send to the model.
SAMPLE_ROWS_FOR_LLM = int(os.environ.get("METADATA_LLM_SAMPLE_ROWS", "25"))

# When a table has more than this many columns the describe call is
# split into batches so the model does not drop columns.
COLUMN_BATCH_SIZE = int(os.environ.get("METADATA_LLM_COLUMN_BATCH", "20"))

SemanticType = Literal[
    "categorical",
    "numeric",
    "temporal",
    "identifier",
    "measure",
    "unknown",
]


# ---------------------------------------------------------------------------
# Structured-output schema
# ---------------------------------------------------------------------------


class _ColumnDescription(BaseModel):
    """LLM-typed description of one column."""

    name: str = Field(..., description="Column name exactly as provided in the input.")
    semantic_type: SemanticType = Field(
        ...,
        description=(
            "categorical=low-cardinality string filter; numeric=continuous; "
            "temporal=date/time; identifier=PK/FK; measure=numeric to be "
            "aggregated; unknown=cannot determine."
        ),
    )
    description: str | None = Field(
        default=None, description="One short sentence explaining the column."
    )


class _TableDescription(BaseModel):
    """LLM-typed description of one dataset."""

    description: str | None = Field(
        default=None,
        description="One or two sentences describing what one row represents.",
    )
    grain: str | None = Field(
        default=None,
        description="Short phrase describing row grain (e.g. 'one row per Shopify order').",
    )
    primary_date_column: str | None = Field(
        default=None,
        description=(
            "Name of the column to anchor time-based filters on. Must be one "
            "of the input columns whose semantic_type is 'temporal'. Null if "
            "no temporal column exists."
        ),
    )
    columns: list[_ColumnDescription] = Field(
        default_factory=list,
        description="One entry per input column. Do not invent or drop columns.",
    )


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = (
    "You are a senior data analyst describing a single dataset for a BI "
    "tool's filter UI. Be concise. Always include EVERY column listed in "
    "the input block — never drop or invent columns. Use the provided "
    "connector source name and stream name to ground your descriptions "
    "in real business terms (e.g. Shopify orders, Google Analytics "
    "sessions)."
)

_USER_TEMPLATE = """Connector source: {source}
Stream name: {stream}
DuckDB view name: {table}
{batch_note}
Columns in this batch (with DuckDB types, approx distinct count, and sampled values):
{columns_block}

Representative sample rows from the full table (JSON, ~{row_count} rows):
{rows_block}

Rules:
- semantic_type must be one of: categorical, numeric, temporal, identifier, measure, unknown.
- 'measure' = numeric column intended to be aggregated (revenue, spend, count).
- 'identifier' = primary/foreign key columns.
- 'temporal' = date/time/timestamp columns.
- Pick primary_date_column from columns whose semantic_type is 'temporal'; null if none.
- Include EVERY column listed above — do not drop or invent any.
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _llm() -> AzureChatOpenAI:
    return AzureChatOpenAI(
        azure_deployment=os.getenv("AZURE_LLM_NAME", "gpt-4.1"),
        api_key=os.getenv("AZURE_LLM_API_KEY"),
        azure_endpoint=os.getenv("AZURE_LLM_ENDPOINT"),
        api_version=os.getenv("AZURE_LLM_API_VERSION", "2024-12-01-preview"),
        temperature=0.25,
    )



def _columns_block(columns: list[ColumnSnapshot]) -> str:
    lines: list[str] = []
    for col in columns:
        sample = ", ".join(repr(v)[:60] for v in col.sample_values[:8])
        lines.append(
            f"- {col.name} ({col.data_type}) "
            f"[approx_distinct={col.cardinality}] "
            f"samples: [{sample}]"
        )
    return "\n".join(lines) if lines else "(no columns)"


def _rows_block(snapshot: TableSnapshot, max_rows: int = SAMPLE_ROWS_FOR_LLM) -> str:
    rows = snapshot.sample_rows[:max_rows]
    safe = [
        {k: (str(v)[:80] if v is not None else None) for k, v in r.items()}
        for r in rows
    ]
    return json.dumps(safe, default=str, indent=2)


def _heuristic_semantic_type(data_type: str, name: str) -> SemanticType:
    dt = (data_type or "").upper()
    n = (name or "").lower()
    if any(k in dt for k in ("DATE", "TIMESTAMP", "TIME")):
        return "temporal"
    if n == "id" or n.endswith("_id"):
        return "identifier"
    if any(k in dt for k in ("INT", "DOUBLE", "DECIMAL", "FLOAT", "NUMERIC", "BIGINT", "REAL")):
        if any(t in n for t in ("price", "amount", "spend", "revenue", "cost", "qty", "count", "total")):
            return "measure"
        return "numeric"
    return "unknown"


def _reconcile(
    snapshot: TableSnapshot,
    parsed: _TableDescription | None,
) -> dict[str, Any]:
    """Merge the structured-output result with the input column set.

    Drops invented columns, fills in missing ones via the heuristic, and
    nullifies ``primary_date_column`` if it doesn't exist in the snapshot.
    """
    cols_in = {c.name: c for c in snapshot.columns}
    cols_out: list[dict[str, Any]] = []
    seen: set[str] = set()

    if parsed is not None:
        for entry in parsed.columns:
            if entry.name not in cols_in or entry.name in seen:
                continue
            seen.add(entry.name)
            cols_out.append(
                {
                    "name": entry.name,
                    "semantic_type": entry.semantic_type,
                    "description": entry.description,
                }
            )

    for name, col in cols_in.items():
        if name in seen:
            continue
        cols_out.append(
            {
                "name": name,
                "semantic_type": _heuristic_semantic_type(col.data_type, name),
                "description": None,
            }
        )

    pdc = parsed.primary_date_column if parsed else None
    if pdc and pdc not in cols_in:
        pdc = None

    return {
        "description": (parsed.description if parsed else None),
        "grain": (parsed.grain if parsed else None),
        "primary_date_column": pdc,
        "columns": cols_out,
    }


# ---------------------------------------------------------------------------
# Public
# ---------------------------------------------------------------------------


def describe_table(snapshot: TableSnapshot) -> dict[str, Any]:
    """Run structured LLM describe call(s) for *snapshot*.

    For small tables (<= ``COLUMN_BATCH_SIZE`` columns) this is one LLM call.
    Larger tables are split into column batches so the model does not drop
    columns; the table-level fields (description, grain, primary_date_column)
    are merged across batches, preferring the first non-null value.
    """
    if not snapshot.columns:
        return {
            "description": None,
            "grain": None,
            "primary_date_column": None,
            "columns": [],
        }

    batch_size = max(1, COLUMN_BATCH_SIZE)
    total = len(snapshot.columns)
    batches = [
        snapshot.columns[i : i + batch_size]
        for i in range(0, total, batch_size)
    ]

    chain = _llm().with_structured_output(_TableDescription)

    merged = _TableDescription()
    seen_names: set[str] = set()
    row_sample_count = min(SAMPLE_ROWS_FOR_LLM, len(snapshot.sample_rows))
    stream = snapshot.stream_name or snapshot.name
    source = snapshot.source_name or "unknown"

    for idx, batch in enumerate(batches):
        batch_note = (
            f"(Column batch {idx + 1} of {len(batches)}; describe only the "
            f"columns listed below — other columns of this table are being "
            f"handled in separate batches.)"
            if len(batches) > 1
            else ""
        )
        prompt = _USER_TEMPLATE.format(
            source=source,
            stream=stream,
            table=snapshot.name,
            batch_note=batch_note,
            columns_block=_columns_block(batch),
            rows_block=_rows_block(snapshot),
            row_count=row_sample_count,
        )
        try:
            result = chain.invoke(
                [
                    SystemMessage(content=_SYSTEM_PROMPT),
                    HumanMessage(content=prompt),
                ]
            )
        except Exception:
            logger.exception(
                "LLM describe failed for table %s (batch %d/%d)",
                snapshot.name,
                idx + 1,
                len(batches),
            )
            continue
        if not isinstance(result, _TableDescription):
            continue

        # Merge table-level fields, preferring the first non-empty value.
        if not merged.description and result.description:
            merged.description = result.description
        if not merged.grain and result.grain:
            merged.grain = result.grain
        if not merged.primary_date_column and result.primary_date_column:
            merged.primary_date_column = result.primary_date_column

        for entry in result.columns:
            if entry.name in seen_names:
                continue
            seen_names.add(entry.name)
            merged.columns.append(entry)

    return _reconcile(snapshot, merged if seen_names else None)
