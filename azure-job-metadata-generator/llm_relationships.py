"""Hybrid heuristic + LLM relationship discovery.

The pipeline is two phases:

**Phase 1 — Heuristic engine (deterministic, ~80% of work):**

1. *Blocking*: group columns by base data type so we never compare a TEXT
   to a BIGINT. Drop low-cardinality columns (booleans, status flags) which
   are never useful as join keys.
2. *PK inference*: a column is a PK candidate when its approximate distinct
   count is within tolerance of the table's row count.
3. *FK inference*: for each non-PK candidate column, score every PK
   candidate from another table by combining:
   - **name similarity** — exact / pattern (`<entity>_id` ↔ `<entity>.id`,
     same-name across tables) plus Jaro-Winkler fuzzy match for slight
     deviations (``cust_id`` ↔ ``customer_id``);
   - **data overlap** — cheap DuckDB subset-inclusion probe.
4. *Routing*:
   - score ≥ 0.80 → auto-approved edge.
   - 0.40 ≤ score < 0.80 → "near-miss" forwarded to the LLM.
   - tables with no auto-approved edges → "orphan", forwarded to the LLM.

**Phase 2 — LLM engine (semantic edge cases):**

Per orphan / near-miss target table we send ONE structured-output request
containing only that target plus a curated catalog of plausible parent
tables (filtered by data-type compatibility, PK-ness, and audit/measure
pruning). Every LLM-proposed edge is then routed back through the same
DuckDB subset-inclusion probe at a stricter threshold (default 0.9) to
discard hallucinations.
"""

from __future__ import annotations

import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Literal

import duckdb
from langchain_openai import AzureChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from db_inspect import ColumnSnapshot, TableSnapshot

logger = logging.getLogger(__name__)

RelationshipKind = Literal["many_to_one", "one_to_one", "many_to_many"]

# ---------------------------------------------------------------------------
# Tunables (env-overridable)
# ---------------------------------------------------------------------------

# Final accept threshold for the heuristic data-overlap probe applied to
# *every* edge (heuristic and LLM-proposed alike).
_MIN_OVERLAP_RATIO = float(os.environ.get("METADATA_RELATIONSHIP_MIN_OVERLAP", "0.5"))
# Stricter overlap demanded of LLM-proposed edges (hallucination filter).
_LLM_VALIDATION_OVERLAP = float(
    os.environ.get("METADATA_RELATIONSHIP_LLM_OVERLAP", "0.80")
)
# Final cap on the number of edges returned overall.
_MAX_EDGES = int(os.environ.get("METADATA_RELATIONSHIP_MAX_EDGES", "40"))
# How many parent candidates to put in a single LLM "catalog" prompt.
_LLM_CATALOG_PARENTS = int(
    os.environ.get("METADATA_RELATIONSHIP_LLM_CATALOG_PARENTS", "15")
)
# Auto-approve / near-miss / orphan score thresholds.
_AUTO_APPROVE_SCORE = float(os.environ.get("METADATA_RELATIONSHIP_AUTO_SCORE", "0.80"))
_NEAR_MISS_SCORE = float(os.environ.get("METADATA_RELATIONSHIP_NEAR_SCORE", "0.40"))
# Tolerance for treating distinct-count == row-count (PK candidate).
_PK_DISTINCT_TOLERANCE = float(
    os.environ.get("METADATA_RELATIONSHIP_PK_TOLERANCE", "0.90")
)
# Skip columns whose approx distinct count is below this (booleans, flags).
_MIN_FK_CARDINALITY = int(os.environ.get("METADATA_RELATIONSHIP_MIN_FK_CARD", "5"))
# Jaro-Winkler similarity threshold for fuzzy name matching.
_FUZZY_NAME_THRESHOLD = float(
    os.environ.get("METADATA_RELATIONSHIP_FUZZY_THRESHOLD", "0.80")
)


# ---------------------------------------------------------------------------
# Structured-output schema (Phase 2)
# ---------------------------------------------------------------------------


class _ProposedEdge(BaseModel):
    target_column: str = Field(..., description="FK column on the target table.")
    parent_table: str = Field(..., description="Referenced parent table.")
    parent_column: str = Field(..., description="Referenced PK column.")
    kind: RelationshipKind = Field(default="many_to_one")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    reasoning: str | None = Field(default=None)


class _RelationshipProposal(BaseModel):
    edges: list[_ProposedEdge] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Regex helpers / type blocking
# ---------------------------------------------------------------------------

_IDENT_NAME_RE = re.compile(r"(^|_)id($|_)|_key$|_uuid$|_fk$", re.IGNORECASE)
_FK_SUFFIX_RE = re.compile(r"^(.+?)(_id|_fk|_uuid|_key)$", re.IGNORECASE)
_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Audit / lifecycle columns and obvious metric columns we never want to
# consider as join keys (and never want to send to the LLM).
_AUDIT_RE = re.compile(
    r"^(created|updated|modified|deleted|inserted)(_at|_on|_by|_date)?$|"
    r"^(is_|has_)|"
    r"^(_airbyte|_dbt|_loaded|_synced)",
    re.IGNORECASE,
)
_MEASURE_RE = re.compile(
    r"(amount|price|cost|spend|revenue|qty|quantity|total|count|sum|avg|"
    r"score|rate|ratio|percent|discount|tax|fee)$",
    re.IGNORECASE,
)


def _safe(name: str) -> bool:
    return bool(_SAFE_IDENT_RE.match(name))


def _base_type(data_type: str) -> str:
    """Coarse data-type bucket used for blocking."""
    dt = (data_type or "").upper()
    if any(k in dt for k in ("DATE", "TIMESTAMP", "TIME")):
        return "temporal"
    if "BOOL" in dt:
        return "boolean"
    if "UUID" in dt:
        return "uuid"
    if any(k in dt for k in ("INT", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT")):
        return "integer"
    if any(k in dt for k in ("DOUBLE", "FLOAT", "DECIMAL", "NUMERIC", "REAL")):
        return "decimal"
    if any(k in dt for k in ("CHAR", "VARCHAR", "TEXT", "STRING")):
        return "string"
    return "other"


# Buckets that may legitimately join across each other.
_JOIN_COMPATIBLE: dict[str, set[str]] = {
    "uuid": {"uuid", "string"},
    "string": {"string", "uuid"},
    "integer": {"integer", "decimal"},
    "decimal": {"integer", "decimal"},
    "temporal": {"temporal"},
    "boolean": set(),
    "other": {"other", "string"},
}


def _types_compatible(a: str, b: str) -> bool:
    return _base_type(b) in _JOIN_COMPATIBLE.get(_base_type(a), set())


def _is_audit_or_measure(col_name: str) -> bool:
    return bool(_AUDIT_RE.search(col_name) or _MEASURE_RE.search(col_name))


def _is_identifier_like(
    col_name: str, data_type: str, semantic_type: str | None = None
) -> bool:
    # LLM-assigned semantic type takes priority.
    if semantic_type and semantic_type.lower() == "identifier":
        return True
    if _IDENT_NAME_RE.search(col_name or ""):
        return True
    bt = _base_type(data_type)
    return bt in ("uuid", "integer", "string")


# ---------------------------------------------------------------------------
# Jaro-Winkler (stdlib only; small enough to ship inline)
# ---------------------------------------------------------------------------


def _jaro(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    match_distance = max(len(s1), len(s2)) // 2 - 1
    if match_distance < 0:
        match_distance = 0
    s1_matches = [False] * len(s1)
    s2_matches = [False] * len(s2)
    matches = 0
    for i, ch in enumerate(s1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len(s2))
        for j in range(start, end):
            if s2_matches[j] or s2[j] != ch:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    transpositions = 0
    k = 0
    for i in range(len(s1)):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    transpositions //= 2
    return (
        matches / len(s1)
        + matches / len(s2)
        + (matches - transpositions) / matches
    ) / 3.0


def _jaro_winkler(s1: str, s2: str, prefix_scale: float = 0.1) -> float:
    s1 = (s1 or "").lower()
    s2 = (s2 or "").lower()
    j = _jaro(s1, s2)
    prefix = 0
    for a, b in zip(s1, s2):
        if a == b and prefix < 4:
            prefix += 1
        else:
            break
    return j + prefix * prefix_scale * (1 - j)


# ---------------------------------------------------------------------------
# Phase 1 — Heuristic engine
# ---------------------------------------------------------------------------


@dataclass
class _ScoredEdge:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    name_score: float
    overlap: float
    score: float

    def to_payload(self) -> dict[str, Any]:
        return {
            "from_table": self.from_table,
            "from_column": self.from_column,
            "to_table": self.to_table,
            "to_column": self.to_column,
            "kind": "many_to_one",
            "confidence": round(self.score, 3),
        }


def _table_row_count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    if not _safe(table):
        return 0
    try:
        row = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table})"
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def _pk_candidates(
    conn: duckdb.DuckDBPyConnection, snapshots: list[TableSnapshot]
) -> dict[str, set[str]]:
    """Per table, columns whose approx-distinct ≈ row count.

    Uses the cached ``approx_count_distinct`` already on each
    ``ColumnSnapshot`` plus a single ``COUNT(*)`` per table.
    """
    out: dict[str, set[str]] = {}
    for snap in snapshots:
        rowcount = _table_row_count(conn, snap.name)
        if rowcount <= 0:
            out[snap.name] = set()
            continue
        pks: set[str] = set()
        for col in snap.columns:
            if col.cardinality is None:
                continue
            if _base_type(col.data_type) == "boolean":
                continue
            # approx_count_distinct may slightly under/overshoot; allow a
            # small tolerance.
            if col.cardinality >= rowcount * _PK_DISTINCT_TOLERANCE:
                pks.add(col.name)
        out[snap.name] = pks
    return out


def _name_similarity(
    fk_col: str,
    fk_table: str,
    pk_col: str,
    pk_table: str,
) -> float:
    """0..1 score for how strongly the names suggest a join.

    The score is a max of several patterns:
    - exact name match across tables (e.g. ``order_id`` in two tables)
    - ``<entity>_id`` ↔ ``<entity>{s,es}.id`` pattern
    - Jaro-Winkler fuzzy match on stripped identifiers (``cust_id`` ↔
      ``customer_id``)
    """
    fk_l = fk_col.lower()
    pk_l = pk_col.lower()
    pk_t = pk_table.lower()

    # 1. Exact same column name in both tables.
    if fk_l == pk_l:
        return 0.85

    # 2. <entity>_id (or _fk/_uuid/_key) → <entity>(s|es).id pattern.
    # pk_t may be connector-prefixed (e.g. "source_facebook_marketing_ads")
    # so we check both exact equality and endswith to handle those cases.
    m = _FK_SUFFIX_RE.match(fk_col)
    if m:
        entity = m.group(1).lower()
        for plural in (entity, entity + "s", entity + "es"):
            if (pk_t == plural or pk_t.endswith("_" + plural)) and pk_l in ("id", entity + "_id", entity + "_sk"):
                return 0.95

    # 3. Strip the trailing _id/_fk/_uuid/_key suffix from both sides and
    #    fuzzy-match the stems plus the parent table name.
    def _stem(s: str) -> str:
        return _FK_SUFFIX_RE.sub(lambda mm: mm.group(1), s).lower()

    fk_stem = _stem(fk_col)
    pk_stem = _stem(pk_col)
    fuzzy = max(
        _jaro_winkler(fk_stem, pk_stem),
        _jaro_winkler(fk_stem, pk_t),
    )
    if fuzzy >= _FUZZY_NAME_THRESHOLD:
        # Map [threshold..1.0] → [0.5..0.8]; below threshold contributes 0.
        return 0.5 + (fuzzy - _FUZZY_NAME_THRESHOLD) * (
            0.3 / max(1e-6, 1.0 - _FUZZY_NAME_THRESHOLD)
        )
    return 0.0


def _probe_overlap(
    conn: duckdb.DuckDBPyConnection,
    fk_table: str,
    fk_col: str,
    pk_table: str,
    pk_col: str,
) -> float:
    """Return ``joined_distinct / left_distinct`` (0 on failure)."""
    if not all(_safe(x) for x in (fk_table, fk_col, pk_table, pk_col)):
        return 0.0
    try:
        left = conn.execute(
            f"SELECT COUNT(DISTINCT {fk_col}) FROM {fk_table} "
            f"WHERE {fk_col} IS NOT NULL"
        ).fetchone()
        joined = conn.execute(
            f"SELECT COUNT(DISTINCT a.{fk_col}) FROM {fk_table} a "
            f"JOIN {pk_table} b ON a.{fk_col} = b.{pk_col} "
            f"WHERE a.{fk_col} IS NOT NULL"
        ).fetchone()
    except Exception:
        return 0.0
    left_n = int(left[0] or 0) if left else 0
    joined_n = int(joined[0] or 0) if joined else 0
    if left_n == 0:
        return 0.0
    return joined_n / left_n


def _heuristic_phase(
    conn: duckdb.DuckDBPyConnection,
    snapshots: list[TableSnapshot],
    pks: dict[str, set[str]],
) -> tuple[list[_ScoredEdge], list[_ScoredEdge], set[str]]:
    """Return ``(auto_approved, near_misses, orphan_tables)``.

    Iterates every (FK candidate × PK candidate) pair across compatible
    base types, computes a name+overlap composite score, then buckets the
    result. Probes are bounded by aggressive name-based pre-filtering.
    """
    table_lookup = {s.name: s for s in snapshots}
    pk_columns: list[tuple[str, ColumnSnapshot]] = [
        (t, c)
        for t, snap in ((t, table_lookup[t]) for t in pks)
        for c in snap.columns
        if c.name in pks[t]
    ]

    auto: list[_ScoredEdge] = []
    near: list[_ScoredEdge] = []
    seen: set[tuple[str, str, str, str]] = set()

    for snap in snapshots:
        for col in snap.columns:
            if _is_audit_or_measure(col.name):
                continue
            if col.cardinality is not None and col.cardinality < _MIN_FK_CARDINALITY:
                continue
            if col.name in pks.get(snap.name, set()):
                # PK of its own table — never an FK from this side.
                continue
            if not _is_identifier_like(col.name, col.data_type, col.semantic_type):
                continue

            for pk_table, pk_col in pk_columns:
                if pk_table == snap.name:
                    continue
                if not _types_compatible(col.data_type, pk_col.data_type):
                    continue
                key = (snap.name, col.name, pk_table, pk_col.name)
                if key in seen:
                    continue

                name_score = _name_similarity(
                    col.name, snap.name, pk_col.name, pk_table
                )
                if name_score == 0.0:
                    # Pure-data-overlap matches without any name signal are
                    # too noisy — skip rather than firing a probe per pair.
                    continue

                overlap = _probe_overlap(
                    conn, snap.name, col.name, pk_table, pk_col.name
                )
                # Composite: name carries semantic intent, overlap proves
                # the data backs it up. Both must be non-trivial.
                composite = round(0.4 * name_score + 0.6 * overlap, 3)
                seen.add(key)

                edge = _ScoredEdge(
                    from_table=snap.name,
                    from_column=col.name,
                    to_table=pk_table,
                    to_column=pk_col.name,
                    name_score=round(name_score, 3),
                    overlap=round(overlap, 3),
                    score=composite,
                )
                if composite >= _AUTO_APPROVE_SCORE and overlap >= _MIN_OVERLAP_RATIO:
                    auto.append(edge)
                elif composite >= _NEAR_MISS_SCORE:
                    near.append(edge)

    # ── Second pass: same-name PK columns across tables ───────────
    # When two tables each have a PK column with the same name (e.g. both
    # have ``id``), the main loop above skips them because it excludes
    # PKs of the FK-side table. Here we check purely by data overlap;
    # if the values in the smaller table are a near-perfect subset of
    # the larger, that's a valid FK direction.
    pk_col_index: defaultdict[str, list[tuple[str, ColumnSnapshot]]] = defaultdict(list)
    for tbl, col in pk_columns:
        pk_col_index[col.name].append((tbl, col))

    for col_name, entries in pk_col_index.items():
        if len(entries) < 2:
            continue
        for i, (t1, c1) in enumerate(entries):
            for t2, c2 in entries[i + 1 :]:
                if not _types_compatible(c1.data_type, c2.data_type):
                    continue
                # Try both directions; the subset direction is the FK side.
                for fk_t, fk_c, pk_t, pk_c in [
                    (t1, c1, t2, c2),
                    (t2, c2, t1, c1),
                ]:
                    key = (fk_t, fk_c.name, pk_t, pk_c.name)
                    if key in seen:
                        continue
                    overlap = _probe_overlap(conn, fk_t, fk_c.name, pk_t, pk_c.name)
                    if overlap < _MIN_OVERLAP_RATIO:
                        continue
                    # Name score is moderate (same name across tables).
                    composite = round(0.4 * 0.85 + 0.6 * overlap, 3)
                    seen.add(key)
                    edge = _ScoredEdge(
                        from_table=fk_t,
                        from_column=fk_c.name,
                        to_table=pk_t,
                        to_column=pk_c.name,
                        name_score=0.85,
                        overlap=round(overlap, 3),
                        score=composite,
                    )
                    if composite >= _AUTO_APPROVE_SCORE and overlap >= _MIN_OVERLAP_RATIO:
                        auto.append(edge)
                    elif composite >= _NEAR_MISS_SCORE:
                        near.append(edge)

    tables_with_edges = {e.from_table for e in auto} | {e.to_table for e in auto}
    orphan = {s.name for s in snapshots} - tables_with_edges
    return auto, near, orphan


# ---------------------------------------------------------------------------
# Phase 2 — LLM catalog prompts
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = (
    "You are an expert database architect proposing foreign-key relationships. "
    "Be conservative — only propose edges where column names AND samples "
    "strongly suggest a real foreign key. Output strictly the JSON schema "
    "you are given; do not invent table or column names."
)

_USER_TEMPLATE = """Connector source: {source}
Target table: `{target_table}`
Target columns (FK candidates):
{target_block}

Potential parent catalog (each entry is a table with PK candidate columns):
{catalog_block}

Identify likely many_to_one foreign-key edges from columns of the target
table to PK columns in the catalog. For each edge return target_column,
parent_table, parent_column, confidence (0..1) and a one-line reasoning.
Hard cap: at most {max_edges} edges across this prompt.
"""


def _llm() -> AzureChatOpenAI:
    return AzureChatOpenAI(
        azure_deployment=os.getenv("AZURE_LLM_NAME"),
        api_key=os.getenv("AZURE_LLM_API_KEY"),
        azure_endpoint=os.getenv("AZURE_LLM_ENDPOINT"),
        api_version=os.getenv("AZURE_LLM_API_VERSION"),
        temperature=0.1,
    )


def _column_descriptor(
    col: ColumnSnapshot, *, is_pk: bool = False
) -> str:
    samples = ", ".join(repr(v)[:40] for v in (col.sample_values or [])[:5])
    role = "PK" if is_pk else (
        "Unique" if col.cardinality and col.cardinality > 1000 else "Duplicates"
    )
    card = f", approx_distinct={col.cardinality}" if col.cardinality else ""
    return f"  - `{col.name}` ({col.data_type}, {role}{card}) samples: [{samples}]"


def _target_block(snap: TableSnapshot, candidate_cols: set[str]) -> str:
    lines = [
        _column_descriptor(c)
        for c in snap.columns
        if c.name in candidate_cols and not _is_audit_or_measure(c.name)
    ]
    return "\n".join(lines) or "  (no candidate columns)"


def _catalog_entry(snap: TableSnapshot, pk_cols: set[str]) -> str:
    pk_lines = [
        _column_descriptor(c, is_pk=True)
        for c in snap.columns
        if c.name in pk_cols
    ]
    if not pk_lines:
        return ""
    source = snap.source_name or "unknown"
    return f"`{source}` | `{snap.name}`\n" + "\n".join(pk_lines)


def _select_parent_catalog(
    target: TableSnapshot,
    target_fk_cols: set[str],
    snapshots: list[TableSnapshot],
    pks: dict[str, set[str]],
    near_misses: list[_ScoredEdge],
) -> list[TableSnapshot]:
    """Pick up to ``_LLM_CATALOG_PARENTS`` candidate parents for *target*.

    Prioritises (1) tables already implicated by near-miss heuristic edges,
    (2) tables whose PK base-types overlap any target FK candidate column.
    """
    target_types = {
        _base_type(c.data_type)
        for c in target.columns
        if c.name in target_fk_cols
    }
    score_by_table: defaultdict[str, float] = defaultdict(float)
    for edge in near_misses:
        if edge.from_table == target.name:
            score_by_table[edge.to_table] += edge.score + 1.0  # priority bump

    candidates: list[tuple[float, TableSnapshot]] = []
    for snap in snapshots:
        if snap.name == target.name:
            continue
        snap_pks = pks.get(snap.name, set())
        if not snap_pks:
            continue
        # Type compatibility filter.
        pk_types = {
            _base_type(c.data_type) for c in snap.columns if c.name in snap_pks
        }
        if not any(_types_compatible(t, p) for t in target_types for p in pk_types):
            continue
        candidates.append((score_by_table[snap.name], snap))

    candidates.sort(key=lambda x: x[0], reverse=True)
    return [c[1] for c in candidates[:_LLM_CATALOG_PARENTS]]


def _llm_phase(
    snapshots: list[TableSnapshot],
    pks: dict[str, set[str]],
    near_misses: list[_ScoredEdge],
    orphan_tables: set[str],
) -> list[dict[str, Any]]:
    """Run one structured-output LLM call per target table."""
    by_name = {s.name: s for s in snapshots}

    # Targets: orphan tables + any "from_table" appearing in near-misses.
    target_to_fk_cols: dict[str, set[str]] = defaultdict(set)
    for edge in near_misses:
        target_to_fk_cols[edge.from_table].add(edge.from_column)
    for orphan in orphan_tables:
        snap = by_name.get(orphan)
        if not snap:
            continue
        for col in snap.columns:
            if _is_audit_or_measure(col.name):
                continue
            if col.cardinality is not None and col.cardinality < _MIN_FK_CARDINALITY:
                continue
            if col.name in pks.get(orphan, set()):
                continue
            if _is_identifier_like(col.name, col.data_type, col.semantic_type):
                target_to_fk_cols[orphan].add(col.name)

    if not target_to_fk_cols:
        return []

    chain = _llm().with_structured_output(_RelationshipProposal)
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()

    for target_name, fk_cols in target_to_fk_cols.items():
        target = by_name.get(target_name)
        if target is None or not fk_cols:
            continue

        parents = _select_parent_catalog(
            target, fk_cols, snapshots, pks, near_misses
        )
        if not parents:
            continue

        catalog_lines = []
        for i, parent in enumerate(parents, 1):
            entry = _catalog_entry(parent, pks.get(parent.name, set()))
            if entry:
                catalog_lines.append(f"{i}. {entry}")
        if not catalog_lines:
            continue

        prompt = _USER_TEMPLATE.format(
            source=target.source_name or "unknown",
            target_table=target.name,
            target_block=_target_block(target, fk_cols),
            catalog_block="\n".join(catalog_lines),
            max_edges=_MAX_EDGES,
        )
        try:
            result = chain.invoke(
                [
                    SystemMessage(content=_SYSTEM_PROMPT),
                    HumanMessage(content=prompt),
                ]
            )
        except Exception:
            logger.exception("LLM relationship call failed for %s", target_name)
            continue
        if not isinstance(result, _RelationshipProposal):
            continue

        valid_parent_cols = {
            p.name: {c.name for c in p.columns} for p in parents
        }
        target_cols = {c.name for c in target.columns}
        for edge in result.edges:
            if edge.target_column not in target_cols:
                continue
            if edge.parent_table not in valid_parent_cols:
                continue
            if edge.parent_column not in valid_parent_cols[edge.parent_table]:
                continue
            key = (target.name, edge.target_column, edge.parent_table, edge.parent_column)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "from_table": target.name,
                    "from_column": edge.target_column,
                    "to_table": edge.parent_table,
                    "to_column": edge.parent_column,
                    "kind": edge.kind,
                    "confidence": float(edge.confidence),
                    "_source": "llm",
                }
            )
    return out


# ---------------------------------------------------------------------------
# Validation loop
# ---------------------------------------------------------------------------


def _validate_with_overlap(
    conn: duckdb.DuckDBPyConnection,
    edges: list[dict[str, Any]],
    *,
    min_overlap: float,
) -> list[dict[str, Any]]:
    """Run the DuckDB join probe; keep edges where overlap ≥ *min_overlap*."""
    accepted: list[dict[str, Any]] = []
    for edge in edges:
        try:
            overlap = _probe_overlap(
                conn,
                edge["from_table"],
                edge["from_column"],
                edge["to_table"],
                edge["to_column"],
            )
        except Exception:
            logger.warning(
                "Probe failed for %s.%s -> %s.%s",
                edge["from_table"],
                edge["from_column"],
                edge["to_table"],
                edge["to_column"],
            )
            continue
        if overlap < min_overlap:
            logger.info(
                "Rejected edge %s.%s -> %s.%s (overlap=%.2f, required=%.2f)",
                edge["from_table"],
                edge["from_column"],
                edge["to_table"],
                edge["to_column"],
                overlap,
                min_overlap,
            )
            continue
        out = dict(edge)
        out["confidence"] = round(
            min(1.0, float(edge.get("confidence", 0.5)) * (0.5 + overlap / 2)), 3
        )
        out.pop("_source", None)
        accepted.append(out)
    return accepted


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def discover_relationships(
    conn: duckdb.DuckDBPyConnection,
    snapshots: list[TableSnapshot],
) -> list[dict[str, Any]]:
    """Hybrid heuristic + LLM relationship discovery.

    Returns a deduplicated, overlap-validated list of edge payloads ready
    to be persisted by the upsert layer.
    """
    if not snapshots:
        return []

    pks = _pk_candidates(conn, snapshots)
    auto, near, orphan = _heuristic_phase(conn, snapshots, pks)

    logger.info(
        "Heuristic phase: auto=%d near_miss=%d orphan_tables=%d",
        len(auto),
        len(near),
        len(orphan),
    )

    # Auto-approved edges already have a valid overlap by construction, but
    # we re-run the strict probe to keep all returned edges on the same
    # confidence scale.
    auto_payloads = _validate_with_overlap(
        conn, [e.to_payload() for e in auto], min_overlap=_MIN_OVERLAP_RATIO
    )

    llm_proposed = _llm_phase(snapshots, pks, near, orphan)
    logger.info("LLM phase proposed %d edge(s)", len(llm_proposed))

    # Hallucination filter: stricter overlap on LLM-only edges.
    llm_validated = _validate_with_overlap(
        conn, llm_proposed, min_overlap=_LLM_VALIDATION_OVERLAP
    )

    # Merge with deduplication; heuristic auto-approvals take precedence.
    seen: set[tuple[str, str, str, str]] = set()
    merged: list[dict[str, Any]] = []
    for edge in auto_payloads + llm_validated:
        key = (
            edge["from_table"],
            edge["from_column"],
            edge["to_table"],
            edge["to_column"],
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(edge)
        if len(merged) >= _MAX_EDGES:
            break
    return merged
