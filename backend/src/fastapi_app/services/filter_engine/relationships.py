"""Relationship-graph BFS for filter rerouting.

When a filter targets a column on a table that the widget SQL does not scan
directly, we look for a safe path through ``tenant_table_relationships`` so
the filter can still be applied via a semi-join.

Direction safety: traversing ``many_to_one`` or ``one_to_one`` edges in the
direction ``from -> to`` cannot fan rows out. We disallow ``many_to_many``
and reverse-traversal of ``many_to_one`` edges to avoid duplicating rows in
the scanning table.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Iterable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Edge:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    kind: str  # ``many_to_one`` | ``one_to_one`` | ``many_to_many``


@dataclass(frozen=True)
class PathStep:
    """One hop in a relationship traversal: ``scan_table.col == next_table.col``."""

    scan_table: str
    scan_column: str
    next_table: str
    next_column: str


_SAFE_KINDS = frozenset({"many_to_one", "one_to_one"})


class RelationshipGraph:
    """In-memory adjacency list built from ``tenant_table_relationships`` rows."""

    def __init__(self, edges: Iterable[dict] | Iterable[Edge]) -> None:
        self._adj: dict[str, list[Edge]] = defaultdict(list)
        for raw in edges:
            edge = self._coerce(raw)
            if edge is None:
                continue
            # Forward (FK side -> PK side): always safe for many_to_one / one_to_one.
            if edge.kind in _SAFE_KINDS:
                self._adj[edge.from_table].append(edge)
            # one_to_one is also safe in reverse (no fan-out).
            if edge.kind == "one_to_one":
                self._adj[edge.to_table].append(
                    Edge(
                        from_table=edge.to_table,
                        from_column=edge.to_column,
                        to_table=edge.from_table,
                        to_column=edge.from_column,
                        kind="one_to_one",
                    )
                )

    @staticmethod
    def _coerce(raw: dict | Edge) -> Edge | None:
        if isinstance(raw, Edge):
            return raw
        try:
            return Edge(
                from_table=str(raw["from_table"]),
                from_column=str(raw["from_column"]),
                to_table=str(raw["to_table"]),
                to_column=str(raw["to_column"]),
                kind=str(raw["kind"]),
            )
        except (KeyError, TypeError):
            return None

    def neighbors(self, table: str) -> list[Edge]:
        return self._adj.get(table, [])


def find_filter_path(
    graph: RelationshipGraph,
    *,
    scanning_tables: set[str],
    target_table: str,
    max_hops: int = 3,
) -> list[PathStep] | None:
    """Find a safe path from any scanning table to *target_table*.

    Returns the sequence of join steps, or ``None`` if no safe path exists
    within *max_hops* hops. The first scanning table reached wins (BFS gives
    the shortest path).
    """
    if target_table in scanning_tables:
        return []

    queue: deque[tuple[str, list[PathStep]]] = deque(
        (t, []) for t in scanning_tables
    )
    visited: set[str] = set(scanning_tables)

    while queue:
        node, path = queue.popleft()
        if len(path) >= max_hops:
            continue
        for edge in graph.neighbors(node):
            if edge.to_table in visited:
                continue
            step = PathStep(
                scan_table=edge.from_table,
                scan_column=edge.from_column,
                next_table=edge.to_table,
                next_column=edge.to_column,
            )
            new_path = [*path, step]
            if edge.to_table == target_table:
                return new_path
            visited.add(edge.to_table)
            queue.append((edge.to_table, new_path))

    return None
