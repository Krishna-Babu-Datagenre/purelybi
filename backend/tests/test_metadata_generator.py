"""Integration test for the metadata-generator orchestration.

Runs ``main.run`` end-to-end against an in-memory DuckDB fixture, with the
LLM and Supabase REST calls patched. Validates:

- Every input column ends up in the column upsert payload.
- Relationship validation rejects edges with poor join overlap.
- Job row transitions pending → running → succeeded.

The container code lives in ``azure-job-metadata-generator/``; this test is
parked under ``backend/tests`` so it runs as part of the standard pytest
sweep.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb

# Add the generator package to sys.path so we can import its modules
# without a separate test runner.
_GENERATOR_DIR = (
    Path(__file__).resolve().parents[2] / "azure-job-metadata-generator"
)
sys.path.insert(0, str(_GENERATOR_DIR))

import db_inspect  # noqa: E402
import llm_describe  # noqa: E402
import llm_relationships  # noqa: E402
import main as generator_main  # noqa: E402


def _seed_duckdb() -> tuple[duckdb.DuckDBPyConnection, list[str]]:
    """Two related tables: orders → customers (many_to_one)."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE customers (
            id BIGINT,
            email VARCHAR,
            country VARCHAR
        );
        INSERT INTO customers VALUES
            (1, 'a@x.com', 'US'),
            (2, 'b@x.com', 'CA'),
            (3, 'c@x.com', 'US');

        CREATE TABLE orders (
            id BIGINT,
            customer_id BIGINT,
            total_price DOUBLE,
            created_at TIMESTAMP
        );
        INSERT INTO orders VALUES
            (10, 1, 50.0, TIMESTAMP '2026-01-01 10:00:00'),
            (11, 1, 30.0, TIMESTAMP '2026-01-02 10:00:00'),
            (12, 2, 75.0, TIMESTAMP '2026-01-03 10:00:00'),
            (13, 3, 12.0, TIMESTAMP '2026-01-04 10:00:00');

        -- Unrelated noise (should not produce a relationship edge)
        CREATE TABLE marketing_spend (
            day DATE,
            spend DOUBLE
        );
        INSERT INTO marketing_spend VALUES
            (DATE '2026-01-01', 100.0),
            (DATE '2026-01-02', 80.0),
            (DATE '2026-01-03', 60.0),
            (DATE '2026-01-04', 90.0);
        """
    )
    return conn, ["customers", "orders", "marketing_spend"]


def _fake_describe(snapshot):
    """Deterministic stand-in for the LLM describe step."""
    semantic = {
        ("customers", "id"): "identifier",
        ("customers", "email"): "identifier",
        ("customers", "country"): "categorical",
        ("orders", "id"): "identifier",
        ("orders", "customer_id"): "identifier",
        ("orders", "total_price"): "measure",
        ("orders", "created_at"): "temporal",
        ("marketing_spend", "day"): "temporal",
        ("marketing_spend", "spend"): "measure",
    }
    primary_date = {
        "orders": "created_at",
        "marketing_spend": "day",
    }.get(snapshot.name)
    return {
        "description": f"Test description for {snapshot.name}",
        "grain": f"one row per {snapshot.name} record",
        "primary_date_column": primary_date,
        "columns": [
            {
                "name": col.name,
                "semantic_type": semantic.get((snapshot.name, col.name), "unknown"),
                "description": f"{snapshot.name}.{col.name}",
            }
            for col in snapshot.columns
        ],
    }


def _fake_propose(snapshots):
    """Stand-in for the LLM relationship proposer.

    Returns one valid edge (orders.customer_id -> customers.id) and one
    bogus edge (orders.id -> customers.id) that the join-probe should
    reject due to insufficient overlap.
    """
    return [
        {
            "from_table": "orders",
            "from_column": "customer_id",
            "to_table": "customers",
            "to_column": "id",
            "kind": "many_to_one",
            "confidence": 0.9,
        },
        {
            "from_table": "orders",
            "from_column": "id",
            "to_table": "customers",
            "to_column": "id",
            "kind": "many_to_one",
            "confidence": 0.5,
        },
    ]


class _FakeUpsertRecorder:
    """Captures calls so the test can assert on payloads."""

    def __init__(self) -> None:
        self.tables: list[dict] = []
        self.columns: list[dict] = []
        self.relationships: list[dict] = []
        self.job_updates: list[dict] = []


class MetadataGeneratorIntegrationTest(unittest.TestCase):
    def test_end_to_end_run(self):
        conn, table_names = _seed_duckdb()
        recorder = _FakeUpsertRecorder()

        # Patch out external integrations.
        with patch.object(generator_main, "open_sandbox", return_value=(conn, table_names)), \
             patch.object(generator_main, "describe_table", side_effect=_fake_describe), \
             patch.object(llm_relationships, "propose_edges", side_effect=_fake_propose), \
             patch.object(generator_main, "_client", return_value=MagicMock()) as client_factory, \
             patch.object(generator_main, "update_job", side_effect=lambda c, **kw: recorder.job_updates.append(kw)), \
             patch.object(generator_main, "upsert_table_metadata", side_effect=lambda c, *, user_id, payloads: recorder.tables.extend(payloads) or len(list(payloads))), \
             patch.object(generator_main, "upsert_column_metadata", side_effect=lambda c, *, user_id, payloads: recorder.columns.extend(payloads) or len(list(payloads))), \
             patch.object(generator_main, "upsert_relationships", side_effect=lambda c, *, user_id, edges: recorder.relationships.extend(edges) or len(list(edges))), \
             patch.object(generator_main, "discover_relationships", side_effect=lambda c, snaps: llm_relationships.validate_edges(c, _fake_propose(snaps))):
            rc = generator_main.run(user_id="test-user", job_id="test-job")

        self.assertEqual(rc, 0)

        # Every column from the seeded schema must appear in the payload.
        expected = {
            ("customers", "id"),
            ("customers", "email"),
            ("customers", "country"),
            ("orders", "id"),
            ("orders", "customer_id"),
            ("orders", "total_price"),
            ("orders", "created_at"),
            ("marketing_spend", "day"),
            ("marketing_spend", "spend"),
        }
        seen = {(c["table_name"], c["column_name"]) for c in recorder.columns}
        self.assertEqual(seen, expected)

        # primary_date_column is propagated for tables that have one.
        by_table = {t["table_name"]: t for t in recorder.tables}
        self.assertEqual(by_table["orders"]["primary_date_column"], "created_at")
        self.assertIsNone(by_table["customers"]["primary_date_column"])

        # Only the valid FK survives the join probe.
        self.assertEqual(len(recorder.relationships), 1)
        edge = recorder.relationships[0]
        self.assertEqual(edge["from_table"], "orders")
        self.assertEqual(edge["from_column"], "customer_id")
        self.assertEqual(edge["to_table"], "customers")
        self.assertEqual(edge["to_column"], "id")

        # Job lifecycle: started running, ended succeeded, hit 100% progress.
        statuses = [u.get("status") for u in recorder.job_updates if u.get("status")]
        self.assertEqual(statuses[0], "running")
        self.assertEqual(statuses[-1], "succeeded")
        last = recorder.job_updates[-1]
        self.assertEqual(last.get("progress"), 100)
        self.assertTrue(last.get("mark_finished"))

    def test_no_tables_marks_job_failed(self):
        recorder = _FakeUpsertRecorder()
        empty_conn = duckdb.connect(":memory:")

        with patch.object(generator_main, "open_sandbox", return_value=(empty_conn, [])), \
             patch.object(generator_main, "_client", return_value=MagicMock()), \
             patch.object(generator_main, "update_job", side_effect=lambda c, **kw: recorder.job_updates.append(kw)):
            rc = generator_main.run(user_id="test-user", job_id="test-job")

        self.assertEqual(rc, 0)
        statuses = [u.get("status") for u in recorder.job_updates if u.get("status")]
        self.assertIn("failed", statuses)


class StructuredOutputTests(unittest.TestCase):
    """Confirm the LLM modules use ``with_structured_output`` (not free-form JSON)."""

    def test_describe_table_uses_structured_output(self):
        snap = db_inspect.TableSnapshot(
            name="orders",
            columns=[
                db_inspect.ColumnSnapshot(name="id", data_type="BIGINT"),
                db_inspect.ColumnSnapshot(name="created_at", data_type="TIMESTAMP"),
                db_inspect.ColumnSnapshot(name="total_price", data_type="DOUBLE"),
            ],
        )

        # Build a fake structured-output chain that returns the Pydantic model directly.
        fake_chain = MagicMock()
        fake_chain.invoke.return_value = llm_describe._TableDescription(
            description="One row per order.",
            grain="one row per order",
            primary_date_column="created_at",
            columns=[
                llm_describe._ColumnDescription(
                    name="id", semantic_type="identifier", description="PK"
                ),
                llm_describe._ColumnDescription(
                    name="created_at", semantic_type="temporal", description="Order time"
                ),
                llm_describe._ColumnDescription(
                    name="total_price", semantic_type="measure", description="Order revenue"
                ),
            ],
        )
        fake_llm = MagicMock()
        fake_llm.with_structured_output.return_value = fake_chain

        with patch.object(llm_describe, "_llm", return_value=fake_llm):
            payload = llm_describe.describe_table(snap)

        fake_llm.with_structured_output.assert_called_once_with(
            llm_describe._TableDescription
        )
        self.assertEqual(payload["primary_date_column"], "created_at")
        self.assertEqual(
            {(c["name"], c["semantic_type"]) for c in payload["columns"]},
            {("id", "identifier"), ("created_at", "temporal"), ("total_price", "measure")},
        )

    def test_propose_edges_uses_structured_output(self):
        snaps = [
            db_inspect.TableSnapshot(
                name="orders",
                columns=[
                    db_inspect.ColumnSnapshot(name="id", data_type="BIGINT"),
                    db_inspect.ColumnSnapshot(name="customer_id", data_type="BIGINT"),
                ],
            ),
            db_inspect.TableSnapshot(
                name="customers",
                columns=[
                    db_inspect.ColumnSnapshot(name="id", data_type="BIGINT"),
                ],
            ),
        ]

        fake_chain = MagicMock()
        fake_chain.invoke.return_value = llm_relationships._RelationshipProposal(
            edges=[
                llm_relationships._ProposedEdge(
                    from_table="orders",
                    from_column="customer_id",
                    to_table="customers",
                    to_column="id",
                    kind="many_to_one",
                    confidence=0.9,
                ),
                # Invented column — should be filtered out.
                llm_relationships._ProposedEdge(
                    from_table="orders",
                    from_column="bogus",
                    to_table="customers",
                    to_column="id",
                    kind="many_to_one",
                    confidence=0.5,
                ),
            ]
        )
        fake_llm = MagicMock()
        fake_llm.with_structured_output.return_value = fake_chain

        with patch.object(llm_relationships, "_llm", return_value=fake_llm):
            edges = llm_relationships.propose_edges(snaps)

        fake_llm.with_structured_output.assert_called_once_with(
            llm_relationships._RelationshipProposal
        )
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["from_column"], "customer_id")

    def test_describe_table_batches_large_tables(self):
        """Tables with > COLUMN_BATCH_SIZE columns must be described in batches."""
        total_cols = llm_describe.COLUMN_BATCH_SIZE * 2 + 3  # 3 batches
        cols = [
            db_inspect.ColumnSnapshot(name=f"col_{i}", data_type="VARCHAR")
            for i in range(total_cols)
        ]
        snap = db_inspect.TableSnapshot(
            name="big_table",
            columns=cols,
            stream_name="shopify_orders",
            source_name="Shopify",
        )

        def _fake_invoke(messages):
            # Echo back exactly the columns listed in this batch's prompt.
            human = messages[-1].content
            echoed = [
                llm_describe._ColumnDescription(
                    name=c.name,
                    semantic_type="categorical",
                    description=None,
                )
                for c in cols
                if f"- {c.name} (" in human
            ]
            return llm_describe._TableDescription(
                description="A big table.",
                grain="one row per thing",
                primary_date_column=None,
                columns=echoed,
            )

        fake_chain = MagicMock()
        fake_chain.invoke.side_effect = _fake_invoke
        fake_llm = MagicMock()
        fake_llm.with_structured_output.return_value = fake_chain

        with patch.object(llm_describe, "_llm", return_value=fake_llm):
            payload = llm_describe.describe_table(snap)

        # Three prompt invocations (one per batch), single structured-output
        # chain reused.
        self.assertEqual(fake_chain.invoke.call_count, 3)
        fake_llm.with_structured_output.assert_called_once_with(
            llm_describe._TableDescription
        )

        # Every input column was described (no drops despite the column count).
        returned_names = {c["name"] for c in payload["columns"]}
        self.assertEqual(returned_names, {c.name for c in cols})
        self.assertEqual(payload["description"], "A big table.")

        # Source + stream surfaced in the prompt.
        first_prompt = fake_chain.invoke.call_args_list[0].args[0][-1].content
        self.assertIn("Shopify", first_prompt)
        self.assertIn("shopify_orders", first_prompt)


class SamplingHygieneTests(unittest.TestCase):
    """Sampling must dedupe rows and skip tables with too little data."""

    def test_sample_rows_returns_distinct_rows(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t (a INT, b VARCHAR)")
        # 10 inserts but only 3 distinct rows.
        for _ in range(4):
            conn.execute("INSERT INTO t VALUES (1, 'x')")
        for _ in range(4):
            conn.execute("INSERT INTO t VALUES (2, 'y')")
        for _ in range(2):
            conn.execute("INSERT INTO t VALUES (3, 'z')")

        rows = db_inspect.sample_rows(conn, "t", limit=100)
        self.assertEqual(len(rows), 3)
        self.assertEqual(
            {(r["a"], r["b"]) for r in rows},
            {(1, "x"), (2, "y"), (3, "z")},
        )

    def test_snapshot_all_skips_tables_with_too_few_rows(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE big (id INT, label VARCHAR)")
        for i in range(10):
            conn.execute(f"INSERT INTO big VALUES ({i}, 'row_{i}')")
        # Below the floor (default MIN_TABLE_ROWS=3), even after dedupe.
        conn.execute("CREATE TABLE tiny (a INT)")
        conn.execute("INSERT INTO tiny VALUES (1), (1), (2)")  # 2 distinct

        snaps = db_inspect.snapshot_all(conn, ["big", "tiny"])
        names = [s.name for s in snaps]
        self.assertEqual(names, ["big"])


if __name__ == "__main__":
    unittest.main()
