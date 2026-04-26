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

        # Patch out external integrations. The hybrid discovery engine is
        # bypassed entirely; we feed the fake LLM proposals straight through
        # the new internal overlap-validation helper so the test still
        # exercises the join-probe rejection path.
        with patch.object(generator_main, "open_sandbox", return_value=(conn, table_names)), \
             patch.object(generator_main, "describe_table", side_effect=_fake_describe), \
             patch.object(generator_main, "_client", return_value=MagicMock()) as client_factory, \
             patch.object(generator_main, "delete_generated_metadata", return_value=None), \
             patch.object(generator_main, "update_job", side_effect=lambda c, **kw: recorder.job_updates.append(kw)), \
             patch.object(generator_main, "upsert_table_metadata", side_effect=lambda c, *, user_id, payloads: recorder.tables.extend(payloads) or len(list(payloads))), \
             patch.object(generator_main, "upsert_column_metadata", side_effect=lambda c, *, user_id, payloads: recorder.columns.extend(payloads) or len(list(payloads))), \
             patch.object(generator_main, "upsert_relationships", side_effect=lambda c, *, user_id, edges: recorder.relationships.extend(edges) or len(list(edges))), \
             patch.object(
                 generator_main,
                 "discover_relationships",
                 side_effect=lambda c, snaps: llm_relationships._validate_with_overlap(
                     c,
                     _fake_propose(snaps),
                     min_overlap=llm_relationships._MIN_OVERLAP_RATIO,
                 ),
             ):
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
             patch.object(generator_main, "delete_generated_metadata", return_value=None), \
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

    def test_llm_phase_uses_structured_output(self):
        snaps = [
            db_inspect.TableSnapshot(
                name="orders",
                columns=[
                    db_inspect.ColumnSnapshot(
                        name="id", data_type="BIGINT", cardinality=100
                    ),
                    db_inspect.ColumnSnapshot(
                        name="weird_ref", data_type="BIGINT", cardinality=50
                    ),
                ],
            ),
            db_inspect.TableSnapshot(
                name="customers",
                columns=[
                    db_inspect.ColumnSnapshot(
                        name="id", data_type="BIGINT", cardinality=50
                    ),
                ],
            ),
        ]
        # ``orders.id`` is the orders PK; ``customers.id`` is the customers PK.
        # ``orders.weird_ref`` has no obvious name link, so the heuristic
        # phase will route ``orders`` to the LLM as an orphan target.
        pks = {"orders": {"id"}, "customers": {"id"}}
        orphan = {"orders"}

        fake_chain = MagicMock()
        fake_chain.invoke.return_value = llm_relationships._RelationshipProposal(
            edges=[
                llm_relationships._ProposedEdge(
                    target_column="weird_ref",
                    parent_table="customers",
                    parent_column="id",
                    kind="many_to_one",
                    confidence=0.9,
                ),
                # Invented column — should be filtered out.
                llm_relationships._ProposedEdge(
                    target_column="bogus",
                    parent_table="customers",
                    parent_column="id",
                    kind="many_to_one",
                    confidence=0.5,
                ),
            ]
        )
        fake_llm = MagicMock()
        fake_llm.with_structured_output.return_value = fake_chain

        with patch.object(llm_relationships, "_llm", return_value=fake_llm):
            edges = llm_relationships._llm_phase(
                snaps, pks, near_misses=[], orphan_tables=orphan
            )

        fake_llm.with_structured_output.assert_called_once_with(
            llm_relationships._RelationshipProposal
        )
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["from_table"], "orders")
        self.assertEqual(edges[0]["from_column"], "weird_ref")
        self.assertEqual(edges[0]["to_table"], "customers")
        self.assertEqual(edges[0]["to_column"], "id")

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


class HeuristicRelationshipTests(unittest.TestCase):
    """Unit tests for the Phase 1 heuristic engine."""

    def test_id_to_id_cross_table_detected(self):
        """Two tables sharing a PK column named ``id`` with high overlap."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE parents (id BIGINT, label VARCHAR);
            INSERT INTO parents VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d'), (5,'e');

            CREATE TABLE children (id BIGINT, value DOUBLE);
            INSERT INTO children VALUES (1, 10), (2, 20), (3, 30);
        """)
        snaps = [
            db_inspect.TableSnapshot(
                name="parents",
                columns=[
                    db_inspect.ColumnSnapshot(name="id", data_type="BIGINT", cardinality=5),
                    db_inspect.ColumnSnapshot(name="label", data_type="VARCHAR", cardinality=5),
                ],
            ),
            db_inspect.TableSnapshot(
                name="children",
                columns=[
                    db_inspect.ColumnSnapshot(name="id", data_type="BIGINT", cardinality=3),
                    db_inspect.ColumnSnapshot(name="value", data_type="DOUBLE", cardinality=3),
                ],
            ),
        ]
        # Both ``id`` columns are PK candidates (cardinality ≈ row count).
        pks = {"parents": {"id"}, "children": {"id"}}

        auto, near, orphan = llm_relationships._heuristic_phase(conn, snaps, pks)
        # children.id values are a subset of parents.id → valid FK detected.
        all_edges = auto + near
        matching = [
            e for e in all_edges
            if e.from_table == "children" and e.to_table == "parents"
            and e.from_column == "id" and e.to_column == "id"
        ]
        self.assertTrue(len(matching) >= 1, f"Expected id→id edge, got {all_edges}")

    def test_semantic_type_identifier_expands_candidates(self):
        """A column not matching name/type regex but tagged 'identifier' is considered."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE dim_store (store_key VARCHAR, name VARCHAR);
            INSERT INTO dim_store VALUES
                ('S1','A'), ('S2','B'), ('S3','C'), ('S4','D'), ('S5','E'),
                ('S6','F'), ('S7','G'), ('S8','H'), ('S9','I'), ('S10','J');

            CREATE TABLE fact_sales (store_ref VARCHAR, amount DOUBLE);
            INSERT INTO fact_sales VALUES
                ('S1',10), ('S2',20), ('S3',15), ('S4',25), ('S5',30),
                ('S6',35), ('S1',12), ('S2',22);
        """)
        snaps = [
            db_inspect.TableSnapshot(
                name="dim_store",
                columns=[
                    db_inspect.ColumnSnapshot(
                        name="store_key", data_type="VARCHAR", cardinality=10,
                    ),
                    db_inspect.ColumnSnapshot(name="name", data_type="VARCHAR", cardinality=10),
                ],
            ),
            db_inspect.TableSnapshot(
                name="fact_sales",
                columns=[
                    db_inspect.ColumnSnapshot(
                        name="store_ref", data_type="VARCHAR", cardinality=6,
                        # LLM describe classified this as "identifier"
                        semantic_type="identifier",
                    ),
                    db_inspect.ColumnSnapshot(
                        name="amount", data_type="DOUBLE", cardinality=7,
                    ),
                ],
            ),
        ]
        pks = {"dim_store": {"store_key"}, "fact_sales": set()}

        auto, near, orphan = llm_relationships._heuristic_phase(conn, snaps, pks)
        # store_ref → store_key should surface via fuzzy name + overlap,
        # but only because semantic_type="identifier" lets store_ref in.
        all_from_col = [e.from_column for e in auto + near]
        self.assertIn("store_ref", all_from_col)


if __name__ == "__main__":
    unittest.main()
