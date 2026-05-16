"""Local smoke-test for preprocess.preprocess_batch.

Run standalone:   python test_preprocess.py
Run in Docker:
```
docker build -t sync-uploader -f Dockerfile.uploader .
docker run --rm --entrypoint python -v "${PWD}\test_preprocess.py:/app/test_preprocess.py" sync-uploader test_preprocess.py
```

Exercises every transformation with realistic Airbyte-style records and
prints a clear PASS / FAIL verdict for each.
"""

import sys
import traceback

import pandas as pd

from preprocess import preprocess_batch

_passed = 0
_failed = 0


def _is_string_dtype(series: pd.Series) -> bool:
    """True if the series holds strings (object in pandas 2, str in pandas 3)."""
    return pd.api.types.is_string_dtype(series) and not pd.api.types.is_numeric_dtype(series)


def check(label: str, condition: bool, detail: str = ""):
    global _passed, _failed
    if condition:
        _passed += 1
        print(f"  PASS  {label}")
    else:
        _failed += 1
        print(f"  FAIL  {label}  {detail}")


# ── Test data ─────────────────────────────────────────────────────────

SAMPLE_RECORDS = [
    {
        "Order ID": "1001",
        "Customer Name": "  Alice Johnson\t",
        "Total Amount": "249.99",
        "Is Refunded": "false",
        "created_at": "2025-03-15T10:30:00Z",
        "notes": "",
        "address": {
            "city": "Portland",
            "state": "OR",
            "zip": "97201",
        },
        "tags": ["vip", "wholesale"],
        "empty_col": None,
    },
    # Exact duplicate of the first record — should be dropped
    {
        "Order ID": "1001",
        "Customer Name": "  Alice Johnson\t",
        "Total Amount": "249.99",
        "Is Refunded": "false",
        "created_at": "2025-03-15T10:30:00Z",
        "notes": "",
        "address": {
            "city": "Portland",
            "state": "OR",
            "zip": "97201",
        },
        "tags": ["vip", "wholesale"],
        "empty_col": None,
    },
    {
        "Order ID": "1002",
        "Customer Name": "Bob Smith",
        "Total Amount": "75",
        "Is Refunded": "true",
        "created_at": "2025-04-01T08:00:00Z",
        "notes": "rush order",
        "address": {
            "city": "Seattle",
            "state": "WA",
            "zip": "98101",
        },
        "tags": ["new"],
        "empty_col": None,
    },
    {
        "Order ID": "1003",
        "Customer Name": " Charlie\x00Davis ",
        "Total Amount": "510.00",
        "Is Refunded": "false",
        "created_at": "2025-04-10T14:22:00Z",
        "notes": "  ",
        "address": {
            "city": "Denver",
            "state": "CO",
            "zip": "80201",
        },
        "tags": [],
        "empty_col": None,
    },
]


def test_flatten():
    """Nested dicts should be flattened into separate columns."""
    print("\n── Flatten nested dicts ──")
    df = preprocess_batch(SAMPLE_RECORDS, "orders")
    check("address.city → address_city", "address_city" in df.columns)
    check("address.state → address_state", "address_state" in df.columns)
    check("no raw 'address' column left", "address" not in df.columns)
    check("Portland in address_city", "Portland" in df["address_city"].values)


def test_column_names():
    """Column names should be lowercase with underscores."""
    print("\n── Column name standardization ──")
    df = preprocess_batch(SAMPLE_RECORDS, "orders")
    for col in df.columns:
        check(
            f"'{col}' is clean",
            col == col.lower() and " " not in col,
            f"got '{col}'",
        )
    check("'Order ID' → 'order_id'", "order_id" in df.columns)
    check("'Customer Name' → 'customer_name'", "customer_name" in df.columns)
    check("'Total Amount' → 'total_amount'", "total_amount" in df.columns)


def test_column_collisions():
    """Columns that normalise to the same name get _N suffixes."""
    print("\n── Column collision handling ──")
    records = [{"My Col": "a", "my_col": "b", "MY-COL": "c"}]
    df = preprocess_batch(records, "collision_test")
    check(
        "3 distinct columns survive",
        len(df.columns) == 3,
        f"got {list(df.columns)}",
    )


def test_empty_strings_to_null():
    """Empty strings should become None (Parquet NULL)."""
    print("\n── Empty strings → NULL ──")
    df = preprocess_batch(SAMPLE_RECORDS, "orders")
    if "notes" in df.columns:
        nulls = df["notes"].isna()
        # Record 0 & 1 (duplicate dropped) had "", record 3 had "  " (whitespace-only → trimmed → "")
        check("empty 'notes' are null", nulls.any(), f"nulls={nulls.tolist()}")


def test_dedup():
    """Exact duplicate rows should be dropped."""
    print("\n── Deduplication ──")
    df = preprocess_batch(SAMPLE_RECORDS, "orders")
    check(
        "4 input rows → 3 after dedup",
        len(df) == 3,
        f"got {len(df)} rows",
    )


def test_text_cleaning():
    """Leading/trailing whitespace and non-printable chars removed."""
    print("\n── Text trimming & cleaning ──")
    df = preprocess_batch(SAMPLE_RECORDS, "orders")
    alice = df[df["order_id"] == "1001"]["customer_name"].iloc[0] if "customer_name" in df.columns else None
    charlie = df[df["order_id"] == "1003"]["customer_name"].iloc[0] if "customer_name" in df.columns else None
    check("Alice trimmed", alice == "Alice Johnson", f"got {repr(alice)}")
    check("Charlie \\x00 removed", charlie is not None and "\x00" not in charlie, f"got {repr(charlie)}")


def test_drop_empty_columns():
    """Columns that are entirely NULL should be removed."""
    print("\n── Drop empty columns ──")
    df = preprocess_batch(SAMPLE_RECORDS, "orders")
    check("'empty_col' dropped", "empty_col" not in df.columns)


def test_types_not_coerced():
    """All values should stay as-is — no type coercion applied."""
    print("\n── No type coercion ──")
    records = [
        {"order_id": "000122", "amount": "249.99", "is_active": "true",
         "created_at": "2025-03-15T10:30:00Z"},
        {"order_id": "000456", "amount": "75.00", "is_active": "false",
         "created_at": "2025-04-01T08:00:00Z"},
    ]
    df = preprocess_batch(records, "no_coerce_test")
    check(
        "order_id stays string (zero-padded ID preserved)",
        df["order_id"].iloc[0] == "000122",
        f"got {repr(df['order_id'].iloc[0])}",
    )
    check(
        "amount stays string (not coerced to float)",
        _is_string_dtype(df["amount"]),
        f"dtype={df['amount'].dtype}",
    )
    check(
        "is_active stays string (not coerced to bool)",
        _is_string_dtype(df["is_active"]),
        f"dtype={df['is_active'].dtype}",
    )
    check(
        "created_at stays string (not coerced to datetime)",
        not pd.api.types.is_datetime64_any_dtype(df["created_at"]),
        f"dtype={df['created_at'].dtype}",
    )


def test_arrays_preserved():
    """Arrays should NOT be exploded — kept as-is (lists)."""
    print("\n── Arrays preserved (not exploded) ──")
    df = preprocess_batch(SAMPLE_RECORDS, "orders")
    if "tags" in df.columns:
        val = df["tags"].iloc[0]
        check(
            "tags stays as list",
            isinstance(val, list),
            f"type={type(val).__name__}",
        )


def test_empty_input():
    """Empty record list should return empty DataFrame."""
    print("\n── Edge case: empty input ──")
    df = preprocess_batch([], "empty_stream")
    check("empty list → empty df", len(df) == 0)


def test_flat_records():
    """Records with no nesting should pass through cleanly."""
    print("\n── Edge case: flat records ──")
    records = [
        {"id": "1", "name": "foo", "value": "42"},
        {"id": "2", "name": "bar", "value": "99"},
    ]
    df = preprocess_batch(records, "flat_stream")
    check("2 rows", len(df) == 2)
    check("value stays string (no coercion)", _is_string_dtype(df["value"]))


def test_datetime_false_positive():
    """Columns whose names contain date hints must stay as strings."""
    print("\n── No date coercion: values stay as-is ──")
    records = [
        {"created_at": "2025-06-01T12:00:00Z", "event_date": "2025-07-20"},
        {"created_at": "2025-07-20T09:30:00Z", "event_date": "2025-08-15"},
    ]
    df = preprocess_batch(records, "no_date_coerce")
    check(
        "created_at stays string",
        not pd.api.types.is_datetime64_any_dtype(df["created_at"]),
        f"dtype={df['created_at'].dtype}",
    )
    check(
        "event_date stays string",
        not pd.api.types.is_datetime64_any_dtype(df["event_date"]),
        f"dtype={df['event_date'].dtype}",
    )


def test_dedup_respects_list_differences():
    """Rows identical except for list columns should NOT be deduped."""
    print("\n── Dedup: respects list column differences ──")
    records = [
        {"id": "1", "name": "Alice", "tags": ["a", "b"]},
        {"id": "1", "name": "Alice", "tags": ["c", "d"]},
    ]
    df = preprocess_batch(records, "list_dedup_test")
    check(
        "2 rows survive (different tags)",
        len(df) == 2,
        f"got {len(df)} rows",
    )


def test_dedup_removes_true_list_duplicates():
    """Rows identical INCLUDING list columns should be deduped."""
    print("\n── Dedup: removes true list duplicates ──")
    records = [
        {"id": "1", "name": "Alice", "tags": ["a", "b"]},
        {"id": "1", "name": "Alice", "tags": ["a", "b"]},
        {"id": "2", "name": "Bob", "tags": ["c"]},
    ]
    df = preprocess_batch(records, "list_dedup_test2")
    check(
        "3 rows → 2 after dedup",
        len(df) == 2,
        f"got {len(df)} rows",
    )


def test_whitespace_only_becomes_null():
    """Whitespace-only strings should be trimmed then become NULL."""
    print("\n── Whitespace-only → NULL ──")
    records = [
        {"name": "Alice", "notes": "   "},
        {"name": "Bob", "notes": "\t\n"},
        {"name": "Charlie", "notes": "real note"},
    ]
    df = preprocess_batch(records, "ws_test")
    check(
        "notes[0] (spaces) is null",
        pd.isna(df["notes"].iloc[0]),
        f"got {repr(df['notes'].iloc[0])}",
    )
    check(
        "notes[1] (tab/newline) is null",
        pd.isna(df["notes"].iloc[1]),
        f"got {repr(df['notes'].iloc[1])}",
    )
    check(
        "notes[2] (real text) preserved",
        df["notes"].iloc[2] == "real note",
        f"got {repr(df['notes'].iloc[2])}",
    )


# ── Runner ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_flatten,
        test_column_names,
        test_column_collisions,
        test_empty_strings_to_null,
        test_dedup,
        test_text_cleaning,
        test_drop_empty_columns,
        test_types_not_coerced,
        test_arrays_preserved,
        test_empty_input,
        test_flat_records,
        test_datetime_false_positive,
        test_dedup_respects_list_differences,
        test_dedup_removes_true_list_duplicates,
        test_whitespace_only_becomes_null,
    ]

    for fn in tests:
        try:
            fn()
        except Exception:
            _failed += 1
            print(f"  FAIL  {fn.__name__} raised an exception:")
            traceback.print_exc()

    print(f"\n{'='*50}")
    print(f"  {_passed} passed, {_failed} failed")
    print(f"{'='*50}")
    sys.exit(1 if _failed else 0)
