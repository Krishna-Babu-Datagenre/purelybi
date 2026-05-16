"""Lightweight universal pre-processing for Airbyte record batches.

Each transformation step is independently guarded so a failure in one
step (e.g. type coercion) never prevents the remaining steps — or the
upload itself — from completing.
"""

import re

import pandas as pd

# ── Compiled patterns ─────────────────────────────────────────────────

_CLEAN_COL_RE = re.compile(r"[^\w]")
_MULTI_UNDERSCORE_RE = re.compile(r"_+")
_NON_PRINTABLE_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")


# ── Internal helpers ──────────────────────────────────────────────────


def _clean_column_name(name: str) -> str:
    """Lowercase, replace spaces/special chars with underscore."""
    name = str(name).lower().strip()
    name = _CLEAN_COL_RE.sub("_", name)
    name = _MULTI_UNDERSCORE_RE.sub("_", name)
    return name.strip("_")


def _resolve_column_collisions(columns: list[str]) -> list[str]:
    """Deduplicate column names by appending ``_N`` suffixes."""
    seen: dict[str, int] = {}
    result: list[str] = []
    for c in columns:
        if c in seen:
            seen[c] += 1
            result.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            result.append(c)
    return result


def _clean_text_series(series: pd.Series) -> pd.Series:
    """Trim whitespace and remove non-printable characters."""
    return (
        series
        .str.strip()
        .str.replace(_NON_PRINTABLE_RE, "", regex=True)
    )


# ── Public API ────────────────────────────────────────────────────────


def preprocess_batch(records: list[dict], stream_name: str) -> pd.DataFrame:
    """Apply lightweight universal pre-processing to a batch of records.

    Transformations (in order):
      1. Flatten one level of nested dicts via json_normalize
      2. Standardize column names (lowercase, underscores)
      3. Trim / clean text fields
      4. Replace empty strings with None (Parquet NULL)
      5. Drop exact duplicate rows
      6. Drop completely empty columns

    Type coercion (numeric, boolean, datetime) is intentionally omitted.
    DuckDB infers types at query time, and automatic coercion risks
    silently corrupting data (e.g. zero-padded IDs like "000122" → 122).

    Each step is independently wrapped in try/except so a failure in one
    step skips it and continues with the rest.  If even the initial
    DataFrame construction fails, raw records are returned unprocessed.

    Note: arrays/lists are NOT exploded (that changes data grain and is
    stream-specific). They are kept as-is and later JSON-serialised by
    sanitize_df_for_parquet.
    """
    # Build the initial DataFrame — if this fails, nothing else can proceed.
    try:
        df = pd.json_normalize(records, sep="_")
    except Exception as exc:
        print(f"  WARNING: [{stream_name}] flatten failed, using raw data: {exc}")
        return pd.DataFrame(records)

    # 2. Standardize column names
    try:
        df.columns = _resolve_column_collisions(
            [_clean_column_name(c) for c in df.columns]
        )
    except Exception as exc:
        print(f"  WARNING: [{stream_name}] column-name cleanup skipped: {exc}")

    # 3. Trim and clean text fields (BEFORE empty-string → NULL so
    #    whitespace-only values like "  " get trimmed to "" first,
    #    then step 4 converts them to NULL in one pass).
    try:
        for col in df.select_dtypes(include=["object"]).columns:
            sample = df[col].dropna().head(20)
            if len(sample) > 0 and sample.apply(lambda v: isinstance(v, str)).all():
                df[col] = _clean_text_series(df[col])
    except Exception as exc:
        print(f"  WARNING: [{stream_name}] text cleaning skipped: {exc}")

    # 4. Empty strings → None (becomes Parquet NULL)
    try:
        df = df.replace({"": None})
    except Exception as exc:
        print(f"  WARNING: [{stream_name}] empty-string replacement skipped: {exc}")

    # 5. Drop exact duplicate rows
    try:
        # JSON-serialize unhashable columns (list, dict, set) so they
        # participate in the duplicate check instead of being ignored.
        import json as _json
        unhashable_cols: list[str] = []
        for col in df.columns:
            sample = df[col].dropna().head(10)
            if len(sample) > 0 and sample.apply(
                lambda v: isinstance(v, (list, dict, set))
            ).any():
                unhashable_cols.append(col)

        if unhashable_cols:
            # Temporarily serialize for dedup comparison
            serialized = {}
            for col in unhashable_cols:
                serialized[col] = df[col].apply(
                    lambda v: _json.dumps(v, sort_keys=True, default=str)
                    if isinstance(v, (list, dict, set)) else v
                )
                df[col] = serialized[col]

            df = df.drop_duplicates()

            # Restore original values via index alignment
            # (drop_duplicates preserves the original index)
            for col in unhashable_cols:
                # Re-read from the un-serialized source — but we already
                # overwrote df[col].  Instead, deserialize back.
                df[col] = df[col].apply(
                    lambda v: _json.loads(v) if isinstance(v, str) else v
                )
        else:
            df = df.drop_duplicates()
    except Exception as exc:
        print(f"  WARNING: [{stream_name}] dedup skipped: {exc}")

    # 6. Drop completely empty columns
    try:
        df = df.dropna(axis=1, how="all")
    except Exception as exc:
        print(f"  WARNING: [{stream_name}] drop-empty-columns skipped: {exc}")

    return df
