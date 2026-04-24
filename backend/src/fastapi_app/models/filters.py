"""Pydantic models for native dashboard filters.

The shapes here mirror the contract documented in
``docs/native_dashboard_filtering.md`` \u00a74. Every filter is anchored to an
explicit ``(table, column)`` pair \u2014 there is no name resolution.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Shared
# ---------------------------------------------------------------------------


class ColumnRef(BaseModel):
    """An explicit ``(table, column)`` reference."""

    table: str = Field(..., min_length=1, max_length=128)
    column: str = Field(..., min_length=1, max_length=128)

    model_config = {"frozen": True}


# ---------------------------------------------------------------------------
# Time filter
# ---------------------------------------------------------------------------


TimePreset = Literal[
    "last_7_days",
    "last_14_days",
    "last_30_days",
    "last_60_days",
    "last_90_days",
    "ytd",
    "mtd",
]


class TimeRange(BaseModel):
    from_: date = Field(..., alias="from")
    to: date

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def _check_order(self) -> "TimeRange":
        if self.to < self.from_:
            raise ValueError("TimeRange.to must be on or after TimeRange.from")
        return self


class TimeFilter(BaseModel):
    column_ref: ColumnRef
    preset: TimePreset | None = None
    range: TimeRange | None = None

    @model_validator(mode="after")
    def _exactly_one_range_source(self) -> "TimeFilter":
        if (self.preset is None) == (self.range is None):
            raise ValueError(
                "TimeFilter must set exactly one of `preset` or `range`."
            )
        return self


# ---------------------------------------------------------------------------
# Categorical / numeric filters
# ---------------------------------------------------------------------------


class CategoricalFilter(BaseModel):
    kind: Literal["categorical"] = "categorical"
    column_ref: ColumnRef
    op: Literal["in", "not_in"] = "in"
    # Allow strings, numbers and booleans \u2014 DuckDB will coerce as needed.
    values: list[Union[str, int, float, bool]] = Field(..., min_length=1, max_length=1000)


class NumericFilter(BaseModel):
    kind: Literal["numeric"] = "numeric"
    column_ref: ColumnRef
    op: Literal["between"] = "between"
    min: float | int | None = None
    max: float | int | None = None

    @model_validator(mode="after")
    def _at_least_one_bound(self) -> "NumericFilter":
        if self.min is None and self.max is None:
            raise ValueError("NumericFilter requires at least one of `min` or `max`.")
        if self.min is not None and self.max is not None and self.max < self.min:
            raise ValueError("NumericFilter `max` must be >= `min`.")
        return self


Filter = Annotated[
    Union[CategoricalFilter, NumericFilter],
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# Top-level spec
# ---------------------------------------------------------------------------


class FilterSpec(BaseModel):
    """The complete filter payload sent from the dashboard UI."""

    time: TimeFilter | None = None
    filters: list[Filter] = Field(default_factory=list)

    def is_empty(self) -> bool:
        return self.time is None and not self.filters

    def all_column_refs(self) -> list[ColumnRef]:
        out: list[ColumnRef] = []
        if self.time is not None:
            out.append(self.time.column_ref)
        for f in self.filters:
            out.append(f.column_ref)
        return out
