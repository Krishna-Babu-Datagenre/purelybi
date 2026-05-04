"""
Pydantic models for the Alerts feature.

Covers alert creation, alert output, alert runs, and the structured
AlertDefinition that the builder agent produces.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Comparator(str, Enum):
    gt = "gt"
    gte = "gte"
    lt = "lt"
    lte = "lte"
    eq = "eq"
    neq = "neq"
    pct_change_gt = "pct_change_gt"
    pct_change_lt = "pct_change_lt"


class Frequency(str, Enum):
    every_15_min = "every_15_min"
    hourly = "hourly"
    daily = "daily"


# ---------------------------------------------------------------------------
# Alert definition (structured output from builder agent)
# ---------------------------------------------------------------------------

class AlertDefinition(BaseModel):
    """Structured alert spec produced by the Alert Builder agent."""

    metric_description: str = Field(
        ..., description="Human summary of what is measured"
    )
    table: str = Field(
        ..., description="Source table in user's DuckDB / parquet"
    )
    metric_sql: str = Field(
        ..., description="SELECT producing one scalar"
    )
    comparator: Comparator
    threshold: float
    time_window: str = Field(
        ..., description='e.g. "yesterday", "last_7_days"'
    )
    frequency: Frequency = Frequency.hourly
    notification_channel: Literal["email"] = "email"
    notification_target: EmailStr | None = None


# ---------------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------------

class AlertCreate(BaseModel):
    """Request body for POST /api/alerts."""

    name: str = Field(..., min_length=1)
    description: str | None = None
    definition: AlertDefinition


class AlertUpdate(BaseModel):
    """Request body for PATCH /api/alerts/{id}."""

    name: str | None = Field(default=None, min_length=1)
    frequency: Frequency | None = None
    enabled: bool | None = None
    notification_target: str | None = None


class AlertOut(BaseModel):
    """Response model for alert endpoints."""

    id: UUID
    name: str
    description: str | None = None
    definition: AlertDefinition
    sql_query: str
    comparator: Comparator
    threshold: float
    frequency: Frequency
    notification_channel: str
    notification_target: str | None = None
    enabled: bool
    last_evaluated_at: datetime | None = None
    last_fired_at: datetime | None = None
    last_state: Literal["ok", "firing", "error"] | None = None
    created_at: datetime
    updated_at: datetime


class AlertRunOut(BaseModel):
    """Response model for alert run history."""

    id: UUID
    evaluated_at: datetime
    status: Literal["ok", "firing", "error"]
    observed_value: float | None = None
    error_message: str | None = None
