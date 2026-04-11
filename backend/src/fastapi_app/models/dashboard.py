"""
Pydantic models matching the Dashboard JSON API Contract (v1.0).

These models enforce the schema that the React frontend expects,
covering KPI widgets, ECharts-based chart widgets, layout, and
the top-level payload actions (create / update / patch).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class WidgetType(str, Enum):
    kpi = "kpi"
    bar = "bar"
    line = "line"
    area = "area"
    pie = "pie"
    scatter = "scatter"
    heatmap = "heatmap"
    boxplot = "boxplot"
    candlestick = "candlestick"
    histogram = "histogram"
    treemap = "treemap"
    sunburst = "sunburst"
    sankey = "sankey"
    graph = "graph"
    tree = "tree"
    radar = "radar"
    funnel = "funnel"
    gauge = "gauge"
    map = "map"
    waterfall = "waterfall"
    chart = "chart"  # catch-all


class KpiIcon(str, Enum):
    revenue = "revenue"
    orders = "orders"
    aov = "aov"
    customers = "customers"
    generic = "generic"


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class WidgetLayout(BaseModel):
    """Grid position on the 12-column dashboard layout."""

    x: int = Field(..., ge=0, le=11, description="Column offset (0-11)")
    y: int = Field(..., ge=0, description="Row offset")
    w: int = Field(..., ge=1, le=12, description="Width in grid columns")
    h: int = Field(..., ge=1, description="Height in row units")
    minW: int | None = None
    minH: int | None = None
    maxW: int | None = None
    maxH: int | None = None


class KpiConfig(BaseModel):
    """Configuration for a KPI card widget."""

    value: float
    prefix: str | None = None
    suffix: str | None = None
    change: float | None = None
    changeLabel: str | None = None
    icon: KpiIcon | None = None
    sparkline: list[float] | None = None


class Widget(BaseModel):
    """A single dashboard widget (KPI card or chart)."""

    id: str
    title: str
    type: WidgetType
    layout: WidgetLayout | None = None
    chartConfig: KpiConfig | dict[str, Any] = Field(
        ...,
        description=(
            "KpiConfig when type='kpi', otherwise a raw Apache ECharts option object."
        ),
    )


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class DashboardMeta(BaseModel):
    id: str
    name: str
    description: str | None = None
    createdAt: str  # ISO-8601
    updatedAt: str  # ISO-8601
    tags: list[str] | None = None


class Dashboard(BaseModel):
    meta: DashboardMeta
    widgets: list[Widget]


# ---------------------------------------------------------------------------
# Payload wrappers (sent to the frontend)
# ---------------------------------------------------------------------------

class DashboardPayload(BaseModel):
    """Top-level payload delivered to the React frontend."""

    action: Literal["create", "update", "patch"]
    dashboard: Dashboard | None = None

    # patch-only fields
    dashboardId: str | None = None
    meta: dict[str, Any] | None = None
    addWidgets: list[Widget] | None = None
    updateWidgets: list[dict[str, Any]] | None = None
    removeWidgetIds: list[str] | None = None
