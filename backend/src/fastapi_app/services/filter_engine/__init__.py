"""Filter engine: translate a :class:`FilterSpec` into per-table TEMP VIEWs.

See ``docs/native_dashboard_filtering.md`` \u00a72 and \u00a75. Public entry point is
:func:`apply_filters`, which installs / drops the necessary views around a
widget SQL execution.
"""

from .apply import apply_filters, install_filter_views, drop_filter_views
from .build_views import ViewPlan, build_view_plans
from .detect_tables import detect_referenced_tables
from .relationships import RelationshipGraph, find_filter_path

__all__ = [
    "apply_filters",
    "install_filter_views",
    "drop_filter_views",
    "ViewPlan",
    "build_view_plans",
    "detect_referenced_tables",
    "RelationshipGraph",
    "find_filter_path",
]
