-- Migration: remove the data_snapshot cache columns from widgets.
-- Charts and KPIs are now always rendered by executing live DuckDB queries;
-- these columns are no longer written to or read from.

ALTER TABLE public.widgets
    DROP COLUMN IF EXISTS data_snapshot,
    DROP COLUMN IF EXISTS data_refreshed_at;
