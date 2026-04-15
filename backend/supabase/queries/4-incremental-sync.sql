-- Incremental sync support: Airbyte STATE persistence + remove sync_start_at scheduling gate.
-- Run this AFTER 1-create-tables.sql, 2-add-sync-schedule-columns.sql, 3-sync-v2-columns.sql.
--
-- Changes:
--   1. Add last_airbyte_state (JSONB) — persisted Airbyte STATE blob for incremental cursors
--   2. Add incremental_enabled (BOOLEAN) — opt-in toggle for incremental sync mode
--   3. Drop sync_start_at — syncing starts immediately; the onboarding form's "start_date"
--      is now the data-range start date (stored inside the connector config JSONB, not here)

-- ── New columns ──────────────────────────────────────────────────────

ALTER TABLE public.user_connector_configs
  ADD COLUMN IF NOT EXISTS last_airbyte_state   JSONB,
  ADD COLUMN IF NOT EXISTS incremental_enabled  BOOLEAN NOT NULL DEFAULT FALSE;

-- ── Remove sync_start_at (scheduling gate) ───────────────────────────
-- The column is no longer used: syncing begins immediately when a config is added.
-- Data-range "start_date" is a connector parameter inside the config JSONB column.

ALTER TABLE public.user_connector_configs
  DROP COLUMN IF EXISTS sync_start_at;
