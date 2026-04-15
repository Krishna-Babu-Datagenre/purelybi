-- Sync V2: Add tracking columns for the state-machine orchestrator.
-- Run this AFTER the existing schema (1-create-tables.sql, 2-add-sync-schedule-columns.sql).

ALTER TABLE public.user_connector_configs
  ADD COLUMN IF NOT EXISTS discovered_catalog      JSONB,
  ADD COLUMN IF NOT EXISTS aca_execution_name       TEXT,
  ADD COLUMN IF NOT EXISTS aca_work_id              TEXT,
  ADD COLUMN IF NOT EXISTS consecutive_failures     INTEGER NOT NULL DEFAULT 0;

-- Index for the orchestrator to find configs by sync status efficiently.
CREATE INDEX IF NOT EXISTS idx_ucc_sync_status_v2
  ON public.user_connector_configs (last_sync_status)
  WHERE last_sync_status IN ('reading', 'uploading');
