-- Sync error log: detailed per-attempt error history.
-- Run this AFTER 1-create-tables.sql … 4-incremental-sync.sql.
--
-- Changes:
--   1. Add sync_error_log (JSONB) — append-only array of error entries,
--      each entry: { "ts": "<ISO-8601>", "phase": "connector|uploader|orchestrator", "error": "<full detail>" }
--      The orchestrator and uploader APPEND to this array on each failure.
--      last_sync_error remains as the short human-readable summary.

ALTER TABLE public.user_connector_configs
  ADD COLUMN IF NOT EXISTS sync_error_log JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN public.user_connector_configs.sync_error_log IS
  'Append-only array of detailed error entries [{ts, phase, error}]. Capped at last 5 consecutive failures.';
