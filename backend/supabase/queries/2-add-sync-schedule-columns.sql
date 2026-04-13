-- One-off migration: add sync scheduling columns to existing installs.
-- Existing rows are backfilled to recurring mode.

ALTER TABLE public.user_connector_configs
ADD COLUMN IF NOT EXISTS sync_mode TEXT;

ALTER TABLE public.user_connector_configs
ADD COLUMN IF NOT EXISTS sync_start_at TIMESTAMPTZ;

UPDATE public.user_connector_configs
SET sync_mode = 'recurring'
WHERE sync_mode IS NULL;

ALTER TABLE public.user_connector_configs
ALTER COLUMN sync_mode SET DEFAULT 'recurring';

ALTER TABLE public.user_connector_configs
ALTER COLUMN sync_mode SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'user_connector_configs_sync_mode_check'
    ) THEN
        ALTER TABLE public.user_connector_configs
        ADD CONSTRAINT user_connector_configs_sync_mode_check
        CHECK (sync_mode IN ('one_off', 'recurring'));
    END IF;
END $$;
