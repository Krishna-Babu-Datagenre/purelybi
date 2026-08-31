-- Add multi-source support for DE pipelines.

ALTER TABLE public.de_pipelines
    ADD COLUMN IF NOT EXISTS source_connector_ids UUID[];

-- Backfill from legacy single-source field.
UPDATE public.de_pipelines
SET source_connector_ids = ARRAY[connector_config_id]
WHERE source_connector_ids IS NULL OR array_length(source_connector_ids, 1) IS NULL;

ALTER TABLE public.de_pipelines
    ALTER COLUMN source_connector_ids SET DEFAULT ARRAY[]::UUID[];

ALTER TABLE public.de_pipelines
    ALTER COLUMN source_connector_ids SET NOT NULL;

-- Ensure the legacy primary source stays aligned to the first source id.
UPDATE public.de_pipelines
SET connector_config_id = source_connector_ids[1]
WHERE array_length(source_connector_ids, 1) >= 1
  AND connector_config_id IS DISTINCT FROM source_connector_ids[1];

CREATE INDEX IF NOT EXISTS idx_de_pipelines_source_connector_ids_gin
    ON public.de_pipelines USING GIN (source_connector_ids);
