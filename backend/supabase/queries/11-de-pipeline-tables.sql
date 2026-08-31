-- Data Engineering pipeline metadata layer.
-- Run this after existing schema scripts (1-create-tables.sql ... 10-seed-plan-price-map.sql).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'de_pipeline_run_status') THEN
        CREATE TYPE public.de_pipeline_run_status AS ENUM (
            'queued',
            'running',
            'succeeded',
            'failed',
            'failed_to_start'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'de_materialization_status') THEN
        CREATE TYPE public.de_materialization_status AS ENUM (
            'none',
            'ready',
            'failed',
            'stale'
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS public.de_pipelines (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id              UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    connector_config_id  UUID NOT NULL REFERENCES public.user_connector_configs (id) ON DELETE CASCADE,
    name                 TEXT NOT NULL,
    is_active            BOOLEAN NOT NULL DEFAULT FALSE,
    version              INTEGER NOT NULL DEFAULT 1,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.de_pipeline_steps (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id  UUID NOT NULL REFERENCES public.de_pipelines (id) ON DELETE CASCADE,
    step_order   INTEGER NOT NULL,
    recipe_type  TEXT NOT NULL,
    config_json  JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_enabled   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT de_pipeline_steps_step_order_positive CHECK (step_order > 0)
);

CREATE TABLE IF NOT EXISTS public.de_pipeline_runs (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id          UUID NOT NULL REFERENCES public.de_pipelines (id) ON DELETE CASCADE,
    user_id              UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    connector_config_id  UUID NOT NULL REFERENCES public.user_connector_configs (id) ON DELETE CASCADE,
    trigger_source       TEXT NOT NULL DEFAULT 'sync_upload_success',
    status               public.de_pipeline_run_status NOT NULL DEFAULT 'queued',
    started_at           TIMESTAMPTZ,
    ended_at             TIMESTAMPTZ,
    error                TEXT,
    sync_work_id         TEXT,
    aca_execution_name   TEXT,
    failed_step_order    INTEGER,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.de_dataset_materializations (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id              UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    connector_config_id  UUID NOT NULL REFERENCES public.user_connector_configs (id) ON DELETE CASCADE,
    pipeline_id          UUID NOT NULL REFERENCES public.de_pipelines (id) ON DELETE CASCADE,
    last_success_run_id  UUID REFERENCES public.de_pipeline_runs (id) ON DELETE SET NULL,
    status               public.de_materialization_status NOT NULL DEFAULT 'none',
    output_prefix        TEXT,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_de_pipelines_active_per_dataset
    ON public.de_pipelines (user_id, connector_config_id)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_de_pipelines_user_connector_active
    ON public.de_pipelines (user_id, connector_config_id, is_active);

CREATE UNIQUE INDEX IF NOT EXISTS uq_de_pipeline_steps_order
    ON public.de_pipeline_steps (pipeline_id, step_order);

CREATE INDEX IF NOT EXISTS idx_de_pipeline_runs_recent
    ON public.de_pipeline_runs (pipeline_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_de_pipeline_runs_lookup
    ON public.de_pipeline_runs (user_id, connector_config_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_de_materializations_dataset
    ON public.de_dataset_materializations (user_id, connector_config_id);

CREATE TRIGGER de_pipelines_updated_at
    BEFORE UPDATE ON public.de_pipelines
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

CREATE TRIGGER de_pipeline_steps_updated_at
    BEFORE UPDATE ON public.de_pipeline_steps
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

CREATE TRIGGER de_pipeline_runs_updated_at
    BEFORE UPDATE ON public.de_pipeline_runs
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

CREATE TRIGGER de_dataset_materializations_updated_at
    BEFORE UPDATE ON public.de_dataset_materializations
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

ALTER TABLE public.de_pipelines ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.de_pipeline_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.de_pipeline_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.de_dataset_materializations ENABLE ROW LEVEL SECURITY;

CREATE POLICY "de_pipelines_select_own"
    ON public.de_pipelines FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "de_pipelines_insert_own"
    ON public.de_pipelines FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "de_pipelines_update_own"
    ON public.de_pipelines FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "de_pipelines_delete_own"
    ON public.de_pipelines FOR DELETE
    USING (auth.uid() = user_id);

CREATE POLICY "de_pipeline_steps_select_own"
    ON public.de_pipeline_steps FOR SELECT
    USING (
        EXISTS (
            SELECT 1
            FROM public.de_pipelines p
            WHERE p.id = pipeline_id
              AND p.user_id = auth.uid()
        )
    );

CREATE POLICY "de_pipeline_steps_insert_own"
    ON public.de_pipeline_steps FOR INSERT
    WITH CHECK (
        EXISTS (
            SELECT 1
            FROM public.de_pipelines p
            WHERE p.id = pipeline_id
              AND p.user_id = auth.uid()
        )
    );

CREATE POLICY "de_pipeline_steps_update_own"
    ON public.de_pipeline_steps FOR UPDATE
    USING (
        EXISTS (
            SELECT 1
            FROM public.de_pipelines p
            WHERE p.id = pipeline_id
              AND p.user_id = auth.uid()
        )
    );

CREATE POLICY "de_pipeline_steps_delete_own"
    ON public.de_pipeline_steps FOR DELETE
    USING (
        EXISTS (
            SELECT 1
            FROM public.de_pipelines p
            WHERE p.id = pipeline_id
              AND p.user_id = auth.uid()
        )
    );

CREATE POLICY "de_pipeline_runs_select_own"
    ON public.de_pipeline_runs FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "de_dataset_materializations_select_own"
    ON public.de_dataset_materializations FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "de_pipelines_service_all"
    ON public.de_pipelines FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "de_pipeline_steps_service_all"
    ON public.de_pipeline_steps FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "de_pipeline_runs_service_all"
    ON public.de_pipeline_runs FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "de_dataset_materializations_service_all"
    ON public.de_dataset_materializations FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

COMMENT ON TABLE public.de_pipelines IS
    'Data engineering pipeline definitions per tenant and connector config.';
COMMENT ON TABLE public.de_pipeline_steps IS
    'Ordered recipe steps attached to a DE pipeline.';
COMMENT ON TABLE public.de_pipeline_runs IS
    'Runtime history of DE pipeline executions, typically triggered post-sync.';
COMMENT ON TABLE public.de_dataset_materializations IS
    'Latest transformed output pointer for transformed-first query routing.';