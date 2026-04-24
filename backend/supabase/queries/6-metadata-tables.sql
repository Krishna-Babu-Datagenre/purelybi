-- Native dashboard filtering — metadata layer (LLM-generated, user-editable).
-- Run this AFTER 1-create-tables.sql … 5-sync-error-log.sql.
--
-- Adds four tables that describe each tenant's DuckDB schema in semantic
-- terms: tables, columns, relationships, and the lifecycle of the metadata
-- generation job that produces them.
--
-- Tenant model: user = tenant (user_id mirrors auth.users.id; RLS via auth.uid()).
-- See docs/native_dashboard_filtering.md §3 for the design.

-- ---------------------------------------------------------------------------
-- Enums
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'metadata_semantic_type') THEN
        CREATE TYPE public.metadata_semantic_type AS ENUM (
            'categorical',
            'numeric',
            'temporal',
            'identifier',
            'measure',
            'unknown'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'metadata_relationship_kind') THEN
        CREATE TYPE public.metadata_relationship_kind AS ENUM (
            'many_to_one',
            'one_to_one',
            'many_to_many'
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'metadata_job_status') THEN
        CREATE TYPE public.metadata_job_status AS ENUM (
            'pending',
            'running',
            'succeeded',
            'failed',
            'cancelled'
        );
    END IF;
END$$;

-- ---------------------------------------------------------------------------
-- tenant_table_metadata — per-table descriptions and primary date column.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.tenant_table_metadata (
    user_id              UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    table_name           TEXT NOT NULL,
    description          TEXT,
    primary_date_column  TEXT,
    grain                TEXT,
    generated_at         TIMESTAMPTZ,
    edited_by_user       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, table_name)
);

CREATE INDEX IF NOT EXISTS idx_tenant_table_metadata_user_id
    ON public.tenant_table_metadata (user_id);

CREATE TRIGGER tenant_table_metadata_updated_at
    BEFORE UPDATE ON public.tenant_table_metadata
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- tenant_column_metadata — per-column semantic info, cardinality, samples.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.tenant_column_metadata (
    user_id              UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    table_name           TEXT NOT NULL,
    column_name          TEXT NOT NULL,
    data_type            TEXT NOT NULL,
    semantic_type        public.metadata_semantic_type NOT NULL DEFAULT 'unknown',
    description          TEXT,
    is_filterable        BOOLEAN NOT NULL DEFAULT TRUE,
    cardinality          INTEGER,
    sample_values        JSONB,
    generated_at         TIMESTAMPTZ,
    edited_by_user       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, table_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_tenant_column_metadata_user_table
    ON public.tenant_column_metadata (user_id, table_name);

CREATE TRIGGER tenant_column_metadata_updated_at
    BEFORE UPDATE ON public.tenant_column_metadata
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- tenant_table_relationships — edges in the join graph (LLM proposed,
-- join-probe validated, user editable).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.tenant_table_relationships (
    user_id          UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    from_table       TEXT NOT NULL,
    from_column      TEXT NOT NULL,
    to_table         TEXT NOT NULL,
    to_column        TEXT NOT NULL,
    kind             public.metadata_relationship_kind NOT NULL,
    confidence       NUMERIC(4, 3),
    edited_by_user   BOOLEAN NOT NULL DEFAULT FALSE,
    generated_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, from_table, from_column, to_table, to_column)
);

CREATE INDEX IF NOT EXISTS idx_tenant_relationships_user_from
    ON public.tenant_table_relationships (user_id, from_table);

CREATE INDEX IF NOT EXISTS idx_tenant_relationships_user_to
    ON public.tenant_table_relationships (user_id, to_table);

CREATE TRIGGER tenant_table_relationships_updated_at
    BEFORE UPDATE ON public.tenant_table_relationships
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- tenant_metadata_jobs — lifecycle of the ACA metadata-generation job.
-- One row per generation attempt; UI polls the latest.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.tenant_metadata_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    status          public.metadata_job_status NOT NULL DEFAULT 'pending',
    progress        NUMERIC(5, 2) NOT NULL DEFAULT 0,
    message         TEXT,
    error           TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    aca_execution_name TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenant_metadata_jobs_user_recent
    ON public.tenant_metadata_jobs (user_id, created_at DESC);

CREATE TRIGGER tenant_metadata_jobs_updated_at
    BEFORE UPDATE ON public.tenant_metadata_jobs
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- Row-level security
-- ---------------------------------------------------------------------------

ALTER TABLE public.tenant_table_metadata        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.tenant_column_metadata       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.tenant_table_relationships   ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.tenant_metadata_jobs         ENABLE ROW LEVEL SECURITY;

-- tenant_table_metadata
CREATE POLICY "tenant_table_metadata_select_own"
    ON public.tenant_table_metadata FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "tenant_table_metadata_insert_own"
    ON public.tenant_table_metadata FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "tenant_table_metadata_update_own"
    ON public.tenant_table_metadata FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "tenant_table_metadata_delete_own"
    ON public.tenant_table_metadata FOR DELETE
    USING (auth.uid() = user_id);

-- tenant_column_metadata
CREATE POLICY "tenant_column_metadata_select_own"
    ON public.tenant_column_metadata FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "tenant_column_metadata_insert_own"
    ON public.tenant_column_metadata FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "tenant_column_metadata_update_own"
    ON public.tenant_column_metadata FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "tenant_column_metadata_delete_own"
    ON public.tenant_column_metadata FOR DELETE
    USING (auth.uid() = user_id);

-- tenant_table_relationships
CREATE POLICY "tenant_table_relationships_select_own"
    ON public.tenant_table_relationships FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "tenant_table_relationships_insert_own"
    ON public.tenant_table_relationships FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "tenant_table_relationships_update_own"
    ON public.tenant_table_relationships FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "tenant_table_relationships_delete_own"
    ON public.tenant_table_relationships FOR DELETE
    USING (auth.uid() = user_id);

-- tenant_metadata_jobs (clients read-only; writes happen via service role)
CREATE POLICY "tenant_metadata_jobs_select_own"
    ON public.tenant_metadata_jobs FOR SELECT
    USING (auth.uid() = user_id);

-- Service-role escape hatches (FastAPI admin client + ACA job)
CREATE POLICY "tenant_table_metadata_service_all"
    ON public.tenant_table_metadata FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "tenant_column_metadata_service_all"
    ON public.tenant_column_metadata FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "tenant_table_relationships_service_all"
    ON public.tenant_table_relationships FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "tenant_metadata_jobs_service_all"
    ON public.tenant_metadata_jobs FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- ---------------------------------------------------------------------------
-- Comments
-- ---------------------------------------------------------------------------

COMMENT ON TABLE public.tenant_table_metadata IS
    'LLM-generated, user-editable per-table metadata used by the dashboard filter engine.';
COMMENT ON TABLE public.tenant_column_metadata IS
    'LLM-generated, user-editable per-column metadata (semantic type, cardinality, samples).';
COMMENT ON TABLE public.tenant_table_relationships IS
    'Join graph edges between tables, validated via DuckDB join probes.';
COMMENT ON TABLE public.tenant_metadata_jobs IS
    'Lifecycle records for the ACA metadata-generator job.';
COMMENT ON COLUMN public.tenant_table_metadata.edited_by_user IS
    'When TRUE, regeneration must not overwrite this row.';
COMMENT ON COLUMN public.tenant_column_metadata.edited_by_user IS
    'When TRUE, regeneration must not overwrite this row.';
COMMENT ON COLUMN public.tenant_table_relationships.edited_by_user IS
    'When TRUE, regeneration must not overwrite this edge.';
