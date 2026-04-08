-- =============================================================================
-- bi-agent: baseline public schema + data-onboarding tables
-- Target: fresh Supabase project (PostgreSQL 15+). Run in SQL Editor as a single
-- script after creating the project. Requires Supabase Auth (auth.users).
-- =============================================================================
-- Tenant model: user = tenant (user_id on all user-owned rows; RLS via auth.uid()).
-- Service-role clients (FastAPI admin, Azure Functions) bypass RLS.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Types (match backend fastapi_app/models and prior production dump)
-- ---------------------------------------------------------------------------

CREATE TYPE public.message_role AS ENUM ('user', 'assistant', 'tool');

CREATE TYPE public.user_role AS ENUM ('admin', 'client');

CREATE TYPE public.dashboard_source AS ENUM ('manual', 'template');

CREATE TYPE public.widget_type AS ENUM (
    'kpi',
    'bar',
    'line',
    'area',
    'pie',
    'scatter',
    'heatmap',
    'boxplot',
    'candlestick',
    'histogram',
    'treemap',
    'sunburst',
    'sankey',
    'graph',
    'tree',
    'radar',
    'funnel',
    'gauge',
    'map',
    'waterfall',
    'chart'
);

-- ---------------------------------------------------------------------------
-- Shared trigger helper
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- Profiles (1:1 with auth.users; row created on sign-up)
-- ---------------------------------------------------------------------------

CREATE TABLE public.profiles (
    id          UUID PRIMARY KEY REFERENCES auth.users (id) ON DELETE CASCADE,
    email       TEXT UNIQUE,
    full_name   TEXT,
    avatar_url  TEXT,
    role        public.user_role NOT NULL DEFAULT 'client',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_profiles_email ON public.profiles (email);

CREATE TRIGGER profiles_updated_at
    BEFORE UPDATE ON public.profiles
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    INSERT INTO public.profiles (id, email, full_name)
    VALUES (
        NEW.id,
        NEW.email,
        COALESCE(NEW.raw_user_meta_data ->> 'full_name', '')
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER on_auth_user_created
    AFTER INSERT ON auth.users
    FOR EACH ROW
    EXECUTE PROCEDURE public.handle_new_user();

-- ---------------------------------------------------------------------------
-- Dashboard templates (catalog; optional FK from user dashboards)
-- ---------------------------------------------------------------------------

CREATE TABLE public.dashboard_templates (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug          TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    description   TEXT,
    platforms     TEXT[] NOT NULL DEFAULT '{}',
    tags          TEXT[] NOT NULL DEFAULT '{}',
    preview_image TEXT,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dashboard_templates_active ON public.dashboard_templates (is_active)
    WHERE is_active = TRUE;

CREATE TRIGGER dashboard_templates_updated_at
    BEFORE UPDATE ON public.dashboard_templates
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

CREATE TABLE public.widget_templates (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_id  UUID NOT NULL REFERENCES public.dashboard_templates (id) ON DELETE CASCADE,
    title        TEXT NOT NULL,
    type         public.widget_type NOT NULL,
    layout       JSONB NOT NULL DEFAULT '{}',
    chart_config JSONB NOT NULL DEFAULT '{}',
    data_config  JSONB,
    sort_order   INTEGER NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_widget_templates_template_id ON public.widget_templates (template_id);

-- ---------------------------------------------------------------------------
-- Legacy / single-user data connections (reserved for future use; user-scoped)
-- ---------------------------------------------------------------------------

CREATE TABLE public.data_connections (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    platform          TEXT NOT NULL,
    connection_config JSONB NOT NULL DEFAULT '{}',
    status            TEXT NOT NULL DEFAULT 'active',
    last_synced_at    TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_data_connections_user_id ON public.data_connections (user_id);

CREATE TRIGGER data_connections_updated_at
    BEFORE UPDATE ON public.data_connections
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- Chat (persisted; optional — app may use in-memory agent state only)
-- ---------------------------------------------------------------------------

CREATE TABLE public.chat_sessions (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    title      TEXT,
    agent_type TEXT NOT NULL DEFAULT 'analyst',
    llm        TEXT NOT NULL DEFAULT 'gpt-4.1',
    database   TEXT NOT NULL DEFAULT 'DuckDB',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_chat_sessions_user_id ON public.chat_sessions (user_id);

CREATE TRIGGER chat_sessions_updated_at
    BEFORE UPDATE ON public.chat_sessions
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

CREATE TABLE public.chat_messages (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES public.chat_sessions (id) ON DELETE CASCADE,
    role       public.message_role NOT NULL,
    content    TEXT,
    metadata   JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_chat_messages_session_id ON public.chat_messages (session_id);

-- ---------------------------------------------------------------------------
-- Dashboards & widgets (user-owned)
-- ---------------------------------------------------------------------------

CREATE TABLE public.dashboards (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    description   TEXT,
    tags          TEXT[] NOT NULL DEFAULT '{}',
    source        public.dashboard_source NOT NULL DEFAULT 'manual',
    template_id   UUID REFERENCES public.dashboard_templates (id) ON DELETE SET NULL,
    connection_id UUID REFERENCES public.data_connections (id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dashboards_user_id ON public.dashboards (user_id);

CREATE TRIGGER dashboards_updated_at
    BEFORE UPDATE ON public.dashboards
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

CREATE TABLE public.widgets (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dashboard_id     UUID NOT NULL REFERENCES public.dashboards (id) ON DELETE CASCADE,
    title            TEXT NOT NULL,
    type             public.widget_type NOT NULL,
    layout           JSONB NOT NULL DEFAULT '{}',
    chart_config     JSONB NOT NULL DEFAULT '{}',
    data_config      JSONB,
    data_snapshot    JSONB,
    data_refreshed_at TIMESTAMPTZ,
    sort_order       INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_widgets_dashboard_id ON public.widgets (dashboard_id);

CREATE TRIGGER widgets_updated_at
    BEFORE UPDATE ON public.widgets
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- Data onboarding: Airbyte connector catalog (Azure Function sync)
-- ---------------------------------------------------------------------------

CREATE TABLE public.connector_schemas (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    docker_repository   TEXT NOT NULL UNIQUE,
    name                TEXT NOT NULL,
    docker_image_tag    TEXT NOT NULL DEFAULT 'latest',
    icon_url            TEXT,
    documentation_url   TEXT,
    config_schema       JSONB,
    oauth_config        JSONB,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_connector_schemas_active ON public.connector_schemas (is_active)
    WHERE is_active = TRUE;

CREATE TRIGGER connector_schemas_updated_at
    BEFORE UPDATE ON public.connector_schemas
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- ---------------------------------------------------------------------------
-- Data onboarding: per-user connector configs (sync orchestrator)
-- Unique active config per user per connector (docker_repository).
-- ---------------------------------------------------------------------------

CREATE TABLE public.user_connector_configs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    connector_name      TEXT NOT NULL,
    docker_repository   TEXT NOT NULL,
    docker_image        TEXT NOT NULL,
    config              JSONB NOT NULL,
    oauth_meta          JSONB,
    selected_streams    TEXT[],
    sync_frequency_minutes INTEGER NOT NULL DEFAULT 360,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    sync_validated      BOOLEAN NOT NULL DEFAULT FALSE,
    last_sync_at        TIMESTAMPTZ,
    last_sync_status    TEXT NOT NULL DEFAULT 'pending',
    last_sync_error     TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_user_connector_active
    ON public.user_connector_configs (user_id, docker_repository)
    WHERE is_active = TRUE;

CREATE INDEX idx_user_connector_sync_eligible
    ON public.user_connector_configs (is_active, sync_validated, last_sync_at, sync_frequency_minutes)
    WHERE is_active = TRUE AND sync_validated = TRUE;

CREATE INDEX idx_user_connector_user_id ON public.user_connector_configs (user_id);

CREATE TRIGGER user_connector_configs_updated_at
    BEFORE UPDATE ON public.user_connector_configs
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- =============================================================================
-- Row Level Security
-- =============================================================================

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.dashboard_templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.widget_templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.data_connections ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.chat_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.chat_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.dashboards ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.widgets ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.connector_schemas ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_connector_configs ENABLE ROW LEVEL SECURITY;

-- Profiles: own row only
CREATE POLICY "profiles_select_own"
    ON public.profiles FOR SELECT
    USING (auth.uid() = id);

CREATE POLICY "profiles_update_own"
    ON public.profiles FOR UPDATE
    USING (auth.uid() = id);

-- Templates: public read for active catalog
CREATE POLICY "dashboard_templates_select_active"
    ON public.dashboard_templates FOR SELECT
    USING (is_active = TRUE);

CREATE POLICY "widget_templates_select_public"
    ON public.widget_templates FOR SELECT
    USING (
        EXISTS (
            SELECT 1
            FROM public.dashboard_templates dt
            WHERE dt.id = widget_templates.template_id
              AND dt.is_active = TRUE
        )
    );

-- Template catalog writes: automation / admin only
CREATE POLICY "dashboard_templates_service_write"
    ON public.dashboard_templates FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "widget_templates_service_write"
    ON public.widget_templates FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- Data connections
CREATE POLICY "data_connections_select_own"
    ON public.data_connections FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "data_connections_insert_own"
    ON public.data_connections FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "data_connections_update_own"
    ON public.data_connections FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "data_connections_delete_own"
    ON public.data_connections FOR DELETE
    USING (auth.uid() = user_id);

-- Chat sessions
CREATE POLICY "chat_sessions_select_own"
    ON public.chat_sessions FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "chat_sessions_insert_own"
    ON public.chat_sessions FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "chat_sessions_update_own"
    ON public.chat_sessions FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "chat_sessions_delete_own"
    ON public.chat_sessions FOR DELETE
    USING (auth.uid() = user_id);

-- Chat messages: must belong to the user's session
CREATE POLICY "chat_messages_select_own"
    ON public.chat_messages FOR SELECT
    USING (
        EXISTS (
            SELECT 1
            FROM public.chat_sessions s
            WHERE s.id = chat_messages.session_id
              AND s.user_id = auth.uid()
        )
    );

CREATE POLICY "chat_messages_insert_own"
    ON public.chat_messages FOR INSERT
    WITH CHECK (
        EXISTS (
            SELECT 1
            FROM public.chat_sessions s
            WHERE s.id = chat_messages.session_id
              AND s.user_id = auth.uid()
        )
    );

CREATE POLICY "chat_messages_update_own"
    ON public.chat_messages FOR UPDATE
    USING (
        EXISTS (
            SELECT 1
            FROM public.chat_sessions s
            WHERE s.id = chat_messages.session_id
              AND s.user_id = auth.uid()
        )
    );

CREATE POLICY "chat_messages_delete_own"
    ON public.chat_messages FOR DELETE
    USING (
        EXISTS (
            SELECT 1
            FROM public.chat_sessions s
            WHERE s.id = chat_messages.session_id
              AND s.user_id = auth.uid()
        )
    );

-- Dashboards
CREATE POLICY "dashboards_select_own"
    ON public.dashboards FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "dashboards_insert_own"
    ON public.dashboards FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "dashboards_update_own"
    ON public.dashboards FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "dashboards_delete_own"
    ON public.dashboards FOR DELETE
    USING (auth.uid() = user_id);

-- Widgets: via dashboard ownership
CREATE POLICY "widgets_select_own"
    ON public.widgets FOR SELECT
    USING (
        EXISTS (
            SELECT 1
            FROM public.dashboards d
            WHERE d.id = widgets.dashboard_id
              AND d.user_id = auth.uid()
        )
    );

CREATE POLICY "widgets_insert_own"
    ON public.widgets FOR INSERT
    WITH CHECK (
        EXISTS (
            SELECT 1
            FROM public.dashboards d
            WHERE d.id = widgets.dashboard_id
              AND d.user_id = auth.uid()
        )
    );

CREATE POLICY "widgets_update_own"
    ON public.widgets FOR UPDATE
    USING (
        EXISTS (
            SELECT 1
            FROM public.dashboards d
            WHERE d.id = widgets.dashboard_id
              AND d.user_id = auth.uid()
        )
    );

CREATE POLICY "widgets_delete_own"
    ON public.widgets FOR DELETE
    USING (
        EXISTS (
            SELECT 1
            FROM public.dashboards d
            WHERE d.id = widgets.dashboard_id
              AND d.user_id = auth.uid()
        )
    );

-- Connector catalog: public read; service-role writes (registry sync)
CREATE POLICY "connector_schemas_select"
    ON public.connector_schemas FOR SELECT
    USING (TRUE);

CREATE POLICY "connector_schemas_insert"
    ON public.connector_schemas FOR INSERT
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "connector_schemas_update"
    ON public.connector_schemas FOR UPDATE
    USING (auth.role() = 'service_role');

-- User connector configs (same as prototype)
CREATE POLICY "user_connector_configs_select"
    ON public.user_connector_configs FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "user_connector_configs_insert"
    ON public.user_connector_configs FOR INSERT
    WITH CHECK (auth.uid() = user_id);

CREATE POLICY "user_connector_configs_update"
    ON public.user_connector_configs FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "user_connector_configs_delete"
    ON public.user_connector_configs FOR DELETE
    USING (auth.uid() = user_id);

CREATE POLICY "user_connector_configs_service_select"
    ON public.user_connector_configs FOR SELECT
    USING (auth.role() = 'service_role');

CREATE POLICY "user_connector_configs_service_update"
    ON public.user_connector_configs FOR UPDATE
    USING (auth.role() = 'service_role');

-- =============================================================================
-- Secrets / credentials (no DB storage change here)
-- =============================================================================
-- config and oauth_meta on user_connector_configs may hold secrets. Prefer
-- Supabase Vault, column-level encryption, or app-side encryption before
-- storing production credentials; document the chosen approach in runbooks.
-- =============================================================================
