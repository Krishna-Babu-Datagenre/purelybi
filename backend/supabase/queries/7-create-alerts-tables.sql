-- ============================================================
-- Migration 7 — Alerts feature tables
-- ============================================================
-- Creates:  alerts, alert_runs, alert_notifications
-- RLS:      owner-only policies on all three tables
-- Indexes:  optimised for evaluator "due-now" query and run history
-- ============================================================

-- ── alerts ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.alerts (
    id                   uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id              uuid        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    name                 text        NOT NULL,
    description          text,
    definition           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    sql_query            text        NOT NULL,
    comparator           text        NOT NULL
        CHECK (comparator IN ('gt','gte','lt','lte','eq','neq','pct_change_gt','pct_change_lt')),
    threshold            numeric     NOT NULL,
    frequency            text        NOT NULL DEFAULT 'hourly'
        CHECK (frequency IN ('every_15_min','hourly','daily')),
    notification_channel text        NOT NULL DEFAULT 'email'
        CHECK (notification_channel IN ('email')),
    notification_target  text,
    enabled              boolean     NOT NULL DEFAULT true,
    last_evaluated_at    timestamptz,
    last_fired_at        timestamptz,
    last_state           text        CHECK (last_state IS NULL OR last_state IN ('ok','firing','error')),
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now()
);

-- Indexes used by the evaluator's "due-now" query
CREATE INDEX IF NOT EXISTS idx_alerts_user_id          ON public.alerts (user_id);
CREATE INDEX IF NOT EXISTS idx_alerts_enabled_eval     ON public.alerts (enabled, last_evaluated_at);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION public.set_alerts_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_alerts_updated_at ON public.alerts;
CREATE TRIGGER trg_alerts_updated_at
    BEFORE UPDATE ON public.alerts
    FOR EACH ROW EXECUTE FUNCTION public.set_alerts_updated_at();

-- RLS
ALTER TABLE public.alerts ENABLE ROW LEVEL SECURITY;
CREATE POLICY alerts_owner ON public.alerts
    FOR ALL USING (auth.uid() = user_id) WITH CHECK (auth.uid() = user_id);


-- ── alert_runs ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.alert_runs (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id         uuid        NOT NULL REFERENCES public.alerts(id) ON DELETE CASCADE,
    user_id          uuid        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    evaluated_at     timestamptz NOT NULL DEFAULT now(),
    status           text        NOT NULL CHECK (status IN ('ok','firing','error')),
    observed_value   numeric,
    error_message    text,
    notification_id  uuid
);

CREATE INDEX IF NOT EXISTS idx_alert_runs_alert_eval
    ON public.alert_runs (alert_id, evaluated_at DESC);

ALTER TABLE public.alert_runs ENABLE ROW LEVEL SECURITY;
CREATE POLICY alert_runs_owner ON public.alert_runs
    FOR ALL USING (auth.uid() = user_id) WITH CHECK (auth.uid() = user_id);


-- ── alert_notifications ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.alert_notifications (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id        uuid        NOT NULL REFERENCES public.alerts(id) ON DELETE CASCADE,
    user_id         uuid        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    channel         text        NOT NULL DEFAULT 'email',
    target          text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    provider_id     text,
    delivered_at    timestamptz,
    error_message   text
);

ALTER TABLE public.alert_notifications ENABLE ROW LEVEL SECURITY;
CREATE POLICY alert_notifications_owner ON public.alert_notifications
    FOR ALL USING (auth.uid() = user_id) WITH CHECK (auth.uid() = user_id);

-- Add FK from alert_runs.notification_id → alert_notifications.id
ALTER TABLE public.alert_runs
    ADD CONSTRAINT fk_alert_runs_notification
    FOREIGN KEY (notification_id) REFERENCES public.alert_notifications(id)
    ON DELETE SET NULL;
