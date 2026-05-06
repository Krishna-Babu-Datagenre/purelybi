-- =============================================================================
-- 8-subscriptions-sharing.sql
-- =============================================================================

-- 1. Subscription Plans
CREATE TABLE public.subscription_plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tier_name TEXT NOT NULL UNIQUE,
    max_data_sources INTEGER NOT NULL DEFAULT 1,
    max_storage_mb INTEGER NOT NULL DEFAULT 100,
    max_dashboards INTEGER NOT NULL DEFAULT 1,
    included_ai_credits INTEGER NOT NULL DEFAULT 25,
    min_sync_frequency_minutes INTEGER NOT NULL DEFAULT 1440,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Insert default plans
INSERT INTO public.subscription_plans (tier_name, max_data_sources, max_storage_mb, max_dashboards, included_ai_credits, min_sync_frequency_minutes) VALUES
('Free', 1, 100, 1, 25, 1440),
('Starter', 3, 1000, 5, 200, 120),
('Pro', 5, 5000, 15, 500, 60),
('Growth', 10, 10000, 999999, 1000, 10),
('Enterprise', 999999, 100000, 999999, 5000, 10);

-- 2. Modify Profiles
ALTER TABLE public.profiles
ADD COLUMN subscription_tier UUID REFERENCES public.subscription_plans(id) ON DELETE SET NULL,
ADD COLUMN ai_credits_balance INTEGER NOT NULL DEFAULT 0,
ADD COLUMN trial_ends_at TIMESTAMPTZ;

-- Update handle_new_user to assign Free plan
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    free_plan_id UUID;
    free_credits INTEGER;
BEGIN
    SELECT id, included_ai_credits INTO free_plan_id, free_credits 
    FROM public.subscription_plans 
    WHERE tier_name = 'Free' LIMIT 1;

    INSERT INTO public.profiles (id, email, full_name, subscription_tier, ai_credits_balance, trial_ends_at)
    VALUES (
        NEW.id,
        NEW.email,
        COALESCE(NEW.raw_user_meta_data ->> 'full_name', ''),
        free_plan_id,
        COALESCE(free_credits, 25),
        now() + interval '7 days'
    );
    RETURN NEW;
END;
$$;

-- 3. AI Usage Logs
CREATE TABLE public.ai_usage_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    session_id UUID REFERENCES public.chat_sessions(id) ON DELETE SET NULL,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cost_usd NUMERIC(10, 6) NOT NULL DEFAULT 0,
    credits_deducted INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_ai_usage_logs_user_id ON public.ai_usage_logs(user_id);

-- 4. Deduct User Credits RPC
CREATE OR REPLACE FUNCTION public.deduct_user_credits(
    p_user_id UUID,
    p_session_id UUID,
    p_input_tokens INTEGER,
    p_output_tokens INTEGER,
    p_cost_usd NUMERIC,
    p_credits_deducted INTEGER
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    -- Deduct credits
    UPDATE public.profiles
    SET ai_credits_balance = ai_credits_balance - p_credits_deducted
    WHERE id = p_user_id;

    -- Insert log
    INSERT INTO public.ai_usage_logs (
        user_id, session_id, input_tokens, output_tokens, cost_usd, credits_deducted
    ) VALUES (
        p_user_id, p_session_id, p_input_tokens, p_output_tokens, p_cost_usd, p_credits_deducted
    );
END;
$$;

-- 5. Dashboard Sharing
CREATE TYPE public.share_permission_level AS ENUM ('read', 'edit');

CREATE TABLE public.dashboard_shares (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dashboard_id UUID NOT NULL REFERENCES public.dashboards(id) ON DELETE CASCADE,
    shared_by_user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    shared_with_email TEXT NOT NULL,
    permission_level public.share_permission_level NOT NULL DEFAULT 'read',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(dashboard_id, shared_with_email)
);

CREATE INDEX idx_dashboard_shares_dashboard_id ON public.dashboard_shares(dashboard_id);
CREATE INDEX idx_dashboard_shares_shared_with_email ON public.dashboard_shares(shared_with_email);

-- 6. Update RLS on dashboards & widgets
DROP POLICY IF EXISTS "dashboards_select_own" ON public.dashboards;
DROP POLICY IF EXISTS "dashboards_update_own" ON public.dashboards;
DROP POLICY IF EXISTS "widgets_select_own" ON public.widgets;
DROP POLICY IF EXISTS "widgets_update_own" ON public.widgets;

-- Dashboards
CREATE POLICY "dashboards_select_shared"
    ON public.dashboards FOR SELECT
    USING (
        auth.uid() = user_id OR
        EXISTS (
            SELECT 1 FROM public.dashboard_shares ds
            JOIN public.profiles p ON p.email = ds.shared_with_email
            WHERE ds.dashboard_id = dashboards.id AND p.id = auth.uid()
        )
    );

CREATE POLICY "dashboards_update_shared"
    ON public.dashboards FOR UPDATE
    USING (
        auth.uid() = user_id OR
        EXISTS (
            SELECT 1 FROM public.dashboard_shares ds
            JOIN public.profiles p ON p.email = ds.shared_with_email
            WHERE ds.dashboard_id = dashboards.id AND p.id = auth.uid() AND ds.permission_level = 'edit'
        )
    );

-- Widgets
CREATE POLICY "widgets_select_shared"
    ON public.widgets FOR SELECT
    USING (
        EXISTS (
            SELECT 1 FROM public.dashboards d
            WHERE d.id = widgets.dashboard_id AND (
                d.user_id = auth.uid() OR
                EXISTS (
                    SELECT 1 FROM public.dashboard_shares ds
                    JOIN public.profiles p ON p.email = ds.shared_with_email
                    WHERE ds.dashboard_id = d.id AND p.id = auth.uid()
                )
            )
        )
    );

CREATE POLICY "widgets_update_shared"
    ON public.widgets FOR UPDATE
    USING (
        EXISTS (
            SELECT 1 FROM public.dashboards d
            WHERE d.id = widgets.dashboard_id AND (
                d.user_id = auth.uid() OR
                EXISTS (
                    SELECT 1 FROM public.dashboard_shares ds
                    JOIN public.profiles p ON p.email = ds.shared_with_email
                    WHERE ds.dashboard_id = d.id AND p.id = auth.uid() AND ds.permission_level = 'edit'
                )
            )
        )
    );

-- Enable RLS for new tables
ALTER TABLE public.subscription_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ai_usage_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.dashboard_shares ENABLE ROW LEVEL SECURITY;

-- RLS policies for new tables
CREATE POLICY "subscription_plans_select_all"
    ON public.subscription_plans FOR SELECT
    USING (TRUE);

CREATE POLICY "ai_usage_logs_select_own"
    ON public.ai_usage_logs FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "dashboard_shares_select_own"
    ON public.dashboard_shares FOR SELECT
    USING (
        auth.uid() = shared_by_user_id OR
        EXISTS (
            SELECT 1 FROM public.profiles p
            WHERE p.email = dashboard_shares.shared_with_email AND p.id = auth.uid()
        )
    );

CREATE POLICY "dashboard_shares_insert_own"
    ON public.dashboard_shares FOR INSERT
    WITH CHECK (auth.uid() = shared_by_user_id);

CREATE POLICY "dashboard_shares_update_own"
    ON public.dashboard_shares FOR UPDATE
    USING (auth.uid() = shared_by_user_id);

CREATE POLICY "dashboard_shares_delete_own"
    ON public.dashboard_shares FOR DELETE
    USING (auth.uid() = shared_by_user_id);
