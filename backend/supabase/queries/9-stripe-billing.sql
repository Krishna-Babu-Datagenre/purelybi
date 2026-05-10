-- =============================================================================
-- 9-stripe-billing.sql
-- =============================================================================
-- Stripe-backed billing data model + credit ledger compatibility upgrade.

-- -----------------------------------------------------------------------------
-- 1) Plan / price mapping (server-side allow-list)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.plan_price_map (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plan_id UUID REFERENCES public.subscription_plans(id) ON DELETE CASCADE,
    price_lookup_key TEXT NOT NULL UNIQUE,
    stripe_price_id TEXT NOT NULL UNIQUE,
    stripe_product_id TEXT NOT NULL,
    amount_usd NUMERIC(12, 2) NOT NULL CHECK (amount_usd >= 0),
    billing_interval TEXT NOT NULL CHECK (billing_interval IN ('month', 'year', 'one_time')),
    currency TEXT NOT NULL DEFAULT 'usd' CHECK (LOWER(currency) = 'usd'),
    is_self_serve BOOLEAN NOT NULL DEFAULT TRUE,
    is_topup BOOLEAN NOT NULL DEFAULT FALSE,
    credits_granted INTEGER CHECK (credits_granted IS NULL OR credits_granted > 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (is_topup = TRUE AND plan_id IS NULL AND billing_interval = 'one_time' AND credits_granted IS NOT NULL)
        OR
        (is_topup = FALSE AND plan_id IS NOT NULL AND billing_interval IN ('month', 'year') AND credits_granted IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_plan_price_map_plan_id
    ON public.plan_price_map(plan_id);

CREATE INDEX IF NOT EXISTS idx_plan_price_map_active
    ON public.plan_price_map(is_active, is_self_serve, is_topup, billing_interval);

DROP TRIGGER IF EXISTS plan_price_map_updated_at ON public.plan_price_map;
CREATE TRIGGER plan_price_map_updated_at
    BEFORE UPDATE ON public.plan_price_map
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- -----------------------------------------------------------------------------
-- 2) Stripe customer / subscription mirrors
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.billing_customers (
    user_id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    stripe_customer_id TEXT NOT NULL UNIQUE,
    email_snapshot TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS billing_customers_updated_at ON public.billing_customers;
CREATE TRIGGER billing_customers_updated_at
    BEFORE UPDATE ON public.billing_customers
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();


CREATE TABLE IF NOT EXISTS public.billing_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    stripe_subscription_id TEXT NOT NULL UNIQUE,
    stripe_customer_id TEXT NOT NULL,
    stripe_price_id TEXT NOT NULL,
    stripe_product_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'incomplete',
            'incomplete_expired',
            'trialing',
            'active',
            'past_due',
            'canceled',
            'unpaid',
            'paused'
        )
    ),
    cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE,
    current_period_start TIMESTAMPTZ,
    current_period_end TIMESTAMPTZ,
    currency TEXT NOT NULL DEFAULT 'usd' CHECK (LOWER(currency) = 'usd'),
    plan_id UUID REFERENCES public.subscription_plans(id) ON DELETE SET NULL,
    latest_invoice_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_user_id
    ON public.billing_subscriptions(user_id);

CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_user_status
    ON public.billing_subscriptions(user_id, status, current_period_end DESC);

DROP TRIGGER IF EXISTS billing_subscriptions_updated_at ON public.billing_subscriptions;
CREATE TRIGGER billing_subscriptions_updated_at
    BEFORE UPDATE ON public.billing_subscriptions
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- -----------------------------------------------------------------------------
-- 3) Top-up purchases + immutable credit ledger
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.billing_credit_purchases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    stripe_checkout_session_id TEXT NOT NULL UNIQUE,
    stripe_payment_intent_id TEXT,
    stripe_invoice_id TEXT,
    price_id UUID REFERENCES public.plan_price_map(id) ON DELETE SET NULL,
    stripe_price_id TEXT NOT NULL,
    credits_granted INTEGER NOT NULL DEFAULT 0 CHECK (credits_granted >= 0),
    amount_usd NUMERIC(12, 2) NOT NULL CHECK (amount_usd >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'failed', 'refunded', 'canceled')),
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_billing_credit_purchases_user_id
    ON public.billing_credit_purchases(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_billing_credit_purchases_status
    ON public.billing_credit_purchases(status, created_at DESC);

DROP TRIGGER IF EXISTS billing_credit_purchases_updated_at ON public.billing_credit_purchases;
CREATE TRIGGER billing_credit_purchases_updated_at
    BEFORE UPDATE ON public.billing_credit_purchases
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();


CREATE TABLE IF NOT EXISTS public.credit_ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    entry_type TEXT NOT NULL CHECK (
        entry_type IN ('subscription_grant', 'topup_grant', 'usage_deduction', 'reversal', 'adjustment')
    ),
    credits_delta INTEGER NOT NULL,
    balance_after INTEGER NOT NULL,
    reference_type TEXT NOT NULL,
    reference_id TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(user_id, entry_type, reference_type, reference_id)
);

CREATE INDEX IF NOT EXISTS idx_credit_ledger_user_id_created_at
    ON public.credit_ledger(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_credit_ledger_reference
    ON public.credit_ledger(reference_type, reference_id);

-- -----------------------------------------------------------------------------
-- 4) Webhook ingestion + idempotency
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.stripe_webhook_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stripe_event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'received' CHECK (status IN ('received', 'processed', 'failed', 'ignored')),
    error_message TEXT,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stripe_webhook_events_status_created
    ON public.stripe_webhook_events(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_stripe_webhook_events_type_created
    ON public.stripe_webhook_events(event_type, created_at DESC);

DROP TRIGGER IF EXISTS stripe_webhook_events_updated_at ON public.stripe_webhook_events;
CREATE TRIGGER stripe_webhook_events_updated_at
    BEFORE UPDATE ON public.stripe_webhook_events
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_updated_at_column();

-- -----------------------------------------------------------------------------
-- 5) RLS policies
-- -----------------------------------------------------------------------------

ALTER TABLE public.plan_price_map ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.billing_customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.billing_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.billing_credit_purchases ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.credit_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stripe_webhook_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "plan_price_map_select_active" ON public.plan_price_map;
CREATE POLICY "plan_price_map_select_active"
    ON public.plan_price_map FOR SELECT
    USING (is_active = TRUE);

DROP POLICY IF EXISTS "plan_price_map_service_write" ON public.plan_price_map;
CREATE POLICY "plan_price_map_service_write"
    ON public.plan_price_map FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

DROP POLICY IF EXISTS "billing_customers_select_own" ON public.billing_customers;
CREATE POLICY "billing_customers_select_own"
    ON public.billing_customers FOR SELECT
    USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "billing_customers_service_write" ON public.billing_customers;
CREATE POLICY "billing_customers_service_write"
    ON public.billing_customers FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

DROP POLICY IF EXISTS "billing_subscriptions_select_own" ON public.billing_subscriptions;
CREATE POLICY "billing_subscriptions_select_own"
    ON public.billing_subscriptions FOR SELECT
    USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "billing_subscriptions_service_write" ON public.billing_subscriptions;
CREATE POLICY "billing_subscriptions_service_write"
    ON public.billing_subscriptions FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

DROP POLICY IF EXISTS "billing_credit_purchases_select_own" ON public.billing_credit_purchases;
CREATE POLICY "billing_credit_purchases_select_own"
    ON public.billing_credit_purchases FOR SELECT
    USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "billing_credit_purchases_service_write" ON public.billing_credit_purchases;
CREATE POLICY "billing_credit_purchases_service_write"
    ON public.billing_credit_purchases FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

DROP POLICY IF EXISTS "credit_ledger_select_own" ON public.credit_ledger;
CREATE POLICY "credit_ledger_select_own"
    ON public.credit_ledger FOR SELECT
    USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "credit_ledger_service_write" ON public.credit_ledger;
CREATE POLICY "credit_ledger_service_write"
    ON public.credit_ledger FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

DROP POLICY IF EXISTS "stripe_webhook_events_service_write" ON public.stripe_webhook_events;
CREATE POLICY "stripe_webhook_events_service_write"
    ON public.stripe_webhook_events FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- -----------------------------------------------------------------------------
-- 6) Credit mutation functions (compatibility + audit)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.grant_user_credits(
    p_user_id UUID,
    p_credits_delta INTEGER,
    p_entry_type TEXT,
    p_reference_type TEXT,
    p_reference_id TEXT,
    p_metadata JSONB DEFAULT '{}'::jsonb
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_current_balance INTEGER;
    v_new_balance INTEGER;
    v_existing_balance INTEGER;
BEGIN
    IF p_credits_delta = 0 THEN
        RAISE EXCEPTION 'p_credits_delta must not be 0';
    END IF;

    IF p_entry_type NOT IN ('subscription_grant', 'topup_grant', 'reversal', 'adjustment') THEN
        RAISE EXCEPTION 'Unsupported grant entry_type: %', p_entry_type;
    END IF;

    IF p_credits_delta > 0 AND p_entry_type NOT IN ('subscription_grant', 'topup_grant', 'adjustment') THEN
        RAISE EXCEPTION 'Positive p_credits_delta is not allowed for entry_type: %', p_entry_type;
    END IF;

    IF p_credits_delta < 0 AND p_entry_type NOT IN ('reversal', 'adjustment') THEN
        RAISE EXCEPTION 'Negative p_credits_delta is not allowed for entry_type: %', p_entry_type;
    END IF;

    -- Serialize grant processing by deterministic reference key.
    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            p_user_id::text || '|' || p_entry_type || '|' || p_reference_type || '|' || p_reference_id,
            0
        )
    );

    SELECT balance_after
      INTO v_existing_balance
      FROM public.credit_ledger
     WHERE user_id = p_user_id
       AND entry_type = p_entry_type
       AND reference_type = p_reference_type
       AND reference_id = p_reference_id
     LIMIT 1;

    IF FOUND THEN
        RETURN v_existing_balance;
    END IF;

    SELECT ai_credits_balance
      INTO v_current_balance
      FROM public.profiles
     WHERE id = p_user_id
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'User profile not found: %', p_user_id;
    END IF;

    v_new_balance := v_current_balance + p_credits_delta;

    UPDATE public.profiles
       SET ai_credits_balance = v_new_balance
     WHERE id = p_user_id;

    INSERT INTO public.credit_ledger (
        user_id,
        entry_type,
        credits_delta,
        balance_after,
        reference_type,
        reference_id,
        metadata
    ) VALUES (
        p_user_id,
        p_entry_type,
        p_credits_delta,
        v_new_balance,
        p_reference_type,
        p_reference_id,
        COALESCE(p_metadata, '{}'::jsonb)
    );

    RETURN v_new_balance;
END;
$$;


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
DECLARE
    v_current_balance INTEGER;
    v_new_balance INTEGER;
    v_usage_log_id UUID;
BEGIN
    SELECT ai_credits_balance
      INTO v_current_balance
      FROM public.profiles
     WHERE id = p_user_id
     FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'User profile not found: %', p_user_id;
    END IF;

    v_new_balance := v_current_balance - p_credits_deducted;

    UPDATE public.profiles
       SET ai_credits_balance = v_new_balance
     WHERE id = p_user_id;

    INSERT INTO public.ai_usage_logs (
        user_id,
        session_id,
        input_tokens,
        output_tokens,
        cost_usd,
        credits_deducted
    ) VALUES (
        p_user_id,
        p_session_id,
        p_input_tokens,
        p_output_tokens,
        p_cost_usd,
        p_credits_deducted
    )
    RETURNING id INTO v_usage_log_id;

    INSERT INTO public.credit_ledger (
        user_id,
        entry_type,
        credits_delta,
        balance_after,
        reference_type,
        reference_id,
        metadata
    ) VALUES (
        p_user_id,
        'usage_deduction',
        -p_credits_deducted,
        v_new_balance,
        'ai_usage_log',
        v_usage_log_id::text,
        jsonb_build_object(
            'session_id', p_session_id,
            'input_tokens', p_input_tokens,
            'output_tokens', p_output_tokens,
            'cost_usd', p_cost_usd
        )
    );
END;
$$;
