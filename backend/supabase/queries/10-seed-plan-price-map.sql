-- =============================================================================
-- 10-seed-plan-price-map.sql
-- =============================================================================
-- Seeds plan_price_map with Stripe product/price IDs provisioned on 2026-05-10.
-- This script is idempotent and safe to re-run.

WITH plan_ids AS (
    SELECT tier_name, id
    FROM public.subscription_plans
    WHERE tier_name IN ('Starter', 'Pro', 'Growth')
)
INSERT INTO public.plan_price_map (
    plan_id,
    price_lookup_key,
    stripe_price_id,
    stripe_product_id,
    amount_usd,
    billing_interval,
    currency,
    is_self_serve,
    is_topup,
    credits_granted,
    is_active
)
VALUES
    ((SELECT id FROM plan_ids WHERE tier_name = 'Starter'), 'starter_monthly', 'price_1TVJuDB8eUf9tlyub0ysbfUv', 'prod_UUISNvaDHrmXqb', 49.00,  'month',    'usd', TRUE, FALSE, NULL, TRUE),
    ((SELECT id FROM plan_ids WHERE tier_name = 'Starter'), 'starter_yearly',  'price_1TVJuDB8eUf9tlyudxONGyRz', 'prod_UUISNvaDHrmXqb', 490.00, 'year',     'usd', TRUE, FALSE, NULL, TRUE),
    ((SELECT id FROM plan_ids WHERE tier_name = 'Pro'),     'pro_monthly',     'price_1TVJuDB8eUf9tlyuYZs0Z7uI', 'prod_UUISX1dGpYC6eW', 149.00, 'month',    'usd', TRUE, FALSE, NULL, TRUE),
    ((SELECT id FROM plan_ids WHERE tier_name = 'Pro'),     'pro_yearly',      'price_1TVJuDB8eUf9tlyuxkN4uu7j', 'prod_UUISX1dGpYC6eW', 1490.00,'year',     'usd', TRUE, FALSE, NULL, TRUE),
    ((SELECT id FROM plan_ids WHERE tier_name = 'Growth'),  'growth_monthly',  'price_1TVJuDB8eUf9tlyuJJbkezjh', 'prod_UUISqTVyWxUIrF', 299.00, 'month',    'usd', TRUE, FALSE, NULL, TRUE),
    ((SELECT id FROM plan_ids WHERE tier_name = 'Growth'),  'growth_yearly',   'price_1TVJuDB8eUf9tlyuvDAERMxv', 'prod_UUISqTVyWxUIrF', 2990.00,'year',     'usd', TRUE, FALSE, NULL, TRUE),
    (NULL,                                                   'credits_small',   'price_1TVJsDB8eUf9tlyus5qzXcPS', 'prod_UUISBLqXUxaUxm', 10.00,  'one_time', 'usd', TRUE, TRUE,  100,  TRUE),
    (NULL,                                                   'credits_medium',  'price_1TVJsLB8eUf9tlyuwEizDhqT', 'prod_UUISBLqXUxaUxm', 50.00,  'one_time', 'usd', TRUE, TRUE,  500,  TRUE),
    (NULL,                                                   'credits_large',   'price_1TVJsRB8eUf9tlyuQF0K8ElY', 'prod_UUISBLqXUxaUxm', 150.00, 'one_time', 'usd', TRUE, TRUE,  1500, TRUE)
ON CONFLICT (price_lookup_key)
DO UPDATE
SET
    plan_id = EXCLUDED.plan_id,
    stripe_price_id = EXCLUDED.stripe_price_id,
    stripe_product_id = EXCLUDED.stripe_product_id,
    amount_usd = EXCLUDED.amount_usd,
    billing_interval = EXCLUDED.billing_interval,
    currency = EXCLUDED.currency,
    is_self_serve = EXCLUDED.is_self_serve,
    is_topup = EXCLUDED.is_topup,
    credits_granted = EXCLUDED.credits_granted,
    is_active = EXCLUDED.is_active,
    updated_at = now();

-- Quick verification
SELECT
    ppm.price_lookup_key,
    ppm.stripe_price_id,
    ppm.amount_usd,
    ppm.billing_interval,
    ppm.is_topup,
    ppm.credits_granted,
    sp.tier_name AS plan_tier
FROM public.plan_price_map ppm
LEFT JOIN public.subscription_plans sp ON sp.id = ppm.plan_id
WHERE ppm.price_lookup_key IN (
    'starter_monthly', 'starter_yearly',
    'pro_monthly', 'pro_yearly',
    'growth_monthly', 'growth_yearly',
    'credits_small', 'credits_medium', 'credits_large'
)
ORDER BY ppm.price_lookup_key;
