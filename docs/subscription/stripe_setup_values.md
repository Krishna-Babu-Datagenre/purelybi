# Stripe Setup Values (Provisioned 2026-05-10)

This file captures the Stripe catalog IDs that were provisioned and the remaining backend values to set.

## 1) Provisioned Stripe Products

- Starter: prod_UUISNvaDHrmXqb
- Pro: prod_UUISX1dGpYC6eW
- Growth: prod_UUISqTVyWxUIrF
- AI Credits: prod_UUISBLqXUxaUxm

## 2) Provisioned Stripe Prices

### Subscriptions (Recurring, USD)

- starter_monthly: price_1TVJuDB8eUf9tlyub0ysbfUv ($49/month)
- starter_yearly: price_1TVJuDB8eUf9tlyudxONGyRz ($490/year)
- pro_monthly: price_1TVJuDB8eUf9tlyuYZs0Z7uI ($149/month)
- pro_yearly: price_1TVJuDB8eUf9tlyuxkN4uu7j ($1490/year)
- growth_monthly: price_1TVJuDB8eUf9tlyuJJbkezjh ($299/month)
- growth_yearly: price_1TVJuDB8eUf9tlyuvDAERMxv ($2990/year)

### Top-up Packs (One-time, USD)

- credits_small: price_1TVJsDB8eUf9tlyus5qzXcPS ($10 => 100 credits)
- credits_medium: price_1TVJsLB8eUf9tlyuwEizDhqT ($50 => 500 credits)
- credits_large: price_1TVJsRB8eUf9tlyuQF0K8ElY ($150 => 1500 credits)

## 3) Apply DB Mapping

Run this SQL file in Supabase SQL editor:

- backend/supabase/queries/10-seed-plan-price-map.sql

This seeds and upserts all nine mappings into plan_price_map.

## 4) Backend Environment Values

Set the following in backend environment:

- STRIPE_SECRET_KEY=sk_live_xxx
- STRIPE_WEBHOOK_SECRET=whsec_xxx
- BILLING_CHECKOUT_SUCCESS_URL=https://<frontend-domain>/billing?checkout=success&session_id={CHECKOUT_SESSION_ID}
- BILLING_CHECKOUT_CANCEL_URL=https://<frontend-domain>/billing?checkout=cancelled
- BILLING_PORTAL_RETURN_URL=https://<frontend-domain>/billing

For local development:

- BILLING_CHECKOUT_SUCCESS_URL=http://localhost:5173/billing?checkout=success&session_id={CHECKOUT_SESSION_ID}
- BILLING_CHECKOUT_CANCEL_URL=http://localhost:5173/billing?checkout=cancelled
- BILLING_PORTAL_RETURN_URL=http://localhost:5173/billing

## 5) Manual Stripe Dashboard Steps Still Required

These are not exposed by the current Stripe MCP operation surface used in this workspace.

1. Create webhook endpoint to backend route:
   - POST /api/billing/webhook
2. Subscribe webhook events:
   - checkout.session.completed
   - customer.subscription.created
   - customer.subscription.updated
   - customer.subscription.deleted
   - invoice.paid
   - invoice.payment_failed
   - charge.refunded
3. Copy webhook signing secret into STRIPE_WEBHOOK_SECRET.
4. Configure Billing Portal behavior and return URL.
