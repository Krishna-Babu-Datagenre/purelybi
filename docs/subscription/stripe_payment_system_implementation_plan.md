# Stripe Subscription & Payment System Implementation Plan

## 1. Purpose

This document defines the implementation plan for introducing a Stripe-based billing system integrated with the current FastAPI + Supabase architecture.

This plan does not modify the already implemented plan in `docs/subscription/subscription_implementation_plan.md`.

## 2. Scope

### In Scope

- Stripe subscriptions for self-serve plans.
- Stripe-powered checkout and billing portal flows.
- Supabase data model additions for billing lifecycle and auditability.
- Webhook-driven billing state sync (Stripe as billing source of truth, Supabase as app source of truth).
- USD-only pricing.
- Enterprise as contact/on-request only (excluded from self-serve checkout).
- Additional AI credit purchases after included plan credits are exhausted.

### Out of Scope

- Full accounting/tax engine implementation details.
- Sales CRM and contract workflow for Enterprise deals.
- One-off historical data migration scripts for legacy external billing systems.

## 3. Existing Architecture Alignment (Current State)

The billing design must align with the current production model:

- Tenant model is user-centric (`profiles.id` maps to `auth.users.id`).
- Plan entitlements are currently enforced via:
  - `fastapi_app/services/subscription_service.py`
  - checks in routers (`chat`, `onboarding`, `connectors`, `dashboards`).
- Current credit gating returns HTTP `402` when balance is zero.
- Current credit deduction path:
  - token usage callback in `src/ai/callbacks.py`
  - RPC `deduct_user_credits` in `backend/supabase/queries/8-subscriptions-sharing.sql`
- Current plan metadata is in `subscription_plans`; profile stores `subscription_tier`, `ai_credits_balance`, `trial_ends_at`.
- No Stripe integration exists yet; frontend billing currently shows placeholder/coming-soon actions.

Design principle: preserve existing entitlement checks and profile shape for backward compatibility, while introducing Stripe-backed billing state and credit purchase flows.

## 4. Target Billing Architecture

### Core Components

1. Stripe
- Product catalog (Starter, Pro, Growth subscriptions; AI credit top-up products).
- Checkout Sessions for self-serve purchase/update entry points.
- Billing Portal for self-service subscription management.
- Webhooks for asynchronous lifecycle events.

2. FastAPI Billing Module
- New router: `fastapi_app/routers/billing.py`.
- New service: `fastapi_app/services/billing_service.py`.
- Webhook handler with signature verification and idempotent processing.

3. Supabase Billing Data Layer
- New billing tables for Stripe mapping, event idempotency, and billing state.
- Existing `profiles` + `subscription_plans` remain entitlement read path.
- Credit grants and deductions stored in auditable ledger-style records.

4. Frontend Billing UX
- Replace disabled "Upgrade" actions with real checkout session creation.
- Add "Buy AI credits" flow (billing page and out-of-credits modal).
- Keep existing usage/entitlement display pattern from `BillingPage` and `useAuthStore`.

## 5. Plan Catalog & Currency Rules

### Pricing Currency

- All Stripe self-serve prices must use `usd`.
- Backend must reject non-USD price IDs.
- API responses should include explicit currency for display consistency.

### Enterprise Handling

- Enterprise remains in `subscription_plans` for entitlement modeling, but is marked non-self-serve.
- Enterprise plan is removed from self-serve checkout APIs and CTA flows.
- Frontend card action becomes "Contact Sales" rather than checkout.

Recommended approach:
- Add a plan metadata flag (for example `is_self_serve` boolean) in DB mapping layer.
- Ensure `Enterprise` has `is_self_serve = false`.

## 6. Stripe Setup & Configuration Runbook (Using Stripe MCP Server)

Use the existing Stripe MCP tooling for provisioning and maintenance.

### Step A: Create Products

- Subscription products: `Starter`, `Pro`, `Growth`.
- One-time top-up products: for example `AI Credits 100`, `AI Credits 500`, `AI Credits 1000`.

MCP tools:
- `mcp_stripe_create_product`
- or `mcp_stripe_stripe_api_execute` after discovery via:
  - `mcp_stripe_stripe_api_search`
  - `mcp_stripe_stripe_api_details`

### Step B: Create Prices (USD)

- Create recurring monthly prices for self-serve subscription products (`currency=usd`).
- Create one-time prices for top-up products (`currency=usd`).

MCP tool:
- `mcp_stripe_create_price`

### Step C: Persist Price Mapping in Supabase

- Store Stripe product/price IDs and map them to internal `subscription_plans` tiers.
- Explicitly flag top-up prices and self-serve eligibility.

### Step D: Configure Billing Portal

- Enable subscription upgrades/downgrades/cancel, payment method updates, and invoice history.
- Create portal sessions server-side per authenticated user.

MCP flow:
- Discover and execute billing portal operations using:
  - `mcp_stripe_stripe_api_search`
  - `mcp_stripe_stripe_api_details`
  - `mcp_stripe_stripe_api_execute`

### Step E: Configure Webhook Endpoint

- Create one secure webhook endpoint for backend billing ingress.
- Store signing secret in backend environment (for example `STRIPE_WEBHOOK_SECRET`).
- Subscribe to required event set (see section 9).

MCP flow:
- Use `mcp_stripe_stripe_api_search` + `mcp_stripe_stripe_api_execute` to create/manage webhook endpoints.

## 7. Supabase Data Model Changes

Add a new migration (for example `backend/supabase/queries/9-stripe-billing.sql`).

### Required New Tables

1. `billing_customers`
- `user_id` (PK/FK to auth user)
- `stripe_customer_id` (unique)
- `email_snapshot`
- timestamps

2. `billing_subscriptions`
- `user_id`
- `stripe_subscription_id` (unique)
- `stripe_price_id`
- `stripe_product_id`
- `status`
- `cancel_at_period_end`
- `current_period_start`, `current_period_end`
- `currency` (must be `usd`)
- `plan_id` (FK to `subscription_plans`)
- timestamps

3. `billing_credit_purchases`
- `id`
- `user_id`
- `stripe_checkout_session_id` (unique)
- `stripe_payment_intent_id`
- `stripe_invoice_id` (nullable)
- `price_id`
- `credits_granted`
- `amount_usd`
- `status`
- timestamps

4. `credit_ledger`
- `id`
- `user_id`
- `entry_type` (subscription_grant, topup_grant, usage_deduction, reversal, adjustment)
- `credits_delta` (positive/negative)
- `balance_after`
- `reference_type`, `reference_id`
- `metadata` (jsonb)
- timestamps

5. `stripe_webhook_events`
- `id`
- `stripe_event_id` (unique)
- `event_type`
- `payload` (jsonb)
- `status` (received, processed, failed, ignored)
- `error_message`
- `processed_at`
- timestamps

### Optional Helper Table

`plan_price_map`
- internal plan tier to Stripe price mapping
- `is_self_serve`
- `billing_interval`
- active flags for rotation/versioning

### RLS and Access Pattern

- Keep owner-read for user-facing billing summary tables.
- Webhook writes use service-role backend path (trusted server).
- Preserve current app pattern: service role bypasses RLS in backend after JWT-based identity resolution.

## 8. Backend API Design

Add `billing` router under `/api/billing`:

1. `POST /api/billing/checkout/subscription`
- Input: target plan tier + billing interval.
- Output: Stripe Checkout URL.
- Guardrails:
  - reject Enterprise self-serve attempts.
  - reject unknown/non-USD/non-active price IDs.

2. `POST /api/billing/checkout/topup`
- Input: top-up pack code/price.
- Output: Stripe Checkout URL for one-time payment.

3. `POST /api/billing/portal-session`
- Output: Stripe Billing Portal URL for current user.

4. `GET /api/billing/summary`
- Returns current billing status, next invoice date, plan, and credit balances.

5. `POST /api/billing/webhook`
- Raw body + Stripe signature verification.
- Idempotent event ingestion and async-safe processing.

6. `GET /api/billing/plans/self-serve`
- Returns only checkout-eligible plans (Starter/Pro/Growth).
- Enterprise excluded by design.

## 9. Webhook Event Handling & Billing Lifecycle

### Minimum Event Set

- `checkout.session.completed`
- `customer.subscription.created`
- `customer.subscription.updated`
- `customer.subscription.deleted`
- `invoice.paid`
- `invoice.payment_failed`
- `charge.refunded` or equivalent refund event for top-up reversals
- optional: `customer.subscription.trial_will_end`

### Processing Rules

1. Verify signature before parsing.
2. Insert into `stripe_webhook_events` with unique `stripe_event_id`.
3. If duplicate event, acknowledge and no-op.
4. Process in transaction boundaries where balance/entitlement changes happen.
5. Update `billing_subscriptions` and synchronize `profiles.subscription_tier`.
6. Grant credits only once per billing transaction reference.

### Stripe vs Internal Source of Truth

- Stripe is source of truth for payment/subscription status.
- Internal source of truth for entitlement enforcement remains Supabase profile + plan limits.

## 10. Entitlements & Credit Tracking Model

### Plan Entitlements

- Continue reading limits from `subscription_plans` to avoid rewriting existing guards.
- On subscription lifecycle updates, map Stripe price to internal `subscription_plans` tier.
- Keep existing checks (`can_add_source`, `can_create_dashboard`, etc.) unchanged in call sites.

### Additional AI Credit Purchases

- When included credits are exhausted, allow one-time top-up checkout.
- On successful top-up webhook, grant credits and update `profiles.ai_credits_balance`.
- All grants and deductions must be auditable in `credit_ledger`.

### Usage Deduction Compatibility

- Keep existing `deduct_user_credits` call path initially for compatibility.
- Extend deduction logic to also write to `credit_ledger` (or wrap with new RPC that does both).

## 11. Upgrade/Downgrade/Cancellation Flows

### Upgrade

- Allow immediate upgrade through checkout/portal.
- Stripe handles proration; webhook updates internal tier once subscription status is valid.

### Downgrade

- Default to end-of-period downgrade to avoid abrupt hard-limit regression mid-cycle.
- Internal tier flips when effective period starts (webhook-driven).

### Cancellation

- Support cancel-at-period-end from portal.
- On final termination, downgrade to Free/read-only behavior aligned with current app policy.

## 12. Frontend Integration Plan

1. Billing Page (`frontend/src/components/BillingPage.tsx`)
- Replace disabled upgrade buttons with calls to `/api/billing/checkout/subscription`.
- Hide Enterprise from self-serve list or render as Contact Sales.
- Add top-up purchase cards/actions.

2. Out of Credits Modal (`frontend/src/components/OutOfCreditsModal.tsx`)
- Replace placeholder alert with top-up checkout redirect flow.
- Secondary action to open billing portal.

3. Auth/User State
- Continue lightweight credit refresh (`/api/auth/credits`).
- Add billing summary fetch for richer status display where needed.

## 13. Security, Reliability, and Observability

### Security

- Verify webhook signatures on every request.
- Never trust client-supplied amount/price/currency.
- Use server-side price lookup by allowed internal identifiers.
- Keep Stripe secret keys server-side only.

### Reliability

- Idempotent webhook handling via `stripe_event_id` uniqueness.
- Retry-safe processing with explicit status transitions.
- Use deterministic reference IDs for credit grants to prevent duplicate allocation.

### Observability

- Structured logs: event type, stripe event id, user id, processing result.
- Metrics: webhook success/failure counts, checkout conversion, top-up count, failed payment count.

## 14. Rollout Plan

### Phase 0: Preparation

- Create Stripe products/prices in USD via MCP.
- Store mapping in Supabase.
- Add env vars and secrets for Stripe.

### Phase 1: Data Layer

- Apply billing migration (`9-stripe-billing.sql`).
- Add RLS + indexes + constraints.

### Phase 2: Backend APIs

- Implement billing router/service and webhook handler.
- Add customer linking and checkout session generation.

### Phase 3: Credits & Entitlements

- Integrate top-up grant flow from webhooks.
- Wire profile plan synchronization from subscription events.

### Phase 4: Frontend

- Enable real checkout/portal/top-up UX.
- Update Enterprise CTA to contact sales.

### Phase 5: Validation

- End-to-end test matrix:
  - new subscription purchase
  - upgrade
  - downgrade
  - cancellation
  - top-up purchase
  - payment failure
  - webhook replay/duplication

## 15. Acceptance Criteria

- Self-serve checkout works for Starter/Pro/Growth only.
- Enterprise is excluded from self-serve checkout.
- All active prices are USD.
- Subscription lifecycle is synchronized from Stripe webhooks to Supabase.
- Additional AI credits can be purchased and reflected in `ai_credits_balance`.
- Credit grants/deductions are auditable and idempotent.
- Existing plan-limit enforcement continues working without regression.
