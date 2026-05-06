# Subscription & Report Sharing Implementation Plan

This document outlines the step-by-step implementation plan for the Purely BI subscription model. It covers centralized limit enforcement, precise AI token-to-credit tracking, and the role-based report sharing architecture.

## Phase 1: Database Schema Updates (Supabase SQL)

### 1.1 Subscription Plans & User Profiles
We will centralize the plan configurations in a new table so they can be modified without deploying code.
* **`subscription_plans`**: Holds plan definitions and limits.
  * Columns: `id`, `tier_name` (Free, Starter, Pro, Growth, Enterprise), `max_data_sources`, `max_storage_mb`, `max_dashboards`, `included_ai_credits`, `min_sync_frequency_minutes`.
* **`profiles` (Modification)**: 
  * Add columns: `subscription_tier` (FK to `subscription_plans`), `ai_credits_balance` (INTEGER), and `trial_ends_at` (TIMESTAMPTZ).

### 1.2 Precise AI Credit Tracking
To prevent massive margin loss from agentic loops, we will track exact token usage instead of flat per-message rates.
* **`ai_usage_logs`**: An audit trail of every agent session.
  * Columns: `id`, `user_id`, `session_id`, `input_tokens`, `output_tokens`, `cost_usd`, `credits_deducted`, `created_at`.
* **Atomic RPC `deduct_user_credits`**: A Postgres Stored Procedure to safely deduct credits from `profiles.ai_credits_balance` and insert the log into `ai_usage_logs` within a single transaction, preventing race conditions.

### 1.3 Report Sharing Architecture
To support sharing with varying permissions (Read-only vs. Read/Write) based on subscription tier:
* **`dashboard_shares`**: Tracks who has access to which report.
  * Columns: `id`, `dashboard_id` (FK), `shared_by_user_id` (FK), `shared_with_email` (or `shared_with_user_id`), `permission_level` (Enum: `'read'`, `'edit'`).
* **RLS Policies Update**:
  * Update `dashboards` and `widgets` Row Level Security (RLS) to permit `SELECT` if the user's ID exists in `dashboard_shares` for that dashboard.
  * Permit `UPDATE` if the user is in `dashboard_shares` with `permission_level = 'edit'`.

## Phase 2: Backend Services & API

### 2.1 Subscription Service (`subscription_service.py`)
A core service responsible for:
* Fetching the user's active plan and limits.
* Validating feature creation (e.g., `can_create_dashboard()`, `can_add_source()`).
* Handling the 7-day "Read-Only" Free Trial logic.

### 2.2 LangChain Token Cost Callback Handler
* Implement a custom `AsyncCallbackHandler` attached to the `ChatAnthropic` agent.
* The handler intercepts every LLM invocation within the LangGraph/ReAct loop, accumulating total `input_tokens` and `output_tokens`.
* **Conversion Logic**: At the end of the streaming response, it calculates the exact API cost (e.g., $0.05) and converts it to fractional AI Credits (e.g., 1 AI Credit = $0.01). It then triggers the `deduct_user_credits` RPC.

### 2.3 Sharing Enforcement
When a user attempts to share a report, the API will verify their subscription tier:
* **Free / Starter**: Returns `403 Forbidden` (Sharing not allowed).
* **Pro**: Forces `permission_level = 'read'` (Read-only sharing).
* **Growth / Enterprise**: Allows `permission_level` to be either `'read'` or `'edit'`.

### 2.4 Pre-Flight Middleware
* Add checks to the chat and onboarding endpoints to verify `ai_credits_balance > 0` before launching the agent. If insufficient, return a `402 Payment Required` to prompt a top-up on the frontend.

## Phase 3: Frontend Integration

### 3.1 Context & Global UI
* Expand `AuthContext` to fetch and store the `SubscriptionPlan`, `ai_credits_balance`, and `trial_ends_at`.
* Display a global "Trial Expired" banner if the user is on the Free tier and > 7 days have elapsed.
* Display a realtime AI Credit Counter in the sidebar/header.

### 3.2 Action Guards & Modals
* Disable "Add Data Source", "New Dashboard", and other creation buttons if the user is at their plan limit, showing an "Upgrade to [Next Tier]" tooltip.
* If a chat request returns `402 Payment Required`, automatically open an "Out of Credits - Buy Top-up" modal.

### 3.3 Report Share Dialog
* Build a "Share Dashboard" modal in the UI.
* For Pro users, the permission dropdown is disabled (locked to "Viewer").
* For Growth/Enterprise users, they can select "Viewer" or "Editor" when inviting emails.
* Show a list of active shares with the ability to revoke access.

## Next Steps
Once this architecture is reviewed, we will begin execution systematically:
1. Write the Supabase migration script for the new tables and RLS policies.
2. Implement the Backend `SubscriptionService` and LangChain Token Handler.
3. Apply the backend limit checks to dashboards, connectors, and sharing.
4. Finalize the frontend UI states and modals.
