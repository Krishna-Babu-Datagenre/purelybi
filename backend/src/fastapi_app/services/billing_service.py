"""Billing service for Stripe-backed checkout, portal, and webhook sync."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, status
from stripe import SignatureVerificationError, StripeClient, StripeError

from fastapi_app.models.auth import UserProfile
from fastapi_app.models.billing import (
    BillingSummaryResponse,
    CheckoutSessionResponse,
    CheckoutSubscriptionRequest,
    CheckoutTopupRequest,
    PortalSessionResponse,
    SelfServePlan,
    SelfServePlansResponse,
    TopupPack,
)
from fastapi_app.settings import (
    BILLING_CHECKOUT_CANCEL_URL,
    BILLING_CHECKOUT_SUCCESS_URL,
    BILLING_PORTAL_RETURN_URL,
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
)
from fastapi_app.utils.supabase_client import get_supabase_admin_client

logger = logging.getLogger(__name__)


def _stripe_client() -> StripeClient:
    if not STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stripe is not configured on the backend.",
        )
    return StripeClient(STRIPE_SECRET_KEY)


def _require_checkout_urls() -> tuple[str, str]:
    if not BILLING_CHECKOUT_SUCCESS_URL or not BILLING_CHECKOUT_CANCEL_URL:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Billing checkout URLs are not configured on the backend.",
        )
    return BILLING_CHECKOUT_SUCCESS_URL, BILLING_CHECKOUT_CANCEL_URL


def _require_portal_return_url() -> str:
    if not BILLING_PORTAL_RETURN_URL:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Billing portal return URL is not configured on the backend.",
        )
    return BILLING_PORTAL_RETURN_URL


def _obj_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    if hasattr(obj, key):
        return getattr(obj, key)
    getter = getattr(obj, "get", None)
    if callable(getter):
        return getter(key, default)
    return default


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    try:
        # Stripe timestamps are Unix seconds.
        ts = int(value)
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _lookup_plan_by_tier(plan_tier: str) -> dict[str, Any] | None:
    admin = get_supabase_admin_client()
    rows = (
        admin.table("subscription_plans")
        .select(
            "id, tier_name, max_data_sources, max_storage_mb, max_dashboards, included_ai_credits, min_sync_frequency_minutes"
        )
        .execute()
    ).data
    wanted = plan_tier.strip().lower()
    for row in rows:
        if str(row.get("tier_name", "")).strip().lower() == wanted:
            return row
    return None


def _lookup_price_map_for_plan(plan_id: str, billing_interval: str) -> dict[str, Any] | None:
    admin = get_supabase_admin_client()
    rows = (
        admin.table("plan_price_map")
        .select(
            "id, plan_id, price_lookup_key, stripe_price_id, stripe_product_id, amount_usd, currency, billing_interval, is_self_serve, is_topup, is_active"
        )
        .eq("plan_id", plan_id)
        .eq("billing_interval", billing_interval)
        .eq("is_topup", False)
        .eq("is_active", True)
        .limit(1)
        .execute()
    ).data
    return rows[0] if rows else None


def _lookup_topup_pack(pack_code: str) -> dict[str, Any] | None:
    admin = get_supabase_admin_client()
    rows = (
        admin.table("plan_price_map")
        .select(
            "id, price_lookup_key, stripe_price_id, stripe_product_id, amount_usd, currency, billing_interval, is_self_serve, is_topup, credits_granted, is_active"
        )
        .eq("price_lookup_key", pack_code)
        .eq("is_topup", True)
        .eq("is_active", True)
        .limit(1)
        .execute()
    ).data
    return rows[0] if rows else None


def _ensure_billing_customer(user: UserProfile) -> str:
    admin = get_supabase_admin_client()

    existing = (
        admin.table("billing_customers")
        .select("stripe_customer_id")
        .eq("user_id", user.id)
        .limit(1)
        .execute()
    ).data
    if existing:
        return str(existing[0]["stripe_customer_id"])

    client = _stripe_client()
    try:
        customer = client.v1.customers.create(
            params={
                "email": user.email,
                "name": user.full_name or "",
                "metadata": {"user_id": user.id},
            }
        )
    except StripeError as exc:
        logger.exception("Stripe customer creation failed for user %s", user.id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Stripe customer creation failed: {exc.user_message or str(exc)}",
        )

    customer_id = str(_obj_get(customer, "id", "")).strip()
    if not customer_id:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Stripe did not return a customer id.",
        )

    admin.table("billing_customers").insert(
        {
            "user_id": user.id,
            "stripe_customer_id": customer_id,
            "email_snapshot": user.email,
        }
    ).execute()
    return customer_id


def get_self_serve_plans() -> SelfServePlansResponse:
    admin = get_supabase_admin_client()
    mappings = (
        admin.table("plan_price_map")
        .select(
            "plan_id, price_lookup_key, stripe_price_id, amount_usd, currency, billing_interval, is_self_serve, is_topup, credits_granted, is_active"
        )
        .eq("is_active", True)
        .execute()
    ).data

    plan_ids = sorted(
        {
            str(row.get("plan_id"))
            for row in mappings
            if row.get("plan_id") is not None and row.get("is_topup") is False
        }
    )

    plans_by_id: dict[str, dict[str, Any]] = {}
    if plan_ids:
        plan_rows = (
            admin.table("subscription_plans")
            .select(
                "id, tier_name, max_data_sources, max_storage_mb, max_dashboards, included_ai_credits, min_sync_frequency_minutes"
            )
            .in_("id", plan_ids)
            .execute()
        ).data
        plans_by_id = {str(row["id"]): row for row in plan_rows}

    plan_out: list[SelfServePlan] = []
    topup_out: list[TopupPack] = []

    for row in mappings:
        currency = str(row.get("currency", "")).lower()
        if currency != "usd":
            continue

        if row.get("is_topup"):
            if not row.get("is_self_serve"):
                continue
            topup_out.append(
                TopupPack(
                    pack_code=str(row.get("price_lookup_key", "")),
                    amount_usd=_safe_float(row.get("amount_usd")),
                    currency=currency,
                    credits_granted=int(row.get("credits_granted") or 0),
                )
            )
            continue

        if not row.get("is_self_serve"):
            continue

        plan = plans_by_id.get(str(row.get("plan_id")))
        if not plan:
            continue
        if str(plan.get("tier_name", "")).strip().lower() == "enterprise":
            continue

        plan_out.append(
            SelfServePlan(
                plan_tier=str(plan["tier_name"]),
                billing_interval=str(row.get("billing_interval", "month")),
                price_lookup_key=str(row.get("price_lookup_key", "")),
                amount_usd=_safe_float(row.get("amount_usd")),
                currency=currency,
                included_ai_credits=int(plan.get("included_ai_credits") or 0),
                max_data_sources=int(plan.get("max_data_sources") or 0),
                max_storage_mb=int(plan.get("max_storage_mb") or 0),
                max_dashboards=int(plan.get("max_dashboards") or 0),
                min_sync_frequency_minutes=int(
                    plan.get("min_sync_frequency_minutes") or 0
                ),
            )
        )

    plan_out.sort(key=lambda x: (x.amount_usd, x.plan_tier))
    topup_out.sort(key=lambda x: x.amount_usd)

    return SelfServePlansResponse(plans=plan_out, topup_packs=topup_out)


def create_subscription_checkout(
    user: UserProfile, body: CheckoutSubscriptionRequest
) -> CheckoutSessionResponse:
    plan = _lookup_plan_by_tier(body.plan_tier)
    if not plan:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Unknown plan tier.",
        )

    if str(plan.get("tier_name", "")).strip().lower() == "enterprise":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Enterprise is not available via self-serve checkout.",
        )

    mapping = _lookup_price_map_for_plan(str(plan["id"]), body.billing_interval.value)
    if not mapping:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No active self-serve Stripe price is configured for this plan.",
        )

    if str(mapping.get("currency", "")).lower() != "usd":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Only USD prices are allowed for self-serve checkout.",
        )

    if not bool(mapping.get("is_self_serve")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The selected plan is not self-serve.",
        )

    success_url, cancel_url = _require_checkout_urls()
    customer_id = _ensure_billing_customer(user)

    client = _stripe_client()
    metadata = {
        "billing_type": "subscription",
        "user_id": user.id,
        "plan_tier": str(plan.get("tier_name", "")),
        "plan_id": str(plan.get("id", "")),
        "price_lookup_key": str(mapping.get("price_lookup_key", "")),
        "billing_interval": body.billing_interval.value,
    }
    try:
        session = client.v1.checkout.sessions.create(
            params={
                "mode": "subscription",
                "customer": customer_id,
                "client_reference_id": user.id,
                "success_url": success_url,
                "cancel_url": cancel_url,
                "line_items": [
                    {
                        "price": str(mapping.get("stripe_price_id")),
                        "quantity": 1,
                    }
                ],
                "metadata": metadata,
                "subscription_data": {"metadata": metadata},
            }
        )
    except StripeError as exc:
        logger.exception("Stripe checkout creation failed for user %s", user.id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Stripe checkout creation failed: {exc.user_message or str(exc)}",
        )

    url = str(_obj_get(session, "url", ""))
    session_id = str(_obj_get(session, "id", ""))
    if not url or not session_id:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Stripe checkout session is missing required fields.",
        )

    return CheckoutSessionResponse(checkout_url=url, session_id=session_id)


def create_topup_checkout(
    user: UserProfile, body: CheckoutTopupRequest
) -> CheckoutSessionResponse:
    mapping = _lookup_topup_pack(body.pack_code)
    if not mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Unknown top-up pack.",
        )

    if str(mapping.get("currency", "")).lower() != "usd":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Only USD prices are allowed for top-up checkout.",
        )

    success_url, cancel_url = _require_checkout_urls()
    customer_id = _ensure_billing_customer(user)

    client = _stripe_client()
    metadata = {
        "billing_type": "topup",
        "user_id": user.id,
        "price_lookup_key": str(mapping.get("price_lookup_key", "")),
        "stripe_price_id": str(mapping.get("stripe_price_id", "")),
    }
    try:
        session = client.v1.checkout.sessions.create(
            params={
                "mode": "payment",
                "customer": customer_id,
                "client_reference_id": user.id,
                "success_url": success_url,
                "cancel_url": cancel_url,
                "line_items": [
                    {
                        "price": str(mapping.get("stripe_price_id")),
                        "quantity": 1,
                    }
                ],
                "metadata": metadata,
            }
        )
    except StripeError as exc:
        logger.exception("Stripe top-up checkout creation failed for user %s", user.id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Stripe checkout creation failed: {exc.user_message or str(exc)}",
        )

    url = str(_obj_get(session, "url", ""))
    session_id = str(_obj_get(session, "id", ""))
    if not url or not session_id:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Stripe checkout session is missing required fields.",
        )

    admin = get_supabase_admin_client()
    admin.table("billing_credit_purchases").upsert(
        {
            "user_id": user.id,
            "stripe_checkout_session_id": session_id,
            "stripe_payment_intent_id": None,
            "stripe_invoice_id": None,
            "price_id": mapping.get("id"),
            "stripe_price_id": mapping.get("stripe_price_id"),
            "credits_granted": int(mapping.get("credits_granted") or 0),
            "amount_usd": _safe_float(mapping.get("amount_usd")),
            "status": "pending",
        },
        on_conflict="stripe_checkout_session_id",
    ).execute()

    return CheckoutSessionResponse(checkout_url=url, session_id=session_id)


def create_portal_session(user: UserProfile) -> PortalSessionResponse:
    customer_id = _ensure_billing_customer(user)
    return_url = _require_portal_return_url()
    client = _stripe_client()
    try:
        session = client.v1.billing_portal.sessions.create(
            params={
                "customer": customer_id,
                "return_url": return_url,
            }
        )
    except StripeError as exc:
        logger.exception("Stripe billing portal creation failed for user %s", user.id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Stripe portal creation failed: {exc.user_message or str(exc)}",
        )
    url = str(_obj_get(session, "url", ""))
    if not url:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Stripe portal session is missing URL.",
        )
    return PortalSessionResponse(portal_url=url)


def get_billing_summary(user: UserProfile) -> BillingSummaryResponse:
    admin = get_supabase_admin_client()

    profile_rows = (
        admin.table("profiles")
        .select("ai_credits_balance, subscription_tier(tier_name)")
        .eq("id", user.id)
        .limit(1)
        .execute()
    ).data

    if not profile_rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User profile not found.",
        )

    profile = profile_rows[0]
    plan_tier = "Free"
    tier_obj = profile.get("subscription_tier")
    if isinstance(tier_obj, dict) and tier_obj.get("tier_name"):
        plan_tier = str(tier_obj["tier_name"])

    sub_rows = (
        admin.table("billing_subscriptions")
        .select(
            "status, cancel_at_period_end, current_period_start, current_period_end, currency"
        )
        .eq("user_id", user.id)
        .order("current_period_end", desc=True)
        .limit(1)
        .execute()
    ).data

    latest_sub = sub_rows[0] if sub_rows else {}
    return BillingSummaryResponse(
        plan_tier=plan_tier,
        ai_credits_balance=int(profile.get("ai_credits_balance") or 0),
        subscription_status=latest_sub.get("status"),
        cancel_at_period_end=bool(latest_sub.get("cancel_at_period_end") or False),
        current_period_start=latest_sub.get("current_period_start"),
        current_period_end=latest_sub.get("current_period_end"),
        currency=str(latest_sub.get("currency") or "usd").lower(),
    )


def _user_id_from_customer_id(customer_id: str | None) -> str | None:
    if not customer_id:
        return None
    admin = get_supabase_admin_client()
    rows = (
        admin.table("billing_customers")
        .select("user_id")
        .eq("stripe_customer_id", customer_id)
        .limit(1)
        .execute()
    ).data
    return str(rows[0]["user_id"]) if rows else None


def _extract_user_id(obj: Any) -> str | None:
    metadata = _obj_get(obj, "metadata", {}) or {}
    user_id = None
    if isinstance(metadata, dict):
        user_id = metadata.get("user_id")
    if user_id:
        return str(user_id)
    return _user_id_from_customer_id(_obj_get(obj, "customer"))


def _record_webhook_received(event_id: str, event_type: str, payload: dict[str, Any]) -> bool:
    admin = get_supabase_admin_client()
    existing = (
        admin.table("stripe_webhook_events")
        .select("id")
        .eq("stripe_event_id", event_id)
        .limit(1)
        .execute()
    ).data
    if existing:
        return False

    admin.table("stripe_webhook_events").insert(
        {
            "stripe_event_id": event_id,
            "event_type": event_type,
            "payload": payload,
            "status": "received",
        }
    ).execute()
    return True


def _mark_webhook_status(event_id: str, status_text: str, error_message: str | None = None) -> None:
    admin = get_supabase_admin_client()
    payload: dict[str, Any] = {
        "status": status_text,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    if error_message:
        payload["error_message"] = error_message[:1200]
    admin.table("stripe_webhook_events").update(payload).eq("stripe_event_id", event_id).execute()


def _free_plan_id() -> str | None:
    admin = get_supabase_admin_client()
    rows = (
        admin.table("subscription_plans")
        .select("id")
        .eq("tier_name", "Free")
        .limit(1)
        .execute()
    ).data
    return str(rows[0]["id"]) if rows else None


def _sync_subscription_from_stripe(subscription_obj: Any) -> None:
    admin = get_supabase_admin_client()
    stripe_subscription_id = str(_obj_get(subscription_obj, "id", ""))
    if not stripe_subscription_id:
        return

    customer_id = str(_obj_get(subscription_obj, "customer", ""))
    user_id = _extract_user_id(subscription_obj)
    if not user_id:
        logger.warning("Could not resolve user_id for subscription %s", stripe_subscription_id)
        return

    items = _obj_get(subscription_obj, "items", {}) or {}
    item_data = _obj_get(items, "data", []) or []
    first_item = item_data[0] if item_data else {}
    price_obj = _obj_get(first_item, "price", {}) or {}
    stripe_price_id = str(_obj_get(price_obj, "id", ""))
    stripe_product_id = str(_obj_get(price_obj, "product", ""))
    status_value = str(_obj_get(subscription_obj, "status", "incomplete"))
    cancel_at_period_end = bool(_obj_get(subscription_obj, "cancel_at_period_end", False))

    mapping_rows = (
        admin.table("plan_price_map")
        .select("plan_id, stripe_price_id, stripe_product_id, currency")
        .eq("stripe_price_id", stripe_price_id)
        .eq("is_topup", False)
        .limit(1)
        .execute()
    ).data
    mapping = mapping_rows[0] if mapping_rows else None
    plan_id = mapping.get("plan_id") if mapping else None

    admin.table("billing_subscriptions").upsert(
        {
            "user_id": user_id,
            "stripe_subscription_id": stripe_subscription_id,
            "stripe_customer_id": customer_id,
            "stripe_price_id": stripe_price_id,
            "stripe_product_id": stripe_product_id,
            "status": status_value,
            "cancel_at_period_end": cancel_at_period_end,
            "current_period_start": _to_iso(_obj_get(subscription_obj, "current_period_start")),
            "current_period_end": _to_iso(_obj_get(subscription_obj, "current_period_end")),
            "currency": str((mapping or {}).get("currency") or "usd").lower(),
            "plan_id": plan_id,
            "latest_invoice_id": str(_obj_get(subscription_obj, "latest_invoice", "") or "") or None,
            "metadata": _obj_get(subscription_obj, "metadata", {}) or {},
        },
        on_conflict="stripe_subscription_id",
    ).execute()

    if customer_id:
        admin.table("billing_customers").upsert(
            {
                "user_id": user_id,
                "stripe_customer_id": customer_id,
                "email_snapshot": None,
            },
            on_conflict="user_id",
        ).execute()

    if status_value == "canceled":
        free_id = _free_plan_id()
        if free_id:
            admin.table("profiles").update({"subscription_tier": free_id}).eq("id", user_id).execute()
        return

    if plan_id:
        admin.table("profiles").update({"subscription_tier": plan_id}).eq("id", user_id).execute()


def _process_invoice_paid(invoice_obj: Any) -> None:
    admin = get_supabase_admin_client()
    invoice_id = str(_obj_get(invoice_obj, "id", ""))
    stripe_subscription_id = str(_obj_get(invoice_obj, "subscription", ""))
    if not invoice_id or not stripe_subscription_id:
        return

    sub_rows = (
        admin.table("billing_subscriptions")
        .select("user_id, plan_id")
        .eq("stripe_subscription_id", stripe_subscription_id)
        .limit(1)
        .execute()
    ).data
    if not sub_rows:
        return

    sub = sub_rows[0]
    plan_id = sub.get("plan_id")
    if not plan_id:
        return

    plan_rows = (
        admin.table("subscription_plans")
        .select("included_ai_credits")
        .eq("id", plan_id)
        .limit(1)
        .execute()
    ).data
    if not plan_rows:
        return

    included_credits = int(plan_rows[0].get("included_ai_credits") or 0)
    if included_credits <= 0:
        return

    admin.rpc(
        "grant_user_credits",
        {
            "p_user_id": sub["user_id"],
            "p_credits_delta": included_credits,
            "p_entry_type": "subscription_grant",
            "p_reference_type": "invoice",
            "p_reference_id": invoice_id,
            "p_metadata": {
                "stripe_subscription_id": stripe_subscription_id,
                "invoice_billing_reason": _obj_get(invoice_obj, "billing_reason"),
            },
        },
    ).execute()


def _process_checkout_completed(session_obj: Any) -> None:
    mode = str(_obj_get(session_obj, "mode", ""))
    if mode == "subscription":
        subscription_id = str(_obj_get(session_obj, "subscription", ""))
        if not subscription_id:
            return
        client = _stripe_client()
        try:
            subscription_obj = client.v1.subscriptions.retrieve(subscription_id)
        except StripeError:
            logger.exception("Failed to retrieve Stripe subscription %s", subscription_id)
            return
        _sync_subscription_from_stripe(subscription_obj)
        return

    if mode != "payment":
        return

    payment_status = str(_obj_get(session_obj, "payment_status", ""))
    if payment_status != "paid":
        logger.info(
            "Skipping top-up grant for checkout session %s with payment_status=%s",
            _obj_get(session_obj, "id", ""),
            payment_status,
        )
        return

    metadata = _obj_get(session_obj, "metadata", {}) or {}
    if not isinstance(metadata, dict) or metadata.get("billing_type") != "topup":
        return

    user_id = str(metadata.get("user_id") or _obj_get(session_obj, "client_reference_id") or "")
    if not user_id:
        return

    pack_code = str(metadata.get("price_lookup_key") or "")
    mapping = _lookup_topup_pack(pack_code)
    if not mapping:
        logger.warning("Top-up pack mapping not found for code %s", pack_code)
        return

    session_id = str(_obj_get(session_obj, "id", ""))
    payment_intent_id = str(_obj_get(session_obj, "payment_intent", "") or "") or None
    amount_total = int(_obj_get(session_obj, "amount_total", 0) or 0)
    amount_usd = round(amount_total / 100.0, 2)

    admin = get_supabase_admin_client()
    admin.table("billing_credit_purchases").upsert(
        {
            "user_id": user_id,
            "stripe_checkout_session_id": session_id,
            "stripe_payment_intent_id": payment_intent_id,
            "stripe_invoice_id": str(_obj_get(session_obj, "invoice", "") or "") or None,
            "price_id": mapping.get("id"),
            "stripe_price_id": mapping.get("stripe_price_id"),
            "credits_granted": int(mapping.get("credits_granted") or 0),
            "amount_usd": amount_usd,
            "status": "paid",
            "processed_at": datetime.now(timezone.utc).isoformat(),
        },
        on_conflict="stripe_checkout_session_id",
    ).execute()

    admin.rpc(
        "grant_user_credits",
        {
            "p_user_id": user_id,
            "p_credits_delta": int(mapping.get("credits_granted") or 0),
            "p_entry_type": "topup_grant",
            "p_reference_type": "checkout_session",
            "p_reference_id": session_id,
            "p_metadata": {
                "pack_code": pack_code,
                "stripe_price_id": mapping.get("stripe_price_id"),
                "payment_intent_id": payment_intent_id,
            },
        },
    ).execute()


def _process_charge_refunded(charge_obj: Any, stripe_event_id: str) -> None:
    payment_intent_id = str(_obj_get(charge_obj, "payment_intent", "") or "")
    if not payment_intent_id:
        return

    admin = get_supabase_admin_client()
    rows = (
        admin.table("billing_credit_purchases")
        .select("user_id, credits_granted, amount_usd, status")
        .eq("stripe_payment_intent_id", payment_intent_id)
        .limit(1)
        .execute()
    ).data
    if not rows:
        return

    purchase = rows[0]
    granted = int(purchase.get("credits_granted") or 0)
    if granted <= 0:
        return

    refunded_cents = int(_obj_get(charge_obj, "amount_refunded", 0) or 0)
    original_cents = int(round(_safe_float(purchase.get("amount_usd")) * 100))
    if original_cents <= 0 or refunded_cents <= 0:
        return

    target_revoke = granted if refunded_cents >= original_cents else int(
        round(granted * (refunded_cents / original_cents))
    )
    if target_revoke <= 0:
        return

    # Stripe emits cumulative refunded amounts across multiple refund events.
    prior_reversals = (
        admin.table("credit_ledger")
        .select("credits_delta, metadata")
        .eq("user_id", purchase["user_id"])
        .eq("entry_type", "reversal")
        .limit(1000)
        .execute()
    ).data
    already_revoked = 0
    for entry in prior_reversals:
        metadata = entry.get("metadata") if isinstance(entry, dict) else None
        if isinstance(metadata, dict) and str(metadata.get("payment_intent_id", "")) == payment_intent_id:
            already_revoked += abs(int(entry.get("credits_delta") or 0))

    revoke = max(0, target_revoke - already_revoked)
    if revoke <= 0:
        return

    admin.rpc(
        "grant_user_credits",
        {
            "p_user_id": purchase["user_id"],
            "p_credits_delta": -revoke,
            "p_entry_type": "reversal",
            "p_reference_type": "stripe_event",
            "p_reference_id": stripe_event_id,
            "p_metadata": {
                "payment_intent_id": payment_intent_id,
                "refunded_cents": refunded_cents,
                "original_cents": original_cents,
            },
        },
    ).execute()

    if refunded_cents >= original_cents:
        admin.table("billing_credit_purchases").update({"status": "refunded"}).eq(
            "stripe_payment_intent_id", payment_intent_id
        ).execute()


def process_webhook(payload: bytes, stripe_signature: str | None) -> bool:
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stripe webhook secret is not configured on the backend.",
        )
    if not stripe_signature:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing Stripe-Signature header.",
        )

    client = _stripe_client()
    try:
        event = client.construct_event(payload, stripe_signature, STRIPE_WEBHOOK_SECRET)
    except SignatureVerificationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid Stripe signature: {exc}",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid webhook payload: {exc}",
        )

    event_id = str(_obj_get(event, "id", ""))
    event_type = str(_obj_get(event, "type", ""))
    if not event_id or not event_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Stripe event is missing id or type.",
        )

    if isinstance(event, dict):
        event_payload = event
    elif hasattr(event, "to_dict_recursive"):
        event_payload = event.to_dict_recursive()
    elif hasattr(event, "to_dict"):
        event_payload = event.to_dict()
    else:
        try:
            event_payload = json.loads(str(event))
        except Exception:
            event_payload = {}
    inserted = _record_webhook_received(event_id, event_type, event_payload)
    if not inserted:
        return True

    obj = _obj_get(_obj_get(event, "data", {}), "object", {})
    try:
        if event_type == "checkout.session.completed":
            _process_checkout_completed(obj)
            _mark_webhook_status(event_id, "processed")
            return False

        if event_type in (
            "customer.subscription.created",
            "customer.subscription.updated",
            "customer.subscription.deleted",
        ):
            _sync_subscription_from_stripe(obj)
            _mark_webhook_status(event_id, "processed")
            return False

        if event_type == "invoice.paid":
            _process_invoice_paid(obj)
            _mark_webhook_status(event_id, "processed")
            return False

        if event_type == "invoice.payment_failed":
            _mark_webhook_status(event_id, "processed")
            return False

        if event_type == "charge.refunded":
            _process_charge_refunded(obj, event_id)
            _mark_webhook_status(event_id, "processed")
            return False

        _mark_webhook_status(event_id, "ignored")
        return False
    except Exception as exc:
        _mark_webhook_status(event_id, "failed", str(exc))
        logger.exception("Stripe webhook processing failed for event=%s type=%s", event_id, event_type)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Webhook processing failed.",
        )
