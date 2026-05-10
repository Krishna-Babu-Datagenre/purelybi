"""Billing routes for Stripe checkout, portal, and webhook flows."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Request

from fastapi_app.models.auth import UserProfile
from fastapi_app.models.billing import (
    BillingSummaryResponse,
    BillingWebhookAck,
    CheckoutSessionResponse,
    CheckoutSubscriptionRequest,
    CheckoutTopupRequest,
    PortalSessionResponse,
    SelfServePlansResponse,
)
from fastapi_app.services.billing_service import (
    create_portal_session,
    create_subscription_checkout,
    create_topup_checkout,
    get_billing_summary,
    get_self_serve_plans,
    process_webhook,
)
from fastapi_app.utils.auth_dep import get_current_user_dep

router = APIRouter(prefix="/api/billing", tags=["billing"])


@router.post("/checkout/subscription", response_model=CheckoutSessionResponse)
async def checkout_subscription(
    body: CheckoutSubscriptionRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    return create_subscription_checkout(user=user, body=body)


@router.post("/checkout/topup", response_model=CheckoutSessionResponse)
async def checkout_topup(
    body: CheckoutTopupRequest,
    user: UserProfile = Depends(get_current_user_dep),
):
    return create_topup_checkout(user=user, body=body)


@router.post("/portal-session", response_model=PortalSessionResponse)
async def portal_session(user: UserProfile = Depends(get_current_user_dep)):
    return create_portal_session(user=user)


@router.get("/summary", response_model=BillingSummaryResponse)
async def billing_summary(user: UserProfile = Depends(get_current_user_dep)):
    return get_billing_summary(user=user)


@router.get("/plans/self-serve", response_model=SelfServePlansResponse)
async def self_serve_plans(user: UserProfile = Depends(get_current_user_dep)):
    del user  # Auth-gated endpoint; response itself is public billing catalog.
    return get_self_serve_plans()


@router.post("/webhook", response_model=BillingWebhookAck)
async def stripe_webhook(
    request: Request,
    stripe_signature: str | None = Header(default=None, alias="Stripe-Signature"),
):
    payload = await request.body()
    duplicate = process_webhook(payload=payload, stripe_signature=stripe_signature)
    return BillingWebhookAck(received=True, duplicate=duplicate)
