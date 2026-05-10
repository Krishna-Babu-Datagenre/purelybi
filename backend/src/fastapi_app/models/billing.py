"""Pydantic models for Stripe billing endpoints."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class BillingInterval(str, Enum):
    month = "month"
    year = "year"


class CheckoutSubscriptionRequest(BaseModel):
    plan_tier: str = Field(..., min_length=1, max_length=64)
    billing_interval: BillingInterval = BillingInterval.month


class CheckoutTopupRequest(BaseModel):
    pack_code: str = Field(..., min_length=1, max_length=128)


class CheckoutSessionResponse(BaseModel):
    checkout_url: str
    session_id: str


class PortalSessionResponse(BaseModel):
    portal_url: str


class SelfServePlan(BaseModel):
    plan_tier: str
    billing_interval: BillingInterval
    price_lookup_key: str
    amount_usd: float
    currency: str
    included_ai_credits: int
    max_data_sources: int
    max_storage_mb: int
    max_dashboards: int
    min_sync_frequency_minutes: int


class TopupPack(BaseModel):
    pack_code: str
    amount_usd: float
    currency: str
    credits_granted: int


class SelfServePlansResponse(BaseModel):
    plans: list[SelfServePlan]
    topup_packs: list[TopupPack]


class BillingSummaryResponse(BaseModel):
    plan_tier: str
    ai_credits_balance: int
    subscription_status: str | None = None
    cancel_at_period_end: bool = False
    current_period_start: str | None = None
    current_period_end: str | None = None
    currency: str = "usd"


class BillingWebhookAck(BaseModel):
    received: bool = True
    duplicate: bool = False
