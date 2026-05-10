import type {
  BillingInterval,
  BillingPortalSessionResponse,
  BillingSelfServePlansResponse,
  BillingSummary,
  CheckoutSessionResponse,
} from '../types';
import { fetchWithAuthRetry } from './authSession';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetchWithAuthRetry(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers as Record<string, string>),
    },
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail =
      typeof body.detail === 'string'
        ? body.detail
        : body.detail?.msg ?? body.message;
    throw new Error(detail ?? `Request failed: ${res.status} ${res.statusText}`);
  }

  return res.json() as Promise<T>;
}

export function fetchSelfServePlans(): Promise<BillingSelfServePlansResponse> {
  return request<BillingSelfServePlansResponse>('/api/billing/plans/self-serve');
}

export function fetchBillingSummary(): Promise<BillingSummary> {
  return request<BillingSummary>('/api/billing/summary');
}

export function createSubscriptionCheckout(
  planTier: string,
  billingInterval: BillingInterval,
): Promise<CheckoutSessionResponse> {
  return request<CheckoutSessionResponse>('/api/billing/checkout/subscription', {
    method: 'POST',
    body: JSON.stringify({
      plan_tier: planTier,
      billing_interval: billingInterval,
    }),
  });
}

export function createTopupCheckout(packCode: string): Promise<CheckoutSessionResponse> {
  return request<CheckoutSessionResponse>('/api/billing/checkout/topup', {
    method: 'POST',
    body: JSON.stringify({ pack_code: packCode }),
  });
}

export function createBillingPortalSession(): Promise<BillingPortalSessionResponse> {
  return request<BillingPortalSessionResponse>('/api/billing/portal-session', {
    method: 'POST',
  });
}
