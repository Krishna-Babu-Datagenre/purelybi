/**
 * Keep Supabase access tokens fresh using the long-lived refresh_token.
 * Access JWTs expire quickly; onboarding/chat flows can outlast them.
 */

import { useAuthStore } from '../store/useAuthStore';
import type { AuthResponse } from '../types';
import { refreshTokens } from './authApi';

const BASE_URL = import.meta.env.VITE_API_BASE_URL?.replace(/\/+$/, '') ?? 'http://localhost:8000';

/** Seconds before access token expiry when we proactively refresh (buffer for clock skew). */
const REFRESH_BUFFER_SEC = 120;

function getJwtExpMs(accessToken: string): number | null {
  try {
    const parts = accessToken.split('.');
    if (parts.length < 2) return null;
    let b64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const pad = b64.length % 4;
    if (pad) b64 += '='.repeat(4 - pad);
    const payload = JSON.parse(atob(b64)) as { exp?: number };
    return typeof payload.exp === 'number' ? payload.exp * 1000 : null;
  } catch {
    return null;
  }
}

function shouldRefreshAccessToken(accessToken: string | null): boolean {
  if (!accessToken) return false;
  const exp = getJwtExpMs(accessToken);
  if (exp === null) return true;
  return Date.now() >= exp - REFRESH_BUFFER_SEC * 1000;
}

let refreshInFlight: Promise<boolean> | null = null;

/**
 * Call POST /api/auth/refresh once; concurrent callers share the same promise.
 * Returns true if the store now has a new access token.
 */
export async function runTokenRefresh(): Promise<boolean> {
  const rt = useAuthStore.getState().refreshToken;
  if (!rt) return false;

  if (!refreshInFlight) {
    refreshInFlight = (async () => {
      try {
        const data: AuthResponse = await refreshTokens(rt);
        useAuthStore.getState().setAuth(data.access_token, data.user, data.refresh_token);
        return true;
      } catch {
        return false;
      } finally {
        refreshInFlight = null;
      }
    })();
  }
  return refreshInFlight;
}

/** If the access token is missing or expires within the buffer, refresh using refresh_token. */
export async function ensureAccessTokenFresh(): Promise<void> {
  const { accessToken, refreshToken } = useAuthStore.getState();
  if (!refreshToken) return;
  if (!shouldRefreshAccessToken(accessToken)) return;
  await runTokenRefresh();
}

/** Current Bearer headers (no refresh — pair with ensureAccessTokenFresh when needed). */
export function authHeadersFromStore(): Record<string, string> {
  const token = useAuthStore.getState().accessToken;
  const h: Record<string, string> = {};
  if (token) h.Authorization = `Bearer ${token}`;
  return h;
}

/** Refresh if needed, then return Authorization headers. */
export async function getAuthHeaders(): Promise<Record<string, string>> {
  await ensureAccessTokenFresh();
  return authHeadersFromStore();
}

/**
 * Authenticated fetch: refresh if needed; on 401 retry once after a forced refresh.
 */
export async function fetchWithAuthRetry(
  path: string,
  init?: RequestInit,
): Promise<Response> {
  await ensureAccessTokenFresh();
  
  const headers: Record<string, string> = {
    ...authHeadersFromStore(),
    ...(init?.headers as Record<string, string>),
  };
  
  if (!(init?.body instanceof FormData) && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }

  let res = await fetch(`${BASE_URL}${path}`, { ...init, headers });

  if (res.status === 401 && useAuthStore.getState().refreshToken) {
    const ok = await runTokenRefresh();
    if (ok) {
      const headers2: Record<string, string> = {
        ...authHeadersFromStore(),
        ...(init?.headers as Record<string, string>),
      };
      
      if (!(init?.body instanceof FormData) && !headers2['Content-Type']) {
        headers2['Content-Type'] = 'application/json';
      }
      
      res = await fetch(`${BASE_URL}${path}`, { ...init, headers: headers2 });
    }
  }
  return res;
}
