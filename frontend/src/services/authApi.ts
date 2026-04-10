import type {
  AuthResponse,
  UserProfile,
  SignInRequest,
  SignUpRequest,
} from '../types';

const BASE_URL = import.meta.env.VITE_API_BASE_URL?.replace(/\/+$/, '') ?? 'http://localhost:8000';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? `Request failed: ${res.status} ${res.statusText}`);
  }

  return res.json() as Promise<T>;
}

export type SignUpResult =
  | { kind: 'session'; data: AuthResponse }
  | { kind: 'confirmation_required'; message: string };

/** POST /api/auth/signup — session immediately, or email confirmation required */
export async function signUp(body: SignUpRequest): Promise<SignUpResult> {
  const res = await fetch(`${BASE_URL}/api/auth/signup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: body.email,
      password: body.password,
      full_name: body.full_name ?? '',
    }),
  });
  const json = (await res.json().catch(() => ({}))) as Record<string, unknown>;
  if (!res.ok) {
    const detail = json.detail;
    throw new Error(
      typeof detail === 'string'
        ? detail
        : `Request failed: ${res.status} ${res.statusText}`
    );
  }
  if (json.requires_confirmation === true && typeof json.message === 'string') {
    return { kind: 'confirmation_required', message: json.message };
  }
  const data = json as unknown as AuthResponse;
  if (data.access_token && data.user) {
    return { kind: 'session', data };
  }
  throw new Error('Unexpected sign-up response from server');
}

function requestWithAuth<T>(path: string, accessToken: string, init?: RequestInit): Promise<T> {
  return request<T>(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${accessToken}`,
    },
  });
}

/** POST /api/auth/signin — returns tokens and user profile */
export function signIn(credentials: SignInRequest): Promise<AuthResponse> {
  return request<AuthResponse>('/api/auth/signin', {
    method: 'POST',
    body: JSON.stringify(credentials),
  });
}

/** GET /api/auth/me — validate token and return current user */
export function getMe(accessToken: string): Promise<UserProfile> {
  return requestWithAuth<UserProfile>('/api/auth/me', accessToken);
}

/** POST /api/auth/refresh — new access + refresh tokens (Supabase rotates refresh_token) */
export function refreshTokens(refreshToken: string): Promise<AuthResponse> {
  return request<AuthResponse>('/api/auth/refresh', {
    method: 'POST',
    body: JSON.stringify({ refresh_token: refreshToken }),
  });
}

/** DELETE /api/auth/account — permanently remove the user from Auth and profiles */
export async function deleteAccount(accessToken: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/api/auth/account`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { detail?: unknown };
    const detail = body.detail;
    throw new Error(
      typeof detail === 'string' ? detail : `Request failed: ${res.status} ${res.statusText}`,
    );
  }
}
