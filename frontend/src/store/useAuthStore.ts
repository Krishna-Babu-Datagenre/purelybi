import { create } from 'zustand';
import type { UserProfile } from '../types';
import { getMe, refreshTokens } from '../services/authApi';

const STORAGE_KEY = 'bi-agent-auth';

interface StoredAuth {
  accessToken: string;
  /** Supabase refresh token — required to rotate short-lived JWTs without re-login */
  refreshToken: string;
  user: UserProfile;
}

function loadStored(): StoredAuth | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as StoredAuth & { refreshToken?: string };
    if (parsed.accessToken && parsed.user?.id) {
      return {
        accessToken: parsed.accessToken,
        refreshToken: parsed.refreshToken ?? '',
        user: parsed.user,
      };
    }
  } catch {
    // ignore
  }
  return null;
}

function saveStored(auth: StoredAuth | null): void {
  if (auth) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(auth));
  } else {
    localStorage.removeItem(STORAGE_KEY);
  }
}

interface AuthState {
  accessToken: string | null;
  refreshToken: string | null;
  user: UserProfile | null;
  /** True while validating stored token on app load */
  validating: boolean;

  setAuth: (accessToken: string, user: UserProfile, refreshToken?: string) => void;
  logout: () => void;
  /** Validate stored token (e.g. on app load). Clears auth if invalid. */
  validateStoredToken: () => Promise<boolean>;
}

export const useAuthStore = create<AuthState>((set) => ({
  accessToken: null,
  refreshToken: null,
  user: null,
  validating: false,

  setAuth: (accessToken, user, refreshToken = '') => {
    saveStored({ accessToken, user, refreshToken });
    set({ accessToken, user, refreshToken: refreshToken || null });
  },

  logout: () => {
    saveStored(null);
    set({ accessToken: null, refreshToken: null, user: null });
    void import('../services/backendClient')
      .then((m) => m.clearMaxDataDateCache())
      .catch(() => {});
  },

  validateStoredToken: async () => {
    const stored = loadStored();
    if (!stored) {
      set({ accessToken: null, refreshToken: null, user: null });
      return false;
    }
    set({ validating: true });
    try {
      const user = await getMe(stored.accessToken);
      set({
        accessToken: stored.accessToken,
        refreshToken: stored.refreshToken || null,
        user,
        validating: false,
      });
      saveStored({
        accessToken: stored.accessToken,
        refreshToken: stored.refreshToken,
        user,
      });
      return true;
    } catch {
      if (stored.refreshToken) {
        try {
          const session = await refreshTokens(stored.refreshToken);
          const user = await getMe(session.access_token);
          saveStored({
            accessToken: session.access_token,
            refreshToken: session.refresh_token,
            user,
          });
          set({
            accessToken: session.access_token,
            refreshToken: session.refresh_token,
            user,
            validating: false,
          });
          return true;
        } catch {
          // fall through — clear session
        }
      }
      saveStored(null);
      set({
        accessToken: null,
        refreshToken: null,
        user: null,
        validating: false,
      });
      return false;
    }
  },
}));

/** Call once on app init to restore session from localStorage */
export function initAuthFromStorage(): void {
  const stored = loadStored();
  if (stored) {
    useAuthStore.setState({
      accessToken: stored.accessToken,
      refreshToken: stored.refreshToken || null,
      user: stored.user,
    });
  }
}
