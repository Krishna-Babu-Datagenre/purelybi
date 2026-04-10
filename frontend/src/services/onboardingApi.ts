/// <reference types="vite/client" />
import { getAuthHeaders, runTokenRefresh } from './authSession';

const BASE_URL = import.meta.env.VITE_API_BASE_URL?.replace(/\/+$/, '') ?? 'http://localhost:8000';

/** One auth option after normalization (always has non-empty label + auth_type for API payloads). */
export type AuthOptionNormalized = {
  label: string;
  description?: string;
  auth_type: string;
};

function pickFirstNonEmpty(...vals: unknown[]): string {
  for (const v of vals) {
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return '';
}

/**
 * Normalize auth options from the agent. LLMs often send only `description`, or `title` instead of `label`.
 * Without this, `JSON.stringify` drops `undefined` fields and the API receives `auth_choice: {}`.
 */
export function normalizeAuthOptionsPayload(options: unknown): AuthOptionNormalized[] {
  if (!Array.isArray(options)) return [];
  return options.map((opt, i) => {
    const rec = opt && typeof opt === 'object' ? (opt as Record<string, unknown>) : {};
    const label =
      pickFirstNonEmpty(
        rec.label,
        rec.title,
        rec.name,
        rec.value,
        rec.key,
      ) ||
      pickFirstNonEmpty(rec.description) ||
      `Option ${i + 1}`;
    const auth_type = pickFirstNonEmpty(rec.auth_type, rec.type, rec.key, label) || label;
    const descRaw = pickFirstNonEmpty(rec.description);
    const description = descRaw && descRaw !== label ? descRaw : undefined;
    return { label, auth_type, ...(description ? { description } : {}) };
  });
}

/** One select option after normalization (avoids rendering raw `{ value, label }` objects as React children). */
export type SelectOptionNormalized = { value: string; label: string };

/**
 * Coerce agent/LLM select options into `{ value, label }` (strings, `{ value, label }`, or similar).
 */
export function normalizeSelectOptions(raw: unknown): SelectOptionNormalized[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((item, i) => {
    if (typeof item === 'string' || typeof item === 'number' || typeof item === 'boolean') {
      const s = String(item);
      return { value: s, label: s };
    }
    if (item && typeof item === 'object') {
      const r = item as Record<string, unknown>;
      const value =
        pickFirstNonEmpty(r.value, r.id, r.key, r.name) || `option_${i + 1}`;
      const label =
        pickFirstNonEmpty(r.label, r.title, r.name, r.value, value) || value;
      return { value, label };
    }
    return { value: `option_${i + 1}`, label: String(item) };
  });
}

/** Field row after normalization (always has non-empty ``key`` for API payloads). */
export type InputFieldNormalized = {
  key: string;
  label?: string;
  type?: string;
  required?: boolean;
  description?: string;
  default?: unknown;
  placeholder?: string;
  options?: SelectOptionNormalized[];
};

/**
 * Normalize input field definitions from the agent. LLMs often send ``name`` instead of ``key``.
 * Without this, the client omits ``key`` when stringifying and POST /api/onboarding/chat returns 422.
 */
export function normalizeInputFieldsPayload(fields: unknown): InputFieldNormalized[] {
  if (!Array.isArray(fields)) return [];
  return fields.map((raw, i) => {
    const rec = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
    const { options: rawOptions, ...recRest } = rec;
    const direct = pickFirstNonEmpty(
      rec.key,
      rec.name,
      rec.id,
      rec.field,
      rec.property,
    );
    const labelRaw = pickFirstNonEmpty(rec.label, rec.title);
    const slugFromLabel = labelRaw
      ? labelRaw
          .toLowerCase()
          .trim()
          .replace(/\s+/g, '_')
          .replace(/[^a-z0-9_]/g, '')
      : '';
    const key = direct || slugFromLabel || `field_${i + 1}`;
    const label = pickFirstNonEmpty(rec.label, rec.title, rec.name, key) || undefined;
    const ftype = pickFirstNonEmpty(rec.type) || 'text';
    const normalizedSelectOptions =
      ftype === 'select' ? normalizeSelectOptions(rawOptions) : undefined;
    return {
      ...recRest,
      key,
      ...(label ? { label } : {}),
      type: ftype,
      ...(ftype === 'select' && normalizedSelectOptions && normalizedSelectOptions.length > 0
        ? { options: normalizedSelectOptions }
        : {}),
    } as InputFieldNormalized;
  });
}

/** Dynamic UI payload from the onboarding agent (mirrors Streamlit ``pending_ui``). */
export type OnboardingUiBlock =
  | { type: 'auth_options'; options: AuthOptionNormalized[] }
  | {
      type: 'input_fields';
      fields: {
        key: string;
        label?: string;
        type?: string;
        required?: boolean;
        description?: string;
        default?: unknown;
        placeholder?: string;
        options?: SelectOptionNormalized[];
      }[];
    }
  | {
      type: 'stream_selector';
      streams: ({ name: string; accessible?: boolean; selected?: boolean } | string)[];
    }
  | { type: 'oauth_button'; url: string; provider: string; state?: string };

export type OnboardingSSEEvent =
  | 'start'
  | 'token'
  | 'tool_call_start'
  | 'tool_call_args'
  | 'tool_result'
  | 'ui_block'
  | 'end'
  | 'error';

export type OnboardingSSEHandler = (event: OnboardingSSEEvent, data: unknown) => void;

export interface FormFieldPayload {
  key: string;
  type?: string;
  value: unknown;
}

export interface OnboardingChatRequest {
  message: string;
  thread_id: string;
  catalog_connector_id?: string;
  form_fields?: FormFieldPayload[];
  auth_choice?: { label: string; auth_type?: string };
  stream_names?: string[];
}

export async function streamOnboardingChat(
  body: OnboardingChatRequest,
  onEvent: OnboardingSSEHandler,
): Promise<void> {
  let auth = await getAuthHeaders();
  let res = await fetch(`${BASE_URL}/api/onboarding/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...auth },
    body: JSON.stringify(body),
  });

  if (!res.ok && res.status === 401) {
    const refreshed = await runTokenRefresh();
    if (refreshed) {
      auth = await getAuthHeaders();
      res = await fetch(`${BASE_URL}/api/onboarding/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...auth },
        body: JSON.stringify(body),
      });
    }
  }

  if (!res.ok) {
    const errBody = await res.json().catch(() => ({}));
    const detail = (errBody as { detail?: string }).detail ?? res.statusText;
    onEvent('error', { detail });
    return;
  }

  const reader = res.body?.getReader();
  if (!reader) {
    onEvent('error', { detail: 'No response body' });
    return;
  }

  const decoder = new TextDecoder();
  let buffer = '';
  let eventType: OnboardingSSEEvent = 'token';

  function processOneFrame(): boolean {
    const normalized = buffer.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
    const lines = normalized.split('\n');
    let currentEvent: OnboardingSSEEvent = eventType;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (line.startsWith('event: ')) {
        const raw = line.slice(7).trim().toLowerCase();
        if (raw.length > 0) currentEvent = raw as OnboardingSSEEvent;
      } else if (line.startsWith('data: ')) {
        const raw = line.slice(6).trim();
        if (raw === '[DONE]' || raw === '') continue;
        try {
          const data = JSON.parse(raw) as unknown;
          // Use the explicit SSE event line only (same contract as POST /api/chat — see chat.md).
          onEvent(currentEvent, data);
          eventType = currentEvent;
          const consumed = lines.slice(0, i + 1).join('\n').length;
          buffer = normalized.length > consumed ? normalized.slice(consumed) : '';
          return true;
        } catch {
          // skip malformed
        }
      }
    }
    return false;
  }

  const yieldToUI = () => new Promise<void>((r) => requestAnimationFrame(() => r()));

  while (true) {
    if (processOneFrame()) {
      await yieldToUI();
      continue;
    }

    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
  }

  while (processOneFrame()) {
    await yieldToUI();
  }
}

export interface OAuthResultPayload {
  agent_message: string;
  display_message: string;
  oauth_meta?: Record<string, unknown> | null;
  suggested_config_fragment?: Record<string, unknown> | null;
}

export async function fetchOnboardingOAuthResult(state: string): Promise<OAuthResultPayload> {
  let auth = await getAuthHeaders();
  let res = await fetch(
    `${BASE_URL}/api/onboarding/oauth/result?${new URLSearchParams({ state })}`,
    { headers: auth },
  );
  if (!res.ok && res.status === 401) {
    const refreshed = await runTokenRefresh();
    if (refreshed) {
      auth = await getAuthHeaders();
      res = await fetch(
        `${BASE_URL}/api/onboarding/oauth/result?${new URLSearchParams({ state })}`,
        { headers: auth },
      );
    }
  }
  if (!res.ok) {
    const errBody = await res.json().catch(() => ({}));
    throw new Error((errBody as { detail?: string }).detail ?? res.statusText);
  }
  return res.json() as Promise<OAuthResultPayload>;
}
