/// <reference types="vite/client" />
import type {
  ChatMessage,
  ChatSendRequest,
  SSEData,
  SSEErrorData,
} from '../types';
import { getAuthHeaders, runTokenRefresh } from './authSession';

const BASE_URL = import.meta.env.VITE_API_BASE_URL?.replace(/\/+$/, '') ?? 'http://localhost:8000';

export type SSEEventType =
  | 'start'
  | 'token'
  | 'tool_call_start'
  | 'tool_call_args'
  | 'tool_result'
  | 'chart'
  | 'end'
  | 'error';

export type SSEHandler = (event: SSEEventType, data: SSEData) => void;

/**
 * POST /api/chat — send message and consume SSE stream.
 * Calls handler(event, data) for each SSE event until stream ends or error.
 *
 * If responses still appear only at the end, the backend may be buffering:
 * ensure each SSE event is flushed (e.g. yield in the generator / flush after each write).
 */
export async function streamChat(
  body: ChatSendRequest,
  onEvent: SSEHandler
): Promise<void> {
  let auth = await getAuthHeaders();
  let res = await fetch(`${BASE_URL}/api/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...auth },
    body: JSON.stringify(body),
  });

  if (!res.ok && res.status === 401) {
    const refreshed = await runTokenRefresh();
    if (refreshed) {
      auth = await getAuthHeaders();
      res = await fetch(`${BASE_URL}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...auth },
        body: JSON.stringify(body),
      });
    }
  }

  if (!res.ok) {
    const errBody = await res.json().catch(() => ({}));
    const detail = (errBody as { detail?: string }).detail ?? res.statusText;
    onEvent('error', { detail } as SSEErrorData);
    return;
  }

  const reader = res.body?.getReader();
  if (!reader) {
    onEvent('error', { detail: 'No response body' } as SSEErrorData);
    return;
  }

  const decoder = new TextDecoder();
  let buffer = '';
  let eventType: SSEEventType = 'token';

  function processOneFrame(): boolean {
    const normalized = buffer.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
    const lines = normalized.split('\n');
    let currentEvent: SSEEventType = eventType;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (line.startsWith('event: ')) {
        const raw = line.slice(7).trim().toLowerCase();
        if (raw.length > 0) currentEvent = raw as SSEEventType;
      } else if (line.startsWith('data: ')) {
        const raw = line.slice(6).trim();
        if (raw === '[DONE]' || raw === '') continue;
        try {
          const data = JSON.parse(raw) as SSEData;
          eventType = currentEvent;
          const effectiveEvent: SSEEventType =
            eventType === 'token' || (typeof data === 'object' && data !== null && 'content' in data && !('tool_call_id' in data))
              ? 'token'
              : eventType;
          onEvent(effectiveEvent, data);
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

  // Yield to UI after every event so tool calls and tokens paint incrementally
  // (avoids React batching all state updates when a burst of events arrives in one read).
  const yieldToUI = () =>
    new Promise<void>((r) => requestAnimationFrame(() => r()));

  while (true) {
    if (processOneFrame()) {
      await yieldToUI();
      continue;
    }

    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
  }

  // Process any remaining frames (e.g. final "end" event) left in buffer
  while (processOneFrame()) {
    await yieldToUI();
  }
}

/** GET /api/chat/history/{session_id} — conversation history */
export async function getChatHistory(
  sessionId: string,
  params?: { agent_type?: string; llm?: string; database?: string }
): Promise<ChatMessage[]> {
  const search = new URLSearchParams(params as Record<string, string>).toString();
  const url = `${BASE_URL}/api/chat/history/${encodeURIComponent(sessionId)}${search ? `?${search}` : ''}`;
  let auth = await getAuthHeaders();
  let res = await fetch(url, { headers: auth });
  if (!res.ok && res.status === 401) {
    const refreshed = await runTokenRefresh();
    if (refreshed) {
      auth = await getAuthHeaders();
      res = await fetch(url, { headers: auth });
    }
  }
  if (!res.ok) {
    const errBody = await res.json().catch(() => ({}));
    throw new Error((errBody as { detail?: string }).detail ?? res.statusText);
  }
  return res.json() as Promise<ChatMessage[]>;
}

/** DELETE /api/chat/history/{session_id} — clear conversation */
export async function clearChatHistory(sessionId: string): Promise<{ status: string; session_id: string }> {
  let auth = await getAuthHeaders();
  let res = await fetch(`${BASE_URL}/api/chat/history/${encodeURIComponent(sessionId)}`, {
    method: 'DELETE',
    headers: auth,
  });
  if (!res.ok && res.status === 401) {
    const refreshed = await runTokenRefresh();
    if (refreshed) {
      auth = await getAuthHeaders();
      res = await fetch(`${BASE_URL}/api/chat/history/${encodeURIComponent(sessionId)}`, {
        method: 'DELETE',
        headers: auth,
      });
    }
  }
  if (!res.ok) {
    const errBody = await res.json().catch(() => ({}));
    throw new Error((errBody as { detail?: string }).detail ?? res.statusText);
  }
  return res.json() as Promise<{ status: string; session_id: string }>;
}
