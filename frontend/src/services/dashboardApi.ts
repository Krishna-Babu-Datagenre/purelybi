import { InboundPayload } from '../types';
import { useDashboardStore } from '../store/useDashboardStore';

/* ─────────────────────────────────────────────
   Dashboard API Service
   ─────────────────────────────────────────────
   This module is the single entry-point for the
   Python backend to communicate with the frontend.

   It exposes:
   1. handleInboundPayload()  — parse & apply a JSON payload
   2. initMessageListener()   — listen for window.postMessage
   3. initPolling()           — poll a REST endpoint (optional)
───────────────────────────────────────────── */

/**
 * Validate the inbound payload has the minimum required shape.
 * Throws descriptive errors so backend developers get clear feedback.
 */
function validatePayload(raw: unknown): InboundPayload {
  if (typeof raw !== 'object' || raw === null) {
    throw new Error('[DashboardAPI] Payload must be a non-null object.');
  }

  const obj = raw as Record<string, unknown>;

  if (!['create', 'update', 'patch'].includes(obj.action as string)) {
    throw new Error(
      `[DashboardAPI] Invalid action "${String(obj.action)}". Expected "create", "update", or "patch".`,
    );
  }

  if (obj.action === 'create' || obj.action === 'update') {
    if (!obj.dashboard || typeof obj.dashboard !== 'object') {
      throw new Error('[DashboardAPI] "dashboard" object is required for create/update actions.');
    }
    const dash = obj.dashboard as Record<string, unknown>;
    if (!dash.meta || typeof dash.meta !== 'object') {
      throw new Error('[DashboardAPI] "dashboard.meta" is required.');
    }
    const meta = dash.meta as Record<string, unknown>;
    if (typeof meta.id !== 'string' || !meta.id) {
      throw new Error('[DashboardAPI] "dashboard.meta.id" must be a non-empty string.');
    }
    if (typeof meta.name !== 'string' || !meta.name) {
      throw new Error('[DashboardAPI] "dashboard.meta.name" must be a non-empty string.');
    }
    if (!Array.isArray(dash.widgets)) {
      throw new Error('[DashboardAPI] "dashboard.widgets" must be an array.');
    }
  }

  if (obj.action === 'patch') {
    if (typeof obj.dashboardId !== 'string' || !obj.dashboardId) {
      throw new Error('[DashboardAPI] "dashboardId" is required for patch action.');
    }
  }

  return obj as unknown as InboundPayload;
}

/**
 * Parse a raw JSON payload and apply it to the dashboard store.
 * Can be called programmatically from anywhere in the app.
 */
export function handleInboundPayload(raw: unknown): { ok: boolean; error?: string } {
  try {
    const payload = validatePayload(raw);
    useDashboardStore.getState().applyPayload(payload);
    return { ok: true };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(message);
    return { ok: false, error: message };
  }
}

/**
 * Listen for `window.postMessage` events from an embedding parent frame
 * or from the Python backend communicating via a WebView bridge.
 *
 * Messages must have `{ type: 'dashboard-payload', payload: InboundPayload }`.
 *
 * Returns an unsubscribe function.
 */
export function initMessageListener(): () => void {
  const handler = (event: MessageEvent) => {
    if (event.data?.type !== 'dashboard-payload') return;
    handleInboundPayload(event.data.payload);
  };

  window.addEventListener('message', handler);
  return () => window.removeEventListener('message', handler);
}

/**
 * Expose a global function on `window` so the Python backend can call it
 * directly (useful in Electron / CEF / WebView scenarios).
 *
 * Usage from Python side:  window.applyDashboardPayload({ action: 'create', ... })
 */
export function exposeGlobalApi(): void {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (window as any).applyDashboardPayload = (payload: unknown) => {
    return handleInboundPayload(payload);
  };
}
