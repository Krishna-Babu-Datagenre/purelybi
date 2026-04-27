/// <reference types="vite/client" />
import {
  TemplateMeta,
  TemplateWithWidgets,
  ApiDashboardMeta,
  ApiDashboard,
  ApiWidget,
  UserConnectorConfig,
  UserConnectorConfigPatch,
  RawTablePreview,
  SyncedTableInfo,
  ConnectorCatalogListItem,
  ConnectorCatalogDetail,
  DashboardBuilderReadiness,
} from '../types';
import { useAuthStore } from '../store/useAuthStore';
import { ensureAccessTokenFresh, fetchWithAuthRetry, runTokenRefresh } from './authSession';

/* ─────────────────────────────────────────────
   Backend API Client
   ─────────────────────────────────────────────
   Thin HTTP wrapper for the FastAPI backend.
   Base URL defaults to localhost:8000 for development;
   override with VITE_API_BASE_URL env var.
   Auth: dashboard endpoints require Authorization header (token from useAuthStore).
───────────────────────────────────────────── */

const BASE_URL = import.meta.env.VITE_API_BASE_URL?.replace(/\/+$/, '') ?? 'http://localhost:8000';

/** Detect browser "Failed to fetch" / network errors for clearer user feedback */
function isNetworkError(err: unknown): boolean {
  if (err instanceof TypeError) {
    const msg = (err as Error).message?.toLowerCase() ?? '';
    return msg.includes('failed to fetch') || msg.includes('network request failed') || msg.includes('load failed');
  }
  return false;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetchWithAuthRetry(path, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        ...(init?.headers as Record<string, string>),
      },
    });
  } catch (err) {
    if (isNetworkError(err)) {
      throw new Error(
        'Request blocked (often CORS preflight). Backend must respond to OPTIONS with 200 and CORS headers for this path. ' +
        `See Network tab for: ${BASE_URL}${path}`,
      );
    }
    throw err;
  }

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = typeof body.detail === 'string' ? body.detail : body.detail?.msg ?? body.message;
    throw new Error(detail ?? `Request failed: ${res.status} ${res.statusText}`);
  }

  return res.json() as Promise<T>;
}

/** Same as request but for 204 No Content — no JSON body. */
async function requestNoContent(path: string, init?: RequestInit): Promise<void> {
  let res: Response;
  try {
    res = await fetchWithAuthRetry(path, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        ...(init?.headers as Record<string, string>),
      },
    });
  } catch (err) {
    if (isNetworkError(err)) {
      throw new Error(
        'Request blocked (often CORS preflight). Backend must respond to OPTIONS with 200 and CORS headers for this path. ' +
          `See Network tab for: ${BASE_URL}${path}`,
      );
    }
    throw err;
  }

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = typeof body.detail === 'string' ? body.detail : body.detail?.msg ?? body.message;
    throw new Error(detail ?? `Request failed: ${res.status} ${res.statusText}`);
  }
  // 204 has no body
}

/* ── Template endpoints (no auth required) ── */

/** GET /api/templates — list all templates; optional platforms filter */
export function fetchTemplates(
  platforms?: string,
  options?: { forceRefresh?: boolean },
): Promise<TemplateMeta[]> {
  const q = platforms ? `?platforms=${encodeURIComponent(platforms)}` : '';
  return request<TemplateMeta[]>(`/api/templates${q}`, {
    ...(options?.forceRefresh ? { cache: 'no-store' as RequestCache } : {}),
  });
}

/** GET /api/templates/{slug} — single template with widget blueprints */
export function fetchTemplateBySlug(slug: string): Promise<TemplateWithWidgets> {
  return request<TemplateWithWidgets>(`/api/templates/${encodeURIComponent(slug)}`);
}

/* ── Dashboard endpoints (require auth) ── */

/** POST /api/dashboards — instantiate template as user-owned dashboard */
export function createDashboardFromTemplate(templateSlug: string): Promise<ApiDashboard> {
  return request<ApiDashboard>('/api/dashboards', {
    method: 'POST',
    body: JSON.stringify({ template_slug: templateSlug }),
  });
}

/** GET /api/dashboards — list user's dashboards (metadata only) */
export function listDashboards(options?: { forceRefresh?: boolean }): Promise<ApiDashboardMeta[]> {
  return request<ApiDashboardMeta[]>('/api/dashboards', {
    ...(options?.forceRefresh ? { cache: 'no-store' as RequestCache } : {}),
  });
}

/** GET /api/dashboards/builder/readiness — data + dataset view names for AI dashboard builder */
export function getDashboardBuilderReadiness(): Promise<DashboardBuilderReadiness> {
  return request<DashboardBuilderReadiness>('/api/dashboards/builder/readiness');
}

/** GET /api/dashboards/{id} — get dashboard with widgets */
export function getDashboard(
  dashboardId: string,
  options?: { forceRefresh?: boolean },
): Promise<ApiDashboard> {
  return request<ApiDashboard>(`/api/dashboards/${encodeURIComponent(dashboardId)}`, {
    ...(options?.forceRefresh ? { cache: 'no-store' as RequestCache } : {}),
  });
}

/** GET /api/dashboards/{id}?preset=...  or  ?start_date=...&end_date=... — get dashboard with date filter */
export function getDashboardFiltered(
  dashboardId: string,
  params: { preset?: string; startDate?: string; endDate?: string },
  options?: { forceRefresh?: boolean },
): Promise<ApiDashboard> {
  const q = new URLSearchParams();
  if (params.preset) q.set('preset', params.preset);
  if (params.startDate) q.set('start_date', params.startDate);
  if (params.endDate) q.set('end_date', params.endDate);
  const qs = q.toString();
  return request<ApiDashboard>(
    `/api/dashboards/${encodeURIComponent(dashboardId)}${qs ? `?${qs}` : ''}`,
    {
      ...(options?.forceRefresh ? { cache: 'no-store' as RequestCache } : {}),
    },
  );
}

/** POST /api/dashboards/{id}/filtered — apply a native FilterSpec to a dashboard */
export function getDashboardFilteredWithSpec(
  dashboardId: string,
  filterSpec?: import('../types/metadata').FilterSpec,
  options?: { forceRefresh?: boolean },
): Promise<ApiDashboard> {
  return request<ApiDashboard>(
    `/api/dashboards/${encodeURIComponent(dashboardId)}/filtered`,
    {
      method: 'POST',
      body: JSON.stringify({
        filter_spec: filterSpec ?? null,
        force_refresh: options?.forceRefresh ?? false,
      }),
    },
  );
}

/* ── Max data date (cached — endpoint can be slow; avoid refetch every navigation/refresh) ── */

const MAX_DATA_DATE_STORAGE_KEY = 'bi-agent.maxDataDate.v1';
/** Client cache TTL; max date changes rarely (ETL), so a long window is fine. */
const MAX_DATA_DATE_CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

type MaxDataDateEntry = { max_date: string; expiresAt: number };

let maxDataDateMemory: MaxDataDateEntry | null = null;
let maxDataDateInflight: Promise<{ max_date: string }> | null = null;

function readMaxDataDateStorage(): MaxDataDateEntry | null {
  try {
    const raw = localStorage.getItem(MAX_DATA_DATE_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as MaxDataDateEntry;
    if (typeof parsed.max_date === 'string' && typeof parsed.expiresAt === 'number') {
      return parsed;
    }
  } catch {
    // ignore
  }
  return null;
}

function writeMaxDataDateStorage(entry: MaxDataDateEntry): void {
  try {
    localStorage.setItem(MAX_DATA_DATE_STORAGE_KEY, JSON.stringify(entry));
  } catch {
    // private mode / quota
  }
}

/** Drop max-data-date cache (e.g. on logout). */
export function clearMaxDataDateCache(): void {
  maxDataDateMemory = null;
  maxDataDateInflight = null;
  try {
    localStorage.removeItem(MAX_DATA_DATE_STORAGE_KEY);
  } catch {
    // ignore
  }
}

/** GET /api/dashboards/data/max-date — cached in memory + localStorage to avoid slow repeat loads. */
export function getMaxDataDate(): Promise<{ max_date: string }> {
  const now = Date.now();

  if (maxDataDateMemory && maxDataDateMemory.expiresAt > now) {
    return Promise.resolve({ max_date: maxDataDateMemory.max_date });
  }

  const stored = readMaxDataDateStorage();
  if (stored && stored.expiresAt > now) {
    maxDataDateMemory = stored;
    return Promise.resolve({ max_date: stored.max_date });
  }

  if (maxDataDateInflight) {
    return maxDataDateInflight;
  }

  maxDataDateInflight = request<{ max_date: string }>('/api/dashboards/data/max-date')
    .then((r) => {
      const expiresAt = Date.now() + MAX_DATA_DATE_CACHE_TTL_MS;
      const entry: MaxDataDateEntry = { max_date: r.max_date, expiresAt };
      maxDataDateMemory = entry;
      writeMaxDataDateStorage(entry);
      return r;
    })
    .finally(() => {
      maxDataDateInflight = null;
    });

  return maxDataDateInflight;
}

/** POST /api/dashboards/create — create a new blank dashboard */
export function createBlankDashboard(
  name: string,
  description?: string,
  tags?: string[],
): Promise<ApiDashboard> {
  return request<ApiDashboard>('/api/dashboards/create', {
    method: 'POST',
    body: JSON.stringify({ name, description, tags }),
  });
}

/**
 * Sanitize payload to plain JSON (strip undefined, functions, circular refs).
 * ECharts config from chat can contain non-serializable values and cause "Failed to fetch" or backend errors.
 */
function toPlainJson<T>(obj: T): T {
  return JSON.parse(JSON.stringify(obj)) as T;
}

/** POST /api/dashboards/{id}/widgets — add widget to existing dashboard */
export function addWidgetToDashboard(
  dashboardId: string,
  widget: {
    title: string;
    type: string;
    chart_config: Record<string, unknown>;
    layout?: { x: number; y: number; w: number; h: number };
    data_config?: Record<string, unknown>;
  },
): Promise<ApiWidget> {
  const payload: {
    title: string;
    type: string;
    chart_config: Record<string, unknown>;
    layout?: { x: number; y: number; w: number; h: number };
    data_config?: Record<string, unknown>;
  } = {
    title: widget.title,
    type: widget.type,
    chart_config: toPlainJson(widget.chart_config),
  };
  if (widget.layout) {
    payload.layout = widget.layout;
  }
  if (widget.data_config) {
    payload.data_config = toPlainJson(widget.data_config);
  }
  return request<ApiWidget>(
    `/api/dashboards/${encodeURIComponent(dashboardId)}/widgets`,
    { method: 'POST', body: JSON.stringify(payload) },
  );
}

/** PUT /api/dashboards/{id}/widgets/{widget_id} — update existing widget */
export function updateWidget(
  dashboardId: string,
  widgetId: string,
  patch: {
    title?: string;
    chart_config?: Record<string, unknown>;
    data_config?: Record<string, unknown>;
  },
): Promise<ApiWidget> {
  const payload: any = {};
  if (patch.title !== undefined) payload.title = patch.title;
  if (patch.chart_config !== undefined) payload.chart_config = toPlainJson(patch.chart_config);
  if (patch.data_config !== undefined) payload.data_config = toPlainJson(patch.data_config);

  return request<ApiWidget>(
    `/api/dashboards/${encodeURIComponent(dashboardId)}/widgets/${encodeURIComponent(widgetId)}`,
    { method: 'PUT', body: JSON.stringify(payload) },
  );
}

/** POST /api/dashboards/preview-widget — run a widget's SQL and return hydrated data */
export function previewWidget(
  widget: Record<string, unknown>
): Promise<ApiWidget> {
  return request<ApiWidget>(
    `/api/dashboards/preview-widget`,
    { method: 'POST', body: JSON.stringify({ widget: toPlainJson(widget) }) },
  );
}

/** POST /api/dashboards/{id}/duplicate — duplicate a dashboard (user-owned or template) */
export function duplicateDashboard(
  dashboardId: string,
  name?: string,
): Promise<ApiDashboard> {
  return request<ApiDashboard>(
    `/api/dashboards/${encodeURIComponent(dashboardId)}/duplicate`,
    {
      method: 'POST',
      body: JSON.stringify(name ? { name } : {}),
    },
  );
}

/** DELETE /api/dashboards/{id} — delete dashboard and all its widgets. Returns 204. */
export function deleteDashboard(dashboardId: string): Promise<void> {
  return requestNoContent(`/api/dashboards/${encodeURIComponent(dashboardId)}`, {
    method: 'DELETE',
  });
}

/** DELETE /api/dashboards/{id}/widgets/{widget_id} — delete a single widget. Returns 204. */
export function deleteWidget(dashboardId: string, widgetId: string): Promise<void> {
  return requestNoContent(
    `/api/dashboards/${encodeURIComponent(dashboardId)}/widgets/${encodeURIComponent(widgetId)}`,
    { method: 'DELETE' },
  );
}

/** PUT /api/dashboards/{id}/widgets/layouts — persist widget grid layouts. Returns 204. */
export function persistWidgetLayouts(
  dashboardId: string,
  layouts: Array<{ id: string; x: number; y: number; w: number; h: number }>,
): Promise<void> {
  return requestNoContent(
    `/api/dashboards/${encodeURIComponent(dashboardId)}/widgets/layouts`,
    {
      method: 'PUT',
      body: JSON.stringify({ layouts }),
    },
  );
}

/* ── Connectors (require auth) ── */

/* --- Catalog cache (avoids re-fetching on every navigation / search clear) --- */

const CATALOG_CACHE_TTL_MS = 10 * 60 * 1000; // 10 min

type CatalogListEntry = { data: ConnectorCatalogListItem[]; expiresAt: number };
let catalogListCache: Record<string, CatalogListEntry> = {};
let catalogListInflight: Record<string, Promise<ConnectorCatalogListItem[]>> = {};

type CatalogDetailEntry = { data: ConnectorCatalogDetail; expiresAt: number };
const catalogDetailCache: Record<string, CatalogDetailEntry> = {};
const catalogDetailInflight: Record<string, Promise<ConnectorCatalogDetail>> = {};

/** Drop all catalog caches (e.g. on logout). */
export function clearCatalogCache(): void {
  catalogListCache = {};
  catalogListInflight = {};
  Object.keys(catalogDetailCache).forEach((k) => delete catalogDetailCache[k]);
  Object.keys(catalogDetailInflight).forEach((k) => delete catalogDetailInflight[k]);
}

/** GET /api/connectors/catalog — cached; pass `forceRefresh` to bypass. */
export function listConnectorCatalog(options?: {
  q?: string;
  activeOnly?: boolean;
  forceRefresh?: boolean;
}): Promise<ConnectorCatalogListItem[]> {
  const q = new URLSearchParams();
  if (options?.q) q.set('q', options.q);
  if (options?.activeOnly === false) q.set('active_only', 'false');
  const qs = q.toString();
  const cacheKey = qs || '__all__';
  const now = Date.now();

  if (!options?.forceRefresh) {
    const cached = catalogListCache[cacheKey];
    if (cached && cached.expiresAt > now) return Promise.resolve(cached.data);
  }

  const inflight = catalogListInflight[cacheKey];
  if (inflight) return inflight;

  const p = request<ConnectorCatalogListItem[]>(
    `/api/connectors/catalog${qs ? `?${qs}` : ''}`,
  ).then((data) => {
    catalogListCache[cacheKey] = { data, expiresAt: Date.now() + CATALOG_CACHE_TTL_MS };
    return data;
  }).finally(() => {
    delete catalogListInflight[cacheKey];
  });
  catalogListInflight[cacheKey] = p;
  return p;
}

/**
 * GET /api/connectors/catalog/{identifier} — full row (cached).
 * Pass catalog id from list, or e.g. `airbyte/source-github`.
 */
export function getConnectorCatalogDetail(
  identifier: string,
  options?: { forceRefresh?: boolean },
): Promise<ConnectorCatalogDetail> {
  const enc = encodeURIComponent(identifier);
  const now = Date.now();

  if (!options?.forceRefresh) {
    const cached = catalogDetailCache[identifier];
    if (cached && cached.expiresAt > now) return Promise.resolve(cached.data);
  }

  const detailInflight = catalogDetailInflight[identifier];
  if (detailInflight) return detailInflight;

  const p = request<ConnectorCatalogDetail>(
    `/api/connectors/catalog/${enc}`,
  ).then((data) => {
    catalogDetailCache[identifier] = { data, expiresAt: Date.now() + CATALOG_CACHE_TTL_MS };
    return data;
  }).finally(() => {
    delete catalogDetailInflight[identifier];
  });
  catalogDetailInflight[identifier] = p;
  return p;
}

/** GET /api/connectors — list current user’s connector configurations */
export function listUserConnectors(options?: { forceRefresh?: boolean }): Promise<UserConnectorConfig[]> {
  return request<UserConnectorConfig[]>('/api/connectors', {
    ...(options?.forceRefresh ? { cache: 'no-store' as RequestCache } : {}),
  });
}

/** PATCH /api/connectors/{configId} */
export function patchUserConnector(
  configId: string,
  body: UserConnectorConfigPatch,
): Promise<UserConnectorConfig> {
  return request<UserConnectorConfig>(`/api/connectors/${encodeURIComponent(configId)}`, {
    method: 'PATCH',
    body: JSON.stringify(body),
  });
}

/** DELETE /api/connectors/{configId} — 204 */
export function deleteUserConnector(configId: string): Promise<void> {
  return requestNoContent(`/api/connectors/${encodeURIComponent(configId)}`, {
    method: 'DELETE',
  });
}

/** GET /api/connectors/synced-tables — sync metadata for “View raw tables” */
export function listSyncedTablesMetadata(options?: {
  forceRefresh?: boolean;
  /** Inclusive ISO date (YYYY-MM-DD); include both for per-stream Parquet inventory */
  startDate?: string;
  endDate?: string;
}): Promise<SyncedTableInfo[]> {
  const q = new URLSearchParams();
  if (options?.startDate) q.set('start_date', options.startDate);
  if (options?.endDate) q.set('end_date', options.endDate);
  const qs = q.toString();
  return request<SyncedTableInfo[]>(`/api/connectors/synced-tables${qs ? `?${qs}` : ''}`, {
    ...(options?.forceRefresh ? { cache: 'no-store' as RequestCache } : {}),
  });
}

/**
 * GET /api/connectors/{configId}/streams/{stream}/download — ZIP of monthly Parquet files in range.
 */
export async function downloadRawStreamZip(
  configId: string,
  streamName: string,
  range: { startDate: string; endDate: string },
): Promise<void> {
  const q = new URLSearchParams();
  q.set('start_date', range.startDate);
  q.set('end_date', range.endDate);
  const path = `/api/connectors/${encodeURIComponent(configId)}/streams/${encodeURIComponent(streamName)}/download?${q}`;
  await ensureAccessTokenFresh();
  let token = useAuthStore.getState().accessToken;
  let res: Response;
  try {
    res = await fetch(`${BASE_URL}${path}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  } catch (err) {
    if (isNetworkError(err)) {
      throw new Error(
        'Request blocked (often CORS preflight). Backend must respond to OPTIONS with 200 and CORS headers for this path. ' +
          `See Network tab for: ${BASE_URL}${path}`,
      );
    }
    throw err;
  }
  if (!res.ok && res.status === 401 && useAuthStore.getState().refreshToken) {
    const ok = await runTokenRefresh();
    if (ok) {
      token = useAuthStore.getState().accessToken;
      res = await fetch(`${BASE_URL}${path}`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
    }
  }
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = typeof body.detail === 'string' ? body.detail : body.detail?.msg ?? body.message;
    throw new Error(detail ?? `Download failed: ${res.status} ${res.statusText}`);
  }
  const blob = await res.blob();
  const cd = res.headers.get('Content-Disposition');
  let filename = `raw_${streamName.replace(/[^\w.-]+/g, '_')}.zip`;
  const m = cd?.match(/filename="([^"]+)"/i) ?? cd?.match(/filename=([^;\s]+)/i);
  if (m?.[1]) filename = m[1].trim();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.rel = 'noopener';
  a.click();
  URL.revokeObjectURL(url);
}

const RAW_PREVIEW_PAGE_SIZE = 50;

/** GET /api/connectors/{configId}/streams/{stream}/preview — paginated JSON rows from Parquet */
export function fetchRawTablePreview(
  configId: string,
  streamName: string,
  range: { startDate: string; endDate: string },
  options?: { limit?: number; offset?: number },
): Promise<RawTablePreview> {
  const q = new URLSearchParams();
  q.set('start_date', range.startDate);
  q.set('end_date', range.endDate);
  q.set('limit', String(options?.limit ?? RAW_PREVIEW_PAGE_SIZE));
  q.set('offset', String(options?.offset ?? 0));
  return request<RawTablePreview>(
    `/api/connectors/${encodeURIComponent(configId)}/streams/${encodeURIComponent(streamName)}/preview?${q}`,
  );
}

export { RAW_PREVIEW_PAGE_SIZE };

/* ── Health ── */

export function healthCheck(): Promise<{ status: string }> {
  return request<{ status: string }>('/health');
}
