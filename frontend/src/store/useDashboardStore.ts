import { create } from 'zustand';
import { LayoutItem } from 'react-grid-layout/legacy';
import {
  Widget,
  Dashboard,
  DashboardMeta,
  WidgetPatch,
  DashboardApiPayload,
  DashboardPatchPayload,
  InboundPayload,
  TemplateMeta,
  ApiDashboardMeta,
  DatePreset,
  CustomDateRange,
} from '../types';
import {
  fetchTemplates as fetchTemplatesApi,
  createDashboardFromTemplate,
  listDashboards,
  getDashboard,
  getDashboardFiltered,
  createBlankDashboard as createBlankDashboardApi,
  addWidgetToDashboard as addWidgetToDashboardApi,
  deleteDashboard as deleteDashboardApiCall,
  deleteWidget as deleteWidgetApiCall,
  persistWidgetLayouts as persistWidgetLayoutsApiCall,
  duplicateDashboard as duplicateDashboardApiCall,
} from '../services/backendClient';
import { apiDashboardToDashboard } from '../utils/apiDashboardToDashboard';
import { findDashboardKey } from '../utils/dashboardId';
import { computeKpiLayouts, isAutoBalancedKpiLayout, SECTION_GAP } from '../utils/layoutEngine';

/* ─────────────────────────────────────────────
   Helpers
───────────────────────────────────────────── */

const now = () => new Date().toISOString();

const BASE_COLS = 12;
const KPI_W = 3;
const KPI_H = 2;
const CHART_W = 6;
const CHART_H = 8;

type GridRect = { x: number; y: number; w: number; h: number };

function rectsOverlap(a: GridRect, b: GridRect): boolean {
  return a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y;
}

function canPlace(candidate: GridRect, occupied: GridRect[]): boolean {
  if (candidate.x < 0 || candidate.y < 0) return false;
  if (candidate.w < 1 || candidate.h < 1) return false;
  if (candidate.x + candidate.w > BASE_COLS) return false;
  return !occupied.some((r) => rectsOverlap(candidate, r));
}

function computeWidgetInsertLayout(existing: Widget[], widgetType: string): GridRect {
  const occupied: GridRect[] = existing
    .map((w) => w.layout)
    .filter((l): l is GridRect => !!l)
    .map((l) => ({ x: l.x, y: l.y, w: l.w, h: l.h }));

  const maxBottom = occupied.reduce((m, r) => Math.max(m, r.y + r.h), 0);

  if (widgetType === 'kpi') {
    const kpiLayouts = existing
      .filter((w) => w.type === 'kpi' && w.layout)
      .map((w) => w.layout as GridRect);
    const kpiBottom = kpiLayouts.reduce((m, r) => Math.max(m, r.y + r.h), 0);
    const numKpiRows = Math.max(1, Math.ceil(kpiBottom / KPI_H));

    for (let row = 0; row < numKpiRows + 1; row++) {
      for (let col = 0; col < 4; col++) {
        const candidate: GridRect = { x: col * KPI_W, y: row * KPI_H, w: KPI_W, h: KPI_H };
        if (canPlace(candidate, occupied)) return candidate;
      }
    }
    return { x: 0, y: maxBottom, w: KPI_W, h: KPI_H };
  }

  const lastChart = existing
    .filter((w) => w.type !== 'kpi' && w.layout)
    .map((w) => w.layout as GridRect)
    .sort((a, b) => (a.y === b.y ? a.x - b.x : a.y - b.y));
  const lastChartLayout = lastChart.length > 0 ? lastChart[lastChart.length - 1] : undefined;

  if (lastChartLayout) {
    const sideBySide: GridRect = { x: 6, y: lastChartLayout.y, w: CHART_W, h: CHART_H };
    const newRow: GridRect = { x: 0, y: maxBottom, w: CHART_W, h: CHART_H };
    if (lastChartLayout.x < 6 && canPlace(sideBySide, occupied)) return sideBySide;
    if (canPlace(newRow, occupied)) return newRow;
  }

  return { x: 0, y: maxBottom, w: CHART_W, h: CHART_H };
}

/** Main app shell route — lives in Zustand so it updates in lockstep with dashboard loads (avoids React prop lag vs Zustand). */
export type ShellPage =
  | 'home'
  | 'dashboard'
  | 'dashboard-ai'
  | 'alerts'
  | 'data-connect'
  | 'data-manage'
  | 'data-raw-tables'
  | 'metadata';

/* ─────────────────────────────────────────────
   Store Interface
───────────────────────────────────────────── */

interface DashboardState {
  /** Sidebar / main area route */
  navigationPage: ShellPage;
  setNavigationPage: (page: ShellPage) => void;
  /** Open the AI dashboard builder route and clear the active canvas selection. */
  openDashboardBuilder: () => void;
  /** Navigate to dashboard view and load a user dashboard (single user gesture — keeps shell route + data in sync). */
  openUserDashboard: (dashboardId: string) => Promise<void>;

  /** All dashboards keyed by ID (full data when loaded) */
  dashboards: Record<string, Dashboard>;
  /** List of user's dashboards from GET /api/dashboards (metadata only) */
  dashboardListMeta: ApiDashboardMeta[];
  /** Whether dashboard list has been fetched at least once */
  dashboardListFetched: boolean;
  /** Currently active dashboard ID */
  activeDashboardId: string | null;
  /** Sidebar-selected dashboard ID from list endpoint (drives row highlight deterministically). */
  activeDashboardListId: string | null;

  /** Available templates fetched from backend */
  templates: TemplateMeta[];
  /** Whether templates have been fetched at least once */
  templatesFetched: boolean;

  /** True while a specific dashboard is being loaded or created from template */
  dashboardLoading: boolean;
  /** Last error message (null = no error) */
  error: string | null;

  /* ── Date filtering ── */
  /** Active quick-select preset (null = no preset active) */
  activePreset: DatePreset | null;
  /** Active custom date range (null = not using custom range) */
  customDateRange: CustomDateRange | null;
  /** True while a filter request is in-flight */
  filterLoading: boolean;

  /* ── Selectors ── */
  getActiveDashboard: () => Dashboard | null;

  /* ── Template operations ── */
  fetchTemplates: (platforms?: string, options?: { forceRefresh?: boolean }) => Promise<void>;
  /** Instantiate template as user dashboard (POST /api/dashboards). Uses template slug. */
  loadFromTemplate: (templateSlug: string) => Promise<void>;

  /* ── User dashboard list ── */
  fetchUserDashboardList: (options?: { forceRefresh?: boolean }) => Promise<void>;
  /** Load a single dashboard by ID (GET /api/dashboards/{id}) and set as active */
  loadDashboardById: (dashboardId: string) => Promise<void>;
  /** Re-fetch the active dashboard from the server (bypasses client cache). Respects date filters. */
  refreshActiveDashboardFromServer: () => Promise<void>;

  /* ── Backend dashboard operations ── */
  /** POST /api/dashboards/create — create blank dashboard via API */
  createBlankDashboard: (name: string, description?: string, tags?: string[]) => Promise<string>;
  /** POST /api/dashboards/{id}/widgets — add a chart widget to a dashboard via API */
  addWidgetToDashboard: (
    dashboardId: string,
    widget: { title: string; type: string; chart_config: Record<string, unknown>; layout?: { x: number; y: number; w: number; h: number }; data_config?: Record<string, unknown> },
  ) => Promise<void>;
  /** POST /api/dashboards/{id}/duplicate — duplicate a dashboard and open the copy */
  duplicateDashboardApi: (dashboardId: string, name?: string) => Promise<void>;
  /** DELETE /api/dashboards/{id} — delete dashboard via API and update local state */
  deleteDashboardApi: (dashboardId: string) => Promise<void>;
  /** DELETE /api/dashboards/{id}/widgets/{widget_id} — delete widget via API and update local state */
  deleteWidgetApi: (dashboardId: string, widgetId: string) => Promise<void>;
  /** PUT /api/dashboards/{id}/widgets/layouts — persist drag/resize widget layout changes */
  persistWidgetLayoutsApi: (
    dashboardId: string,
    layouts: Array<{ id: string; x: number; y: number; w: number; h: number }>,
  ) => Promise<void>;

  /* ── Dashboard-level operations ── */
  createDashboard: (dashboard: Dashboard) => void;
  updateDashboardMeta: (dashboardId: string, meta: Partial<DashboardMeta>) => void;
  deleteDashboard: (dashboardId: string) => void;
  setActiveDashboard: (dashboardId: string) => void;
  clearActiveDashboard: () => void;
  clearError: () => void;

  /* ── Widget-level operations ── */
  addWidgets: (dashboardId: string, widgets: Widget[]) => void;
  updateWidget: (dashboardId: string, patch: WidgetPatch) => void;
  removeWidgets: (dashboardId: string, widgetIds: string[]) => void;
  updateWidgetLayout: (layouts: LayoutItem[]) => void;

  /* ── Bulk API handler ── */
  applyPayload: (payload: InboundPayload) => void;

  /* ── Date filtering ── */
  /** Apply a quick-select date preset to the active dashboard */
  applyDatePreset: (preset: DatePreset) => Promise<void>;
  /** Apply a custom date range to the active dashboard */
  applyCustomDateRange: (range: CustomDateRange) => Promise<void>;
  /** Clear all date filters and reload with unfiltered data */
  clearDateFilter: () => Promise<void>;
}

/* ─────────────────────────────────────────────
   Store Implementation
───────────────────────────────────────────── */

export const useDashboardStore = create<DashboardState>((set, get) => ({
  navigationPage: 'home',
  setNavigationPage: (page) => set({ navigationPage: page }),
  openDashboardBuilder: () => {
    set({ navigationPage: 'dashboard-ai' });
    get().clearActiveDashboard();
  },
  openUserDashboard: async (dashboardId: string) => {
    set({ navigationPage: 'dashboard', activeDashboardListId: dashboardId });
    await get().loadDashboardById(dashboardId);
  },

  dashboards: {},
  dashboardListMeta: [],
  dashboardListFetched: false,
  activeDashboardId: null,
  activeDashboardListId: null,

  templates: [],
  templatesFetched: false,

  dashboardLoading: false,
  error: null,

  activePreset: null,
  customDateRange: null,
  filterLoading: false,

  /* ── Selectors ── */
  getActiveDashboard: () => {
    const { dashboards, activeDashboardId } = get();
    return activeDashboardId ? dashboards[activeDashboardId] ?? null : null;
  },

  /* ── Template operations ── */
  fetchTemplates: async (platforms, options) => {
    try {
      const templates = await fetchTemplatesApi(
        platforms,
        options?.forceRefresh ? { forceRefresh: true } : undefined,
      );
      set({ templates, templatesFetched: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
    }
  },

  loadFromTemplate: async (templateSlug: string) => {
    set({ dashboardLoading: true, error: null });
    try {
      const api = await createDashboardFromTemplate(templateSlug);
      const dashboard = apiDashboardToDashboard(api);
      get().createDashboard(dashboard);
      set({ dashboardLoading: false });
      // Re-fetch the full list from the server so the sidebar stays in sync
      get().fetchUserDashboardList({ forceRefresh: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message, dashboardLoading: false });
    }
  },

  fetchUserDashboardList: async (options) => {
    set({ error: null });
    try {
      const list = await listDashboards(
        options?.forceRefresh ? { forceRefresh: true } : undefined,
      );
      set({ dashboardListMeta: list, dashboardListFetched: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message, dashboardListFetched: true });
    }
  },

  loadDashboardById: async (dashboardId: string) => {
    const { dashboards } = get();
    const cachedKey = findDashboardKey(dashboards, dashboardId);
    if (cachedKey) {
      set({ activeDashboardId: cachedKey, activeDashboardListId: dashboardId });
      return;
    }
    set({
      dashboardLoading: true,
      activeDashboardId: dashboardId,
      activeDashboardListId: dashboardId,
      error: null,
    });
    try {
      const api = await getDashboard(dashboardId);
      const dashboard = apiDashboardToDashboard(api);
      set((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
        activeDashboardId: dashboard.meta.id,
        dashboardLoading: false,
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message, dashboardLoading: false, activeDashboardId: null });
    }
  },

  refreshActiveDashboardFromServer: async () => {
    const { activeDashboardId, activePreset, customDateRange } = get();
    if (!activeDashboardId) return;
    const useFilter = !!(activePreset || customDateRange);
    set({ error: null, ...(useFilter ? { filterLoading: true } : { dashboardLoading: true }) });
    try {
      let api;
      if (activePreset) {
        api = await getDashboardFiltered(
          activeDashboardId,
          { preset: activePreset },
          { forceRefresh: true },
        );
      } else if (customDateRange) {
        api = await getDashboardFiltered(
          activeDashboardId,
          {
            startDate: customDateRange.startDate,
            endDate: customDateRange.endDate,
          },
          { forceRefresh: true },
        );
      } else {
        api = await getDashboard(activeDashboardId, { forceRefresh: true });
      }
      const dashboard = apiDashboardToDashboard(api);
      set((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
        ...(useFilter ? { filterLoading: false } : { dashboardLoading: false }),
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message, filterLoading: false, dashboardLoading: false });
    }
  },

  /* ── Backend dashboard operations ── */
  createBlankDashboard: async (name, description, tags) => {
    set({ error: null });
    try {
      const api = await createBlankDashboardApi(name, description, tags);
      const dashboard = apiDashboardToDashboard(api);
      set((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
      }));
      return dashboard.meta.id;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
      throw err;
    }
  },

  addWidgetToDashboard: async (dashboardId, widget) => {
    set({ error: null });
    try {
      const dashboard = get().dashboards[dashboardId];
      let computedLayout = widget.layout;
      let rebalancePayloads: Array<{ id: string; x: number; y: number; w: number; h: number }> | null = null;

      if (!computedLayout && dashboard) {
        if (widget.type === 'kpi') {
          const existingKpis = dashboard.widgets.filter((w) => w.type === 'kpi');
          const shouldRebalance = isAutoBalancedKpiLayout(existingKpis);

          if (shouldRebalance) {
            // Compute balanced layout for all KPIs including the new one
            const allKpiIds = [...existingKpis.map((k) => k.id), '__pending__'];
            const balanced = computeKpiLayouts(allKpiIds);
            computedLayout = balanced.get('__pending__') ?? { x: 0, y: 0, w: 12, h: 2 };

            // Rebalance existing KPI positions
            const kpiUpdates = existingKpis
              .filter((k) => balanced.has(k.id))
              .map((k) => {
                const r = balanced.get(k.id)!;
                return { id: k.id, x: r.x, y: r.y, w: r.w, h: r.h };
              });

            // Shift charts down if KPI section grew
            const oldKpiBottom = existingKpis.reduce(
              (m, k) => Math.max(m, (k.layout?.y ?? 0) + (k.layout?.h ?? 2)),
              0,
            );
            const balancedArr = Array.from(balanced.values());
            const newKpiBottom = balancedArr.length > 0
              ? Math.max(...balancedArr.map((r) => r.y + r.h))
              : 0;
            const hadKpis = existingKpis.length > 0;
            const yShift = hadKpis
              ? newKpiBottom - oldKpiBottom
              : newKpiBottom + SECTION_GAP;

            const chartShifts = yShift > 0
              ? dashboard.widgets
                  .filter((w) => w.type !== 'kpi' && w.layout)
                  .map((w) => ({
                    id: w.id,
                    x: w.layout!.x,
                    y: w.layout!.y + yShift,
                    w: w.layout!.w,
                    h: w.layout!.h,
                  }))
              : [];

            rebalancePayloads = [...kpiUpdates, ...chartShifts];
          } else {
            // KPIs were manually arranged — just find the next open slot
            computedLayout = computeWidgetInsertLayout(dashboard.widgets, widget.type);
          }
        } else {
          computedLayout = computeWidgetInsertLayout(dashboard.widgets, widget.type);
        }
      }

      const apiWidget = await addWidgetToDashboardApi(dashboardId, { ...widget, layout: computedLayout });
      const mapped: Widget = {
        id: apiWidget.id,
        title: apiWidget.title,
        type: apiWidget.type,
        layout: apiWidget.layout,
        chartConfig: apiWidget.chart_config,
      };

      // Apply new widget + rebalancing atomically to avoid intermediate flicker
      set((s) => {
        const db = s.dashboards[dashboardId];
        if (!db) return s;
        let updatedWidgets = [...db.widgets, mapped];
        if (rebalancePayloads && rebalancePayloads.length > 0) {
          const updateMap = new Map(rebalancePayloads!.map((u) => [u.id, u]));
          updatedWidgets = updatedWidgets.map((w) => {
            const upd = updateMap.get(w.id);
            if (!upd) return w;
            return { ...w, layout: { x: upd.x, y: upd.y, w: upd.w, h: upd.h } };
          });
        }
        return {
          dashboards: {
            ...s.dashboards,
            [dashboardId]: {
              ...db,
              meta: { ...db.meta, updatedAt: now() },
              widgets: updatedWidgets,
            },
          },
        };
      });

      // Persist rebalanced layouts to backend in background
      if (rebalancePayloads && rebalancePayloads.length > 0) {
        persistWidgetLayoutsApiCall(dashboardId, rebalancePayloads).catch(() => {});
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
      throw err;
    }
  },

  duplicateDashboardApi: async (dashboardId, name) => {
    set({ dashboardLoading: true, error: null });
    try {
      const api = await duplicateDashboardApiCall(dashboardId, name);
      const dashboard = apiDashboardToDashboard(api);
      set((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
        activeDashboardId: dashboard.meta.id,
        activeDashboardListId: dashboard.meta.id,
        navigationPage: 'dashboard',
        dashboardLoading: false,
      }));
      await get().fetchUserDashboardList({ forceRefresh: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message, dashboardLoading: false });
    }
  },

  deleteDashboardApi: async (dashboardId) => {
    set({ error: null });
    try {
      await deleteDashboardApiCall(dashboardId);
      get().deleteDashboard(dashboardId);
      await get().fetchUserDashboardList({ forceRefresh: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
      throw err;
    }
  },

  deleteWidgetApi: async (dashboardId, widgetId) => {
    set({ error: null });
    try {
      await deleteWidgetApiCall(dashboardId, widgetId);
      get().removeWidgets(dashboardId, [widgetId]);
      // Re-fetch from server so grid gets clean layout data after removal.
      const api = await getDashboard(dashboardId, { forceRefresh: true });
      const dashboard = apiDashboardToDashboard(api);
      set((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
      throw err;
    }
  },

  persistWidgetLayoutsApi: async (dashboardId, layouts) => {
    set({ error: null });
    try {
      await persistWidgetLayoutsApiCall(dashboardId, layouts);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
      throw err;
    }
  },

  /* ── Dashboard-level ── */
  createDashboard: (dashboard) => {
    set((s) => ({
      dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
      activeDashboardId: dashboard.meta.id,
    }));
  },

  updateDashboardMeta: (dashboardId, meta) => {
    set((s) => {
      const db = s.dashboards[dashboardId];
      if (!db) return s;
      return {
        dashboards: {
          ...s.dashboards,
          [dashboardId]: {
            ...db,
            meta: { ...db.meta, ...meta, updatedAt: now() },
          },
        },
      };
    });
  },

  deleteDashboard: (dashboardId) => {
    set((s) => {
      const { [dashboardId]: _, ...rest } = s.dashboards;
      const newActive =
        s.activeDashboardId === dashboardId
          ? Object.keys(rest)[0] ?? null
          : s.activeDashboardId;
      const newActiveListId =
        s.activeDashboardListId === dashboardId ? null : s.activeDashboardListId;
      return { dashboards: rest, activeDashboardId: newActive, activeDashboardListId: newActiveListId };
    });
  },

  setActiveDashboard: (dashboardId) => {
    set({ activeDashboardId: dashboardId });
  },

  clearActiveDashboard: () => {
    set({ activeDashboardId: null, activeDashboardListId: null });
  },

  clearError: () => {
    set({ error: null });
  },
  addWidgets: (dashboardId, widgets) => {
    set((s) => {
      const db = s.dashboards[dashboardId];
      if (!db) return s;
      return {
        dashboards: {
          ...s.dashboards,
          [dashboardId]: {
            ...db,
            meta: { ...db.meta, updatedAt: now() },
            widgets: [...db.widgets, ...widgets],
          },
        },
      };
    });
  },

  updateWidget: (dashboardId, patch) => {
    set((s) => {
      const db = s.dashboards[dashboardId];
      if (!db) return s;
      return {
        dashboards: {
          ...s.dashboards,
          [dashboardId]: {
            ...db,
            meta: { ...db.meta, updatedAt: now() },
            widgets: db.widgets.map((w) =>
              w.id === patch.id ? { ...w, ...patch } : w,
            ),
          },
        },
      };
    });
  },

  removeWidgets: (dashboardId, widgetIds) => {
    set((s) => {
      const db = s.dashboards[dashboardId];
      if (!db) return s;
      const idsToRemove = new Set(widgetIds);
      return {
        dashboards: {
          ...s.dashboards,
          [dashboardId]: {
            ...db,
            meta: { ...db.meta, updatedAt: now() },
            widgets: db.widgets.filter((w) => !idsToRemove.has(w.id)),
          },
        },
      };
    });
  },

  updateWidgetLayout: (layouts) => {
    const { activeDashboardId } = get();
    if (!activeDashboardId) return;

    set((s) => {
      const db = s.dashboards[activeDashboardId];
      if (!db) return s;
      return {
        dashboards: {
          ...s.dashboards,
          [activeDashboardId]: {
            ...db,
            widgets: db.widgets.map((widget) => {
              const updated = layouts.find((l) => l.i === widget.id);
              if (!updated) return widget;
              return {
                ...widget,
                layout: { x: updated.x, y: updated.y, w: updated.w, h: updated.h },
              };
            }),
          },
        },
      };
    });
  },

  /* ── Bulk API handler ── */
  applyPayload: (payload) => {
    const store = get();

    if (payload.action === 'create') {
      const p = payload as DashboardApiPayload;
      store.createDashboard(p.dashboard);
      return;
    }

    if (payload.action === 'update') {
      const p = payload as DashboardApiPayload;
      // Full replacement of dashboard
      set((s) => ({
        dashboards: {
          ...s.dashboards,
          [p.dashboard.meta.id]: {
            ...p.dashboard,
            meta: { ...p.dashboard.meta, updatedAt: now() },
          },
        },
        activeDashboardId: p.dashboard.meta.id,
      }));
      return;
    }

    if (payload.action === 'patch') {
      const p = payload as DashboardPatchPayload;
      const dashboardId = p.dashboardId;

      if (p.meta) {
        store.updateDashboardMeta(dashboardId, p.meta);
      }
      if (p.removeWidgetIds?.length) {
        store.removeWidgets(dashboardId, p.removeWidgetIds);
      }
      if (p.updateWidgets?.length) {
        for (const patch of p.updateWidgets) {
          store.updateWidget(dashboardId, patch);
        }
      }
      if (p.addWidgets?.length) {
        store.addWidgets(dashboardId, p.addWidgets);
      }
    }
  },

  /* ── Date filtering ── */
  applyDatePreset: async (preset) => {
    const { activeDashboardId } = get();
    if (!activeDashboardId) return;
    set({ filterLoading: true, error: null, activePreset: preset, customDateRange: null });
    try {
      const api = await getDashboardFiltered(activeDashboardId, { preset });
      console.group('[DateFilter] applyDatePreset:', preset);
      console.log('Raw API response:', JSON.parse(JSON.stringify(api)));
      console.log('Widgets chart_config samples:', api.widgets.slice(0, 3).map((w) => ({
        title: w.title, type: w.type,
        chart_config: w.chart_config,
        data_snapshot: w.data_snapshot,
      })));
      console.groupEnd();
      const dashboard = apiDashboardToDashboard(api);
      set((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
        filterLoading: false,
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message, filterLoading: false });
    }
  },

  applyCustomDateRange: async (range) => {
    const { activeDashboardId } = get();
    if (!activeDashboardId) return;
    set({ filterLoading: true, error: null, customDateRange: range, activePreset: null });
    try {
      const api = await getDashboardFiltered(activeDashboardId, {
        startDate: range.startDate,
        endDate: range.endDate,
      });
      console.group('[DateFilter] applyCustomDateRange:', range);
      console.log('Raw API response:', JSON.parse(JSON.stringify(api)));
      console.log('Widgets chart_config samples:', api.widgets.slice(0, 3).map((w) => ({
        title: w.title, type: w.type,
        chart_config: w.chart_config,
        data_snapshot: w.data_snapshot,
      })));
      console.groupEnd();
      const dashboard = apiDashboardToDashboard(api);
      set((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
        filterLoading: false,
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message, filterLoading: false });
    }
  },

  clearDateFilter: async () => {
    const { activeDashboardId } = get();
    if (!activeDashboardId) return;
    set({ filterLoading: true, error: null, activePreset: null, customDateRange: null });
    try {
      const api = await getDashboard(activeDashboardId);
      const dashboard = apiDashboardToDashboard(api);
      set((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
        filterLoading: false,
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message, filterLoading: false });
    }
  },
}));
