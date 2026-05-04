import { create } from 'zustand';
import {
  listAlerts,
  createAlert,
  updateAlert,
  deleteAlert as deleteAlertApi,
  listAlertRuns,
  testAlert as testAlertApi,
  type ApiAlert,
  type ApiAlertRun,
  type AlertCreatePayload,
  type AlertUpdatePayload,
} from '../services/backendClient';

/* ─────────────────────────────────────────────
   Alert Store Types
───────────────────────────────────────────── */

interface AlertState {
  /** List of all user alerts */
  alerts: ApiAlert[];
  /** Whether the list is loading */
  loading: boolean;
  /** Error from the last operation */
  error: string | null;

  /** Selected alert ID (for detail view) */
  selectedAlertId: string | null;

  /** Run history for the selected alert */
  runs: ApiAlertRun[];
  runsLoading: boolean;

  /** Test result from the latest test evaluation */
  testResult: ApiAlertRun | null;
  testLoading: boolean;

  /** Actions */
  fetchAlerts: () => Promise<void>;
  selectAlert: (id: string | null) => void;
  addAlert: (payload: AlertCreatePayload) => Promise<ApiAlert>;
  patchAlert: (id: string, payload: AlertUpdatePayload) => Promise<void>;
  removeAlert: (id: string) => Promise<void>;
  toggleAlert: (id: string, enabled: boolean) => Promise<void>;
  fetchRuns: (alertId: string) => Promise<void>;
  runTest: (alertId: string) => Promise<void>;
  clearError: () => void;
}

/* ─────────────────────────────────────────────
   Store Implementation
───────────────────────────────────────────── */

export const useAlertStore = create<AlertState>((set, get) => ({
  alerts: [],
  loading: false,
  error: null,
  selectedAlertId: null,
  runs: [],
  runsLoading: false,
  testResult: null,
  testLoading: false,

  fetchAlerts: async () => {
    set({ loading: true, error: null });
    try {
      const alerts = await listAlerts();
      set({ alerts, loading: false });
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to fetch alerts';
      set({ error: msg, loading: false });
    }
  },

  selectAlert: (id) => {
    set({ selectedAlertId: id, runs: [], testResult: null });
    if (id) {
      void get().fetchRuns(id);
    }
  },

  addAlert: async (payload) => {
    set({ error: null });
    try {
      const alert = await createAlert(payload);
      set((s) => ({ alerts: [alert, ...s.alerts] }));
      return alert;
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to create alert';
      set({ error: msg });
      throw err;
    }
  },

  patchAlert: async (id, payload) => {
    set({ error: null });
    try {
      const updated = await updateAlert(id, payload);
      set((s) => ({
        alerts: s.alerts.map((a) => (a.id === id ? updated : a)),
      }));
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to update alert';
      set({ error: msg });
    }
  },

  removeAlert: async (id) => {
    set({ error: null });
    try {
      await deleteAlertApi(id);
      set((s) => ({
        alerts: s.alerts.filter((a) => a.id !== id),
        selectedAlertId: s.selectedAlertId === id ? null : s.selectedAlertId,
      }));
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to delete alert';
      set({ error: msg });
    }
  },

  toggleAlert: async (id, enabled) => {
    // Optimistic update
    set((s) => ({
      alerts: s.alerts.map((a) => (a.id === id ? { ...a, enabled } : a)),
    }));
    try {
      await updateAlert(id, { enabled });
    } catch (err) {
      // Revert
      set((s) => ({
        alerts: s.alerts.map((a) => (a.id === id ? { ...a, enabled: !enabled } : a)),
        error: err instanceof Error ? err.message : 'Toggle failed',
      }));
    }
  },

  fetchRuns: async (alertId) => {
    set({ runsLoading: true });
    try {
      const runs = await listAlertRuns(alertId);
      set({ runs, runsLoading: false });
    } catch (err) {
      set({ runsLoading: false, error: err instanceof Error ? err.message : 'Failed to fetch runs' });
    }
  },

  runTest: async (alertId) => {
    set({ testLoading: true, testResult: null });
    try {
      const result = await testAlertApi(alertId);
      set({ testResult: result, testLoading: false });
      // Refresh runs list
      void get().fetchRuns(alertId);
      // Refresh the alert to get updated last_state
      void get().fetchAlerts();
    } catch (err) {
      set({ testLoading: false, error: err instanceof Error ? err.message : 'Test failed' });
    }
  },

  clearError: () => set({ error: null }),
}));
