import { useEffect, useState } from 'react';
import {
  Bell,
  BellPlus,
  BellRing,
  Plus,
  Trash2,
  PlayCircle,
  Clock,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  ChevronRight,
  ToggleLeft,
  ToggleRight,
  Loader2,
} from 'lucide-react';
import DataPageFrame from './data/DataPageFrame';
import { useAlertStore } from '../store/useAlertStore';
import { useDashboardStore } from '../store/useDashboardStore';
import type { ApiAlert, ApiAlertRun } from '../services/backendClient';

interface AlertsPageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

/* ─────────────────────────────────────────────
   Status badge
───────────────────────────────────────────── */

const StatusBadge = ({ status }: { status: 'ok' | 'firing' | 'error' | null | undefined }) => {
  if (!status) return <span className="alerts-badge alerts-badge--muted">Never run</span>;
  if (status === 'firing')
    return (
      <span className="alerts-badge alerts-badge--firing">
        <BellRing size={12} /> Firing
      </span>
    );
  if (status === 'error')
    return (
      <span className="alerts-badge alerts-badge--error">
        <XCircle size={12} /> Error
      </span>
    );
  return (
    <span className="alerts-badge alerts-badge--ok">
      <CheckCircle2 size={12} /> OK
    </span>
  );
};

/* ─────────────────────────────────────────────
   Comparator display
───────────────────────────────────────────── */

const COMP_LABELS: Record<string, string> = {
  gt: '>', gte: '≥', lt: '<', lte: '≤', eq: '=', neq: '≠',
  pct_change_gt: '% change >', pct_change_lt: '% change <',
};

/* ─────────────────────────────────────────────
   Run History Row
───────────────────────────────────────────── */

const RunRow = ({ run }: { run: ApiAlertRun }) => {
  const ts = new Date(run.evaluated_at).toLocaleString();
  return (
    <div className="alerts-run-row">
      <div className="alerts-run-status">
        {run.status === 'ok' && <CheckCircle2 size={14} className="text-emerald-400" />}
        {run.status === 'firing' && <BellRing size={14} className="text-amber-400" />}
        {run.status === 'error' && <XCircle size={14} className="text-rose-400" />}
        <span className="text-xs font-medium capitalize">{run.status}</span>
      </div>
      <span className="text-xs text-[var(--text-secondary)]">{ts}</span>
      {run.observed_value != null && (
        <span className="text-xs font-mono text-[var(--text-primary)]">{run.observed_value}</span>
      )}
      {run.error_message && (
        <span className="text-xs text-rose-400 truncate max-w-[200px]" title={run.error_message}>
          {run.error_message}
        </span>
      )}
    </div>
  );
};

/* ─────────────────────────────────────────────
   Detail Panel
───────────────────────────────────────────── */

const DetailPanel = ({ alert }: { alert: ApiAlert }) => {
  const runs = useAlertStore((s) => s.runs);
  const runsLoading = useAlertStore((s) => s.runsLoading);
  const testResult = useAlertStore((s) => s.testResult);
  const testLoading = useAlertStore((s) => s.testLoading);
  const runTest = useAlertStore((s) => s.runTest);
  const removeAlert = useAlertStore((s) => s.removeAlert);
  const toggleAlert = useAlertStore((s) => s.toggleAlert);
  const fetchRuns = useAlertStore((s) => s.fetchRuns);
  const selectAlert = useAlertStore((s) => s.selectAlert);
  const [confirming, setConfirming] = useState(false);

  const defn = (alert.definition ?? {}) as Record<string, unknown>;
  const comp = COMP_LABELS[alert.comparator] ?? alert.comparator;

  useEffect(() => {
    fetchRuns(alert.id);
  }, [alert.id, fetchRuns]);

  return (
    <div className="alerts-detail">
      {/* ── Header ── */}
      <div className="alerts-detail-header">
        <div className="alerts-detail-header-left">
          <div className="alerts-detail-icon-wrap">
            <Bell size={16} />
          </div>
          <div className="min-w-0">
            <h2 className="text-base font-semibold text-[var(--text-primary)] truncate leading-tight">{alert.name}</h2>
            {alert.description && (
              <p className="text-xs text-[var(--text-secondary)] mt-0.5 leading-relaxed">{alert.description}</p>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <StatusBadge status={alert.last_state} />
          <button
            type="button"
            onClick={() => toggleAlert(alert.id, !alert.enabled)}
            className="alerts-toggle-chip"
            title={alert.enabled ? 'Pause alert' : 'Enable alert'}
          >
            {alert.enabled ? (
              <><ToggleRight size={14} className="text-emerald-400" /><span>On</span></>
            ) : (
              <><ToggleLeft size={14} className="text-[var(--text-muted)]" /><span>Off</span></>
            )}
          </button>
        </div>
      </div>

      {/* ── Scrollable content ── */}
      <div className="alerts-detail-body">
        {/* Metadata grid */}
        <div className="alerts-detail-meta">
          <div className="alerts-meta-row">
            <span className="alerts-meta-label">Condition</span>
            <span className="text-sm font-mono text-[var(--text-primary)]">
              value {comp} {alert.threshold}
            </span>
          </div>
          {(defn.metric_description as string) && (
            <div className="alerts-meta-row">
              <span className="alerts-meta-label">Metric</span>
              <span className="text-sm text-[var(--text-primary)]">{defn.metric_description as string}</span>
            </div>
          )}
          <div className="alerts-meta-row">
            <span className="alerts-meta-label">Channel</span>
            <span className="text-sm text-[var(--text-primary)] capitalize">{alert.notification_channel}</span>
          </div>
          {alert.notification_target && (
            <div className="alerts-meta-row">
              <span className="alerts-meta-label">Target</span>
              <span className="text-sm text-[var(--text-primary)]">{alert.notification_target}</span>
            </div>
          )}
          {alert.last_evaluated_at && (
            <div className="alerts-meta-row">
              <span className="alerts-meta-label">Last run</span>
              <span className="text-sm text-[var(--text-secondary)]">
                {new Date(alert.last_evaluated_at).toLocaleString()}
              </span>
            </div>
          )}
        </div>

        {/* SQL Query */}
        <details className="alerts-sql-details">
          <summary className="alerts-sql-summary">
            <span>SQL Query</span>
            <ChevronRight size={14} className="alerts-sql-chevron" />
          </summary>
          <pre className="alerts-sql-block">{alert.sql_query}</pre>
        </details>

        {/* Test result */}
        {testResult && (
          <div className="alerts-test-result">
            <h4 className="text-xs font-semibold text-[var(--text-secondary)] mb-1.5">Test Result</h4>
            <RunRow run={testResult} />
          </div>
        )}

        {/* Run history */}
        <div className="alerts-runs-section">
          <div className="alerts-runs-header">
            <Clock size={13} className="text-[var(--text-muted)]" />
            <h3 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-muted)]">
              Run History
            </h3>
          </div>
          {runsLoading ? (
            <div className="flex items-center gap-2 text-xs text-[var(--text-secondary)] py-2">
              <Loader2 size={14} className="animate-spin" /> Loading...
            </div>
          ) : runs.length === 0 ? (
            <div className="alerts-runs-empty">
              <Clock size={16} className="text-[var(--text-muted)]" />
              <p className="text-xs text-[var(--text-secondary)]">No runs yet. Use "Test now" to evaluate.</p>
            </div>
          ) : (
            <div className="alerts-runs-list">
              {runs.map((r) => (
                <RunRow key={r.id} run={r} />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* ── Bottom actions bar ── */}
      <div className="alerts-detail-actions">
        <button
          type="button"
          onClick={() => runTest(alert.id)}
          disabled={testLoading}
          className="alerts-action-btn alerts-action-btn--primary"
        >
          {testLoading ? <Loader2 size={14} className="animate-spin" /> : <PlayCircle size={14} />}
          <span>Test now</span>
        </button>

        <div className="flex-1" />

        {!confirming ? (
          <button
            type="button"
            onClick={() => setConfirming(true)}
            className="alerts-action-btn alerts-action-btn--danger"
          >
            <Trash2 size={14} />
            <span>Delete</span>
          </button>
        ) : (
          <div className="flex gap-1.5 items-center">
            <span className="text-xs text-rose-400">Confirm?</span>
            <button
              type="button"
              onClick={async () => {
                await removeAlert(alert.id);
                selectAlert(null);
              }}
              className="alerts-action-btn alerts-action-btn--danger"
            >
              Yes
            </button>
            <button
              type="button"
              onClick={() => setConfirming(false)}
              className="alerts-action-btn"
            >
              No
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

/* ─────────────────────────────────────────────
   Alert Card (list item)
───────────────────────────────────────────── */

const AlertCard = ({
  alert,
  isSelected,
  onSelect,
}: {
  alert: ApiAlert;
  isSelected: boolean;
  onSelect: () => void;
}) => {
  const comp = COMP_LABELS[alert.comparator] ?? alert.comparator;
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`alerts-card ${isSelected ? 'alerts-card--selected' : ''} ${!alert.enabled ? 'alerts-card--disabled' : ''}`}
    >
      <div className="alerts-card-top">
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <Bell
            size={16}
            className={
              alert.last_state === 'firing'
                ? 'text-amber-400 shrink-0'
                : alert.enabled
                  ? 'text-[var(--brand)] shrink-0'
                  : 'text-[var(--text-muted)] shrink-0'
            }
          />
          <span className="text-sm font-medium text-[var(--text-primary)] truncate">{alert.name}</span>
        </div>
        <ChevronRight size={14} className="text-[var(--text-muted)] shrink-0" />
      </div>

      <div className="alerts-card-bottom">
        <span className="text-xs text-[var(--text-secondary)]">
          {comp} {alert.threshold}
        </span>
        <StatusBadge status={alert.last_state} />
      </div>
    </button>
  );
};

/* ─────────────────────────────────────────────
   Empty State
───────────────────────────────────────────── */

const EmptyState = ({ onCreateClick }: { onCreateClick: () => void }) => (
  <div className="alerts-empty">
    <div className="alerts-empty-icon">
      <Bell size={32} strokeWidth={1.5} />
    </div>
    <h2 className="text-lg font-semibold text-[var(--text-primary)] tracking-tight">No alerts yet</h2>
    <p className="text-sm text-[var(--text-secondary)] max-w-sm text-center leading-relaxed mt-1">
      Set up data-driven alerts to get notified when your metrics cross thresholds.
    </p>
    <button
      type="button"
      onClick={onCreateClick}
      className="mt-4 inline-flex items-center gap-2 rounded-xl border border-[var(--brand)] bg-[var(--brand-dim)] px-5 py-2.5 text-sm font-semibold text-[var(--brand)] cursor-pointer transition-colors duration-200 hover:bg-[rgba(139,92,246,0.2)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--brand)]"
    >
      <BellPlus size={16} />
      Create your first alert
    </button>
  </div>
);

/* ─────────────────────────────────────────────
   Main Page
───────────────────────────────────────────── */

const AlertsPage = ({ sidebarCollapsed, chatOpen, chatModal, chatWidthPx }: AlertsPageProps) => {
  const alerts = useAlertStore((s) => s.alerts);
  const loading = useAlertStore((s) => s.loading);
  const error = useAlertStore((s) => s.error);
  const selectedAlertId = useAlertStore((s) => s.selectedAlertId);
  const fetchAlerts = useAlertStore((s) => s.fetchAlerts);
  const selectAlert = useAlertStore((s) => s.selectAlert);
  const clearError = useAlertStore((s) => s.clearError);
  const setNavigationPage = useDashboardStore((s) => s.setNavigationPage);

  const goToCreate = () => setNavigationPage('alerts-create');

  const selectedAlert = alerts.find((a) => a.id === selectedAlertId) ?? null;

  useEffect(() => {
    fetchAlerts();
  }, [fetchAlerts]);

  return (
    <DataPageFrame
      sidebarCollapsed={sidebarCollapsed}
      chatOpen={chatOpen}
      chatModal={chatModal}
      chatWidthPx={chatWidthPx}
    >
      <div className="mx-auto w-full max-w-7xl pb-8">
        <header className="mb-5">
          <div className="flex items-center justify-between gap-4 mb-1">
            <div>
              <p className="text-xs font-semibold uppercase tracking-wider text-[var(--brand)] mb-1.5">
                Alerts
              </p>
              <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight text-[var(--text-primary)]">
                Manage Alerts
              </h1>
            </div>
            {alerts.length > 0 && (
              <button
                type="button"
                onClick={goToCreate}
                className="shrink-0 inline-flex items-center gap-2 rounded-xl bg-[var(--brand)] px-4 py-2 text-sm font-semibold text-white cursor-pointer transition-all duration-200 hover:opacity-90 hover:shadow-[0_4px_16px_rgba(139,92,246,0.3)] active:scale-[0.98]"
              >
                <Plus size={15} />
                New Alert
              </button>
            )}
          </div>
          <p className="text-[0.9375rem] leading-relaxed text-[var(--text-secondary)]">
            Monitor your metrics and get notified when thresholds are crossed.
          </p>
        </header>

        {/* Error banner */}
        {error && (
          <div className="alerts-error-banner">
            <AlertTriangle size={14} />
            <span className="text-xs">{error}</span>
            <button type="button" onClick={clearError} className="text-xs underline ml-auto">
              Dismiss
            </button>
          </div>
        )}

        {loading && alerts.length === 0 ? (
          <div className="flex items-center justify-center py-20 gap-2 text-[var(--text-secondary)]">
            <Loader2 size={20} className="animate-spin" />
            <span className="text-sm">Loading alerts...</span>
          </div>
        ) : alerts.length === 0 ? (
          <EmptyState onCreateClick={goToCreate} />
        ) : (
          <div className="alerts-layout">
            {/* Alert list */}
            <div className="alerts-list">
              <div className="alerts-list-header">
                <span className="text-xs font-semibold uppercase tracking-wider text-[var(--text-muted)]">
                  {alerts.length} alert{alerts.length !== 1 ? 's' : ''}
                </span>
              </div>
              <div className="alerts-list-scroll">
                {alerts.map((a) => (
                  <AlertCard
                    key={a.id}
                    alert={a}
                    isSelected={a.id === selectedAlertId}
                    onSelect={() => selectAlert(a.id === selectedAlertId ? null : a.id)}
                  />
                ))}
              </div>
            </div>

            {/* Detail panel */}
            <div className="alerts-detail-panel">
              {selectedAlert ? (
                <DetailPanel alert={selectedAlert} />
              ) : (
                <div className="alerts-detail-empty">
                  <div className="alerts-detail-empty-icon">
                    <Bell size={22} />
                  </div>
                  <p className="text-sm text-[var(--text-secondary)]">Select an alert to view details</p>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </DataPageFrame>
  );
};

export default AlertsPage;
