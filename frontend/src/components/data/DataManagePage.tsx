import { useCallback, useEffect, useState } from 'react';
import { Pause, Pencil, Play, Trash2 } from 'lucide-react';
import type { UserConnectorConfig } from '../../types';
import {
  deleteUserConnector,
  listUserConnectors,
  patchUserConnector,
} from '../../services/backendClient';
import { useDashboardStore } from '../../store/useDashboardStore';
import DataPageFrame from './DataPageFrame';

interface DataManagePageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

function formatWhen(iso: string | null | undefined): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

const DataManagePage = ({
  sidebarCollapsed,
  chatOpen,
  chatModal,
  chatWidthPx,
}: DataManagePageProps) => {
  const setNavigationPage = useDashboardStore((s) => s.setNavigationPage);
  const [rows, setRows] = useState<UserConnectorConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);

  const [editOpen, setEditOpen] = useState(false);
  const [editing, setEditing] = useState<UserConnectorConfig | null>(null);
  const [editName, setEditName] = useState('');
  const [editImage, setEditImage] = useState('');
  const [editFreq, setEditFreq] = useState(360);
  const [editError, setEditError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const data = await listUserConnectors({ forceRefresh: true });
      setRows(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load connections.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const openEdit = (r: UserConnectorConfig) => {
    setEditing(r);
    setEditName(r.connector_name);
    setEditImage(r.docker_image);
    setEditFreq(r.sync_frequency_minutes);
    setEditError(null);
    setEditOpen(true);
  };

  const closeEdit = () => {
    setEditOpen(false);
    setEditing(null);
  };

  const saveEdit = async () => {
    if (!editing) return;
    setEditError(null);
    setBusyId(editing.id);
    try {
      const updated = await patchUserConnector(editing.id, {
        connector_name: editName.trim() || editing.connector_name,
        docker_image: editImage.trim(),
        sync_frequency_minutes: editFreq,
      });
      setRows((prev) => prev.map((x) => (x.id === updated.id ? updated : x)));
      closeEdit();
    } catch (e) {
      setEditError(e instanceof Error ? e.message : 'Could not save.');
    } finally {
      setBusyId(null);
    }
  };

  const togglePause = async (r: UserConnectorConfig) => {
    setBusyId(r.id);
    setError(null);
    try {
      const updated = await patchUserConnector(r.id, { is_active: !r.is_active });
      setRows((prev) => prev.map((x) => (x.id === updated.id ? updated : x)));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not update connection.');
    } finally {
      setBusyId(null);
    }
  };

  const remove = async (r: UserConnectorConfig) => {
    if (
      !window.confirm(
        `Delete connection “${r.connector_name}”?\nThis cannot be undone.`,
      )
    ) {
      return;
    }
    setBusyId(r.id);
    setError(null);
    try {
      await deleteUserConnector(r.id);
      setRows((prev) => prev.filter((x) => x.id !== r.id));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not delete.');
    } finally {
      setBusyId(null);
    }
  };

  return (
    <DataPageFrame
      sidebarCollapsed={sidebarCollapsed}
      chatOpen={chatOpen}
      chatModal={chatModal}
      chatWidthPx={chatWidthPx}
    >
      <div className="flex flex-col min-h-0 max-w-4xl mx-auto w-full gap-4">
        <div>
          <h2 className="text-lg font-semibold text-[var(--text-primary)] tracking-tight">Manage connections</h2>
          <p className="text-sm text-[var(--text-secondary)] mt-1">
            Pause, edit, or remove saved data source configurations.
          </p>
        </div>

        {error && (
          <div
            className="rounded-xl border border-red-500/30 bg-red-950/30 px-4 py-3 text-sm text-red-200/95"
            role="alert"
          >
            {error}
          </div>
        )}

        {loading && (
          <div className="flex flex-col items-center justify-center gap-3 py-16 text-[var(--text-secondary)]">
            <div className="animate-spin text-[var(--brand)]">
              <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M21 12a9 9 0 1 1-6.219-8.56" />
              </svg>
            </div>
            <p className="text-sm font-medium">Loading connections…</p>
          </div>
        )}

        {!loading && rows.length === 0 && (
          <div className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-8 text-center">
            <p className="text-sm text-[var(--text-secondary)] mb-4">
              You do not have any saved connections yet. When onboarding is available, add sources from{' '}
              <button
                type="button"
                className="text-[var(--brand)] font-medium hover:underline"
                onClick={() => setNavigationPage('data-connect')}
              >
                Connect a new source
              </button>
              .
            </p>
          </div>
        )}

        {!loading && rows.length > 0 && (
          <ul className="flex flex-col gap-3 overflow-y-auto min-h-0 pr-1">
            {rows.map((r) => (
              <li
                key={r.id}
                className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-4 flex flex-col sm:flex-row sm:items-start sm:justify-between gap-3"
              >
                <div className="min-w-0 space-y-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-medium text-[var(--text-primary)] truncate">{r.connector_name}</span>
                    <span
                      className={`text-xs font-medium px-2 py-0.5 rounded-md border ${
                        r.is_active
                          ? 'border-emerald-500/30 text-emerald-300/95 bg-emerald-950/20'
                          : 'border-[var(--border-default)] text-[var(--text-muted)] bg-[var(--bg-surface-alt)]'
                      }`}
                    >
                      {r.is_active ? 'Active' : 'Paused'}
                    </span>
                  </div>
                  <p className="text-xs text-[var(--text-muted)] font-mono truncate" title={r.docker_repository}>
                    {r.docker_repository}
                  </p>
                  <p className="text-xs text-[var(--text-secondary)]">
                    Last sync: {formatWhen(r.last_sync_at)} · Status:{' '}
                    <span className="text-[var(--text-primary)]">{r.last_sync_status}</span>
                  </p>
                </div>
                <div className="flex flex-wrap items-center gap-2 shrink-0">
                  <button
                    type="button"
                    className="inline-flex items-center gap-1.5 rounded-lg border border-[var(--border-default)] px-3 py-1.5 text-xs font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-elevated)] hover:text-[var(--text-primary)] disabled:opacity-50 cursor-pointer transition-colors duration-200"
                    disabled={busyId === r.id}
                    onClick={() => togglePause(r)}
                    title={r.is_active ? 'Pause sync' : 'Resume sync'}
                  >
                    {r.is_active ? <Pause size={14} /> : <Play size={14} />}
                    {r.is_active ? 'Pause' : 'Resume'}
                  </button>
                  <button
                    type="button"
                    className="inline-flex items-center gap-1.5 rounded-lg border border-[var(--border-default)] px-3 py-1.5 text-xs font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-elevated)] hover:text-[var(--text-primary)] disabled:opacity-50 cursor-pointer transition-colors duration-200"
                    disabled={busyId === r.id}
                    onClick={() => openEdit(r)}
                  >
                    <Pencil size={14} />
                    Edit
                  </button>
                  <button
                    type="button"
                    className="inline-flex items-center gap-1.5 rounded-lg border border-red-500/25 px-3 py-1.5 text-xs font-medium text-red-300/95 hover:bg-red-950/30 disabled:opacity-50 cursor-pointer transition-colors duration-200"
                    disabled={busyId === r.id}
                    onClick={() => void remove(r)}
                  >
                    <Trash2 size={14} />
                    Delete
                  </button>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>

      {editOpen && editing && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60"
          role="dialog"
          aria-modal="true"
          aria-labelledby="edit-conn-title"
        >
          <div className="w-full max-w-md rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-6 shadow-xl">
            <h3 id="edit-conn-title" className="text-base font-semibold text-[var(--text-primary)] mb-4">
              Edit connection
            </h3>
            <div className="space-y-3">
              <label className="block">
                <span className="text-xs font-medium text-[var(--text-secondary)]">Display name</span>
                <input
                  className="mt-1 w-full rounded-lg border border-[var(--border-default)] bg-[var(--bg-canvas)] px-3 py-2 text-sm text-[var(--text-primary)]"
                  value={editName}
                  onChange={(e) => setEditName(e.target.value)}
                />
              </label>
              <label className="block">
                <span className="text-xs font-medium text-[var(--text-secondary)]">Docker image</span>
                <input
                  className="mt-1 w-full rounded-lg border border-[var(--border-default)] bg-[var(--bg-canvas)] px-3 py-2 text-sm font-mono text-[var(--text-primary)]"
                  value={editImage}
                  onChange={(e) => setEditImage(e.target.value)}
                />
              </label>
              <label className="block">
                <span className="text-xs font-medium text-[var(--text-secondary)]">Sync frequency (minutes)</span>
                <input
                  type="number"
                  min={1}
                  className="mt-1 w-full rounded-lg border border-[var(--border-default)] bg-[var(--bg-canvas)] px-3 py-2 text-sm text-[var(--text-primary)]"
                  value={editFreq}
                  onChange={(e) => setEditFreq(Number(e.target.value) || 1)}
                />
              </label>
            </div>
            {editError && <p className="mt-3 text-sm text-red-300/95">{editError}</p>}
            <div className="mt-6 flex justify-end gap-2">
              <button
                type="button"
                className="rounded-lg px-4 py-2 text-sm font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-elevated)]"
                onClick={closeEdit}
                disabled={busyId === editing.id}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded-lg px-4 py-2 text-sm font-medium bg-[var(--brand)] text-white hover:opacity-90 disabled:opacity-50"
                onClick={() => void saveEdit()}
                disabled={busyId === editing.id}
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </DataPageFrame>
  );
};

export default DataManagePage;
