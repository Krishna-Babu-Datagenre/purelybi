import {
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { createPortal } from 'react-dom';
import {
  AlertCircle,
  Calendar,
  ChevronDown,
  ChevronRight,
  Database,
  Download,
  Eye,
  FolderTree,
  RefreshCw,
  Search,
  Table,
  X,
} from 'lucide-react';
import type { RawTablePreview, StreamInventoryItem, SyncedTableInfo } from '../../types';
import {
  RAW_PREVIEW_PAGE_SIZE,
  downloadRawStreamZip,
  fetchRawTablePreview,
  listSyncedTablesMetadata,
} from '../../services/backendClient';
import { useDashboardStore } from '../../store/useDashboardStore';
import DataPageFrame from './DataPageFrame';

interface DataRawTablesPageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

type RawPreset = '7d' | '14d' | '30d' | '6m' | '1y' | 'custom';

const RAW_PRESETS: { id: Exclude<RawPreset, 'custom'>; label: string }[] = [
  { id: '7d', label: 'Last 7 days' },
  { id: '14d', label: 'Last 14 days' },
  { id: '30d', label: 'Last 30 days' },
  { id: '6m', label: 'Last 6 months' },
  { id: '1y', label: 'Last year' },
];

function toIsoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function computeRangeForPreset(preset: Exclude<RawPreset, 'custom'>): {
  startDate: string;
  endDate: string;
} {
  const end = new Date();
  end.setHours(0, 0, 0, 0);
  const start = new Date(end);
  switch (preset) {
    case '7d':
      start.setDate(start.getDate() - 6);
      break;
    case '14d':
      start.setDate(start.getDate() - 13);
      break;
    case '30d':
      start.setDate(start.getDate() - 29);
      break;
    case '6m':
      start.setMonth(start.getMonth() - 6);
      break;
    case '1y':
      start.setFullYear(start.getFullYear() - 1);
      break;
    default:
      start.setDate(start.getDate() - 29);
  }
  return { startDate: toIsoDate(start), endDate: toIsoDate(end) };
}

function formatWhen(iso: string | null | undefined): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

function formatRelativeSync(iso: string | null | undefined): string {
  if (!iso) return 'Never synced';
  try {
    const t = new Date(iso).getTime();
    const diff = Date.now() - t;
    const sec = Math.floor(diff / 1000);
    if (sec < 60) return 'Just now';
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m ago`;
    const h = Math.floor(min / 60);
    if (h < 48) return `${h}h ago`;
    const d = Math.floor(h / 24);
    if (d < 14) return `${d}d ago`;
    return formatWhen(iso);
  } catch {
    return formatWhen(iso);
  }
}

function humanizeStatus(status: string): string {
  const s = status.replace(/_/g, ' ').trim();
  if (!s) return status;
  return s.replace(/\b\w/g, (c) => c.toUpperCase());
}

function statusBadgeClass(status: string): string {
  const normalized = status.toLowerCase();
  if (normalized === 'success')
    return 'bg-emerald-500/15 text-emerald-300 border-emerald-400/30';
  if (normalized === 'running' || normalized === 'queued')
    return 'bg-blue-500/15 text-blue-300 border-blue-400/30';
  if (normalized === 'failed' || normalized === 'reauth_required')
    return 'bg-red-500/15 text-red-300 border-red-400/30';
  return 'bg-[var(--bg-surface-alt)] text-[var(--text-secondary)] border-[var(--border-subtle)]';
}

function formatBytes(n: number | null | undefined): string {
  if (n == null || n <= 0) return '—';
  const u = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i += 1;
  }
  const rounded = i === 0 && v < 10 ? v : v < 10 && i > 0 ? Math.round(v * 10) / 10 : Math.round(v);
  return `${rounded} ${u[i]}`;
}

function formatPreviewCell(v: string | number | boolean | null | undefined): string {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return String(v);
}

function streamRowsForConnector(
  row: SyncedTableInfo,
): StreamInventoryItem[] {
  if (row.stream_inventory && row.stream_inventory.length > 0) {
    return row.stream_inventory;
  }
  return row.synced_tables.map((stream) => ({ stream, months: [] }));
}

const DataRawTablesPage = ({
  sidebarCollapsed,
  chatOpen,
  chatModal,
  chatWidthPx,
}: DataRawTablesPageProps) => {
  const setNavigationPage = useDashboardStore((s) => s.setNavigationPage);
  const [rows, setRows] = useState<SyncedTableInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState('');
  const deferredQuery = useDeferredValue(query);

  const [preset, setPreset] = useState<RawPreset>('30d');
  const [customRange, setCustomRange] = useState<{ start: string; end: string } | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [startInput, setStartInput] = useState('');
  const [endInput, setEndInput] = useState('');
  const triggerRef = useRef<HTMLButtonElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [dropdownPos, setDropdownPos] = useState<{ top: number; right: number } | null>(null);

  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  const [downloadingKey, setDownloadingKey] = useState<string | null>(null);

  const [tablePreview, setTablePreview] = useState<{
    configId: string;
    stream: string;
    offset: number;
  } | null>(null);
  const [previewUi, setPreviewUi] = useState<{
    data: RawTablePreview | null;
    loading: boolean;
    error: string | null;
  }>({ data: null, loading: false, error: null });

  const range = useMemo(() => {
    if (preset === 'custom') {
      if (customRange) return { startDate: customRange.start, endDate: customRange.end };
      return computeRangeForPreset('30d');
    }
    return computeRangeForPreset(preset);
  }, [preset, customRange]);

  const load = useCallback(
    async (isRefresh = false) => {
      setError(null);
      if (isRefresh) setRefreshing(true);
      else setLoading(true);
      try {
        const data = await listSyncedTablesMetadata({
          forceRefresh: true,
          startDate: range.startDate,
          endDate: range.endDate,
        });
        setRows(data);
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to load sync metadata.');
      } finally {
        if (isRefresh) setRefreshing(false);
        else setLoading(false);
      }
    },
    [range.startDate, range.endDate],
  );

  useEffect(() => {
    void load(false);
  }, [load]);

  const updatePosition = useCallback(() => {
    if (!triggerRef.current) return;
    const rect = triggerRef.current.getBoundingClientRect();
    setDropdownPos({
      top: rect.bottom + 6,
      right: window.innerWidth - rect.right,
    });
  }, []);

  useEffect(() => {
    if (!pickerOpen) return;
    updatePosition();
    const handleClick = (e: MouseEvent) => {
      const target = e.target as Node;
      if (triggerRef.current?.contains(target) || dropdownRef.current?.contains(target)) return;
      setPickerOpen(false);
    };
    document.addEventListener('mousedown', handleClick);
    window.addEventListener('resize', updatePosition);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      window.removeEventListener('resize', updatePosition);
    };
  }, [pickerOpen, updatePosition]);

  useEffect(() => {
    setExpanded((prev) => {
      const next = { ...prev };
      for (const r of rows) {
        if (next[r.connector_config_id] === undefined) next[r.connector_config_id] = true;
      }
      return next;
    });
  }, [rows]);

  useEffect(() => {
    if (!tablePreview) return;
    let cancelled = false;
    setPreviewUi({ data: null, loading: true, error: null });
    void fetchRawTablePreview(tablePreview.configId, tablePreview.stream, range, {
      offset: tablePreview.offset,
      limit: RAW_PREVIEW_PAGE_SIZE,
    })
      .then((data) => {
        if (!cancelled) setPreviewUi({ data, loading: false, error: null });
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setPreviewUi({
            data: null,
            loading: false,
            error: e instanceof Error ? e.message : 'Failed to load preview.',
          });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [tablePreview, range.startDate, range.endDate]);

  useEffect(() => {
    if (!tablePreview) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setTablePreview(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [tablePreview]);

  useEffect(() => {
    if (!tablePreview) {
      setPreviewUi({ data: null, loading: false, error: null });
    }
  }, [tablePreview]);

  const filteredRows = useMemo(() => {
    const q = deferredQuery.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => {
      if ((r.connector_name || '').toLowerCase().includes(q)) return true;
      if ((r.docker_repository || '').toLowerCase().includes(q)) return true;
      if (r.synced_tables.some((t) => t.toLowerCase().includes(q))) return true;
      return streamRowsForConnector(r).some((s) => s.stream.toLowerCase().includes(q));
    });
  }, [rows, deferredQuery]);

  const stats = useMemo(() => {
    let streamsWithData = 0;
    let totalBytes = 0;
    let totalMonthFiles = 0;
    for (const r of rows) {
      for (const inv of streamRowsForConnector(r)) {
        const bytes = inv.months.reduce((s, m) => s + (m.size_bytes ?? 0), 0);
        if (inv.months.length > 0) {
          streamsWithData += 1;
          totalMonthFiles += inv.months.length;
        }
        totalBytes += bytes;
      }
    }
    return { streamsWithData, totalBytes, totalMonthFiles };
  }, [rows]);

  const hasTableNames = rows.some((r) => r.synced_tables.length > 0);

  const rangeLabel = useMemo(() => {
    if (preset === 'custom' && customRange) {
      return 'Custom range';
    }
    const match = RAW_PRESETS.find((p) => p.id === preset);
    if (match) return match.label;
    return `${range.startDate} → ${range.endDate}`;
  }, [preset, customRange, range.startDate, range.endDate]);

  const openTablePreview = (configId: string, stream: string) => {
    setTablePreview({ configId, stream, offset: 0 });
  };

  const handleDownload = async (configId: string, stream: string) => {
    const key = `${configId}::${stream}`;
    setDownloadingKey(key);
    try {
      await downloadRawStreamZip(configId, stream, {
        startDate: range.startDate,
        endDate: range.endDate,
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Download failed.');
    } finally {
      setDownloadingKey(null);
    }
  };

  const openCustomPicker = () => {
    setStartInput(range.startDate);
    setEndInput(range.endDate);
    setPickerOpen(true);
    setTimeout(updatePosition, 0);
  };

  const applyCustomRange = () => {
    if (!startInput || !endInput) return;
    if (startInput > endInput) {
      setError('Custom range: start date must be on or before end date.');
      return;
    }
    setError(null);
    setCustomRange({ start: startInput, end: endInput });
    setPreset('custom');
    setPickerOpen(false);
  };

  return (
    <DataPageFrame
      sidebarCollapsed={sidebarCollapsed}
      chatOpen={chatOpen}
      chatModal={chatModal}
      chatWidthPx={chatWidthPx}
    >
      <div className="flex flex-col min-h-0 max-w-5xl mx-auto w-full gap-5 pb-6">
        {/* Header + compact controls */}
        <div className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-4 sm:p-5 shadow-sm shadow-black/15">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0 flex items-start gap-2.5">
              <FolderTree size={20} className="shrink-0 text-[var(--brand)] mt-0.5" aria-hidden />
              <div>
                <h2 className="text-lg sm:text-xl font-semibold text-[var(--text-primary)] tracking-tight">
                  View raw tables
                </h2>
                <p className="text-sm text-[var(--text-secondary)] mt-1 max-w-2xl leading-snug">
                  Filter by time range, search connections, then open a stream to preview or download.
                </p>
              </div>
            </div>
            <button
              type="button"
              onClick={() => void load(true)}
              disabled={loading || refreshing}
              className="inline-flex items-center gap-2 rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-3 py-2 text-sm font-medium text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] hover:border-[var(--border-strong)] disabled:opacity-60 disabled:cursor-not-allowed transition-colors duration-200 cursor-pointer shrink-0"
            >
              <RefreshCw
                size={16}
                className={refreshing ? 'motion-safe:animate-spin' : ''}
                aria-hidden
              />
              {refreshing ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>

          <div className="mt-5 space-y-3">
            <label htmlFor="raw-tables-search" className="sr-only">
              Search connections and streams
            </label>
            <div className="relative">
              <Search
                size={17}
                className="absolute left-3 top-1/2 -translate-y-1/2 text-[var(--text-muted)] pointer-events-none"
                aria-hidden
              />
              <input
                id="raw-tables-search"
                type="search"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search connections, sources, or tables…"
                autoComplete="off"
                className="w-full rounded-lg border border-[var(--border-default)] bg-[var(--bg-canvas)] pl-10 pr-3 py-2.5 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:ring-2 focus:ring-[var(--brand)]/35 focus:border-[var(--brand)]/40"
                aria-label="Search connections and streams"
              />
            </div>

            <div className="flex flex-wrap items-center gap-x-2 gap-y-2">
              {RAW_PRESETS.map(({ id, label }) => (
                <button
                  key={id}
                  type="button"
                  disabled={loading}
                  onClick={() => {
                    setPreset(id);
                    setCustomRange(null);
                    setError(null);
                  }}
                  className={`rounded-md px-2.5 py-1.5 text-xs sm:text-sm font-medium transition-colors duration-200 cursor-pointer border ${
                    preset === id
                      ? 'border-[var(--brand)] bg-[var(--brand-dim)] text-[var(--text-primary)]'
                      : 'border-[var(--border-default)] bg-[var(--bg-surface-alt)] text-[var(--text-secondary)] hover:border-[var(--border-strong)] hover:text-[var(--text-primary)]'
                  } disabled:opacity-50 disabled:cursor-not-allowed`}
                >
                  {label}
                </button>
              ))}
              <button
                ref={triggerRef}
                type="button"
                disabled={loading}
                onClick={() => {
                  if (pickerOpen) setPickerOpen(false);
                  else openCustomPicker();
                }}
                className={`rounded-md px-2.5 py-1.5 text-xs sm:text-sm font-medium transition-colors duration-200 cursor-pointer border inline-flex items-center gap-1.5 ${
                  preset === 'custom'
                    ? 'border-[var(--brand)] bg-[var(--brand-dim)] text-[var(--text-primary)]'
                    : 'border-[var(--border-default)] bg-[var(--bg-surface-alt)] text-[var(--text-secondary)] hover:border-[var(--border-strong)] hover:text-[var(--text-primary)]'
                } disabled:opacity-50 disabled:cursor-not-allowed`}
              >
                <Calendar size={14} aria-hidden />
                {preset === 'custom' && customRange
                  ? `${customRange.start} → ${customRange.end}`
                  : 'Custom'}
              </button>
            </div>

            <p className="text-xs text-[var(--text-muted)] leading-relaxed pt-0.5">
              <span className="text-[var(--text-secondary)]">{rangeLabel}</span>
              <span className="mx-1.5 text-[var(--border-strong)]" aria-hidden>
                ·
              </span>
              <span className="tabular-nums">
                {range.startDate} – {range.endDate}
              </span>
              {!loading && (
                <>
                  <span className="mx-1.5 text-[var(--border-strong)]" aria-hidden>
                    ·
                  </span>
                  <span className="tabular-nums">{rows.length} connection{rows.length !== 1 ? 's' : ''}</span>
                  <span className="mx-1.5 text-[var(--border-strong)]" aria-hidden>
                    ·
                  </span>
                  <span className="tabular-nums">
                    {stats.streamsWithData} stream{stats.streamsWithData !== 1 ? 's' : ''}
                  </span>
                  <span className="mx-1.5 text-[var(--border-strong)]" aria-hidden>
                    ·
                  </span>
                  <span className="tabular-nums">{stats.totalMonthFiles} period{stats.totalMonthFiles !== 1 ? 's' : ''}</span>
                  <span className="mx-1.5 text-[var(--border-strong)]" aria-hidden>
                    ·
                  </span>
                  <span className="tabular-nums">{formatBytes(stats.totalBytes)}</span>
                </>
              )}
            </p>
          </div>

          {pickerOpen && dropdownPos && createPortal(
            <div
              ref={dropdownRef}
              className="date-picker-dropdown z-[100]"
              style={{ position: 'fixed', top: dropdownPos.top, right: dropdownPos.right }}
            >
              <label className="date-picker-label">
                Start date
                <input
                  type="date"
                  value={startInput}
                  onChange={(e) => setStartInput(e.target.value)}
                  className="date-picker-input"
                />
              </label>
              <label className="date-picker-label">
                End date
                <input
                  type="date"
                  value={endInput}
                  onChange={(e) => setEndInput(e.target.value)}
                  className="date-picker-input"
                />
              </label>
              <button
                type="button"
                disabled={!startInput || !endInput || loading}
                onClick={applyCustomRange}
                className="date-picker-apply"
              >
                Apply range
              </button>
            </div>,
            document.body,
          )}
        </div>

        {error && (
          <div
            className="rounded-xl border border-red-500/30 bg-red-950/30 px-4 py-3 text-sm text-red-200/95 flex items-start gap-2"
            role="alert"
          >
            <AlertCircle size={16} className="shrink-0 mt-0.5" aria-hidden />
            <span>{error}</span>
            <button
              type="button"
              onClick={() => setError(null)}
              className="ml-auto p-0.5 rounded hover:bg-red-500/20 cursor-pointer transition-colors"
              aria-label="Dismiss error"
            >
              <X size={16} />
            </button>
          </div>
        )}

        {loading && (
          <div className="space-y-3" aria-busy="true" aria-live="polite">
            {[0, 1, 2].map((i) => (
              <div key={i} className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-4">
                <div className="h-4 w-1/3 rounded bg-[var(--bg-surface-alt)] motion-safe:animate-pulse" />
                <div className="h-3 w-2/3 rounded bg-[var(--bg-surface-alt)] mt-2 motion-safe:animate-pulse" />
                <div className="h-3 w-full rounded bg-[var(--bg-surface-alt)] mt-4 motion-safe:animate-pulse" />
              </div>
            ))}
            <div className="flex items-center justify-center gap-2 text-sm text-[var(--text-secondary)] pt-2">
              <RefreshCw size={14} className="motion-safe:animate-spin" aria-hidden />
              Loading raw table metadata…
            </div>
          </div>
        )}

        {!loading && rows.length === 0 && (
          <div className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-10 text-center">
            <Table className="mx-auto mb-3 text-[var(--text-muted)]" size={32} strokeWidth={1.25} aria-hidden />
            <p className="text-sm text-[var(--text-secondary)] mb-2">
              No data sources configured yet, so there is nothing to show here.
            </p>
            <p className="text-sm text-[var(--text-muted)]">
              Add a connection from{' '}
              <button
                type="button"
                className="text-[var(--brand)] font-medium hover:underline cursor-pointer"
                onClick={() => setNavigationPage('data-connect')}
              >
                Connect a new source
              </button>{' '}
              (when available) or{' '}
              <button
                type="button"
                className="text-[var(--brand)] font-medium hover:underline cursor-pointer"
                onClick={() => setNavigationPage('data-manage')}
              >
                Manage
              </button>{' '}
              if you already have configs in the database.
            </p>
          </div>
        )}

        {!loading && rows.length > 0 && !hasTableNames && (
          <div className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface-alt)]/80 px-4 py-3 text-sm text-[var(--text-secondary)]">
            Table names for your connections are not available yet. After the next successful sync, streams will appear
            here automatically.
          </div>
        )}

        {!loading && rows.length > 0 && filteredRows.length === 0 && (
          <div className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] px-4 py-5 text-sm text-[var(--text-secondary)] flex items-center gap-2">
            <AlertCircle size={16} className="text-[var(--text-muted)] shrink-0" />
            No connectors or streams match your filter.
          </div>
        )}

        {!loading && filteredRows.length > 0 && (
          <ul className="flex flex-col gap-3 overflow-y-auto min-h-0 pr-0.5">
            {filteredRows.map((r) => {
              const isOpen = expanded[r.connector_config_id] !== false;
              const streamRows = streamRowsForConnector(r);
              return (
                <li
                  key={r.connector_config_id}
                  className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] overflow-hidden transition-shadow duration-200 hover:shadow-md hover:shadow-black/20"
                >
                  <button
                    type="button"
                    onClick={() =>
                      setExpanded((prev) => ({
                        ...prev,
                        [r.connector_config_id]: !isOpen,
                      }))
                    }
                    className="w-full flex flex-wrap items-start gap-3 p-4 text-left cursor-pointer transition-colors duration-200 hover:bg-[var(--bg-surface-alt)]/80"
                    aria-expanded={isOpen}
                  >
                    <span className="mt-0.5 text-[var(--text-muted)] shrink-0" aria-hidden>
                      {isOpen ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
                    </span>
                    <div className="flex-1 min-w-0 space-y-2">
                      <div className="flex flex-wrap items-center gap-2">
                        <Database size={16} className="text-[var(--text-muted)] shrink-0" aria-hidden />
                        <span className="font-semibold text-[var(--text-primary)] truncate">{r.connector_name}</span>
                        <span
                          className={`text-[11px] px-2 py-0.5 rounded-full border shrink-0 ${statusBadgeClass(r.last_sync_status)}`}
                        >
                          {humanizeStatus(r.last_sync_status)}
                        </span>
                      </div>
                      <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-[var(--text-secondary)]">
                        <span title={formatWhen(r.last_sync_at)}>
                          Last sync:{' '}
                          <span className="text-[var(--text-primary)] font-medium">
                            {formatRelativeSync(r.last_sync_at)}
                          </span>
                        </span>
                        <span className="text-[var(--text-muted)] font-mono truncate max-w-full" title={r.docker_repository}>
                          {r.docker_repository}
                        </span>
                      </div>
                      {r.last_sync_error ? (
                        <p className="text-xs text-red-300/90">{r.last_sync_error}</p>
                      ) : null}
                    </div>
                  </button>

                  {isOpen && (
                    <div className="border-t border-[var(--border-subtle)] px-4 pb-4 pt-3 bg-[var(--bg-canvas)]/40">
                      <div>
                        <span className="text-[11px] font-semibold text-[var(--text-muted)] uppercase tracking-wide">
                          Streams
                        </span>
                        {streamRows.length === 0 ? (
                          <p className="text-sm text-[var(--text-muted)] mt-2">No streams listed yet.</p>
                        ) : (
                          <ul className="mt-2 space-y-2">
                            {streamRows.map((s) => {
                              const monthCount = s.months.length;
                              const streamBytes = s.months.reduce((acc, m) => acc + (m.size_bytes ?? 0), 0);
                              const dlKey = `${r.connector_config_id}::${s.stream}`;
                              const isDl = downloadingKey === dlKey;
                              const isViewLoading =
                                tablePreview?.configId === r.connector_config_id &&
                                tablePreview?.stream === s.stream &&
                                previewUi.loading;
                              return (
                                <li
                                  key={s.stream}
                                  className="rounded-lg border border-[var(--border-subtle)] bg-[var(--bg-surface)] p-3 flex flex-col sm:flex-row sm:items-start sm:justify-between gap-3"
                                >
                                  <div className="min-w-0 flex-1 space-y-2">
                                    <p className="font-mono text-sm font-medium text-[var(--text-primary)] break-all">
                                      {s.stream}
                                    </p>
                                    <div className="flex flex-wrap gap-1.5">
                                      {s.months.length === 0 ? (
                                        <span className="text-xs text-[var(--text-muted)]">
                                          No data in this date range.
                                        </span>
                                      ) : (
                                        s.months.filter((m) => m.month !== 'unpartitioned').map((m) => (
                                          <span
                                            key={m.month}
                                            className="inline-flex items-center rounded-md border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-2 py-0.5 text-[11px] font-mono text-[var(--text-secondary)]"
                                            title={m.size_bytes != null ? formatBytes(m.size_bytes) : undefined}
                                          >
                                            {m.month}
                                          </span>
                                        ))
                                      )}
                                    </div>
                                    <p className="text-[11px] text-[var(--text-muted)]">
                                      {monthCount > 0
                                        ? `${monthCount} period${monthCount === 1 ? '' : 's'} · ${formatBytes(streamBytes)}`
                                        : '—'}
                                    </p>
                                  </div>
                                  <div className="flex flex-wrap gap-2 shrink-0">
                                    <button
                                      type="button"
                                      disabled={monthCount === 0 || isViewLoading}
                                      onClick={() => openTablePreview(r.connector_config_id, s.stream)}
                                      className="inline-flex items-center justify-center gap-2 rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-3 py-2 text-sm font-medium text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] hover:border-[var(--border-strong)] disabled:opacity-45 disabled:cursor-not-allowed transition-colors duration-200 cursor-pointer"
                                    >
                                      <Eye size={16} className={isViewLoading ? 'motion-safe:animate-pulse' : ''} aria-hidden />
                                      {isViewLoading ? 'Loading…' : 'View'}
                                    </button>
                                    <button
                                      type="button"
                                      disabled={monthCount === 0 || isDl}
                                      onClick={() => void handleDownload(r.connector_config_id, s.stream)}
                                      className="inline-flex items-center justify-center gap-2 rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-3 py-2 text-sm font-medium text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] hover:border-[var(--border-strong)] disabled:opacity-45 disabled:cursor-not-allowed transition-colors duration-200 cursor-pointer"
                                    >
                                      <Download size={16} className={isDl ? 'motion-safe:animate-pulse' : ''} aria-hidden />
                                      {isDl ? 'Preparing…' : 'Download'}
                                    </button>
                                  </div>
                                </li>
                              );
                            })}
                          </ul>
                        )}
                      </div>
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        )}
      </div>

      {tablePreview &&
        createPortal(
          <div
            className="fixed inset-0 z-[200] flex items-center justify-center p-4 bg-black/55 motion-safe:transition-opacity"
            style={{ backdropFilter: 'blur(2px)' }}
            onClick={() => setTablePreview(null)}
            role="presentation"
          >
            <div
              role="dialog"
              aria-modal="true"
              aria-labelledby="raw-preview-title"
              onClick={(e) => e.stopPropagation()}
              className="w-full max-w-6xl max-h-[85vh] flex flex-col rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] shadow-xl shadow-black/40"
            >
              <div className="flex items-start justify-between gap-3 p-4 border-b border-[var(--border-subtle)] shrink-0">
                <div className="min-w-0">
                  <h3
                    id="raw-preview-title"
                    className="text-lg font-semibold text-[var(--text-primary)] truncate font-mono break-all"
                  >
                    {tablePreview.stream}
                  </h3>
                  <p className="text-xs text-[var(--text-secondary)] mt-1">
                    {range.startDate} – {range.endDate}
                    {previewUi.data?.months_included?.length
                      ? ` · ${previewUi.data.months_included.join(', ')}`
                      : ''}
                  </p>
                </div>
                <button
                  type="button"
                  onClick={() => setTablePreview(null)}
                  className="shrink-0 rounded-lg p-2 text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-surface-alt)] cursor-pointer transition-colors duration-200"
                  aria-label="Close preview"
                >
                  <X size={20} />
                </button>
              </div>
              <div className="flex-1 min-h-0 overflow-hidden flex flex-col p-4">
                {previewUi.loading && (
                  <div className="flex items-center justify-center gap-2 py-16 text-sm text-[var(--text-secondary)]">
                    <RefreshCw size={16} className="motion-safe:animate-spin text-[var(--brand)]" aria-hidden />
                    Loading rows…
                  </div>
                )}
                {!previewUi.loading && previewUi.error && (
                  <div className="rounded-lg border border-red-500/30 bg-red-950/25 px-3 py-2 text-sm text-red-200/95">
                    {previewUi.error}
                  </div>
                )}
                {!previewUi.loading && !previewUi.error && previewUi.data && (
                  <>
                    {previewUi.data.columns.length === 0 && previewUi.data.rows.length === 0 ? (
                      <p className="text-sm text-[var(--text-secondary)] py-8 text-center">
                        No rows in this date window for this stream.
                      </p>
                    ) : (
                      <div className="overflow-auto max-h-[min(55vh,520px)] rounded-lg border border-[var(--border-subtle)]">
                        <table className="w-full text-left text-xs border-collapse min-w-max">
                          <thead className="sticky top-0 bg-[var(--bg-elevated)] z-10 border-b border-[var(--border-default)]">
                            <tr>
                              {previewUi.data.columns.map((c) => (
                                <th
                                  key={c}
                                  className="px-2.5 py-2.5 font-semibold text-[var(--text-primary)] whitespace-nowrap border-r border-[var(--border-subtle)] last:border-r-0"
                                >
                                  {c}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {previewUi.data.rows.map((row, ri) => (
                              <tr
                                key={ri}
                                className="border-b border-[var(--border-subtle)] last:border-b-0 hover:bg-[var(--bg-surface-alt)]/50 transition-colors duration-150"
                              >
                                {row.map((cell, ci) => (
                                  <td
                                    key={ci}
                                    className="px-2.5 py-1.5 text-[var(--text-secondary)] align-top max-w-[min(320px,40vw)] break-words"
                                  >
                                    {formatPreviewCell(cell)}
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                    <div className="flex flex-wrap items-center justify-between gap-3 mt-4 pt-3 border-t border-[var(--border-subtle)]">
                      <p className="text-xs text-[var(--text-muted)]">
                        {previewUi.data.rows.length === 0
                          ? 'No rows on this page'
                          : `Rows ${previewUi.data.offset + 1}–${previewUi.data.offset + previewUi.data.rows.length}`}
                        {previewUi.data.has_more ? ' · more on next page' : ''}
                      </p>
                      <div className="flex flex-wrap gap-2">
                        <button
                          type="button"
                          disabled={tablePreview.offset === 0 || previewUi.loading}
                          onClick={() =>
                            setTablePreview((p) =>
                              p ? { ...p, offset: Math.max(0, p.offset - RAW_PREVIEW_PAGE_SIZE) } : null,
                            )
                          }
                          className="rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-3 py-1.5 text-xs font-medium text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] disabled:opacity-45 disabled:cursor-not-allowed cursor-pointer transition-colors duration-200"
                        >
                          Previous
                        </button>
                        <button
                          type="button"
                          disabled={!previewUi.data.has_more || previewUi.loading}
                          onClick={() =>
                            setTablePreview((p) =>
                              p ? { ...p, offset: p.offset + RAW_PREVIEW_PAGE_SIZE } : null,
                            )
                          }
                          className="rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-3 py-1.5 text-xs font-medium text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] disabled:opacity-45 disabled:cursor-not-allowed cursor-pointer transition-colors duration-200"
                        >
                          Next
                        </button>
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>
          </div>,
          document.body,
        )}
    </DataPageFrame>
  );
};

export default DataRawTablesPage;
