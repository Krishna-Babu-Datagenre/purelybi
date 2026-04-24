import { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import {
  ChevronDown,
  ChevronRight,
  Database,
  Edit3,
  Link2,
  Loader2,
  RefreshCw,
  Save,
  Sparkles,
  User,
  X,
} from 'lucide-react';
import DataPageFrame from './DataPageFrame';
import { SchemaVisualizer } from './schema-visualizer';
import {
  listTableMetadata,
  listColumnMetadata,
  listRelationships,
  patchTableMetadata,
  patchColumnMetadata,
  createRelationship,
  patchRelationship,
  deleteRelationship,
  triggerGeneration,
  getLatestJob,
  getJob,
} from '../../services/metadataApi';
import type {
  TableMetadata,
  ColumnMetadata,
  Relationship,
  RelationshipCreate,
  MetadataJob,
  SemanticType,
  ColumnMetadataPatch,
  RelationshipKind,
} from '../../types/metadata';

/* ─────────────────────────────────────────────
   Metadata Review Page
   ─────────────────────────────────────────────
   Groups F1–F5: generate button, job status,
   table/column tree with inline edits,
   relationship editor, edited-by-user badge.
───────────────────────────────────────────── */

interface MetadataReviewPageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

const SEMANTIC_TYPE_OPTIONS: SemanticType[] = [
  'categorical',
  'numeric',
  'temporal',
  'identifier',
  'measure',
  'unknown',
];

const SEMANTIC_TYPE_COLORS: Record<SemanticType, string> = {
  categorical: '#8B5CF6',
  numeric: '#3B82F6',
  temporal: '#F59E0B',
  identifier: '#6366F1',
  measure: '#10B981',
  unknown: '#6B7280',
};

/* ── Edited-by-user badge (F5) ── */
const EditedBadge = ({ edited }: { edited: boolean }) =>
  edited ? (
    <span
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[0.625rem] font-medium"
      style={{ background: 'rgba(59, 130, 246, 0.15)', color: '#60A5FA' }}
      title="Edited by user — regeneration will not overwrite"
    >
      <User size={10} />
      Edited
    </span>
  ) : null;

/* ── Semantic type badge ── */
const SemanticBadge = ({ type }: { type: SemanticType }) => (
  <span
    className="inline-flex items-center px-1.5 py-0.5 rounded text-[0.625rem] font-semibold uppercase tracking-wide"
    style={{
      background: `${SEMANTIC_TYPE_COLORS[type]}20`,
      color: SEMANTIC_TYPE_COLORS[type],
    }}
  >
    {type}
  </span>
);

const MetadataReviewPage = ({
  sidebarCollapsed,
  chatOpen,
  chatModal,
  chatWidthPx,
}: MetadataReviewPageProps) => {
  /* ── Data state ── */
  const [tables, setTables] = useState<TableMetadata[]>([]);
  const [columns, setColumns] = useState<ColumnMetadata[]>([]);
  const [relationships, setRelationships] = useState<Relationship[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  /* ── Job state (F1 + F2) ── */
  const [job, setJob] = useState<MetadataJob | null>(null);
  const [, setJobPolling] = useState(false);
  const [generating, setGenerating] = useState(false);
  /** When true, show the verbose status pill (succeeded/failed colored chip
   *  with message). When false, collapse to a subtle "Last generated X ago"
   *  label. We auto-collapse a few seconds after a job transitions to a
   *  terminal state so users aren't permanently staring at "Succeeded". */
  const [showVerboseStatus, setShowVerboseStatus] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const verboseTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  /* ── UI state ── */
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [activeTab, setActiveTab] = useState<'tables' | 'relationships'>('tables');

  /* ── Inline edit state ── */
  const [editingCell, setEditingCell] = useState<string | null>(null);
  const [editValue, setEditValue] = useState('');

  /* ── Load all data ── */
  const loadData = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const [t, c, r] = await Promise.all([
        listTableMetadata(),
        listColumnMetadata(),
        listRelationships(),
      ]);
      setTables(t);
      setColumns(c);
      setRelationships(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load metadata.');
    } finally {
      setLoading(false);
    }
  }, []);

  /* ── Load latest job status ── */
  const loadLatestJob = useCallback(async () => {
    try {
      const latest = await getLatestJob();
      setJob(latest);
      if (latest && (latest.status === 'pending' || latest.status === 'running')) {
        startPolling(latest.id);
      }
    } catch {
      // ignore
    }
  }, []);

  const startPolling = useCallback((jobId: string) => {
    if (pollRef.current) clearInterval(pollRef.current);
    setJobPolling(true);
    setShowVerboseStatus(true);
    if (verboseTimerRef.current) {
      clearTimeout(verboseTimerRef.current);
      verboseTimerRef.current = null;
    }
    pollRef.current = setInterval(async () => {
      try {
        const updated = await getJob(jobId);
        setJob(updated);
        if (updated.status !== 'pending' && updated.status !== 'running') {
          clearInterval(pollRef.current!);
          pollRef.current = null;
          setJobPolling(false);
          // Refresh metadata after job completes
          if (updated.status === 'succeeded') {
            loadData();
          }
          // Auto-collapse the verbose status pill after a grace period
          if (verboseTimerRef.current) clearTimeout(verboseTimerRef.current);
          verboseTimerRef.current = setTimeout(() => {
            setShowVerboseStatus(false);
            verboseTimerRef.current = null;
          }, 8000);
        }
      } catch {
        clearInterval(pollRef.current!);
        pollRef.current = null;
        setJobPolling(false);
      }
    }, 3000);
  }, [loadData]);

  useEffect(() => {
    loadData();
    loadLatestJob();
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
      if (verboseTimerRef.current) clearTimeout(verboseTimerRef.current);
    };
  }, [loadData, loadLatestJob]);

  /* ── Generate metadata (F1) ── */
  const handleGenerate = useCallback(async () => {
    setGenerating(true);
    try {
      const resp = await triggerGeneration();
      setJob(resp.job);
      startPolling(resp.job.id);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to start generation.');
    } finally {
      setGenerating(false);
    }
  }, [startPolling]);

  /* ── Toggle table tree ── */
  const toggleTable = useCallback((tableName: string) => {
    setExpandedTables((prev) => {
      const next = new Set(prev);
      if (next.has(tableName)) next.delete(tableName);
      else next.add(tableName);
      return next;
    });
  }, []);

  /* ── Columns grouped by table ── */
  const columnsByTable = useMemo(() => {
    const map = new Map<string, ColumnMetadata[]>();
    for (const col of columns) {
      const list = map.get(col.table_name) ?? [];
      list.push(col);
      map.set(col.table_name, list);
    }
    return map;
  }, [columns]);

  /* ── Inline edit save handlers ── */
  const saveTableDescription = useCallback(
    async (tableName: string, desc: string) => {
      try {
        const updated = await patchTableMetadata(tableName, { description: desc });
        setTables((prev) =>
          prev.map((t) => (t.table_name === tableName ? updated : t)),
        );
      } catch {
        // keep old value
      }
      setEditingCell(null);
    },
    [],
  );

  const saveColumnField = useCallback(
    async (
      tableName: string,
      columnName: string,
      patch: ColumnMetadataPatch,
    ) => {
      try {
        const updated = await patchColumnMetadata(tableName, columnName, patch);
        setColumns((prev) =>
          prev.map((c) =>
            c.table_name === tableName && c.column_name === columnName ? updated : c,
          ),
        );
      } catch {
        // keep old value
      }
      setEditingCell(null);
    },
    [],
  );

  /* ── Delete relationship (for SchemaVisualizer) ── */
  const handleDeleteRelationship = useCallback(
    async (r: Relationship) => {
      try {
        await deleteRelationship(r.from_table, r.from_column, r.to_table, r.to_column);
        setRelationships((prev) =>
          prev.filter(
            (x) =>
              !(
                x.from_table === r.from_table &&
                x.from_column === r.from_column &&
                x.to_table === r.to_table &&
                x.to_column === r.to_column
              ),
          ),
        );
      } catch {
        // ignore
      }
    },
    [],
  );

  /* ── Create relationship (for SchemaVisualizer) ── */
  const handleCreateRelationship = useCallback(
    async (rel: {
      from_table: string;
      from_column: string;
      to_table: string;
      to_column: string;
      kind: RelationshipKind;
    }) => {
      try {
        const created = await createRelationship(rel as RelationshipCreate);
        setRelationships((prev) => [...prev, created]);
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to add relationship.');
      }
    },
    [],
  );

  /* ── Update relationship kind (for SchemaVisualizer) ── */
  const handleUpdateRelationshipKind = useCallback(
    async (r: Relationship, newKind: RelationshipKind) => {
      try {
        const updated = await patchRelationship(
          r.from_table,
          r.from_column,
          r.to_table,
          r.to_column,
          { kind: newKind },
        );
        setRelationships((prev) =>
          prev.map((x) =>
            x.from_table === r.from_table &&
            x.from_column === r.from_column &&
            x.to_table === r.to_table &&
            x.to_column === r.to_column
              ? updated
              : x,
          ),
        );
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to update relationship.');
      }
    },
    [],
  );

  /* ── Job status (F2) ── */
  const isJobActive = job && (job.status === 'pending' || job.status === 'running');

  const formatRelativeTime = (iso: string | null): string => {
    if (!iso) return '';
    const then = new Date(iso).getTime();
    if (Number.isNaN(then)) return '';
    const secs = Math.max(1, Math.round((Date.now() - then) / 1000));
    if (secs < 60) return `${secs}s ago`;
    const mins = Math.round(secs / 60);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.round(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.round(hrs / 24);
    if (days < 7) return `${days}d ago`;
    return new Date(iso).toLocaleDateString();
  };

  const showVerbose = isJobActive || showVerboseStatus;

  let jobStatusEl: React.ReactNode = null;
  if (job && showVerbose) {
    jobStatusEl = (
      <div className="metadata-job-status" data-status={job.status}>
        {isJobActive ? (
          <Loader2 size={13} className="animate-spin" />
        ) : job.status === 'succeeded' ? (
          <span className="metadata-job-status__dot metadata-job-status__dot--ok" />
        ) : job.status === 'failed' ? (
          <span className="metadata-job-status__dot metadata-job-status__dot--err" />
        ) : null}
        <span className="metadata-job-status__label">{job.status}</span>
        {job.progress > 0 && job.progress < 1 && (
          <span className="metadata-job-status__progress">{Math.round(job.progress * 100)}%</span>
        )}
        {job.message && (
          <span className="metadata-job-status__message">· {job.message}</span>
        )}
      </div>
    );
  } else if (job && job.finished_at) {
    jobStatusEl = (
      <div
        className="metadata-job-status metadata-job-status--quiet"
        title={
          (job.status === 'succeeded'
            ? 'Last generation succeeded'
            : job.status === 'failed'
              ? 'Last generation failed'
              : `Last run: ${job.status}`) +
          `\n${new Date(job.finished_at).toLocaleString()}`
        }
      >
        <span
          className={
            'metadata-job-status__dot ' +
            (job.status === 'succeeded'
              ? 'metadata-job-status__dot--ok'
              : 'metadata-job-status__dot--err')
          }
        />
        <span className="metadata-job-status__message">
          Last generated {formatRelativeTime(job.finished_at)}
        </span>
      </div>
    );
  }

  return (
    <DataPageFrame
      sidebarCollapsed={sidebarCollapsed}
      chatOpen={chatOpen}
      chatModal={chatModal}
      chatWidthPx={chatWidthPx}
    >
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-lg font-semibold text-[var(--text-primary)] tracking-tight flex items-center gap-2">
          <Database size={20} className="text-[var(--brand)]" />
          Metadata Review
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-0.5">
          Review and edit table/column metadata and relationships. LLM-generated values are editable — edited rows are preserved on regeneration.
        </p>
      </div>

      {/* Error banner */}
      {error && (
        <div className="metadata-error-banner">
          <span>{error}</span>
          <button type="button" onClick={() => setError(null)}>
            <X size={14} />
          </button>
        </div>
      )}

      {/* Tabs + actions row */}
      <div className="flex items-center justify-between gap-3 mb-4 flex-wrap">
        <div className="flex gap-1">
          <button
            type="button"
            className={`metadata-tab ${activeTab === 'tables' ? 'metadata-tab--active' : ''}`}
            onClick={() => setActiveTab('tables')}
          >
            <Database size={14} />
            Tables & Columns
            <span className="metadata-tab__count">{tables.length}</span>
          </button>
          <button
            type="button"
            className={`metadata-tab ${activeTab === 'relationships' ? 'metadata-tab--active' : ''}`}
            onClick={() => setActiveTab('relationships')}
          >
            <Link2 size={14} />
            Relationships
            <span className="metadata-tab__count">{relationships.length}</span>
          </button>
        </div>

        <div className="flex items-center gap-2">
          {jobStatusEl}

          <button
            type="button"
            disabled={generating || !!isJobActive}
            onClick={handleGenerate}
            className="metadata-generate-btn"
          >
            {generating || isJobActive ? (
              <Loader2 size={14} className="animate-spin" />
            ) : (
              <Sparkles size={14} />
            )}
            {generating ? 'Starting…' : isJobActive ? 'Generating…' : 'Generate Metadata'}
          </button>

          <button
            type="button"
            onClick={loadData}
            disabled={loading}
            className="metadata-refresh-btn"
            title="Refresh"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          </button>
        </div>
      </div>

      {/* Loading */}
      {loading && (
        <div className="flex items-center justify-center gap-2 py-12">
          <Loader2 size={20} className="animate-spin text-[var(--brand)]" />
          <span className="text-sm text-[var(--text-secondary)]">Loading metadata…</span>
        </div>
      )}

      {/* Tables & Columns tab (F3) */}
      {!loading && activeTab === 'tables' && (
        <div className="space-y-1">
          {tables.length === 0 ? (
            <div className="text-center py-12">
              <Database size={32} className="mx-auto mb-3 text-[var(--text-muted)]" />
              <p className="text-sm text-[var(--text-secondary)]">
                No metadata found. Connect a data source and generate metadata to get started.
              </p>
            </div>
          ) : (
            tables.map((table) => {
              const isExpanded = expandedTables.has(table.table_name);
              const cols = columnsByTable.get(table.table_name) ?? [];
              const isEditingDesc = editingCell === `table-desc:${table.table_name}`;

              return (
                <div key={table.table_name} className="metadata-table-row">
                  {/* Table header row */}
                  <button
                    type="button"
                    className="metadata-table-header"
                    onClick={() => toggleTable(table.table_name)}
                  >
                    {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                    <span className="font-semibold text-[var(--text-primary)] text-sm">
                      {table.table_name}
                    </span>
                    <span className="text-[var(--text-muted)] text-xs ml-1">
                      ({cols.length} columns)
                    </span>
                    <EditedBadge edited={table.edited_by_user} />
                    {table.primary_date_column && (
                      <span className="ml-auto text-[0.625rem] text-[var(--text-muted)]">
                        📅 {table.primary_date_column}
                      </span>
                    )}
                  </button>

                  {/* Table description (editable) */}
                  {isExpanded && (
                    <div className="metadata-table-desc">
                      {isEditingDesc ? (
                        <div className="flex gap-2 items-start">
                          <textarea
                            className="metadata-edit-textarea"
                            value={editValue}
                            onChange={(e) => setEditValue(e.target.value)}
                            rows={2}
                            autoFocus
                          />
                          <button
                            type="button"
                            className="metadata-save-btn"
                            onClick={() => saveTableDescription(table.table_name, editValue)}
                          >
                            <Save size={12} />
                          </button>
                          <button
                            type="button"
                            className="metadata-cancel-btn"
                            onClick={() => setEditingCell(null)}
                          >
                            <X size={12} />
                          </button>
                        </div>
                      ) : (
                        <div
                          className="metadata-desc-text group cursor-pointer"
                          onClick={() => {
                            setEditingCell(`table-desc:${table.table_name}`);
                            setEditValue(table.description ?? '');
                          }}
                        >
                          <span>{table.description || 'No description'}</span>
                          <Edit3 size={11} className="text-[var(--text-muted)] opacity-0 group-hover:opacity-100 transition-opacity ml-1 flex-shrink-0" />
                        </div>
                      )}
                      {table.grain && (
                        <span className="text-[0.625rem] text-[var(--text-muted)]">
                          Grain: {table.grain}
                        </span>
                      )}
                    </div>
                  )}

                  {/* Column rows */}
                  {isExpanded && cols.length > 0 && (
                    <div className="metadata-columns">
                      <div className="metadata-col-header-row">
                        <span className="metadata-col-header" style={{ width: '22%' }}>Column</span>
                        <span className="metadata-col-header" style={{ width: '12%' }}>Type</span>
                        <span className="metadata-col-header" style={{ width: '14%' }}>Semantic</span>
                        <span className="metadata-col-header" style={{ width: '35%' }}>Description</span>
                        <span className="metadata-col-header" style={{ width: '8%' }}>Filter</span>
                        <span className="metadata-col-header" style={{ width: '9%' }}></span>
                      </div>
                      {cols.map((col) => {
                        const cellKey = `col-desc:${col.table_name}.${col.column_name}`;
                        const isEditingColDesc = editingCell === cellKey;
                        const semKey = `col-sem:${col.table_name}.${col.column_name}`;
                        const isEditingSem = editingCell === semKey;

                        return (
                          <div key={col.column_name} className="metadata-col-row">
                            <div style={{ width: '22%' }} className="flex items-center gap-1.5">
                              <span className="text-[var(--text-primary)] text-xs font-medium truncate">
                                {col.column_name}
                              </span>
                              <EditedBadge edited={col.edited_by_user} />
                            </div>
                            <div style={{ width: '12%' }}>
                              <span className="text-[var(--text-muted)] text-[0.6875rem] font-mono">
                                {col.data_type}
                              </span>
                            </div>
                            <div style={{ width: '14%' }}>
                              {isEditingSem ? (
                                <select
                                  className="metadata-edit-select"
                                  value={editValue}
                                  onChange={(e) => {
                                    const val = e.target.value as SemanticType;
                                    setEditValue(val);
                                    saveColumnField(col.table_name, col.column_name, { semantic_type: val });
                                  }}
                                  autoFocus
                                  onBlur={() => setEditingCell(null)}
                                >
                                  {SEMANTIC_TYPE_OPTIONS.map((st) => (
                                    <option key={st} value={st}>{st}</option>
                                  ))}
                                </select>
                              ) : (
                                <button
                                  type="button"
                                  className="cursor-pointer"
                                  onClick={() => {
                                    setEditingCell(semKey);
                                    setEditValue(col.semantic_type);
                                  }}
                                >
                                  <SemanticBadge type={col.semantic_type} />
                                </button>
                              )}
                            </div>
                            <div style={{ width: '35%' }}>
                              {isEditingColDesc ? (
                                <div className="flex gap-1.5 items-center">
                                  <input
                                    type="text"
                                    className="metadata-edit-input"
                                    value={editValue}
                                    onChange={(e) => setEditValue(e.target.value)}
                                    autoFocus
                                    onKeyDown={(e) => {
                                      if (e.key === 'Enter') {
                                        saveColumnField(col.table_name, col.column_name, { description: editValue });
                                      }
                                      if (e.key === 'Escape') setEditingCell(null);
                                    }}
                                  />
                                  <button
                                    type="button"
                                    className="metadata-save-btn"
                                    onClick={() => saveColumnField(col.table_name, col.column_name, { description: editValue })}
                                  >
                                    <Save size={11} />
                                  </button>
                                </div>
                              ) : (
                                <div
                                  className="metadata-desc-text group cursor-pointer text-xs"
                                  onClick={() => {
                                    setEditingCell(cellKey);
                                    setEditValue(col.description ?? '');
                                  }}
                                >
                                  <span className="truncate">{col.description || '—'}</span>
                                  <Edit3 size={10} className="text-[var(--text-muted)] opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0" />
                                </div>
                              )}
                            </div>
                            <div style={{ width: '8%' }}>
                              <button
                                type="button"
                                className={`metadata-toggle ${col.is_filterable ? 'metadata-toggle--on' : ''}`}
                                onClick={() =>
                                  saveColumnField(col.table_name, col.column_name, {
                                    is_filterable: !col.is_filterable,
                                  })
                                }
                              >
                                {col.is_filterable ? 'Yes' : 'No'}
                              </button>
                            </div>
                            <div style={{ width: '9%' }} className="text-right">
                              {col.cardinality != null && (
                                <span className="text-[var(--text-muted)] text-[0.625rem]">
                                  {col.cardinality.toLocaleString()}
                                </span>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      )}

      {/* Relationships tab — React Flow Schema Visualizer */}
      {!loading && activeTab === 'relationships' && (
        <SchemaVisualizer
          tables={tables}
          columns={columns}
          relationships={relationships}
          onCreateRelationship={handleCreateRelationship}
          onDeleteRelationship={handleDeleteRelationship}
          onUpdateRelationshipKind={handleUpdateRelationshipKind}
        />
      )}
    </DataPageFrame>
  );
};

export default MetadataReviewPage;
