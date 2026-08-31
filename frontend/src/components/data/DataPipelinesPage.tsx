import { useEffect, useState, useCallback, useMemo } from 'react';
import { listUserConnectors } from '../../services/backendClient';
import type { UserConnectorConfig } from '../../types';
import { listColumnMetadata, listTableMetadata } from '../../services/metadataApi';
import type { TableMetadata, ColumnMetadata } from '../../types/metadata';
import DataPageFrame from './DataPageFrame';
import {
  listRecipes,
  listPipelines,
  getPipeline,
  createPipeline,
  patchPipeline,
  deletePipeline,
  upsertPipelineStep,
  deletePipelineStep,
  validatePipeline,
  listPipelineRuns,
  triggerPipelineRun,
} from '../../services/deApi';
import type {
  RecipeDefinition,
  DEPipelineDetail,
  DEPipelineRun,
  DEPipelineStep,
  DEPipelineStepUpsert,
  DEValidationResponse,
} from '../../types/de';
import {
  Play,
  Loader2,
  XCircle,
  CheckCircle2,
  Plus,
  FolderKanban,
  Trash2,
  ChevronDown,
  ChevronUp,
  ChevronsUpDown,
  ToggleLeft,
  ToggleRight,
  Save,
  X,
} from 'lucide-react';
import AddStepModal from './AddStepModal';
import DEChatPanel from './DEChatPanel';

function resolveConnectorTableNames(
  connector: UserConnectorConfig | undefined,
  tables: TableMetadata[],
): Set<string> {
  if (!connector) return new Set();

  const streamSet = new Set((connector.selected_streams ?? []).map((s) => s.toLowerCase()));
  const lastSegment = (connector.docker_repository || '').split('/').pop() || '';
  const folderPrefix = lastSegment.replace(/[^a-z0-9]/gi, '_').toLowerCase();
  const matched = new Set<string>();

  for (const t of tables) {
    const tn = t.table_name.toLowerCase();
    if (folderPrefix && tn.startsWith(folderPrefix + '_')) {
      const streamPart = tn.slice(folderPrefix.length + 1);
      if (streamSet.size === 0 || streamSet.has(streamPart)) {
        matched.add(t.table_name);
      }
    }
  }

  if (matched.size === 0 && streamSet.size > 0) {
    for (const t of tables) {
      if (streamSet.has(t.table_name.toLowerCase())) {
        matched.add(t.table_name);
      }
    }
  }

  return matched;
}

interface DataPipelinesPageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

type PipelineNotice = {
  type: 'success' | 'error' | 'info';
  text: string;
};

function sourceNamesForPipeline(
  pipeline: DEPipelineDetail,
  connectorMap: Map<string, UserConnectorConfig>,
): string[] {
  return pipeline.source_connector_ids
    .map((id) => connectorMap.get(id)?.connector_name ?? 'Unknown source')
    .filter((name) => name.trim().length > 0);
}

export default function DataPipelinesPage({
  sidebarCollapsed,
  chatOpen,
  chatModal,
  chatWidthPx,
}: DataPipelinesPageProps) {
  const [connectors, setConnectors] = useState<UserConnectorConfig[]>([]);
  const [recipes, setRecipes] = useState<RecipeDefinition[]>([]);
  const [pipelines, setPipelines] = useState<DEPipelineDetail[]>([]);

  const [selectedPipelineId, setSelectedPipelineId] = useState<string | null>(null);
  const [expandedByPipeline, setExpandedByPipeline] = useState<Record<string, boolean>>({});
  const [editingSourcesByPipeline, setEditingSourcesByPipeline] = useState<Record<string, boolean>>({});

  const [runsByPipeline, setRunsByPipeline] = useState<Record<string, DEPipelineRun[]>>({});
  const [validationByPipeline, setValidationByPipeline] = useState<Record<string, DEValidationResponse | null>>({});
  const [validationOpenByPipeline, setValidationOpenByPipeline] = useState<Record<string, boolean>>({});

  const [loading, setLoading] = useState(true);
  const [validatingPipelineId, setValidatingPipelineId] = useState<string | null>(null);
  const [savingPipelineId, setSavingPipelineId] = useState<string | null>(null);
  const [runningPipelineId, setRunningPipelineId] = useState<string | null>(null);
  const [deletingPipelineId, setDeletingPipelineId] = useState<string | null>(null);
  const [deletingStepId, setDeletingStepId] = useState<string | null>(null);
  const [notice, setNotice] = useState<PipelineNotice | null>(null);

  const [showCreatePanel, setShowCreatePanel] = useState(false);
  const [newPipelineName, setNewPipelineName] = useState('');
  const [newPipelineSources, setNewPipelineSources] = useState<string[]>([]);

  const [addStepPipelineId, setAddStepPipelineId] = useState<string | null>(null);

  const [tables, setTables] = useState<TableMetadata[]>([]);
  const [columns, setColumns] = useState<ColumnMetadata[]>([]);

  const connectorMap = useMemo(
    () => new Map(connectors.map((c) => [c.id, c])),
    [connectors],
  );

  const selectedPipeline = useMemo(
    () => pipelines.find((p) => p.id === selectedPipelineId) ?? null,
    [pipelines, selectedPipelineId],
  );

  const availableColumns = useMemo(() => {
    if (!selectedPipeline) return [];

    const scopedTableNames = new Set<string>();
    for (const sourceId of selectedPipeline.source_connector_ids) {
      const connector = connectorMap.get(sourceId);
      const matched = resolveConnectorTableNames(connector, tables);
      for (const name of matched) scopedTableNames.add(name);
    }

    const scopedColumns =
      scopedTableNames.size > 0
        ? columns.filter((c) => scopedTableNames.has(c.table_name))
        : columns;

    return Array.from(new Set(scopedColumns.map((c) => c.column_name))).sort((a, b) =>
      a.localeCompare(b),
    );
  }, [selectedPipeline, connectorMap, tables, columns]);

  const addStepPipeline = useMemo(
    () => pipelines.find((p) => p.id === addStepPipelineId) ?? null,
    [pipelines, addStepPipelineId],
  );

  const refreshPipelines = useCallback(async () => {
    const summaries = await listPipelines();
    const details = await Promise.all(summaries.map((p) => getPipeline(p.id)));
    setPipelines(details);

    setExpandedByPipeline((prev) => {
      const next = { ...prev };
      for (const p of details) {
        if (next[p.id] === undefined) {
          next[p.id] = true;
        }
      }
      return next;
    });

    setSelectedPipelineId((prev) => {
      if (prev && details.some((p) => p.id === prev)) {
        return prev;
      }
      return details[0]?.id ?? null;
    });
  }, []);

  useEffect(() => {
    async function load() {
      try {
        const [connData, recipeData, tableData, columnData] = await Promise.all([
          listUserConnectors(),
          listRecipes(),
          listTableMetadata(),
          listColumnMetadata(),
        ]);
        setConnectors(connData);
        setRecipes(recipeData);
        setTables(tableData);
        setColumns(columnData);
        setNewPipelineSources(connData.length > 0 ? [connData[0].id] : []);
        await refreshPipelines();
      } catch {
        // noop
      } finally {
        setLoading(false);
      }
    }

    void load();
  }, [refreshPipelines]);

  useEffect(() => {
    async function loadRuns() {
      if (!selectedPipelineId) return;
      try {
        const runs = await listPipelineRuns(selectedPipelineId);
        setRunsByPipeline((prev) => ({ ...prev, [selectedPipelineId]: runs }));
      } catch {
        // noop
      }
    }
    void loadRuns();
  }, [selectedPipelineId]);

  const toggleCreateSource = (id: string) => {
    setNewPipelineSources((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id],
    );
  };

  const handleCreatePipeline = async () => {
    const sourceIds = Array.from(new Set(newPipelineSources));
    if (sourceIds.length === 0) {
      alert('Select at least one data source for the pipeline.');
      return;
    }
    try {
      await createPipeline({
        name: newPipelineName.trim() || `Pipeline ${pipelines.length + 1}`,
        source_connector_ids: sourceIds,
        connector_config_id: sourceIds[0],
      });
      setShowCreatePanel(false);
      setNewPipelineName('');
      await refreshPipelines();
      setNotice({ type: 'success', text: 'Pipeline created successfully.' });
    } catch (e) {
      setNotice({ type: 'error', text: e instanceof Error ? e.message : 'Failed to create pipeline.' });
    }
  };

  const updatePipelineLocal = (pipelineId: string, updater: (p: DEPipelineDetail) => DEPipelineDetail) => {
    setPipelines((prev) => prev.map((p) => (p.id === pipelineId ? updater(p) : p)));
  };

  const handlePipelineNameChange = (pipelineId: string, name: string) => {
    updatePipelineLocal(pipelineId, (p) => ({ ...p, name }));
  };

  const handleTogglePipelineSource = (pipelineId: string, sourceId: string) => {
    updatePipelineLocal(pipelineId, (p) => {
      const has = p.source_connector_ids.includes(sourceId);
      const nextSources = has
        ? p.source_connector_ids.filter((id) => id !== sourceId)
        : [...p.source_connector_ids, sourceId];

      if (nextSources.length === 0) {
        return p;
      }

      return {
        ...p,
        source_connector_ids: nextSources,
        connector_config_id: nextSources[0],
      };
    });
  };

  const handleToggleActive = async (pipeline: DEPipelineDetail) => {
    try {
      const updated = await patchPipeline(pipeline.id, { is_active: !pipeline.is_active });
      updatePipelineLocal(pipeline.id, (p) => ({ ...p, is_active: updated.is_active }));
    } catch {
      alert('Failed to update pipeline state.');
    }
  };

  const handleToggleStep = async (pipelineId: string, step: DEPipelineStep) => {
    try {
      const updated = await upsertPipelineStep(pipelineId, {
        step_order: step.step_order,
        recipe_type: step.recipe_type,
        config_json: step.config_json,
        is_enabled: !step.is_enabled,
      });
      updatePipelineLocal(pipelineId, (p) => ({
        ...p,
        steps: p.steps.map((s) => (s.id === step.id ? { ...s, is_enabled: updated.is_enabled } : s)),
      }));
    } catch {
      alert('Failed to update step status.');
    }
  };

  const handleDeleteStep = async (pipelineId: string, stepId: string) => {
    setDeletingStepId(stepId);
    try {
      await deletePipelineStep(pipelineId, stepId);
      updatePipelineLocal(pipelineId, (p) => ({ ...p, steps: p.steps.filter((s) => s.id !== stepId) }));
      setValidationByPipeline((prev) => ({ ...prev, [pipelineId]: null }));
    } catch {
      alert('Failed to delete step.');
    } finally {
      setDeletingStepId(null);
    }
  };

  const handleAddStep = async (body: DEPipelineStepUpsert) => {
    if (!addStepPipelineId) return;
    const step = await upsertPipelineStep(addStepPipelineId, body);
    updatePipelineLocal(addStepPipelineId, (p) => ({ ...p, steps: [...p.steps, step] }));
  };

  const handleRunValidation = async (pipelineId: string) => {
    setValidatingPipelineId(pipelineId);
    setValidationOpenByPipeline((prev) => ({ ...prev, [pipelineId]: true }));
    try {
      const result = await validatePipeline(pipelineId, { sample_rows: [{ _sample: true }] });
      setValidationByPipeline((prev) => ({ ...prev, [pipelineId]: result }));
    } catch (e) {
      alert(e instanceof Error ? e.message : 'Validation failed');
    } finally {
      setValidatingPipelineId(null);
    }
  };

  const handleSavePipeline = async (pipeline: DEPipelineDetail) => {
    if (savingPipelineId === pipeline.id) return;
    if (pipeline.source_connector_ids.length === 0) {
      alert('A pipeline must have at least one data source.');
      return;
    }

    setSavingPipelineId(pipeline.id);
    try {
      await patchPipeline(pipeline.id, {
        name: pipeline.name,
        is_active: pipeline.is_active,
        source_connector_ids: pipeline.source_connector_ids,
      });
      const orderedSteps = [...pipeline.steps].sort((a, b) => a.step_order - b.step_order);
      for (const step of orderedSteps) {
        await upsertPipelineStep(pipeline.id, {
          step_order: step.step_order,
          recipe_type: step.recipe_type,
          config_json: step.config_json,
          is_enabled: step.is_enabled,
        });
      }
      await refreshPipelines();
      setNotice({ type: 'success', text: `Pipeline "${pipeline.name}" was saved successfully.` });
    } catch (e) {
      setNotice({ type: 'error', text: e instanceof Error ? e.message : 'Failed to save pipeline.' });
    } finally {
      setSavingPipelineId(null);
    }
  };

  const handleDeletePipeline = async (pipeline: DEPipelineDetail) => {
    const confirmed = window.confirm(
      `Delete pipeline "${pipeline.name}"?\n\nThis will remove the pipeline, its steps, and run history. Transformed data is removed only for sources not used by other pipelines. This cannot be undone.`,
    );
    if (!confirmed) return;

    setDeletingPipelineId(pipeline.id);
    try {
      const result = await deletePipeline(pipeline.id);
      setNotice({
        type: 'success',
        text:
          `Pipeline "${pipeline.name}" deleted.` +
          (result.cleaned_source_connector_ids.length > 0
            ? ` Cleaned transformed data for ${result.cleaned_source_connector_ids.length} source${result.cleaned_source_connector_ids.length === 1 ? '' : 's'}.`
            : '') +
          (result.retained_source_connector_ids.length > 0
            ? ` Kept shared transformed data for ${result.retained_source_connector_ids.length} source${result.retained_source_connector_ids.length === 1 ? '' : 's'}.`
            : ''),
      });
      await refreshPipelines();
      setRunsByPipeline((prev) => {
        const next = { ...prev };
        delete next[pipeline.id];
        return next;
      });
      setValidationByPipeline((prev) => {
        const next = { ...prev };
        delete next[pipeline.id];
        return next;
      });
      setValidationOpenByPipeline((prev) => {
        const next = { ...prev };
        delete next[pipeline.id];
        return next;
      });
      setEditingSourcesByPipeline((prev) => {
        const next = { ...prev };
        delete next[pipeline.id];
        return next;
      });
      setExpandedByPipeline((prev) => {
        const next = { ...prev };
        delete next[pipeline.id];
        return next;
      });
    } catch (e) {
      setNotice({ type: 'error', text: e instanceof Error ? e.message : 'Failed to delete pipeline.' });
    } finally {
      setDeletingPipelineId(null);
    }
  };

  const handleRunPipeline = async (pipeline: DEPipelineDetail) => {
    if (runningPipelineId === pipeline.id) return;
    setRunningPipelineId(pipeline.id);
    try {
      const run = await triggerPipelineRun(pipeline.id);
      setRunsByPipeline((prev) => ({
        ...prev,
        [pipeline.id]: [run, ...(prev[pipeline.id] ?? [])].slice(0, 20),
      }));
      if (run.status === 'failed_to_start') {
        setNotice({
          type: 'error',
          text: run.error || 'Pipeline run could not be started.',
        });
      } else {
        setNotice({
          type: 'success',
          text: `Pipeline "${pipeline.name}" run was queued successfully.`,
        });
      }
    } catch (e) {
      setNotice({ type: 'error', text: e instanceof Error ? e.message : 'Failed to trigger pipeline run.' });
    } finally {
      setRunningPipelineId(null);
    }
  };

  const togglePipelineExpanded = (pipelineId: string) => {
    setExpandedByPipeline((prev) => ({ ...prev, [pipelineId]: !(prev[pipelineId] ?? true) }));
  };

  const togglePipelineValidation = (pipelineId: string) => {
    setValidationOpenByPipeline((prev) => ({ ...prev, [pipelineId]: !(prev[pipelineId] ?? false) }));
  };

  const toggleSourceEditor = (pipelineId: string) => {
    setEditingSourcesByPipeline((prev) => ({ ...prev, [pipelineId]: !(prev[pipelineId] ?? false) }));
  };

  const recipeMap = Object.fromEntries(recipes.map((r) => [r.recipe_type, r.label]));

  const selectedPipelineSourceName = selectedPipeline
    ? sourceNamesForPipeline(selectedPipeline, connectorMap).join(', ')
    : null;
  const selectedPipelinePrimarySourceId = selectedPipeline?.source_connector_ids[0] ?? null;
  const selectedPipelinePrimarySource = selectedPipelinePrimarySourceId
    ? connectorMap.get(selectedPipelinePrimarySourceId)
    : undefined;

  return (
    <DataPageFrame
      sidebarCollapsed={sidebarCollapsed}
      chatOpen={chatOpen}
      chatModal={chatModal}
      chatWidthPx={chatWidthPx}
    >
      <div className="flex flex-col h-full">
        <div className="flex-none mb-4 flex items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-[var(--text-primary)] flex items-center gap-2">
              Data Engineering
              <span className="text-xs px-2 py-0.5 rounded-full bg-[rgba(139,92,246,0.16)] text-[var(--brand)] border border-[rgba(139,92,246,0.3)] font-medium">
                BETA
              </span>
            </h2>
            <p className="text-sm text-[var(--text-secondary)] mt-0.5">
              Pipelines are saved independently. Each pipeline can include multiple data sources.
            </p>
          </div>
          <button
            type="button"
            className="btn-primary"
            onClick={() => setShowCreatePanel((v) => !v)}
          >
            <Plus size={15} /> New Pipeline
          </button>
        </div>

        {showCreatePanel && (
          <div className="mb-4 p-4 rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-semibold text-[var(--text-primary)]">Create Pipeline</h3>
              <button
                type="button"
                className="text-[var(--text-muted)] hover:text-[var(--text-primary)]"
                onClick={() => setShowCreatePanel(false)}
              >
                <X size={16} />
              </button>
            </div>
            <input
              className="modal-input"
              placeholder="Pipeline name"
              value={newPipelineName}
              onChange={(e) => setNewPipelineName(e.target.value)}
            />
            <div>
              <p className="text-xs font-medium text-[var(--text-secondary)] mb-2">Select data sources</p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                {connectors.map((connector) => {
                  const checked = newPipelineSources.includes(connector.id);
                  return (
                    <label
                      key={connector.id}
                      className="flex items-center gap-2 rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-3 py-2 text-sm text-[var(--text-primary)]"
                    >
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleCreateSource(connector.id)}
                      />
                      <span>{connector.connector_name}</span>
                    </label>
                  );
                })}
              </div>
            </div>
            <div className="flex justify-end">
              <button type="button" className="btn-primary" onClick={() => void handleCreatePipeline()}>
                Create
              </button>
            </div>
          </div>
        )}

        {notice && (
          <div
            className={`mb-4 rounded-lg border px-3 py-2 text-sm flex items-start justify-between gap-3 ${
              notice.type === 'success'
                ? 'border-emerald-500/35 bg-emerald-500/10 text-emerald-200'
                : notice.type === 'error'
                ? 'border-red-500/35 bg-red-500/10 text-red-200'
                : 'border-[var(--border-default)] bg-[var(--bg-surface)] text-[var(--text-secondary)]'
            }`}
          >
            <span>{notice.text}</span>
            <button
              type="button"
              onClick={() => setNotice(null)}
              className="text-[var(--text-muted)] hover:text-[var(--text-primary)]"
              aria-label="Dismiss message"
            >
              <X size={14} />
            </button>
          </div>
        )}

        {loading ? (
          <div className="flex-1 flex items-center justify-center">
            <Loader2 className="animate-spin text-[var(--text-muted)]" size={32} />
          </div>
        ) : pipelines.length === 0 ? (
          <div className="flex-1 flex flex-col items-center justify-center gap-3 text-center">
            <FolderKanban size={44} className="text-[var(--text-muted)]" />
            <p className="text-sm text-[var(--text-secondary)]">No pipelines yet. Create one to get started.</p>
          </div>
        ) : (
          <div className="flex-1 flex gap-5 min-h-0">
            <div className="flex-1 min-h-0 overflow-y-auto space-y-4 pr-1">
              {pipelines.map((pipeline) => {
                const expanded = expandedByPipeline[pipeline.id] ?? true;
                const sourceNames = sourceNamesForPipeline(pipeline, connectorMap);
                const validation = validationByPipeline[pipeline.id] ?? null;
                const validationOpen = validationOpenByPipeline[pipeline.id] ?? false;
                const runs = runsByPipeline[pipeline.id] ?? [];
                const isSourceEditorOpen = editingSourcesByPipeline[pipeline.id] ?? false;

                return (
                  <div
                    key={pipeline.id}
                    className={`rounded-xl border transition-colors ${
                      selectedPipelineId === pipeline.id
                        ? 'border-[rgba(139,92,246,0.45)] bg-[rgba(139,92,246,0.07)]'
                        : 'border-[var(--border-default)] bg-[var(--bg-surface)]'
                    }`}
                  >
                    <div className="px-4 py-3 border-b border-[var(--border-subtle)]">
                      <div className="flex items-start gap-3">
                        <button
                          type="button"
                          className="mt-1 text-[var(--text-muted)] hover:text-[var(--text-primary)]"
                          onClick={() => togglePipelineExpanded(pipeline.id)}
                          aria-label="Toggle pipeline"
                        >
                          {expanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
                        </button>
                        <div className="flex-1 min-w-0">
                          <input
                            className="w-full bg-transparent text-sm font-semibold text-[var(--text-primary)] outline-none"
                            value={pipeline.name}
                            onChange={(e) => handlePipelineNameChange(pipeline.id, e.target.value)}
                            onFocus={() => setSelectedPipelineId(pipeline.id)}
                          />
                          <p className="text-xs text-[var(--text-muted)] mt-0.5">Pipeline ID: {pipeline.id}</p>
                        </div>
                        <button
                          type="button"
                          onClick={() => setSelectedPipelineId(pipeline.id)}
                          className="text-xs px-2 py-1 rounded border border-[var(--border-default)] text-[var(--text-secondary)] hover:text-[var(--text-primary)]"
                        >
                          Select
                        </button>
                      </div>

                      <div className="mt-3 flex flex-wrap items-center gap-2">
                        <button
                          type="button"
                          className="text-xs px-2 py-1 rounded border border-[var(--border-default)] text-[var(--text-secondary)] hover:text-[var(--text-primary)] inline-flex items-center gap-1"
                          onClick={() => toggleSourceEditor(pipeline.id)}
                        >
                          <ChevronsUpDown size={12} /> Sources
                        </button>
                        {sourceNames.map((name) => (
                          <span
                            key={name}
                            className="text-xs px-2 py-1 rounded-full border border-[var(--border-default)] bg-[var(--bg-surface-alt)] text-[var(--text-secondary)]"
                          >
                            {name}
                          </span>
                        ))}

                        <span className="flex-1" />

                        <button
                          type="button"
                          onClick={() => void handleToggleActive(pipeline)}
                          className={`flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full transition-colors ${
                            pipeline.is_active
                              ? 'bg-emerald-500/15 text-emerald-300 border border-emerald-500/35'
                              : 'bg-[var(--bg-surface-alt)] text-[var(--text-secondary)] border border-[var(--border-default)]'
                          }`}
                        >
                          {pipeline.is_active ? (
                            <><ToggleRight size={14} /> Active</>
                          ) : (
                            <><ToggleLeft size={14} /> Paused</>
                          )}
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleRunPipeline(pipeline)}
                          disabled={runningPipelineId === pipeline.id || pipeline.steps.length === 0}
                          className="flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 bg-blue-500/15 hover:bg-blue-500/25 text-blue-200 rounded-lg transition-colors disabled:opacity-40"
                        >
                          {runningPipelineId === pipeline.id ? (
                            <Loader2 size={13} className="animate-spin" />
                          ) : (
                            <Play size={13} />
                          )}
                          Run
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleRunValidation(pipeline.id)}
                          disabled={validatingPipelineId === pipeline.id || pipeline.steps.length === 0}
                          className="flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 bg-[var(--bg-surface-alt)] hover:bg-[var(--bg-elevated)] text-[var(--text-primary)] rounded-lg transition-colors disabled:opacity-40"
                        >
                          {validatingPipelineId === pipeline.id ? (
                            <Loader2 size={13} className="animate-spin" />
                          ) : (
                            <Play size={13} />
                          )}
                          Validate
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleSavePipeline(pipeline)}
                          disabled={savingPipelineId === pipeline.id}
                          className="flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 bg-[var(--brand)] hover:opacity-90 text-white rounded-lg transition-opacity disabled:opacity-40"
                        >
                          {savingPipelineId === pipeline.id ? (
                            <Loader2 size={13} className="animate-spin" />
                          ) : (
                            <Save size={13} />
                          )}
                          Save
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleDeletePipeline(pipeline)}
                          disabled={deletingPipelineId === pipeline.id}
                          className="flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 bg-red-500/15 hover:bg-red-500/25 text-red-200 rounded-lg transition-colors disabled:opacity-40"
                        >
                          {deletingPipelineId === pipeline.id ? (
                            <Loader2 size={13} className="animate-spin" />
                          ) : (
                            <Trash2 size={13} />
                          )}
                          Delete
                        </button>
                      </div>

                      {isSourceEditorOpen && (
                        <div className="mt-3 p-3 rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface-alt)] grid grid-cols-1 md:grid-cols-2 gap-2">
                          {connectors.map((connector) => {
                            const checked = pipeline.source_connector_ids.includes(connector.id);
                            return (
                              <label key={connector.id} className="flex items-center gap-2 text-sm text-[var(--text-primary)]">
                                <input
                                  type="checkbox"
                                  checked={checked}
                                  onChange={() => handleTogglePipelineSource(pipeline.id, connector.id)}
                                />
                                <span>{connector.connector_name}</span>
                              </label>
                            );
                          })}
                        </div>
                      )}
                    </div>

                    {expanded && (
                      <div className="p-4 space-y-3">
                        <div className="space-y-2">
                          {pipeline.steps.length === 0 ? (
                            <p className="text-sm text-[var(--text-muted)] italic text-center py-4 border border-dashed border-[var(--border-default)] rounded-lg">
                              No steps yet. Add a step manually or ask the AI assistant.
                            </p>
                          ) : (
                            pipeline.steps.map((step) => (
                              <div
                                key={step.id}
                                className={`flex items-start gap-3 p-3 rounded-lg border transition-colors ${
                                  step.is_enabled
                                    ? 'border-[var(--border-default)] bg-[var(--bg-surface-alt)]'
                                    : 'border-dashed border-[var(--border-default)] bg-[var(--bg-surface)] opacity-60'
                                }`}
                              >
                                <span className="text-xs font-mono bg-[var(--bg-canvas)] text-[var(--text-secondary)] px-1.5 py-0.5 rounded mt-0.5 flex-shrink-0 border border-[var(--border-subtle)]">
                                  #{step.step_order}
                                </span>
                                <div className="flex-1 min-w-0">
                                  <p className="text-sm font-medium text-[var(--text-primary)]">
                                    {recipeMap[step.recipe_type] ?? step.recipe_type}
                                  </p>
                                  <p className="text-xs text-[var(--text-secondary)] mt-0.5 font-mono truncate">
                                    {JSON.stringify(step.config_json)}
                                  </p>
                                </div>
                                <div className="flex items-center gap-1 flex-shrink-0">
                                  <button
                                    type="button"
                                    onClick={() => void handleToggleStep(pipeline.id, step)}
                                    title={step.is_enabled ? 'Disable step' : 'Enable step'}
                                    className="p-1 rounded text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"
                                  >
                                    {step.is_enabled ? (
                                      <ToggleRight size={15} className="text-[var(--brand)]" />
                                    ) : (
                                      <ToggleLeft size={15} />
                                    )}
                                  </button>
                                  <button
                                    type="button"
                                    onClick={() => void handleDeleteStep(pipeline.id, step.id)}
                                    disabled={deletingStepId === step.id}
                                    className="p-1 rounded text-[var(--text-muted)] hover:text-red-400 transition-colors disabled:opacity-40"
                                  >
                                    {deletingStepId === step.id ? (
                                      <Loader2 size={14} className="animate-spin" />
                                    ) : (
                                      <Trash2 size={14} />
                                    )}
                                  </button>
                                </div>
                              </div>
                            ))
                          )}
                          <button
                            type="button"
                            onClick={() => {
                              setAddStepPipelineId(pipeline.id);
                              setSelectedPipelineId(pipeline.id);
                            }}
                            className="self-start flex items-center gap-1.5 text-sm text-[var(--brand)] hover:opacity-90 font-medium transition-opacity"
                          >
                            <Plus size={15} /> Add Step
                          </button>
                        </div>

                        <div className="bg-[var(--bg-surface)] border border-[var(--border-default)] rounded-xl shadow-sm overflow-hidden">
                          <button
                            type="button"
                            onClick={() => togglePipelineValidation(pipeline.id)}
                            className="w-full px-4 py-3 flex items-center justify-between text-left hover:bg-[var(--bg-surface-alt)] transition-colors"
                          >
                            <span className="text-sm font-medium text-[var(--text-primary)]">Validation Preview</span>
                            {validationOpen ? (
                              <ChevronUp size={15} className="text-[var(--text-muted)]" />
                            ) : (
                              <ChevronDown size={15} className="text-[var(--text-muted)]" />
                            )}
                          </button>

                          {validationOpen && (
                            <div className="px-4 pb-4">
                              {!validation ? (
                                <p className="text-sm text-[var(--text-secondary)] text-center py-3 border border-dashed border-[var(--border-default)] rounded-lg">
                                  Click Validate to preview this pipeline.
                                </p>
                              ) : validation.ok ? (
                                <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-lg p-3">
                                  <div className="flex items-center gap-2 mb-2">
                                    <CheckCircle2 size={16} className="text-emerald-300" />
                                    <span className="text-sm font-medium text-emerald-300">All steps passed</span>
                                  </div>
                                  {validation.output_sample.length > 0 && (
                                    <pre className="text-xs bg-[var(--bg-surface)] border border-emerald-500/20 rounded p-3 overflow-x-auto text-[var(--text-secondary)]">
                                      {JSON.stringify(validation.output_sample.slice(0, 3), null, 2)}
                                    </pre>
                                  )}
                                </div>
                              ) : (
                                <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
                                  <div className="flex items-center gap-2 mb-2">
                                    <XCircle size={16} className="text-red-300" />
                                    <span className="text-sm font-medium text-red-300">Validation failed</span>
                                  </div>
                                  <ul className="text-xs space-y-1">
                                    {validation.step_results
                                      .filter((result) => !result.ok)
                                      .map((result) => (
                                        <li
                                          key={`${result.step_order}-${result.recipe_type}`}
                                          className="text-red-200"
                                        >
                                          Step {result.step_order} ({result.recipe_type}): {result.error}
                                        </li>
                                      ))}
                                  </ul>
                                </div>
                              )}
                            </div>
                          )}
                        </div>

                        {runs.length > 0 && (
                          <div className="bg-[var(--bg-surface)] border border-[var(--border-default)] rounded-xl shadow-sm p-3">
                            <h3 className="text-sm font-medium text-[var(--text-primary)] mb-2">Recent Runs</h3>
                            <div className="space-y-1">
                              {runs.slice(0, 5).map((run) => (
                                <div key={run.id} className="flex items-center justify-between text-xs py-1.5 border-b border-[var(--border-subtle)] last:border-0">
                                  <span className="text-[var(--text-secondary)]">
                                    {new Date(run.started_at || Date.now()).toLocaleDateString(undefined, {
                                      month: 'short',
                                      day: 'numeric',
                                      hour: '2-digit',
                                      minute: '2-digit',
                                    })}
                                  </span>
                                  <span
                                    className={`px-1.5 py-0.5 rounded font-medium ${
                                      run.status === 'succeeded'
                                        ? 'bg-emerald-500/15 text-emerald-300 border border-emerald-500/35'
                                        : run.status === 'failed'
                                        ? 'bg-red-500/15 text-red-300 border border-red-500/35'
                                        : 'bg-amber-500/15 text-amber-300 border border-amber-500/35'
                                    }`}
                                  >
                                    {run.status}
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        <div className="text-right">
                          <button
                            type="button"
                            className="text-xs text-[var(--text-muted)] hover:text-[var(--text-primary)]"
                            onClick={() => setSelectedPipelineId(pipeline.id)}
                          >
                            Keep chat focused on this pipeline
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>

            <div className="w-80 flex-none flex flex-col min-h-0">
              {selectedPipeline ? (
                <>
                  <div className="mb-2 text-xs text-[var(--text-muted)]">
                    Chat context: {selectedPipeline.name}
                    {selectedPipelineSourceName ? ` (${selectedPipelineSourceName})` : ''}
                  </div>
                  <DEChatPanel
                    pipelineId={selectedPipeline.id}
                    connectorConfigId={selectedPipelinePrimarySource?.id ?? null}
                    connectorName={selectedPipelineSourceName || selectedPipelinePrimarySource?.connector_name}
                    onPipelineChanged={refreshPipelines}
                  />
                </>
              ) : (
                <div className="h-full rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] flex items-center justify-center text-sm text-[var(--text-muted)]">
                  Select a pipeline to open chat.
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {addStepPipeline && addStepPipelineId && (
        <AddStepModal
          recipes={recipes}
          nextStepOrder={
            addStepPipeline.steps.length > 0
              ? Math.max(...addStepPipeline.steps.map((s) => s.step_order)) + 1
              : 1
          }
          availableColumns={availableColumns}
          onSave={handleAddStep}
          onClose={() => setAddStepPipelineId(null)}
        />
      )}
    </DataPageFrame>
  );
}