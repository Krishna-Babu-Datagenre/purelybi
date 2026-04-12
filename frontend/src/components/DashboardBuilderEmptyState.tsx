import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import {
  Sparkles,
  MessagesSquare,
  Loader2,
  RefreshCw,
  AlertCircle,
  LayoutDashboard,
  ArrowLeft,
  Send,
} from 'lucide-react';
import { getDashboardBuilderReadiness } from '../services/backendClient';
import type { DashboardBuilderReadiness } from '../types';
import { useDashboardBuilderStore } from '../store/useDashboardBuilderStore';
import { useDashboardStore } from '../store/useDashboardStore';
import DashboardBuilderMessageList from './DashboardBuilderMessageList';
import DashboardBuilderSetupDialog from './DashboardBuilderSetupDialog';

function toggleInList(list: string[], id: string): string[] {
  if (list.includes(id)) return list.filter((x) => x !== id);
  return [...list, id];
}

const GUIDED_KICKOFF_INSTRUCTIONS = `Please propose a short plan based on my data and goals, then ask me to confirm before you create dashboards or add widgets. After I confirm, create KPIs and charts, show them in this chat, and ask for feedback so we can refine or add elements. When I am satisfied, help me confirm and save everything to the dashboard.`;

/** Survives tab switches: `DashboardBuilderEmptyState` unmounts and local React state resets. */
let dashboardBuilderReadinessCache: DashboardBuilderReadiness | null = null;

export default function DashboardBuilderEmptyState() {
  const setNavigationPage = useDashboardStore((s) => s.setNavigationPage);
  const [readiness, setReadiness] = useState<DashboardBuilderReadiness | null>(
    () => dashboardBuilderReadinessCache,
  );
  const [readinessLoadError, setReadinessLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(() => dashboardBuilderReadinessCache === null);
  const [selected, setSelected] = useState<string[]>(() =>
    dashboardBuilderReadinessCache?.datasets?.length
      ? [...dashboardBuilderReadinessCache.datasets]
      : [],
  );
  const [setupName, setSetupName] = useState('');
  const [setupGoal, setSetupGoal] = useState('');
  const [input, setInput] = useState('');

  const setMode = useDashboardBuilderStore((s) => s.setMode);
  const mode = useDashboardBuilderStore((s) => s.mode);
  const wizardStep = useDashboardBuilderStore((s) => s.wizardStep);
  const setWizardStep = useDashboardBuilderStore((s) => s.setWizardStep);
  const pendingMode = useDashboardBuilderStore((s) => s.pendingMode);
  const setPendingMode = useDashboardBuilderStore((s) => s.setPendingMode);
  const setSelectedDatasets = useDashboardBuilderStore((s) => s.setSelectedDatasets);
  const sendMessage = useDashboardBuilderStore((s) => s.sendMessage);
  const sendMagicKickoff = useDashboardBuilderStore((s) => s.sendMagicKickoff);
  const clearHistory = useDashboardBuilderStore((s) => s.clearHistory);
  const resetWizard = useDashboardBuilderStore((s) => s.resetWizard);
  const abortActiveStream = useDashboardBuilderStore((s) => s.abortActiveStream);
  const isStreaming = useDashboardBuilderStore((s) => s.isStreaming);
  const streamingToolCalls = useDashboardBuilderStore((s) => s.streamingToolCalls);
  const currentTurnToolResults = useDashboardBuilderStore((s) => s.currentTurnToolResults);
  const builderMessages = useDashboardBuilderStore((s) => s.messages);
  const streamingContent = useDashboardBuilderStore((s) => s.streamingContent);
  const magicSegments = useDashboardBuilderStore((s) => s.magicSegments);

  /** Scroll container for Magic / Guided unified timeline (only one visible at a time). */
  const builderChatScrollRef = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const el = builderChatScrollRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [
    builderMessages,
    streamingContent,
    magicSegments,
    streamingToolCalls,
    currentTurnToolResults,
    isStreaming,
    wizardStep,
  ]);

  const load = useCallback(
    async (opts?: { silent?: boolean }) => {
      const silent = opts?.silent === true;
      if (!silent) {
        setLoading(true);
        setReadinessLoadError(null);
      }
      try {
        const r = await getDashboardBuilderReadiness();
        dashboardBuilderReadinessCache = r;
        setReadiness(r);
        setSelected(r.datasets.length ? [...r.datasets] : []);
        setSelectedDatasets(r.datasets.length ? [...r.datasets] : null);
        if (!silent) {
          setReadinessLoadError(null);
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : 'Could not load data status.';
        if (!silent || !dashboardBuilderReadinessCache) {
          setReadiness(null);
          dashboardBuilderReadinessCache = null;
          setReadinessLoadError(msg);
        }
      } finally {
        if (!silent) {
          setLoading(false);
        }
      }
    },
    [setSelectedDatasets],
  );

  useEffect(() => {
    void load({ silent: dashboardBuilderReadinessCache !== null });
  }, [load]);

  useEffect(() => {
    if (!readiness?.datasets.length) {
      setSelectedDatasets(null);
      return;
    }
    if (selected.length === readiness.datasets.length) {
      setSelectedDatasets(null);
      return;
    }
    setSelectedDatasets(selected);
  }, [selected, readiness?.datasets, setSelectedDatasets]);

  /** Backend can report readiness in more than one field; treat any as usable. */
  const canBuild =
    readiness != null &&
    (readiness.status === 'ready' ||
      readiness.has_synced_data === true ||
      (readiness.datasets?.length ?? 0) > 0);

  const waiting = readiness?.status === 'waiting_sync';
  const noConn = readiness?.status === 'no_connector';

  const openSetup = (m: 'magic' | 'guided') => {
    setPendingMode(m);
    setSetupName('');
    setSetupGoal('');
    if (readiness?.datasets.length) {
      setSelected([...readiness.datasets]);
    }
    setWizardStep('setup');
  };

  const handleSetupStart = () => {
    const name = setupName.trim();
    if (!name || !pendingMode || !canBuild) return;
    if (pendingMode === 'magic') {
      void sendMagicKickoff({
        dashboardName: name,
        goal: setupGoal.trim() || undefined,
      });
      return;
    }
    setMode('guided');
    setWizardStep('guided');
    const goalLine = setupGoal.trim() ? ` Context: ${setupGoal.trim()}.` : '';
    void sendMessage(
      `I'd like to create a dashboard named "${name}".${goalLine}\n\n${GUIDED_KICKOFF_INSTRUCTIONS}`,
    );
  };

  const setupDialogOpen = wizardStep === 'setup' && pendingMode !== null;
  /** Keep mode cards visible behind the setup dialog (same pattern as Data: list → detail → chat). */
  const showModePicker = wizardStep === 'mode' || wizardStep === 'setup';
  /** Chat + composer only after the user completes setup and clicks Start (like “Start guided setup” → chat). */
  const showChatPanel =
    wizardStep === 'magic-running' || wizardStep === 'magic-done' || wizardStep === 'guided';
  const showMagicPanels = wizardStep === 'magic-running' || wizardStep === 'magic-done';
  const showGuidedChrome = wizardStep === 'guided';
  const allowChatInput = showGuidedChrome || wizardStep === 'magic-done';

  const runOptionSummary =
    mode === 'magic'
      ? wizardStep === 'magic-done' && !isStreaming
        ? 'Your dashboard was generated from your connected data. You can open it from the sidebar when it appears, or keep chatting to refine it.'
        : 'We auto-generate KPIs and charts in one run while you watch the activity timeline below.'
      : 'Co-design step by step in chat: confirm the plan, then refine KPIs and charts before saving.';
  const showPatienceLine = isStreaming || wizardStep === 'magic-running';

  const handleGoBackToMode = () => {
    abortActiveStream();
    resetWizard({ clearChat: true });
  };

  /** Fills space below header; scrolls internally as messages grow (needs parent overflow-hidden + min-h-0 chain). */
  const transcriptScrollClass = 'flex-1 min-h-0 overflow-y-auto overscroll-y-contain';

  /** Shared with onboarding chat: transcript in a card; composer is a separate row below (not glued to viewport edge). */
  const composerTextareaClass =
    'flex-1 min-w-0 rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] px-4 py-3 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--border-strong)] focus:ring-1 focus:ring-[var(--brand)] disabled:opacity-50 resize-y min-h-[2.5rem] max-h-36 transition-colors duration-200';
  const composerSendClass =
    'shrink-0 inline-flex items-center justify-center rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-4 py-3 text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] transition-colors duration-200 cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]';

  return (
    <div
      className={
        showChatPanel
          ? 'w-full max-w-3xl mx-auto px-6 pt-3 pb-4 md:pb-5 grid grid-rows-[auto_minmax(0,1fr)_auto] min-h-0 h-full flex-1 overflow-hidden gap-3'
          : 'w-full max-w-5xl mx-auto px-6 py-8 flex flex-col gap-8 min-h-full'
      }
      style={{ paddingLeft: 'max(1.5rem, 0.9375rem)', paddingRight: 'max(1.5rem, 0.9375rem)' }}
    >
      {showChatPanel ? (
        <header className="flex flex-wrap items-start justify-between gap-3 shrink-0 pb-3 border-b border-[var(--border-subtle)]">
          <div className="space-y-1 min-w-0 max-w-2xl">
            <h1 className="text-base font-semibold tracking-tight text-[var(--text-primary)]">
              {mode === 'magic' && wizardStep === 'magic-done' && !isStreaming
                ? 'Dashboard ready'
                : mode === 'magic'
                  ? 'Magic run'
                  : 'Guided co-design'}
            </h1>
            <p className="text-xs text-[var(--text-secondary)] leading-relaxed">{runOptionSummary}</p>
            {showPatienceLine && (
              <p className="text-xs text-[var(--text-muted)] leading-relaxed">
                Please keep this page open and wait—the assistant may take a minute while it queries your data and
                assembles widgets.
              </p>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-2 shrink-0">
            <button
              type="button"
              onClick={handleGoBackToMode}
              className="inline-flex items-center gap-1.5 rounded-lg border border-[var(--border-default)] bg-[var(--bg-elevated)] px-2.5 py-1.5 text-xs font-semibold text-[var(--text-primary)] hover:bg-[var(--bg-surface-alt)] transition-colors duration-200 cursor-pointer"
            >
              <ArrowLeft size={14} className="shrink-0" aria-hidden />
              Go back
            </button>
            <button
              type="button"
              onClick={() => void clearHistory()}
              disabled={isStreaming}
              className="text-xs font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors duration-200 cursor-pointer rounded-lg px-2.5 py-1.5 disabled:opacity-40 disabled:cursor-not-allowed focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
            >
              Start over
            </button>
          </div>
        </header>
      ) : (
        <>
          <header className="space-y-2">
            <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
              Build a dashboard with AI
            </h1>
            <p className="text-sm text-[var(--text-secondary)] max-w-2xl leading-relaxed">
              Choose Magic for an instant layout, or Guided to co-design step by step. You will name your dashboard and
              pick datasets before we start.
            </p>
          </header>

          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => void load()}
              disabled={loading}
              className="inline-flex items-center gap-2 rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface)] px-3 py-1.5 text-sm font-medium text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] cursor-pointer disabled:opacity-50"
            >
              {loading ? <Loader2 size={16} className="animate-spin" /> : <RefreshCw size={16} />}
              Refresh status
            </button>
          </div>
        </>
      )}

      <div
        className={
          showChatPanel
            ? 'min-h-0 overflow-hidden flex flex-col gap-4'
            : 'flex flex-col gap-4'
        }
      >
      {readinessLoadError && (
        <div className="rounded-xl border border-red-200 bg-red-50/90 dark:border-red-900/50 dark:bg-red-950/30 px-4 py-3 flex gap-3 items-start">
          <AlertCircle className="flex-shrink-0 mt-0.5 text-red-600 dark:text-red-400" size={20} />
          <div className="min-w-0 space-y-1">
            <p className="text-sm font-medium text-[var(--text-primary)]">Could not verify your data status</p>
            <p className="text-xs text-[var(--text-secondary)]">{readinessLoadError}</p>
          </div>
        </div>
      )}

      {loading && !readiness && !readinessLoadError && (
        <div className="flex items-center gap-2 text-sm text-[var(--text-secondary)]">
          <Loader2 size={18} className="animate-spin text-[var(--brand)]" />
          Checking your data…
        </div>
      )}

      {readiness && !canBuild && (
        <div
          className={`rounded-xl border px-4 py-3 flex gap-3 items-start ${
            waiting
              ? 'border-amber-200/80 bg-amber-50/90 dark:border-amber-900/50 dark:bg-amber-950/30'
              : 'border-slate-200 bg-slate-50/90 dark:border-slate-700 dark:bg-slate-900/40'
          }`}
        >
          <AlertCircle
            className={`flex-shrink-0 mt-0.5 ${waiting ? 'text-amber-600 dark:text-amber-400' : 'text-slate-500'}`}
            size={20}
          />
          <div className="min-w-0 space-y-2">
            <p className="text-sm font-medium text-[var(--text-primary)]">{readiness.message}</p>
            {noConn && (
              <button
                type="button"
                onClick={() => setNavigationPage('data-connect')}
                className="text-sm font-medium text-[var(--brand)] hover:underline cursor-pointer"
              >
                Connect a data source
              </button>
            )}
            {waiting && (
              <p className="text-xs text-[var(--text-secondary)]">
                After sync completes, use Refresh status or open{' '}
                <button
                  type="button"
                  onClick={() => setNavigationPage('data-raw-tables')}
                  className="text-[var(--brand)] font-medium hover:underline cursor-pointer"
                >
                  View raw tables
                </button>{' '}
                to confirm files landed.
              </p>
            )}
          </div>
        </div>
      )}

      <AnimatePresence mode="wait">
        {showModePicker && (
          <motion.section
            key="mode-pick"
            className="space-y-4"
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            transition={{ duration: 0.2 }}
          >
            <p className="text-xs font-semibold uppercase tracking-wide text-[var(--text-secondary)]">
              Step 1 — Choose creation mode
              {wizardStep === 'setup' && (
                <span className="ml-2 font-normal normal-case text-[var(--text-muted)]">(continue in the dialog)</span>
              )}
            </p>
            <div className="grid gap-4 sm:grid-cols-2">
              <motion.button
                type="button"
                disabled={!canBuild || isStreaming || wizardStep === 'setup'}
                onClick={() => openSetup('magic')}
                className="group text-left rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-5 shadow-sm hover:border-[var(--brand)]/35 hover:shadow-md cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
                whileHover={canBuild && !isStreaming ? { y: -2 } : undefined}
                whileTap={canBuild && !isStreaming ? { scale: 0.99 } : undefined}
              >
                <div className="flex items-center gap-2 mb-2">
                  <span className="inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[var(--brand)]/10 text-[var(--brand)]">
                    <Sparkles size={22} />
                  </span>
                  <span className="text-base font-semibold text-[var(--text-primary)]">Magic (Surprise me)</span>
                </div>
                <p className="text-sm text-[var(--text-secondary)] leading-relaxed">
                  Auto-generate KPIs and charts in one run with a focused activity view while the assistant works.
                </p>
              </motion.button>

              <motion.button
                type="button"
                disabled={!canBuild || isStreaming || wizardStep === 'setup'}
                onClick={() => openSetup('guided')}
                className="group text-left rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-5 shadow-sm hover:border-[var(--brand)]/35 hover:shadow-md cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
                whileHover={canBuild && !isStreaming ? { y: -2 } : undefined}
                whileTap={canBuild && !isStreaming ? { scale: 0.99 } : undefined}
              >
                <div className="flex items-center gap-2 mb-2">
                  <span className="inline-flex h-10 w-10 items-center justify-center rounded-xl bg-[var(--brand)]/10 text-[var(--brand)]">
                    <MessagesSquare size={22} />
                  </span>
                  <span className="text-base font-semibold text-[var(--text-primary)]">Guided (Interactive)</span>
                </div>
                <p className="text-sm text-[var(--text-secondary)] leading-relaxed">
                  Chat-based flow: plan, confirm, then iterate on KPIs and charts before saving.
                </p>
              </motion.button>
            </div>
          </motion.section>
        )}
      </AnimatePresence>

      {!showChatPanel && canBuild && (
        <div className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-5 py-4">
          <p className="text-sm text-[var(--text-secondary)] leading-relaxed">
            {wizardStep === 'setup' ? (
              <>
                Finish <span className="font-medium text-[var(--text-primary)]">Step 2</span> in the dialog, then click{' '}
                <span className="font-medium text-[var(--text-primary)]">Start</span>. The assistant chat opens only after
                that — same pattern as Data → Connect a source → <span className="font-medium text-[var(--text-primary)]">Start guided setup</span>.
              </>
            ) : (
              <>
                Pick Magic or Guided, then complete Step 2 in the dialog. The chat appears only after{' '}
                <span className="font-medium text-[var(--text-primary)]">Start</span>, like the connect flow.
              </>
            )}
          </p>
        </div>
      )}

      <DashboardBuilderSetupDialog
        open={setupDialogOpen}
        mode={pendingMode ?? 'magic'}
        dashboardName={setupName}
        onDashboardNameChange={setSetupName}
        goal={setupGoal}
        onGoalChange={setSetupGoal}
        datasetNames={readiness?.datasets ?? []}
        selectedDatasets={selected}
        onToggleDataset={(name) =>
          setSelected((s) => {
            const next = toggleInList(s, name);
            if (readiness && next.length === 0) return [...readiness.datasets];
            return next;
          })
        }
        canSubmit={setupName.trim().length > 0}
        disabled={isStreaming || !canBuild}
        onBack={() => {
          setWizardStep('mode');
          setPendingMode(null);
        }}
        onStart={handleSetupStart}
      />

      {showMagicPanels && showChatPanel && (
        <div className="flex flex-1 min-h-0 flex-col gap-3 min-w-0">
          <div className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] shadow-[0_1px_0_rgba(255,255,255,0.04)_inset] flex min-h-0 flex-1 flex-col overflow-hidden">
            <div ref={builderChatScrollRef} className={transcriptScrollClass}>
              <div className="p-4 md:p-5 space-y-1">
                <DashboardBuilderMessageList
                  variant="magicUnified"
                  emptyHint="The assistant will outline the plan, then run tools below each message."
                />
              </div>
            </div>
            {wizardStep === 'magic-running' && (
              <div className="px-4 py-2.5 border-t border-[var(--border-subtle)] bg-[var(--bg-canvas)] text-xs text-[var(--text-secondary)]">
                {isStreaming ? 'Building your dashboard automatically…' : 'Finishing up…'}
              </div>
            )}
          </div>
          {wizardStep !== 'magic-running' && (
            <form
              className="flex gap-2 items-end shrink-0"
              onSubmit={(e) => {
                e.preventDefault();
                if (!input.trim() || isStreaming || !canBuild || !allowChatInput) return;
                if (wizardStep === 'magic-done') {
                  setMode('guided');
                  setWizardStep('guided');
                }
                void sendMessage(input);
                setInput('');
              }}
            >
              <textarea
                rows={2}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    if (!input.trim() || isStreaming || !canBuild || !allowChatInput) return;
                    e.currentTarget.form?.requestSubmit();
                  }
                }}
                placeholder={
                  !canBuild
                    ? 'Connect and sync data to chat with the assistant'
                    : wizardStep === 'magic-done'
                      ? 'Reply to refine, regenerate, or add dashboard elements…'
                      : 'Choose Magic or Guided above to begin.'
                }
                disabled={!canBuild || isStreaming || !allowChatInput}
                className={composerTextareaClass}
                aria-label="Message to assistant"
              />
              <button
                type="submit"
                disabled={!canBuild || isStreaming || !input.trim() || !allowChatInput}
                className={composerSendClass}
                aria-label="Send message"
              >
                {isStreaming ? <Loader2 size={18} className="animate-spin" /> : <Send size={18} aria-hidden />}
              </button>
            </form>
          )}
          {wizardStep === 'magic-done' && (
            <motion.div
              className="rounded-2xl border border-emerald-200/80 bg-emerald-50/90 dark:border-emerald-900/40 dark:bg-emerald-950/25 px-5 py-4"
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ type: 'spring', stiffness: 380, damping: 28 }}
            >
              <div className="flex items-start gap-3">
                <LayoutDashboard className="text-emerald-700 dark:text-emerald-400 shrink-0 mt-0.5" size={22} />
                <div className="min-w-0 space-y-2">
                  <p className="text-sm font-semibold text-[var(--text-primary)]">Your dashboard is ready</p>
                  <p className="text-sm text-[var(--text-secondary)] leading-relaxed">
                    You can open it from the sidebar when it appears in your list. To add more charts or KPI cards, use
                    the Copilot in the top-right corner.
                  </p>
                  <button
                    type="button"
                    onClick={() => resetWizard({ clearChat: true })}
                    className="text-sm font-semibold text-[var(--brand)] hover:underline cursor-pointer"
                  >
                    Create another dashboard
                  </button>
                </div>
              </div>
            </motion.div>
          )}
        </div>
      )}

      {showGuidedChrome && showChatPanel && (
        <div className="flex flex-1 min-h-0 flex-col gap-3 min-w-0">
          <div className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] shadow-[0_1px_0_rgba(255,255,255,0.04)_inset] flex min-h-0 flex-1 flex-col overflow-hidden">
            <div ref={builderChatScrollRef} className={transcriptScrollClass}>
              <div className="p-4 md:p-5 space-y-1">
                <DashboardBuilderMessageList
                  variant="magicUnified"
                  emptyHint="The assistant will propose a plan first. Reply to confirm or adjust before widgets are saved."
                />
              </div>
            </div>
          </div>
          <form
            className="flex gap-2 items-end shrink-0"
            onSubmit={(e) => {
              e.preventDefault();
              if (!input.trim() || isStreaming || !canBuild || !allowChatInput) return;
              void sendMessage(input);
              setInput('');
            }}
          >
            <textarea
              rows={2}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault();
                  if (!input.trim() || isStreaming || !canBuild || !allowChatInput) return;
                  e.currentTarget.form?.requestSubmit();
                }
              }}
              placeholder={
                !canBuild
                  ? 'Connect and sync data to chat with the assistant'
                  : 'Reply to refine, regenerate, or add dashboard elements…'
              }
              disabled={!canBuild || isStreaming || !allowChatInput}
              className={composerTextareaClass}
              aria-label="Message to assistant"
            />
            <button
              type="submit"
              disabled={!canBuild || isStreaming || !input.trim() || !allowChatInput}
              className={composerSendClass}
              aria-label="Send message"
            >
              {isStreaming ? <Loader2 size={18} className="animate-spin" /> : <Send size={18} aria-hidden />}
            </button>
          </form>
        </div>
      )}
      </div>

      {showChatPanel && (
        <p className="text-xs text-[var(--text-muted)] text-center shrink-0 pt-1">
          Use the sidebar to open a saved dashboard, or stay here to keep iterating with the assistant.
        </p>
      )}
    </div>
  );
}
