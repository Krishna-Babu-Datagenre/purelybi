import { useId, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'motion/react';
import { Sparkles, MessagesSquare, Loader2, X, Database } from 'lucide-react';

export interface DashboardBuilderSetupDialogProps {
  open: boolean;
  mode: 'magic' | 'guided';
  dashboardName: string;
  onDashboardNameChange: (v: string) => void;
  goal: string;
  onGoalChange: (v: string) => void;
  datasetNames: string[];
  selectedDatasets: string[];
  onToggleDataset: (name: string) => void;
  canSubmit: boolean;
  disabled: boolean;
  onBack: () => void;
  onStart: () => void;
}

export default function DashboardBuilderSetupDialog({
  open,
  mode,
  dashboardName,
  onDashboardNameChange,
  goal,
  onGoalChange,
  datasetNames,
  selectedDatasets,
  onToggleDataset,
  canSubmit,
  disabled,
  onBack,
  onStart,
}: DashboardBuilderSetupDialogProps) {
  const titleId = useId();
  const descId = useId();
  const isMagic = mode === 'magic';

  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = prev;
    };
  }, [open]);

  const modal = (
    <AnimatePresence>
      {open && (
        <motion.div
          className="fixed inset-0 z-[200] flex items-center justify-center p-4 sm:p-6"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
        >
          <motion.button
            type="button"
            className="absolute inset-0 bg-black/65 cursor-pointer"
            aria-label="Close setup"
            onClick={onBack}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          />
          <motion.div
            className="relative z-10 w-full max-w-md rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] shadow-[0_24px_48px_rgba(0,0,0,0.45)] overflow-hidden"
            initial={{ opacity: 0, y: 16, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 12, scale: 0.98 }}
            transition={{ type: 'spring', stiffness: 420, damping: 32 }}
            onClick={(e) => e.stopPropagation()}
            aria-modal
            role="dialog"
            aria-labelledby={titleId}
            aria-describedby={descId}
          >
            <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-4 border-b border-[var(--border-subtle)] bg-[var(--bg-elevated)]">
              <div className="flex items-center gap-3 min-w-0">
                <span className="inline-flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface-alt)] text-[var(--brand)]">
                  {isMagic ? <Sparkles size={22} /> : <MessagesSquare size={22} />}
                </span>
                <div className="min-w-0">
                  <p className="text-[10px] font-semibold uppercase tracking-wider text-[var(--text-muted)] mb-0.5">
                    Step 2 — Setup
                  </p>
                  <h2 id={titleId} className="text-lg font-semibold text-[var(--text-primary)] tracking-tight">
                    {isMagic ? 'Magic (Surprise me)' : 'Guided (Interactive)'}
                  </h2>
                  <p id={descId} className="text-xs text-[var(--text-secondary)] mt-0.5">
                    {isMagic
                      ? 'We will generate KPIs and charts in one run.'
                      : 'We will co-design step by step in chat.'}
                  </p>
                </div>
              </div>
              <button
                type="button"
                onClick={onBack}
                className="shrink-0 rounded-lg p-2 text-[var(--text-secondary)] hover:bg-[var(--bg-elevated)] hover:text-[var(--text-primary)] cursor-pointer"
                aria-label="Back"
              >
                <X size={18} />
              </button>
            </div>

            <div className="px-5 pb-5 space-y-4 max-h-[min(70vh,520px)] overflow-y-auto">
              <div className="space-y-1.5">
                <label htmlFor="dash-setup-name" className="text-xs font-medium text-[var(--text-secondary)]">
                  Dashboard name <span className="text-red-600 dark:text-red-400">*</span>
                </label>
                <input
                  id="dash-setup-name"
                  type="text"
                  value={dashboardName}
                  onChange={(e) => onDashboardNameChange(e.target.value)}
                  disabled={disabled}
                  placeholder="e.g. Sales overview"
                  className="w-full rounded-xl border border-[var(--border-default)] bg-[var(--bg-elevated)] px-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:ring-2 focus:ring-[var(--brand)]/30 disabled:opacity-50"
                />
              </div>

              {datasetNames.length > 0 && (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-xs font-semibold text-[var(--text-primary)]">
                    <Database size={14} className="text-[var(--brand)] shrink-0" />
                    Datasets
                  </div>
                  <p className="text-[11px] text-[var(--text-secondary)] leading-snug">
                    All are selected by default. Clear checkboxes to limit which tables the agent can use.
                  </p>
                  <div className="flex flex-wrap gap-2 max-h-28 overflow-y-auto rounded-xl border border-[var(--border-default)] bg-[var(--bg-elevated)] p-2.5">
                    {datasetNames.map((name) => {
                      const checked = selectedDatasets.includes(name);
                      return (
                        <label
                          key={name}
                          className="inline-flex items-center gap-2 rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-2 py-1 text-[11px] font-medium text-[var(--text-primary)] cursor-pointer hover:border-[var(--brand)]/40"
                        >
                          <input
                            type="checkbox"
                            className="rounded border-[var(--border-default)] cursor-pointer"
                            checked={checked}
                            disabled={disabled}
                            onChange={() => onToggleDataset(name)}
                          />
                          <span className="font-mono truncate max-w-[200px]" title={name}>
                            {name}
                          </span>
                        </label>
                      );
                    })}
                  </div>
                </div>
              )}

              <div className="space-y-1.5">
                <label htmlFor="dash-setup-goal" className="text-xs font-medium text-[var(--text-secondary)]">
                  High-level goal <span className="text-[var(--text-muted)] font-normal">(optional)</span>
                </label>
                <textarea
                  id="dash-setup-goal"
                  rows={2}
                  value={goal}
                  onChange={(e) => onGoalChange(e.target.value)}
                  disabled={disabled}
                  placeholder="e.g. Revenue trends and top products"
                  className="w-full rounded-xl border border-[var(--border-default)] bg-[var(--bg-elevated)] px-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:ring-2 focus:ring-[var(--brand)]/30 disabled:opacity-50 resize-none"
                />
              </div>

              <div className="flex flex-col-reverse sm:flex-row sm:justify-end gap-2 pt-1">
                <button
                  type="button"
                  onClick={onBack}
                  disabled={disabled}
                  className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-elevated)] px-4 py-2.5 text-sm font-medium text-[var(--text-primary)] hover:bg-[var(--bg-surface-alt)] cursor-pointer disabled:opacity-50"
                >
                  Back
                </button>
                <button
                  type="button"
                  onClick={onStart}
                  disabled={disabled || !canSubmit}
                  className="inline-flex items-center justify-center gap-2 rounded-xl bg-[var(--brand)] px-4 py-2.5 text-sm font-semibold text-white shadow-sm hover:opacity-95 cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {disabled ? <Loader2 size={18} className="animate-spin" /> : null}
                  Start
                </button>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );

  return createPortal(modal, document.body);
}
