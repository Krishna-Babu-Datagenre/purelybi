import { useMemo } from 'react';
import { motion, MotionConfig } from 'motion/react';
import { CheckCircle2, Loader2 } from 'lucide-react';
import type { RichToolStep } from '../utils/magicToolTimeline';
import { formatToolPayload } from '../utils/magicToolTimeline';
import { friendlyDashboardToolLabel } from '../utils/dashboardToolLabels';

/**
 * Product UI: label + status only (no expand). Set to true locally to inspect tool I/O
 * (input/output payloads are still built in the store; this only toggles visibility).
 */
const SHOW_MAGIC_TOOL_IO_DETAILS = false;

interface MagicToolTimelineRowsProps {
  steps: RichToolStep[];
  sectionLabel?: string;
}

export default function MagicToolTimelineRows({ steps, sectionLabel }: MagicToolTimelineRowsProps) {
  const timeline = steps;

  const lastRunningIndex = useMemo(() => {
    for (let i = timeline.length - 1; i >= 0; i--) {
      if (timeline[i].phase === 'running') return i;
    }
    return -1;
  }, [timeline]);

  if (timeline.length === 0) return null;

  return (
    <MotionConfig reducedMotion="user">
      <div className="space-y-2 mt-2">
        {sectionLabel && (
          <div className="flex items-center gap-2 px-0.5 pb-1">
            <span className="text-xs font-semibold uppercase tracking-wide text-[var(--text-secondary)]">
              {sectionLabel}
            </span>
          </div>
        )}

        {timeline.map((step, i) => {
          const hasOutput = Boolean(step.output && step.output.trim());
          /** Older "running" rows when a newer tool started — treat as finished */
          const isStaleRunning = step.phase === 'running' && i < lastRunningIndex;
          /** Defensive: result arrived but phase/id matching failed upstream */
          const isDone = step.phase === 'done' || hasOutput || isStaleRunning;
          const isActiveRunning = !isDone && step.phase === 'running' && i === lastRunningIndex;

          return (
            <motion.div
              key={step.id}
              layout
              initial={{ opacity: 0, y: 4 }}
              animate={{ opacity: 1, y: 0 }}
              className={`rounded-xl border overflow-hidden ${
                isActiveRunning
                  ? 'border-emerald-500/45 bg-emerald-500/[0.07] dark:bg-emerald-500/[0.1] shadow-[0_0_0_1px_rgba(16,185,129,0.12)]'
                  : 'border-[var(--border-default)] bg-[var(--bg-elevated)]'
              }`}
            >
              <div className="flex items-center gap-3 px-3 py-2.5">
                {isDone ? (
                  <CheckCircle2 size={16} className="text-emerald-600 dark:text-emerald-400 shrink-0" aria-hidden />
                ) : (
                  <Loader2
                    size={16}
                    className="animate-spin text-emerald-600 dark:text-emerald-400 shrink-0"
                    aria-hidden
                  />
                )}
                <span
                  className={`text-sm font-semibold flex-1 min-w-0 ${
                    isDone
                      ? 'text-emerald-700 dark:text-emerald-300'
                      : 'text-emerald-600 dark:text-emerald-400'
                  }`}
                >
                  {friendlyDashboardToolLabel(step.toolName)}
                </span>
              </div>

              {SHOW_MAGIC_TOOL_IO_DETAILS && (
                <div className="px-3 pb-3 pt-0 space-y-2 border-t border-[var(--border-subtle)]">
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-[var(--text-muted)] mb-1">
                      Input
                    </p>
                    <pre className="text-xs text-[var(--text-secondary)] bg-[var(--bg-surface)] rounded-lg p-2 overflow-x-auto max-h-40 overflow-y-auto border border-[var(--border-subtle)]">
                      {step.input && step.input.trim() ? formatToolPayload(step.input) : isActiveRunning ? '…' : '—'}
                    </pre>
                  </div>
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-wide text-[var(--text-muted)] mb-1">
                      Output
                    </p>
                    <pre className="text-xs text-[var(--text-secondary)] bg-[var(--bg-surface)] rounded-lg p-2 overflow-x-auto max-h-48 overflow-y-auto border border-[var(--border-subtle)] whitespace-pre-wrap break-words">
                      {isDone && step.output && step.output.trim()
                        ? formatToolPayload(step.output)
                        : isActiveRunning
                          ? 'Executing…'
                          : '—'}
                    </pre>
                  </div>
                </div>
              )}
            </motion.div>
          );
        })}
      </div>
    </MotionConfig>
  );
}
