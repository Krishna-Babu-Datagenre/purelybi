import type { ChatChartItem } from '../types';
import type { ChatMessageWithCharts, StreamingToolCall, ToolCallResult } from '../store/useChatStore';

/** One chronological block: assistant text, tool activity, or a rendered chart/KPI (guided mode). */
export type MagicTimelineSegment =
  | { type: 'text'; content: string }
  | { type: 'tools'; steps: RichToolStep[] }
  | { type: 'chart'; item: ChatChartItem };

/** Collect chart widgets from timeline segments (for assistant `charts` on the message). */
export function chartsFromTimeline(segments: MagicTimelineSegment[]): ChatChartItem[] {
  const out: ChatChartItem[] = [];
  for (const seg of segments) {
    if (seg.type === 'chart') out.push(seg.item);
  }
  return out;
}

export interface RichToolStep {
  id: string;
  toolName: string;
  phase: 'done' | 'running';
  input: string | null;
  output: string | null;
}

function toolCallIdsMatch(a: string, b: string): boolean {
  return String(a) === String(b);
}

export function formatToolPayload(raw: string | null | undefined): string {
  if (raw == null || !String(raw).trim()) return '';
  const s = String(raw);
  try {
    const j = JSON.parse(s);
    return JSON.stringify(j, null, 2);
  } catch {
    return s;
  }
}

/** Tool calls saved on a completed assistant message */
export function toolCallsToRichSteps(toolCalls: ToolCallResult[], keyPrefix: string): RichToolStep[] {
  return toolCalls.map((tc, ti) => ({
    id: `${keyPrefix}-${ti}-${tc.tool_name ?? 't'}`,
    toolName: tc.tool_name ?? 'tool',
    phase: 'done' as const,
    input: null,
    output: tc.content ?? null,
  }));
}

/** In-flight turn: completed results first, then active streaming tool calls */
export function buildCurrentTurnSteps(
  streamingToolCalls: StreamingToolCall[],
  currentTurnToolResults: ChatMessageWithCharts[],
): RichToolStep[] {
  const out: RichToolStep[] = [];
  currentTurnToolResults.forEach((m, i) => {
    out.push({
      id: m.tool_call_id ?? `cur-done-${i}`,
      toolName: m.tool_name ?? 'tool',
      phase: 'done',
      input: null,
      output: m.content ?? null,
    });
  });
  streamingToolCalls.forEach((tc) => {
    out.push({
      id: tc.id,
      toolName: tc.name,
      phase: 'running',
      input: tc.args || null,
      output: null,
    });
  });
  return out;
}

/**
 * Before a new tool call starts, mark every still-`running` step as `done` across all
 * timeline segments. Without this, assistant text (or chart segments in guided mode)
 * splits tools into multiple `{ type: 'tools' }` blocks; each `MagicToolTimelineRows` only
 * sees its own steps, so earlier rows never receive the in-row "stale running" fix.
 */
export function markAllRunningToolStepsComplete(segments: MagicTimelineSegment[]): MagicTimelineSegment[] {
  return segments.map((seg) => {
    if (seg.type !== 'tools') return seg;
    return {
      ...seg,
      steps: seg.steps.map((s) =>
        s.phase === 'running'
          ? { ...s, phase: 'done' as const, output: s.output ?? null }
          : s,
      ),
    };
  });
}

export function patchToolStepArgs(
  segments: MagicTimelineSegment[],
  toolCallId: string,
  args: string,
): MagicTimelineSegment[] {
  return segments.map((seg) => {
    if (seg.type !== 'tools') return seg;
    return {
      ...seg,
      steps: seg.steps.map((s) =>
        toolCallIdsMatch(s.id, toolCallId) ? { ...s, input: args || null } : s,
      ),
    };
  });
}

export function patchToolStepComplete(
  segments: MagicTimelineSegment[],
  toolCallId: string,
  toolName: string | undefined,
  output: string,
): MagicTimelineSegment[] {
  return segments.map((seg) => {
    if (seg.type !== 'tools') return seg;
    return {
      ...seg,
      steps: seg.steps.map((s) =>
        toolCallIdsMatch(s.id, toolCallId)
          ? {
              ...s,
              toolName: toolName ?? s.toolName,
              phase: 'done' as const,
              output,
            }
          : s,
      ),
    };
  });
}

/** Join all text segments + optional trailing text for a single assistant `content` string */
export function joinMagicTextContent(
  segments: MagicTimelineSegment[],
  trailing: string,
): string {
  const parts: string[] = [];
  for (const seg of segments) {
    if (seg.type === 'text' && seg.content.trim()) parts.push(seg.content);
  }
  if (trailing.trim()) parts.push(trailing);
  return parts.join('\n\n');
}
