import { create } from 'zustand';
import type { ChatMessage, ChatChartItem, SSEData, EChartsConfig } from '../types';
import type { SSEEventType } from '../services/chatApi';
import { streamChat, getChatHistory, clearChatHistory } from '../services/chatApi';

/** In-flight POST /api/chat stream for dashboard builder (aborted on reset / Go back). */
let dashboardStreamAbort: AbortController | null = null;
import type { ChatMessageWithCharts, StreamingToolCall } from './useChatStore';
import { useDashboardStore } from './useDashboardStore';
import type { MagicTimelineSegment, RichToolStep } from '../utils/magicToolTimeline';
import {
  chartsFromTimeline,
  joinMagicTextContent,
  markAllRunningToolStepsComplete,
  patchToolStepArgs,
  patchToolStepComplete,
} from '../utils/magicToolTimeline';

/** Attach charts from preceding tool messages onto each assistant message (history). */
function normalizeHistoryWithCharts(
  history: (ChatMessage & {
    chart?: {
      chartConfig: EChartsConfig;
      chart_type?: string;
      title?: string;
      dataConfig?: Record<string, unknown>;
    };
  })[],
): ChatMessageWithCharts[] {
  return history.map((msg, i) => {
    if (msg.role !== 'assistant') return msg as ChatMessageWithCharts;
    const withCharts = { ...msg } as ChatMessageWithCharts;
    const charts: ChatChartItem[] = [];
    for (let j = i - 1; j >= 0 && history[j].role === 'tool'; j--) {
      const t = history[j];
      if (t.chart?.chartConfig) {
        charts.unshift({
          chart_type: t.chart.chart_type,
          title: t.chart.title,
          chartConfig: t.chart.chartConfig,
          dataConfig: t.chart.dataConfig,
        });
      }
    }
    if (charts.length > 0) withCharts.charts = charts;
    return withCharts;
  });
}

function newSessionId(): string {
  return `dashboard-ai-${crypto.randomUUID()}`;
}

function finalizeMagicTimelineForMessage(state: {
  magicSegments: MagicTimelineSegment[];
  streamingContent: string;
}): { magicTimeline: MagicTimelineSegment[]; content: string | null } {
  const segs = [...state.magicSegments];
  if (state.streamingContent.trim()) {
    segs.push({ type: 'text', content: state.streamingContent });
  }
  const content = joinMagicTextContent(segs, '').trim() || null;
  return { magicTimeline: segs, content };
}

/** UI wizard for dashboard creation (mode pick → setup → magic/guided). */
export type DashboardBuilderWizardStep =
  | 'mode'
  | 'setup'
  | 'magic-running'
  | 'magic-done'
  | 'guided';

interface DashboardBuilderState {
  sessionId: string;
  mode: 'magic' | 'guided';
  wizardStep: DashboardBuilderWizardStep;
  /** Mode chosen before setup dialog (Magic vs Guided). */
  pendingMode: 'magic' | 'guided' | null;
  /** null = all datasets (omit from API) */
  selectedDatasets: string[] | null;
  /** Magic setup context for User Proxy AI (dashboard name + goal) */
  magicSetup: { dashboardName?: string; goal?: string } | null;
  messages: ChatMessageWithCharts[];
  streamingContent: string;
  streamingCharts: ChatChartItem[];
  isStreaming: boolean;
  error: string | null;
  streamingToolCalls: StreamingToolCall[];
  currentTurnToolResults: ChatMessageWithCharts[];
  /** Magic + guided: interleaved text, tools, and (guided) charts as SSE arrives */
  magicSegments: MagicTimelineSegment[];
  thoughtSectionCollapsed: boolean;

  setMode: (mode: 'magic' | 'guided') => void;
  setWizardStep: (step: DashboardBuilderWizardStep) => void;
  setPendingMode: (mode: 'magic' | 'guided' | null) => void;
  setSelectedDatasets: (ids: string[] | null) => void;
  resetConversation: () => void;
  /** Back to mode selection; optional full reset of chat. */
  resetWizard: (opts?: { clearChat?: boolean }) => void;
  /** Stop the active dashboard chat request (SSE). */
  abortActiveStream: () => void;

  sendMessage: (message: string) => Promise<void>;
  /** Magic mode: dashboard name + optional goal; sets wizard to magic-running. */
  sendMagicKickoff: (opts?: { goal?: string; dashboardName?: string }) => Promise<void>;
  loadHistory: () => Promise<void>;
  clearHistory: () => Promise<void>;
  clearError: () => void;
  setThoughtSectionCollapsed: (collapsed: boolean) => void;
}

export const useDashboardBuilderStore = create<DashboardBuilderState>((set, get) => ({
  sessionId: newSessionId(),
  mode: 'guided',
  wizardStep: 'mode',
  pendingMode: null,
  selectedDatasets: null,
  magicSetup: null,
  messages: [],
  streamingContent: '',
  streamingCharts: [],
  isStreaming: false,
  error: null,
  streamingToolCalls: [],
  currentTurnToolResults: [],
  magicSegments: [],
  thoughtSectionCollapsed: true,

  setMode: (mode) => set({ mode }),
  setWizardStep: (wizardStep) => set({ wizardStep }),
  setPendingMode: (pendingMode) => set({ pendingMode }),
  setSelectedDatasets: (ids) => set({ selectedDatasets: ids }),
  resetConversation: () =>
    set({
      sessionId: newSessionId(),
      messages: [],
      error: null,
      streamingContent: '',
      streamingCharts: [],
      magicSegments: [],
      magicSetup: null,
    }),

  abortActiveStream: () => {
    dashboardStreamAbort?.abort();
    dashboardStreamAbort = null;
  },

  resetWizard: (opts) => {
    dashboardStreamAbort?.abort();
    dashboardStreamAbort = null;
    const clearChat = opts?.clearChat ?? false;
    if (clearChat) {
      get().resetConversation();
    }
    set({
      wizardStep: 'mode',
      pendingMode: null,
      magicSetup: null,
      error: null,
      mode: 'guided',
      isStreaming: false,
      streamingContent: '',
      streamingCharts: [],
      streamingToolCalls: [],
      currentTurnToolResults: [],
      magicSegments: [],
      thoughtSectionCollapsed: true,
    });
  },

  clearError: () => set({ error: null }),
  setThoughtSectionCollapsed: (collapsed) => set({ thoughtSectionCollapsed: collapsed }),

  sendMagicKickoff: async (opts?: { goal?: string; dashboardName?: string }) => {
    const name = opts?.dashboardName?.trim();
    const goal = opts?.goal?.trim();
    const parts: string[] = [];
    if (name) {
      parts.push(`Create a new dashboard named "${name}".`);
    }
    if (goal) {
      parts.push(`Objective: ${goal}`);
    }
    const text =
      parts.length > 0
        ? `${parts.join(' ')} Generate a useful dashboard from my data with a mix of KPIs and charts.`
        : 'Generate a useful dashboard from my available data. Include a mix of KPIs and charts.';
    set({
      mode: 'magic',
      wizardStep: 'magic-running',
      magicSetup: {
        ...(name ? { dashboardName: name } : {}),
        ...(goal ? { goal } : {}),
      },
    });
    await get().sendMessage(text);
  },

  sendMessage: async (message: string) => {
    const { sessionId, messages, mode, selectedDatasets, magicSetup } = get();
    const trimmed = message.trim();
    if (!trimmed || get().isStreaming) return;

    const startedSessionId = sessionId;
    dashboardStreamAbort?.abort();
    const ac = new AbortController();
    dashboardStreamAbort = ac;

    const userMessage: ChatMessageWithCharts = { role: 'user', content: trimmed };

    set({
      messages: [...messages, userMessage],
      isStreaming: true,
      streamingContent: '',
      streamingCharts: [],
      streamingToolCalls: [],
      currentTurnToolResults: [],
      magicSegments: [],
      thoughtSectionCollapsed: false,
      error: null,
    });

    const onEvent = (event: SSEEventType, data: SSEData) => {
      if (get().sessionId !== startedSessionId) return;
      const state = get();
      if (event === 'start') return;

      if (event === 'token' && 'content' in data) {
        set({ streamingContent: state.streamingContent + data.content });
        return;
      }

      if (event === 'chart' && 'chartConfig' in data) {
        const chartData = data as import('../types').SSEChartData;
        const item: ChatChartItem = {
          chart_type: chartData.chart_type,
          title: chartData.title,
          chartConfig: chartData.chartConfig,
          dataConfig: chartData.dataConfig,
        };
        if (state.mode === 'guided') {
          const segs = [...state.magicSegments];
          let streamingContent = state.streamingContent;
          if (streamingContent.trim()) {
            segs.push({ type: 'text', content: streamingContent });
            streamingContent = '';
          }
          segs.push({ type: 'chart', item });
          set({ magicSegments: segs, streamingContent });
          return;
        }
        set({ streamingCharts: [...state.streamingCharts, item] });
        return;
      }

      if (event === 'tool_call_start' && 'tool_call_id' in data && 'tool_name' in data) {
        const d = data as import('../types').SSEToolCallStartData;
        const next = [
          ...state.streamingToolCalls,
          { id: String(d.tool_call_id), name: d.tool_name, args: '' },
        ];
        if (state.mode === 'magic' || state.mode === 'guided') {
          const segs = markAllRunningToolStepsComplete([...state.magicSegments]);
          let streamingContent = state.streamingContent;
          if (streamingContent.trim()) {
            segs.push({ type: 'text', content: streamingContent });
            streamingContent = '';
          }
          const step: RichToolStep = {
            id: String(d.tool_call_id),
            toolName: d.tool_name,
            phase: 'running',
            input: '',
            output: null,
          };
          const last = segs[segs.length - 1];
          let magicSegments: MagicTimelineSegment[];
          if (last?.type === 'tools') {
            magicSegments = [
              ...segs.slice(0, -1),
              { type: 'tools', steps: [...last.steps, step] },
            ];
          } else {
            magicSegments = [...segs, { type: 'tools', steps: [step] }];
          }
          set({ streamingToolCalls: next, streamingContent, magicSegments });
          return;
        }
        set({ streamingToolCalls: next });
        return;
      }

      if (event === 'tool_call_args' && 'tool_call_id' in data && 'args_chunk' in data) {
        const aid = String(data.tool_call_id);
        const next = state.streamingToolCalls.map((tc) =>
          String(tc.id) === aid ? { ...tc, args: tc.args + data.args_chunk } : tc,
        );
        if (state.mode === 'magic' || state.mode === 'guided') {
          const tid = String(data.tool_call_id);
          const tc = next.find((t) => t.id === tid);
          const magicSegments = patchToolStepArgs(state.magicSegments, tid, tc?.args ?? '');
          set({ streamingToolCalls: next, magicSegments });
          return;
        }
        set({ streamingToolCalls: next });
        return;
      }

      if (event === 'tool_result' && 'tool_call_id' in data && 'result' in data) {
        const toolResult = data as { tool_call_id: string; tool_name?: string; result: string };
        const toolMsg: ChatMessageWithCharts = {
          role: 'tool',
          content: toolResult.result,
          tool_call_id: toolResult.tool_call_id,
          tool_name: toolResult.tool_name,
        };
        const nextResults = [...state.currentTurnToolResults, toolMsg];
        const rid = String(toolResult.tool_call_id);
        const nextCalls = state.streamingToolCalls.filter((tc) => String(tc.id) !== rid);
        if (state.mode === 'magic' || state.mode === 'guided') {
          const magicSegments = patchToolStepComplete(
            state.magicSegments,
            String(toolResult.tool_call_id),
            toolResult.tool_name,
            toolResult.result,
          );
          set({
            currentTurnToolResults: nextResults,
            streamingToolCalls: nextCalls,
            magicSegments,
          });
          return;
        }
        set({ currentTurnToolResults: nextResults, streamingToolCalls: nextCalls });
        return;
      }

      if (event === 'segment_end') {
        const st = get();
        const {
          messages: prev,
          streamingCharts: charts,
          currentTurnToolResults,
        } = st;
        if (st.mode === 'magic') {
          const { magicTimeline, content } = finalizeMagicTimelineForMessage(st);
          const assistantMessage: ChatMessageWithCharts = {
            role: 'assistant',
            content,
            magicTimeline,
            charts: charts.length > 0 ? charts : undefined,
            toolCalls:
              currentTurnToolResults.length > 0
                ? currentTurnToolResults.map((m) => ({ tool_name: m.tool_name, content: m.content ?? '' }))
                : undefined,
          };
          set({
            messages: [...prev, assistantMessage],
            streamingContent: '',
            streamingCharts: [],
            streamingToolCalls: [],
            currentTurnToolResults: [],
            magicSegments: [],
            thoughtSectionCollapsed: false,
          });
          return;
        }
        if (st.mode === 'guided') {
          const { magicTimeline, content } = finalizeMagicTimelineForMessage(st);
          const chartList = chartsFromTimeline(magicTimeline);
          const assistantMessage: ChatMessageWithCharts = {
            role: 'assistant',
            content,
            magicTimeline,
            charts: chartList.length > 0 ? chartList : undefined,
            toolCalls:
              currentTurnToolResults.length > 0
                ? currentTurnToolResults.map((m) => ({ tool_name: m.tool_name, content: m.content ?? '' }))
                : undefined,
          };
          set({
            messages: [...prev, assistantMessage],
            streamingContent: '',
            streamingCharts: [],
            streamingToolCalls: [],
            currentTurnToolResults: [],
            magicSegments: [],
            thoughtSectionCollapsed: false,
          });
          return;
        }
        const content = st.streamingContent;
        const assistantMessage: ChatMessageWithCharts = {
          role: 'assistant',
          content: content || null,
          charts: charts.length > 0 ? charts : undefined,
          toolCalls:
            currentTurnToolResults.length > 0
              ? currentTurnToolResults.map((m) => ({ tool_name: m.tool_name, content: m.content ?? '' }))
              : undefined,
        };
        set({
          messages: [...prev, assistantMessage],
          streamingContent: '',
          streamingCharts: [],
          streamingToolCalls: [],
          currentTurnToolResults: [],
          thoughtSectionCollapsed: false,
        });
        return;
      }

      if (event === 'proxy_reply' && typeof data === 'object' && data !== null && 'content' in data) {
        const text = String((data as { content: string }).content);
        set({
          messages: [...get().messages, { role: 'user', content: text, proxyAuto: true }],
        });
        return;
      }

      if (event === 'end') {
        const st = get();
        const { messages: prev, streamingCharts: charts, currentTurnToolResults, mode } = st;
        const nextWizard =
          mode === 'magic' && get().wizardStep === 'magic-running'
            ? ({ wizardStep: 'magic-done' as const } as const)
            : {};
        if (mode === 'magic') {
          const { magicTimeline, content } = finalizeMagicTimelineForMessage(st);
          const assistantMessage: ChatMessageWithCharts = {
            role: 'assistant',
            content,
            magicTimeline,
            charts: charts.length > 0 ? charts : undefined,
            toolCalls:
              currentTurnToolResults.length > 0
                ? currentTurnToolResults.map((m) => ({ tool_name: m.tool_name, content: m.content ?? '' }))
                : undefined,
          };
          set({
            messages: [...prev, assistantMessage],
            streamingContent: '',
            streamingCharts: [],
            streamingToolCalls: [],
            currentTurnToolResults: [],
            magicSegments: [],
            thoughtSectionCollapsed: true,
            isStreaming: false,
            ...nextWizard,
          });
          void useDashboardStore.getState().fetchUserDashboardList({ forceRefresh: true });
          return;
        }
        if (mode === 'guided') {
          const { magicTimeline, content } = finalizeMagicTimelineForMessage(st);
          const chartList = chartsFromTimeline(magicTimeline);
          const assistantMessage: ChatMessageWithCharts = {
            role: 'assistant',
            content,
            magicTimeline,
            charts: chartList.length > 0 ? chartList : undefined,
            toolCalls:
              currentTurnToolResults.length > 0
                ? currentTurnToolResults.map((m) => ({ tool_name: m.tool_name, content: m.content ?? '' }))
                : undefined,
          };
          set({
            messages: [...prev, assistantMessage],
            streamingContent: '',
            streamingCharts: [],
            streamingToolCalls: [],
            currentTurnToolResults: [],
            magicSegments: [],
            thoughtSectionCollapsed: true,
            isStreaming: false,
            ...nextWizard,
          });
          void useDashboardStore.getState().fetchUserDashboardList({ forceRefresh: true });
          return;
        }
        const content = st.streamingContent;
        const assistantMessage: ChatMessageWithCharts = {
          role: 'assistant',
          content: content || null,
          charts: charts.length > 0 ? charts : undefined,
          toolCalls:
            currentTurnToolResults.length > 0
              ? currentTurnToolResults.map((m) => ({ tool_name: m.tool_name, content: m.content ?? '' }))
              : undefined,
        };
        set({
          messages: [...prev, assistantMessage],
          streamingContent: '',
          streamingCharts: [],
          streamingToolCalls: [],
          currentTurnToolResults: [],
          thoughtSectionCollapsed: true,
          isStreaming: false,
          ...nextWizard,
        });
        void useDashboardStore.getState().fetchUserDashboardList({ forceRefresh: true });
        return;
      }

      if (event === 'error' && 'detail' in data) {
        const ws = get().wizardStep;
        const detail = String((data as import('../types').SSEErrorData).detail);
        set({
          error: detail,
          isStreaming: false,
          streamingContent: '',
          streamingCharts: [],
          streamingToolCalls: [],
          currentTurnToolResults: [],
          magicSegments: [],
          ...(ws === 'magic-running' ? { wizardStep: 'setup' as const } : {}),
        });
      }
    };

    const body: import('../types').ChatSendRequest = {
      message: trimmed,
      session_id: sessionId,
      agent_type: 'dashboard',
      llm: 'gpt-4.1',
      database: 'DuckDB',
      dashboard_mode: mode,
    };
    if (selectedDatasets !== null && selectedDatasets.length > 0) {
      body.selected_datasets = selectedDatasets;
    }
    if (mode === 'magic' && magicSetup) {
      if (magicSetup.dashboardName) body.magic_dashboard_name = magicSetup.dashboardName;
      if (magicSetup.goal) body.magic_goal = magicSetup.goal;
    }

    try {
      await streamChat(body, onEvent, { signal: ac.signal });
    } catch (err) {
      if (get().sessionId !== startedSessionId) return;
      if (err instanceof Error && err.name === 'AbortError') {
        set({
          isStreaming: false,
          streamingContent: '',
          streamingCharts: [],
          streamingToolCalls: [],
          currentTurnToolResults: [],
          magicSegments: [],
        });
        return;
      }
      const detail = err instanceof Error ? err.message : String(err);
      const ws = get().wizardStep;
      set({
        error: detail,
        isStreaming: false,
        streamingContent: '',
        streamingCharts: [],
        magicSegments: [],
        ...(ws === 'magic-running' ? { wizardStep: 'setup' as const } : {}),
      });
    } finally {
      if (get().sessionId === startedSessionId && dashboardStreamAbort === ac) {
        dashboardStreamAbort = null;
      }
    }
  },

  loadHistory: async () => {
    const { sessionId } = get();
    set({ error: null });
    try {
      const history = await getChatHistory(sessionId, {
        agent_type: 'dashboard',
        llm: 'gpt-4.1',
        database: 'DuckDB',
      });
      const normalized = normalizeHistoryWithCharts(
        history as (ChatMessage & {
          chart?: {
            chartConfig: EChartsConfig;
            chart_type?: string;
            title?: string;
            dataConfig?: Record<string, unknown>;
          };
        })[],
      );
      set({ messages: normalized });
    } catch {
      /* session may not exist yet */
    }
  },

  clearHistory: async () => {
    const { sessionId } = get();
    try {
      await clearChatHistory(sessionId);
      set({
        messages: [],
        error: null,
        sessionId: newSessionId(),
        wizardStep: 'mode',
        pendingMode: null,
        magicSetup: null,
        magicSegments: [],
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
    }
  },
}));
