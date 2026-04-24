import { create } from 'zustand';
import type { ChatMessage, ChatChartItem, SSEData, EChartsConfig } from '../types';
import type { MagicTimelineSegment } from '../utils/magicToolTimeline';
import type { SSEEventType } from '../services/chatApi';
import { streamChat, getChatHistory, clearChatHistory } from '../services/chatApi';
import { useAuthStore } from './useAuthStore';

/** One tool call result shown in the agent activity block for an assistant message */
export interface ToolCallResult {
  tool_name?: string;
  content: string;
}

/** Assistant messages can have charts and/or tool call results (agent activity) */
export interface ChatMessageWithCharts extends ChatMessage {
  /** ECharts configs to render in the message (from SSE `chart` events or history) */
  charts?: ChatChartItem[];
  /** Tool calls made to produce this reply; shown in a collapsible "Agent activity" block */
  toolCalls?: ToolCallResult[];
  /** Magic mode: text + tools in true chronological order */
  magicTimeline?: MagicTimelineSegment[];
}

/** In-progress tool call shown in the thought process section */
export interface StreamingToolCall {
  id: string;
  name: string;
  args: string;
}

/** Attach charts from preceding tool messages onto each assistant message (for GET history). */
function normalizeHistoryWithCharts(
  history: (ChatMessage & { chart?: { chartConfig: EChartsConfig; chart_type?: string; title?: string; dataConfig?: Record<string, unknown> } })[]
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

function getUserSessionId(): string {
  const user = useAuthStore.getState().user;
  return user ? `user-${user.id}` : 'default';
}

const MIN_WIDTH_PX = 280;
const MAX_WIDTH_PERCENT = 50;
/** Default drawer width — aligned with global --ui-scale (~0.8 of 400px) */
const DEFAULT_WIDTH_PX = 320;

interface ChatState {
  isOpen: boolean;
  isModal: boolean;
  widthPx: number;
  sessionId: string;
  messages: ChatMessageWithCharts[];
  streamingContent: string;
  streamingCharts: ChatChartItem[];
  isStreaming: boolean;
  error: string | null;

  streamingToolCalls: StreamingToolCall[];
  currentTurnToolResults: ChatMessageWithCharts[];
  lastTurnToolResults: ChatMessageWithCharts[];
  thoughtSectionCollapsed: boolean;

  /** Name of the dashboard the user has attached to this chat (if any). */
  attachedDashboardName: string | null;
  attachDashboard: (name: string) => void;
  clearAttachedDashboard: () => void;

  openChat: () => void;
  closeChat: () => void;
  toggleChat: () => void;
  setModal: (value: boolean) => void;
  setWidthPx: (px: number) => void;
  setSessionId: (id: string) => void;
  setThoughtSectionCollapsed: (collapsed: boolean) => void;

  sendMessage: (message: string) => Promise<void>;
  loadHistory: () => Promise<void>;
  clearHistory: () => Promise<void>;
  clearError: () => void;

  /** Resize: clamp width to min and max (50% of window) */
  resizeWidth: (clientX: number) => void;
}

export const useChatStore = create<ChatState>((set, get) => ({
  isOpen: false,
  isModal: false,
  widthPx: DEFAULT_WIDTH_PX,
  sessionId: getUserSessionId(),
  messages: [],
  streamingContent: '',
  streamingCharts: [],
  isStreaming: false,
  error: null,
  streamingToolCalls: [],
  currentTurnToolResults: [],
  lastTurnToolResults: [],
  thoughtSectionCollapsed: true,
  attachedDashboardName: null,

  attachDashboard: (name) => set({ attachedDashboardName: name.trim() || null }),
  clearAttachedDashboard: () => set({ attachedDashboardName: null }),

  openChat: () => {
    const currentUserId = getUserSessionId();
    const state = get();
    if (state.sessionId !== currentUserId) {
      set({ isOpen: true, error: null, sessionId: currentUserId, messages: [] });
    } else {
      set({ isOpen: true, error: null });
    }
  },
  closeChat: () => set({ isOpen: false }),
  toggleChat: () => {
    const s = get();
    if (s.isOpen) {
      set({ isOpen: false });
    } else {
      get().openChat();
    }
  },
  setModal: (value) => set({ isModal: value }),
  setSessionId: (id) => set({ sessionId: id }),
  setThoughtSectionCollapsed: (collapsed) => set({ thoughtSectionCollapsed: collapsed }),
  clearError: () => set({ error: null }),

  setWidthPx: (px) => {
    const maxPx = (typeof window !== 'undefined' ? window.innerWidth : 1200) * (MAX_WIDTH_PERCENT / 100);
    const clamped = Math.round(Math.min(maxPx, Math.max(MIN_WIDTH_PX, px)));
    set({ widthPx: clamped });
  },

  resizeWidth: (clientX) => {
    if (typeof window === 'undefined') return;
    const rightEdge = window.innerWidth;
    const newWidth = rightEdge - clientX;
    get().setWidthPx(newWidth);
  },

  sendMessage: async (message: string) => {
    const { sessionId, messages } = get();
    const trimmed = message.trim();
    if (!trimmed || get().isStreaming) return;

    const userMessage: ChatMessageWithCharts = { role: 'user', content: trimmed };
    const toolMessages: ChatMessageWithCharts[] = [];

    set({
      messages: [...messages, userMessage],
      isStreaming: true,
      streamingContent: '',
      streamingCharts: [],
      streamingToolCalls: [],
      currentTurnToolResults: [],
      lastTurnToolResults: [],
      thoughtSectionCollapsed: false,
      error: null,
    });

    const onEvent = (event: SSEEventType, data: SSEData) => {
      const state = get();

      // Backend sends "start" immediately so we get headers and can show "Agent is thinking"
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
        set({ streamingCharts: [...state.streamingCharts, item] });
        return;
      }

      if (event === 'tool_call_start' && 'tool_call_id' in data && 'tool_name' in data) {
        const d = data as import('../types').SSEToolCallStartData;
        const next = [
          ...state.streamingToolCalls,
          { id: d.tool_call_id, name: d.tool_name, args: '' },
        ];
        set({ streamingToolCalls: next });
        return;
      }

      if (event === 'tool_call_args' && 'tool_call_id' in data && 'args_chunk' in data) {
        const next = state.streamingToolCalls.map((tc) =>
          tc.id === data.tool_call_id ? { ...tc, args: tc.args + data.args_chunk } : tc
        );
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
        toolMessages.push(toolMsg);
        const nextResults = [...state.currentTurnToolResults, toolMsg];
        const nextCalls = state.streamingToolCalls.filter((tc) => tc.id !== toolResult.tool_call_id);
        set({ currentTurnToolResults: nextResults, streamingToolCalls: nextCalls });
        return;
      }

      if (event === 'end') {
        const { messages: prev, streamingContent: content, streamingCharts: charts, currentTurnToolResults } = get();
        const assistantMessage: ChatMessageWithCharts = {
          role: 'assistant',
          content: content || null,
          charts: charts.length > 0 ? charts : undefined,
          toolCalls: currentTurnToolResults.length > 0 ? currentTurnToolResults.map((m) => ({ tool_name: m.tool_name, content: m.content ?? '' })) : undefined,
        };
        set({
          messages: [...prev, assistantMessage],
          streamingContent: '',
          streamingCharts: [],
          streamingToolCalls: [],
          currentTurnToolResults: [],
          lastTurnToolResults: [],
          thoughtSectionCollapsed: true,
          isStreaming: false,
        });
        return;
      }

      if (event === 'error' && 'detail' in data) {
        const detail = String((data as import('../types').SSEErrorData).detail);
        set({
          error: detail,
          isStreaming: false,
          streamingContent: '',
          streamingCharts: [],
          streamingToolCalls: [],
          currentTurnToolResults: [],
        });
      }
    };

    try {
      const attachedDashboardName = get().attachedDashboardName;
      await streamChat(
        {
          message: trimmed,
          session_id: sessionId,
          agent_type: 'analyst',
          llm: 'gpt-4.1',
          database: 'DuckDB',
          ...(attachedDashboardName ? { attached_dashboard_name: attachedDashboardName } : {}),
        },
        onEvent
      );
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      set({
        error: detail,
        isStreaming: false,
        streamingContent: '',
        streamingCharts: [],
      });
    }
  },

  loadHistory: async () => {
    const { sessionId } = get();
    set({ error: null });
    try {
      const history = await getChatHistory(sessionId);
      const normalized = normalizeHistoryWithCharts(
        history as (ChatMessage & { chart?: { chartConfig: EChartsConfig; chart_type?: string; title?: string; dataConfig?: Record<string, unknown> } })[],
      );
      set({ messages: normalized });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
    }
  },

  clearHistory: async () => {
    const { sessionId } = get();
    try {
      await clearChatHistory(sessionId);
      set({ messages: [], error: null });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      set({ error: message });
    }
  },
}));
