import { useRef, useEffect, useState } from 'react';
import { ChevronDown, ChevronUp, Loader2, Wrench } from 'lucide-react';
import { useDashboardBuilderStore } from '../store/useDashboardBuilderStore';
import type { ChatMessageWithCharts, StreamingToolCall, ToolCallResult } from '../store/useChatStore';
import type { ChatChartItem } from '../types';
import type { MagicTimelineSegment } from '../utils/magicToolTimeline';
import { toolCallsToRichSteps } from '../utils/magicToolTimeline';
import ChatChartBlock from './ChatChartBlock';
import MagicToolTimelineRows from './MagicToolTimelineRows';
import MarkdownMessage from './data/MarkdownMessage';

function MessageBubble({
  message,
  hideCharts,
  hideAddToDashboard,
}: {
  message: ChatMessageWithCharts;
  hideCharts?: boolean;
  hideAddToDashboard?: boolean;
}) {
  const isUser = message.role === 'user';

  return (
    <div className={`chat-msg ${isUser ? 'chat-msg--user' : 'chat-msg--assistant'}`}>
      {message.proxyAuto && (
        <p className="text-[10px] font-semibold uppercase tracking-wide text-[var(--text-muted)] mb-1">
          Auto-reply
        </p>
      )}
      <div className="chat-msg-content chat-msg-content--md min-w-0">
        <MarkdownMessage content={message.content ?? ''} className="text-[0.8125rem] leading-[1.45]" />
      </div>
      {!hideCharts && message.charts && message.charts.length > 0 && (
        <div className="chat-msg-charts">
          {message.charts.map((item, i) => (
            <ChatChartBlock key={i} item={item} hideAddToDashboard={hideAddToDashboard} />
          ))}
        </div>
      )}
    </div>
  );
}

function AgentActivityBlock({
  toolCalls,
  messageIndex,
  expandedByIndex,
  onToggle,
}: {
  toolCalls: ToolCallResult[];
  messageIndex: number;
  expandedByIndex: Record<number, boolean>;
  onToggle: (index: number) => void;
}) {
  const expanded = expandedByIndex[messageIndex] ?? false;

  return (
    <div className="chat-thought-section">
      <button
        type="button"
        className="chat-thought-header"
        onClick={() => onToggle(messageIndex)}
        aria-expanded={expanded}
      >
        <Wrench size={16} className="text-[var(--brand)]" />
        <span className="chat-thought-title">Agent activity</span>
        <span className="chat-thought-badge">
          {toolCalls.length} tool{toolCalls.length !== 1 ? 's' : ''} used
        </span>
        {expanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
      </button>
      {expanded && (
        <div className="chat-thought-body">
          {toolCalls.map((msg, i) => (
            <div key={i} className="chat-thought-item chat-thought-item--result">
              <span className="chat-thought-tool-name">{msg.tool_name ?? 'Tool'}</span>
              <pre className="chat-thought-tool-result">{msg.content}</pre>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function StreamingThoughtSection() {
  const isStreaming = useDashboardBuilderStore((s) => s.isStreaming);
  const streamingToolCalls = useDashboardBuilderStore((s) => s.streamingToolCalls);
  const currentTurnToolResults = useDashboardBuilderStore((s) => s.currentTurnToolResults);

  const hasActivity = streamingToolCalls.length > 0 || currentTurnToolResults.length > 0;

  if (!isStreaming) return null;

  return (
    <div className="chat-thought-section">
      <button type="button" className="chat-thought-header chat-thought-header--no-toggle" aria-expanded>
        <Wrench size={16} className="text-[var(--brand)]" />
        <span className="chat-thought-title">Agent activity</span>
        {streamingToolCalls.length > 0 && (
          <Loader2 size={14} className="animate-spin text-[var(--brand)]" />
        )}
      </button>
      <div className="chat-thought-body">
        {!hasActivity && (
          <div className="chat-thought-placeholder">
            <Loader2 size={18} className="animate-spin text-[var(--brand)]" />
            <span>Agent is thinking…</span>
          </div>
        )}
        {streamingToolCalls.map((tc: StreamingToolCall) => (
          <div key={tc.id} className="chat-thought-item chat-thought-item--in-progress">
            <Loader2 size={14} className="animate-spin flex-shrink-0" />
            <span>
              Calling <code>{tc.name}</code>…
            </span>
          </div>
        ))}
        {currentTurnToolResults.map((msg, i) => (
          <div key={i} className="chat-thought-item chat-thought-item--result">
            <span className="chat-thought-tool-name">{msg.tool_name ?? 'Tool'}</span>
            <pre className="chat-thought-tool-result">{msg.content}</pre>
          </div>
        ))}
      </div>
    </div>
  );
}

interface DashboardBuilderMessageListProps {
  /** Shown when there are no messages and no streaming content. */
  emptyHint?: string;
  /** Single vertical timeline: assistant text, tools, and (guided) charts in order. */
  variant?: 'default' | 'magicUnified';
}

const DEFAULT_EMPTY_HINT =
  'Choose a mode above, then describe what you want—or use Surprise me for an instant layout.';

function renderTimelineSegment(seg: MagicTimelineSegment, si: number | string, hideAddToDashboard?: boolean) {
  if (seg.type === 'text') {
    return (
      <div key={si} className="chat-msg chat-msg--assistant">
        <div className="chat-msg-content chat-msg-content--md min-w-0">
          <MarkdownMessage content={seg.content} className="text-[0.8125rem] leading-[1.45]" />
        </div>
      </div>
    );
  }
  if (seg.type === 'chart') {
    return (
      <div key={si} className="chat-msg-charts w-full min-w-0 max-w-full">
        <ChatChartBlock item={seg.item} hideAddToDashboard={hideAddToDashboard} />
      </div>
    );
  }
  return <MagicToolTimelineRows key={si} steps={seg.steps} />;
}

export default function DashboardBuilderMessageList({
  emptyHint = DEFAULT_EMPTY_HINT,
  variant = 'default',
}: DashboardBuilderMessageListProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const messages = useDashboardBuilderStore((s) => s.messages);
  const mode = useDashboardBuilderStore((s) => s.mode);
  const streamingContent = useDashboardBuilderStore((s) => s.streamingContent);
  const streamingCharts = useDashboardBuilderStore((s) => s.streamingCharts);
  const isStreaming = useDashboardBuilderStore((s) => s.isStreaming);
  const error = useDashboardBuilderStore((s) => s.error);
  const streamingToolCalls = useDashboardBuilderStore((s) => s.streamingToolCalls);
  const currentTurnToolResults = useDashboardBuilderStore((s) => s.currentTurnToolResults);
  const magicSegments = useDashboardBuilderStore((s) => s.magicSegments);
  const isMagic = mode === 'magic';
  const isUnified = variant === 'magicUnified';
  const hideAddToDashboard = mode === 'guided';

  const [expandedAgentActivity, setExpandedAgentActivity] = useState<Record<number, boolean>>({});

  const toggleAgentActivity = (index: number) => {
    setExpandedAgentActivity((prev) => ({ ...prev, [index]: !prev[index] }));
  };

  useEffect(() => {
    if (variant === 'magicUnified') return;
    const listEl = scrollRef.current;
    if (!listEl) return;
    const scrollToBottom = () => {
      listEl.scrollTop = listEl.scrollHeight;
    };
    scrollToBottom();
    if (isStreaming) {
      requestAnimationFrame(scrollToBottom);
    }
  }, [messages, streamingContent, magicSegments, streamingToolCalls, currentTurnToolResults, isStreaming, variant]);

  return (
    <div
      className={
        variant === 'magicUnified' ? 'chat-message-list chat-message-list--magic-unified' : 'chat-message-list'
      }
      ref={scrollRef}
    >
      {messages.length === 0 &&
        !streamingContent &&
        !error &&
        !(isUnified && isStreaming) && (
          <div className="chat-empty">
            <p className="text-[var(--text-secondary)] text-sm">{emptyHint}</p>
          </div>
        )}

      {messages.map((msg, i) => {
        if (msg.role === 'user') {
          return <MessageBubble key={i} message={msg} hideCharts={isMagic} hideAddToDashboard={hideAddToDashboard} />;
        }
        if (msg.role === 'tool') {
          return null;
        }
        if (msg.role === 'assistant') {
          const assistantMsg = msg as ChatMessageWithCharts;
          let toolCalls: ToolCallResult[] = assistantMsg.toolCalls ?? [];
          if (toolCalls.length === 0) {
            const prevTools: ToolCallResult[] = [];
            for (let j = i - 1; j >= 0 && messages[j].role === 'tool'; j--) {
              const t = messages[j] as ChatMessageWithCharts;
              prevTools.unshift({ tool_name: t.tool_name, content: t.content ?? '' });
            }
            toolCalls = prevTools;
          }
          if (assistantMsg.magicTimeline && assistantMsg.magicTimeline.length > 0) {
            return (
              <div key={i} className="chat-message-group flex flex-col gap-2">
                {assistantMsg.magicTimeline.map((seg: MagicTimelineSegment, si: number) =>
                  renderTimelineSegment(seg, `${i}-${si}`, hideAddToDashboard),
                )}
              </div>
            );
          }
          return (
            <div key={i} className="chat-message-group">
              <MessageBubble message={assistantMsg} hideCharts={isMagic} hideAddToDashboard={hideAddToDashboard} />
              {isMagic && toolCalls.length > 0 && (
                <MagicToolTimelineRows steps={toolCallsToRichSteps(toolCalls, `hist-${i}`)} />
              )}
              {!isMagic && toolCalls.length > 0 && (
                <AgentActivityBlock
                  toolCalls={toolCalls}
                  messageIndex={i}
                  expandedByIndex={expandedAgentActivity}
                  onToggle={toggleAgentActivity}
                />
              )}
            </div>
          );
        }
        return null;
      })}

      {isStreaming && (
        <div className="chat-streaming-block flex flex-col gap-2" data-streaming>
          {isUnified ? (
            <>
              {magicSegments.map((seg, idx) => renderTimelineSegment(seg, `live-${idx}`, hideAddToDashboard))}
              {streamingContent?.trim() ? (
                <div className="chat-msg chat-msg--assistant chat-msg--streaming">
                  <div className="chat-msg-content chat-msg-content--md min-w-0">
                    <MarkdownMessage content={streamingContent} className="text-[0.8125rem] leading-[1.45]" />
                  </div>
                </div>
              ) : null}
              {magicSegments.length === 0 &&
                !streamingContent?.trim() &&
                streamingToolCalls.length === 0 &&
                currentTurnToolResults.length === 0 && (
                  <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/[0.06] dark:bg-emerald-500/[0.08] px-4 py-3 flex items-center gap-2">
                    <Loader2 size={16} className="animate-spin text-emerald-600 dark:text-emerald-400 shrink-0" />
                    <span className="text-sm font-medium text-emerald-700 dark:text-emerald-300">
                      Planning next steps…
                    </span>
                  </div>
                )}
            </>
          ) : (
            <>
              <div className="chat-msg chat-msg--assistant chat-msg--streaming">
                <div className="chat-msg-content chat-msg-content--md min-w-0">
                  <MarkdownMessage
                    content={streamingContent || (streamingToolCalls.length > 0 ? '' : '…')}
                    className="text-[0.8125rem] leading-[1.45]"
                  />
                </div>
                {streamingCharts.length > 0 && (
                  <div className="chat-msg-charts">
                    {streamingCharts.map((item: ChatChartItem, j) => (
                      <ChatChartBlock key={j} item={item} hideAddToDashboard={hideAddToDashboard} />
                    ))}
                  </div>
                )}
              </div>
              <StreamingThoughtSection />
            </>
          )}
        </div>
      )}

      {error && (
        <div className="chat-msg chat-msg--error">
          <span>{error}</span>
        </div>
      )}
    </div>
  );
}
