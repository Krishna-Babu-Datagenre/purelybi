import { useRef, useEffect, useState } from 'react';
import { ChevronDown, ChevronUp, Loader2, Wrench } from 'lucide-react';
import { useChatStore } from '../store/useChatStore';
import type { ChatMessageWithCharts, StreamingToolCall, ToolCallResult } from '../store/useChatStore';
import type { ChatChartItem } from '../types';
import ChatChartBlock from './ChatChartBlock';

function MessageBubble({ message }: { message: ChatMessageWithCharts }) {
  const isUser = message.role === 'user';

  return (
    <div className={`chat-msg ${isUser ? 'chat-msg--user' : 'chat-msg--assistant'}`}>
      <div className="chat-msg-content">{message.content ?? ''}</div>
      {message.charts && message.charts.length > 0 && (
        <div className="chat-msg-charts">
          {message.charts.map((item, i) => (
            <ChatChartBlock key={i} item={item} />
          ))}
        </div>
      )}
    </div>
  );
}

/** Collapsible "Agent activity" block for one assistant message that had tool calls */
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

/** Live "Agent activity" section shown only while the current turn is streaming */
function StreamingThoughtSection() {
  const isStreaming = useChatStore((s) => s.isStreaming);
  const streamingToolCalls = useChatStore((s) => s.streamingToolCalls);
  const currentTurnToolResults = useChatStore((s) => s.currentTurnToolResults);

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
            <span>Calling <code>{tc.name}</code>…</span>
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

export default function ChatMessageList() {
  const scrollRef = useRef<HTMLDivElement>(null);
  const messages = useChatStore((s) => s.messages);
  const streamingContent = useChatStore((s) => s.streamingContent);
  const streamingCharts = useChatStore((s) => s.streamingCharts);
  const isStreaming = useChatStore((s) => s.isStreaming);
  const error = useChatStore((s) => s.error);
  const streamingToolCalls = useChatStore((s) => s.streamingToolCalls);
  const currentTurnToolResults = useChatStore((s) => s.currentTurnToolResults);

  const [expandedAgentActivity, setExpandedAgentActivity] = useState<Record<number, boolean>>({});

  const toggleAgentActivity = (index: number) => {
    setExpandedAgentActivity((prev) => ({ ...prev, [index]: !prev[index] }));
  };

  // Scroll list to bottom so the live "Agent activity" section stays in view every turn (including second+ message)
  useEffect(() => {
    const listEl = scrollRef.current;
    if (!listEl) return;
    const scrollToBottom = () => {
      listEl.scrollTop = listEl.scrollHeight;
    };
    scrollToBottom();
    if (isStreaming) {
      requestAnimationFrame(scrollToBottom);
    }
  }, [messages, streamingContent, streamingToolCalls, currentTurnToolResults, isStreaming]);

  return (
    <div className="chat-message-list" ref={scrollRef}>
      {messages.length === 0 && !streamingContent && !error && (
        <div className="chat-empty">
          <p className="text-[var(--text-secondary)] text-sm">Ask a question about your data.</p>
          <p className="text-[var(--text-muted)] text-xs mt-1">e.g. &quot;What were total sales last month?&quot;</p>
        </div>
      )}

      {messages.map((msg, i) => {
        if (msg.role === 'user') {
          return <MessageBubble key={i} message={msg} />;
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
          return (
            <div key={i} className="chat-message-group">
              {toolCalls.length > 0 && (
                <AgentActivityBlock
                  toolCalls={toolCalls}
                  messageIndex={i}
                  expandedByIndex={expandedAgentActivity}
                  onToggle={toggleAgentActivity}
                />
              )}
              <MessageBubble message={assistantMsg} />
            </div>
          );
        }
        return null;
      })}

      {/* Live agent activity + streaming reply: always show for current turn (first or follow-up) */}
      {isStreaming && (
        <div className="chat-streaming-block" data-streaming>
          <StreamingThoughtSection />
          <div className="chat-msg chat-msg--assistant chat-msg--streaming">
          <div className="chat-msg-content">{streamingContent || (streamingToolCalls.length > 0 ? '' : '…')}</div>
          {streamingCharts.length > 0 && (
            <div className="chat-msg-charts">
              {streamingCharts.map((item: ChatChartItem, j) => (
                <ChatChartBlock key={j} item={item} />
              ))}
            </div>
          )}
        </div>
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
