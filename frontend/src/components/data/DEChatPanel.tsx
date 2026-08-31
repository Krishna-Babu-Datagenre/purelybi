import { useState, useRef, useCallback, useEffect } from 'react';
import { Send, Loader2, MessageSquare, Trash2, ChevronDown, ChevronUp, Wrench } from 'lucide-react';
import { clearChatHistory, getChatHistory, streamChat } from '../../services/chatApi';
import type { SSEEventType, SSEHandler } from '../../services/chatApi';
import type { ChatMessage, SSEData } from '../../types';
import { useAuthStore } from '../../store/useAuthStore';
import MarkdownMessage from './MarkdownMessage';

interface ToolCallResult {
  tool_name?: string;
  content: string;
}

interface StreamingToolCall {
  id: string;
  name: string;
  args: string;
}

interface DEMessage {
  role: 'user' | 'assistant' | 'tool';
  content: string | null;
  tool_name?: string;
  tool_call_id?: string;
  toolCalls?: ToolCallResult[];
}

interface DEChatPanelProps {
  pipelineId: string;
  connectorConfigId?: string | null;
  connectorName?: string | null;
  /** Called when the agent modifies the pipeline — parent should refresh steps */
  onPipelineChanged: () => void;
}

function getUserSessionId(pipelineId: string): string {
  const user = useAuthStore.getState().user;
  const uid = user ? `user-${user.id}` : 'anon';
  return `de-${uid}-${pipelineId}`;
}

const STARTER_PROMPTS = [
  'What tables do I have available?',
  'Rename the "id" column to "customer_id"',
  'Filter rows where status equals "active"',
  'Show me what recipes are available',
];

function normalizeHistory(history: ChatMessage[]): DEMessage[] {
  const normalized: DEMessage[] = [];

  for (let i = 0; i < history.length; i++) {
    const msg = history[i];
    if (msg.role === 'tool') continue;

    if (msg.role === 'assistant') {
      const toolCalls: ToolCallResult[] = [];
      for (let j = i - 1; j >= 0 && history[j].role === 'tool'; j--) {
        toolCalls.unshift({
          tool_name: history[j].tool_name,
          content: history[j].content ?? '',
        });
      }
      normalized.push({
        role: 'assistant',
        content: msg.content ?? '',
        toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
      });
      continue;
    }

    normalized.push({ role: 'user', content: msg.content ?? '' });
  }

  return normalized;
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

export default function DEChatPanel({
  pipelineId,
  connectorConfigId,
  connectorName,
  onPipelineChanged,
}: DEChatPanelProps) {
  const [messages, setMessages] = useState<DEMessage[]>([]);
  const [input, setInput] = useState('');
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamingContent, setStreamingContent] = useState('');
  const [streamingToolCalls, setStreamingToolCalls] = useState<StreamingToolCall[]>([]);
  const [currentTurnToolResults, setCurrentTurnToolResults] = useState<DEMessage[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [expandedAgentActivity, setExpandedAgentActivity] = useState<Record<number, boolean>>({});

  const abortRef = useRef<AbortController | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const sessionId = useRef(getUserSessionId(pipelineId));
  const streamingContentRef = useRef('');
  const currentTurnResultsRef = useRef<DEMessage[]>([]);
  const pipelineMutatedRef = useRef(false);

  useEffect(() => {
    sessionId.current = getUserSessionId(pipelineId);
    setMessages([]);
    setStreamingContent('');
    setStreamingToolCalls([]);
    setCurrentTurnToolResults([]);
    setError(null);
    setExpandedAgentActivity({});

    async function loadHistory() {
      try {
        const history = await getChatHistory(sessionId.current, {
          agent_type: 'de',
          llm: 'gpt-4.1',
          database: 'DuckDB',
        });
        setMessages(normalizeHistory(history));
      } catch {
        // Ignore history load failure for the DE side panel.
      }
    }

    void loadHistory();
  }, [pipelineId]);

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

  const send = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed || isStreaming) return;

      setInput('');
      setError(null);
      setMessages((prev) => [...prev, { role: 'user', content: trimmed }]);
      setIsStreaming(true);
      setStreamingContent('');
      setStreamingToolCalls([]);
      setCurrentTurnToolResults([]);
      streamingContentRef.current = '';
      currentTurnResultsRef.current = [];
      pipelineMutatedRef.current = false;

      abortRef.current = new AbortController();

      const onEvent: SSEHandler = (event: SSEEventType, data: SSEData) => {
        if (event === 'start') return;

        if (event === 'token' && 'content' in data) {
          const piece = (data as { content: string }).content;
          streamingContentRef.current += piece;
          setStreamingContent(streamingContentRef.current);
          return;
        }

        if (event === 'tool_call_start' && 'tool_call_id' in data && 'tool_name' in data) {
          const d = data as { tool_call_id: string; tool_name: string };
          setStreamingToolCalls((prev) => [...prev, { id: d.tool_call_id, name: d.tool_name, args: '' }]);
          return;
        }

        if (event === 'tool_call_args' && 'tool_call_id' in data && 'args_chunk' in data) {
          const d = data as { tool_call_id: string; args_chunk: string };
          setStreamingToolCalls((prev) =>
            prev.map((tc) => (tc.id === d.tool_call_id ? { ...tc, args: tc.args + d.args_chunk } : tc)),
          );
          return;
        }

        if (event === 'tool_result' && 'tool_call_id' in data && 'result' in data) {
          const d = data as { tool_call_id: string; tool_name?: string; result: string };
          const toolMsg: DEMessage = {
            role: 'tool',
            content: d.result,
            tool_name: d.tool_name,
            tool_call_id: d.tool_call_id,
          };
          currentTurnResultsRef.current = [...currentTurnResultsRef.current, toolMsg];
          setCurrentTurnToolResults(currentTurnResultsRef.current);
          setStreamingToolCalls((prev) => prev.filter((tc) => tc.id !== d.tool_call_id));

          if (d.tool_name && ['add_step', 'remove_step'].includes(d.tool_name)) {
            pipelineMutatedRef.current = true;
          }
          return;
        }

        if (event === 'end') {
          const toolCalls = currentTurnResultsRef.current
            .filter((m) => m.role === 'tool')
            .map((m) => ({ tool_name: m.tool_name, content: m.content ?? '' }));

          setMessages((prev) => [
            ...prev,
            {
              role: 'assistant',
              content: streamingContentRef.current || '_(no response)_',
              toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
            },
          ]);

          setStreamingContent('');
          setStreamingToolCalls([]);
          setCurrentTurnToolResults([]);
          setIsStreaming(false);
          if (pipelineMutatedRef.current) {
            onPipelineChanged();
          }
          return;
        }

        if (event === 'error' && 'detail' in data) {
          const detail = String((data as { detail: string }).detail);
          setError(detail);
          setStreamingContent('');
          setStreamingToolCalls([]);
          setCurrentTurnToolResults([]);
          setIsStreaming(false);
        }
      };

      try {
        await streamChat(
          {
            message: trimmed,
            session_id: sessionId.current,
            agent_type: 'de',
            llm: 'gpt-4.1',
            database: 'DuckDB',
            pipeline_id: pipelineId,
            ...(connectorConfigId ? { connector_config_id: connectorConfigId } : {}),
            ...(connectorName ? { connector_name: connectorName } : {}),
          },
          onEvent,
          { signal: abortRef.current.signal },
        );
      } catch (e) {
        if ((e as Error).name !== 'AbortError') {
          setError('Connection error. Please try again.');
        }
        setStreamingContent('');
        setStreamingToolCalls([]);
        setCurrentTurnToolResults([]);
        setIsStreaming(false);
      }
    },
    [isStreaming, pipelineId, connectorConfigId, connectorName, onPipelineChanged],
  );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    send(input);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send(input);
    }
  };

  const handleClear = async () => {
    abortRef.current?.abort();
    try {
      await clearChatHistory(sessionId.current);
    } catch {
      // If server state is already gone, local reset is still valid.
    }
    setMessages([]);
    setStreamingContent('');
    setStreamingToolCalls([]);
    setCurrentTurnToolResults([]);
    setError(null);
    setIsStreaming(false);
  };

  const isEmpty = messages.length === 0 && !streamingContent && !isStreaming;
  const toggleAgentActivity = (index: number) => {
    setExpandedAgentActivity((prev) => ({ ...prev, [index]: !prev[index] }));
  };

  const renderMessage = (msg: DEMessage, i: number) => {
    if (msg.role === 'tool') return null;

    if (msg.role === 'user') {
      return (
        <div key={i} className="chat-msg chat-msg--user">
          <div className="chat-msg-content">{msg.content ?? ''}</div>
        </div>
      );
    }

    const toolCalls = msg.toolCalls ?? [];
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
        <div className="chat-msg chat-msg--assistant">
          <div className="chat-msg-content chat-msg-content--md min-w-0">
            <MarkdownMessage content={msg.content ?? ''} className="text-[0.8125rem] leading-[1.45]" />
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="chat-drawer-inner rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] overflow-hidden">
      <header className="chat-drawer-header">
        <div className="flex items-center gap-2">
          <MessageSquare size={18} className="text-[var(--brand)]" />
          <span className="chat-drawer-title">Chat</span>
        </div>
        <button
          type="button"
          className="chat-drawer-icon-btn"
          onClick={() => void handleClear()}
          title="Clear conversation"
          disabled={isStreaming}
        >
          <Trash2 size={16} />
        </button>
      </header>

      <div className="chat-message-list" ref={scrollRef}>
        {isEmpty ? (
          <div className="chat-empty">
            <div>
              <p className="text-[var(--text-secondary)] text-sm">
                Describe your transformation
              </p>
              <p className="text-[var(--text-muted)] text-xs mt-1">
                The AI will build pipeline steps for you automatically.
              </p>
            </div>
            <div className="flex flex-col gap-1.5 w-full mt-3">
              {STARTER_PROMPTS.map((p) => (
                <button
                  key={p}
                  type="button"
                  onClick={() => send(p)}
                  className="text-xs text-left px-3 py-2 rounded-lg border border-[var(--border-default)] hover:border-[rgba(139,92,246,0.35)] hover:bg-[rgba(139,92,246,0.08)] text-[var(--text-secondary)] transition-colors"
                >
                  {p}
                </button>
              ))}
            </div>
          </div>
        ) : (
          messages.map((msg, i) => renderMessage(msg, i))
        )}

        {isStreaming && (
          <div className="chat-streaming-block" data-streaming>
            <div className="chat-thought-section">
              <button type="button" className="chat-thought-header chat-thought-header--no-toggle" aria-expanded>
                <Wrench size={16} className="text-[var(--brand)]" />
                <span className="chat-thought-title">Agent activity</span>
                {streamingToolCalls.length > 0 && <Loader2 size={14} className="animate-spin text-[var(--brand)]" />}
              </button>
              <div className="chat-thought-body">
                {streamingToolCalls.length === 0 && currentTurnToolResults.length === 0 && (
                  <div className="chat-thought-placeholder">
                    <Loader2 size={18} className="animate-spin text-[var(--brand)]" />
                    <span>Agent is thinking…</span>
                  </div>
                )}
                {streamingToolCalls.map((tc) => (
                  <div key={tc.id} className="chat-thought-item chat-thought-item--in-progress">
                    <Loader2 size={14} className="animate-spin flex-shrink-0" />
                    <span>Calling <code>{tc.name}</code>…</span>
                  </div>
                ))}
                {currentTurnToolResults.map((msg, i) => (
                  <div key={i} className="chat-thought-item chat-thought-item--result">
                    <span className="chat-thought-tool-name">{msg.tool_name ?? 'Tool'}</span>
                    <pre className="chat-thought-tool-result">{msg.content ?? ''}</pre>
                  </div>
                ))}
              </div>
            </div>

            <div className="chat-msg chat-msg--assistant chat-msg--streaming">
              <div className="chat-msg-content chat-msg-content--md min-w-0">
                <MarkdownMessage
                  content={streamingContent || (streamingToolCalls.length > 0 ? '' : '…')}
                  className="text-[0.8125rem] leading-[1.45]"
                />
              </div>
            </div>
          </div>
        )}

        {error && (
          <div className="chat-msg chat-msg--error">
            <span>{error}</span>
          </div>
        )}
      </div>

      <div className="chat-input-region">
        <form className="chat-input-wrap" onSubmit={handleSubmit}>
          <textarea
            className="chat-input"
            placeholder="Describe a transformation…"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={1}
            disabled={isStreaming}
          />
          <button
            type="submit"
            className="chat-send-btn"
            disabled={!input.trim() || isStreaming}
            aria-label="Send"
          >
            {isStreaming ? <Loader2 size={16} className="animate-spin" /> : <Send size={18} />}
          </button>
        </form>
      </div>
    </div>
  );
}
