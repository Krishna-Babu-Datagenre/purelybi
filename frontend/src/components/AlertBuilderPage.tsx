import { useState, useRef, useEffect, useCallback } from 'react';
import {
  BellPlus,
  Send,
  Loader2,
  CheckCircle2,
  AlertTriangle,
  Sparkles,
  Bell,
  Clock,
  Mail,
  Wrench,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';
import DataPageFrame from './data/DataPageFrame';
import MarkdownMessage from './data/MarkdownMessage';
import { streamAlertBuilder, createAlert } from '../services/backendClient';
import { useAlertStore } from '../store/useAlertStore';
import { useDashboardStore } from '../store/useDashboardStore';

interface AlertBuilderPageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

/* ─────────────────────────────────────────────
   Types
───────────────────────────────────────────── */

interface ToolCallResult {
  tool_name?: string;
  content: string;
}

interface StreamingToolCall {
  id: string;
  name: string;
}

interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  toolCalls?: ToolCallResult[];
}

interface AlertPreview {
  name?: string;
  metric_description?: string;
  table?: string;
  metric_sql?: string;
  comparator?: string;
  threshold?: number;
  time_window?: string;
  notification_target?: string;
}

const COMP_LABELS: Record<string, string> = {
  gt: '>', gte: '≥', lt: '<', lte: '≤', eq: '=', neq: '≠',
  pct_change_gt: '% change >', pct_change_lt: '% change <',
};

/* ─────────────────────────────────────────────
   Preview Card
───────────────────────────────────────────── */

const PreviewCard = ({ preview, onChange }: { preview: AlertPreview | null, onChange: (key: keyof AlertPreview, value: any) => void }) => {
  if (!preview) {
    return (
      <div className="alert-builder-preview-empty">
        <BellPlus size={28} className="text-[var(--text-muted)]" />
        <p className="text-sm text-[var(--text-secondary)] text-center leading-relaxed">
          Describe the alert you want to create and a live preview will appear here.
        </p>
      </div>
    );
  }

  const comp = COMP_LABELS[preview.comparator ?? ''] ?? preview.comparator;

  return (
    <div className="alert-builder-preview-card">
      <div className="alert-builder-preview-header">
        <Bell size={16} className="text-[var(--brand)]" />
        <input
          type="text"
          value={preview.name || ''}
          onChange={(e) => onChange('name', e.target.value)}
          placeholder="Alert Name"
          className="text-sm font-semibold text-[var(--text-primary)] bg-transparent border-b border-transparent hover:border-[var(--border-strong)] focus:border-[var(--brand)] focus:outline-none px-1 py-0.5 flex-1"
        />
      </div>

      <div className="alert-builder-preview-field">
        <span className="alert-builder-preview-label">Metric</span>
        <textarea
          value={preview.metric_description || ''}
          onChange={(e) => onChange('metric_description', e.target.value)}
          placeholder="Metric description..."
          className="text-sm text-[var(--text-primary)] bg-transparent border border-transparent hover:border-[var(--border-strong)] focus:border-[var(--brand)] focus:outline-none rounded px-1.5 py-1 w-full resize-y min-h-[40px]"
          rows={2}
        />
      </div>

      <div className="alert-builder-preview-field">
        <span className="alert-builder-preview-label">Condition</span>
        <div className="flex items-center gap-2 flex-wrap">
          <select
            value={preview.comparator || ''}
            onChange={(e) => onChange('comparator', e.target.value)}
            className="text-sm font-mono text-[var(--text-primary)] bg-[var(--surface-sunken)] border border-[var(--border-color)] rounded px-1.5 py-1 focus:outline-none focus:border-[var(--brand)]"
          >
            <option value="gt">{COMP_LABELS.gt}</option>
            <option value="gte">{COMP_LABELS.gte}</option>
            <option value="lt">{COMP_LABELS.lt}</option>
            <option value="lte">{COMP_LABELS.lte}</option>
            <option value="eq">{COMP_LABELS.eq}</option>
            <option value="neq">{COMP_LABELS.neq}</option>
            <option value="pct_change_gt">{COMP_LABELS.pct_change_gt}</option>
            <option value="pct_change_lt">{COMP_LABELS.pct_change_lt}</option>
          </select>
          <input
            type="number"
            value={preview.threshold ?? ''}
            onChange={(e) => onChange('threshold', e.target.value === '' ? undefined : Number(e.target.value))}
            placeholder="Threshold"
            className="text-sm font-mono text-[var(--text-primary)] bg-transparent border-b border-[var(--border-color)] hover:border-[var(--border-strong)] focus:border-[var(--brand)] focus:outline-none px-1 py-0.5 w-24"
          />
        </div>
      </div>

      <div className="alert-builder-preview-field">
        <span className="alert-builder-preview-label">Source</span>
        <input
          type="text"
          value={preview.table || ''}
          onChange={(e) => onChange('table', e.target.value)}
          placeholder="Table name"
          className="text-sm font-mono text-[var(--text-secondary)] bg-transparent border-b border-transparent hover:border-[var(--border-strong)] focus:border-[var(--brand)] focus:outline-none px-1 py-0.5 flex-1"
        />
      </div>

      <div className="alert-builder-preview-field">
        <span className="alert-builder-preview-label">Window</span>
        <input
          type="text"
          value={preview.time_window || ''}
          onChange={(e) => onChange('time_window', e.target.value)}
          placeholder="e.g. yesterday, last_7_days"
          className="text-sm text-[var(--text-primary)] bg-transparent border-b border-transparent hover:border-[var(--border-strong)] focus:border-[var(--brand)] focus:outline-none px-1 py-0.5 flex-1"
        />
      </div>

      <div className="alert-builder-preview-field items-center">
        <Mail size={13} className="text-[var(--text-muted)]" />
        <span className="text-sm text-[var(--text-primary)] capitalize mr-2">Email →</span>
        <input
          type="email"
          value={preview.notification_target || ''}
          onChange={(e) => onChange('notification_target', e.target.value)}
          placeholder="Email address"
          className="text-sm text-[var(--text-primary)] bg-transparent border-b border-transparent hover:border-[var(--border-strong)] focus:border-[var(--brand)] focus:outline-none px-1 py-0.5 flex-1 min-w-[120px]"
        />
      </div>

      {preview.metric_sql && (
        <details className="mt-2">
          <summary className="text-xs text-[var(--text-muted)] cursor-pointer hover:text-[var(--text-secondary)] transition-colors">
            View SQL
          </summary>
          <pre className="alerts-sql-block mt-1">{preview.metric_sql}</pre>
        </details>
      )}
    </div>
  );
};

/* ─────────────────────────────────────────────
   Main Component
───────────────────────────────────────────── */

const AlertBuilderPage = ({ sidebarCollapsed, chatOpen, chatModal, chatWidthPx }: AlertBuilderPageProps) => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [streaming, setStreaming] = useState(false);
  const [preview, setPreview] = useState<AlertPreview | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sessionId] = useState(() => crypto.randomUUID());

  const [streamingToolCalls, setStreamingToolCalls] = useState<StreamingToolCall[]>([]);
  const [expandedAgentActivity, setExpandedAgentActivity] = useState<Record<number, boolean>>({});

  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const fetchAlerts = useAlertStore((s) => s.fetchAlerts);
  const setNavigationPage = useDashboardStore((s) => s.setNavigationPage);

  // Auto-scroll on new messages
  useEffect(() => {
    const listEl = scrollRef.current;
    if (!listEl) return;
    const scrollToBottom = () => {
      listEl.scrollTop = listEl.scrollHeight;
    };
    scrollToBottom();
    if (streaming) {
      requestAnimationFrame(scrollToBottom);
    }
  }, [messages, streamingToolCalls, streaming]);

  // Focus input on mount
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const handleSend = useCallback(async () => {
    const text = input.trim();
    if (!text || streaming) return;

    setInput('');
    setError(null);
    setSaved(false);
    setMessages((prev) => [...prev, { role: 'user', content: text }]);
    setStreaming(true);
    setStreamingToolCalls([]);

    let assistantText = '';
    let needsPostToolSeparator = false;

    setMessages((prev) => [...prev, { role: 'assistant', content: '', toolCalls: [] }]);

    try {
      await streamAlertBuilder(sessionId, text, (event, data) => {
        if (event === 'token') {
          const d = data as { content?: string };
          if (d.content) {
            let piece = d.content;
            if (needsPostToolSeparator && assistantText.trim().length > 0 && piece.trim().length > 0) {
              piece = `\n\n---\n\n${piece}`;
              needsPostToolSeparator = false;
            }
            assistantText += piece;
            setMessages((prev) => {
              const updated = [...prev];
              const last = updated[updated.length - 1];
              if (last && last.role === 'assistant') {
                updated[updated.length - 1] = { ...last, content: assistantText };
              }
              return updated;
            });
          }
        } else if (event === 'tool_call_start') {
          needsPostToolSeparator = true;
          const d = data as { tool_call_id: string; tool_name: string };
          setStreamingToolCalls((prev) => [...prev, { id: d.tool_call_id, name: d.tool_name }]);
        } else if (event === 'tool_result') {
          needsPostToolSeparator = true;
          const d = data as { tool_call_id: string; tool_name: string; result: string };
          setStreamingToolCalls((prev) => prev.filter((tc) => tc.id !== d.tool_call_id));
          
          const newResult = { tool_name: d.tool_name, content: d.result };
          setMessages((msgs) => {
            const newMsgs = [...msgs];
            const lastMsg = newMsgs[newMsgs.length - 1];
            if (lastMsg && lastMsg.role === 'assistant') {
              const currentTools = lastMsg.toolCalls || [];
              newMsgs[newMsgs.length - 1] = { ...lastMsg, toolCalls: [...currentTools, newResult] };
            }
            return newMsgs;
          });
        } else if (event === 'alert_preview') {
          setPreview(data as AlertPreview);
        } else if (event === 'error') {
          const d = data as { message?: string };
          setError(d.message ?? 'An error occurred');
        }
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Stream failed';
      const isNetErr = msg.toLowerCase().includes('failed to fetch') || msg.toLowerCase().includes('network');
      setError(isNetErr ? 'network error' : msg);
    } finally {
      setStreaming(false);
      setStreamingToolCalls([]);
    }
  }, [input, streaming, sessionId]);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      void handleSend();
    }
  };

  const handleSaveAlert = async () => {
    if (!preview?.name || !preview?.metric_sql) return;
    setSaving(true);
    setError(null);
    try {
      await createAlert({
        name: preview.name,
        description: preview.metric_description,
        definition: preview as Record<string, unknown>,
      });
      setSaved(true);
      await fetchAlerts();
      // Redirect to manage after a short delay
      setTimeout(() => setNavigationPage('alerts'), 1500);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save alert');
    } finally {
      setSaving(false);
    }
  };

  const isPreviewComplete = !!(preview?.name && preview?.metric_sql && preview?.comparator && preview?.threshold != null && preview?.notification_target);

  return (
    <DataPageFrame
      sidebarCollapsed={sidebarCollapsed}
      chatOpen={chatOpen}
      chatModal={chatModal}
      chatWidthPx={chatWidthPx}
    >
      <div className="mx-auto w-full max-w-7xl pb-8">
        <header className="mb-6">
          <p className="text-xs font-semibold uppercase tracking-wider text-[var(--brand)] mb-2">
            Alerts
          </p>
          <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight text-[var(--text-primary)] mb-2">
            Create Alert
          </h1>
          <p className="text-[0.9375rem] leading-relaxed text-[var(--text-secondary)] whitespace-nowrap">
            Describe the alert you'd like in natural language. The AI will build a structured definition with live preview.
          </p>
        </header>

        {error && (
          <div className="alerts-error-banner mb-4">
            <AlertTriangle size={14} />
            <span className="text-xs">{error}</span>
            <button type="button" onClick={() => setError(null)} className="text-xs underline ml-auto">
              Dismiss
            </button>
          </div>
        )}

        <div className="alert-builder-layout">
          {/* Chat Column */}
          <div className="alert-builder-chat">
            <div className="alert-builder-chat-header">
              <Sparkles size={16} className="text-[var(--brand)]" />
              <span className="text-sm font-semibold text-[var(--text-primary)]">Alert Builder</span>
            </div>

            <div className="alert-builder-messages" ref={scrollRef}>
              {messages.length === 0 && (
                <div className="alert-builder-placeholder">
                  <BellPlus size={24} className="text-[var(--text-muted)]" />
                  <p className="text-sm text-[var(--text-secondary)] text-center">
                    Tell me what you'd like to be alerted about.
                  </p>
                  <div className="alert-builder-examples">
                    <button
                      type="button"
                      className="alert-builder-example"
                      onClick={() => setInput('Alert me when daily ad spend exceeds $500')}
                    >
                      "Alert me when daily ad spend exceeds $500"
                    </button>
                    <button
                      type="button"
                      className="alert-builder-example"
                      onClick={() => setInput('Notify me if weekly revenue drops below $10,000')}
                    >
                      "Notify me if weekly revenue drops below $10,000"
                    </button>
                    <button
                      type="button"
                      className="alert-builder-example"
                      onClick={() => setInput('Tell me when yesterday\'s order count is zero')}
                    >
                      "Tell me when yesterday's order count is zero"
                    </button>
                  </div>
                </div>
              )}

              {messages.map((msg, i) => {
                const isUser = msg.role === 'user';
                const hasToolCalls = msg.toolCalls && msg.toolCalls.length > 0;
                const isExpanded = expandedAgentActivity[i] ?? false;
                const hasContent = !!msg.content;

                return (
                  <div key={i} className="chat-message-group flex flex-col">
                    {hasContent ? (
                      <div className={`chat-msg ${isUser ? 'chat-msg--user' : 'chat-msg--assistant'}`}>
                        {isUser ? (
                          <div className="chat-msg-content">{msg.content}</div>
                        ) : (
                          <div className="chat-msg-content chat-msg-content--md min-w-0">
                            <MarkdownMessage content={msg.content} className="text-[0.8125rem] leading-[1.45]" />
                          </div>
                        )}
                      </div>
                    ) : (
                      (!hasToolCalls && streaming && i === messages.length - 1) ? (
                        <div className="chat-msg chat-msg--assistant">
                          <div className="chat-msg-content chat-msg-content--md min-w-0">
                            <span className="flex items-center gap-1.5 text-[var(--text-muted)]">
                              <Loader2 size={12} className="animate-spin" /> Thinking...
                            </span>
                          </div>
                        </div>
                      ) : null
                    )}

                    {msg.role === 'assistant' && hasToolCalls && (
                      <div className="chat-thought-section mt-3">
                        <button
                          type="button"
                          className="chat-thought-header"
                          onClick={() => setExpandedAgentActivity(p => ({ ...p, [i]: !p[i] }))}
                          aria-expanded={isExpanded}
                        >
                          <Wrench size={16} className="text-[var(--brand)]" />
                          <span className="chat-thought-title">Agent activity</span>
                          <span className="chat-thought-badge">
                            {msg.toolCalls!.length} tool{msg.toolCalls!.length !== 1 ? 's' : ''} used
                          </span>
                          {isExpanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
                        </button>
                        {isExpanded && (
                          <div className="chat-thought-body">
                            {msg.toolCalls!.map((tc, j) => (
                              <div key={j} className="chat-thought-item chat-thought-item--result">
                                <span className="chat-thought-tool-name">{tc.tool_name ?? 'Tool'}</span>
                                <pre className="chat-thought-tool-result">{tc.content}</pre>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}

              {streaming && streamingToolCalls.length > 0 && (
                <div className="chat-message-group flex flex-col">
                  <div className="chat-thought-section mt-3 mb-2">
                    <button type="button" className="chat-thought-header chat-thought-header--no-toggle" aria-expanded>
                      <Wrench size={16} className="text-[var(--brand)]" />
                      <span className="chat-thought-title">Agent activity</span>
                      <Loader2 size={14} className="animate-spin text-[var(--brand)]" />
                    </button>
                    <div className="chat-thought-body">
                      {streamingToolCalls.map((tc) => (
                        <div key={tc.id} className="chat-thought-item chat-thought-item--in-progress">
                          <Loader2 size={14} className="animate-spin flex-shrink-0" />
                          <span>Calling <code>{tc.name}</code>…</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>

            <div className="alert-builder-input-area">
              <textarea
                ref={inputRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Describe your alert..."
                rows={1}
                className="alert-builder-textarea"
                disabled={streaming}
              />
              <button
                type="button"
                onClick={handleSend}
                disabled={!input.trim() || streaming}
                className="alert-builder-send-btn"
              >
                {streaming ? <Loader2 size={16} className="animate-spin" /> : <Send size={16} />}
              </button>
            </div>
          </div>

          {/* Preview Column */}
          <div className="alert-builder-preview-col">
            <div className="alert-builder-preview-title">
              <Bell size={14} className="text-[var(--brand)]" />
              <span className="text-xs font-semibold uppercase tracking-wider text-[var(--text-muted)]">
                Alert Preview
              </span>
            </div>

            <PreviewCard 
              preview={preview} 
              onChange={(key, value) => setPreview(prev => prev ? { ...prev, [key]: value } : null)}
            />

            {isPreviewComplete && !saved && (
              <button
                type="button"
                onClick={handleSaveAlert}
                disabled={saving}
                className="alert-builder-save-btn"
              >
                {saving ? (
                  <><Loader2 size={14} className="animate-spin" /> Saving...</>
                ) : (
                  <><CheckCircle2 size={14} /> Save Alert</>
                )}
              </button>
            )}

            {saved && (
              <div className="alert-builder-saved">
                <CheckCircle2 size={16} className="text-emerald-400" />
                <span className="text-sm font-medium text-emerald-400">Alert saved! Redirecting...</span>
              </div>
            )}
          </div>
        </div>
      </div>
    </DataPageFrame>
  );
};

export default AlertBuilderPage;
