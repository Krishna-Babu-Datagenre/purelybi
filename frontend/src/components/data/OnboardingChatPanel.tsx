import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { Loader2, MessageSquare, Send } from 'lucide-react';
import {
  fetchOnboardingOAuthResult,
  normalizeAuthOptionsPayload,
  normalizeInputFieldsPayload,
  streamOnboardingChat,
  type OnboardingUiBlock,
} from '../../services/onboardingApi';

/** Default value for `<select>` when agent sends string or `{ value, label }`. */
function selectDefaultString(def: unknown): string {
  if (def == null) return '';
  if (typeof def === 'object' && def !== null && 'value' in def) {
    return String((def as { value: unknown }).value ?? '');
  }
  return String(def);
}
import { friendlyToolLabel, normalizeTokenContent } from './onboardingToolLabels';
import MarkdownMessage from './MarkdownMessage';

interface Msg {
  role: 'user' | 'assistant';
  content: string;
}

interface ActivityRow {
  callId: string;
  toolName: string;
  phase: 'running' | 'done';
}

interface StreamOption {
  name: string;
  accessible: boolean;
  selected?: boolean;
}

type RawStreamOption = { name: string; accessible?: boolean; selected?: boolean } | string;

interface OnboardingChatPanelProps {
  catalogConnectorId: string;
  connectorName: string;
  onBack: () => void;
}

function StreamSkeleton() {
  return (
    <div
      className="rounded-lg border border-[var(--border-subtle)] bg-[var(--bg-elevated)]/50 p-3 space-y-2 animate-pulse"
      aria-busy="true"
      aria-label="Connecting to assistant"
    >
      <div className="h-2.5 w-36 rounded bg-[var(--bg-elevated)]" />
      <div className="h-2 w-full max-w-sm rounded bg-[var(--bg-elevated)]" />
    </div>
  );
}

/**
 * Single status area: current step + nested completed steps + live reply text.
 * Matches where “Assistant is responding” lived — one container for all agent activity.
 */
function AssistantStatusBlock({
  awaitingFirstEvent,
  streamBusy,
  streamingText,
  activityRows,
}: {
  awaitingFirstEvent: boolean;
  streamBusy: boolean;
  streamingText: string;
  activityRows: ActivityRow[];
}) {
  const running = activityRows.filter((r) => r.phase === 'running');
  const done = activityRows.filter((r) => r.phase === 'done');
  const currentRunning = running[running.length - 1];

  let statusLine: string | null = null;
  if (currentRunning) {
    statusLine = friendlyToolLabel(currentRunning.toolName);
  } else if (streamingText) {
    statusLine = 'Writing reply…';
  } else if (streamBusy && !awaitingFirstEvent) {
    statusLine = 'Assistant is responding…';
  }

  const showCard =
    (awaitingFirstEvent && streamBusy) ||
    streamBusy ||
    activityRows.length > 0 ||
    streamingText.length > 0;

  if (!showCard) return null;

  return (
    <div className="rounded-xl border border-[var(--border-subtle)] bg-[var(--bg-canvas)] p-3 space-y-2">
      {awaitingFirstEvent && streamBusy && <StreamSkeleton />}

      {!awaitingFirstEvent && (statusLine || (streamBusy && !streamingText)) && (
        <div className="flex items-start gap-2 text-sm text-[var(--text-primary)]">
          {(currentRunning || streamBusy) && (
            <Loader2 size={16} className="animate-spin text-[var(--brand)] shrink-0 mt-0.5" aria-hidden />
          )}
          <span className="leading-snug">
            {statusLine ?? (streamBusy ? 'Working…' : '')}
          </span>
        </div>
      )}

      {done.length > 0 && (
        <div className="pl-3 border-l-2 border-[var(--border-subtle)] space-y-1">
          <p className="text-[10px] font-semibold uppercase tracking-wide text-[var(--text-muted)]">
            Previous steps
          </p>
          <ul className="space-y-1">
            {done.map((row) => (
              <li key={row.callId} className="flex items-start gap-2 text-xs text-[var(--text-secondary)]">
                <span className="text-[var(--brand)] shrink-0" aria-hidden>
                  ✓
                </span>
                <span>{friendlyToolLabel(row.toolName)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {streamingText.length > 0 && (
        <div className="pt-1 border-t border-[var(--border-subtle)]">
          <MarkdownMessage content={streamingText} />
        </div>
      )}
    </div>
  );
}

export default function OnboardingChatPanel({
  catalogConnectorId,
  connectorName,
  onBack,
}: OnboardingChatPanelProps) {
  const threadId = useMemo(() => crypto.randomUUID(), []);
  const startedRef = useRef(false);
  const oauthHandledRef = useRef(false);
  const formId = useId();
  const [messages, setMessages] = useState<Msg[]>([]);
  const [input, setInput] = useState('');
  const [streamingText, setStreamingText] = useState('');
  const [activityRows, setActivityRows] = useState<ActivityRow[]>([]);
  const [uiBlock, setUiBlock] = useState<OnboardingUiBlock | null>(null);
  /** True while the SSE request is in flight (headers + body). */
  const [streamBusy, setStreamBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [awaitingFirstEvent, setAwaitingFirstEvent] = useState(true);
  const bottomRef = useRef<HTMLDivElement>(null);

  const normalizeUiBlock = (raw: OnboardingUiBlock): OnboardingUiBlock => {
    if (raw.type === 'auth_options') {
      return {
        type: 'auth_options',
        options: normalizeAuthOptionsPayload(
          (raw as { options?: unknown }).options,
        ),
      };
    }
    if (raw.type === 'input_fields') {
      return {
        type: 'input_fields',
        fields: normalizeInputFieldsPayload((raw as { fields?: unknown }).fields),
      };
    }
    if (raw.type !== 'stream_selector') return raw;
    const streams = (raw.streams ?? [])
      .map((s) => {
        if (typeof s === 'string') {
          return { name: s, accessible: true } as StreamOption;
        }
        if (!s || typeof s !== 'object') return null;
        const rec = s as Record<string, unknown>;
        const name = String(rec.name ?? '').trim();
        if (!name) return null;
        return {
          name,
          // Treat missing accessible as true so the selector does not render empty.
          accessible: rec.accessible !== false,
          selected: rec.selected === true,
        } as StreamOption;
      })
      .filter((s): s is StreamOption => Boolean(s));

    return { ...raw, streams };
  };

  const scrollToBottom = () => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, streamingText, uiBlock, activityRows]);

  const runStream = useCallback(
    async (req: Parameters<typeof streamOnboardingChat>[0]) => {
      setStreamBusy(true);
      setError(null);
      setStreamingText('');
      setActivityRows([]);
      setUiBlock(null);
      setAwaitingFirstEvent(true);

      let acc = '';
      try {
        await streamOnboardingChat(req, (ev, data) => {
          if (ev === 'start') {
            setAwaitingFirstEvent(false);
            return;
          }
          setAwaitingFirstEvent(false);
          if (ev === 'error') {
            const d = data as { detail?: string };
            setError(d.detail ?? 'Request failed');
            return;
          }
          if (ev === 'token' && data && typeof data === 'object' && 'content' in data) {
            const piece = normalizeTokenContent(
              (data as { content?: unknown }).content,
            );
            acc += piece;
            setStreamingText(acc);
            return;
          }
          if (ev === 'tool_call_start' && data && typeof data === 'object') {
            const d = data as { tool_call_id?: string; tool_name?: string };
            if (d.tool_call_id && d.tool_name) {
              setActivityRows((prev) => [
                ...prev,
                {
                  callId: d.tool_call_id!,
                  toolName: d.tool_name!,
                  phase: 'running',
                },
              ]);
            }
            return;
          }
          if (ev === 'tool_result' && data && typeof data === 'object') {
            const d = data as { tool_call_id?: string; tool_name?: string };
            if (d.tool_call_id) {
              setActivityRows((prev) =>
                prev.map((r) =>
                  r.callId === d.tool_call_id ? { ...r, phase: 'done' as const } : r,
                ),
              );
            }
            return;
          }
          if (ev === 'ui_block' && data && typeof data === 'object' && 'ui' in data) {
            const ui = (data as { ui: OnboardingUiBlock }).ui;
            // Stream is still open until `end` — release UI so buttons are not stuck disabled.
            setStreamBusy(false);
            setUiBlock(normalizeUiBlock(ui));
            return;
          }
          if (ev === 'end') {
            setMessages((prev) => {
              const next = [...prev];
              const t = acc.trim();
              if (t) next.push({ role: 'assistant', content: t });
              return next;
            });
            setStreamingText('');
          }
        });
      } finally {
        setStreamBusy(false);
        setAwaitingFirstEvent(false);
      }
    },
    [],
  );

  useEffect(() => {
    if (startedRef.current) return;
    startedRef.current = true;
    void (async () => {
      await runStream({
        message: '',
        thread_id: threadId,
        catalog_connector_id: catalogConnectorId,
      });
    })();
  }, [catalogConnectorId, runStream, threadId]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get('onboarding_oauth') !== '1') return;
    const state = params.get('state');
    if (!state) return;
    if (oauthHandledRef.current) return;
    oauthHandledRef.current = true;

    const cleanUrl = `${window.location.pathname}${window.location.hash}`;
    window.history.replaceState({}, '', cleanUrl);

    void (async () => {
      try {
        const res = await fetchOnboardingOAuthResult(state);
        setMessages((prev) => [
          ...prev,
          { role: 'user', content: res.display_message || 'OAuth completed.' },
        ]);
        await runStream({
          message: res.agent_message,
          thread_id: threadId,
        });
      } catch (e) {
        setError(e instanceof Error ? e.message : 'OAuth completion failed');
      }
    })();
  }, [runStream, threadId]);

  const sendFreeform = async () => {
    const t = input.trim();
    if (!t || streamBusy) return;
    setInput('');
    setMessages((prev) => [...prev, { role: 'user', content: t }]);
    await runStream({ message: t, thread_id: threadId });
  };

  const submitAuthOption = async (label: string, authType: string) => {
    if (streamBusy) return;
    const safeLabel = label.trim() || 'Selected authentication method';
    const safeAuth = (authType || label).trim() || safeLabel;
    setUiBlock(null);
    setMessages((prev) => [...prev, { role: 'user', content: `Using **${safeLabel}**.` }]);
    await runStream({
      message: '',
      thread_id: threadId,
      auth_choice: { label: safeLabel, auth_type: safeAuth },
    });
  };

  const submitStreams = async (names: string[]) => {
    if (streamBusy) return;
    setUiBlock(null);
    setMessages((prev) => [
      ...prev,
      { role: 'user', content: `Selected ${names.length} stream(s).` },
    ]);
    await runStream({
      message: '',
      thread_id: threadId,
      stream_names: names,
    });
  };

  const submitForm = async (form: HTMLFormElement) => {
    if (streamBusy || uiBlock?.type !== 'input_fields') return;
    const fields = uiBlock.fields.map((f, idx) => {
      const fieldKey = String(f.key ?? '').trim() || `field_${idx + 1}`;
      const el = form.elements.namedItem(`f_${fieldKey}`) as
        | HTMLInputElement
        | HTMLTextAreaElement
        | HTMLSelectElement
        | null;
      let value: unknown = el?.value ?? '';
      const ftype = f.type ?? 'text';
      if (ftype === 'boolean' && el && 'checked' in el) {
        value = (el as HTMLInputElement).checked;
      }
      if (ftype === 'number' && value !== '') {
        value = Number(value);
      }
      if (ftype === 'array' && typeof value === 'string') {
        const s = value.trim();
        try {
          value = JSON.parse(s);
        } catch {
          value = s.split(',').map((x) => x.trim()).filter(Boolean);
        }
      }
      return { key: fieldKey, type: ftype, value };
    });

    setUiBlock(null);
    setMessages((prev) => [...prev, { role: 'user', content: 'Submitted configuration.' }]);
    await runStream({
      message: '',
      thread_id: threadId,
      form_fields: fields,
    });
  };

  return (
    <div className="flex flex-col min-h-0 flex-1 w-full max-w-3xl mx-auto gap-4">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2 min-w-0">
          <MessageSquare className="text-[var(--brand)] shrink-0" size={20} aria-hidden />
          <div className="min-w-0">
            <h3 className="text-base font-semibold text-[var(--text-primary)] truncate">
              Guided setup — {connectorName}
            </h3>
            <p className="text-xs text-[var(--text-muted)]">
              Status, tool activity, and the live reply appear together below.
            </p>
          </div>
        </div>
        <button
          type="button"
          onClick={onBack}
          className="text-sm font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors duration-200 cursor-pointer rounded-lg px-3 py-2 focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
        >
          Back to details
        </button>
      </div>

      {error && (
        <div
          className="rounded-xl border border-red-500/25 bg-red-950/25 px-4 py-3 text-sm text-red-100"
          role="alert"
        >
          {error}
        </div>
      )}

      <div className="flex-1 min-h-[12rem] max-h-[min(52vh,28rem)] overflow-y-auto rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-4 space-y-4">
        {messages.map((m, i) => (
          <div
            key={i}
            className={`chat-msg ${m.role === 'user' ? 'chat-msg--user' : 'chat-msg--assistant'}`}
          >
            <MarkdownMessage content={m.content} />
          </div>
        ))}

        <AssistantStatusBlock
          awaitingFirstEvent={awaitingFirstEvent}
          streamBusy={streamBusy}
          streamingText={streamingText}
          activityRows={activityRows}
        />

        <div ref={bottomRef} />
      </div>

      {uiBlock && uiBlock.type === 'auth_options' && (
        <div className="rounded-xl border border-[var(--border-strong)] bg-[var(--bg-surface-alt)] p-4 space-y-3">
          <p className="text-sm font-medium text-[var(--text-primary)]">Choose an authentication method</p>
          <div className="flex flex-wrap gap-2">
            {uiBlock.options.map((opt, i) => (
              <button
                key={i}
                type="button"
                disabled={streamBusy}
                onClick={() => void submitAuthOption(opt.label, opt.auth_type)}
                className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] px-4 py-3 text-left text-sm transition-colors duration-200 hover:border-[var(--border-strong)] hover:bg-[var(--bg-elevated)] cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] disabled:opacity-50 disabled:pointer-events-none disabled:cursor-not-allowed"
              >
                <span className="block font-medium text-[var(--text-primary)]">{opt.label}</span>
                {opt.description && (
                  <span className="block text-xs font-normal text-[var(--text-muted)] mt-1">
                    {opt.description}
                  </span>
                )}
              </button>
            ))}
          </div>
        </div>
      )}

      {uiBlock && uiBlock.type === 'oauth_button' && (
        <div className="rounded-xl border border-[var(--border-strong)] bg-[var(--bg-surface-alt)] p-4 space-y-3">
          <p className="text-sm font-medium text-[var(--text-primary)]">OAuth authorization</p>
          <p className="text-xs text-[var(--text-secondary)]">
            Continue in a new tab. After you approve access, you will return here automatically.
          </p>
          <a
            href={uiBlock.url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center justify-center rounded-xl border border-[var(--border-default)] bg-[var(--brand-dim)] px-4 py-3 text-sm font-medium text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] transition-colors duration-200 cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
          >
            Authorize with provider
          </a>
        </div>
      )}

      {uiBlock && uiBlock.type === 'input_fields' && (
        <form
          id={formId}
          className="rounded-xl border border-[var(--border-strong)] bg-[var(--bg-surface-alt)] p-4 space-y-4"
          onSubmit={(e) => {
            e.preventDefault();
            void submitForm(e.currentTarget);
          }}
        >
          <p className="text-sm font-medium text-[var(--text-primary)]">Configuration</p>
          {uiBlock.fields.map((f) => {
            const id = `${formId}-${f.key}`;
            const label = f.required ? `${f.label ?? f.key} *` : (f.label ?? f.key);
            const ftype = f.type ?? 'text';
            const common =
              'w-full rounded-lg border border-[var(--border-default)] bg-[var(--bg-surface)] px-3 py-2 text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--border-strong)] focus:ring-1 focus:ring-[var(--brand)]';
            if (ftype === 'password') {
              return (
                <div key={f.key}>
                  <label htmlFor={id} className="block text-xs font-medium text-[var(--text-secondary)] mb-1">
                    {label}
                  </label>
                  <input id={id} name={`f_${f.key}`} type="password" className={common} autoComplete="off" />
                  {f.description && (
                    <p className="text-xs text-[var(--text-muted)] mt-1">{f.description}</p>
                  )}
                </div>
              );
            }
            if (ftype === 'boolean') {
              return (
                <label key={f.key} className="flex items-center gap-2 text-sm cursor-pointer">
                  <input name={`f_${f.key}`} type="checkbox" defaultChecked={Boolean(f.default)} />
                  <span>{label}</span>
                </label>
              );
            }
            if (ftype === 'select' && f.options && f.options.length > 0) {
              return (
                <div key={f.key}>
                  <label htmlFor={id} className="block text-xs font-medium text-[var(--text-secondary)] mb-1">
                    {label}
                  </label>
                  <select
                    id={id}
                    name={`f_${f.key}`}
                    className={common}
                    defaultValue={selectDefaultString(f.default)}
                  >
                    {f.options.map((o, i) => (
                      <option key={`${o.value}-${i}`} value={o.value}>
                        {o.label}
                      </option>
                    ))}
                  </select>
                </div>
              );
            }
            if (ftype === 'array') {
              return (
                <div key={f.key}>
                  <label htmlFor={id} className="block text-xs font-medium text-[var(--text-secondary)] mb-1">
                    {label}
                  </label>
                  <textarea
                    id={id}
                    name={`f_${f.key}`}
                    className={`${common} min-h-[4rem] font-mono text-xs`}
                    placeholder={f.placeholder}
                    defaultValue={f.default != null ? String(f.default) : ''}
                  />
                  {f.description && (
                    <p className="text-xs text-[var(--text-muted)] mt-1">{f.description}</p>
                  )}
                </div>
              );
            }
            if (ftype === 'number') {
              return (
                <div key={f.key}>
                  <label htmlFor={id} className="block text-xs font-medium text-[var(--text-secondary)] mb-1">
                    {label}
                  </label>
                  <input
                    id={id}
                    name={`f_${f.key}`}
                    type="number"
                    className={common}
                    defaultValue={f.default != null ? Number(f.default) : undefined}
                  />
                  {f.description && (
                    <p className="text-xs text-[var(--text-muted)] mt-1">{f.description}</p>
                  )}
                </div>
              );
            }
            if (ftype === 'date') {
              return (
                <div key={f.key}>
                  <label htmlFor={id} className="block text-xs font-medium text-[var(--text-secondary)] mb-1">
                    {label}
                  </label>
                  <input id={id} name={`f_${f.key}`} type="date" className={common} />
                </div>
              );
            }
            return (
              <div key={f.key}>
                <label htmlFor={id} className="block text-xs font-medium text-[var(--text-secondary)] mb-1">
                  {label}
                </label>
                <input
                  id={id}
                  name={`f_${f.key}`}
                  type="text"
                  className={common}
                  placeholder={f.placeholder}
                  defaultValue={f.default != null ? String(f.default) : ''}
                />
                {f.description && (
                  <p className="text-xs text-[var(--text-muted)] mt-1">{f.description}</p>
                )}
              </div>
            );
          })}
          <button
            type="submit"
            disabled={streamBusy}
            className="w-full rounded-xl bg-[var(--brand)] text-white font-medium py-2.5 text-sm hover:opacity-95 transition-opacity cursor-pointer disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
          >
            Submit configuration
          </button>
        </form>
      )}

      {uiBlock && uiBlock.type === 'stream_selector' && (
        <StreamSelectorForm
          streamBusy={streamBusy}
          streams={uiBlock.streams}
          onConfirm={(names) => void submitStreams(names)}
        />
      )}

      <div className="flex gap-2">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault();
              void sendFreeform();
            }
          }}
          placeholder="Ask a question or add details…"
          disabled={streamBusy}
          className="flex-1 rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] px-4 py-3 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--border-strong)] focus:ring-1 focus:ring-[var(--brand)] disabled:opacity-50"
          aria-label="Onboarding chat input"
        />
        <button
          type="button"
          disabled={streamBusy || !input.trim()}
          onClick={() => void sendFreeform()}
          className="shrink-0 rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface-alt)] px-4 py-3 text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] transition-colors cursor-pointer disabled:opacity-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
          aria-label="Send message"
        >
          <Send size={18} />
        </button>
      </div>
    </div>
  );
}

function StreamSelectorForm({
  streams,
  streamBusy,
  onConfirm,
}: {
  streams: RawStreamOption[];
  streamBusy: boolean;
  onConfirm: (names: string[]) => void;
}) {
  const normalized = useMemo(
    () =>
      (streams ?? [])
        .map((s) => {
          if (typeof s === 'string') return { name: s, accessible: true } as StreamOption;
          if (!s || typeof s !== 'object') return null;
          const name = String(s.name ?? '').trim();
          if (!name) return null;
          return {
            name,
            accessible: s.accessible !== false,
            selected: s.selected === true,
          } as StreamOption;
        })
        .filter((s): s is StreamOption => Boolean(s)),
    [streams],
  );

  const accessible = useMemo(
    () => normalized.filter((s) => s.accessible).map((s) => s.name),
    [normalized],
  );
  const [sel, setSel] = useState<string[]>(() =>
    normalized.filter((s) => s.accessible && s.selected).map((s) => s.name),
  );

  useEffect(() => {
    const defaults = normalized.filter((s) => s.accessible && s.selected).map((s) => s.name);
    setSel(defaults);
  }, [normalized]);

  const hasAny = accessible.length > 0;

  return (
    <div className="rounded-xl border border-[var(--border-strong)] bg-[var(--bg-surface-alt)] p-4 space-y-3">
      <p className="text-sm font-medium text-[var(--text-primary)]">Select streams</p>
      <div className="flex flex-wrap gap-2 max-h-48 overflow-y-auto">
        {accessible.map((name) => {
          const active = sel.includes(name);
          return (
            <button
              key={name}
              type="button"
              onClick={() =>
                setSel((prev) =>
                  active ? prev.filter((x) => x !== name) : [...prev, name],
                )
              }
              className={`rounded-lg px-3 py-1.5 text-xs font-medium border transition-colors cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] ${
                active
                  ? 'border-[var(--brand)] bg-[var(--brand-dim)] text-[var(--text-primary)]'
                  : 'border-[var(--border-default)] bg-[var(--bg-surface)] text-[var(--text-secondary)] hover:border-[var(--border-strong)]'
              }`}
            >
              {name}
            </button>
          );
        })}
      </div>
      {!hasAny && (
        <p className="text-xs text-[var(--text-muted)]">
          No selectable streams were provided yet. Ask the assistant to refresh stream discovery.
        </p>
      )}
      <button
        type="button"
        disabled={streamBusy || sel.length === 0 || !hasAny}
        onClick={() => onConfirm(sel)}
        className="w-full rounded-xl bg-[var(--brand)] text-white font-medium py-2.5 text-sm hover:opacity-95 cursor-pointer disabled:opacity-50"
      >
        Confirm selection
      </button>
    </div>
  );
}
