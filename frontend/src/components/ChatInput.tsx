import { useState, useRef, useCallback } from 'react';
import { Paperclip, Send, X } from 'lucide-react';
import { useChatStore } from '../store/useChatStore';
import { useDashboardStore } from '../store/useDashboardStore';

export default function ChatInput() {
  const [value, setValue] = useState('');
  const textRef = useRef<HTMLTextAreaElement>(null);
  const sendMessage = useChatStore((s) => s.sendMessage);
  const isStreaming = useChatStore((s) => s.isStreaming);
  const attachedDashboardName = useChatStore((s) => s.attachedDashboardName);
  const attachDashboard = useChatStore((s) => s.attachDashboard);
  const clearAttachedDashboard = useChatStore((s) => s.clearAttachedDashboard);
  const activeDashboard = useDashboardStore((s) => s.getActiveDashboard());
  const activeDashboardName = activeDashboard?.meta?.name ?? null;

  const canAttach = !!activeDashboardName && activeDashboardName !== attachedDashboardName;

  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();
      const trimmed = value.trim();
      if (!trimmed || isStreaming) return;
      sendMessage(trimmed);
      setValue('');
    },
    [value, isStreaming, sendMessage]
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  const handleAttach = () => {
    if (activeDashboardName) attachDashboard(activeDashboardName);
  };

  return (
    <div className="chat-input-region">
      {attachedDashboardName && (
        <div className="chat-attachment-chip" title="Dashboard attached to this chat">
          <Paperclip size={12} />
          <span className="truncate max-w-[14rem]">{attachedDashboardName}</span>
          <button
            type="button"
            className="chat-attachment-chip-remove"
            onClick={clearAttachedDashboard}
            aria-label="Remove attachment"
          >
            <X size={12} />
          </button>
        </div>
      )}
      <form className="chat-input-wrap" onSubmit={handleSubmit}>
        <button
          type="button"
          className="chat-attach-btn"
          onClick={handleAttach}
          disabled={!canAttach || isStreaming}
          title={
            !activeDashboardName
              ? 'Open a dashboard to attach it'
              : attachedDashboardName === activeDashboardName
                ? `Attached: ${activeDashboardName}`
                : `Attach "${activeDashboardName}"`
          }
          aria-label="Attach current dashboard"
        >
          <Paperclip size={16} />
        </button>
        <textarea
          ref={textRef}
          className="chat-input"
          placeholder={
            attachedDashboardName
              ? `Ask about or edit "${attachedDashboardName}"…`
              : 'Ask about your data…'
          }
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={handleKeyDown}
          rows={1}
          disabled={isStreaming}
        />
        <button
          type="submit"
          className="chat-send-btn"
          disabled={!value.trim() || isStreaming}
          aria-label="Send"
        >
          <Send size={18} />
        </button>
      </form>
    </div>
  );
}
