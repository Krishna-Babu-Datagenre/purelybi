import { useState, useRef, useCallback } from 'react';
import { Send } from 'lucide-react';
import { useChatStore } from '../store/useChatStore';

export default function ChatInput() {
  const [value, setValue] = useState('');
  const textRef = useRef<HTMLTextAreaElement>(null);
  const sendMessage = useChatStore((s) => s.sendMessage);
  const isStreaming = useChatStore((s) => s.isStreaming);

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

  return (
    <form className="chat-input-wrap" onSubmit={handleSubmit}>
      <textarea
        ref={textRef}
        className="chat-input"
        placeholder="Ask about your data…"
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
  );
}
