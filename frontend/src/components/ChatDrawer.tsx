import { useRef, useEffect, useCallback } from 'react';
import { MessageSquare, X, Trash2, Maximize2, Minimize2 } from 'lucide-react';
import { useChatStore } from '../store/useChatStore';
import ChatMessageList from './ChatMessageList';
import ChatInput from './ChatInput';

const MAX_WIDTH_PERCENT = 50;

const ChatDrawer = () => {
  const isOpen = useChatStore((s) => s.isOpen);
  const isModal = useChatStore((s) => s.isModal);
  const widthPx = useChatStore((s) => s.widthPx);
  const setWidthPx = useChatStore((s) => s.setWidthPx);
  const resizeWidth = useChatStore((s) => s.resizeWidth);
  const closeChat = useChatStore((s) => s.closeChat);
  const setModal = useChatStore((s) => s.setModal);
  const clearHistory = useChatStore((s) => s.clearHistory);
  const loadHistory = useChatStore((s) => s.loadHistory);

  const isResizing = useRef(false);

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      if (e.button !== 0) return;
      e.preventDefault();
      isResizing.current = true;

      const onMove = (e: MouseEvent) => {
        resizeWidth(e.clientX);
      };
      const onUp = () => {
        isResizing.current = false;
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
      };
      window.addEventListener('mousemove', onMove);
      window.addEventListener('mouseup', onUp);
    },
    [resizeWidth]
  );

  useEffect(() => {
    if (isOpen && !isModal) {
      const maxPx = window.innerWidth * (MAX_WIDTH_PERCENT / 100);
      setWidthPx(Math.min(widthPx, maxPx));
    }
  }, [isOpen, isModal, setWidthPx, widthPx]);

  useEffect(() => {
    if (isOpen) loadHistory();
  }, [isOpen, loadHistory]);

  if (!isOpen) return null;

  const handleClear = () => {
    clearHistory();
  };

  const panel = (
    <>
      {/* Resize handle: left edge (only in drawer mode, not modal) */}
      {!isModal && (
        <button
          type="button"
          aria-label="Resize chat"
          className="chat-drawer-resize-handle"
          onMouseDown={handleMouseDown}
        />
      )}

      <div className="chat-drawer-inner">
        <header className="chat-drawer-header">
          <div className="flex items-center gap-2">
            <MessageSquare size={20} className="text-[var(--brand)]" />
            <span className="chat-drawer-title">Chat</span>
          </div>
          <div className="flex items-center gap-1">
            <button
              type="button"
              className="chat-drawer-icon-btn"
              onClick={handleClear}
              title="Clear conversation"
            >
              <Trash2 size={18} />
            </button>
            <button
              type="button"
              className="chat-drawer-icon-btn"
              onClick={() => setModal(!isModal)}
              title={isModal ? 'Collapse to drawer' : 'Pop out'}
            >
              {isModal ? <Minimize2 size={18} /> : <Maximize2 size={18} />}
            </button>
            <button
              type="button"
              className="chat-drawer-icon-btn"
              onClick={closeChat}
              title="Close"
            >
              <X size={18} />
            </button>
          </div>
        </header>

        <ChatMessageList />
        <ChatInput />
      </div>
    </>
  );

  if (isModal) {
    return (
      <div className="chat-drawer-modal-backdrop" onClick={closeChat}>
        <div
          className="chat-drawer chat-drawer--modal"
          style={{ width: Math.min(widthPx, 448) }}
          onClick={(e) => e.stopPropagation()}
        >
          {panel}
        </div>
      </div>
    );
  }

  return (
    <div
      className="chat-drawer chat-drawer--sidebar"
      style={{ width: widthPx }}
    >
      {panel}
    </div>
  );
};

export default ChatDrawer;
