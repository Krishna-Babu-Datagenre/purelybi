import { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { MoreVertical, Trash2, Loader2, Sparkles, Code } from 'lucide-react';

interface WidgetContextMenuProps {
  widgetTitle: string;
  onDelete: () => Promise<void> | void;
  onEditWithAI?: () => void;
  onEditSql?: () => void;
}

const WidgetContextMenu = ({ widgetTitle, onDelete, onEditWithAI, onEditSql }: WidgetContextMenuProps) => {
  const [open, setOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [coords, setCoords] = useState({ top: 0, right: 0 });
  const buttonRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (
        menuRef.current && !menuRef.current.contains(e.target as Node) &&
        buttonRef.current && !buttonRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    
    // Update coordinates on scroll or resize
    const updatePosition = () => {
      if (buttonRef.current) {
        const rect = buttonRef.current.getBoundingClientRect();
        setCoords({
          top: rect.bottom + window.scrollY + 4,
          right: window.innerWidth - rect.right - window.scrollX
        });
      }
    };
    
    updatePosition();
    window.addEventListener('scroll', updatePosition, true);
    window.addEventListener('resize', updatePosition);
    
    return () => {
      document.removeEventListener('mousedown', handleClick);
      window.removeEventListener('scroll', updatePosition, true);
      window.removeEventListener('resize', updatePosition);
    };
  }, [open]);

  const handleDelete = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!window.confirm(`Remove "${widgetTitle}" from this dashboard?`)) return;
    setOpen(false);
    setDeleting(true);
    try {
      await onDelete();
    } catch {
      setDeleting(false);
    }
  };

  const handleEditWithAI = (e: React.MouseEvent) => {
    e.stopPropagation();
    setOpen(false);
    onEditWithAI?.();
  };

  const handleEditSql = (e: React.MouseEvent) => {
    e.stopPropagation();
    setOpen(false);
    onEditSql?.();
  };

  if (deleting) {
    return (
      <div className="widget-context-menu inline-block">
        <div className="widget-context-trigger" style={{ cursor: 'default' }}>
          <Loader2 size={15} className="animate-spin" style={{ opacity: 0.7 }} />
        </div>
      </div>
    );
  }

  const dropdownContent = open ? createPortal(
    <div 
      ref={menuRef}
      className="widget-context-dropdown"
      style={{
        position: 'absolute',
        top: `${coords.top}px`,
        right: `${coords.right}px`,
        zIndex: 99999
      }}
    >
      {onEditWithAI && (
        <button
          type="button"
          className="widget-context-item"
          onMouseDown={(e) => e.stopPropagation()}
          onClick={handleEditWithAI}
        >
          <Sparkles size={13} style={{ color: '#c4b5fd' }} />
          <span>Edit with AI</span>
        </button>
      )}
      {onEditSql && (
        <button
          type="button"
          className="widget-context-item"
          onMouseDown={(e) => e.stopPropagation()}
          onClick={handleEditSql}
        >
          <Code size={13} />
          <span>Edit SQL</span>
        </button>
      )}
      <button
        type="button"
        className="widget-context-item widget-context-item--danger"
        onMouseDown={(e) => e.stopPropagation()}
        onClick={handleDelete}
      >
        <Trash2 size={13} />
        <span>Delete</span>
      </button>
    </div>,
    document.body
  ) : null;

  return (
    <div className="widget-context-menu inline-block">
      <button
        ref={buttonRef}
        type="button"
        className="widget-context-trigger"
        onMouseDown={(e) => e.stopPropagation()}
        onPointerDown={(e) => e.stopPropagation()}
        onClick={(e) => {
          e.stopPropagation();
          setOpen((v) => !v);
        }}
        title="Widget options"
      >
        <MoreVertical size={15} />
      </button>

      {dropdownContent}
    </div>
  );
};

export default WidgetContextMenu;

