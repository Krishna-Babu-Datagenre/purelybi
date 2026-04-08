import { useState, useRef, useEffect } from 'react';
import { MoreVertical, Trash2, Loader2 } from 'lucide-react';

interface WidgetContextMenuProps {
  widgetTitle: string;
  onDelete: () => Promise<void> | void;
}

const WidgetContextMenu = ({ widgetTitle, onDelete }: WidgetContextMenuProps) => {
  const [open, setOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
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

  if (deleting) {
    return (
      <div className="widget-context-menu">
        <div className="widget-context-trigger" style={{ cursor: 'default' }}>
          <Loader2 size={15} className="animate-spin" style={{ opacity: 0.7 }} />
        </div>
      </div>
    );
  }

  return (
    <div ref={menuRef} className="widget-context-menu">
      <button
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

      {open && (
        <div className="widget-context-dropdown">
          <button
            type="button"
            className="widget-context-item widget-context-item--danger"
            onMouseDown={(e) => e.stopPropagation()}
            onClick={handleDelete}
          >
            <Trash2 size={13} />
            <span>Delete</span>
          </button>
        </div>
      )}
    </div>
  );
};

export default WidgetContextMenu;
