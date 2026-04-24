import { memo, useState, useCallback } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import type { TableNodeData } from './SchemaVisualizer.types';

/* ─────────────────────────────────────────────
   TableNode — custom React Flow node
   ─────────────────────────────────────────────
   • Collapsed by default — shows table name +
     linked (FK/PK) columns only.
   • Expand reveals all remaining columns.
   • Per-column source / target handles.
   • Highlighted columns glow when an edge is hovered.
───────────────────────────────────────────── */

function isKeyColumn(name: string): boolean {
  const lower = name.toLowerCase();
  return (
    lower === 'id' ||
    lower.endsWith('_id') ||
    lower.startsWith('id_') ||
    lower === 'uuid' ||
    lower === 'pk' ||
    lower === 'fk' ||
    lower.includes('_fk') ||
    lower.includes('_pk')
  );
}

function TableNode({ data }: NodeProps & { data: TableNodeData }) {
  const { tableName, columns, description, linkedColumns, highlightedColumns, isEditMode, dimmed } = data;
  const [expanded, setExpanded] = useState(false);
  const toggleExpand = useCallback(() => setExpanded((v) => !v), []);

  // Linked cols = those that are endpoints of a relationship or look like PK/FK
  const linkedCols = columns.filter(
    (c) => linkedColumns.has(c.column_name) || isKeyColumn(c.column_name),
  );
  const otherCols = columns.filter(
    (c) => !linkedColumns.has(c.column_name) && !isKeyColumn(c.column_name),
  );
  const hiddenCount = otherCols.length;
  const visibleCols = expanded ? columns : linkedCols;

  const rootClass = [
    'sv-table-node',
    isEditMode ? 'sv-table-node--edit-mode' : '',
    dimmed ? 'sv-table-node--dimmed' : '',
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div className={rootClass}>
      {/* ── Header ── */}
      <div className="sv-table-header" onClick={toggleExpand}>
        <div className="sv-table-header-left">
          <span className="sv-table-expand-icon">
            {expanded ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
          </span>
          <span className="sv-table-name" title={tableName}>{tableName}</span>
        </div>
        <span className="sv-table-col-count">{columns.length}</span>
      </div>

      {/* Description row — only when expanded */}
      {expanded && description && (
        <div className="sv-table-desc-row">{description}</div>
      )}

      {/* ── Column list ── */}
      <div className="sv-table-columns">
        {visibleCols.map((col) => {
          const isLinked = linkedColumns.has(col.column_name);
          const isKey = isKeyColumn(col.column_name);
          const handleId = `${tableName}.${col.column_name}`;
          const isHighlighted = highlightedColumns.has(handleId);
          const keyLabel = col.column_name.toLowerCase() === 'id' ? 'PK' : isKey ? 'FK' : null;

          return (
            <div
              key={col.column_name}
              className={
                'sv-col-row' +
                (isHighlighted ? ' sv-col-row--highlighted' : '') +
                (isLinked ? ' sv-col-row--linked' : '')
              }
            >
              {/* Left-side handles (for edges coming from the left) */}
              <Handle
                type="target"
                position={Position.Left}
                id={`${handleId}:left`}
                className={'sv-handle sv-handle--target' + (isLinked ? ' sv-handle--linked' : '')}
                style={{ top: '50%' }}
              />
              <Handle
                type="source"
                position={Position.Left}
                id={`${handleId}:left-src`}
                className={'sv-handle sv-handle--target sv-handle--stacked' + (isLinked ? ' sv-handle--linked' : '')}
                style={{ top: '50%' }}
              />

              {keyLabel && <span className="sv-col-key-badge">{keyLabel}</span>}
              <span className="sv-col-name" title={col.column_name}>{col.column_name}</span>
              <span className="sv-col-type">{col.data_type}</span>

              {/* Right-side handles (for edges going to the right) */}
              <Handle
                type="source"
                position={Position.Right}
                id={`${handleId}:right`}
                className={'sv-handle sv-handle--source' + (isLinked ? ' sv-handle--linked' : '')}
                style={{ top: '50%' }}
              />
              <Handle
                type="target"
                position={Position.Right}
                id={`${handleId}:right-tgt`}
                className={'sv-handle sv-handle--source sv-handle--stacked' + (isLinked ? ' sv-handle--linked' : '')}
                style={{ top: '50%' }}
              />
            </div>
          );
        })}
      </div>

      {/* ── Expand / collapse footer ── */}
      {hiddenCount > 0 && (
        <button type="button" className="sv-table-expand-btn" onClick={toggleExpand}>
          {expanded
            ? `Hide ${hiddenCount} column${hiddenCount !== 1 ? 's' : ''}`
            : `+ ${hiddenCount} more column${hiddenCount !== 1 ? 's' : ''}`}
        </button>
      )}

      {/* ── Drag hint (shown when no columns are linked yet) ── */}
      {linkedCols.length === 0 && columns.length > 0 && !expanded && (
        <div className="sv-drag-hint">Drag a column handle to connect</div>
      )}
    </div>
  );
}

export default memo(TableNode);
