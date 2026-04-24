import { memo, useCallback, useState, useRef, useEffect } from 'react';
import {
  BaseEdge,
  EdgeLabelRenderer,
  getSmoothStepPath,
  type EdgeProps,
} from '@xyflow/react';
import { Trash2, Pencil } from 'lucide-react';
import type { RelationshipEdgeData } from './SchemaVisualizer.types';
import type { RelationshipKind } from '../../../types/metadata';

/* ─────────────────────────────────────────────
   Cardinality label text
───────────────────────────────────────────── */
const KIND_LABELS: Record<RelationshipKind, string> = {
  many_to_one: 'Many → One',
  one_to_one: 'One → One',
  many_to_many: 'Many → Many',
};
const KIND_SHORT: Record<RelationshipKind, string> = {
  many_to_one: 'N : 1',
  one_to_one: '1 : 1',
  many_to_many: 'N : N',
};

const KIND_OPTIONS: RelationshipKind[] = ['many_to_one', 'one_to_one', 'many_to_many'];

/* ─────────────────────────────────────────────
   Build an orthogonal SVG path from a polyline
   of dagre waypoints with rounded corners. The
   dagre waypoints already route around
   obstacle nodes; we just render them nicely.
───────────────────────────────────────────── */
function buildRoundedPath(
  pts: { x: number; y: number }[],
  radius = 14,
): { d: string; midX: number; midY: number } {
  if (pts.length < 2) return { d: '', midX: 0, midY: 0 };
  if (pts.length === 2) {
    const [a, b] = pts;
    return {
      d: `M ${a.x} ${a.y} L ${b.x} ${b.y}`,
      midX: (a.x + b.x) / 2,
      midY: (a.y + b.y) / 2,
    };
  }

  let d = `M ${pts[0].x} ${pts[0].y}`;
  for (let i = 1; i < pts.length - 1; i++) {
    const prev = pts[i - 1];
    const curr = pts[i];
    const next = pts[i + 1];
    const vIn = { x: curr.x - prev.x, y: curr.y - prev.y };
    const vOut = { x: next.x - curr.x, y: next.y - curr.y };
    const lenIn = Math.hypot(vIn.x, vIn.y) || 1;
    const lenOut = Math.hypot(vOut.x, vOut.y) || 1;
    const r = Math.min(radius, lenIn / 2, lenOut / 2);
    const p1 = { x: curr.x - (vIn.x / lenIn) * r, y: curr.y - (vIn.y / lenIn) * r };
    const p2 = { x: curr.x + (vOut.x / lenOut) * r, y: curr.y + (vOut.y / lenOut) * r };
    d += ` L ${p1.x} ${p1.y} Q ${curr.x} ${curr.y} ${p2.x} ${p2.y}`;
  }
  const last = pts[pts.length - 1];
  d += ` L ${last.x} ${last.y}`;

  // Midpoint by arc length along the polyline
  let totalLen = 0;
  const segLens: number[] = [];
  for (let i = 1; i < pts.length; i++) {
    const l = Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y);
    segLens.push(l);
    totalLen += l;
  }
  const half = totalLen / 2;
  let acc = 0;
  let midX = pts[0].x;
  let midY = pts[0].y;
  for (let i = 0; i < segLens.length; i++) {
    if (acc + segLens[i] >= half) {
      const t = (half - acc) / (segLens[i] || 1);
      midX = pts[i].x + (pts[i + 1].x - pts[i].x) * t;
      midY = pts[i].y + (pts[i + 1].y - pts[i].y) * t;
      break;
    }
    acc += segLens[i];
  }

  return { d, midX, midY };
}

/* ─────────────────────────────────────────────
   RelationshipEdge

   Flicker-free design: the label is ALWAYS
   rendered and interactive. No hover state.
   Click to edit kind; React Flow `selected`
   drives the "active" styling.

   Obstacle-avoiding routing: if dagre provided
   waypoints, we render along them so the edge
   bends around intervening nodes.
───────────────────────────────────────────── */
function RelationshipEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
}: EdgeProps & { data: RelationshipEdgeData }) {
  const {
    dimmed,
    edgeOffset,
    relationship,
    kind,
    fromColumn,
    toColumn,
    waypoints,
    labelYOffset = 0,
    isEditMode,
  } = data;

  let pathD: string;
  let labelX: number;
  let labelY: number;

  if (waypoints && waypoints.length >= 2) {
    // Replace dagre's first/last (node-center anchored) with React Flow's
    // actual handle-anchored coords so the edge joins the handle cleanly.
    const mid = waypoints.length > 2 ? waypoints.slice(1, -1) : [];
    const pts = [{ x: sourceX, y: sourceY }, ...mid, { x: targetX, y: targetY }];
    const rounded = buildRoundedPath(pts);
    pathD = rounded.d;
    labelX = rounded.midX;
    labelY = rounded.midY;
  } else {
    const [d, lx, ly] = getSmoothStepPath({
      sourceX,
      sourceY,
      sourcePosition,
      targetX,
      targetY,
      targetPosition,
      borderRadius: 20,
      offset: edgeOffset ?? 30,
    });
    pathD = d;
    labelX = lx;
    labelY = ly;
  }

  const [editing, setEditing] = useState(false);
  const labelRef = useRef<HTMLDivElement>(null);

  const active = !!selected;
  const strokeColor = active ? '#8B5CF6' : 'rgba(139, 92, 246, 0.55)';
  const strokeWidth = active ? 2.5 : 1.5;
  const edgeOpacity = dimmed ? 0.08 : 1;
  const labelOpacity = dimmed ? 0 : 1;

  const handleKindChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const newKind = e.target.value as RelationshipKind;
      setEditing(false);
      window.dispatchEvent(
        new CustomEvent('sv-update-kind', { detail: { relationship, newKind } }),
      );
    },
    [relationship],
  );

  const handleDelete = useCallback(() => {
    window.dispatchEvent(
      new CustomEvent('sv-delete-relationship', { detail: { relationship } }),
    );
  }, [relationship]);

  const startEditing = useCallback(() => {
    if (!isEditMode) return;
    setEditing(true);
  }, [isEditMode]);

  useEffect(() => {
    if (!editing) return;
    const handler = (e: MouseEvent) => {
      if (labelRef.current && !labelRef.current.contains(e.target as Node)) {
        setEditing(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [editing]);

  return (
    <>
      {!dimmed && (
        <path
          d={pathD}
          fill="none"
          stroke="transparent"
          strokeWidth={18}
          style={{ cursor: 'pointer', pointerEvents: 'stroke' }}
        />
      )}

      <BaseEdge
        id={id}
        path={pathD}
        style={{
          stroke: strokeColor,
          strokeWidth,
          transition: 'stroke 0.12s, stroke-width 0.12s, opacity 0.25s',
          opacity: edgeOpacity,
        }}
        markerEnd={active ? 'url(#sv-arrow-active)' : 'url(#sv-arrow)'}
      />

      <EdgeLabelRenderer>
        <div
          ref={labelRef}
          className="sv-edge-label-wrapper"
          style={{
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY + labelYOffset}px)`,
            pointerEvents: dimmed ? 'none' : 'all',
            opacity: labelOpacity,
            zIndex: active ? 1001 : 101,
            transition: 'opacity 0.2s',
          }}
        >
          {editing ? (
            <select
              className="sv-edge-kind-select"
              value={kind}
              onChange={handleKindChange}
              autoFocus
            >
              {KIND_OPTIONS.map((k) => (
                <option key={k} value={k}>{KIND_LABELS[k]}</option>
              ))}
            </select>
          ) : (
            <button
              type="button"
              className={
                'sv-edge-label' +
                (active ? ' sv-edge-label--active' : '') +
                (isEditMode ? '' : ' sv-edge-label--readonly')
              }
              onClick={startEditing}
              title={
                isEditMode
                  ? `${fromColumn} → ${toColumn}\nClick to change cardinality`
                  : `${fromColumn} → ${toColumn}\nEnable Edit Mode to modify`
              }
            >
              <span className="sv-edge-col-hint">
                {fromColumn} → {toColumn}
              </span>
              <span className="sv-edge-kind">{KIND_SHORT[kind]}</span>
              {isEditMode && <Pencil size={9} className="sv-edge-edit-icon" />}
            </button>
          )}

          {isEditMode && active && !editing && (
            <button
              type="button"
              className="sv-edge-delete"
              onClick={handleDelete}
              title="Delete relationship"
            >
              <Trash2 size={12} />
            </button>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

export default memo(RelationshipEdge);
