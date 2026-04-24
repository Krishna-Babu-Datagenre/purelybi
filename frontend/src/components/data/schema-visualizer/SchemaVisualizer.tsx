import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  type Connection,
  Panel,
  useReactFlow,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from '@dagrejs/dagre';
import { Eye, Pencil, X } from 'lucide-react';

import TableNode from './TableNode';
import RelationshipEdge from './RelationshipEdge';
import AddRelationshipPanel from './AddRelationshipPanel';
import type {
  SchemaVisualizerProps,
  TableNodeData,
  RelationshipEdgeData,
} from './SchemaVisualizer.types';
import type { Relationship, RelationshipKind } from '../../../types/metadata';

/* ── Custom node / edge types ── */
const nodeTypes = { table: TableNode };
const edgeTypes = { relationship: RelationshipEdge };

/* ─────────────────────────────────────────────
   Derive which tables have at least one
   relationship (so unconnected tables are
   excluded from the canvas).
───────────────────────────────────────────── */
function getConnectedTableNames(relationships: Relationship[]): Set<string> {
  const s = new Set<string>();
  for (const r of relationships) {
    s.add(r.from_table);
    s.add(r.to_table);
  }
  return s;
}

/* ─────────────────────────────────────────────
   Build per-table sets of linked column names
───────────────────────────────────────────── */
function getLinkedColumnsMap(relationships: Relationship[]): Map<string, Set<string>> {
  const map = new Map<string, Set<string>>();
  for (const r of relationships) {
    if (!map.has(r.from_table)) map.set(r.from_table, new Set());
    if (!map.has(r.to_table)) map.set(r.to_table, new Set());
    map.get(r.from_table)!.add(r.from_column);
    map.get(r.to_table)!.add(r.to_column);
  }
  return map;
}

/* ─────────────────────────────────────────────
   Layout via dagre. Produces proper layered
   left→right placement with node ordering that
   minimizes edge crossings, and crucially
   gives enough spacing between ranks/rows so
   orthogonal edges don't need to cut through
   other nodes.

   Also returns dagre's computed edge waypoints
   so edges can route AROUND obstacle nodes
   (smoothstep alone cannot avoid nodes).
───────────────────────────────────────────── */
interface DagreLayout {
  positions: Map<string, { x: number; y: number }>;
  /** Map of edge-id → waypoints (in top-left coordinate space) */
  edgePoints: Map<string, { x: number; y: number }[]>;
}

function computeDagreLayout(
  tableNames: string[],
  relationships: Relationship[],
  _tables: SchemaVisualizerProps['tables'],
  columns: SchemaVisualizerProps['columns'],
): DagreLayout {
  const g = new dagre.graphlib.Graph({ multigraph: true });
  g.setGraph({
    rankdir: 'LR',
    align: 'UL',
    ranksep: 220, // horizontal gap between ranks (wide — room for edge bends)
    nodesep: 100, // vertical gap between siblings at the same rank
    edgesep: 40,
    marginx: 40,
    marginy: 40,
  });
  g.setDefaultEdgeLabel(() => ({}));

  const NODE_WIDTH = 280;
  const HEADER_H = 36;
  const ROW_H = 28;
  const FOOTER_H = 28;

  const nodeSizes = new Map<string, { width: number; height: number }>();
  for (const name of tableNames) {
    const tableColCount = columns.filter((c) => c.table_name === name).length;
    const linkedCount = columns.filter((c) => {
      if (c.table_name !== name) return false;
      const isKey =
        c.column_name.toLowerCase() === 'id' ||
        c.column_name.toLowerCase().endsWith('_id');
      return isKey;
    }).length;
    const visibleRows = Math.max(1, linkedCount);
    const height = HEADER_H + visibleRows * ROW_H + (tableColCount > linkedCount ? FOOTER_H : 0);
    nodeSizes.set(name, { width: NODE_WIDTH, height });
    g.setNode(name, { width: NODE_WIDTH, height });
  }

  // Use per-edge ID as the dagre edge "name" (4th arg) so parallel edges
  // between the same table pair each get their own routing.
  for (const r of relationships) {
    if (!tableNames.includes(r.from_table) || !tableNames.includes(r.to_table)) continue;
    const edgeId = `${r.from_table}.${r.from_column}__${r.to_table}.${r.to_column}`;
    g.setEdge(r.from_table, r.to_table, {}, edgeId);
  }

  dagre.layout(g);

  const positions = new Map<string, { x: number; y: number }>();
  for (const name of tableNames) {
    const node = g.node(name);
    if (!node) continue;
    positions.set(name, {
      x: node.x - node.width / 2,
      y: node.y - node.height / 2,
    });
  }

  // Extract waypoints for each edge. Dagre points are in center-coordinate
  // space and include 2–N points. We pass them through untouched; the edge
  // component will replace the first/last with React Flow's actual
  // handle-anchored (sourceX, sourceY) / (targetX, targetY).
  const edgePoints = new Map<string, { x: number; y: number }[]>();
  for (const e of g.edges()) {
    const edgeId = e.name ?? `${e.v}__${e.w}`;
    const dagreEdge = g.edge(e);
    if (dagreEdge?.points) {
      edgePoints.set(edgeId, dagreEdge.points.map((p: { x: number; y: number }) => ({ x: p.x, y: p.y })));
    }
  }

  return { positions, edgePoints };
}

/* ─────────────────────────────────────────────
   Build React Flow nodes
───────────────────────────────────────────── */
function layoutNodes(
  tables: SchemaVisualizerProps['tables'],
  columns: SchemaVisualizerProps['columns'],
  relationships: Relationship[],
  highlightedHandles: Set<string>,
  focusedTable: string | null,
  isEditMode: boolean,
  positions: Map<string, { x: number; y: number }>,
): Node[] {
  const connected = getConnectedTableNames(relationships);
  const linkedMap = getLinkedColumnsMap(relationships);

  // Determine which tables are visible in focus mode
  const focusedSet = new Set<string>();
  if (focusedTable) {
    focusedSet.add(focusedTable);
    for (const r of relationships) {
      if (r.from_table === focusedTable) focusedSet.add(r.to_table);
      if (r.to_table === focusedTable) focusedSet.add(r.from_table);
    }
  }

  return tables
    .filter((t) => connected.has(t.table_name))
    .map((table) => {
      const tableCols = columns.filter((c) => c.table_name === table.table_name);
      const linkedCols = linkedMap.get(table.table_name) ?? new Set<string>();
      const pos = positions.get(table.table_name) ?? { x: 0, y: 0 };
      const isDimmed = focusedTable !== null && !focusedSet.has(table.table_name);

      const data: TableNodeData = {
        tableName: table.table_name,
        columns: tableCols,
        description: table.description,
        linkedColumns: linkedCols,
        highlightedColumns: highlightedHandles,
        isEditMode,
        dimmed: isDimmed,
      };

      return {
        id: table.table_name,
        type: 'table' as const,
        position: pos,
        data,
        style: isDimmed
          ? { opacity: 0.15, pointerEvents: 'none' as const, transition: 'opacity 0.3s' }
          : { opacity: 1, transition: 'opacity 0.3s' },
      };
    });
}

/* ─────────────────────────────────────────────
   Build React Flow edges.
   Spreads parallel edges (same table pair) by
   giving them different step offsets so they
   don't stack on top of each other.

   Also chooses the correct handle side (left vs
   right) per edge based on the relative x-
   positions of the two tables, so edges don't
   cross through the source/target table body.
───────────────────────────────────────────── */
function buildEdges(
  relationships: Relationship[],
  focusedTable: string | null,
  isEditMode: boolean,
  positions: Map<string, { x: number; y: number }>,
  edgePoints: Map<string, { x: number; y: number }[]>,
): Edge[] {
  // Track how many parallel edges we've already built for each undirected
  // table pair, so each new edge gets a larger step offset.
  const pairIndex = new Map<string, number>();

  return relationships.map((r) => {
    const edgeId = `${r.from_table}.${r.from_column}__${r.to_table}.${r.to_column}`;
    const pairKey = [r.from_table, r.to_table].sort().join('||');
    const idx = pairIndex.get(pairKey) ?? 0;
    pairIndex.set(pairKey, idx + 1);

    // Spread parallel edges apart using STEPPED POSITIVE offsets.
    const edgeOffset = 30 + idx * 45;

    // Stagger label Y so parallel-edge labels don't pile on top of each other.
    // Alternating up/down around the midpoint.
    const sign = idx % 2 === 0 ? 1 : -1;
    const magnitude = Math.ceil(idx / 2) * 34;
    const labelYOffset = sign * magnitude;

    const isDimmed =
      focusedTable !== null &&
      r.from_table !== focusedTable &&
      r.to_table !== focusedTable;

    // Pick handle side based on layout x-position so edges don't enter
    // from the back of the source / front of the target. If target is
    // right of source, use source:right → target:left.
    const srcPos = positions.get(r.from_table);
    const tgtPos = positions.get(r.to_table);
    const targetIsRight = (tgtPos?.x ?? 0) >= (srcPos?.x ?? 0);
    const sourceHandle = targetIsRight
      ? `${r.from_table}.${r.from_column}:right`
      : `${r.from_table}.${r.from_column}:left-src`;
    const targetHandle = targetIsRight
      ? `${r.to_table}.${r.to_column}:left`
      : `${r.to_table}.${r.to_column}:right-tgt`;

    const waypoints = edgePoints.get(edgeId);

    // eslint-disable-next-line no-console
    console.debug(
      `[SchemaViz] edge ${r.from_table}.${r.from_column} → ${r.to_table}.${r.to_column} ` +
        `| src.x=${srcPos?.x} tgt.x=${tgtPos?.x} | targetIsRight=${targetIsRight} ` +
        `| waypoints=${waypoints?.length ?? 0}`,
    );

    const data: RelationshipEdgeData = {
      relationship: r,
      fromColumn: r.from_column,
      toColumn: r.to_column,
      kind: r.kind,
      confidence: r.confidence,
      editedByUser: r.edited_by_user,
      dimmed: isDimmed,
      isEditMode,
      edgeOffset,
      waypoints,
      labelYOffset,
    };

    return {
      id: edgeId,
      source: r.from_table,
      target: r.to_table,
      sourceHandle,
      targetHandle,
      type: 'relationship',
      data,
      animated: false,
      zIndex: isDimmed ? 10 : 100,
    };
  });
}

/* ─────────────────────────────────────────────
   SVG defs injected into the React Flow SVG
   for the directional arrow marker
───────────────────────────────────────────── */
function SvgDefs() {
  return (
    <svg style={{ position: 'absolute', width: 0, height: 0 }}>
      <defs>
        <marker
          id="sv-arrow"
          markerWidth="8"
          markerHeight="8"
          refX="6"
          refY="3"
          orient="auto"
        >
          <path d="M0,0 L0,6 L8,3 z" fill="rgba(139,92,246,0.5)" />
        </marker>
        <marker
          id="sv-arrow-active"
          markerWidth="8"
          markerHeight="8"
          refX="6"
          refY="3"
          orient="auto"
        >
          <path d="M0,0 L0,6 L8,3 z" fill="#8B5CF6" />
        </marker>
      </defs>
    </svg>
  );
}

/* ─────────────────────────────────────────────
   SchemaVisualizer (inner — needs ReactFlowProvider)
───────────────────────────────────────────── */
function SchemaVisualizerInner({
  tables,
  columns,
  relationships,
  onCreateRelationship,
  onDeleteRelationship,
  onUpdateRelationshipKind,
}: SchemaVisualizerProps) {
  const [focusedTable, setFocusedTable] = useState<string | null>(null);
  const [isEditMode, setIsEditMode] = useState(false);

  // Stable positions — only recomputed when schema data changes, not on UI state changes
  const { nodePositions, edgePoints } = useMemo(() => {
    const connected = getConnectedTableNames(relationships);
    const visibleNames = tables
      .filter((t) => connected.has(t.table_name))
      .map((t) => t.table_name);
    const layout = computeDagreLayout(visibleNames, relationships, tables, columns);
    // eslint-disable-next-line no-console
    console.debug('[SchemaViz] dagre layout positions', Object.fromEntries(layout.positions));
    // eslint-disable-next-line no-console
    console.debug('[SchemaViz] dagre edge waypoints', Object.fromEntries(layout.edgePoints));
    return { nodePositions: layout.positions, edgePoints: layout.edgePoints };
  }, [tables, columns, relationships]);

  const buildNodesCallback = useCallback(
    () =>
      layoutNodes(
        tables,
        columns,
        relationships,
        new Set<string>(),
        focusedTable,
        isEditMode,
        nodePositions,
      ),
    [tables, columns, relationships, focusedTable, isEditMode, nodePositions],
  );

  const buildEdgesCallback = useCallback(
    () => buildEdges(relationships, focusedTable, isEditMode, nodePositions, edgePoints),
    [relationships, focusedTable, isEditMode, nodePositions, edgePoints],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState(buildNodesCallback());
  const [edges, setEdges, onEdgesChange] = useEdgesState(buildEdgesCallback());

  const { fitView } = useReactFlow();
  const initialFit = useRef(false);

  useEffect(() => {
    setNodes(buildNodesCallback());
  }, [buildNodesCallback, setNodes]);

  useEffect(() => {
    setEdges(buildEdgesCallback());
    if (!initialFit.current) {
      initialFit.current = true;
      requestAnimationFrame(() => fitView({ padding: 0.3 }));
    }
  }, [buildEdgesCallback, setEdges, fitView]);

  /* ── Update kind ── */
  useEffect(() => {
    const handler = (e: Event) => {
      const { relationship, newKind } = (
        e as CustomEvent<{ relationship: Relationship; newKind: RelationshipKind }>
      ).detail;
      onUpdateRelationshipKind(relationship, newKind);
    };
    window.addEventListener('sv-update-kind', handler);
    return () => window.removeEventListener('sv-update-kind', handler);
  }, [onUpdateRelationshipKind]);

  /* ── Delete relationship ── */
  useEffect(() => {
    const handler = (e: Event) => {
      const { relationship } = (e as CustomEvent<{ relationship: Relationship }>).detail;
      onDeleteRelationship(relationship);
    };
    window.addEventListener('sv-delete-relationship', handler);
    return () => window.removeEventListener('sv-delete-relationship', handler);
  }, [onDeleteRelationship]);

  /* ── Drag-to-connect (Edit Mode only) ──
     Handle IDs are formatted "table.column:side" (e.g. ":right",
     ":left", ":left-src", ":right-tgt"). Strip the side suffix to
     recover the real column name. */
  const onConnect = useCallback(
    async (connection: Connection) => {
      if (!connection.sourceHandle || !connection.targetHandle) return;
      const stripSide = (h: string) => h.split(':')[0];
      const src = stripSide(connection.sourceHandle);
      const tgt = stripSide(connection.targetHandle);
      const dotS = src.indexOf('.');
      const dotT = tgt.indexOf('.');
      if (dotS === -1 || dotT === -1) return;
      const fromTable = src.slice(0, dotS);
      const fromColumn = src.slice(dotS + 1);
      const toTable = tgt.slice(0, dotT);
      const toColumn = tgt.slice(dotT + 1);
      if (!fromTable || !fromColumn || !toTable || !toColumn) return;
      await onCreateRelationship({
        from_table: fromTable,
        from_column: fromColumn,
        to_table: toTable,
        to_column: toColumn,
        kind: 'many_to_one',
      });
    },
    [onCreateRelationship],
  );

  /* ── Node click → focus mode (view mode only) ── */
  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      if (isEditMode) return;
      setFocusedTable((prev) => (prev === node.id ? null : node.id));
    },
    [isEditMode],
  );

  /* ── Pane click → clear focus ── */
  const onPaneClick = useCallback(() => {
    setFocusedTable(null);
  }, []);

  const connectedCount = getConnectedTableNames(relationships).size;
  const hiddenCount = tables.length - connectedCount;

  return (
    <div className="sv-container">
      <SvgDefs />
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{ padding: 0.3 }}
        proOptions={{ hideAttribution: true }}
        minZoom={0.1}
        maxZoom={2}
        defaultEdgeOptions={{ animated: false, zIndex: 100 }}
        elevateEdgesOnSelect
        snapToGrid
        snapGrid={[16, 16]}
        connectionLineStyle={{ stroke: '#8B5CF6', strokeWidth: 2 }}
        connectionLineType={'smoothstep' as never}
        nodesDraggable
        nodesConnectable={isEditMode}
        elementsSelectable
      >
        <Background
          color="rgba(139, 92, 246, 0.07)"
          variant={BackgroundVariant.Dots}
          gap={24}
          size={1.5}
        />
        <Controls showInteractive={false} className="sv-controls" />
        <MiniMap
          nodeColor="#8B5CF6"
          maskColor="rgba(10, 10, 15, 0.80)"
          className="sv-minimap"
        />

        {/* Top-left: Edit Mode toggle + Clear Focus */}
        <Panel position="top-left" className="sv-panel-tl">
          <button
            type="button"
            className={`sv-mode-btn${isEditMode ? ' sv-mode-btn--active' : ''}`}
            onClick={() => {
              setIsEditMode((v) => !v);
              setFocusedTable(null);
            }}
          >
            {isEditMode ? <Eye size={13} /> : <Pencil size={13} />}
            {isEditMode ? 'Exit Edit Mode' : 'Edit Mode'}
          </button>
          {focusedTable && (
            <button
              type="button"
              className="sv-clear-focus-btn"
              onClick={() => setFocusedTable(null)}
            >
              <X size={11} />
              Clear Focus
            </button>
          )}
        </Panel>

        {/* Top-right: Add Relationship */}
        <Panel position="top-right" className="sv-panel-tr">
          <AddRelationshipPanel
            tables={tables}
            columns={columns}
            onAdd={onCreateRelationship}
          />
        </Panel>

        {/* Bottom-left: stats + contextual hints */}
        <Panel position="bottom-left" className="sv-panel-bl">
          <span className="sv-stat">{connectedCount} tables</span>
          <span className="sv-stat-sep">·</span>
          <span className="sv-stat">{relationships.length} relationships</span>
          {hiddenCount > 0 && (
            <>
              <span className="sv-stat-sep">·</span>
              <span className="sv-stat sv-stat--muted">{hiddenCount} unrelated tables hidden</span>
            </>
          )}
          {!focusedTable && !isEditMode && relationships.length > 0 && (
            <>
              <span className="sv-stat-sep">·</span>
              <span className="sv-stat sv-stat--hint">Click a table to focus</span>
            </>
          )}
          {focusedTable && (
            <>
              <span className="sv-stat-sep">·</span>
              <span className="sv-stat sv-stat--focus">Focused: {focusedTable}</span>
            </>
          )}
          {isEditMode && (
            <>
              <span className="sv-stat-sep">·</span>
              <span className="sv-stat sv-stat--edit">Edit Mode — drag handles to connect</span>
            </>
          )}
        </Panel>

        {/* Hint banner (only when no relationships) */}
        {relationships.length === 0 && (
          <Panel position="top-center">
            <div className="sv-empty-hint">
              No relationships yet — enable <strong>Edit Mode</strong> to drag column handles, or use <strong>Add Relationship</strong>.
            </div>
          </Panel>
        )}
      </ReactFlow>
    </div>
  );
}

export default function SchemaVisualizer(props: SchemaVisualizerProps) {
  return (
    <ReactFlowProvider>
      <SchemaVisualizerInner {...props} />
    </ReactFlowProvider>
  );
}
