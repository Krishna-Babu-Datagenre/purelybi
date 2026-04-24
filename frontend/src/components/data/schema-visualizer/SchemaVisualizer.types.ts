import type { RelationshipKind, Relationship, TableMetadata, ColumnMetadata } from '../../../types/metadata';

/* ─────────────────────────────────────────────
   Schema Visualizer Types
───────────────────────────────────────────── */

/** Data shape passed into a TableNode */
export interface TableNodeData {
  tableName: string;
  columns: ColumnMetadata[];
  description: string | null;
  /** Set of column names that are endpoints of at least one relationship */
  linkedColumns: Set<string>;
  /** Column names that are highlighted (because an edge is hovered) */
  highlightedColumns: Set<string>;
  /** Whether Edit Mode is active (shows handles, enables connections) */
  isEditMode: boolean;
  /** Whether this node should be dimmed (focus mode — not connected to focused table) */
  dimmed: boolean;
  [key: string]: unknown;
}

/** Relationship edge metadata stored on each React Flow edge */
export interface RelationshipEdgeData {
  relationship: Relationship;
  fromColumn: string;
  toColumn: string;
  kind: RelationshipKind;
  confidence: number | null;
  editedByUser: boolean;
  /** Whether this edge should be dimmed (focus mode) */
  dimmed: boolean;
  /** Whether Edit Mode is active */
  isEditMode: boolean;
  /** Step offset for the smoothstep path (spreads parallel edges apart) */
  edgeOffset: number;
  /** Waypoints computed by dagre to route edge around obstacle nodes */
  waypoints?: { x: number; y: number }[];
  /** Extra Y-offset for the edge label to prevent parallel-edge label pile-up */
  labelYOffset?: number;
  [key: string]: unknown;
}

/** Props for the SchemaVisualizer wrapper */
export interface SchemaVisualizerProps {
  tables: TableMetadata[];
  columns: ColumnMetadata[];
  relationships: Relationship[];
  onCreateRelationship: (rel: {
    from_table: string;
    from_column: string;
    to_table: string;
    to_column: string;
    kind: RelationshipKind;
  }) => Promise<void>;
  onDeleteRelationship: (rel: Relationship) => Promise<void>;
  onUpdateRelationshipKind: (
    rel: Relationship,
    newKind: RelationshipKind,
  ) => Promise<void>;
}
