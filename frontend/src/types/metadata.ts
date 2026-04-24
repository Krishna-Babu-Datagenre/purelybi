/* ─────────────────────────────────────────────
   Metadata Types (from backend /api/metadata)
───────────────────────────────────────────── */

export type SemanticType =
  | 'categorical'
  | 'numeric'
  | 'temporal'
  | 'identifier'
  | 'measure'
  | 'unknown';

export type RelationshipKind = 'many_to_one' | 'one_to_one' | 'many_to_many';

export type MetadataJobStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';

export interface TableMetadata {
  user_id: string;
  table_name: string;
  description: string | null;
  primary_date_column: string | null;
  grain: string | null;
  generated_at: string | null;
  edited_by_user: boolean;
  created_at: string | null;
  updated_at: string | null;
}

export interface TableMetadataPatch {
  description?: string;
  primary_date_column?: string;
  grain?: string;
}

export interface ColumnMetadata {
  user_id: string;
  table_name: string;
  column_name: string;
  data_type: string;
  semantic_type: SemanticType;
  description: string | null;
  is_filterable: boolean;
  cardinality: number | null;
  sample_values: unknown[] | null;
  generated_at: string | null;
  edited_by_user: boolean;
  created_at: string | null;
  updated_at: string | null;
}

export interface ColumnMetadataPatch {
  semantic_type?: SemanticType;
  description?: string;
  is_filterable?: boolean;
}

export interface Relationship {
  user_id: string;
  from_table: string;
  from_column: string;
  to_table: string;
  to_column: string;
  kind: RelationshipKind;
  confidence: number | null;
  edited_by_user: boolean;
  generated_at: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface RelationshipCreate {
  from_table: string;
  from_column: string;
  to_table: string;
  to_column: string;
  kind: RelationshipKind;
  confidence?: number;
}

export interface RelationshipPatch {
  kind?: RelationshipKind;
}

export interface MetadataJob {
  id: string;
  user_id: string;
  status: MetadataJobStatus;
  progress: number;
  message: string | null;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
  aca_execution_name: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface MetadataGenerationResponse {
  job: MetadataJob;
}

export interface ColumnValuesResponse {
  table: string;
  column: string;
  values: (string | number | boolean)[];
}

/* ─────────────────────────────────────────────
   FilterSpec types (mirrors backend models/filters.py)
───────────────────────────────────────────── */

export interface ColumnRef {
  table: string;
  column: string;
}

export type TimePreset =
  | 'last_7_days'
  | 'last_14_days'
  | 'last_30_days'
  | 'last_60_days'
  | 'last_90_days'
  | 'ytd'
  | 'mtd';

export interface TimeRange {
  from: string; // ISO date
  to: string;   // ISO date
}

export interface TimeFilter {
  column_ref: ColumnRef;
  preset?: TimePreset;
  range?: TimeRange;
}

export interface CategoricalFilter {
  kind: 'categorical';
  column_ref: ColumnRef;
  op: 'in' | 'not_in';
  values: (string | number | boolean)[];
}

export interface NumericFilter {
  kind: 'numeric';
  column_ref: ColumnRef;
  op: 'between';
  min?: number;
  max?: number;
}

export type Filter = CategoricalFilter | NumericFilter;

export interface FilterSpec {
  time?: TimeFilter;
  filters: Filter[];
}
