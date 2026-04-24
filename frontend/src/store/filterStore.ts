import { create } from 'zustand';
import type {
  TableMetadata,
  ColumnMetadata,
  FilterSpec,
  Filter,
  TimeFilter,
} from '../types/metadata';
import type { SyncedTableInfo } from '../types/index';
import {
  listTableMetadata,
  listColumnMetadata,
  getColumnValues,
} from '../services/metadataApi';
import { listSyncedTablesMetadata } from '../services/backendClient';

/* ─────────────────────────────────────────────
   Filter Store
   ─────────────────────────────────────────────
   Holds the native FilterSpec per dashboard and
   the metadata needed to populate filter dropdowns.
───────────────────────────────────────────── */

interface FilterState {
  /* ── Metadata cache ── */
  tables: TableMetadata[];
  columns: ColumnMetadata[];
  sources: SyncedTableInfo[];
  metadataLoading: boolean;
  metadataLoaded: boolean;
  metadataError: string | null;

  /* ── Distinct values cache: key = "table.column" ── */
  columnValues: Record<string, (string | number | boolean)[]>;
  columnValuesLoading: Record<string, boolean>;

  /* ── Active filter spec ── */
  filterSpec: FilterSpec;

  /* ── Source filter ── */
  selectedSource: string | null;

  /* ── Pane UI state ── */
  paneOpen: boolean;

  /* ── Actions ── */
  fetchMetadata: () => Promise<void>;
  fetchColumnValues: (table: string, column: string) => Promise<void>;

  setSelectedSource: (sourceId: string | null) => void;
  setTimeFilter: (time: TimeFilter | undefined) => void;
  addFilter: (filter: Filter) => void;
  updateFilter: (index: number, filter: Filter) => void;
  removeFilter: (index: number) => void;
  clearAllFilters: () => void;

  togglePane: () => void;
  setPaneOpen: (open: boolean) => void;

  /* ── Derived helpers ── */
  getSourceTableNames: () => Set<string> | null;
  getFilterableColumns: () => ColumnMetadata[];
  getTemporalColumns: () => ColumnMetadata[];
  getCategoricalColumns: () => ColumnMetadata[];
  getNumericColumns: () => ColumnMetadata[];
  buildFilterSpec: () => FilterSpec | undefined;
}

const EMPTY_FILTER_SPEC: FilterSpec = { filters: [] };

const FILTERABLE_SEMANTIC_TYPES = new Set([
  'categorical',
  'numeric',
  'temporal',
  'measure',
]);

export const useFilterStore = create<FilterState>((set, get) => ({
  tables: [],
  columns: [],
  sources: [],
  metadataLoading: false,
  metadataLoaded: false,
  metadataError: null,

  columnValues: {},
  columnValuesLoading: {},

  filterSpec: { ...EMPTY_FILTER_SPEC },

  selectedSource: null,

  paneOpen: false,

  /* ── Fetch metadata ── */
  fetchMetadata: async () => {
    if (get().metadataLoading) return;
    set({ metadataLoading: true, metadataError: null });
    try {
      const [tables, columns, sources] = await Promise.all([
        listTableMetadata(),
        listColumnMetadata(),
        listSyncedTablesMetadata().catch(() => [] as SyncedTableInfo[]),
      ]);
      set({ tables, columns, sources, metadataLoading: false, metadataLoaded: true });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      set({ metadataError: msg, metadataLoading: false });
    }
  },

  /* ── Fetch distinct values for a specific column ── */
  fetchColumnValues: async (table, column) => {
    const key = `${table}.${column}`;
    if (get().columnValues[key] || get().columnValuesLoading[key]) return;
    set((s) => ({
      columnValuesLoading: { ...s.columnValuesLoading, [key]: true },
    }));
    try {
      const res = await getColumnValues(table, column);
      set((s) => ({
        columnValues: { ...s.columnValues, [key]: res.values },
        columnValuesLoading: { ...s.columnValuesLoading, [key]: false },
      }));
    } catch {
      set((s) => ({
        columnValuesLoading: { ...s.columnValuesLoading, [key]: false },
      }));
    }
  },

  /* ── Source filter ── */
  setSelectedSource: (sourceId) => {
    set({ selectedSource: sourceId });
  },

  /* ── Time filter ── */
  setTimeFilter: (time) => {
    set((s) => ({ filterSpec: { ...s.filterSpec, time } }));
  },

  /* ── Attribute filters ── */
  addFilter: (filter) => {
    set((s) => ({
      filterSpec: { ...s.filterSpec, filters: [...s.filterSpec.filters, filter] },
    }));
  },

  updateFilter: (index, filter) => {
    set((s) => {
      const filters = [...s.filterSpec.filters];
      filters[index] = filter;
      return { filterSpec: { ...s.filterSpec, filters } };
    });
  },

  removeFilter: (index) => {
    set((s) => {
      const filters = s.filterSpec.filters.filter((_, i) => i !== index);
      return { filterSpec: { ...s.filterSpec, filters } };
    });
  },

  clearAllFilters: () => {
    set({ filterSpec: { ...EMPTY_FILTER_SPEC }, selectedSource: null });
  },

  /* ── Pane toggle ── */
  togglePane: () => set((s) => ({ paneOpen: !s.paneOpen })),
  setPaneOpen: (open) => set({ paneOpen: open }),

  /* ── Derived helpers (selectors) ── */
  getSourceTableNames: () => {
    const { selectedSource, sources, tables } = get();
    if (!selectedSource) return null;
    const src = sources.find((s) => s.connector_config_id === selectedSource);
    if (!src) return null;

    // Derive the connector folder prefix used in DuckDB view names.
    // docker_repository e.g. "airbyte/source-mongodb-v2" → last segment "source-mongodb-v2"
    // DuckDB view names replace non-alphanumeric chars with "_": "source_mongodb_v2"
    const lastSegment = (src.docker_repository || '').split('/').pop() || '';
    const folderPrefix = lastSegment.replace(/[^a-z0-9]/gi, '_').toLowerCase();

    // Match metadata table names that belong to this connector.
    // Convention: table_name = "<folder_prefix>_<stream_name>" (with stream also normalised)
    const streamSet = new Set(src.synced_tables.map((s) => s.toLowerCase()));
    const matched = new Set<string>();
    for (const t of tables) {
      const tn = t.table_name.toLowerCase();
      // Check if the table belongs to this connector folder
      if (folderPrefix && tn.startsWith(folderPrefix + '_')) {
        const streamPart = tn.slice(folderPrefix.length + 1);
        if (streamSet.has(streamPart)) {
          matched.add(t.table_name);
        }
      }
    }

    // Fallback: if no matches found via prefix, try direct stream name match
    if (matched.size === 0) {
      for (const t of tables) {
        if (streamSet.has(t.table_name.toLowerCase())) {
          matched.add(t.table_name);
        }
      }
    }

    return matched.size > 0 ? matched : null;
  },

  getFilterableColumns: () => {
    return get().columns.filter(
      (c) => c.is_filterable && FILTERABLE_SEMANTIC_TYPES.has(c.semantic_type),
    );
  },

  getTemporalColumns: () => {
    return get().columns.filter(
      (c) => c.is_filterable && c.semantic_type === 'temporal',
    );
  },

  getCategoricalColumns: () => {
    return get().columns.filter(
      (c) => c.is_filterable && c.semantic_type === 'categorical',
    );
  },

  getNumericColumns: () => {
    return get().columns.filter(
      (c) =>
        c.is_filterable &&
        (c.semantic_type === 'numeric' || c.semantic_type === 'measure'),
    );
  },

  buildFilterSpec: () => {
    const spec = get().filterSpec;
    if (!spec.time && spec.filters.length === 0) return undefined;
    return spec;
  },
}));
