import { create } from 'zustand';
import type {
  TableMetadata,
  ColumnMetadata,
  FilterSpec,
  Filter,
  TimeFilter,
  ColumnRef,
  TimePreset,
  CategoricalFilter,
  NumericFilter,
} from '../types/metadata';
import {
  listTableMetadata,
  listColumnMetadata,
  getColumnValues,
} from '../services/metadataApi';

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
  metadataLoading: boolean;
  metadataLoaded: boolean;
  metadataError: string | null;

  /* ── Distinct values cache: key = "table.column" ── */
  columnValues: Record<string, (string | number | boolean)[]>;
  columnValuesLoading: Record<string, boolean>;

  /* ── Active filter spec ── */
  filterSpec: FilterSpec;

  /* ── Pane UI state ── */
  paneOpen: boolean;

  /* ── Actions ── */
  fetchMetadata: () => Promise<void>;
  fetchColumnValues: (table: string, column: string) => Promise<void>;

  setTimeFilter: (time: TimeFilter | undefined) => void;
  addFilter: (filter: Filter) => void;
  updateFilter: (index: number, filter: Filter) => void;
  removeFilter: (index: number) => void;
  clearAllFilters: () => void;

  togglePane: () => void;
  setPaneOpen: (open: boolean) => void;

  /* ── Derived helpers ── */
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
  metadataLoading: false,
  metadataLoaded: false,
  metadataError: null,

  columnValues: {},
  columnValuesLoading: {},

  filterSpec: { ...EMPTY_FILTER_SPEC },

  paneOpen: false,

  /* ── Fetch metadata ── */
  fetchMetadata: async () => {
    if (get().metadataLoading) return;
    set({ metadataLoading: true, metadataError: null });
    try {
      const [tables, columns] = await Promise.all([
        listTableMetadata(),
        listColumnMetadata(),
      ]);
      set({ tables, columns, metadataLoading: false, metadataLoaded: true });
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
    set({ filterSpec: { ...EMPTY_FILTER_SPEC } });
  },

  /* ── Pane toggle ── */
  togglePane: () => set((s) => ({ paneOpen: !s.paneOpen })),
  setPaneOpen: (open) => set({ paneOpen: open }),

  /* ── Derived helpers (selectors) ── */
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
