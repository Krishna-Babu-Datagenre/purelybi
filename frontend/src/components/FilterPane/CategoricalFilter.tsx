import { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { ListFilter, ChevronDown, X, Search } from 'lucide-react';
import { useFilterStore } from '../../store/filterStore';
import type { ColumnMetadata, CategoricalFilter as CatFilterType, ColumnRef } from '../../types/metadata';

/* ─────────────────────────────────────────────
   Categorical Filter
   ─────────────────────────────────────────────
   Table → Column → multi-select of unique values.
   Values fetched on demand via GET /metadata/values.
───────────────────────────────────────────── */

const CategoricalFilter = () => {
  const columns = useFilterStore((s) => s.columns);
  const categoricalCols = useMemo(
    () => columns.filter((c) => c.is_filterable && c.semantic_type === 'categorical'),
    [columns],
  );
  const columnValues = useFilterStore((s) => s.columnValues);
  const columnValuesLoading = useFilterStore((s) => s.columnValuesLoading);
  const fetchColumnValues = useFilterStore((s) => s.fetchColumnValues);
  const filterSpec = useFilterStore((s) => s.filterSpec);
  const addFilter = useFilterStore((s) => s.addFilter);
  const updateFilter = useFilterStore((s) => s.updateFilter);
  const removeFilter = useFilterStore((s) => s.removeFilter);

  const [selectedColKey, setSelectedColKey] = useState<string | null>(null);
  const [colPickerOpen, setColPickerOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedValues, setSelectedValues] = useState<Set<string | number | boolean>>(new Set());
  const searchRef = useRef<HTMLInputElement>(null);

  // Group columns by table for the dropdown
  const columnsByTable = useMemo(() => {
    const map = new Map<string, ColumnMetadata[]>();
    for (const col of categoricalCols) {
      const list = map.get(col.table_name) ?? [];
      list.push(col);
      map.set(col.table_name, list);
    }
    return map;
  }, [categoricalCols]);

  // Get existing categorical filters from spec
  const existingCatFilters = useMemo(
    () =>
      filterSpec.filters
        .map((f, i) => ({ filter: f, index: i }))
        .filter((x): x is { filter: CatFilterType; index: number } => x.filter.kind === 'categorical'),
    [filterSpec.filters],
  );

  // When a column is selected, fetch its values
  const selectedCol = useMemo(() => {
    if (!selectedColKey) return null;
    const [table, ...rest] = selectedColKey.split('.');
    const column = rest.join('.');
    return categoricalCols.find((c) => c.table_name === table && c.column_name === column) ?? null;
  }, [selectedColKey, categoricalCols]);

  useEffect(() => {
    if (selectedCol) {
      fetchColumnValues(selectedCol.table_name, selectedCol.column_name);
    }
  }, [selectedCol, fetchColumnValues]);

  // Focus search when opening value picker
  useEffect(() => {
    if (selectedCol && searchRef.current) {
      searchRef.current.focus();
    }
  }, [selectedCol]);

  const valuesKey = selectedCol ? `${selectedCol.table_name}.${selectedCol.column_name}` : '';
  const availableValues = columnValues[valuesKey] ?? [];
  const isLoading = columnValuesLoading[valuesKey] ?? false;

  const filteredValues = useMemo(() => {
    if (!searchQuery) return availableValues;
    const q = searchQuery.toLowerCase();
    return availableValues.filter((v) => String(v).toLowerCase().includes(q));
  }, [availableValues, searchQuery]);

  const handleSelectColumn = useCallback((table: string, column: string) => {
    setSelectedColKey(`${table}.${column}`);
    setColPickerOpen(false);
    setSearchQuery('');
    setSelectedValues(new Set());
  }, []);

  const toggleValue = useCallback((val: string | number | boolean) => {
    setSelectedValues((prev) => {
      const next = new Set(prev);
      if (next.has(val)) next.delete(val);
      else next.add(val);
      return next;
    });
  }, []);

  const handleApplySelection = useCallback(() => {
    if (!selectedCol || selectedValues.size === 0) return;
    const ref: ColumnRef = { table: selectedCol.table_name, column: selectedCol.column_name };

    // Check if filter for this column already exists
    const existingIdx = existingCatFilters.findIndex(
      (x) =>
        x.filter.column_ref.table === ref.table &&
        x.filter.column_ref.column === ref.column,
    );

    const filter: CatFilterType = {
      kind: 'categorical',
      column_ref: ref,
      op: 'in',
      values: Array.from(selectedValues),
    };

    if (existingIdx >= 0) {
      updateFilter(existingCatFilters[existingIdx].index, filter);
    } else {
      addFilter(filter);
    }

    setSelectedColKey(null);
    setSelectedValues(new Set());
    setSearchQuery('');
  }, [selectedCol, selectedValues, existingCatFilters, addFilter, updateFilter]);

  if (categoricalCols.length === 0) {
    return (
      <div className="filter-section">
        <div className="filter-section__header">
          <ListFilter size={14} className="text-[var(--brand)]" />
          <span className="filter-section__title">Categorical Filters</span>
        </div>
        <p className="filter-empty-hint">No categorical columns available</p>
      </div>
    );
  }

  return (
    <div className="filter-section">
      <div className="filter-section__header">
        <ListFilter size={14} className="text-[var(--brand)]" />
        <span className="filter-section__title">Categorical Filters</span>
      </div>

      {/* Active categorical filter chips */}
      {existingCatFilters.length > 0 && (
        <div className="filter-chips">
          {existingCatFilters.map(({ filter, index }) => (
            <div key={`${filter.column_ref.table}.${filter.column_ref.column}`} className="filter-chip">
              <span className="filter-chip__label">
                {filter.column_ref.column}
              </span>
              <span className="filter-chip__values">
                {filter.values.length <= 3
                  ? filter.values.join(', ')
                  : `${filter.values.slice(0, 2).join(', ')} +${filter.values.length - 2}`}
              </span>
              <button
                type="button"
                className="filter-chip__remove"
                onClick={() => removeFilter(index)}
              >
                <X size={12} />
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Column picker */}
      <div className="filter-column-select">
        <button
          type="button"
          className="filter-column-btn"
          onClick={() => setColPickerOpen(!colPickerOpen)}
        >
          <span className="truncate">
            {selectedColKey ?? 'Select column…'}
          </span>
          <ChevronDown size={12} />
        </button>

        {colPickerOpen && (
          <div className="filter-dropdown filter-dropdown--wide">
            {Array.from(columnsByTable.entries()).map(([table, cols]) => (
              <div key={table}>
                <div className="filter-dropdown__group">{table}</div>
                {cols.map((c) => (
                  <button
                    key={`${c.table_name}.${c.column_name}`}
                    type="button"
                    className={`filter-dropdown__item ${
                      selectedColKey === `${c.table_name}.${c.column_name}`
                        ? 'filter-dropdown__item--active'
                        : ''
                    }`}
                    onClick={() => handleSelectColumn(c.table_name, c.column_name)}
                  >
                    <span>{c.column_name}</span>
                    {c.cardinality != null && (
                      <span className="text-[var(--text-muted)] text-[0.625rem] ml-auto">
                        {c.cardinality.toLocaleString()} unique
                      </span>
                    )}
                  </button>
                ))}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Value multi-select */}
      {selectedCol && (
        <div className="filter-value-picker">
          <div className="filter-value-search">
            <Search size={13} className="text-[var(--text-muted)]" />
            <input
              ref={searchRef}
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search values…"
              className="filter-value-search__input"
            />
          </div>

          <div className="filter-value-list">
            {isLoading ? (
              <div className="filter-value-loading">
                <div className="animate-spin text-[var(--brand)]">
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                  </svg>
                </div>
                <span className="text-[var(--text-muted)] text-xs">Loading values…</span>
              </div>
            ) : filteredValues.length === 0 ? (
              <p className="filter-empty-hint">No matching values</p>
            ) : (
              filteredValues.map((val) => (
                <label
                  key={String(val)}
                  className="filter-value-item"
                >
                  <input
                    type="checkbox"
                    checked={selectedValues.has(val)}
                    onChange={() => toggleValue(val)}
                    className="filter-checkbox"
                  />
                  <span className="truncate">{String(val)}</span>
                </label>
              ))
            )}
          </div>

          <div className="filter-value-actions">
            <span className="text-[var(--text-muted)] text-[0.6875rem]">
              {selectedValues.size} selected
            </span>
            <button
              type="button"
              disabled={selectedValues.size === 0}
              onClick={handleApplySelection}
              className="filter-apply-btn"
            >
              Apply
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default CategoricalFilter;
