import { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import { SlidersHorizontal, ChevronDown, X, Search } from 'lucide-react';
import { useFilterStore } from '../../store/filterStore';
import type { NumericFilter as NumFilterType, ColumnRef, ColumnMetadata } from '../../types/metadata';

/* ─────────────────────────────────────────────
   Numeric Range Filter
   ─────────────────────────────────────────────
   Table → Column → min/max inputs.
───────────────────────────────────────────── */

const NumericFilter = () => {
  const columns = useFilterStore((s) => s.columns);
  const selectedSource = useFilterStore((s) => s.selectedSource);
  const getSourceTableNames = useFilterStore((s) => s.getSourceTableNames);
  const numericCols = useMemo(() => {
    const sourceTables = getSourceTableNames();
    return columns.filter(
      (c) =>
        c.is_filterable &&
        (c.semantic_type === 'numeric' || c.semantic_type === 'measure') &&
        (!sourceTables || sourceTables.has(c.table_name)),
    );
  }, [columns, selectedSource, getSourceTableNames]);
  const filterSpec = useFilterStore((s) => s.filterSpec);
  const addFilter = useFilterStore((s) => s.addFilter);
  const updateFilter = useFilterStore((s) => s.updateFilter);
  const removeFilter = useFilterStore((s) => s.removeFilter);

  const [selectedColKey, setSelectedColKey] = useState<string | null>(null);
  const [colPickerOpen, setColPickerOpen] = useState(false);
  const [colSearch, setColSearch] = useState('');
  const [minVal, setMinVal] = useState('');
  const [maxVal, setMaxVal] = useState('');
  const colSearchRef = useRef<HTMLInputElement>(null);

  // Group columns by table
  const columnsByTable = useMemo(() => {
    const map = new Map<string, ColumnMetadata[]>();
    for (const col of numericCols) {
      const list = map.get(col.table_name) ?? [];
      list.push(col);
      map.set(col.table_name, list);
    }
    return map;
  }, [numericCols]);

  // Focus column search when picker opens
  useEffect(() => {
    if (colPickerOpen && colSearchRef.current) {
      colSearchRef.current.focus();
    }
  }, [colPickerOpen]);

  // Filter column groups by search
  const filteredColumnsByTable = useMemo(() => {
    if (!colSearch) return columnsByTable;
    const q = colSearch.toLowerCase();
    const result = new Map<string, ColumnMetadata[]>();
    for (const [table, cols] of columnsByTable) {
      const filtered = cols.filter(
        (c) =>
          c.column_name.toLowerCase().includes(q) ||
          table.toLowerCase().includes(q),
      );
      if (filtered.length > 0) result.set(table, filtered);
    }
    return result;
  }, [columnsByTable, colSearch]);

  // Get existing numeric filters from spec
  const existingNumFilters = useMemo(
    () =>
      filterSpec.filters
        .map((f, i) => ({ filter: f, index: i }))
        .filter((x): x is { filter: NumFilterType; index: number } => x.filter.kind === 'numeric'),
    [filterSpec.filters],
  );

  const selectedCol = useMemo(() => {
    if (!selectedColKey) return null;
    const [table, ...rest] = selectedColKey.split('.');
    const column = rest.join('.');
    return numericCols.find((c) => c.table_name === table && c.column_name === column) ?? null;
  }, [selectedColKey, numericCols]);

  const handleSelectColumn = useCallback((table: string, column: string) => {
    setSelectedColKey(`${table}.${column}`);
    setColPickerOpen(false);
    setColSearch('');
    setMinVal('');
    setMaxVal('');
  }, []);

  const handleApply = useCallback(() => {
    if (!selectedCol) return;
    const min = minVal !== '' ? Number(minVal) : undefined;
    const max = maxVal !== '' ? Number(maxVal) : undefined;
    if (min === undefined && max === undefined) return;

    const ref: ColumnRef = { table: selectedCol.table_name, column: selectedCol.column_name };

    // Check if filter for this column already exists
    const existingIdx = existingNumFilters.findIndex(
      (x) =>
        x.filter.column_ref.table === ref.table &&
        x.filter.column_ref.column === ref.column,
    );

    const filter: NumFilterType = {
      kind: 'numeric',
      column_ref: ref,
      op: 'between',
      min,
      max,
    };

    if (existingIdx >= 0) {
      updateFilter(existingNumFilters[existingIdx].index, filter);
    } else {
      addFilter(filter);
    }

    setSelectedColKey(null);
    setMinVal('');
    setMaxVal('');
  }, [selectedCol, minVal, maxVal, existingNumFilters, addFilter, updateFilter]);

  if (numericCols.length === 0) {
    return (
      <div className="filter-section">
        <div className="filter-section__header">
          <SlidersHorizontal size={14} className="text-[var(--brand)]" />
          <span className="filter-section__title">Numeric Filters</span>
        </div>
        <p className="filter-empty-hint">No numeric columns available</p>
      </div>
    );
  }

  return (
    <div className="filter-section">
      <div className="filter-section__header">
        <SlidersHorizontal size={14} className="text-[var(--brand)]" />
        <span className="filter-section__title">Numeric Filters</span>
      </div>

      {/* Active numeric filter chips */}
      {existingNumFilters.length > 0 && (
        <div className="filter-chips">
          {existingNumFilters.map(({ filter, index }) => (
            <div key={`${filter.column_ref.table}.${filter.column_ref.column}`} className="filter-chip">
              <span className="filter-chip__label">
                {filter.column_ref.column}
              </span>
              <span className="filter-chip__values">
                {filter.min != null && filter.max != null
                  ? `${filter.min} – ${filter.max}`
                  : filter.min != null
                    ? `≥ ${filter.min}`
                    : `≤ ${filter.max}`}
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
            {numericCols.length > 5 && (
              <div className="filter-dropdown__search">
                <Search size={12} className="text-[var(--text-muted)]" />
                <input
                  ref={colSearchRef}
                  type="text"
                  value={colSearch}
                  onChange={(e) => setColSearch(e.target.value)}
                  placeholder="Search columns…"
                  className="filter-dropdown__search-input"
                />
              </div>
            )}
            {Array.from(filteredColumnsByTable.entries()).map(([table, cols]) => (
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
                    <span className="text-[var(--text-muted)] text-[0.625rem] ml-auto">
                      {c.semantic_type}
                    </span>
                  </button>
                ))}
              </div>
            ))}
            {filteredColumnsByTable.size === 0 && (
              <p className="filter-empty-hint" style={{ padding: '0.5rem' }}>No matching columns</p>
            )}
          </div>
        )}
      </div>

      {/* Min/Max inputs */}
      {selectedCol && (
        <div className="filter-numeric-inputs">
          <div className="filter-range-row">
            <input
              type="number"
              value={minVal}
              onChange={(e) => setMinVal(e.target.value)}
              placeholder="Min"
              className="filter-range-input"
            />
            <span className="text-[var(--text-muted)] text-xs">–</span>
            <input
              type="number"
              value={maxVal}
              onChange={(e) => setMaxVal(e.target.value)}
              placeholder="Max"
              className="filter-range-input"
            />
            <button
              type="button"
              disabled={minVal === '' && maxVal === ''}
              onClick={handleApply}
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

export default NumericFilter;
