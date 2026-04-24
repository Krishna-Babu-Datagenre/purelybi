import { useState, useMemo, useCallback } from 'react';
import { Calendar, ChevronDown } from 'lucide-react';
import { useFilterStore } from '../../store/filterStore';
import type { TimePreset, ColumnRef } from '../../types/metadata';

/* ─────────────────────────────────────────────
   Time Filter
   ─────────────────────────────────────────────
   Preset quick-select + custom date range picker.
   Anchored to a temporal column chosen from metadata.
───────────────────────────────────────────── */

const PRESETS: { label: string; value: TimePreset }[] = [
  { label: 'Last 7 days', value: 'last_7_days' },
  { label: 'Last 14 days', value: 'last_14_days' },
  { label: 'Last 30 days', value: 'last_30_days' },
  { label: 'Last 90 days', value: 'last_90_days' },
  { label: 'YTD', value: 'ytd' },
  { label: 'MTD', value: 'mtd' },
];

const TimeFilter = () => {
  const columns = useFilterStore((s) => s.columns);
  const temporalCols = useMemo(
    () => columns.filter((c) => c.is_filterable && c.semantic_type === 'temporal'),
    [columns],
  );
  const tables = useFilterStore((s) => s.tables);
  const filterSpec = useFilterStore((s) => s.filterSpec);
  const setTimeFilter = useFilterStore((s) => s.setTimeFilter);

  const [customFrom, setCustomFrom] = useState('');
  const [customTo, setCustomTo] = useState('');
  const [columnPickerOpen, setColumnPickerOpen] = useState(false);
  // Locally-selected column ref (survives before any preset/range is chosen)
  const [localSelectedRef, setLocalSelectedRef] = useState<ColumnRef | null>(null);

  // Group temporal columns by table, preferring primary_date_column
  const columnOptions = useMemo(() => {
    const primaryDates = new Map<string, string>();
    for (const t of tables) {
      if (t.primary_date_column) {
        primaryDates.set(t.table_name, t.primary_date_column);
      }
    }
    // Sort: primary date columns first, then others
    return [...temporalCols].sort((a, b) => {
      const aIsPrimary = primaryDates.get(a.table_name) === a.column_name;
      const bIsPrimary = primaryDates.get(b.table_name) === b.column_name;
      if (aIsPrimary && !bIsPrimary) return -1;
      if (!aIsPrimary && bIsPrimary) return 1;
      return `${a.table_name}.${a.column_name}`.localeCompare(`${b.table_name}.${b.column_name}`);
    });
  }, [temporalCols, tables]);

  const activeTimeFilter = filterSpec.time;
  // Effective selected ref: prefer what's stored in the active filter, then local pick
  const selectedRef: ColumnRef | undefined = activeTimeFilter?.column_ref ?? localSelectedRef ?? undefined;

  const selectColumn = useCallback(
    (ref: ColumnRef) => {
      setLocalSelectedRef(ref);
      // If a filter is already active, update its column too
      if (activeTimeFilter) {
        setTimeFilter({ ...activeTimeFilter, column_ref: ref });
      }
      setColumnPickerOpen(false);
    },
    [activeTimeFilter, setTimeFilter],
  );

  const handlePresetClick = useCallback(
    (preset: TimePreset) => {
      const col = selectedRef ?? (columnOptions[0] ? { table: columnOptions[0].table_name, column: columnOptions[0].column_name } : undefined);
      if (!col) return;

      if (activeTimeFilter?.preset === preset) {
        setTimeFilter(undefined);
        return;
      }
      setTimeFilter({ column_ref: col, preset });
    },
    [selectedRef, columnOptions, activeTimeFilter, setTimeFilter],
  );

  const handleApplyRange = useCallback(() => {
    if (!customFrom || !customTo) return;
    const col = selectedRef ?? (columnOptions[0] ? { table: columnOptions[0].table_name, column: columnOptions[0].column_name } : undefined);
    if (!col) return;
    setTimeFilter({ column_ref: col, range: { from: customFrom, to: customTo } });
  }, [customFrom, customTo, selectedRef, columnOptions, setTimeFilter]);

  if (columnOptions.length === 0) {
    return (
      <div className="filter-section">
        <div className="filter-section__header">
          <Calendar size={14} className="text-[var(--brand)]" />
          <span className="filter-section__title">Time Filter</span>
        </div>
        <p className="filter-empty-hint">No temporal columns available</p>
      </div>
    );
  }

  const selectedLabel = selectedRef
    ? `${selectedRef.table}.${selectedRef.column}`
    : `${columnOptions[0].table_name}.${columnOptions[0].column_name}`;

  return (
    <div className="filter-section">
      <div className="filter-section__header">
        <Calendar size={14} className="text-[var(--brand)]" />
        <span className="filter-section__title">Time Filter</span>
      </div>

      {/* Column selector */}
      {columnOptions.length > 1 && (
        <div className="filter-column-select">
          <button
            type="button"
            className="filter-column-btn"
            onClick={() => setColumnPickerOpen(!columnPickerOpen)}
          >
            <span className="truncate">{selectedLabel}</span>
            <ChevronDown size={12} />
          </button>
          {columnPickerOpen && (
            <div className="filter-dropdown">
              {columnOptions.map((c) => (
                <button
                  key={`${c.table_name}.${c.column_name}`}
                  type="button"
                  className={`filter-dropdown__item ${
                    selectedRef?.table === c.table_name && selectedRef?.column === c.column_name
                      ? 'filter-dropdown__item--active'
                      : ''
                  }`}
                  onClick={() => selectColumn({ table: c.table_name, column: c.column_name })}
                >
                  <span className="text-[var(--text-muted)] text-[0.6875rem]">{c.table_name}</span>
                  <span>{c.column_name}</span>
                </button>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Preset buttons */}
      <div className="filter-presets">
        {PRESETS.map(({ label, value }) => (
          <button
            key={value}
            type="button"
            className={`filter-preset-btn ${activeTimeFilter?.preset === value ? 'filter-preset-btn--active' : ''}`}
            onClick={() => handlePresetClick(value)}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Custom range */}
      <div className="filter-range-row">
        <input
          type="date"
          value={customFrom}
          onChange={(e) => setCustomFrom(e.target.value)}
          className="filter-range-input"
          placeholder="From"
        />
        <span className="text-[var(--text-muted)] text-xs">→</span>
        <input
          type="date"
          value={customTo}
          onChange={(e) => setCustomTo(e.target.value)}
          className="filter-range-input"
          placeholder="To"
        />
        <button
          type="button"
          disabled={!customFrom || !customTo}
          onClick={handleApplyRange}
          className="filter-apply-btn"
        >
          Apply
        </button>
      </div>

      {/* Active indicator */}
      {activeTimeFilter && (
        <div className="filter-active-badge">
          {activeTimeFilter.preset
            ? PRESETS.find((p) => p.value === activeTimeFilter.preset)?.label
            : `${activeTimeFilter.range?.from} → ${activeTimeFilter.range?.to}`}
        </div>
      )}
    </div>
  );
};

export default TimeFilter;
