import { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import { Database, ChevronDown, Search, X } from 'lucide-react';
import { useFilterStore } from '../../store/filterStore';

/* ─────────────────────────────────────────────
   Source Filter
   ─────────────────────────────────────────────
   Connector source selector. Limits the columns
   visible in Time, Categorical, and Numeric filters.
───────────────────────────────────────────── */

const SourceFilter = () => {
  const sources = useFilterStore((s) => s.sources);
  const selectedSource = useFilterStore((s) => s.selectedSource);
  const setSelectedSource = useFilterStore((s) => s.setSelectedSource);

  const [pickerOpen, setPickerOpen] = useState(false);
  const [search, setSearch] = useState('');
  const searchRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (pickerOpen && searchRef.current) {
      searchRef.current.focus();
    }
  }, [pickerOpen]);

  const filteredSources = useMemo(() => {
    if (!search) return sources;
    const q = search.toLowerCase();
    return sources.filter((s) => s.connector_name.toLowerCase().includes(q));
  }, [sources, search]);

  const selectedLabel = useMemo(() => {
    if (!selectedSource) return 'All sources';
    const src = sources.find((s) => s.connector_config_id === selectedSource);
    return src?.connector_name ?? 'All sources';
  }, [selectedSource, sources]);

  const handleSelect = useCallback(
    (sourceId: string | null) => {
      setSelectedSource(sourceId);
      setPickerOpen(false);
      setSearch('');
    },
    [setSelectedSource],
  );

  if (sources.length <= 1) return null;

  return (
    <div className="filter-section">
      <div className="filter-section__header">
        <Database size={14} className="text-[var(--brand)]" />
        <span className="filter-section__title">Source</span>
      </div>

      <div className="filter-column-select">
        <button
          type="button"
          className="filter-column-btn"
          onClick={() => setPickerOpen(!pickerOpen)}
        >
          <span className="truncate">{selectedLabel}</span>
          <div className="flex items-center gap-1">
            {selectedSource && (
              <span
                role="button"
                tabIndex={0}
                className="filter-column-clear"
                onClick={(e) => {
                  e.stopPropagation();
                  handleSelect(null);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.stopPropagation();
                    handleSelect(null);
                  }
                }}
              >
                <X size={11} />
              </span>
            )}
            <ChevronDown size={12} />
          </div>
        </button>

        {pickerOpen && (
          <div className="filter-dropdown filter-dropdown--wide">
            {sources.length > 5 && (
              <div className="filter-dropdown__search">
                <Search size={12} className="text-[var(--text-muted)]" />
                <input
                  ref={searchRef}
                  type="text"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search sources…"
                  className="filter-dropdown__search-input"
                />
              </div>
            )}
            <button
              type="button"
              className={`filter-dropdown__item ${!selectedSource ? 'filter-dropdown__item--active' : ''}`}
              onClick={() => handleSelect(null)}
            >
              <span>All sources</span>
            </button>
            {filteredSources.map((src) => (
              <button
                key={src.connector_config_id}
                type="button"
                className={`filter-dropdown__item ${
                  selectedSource === src.connector_config_id
                    ? 'filter-dropdown__item--active'
                    : ''
                }`}
                onClick={() => handleSelect(src.connector_config_id)}
              >
                <span>{src.connector_name}</span>
                <span className="text-[var(--text-muted)] text-[0.625rem]">
                  {src.synced_tables.length} table{src.synced_tables.length !== 1 ? 's' : ''}
                </span>
              </button>
            ))}
            {filteredSources.length === 0 && (
              <p className="filter-empty-hint" style={{ padding: '0.5rem' }}>No matching sources</p>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default SourceFilter;
