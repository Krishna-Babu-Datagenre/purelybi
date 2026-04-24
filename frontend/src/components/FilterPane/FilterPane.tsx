import { useEffect, useCallback } from 'react';
import { Filter, X, RefreshCw } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { useFilterStore } from '../../store/filterStore';
import { useDashboardStore } from '../../store/useDashboardStore';
import SourceFilter from './SourceFilter';
import TimeFilter from './TimeFilter';
import CategoricalFilter from './CategoricalFilter';
import NumericFilter from './NumericFilter';

/* ─────────────────────────────────────────────
   Filter Pane
   ─────────────────────────────────────────────
   Slide-out panel that orchestrates Time,
   Categorical, and Numeric filter components.
   "Apply" sends the full FilterSpec to the
   dashboard hydration endpoint.
───────────────────────────────────────────── */

const FilterPane = () => {
  const paneOpen = useFilterStore((s) => s.paneOpen);
  const setPaneOpen = useFilterStore((s) => s.setPaneOpen);
  const filterSpec = useFilterStore((s) => s.filterSpec);
  const clearAllFilters = useFilterStore((s) => s.clearAllFilters);
  const buildFilterSpec = useFilterStore((s) => s.buildFilterSpec);
  const metadataLoaded = useFilterStore((s) => s.metadataLoaded);
  const metadataLoading = useFilterStore((s) => s.metadataLoading);
  const metadataError = useFilterStore((s) => s.metadataError);
  const fetchMetadata = useFilterStore((s) => s.fetchMetadata);

  const activeDashboardId = useDashboardStore((s) => s.activeDashboardId);
  const filterLoading = useDashboardStore((s) => s.filterLoading);

  // Fetch metadata on first open
  useEffect(() => {
    if (paneOpen && !metadataLoaded && !metadataLoading) {
      fetchMetadata();
    }
  }, [paneOpen, metadataLoaded, metadataLoading, fetchMetadata]);

  const activeFilterCount =
    (filterSpec.time ? 1 : 0) + filterSpec.filters.length;

  const handleApply = useCallback(async () => {
    if (!activeDashboardId) return;
    const spec = buildFilterSpec();

    // Import backendClient dynamically to avoid circular dependency
    const { getDashboardFilteredWithSpec } = await import('../../services/backendClient');
    const { apiDashboardToDashboard } = await import('../../utils/apiDashboardToDashboard');

    useDashboardStore.setState({ filterLoading: true, error: null });
    try {
      const api = await getDashboardFilteredWithSpec(activeDashboardId, spec);
      const dashboard = apiDashboardToDashboard(api);
      useDashboardStore.setState((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
        filterLoading: false,
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      useDashboardStore.setState({ error: message, filterLoading: false });
    }
  }, [activeDashboardId, buildFilterSpec]);

  const handleClear = useCallback(async () => {
    clearAllFilters();
    if (!activeDashboardId) return;

    // Re-fetch unfiltered dashboard
    const { getDashboard } = await import('../../services/backendClient');
    const { apiDashboardToDashboard } = await import('../../utils/apiDashboardToDashboard');

    useDashboardStore.setState({ filterLoading: true, error: null });
    try {
      const api = await getDashboard(activeDashboardId, { forceRefresh: true });
      const dashboard = apiDashboardToDashboard(api);
      useDashboardStore.setState((s) => ({
        dashboards: { ...s.dashboards, [dashboard.meta.id]: dashboard },
        filterLoading: false,
      }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      useDashboardStore.setState({ error: message, filterLoading: false });
    }
  }, [activeDashboardId, clearAllFilters]);

  return (
    <>
      {/* Toggle button (always visible on dashboard page) */}
      <button
        type="button"
        onClick={() => setPaneOpen(!paneOpen)}
        className={`filter-toggle-btn ${paneOpen ? 'filter-toggle-btn--active' : ''}`}
        title="Toggle filter pane"
      >
        <Filter size={15} />
        {activeFilterCount > 0 && (
          <span className="filter-toggle-badge">{activeFilterCount}</span>
        )}
      </button>

      {/* Slide-out pane */}
      <AnimatePresence>
        {paneOpen && (
          <motion.div
            className="filter-pane"
            initial={{ x: '100%', opacity: 0 }}
            animate={{ x: 0, opacity: 1 }}
            exit={{ x: '100%', opacity: 0 }}
            transition={{ type: 'spring', damping: 30, stiffness: 350 }}
          >
            {/* Header */}
            <div className="filter-pane__header">
              <div className="flex items-center gap-2">
                <Filter size={16} className="text-[var(--brand)]" />
                <h3 className="filter-pane__title">Dashboard Filters</h3>
                {activeFilterCount > 0 && (
                  <span className="filter-count-badge">{activeFilterCount}</span>
                )}
              </div>
              <button
                type="button"
                onClick={() => setPaneOpen(false)}
                className="filter-pane__close"
              >
                <X size={16} />
              </button>
            </div>

            {/* Body */}
            <div className="filter-pane__body">
              {metadataLoading ? (
                <div className="filter-pane__loading">
                  <div className="animate-spin text-[var(--brand)]">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                    </svg>
                  </div>
                  <span className="text-[var(--text-secondary)] text-sm">Loading metadata…</span>
                </div>
              ) : metadataError ? (
                <div className="filter-pane__error">
                  <p className="text-sm text-red-400">{metadataError}</p>
                  <button
                    type="button"
                    onClick={fetchMetadata}
                    className="filter-apply-btn mt-2"
                  >
                    <RefreshCw size={13} />
                    Retry
                  </button>
                </div>
              ) : (
                <>
                  <SourceFilter />
                  <TimeFilter />
                  <CategoricalFilter />
                  <NumericFilter />
                </>
              )}
            </div>

            {/* Footer actions */}
            <div className="filter-pane__footer">
              <button
                type="button"
                onClick={handleClear}
                disabled={activeFilterCount === 0 || filterLoading}
                className="filter-clear-btn"
              >
                Clear All
              </button>
              <button
                type="button"
                onClick={handleApply}
                disabled={filterLoading}
                className="filter-submit-btn"
              >
                {filterLoading ? (
                  <div className="animate-spin">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                    </svg>
                  </div>
                ) : (
                  'Apply Filters'
                )}
              </button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
};

export default FilterPane;
