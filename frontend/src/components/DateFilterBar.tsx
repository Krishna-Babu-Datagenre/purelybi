import { useState, useRef, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { Calendar, X } from 'lucide-react';
import { useDashboardStore } from '../store/useDashboardStore';
import type { DatePreset } from '../types';

const PRESETS: { label: string; value: DatePreset }[] = [
  { label: 'Last 7 days', value: 'last_7_days' },
  { label: 'Last 14 days', value: 'last_14_days' },
  { label: 'Last 30 days', value: 'last_30_days' },
];

const DateFilterBar = () => {
  const activePreset = useDashboardStore((s) => s.activePreset);
  const customDateRange = useDashboardStore((s) => s.customDateRange);
  const filterLoading = useDashboardStore((s) => s.filterLoading);
  const applyDatePreset = useDashboardStore((s) => s.applyDatePreset);
  const applyCustomDateRange = useDashboardStore((s) => s.applyCustomDateRange);
  const clearDateFilter = useDashboardStore((s) => s.clearDateFilter);

  const [pickerOpen, setPickerOpen] = useState(false);
  const [startInput, setStartInput] = useState('');
  const [endInput, setEndInput] = useState('');
  const triggerRef = useRef<HTMLButtonElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [dropdownPos, setDropdownPos] = useState<{ top: number; right: number } | null>(null);

  const hasActiveFilter = activePreset !== null || customDateRange !== null;

  // Position the dropdown relative to the trigger button
  const updatePosition = useCallback(() => {
    if (!triggerRef.current) return;
    const rect = triggerRef.current.getBoundingClientRect();
    setDropdownPos({
      top: rect.bottom + 6,
      right: window.innerWidth - rect.right,
    });
  }, []);

  // Close picker on outside click
  useEffect(() => {
    if (!pickerOpen) return;
    updatePosition();
    const handleClick = (e: MouseEvent) => {
      const target = e.target as Node;
      if (
        triggerRef.current?.contains(target) ||
        dropdownRef.current?.contains(target)
      ) return;
      setPickerOpen(false);
    };
    document.addEventListener('mousedown', handleClick);
    window.addEventListener('resize', updatePosition);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      window.removeEventListener('resize', updatePosition);
    };
  }, [pickerOpen, updatePosition]);

  const handlePresetClick = (preset: DatePreset) => {
    if (activePreset === preset) {
      clearDateFilter();
    } else {
      applyDatePreset(preset);
    }
  };

  const handleApplyRange = () => {
    if (!startInput || !endInput) return;
    applyCustomDateRange({ startDate: startInput, endDate: endInput });
    setPickerOpen(false);
  };

  return (
    <div className="flex items-center gap-3.5 flex-wrap justify-end">
      {/* Quick-select preset buttons */}
      {PRESETS.map(({ label, value }) => (
        <button
          key={value}
          type="button"
          disabled={filterLoading}
          onClick={() => handlePresetClick(value)}
          className={`date-filter-btn ${activePreset === value ? 'date-filter-btn--active' : ''}`}
        >
          {label}
        </button>
      ))}

      {/* Date range picker trigger */}
      <button
        ref={triggerRef}
        type="button"
        disabled={filterLoading}
        onClick={() => setPickerOpen(!pickerOpen)}
        className={`date-filter-btn flex items-center gap-1.5 ${customDateRange ? 'date-filter-btn--active' : ''}`}
      >
        <Calendar size={14} />
        {customDateRange
          ? `${customDateRange.startDate} → ${customDateRange.endDate}`
          : 'Custom Range'}
      </button>

      {/* Dropdown rendered via portal so it's never clipped */}
      {pickerOpen && dropdownPos && createPortal(
        <div
          ref={dropdownRef}
          className="date-picker-dropdown"
          style={{ position: 'fixed', top: dropdownPos.top, right: dropdownPos.right }}
        >
          <label className="date-picker-label">
            Start Date
            <input
              type="date"
              value={startInput}
              onChange={(e) => setStartInput(e.target.value)}
              className="date-picker-input"
            />
          </label>
          <label className="date-picker-label">
            End Date
            <input
              type="date"
              value={endInput}
              onChange={(e) => setEndInput(e.target.value)}
              className="date-picker-input"
            />
          </label>
          <button
            type="button"
            disabled={!startInput || !endInput || filterLoading}
            onClick={handleApplyRange}
            className="date-picker-apply"
          >
            Apply
          </button>
        </div>,
        document.body,
      )}

      {/* Clear all filters */}
      {hasActiveFilter && (
        <button
          type="button"
          disabled={filterLoading}
          onClick={() => {
            clearDateFilter();
            setStartInput('');
            setEndInput('');
          }}
          className="date-filter-btn date-filter-btn--clear flex items-center gap-1"
        >
          <X size={14} />
          Clear
        </button>
      )}

      {/* Loading spinner */}
      {filterLoading && (
        <div className="animate-spin text-[var(--brand)] ml-1">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M21 12a9 9 0 1 1-6.219-8.56" />
          </svg>
        </div>
      )}
    </div>
  );
};

export default DateFilterBar;
