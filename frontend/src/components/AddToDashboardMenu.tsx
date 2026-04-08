import { useState, useRef, useEffect, useCallback } from 'react';
import { LayoutDashboard, Plus, Check, Loader2, ChevronDown } from 'lucide-react';
import { useDashboardStore } from '../store/useDashboardStore';
import type { EChartsConfig } from '../types';

interface AddToDashboardMenuProps {
  chartConfig: EChartsConfig;
  chartType?: string;
  chartTitle?: string;
  dataConfig?: Record<string, unknown>;
}

type MenuState = 'idle' | 'open' | 'creating' | 'saving' | 'done';

function AddToDashboardMenu({ chartConfig, chartType, chartTitle, dataConfig }: AddToDashboardMenuProps) {
  const [menuState, setMenuState] = useState<MenuState>('idle');
  const [newName, setNewName] = useState('');
  const [showNewInput, setShowNewInput] = useState(false);
  const [savedTo, setSavedTo] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const menuRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const dashboardListMeta = useDashboardStore((s) => s.dashboardListMeta);
  const dashboardListFetched = useDashboardStore((s) => s.dashboardListFetched);
  const fetchUserDashboardList = useDashboardStore((s) => s.fetchUserDashboardList);
  const createBlankDashboard = useDashboardStore((s) => s.createBlankDashboard);
  const addWidgetToDashboard = useDashboardStore((s) => s.addWidgetToDashboard);

  useEffect(() => {
    if (menuState === 'open' && !dashboardListFetched) {
      fetchUserDashboardList();
    }
  }, [menuState, dashboardListFetched, fetchUserDashboardList]);

  useEffect(() => {
    if (showNewInput && inputRef.current) {
      inputRef.current.focus();
    }
  }, [showNewInput]);

  const closeMenu = useCallback(() => {
    setMenuState('idle');
    setShowNewInput(false);
    setNewName('');
    setError(null);
  }, []);

  useEffect(() => {
    if (menuState !== 'open') return;
    const handleClick = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        closeMenu();
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [menuState, closeMenu]);

  const fallbackTitle =
    chartType === 'kpi'
      ? `KPI ${new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`
      : `${(chartType ?? 'chart').charAt(0).toUpperCase()}${(chartType ?? 'chart').slice(1)} Chart`;

  const widgetPayload = {
    title: chartTitle || fallbackTitle,
    type: chartType ?? 'chart',
    chart_config: chartConfig,
    data_config: dataConfig,
  };

  const showSuccess = (dashboardName: string) => {
    setSavedTo(dashboardName);
    setMenuState('done');
    fetchUserDashboardList();
    setTimeout(() => {
      setMenuState('idle');
      setSavedTo(null);
      setNewName('');
      setShowNewInput(false);
    }, 2500);
  };

  const handleAddToExisting = async (dashboardId: string, dashboardName: string) => {
    setMenuState('saving');
    setError(null);
    try {
      await addWidgetToDashboard(dashboardId, widgetPayload);
      showSuccess(dashboardName);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to add widget');
      setMenuState('open');
    }
  };

  const handleCreateAndAdd = async () => {
    const name = newName.trim();
    if (!name) return;
    setMenuState('creating');
    setError(null);
    try {
      const newId = await createBlankDashboard(name);
      setMenuState('saving');
      await addWidgetToDashboard(newId, widgetPayload);
      showSuccess(name);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create dashboard');
      setMenuState('open');
    }
  };

  if (menuState === 'done') {
    return (
      <div className="add-to-dash-done">
        <Check size={14} />
        <span>Added to {savedTo}</span>
      </div>
    );
  }

  if (menuState === 'saving' || menuState === 'creating') {
    return (
      <div className="add-to-dash-loading">
        <Loader2 size={14} className="animate-spin" />
        <span>{menuState === 'creating' ? 'Creating dashboard…' : 'Adding to dashboard…'}</span>
      </div>
    );
  }

  return (
    <div className="add-to-dash-root" ref={menuRef}>
      <button
        type="button"
        className="add-to-dash-trigger"
        onClick={() => setMenuState(menuState === 'open' ? 'idle' : 'open')}
      >
        <LayoutDashboard size={14} />
        <span>Add to Dashboard</span>
        <ChevronDown size={12} className={`add-to-dash-chevron ${menuState === 'open' ? 'add-to-dash-chevron--open' : ''}`} />
      </button>

      {menuState === 'open' && (
        <div className="add-to-dash-dropdown">
          {error && (
            <div className="add-to-dash-error">{error}</div>
          )}

          {dashboardListMeta.length > 0 && (
            <>
              <div className="add-to-dash-section-label">Existing dashboards</div>
              <div className="add-to-dash-list">
                {dashboardListMeta.map((db) => (
                  <button
                    key={db.id}
                    type="button"
                    className="add-to-dash-option"
                    onClick={() => handleAddToExisting(db.id, db.name)}
                  >
                    <LayoutDashboard size={14} />
                    <span className="truncate">{db.name}</span>
                  </button>
                ))}
              </div>
              <div className="add-to-dash-divider" />
            </>
          )}

          {!showNewInput ? (
            <button
              type="button"
              className="add-to-dash-option add-to-dash-option--create"
              onClick={() => setShowNewInput(true)}
            >
              <Plus size={14} />
              <span>Create new dashboard</span>
            </button>
          ) : (
            <div className="add-to-dash-new-form">
              <input
                ref={inputRef}
                type="text"
                className="add-to-dash-input"
                placeholder="Dashboard name…"
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') handleCreateAndAdd();
                  if (e.key === 'Escape') {
                    setShowNewInput(false);
                    setNewName('');
                  }
                }}
              />
              <button
                type="button"
                className="add-to-dash-create-btn"
                disabled={!newName.trim()}
                onClick={handleCreateAndAdd}
              >
                Create & Add
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default AddToDashboardMenu;
