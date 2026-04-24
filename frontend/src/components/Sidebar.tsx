import { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { createPortal } from 'react-dom';
import {
  Home,
  ChartLine,
  Sparkles,
  RotateCw,
  ChevronLeft,
  ChevronRight,
  Bell,
  MoreVertical,
  Trash2,
  Copy,
  Plug,
  SlidersHorizontal,
  Table,
  Database,
} from 'lucide-react';
import { useDashboardStore } from '../store/useDashboardStore';
import type { ApiDashboardMeta, TemplateMeta } from '../types';

type CombinedDashRow =
  | { kind: 'user'; db: ApiDashboardMeta; userIndex: number }
  | { kind: 'template'; tmpl: TemplateMeta };

interface SidebarProps {
  collapsed: boolean;
  onToggleCollapse: () => void;
}

const logSidebarDebug = (...args: unknown[]) => {
  if (!import.meta.env.DEV) return;
  console.debug('[SidebarDebug]', ...args);
};

const REFRESH_MIN_MS = 550;

type RefreshFeedback =
  | null
  | { kind: 'ok' }
  | { kind: 'err'; message: string };

const Sidebar = ({ collapsed, onToggleCollapse }: SidebarProps) => {
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshFeedback, setRefreshFeedback] = useState<RefreshFeedback>(null);
  const [openMenuId, setOpenMenuId] = useState<string | null>(null);
  const [openTemplateMenuSlug, setOpenTemplateMenuSlug] = useState<string | null>(null);
  const [menuPos, setMenuPos] = useState<{ top: number; left: number } | null>(null);
  const [clickedDashboardId, setClickedDashboardId] = useState<string | null>(null);
  /** Row index in dashboardList — IDs from the API can duplicate; index is the reliable selection key for UI. */
  const [selectedDashboardRowIndex, setSelectedDashboardRowIndex] = useState<number | null>(null);
  const [selectedTemplateSlug, setSelectedTemplateSlug] = useState<string | null>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const openMenu = useCallback((id: string, triggerEl: HTMLButtonElement) => {
    const rect = triggerEl.getBoundingClientRect();
    setMenuPos({ top: rect.bottom + 4, left: rect.right - 104 });
    setOpenMenuId(id);
    setOpenTemplateMenuSlug(null);
  }, []);

  const openTemplateMenu = useCallback((slug: string, triggerEl: HTMLButtonElement) => {
    const rect = triggerEl.getBoundingClientRect();
    setMenuPos({ top: rect.bottom + 4, left: rect.right - 104 });
    setOpenTemplateMenuSlug(slug);
    setOpenMenuId(null);
  }, []);

  useEffect(() => {
    if (!openMenuId && !openTemplateMenuSlug) return;
    const handleClick = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpenMenuId(null);
        setOpenTemplateMenuSlug(null);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [openMenuId, openTemplateMenuSlug]);

  const navigationPage = useDashboardStore((s) => s.navigationPage);
  const setNavigationPage = useDashboardStore((s) => s.setNavigationPage);
  const openUserDashboard = useDashboardStore((s) => s.openUserDashboard);
  const templates = useDashboardStore((s) => s.templates);
  const dashboardListMeta = useDashboardStore((s) => s.dashboardListMeta);
  const dashboardLoading = useDashboardStore((s) => s.dashboardLoading);
  const activeDashboardListId = useDashboardStore((s) => s.activeDashboardListId);
  const loadDashboardById = useDashboardStore((s) => s.loadDashboardById);
  const fetchUserDashboardList = useDashboardStore((s) => s.fetchUserDashboardList);
  const fetchTemplates = useDashboardStore((s) => s.fetchTemplates);
  const refreshActiveDashboardFromServer = useDashboardStore((s) => s.refreshActiveDashboardFromServer);
  const clearError = useDashboardStore((s) => s.clearError);
  const deleteDashboardApi = useDashboardStore((s) => s.deleteDashboardApi);
  const duplicateDashboardApi = useDashboardStore((s) => s.duplicateDashboardApi);
  const openDashboardBuilder = useDashboardStore((s) => s.openDashboardBuilder);

  const handleOpenAiDashboardBuilder = () => {
    setSelectedTemplateSlug(null);
    setSelectedDashboardRowIndex(null);
    setClickedDashboardId(null);
    openDashboardBuilder();
  };

  const handleRefresh = async () => {
    if (isRefreshing) return;
    clearError();
    setRefreshFeedback(null);
    setIsRefreshing(true);
    const t0 = Date.now();
    let err: string | null = null;
    try {
      await fetchUserDashboardList({ forceRefresh: true });
      err = useDashboardStore.getState().error;
      if (!err) {
        await fetchTemplates(undefined, { forceRefresh: true });
        err = useDashboardStore.getState().error;
      }
      if (!err && navigationPage === 'dashboard' && activeDashboardListId) {
        await refreshActiveDashboardFromServer();
        err = useDashboardStore.getState().error;
      }
    } finally {
      const elapsed = Date.now() - t0;
      if (elapsed < REFRESH_MIN_MS) {
        await new Promise((r) => setTimeout(r, REFRESH_MIN_MS - elapsed));
      }
      setIsRefreshing(false);
      if (err) {
        setRefreshFeedback({ kind: 'err', message: err });
      } else {
        setRefreshFeedback({ kind: 'ok' });
      }
      window.setTimeout(() => setRefreshFeedback(null), 2800);
    }
  };

  const handleTemplateClick = (templateId: string, templateSlug: string) => {
    logSidebarDebug('Template clicked', {
      templateId,
      templateSlug,
      selectedTemplateSlug,
      navigationPage,
    });
    setSelectedTemplateSlug(templateSlug);
    setSelectedDashboardRowIndex(null);
    setClickedDashboardId(null);
    setNavigationPage('dashboard');
    void loadDashboardById(templateId);
  };

  const handleDuplicateTemplate = async (templateId: string) => {
    setOpenTemplateMenuSlug(null);
    setSelectedTemplateSlug(null);
    setSelectedDashboardRowIndex(null);
    setClickedDashboardId(null);
    await duplicateDashboardApi(templateId);
  };

  const handleDuplicateDashboard = async (dashboardId: string) => {
    setOpenMenuId(null);
    setSelectedTemplateSlug(null);
    setSelectedDashboardRowIndex(null);
    setClickedDashboardId(null);
    await duplicateDashboardApi(dashboardId);
  };

  const handleDashboardClick = (dashboardId: string, rowIndex: number) => {
    logSidebarDebug('Dashboard clicked', {
      dashboardId,
      rowIndex,
      clickedDashboardId,
      activeDashboardListId,
      navigationPage,
      dashboardLoading,
    });
    setClickedDashboardId(dashboardId);
    setSelectedTemplateSlug(null);
    setSelectedDashboardRowIndex(rowIndex);
    void openUserDashboard(dashboardId);
  };

  const handleDeleteDashboard = (e: React.MouseEvent, dashboardId: string, name: string) => {
    e.stopPropagation();
    if (!window.confirm(`Delete dashboard "${name}"?\nThis removes all widgets and cannot be undone.`)) return;
    deleteDashboardApi(dashboardId);
  };

  const dashboardList = dashboardListMeta;
  const loadedNames = new Set(dashboardList.map((d) => d.name.toLowerCase()));
  const templatesNotLoaded = templates.filter((t) => !loadedNames.has(t.name.toLowerCase()));
  const combinedDashRows = useMemo<CombinedDashRow[]>(() => {
    const users = dashboardList.map((db, userIndex) => ({ kind: 'user' as const, db, userIndex }));
    const tmpls = templatesNotLoaded.map((tmpl) => ({ kind: 'template' as const, tmpl }));
    return [...users, ...tmpls];
  }, [dashboardList, templatesNotLoaded]);
  const effectiveSelectedDashboardId = clickedDashboardId ?? activeDashboardListId;

  useEffect(() => {
    logSidebarDebug('Selection state updated', {
      navigationPage,
      clickedDashboardId,
      activeDashboardListId,
      effectiveSelectedDashboardId,
      selectedDashboardRowIndex,
      selectedTemplateSlug,
      dashboardLoading,
    });
  }, [
    navigationPage,
    clickedDashboardId,
    activeDashboardListId,
    effectiveSelectedDashboardId,
    selectedDashboardRowIndex,
    selectedTemplateSlug,
    dashboardLoading,
  ]);

  useEffect(() => {
    const ids = dashboardList.map((d) => d.id);
    const uniqueIds = new Set(ids);
    if (import.meta.env.DEV && uniqueIds.size !== ids.length) {
      console.warn(
        '[Sidebar] Duplicate dashboard id(s) in GET /api/dashboards list — sidebar row keys and highlight used to rely on id only; now using row index.',
        ids,
      );
    }
    logSidebarDebug(
      'Dashboard rows snapshot',
      dashboardList.map((d, i) => ({
        index: i,
        id: d.id,
        name: d.name,
        isActive: navigationPage === 'dashboard' && selectedDashboardRowIndex === i,
      })),
    );
  }, [dashboardList, navigationPage, selectedDashboardRowIndex]);

  useEffect(() => {
    if (navigationPage !== 'dashboard' || activeDashboardListId == null || dashboardList.length === 0) return;
    const ids = dashboardList.map((d) => d.id);
    if (new Set(ids).size !== ids.length) return;
    const idx = dashboardList.findIndex((d) => d.id === activeDashboardListId);
    if (idx >= 0) setSelectedDashboardRowIndex(idx);
  }, [navigationPage, activeDashboardListId, dashboardList]);

  return (
    <div className={`sidebar ${collapsed ? 'sidebar--collapsed' : 'sidebar--expanded'}`}>
      {/* Header spacer */}
      <div className="sidebar-header" />

      {/* Navigation */}
      <div className="sidebar-nav">
        {/* ── Workspace (primary nav) ── */}
        {!collapsed && <div className="sidebar-section-title sidebar-section-title--purple">Workspace</div>}
        <div className={collapsed ? 'flex flex-col items-center gap-1.5' : 'space-y-0.5'}>
          {collapsed ? (
            <button
              type="button"
              onClick={() => setNavigationPage('home')}
              className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'home' ? 'sidebar-item--active' : ''}`}
              title="Home"
            >
              <Home size={20} />
            </button>
          ) : (
            <button
              type="button"
              onClick={() => setNavigationPage('home')}
              className={`sidebar-item ${navigationPage === 'home' ? 'sidebar-item--active' : ''}`}
            >
              <Home size={20} />
              <span className="truncate">Home</span>
            </button>
          )}
        </div>

        {/* ── Data ── */}
        {!collapsed && (
          <div className="sidebar-section-title sidebar-section-title--purple" style={{ marginTop: '1.25rem' }}>
            Data
          </div>
        )}
        <div className={collapsed ? 'flex flex-col items-center gap-1.5' : 'space-y-0.5'} style={{ marginTop: '0.5rem' }}>
          {collapsed ? (
            <>
              <button
                type="button"
                onClick={() => setNavigationPage('data-connect')}
                className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'data-connect' ? 'sidebar-item--active' : ''}`}
                title="Connect a new source"
              >
                <Plug size={20} />
              </button>
              <button
                type="button"
                onClick={() => setNavigationPage('data-manage')}
                className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'data-manage' ? 'sidebar-item--active' : ''}`}
                title="Manage"
              >
                <SlidersHorizontal size={20} />
              </button>
              <button
                type="button"
                onClick={() => setNavigationPage('data-raw-tables')}
                className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'data-raw-tables' ? 'sidebar-item--active' : ''}`}
                title="View raw tables"
              >
                <Table size={20} />
              </button>
              <button
                type="button"
                onClick={() => setNavigationPage('metadata')}
                className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'metadata' ? 'sidebar-item--active' : ''}`}
                title="Metadata"
              >
                <Database size={20} />
              </button>
            </>
          ) : (
            <>
              <button
                type="button"
                onClick={() => setNavigationPage('data-connect')}
                className={`sidebar-item ${navigationPage === 'data-connect' ? 'sidebar-item--active' : ''}`}
              >
                <Plug size={20} />
                <span className="truncate">Connect a new source</span>
              </button>
              <button
                type="button"
                onClick={() => setNavigationPage('data-manage')}
                className={`sidebar-item ${navigationPage === 'data-manage' ? 'sidebar-item--active' : ''}`}
              >
                <SlidersHorizontal size={20} />
                <span className="truncate">Manage</span>
              </button>
              <button
                type="button"
                onClick={() => setNavigationPage('data-raw-tables')}
                className={`sidebar-item ${navigationPage === 'data-raw-tables' ? 'sidebar-item--active' : ''}`}
              >
                <Table size={20} />
                <span className="truncate">View raw tables</span>
              </button>
              <button
                type="button"
                onClick={() => setNavigationPage('metadata')}
                className={`sidebar-item ${navigationPage === 'metadata' ? 'sidebar-item--active' : ''}`}
              >
                <Database size={20} />
                <span className="truncate">Metadata</span>
              </button>
            </>
          )}
        </div>

        {/* Collapsed: dashboard shortcuts (expanded list is below when !collapsed) */}
        {collapsed && (
          <div className="flex flex-col items-center gap-1.5" style={{ marginTop: '1.25rem' }}>
            <button
              type="button"
              onClick={handleOpenAiDashboardBuilder}
              className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'dashboard-ai' ? 'sidebar-item--active' : ''}`}
              title="Build with AI"
            >
              <Sparkles size={20} />
            </button>
            <button
              type="button"
              onClick={() => setNavigationPage('dashboard')}
              className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'dashboard' ? 'sidebar-item--active' : ''}`}
              title="Dashboards"
            >
              <ChartLine size={20} />
            </button>
          </div>
        )}

        {/* ── DASHBOARDS ── */}
        {!collapsed && <div className="sidebar-section-title sidebar-section-title--purple" style={{ marginTop: '1.25rem' }}>DASHBOARDS</div>}
        {!collapsed && (
          <div className="sidebar-section" style={{ marginTop: '0.5rem' }}>
            <div className="flex flex-col gap-0.5">
              <button
                type="button"
                onClick={handleOpenAiDashboardBuilder}
                className={`sidebar-item ${navigationPage === 'dashboard-ai' ? 'sidebar-item--active' : ''}`}
                title="Build dashboards with AI (Surprise me or Guided)"
              >
                <Sparkles size={20} />
                <span className="truncate">Build with AI</span>
              </button>
              {combinedDashRows.map((row, listIndex) => {
                if (row.kind === 'user') {
                  const { db, userIndex } = row;
                  const isActive =
                    navigationPage === 'dashboard' &&
                    selectedTemplateSlug == null &&
                    selectedDashboardRowIndex === userIndex;
                  return (
                    <div
                      key={`dash-row-${userIndex}-${db.id}`}
                      className={`sidebar-dash-row ${isActive ? 'sidebar-dash-row--active' : ''}`}
                      role="button"
                      tabIndex={0}
                      onClick={() => handleDashboardClick(db.id, userIndex)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          handleDashboardClick(db.id, userIndex);
                        }
                      }}
                    >
                      <button
                        type="button"
                        className={`sidebar-dash-item ${isActive ? 'sidebar-dash-item--active' : ''}`}
                        aria-busy={dashboardLoading ? 'true' : undefined}
                        tabIndex={-1}
                        title={db.name}
                      >
                        <ChartLine size={14} className="shrink-0" />
                        <span className="sidebar-dash-label">{db.name}</span>
                      </button>
                      <button
                        type="button"
                        className="sidebar-dash-menu-trigger"
                        onMouseDown={(e) => e.stopPropagation()}
                        onPointerDown={(e) => e.stopPropagation()}
                        onClick={(e) => {
                          e.stopPropagation();
                          e.preventDefault();
                          if (openMenuId === db.id) {
                            setOpenMenuId(null);
                          } else {
                            openMenu(db.id, e.currentTarget);
                          }
                        }}
                        title="Dashboard options"
                      >
                        <MoreVertical size={14} />
                      </button>
                    </div>
                  );
                }
                const { tmpl } = row;
                const isTemplateActive =
                  navigationPage === 'dashboard' && selectedTemplateSlug === tmpl.slug;
                return (
                  <div
                    key={`tmpl-row-${tmpl.id}-${listIndex}`}
                    className={`sidebar-dash-row ${isTemplateActive ? 'sidebar-dash-row--active' : ''}`}
                    role="button"
                    tabIndex={0}
                    onClick={() => handleTemplateClick(tmpl.id, tmpl.slug)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        handleTemplateClick(tmpl.id, tmpl.slug);
                      }
                    }}
                  >
                    <button
                      type="button"
                      className={`sidebar-dash-item ${isTemplateActive ? 'sidebar-dash-item--active' : ''}`}
                      tabIndex={-1}
                      title={tmpl.name}
                    >
                      <ChartLine size={14} className="shrink-0" />
                      <span className="sidebar-dash-label">{tmpl.name}</span>
                    </button>
                    <button
                      type="button"
                      className="sidebar-dash-menu-trigger"
                      onMouseDown={(e) => e.stopPropagation()}
                      onPointerDown={(e) => e.stopPropagation()}
                      onClick={(e) => {
                        e.stopPropagation();
                        e.preventDefault();
                        if (openTemplateMenuSlug === tmpl.slug) {
                          setOpenTemplateMenuSlug(null);
                        } else {
                          openTemplateMenu(tmpl.slug, e.currentTarget);
                        }
                      }}
                      title="Template options"
                    >
                      <MoreVertical size={14} />
                    </button>
                  </div>
                );
              })}

              {(openMenuId || openTemplateMenuSlug) && menuPos && createPortal(
                <div
                  ref={dropdownRef}
                  className="sidebar-dash-dropdown"
                  style={{ top: menuPos.top, left: menuPos.left }}
                >
                  {openMenuId && (
                    <>
                      <button
                        type="button"
                        className="sidebar-dash-dropdown-item"
                        onClick={() => {
                          const id = openMenuId;
                          setOpenMenuId(null);
                          void handleDuplicateDashboard(id);
                        }}
                      >
                        <Copy size={13} />
                        <span>Duplicate</span>
                      </button>
                      <button
                        type="button"
                        className="sidebar-dash-dropdown-item sidebar-dash-dropdown-item--danger"
                        onClick={(e) => {
                          const id = openMenuId;
                          const db = dashboardList.find((d) => d.id === id);
                          setOpenMenuId(null);
                          if (db) handleDeleteDashboard(e, db.id, db.name);
                        }}
                      >
                        <Trash2 size={13} />
                        <span>Delete</span>
                      </button>
                    </>
                  )}
                  {openTemplateMenuSlug && (
                    <button
                      type="button"
                      className="sidebar-dash-dropdown-item"
                      onClick={() => {
                        const tmpl = templates.find((t) => t.slug === openTemplateMenuSlug);
                        if (tmpl) void handleDuplicateTemplate(tmpl.id);
                      }}
                    >
                      <Copy size={13} />
                      <span>Duplicate</span>
                    </button>
                  )}
                </div>,
                document.body
              )}
            </div>
          </div>
        )}

        {/* ── Alerts ── */}
        {!collapsed && <div className="sidebar-section-title sidebar-section-title--purple" style={{ marginTop: '1.25rem' }}>Alerts</div>}
        <div className={collapsed ? 'flex flex-col items-center gap-1.5' : 'space-y-0.5'} style={!collapsed ? { marginTop: '0.5rem' } : undefined}>
          {collapsed ? (
            <button
              type="button"
              onClick={() => setNavigationPage('alerts')}
              className={`sidebar-item sidebar-item--collapsed ${navigationPage === 'alerts' ? 'sidebar-item--active' : ''}`}
              title="Alerts"
            >
              <Bell size={20} />
            </button>
          ) : (
            <button
              type="button"
              onClick={() => setNavigationPage('alerts')}
              className={`sidebar-item ${navigationPage === 'alerts' ? 'sidebar-item--active' : ''}`}
            >
              <Bell size={20} />
              <span className="truncate">Alerts</span>
            </button>
          )}
        </div>
      </div>

      {/* Footer */}
      <div className="sidebar-footer">
        <div className={collapsed ? 'flex flex-col items-center gap-2' : 'space-y-2'}>
          <button
            type="button"
            onClick={handleRefresh}
            disabled={isRefreshing}
            aria-busy={isRefreshing}
            className={`sidebar-item ${collapsed ? 'sidebar-item--collapsed' : ''} ${isRefreshing ? 'opacity-90 cursor-wait' : ''}`}
            title={
              refreshFeedback?.kind === 'ok'
                ? 'Refreshed'
                : refreshFeedback?.kind === 'err'
                  ? refreshFeedback.message
                  : 'Refresh dashboards, templates, and the open dashboard'
            }
          >
            <RotateCw
              size={20}
              className={isRefreshing ? 'animate-spin shrink-0' : 'shrink-0'}
              aria-hidden
            />
            {!collapsed && (
              <span className="truncate">{isRefreshing ? 'Refreshing…' : 'Refresh Everything'}</span>
            )}
          </button>
          {!collapsed && refreshFeedback?.kind === 'ok' && (
            <p
              className="sidebar-refresh-hint sidebar-refresh-hint--ok"
              role="status"
              aria-live="polite"
            >
              Updated
            </p>
          )}
          {!collapsed && refreshFeedback?.kind === 'err' && (
            <p
              className="sidebar-refresh-hint sidebar-refresh-hint--err"
              role="alert"
            >
              {refreshFeedback.message}
            </p>
          )}
          {collapsed && (
            <span className="sr-only" role="status" aria-live="polite">
              {refreshFeedback?.kind === 'ok'
                ? 'Refresh complete'
                : refreshFeedback?.kind === 'err'
                  ? refreshFeedback.message
                  : ''}
            </span>
          )}

          <button
            onClick={onToggleCollapse}
            className={`sidebar-item ${collapsed ? 'sidebar-item--collapsed' : ''}`}
            title={collapsed ? 'Expand' : 'Collapse'}
          >
            {collapsed ? <ChevronRight size={20} /> : <ChevronLeft size={20} />}
            {!collapsed && <span className="truncate">Collapse</span>}
          </button>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;
