import { useEffect, useMemo, useState } from 'react';
import LandingPage from './components/landing/LandingPage';
import DashboardGrid, { DASHBOARD_GRID_PAD_X } from './layouts/DashboardGrid';
import Sidebar from './components/Sidebar';
import Topbar from './components/Topbar';
import ChatDrawer from './components/ChatDrawer';
import LoginPage from './components/LoginPage';
import DateFilterBar from './components/DateFilterBar';
import FilterPane from './components/FilterPane/FilterPane';
import { LayoutDashboard, Check, X, Loader2, Users, AlertCircle } from 'lucide-react';
import OutOfCreditsModal from './components/OutOfCreditsModal';
import DashboardShareModal from './components/DashboardShareModal';

import DataConnectPage from './components/data/DataConnectPage';
import DataManagePage from './components/data/DataManagePage';
import DataRawTablesPage from './components/data/DataRawTablesPage';
import MetadataReviewPage from './components/data/MetadataReviewPage';
import HomePage from './components/HomePage';
import AlertsPage from './components/AlertsPage';
import AlertBuilderPage from './components/AlertBuilderPage';
import DashboardBuilderEmptyState from './components/DashboardBuilderEmptyState';
import { useDashboardStore } from './store/useDashboardStore';
import { useAuthStore } from './store/useAuthStore';
import { useChatStore } from './store/useChatStore';
import { initMessageListener, exposeGlobalApi } from './services/dashboardApi';

const App = () => {
  const accessToken = useAuthStore((s) => s.accessToken);
  const validating = useAuthStore((s) => s.validating);
  const validateStoredToken = useAuthStore((s) => s.validateStoredToken);
  const user = useAuthStore((s) => s.user);

  const [shareModalOpen, setShareModalOpen] = useState(false);

  const dashboard = useDashboardStore((s) =>
    s.activeDashboardId ? s.dashboards[s.activeDashboardId] ?? null : null,
  );
  const dashboardLoading = useDashboardStore((s) => s.dashboardLoading);
  const activeDashboardId = useDashboardStore((s) => s.activeDashboardId);
  const chatOpen = useChatStore((s) => s.isOpen);
  const chatModal = useChatStore((s) => s.isModal);
  const chatWidthPx = useChatStore((s) => s.widthPx);
  const fetchUserDashboardList = useDashboardStore((s) => s.fetchUserDashboardList);
  const fetchTemplates = useDashboardStore((s) => s.fetchTemplates);
  const navigationPage = useDashboardStore((s) => s.navigationPage);
  const setNavigationPage = useDashboardStore((s) => s.setNavigationPage);

  const isEditMode = useDashboardStore((s) => s.isEditMode);
  const isSavingLayout = useDashboardStore((s) => s.isSavingLayout);
  const setEditMode = useDashboardStore((s) => s.setEditMode);
  const saveLayout = useDashboardStore((s) => s.saveLayout);
  const cancelEditMode = useDashboardStore((s) => s.cancelEditMode);

  const isTemplateDashboard = dashboard?.meta.source === 'template';

  const hasDashboard = !!dashboard;
  const showDashboardLoader = dashboardLoading || (!!activeDashboardId && !dashboard);

  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [authScreen, setAuthScreen] = useState<'landing' | 'login'>('landing');

  const topbarSubtitle = useMemo(() => {
    if (navigationPage === 'dashboard' && hasDashboard) return dashboard!.meta.name;
    if (navigationPage === 'home') return 'Your workspace';
    if (navigationPage === 'dashboard-ai') return 'AI dashboard builder';
    if (navigationPage === 'data-connect') return 'Connect a new source';
    if (navigationPage === 'data-manage') return 'Manage connections';
    if (navigationPage === 'data-raw-tables') return 'View raw tables';
    if (navigationPage === 'metadata') return 'Metadata review';
    if (navigationPage === 'alerts') return 'Manage alerts';
    if (navigationPage === 'alerts-create') return 'Create alert';
    return undefined;
  }, [navigationPage, hasDashboard, dashboard]);

  // Restore and validate session on mount
  useEffect(() => {
    validateStoredToken();
  }, [validateStoredToken]);

  // When authenticated, fetch user's dashboard list and templates
  useEffect(() => {
    if (!accessToken) return;
    fetchUserDashboardList();
    fetchTemplates();
  }, [accessToken, fetchUserDashboardList, fetchTemplates]);

  // Reset shell route when session ends (local React state would reset anyway on unmount)
  useEffect(() => {
    if (!accessToken) {
      setNavigationPage('home');
      setAuthScreen('landing');
    }
  }, [accessToken, setNavigationPage]);

  // Initialize backend communication channels when in app
  useEffect(() => {
    if (!accessToken) return;
    exposeGlobalApi();
    const unsub = initMessageListener();
    return unsub;
  }, [accessToken]);

  if (validating) {
    return (
      <div className="app-shell min-h-screen flex items-center justify-center">
        <div className="animate-spin text-[var(--brand)]">
          <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M21 12a9 9 0 1 1-6.219-8.56" />
          </svg>
        </div>
      </div>
    );
  }

  if (!accessToken) {
    if (authScreen === 'landing') {
      return <LandingPage onOpenAuth={() => setAuthScreen('login')} />;
    }
    return <LoginPage onBackToLanding={() => setAuthScreen('landing')} />;
  }

  return (
    <div className="app-shell min-h-screen">
      {/* Sidebar */}
      <Sidebar
        collapsed={sidebarCollapsed}
        onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
      />

      {/* Trial Warning */}
      {user?.subscription_tier?.tier_name === 'Free' && user?.trial_ends_at && (
        <div 
          className="fixed z-50 flex items-center justify-center gap-2 bg-gradient-to-r from-amber-500/20 to-orange-500/20 border-b border-amber-500/30 px-4 py-1.5 text-xs text-amber-200 backdrop-blur-sm w-full"
          style={{ 
            top: 'var(--topbar-height)', 
            left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
            width: `calc(100% - ${sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)'})`
          }}
        >
          <AlertCircle size={14} className="shrink-0" />
          <span className="font-medium">
            Your Free trial ends on {new Date(user.trial_ends_at).toLocaleDateString()}.
          </span>
          <a href="#" className="underline hover:text-amber-100 font-semibold ml-1">Upgrade now</a>
        </div>
      )}

      {/* Topbar */}
      <Topbar
        sidebarCollapsed={sidebarCollapsed}
        title="Business Intelligence AI"
        subtitle={topbarSubtitle}
      />

      {/* Main Content */}
      <div
        className="transition-[margin] duration-300 flex flex-col"
        style={{
          marginLeft: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
          marginRight: chatOpen && !chatModal ? chatWidthPx : 0,
          marginTop: (user?.subscription_tier?.tier_name === 'Free' && user?.trial_ends_at) ? 'calc(var(--topbar-height) + 32px)' : 'var(--topbar-height)',
          height: (user?.subscription_tier?.tier_name === 'Free' && user?.trial_ends_at) ? 'calc(100vh - var(--topbar-height) - 32px)' : 'calc(100vh - var(--topbar-height))',
        }}
      >
        <main
          className="flex-1 flex flex-col min-h-0 pt-8 pb-10"
          style={
            navigationPage === 'dashboard' && hasDashboard
              ? { paddingLeft: '0.9375rem', paddingRight: '0.9375rem' }
              : navigationPage === 'data-connect' ||
                  navigationPage === 'data-manage' ||
                  navigationPage === 'data-raw-tables' ||
                  navigationPage === 'home' ||
                  navigationPage === 'dashboard-ai' ||
                  navigationPage === 'alerts' ||
                  navigationPage === 'alerts-create'
                ? { paddingLeft: 0, paddingRight: 0 }
                : { paddingLeft: '1.5rem', paddingRight: '1.5rem' }
          }
        >
          {navigationPage === 'dashboard-ai' && (
            <div
              className="main-empty-state transition-[left] duration-300 overflow-hidden flex flex-col min-h-0 h-full"
              style={{
                position: 'fixed',
                top: 'var(--topbar-height)',
                left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
                right: chatOpen && !chatModal ? chatWidthPx : 0,
                height: 'calc(100dvh - var(--topbar-height))',
                padding: 0,
              }}
            >
              <DashboardBuilderEmptyState />
            </div>
          )}
          {navigationPage === 'dashboard' && (
            <>
              {showDashboardLoader && (
                <div
                  className="transition-[left] duration-300"
                  style={{
                    position: 'fixed',
                    top: 'var(--topbar-height)',
                    left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
                    right: chatOpen && !chatModal ? chatWidthPx : 0,
                    bottom: 0,
                    display: 'grid',
                    placeItems: 'center',
                    padding: 24,
                  }}
                >
                  <div className="flex flex-col items-center justify-center gap-3">
                    <div className="animate-spin text-[var(--brand)]">
                      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                      </svg>
                    </div>
                    <p className="text-sm text-[var(--text-secondary)] font-medium">Loading dashboard…</p>
                  </div>
                </div>
              )}
              {hasDashboard && !showDashboardLoader && (
                <>
                  <div
                    style={{
                      width: '100%',
                      display: 'grid',
                      gridTemplateColumns: '1fr auto',
                      alignItems: 'center',
                      gap: '1rem',
                      paddingTop: '0.5rem',
                      paddingLeft: DASHBOARD_GRID_PAD_X,
                      paddingRight: '1.75rem',
                      paddingBottom: '0.75rem',
                      flexShrink: 0,
                    }}
                  >
                    <div className="flex items-center gap-2">
                      {!isTemplateDashboard && (
                        isEditMode ? (
                          <>
                            <button
                              onClick={() => saveLayout()}
                              disabled={isSavingLayout}
                              className={`flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-white bg-[var(--brand)] hover:bg-[var(--brand-hover)] rounded-md shadow-sm transition-colors ${
                                isSavingLayout ? 'opacity-75 cursor-not-allowed' : ''
                              }`}
                            >
                              {isSavingLayout ? (
                                <Loader2 size={16} className="animate-spin" />
                              ) : (
                                <Check size={16} />
                              )}
                              Save Layout
                            </button>
                            <button
                              onClick={() => cancelEditMode()}
                              disabled={isSavingLayout}
                              className={`flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] rounded-md transition-colors ${
                                isSavingLayout ? 'opacity-50 cursor-not-allowed' : ''
                              }`}
                            >
                              <X size={16} />
                              Cancel
                            </button>
                          </>
                        ) : (
                          <>
                            <button
                              onClick={() => setEditMode(true)}
                              className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] rounded-md transition-colors"
                            >
                              <LayoutDashboard size={16} />
                              Edit Layout
                            </button>
                            <button
                              onClick={() => setShareModalOpen(true)}
                              className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] rounded-md transition-colors"
                            >
                              <Users size={16} />
                              Share
                            </button>
                          </>
                        )
                      )}
                    </div>
                    <div className="min-w-0 justify-self-end flex items-center gap-2">
                      <DateFilterBar />
                      <FilterPane />
                    </div>
                  </div>
                  <DashboardGrid />
                </>
              )}
              {!hasDashboard && !showDashboardLoader && (
                <div
                  className="main-empty-state transition-[left] duration-300 overflow-y-auto flex flex-col min-h-0 h-full"
                  style={{
                    position: 'fixed',
                    top: 'var(--topbar-height)',
                    left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
                    right: chatOpen && !chatModal ? chatWidthPx : 0,
                    bottom: 0,
                    display: 'flex',
                    padding: 0,
                  }}
                >
                  <DashboardBuilderEmptyState />
                </div>
              )}
            </>
          )}
          {navigationPage === 'home' && (
            <HomePage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}
          {navigationPage === 'alerts' && (
            <AlertsPage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}
          {navigationPage === 'alerts-create' && (
            <AlertBuilderPage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}
          {navigationPage === 'data-connect' && (
            <DataConnectPage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}
          {navigationPage === 'data-manage' && (
            <DataManagePage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}
          {navigationPage === 'data-raw-tables' && (
            <DataRawTablesPage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}
          {navigationPage === 'metadata' && (
            <MetadataReviewPage
              sidebarCollapsed={sidebarCollapsed}
              chatOpen={chatOpen}
              chatModal={chatModal}
              chatWidthPx={chatWidthPx}
            />
          )}
        </main>
      </div>

      {/* Chat: right-edge drawer (resizable) or modal pop-out */}
      <ChatDrawer />

      <OutOfCreditsModal />
      {shareModalOpen && dashboard && (
        <DashboardShareModal
          dashboardId={dashboard.meta.id}
          dashboardName={dashboard.meta.name}
          onClose={() => setShareModalOpen(false)}
        />
      )}
    </div>
  );
};

export default App;
