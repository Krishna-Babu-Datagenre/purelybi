import { useEffect, useMemo, useState } from 'react';
import LandingPage from './components/landing/LandingPage';
import DashboardGrid, { DASHBOARD_GRID_PAD_X } from './layouts/DashboardGrid';
import Sidebar from './components/Sidebar';
import Topbar from './components/Topbar';
import ChatDrawer from './components/ChatDrawer';
import LoginPage from './components/LoginPage';
import DateFilterBar from './components/DateFilterBar';
import LatestDataLabel from './components/LatestDataLabel';
import DataConnectPage from './components/data/DataConnectPage';
import DataManagePage from './components/data/DataManagePage';
import DataRawTablesPage from './components/data/DataRawTablesPage';
import HomePage from './components/HomePage';
import { useDashboardStore } from './store/useDashboardStore';
import { useAuthStore } from './store/useAuthStore';
import { useChatStore } from './store/useChatStore';
import { initMessageListener, exposeGlobalApi } from './services/dashboardApi';

const App = () => {
  const accessToken = useAuthStore((s) => s.accessToken);
  const validating = useAuthStore((s) => s.validating);
  const validateStoredToken = useAuthStore((s) => s.validateStoredToken);

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

  const hasDashboard = !!dashboard;
  const showDashboardLoader = dashboardLoading || (!!activeDashboardId && !dashboard);

  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [authScreen, setAuthScreen] = useState<'landing' | 'login'>('landing');

  const topbarSubtitle = useMemo(() => {
    if (navigationPage === 'dashboard' && hasDashboard) return dashboard!.meta.name;
    if (navigationPage === 'home') return 'Your workspace';
    if (navigationPage === 'data-connect') return 'Connect a new source';
    if (navigationPage === 'data-manage') return 'Manage connections';
    if (navigationPage === 'data-raw-tables') return 'View raw tables';
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
          marginTop: 'var(--topbar-height)',
          height: 'calc(100vh - var(--topbar-height))',
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
                  navigationPage === 'home'
                ? { paddingLeft: 0, paddingRight: 0 }
                : { paddingLeft: '1.5rem', paddingRight: '1.5rem' }
          }
        >
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
                    <div className="min-w-0 justify-self-start">
                      <LatestDataLabel />
                    </div>
                    <div className="min-w-0 justify-self-end">
                      <DateFilterBar />
                    </div>
                  </div>
                  <DashboardGrid />
                </>
              )}
              {!hasDashboard && !showDashboardLoader && (
                <div
                  className="main-empty-state transition-[left] duration-300"
                  style={{
                    position: 'fixed',
                    top: 'var(--topbar-height)',
                    left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
                    right: 0,
                    bottom: 0,
                    display: 'grid',
                    placeItems: 'center',
                    padding: '1.5rem',
                  }}
                >
                  <div className="flex flex-col items-center justify-center text-center max-w-sm">
                    <h2 className="text-lg font-semibold text-[var(--text-primary)] tracking-tight mb-1">
                      Select a Dashboard
                    </h2>
                    <p className="text-sm text-[var(--text-secondary)]">
                      Pick a dashboard from the sidebar or create one with <strong className="text-[var(--brand)]">New Dashboard</strong>.
                    </p>
                  </div>
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
            <div
              className="main-empty-state transition-[left] duration-300"
              style={{
                position: 'fixed',
                top: 'var(--topbar-height)',
                left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
                right: 0,
                bottom: 0,
                display: 'grid',
                placeItems: 'center',
                padding: '1.5rem',
              }}
            >
              <div className="flex flex-col items-center justify-center text-center max-w-sm">
                <h2 className="text-lg font-semibold text-[var(--text-primary)] tracking-tight">
                  COMING SOON
                </h2>
              </div>
            </div>
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
        </main>
      </div>

      {/* Chat: right-edge drawer (resizable) or modal pop-out */}
      <ChatDrawer />
    </div>
  );
};

export default App;
