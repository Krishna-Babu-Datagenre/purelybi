import type { ShellPage } from '../store/useDashboardStore';

/** Maps each ShellPage to its canonical URL path.
 *  Dashboard view with a specific ID uses '/dashboard/:id' — handled separately. */
export const PAGE_PATHS: Record<ShellPage, string> = {
  home: '/home',
  'dashboard-ai': '/dashboard/builder',
  dashboard: '/dashboard',
  'data-connect': '/data/connect',
  'data-manage': '/data/manage',
  'data-raw-tables': '/data/raw-tables',
  metadata: '/metadata',
  alerts: '/alerts',
  'alerts-create': '/alerts/create',
  profile: '/profile',
  billing: '/billing',
};

/** Convert a ShellPage (+ optional dashboard ID) to the URL path to navigate to. */
export function pageToPath(page: ShellPage, dashboardId?: string | null): string {
  if (page === 'dashboard' && dashboardId) return `/dashboard/${dashboardId}`;
  return PAGE_PATHS[page] ?? '/home';
}

/** Parse a URL pathname into a ShellPage (and optional dashboard ID).
 *  Returns `{ page: null }` for auth routes ('/') and ('/login'). */
export function pathToPage(pathname: string): { page: ShellPage | null; dashboardId?: string } {
  if (pathname === '/' || pathname === '') return { page: null }; // landing
  if (pathname === '/login') return { page: null }; // login

  if (pathname === '/home') return { page: 'home' };
  if (pathname === '/dashboard/builder') return { page: 'dashboard-ai' };

  const dashboardMatch = pathname.match(/^\/dashboard\/([^/]+)$/);
  if (dashboardMatch) return { page: 'dashboard', dashboardId: dashboardMatch[1] };

  if (pathname === '/dashboard') return { page: 'dashboard' };
  if (pathname === '/data/connect') return { page: 'data-connect' };
  if (pathname === '/data/manage') return { page: 'data-manage' };
  if (pathname === '/data/raw-tables') return { page: 'data-raw-tables' };
  if (pathname === '/metadata') return { page: 'metadata' };
  if (pathname === '/alerts') return { page: 'alerts' };
  if (pathname === '/alerts/create') return { page: 'alerts-create' };
  if (pathname === '/profile') return { page: 'profile' };
  if (pathname === '/billing') return { page: 'billing' };

  return { page: 'home' }; // fallback for unknown routes
}
