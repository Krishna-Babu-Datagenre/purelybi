import { useMemo } from 'react';
import {
  BarChart3,
  Database,
  FileDown,
  LayoutDashboard,
  MessageSquare,
  Plug,
  RefreshCw,
  Sparkles,
  Table,
} from 'lucide-react';
import { useAuthStore } from '../store/useAuthStore';
import { useDashboardStore } from '../store/useDashboardStore';
import { useChatStore } from '../store/useChatStore';
import DataPageFrame from './data/DataPageFrame';

interface HomePageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

const HomePage = ({ sidebarCollapsed, chatOpen, chatModal, chatWidthPx }: HomePageProps) => {
  const user = useAuthStore((s) => s.user);
  const setNavigationPage = useDashboardStore((s) => s.setNavigationPage);
  const openUserDashboard = useDashboardStore((s) => s.openUserDashboard);
  const dashboardListMeta = useDashboardStore((s) => s.dashboardListMeta);
  const activeDashboardListId = useDashboardStore((s) => s.activeDashboardListId);
  const openChat = useChatStore((s) => s.openChat);

  const displayName = useMemo(() => {
    const n = user?.full_name?.trim();
    if (n) return n.split(/\s+/)[0] ?? n;
    if (user?.email) return user.email.split('@')[0] ?? user.email;
    return 'there';
  }, [user]);

  const primaryDashboardId = useMemo(() => {
    if (activeDashboardListId) return activeDashboardListId;
    if (dashboardListMeta.length > 0) return dashboardListMeta[0].id;
    return null;
  }, [activeDashboardListId, dashboardListMeta]);

  const dashboardCount = dashboardListMeta.length;

  return (
    <DataPageFrame
      sidebarCollapsed={sidebarCollapsed}
      chatOpen={chatOpen}
      chatModal={chatModal}
      chatWidthPx={chatWidthPx}
    >
      <div className="mx-auto w-full max-w-5xl pb-8">
        <header className="mb-10">
          <p className="text-xs font-semibold uppercase tracking-wider text-[var(--brand)] mb-2">
            Workspace
          </p>
          <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight text-[var(--text-primary)] mb-3">
            Welcome back{displayName ? `, ${displayName}` : ''}
          </h1>
          <p className="text-[0.9375rem] leading-relaxed text-[var(--text-secondary)] max-w-2xl">
            Connect your data sources, keep them in sync, then explore KPIs and charts on dashboards—or ask
            questions in plain language and build reports you can rearrange and export.
          </p>
          {dashboardCount > 0 && (
            <p className="mt-4 text-sm text-[var(--text-secondary)]">
              You have{' '}
              <span className="font-medium text-[var(--text-primary)]">
                {dashboardCount} {dashboardCount === 1 ? 'dashboard' : 'dashboards'}
              </span>{' '}
              in this workspace.
            </p>
          )}
        </header>

        <section aria-labelledby="home-quick-actions" className="mb-12">
          <h2 id="home-quick-actions" className="sr-only">
            Quick actions
          </h2>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <button
              type="button"
              onClick={() => setNavigationPage('data-connect')}
              className="group flex flex-col items-start gap-3 rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-4 text-left cursor-pointer transition-colors duration-200 hover:border-[var(--border-strong)] hover:bg-[var(--bg-elevated)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--brand)] motion-reduce:transition-none"
            >
              <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-[var(--brand-dim)] text-[var(--brand)]">
                <Plug className="h-5 w-5" aria-hidden />
              </span>
              <span>
                <span className="block text-sm font-semibold text-[var(--text-primary)]">Connect data</span>
                <span className="mt-0.5 block text-xs leading-snug text-[var(--text-secondary)]">
                  Add a source from the catalog and configure sync.
                </span>
              </span>
            </button>

            <button
              type="button"
              onClick={() => setNavigationPage('data-manage')}
              className="group flex flex-col items-start gap-3 rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-4 text-left cursor-pointer transition-colors duration-200 hover:border-[var(--border-strong)] hover:bg-[var(--bg-elevated)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--brand)] motion-reduce:transition-none"
            >
              <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-[var(--brand-dim)] text-[var(--brand)]">
                <RefreshCw className="h-5 w-5" aria-hidden />
              </span>
              <span>
                <span className="block text-sm font-semibold text-[var(--text-primary)]">
                  Manage connections
                </span>
                <span className="mt-0.5 block text-xs leading-snug text-[var(--text-secondary)]">
                  Pause, edit schedules, and review sync status.
                </span>
              </span>
            </button>

            <button
              type="button"
              onClick={() => setNavigationPage('data-raw-tables')}
              className="group flex flex-col items-start gap-3 rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-4 text-left cursor-pointer transition-colors duration-200 hover:border-[var(--border-strong)] hover:bg-[var(--bg-elevated)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--brand)] motion-reduce:transition-none"
            >
              <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-[var(--brand-dim)] text-[var(--brand)]">
                <Table className="h-5 w-5" aria-hidden />
              </span>
              <span>
                <span className="block text-sm font-semibold text-[var(--text-primary)]">Raw tables</span>
                <span className="mt-0.5 block text-xs leading-snug text-[var(--text-secondary)]">
                  Inspect synced tables and preview rows.
                </span>
              </span>
            </button>

            <button
              type="button"
              disabled={!primaryDashboardId}
              onClick={() => {
                if (primaryDashboardId) void openUserDashboard(primaryDashboardId);
              }}
              title={!primaryDashboardId ? 'Create or pick a dashboard from the sidebar' : undefined}
              className="group flex flex-col items-start gap-3 rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-4 text-left cursor-pointer transition-colors duration-200 hover:border-[var(--border-strong)] hover:bg-[var(--bg-elevated)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--brand)] motion-reduce:transition-none disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:border-[var(--border-default)] disabled:hover:bg-[var(--bg-surface)]"
            >
              <span className="flex h-10 w-10 items-center justify-center rounded-xl bg-[var(--brand-dim)] text-[var(--brand)]">
                <LayoutDashboard className="h-5 w-5" aria-hidden />
              </span>
              <span>
                <span className="block text-sm font-semibold text-[var(--text-primary)]">Open dashboard</span>
                <span className="mt-0.5 block text-xs leading-snug text-[var(--text-secondary)]">
                  {primaryDashboardId
                    ? 'Jump to your dashboard canvas and widgets.'
                    : 'Create one from the sidebar to get started.'}
                </span>
              </span>
            </button>
          </div>

          <div className="mt-4">
            <button
              type="button"
              onClick={() => openChat()}
              className="inline-flex w-full sm:w-auto items-center justify-center gap-2 rounded-xl border border-[var(--border-strong)] bg-[var(--bg-elevated)] px-4 py-2.5 text-sm font-medium text-[var(--text-primary)] cursor-pointer transition-colors duration-200 hover:bg-[var(--bg-surface-alt)] hover:border-[var(--brand)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--brand)] motion-reduce:transition-none"
            >
              <MessageSquare className="h-4 w-4 text-[var(--brand)]" aria-hidden />
              Ask the AI assistant
            </button>
          </div>
        </section>

        <section aria-labelledby="home-capabilities">
          <h2
            id="home-capabilities"
            className="text-xs font-semibold uppercase tracking-wider text-[var(--text-muted)] mb-4"
          >
            What you can do
          </h2>
          <ul className="grid gap-4 sm:grid-cols-2">
            <li className="rounded-2xl border border-[var(--border-subtle)] bg-[var(--bg-surface)]/80 p-5">
              <div className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-[var(--cyan)] mt-0.5" aria-hidden />
                <div>
                  <h3 className="text-sm font-semibold text-[var(--text-primary)]">Integrate & sync</h3>
                  <p className="mt-1 text-xs leading-relaxed text-[var(--text-secondary)]">
                    Pull data from databases, CRMs, ads, files, and more—then keep it fresh on a schedule you
                    control.
                  </p>
                </div>
              </div>
            </li>
            <li className="rounded-2xl border border-[var(--border-subtle)] bg-[var(--bg-surface)]/80 p-5">
              <div className="flex gap-3">
                <BarChart3 className="h-5 w-5 shrink-0 text-[var(--emerald)] mt-0.5" aria-hidden />
                <div>
                  <h3 className="text-sm font-semibold text-[var(--text-primary)]">Dashboards & visuals</h3>
                  <p className="mt-1 text-xs leading-relaxed text-[var(--text-secondary)]">
                    Combine KPI blocks and charts on a flexible grid; filter by date to focus the story.
                  </p>
                </div>
              </div>
            </li>
            <li className="rounded-2xl border border-[var(--border-subtle)] bg-[var(--bg-surface)]/80 p-5">
              <div className="flex gap-3">
                <Sparkles className="h-5 w-5 shrink-0 text-[var(--brand)] mt-0.5" aria-hidden />
                <div>
                  <h3 className="text-sm font-semibold text-[var(--text-primary)]">Natural language</h3>
                  <p className="mt-1 text-xs leading-relaxed text-[var(--text-secondary)]">
                    Ask questions in everyday language and get answers backed by your connected data.
                  </p>
                </div>
              </div>
            </li>
            <li className="rounded-2xl border border-[var(--border-subtle)] bg-[var(--bg-surface)]/80 p-5">
              <div className="flex gap-3">
                <FileDown className="h-5 w-5 shrink-0 text-[var(--indigo)] mt-0.5" aria-hidden />
                <div>
                  <h3 className="text-sm font-semibold text-[var(--text-primary)]">Reports & layout</h3>
                  <p className="mt-1 text-xs leading-relaxed text-[var(--text-secondary)]">
                    Build mixed reports, move blocks where you need them, and export when you are ready.
                  </p>
                </div>
              </div>
            </li>
          </ul>
        </section>
      </div>
    </DataPageFrame>
  );
};

export default HomePage;
