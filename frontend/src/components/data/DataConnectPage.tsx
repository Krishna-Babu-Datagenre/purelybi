import { useCallback, useDeferredValue, useEffect, useId, useMemo, useRef, useState } from 'react';
import { ArrowLeft, ExternalLink, Plug, Search, X } from 'lucide-react';
import type { ConnectorCatalogDetail, ConnectorCatalogListItem } from '../../types';
import { getConnectorCatalogDetail, listConnectorCatalog } from '../../services/backendClient';
import DataPageFrame from './DataPageFrame';
import OnboardingChatPanel from './OnboardingChatPanel';

/* ── Props ── */

interface DataConnectPageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

/* ── Category definitions ── */

interface Category {
  label: string;
  subtitle: string;
  keywords: string[];
}

const ALL_SOURCES_LABEL = 'All Data Sources';

const CATEGORIES: Category[] = [
  {
    label: 'Databases',
    subtitle: 'Connect databases, warehouses, and data lakes with a secure connection.',
    keywords: [
      'postgres', 'mysql', 'mssql', 'mongodb', 'clickhouse', 'bigquery',
      'snowflake', 'redshift', 'databricks', 'supabase', 'cockroach',
      'mariadb', 'oracle', 'duckdb', 'sqlite', 'dynamodb', 'elasticsearch',
      'couchbase', 'fauna', 'tidb', 'singlestore', 'planetscale',
      'db2', 'teradata', 'alloydb',
    ],
  },
  {
    label: 'CRM & Sales',
    subtitle: 'Sync customer data from your favorite CRM tools.',
    keywords: [
      'salesforce', 'hubspot', 'pipedrive', 'close-com', 'freshsales',
      'zoho-crm', 'copper', 'attio', 'insightly',
    ],
  },
  {
    label: 'Marketing & Ads',
    subtitle: 'Pull campaign performance and audience data.',
    keywords: [
      'google-ads', 'facebook-marketing', 'facebook-pages', 'linkedin-ads',
      'tiktok-marketing', 'pinterest', 'bing-ads', 'twitter', 'snapchat',
      'mailchimp', 'sendgrid', 'brevo', 'klaviyo', 'iterable', 'instagram',
      'surveymonkey', 'marketo', 'outreach',
    ],
  },
  {
    label: 'Analytics & Product',
    subtitle: 'Connect product analytics and tracking platforms.',
    keywords: [
      'google-analytics', 'mixpanel', 'amplitude', 'posthog', 'segment',
      'heap', 'plausible', 'google-search-console', 'semrush',
    ],
  },
  {
    label: 'Files & Storage',
    subtitle: 'Import data from files, cloud storage, and spreadsheets.',
    keywords: [
      's3', 'gcs', 'azure-blob', 'sftp', 'ftp', 'file', 'google-sheets',
      'airtable', 'notion', 'google-drive', 'smartsheets', 'excel',
    ],
  },
  {
    label: 'Developer Tools',
    subtitle: 'Integrate with version control and project management.',
    keywords: [
      'github', 'gitlab', 'jira', 'confluence', 'bitbucket', 'linear',
      'shortcut', 'pagerduty', 'datadog', 'sentry', 'sonar', 'circleci',
    ],
  },
  {
    label: 'E-commerce & Payments',
    subtitle: 'Sync orders, transactions, and product data.',
    keywords: [
      'shopify', 'woocommerce', 'stripe', 'square', 'paypal', 'chargebee',
      'recurly', 'braintree', 'amazon-seller', 'magento', 'bigcommerce',
      'recharge', 'lemonsqueezy',
    ],
  },
  {
    label: 'Communication & Support',
    subtitle: 'Connect customer support and communication platforms.',
    keywords: [
      'slack', 'intercom', 'zendesk', 'freshdesk', 'drift', 'twilio',
      'front', 'kustomer', 'dixa', 'talkdesk', 'gong',
    ],
  },
];

function categorize(items: ConnectorCatalogListItem[]) {
  const grouped: { category: Category; items: ConnectorCatalogListItem[] }[] = [];
  const placed = new Set<string>();

  for (const cat of CATEGORIES) {
    const matches = items.filter((item) => {
      if (placed.has(item.id)) return false;
      const repo = (item.docker_repository || '').toLowerCase();
      const name = (item.name || '').toLowerCase();
      return cat.keywords.some((k) => repo.includes(k) || name.includes(k));
    });
    if (matches.length > 0) {
      matches.forEach((m) => placed.add(m.id));
      grouped.push({ category: cat, items: matches });
    }
  }

  const remaining = items.filter((i) => !placed.has(i.id));
  if (remaining.length > 0) {
    grouped.push({
      category: {
        label: 'Other Sources',
        subtitle: 'Additional data sources from the connector registry.',
        keywords: [],
      },
      items: remaining,
    });
  }
  return grouped;
}

/** Match backend `list_connector_catalog` filter (substring on name / docker_repository). */
function filterCatalogByQuery(
  list: ConnectorCatalogListItem[],
  rawQuery: string,
): ConnectorCatalogListItem[] {
  const q = rawQuery.trim().toLowerCase();
  if (!q) return list;
  return list.filter((item) => {
    const name = (item.name || '').toLowerCase();
    const repo = (item.docker_repository || '').toLowerCase();
    return name.includes(q) || repo.includes(q);
  });
}

/* ── Branded spinner ── */

function Spinner({ size = 28, className = '' }: { size?: number; className?: string }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.5"
      strokeLinecap="round"
      className={`animate-spin text-[var(--brand)] ${className}`}
      aria-hidden
    >
      <path d="M21 12a9 9 0 1 1-6.219-8.56" />
    </svg>
  );
}

/* ── Connector icon with lazy-load + fallback ── */

function ConnectorIcon({ iconUrl, alt, size = 'md' }: { iconUrl: string | null | undefined; alt: string; size?: 'sm' | 'md' }) {
  const [broken, setBroken] = useState(false);
  const px = size === 'sm' ? 'h-6 w-6' : 'h-8 w-8';
  const dim = size === 'sm' ? 24 : 32;

  if (!iconUrl || broken) {
    return (
      <span
        className={`flex ${px} shrink-0 items-center justify-center rounded-lg bg-[var(--bg-surface-alt)] border border-[var(--border-subtle)]`}
        aria-hidden
      >
        <Plug className="text-[var(--text-muted)]" size={dim / 2} strokeWidth={1.6} />
      </span>
    );
  }

  return (
    <img
      src={iconUrl}
      alt={alt}
      loading="lazy"
      decoding="async"
      className={`${px} shrink-0 object-contain`}
      width={dim}
      height={dim}
      onError={() => setBroken(true)}
    />
  );
}

/* ── Skeleton that mirrors the actual grid shape ── */

function SkeletonGrid() {
  return (
    <div className="space-y-6" aria-busy="true" aria-label="Loading catalog">
      {[1, 2].map((s) => (
        <div
          key={s}
          className="animate-pulse rounded-2xl border border-[var(--border-subtle)] bg-[var(--bg-surface)]/80 p-5 sm:p-6"
        >
          <div className="space-y-1.5 mb-5 pb-5 border-b border-[var(--border-subtle)]">
            <div className="h-5 w-40 rounded-md bg-[var(--bg-elevated)]" />
            <div className="h-3.5 w-72 max-w-full rounded-md bg-[var(--bg-elevated)]" />
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
            {Array.from({ length: 8 }).map((_, i) => (
              <div
                key={i}
                className="h-[52px] rounded-xl bg-[var(--bg-elevated)]/60 border border-[var(--border-subtle)]"
              />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

/* ── JSON block for detail view ── */

function JsonBlock({
  label,
  value,
}: {
  label: string;
  value: Record<string, unknown> | null | undefined;
}) {
  const has = value && Object.keys(value).length > 0;
  return (
    <div className="space-y-2">
      <h4 className="text-xs font-semibold uppercase tracking-wide text-[var(--text-muted)]">
        {label}
      </h4>
      {has ? (
        <pre
          className="text-xs font-mono leading-relaxed rounded-xl border border-[var(--border-default)] bg-[var(--bg-canvas)] p-4 overflow-x-auto max-h-72 text-[var(--text-secondary)]"
          tabIndex={0}
        >
          {JSON.stringify(value, null, 2)}
        </pre>
      ) : (
        <p className="text-sm text-[var(--text-muted)] italic">
          Not available for this connector.
        </p>
      )}
    </div>
  );
}

/* ── Category filter chips (horizontal — avoids a second “sidebar” next to the app nav) ── */

function CategoryFilterChip({
  label,
  count,
  active,
  onClick,
}: {
  label: string;
  count?: number;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`
        inline-flex shrink-0 items-center gap-2 rounded-full border px-3.5 py-2 text-sm font-medium transition-colors duration-200 cursor-pointer
        focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--bg-canvas)]
        motion-reduce:transition-none
        ${active
          ? 'border-[var(--brand)] bg-[var(--brand)]/15 text-[var(--text-primary)] shadow-[inset_0_0_0_1px_rgba(139,92,246,0.35)]'
          : 'border-[var(--border-default)] bg-[var(--bg-surface)] text-[var(--text-secondary)] hover:border-[var(--border-strong)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)]'
        }
      `}
    >
      <span className="truncate max-w-[14rem] sm:max-w-none">{label}</span>
      {count !== undefined && (
        <span
          className={`
            tabular-nums text-xs rounded-md px-1.5 py-0.5
            ${active ? 'bg-[var(--brand)]/25 text-[var(--text-primary)]' : 'bg-[var(--bg-elevated)] text-[var(--text-muted)]'}
          `}
        >
          {count}
        </span>
      )}
    </button>
  );
}

/* ── Main page ── */

const DataConnectPage = ({
  sidebarCollapsed,
  chatOpen,
  chatModal,
  chatWidthPx,
}: DataConnectPageProps) => {
  const searchId = useId();
  const detailTitleId = useId();
  const detailRef = useRef<HTMLHeadingElement>(null);

  /* Step 1 state — full catalog loaded once; search filters client-side (instant, no races). */
  const [search, setSearch] = useState('');
  const deferredSearch = useDeferredValue(search);
  const [activeCategory, setActiveCategory] = useState<string>(ALL_SOURCES_LABEL);
  const [items, setItems] = useState<ConnectorCatalogListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<string | null>(null);
  const catalogFetchGen = useRef(0);

  /* Step 2 = connector detail; step 3 = guided onboarding chat */
  const [step, setStep] = useState<1 | 2 | 3>(1);
  const [selected, setSelected] = useState<ConnectorCatalogListItem | null>(null);
  const [detail, setDetail] = useState<ConnectorCatalogDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  /* Fetch full catalog once per visit (see `listConnectorCatalog` TTL cache in backendClient). */
  const loadCatalog = useCallback(async (forceRefresh = false) => {
    const id = ++catalogFetchGen.current;
    setListError(null);
    setLoading(true);
    try {
      const data = await listConnectorCatalog({
        activeOnly: true,
        forceRefresh,
      });
      if (id !== catalogFetchGen.current) return;
      setItems(data);
    } catch (e) {
      if (id !== catalogFetchGen.current) return;
      setItems([]);
      setListError(
        e instanceof Error ? e.message : 'Could not load the connector catalog.',
      );
    } finally {
      if (id === catalogFetchGen.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadCatalog();
  }, [loadCatalog]);

  const filteredItems = useMemo(
    () => filterCatalogByQuery(items, deferredSearch),
    [items, deferredSearch],
  );

  /* Grouped catalog for step 1 */
  const grouped = useMemo(() => categorize(filteredItems), [filteredItems]);

  /* Category counts (computed from grouped, not filtered by active category) */
  const categoryCounts = useMemo(() => {
    const counts = new Map<string, number>();
    counts.set(ALL_SOURCES_LABEL, filteredItems.length);
    for (const g of grouped) {
      counts.set(g.category.label, g.items.length);
    }
    return counts;
  }, [grouped, filteredItems.length]);

  /* Sidebar labels: All + each non-empty category */
  const sidebarLabels = useMemo(
    () => [ALL_SOURCES_LABEL, ...grouped.map((g) => g.category.label)],
    [grouped],
  );

  /* Visible groups based on activeCategory */
  const visibleGroups = useMemo(() => {
    if (activeCategory === ALL_SOURCES_LABEL) return grouped;
    return grouped.filter((g) => g.category.label === activeCategory);
  }, [grouped, activeCategory]);

  /* Step transitions — detail uses cache too */
  const openDetail = useCallback((item: ConnectorCatalogListItem) => {
    setSelected(item);
    setStep(2);
    setDetail(null);
    setDetailError(null);
    setDetailLoading(true);
    void (async () => {
      try {
        const row = await getConnectorCatalogDetail(item.id);
        setDetail(row);
      } catch (e) {
        setDetailError(
          e instanceof Error ? e.message : 'Could not load connector details.',
        );
      } finally {
        setDetailLoading(false);
      }
    })();
  }, []);

  const backToList = useCallback(() => {
    setStep(1);
    setSelected(null);
    setDetail(null);
    setDetailError(null);
    setDetailLoading(false);
  }, []);

  const backToDetail = useCallback(() => {
    setStep(2);
  }, []);

  /* Focus management */
  useEffect(() => {
    if (step === 2 && detailRef.current && !detailLoading) {
      detailRef.current.focus();
    }
  }, [step, detailLoading, detail]);

  /* Reset active category when search changes */
  useEffect(() => {
    setActiveCategory(ALL_SOURCES_LABEL);
  }, [deferredSearch]);

  return (
    <DataPageFrame
      sidebarCollapsed={sidebarCollapsed}
      chatOpen={chatOpen}
      chatModal={chatModal}
      chatWidthPx={chatWidthPx}
    >
        <div className="-m-6 w-[calc(100%+3rem)] h-[calc(100%+3rem)] flex flex-col">
          {/* ────────────────────────── Step 1: Catalog ────────────────────────── */}
          {step === 1 && (
            <div className="flex flex-col h-full">
              {/* Dialog-style header */}
              <header className="shrink-0 flex items-center justify-between border-b border-[var(--border-default)] px-6 py-4">
                <h2 className="text-lg font-semibold text-[var(--text-primary)] tracking-tight">
                  Connect your data source
                </h2>
                {/* X button is decorative for now — could wire to navigate away */}
                <button
                  type="button"
                  onClick={() => window.history.back()}
                  className="rounded-lg p-1.5 text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] transition-colors duration-150 cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
                  aria-label="Close"
                >
                  <X size={20} aria-hidden />
                </button>
              </header>

              {/* Single scroll column: search + horizontal category filters + grouped cards (no second sidebar) */}
              <div className="flex flex-1 min-h-0 flex-col overflow-hidden">
                <div className="flex-1 min-h-0 min-w-0 overflow-y-auto overflow-x-hidden px-6 py-5 space-y-6">
                  {/* Search + filters */}
                  <div className="space-y-4">
                    <div className="relative max-w-2xl">
                      <Search
                        className="absolute left-4 top-1/2 -translate-y-1/2 text-[var(--text-muted)] pointer-events-none"
                        size={18}
                        aria-hidden
                      />
                      <input
                        id={searchId}
                        type="search"
                        autoComplete="off"
                        placeholder="Search your data source"
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        className="w-full rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] pl-11 pr-5 py-3 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] transition-all duration-200 focus:outline-none focus:border-[var(--brand)] focus:ring-2 focus:ring-[var(--brand)]/20"
                        aria-label="Search data sources"
                      />
                    </div>

                    <div className="space-y-2">
                      <p className="text-xs font-medium uppercase tracking-wider text-[var(--text-muted)]">
                        Browse by category
                      </p>
                      <nav
                        className="data-connect-category-nav flex gap-2 overflow-x-auto overflow-y-hidden pb-1 -mx-1 px-1"
                        aria-label="Source categories"
                      >
                        {sidebarLabels.map((label) => (
                          <CategoryFilterChip
                            key={label}
                            label={label}
                            count={categoryCounts.get(label)}
                            active={activeCategory === label}
                            onClick={() => setActiveCategory(label)}
                          />
                        ))}
                      </nav>
                    </div>
                  </div>

                  {/* Error banner */}
                  {listError && (
                    <div
                      className="rounded-xl border border-red-500/25 bg-red-950/25 px-5 py-4 flex items-center justify-between gap-4"
                      role="alert"
                    >
                      <span className="text-sm text-red-200/90">{listError}</span>
                      <button
                        type="button"
                        className="shrink-0 rounded-lg border border-red-500/30 px-3.5 py-2 text-xs font-medium text-red-200 hover:bg-red-950/40 transition-colors duration-200 cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-red-400/50"
                        onClick={() => void loadCatalog(true)}
                      >
                        Retry
                      </button>
                    </div>
                  )}

                  {/* Loading state */}
                  {loading && (
                    <div className="space-y-6">
                      <div className="flex items-center gap-3 py-2">
                        <Spinner size={20} />
                        <span className="text-sm font-medium text-[var(--text-secondary)]">
                          Loading connector catalog…
                        </span>
                      </div>
                      <SkeletonGrid />
                    </div>
                  )}

                  {/* Empty: no rows in DB */}
                  {!loading && !listError && items.length === 0 && (
                    <div className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] px-6 py-12 text-center text-sm text-[var(--text-secondary)]">
                      No connectors in the catalog yet. Ask an admin to sync the connector registry.
                    </div>
                  )}

                  {/* Empty: search filtered everything */}
                  {!loading &&
                    !listError &&
                    items.length > 0 &&
                    filteredItems.length === 0 && (
                      <div className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] px-6 py-12 text-center text-sm text-[var(--text-secondary)]">
                        No connectors match &ldquo;{search}&rdquo;. Try a different term.
                      </div>
                    )}

                  {/* Category sections — each group in its own surface for clear hierarchy */}
                  {!loading && visibleGroups.length > 0 && (
                    <div className="space-y-6">
                      {visibleGroups.map(({ category, items: catItems }, groupIndex) => {
                        const headingId = `connector-group-${groupIndex}`;
                        return (
                          <section
                            key={category.label}
                            aria-labelledby={headingId}
                            className="rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)]/90 shadow-[0_1px_0_rgba(255,255,255,0.04)_inset] overflow-hidden"
                          >
                            <header className="px-5 py-4 sm:px-6 sm:py-5 border-b border-[var(--border-subtle)] bg-[var(--bg-elevated)]/30">
                              <h3
                                id={headingId}
                                className="text-base font-semibold text-[var(--text-primary)] tracking-tight"
                              >
                                {category.label}
                              </h3>
                              <p className="text-xs text-[var(--text-muted)] mt-1 leading-relaxed max-w-3xl">
                                {category.subtitle}
                              </p>
                            </header>

                            <div className="p-4 sm:p-5 sm:pt-4">
                              <div
                                className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3"
                                role="list"
                              >
                                {catItems.map((item) => (
                                  <button
                                    key={item.id}
                                    type="button"
                                    role="listitem"
                                    onClick={() => openDetail(item)}
                                    className="group flex items-center gap-3 min-w-0 w-full rounded-xl border border-[var(--border-subtle)] bg-[var(--bg-canvas)] px-3 py-2.5 sm:px-3.5 sm:py-3 text-left transition-all duration-200 hover:border-[var(--brand)]/45 hover:bg-[var(--bg-elevated)] hover:shadow-[0_2px_14px_rgba(139,92,246,0.12)] cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--bg-surface)] motion-reduce:transition-none"
                                  >
                                    <ConnectorIcon
                                      iconUrl={item.icon_url}
                                      alt={`${item.name} logo`}
                                    />
                                    <span
                                      className="text-sm font-medium text-[var(--text-primary)] leading-snug line-clamp-2 min-w-0 group-hover:text-[var(--text-primary)]"
                                      title={item.name}
                                    >
                                      {item.name}
                                    </span>
                                  </button>
                                ))}
                              </div>
                            </div>
                          </section>
                        );
                      })}
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* ────────────────────────── Step 2: Detail ────────────────────────── */}
          {step === 2 && selected && (
            <div className="w-full max-w-3xl mx-auto px-4 sm:px-8 pt-8 pb-10">
              <section aria-labelledby={detailTitleId} className="space-y-6">
                {/* Back link */}
                <button
                  type="button"
                  onClick={backToList}
                  className="inline-flex items-center gap-1.5 text-sm font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] transition-colors duration-200 cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] rounded"
                >
                  <ArrowLeft size={16} aria-hidden />
                  Back to catalog
                </button>

                {/* Connector header card */}
                <div className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-5 sm:p-6">
                  <div className="flex items-start gap-4">
                    <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl border border-[var(--border-default)] bg-[var(--bg-canvas)] overflow-hidden">
                      {selected.icon_url ? (
                        <img
                          src={selected.icon_url}
                          alt={`${selected.name} logo`}
                          className="h-8 w-8 object-contain"
                          width={32}
                          height={32}
                        />
                      ) : (
                        <Plug
                          className="text-[var(--text-muted)]"
                          size={22}
                          strokeWidth={1.5}
                        />
                      )}
                    </span>
                    <div className="min-w-0 flex-1 space-y-1">
                      <h2
                        ref={detailRef}
                        id={detailTitleId}
                        tabIndex={-1}
                        className="text-lg font-semibold text-[var(--text-primary)] tracking-tight outline-none"
                      >
                        {selected.name}
                      </h2>
                      <p className="text-xs font-mono text-[var(--text-muted)] break-all">
                        {selected.docker_repository}
                      </p>
                      {selected.documentation_url && (
                        <a
                          href={selected.documentation_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-1.5 text-sm text-[var(--brand)] hover:underline cursor-pointer transition-colors duration-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] rounded mt-1"
                        >
                          Documentation
                          <ExternalLink size={13} aria-hidden />
                        </a>
                      )}
                    </div>
                  </div>
                </div>

                {/* Detail loading */}
                {detailLoading && (
                  <div className="flex flex-col items-center justify-center gap-3 py-12" aria-busy="true">
                    <Spinner size={32} />
                    <p className="text-sm font-medium text-[var(--text-secondary)]">
                      Loading connector schema…
                    </p>
                  </div>
                )}

                {/* Detail error */}
                {detailError && (
                  <div
                    className="rounded-xl border border-red-500/25 bg-red-950/25 px-5 py-4 flex items-center justify-between gap-4"
                    role="alert"
                  >
                    <span className="text-sm text-red-200/90">{detailError}</span>
                    <button
                      type="button"
                      className="shrink-0 rounded-lg border border-red-500/30 px-3.5 py-2 text-xs font-medium text-red-200 hover:bg-red-950/40 cursor-pointer transition-colors duration-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-red-400/50"
                      onClick={() => openDetail(selected)}
                    >
                      Retry
                    </button>
                  </div>
                )}

                {/* Detail panels */}
                {!detailLoading && detail && !detailError && (
                  <div className="space-y-6">
                    <JsonBlock
                      label="Configuration schema"
                      value={detail.config_schema ?? null}
                    />
                    <JsonBlock
                      label="OAuth configuration"
                      value={detail.oauth_config ?? null}
                    />

                    <div className="rounded-xl border border-[var(--border-strong)] bg-[var(--bg-surface-alt)] px-5 py-5 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
                      <p className="text-sm text-[var(--text-secondary)] leading-relaxed flex-1">
                        Start the guided assistant to enter credentials, run checks, and save your
                        connection to your profile.
                      </p>
                      <button
                        type="button"
                        onClick={() => setStep(3)}
                        className="shrink-0 rounded-xl bg-[var(--brand)] text-white font-medium px-5 py-3 text-sm hover:opacity-95 transition-opacity cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--bg-surface-alt)]"
                      >
                        Start guided setup
                      </button>
                    </div>

                    <p className="text-xs text-[var(--text-muted)]">
                      You can also manage existing connections under{' '}
                      <span className="text-[var(--text-primary)] font-medium">Data → Manage</span>.
                    </p>
                  </div>
                )}
              </section>
            </div>
          )}

          {step === 3 && selected && detail && !detailError && (
            <section className="w-full max-w-3xl mx-auto px-4 sm:px-8 pt-8 pb-10" aria-label="Guided onboarding">
              <OnboardingChatPanel
                catalogConnectorId={selected.id}
                connectorName={selected.name}
                onBack={backToDetail}
              />
            </section>
          )}
        </div>
    </DataPageFrame>
  );
};

export default DataConnectPage;
