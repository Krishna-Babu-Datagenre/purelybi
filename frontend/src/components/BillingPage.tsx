import { useState, useEffect } from 'react';
import { useAuthStore } from '../store/useAuthStore';
import {
  createBillingPortalSession,
  createSubscriptionCheckout,
  createTopupCheckout,
  fetchBillingSummary,
  fetchSelfServePlans,
} from '../services/billingApi';
import {
  CreditCard,
  Sparkles,
  Database,
  LayoutDashboard,
  Clock,
  HardDrive,
  Check,
  Loader2,
  Crown,
  Zap,
  Rocket,
  Building2,
  Star,
} from 'lucide-react';
import type { BillingSelfServePlan, BillingSummary, BillingTopupPack } from '../types';

interface BillingPageProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
}

/** Map tier names to accent colors and icons for the plan cards */
const TIER_THEME: Record<string, { gradient: string; icon: typeof Star; accentVar: string }> = {
  Free:       { gradient: 'linear-gradient(135deg, #334155 0%, #1e293b 100%)', icon: Star,      accentVar: '#94a3b8' },
  Starter:    { gradient: 'linear-gradient(135deg, #1e40af 0%, #1e3a5f 100%)', icon: Zap,       accentVar: '#60a5fa' },
  Pro:        { gradient: 'linear-gradient(135deg, #7c3aed 0%, #4c1d95 100%)', icon: Crown,     accentVar: '#a78bfa' },
  Growth:     { gradient: 'linear-gradient(135deg, #059669 0%, #064e3b 100%)', icon: Rocket,    accentVar: '#34d399' },
  Enterprise: { gradient: 'linear-gradient(135deg, #b45309 0%, #78350f 100%)', icon: Building2, accentVar: '#fbbf24' },
};

function formatSyncFrequency(minutes: number): string {
  if (minutes >= 1440) return `${Math.round(minutes / 1440)}d`;
  if (minutes >= 60) return `${Math.round(minutes / 60)}h`;
  return `${minutes}m`;
}

function formatLimit(value: number): string {
  if (value >= 999999) return 'Unlimited';
  return value.toLocaleString();
}

function formatUsd(value: number): string {
  return `$${value.toFixed(2)}`;
}

function topupLabel(packCode: string, creditsGranted: number): string {
  const normalized = packCode.replace(/[_-]+/g, ' ').trim();
  if (!normalized) return `${creditsGranted} AI Credits`;
  return normalized.replace(/\b\w/g, (c) => c.toUpperCase());
}

const BillingPage = ({ sidebarCollapsed, chatOpen, chatModal, chatWidthPx }: BillingPageProps) => {
  const user = useAuthStore((s) => s.user);
  const [plans, setPlans] = useState<BillingSelfServePlan[]>([]);
  const [topupPacks, setTopupPacks] = useState<BillingTopupPack[]>([]);
  const [summary, setSummary] = useState<BillingSummary | null>(null);
  const [selectedInterval, setSelectedInterval] = useState<BillingSelfServePlan['billing_interval']>('month');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [processingKey, setProcessingKey] = useState<string | null>(null);

  const currentTierName = summary?.plan_tier || user?.subscription_tier?.tier_name || 'Free';

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    Promise.all([fetchSelfServePlans(), fetchBillingSummary()])
      .then(([plansRes, summaryRes]) => {
        if (cancelled) return;
        setPlans(plansRes.plans);
        setTopupPacks(plansRes.topup_packs);
        setSummary(summaryRes);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Failed to load plans');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, []);

  const creditsBalance = summary?.ai_credits_balance ?? user?.ai_credits_balance ?? 0;
  const currentPlan = plans.find((p) => p.plan_tier === currentTierName);
  const creditsTotal = currentPlan?.included_ai_credits ?? 25;
  const creditsPercent = creditsTotal > 0 ? Math.min(100, Math.round((creditsBalance / creditsTotal) * 100)) : 0;

  const dashboardCount = user?.dashboard_count ?? 0;
  const dashboardLimit = user?.subscription_tier?.max_dashboards ?? 1;
  const dashboardPercent = dashboardLimit >= 999999 ? 5 : Math.min(100, Math.round((dashboardCount / dashboardLimit) * 100));

  const connectorCount = user?.active_connector_count ?? 0;
  const connectorLimit = user?.subscription_tier?.max_data_sources ?? 1;
  const connectorPercent = connectorLimit >= 999999 ? 5 : Math.min(100, Math.round((connectorCount / connectorLimit) * 100));

  const visiblePlans = plans.filter((plan) => plan.billing_interval === selectedInterval);

  const handleSubscriptionCheckout = async (plan: BillingSelfServePlan) => {
    try {
      setProcessingKey(`plan:${plan.price_lookup_key}`);
      const session = await createSubscriptionCheckout(plan.plan_tier, plan.billing_interval);
      window.location.assign(session.checkout_url);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create checkout session');
    } finally {
      setProcessingKey(null);
    }
  };

  const handleTopupCheckout = async (pack: BillingTopupPack) => {
    try {
      setProcessingKey(`topup:${pack.pack_code}`);
      const session = await createTopupCheckout(pack.pack_code);
      window.location.assign(session.checkout_url);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create top-up checkout session');
    } finally {
      setProcessingKey(null);
    }
  };

  const handleOpenPortal = async () => {
    try {
      setProcessingKey('portal');
      const session = await createBillingPortalSession();
      window.location.assign(session.portal_url);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to open billing portal');
    } finally {
      setProcessingKey(null);
    }
  };

  return (
    <div
      className="billing-page"
      style={{
        position: 'fixed',
        top: 'var(--topbar-height)',
        left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
        right: chatOpen && !chatModal ? chatWidthPx : 0,
        bottom: 0,
        overflowY: 'auto',
        padding: '2rem 2.5rem',
      }}
    >
      <div className="billing-page__container">
        {/* Header */}
        <div className="billing-page__header">
          <h1 className="billing-page__title">Billing & Plans</h1>
          <p className="billing-page__subtitle">Manage your subscription and view usage</p>
          <div className="billing-page__plan-btn-wrap" style={{ marginTop: '0.75rem', maxWidth: 280 }}>
            <button
              type="button"
              className="billing-page__plan-btn billing-page__plan-btn--upgrade"
              onClick={handleOpenPortal}
              disabled={processingKey === 'portal'}
            >
              <CreditCard size={16} />
              {processingKey === 'portal' ? 'Opening…' : 'Open Billing Portal'}
            </button>
          </div>
        </div>

        {/* Current Plan Overview */}
        <div className="billing-page__current-plan">
          <div className="billing-page__plan-badge-row">
            <div className="billing-page__plan-icon-wrap" style={{ background: TIER_THEME[currentTierName]?.gradient || TIER_THEME.Free.gradient }}>
              {(() => {
                const Icon = TIER_THEME[currentTierName]?.icon || Star;
                return <Icon size={22} strokeWidth={2} />;
              })()}
            </div>
            <div>
              <div className="billing-page__plan-name">{currentTierName} Plan</div>
              <div className="billing-page__plan-status">
                {user?.trial_ends_at ? (
                  <>Trial ends {new Date(user.trial_ends_at).toLocaleDateString()}</>
                ) : (
                  'Active'
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Usage Cards */}
        <div className="billing-page__usage-grid">
          {/* AI Credits */}
          <div className="billing-page__usage-card">
            <div className="billing-page__usage-header">
              <Sparkles size={18} className="billing-page__usage-icon billing-page__usage-icon--purple" />
              <span className="billing-page__usage-label">AI Credits</span>
            </div>
            <div className="billing-page__usage-value">
              <span className="billing-page__usage-current">{creditsBalance}</span>
              <span className="billing-page__usage-sep">/</span>
              <span className="billing-page__usage-limit">{formatLimit(creditsTotal)}</span>
            </div>
            <div className="billing-page__progress-track">
              <div
                className="billing-page__progress-bar billing-page__progress-bar--purple"
                style={{ width: `${creditsPercent}%` }}
              />
            </div>
          </div>

          {/* Dashboards */}
          <div className="billing-page__usage-card">
            <div className="billing-page__usage-header">
              <LayoutDashboard size={18} className="billing-page__usage-icon billing-page__usage-icon--blue" />
              <span className="billing-page__usage-label">Dashboards</span>
            </div>
            <div className="billing-page__usage-value">
              <span className="billing-page__usage-current">{dashboardCount}</span>
              <span className="billing-page__usage-sep">/</span>
              <span className="billing-page__usage-limit">{formatLimit(dashboardLimit)}</span>
            </div>
            <div className="billing-page__progress-track">
              <div
                className="billing-page__progress-bar billing-page__progress-bar--blue"
                style={{ width: `${dashboardPercent}%` }}
              />
            </div>
          </div>

          {/* Data Sources */}
          <div className="billing-page__usage-card">
            <div className="billing-page__usage-header">
              <Database size={18} className="billing-page__usage-icon billing-page__usage-icon--green" />
              <span className="billing-page__usage-label">Data Sources</span>
            </div>
            <div className="billing-page__usage-value">
              <span className="billing-page__usage-current">{connectorCount}</span>
              <span className="billing-page__usage-sep">/</span>
              <span className="billing-page__usage-limit">{formatLimit(connectorLimit)}</span>
            </div>
            <div className="billing-page__progress-track">
              <div
                className="billing-page__progress-bar billing-page__progress-bar--green"
                style={{ width: `${connectorPercent}%` }}
              />
            </div>
          </div>
        </div>

        {/* Plans Comparison */}
        <div className="billing-page__plans-section">
          <div className="billing-page__plans-head">
            <div>
              <h2 className="billing-page__section-title">Available Plans</h2>
              <p className="billing-page__section-desc">Compare plans and choose the best fit for your needs.</p>
            </div>
            <div className="billing-page__interval-toggle" role="tablist" aria-label="Billing interval">
              <button
                type="button"
                role="tab"
                aria-selected={selectedInterval === 'month'}
                className={`billing-page__interval-btn ${selectedInterval === 'month' ? 'billing-page__interval-btn--active' : ''}`}
                onClick={() => setSelectedInterval('month')}
              >
                Monthly
              </button>
              <button
                type="button"
                role="tab"
                aria-selected={selectedInterval === 'year'}
                className={`billing-page__interval-btn ${selectedInterval === 'year' ? 'billing-page__interval-btn--active' : ''}`}
                onClick={() => setSelectedInterval('year')}
              >
                Yearly
              </button>
            </div>
          </div>

          {loading && (
            <div className="billing-page__plans-loading">
              <Loader2 size={24} className="animate-spin" />
              <span>Loading plans…</span>
            </div>
          )}

          {error && (
            <div className="billing-page__plans-error">
              <span>{error}</span>
            </div>
          )}

          {!loading && !error && (
            <div className="billing-page__plans-grid">
              {visiblePlans.map((plan) => {
                const isCurrent = plan.plan_tier === currentTierName;
                const theme = TIER_THEME[plan.plan_tier] || TIER_THEME.Free;
                const PlanIcon = theme.icon;
                const isUpgrade = !isCurrent;
                const key = `plan:${plan.price_lookup_key}`;
                const processing = processingKey === key;

                return (
                  <div
                    key={key}
                    className={`billing-page__plan-card ${isCurrent ? 'billing-page__plan-card--current' : ''}`}
                    style={{ '--plan-accent': theme.accentVar } as React.CSSProperties}
                  >
                    {isCurrent && <div className="billing-page__plan-card-badge">Current Plan</div>}
                    <div className="billing-page__plan-card-icon" style={{ background: theme.gradient }}>
                      <PlanIcon size={24} strokeWidth={1.8} />
                    </div>
                    <h3 className="billing-page__plan-card-name">{plan.plan_tier}</h3>
                    <div className="billing-page__plan-status" style={{ marginTop: '-0.25rem', marginBottom: '0.75rem' }}>
                      {formatUsd(plan.amount_usd)} / {plan.billing_interval}
                    </div>

                    <ul className="billing-page__plan-features">
                      <li>
                        <Sparkles size={14} />
                        <span><strong>{formatLimit(plan.included_ai_credits)}</strong> AI credits</span>
                      </li>
                      <li>
                        <LayoutDashboard size={14} />
                        <span><strong>{formatLimit(plan.max_dashboards)}</strong> dashboards</span>
                      </li>
                      <li>
                        <Database size={14} />
                        <span><strong>{formatLimit(plan.max_data_sources)}</strong> data sources</span>
                      </li>
                      <li>
                        <HardDrive size={14} />
                        <span><strong>{formatLimit(plan.max_storage_mb)}</strong> MB storage</span>
                      </li>
                      <li>
                        <Clock size={14} />
                        <span>Sync every <strong>{formatSyncFrequency(plan.min_sync_frequency_minutes)}</strong></span>
                      </li>
                    </ul>

                    {isCurrent ? (
                      <button type="button" className="billing-page__plan-btn billing-page__plan-btn--current" disabled>
                        <Check size={16} />
                        Current Plan
                      </button>
                    ) : (
                      <div className="billing-page__plan-btn-wrap">
                        <button
                          type="button"
                          className="billing-page__plan-btn billing-page__plan-btn--upgrade"
                          onClick={() => void handleSubscriptionCheckout(plan)}
                          disabled={processing}
                        >
                          <CreditCard size={16} />
                          {processing ? 'Redirecting…' : isUpgrade ? 'Upgrade' : 'Select'}
                        </button>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {!loading && !error && visiblePlans.length === 0 && (
            <div className="billing-page__plans-error">
              <span>No {selectedInterval} plans are configured yet.</span>
            </div>
          )}
        </div>

        {!!topupPacks.length && (
          <div className="billing-page__plans-section" style={{ marginTop: '2rem' }}>
            <h2 className="billing-page__section-title">Buy AI Credits</h2>
            <p className="billing-page__section-desc">Purchase additional credits when your included balance is exhausted.</p>
            <div className="billing-page__plans-grid">
              {topupPacks.map((pack) => {
                const key = `topup:${pack.pack_code}`;
                const processing = processingKey === key;
                return (
                  <div key={pack.pack_code} className="billing-page__plan-card">
                    <div className="billing-page__plan-card-icon" style={{ background: TIER_THEME.Starter.gradient }}>
                      <Sparkles size={24} strokeWidth={1.8} />
                    </div>
                    <h3 className="billing-page__plan-card-name">{topupLabel(pack.pack_code, pack.credits_granted)}</h3>
                    <div className="billing-page__plan-status" style={{ marginTop: '-0.25rem', marginBottom: '0.75rem' }}>
                      {formatUsd(pack.amount_usd)} one-time
                    </div>
                    <ul className="billing-page__plan-features">
                      <li>
                        <Sparkles size={14} />
                        <span><strong>{formatLimit(pack.credits_granted)}</strong> AI credits</span>
                      </li>
                    </ul>
                    <div className="billing-page__plan-btn-wrap">
                      <button
                        type="button"
                        className="billing-page__plan-btn billing-page__plan-btn--upgrade"
                        onClick={() => void handleTopupCheckout(pack)}
                        disabled={processing}
                      >
                        <CreditCard size={16} />
                        {processing ? 'Redirecting…' : 'Buy Credits'}
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default BillingPage;
