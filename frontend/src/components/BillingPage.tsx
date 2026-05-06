import { useState, useEffect } from 'react';
import { useAuthStore } from '../store/useAuthStore';
import { fetchSubscriptionPlans } from '../services/authApi';
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
import type { SubscriptionPlan } from '../types';

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

const BillingPage = ({ sidebarCollapsed, chatOpen, chatModal, chatWidthPx }: BillingPageProps) => {
  const user = useAuthStore((s) => s.user);
  const [plans, setPlans] = useState<SubscriptionPlan[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const currentTierName = user?.subscription_tier?.tier_name || 'Free';

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchSubscriptionPlans()
      .then((data) => {
        if (!cancelled) setPlans(data);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Failed to load plans');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, []);

  const creditsBalance = user?.ai_credits_balance ?? 0;
  const currentPlan = plans.find((p) => p.tier_name === currentTierName);
  const creditsTotal = currentPlan?.included_ai_credits ?? 25;
  const creditsPercent = creditsTotal > 0 ? Math.min(100, Math.round((creditsBalance / creditsTotal) * 100)) : 0;

  const dashboardCount = user?.dashboard_count ?? 0;
  const dashboardLimit = user?.subscription_tier?.max_dashboards ?? 1;
  const dashboardPercent = dashboardLimit >= 999999 ? 5 : Math.min(100, Math.round((dashboardCount / dashboardLimit) * 100));

  const connectorCount = user?.active_connector_count ?? 0;
  const connectorLimit = user?.subscription_tier?.max_data_sources ?? 1;
  const connectorPercent = connectorLimit >= 999999 ? 5 : Math.min(100, Math.round((connectorCount / connectorLimit) * 100));

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
          <h2 className="billing-page__section-title">Available Plans</h2>
          <p className="billing-page__section-desc">Compare plans and choose the best fit for your needs.</p>

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
              {plans.map((plan) => {
                const isCurrent = plan.tier_name === currentTierName;
                const theme = TIER_THEME[plan.tier_name] || TIER_THEME.Free;
                const PlanIcon = theme.icon;
                const isUpgrade = !isCurrent;

                return (
                  <div
                    key={plan.id}
                    className={`billing-page__plan-card ${isCurrent ? 'billing-page__plan-card--current' : ''}`}
                    style={{ '--plan-accent': theme.accentVar } as React.CSSProperties}
                  >
                    {isCurrent && <div className="billing-page__plan-card-badge">Current Plan</div>}
                    <div className="billing-page__plan-card-icon" style={{ background: theme.gradient }}>
                      <PlanIcon size={24} strokeWidth={1.8} />
                    </div>
                    <h3 className="billing-page__plan-card-name">{plan.tier_name}</h3>

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
                          disabled
                          title="Payment integration coming soon"
                        >
                          <CreditCard size={16} />
                          {isUpgrade ? 'Upgrade' : 'Select'}
                        </button>
                        <span className="billing-page__coming-soon">Coming Soon</span>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default BillingPage;
