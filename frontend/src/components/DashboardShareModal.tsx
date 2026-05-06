import { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { X, Users, Loader2, Shield, AlertCircle } from 'lucide-react';
import { useAuthStore } from '../store/useAuthStore';
import { shareDashboard, listDashboardShares, revokeDashboardShare } from '../services/backendClient';

interface DashboardShareModalProps {
  dashboardId: string;
  dashboardName: string;
  onClose: () => void;
}

export default function DashboardShareModal({ dashboardId, dashboardName, onClose }: DashboardShareModalProps) {
  const user = useAuthStore((s) => s.user);
  const tier = user?.subscription_tier?.tier_name?.toLowerCase() || 'free';
  
  const [shares, setShares] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  const [email, setEmail] = useState('');
  const [permission, setPermission] = useState('read');
  const [sharing, setSharing] = useState(false);

  const canShare = tier !== 'free' && tier !== 'starter';
  const canEditShare = tier === 'growth' || tier === 'enterprise';

  useEffect(() => {
    if (!canShare) {
      setLoading(false);
      return;
    }
    listDashboardShares(dashboardId)
      .then(setShares)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [dashboardId, canShare]);

  const handleShare = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email.trim() || !canShare) return;
    
    setSharing(true);
    setError(null);
    try {
      await shareDashboard(dashboardId, email.trim(), permission);
      setEmail('');
      const updated = await listDashboardShares(dashboardId);
      setShares(updated);
    } catch (err: any) {
      setError(err.message || 'Failed to share dashboard');
    } finally {
      setSharing(false);
    }
  };

  const handleRevoke = async (shareId: string) => {
    try {
      await revokeDashboardShare(dashboardId, shareId);
      setShares((s) => s.filter((x) => x.id !== shareId));
    } catch (err: any) {
      setError(err.message || 'Failed to revoke share');
    }
  };

  return createPortal(
    <div
      className="fixed inset-0 z-[200] flex items-center justify-center p-4 bg-black/55 backdrop-blur-[2px]"
      role="presentation"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        className="w-full max-w-md rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-6 shadow-2xl shadow-black/50 relative overflow-hidden flex flex-col max-h-[90vh]"
        role="dialog"
        aria-modal="true"
      >
        <button
          type="button"
          className="absolute top-4 right-4 text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors rounded"
          onClick={onClose}
        >
          <X size={20} />
        </button>

        <div className="flex items-center gap-3 mb-6">
          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[var(--brand)]/10 text-[var(--brand)]">
            <Users size={20} />
          </div>
          <div>
            <h2 className="text-lg font-semibold text-[var(--text-primary)] tracking-tight">Share Dashboard</h2>
            <p className="text-sm text-[var(--text-muted)] truncate max-w-[250px]">{dashboardName}</p>
          </div>
        </div>

        {!canShare ? (
          <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 p-4 flex gap-3 text-amber-500 mb-2">
            <AlertCircle size={20} className="shrink-0 mt-0.5" />
            <div className="text-sm leading-relaxed text-amber-200">
              Dashboard sharing is not available on your current {user?.subscription_tier?.tier_name} plan.
              Please upgrade to the Pro plan or higher to share dashboards with your team.
            </div>
          </div>
        ) : (
          <>
            <form onSubmit={handleShare} className="space-y-3 mb-6">
              <label className="block text-sm font-medium text-[var(--text-primary)]">Invite by email</label>
              <div className="flex gap-2">
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="colleague@company.com"
                  className="flex-1 rounded-xl border border-[var(--border-default)] bg-[var(--bg-canvas)] px-3 py-2 text-sm text-[var(--text-primary)] focus:border-[var(--brand)] focus:ring-1 focus:ring-[var(--brand)] outline-none"
                  required
                />
                <select
                  value={permission}
                  onChange={(e) => setPermission(e.target.value)}
                  disabled={!canEditShare}
                  className="rounded-xl border border-[var(--border-default)] bg-[var(--bg-canvas)] px-3 py-2 text-sm text-[var(--text-primary)] focus:border-[var(--brand)] focus:ring-1 focus:ring-[var(--brand)] outline-none disabled:opacity-50"
                  title={!canEditShare ? "Edit permissions require Growth plan" : undefined}
                >
                  <option value="read">Read-only</option>
                  {canEditShare && <option value="edit">Can Edit</option>}
                </select>
              </div>
              {error && <div className="text-xs text-red-400">{error}</div>}
              <button
                type="submit"
                disabled={sharing || !email.trim()}
                className="w-full inline-flex items-center justify-center gap-2 rounded-xl bg-[var(--brand)] px-4 py-2.5 text-sm font-medium text-white hover:bg-[var(--brand-hover)] transition-colors shadow-sm disabled:opacity-50"
              >
                {sharing && <Loader2 size={16} className="animate-spin" />}
                Send Invite
              </button>
            </form>

            <div className="flex-1 overflow-y-auto min-h-0 border-t border-[var(--border-subtle)] pt-4">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-muted)] mb-3">People with access</h3>
              {loading ? (
                <div className="flex items-center justify-center py-4"><Loader2 size={16} className="animate-spin text-[var(--text-muted)]" /></div>
              ) : shares.length === 0 ? (
                <div className="text-sm text-[var(--text-muted)] text-center py-4">This dashboard hasn't been shared with anyone yet.</div>
              ) : (
                <div className="space-y-2">
                  {shares.map((share) => (
                    <div key={share.id} className="flex items-center justify-between p-2 rounded-lg hover:bg-[var(--bg-elevated)] transition-colors">
                      <div className="flex items-center gap-3">
                        <div className="h-8 w-8 rounded-full bg-[var(--bg-surface-alt)] border border-[var(--border-default)] flex items-center justify-center text-xs font-medium text-[var(--text-secondary)]">
                          {share.shared_with_email.charAt(0).toUpperCase()}
                        </div>
                        <div className="flex flex-col">
                          <span className="text-sm font-medium text-[var(--text-primary)]">{share.shared_with_email}</span>
                          <span className="text-xs text-[var(--text-muted)] flex items-center gap-1">
                            <Shield size={10} />
                            {share.permission_tier === 'edit' ? 'Can Edit' : 'Read-only'}
                          </span>
                        </div>
                      </div>
                      <button
                        type="button"
                        onClick={() => handleRevoke(share.id)}
                        className="text-xs font-medium text-[var(--text-muted)] hover:text-red-400 transition-colors p-1"
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>,
    document.body,
  );
}
