import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { AlertCircle, CreditCard, X } from 'lucide-react';
import { useAuthStore } from '../store/useAuthStore';

export default function OutOfCreditsModal() {
  const [isOpen, setIsOpen] = useState(false);
  const user = useAuthStore((s) => s.user);

  useEffect(() => {
    const handleEvent = () => setIsOpen(true);
    window.addEventListener('out-of-credits', handleEvent);
    return () => window.removeEventListener('out-of-credits', handleEvent);
  }, []);

  if (!isOpen) return null;

  return createPortal(
    <div
      className="fixed inset-0 z-[200] flex items-center justify-center p-4 bg-black/55 backdrop-blur-[2px]"
      role="presentation"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) setIsOpen(false);
      }}
    >
      <div
        className="w-full max-w-md rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] p-6 shadow-2xl shadow-black/50 relative overflow-hidden"
        role="dialog"
        aria-modal="true"
        aria-labelledby="out-of-credits-title"
        aria-describedby="out-of-credits-desc"
      >
        <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-red-500 to-orange-500" />
        <button
          type="button"
          className="absolute top-4 right-4 text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] rounded"
          onClick={() => setIsOpen(false)}
          aria-label="Close"
        >
          <X size={20} />
        </button>

        <div className="flex items-start gap-4">
          <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-full bg-red-500/10 text-red-500">
            <AlertCircle size={24} />
          </div>
          <div className="space-y-2 flex-1 mt-1">
            <h2 id="out-of-credits-title" className="text-xl font-semibold text-[var(--text-primary)] tracking-tight">
              Out of AI Credits
            </h2>
            <p id="out-of-credits-desc" className="text-sm leading-relaxed text-[var(--text-secondary)]">
              You've exhausted your AI credit allowance for the {user?.subscription_tier?.tier_name || 'current'} plan.
              Please upgrade or purchase more credits to continue using AI tools.
            </p>
          </div>
        </div>

        <div className="mt-8 flex flex-col sm:flex-row gap-3">
          <button
            type="button"
            className="flex-1 rounded-xl border border-[var(--border-strong)] bg-[var(--bg-canvas)] px-4 py-2.5 text-sm font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] transition-colors"
            onClick={() => setIsOpen(false)}
          >
            Cancel
          </button>
          <button
            type="button"
            className="flex-1 inline-flex items-center justify-center gap-2 rounded-xl bg-[var(--brand)] px-4 py-2.5 text-sm font-medium text-white hover:bg-[var(--brand-hover)] transition-colors shadow-sm"
            onClick={() => {
              // TODO: Integrate Stripe billing portal link here
              window.alert('Redirecting to billing portal...');
              setIsOpen(false);
            }}
          >
            <CreditCard size={16} />
            Upgrade Plan
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
