import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { LayoutDashboard, LogOut, UserX } from 'lucide-react';
import { useChatStore } from '../store/useChatStore';
import { useAuthStore } from '../store/useAuthStore';
import { deleteAccount } from '../services/authApi';

interface TopbarProps {
  sidebarCollapsed: boolean;
  title: string;
  subtitle?: string;
}

const Topbar = ({ sidebarCollapsed, title, subtitle }: TopbarProps) => {
  const leftOffset = sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)';
  const toggleChat = useChatStore((s) => s.toggleChat);
  const isChatOpen = useChatStore((s) => s.isOpen);
  const user = useAuthStore((s) => s.user);
  const accessToken = useAuthStore((s) => s.accessToken);
  const logout = useAuthStore((s) => s.logout);
  const displayName = user?.full_name || user?.email || 'User';
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleteInProgress, setDeleteInProgress] = useState(false);
  const userMenuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!userMenuOpen) return;
    const handle = (e: MouseEvent) => {
      if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) {
        setUserMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, [userMenuOpen]);

  useEffect(() => {
    if (!deleteDialogOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setDeleteDialogOpen(false);
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [deleteDialogOpen]);

  return (
    <div className="topbar" style={{ left: leftOffset }}>
      {/* Left: Brand */}
      <div className="topbar-brand">
        <div className="topbar-logo">
          <LayoutDashboard size={18} />
        </div>
        <div className="flex items-center gap-2">
          <span className="topbar-title">{title}</span>
          {subtitle && (
            <>
              <span className="text-[var(--text-muted)]">·</span>
              <span className="topbar-subtitle">{subtitle}</span>
            </>
          )}
        </div>
      </div>

      {/* Right: Actions */}
      <div className="topbar-actions">
        <button
          type="button"
          className={`topbar-btn topbar-btn--copilot flex items-center gap-2.5 ${isChatOpen ? 'bg-[var(--brand-dim)] border-[var(--border-strong)] text-[var(--brand)]' : ''}`}
          onClick={toggleChat}
          title="Copilot"
        >
          <img
            src="https://img.icons8.com/?size=48&id=1G3UNEZHMjPH&format=png&color=FFFFFF"
            alt=""
            className="w-5 h-5 object-contain"
            width={20}
            height={20}
          />
          <span className="text-sm font-medium">{"\u00A0"}Copilot</span>
        </button>
        <div className="relative" ref={userMenuRef}>
          <button
            type="button"
            className="topbar-avatar topbar-avatar--trigger"
            title={displayName}
            aria-expanded={userMenuOpen}
            aria-haspopup="menu"
            onClick={() => setUserMenuOpen((o) => !o)}
          >
            {displayName.slice(0, 2).toUpperCase()}
          </button>
          {userMenuOpen && (
            <div
              className="topbar-user-menu"
              role="menu"
              aria-label="Account menu"
            >
              <div className="topbar-user-menu__identity" role="none">
                <span className="topbar-user-menu__name">{displayName}</span>
                {user?.email && displayName !== user.email && (
                  <span className="topbar-user-menu__email">{user.email}</span>
                )}
              </div>
              <button
                type="button"
                className="topbar-user-menu__item"
                role="menuitem"
                onClick={() => {
                  setUserMenuOpen(false);
                  logout();
                }}
              >
                <LogOut size={16} strokeWidth={2} />
                <span>Sign out</span>
              </button>
              <button
                type="button"
                className="topbar-user-menu__item topbar-user-menu__item--danger"
                role="menuitem"
                onClick={() => {
                  setUserMenuOpen(false);
                  setDeleteDialogOpen(true);
                }}
              >
                <UserX size={16} strokeWidth={2} />
                <span>Delete account</span>
              </button>
            </div>
          )}
        </div>
      </div>

      {deleteDialogOpen &&
        createPortal(
          <div
            className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/55 backdrop-blur-[2px]"
            role="presentation"
            onMouseDown={(e) => {
              if (e.target === e.currentTarget) setDeleteDialogOpen(false);
            }}
          >
            <div
              className="w-full max-w-md rounded-xl border border-red-500/25 bg-[#14141f] p-5 shadow-2xl shadow-black/50"
              role="dialog"
              aria-modal="true"
              aria-labelledby="delete-account-title"
              aria-describedby="delete-account-desc"
            >
              <h2 id="delete-account-title" className="text-base font-semibold text-[var(--text-primary)]">
                Delete your account?
              </h2>
              <p id="delete-account-desc" className="mt-2 text-sm leading-relaxed text-[var(--text-secondary)]">
                This permanently removes your account, profile, and associated workspace data. This cannot be undone.
              </p>
              <div className="mt-5 flex flex-wrap justify-end gap-2">
                <button
                  type="button"
                  className="rounded-lg border border-white/10 bg-transparent px-3 py-2 text-sm text-[var(--text-secondary)] hover:bg-white/5 hover:text-[var(--text-primary)]"
                  onClick={() => setDeleteDialogOpen(false)}
                  disabled={deleteInProgress}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  className="rounded-lg bg-red-600 px-3 py-2 text-sm font-medium text-white hover:bg-red-500 disabled:opacity-50"
                  disabled={deleteInProgress || !accessToken}
                  onClick={async () => {
                    if (!accessToken) return;
                    setDeleteInProgress(true);
                    try {
                      await deleteAccount(accessToken);
                      setDeleteDialogOpen(false);
                      logout();
                    } catch (err) {
                      const msg = err instanceof Error ? err.message : 'Could not delete account';
                      window.alert(msg);
                    } finally {
                      setDeleteInProgress(false);
                    }
                  }}
                >
                  {deleteInProgress ? 'Deleting…' : 'Delete permanently'}
                </button>
              </div>
            </div>
          </div>,
          document.body,
        )}
    </div>
  );
};

export default Topbar;
