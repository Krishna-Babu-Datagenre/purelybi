import { useEffect, useRef, useState } from 'react';
import { LayoutDashboard, LogOut } from 'lucide-react';
import { useChatStore } from '../store/useChatStore';
import { useAuthStore } from '../store/useAuthStore';

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
  const logout = useAuthStore((s) => s.logout);
  const displayName = user?.full_name || user?.email || 'User';
  const [userMenuOpen, setUserMenuOpen] = useState(false);
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
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Topbar;
