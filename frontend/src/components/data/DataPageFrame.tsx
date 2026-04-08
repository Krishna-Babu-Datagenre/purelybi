import type { ReactNode } from 'react';

interface DataPageFrameProps {
  sidebarCollapsed: boolean;
  chatOpen: boolean;
  chatModal: boolean;
  chatWidthPx: number;
  children: ReactNode;
}

/** Full main-area frame matching other shell routes (sidebar + optional chat drawer). */
const DataPageFrame = ({
  sidebarCollapsed,
  chatOpen,
  chatModal,
  chatWidthPx,
  children,
}: DataPageFrameProps) => (
  <div
    className="transition-[left,right] duration-300"
    style={{
      position: 'fixed',
      top: 'var(--topbar-height)',
      left: sidebarCollapsed ? 'var(--sidebar-collapsed-width)' : 'var(--sidebar-width)',
      right: chatOpen && !chatModal ? chatWidthPx : 0,
      bottom: 0,
      padding: '1.5rem',
      overflowY: 'auto',
      overflowX: 'hidden',
    }}
  >
    {children}
  </div>
);

export default DataPageFrame;
