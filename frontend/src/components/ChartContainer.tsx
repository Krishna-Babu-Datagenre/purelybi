import { ReactNode } from 'react';

interface ChartContainerProps {
  /** Inset from each edge of the parent in px — the chart canvas is this much smaller */
  inset: { top: number; right: number; bottom: number; left: number };
  children: ReactNode;
}

/**
 * ChartContainer
 *
 * Uses absolute positioning with inset offsets so the chart area has internal padding
 * from the grid block. The chart is center-aligned and constrained to ~92% of the
 * content area so it doesn't feel cramped. overflow: auto allows scrolling when
 * axis labels or legends extend beyond the visible area.
 */
const ChartContainer = ({ inset, children }: ChartContainerProps) => (
  <div
    className="chart-container"
    style={{
      position: 'absolute',
      top: inset.top,
      right: inset.right,
      bottom: inset.bottom,
      left: inset.left,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      overflow: 'auto',
      minWidth: 0,
      minHeight: 0,
    }}
  >
    <div
      style={{
        width: '92%',
        height: '92%',
        maxWidth: '100%',
        maxHeight: '100%',
        minWidth: 0,
        minHeight: 0,
        flexShrink: 0,
      }}
    >
      {children}
    </div>
  </div>
);

export default ChartContainer;
