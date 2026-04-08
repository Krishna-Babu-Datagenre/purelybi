import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Responsive, WidthProvider, Layout } from 'react-grid-layout/legacy';
import { GripVertical, Loader2 } from 'lucide-react';
import { useDashboardStore } from '../store/useDashboardStore';
import ChartWidget from '../widgets/ChartWidget';
import WidgetContextMenu from '../components/WidgetContextMenu';
import { computeAllLayouts, BREAKPOINT_COLS } from '../utils/layoutEngine';

import 'react-grid-layout/css/styles.css';

const ResponsiveGridLayout = WidthProvider(Responsive);

const BREAKPOINTS = { lg: 1200, md: 768, sm: 480, xs: 320, xxs: 0 };

/** Horizontal `containerPadding` for react-grid-layout — toolbar labels align to this inset. */
export const DASHBOARD_GRID_PAD_X = 22;

const HANDLE_HEIGHT = 35;
const BASE_COLS = 12;

function normalizeToBaseCols(layout: Layout, currentCols: number): Layout {
  const sourceCols = currentCols > 0 ? currentCols : BASE_COLS;
  const scale = BASE_COLS / sourceCols;
  return layout.map((item) => {
    const scaledW = Math.max(1, Math.min(BASE_COLS, Math.round(item.w * scale)));
    const scaledX = Math.max(0, Math.round(item.x * scale));
    const clampedX = Math.min(scaledX, Math.max(0, BASE_COLS - scaledW));
    return {
      ...item,
      x: clampedX,
      w: scaledW,
      y: Math.max(0, item.y),
      h: Math.max(1, item.h),
    };
  });
}

const DashboardGrid = () => {
  const dashboard = useDashboardStore((s) => {
    const id = s.activeDashboardId;
    return id ? s.dashboards[id] ?? null : null;
  });
  const updateWidgetLayout = useDashboardStore((s) => s.updateWidgetLayout);
  const deleteWidgetApi = useDashboardStore((s) => s.deleteWidgetApi);
  const persistWidgetLayoutsApi = useDashboardStore((s) => s.persistWidgetLayoutsApi);

  const widgets = dashboard?.widgets ?? [];
  const dashboardId = dashboard?.meta.id ?? null;
  const isTemplateDashboard = dashboard?.meta.source === 'template';
  const persistTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [activeCols, setActiveCols] = useState<number>(BASE_COLS);
  const [deletingWidgetIds, setDeletingWidgetIds] = useState<Set<string>>(new Set());

  const handleDeleteWidget = async (widgetId: string) => {
    if (isTemplateDashboard || !dashboardId) return;
    setDeletingWidgetIds((prev) => new Set(prev).add(widgetId));
    try {
      await deleteWidgetApi(dashboardId, widgetId);
    } finally {
      setDeletingWidgetIds((prev) => {
        const next = new Set(prev);
        next.delete(widgetId);
        return next;
      });
    }
  };

  const layouts = useMemo(
    () => computeAllLayouts(widgets, { preferStoredLayout: !isTemplateDashboard }),
    [widgets, isTemplateDashboard],
  );

  // Stable key that only changes when widget IDs change (add/remove),
  // forcing react-grid-layout to remount and apply fresh computed layouts.
  const gridKey = useMemo(
    () => widgets.map((w) => w.id).sort().join(','),
    [widgets],
  );

  const commitLayout = useCallback((layout: Layout) => {
    const normalized = normalizeToBaseCols(layout, activeCols);
    updateWidgetLayout([...normalized]);

    if (isTemplateDashboard || !dashboardId) return;
    if (persistTimerRef.current) {
      clearTimeout(persistTimerRef.current);
    }
    persistTimerRef.current = setTimeout(() => {
      const payload = normalized.map((item) => ({
        id: item.i,
        x: item.x,
        y: item.y,
        w: item.w,
        h: item.h,
      }));
      persistWidgetLayoutsApi(dashboardId, payload).catch(() => {});
    }, 300);
  }, [activeCols, dashboardId, isTemplateDashboard, persistWidgetLayoutsApi, updateWidgetLayout]);

  const handleDragStop = (currentLayout: Layout) => {
    commitLayout(currentLayout);
  };

  const handleResizeStop = (currentLayout: Layout) => {
    commitLayout(currentLayout);
  };

  useEffect(() => {
    return () => {
      if (persistTimerRef.current) {
        clearTimeout(persistTimerRef.current);
      }
    };
  }, []);

  if (!dashboard) {
    return null;
  }

  return (
    <ResponsiveGridLayout
      key={gridKey}
      className="layout"
      layouts={layouts}
      breakpoints={BREAKPOINTS}
      cols={BREAKPOINT_COLS}
      onBreakpointChange={(newBreakpoint) => {
        setActiveCols(BREAKPOINT_COLS[newBreakpoint] ?? BASE_COLS);
      }}
      rowHeight={45}
      margin={[19, 19]}
      containerPadding={[DASHBOARD_GRID_PAD_X, 10]}
      onDragStop={handleDragStop}
      onResizeStop={handleResizeStop}
      isDraggable={!isTemplateDashboard}
      isResizable={!isTemplateDashboard}
      draggableHandle=".drag-handle"
      resizeHandles={['s', 'w', 'e', 'sw', 'se']}
    >
      {widgets.map((widget) => {
        const isKpi = widget.type === 'kpi';
        const isDeleting = deletingWidgetIds.has(widget.id);

        return (
          <div
            key={widget.id}
            className={`widget-card flex flex-col overflow-hidden group ${
              isKpi ? 'widget-card--kpi' : 'widget-card--chart'
            }`}
            style={{ position: 'relative' }}
          >
            {isDeleting && (
              <div
                style={{
                  position: 'absolute',
                  inset: 0,
                  zIndex: 50,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  background: 'rgba(255,255,255,0.7)',
                  backdropFilter: 'blur(2px)',
                  borderRadius: 'inherit',
                }}
              >
                <Loader2 size={28} className="animate-spin" style={{ color: '#6366f1' }} />
              </div>
            )}

            {!isTemplateDashboard && (
              <WidgetContextMenu
                widgetTitle={widget.title}
                onDelete={() => handleDeleteWidget(widget.id)}
              />
            )}

            {isKpi ? (
              <div className="drag-handle kpi-drag-handle flex items-center gap-2" style={{ position: 'relative', zIndex: 1 }}>
                <GripVertical size={12} className="widget-grip" />
              </div>
            ) : (
              <div
                className="widget-header flex flex-row items-center flex-nowrap shrink-0"
                style={{ height: HANDLE_HEIGHT, position: 'relative', zIndex: 1 }}
              >
                <div className="drag-handle flex flex-1 items-center min-w-0 h-full cursor-grab active:cursor-grabbing select-none">
                  <span className="widget-title truncate min-w-0 mr-4">
                    {widget.title}
                  </span>
                  <GripVertical size={13} className="widget-grip transition-colors duration-150 shrink-0 ml-auto" />
                </div>
              </div>
            )}

            <div
              className={isKpi ? 'w-full min-h-0 flex-1 flex flex-col' : 'w-full'}
              style={
                isKpi
                  ? { position: 'relative', zIndex: 1, minHeight: 0 }
                  : {
                      height: `calc(100% - ${HANDLE_HEIGHT}px)`,
                      position: 'relative',
                      zIndex: 1,
                    }
              }
            >
              <ChartWidget widget={widget} />
            </div>
          </div>
        );
      })}
    </ResponsiveGridLayout>
  );
};

export default DashboardGrid;
