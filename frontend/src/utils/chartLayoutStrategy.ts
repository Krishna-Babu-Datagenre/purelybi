/**
 * chartLayoutStrategy.ts
 *
 * Structured visual rendering strategy for charts within grid blocks.
 *
 * Handles:
 *  - Smart internal padding rules per chart type
 *  - Legend positioning & overflow (horizontal wrap, scroll, repositioning)
 *  - Grid (plot area) insets that guarantee axis labels are never clipped
 *  - Whether to show data labels on the chart
 *  - Container padding that creates breathing room between grid block edge and chart
 */

import { EChartsConfig } from '../types';

/** Matches global `--ui-scale` in index.css — keeps chart chrome proportional to the scaled UI */
const UI_SCALE = 0.8;
const scalePx = (n: number) => Math.round(n * UI_SCALE * 10) / 10;

/* ─────────────────────────────────────────────
   Types
───────────────────────────────────────────── */

/** Detected high-level chart family — drives layout decisions */
export type ChartFamily =
  | 'line'
  | 'bar-vertical'
  | 'bar-horizontal'
  | 'pie'          // includes donut
  | 'scatter'
  | 'radar'
  | 'funnel'
  | 'gauge'
  | 'treemap'
  | 'heatmap'
  | 'other';

/** Resolved layout rules the theme applicator should apply */
export interface ChartLayoutRules {
  /** Chart family detected */
  family: ChartFamily;
  /** ECharts `grid` overrides (plot area insets) */
  grid: { top: number; right: number; bottom: number; left: number; containLabel: boolean };
  /** Legend configuration overrides */
  legend: Record<string, unknown> | false;
  /** Whether data labels should be shown on series */
  showDataLabels: boolean;
  /** Absolute inset (px) from each edge of the content area for the chart canvas */
  containerInset: { top: number; right: number; bottom: number; left: number };
}

/* ─────────────────────────────────────────────
   Detection helpers
───────────────────────────────────────────── */

function getSeriesTypes(cfg: EChartsConfig): string[] {
  const series = cfg.series;
  if (!Array.isArray(series)) return [];
  return (series as Record<string, unknown>[]).map((s) => String(s.type ?? ''));
}

function countSeriesItems(cfg: EChartsConfig): number {
  const series = cfg.series;
  if (!Array.isArray(series)) return 0;
  return (series as Record<string, unknown>[]).length;
}

function countLegendItems(cfg: EChartsConfig): number {
  // If legend data is explicit, use that length
  const legend = cfg.legend as Record<string, unknown> | undefined;
  if (legend?.data && Array.isArray(legend.data)) return (legend.data as unknown[]).length;

  // For pie charts, count data items in the first pie series
  const series = cfg.series;
  if (Array.isArray(series)) {
    const pieSeries = (series as Record<string, unknown>[]).find((s) => s.type === 'pie');
    if (pieSeries?.data && Array.isArray(pieSeries.data)) {
      return (pieSeries.data as unknown[]).length;
    }
  }

  // Fallback: number of series
  return countSeriesItems(cfg);
}

function hasCategoryOnYAxis(cfg: EChartsConfig): boolean {
  const yAxis = cfg.yAxis;
  if (!yAxis) return false;
  const first = Array.isArray(yAxis) ? (yAxis as Record<string, unknown>[])[0] : yAxis as Record<string, unknown>;
  return first?.type === 'category';
}

function countXAxisLabels(cfg: EChartsConfig): number {
  const xAxis = cfg.xAxis;
  if (!xAxis) return 0;
  const first = Array.isArray(xAxis) ? (xAxis as Record<string, unknown>[])[0] : xAxis as Record<string, unknown>;
  if (first?.data && Array.isArray(first.data)) return (first.data as unknown[]).length;
  return 0;
}

function maxXLabelLength(cfg: EChartsConfig): number {
  const xAxis = cfg.xAxis;
  if (!xAxis) return 0;
  const first = Array.isArray(xAxis) ? (xAxis as Record<string, unknown>[])[0] : xAxis as Record<string, unknown>;
  if (first?.data && Array.isArray(first.data)) {
    return Math.max(0, ...(first.data as unknown[]).map((d) => String(d).length));
  }
  return 0;
}

/* ─────────────────────────────────────────────
   Family detection
───────────────────────────────────────────── */

export function detectChartFamily(cfg: EChartsConfig): ChartFamily {
  const types = getSeriesTypes(cfg);
  if (types.length === 0) return 'other';

  const primary = types[0];

  if (primary === 'pie') return 'pie';
  if (primary === 'radar') return 'radar';
  if (primary === 'funnel') return 'funnel';
  if (primary === 'gauge') return 'gauge';
  if (primary === 'treemap' || primary === 'sunburst') return 'treemap';
  if (primary === 'heatmap') return 'heatmap';
  if (primary === 'scatter' || primary === 'effectScatter') return 'scatter';
  if (primary === 'bar') {
    return hasCategoryOnYAxis(cfg) ? 'bar-horizontal' : 'bar-vertical';
  }
  if (primary === 'line') return 'line';

  return 'other';
}

/* ─────────────────────────────────────────────
   Legend strategy
───────────────────────────────────────────── */

function buildLegendConfig(
  family: ChartFamily,
  legendCount: number,
): Record<string, unknown> | false {
  // No legend needed for single-series cartesian charts
  if (legendCount <= 1 && (family === 'bar-vertical' || family === 'bar-horizontal' || family === 'line')) {
    return false;
  }

  const baseStyle = {
    show: true,
    icon: 'circle',
    itemWidth: scalePx(8),
    itemHeight: scalePx(8),
    itemGap: scalePx(12),
    textStyle: {
      color: '#8B95B0',
      fontSize: scalePx(11),
      fontFamily: 'Inter, sans-serif',
      overflow: 'truncate' as const,
      ellipsis: '…',
      width: scalePx(120),
    },
  };

  // Pie / donut: if many legend items, use scrollable legend at bottom (avoids right-side truncation)
  if (family === 'pie') {
    if (legendCount > 6) {
      return {
        ...baseStyle,
        type: 'scroll',
        orient: 'horizontal',
        bottom: scalePx(12),
        left: 'center',
        right: undefined,
        top: undefined,
        pageButtonPosition: 'end',
        pageIconColor: '#8B5CF6',
        pageIconInactiveColor: '#3A3A5C',
        pageTextStyle: { color: '#6B7280', fontSize: scalePx(10) },
        textStyle: {
          ...baseStyle.textStyle,
          width: scalePx(160),
        },
      };
    }
    // ≤ 6 items: horizontal legend at bottom
    return {
      ...baseStyle,
      orient: 'horizontal',
      top: undefined,
      right: undefined,
      bottom: scalePx(12),
      left: 'center',
      textStyle: {
        ...baseStyle.textStyle,
        width: undefined,
      },
    };
  }

  // Cartesian charts with many series: scrollable horizontal at bottom
  if (legendCount > 5) {
    return {
      ...baseStyle,
      type: 'scroll',
      orient: 'horizontal',
      top: undefined,
      right: undefined,
      bottom: scalePx(12),
      left: 'center',
      pageButtonPosition: 'end',
      pageIconColor: '#8B5CF6',
      pageIconInactiveColor: '#3A3A5C',
      pageTextStyle: { color: '#6B7280', fontSize: scalePx(10) },
    };
  }

  // Default: horizontal legend at bottom
  return {
    ...baseStyle,
    orient: 'horizontal',
    top: undefined,
    right: undefined,
    bottom: scalePx(12),
    left: 'center',
  };
}

/* ─────────────────────────────────────────────
   Grid (plot area insets) strategy
───────────────────────────────────────────── */

function buildGridConfig(
  family: ChartFamily,
  cfg: EChartsConfig,
  hasLegend: boolean,
): { top: number; right: number; bottom: number; left: number; containLabel: boolean } {
  // Pie, radar, funnel, gauge, treemap — no cartesian grid
  if (['pie', 'radar', 'funnel', 'gauge', 'treemap'].includes(family)) {
    return {
      top: scalePx(16),
      right: scalePx(16),
      bottom: hasLegend ? scalePx(44) : scalePx(16),
      left: scalePx(16),
      containLabel: false,
    };
  }

  const xLabelLen = maxXLabelLength(cfg);
  const xLabelCount = countXAxisLabels(cfg);

  // Determine if X labels will be long and need rotation
  const needsRotation = xLabelLen > 8 || xLabelCount > 12;
  const extraBottom = needsRotation ? scalePx(28) : 0;

  // Legend at bottom takes ~36px
  const legendSpace = hasLegend ? scalePx(36) : 0;

  if (family === 'bar-horizontal') {
    return {
      top: scalePx(16),
      right: scalePx(28),
      bottom: scalePx(40) + legendSpace,
      left: scalePx(16),
      containLabel: true,
    };
  }

  // Multi Y-axis charts (dual axis) need right margin for 2nd axis labels
  const hasMultiYAxis = Array.isArray(cfg.yAxis) && (cfg.yAxis as unknown[]).length > 1;
  const rightPad = hasMultiYAxis ? scalePx(64) : scalePx(32);

  // Generous left padding so Y-axis labels are never clipped or cramped
  return {
    top: scalePx(20),
    right: rightPad,
    bottom: scalePx(44) + extraBottom + legendSpace,
    left: scalePx(56),
    containLabel: true,
  };
}

/* ─────────────────────────────────────────────
   Data label strategy
───────────────────────────────────────────── */

function shouldShowDataLabels(family: ChartFamily, _cfg: EChartsConfig): boolean {
  // Pie/donut: always show percentage labels (like "Order by Financial Status")
  if (family === 'pie') return true;

  // Gauges always show their value
  if (family === 'gauge') return true;

  // Funnel: show labels
  if (family === 'funnel') return true;

  // Bar/line/scatter: never auto-add data labels (too cluttered)
  return false;
}

/* ─────────────────────────────────────────────
   Container inset strategy
───────────────────────────────────────────── */

/**
 * Returns absolute inset values (px) from each edge of the content area.
 * The chart canvas is absolutely positioned using these offsets, so it is
 * genuinely smaller than the grid block. overflow:visible on the container
 * ensures axis labels are never clipped.
 */
function buildContainerInset(family: ChartFamily): {
  top: number;
  right: number;
  bottom: number;
  left: number;
} {
  const padding = {
    top: scalePx(16),
    right: scalePx(20),
    bottom: scalePx(16),
    left: scalePx(20),
  };
  if (family === 'pie') return padding;
  if (family === 'bar-horizontal') return padding;
  return padding;
}

/* ─────────────────────────────────────────────
   Public API
───────────────────────────────────────────── */

/**
 * Given a raw ECharts config, compute the complete layout rules
 * the chart renderer should apply.
 */
export function computeChartLayout(cfg: EChartsConfig): ChartLayoutRules {
  const family = detectChartFamily(cfg);
  const legendCount = countLegendItems(cfg);
  const legend = buildLegendConfig(family, legendCount);
  const hasLegend = legend !== false;
  const grid = buildGridConfig(family, cfg, hasLegend);
  const showDataLabels = shouldShowDataLabels(family, cfg);
  const containerInset = buildContainerInset(family);

  return { family, grid, legend, showDataLabels, containerInset };
}
