import { Widget, WidgetLayout } from '../types';

// KPI cards: 2 row-units tall
const KPI_HEIGHT = 2;
// Charts: 8 row-units tall (extra room for axes + legends)
const CHART_HEIGHT = 8;
// Spacer row between KPI section and charts section
export const SECTION_GAP = 1;

/** Max KPIs in a single row — hard cap for visual consistency. */
const MAX_KPIS_PER_ROW = 4;

/**
 * Pick how many KPIs per row so that `cols` divides evenly
 * (no remainder → every card in a row has the exact same width).
 * Each KPI must be at least 2 grid-columns wide, and never more than MAX_KPIS_PER_ROW.
 */
function kpisPerRow(count: number, cols: number): number {
  const cap = Math.min(count, MAX_KPIS_PER_ROW);
  // Try preferred counts (descending from cap) that divide cols evenly
  for (let n = cap; n >= 1; n--) {
    if (cols % n === 0 && cols / n >= 2) return n;
  }
  return Math.min(cap, Math.max(1, Math.floor(cols / 2)));
}

/**
 * Build an array where each element is the number of KPIs in that row.
 * Prefers even distribution (all rows same count) when possible;
 * falls back to filling rows from the top with the last row getting the remainder.
 *
 *   1 → [1]  (full width)         5 → [4,1]   (4 + full-width)
 *   2 → [2]  (50 / 50)            6 → [3,3]   (even split)
 *   3 → [3]  (thirds)             8 → [4,4]   (even split)
 *   4 → [4]  (quarters)           9 → [3,3,3] (even split)
 */
function splitKpiRows(count: number, cols: number): number[] {
  if (count === 0) return [];

  const maxPerRow = Math.min(count, MAX_KPIS_PER_ROW);
  const minRows = Math.ceil(count / maxPerRow);

  // Prefer even split: all rows identical count, each divides cols evenly
  for (let perRow = maxPerRow; perRow >= 1; perRow--) {
    if (count % perRow !== 0) continue;
    if (cols % perRow !== 0) continue;
    if (cols / perRow < 2) continue;
    const numRows = count / perRow;
    if (numRows > minRows) continue;
    return Array(numRows).fill(perRow);
  }

  // Fallback: fill rows from the top, last row gets the remainder
  const perRow = kpisPerRow(count, cols);
  const rows: number[] = [];
  let remaining = count;
  while (remaining > 0) {
    rows.push(Math.min(remaining, perRow));
    remaining -= perRow;
  }
  return rows;
}

type GridRect = { x: number; y: number; w: number; h: number };

/**
 * Compute auto-balanced layouts for a set of KPI widgets.
 * Returns a Map from KPI ID → grid rectangle.
 */
export function computeKpiLayouts(
  kpiIds: string[],
  cols: number = 12,
): Map<string, GridRect> {
  const result = new Map<string, GridRect>();
  if (kpiIds.length === 0) return result;

  const rows = splitKpiRows(kpiIds.length, cols);
  let kpiIndex = 0;
  let currentY = 0;

  for (const rowCount of rows) {
    const itemWidth = Math.floor(cols / rowCount);
    for (let col = 0; col < rowCount; col++) {
      const id = kpiIds[kpiIndex++];
      result.set(id, { x: col * itemWidth, y: currentY, w: itemWidth, h: KPI_HEIGHT });
    }
    currentY += KPI_HEIGHT;
  }

  return result;
}

/**
 * Check whether the current KPI layouts match what auto-balance would produce.
 * Returns `true` when KPIs are in an auto-balanced arrangement (or there are none),
 * meaning it's safe to rebalance when a new KPI is added.
 * Returns `false` when the user has manually repositioned/resized KPI cards.
 */
export function isAutoBalancedKpiLayout(kpis: Widget[], cols: number = 12): boolean {
  if (kpis.length === 0) return true;

  const withLayouts = kpis.filter((k) => k.layout);
  if (withLayouts.length !== kpis.length) return true; // missing layouts → treat as auto

  // Sort by visual position to match auto-balance order
  const sorted = [...withLayouts].sort((a, b) => {
    const la = a.layout!;
    const lb = b.layout!;
    return la.y !== lb.y ? la.y - lb.y : la.x - lb.x;
  });

  const expected = computeKpiLayouts(
    sorted.map((_, i) => String(i)),
    cols,
  );
  const expectedArr = Array.from(expected.values());

  for (let i = 0; i < sorted.length; i++) {
    const actual = sorted[i].layout!;
    const exp = expectedArr[i];
    if (actual.x !== exp.x || actual.y !== exp.y || actual.w !== exp.w || actual.h !== exp.h) {
      return false;
    }
  }

  return true;
}

/**
 * How many chart columns for a given grid column count.
 * ≥ 6 cols → 2-column chart grid, otherwise stack to 1.
 */
function chartColumns(cols: number): number {
  return cols >= 6 ? 2 : 1;
}

type GridLayoutItem = { i: string; x: number; y: number; w: number; h: number };

function computeDefaultLayoutForCols(
  widgets: Widget[],
  cols: number,
): GridLayoutItem[] {
  const result: GridLayoutItem[] = [];

  const kpis = widgets.filter((w) => w.type === 'kpi');
  const charts = widgets.filter((w) => w.type !== 'kpi');

  let currentY = 0;

  // ── KPI section ──
  if (kpis.length > 0) {
    const rows = splitKpiRows(kpis.length, cols);
    let kpiIndex = 0;

    for (const rowCount of rows) {
      const itemWidth = Math.floor(cols / rowCount);
      for (let col = 0; col < rowCount; col++) {
        const kpi = kpis[kpiIndex++];
        result.push({
          i: kpi.id,
          x: col * itemWidth,
          y: currentY,
          w: itemWidth,
          h: KPI_HEIGHT,
        });
      }
      currentY += KPI_HEIGHT;
    }

    currentY += SECTION_GAP;
  }

  // ── Charts section ──
  const numChartCols = chartColumns(cols);
  const chartWidth = Math.floor(cols / numChartCols);

  for (let i = 0; i < charts.length; i++) {
    const chart = charts[i];
    const col = i % numChartCols;
    const row = Math.floor(i / numChartCols);
    result.push({
      i: chart.id,
      x: col * chartWidth,
      y: currentY + row * CHART_HEIGHT,
      w: chartWidth,
      h: CHART_HEIGHT,
    });
  }

  return result;
}

/**
 * Computes a dynamic layout for a set of widgets at a given column count.
 * Returns an array of { id, layout } tuples.
 */
export function computeLayoutForCols(
  widgets: Widget[],
  cols: number,
  options?: { preferStoredLayout?: boolean },
): GridLayoutItem[] {
  const defaults = computeDefaultLayoutForCols(widgets, cols);
  if (!options?.preferStoredLayout) {
    return defaults;
  }

  const defaultsById = new Map(defaults.map((item) => [item.i, item]));
  const scale = cols / 12;

  return widgets.map((widget) => {
    const fallback = defaultsById.get(widget.id) ?? { i: widget.id, x: 0, y: 0, w: 1, h: 1 };
    const stored = widget.layout as WidgetLayout | undefined;
    if (!stored) return fallback;

    const scaledW = Math.max(1, Math.min(cols, Math.round(stored.w * scale)));
    const scaledX = Math.max(0, Math.round(stored.x * scale));
    const clampedX = Math.min(scaledX, Math.max(0, cols - scaledW));
    return {
      i: widget.id,
      x: clampedX,
      y: Math.max(0, stored.y),
      w: scaledW,
      h: Math.max(1, stored.h),
    };
  });
}

/** Breakpoint → column count mapping (must match DashboardGrid) */
export const BREAKPOINT_COLS: Record<string, number> = {
  lg: 12,
  md: 12,
  sm: 8,
  xs: 4,
  xxs: 2,
};

/**
 * Computes layouts for ALL breakpoints at once.
 * Returns an object keyed by breakpoint name → layout item array.
 */
export function computeAllLayouts(
  widgets: Widget[],
  options?: { preferStoredLayout?: boolean },
): Record<string, GridLayoutItem[]> {
  const result: Record<string, GridLayoutItem[]> = {};
  for (const [bp, cols] of Object.entries(BREAKPOINT_COLS)) {
    result[bp] = computeLayoutForCols(widgets, cols, options);
  }
  return result;
}

/**
 * Legacy helper: returns a Map<id, WidgetLayout> for 12 columns.
 */
export function computeLayouts(widgets: Widget[]): Map<string, WidgetLayout> {
  const items = computeLayoutForCols(widgets, 12);
  const map = new Map<string, WidgetLayout>();
  for (const item of items) {
    map.set(item.i, { x: item.x, y: item.y, w: item.w, h: item.h });
  }
  return map;
}
