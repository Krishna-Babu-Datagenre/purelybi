import { useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import { Widget, KpiConfig, EChartsConfig } from '../types';
import KpiWidget from './KpiWidget';
import ChartContainer from '../components/ChartContainer';
import {
  computeChartLayout,
  ChartFamily,
  ChartLayoutRules,
} from '../utils/chartLayoutStrategy';

interface ChartWidgetProps {
  widget: Widget;
}

/** Matches global UI scale — ECharts uses px, not rem */
const CHART_UI_SCALE = 0.8;
const chartPx = (n: number) => Math.round(n * CHART_UI_SCALE * 10) / 10;

// ── Cohesive brand palette (purple-first, then complementary accents) ──
const PALETTE = [
  '#8B5CF6', // violet
  '#06B6D4', // cyan
  '#10B981', // emerald
  '#F59E0B', // amber
  '#6366F1', // indigo
  '#EC4899', // pink
  '#3B82F6', // blue
  '#A78BFA', // lavender
];

// ── Shared formatters ──────────────────────────────────────────────────
/** Compact number formatter: 1500000 → "1.5M", 12000 → "12K", 500 → "500" */
const compactFormatter = (val: unknown): string => {
  const n = Number(val);
  if (isNaN(n)) return String(val);
  if (Math.abs(n) >= 1_000_000) return `${+(n / 1_000_000).toPrecision(3)}M`;
  if (Math.abs(n) >= 1_000) return `${+(n / 1_000).toPrecision(3)}K`;
  return String(n);
};

/** Convert #RRGGBB → rgba(r,g,b,alpha) */
const hexToRgba = (hex: string, alpha: number) => {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
};

// ── Tooltip ────────────────────────────────────────────────────────────
function applyTooltip(cfg: EChartsConfig, family: ChartFamily): void {
  const backendTooltip = (typeof cfg.tooltip === 'object' && !Array.isArray(cfg.tooltip))
    ? cfg.tooltip as Record<string, unknown>
    : {};

  const isPie = family === 'pie';
  cfg.tooltip = {
    trigger: isPie ? 'item' : 'axis',
    ...backendTooltip,
    backgroundColor: 'rgba(14, 14, 24, 0.97)',
    borderColor: 'rgba(139, 92, 246, 0.3)',
    borderWidth: 1,
    padding: [chartPx(10), chartPx(14)],
    textStyle: {
      color: '#F0F0F8',
      fontSize: chartPx(12),
      fontFamily: 'Inter, sans-serif',
      lineHeight: chartPx(20),
    },
    confine: true,  // keep tooltip inside chart container
    ...(isPie
      ? {}
      : {
          axisPointer: {
            type: 'line',
            lineStyle: { color: 'rgba(139, 92, 246, 0.25)', type: 'dashed', width: 1 },
          },
        }),
    extraCssText: 'box-shadow: 0 8px 32px rgba(0,0,0,0.5); border-radius: 10px;',
  };
}

// ── X-Axis ─────────────────────────────────────────────────────────────
function applyXAxis(cfg: EChartsConfig, family: ChartFamily): void {
  const isHoriz = family === 'bar-horizontal';

  const transform = (ax: Record<string, unknown>): Record<string, unknown> => {
    const merged = { ...ax };
    merged.axisLine = { lineStyle: { color: 'rgba(139, 92, 246, 0.12)' } };
    merged.axisTick = { show: false };

    const data = ax.data as unknown[] | undefined;
    const maxLen = data ? Math.max(0, ...data.map((d) => String(d).length)) : 0;
    const labelCount = data?.length ?? 0;
    // Match chartLayoutStrategy thresholds so grid bottom and axis styling stay in sync
    const needsRotation = !isHoriz && (maxLen > 8 || labelCount > 12);

    // Axis name styling and spacing — applied to all cartesian charts (bar, line, scatter, heatmap, etc.)
    if (merged.name != null && merged.name !== '') {
      merged.nameTextStyle = {
        color: '#8B95B0',
        fontSize: chartPx(11),
        fontFamily: 'Inter, sans-serif',
        ...(merged.nameTextStyle as Record<string, unknown>),
      };
      merged.nameLocation = merged.nameLocation ?? 'middle';
      const minNameGap = needsRotation ? chartPx(72) : chartPx(40);
      merged.nameGap = Math.max(Number(merged.nameGap) || 0, minNameGap);
    }

    if (isHoriz) {
      merged.axisLine = { show: false };
      merged.splitLine = { lineStyle: { color: 'rgba(139, 92, 246, 0.06)', type: 'dashed' } };
      merged.axisLabel = {
        show: true,
        color: '#6B7280',
        fontSize: chartPx(10),
        fontFamily: 'Inter, sans-serif',
        margin: chartPx(8),
        interval: 'auto',
        hideOverlap: true,
        formatter: compactFormatter,
      };
    } else {
      // Vertical category axis: apply rotation and spacing for long/many labels
      merged.axisLabel = {
        show: true,
        color: '#6B7280',
        fontSize: chartPx(11),
        fontFamily: 'Inter, sans-serif',
        margin: needsRotation ? chartPx(14) : chartPx(10),
        hideOverlap: true,
        ...(needsRotation
          ? { rotate: 35, fontSize: chartPx(10), width: chartPx(80), overflow: 'truncate', ellipsis: '…' }
          : {}),
      };
    }
    return merged;
  };

  if (Array.isArray(cfg.xAxis)) {
    cfg.xAxis = (cfg.xAxis as Record<string, unknown>[]).map(transform);
  } else if (cfg.xAxis && typeof cfg.xAxis === 'object') {
    cfg.xAxis = transform(cfg.xAxis as Record<string, unknown>);
  }
}

// ── Y-Axis ─────────────────────────────────────────────────────────────
function applyYAxis(cfg: EChartsConfig, family: ChartFamily): void {
  const isHoriz = family === 'bar-horizontal';

  const transform = (ax: Record<string, unknown>): Record<string, unknown> => {
    const merged = { ...ax };
    merged.axisLine = { show: false };
    merged.axisTick = { show: false };

    // Axis name styling and spacing — applied to all cartesian charts (bar, line, scatter, etc.)
    if (merged.name != null && merged.name !== '') {
      merged.nameTextStyle = {
        color: '#8B95B0',
        fontSize: chartPx(11),
        fontFamily: 'Inter, sans-serif',
        ...(merged.nameTextStyle as Record<string, unknown>),
      };
      merged.nameLocation = merged.nameLocation ?? 'middle';
      merged.nameGap = Math.max(Number(merged.nameGap) || 0, chartPx(50));
    }

    if (isHoriz) {
      merged.splitLine = { show: false };
      merged.axisLabel = {
        show: true,
        color: '#8B95B0',
        fontSize: chartPx(11),
        fontFamily: 'Inter, sans-serif',
        width: chartPx(140),
        overflow: 'truncate',
        ellipsis: '…',
        formatter: (val: unknown) => {
          const s = String(val);
          return s.length > 20 ? s.slice(0, 19) + '…' : s;
        },
      };
    } else {
      merged.splitLine = {
        lineStyle: { color: 'rgba(139, 92, 246, 0.06)', type: 'dashed' },
      };
      merged.axisLabel = {
        show: true,
        color: '#6B7280',
        fontSize: chartPx(11),
        fontFamily: 'Inter, sans-serif',
        margin: chartPx(10),
        formatter: compactFormatter,
      };
    }
    return merged;
  };

  if (Array.isArray(cfg.yAxis)) {
    cfg.yAxis = (cfg.yAxis as Record<string, unknown>[]).map(transform);
  } else if (cfg.yAxis && typeof cfg.yAxis === 'object') {
    cfg.yAxis = transform(cfg.yAxis as Record<string, unknown>);
  }
}

// ── Series styling ─────────────────────────────────────────────────────
function applySeries(
  cfg: EChartsConfig,
  family: ChartFamily,
  rules: ChartLayoutRules,
): void {
  if (!Array.isArray(cfg.series)) return;

  cfg.series = (cfg.series as Record<string, unknown>[]).map((s, idx) => {
    const color = PALETTE[idx % PALETTE.length];

    if (s.type === 'line') {
      const areaStyle = s.areaStyle != null
        ? {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: hexToRgba(color, 0.25) },
                { offset: 1, color: hexToRgba(color, 0) },
              ],
            },
            ...(s.areaStyle as Record<string, unknown>),
          }
        : undefined;
      return {
        ...s,
        smooth: true,
        smoothMonotone: 'x',
        lineStyle: { width: chartPx(2), color, ...(s.lineStyle as Record<string, unknown> ?? {}) },
        itemStyle: { color, ...(s.itemStyle as Record<string, unknown> ?? {}) },
        symbolSize: 0,
        emphasis: { focus: 'series', symbolSize: chartPx(6) },
        ...(areaStyle ? { areaStyle } : {}),
      };
    }

    if (s.type === 'bar') {
      const isHoriz = family === 'bar-horizontal';
      return {
        ...s,
        barMaxWidth: isHoriz ? undefined : chartPx(28),
        barMinHeight: chartPx(2),
        itemStyle: {
          borderRadius: isHoriz
            ? [0, chartPx(4), chartPx(4), 0]
            : [chartPx(4), chartPx(4), 0, 0],
          color: {
            type: 'linear',
            x: 0, y: 0,
            x2: isHoriz ? 1 : 0,
            y2: isHoriz ? 0 : 1,
            colorStops: [
              { offset: 0, color },
              { offset: 1, color: color + (isHoriz ? 'CC' : '66') },
            ],
          },
          ...(s.itemStyle as Record<string, unknown> ?? {}),
        },
      };
    }

    if (s.type === 'pie') {
      // Pie/donut: when legend is at bottom (horizontal), keep chart centered
      const legendOnRight = rules.legend !== false
        && (rules.legend as Record<string, unknown>).orient === 'vertical';
      const center = legendOnRight ? ['35%', '48%'] : ['50%', '46%'];
      const radius = legendOnRight ? ['28%', '55%'] : ['30%', '58%'];

      return {
        ...s,
        center,
        radius,
        padAngle: 3,
        itemStyle: {
          borderRadius: chartPx(4),
          borderColor: 'transparent',
          borderWidth: chartPx(2),
        },
        label: rules.showDataLabels
          ? {
              show: true,
              color: '#8B95B0',
              fontSize: chartPx(11),
              fontFamily: 'Inter, sans-serif',
              formatter: '{d}%',
              position: 'outside',
            }
          : { show: false },
        labelLine: rules.showDataLabels
          ? { show: true, lineStyle: { color: 'rgba(139,92,246,0.2)' } }
          : { show: false },
        emphasis: {
          scaleSize: chartPx(6),
          label: { show: true, fontSize: chartPx(13), fontWeight: 600, color: '#F0F0F8' },
        },
      };
    }

    return s;
  });
}

// ── Main theme applicator ──────────────────────────────────────────────
/**
 * Deep-merge dark-theme defaults into any ECharts config the backend sends,
 * guided by the layout rules from `chartLayoutStrategy`.
 */
function applyDarkTheme(raw: EChartsConfig, rules: ChartLayoutRules): EChartsConfig {
  const cfg = JSON.parse(JSON.stringify(raw)) as EChartsConfig;
  const { family, grid, legend } = rules;

  // ── Strip title — already shown in the widget header bar ──
  cfg.title = undefined;

  // ── Opaque background (matches dashboard widget card) ──
  cfg.backgroundColor = '#14141F';

  // ── Palette ──
  cfg.color = PALETTE;

  // ── Grid (plot area insets) — strategy-driven ──
  if (!['pie', 'radar', 'funnel', 'gauge', 'treemap'].includes(family)) {
    cfg.grid = {
      top: grid.top,
      right: grid.right,
      bottom: grid.bottom,
      left: grid.left,
      containLabel: grid.containLabel,
    };
  }

  // ── Tooltip ──
  applyTooltip(cfg, family);

  // ── Axes (only for cartesian families) ──
  if (!['pie', 'radar', 'funnel', 'gauge', 'treemap'].includes(family)) {
    applyXAxis(cfg, family);
    applyYAxis(cfg, family);
  }

  // ── Legend — strategy-driven ──
  // Fully replace backend legend to prevent stale positioning.
  if (legend !== false) {
    cfg.legend = legend;
  } else {
    cfg.legend = { show: false };
  }

  // ── Series styling ──
  applySeries(cfg, family, rules);

  return cfg;
}

// ── Component ──────────────────────────────────────────────────────────
const ChartWidget = ({ widget }: ChartWidgetProps) => {
  if (widget.type === 'kpi') {
    return <KpiWidget config={widget.chartConfig as KpiConfig} title={widget.title} />;
  }

  const raw = widget.chartConfig as EChartsConfig;

  const { themedOption, containerInset } = useMemo(() => {
    const rules = computeChartLayout(raw);
    return {
      themedOption: applyDarkTheme(raw, rules),
      containerInset: rules.containerInset,
    };
  }, [raw]);

  return (
    <ChartContainer inset={containerInset}>
      <ReactECharts
        option={themedOption}
        style={{ height: '100%', width: '100%' }}
        opts={{ renderer: 'svg' }}
        notMerge
      />
    </ChartContainer>
  );
};

/** Themed ECharts option for use outside the widget (e.g. chat). */
export function getThemedEChartsOption(raw: EChartsConfig): EChartsConfig {
  const rules = computeChartLayout(raw);
  return applyDarkTheme(raw, rules);
}

export default ChartWidget;

