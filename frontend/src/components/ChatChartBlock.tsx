import { useMemo, useState } from 'react';
import ReactECharts from 'echarts-for-react';
import { Maximize2, X } from 'lucide-react';
import type { ChatChartItem, EChartsConfig, KpiConfig } from '../types';
import { getThemedEChartsOption } from '../widgets/ChartWidget';
import KpiWidget from '../widgets/KpiWidget';
import AddToDashboardMenu from './AddToDashboardMenu';

interface ChatChartBlockProps {
  item: ChatChartItem;
}

const INLINE_HEIGHT = 240;

function isKpiConfig(config: unknown): config is KpiConfig {
  return (
    typeof config === 'object' &&
    config !== null &&
    'value' in config &&
    typeof (config as KpiConfig).value === 'number'
  );
}

function ChatChartBlock({ item }: ChatChartBlockProps) {
  const [expanded, setExpanded] = useState(false);

  const isKpi = item.chart_type === 'kpi' && isKpiConfig(item.chartConfig);
  const raw = item.chartConfig as EChartsConfig;

  const option = useMemo(
    () => (isKpi ? null : getThemedEChartsOption(raw)),
    [raw, isKpi],
  );

  const chartTitle =
    item.title ?? (isKpi ? undefined : (raw.title as { text?: string } | undefined)?.text) ?? undefined;

  return (
    <>
      <div className="chat-chart-block">
        <div className="chat-chart-inner" style={{ height: INLINE_HEIGHT }}>
          {isKpi ? (
            <KpiWidget config={item.chartConfig as unknown as KpiConfig} title={chartTitle ?? 'KPI'} />
          ) : (
            <ReactECharts
              option={option!}
              style={{ height: '100%', width: '100%' }}
              opts={{ renderer: 'svg' }}
              notMerge
            />
          )}
        </div>
        <button
          type="button"
          className="chat-chart-expand-btn"
          onClick={() => setExpanded(true)}
          aria-label="Expand chart"
          title="Expand chart"
        >
          <Maximize2 size={16} />
        </button>
      </div>

      <div className="chat-chart-actions">
        <AddToDashboardMenu
          chartConfig={raw}
          chartType={item.chart_type}
          chartTitle={chartTitle}
          dataConfig={item.dataConfig}
        />
      </div>

      {expanded && (
        <div
          className="chat-chart-modal-backdrop"
          role="dialog"
          aria-modal="true"
          aria-label="Chart expanded view"
          onClick={() => setExpanded(false)}
        >
          <div
            className="chat-chart-modal-content"
            onClick={(e) => e.stopPropagation()}
          >
            <button
              type="button"
              className="chat-chart-modal-close"
              onClick={() => setExpanded(false)}
              aria-label="Close"
            >
              <X size={20} />
            </button>
            <div className="chat-chart-modal-chart">
              {isKpi ? (
                <KpiWidget config={item.chartConfig as unknown as KpiConfig} title={chartTitle ?? 'KPI'} />
              ) : (
                <ReactECharts
                  option={option!}
                  style={{ height: '100%', width: '100%' }}
                  opts={{ renderer: 'svg' }}
                  notMerge
                />
              )}
            </div>
          </div>
        </div>
      )}
    </>
  );
}

export default ChatChartBlock;
