import { DollarSign, ShoppingCart, Users, Tag, Activity } from 'lucide-react';
import { KpiConfig, KpiIcon } from '../types';

interface KpiWidgetProps {
  config: KpiConfig;
  title: string;
}

const KPI_ICON_STYLES: Record<KpiIcon, { bg: string; color: string }> = {
  revenue:   { bg: 'rgba(99, 102, 241, 0.12)',  color: '#818cf8' },
  orders:    { bg: 'rgba(139, 92, 246, 0.12)',   color: '#a78bfa' },
  aov:       { bg: 'rgba(6, 182, 212, 0.12)',    color: '#22d3ee' },
  customers: { bg: 'rgba(16, 185, 129, 0.12)',   color: '#34d399' },
  generic:   { bg: 'rgba(99, 102, 241, 0.12)',   color: '#818cf8' },
};

const KPI_ICONS: Record<KpiIcon, React.ComponentType<{ size: number; className?: string; style?: React.CSSProperties }>> = {
  revenue:   DollarSign,
  orders:    ShoppingCart,
  aov:       Tag,
  customers: Users,
  generic:   Activity,
};

const KpiWidget = ({ config, title }: KpiWidgetProps) => {
  const change = config.change ?? 0;
  const isPositive = change > 0;
  const isNeutral = Math.abs(change) < 0.1;
  const iconKey: KpiIcon = config.icon ?? 'generic';
  const iconStyle = KPI_ICON_STYLES[iconKey];
  const Icon = KPI_ICONS[iconKey];

  const deltaColor = isNeutral
    ? 'var(--text-muted)'
    : isPositive
      ? '#2AF07A'
      : '#FF5C8A';
  const deltaArrow = isNeutral ? '▬' : isPositive ? '▲' : '▼';

  return (
    <div className="kpi-widget-root">
      <div className="kpi-card-inner">
        {/* Top: label + icon */}
        <div className="kpi-header-row">
          <span className="kpi-label">{title}</span>
          <div
            className="kpi-icon-circle"
            style={{ background: iconStyle.bg }}
          >
            <Icon size={14} style={{ color: iconStyle.color }} />
          </div>
        </div>

        {/* Value — middle row grows; keeps metric vertically centered in remaining space */}
        <div className="kpi-value-row">
          <div
            className="kpi-value"
            title={`${config.prefix ?? ''}${config.value.toLocaleString()}${config.suffix ?? ''}`}
          >
            {config.prefix ?? ''}{config.value.toLocaleString()}{config.suffix ?? ''}
          </div>
        </div>

        {/* Delta + subtitle */}
        <div className="kpi-footer">
          {config.change !== undefined && (
            <span className="kpi-delta" style={{ color: deltaColor }}>
              <span className="kpi-delta-arrow">{deltaArrow}</span>
              {isNeutral ? '0%' : `${isPositive ? '+' : ''}${config.change}%`}
            </span>
          )}
          {config.changeLabel && (
            <span className="kpi-subtitle">{config.changeLabel}</span>
          )}
        </div>
      </div>
    </div>
  );
};

export default KpiWidget;
