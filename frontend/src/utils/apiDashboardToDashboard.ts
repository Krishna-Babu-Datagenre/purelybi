import type { ApiDashboard, ApiDashboardMeta, Dashboard, DashboardMeta, Widget } from '../types';

/**
 * Convert API dashboard response to frontend Dashboard shape.
 */
export function apiDashboardToDashboard(api: ApiDashboard): Dashboard {
  const meta: DashboardMeta = {
    id: api.id,
    name: api.name,
    description: api.description ?? undefined,
    createdAt: api.created_at,
    updatedAt: api.updated_at,
    tags: api.tags,
    source: api.source,
  };
  const widgets: Widget[] = api.widgets.map((w) => ({
    id: w.id,
    title: w.title,
    type: w.type,
    layout: w.layout,
    chartConfig: w.chart_config,
    dataConfig: w.data_config,
  }));
  return { meta, widgets };
}

/**
 * Convert API dashboard list item to minimal meta (for sidebar list).
 */
export function apiDashboardMetaToMeta(api: ApiDashboardMeta): DashboardMeta {
  return {
    id: api.id,
    name: api.name,
    description: api.description ?? undefined,
    createdAt: api.created_at,
    updatedAt: api.updated_at,
    tags: api.tags,
    source: api.source,
  };
}
