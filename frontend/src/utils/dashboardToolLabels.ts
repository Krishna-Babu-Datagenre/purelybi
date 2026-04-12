/** User-facing labels for dashboard builder agent tools (Magic run timeline). */
export function friendlyDashboardToolLabel(raw: string | undefined): string {
  if (!raw) return 'Working';
  const name = raw.replace(/^tools?\./, '');
  const map: Record<string, string> = {
    sql_db_list_tables: 'Listing available tables',
    sql_db_schema: 'Reading table schema',
    sql_db_query: 'Running SQL query',
    sql_db_query_checker: 'Validating SQL',
    create_react_chart: 'Generating chart',
    create_react_kpi: 'Generating KPI',
    dashboard_create: 'Creating dashboard',
    dashboard_add_widget: 'Adding widget to dashboard',
    dashboard_list_my_dashboards: 'Listing your dashboards',
    dashboard_update_metadata: 'Updating dashboard',
    dashboard_remove_widget: 'Removing widget',
    dashboard_delete: 'Removing dashboard',
    calculate: 'Calculating',
    get_current_time: 'Checking time',
  };
  return map[name] ?? name.replace(/_/g, ' ');
}
