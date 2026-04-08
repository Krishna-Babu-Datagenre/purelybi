/**
 * List (GET /api/dashboards) and single-dashboard (GET /api/dashboards/{id})
 * responses sometimes represent the same UUID with different casing or punctuation.
 * Object keys in the store use whatever the last GET returned, so strict === fails.
 */
export function normalizeDashboardId(id: string | null | undefined): string {
  if (id == null) return '';
  return String(id).trim().toLowerCase().replace(/-/g, '');
}

export function dashboardIdsEqual(a: string | null | undefined, b: string | null | undefined): boolean {
  const na = normalizeDashboardId(a);
  const nb = normalizeDashboardId(b);
  return na !== '' && na === nb;
}

/** Find the key used in `dashboards` that matches `requestedId`, or null. */
export function findDashboardKey(
  dashboards: Record<string, unknown>,
  requestedId: string,
): string | null {
  if (dashboards[requestedId] !== undefined) return requestedId;
  for (const k of Object.keys(dashboards)) {
    if (dashboardIdsEqual(k, requestedId)) return k;
  }
  return null;
}
