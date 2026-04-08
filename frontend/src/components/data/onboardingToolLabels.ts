/** User-facing copy for onboarding agent tool names (matches backend tool ids). */

export function friendlyToolLabel(toolName: string | undefined | null): string {
  if (toolName == null || String(toolName).trim() === '') {
    return 'Working…';
  }
  const map: Record<string, string> = {
    get_connector_spec: 'Reading connector settings from the catalog',
    render_auth_options: 'Preparing authentication choices',
    render_input_fields: 'Preparing a configuration form',
    render_stream_selector: 'Preparing stream selection',
    start_oauth_flow: 'Opening OAuth sign-in',
    test_connection: 'Validating your connection',
    discover_streams: 'Listing available data streams',
    run_sync: 'Running test sync (validation)',
    save_config: 'Saving your connection',
  };
  return (
    map[toolName] ??
    String(toolName).replace(/^([a-z])/, (_, c) => c.toUpperCase()).replace(/_/g, ' ')
  );
}

/** Turn streamed token payloads into plain text (handles structured provider chunks). */
export function normalizeTokenContent(content: unknown): string {
  if (content == null) return '';
  if (typeof content === 'string') return content;
  if (typeof content === 'number' || typeof content === 'boolean') return String(content);
  if (Array.isArray(content)) {
    return content
      .map((block) => {
        if (typeof block === 'string') return block;
        if (block && typeof block === 'object' && 'text' in block) {
          return String((block as { text: unknown }).text ?? '');
        }
        return '';
      })
      .join('');
  }
  if (typeof content === 'object') {
    try {
      return JSON.stringify(content);
    } catch {
      return String(content);
    }
  }
  return String(content);
}
