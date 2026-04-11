/**
 * User-facing copy for onboarding agent tool names.
 * Keys must match `@tool` function names in `backend/src/ai/agents/onboarding/tools/__init__.py`.
 */
const TOOL_LABELS: Record<string, string> = {
  get_connector_spec: 'Loading connector details from the catalog',
  render_auth_options: 'Preparing how you will sign in',
  render_input_fields: 'Preparing the configuration form',
  render_stream_selector: 'Preparing stream selection',
  start_oauth_flow: 'Starting OAuth with your provider',
  test_connection: 'Verifying the connection',
  discover_streams: 'Discovering available data streams',
  run_sync: 'Running a validation sync',
  save_config: 'Saving your connection',
};

function titleCaseFromSnake(name: string): string {
  return name
    .split('_')
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(' ');
}

export function friendlyToolLabel(toolName: string | undefined | null): string {
  if (toolName == null || String(toolName).trim() === '') {
    return 'Working…';
  }
  const key = String(toolName).trim();
  return TOOL_LABELS[key] ?? titleCaseFromSnake(key);
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
