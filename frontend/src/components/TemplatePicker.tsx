import { LayoutDashboard, Database, Tag, Loader2, AlertCircle } from 'lucide-react';
import { useDashboardStore } from '../store/useDashboardStore';

const TemplatePicker = () => {
  const templates = useDashboardStore((s) => s.templates);
  const templatesFetched = useDashboardStore((s) => s.templatesFetched);
  const dashboardLoading = useDashboardStore((s) => s.dashboardLoading);
  const error = useDashboardStore((s) => s.error);
  const loadFromTemplate = useDashboardStore((s) => s.loadFromTemplate);
  const clearError = useDashboardStore((s) => s.clearError);

  return (
    <div className="template-picker flex flex-col items-center justify-center min-h-[60vh] px-6">
      {/* Hero */}
      <div className="flex flex-col items-center mb-10">
        <div className="template-picker-icon mb-4">
          <LayoutDashboard size={28} className="text-white" />
        </div>
        <h2 className="text-[22px] font-bold text-[var(--text-primary)] tracking-tight mb-1">
          Create a Dashboard
        </h2>
        <p className="text-sm text-[var(--text-secondary)] text-center max-w-md">
          Choose a template below to generate a fully configured dashboard from your data.
        </p>
      </div>

      {/* Error banner */}
      {error && (
        <div className="template-error flex items-center gap-3 mb-6 px-5 py-3 rounded-xl max-w-lg w-full">
          <AlertCircle size={16} className="text-red-400 shrink-0" />
          <p className="text-sm text-red-300 flex-1">{error}</p>
          <button
            onClick={clearError}
            className="text-xs text-red-400 hover:text-red-300 underline shrink-0"
          >
            Dismiss
          </button>
        </div>
      )}

      {/* Loading state — templates haven't arrived yet */}
      {!templatesFetched && !templates.length && (
        <div className="flex items-center gap-3 text-[var(--text-secondary)]">
          <Loader2 size={20} className="animate-spin" />
          <span className="text-sm">Loading templates…</span>
        </div>
      )}

      {/* Template cards */}
      {templates.length > 0 && (
        <div className="grid gap-4 w-full max-w-2xl">
          {templates.map((tmpl) => (
            <button
              key={tmpl.id}
              onClick={() => loadFromTemplate(tmpl.slug)}
              disabled={dashboardLoading}
              className="template-card group text-left w-full px-6 py-5 rounded-2xl transition-all duration-200 disabled:opacity-60 disabled:cursor-wait"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1 min-w-0">
                  <h3 className="template-card-name text-[15px] font-semibold mb-1 truncate">
                    {tmpl.name}
                  </h3>
                  <p className="template-card-desc text-[13px] leading-relaxed mb-3">
                    {tmpl.description}
                  </p>
                  <div className="flex flex-wrap gap-2">
                    {(tmpl.platforms ?? []).map((ds) => (
                      <span key={ds} className="template-source-badge inline-flex items-center gap-1 text-[11px] font-medium px-2 py-0.5 rounded-md">
                        <Database size={10} />
                        {ds}
                      </span>
                    ))}
                    {tmpl.tags.slice(0, 3).map((tag) => (
                      <span key={tag} className="template-tag-badge inline-flex items-center gap-1 text-[11px] font-medium px-2 py-0.5 rounded-md">
                        <Tag size={10} />
                        {tag}
                      </span>
                    ))}
                  </div>
                </div>

                {/* Generate arrow */}
                <div className="template-card-arrow shrink-0 mt-1 flex items-center justify-center w-9 h-9 rounded-xl transition-all duration-200">
                  {dashboardLoading ? (
                    <Loader2 size={16} className="animate-spin text-[var(--brand)]" />
                  ) : (
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className="text-[var(--text-secondary)] group-hover:text-[var(--brand)] transition-colors">
                      <path d="M6 3l5 5-5 5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                    </svg>
                  )}
                </div>
              </div>
            </button>
          ))}
        </div>
      )}

      {/* Empty state — templates fetched but none available */}
      {templatesFetched && templates.length === 0 && !error && (
        <p className="text-sm text-[var(--text-muted)]">No templates available.</p>
      )}
    </div>
  );
};

export default TemplatePicker;
