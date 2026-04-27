import { useState, useEffect } from 'react';
import { X, Play, Save, Loader2, CheckCircle, AlertCircle } from 'lucide-react';
import { Widget } from '../types';
import { useDashboardStore } from '../store/useDashboardStore';
import { previewWidget } from '../services/backendClient';

interface WidgetSqlEditorModalProps {
  widget: Widget;
  dashboardId: string;
  isOpen: boolean;
  onClose: () => void;
}

export default function WidgetSqlEditorModal({ widget, dashboardId, isOpen, onClose }: WidgetSqlEditorModalProps) {
  const [sql, setSql] = useState('');
  const [title, setTitle] = useState('');
  const [isValidating, setIsValidating] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [validationSuccess, setValidationSuccess] = useState<string | null>(null);

  const { updateWidgetApi } = useDashboardStore();

  useEffect(() => {
    if (isOpen) {
      setSql((widget.dataConfig?.query as string) || '');
      setTitle(widget.title || '');
      setPreviewError(null);
      setValidationSuccess(null);
    }
  }, [isOpen, widget]);

  if (!isOpen) return null;

  const handleValidate = async () => {
    if (!sql.trim()) return;
    setIsValidating(true);
    setPreviewError(null);
    setValidationSuccess(null);
    try {
      const widgetPayload = {
        ...widget,
        data_config: {
          ...widget.dataConfig,
          query: sql,
        },
        chart_config: widget.chartConfig,
      };
      
      const hydrated = await previewWidget(widgetPayload);
      
      let rowsCount = 0;
      const cc = hydrated.chart_config || {};
      
      if (hydrated.type === 'kpi') {
        rowsCount = (cc as any).value !== undefined ? 1 : 0;
      } else {
        // Chart widget
        const chartConfig = cc as any;
        if (chartConfig.series && Array.isArray(chartConfig.series) && chartConfig.series.length > 0) {
          const s = chartConfig.series[0];
          if (s.data && Array.isArray(s.data)) {
            rowsCount = s.data.length;
          }
        } else if (chartConfig.xAxis && chartConfig.xAxis.data && Array.isArray(chartConfig.xAxis.data)) {
          rowsCount = chartConfig.xAxis.data.length;
        }
      }
      
      setValidationSuccess(`Validation successful. Query returned data (approx ${rowsCount} rows).`);
    } catch (err) {
      setPreviewError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsValidating(false);
    }
  };

  const handleSave = async () => {
    setIsSaving(true);
    try {
      await updateWidgetApi(dashboardId, widget.id, {
        title: title.trim() || widget.title,
        data_config: {
          ...widget.dataConfig,
          query: sql,
        },
      });
      // Quick refresh to hydrate the new query results
      await useDashboardStore.getState().refreshActiveDashboardFromServer();
      onClose();
    } catch (err) {
      setPreviewError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm">
      <div 
        className="w-full max-w-3xl flex flex-col max-h-[85vh] rounded-xl shadow-[0_12px_40px_rgba(0,0,0,0.45)]"
        style={{ 
          background: 'var(--bg-elevated)',
          border: '1px solid var(--border-default)'
        }}
      >
        
        {/* Header */}
        <div 
          className="flex items-center justify-between p-4"
          style={{ borderBottom: '1px solid var(--border-subtle)' }}
        >
          <div>
            <h3 className="text-[0.9375rem] font-medium text-[var(--text-primary)]">Edit Widget</h3>
            <p className="text-[0.8125rem] text-[var(--text-secondary)] mt-0.5">Modify settings and underlying SQL</p>
          </div>
          <button 
            onClick={onClose}
            className="p-1.5 rounded-md text-[var(--text-secondary)] transition-colors hover:text-[var(--text-primary)] hover:bg-[rgba(139,92,246,0.08)]"
          >
            <X size={18} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 p-4 flex flex-col gap-4 overflow-auto">
          {/* Title Input */}
          <div className="flex flex-col gap-1.5">
            <label className="text-[0.8125rem] font-medium text-[var(--text-secondary)]">Widget Title</label>
            <input
              type="text"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              className="w-full rounded-lg px-3 py-2 text-[0.875rem] outline-none transition-colors"
              style={{
                background: 'var(--bg-surface)',
                border: '1px solid var(--border-default)',
                color: 'var(--text-primary)'
              }}
              onFocus={(e) => (e.target.style.borderColor = 'var(--brand)')}
              onBlur={(e) => (e.target.style.borderColor = 'var(--border-default)')}
              placeholder="E.g., Total Revenue"
            />
          </div>

          <div className="flex-1 min-h-[300px] flex flex-col gap-1.5">
            <label className="text-[0.8125rem] font-medium text-[var(--text-secondary)]">SQL Query</label>
            <textarea
              className="w-full flex-1 rounded-lg p-4 font-mono text-[0.8125rem] resize-none outline-none transition-colors"
              style={{
                background: 'var(--bg-surface)',
                border: '1px solid var(--border-default)',
                color: 'var(--text-primary)'
              }}
              onFocus={(e) => (e.target.style.borderColor = 'var(--brand)')}
              onBlur={(e) => (e.target.style.borderColor = 'var(--border-default)')}
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              placeholder="SELECT * FROM table..."
              spellCheck={false}
            />
          </div>

          {previewError && (
            <div className="p-3 rounded-lg flex gap-2 items-start" style={{ background: 'rgba(239, 68, 68, 0.1)', border: '1px solid rgba(239, 68, 68, 0.2)', color: 'rgb(252, 165, 165)' }}>
              <AlertCircle size={16} className="mt-0.5 shrink-0" />
              <div className="whitespace-pre-wrap break-words text-[0.8125rem]">{previewError}</div>
            </div>
          )}

          {validationSuccess && !previewError && (
            <div className="p-3 rounded-lg flex gap-2 items-start" style={{ background: 'rgba(16, 185, 129, 0.1)', border: '1px solid rgba(16, 185, 129, 0.2)', color: 'rgb(110, 231, 183)' }}>
              <CheckCircle size={16} className="mt-0.5 shrink-0" />
              <div className="text-[0.8125rem]">{validationSuccess}</div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div 
          className="p-4 flex items-center justify-between rounded-b-xl"
          style={{ 
            borderTop: '1px solid var(--border-subtle)',
            background: 'linear-gradient(180deg, rgba(14,14,24,0.3) 0%, rgba(10,10,15,0.3) 100%)' 
          }}
        >
          <button
            onClick={handleValidate}
            disabled={isValidating || !sql.trim()}
            className="px-4 py-2 rounded-lg text-[0.8125rem] font-semibold transition-all flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
            style={{
              background: 'var(--bg-surface-alt)',
              border: '1px solid var(--border-default)',
              color: 'var(--text-primary)'
            }}
          >
            {isValidating ? <Loader2 size={16} className="animate-spin" /> : <Play size={16} />}
            Run & Validate
          </button>

          <div className="flex items-center gap-3">
            <button
              onClick={onClose}
              className="px-4 py-2 text-[0.8125rem] font-medium transition-colors hover:text-[var(--text-primary)]"
              style={{ color: 'var(--text-secondary)' }}
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={isSaving || !sql.trim() || isValidating}
              className="px-4 py-2 rounded-lg text-[0.8125rem] font-semibold transition-all flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
              style={{
                background: 'var(--brand)',
                color: 'white',
                border: 'none',
              }}
            >
              {isSaving ? <Loader2 size={16} className="animate-spin" /> : <Save size={16} />}
              Save Changes
            </button>
          </div>
        </div>

      </div>
    </div>
  );
}
