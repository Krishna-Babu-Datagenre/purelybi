import { useEffect, useState } from 'react';
import { Search, X } from 'lucide-react';
import type { DEPipelineStepUpsert, RecipeDefinition } from '../../types/de';

interface AddStepModalProps {
  recipes: RecipeDefinition[];
  nextStepOrder: number;
  availableColumns: string[];
  onSave: (step: DEPipelineStepUpsert) => Promise<void>;
  onClose: () => void;
}

function SchemaField({
  name,
  schema,
  recipeType,
  availableColumns,
  value,
  onChange,
}: {
  name: string;
  schema: {
    type?: string;
    description?: string;
    default?: unknown;
    items?: { type?: string };
  };
  recipeType?: string;
  availableColumns: string[];
  value: unknown;
  onChange: (name: string, value: unknown) => void;
}) {
  const label = name.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  const hint = schema.description ?? '';
  const columnOptions = Array.from(new Set(availableColumns.filter((c) => c.trim().length > 0))).sort();

  if (schema.type === 'integer' || schema.type === 'number') {
    return (
      <div className="flex flex-col gap-1">
        <label className="text-xs font-medium text-[var(--text-primary)]">{label}</label>
        {hint && <p className="text-xs text-[var(--text-secondary)]">{hint}</p>}
        <input
          type="number"
          className="modal-input"
          value={value === undefined || value === null ? '' : String(value)}
          onChange={(e) => onChange(name, e.target.value === '' ? undefined : Number(e.target.value))}
        />
      </div>
    );
  }

  if (schema.type === 'object') {
    const mapValue =
      typeof value === 'object' && value !== null && !Array.isArray(value)
        ? (value as Record<string, string>)
        : {};
    const entries = Object.entries(mapValue);

    const addRow = () => {
      const firstUnused = columnOptions.find((col) => !(col in mapValue)) ?? '';
      onChange(name, { ...mapValue, [firstUnused]: '' });
    };

    const updateKey = (oldKey: string, newKey: string) => {
      const updated: Record<string, string> = {};
      for (const [k, v] of Object.entries(mapValue)) {
        updated[k === oldKey ? newKey : k] = v;
      }
      onChange(name, updated);
    };

    const updateVal = (key: string, val: string) => {
      onChange(name, { ...mapValue, [key]: val });
    };

    const removeRow = (key: string) => {
      const updated = { ...mapValue };
      delete updated[key];
      onChange(name, updated);
    };

    return (
      <div className="flex flex-col gap-1">
        <label className="text-xs font-medium text-[var(--text-primary)]">{label}</label>
        {hint && <p className="text-xs text-[var(--text-secondary)]">{hint}</p>}
        <div className="space-y-1">
          {entries.map(([k, v], i) => (
            <div key={i} className="flex gap-2 items-center">
              {columnOptions.length > 0 ? (
                <select className="modal-input flex-1" value={k} onChange={(e) => updateKey(k, e.target.value)}>
                  <option value="">Current name</option>
                  {columnOptions.map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
                  {k && !columnOptions.includes(k) && <option value={k}>{k}</option>}
                </select>
              ) : (
                <input
                  placeholder="Current name"
                  className="modal-input flex-1"
                  value={k}
                  onChange={(e) => updateKey(k, e.target.value)}
                />
              )}
              <span className="text-[var(--text-muted)] text-xs">→</span>
              <input
                placeholder="New name"
                className="modal-input flex-1"
                value={v}
                onChange={(e) => updateVal(k, e.target.value)}
              />
              <button
                type="button"
                onClick={() => removeRow(k)}
                className="text-[var(--text-muted)] hover:text-red-400"
              >
                <X size={14} />
              </button>
            </div>
          ))}
          <button
            type="button"
            onClick={addRow}
            className="text-xs text-[var(--brand)] hover:opacity-90 font-medium"
          >
            + Add row
          </button>
        </div>
      </div>
    );
  }

  if (schema.type === 'array') {
    const arrValue = Array.isArray(value) ? value.map((v) => String(v ?? '')) : [];

    const addItem = () => onChange(name, [...arrValue, '']);
    const updateItem = (idx: number, next: string) => {
      const updated = [...arrValue];
      updated[idx] = next;
      onChange(name, updated);
    };
    const removeItem = (idx: number) => {
      onChange(
        name,
        arrValue.filter((_, i) => i !== idx),
      );
    };

    return (
      <div className="flex flex-col gap-1">
        <label className="text-xs font-medium text-[var(--text-primary)]">{label}</label>
        {hint && <p className="text-xs text-[var(--text-secondary)]">{hint}</p>}
        <div className="space-y-1">
          {arrValue.map((item, idx) => (
            <div key={idx} className="flex gap-2 items-center">
              {columnOptions.length > 0 && schema.items?.type === 'string' ? (
                <select className="modal-input flex-1" value={item} onChange={(e) => updateItem(idx, e.target.value)}>
                  <option value="">Select column</option>
                  {columnOptions.map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
                  {item && !columnOptions.includes(item) && <option value={item}>{item}</option>}
                </select>
              ) : (
                <input className="modal-input flex-1" value={item} onChange={(e) => updateItem(idx, e.target.value)} />
              )}
              <button
                type="button"
                onClick={() => removeItem(idx)}
                className="text-[var(--text-muted)] hover:text-red-400"
              >
                <X size={14} />
              </button>
            </div>
          ))}
          <button
            type="button"
            onClick={addItem}
            className="text-xs text-[var(--brand)] hover:opacity-90 font-medium"
          >
            + Add row
          </button>
        </div>
      </div>
    );
  }

  const shouldUseColumnDropdown =
    columnOptions.length > 0 &&
    ((recipeType === 'replace_values' && name === 'column') ||
      (recipeType === 'extract_regex' && name === 'source_column'));

  return (
    <div className="flex flex-col gap-1">
      <label className="text-xs font-medium text-[var(--text-primary)]">{label}</label>
      {hint && <p className="text-xs text-[var(--text-secondary)]">{hint}</p>}
      {shouldUseColumnDropdown ? (
        <select
          className="modal-input"
          value={value === undefined || value === null ? '' : String(value)}
          onChange={(e) => onChange(name, e.target.value)}
        >
          <option value="">Select column</option>
          {columnOptions.map((col) => (
            <option key={col} value={col}>
              {col}
            </option>
          ))}
          {value !== undefined &&
            value !== null &&
            value !== '' &&
            !columnOptions.includes(String(value)) && <option value={String(value)}>{String(value)}</option>}
        </select>
      ) : (
        <input
          type="text"
          className="modal-input"
          value={value === undefined || value === null ? '' : String(value)}
          onChange={(e) => onChange(name, e.target.value)}
        />
      )}
    </div>
  );
}

export default function AddStepModal({
  recipes,
  nextStepOrder,
  availableColumns,
  onSave,
  onClose,
}: AddStepModalProps) {
  const [search, setSearch] = useState('');
  const [selectedRecipe, setSelectedRecipe] = useState<RecipeDefinition | null>(null);
  const [config, setConfig] = useState<Record<string, unknown>>({});
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const filtered = recipes.filter(
    (r) =>
      r.label.toLowerCase().includes(search.toLowerCase()) ||
      (r.description ?? '').toLowerCase().includes(search.toLowerCase()),
  );

  useEffect(() => {
    if (!selectedRecipe) return;
    const defaults: Record<string, unknown> = {};
    const props = selectedRecipe.config_schema?.properties ?? {};

    for (const [key, def] of Object.entries(props as Record<string, { default?: unknown; type?: string }>)) {
      if (def.default !== undefined) {
        defaults[key] = def.default;
      } else if (def.type === 'object') {
        defaults[key] = {};
      } else if (def.type === 'array') {
        defaults[key] = [];
      }
    }

    setConfig(defaults);
    setError(null);
  }, [selectedRecipe]);

  const handleFieldChange = (name: string, value: unknown) => {
    setConfig((prev) => ({ ...prev, [name]: value }));
  };

  const handleSave = async () => {
    if (!selectedRecipe) return;

    const required: string[] = selectedRecipe.config_schema?.required ?? [];
    for (const field of required) {
      const v = config[field];
      if (
        v === undefined ||
        v === null ||
        v === '' ||
        (typeof v === 'object' && Object.keys(v as object).length === 0)
      ) {
        setError(`"${field}" is required.`);
        return;
      }
    }

    setSaving(true);
    setError(null);
    try {
      await onSave({
        step_order: nextStepOrder,
        recipe_type: selectedRecipe.recipe_type,
        config_json: config,
        is_enabled: true,
      });
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save step.');
      setSaving(false);
    }
  };

  const schemaProps = selectedRecipe?.config_schema?.properties ?? {};

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
      <div className="bg-[var(--bg-surface)] rounded-xl shadow-2xl w-full max-w-xl mx-4 flex flex-col max-h-[80vh] border border-[var(--border-default)]">
        <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border-subtle)]">
          <h3 className="font-semibold text-[var(--text-primary)]">Add Transformation Step</h3>
          <button type="button" onClick={onClose} className="text-[var(--text-muted)] hover:text-[var(--text-primary)]">
            <X size={18} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-5 flex flex-col gap-5">
          {!selectedRecipe ? (
            <div className="flex flex-col gap-3">
              <div className="relative">
                <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-[var(--text-muted)]" />
                <input
                  className="modal-input pl-8"
                  placeholder="Search recipes..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  autoFocus
                />
              </div>
              <div className="flex flex-col gap-1 max-h-72 overflow-y-auto">
                {filtered.map((r) => (
                  <button
                    key={r.recipe_type}
                    type="button"
                    onClick={() => setSelectedRecipe(r)}
                    className="flex flex-col items-start px-4 py-3 rounded-lg border border-[var(--border-default)] hover:border-[rgba(139,92,246,0.35)] hover:bg-[rgba(139,92,246,0.08)] transition-colors text-left"
                  >
                    <span className="text-sm font-medium text-[var(--text-primary)]">{r.label}</span>
                    <span className="text-xs text-[var(--text-secondary)] mt-0.5">{r.description}</span>
                  </button>
                ))}
                {filtered.length === 0 && (
                  <p className="text-sm text-[var(--text-muted)] text-center py-6">No matching recipes.</p>
                )}
              </div>
            </div>
          ) : (
            <div className="flex flex-col gap-4">
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => setSelectedRecipe(null)}
                  className="text-xs text-[var(--brand)] hover:underline"
                >
                  {'<-'} Back
                </button>
                <span className="text-xs text-[var(--text-muted)]">.</span>
                <span className="text-sm font-medium text-[var(--text-primary)]">{selectedRecipe.label}</span>
              </div>

              {Object.entries(
                schemaProps as Record<string, { type?: string; description?: string; default?: unknown; items?: { type?: string } }>,
              ).map(([fieldName, fieldSchema]) => (
                <SchemaField
                  key={fieldName}
                  name={fieldName}
                  schema={fieldSchema}
                  recipeType={selectedRecipe.recipe_type}
                  availableColumns={availableColumns}
                  value={config[fieldName]}
                  onChange={handleFieldChange}
                />
              ))}

              {Object.keys(schemaProps).length === 0 && (
                <p className="text-sm text-[var(--text-secondary)]">This recipe has no configuration.</p>
              )}

              {error && <p className="text-sm text-red-300">{error}</p>}
            </div>
          )}
        </div>

        {selectedRecipe && (
          <div className="px-5 py-4 border-t border-[var(--border-subtle)] flex justify-end gap-3">
            <button type="button" onClick={onClose} className="btn-ghost text-sm">
              Cancel
            </button>
            <button type="button" onClick={handleSave} disabled={saving} className="btn-primary text-sm">
              {saving ? 'Saving...' : 'Add Step'}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
