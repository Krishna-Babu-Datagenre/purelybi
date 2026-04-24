import { useState, useCallback } from 'react';
import { Plus, Save, X } from 'lucide-react';
import type { RelationshipKind, ColumnMetadata, TableMetadata } from '../../../types/metadata';

const KIND_OPTIONS: { value: RelationshipKind; label: string }[] = [
  { value: 'many_to_one', label: 'Many to One' },
  { value: 'one_to_one', label: 'One to One' },
  { value: 'many_to_many', label: 'Many to Many' },
];

interface AddRelationshipPanelProps {
  tables: TableMetadata[];
  columns: ColumnMetadata[];
  onAdd: (rel: {
    from_table: string;
    from_column: string;
    to_table: string;
    to_column: string;
    kind: RelationshipKind;
  }) => Promise<void>;
}

export default function AddRelationshipPanel({
  tables,
  columns,
  onAdd,
}: AddRelationshipPanelProps) {
  const [open, setOpen] = useState(false);
  const [fromTable, setFromTable] = useState('');
  const [fromColumn, setFromColumn] = useState('');
  const [toTable, setToTable] = useState('');
  const [toColumn, setToColumn] = useState('');
  const [kind, setKind] = useState<RelationshipKind | ''>('');
  const [saving, setSaving] = useState(false);

  const tableNames = tables.map((t) => t.table_name);
  const colsFor = (tn: string) =>
    columns.filter((c) => c.table_name === tn).map((c) => c.column_name);

  const reset = useCallback(() => {
    setFromTable('');
    setFromColumn('');
    setToTable('');
    setToColumn('');
    setKind('');
  }, []);

  const canSave = fromTable && fromColumn && toTable && toColumn && kind;

  const handleSave = useCallback(async () => {
    if (!canSave) return;
    setSaving(true);
    try {
      await onAdd({
        from_table: fromTable,
        from_column: fromColumn,
        to_table: toTable,
        to_column: toColumn,
        kind: kind as RelationshipKind,
      });
      reset();
      setOpen(false);
    } finally {
      setSaving(false);
    }
  }, [fromTable, fromColumn, toTable, toColumn, kind, canSave, onAdd, reset]);

  if (!open) {
    return (
      <button
        type="button"
        className="sv-add-btn"
        onClick={() => setOpen(true)}
      >
        <Plus size={14} />
        Add Relationship
      </button>
    );
  }

  return (
    <div className="sv-add-panel">
      <div className="sv-add-panel__grid">
        <div className="sv-add-panel__field">
          <label className="sv-add-panel__label">From Table</label>
          <select
            className="sv-select"
            value={fromTable}
            onChange={(e) => {
              setFromTable(e.target.value);
              setFromColumn('');
            }}
          >
            <option value="">Select…</option>
            {tableNames.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>

        <div className="sv-add-panel__field">
          <label className="sv-add-panel__label">From Column</label>
          <select
            className="sv-select"
            value={fromColumn}
            onChange={(e) => setFromColumn(e.target.value)}
            disabled={!fromTable}
          >
            <option value="">Select…</option>
            {fromTable && colsFor(fromTable).map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>

        <div className="sv-add-panel__field">
          <label className="sv-add-panel__label">To Table</label>
          <select
            className="sv-select"
            value={toTable}
            onChange={(e) => {
              setToTable(e.target.value);
              setToColumn('');
            }}
          >
            <option value="">Select…</option>
            {tableNames.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>

        <div className="sv-add-panel__field">
          <label className="sv-add-panel__label">To Column</label>
          <select
            className="sv-select"
            value={toColumn}
            onChange={(e) => setToColumn(e.target.value)}
            disabled={!toTable}
          >
            <option value="">Select…</option>
            {toTable && colsFor(toTable).map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>
      </div>

      <div className="sv-add-panel__actions">
        <div className="sv-add-panel__field">
          <label className="sv-add-panel__label">Kind</label>
          <select
            className="sv-select"
            value={kind}
            onChange={(e) => setKind(e.target.value as RelationshipKind)}
          >
            <option value="">Select…</option>
            {KIND_OPTIONS.map((k) => (
              <option key={k.value} value={k.value}>{k.label}</option>
            ))}
          </select>
        </div>

        <div className="sv-add-panel__btns">
          <button
            type="button"
            className="sv-cancel-btn"
            onClick={() => { reset(); setOpen(false); }}
          >
            <X size={13} />
            Cancel
          </button>
          <button
            type="button"
            className="sv-save-btn"
            disabled={!canSave || saving}
            onClick={handleSave}
          >
            <Save size={13} />
            {saving ? 'Saving…' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  );
}
