import React, { useState, useEffect, useRef } from 'react';
import { X, UploadCloud, FileText, CheckCircle2, AlertCircle } from 'lucide-react';
import { previewLocalFile, uploadLocalFiles } from '../../services/backendClient';
import type { ExcelSheetUploadSelection } from '../../services/backendClient';
import type { RawTablePreview, UserConnectorConfig } from '../../types';

interface LocalFileUploadModalProps {
  onClose: () => void;
  onSuccess: (config: UserConnectorConfig) => void;
  initialConfigId?: string;
  initialSourceName?: string;
}

const ACCEPTED_FORMATS = '.csv,.json,.parquet,.xlsx,.xls';
const ALL_SHEETS_OPTION = '__all_sheets__';

type ExcelUploadChoice = {
  mode: 'single' | 'all';
  sheetName?: string;
  previewSheetName?: string;
};

function isExcelFile(file: File): boolean {
  return /\.(xlsx|xls)$/i.test(file.name);
}

function getFileBaseKey(file: File): string {
  return `${file.name}-${file.size}`;
}

function getPreviewKey(file: File, sheetName?: string): string {
  const base = getFileBaseKey(file);
  return `${base}::${sheetName ?? '__default__'}`;
}

export default function LocalFileUploadModal({
  onClose,
  onSuccess,
  initialConfigId,
  initialSourceName,
}: LocalFileUploadModalProps) {
  const [files, setFiles] = useState<File[]>([]);
  const [sourceName, setSourceName] = useState(initialSourceName || '');
  const [activeTab, setActiveTab] = useState(0);
  const [previews, setPreviews] = useState<Record<string, RawTablePreview>>({});
  const [previewLoading, setPreviewLoading] = useState<Record<string, boolean>>({});
  const [previewError, setPreviewError] = useState<Record<string, string>>({});
  const [excelSheetsByFile, setExcelSheetsByFile] = useState<Record<string, string[]>>({});
  const [excelChoiceByFile, setExcelChoiceByFile] = useState<Record<string, ExcelUploadChoice>>({});
  
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      const newFiles = Array.from(e.target.files);
      setFiles((prev) => [...prev, ...newFiles]);
    }
    // reset input
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const removeFile = (index: number) => {
    const removedFile = files[index];
    if (removedFile) {
      const removedKey = getFileBaseKey(removedFile);
      setExcelSheetsByFile((prev) => {
        if (!(removedKey in prev)) return prev;
        const next = { ...prev };
        delete next[removedKey];
        return next;
      });
      setExcelChoiceByFile((prev) => {
        if (!(removedKey in prev)) return prev;
        const next = { ...prev };
        delete next[removedKey];
        return next;
      });
    }

    setFiles((prev) => prev.filter((_, i) => i !== index));
    if (activeTab === index) {
      setActiveTab(Math.max(0, index - 1));
    } else if (activeTab > index) {
      setActiveTab(activeTab - 1);
    }
  };

  useEffect(() => {
    // Lazily fetch preview for active tab
    if (files.length === 0) return;
    const activeFile = files[activeTab];
    if (!activeFile) return;

    const fileBaseKey = getFileBaseKey(activeFile);
    const choice = excelChoiceByFile[fileBaseKey];
    const previewSheetName = isExcelFile(activeFile)
      ? (choice?.mode === 'all' ? choice.previewSheetName : choice?.sheetName)
      : undefined;
    const filePreviewKey = getPreviewKey(activeFile, previewSheetName);

    if (previews[filePreviewKey] || previewLoading[filePreviewKey] || previewError[filePreviewKey]) {
      return;
    }

    setPreviewLoading((prev) => ({ ...prev, [filePreviewKey]: true }));
    previewLocalFile(activeFile, {
      sheetName: previewSheetName,
      allSheets: choice?.mode === 'all',
    })
      .then((data) => {
        setPreviews((prev) => ({ ...prev, [filePreviewKey]: data }));

        if (!isExcelFile(activeFile)) return;

        const availableSheets = data.available_sheets ?? [];
        if (availableSheets.length === 0) return;

        setExcelSheetsByFile((prev) => ({
          ...prev,
          [fileBaseKey]: availableSheets,
        }));

        setExcelChoiceByFile((prev) => {
          const existing = prev[fileBaseKey];
          const fallbackSheet = data.sheet_name ?? availableSheets[0];
          if (!fallbackSheet) return prev;
          if (!existing) {
            return {
              ...prev,
              [fileBaseKey]: {
                mode: 'single',
                sheetName: fallbackSheet,
                previewSheetName: fallbackSheet,
              },
            };
          }

          if (existing.mode === 'all') {
            const nextPreview =
              existing.previewSheetName && availableSheets.includes(existing.previewSheetName)
                ? existing.previewSheetName
                : fallbackSheet;
            return {
              ...prev,
              [fileBaseKey]: {
                ...existing,
                previewSheetName: nextPreview,
              },
            };
          }

          const nextSheet =
            existing.sheetName && availableSheets.includes(existing.sheetName)
              ? existing.sheetName
              : fallbackSheet;
          return {
            ...prev,
            [fileBaseKey]: {
              mode: 'single',
              sheetName: nextSheet,
              previewSheetName: nextSheet,
            },
          };
        });
      })
      .catch((err) => {
        setPreviewError((prev) => ({
          ...prev,
          [filePreviewKey]: err instanceof Error ? err.message : 'Could not preview file',
        }));
      })
      .finally(() => {
        setPreviewLoading((prev) => ({ ...prev, [filePreviewKey]: false }));
      });
  }, [activeTab, files, previews, previewLoading, previewError, excelChoiceByFile]);

  const handleExcelUploadChoiceChange = (selection: string) => {
    const active = files[activeTab];
    if (!active || !isExcelFile(active)) return;

    const fileBaseKey = getFileBaseKey(active);
    const sheets = excelSheetsByFile[fileBaseKey] ?? [];
    const fallbackSheet = sheets[0];
    if (!fallbackSheet) return;

    if (selection === ALL_SHEETS_OPTION) {
      setExcelChoiceByFile((prev) => ({
        ...prev,
        [fileBaseKey]: {
          mode: 'all',
          previewSheetName: prev[fileBaseKey]?.previewSheetName ?? fallbackSheet,
        },
      }));
      return;
    }

    setExcelChoiceByFile((prev) => ({
      ...prev,
      [fileBaseKey]: {
        mode: 'single',
        sheetName: selection,
        previewSheetName: selection,
      },
    }));
  };

  const handleExcelPreviewSheetChange = (sheetName: string) => {
    const active = files[activeTab];
    if (!active || !isExcelFile(active)) return;

    const fileBaseKey = getFileBaseKey(active);
    setExcelChoiceByFile((prev) => {
      const existing = prev[fileBaseKey];
      if (!existing || existing.mode !== 'all') return prev;
      return {
        ...prev,
        [fileBaseKey]: {
          ...existing,
          previewSheetName: sheetName,
        },
      };
    });
  };

  const handleUpload = async () => {
    if (!sourceName.trim() && !initialConfigId) {
      setUploadError('Please provide a name for this data source.');
      return;
    }
    if (files.length === 0) {
      setUploadError('Please select at least one file.');
      return;
    }

    setUploading(true);
    setUploadError(null);
    try {
      const excelSelections: Array<ExcelSheetUploadSelection | null> = files.map((file) => {
        if (!isExcelFile(file)) return null;
        const fileBaseKey = getFileBaseKey(file);
        const choice = excelChoiceByFile[fileBaseKey];
        if (!choice || choice.mode === 'single') {
          return {
            mode: 'single',
            sheetName: choice?.sheetName,
          };
        }
        return { mode: 'all' };
      });

      const config = await uploadLocalFiles(
        files,
        sourceName.trim(),
        initialConfigId,
        excelSelections,
      );
      onSuccess(config);
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : 'Upload failed.');
    } finally {
      setUploading(false);
    }
  };

  const activeFile = files[activeTab];
  const activeFileBaseKey = activeFile ? getFileBaseKey(activeFile) : '';
  const activeChoice = activeFileBaseKey ? excelChoiceByFile[activeFileBaseKey] : undefined;
  const activeSheets = activeFileBaseKey ? excelSheetsByFile[activeFileBaseKey] ?? [] : [];
  const activePreviewSheet = activeFile
    ? (isExcelFile(activeFile)
      ? (activeChoice?.mode === 'all' ? activeChoice.previewSheetName : activeChoice?.sheetName)
      : undefined)
    : undefined;
  const activePreviewKey = activeFile ? getPreviewKey(activeFile, activePreviewSheet) : '';

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60"
      role="dialog"
      aria-modal="true"
      aria-labelledby="upload-modal-title"
    >
      <div className="w-full max-w-4xl max-h-[90vh] flex flex-col rounded-2xl border border-[var(--border-default)] bg-[var(--bg-surface)] shadow-2xl">
        {/* Header */}
        <header className="shrink-0 flex items-center justify-between border-b border-[var(--border-default)] px-6 py-4">
          <h2 id="upload-modal-title" className="text-lg font-semibold text-[var(--text-primary)] tracking-tight">
            {initialConfigId ? 'Upload Additional Files' : 'Upload Local Files'}
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1.5 text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-elevated)] transition-colors duration-150 focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
          >
            <X size={20} aria-hidden />
          </button>
        </header>

        {/* Body */}
        <div className="flex-1 min-h-0 overflow-y-auto px-6 py-5 space-y-6">
          <div className="space-y-4">
            {!initialConfigId && (
              <label className="block">
                <span className="block text-sm font-medium text-[var(--text-primary)] mb-1">
                  Source Name
                </span>
                <input
                  type="text"
                  placeholder="e.g. Sales Data 2026"
                  value={sourceName}
                  onChange={(e) => setSourceName(e.target.value)}
                  className="w-full rounded-xl border border-[var(--border-default)] bg-[var(--bg-canvas)] px-4 py-2.5 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--brand)] focus:ring-2 focus:ring-[var(--brand)]/20 transition-all duration-200"
                />
              </label>
            )}

            <div>
              <span className="block text-sm font-medium text-[var(--text-primary)] mb-1">
                Select Files
              </span>
              <div
                className="mt-1 flex justify-center rounded-xl border border-dashed border-[var(--border-strong)] px-6 py-10 transition-colors hover:border-[var(--brand)]/50 hover:bg-[var(--bg-elevated)]/30 cursor-pointer"
                onClick={() => fileInputRef.current?.click()}
              >
                <div className="text-center">
                  <UploadCloud className="mx-auto h-10 w-10 text-[var(--text-muted)] mb-3" />
                  <div className="mt-2 flex text-sm leading-6 text-[var(--text-secondary)]">
                    <label className="relative cursor-pointer rounded-md font-semibold text-[var(--brand)] focus-within:outline-none focus-within:ring-2 focus-within:ring-[var(--brand)] focus-within:ring-offset-2 hover:underline">
                      <span>Click to upload</span>
                      <input
                        ref={fileInputRef}
                        type="file"
                        multiple
                        accept={ACCEPTED_FORMATS}
                        className="sr-only"
                        onChange={handleFileSelect}
                        onClick={(e) => e.stopPropagation()}
                      />
                    </label>
                    <p className="pl-1">or drag and drop</p>
                  </div>
                  <p className="text-xs leading-5 text-[var(--text-muted)] mt-1">
                    CSV, JSON, Excel, or Parquet
                  </p>
                </div>
              </div>
            </div>
          </div>

          {files.length > 0 && (
            <div className="border border-[var(--border-default)] rounded-xl overflow-hidden flex flex-col h-96">
              {/* File Tabs */}
              <div className="flex items-center overflow-x-auto border-b border-[var(--border-default)] bg-[var(--bg-elevated)] px-2 pt-2 gap-1 shrink-0">
                {files.map((f, idx) => (
                  <div
                    key={`${f.name}-${idx}`}
                    className={`group relative flex items-center gap-2 rounded-t-lg px-3 py-2 text-sm font-medium cursor-pointer ${
                      activeTab === idx
                        ? 'bg-[var(--bg-surface)] text-[var(--brand)] border-t border-l border-r border-[var(--border-default)]'
                        : 'text-[var(--text-secondary)] hover:bg-[var(--bg-surface)]/50 hover:text-[var(--text-primary)]'
                    }`}
                    onClick={() => setActiveTab(idx)}
                  >
                    <FileText size={14} />
                    <span className="truncate max-w-[120px]" title={f.name}>{f.name}</span>
                    <button
                      type="button"
                      className="ml-1 p-0.5 rounded-md text-[var(--text-muted)] hover:text-red-400 hover:bg-red-400/10 opacity-0 group-hover:opacity-100 transition-opacity"
                      onClick={(e) => {
                        e.stopPropagation();
                        removeFile(idx);
                      }}
                    >
                      <X size={14} />
                    </button>
                  </div>
                ))}
              </div>

              {/* Preview Content */}
              <div className="flex-1 min-h-0 bg-[var(--bg-canvas)] overflow-auto relative">
                {activeFile && isExcelFile(activeFile) && activeSheets.length > 0 && (
                  <div className="sticky top-0 z-20 border-b border-[var(--border-default)] bg-[var(--bg-surface)]/95 px-4 py-3 backdrop-blur-sm">
                    <div className="flex flex-wrap items-center gap-3">
                      <label className="text-xs text-[var(--text-secondary)] flex items-center gap-2">
                        <span className="font-medium">Upload</span>
                        <select
                          value={activeChoice?.mode === 'all' ? ALL_SHEETS_OPTION : (activeChoice?.sheetName ?? activeSheets[0])}
                          onChange={(e) => handleExcelUploadChoiceChange(e.target.value)}
                          className="rounded-lg border border-[var(--border-default)] bg-[var(--bg-canvas)] px-2.5 py-1.5 text-xs text-[var(--text-primary)] focus:outline-none focus:border-[var(--brand)]"
                        >
                          <option value={ALL_SHEETS_OPTION}>All Sheets</option>
                          {activeSheets.map((sheet) => (
                            <option key={sheet} value={sheet}>{sheet}</option>
                          ))}
                        </select>
                      </label>
                      {activeChoice?.mode === 'all' && (
                        <label className="text-xs text-[var(--text-secondary)] flex items-center gap-2">
                          <span className="font-medium">Preview</span>
                          <select
                            value={activeChoice.previewSheetName ?? activeSheets[0]}
                            onChange={(e) => handleExcelPreviewSheetChange(e.target.value)}
                            className="rounded-lg border border-[var(--border-default)] bg-[var(--bg-canvas)] px-2.5 py-1.5 text-xs text-[var(--text-primary)] focus:outline-none focus:border-[var(--brand)]"
                          >
                            {activeSheets.map((sheet) => (
                              <option key={sheet} value={sheet}>{sheet}</option>
                            ))}
                          </select>
                        </label>
                      )}
                    </div>
                  </div>
                )}

                {!activePreviewKey && (
                  <div className="absolute inset-0 flex items-center justify-center text-[var(--text-muted)]">
                    Select a file to preview
                  </div>
                )}
                {activePreviewKey && previewLoading[activePreviewKey] && (
                  <div className="absolute inset-0 flex items-center justify-center">
                    <div className="animate-spin text-[var(--brand)]">
                      <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                      </svg>
                    </div>
                  </div>
                )}
                {activePreviewKey && previewError[activePreviewKey] && (
                  <div className="absolute inset-0 flex items-center justify-center flex-col text-red-400/90 gap-2 p-6 text-center">
                    <AlertCircle size={32} />
                    <p className="text-sm">{previewError[activePreviewKey]}</p>
                  </div>
                )}
                {activePreviewKey && previews[activePreviewKey] && (
                  <table className="w-full text-left border-collapse text-sm">
                    <thead className="sticky top-0 bg-[var(--bg-surface)] z-10 border-b border-[var(--border-default)] shadow-sm">
                      <tr>
                        {previews[activePreviewKey].columns.map((col, i) => (
                          <th key={i} className="px-4 py-2 font-medium text-[var(--text-secondary)] whitespace-nowrap">
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[var(--border-subtle)]">
                      {previews[activePreviewKey].rows.map((row, rIdx) => (
                        <tr key={rIdx} className="hover:bg-[var(--bg-surface)]/50 transition-colors">
                          {row.map((cell, cIdx) => (
                            <td key={cIdx} className="px-4 py-2 text-[var(--text-primary)] whitespace-nowrap overflow-hidden max-w-[200px] text-ellipsis">
                              {String(cell ?? '')}
                            </td>
                          ))}
                        </tr>
                      ))}
                      {previews[activePreviewKey].rows.length === 0 && (
                        <tr>
                          <td colSpan={previews[activePreviewKey].columns.length || 1} className="px-4 py-8 text-center text-[var(--text-muted)] italic">
                            No rows found in this file.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          )}

          {uploadError && (
            <div className="rounded-xl border border-red-500/25 bg-red-950/25 px-4 py-3 text-sm text-red-200/95 flex items-start gap-3">
              <AlertCircle size={18} className="shrink-0 mt-0.5" />
              <span>{uploadError}</span>
            </div>
          )}
        </div>

        {/* Footer */}
        <footer className="shrink-0 border-t border-[var(--border-default)] bg-[var(--bg-elevated)]/30 px-6 py-4 flex items-center justify-end gap-3 rounded-b-2xl">
          <button
            type="button"
            className="rounded-lg px-4 py-2 text-sm font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-surface)] hover:text-[var(--text-primary)] transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)]"
            onClick={onClose}
            disabled={uploading}
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => void handleUpload()}
            disabled={uploading || files.length === 0}
            className="inline-flex items-center gap-2 rounded-lg bg-[var(--brand)] px-5 py-2 text-sm font-medium text-white hover:opacity-90 disabled:opacity-50 transition-opacity focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--brand)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--bg-surface)] shadow-md"
          >
            {uploading ? (
              <>
                <div className="animate-spin">
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                    <path d="M21 12a9 9 0 1 1-6.219-8.56" />
                  </svg>
                </div>
                Uploading...
              </>
            ) : (
              <>
                <CheckCircle2 size={16} />
                Upload {files.length > 0 && `${files.length} File${files.length !== 1 ? 's' : ''}`}
              </>
            )}
          </button>
        </footer>
      </div>
    </div>
  );
}
