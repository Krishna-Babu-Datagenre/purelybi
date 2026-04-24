/// <reference types="vite/client" />
import {
  TableMetadata,
  TableMetadataPatch,
  ColumnMetadata,
  ColumnMetadataPatch,
  Relationship,
  RelationshipCreate,
  RelationshipPatch,
  MetadataJob,
  MetadataGenerationResponse,
  ColumnValuesResponse,
} from '../types/metadata';
import { fetchWithAuthRetry } from './authSession';

/* ─────────────────────────────────────────────
   Metadata API Client
   ─────────────────────────────────────────────
   Typed wrapper for /api/metadata/* endpoints.
   Uses the same auth pattern as backendClient.ts.
───────────────────────────────────────────── */

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetchWithAuthRetry(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers as Record<string, string>),
    },
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = typeof body.detail === 'string' ? body.detail : body.detail?.msg ?? body.message;
    throw new Error(detail ?? `Request failed: ${res.status} ${res.statusText}`);
  }

  return res.json() as Promise<T>;
}

async function requestNoContent(path: string, init?: RequestInit): Promise<void> {
  const res = await fetchWithAuthRetry(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers as Record<string, string>),
    },
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = typeof body.detail === 'string' ? body.detail : body.detail?.msg ?? body.message;
    throw new Error(detail ?? `Request failed: ${res.status} ${res.statusText}`);
  }
}

/* ── Tables ── */

export function listTableMetadata(): Promise<TableMetadata[]> {
  return request<TableMetadata[]>('/api/metadata/tables');
}

export function patchTableMetadata(
  tableName: string,
  patch: TableMetadataPatch,
): Promise<TableMetadata> {
  return request<TableMetadata>(
    `/api/metadata/tables/${encodeURIComponent(tableName)}`,
    { method: 'PATCH', body: JSON.stringify(patch) },
  );
}

/* ── Columns ── */

export function listColumnMetadata(table?: string): Promise<ColumnMetadata[]> {
  const q = table ? `?table=${encodeURIComponent(table)}` : '';
  return request<ColumnMetadata[]>(`/api/metadata/columns${q}`);
}

export function patchColumnMetadata(
  tableName: string,
  columnName: string,
  patch: ColumnMetadataPatch,
): Promise<ColumnMetadata> {
  return request<ColumnMetadata>(
    `/api/metadata/columns/${encodeURIComponent(tableName)}/${encodeURIComponent(columnName)}`,
    { method: 'PATCH', body: JSON.stringify(patch) },
  );
}

/* ── Distinct values (for categorical filter dropdowns) ── */

export function getColumnValues(
  table: string,
  column: string,
  limit = 500,
): Promise<ColumnValuesResponse> {
  const q = new URLSearchParams({ table, column, limit: String(limit) });
  return request<ColumnValuesResponse>(`/api/metadata/values?${q}`);
}

/* ── Relationships ── */

export function listRelationships(): Promise<Relationship[]> {
  return request<Relationship[]>('/api/metadata/relationships');
}

export function createRelationship(body: RelationshipCreate): Promise<Relationship> {
  return request<Relationship>('/api/metadata/relationships', {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export function patchRelationship(
  fromTable: string,
  fromColumn: string,
  toTable: string,
  toColumn: string,
  patch: RelationshipPatch,
): Promise<Relationship> {
  const path = [fromTable, fromColumn, toTable, toColumn].map(encodeURIComponent).join('/');
  return request<Relationship>(`/api/metadata/relationships/${path}`, {
    method: 'PATCH',
    body: JSON.stringify(patch),
  });
}

export function deleteRelationship(
  fromTable: string,
  fromColumn: string,
  toTable: string,
  toColumn: string,
): Promise<void> {
  const path = [fromTable, fromColumn, toTable, toColumn].map(encodeURIComponent).join('/');
  return requestNoContent(`/api/metadata/relationships/${path}`, {
    method: 'DELETE',
  });
}

/* ── Jobs ── */

export function getLatestJob(): Promise<MetadataJob | null> {
  return request<MetadataJob | null>('/api/metadata/jobs/latest');
}

export function getJob(jobId: string): Promise<MetadataJob> {
  return request<MetadataJob>(`/api/metadata/jobs/${encodeURIComponent(jobId)}`);
}

export function triggerGeneration(): Promise<MetadataGenerationResponse> {
  return request<MetadataGenerationResponse>('/api/metadata/generate', {
    method: 'POST',
  });
}
