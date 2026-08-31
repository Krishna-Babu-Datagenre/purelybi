import { request, requestNoContent } from './backendClient';
import type {
  RecipeDefinition,
  DEPipeline,
  DEPipelineCreate,
  DEPipelinePatch,
  DEPipelineStep,
  DEPipelineStepUpsert,
  DEPipelineDetail,
  DEPipelineDeleteResponse,
  DEValidationRequest,
  DEValidationResponse,
  DEPipelineRun,
} from '../types/de';

export function listRecipes(): Promise<RecipeDefinition[]> {
  return request<RecipeDefinition[]>('/api/de/recipes');
}

export function createPipeline(body: DEPipelineCreate): Promise<DEPipeline> {
  return request<DEPipeline>('/api/de/pipelines', {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export function listPipelines(connector_config_id?: string): Promise<DEPipeline[]> {
  const suffix = connector_config_id
    ? `?connector_config_id=${encodeURIComponent(connector_config_id)}`
    : '';
  return request<DEPipeline[]>(`/api/de/pipelines/all${suffix}`);
}

export function getPipeline(id: string): Promise<DEPipelineDetail> {
  return request<DEPipelineDetail>(`/api/de/pipelines/${encodeURIComponent(id)}`);
}

export function getPipelineByConnector(connector_config_id: string): Promise<DEPipelineDetail | null> {
  return request<DEPipelineDetail | null>(`/api/de/pipelines?connector_config_id=${encodeURIComponent(connector_config_id)}`);
}

export function patchPipeline(id: string, body: DEPipelinePatch): Promise<DEPipeline> {
  return request<DEPipeline>(`/api/de/pipelines/${encodeURIComponent(id)}`, {
    method: 'PATCH',
    body: JSON.stringify(body),
  });
}

export function deletePipeline(id: string): Promise<DEPipelineDeleteResponse> {
  return request<DEPipelineDeleteResponse>(`/api/de/pipelines/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  });
}

export function upsertPipelineStep(pipelineId: string, body: DEPipelineStepUpsert): Promise<DEPipelineStep> {
  return request<DEPipelineStep>(`/api/de/pipelines/${encodeURIComponent(pipelineId)}/steps`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export function deletePipelineStep(pipelineId: string, stepId: string): Promise<void> {
  return requestNoContent(`/api/de/pipelines/${encodeURIComponent(pipelineId)}/steps/${encodeURIComponent(stepId)}`, {
    method: 'DELETE',
  });
}

export function validatePipeline(pipelineId: string, body: DEValidationRequest): Promise<DEValidationResponse> {
  return request<DEValidationResponse>(`/api/de/pipelines/${encodeURIComponent(pipelineId)}/validate`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export function listPipelineRuns(pipelineId: string): Promise<DEPipelineRun[]> {
  return request<DEPipelineRun[]>(`/api/de/pipelines/${encodeURIComponent(pipelineId)}/runs`);
}

export function triggerPipelineRun(pipelineId: string): Promise<DEPipelineRun> {
  return request<DEPipelineRun>(`/api/de/pipelines/${encodeURIComponent(pipelineId)}/runs`, {
    method: 'POST',
  });
}
