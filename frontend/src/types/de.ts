export interface RecipeDefinition {
  recipe_type: string;
  label: string;
  description: string | null;
  config_schema: any;
}

export interface DEPipeline {
  id: string;
  user_id: string;
  connector_config_id: string;
  source_connector_ids: string[];
  name: string;
  is_active: boolean;
  version: number;
}

export interface DEPipelineCreate {
  connector_config_id?: string;
  source_connector_ids: string[];
  name?: string;
}

export interface DEPipelinePatch {
  name?: string;
  is_active?: boolean;
  source_connector_ids?: string[];
}

export interface DEPipelineStep {
  id: string;
  pipeline_id: string;
  step_order: number;
  recipe_type: string;
  config_json: any;
  is_enabled: boolean;
}

export interface DEPipelineStepUpsert {
  step_order: number;
  recipe_type: string;
  config_json: any;
  is_enabled?: boolean;
}

export interface DEPipelineDetail extends DEPipeline {
  steps: DEPipelineStep[];
}

export interface DEPipelineDeleteResponse {
  pipeline_id: string;
  cleaned_source_connector_ids: string[];
  retained_source_connector_ids: string[];
  deleted_output_prefixes: string[];
  deleted_blob_count: number;
}

export interface DEPipelineRun {
  id: string;
  pipeline_id: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'failed_to_start';
  started_at: string | null;
  ended_at: string | null;
  error: string | null;
}

export interface DEValidationRequest {
  sample_rows: any[];
}

export interface DEStepValidationResult {
  step_order: number;
  recipe_type: string;
  ok: boolean;
  error: string | null;
}

export interface DEValidationResponse {
  ok: boolean;
  step_results: DEStepValidationResult[];
  output_sample: any[];
}
