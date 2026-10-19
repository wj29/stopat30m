import api from './client';

export interface DataStatus {
  data_dir: string;
  data_available: boolean;
  calendar_exists?: boolean;
  trusted_until?: string;
  last_append?: string;
  stock_count?: number;
  status: 'missing' | 'degraded' | 'stale' | 'available';
  last_data_date: string | null;
  latest_calendar_date: string | null;
  age_calendar_days: number | null;
  missing_stock_count: number;
  lagging_stock_count: number;
  excluded_stock_count: number;
  coverage_pct: number | null;
  warnings: string[];
  freshness_confirmed: boolean;
  checked_at: string;
}

export interface DataUpdateJob {
  id: string;
  status: 'running' | 'completed' | 'failed' | 'interrupted';
  source: string;
  started_at: string;
  completed_at: string | null;
  error: string | null;
  requested_end_date: string;
}

export async function getDataUpdate(): Promise<DataUpdateJob | null> {
  return (await api.get('/system/data-update')).data;
}

export async function startDataUpdate(payload: { source: string; start_date?: string; end_date?: string }): Promise<DataUpdateJob> {
  return (await api.post('/system/data-update', payload)).data;
}

export interface ConfigSummary {
  qlib_provider_uri: string;
  universe: string;
  model_type: string;
  signal_method: string;
  signal_top_k: number;
  llm_enabled: boolean;
  llm_model: string;
  llm_keys_configured: Record<string, boolean>;
  llm_base_urls: Record<string, string>;
}

export interface ModelFile {
  name: string;
  size_mb: number;
}

export async function getDataStatus(): Promise<DataStatus> {
  const { data } = await api.get('/system/data-status');
  return data;
}

export async function getConfigSummary(): Promise<ConfigSummary> {
  const { data } = await api.get('/system/config');
  return data;
}

export async function listModels(): Promise<ModelFile[]> {
  const { data } = await api.get('/system/models');
  return data;
}

// -- Model Lab APIs --

export interface ModelConfiguration extends Record<string, unknown> {
  model?: { type?: string; params?: Record<string, Record<string, unknown>> };
  factors?: { label?: string; [key: string]: unknown };
  data?: { universe?: string; [key: string]: unknown };
}

export async function getModelConfig(): Promise<ModelConfiguration> {
  const { data } = await api.get('/system/model-config');
  return data;
}

export async function updateModelConfig(payload: Record<string, unknown>): Promise<{ status: string; updated_sections: string }> {
  const { data } = await api.put('/system/model-config', payload);
  return data;
}

export async function generateTrainCommand(payload: {
  model_type?: string;
  universe?: string;
  save_name?: string;
  factor_groups?: string;
  top_k?: number;
}): Promise<{ command: string }> {
  const { data } = await api.post('/system/generate-train-command', payload);
  return data;
}
