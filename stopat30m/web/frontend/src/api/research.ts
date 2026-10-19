import api from './client';

export interface ResearchProtocol {
  id: string;
  label: string;
  source: string;
  calendar_source: string;
  request: { instruments: string[]; start_date: string; end_date: string; adjustment: string };
  config: Record<string, unknown>;
  universe_kind: string;
  protocol_sha256: string;
  limitations: string[];
}

export interface ResearchSnapshot {
  id: string;
  created_at: string;
  source: string;
  start_date: string;
  end_date: string;
  adjustment: string;
  instruments: string[];
  session_count: number;
  compatible_protocols: string[];
  coverage: Record<string, unknown>;
  execution_ready: false;
  validation_status?: 'valid' | 'invalid';
  validation_error?: string | null;
}

export interface ResearchJob {
  id: string;
  kind: 'download' | 'run' | 'unknown';
  status: 'running' | 'completed' | 'failed' | 'interrupted' | 'invalid';
  protocol_id: string;
  snapshot_id: string | null;
  run_id: string | null;
  started_at: string;
  completed_at: string | null;
  progress: string;
  error: string | null;
}

export type ResearchMetrics = Record<string, unknown>;
export interface ResearchRunSummary {
  id: string;
  generated_at: string;
  protocol_id: string | null;
  source: string;
  snapshot_id: string;
  metrics: Record<string, ResearchMetrics>;
  historical_oos_is_retrospective: true;
  execution_ready: false;
  validation_status?: 'valid' | 'invalid';
  validation_error?: string | null;
}

export interface ResearchChartPoint {
  date: string;
  momentum: number | null;
  equal_weight: number | null;
  buy_hold: number | null;
  benchmark: number | null;
  momentum_drawdown: number | null;
  equal_weight_drawdown: number | null;
  buy_hold_drawdown: number | null;
  benchmark_drawdown: number | null;
}

export interface ResearchRunDetail {
  id: string;
  manifest: Record<string, unknown>;
  report: Record<string, unknown>;
  config: Record<string, unknown>;
  chart: ResearchChartPoint[];
}

export type ResearchTableName = 'trades' | 'positions' | 'signals' | 'folds';
export interface ResearchTable {
  rows: Record<string, unknown>[];
  total: number;
  offset: number;
  limit: number;
}

export interface ResearchDownloadParams {
  protocol_id: string;
  refresh?: boolean;
  min_interval?: number;
  max_attempts?: number;
  request_timeout?: number;
}

export async function listResearchProtocols(signal?: AbortSignal): Promise<ResearchProtocol[]> {
  return (await api.get('/research/protocols', { signal })).data;
}
export async function listResearchSnapshots(signal?: AbortSignal): Promise<ResearchSnapshot[]> {
  return (await api.get('/research/snapshots', { signal })).data;
}
export async function listResearchJobs(signal?: AbortSignal): Promise<ResearchJob[]> {
  return (await api.get('/research/jobs', { signal })).data;
}
export async function getResearchJob(id: string, signal?: AbortSignal): Promise<ResearchJob> {
  return (await api.get(`/research/jobs/${encodeURIComponent(id)}`, { signal })).data;
}
export async function startResearchDownload(params: ResearchDownloadParams): Promise<ResearchJob> {
  return (await api.post('/research/download', params)).data;
}
export async function startResearchRun(params: { protocol_id: string; snapshot_id: string }): Promise<ResearchJob> {
  return (await api.post('/research/run', params)).data;
}
export async function listResearchRuns(signal?: AbortSignal): Promise<ResearchRunSummary[]> {
  return (await api.get('/research/runs', { signal })).data;
}
export async function getResearchRun(id: string, signal?: AbortSignal): Promise<ResearchRunDetail> {
  return (await api.get(`/research/runs/${encodeURIComponent(id)}`, { signal })).data;
}
export async function getResearchTable(id: string, name: ResearchTableName, params: { offset?: number; limit?: number; split?: string; strategy?: string } = {}, signal?: AbortSignal): Promise<ResearchTable> {
  return (await api.get(`/research/runs/${encodeURIComponent(id)}/table/${name}`, { params, signal })).data;
}
