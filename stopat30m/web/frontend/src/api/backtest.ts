import api from './client';

export interface BacktestRunSummary {
  id: number;
  kind: string;
  tag: string;
  created_at: string;
  annual_return: number | null;
  sharpe: number | null;
  max_drawdown: number | null;
  total_trades: number | null;
  win_rate: number | null;
  ic_mean: number | null;
  rank_ic_mean: number | null;
  ending_equity: number | null;
  model_type: string;
  universe: string;
}

export interface BacktestRunDetail {
  id: number;
  kind: string;
  tag: string;
  run_dir: string;
  created_at: string;
  report: Record<string, unknown>;
  config: Record<string, unknown>;
}

export interface AssetItem {
  name: string;
  path: string;
  size_mb: number;
  modified: string;
}

export interface BacktestTaskStatus {
  task_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  kind: string;
  progress: string;
  run_id: number | null;
  error: string | null;
}

export interface BacktestRunParams {
  kind: 'backtest' | 'signal' | 'account';
  model_path?: string;
  pred_path?: string;
  tag?: string;
  top_k?: number;
  rebalance_freq?: number;
  deal_price?: string;
  method?: string;
  horizons?: number[];
  group_count?: number;
  benchmark?: string;
  execution_price?: string;
  order_type?: string;
  slippage_bps?: number;
  allow_partial_fill?: boolean;
  participation_rate?: number;
  initial_capital?: number;
  cash_reserve_pct?: number;
  enable_risk_manager?: boolean;
}

export async function listRuns(kind?: string, limit = 40): Promise<BacktestRunSummary[]> {
  const { data } = await api.get('/backtest/runs', {
    params: { kind: kind || undefined, limit },
  });
  return data;
}

export async function getRunDetail(runId: number): Promise<BacktestRunDetail> {
  const { data } = await api.get(`/backtest/runs/${runId}`);
  return data;
}

export async function getRunCharts(runId: number): Promise<Record<string, unknown>> {
  const { data } = await api.get(`/backtest/runs/${runId}/charts`);
  return data.files ?? {};
}

export async function listModels(): Promise<AssetItem[]> {
  const { data } = await api.get('/backtest/models');
  return data;
}

export async function listPredictions(): Promise<AssetItem[]> {
  const { data } = await api.get('/backtest/predictions');
  return data;
}

export async function submitBacktest(params: BacktestRunParams): Promise<{ task_id: string }> {
  const { data } = await api.post('/backtest/run', params);
  return data;
}

export async function getTaskStatus(taskId: string): Promise<BacktestTaskStatus> {
  const { data } = await api.get(`/backtest/task/${taskId}`);
  return data;
}

export async function getActiveTask(): Promise<BacktestTaskStatus | null> {
  const { data } = await api.get('/backtest/active-task');
  return data.task_id ? data : null;
}
