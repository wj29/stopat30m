import api from './client';

// ---------------------------------------------------------------------------
// Dashboard sub-types (matching DSA decision dashboard JSON schema)
// ---------------------------------------------------------------------------

export interface CoreConclusion {
  one_sentence?: string;
  signal_type?: string;
  time_sensitivity?: string;
  position_advice?: {
    no_position?: string;
    has_position?: string;
  };
}

export interface TrendStatus {
  ma_alignment?: string;
  is_bullish?: boolean;
  trend_score?: number;
}

export interface PricePosition {
  current_price?: number;
  ma5?: number;
  ma10?: number;
  ma20?: number;
  bias_ma5?: number;
  bias_status?: string;
  support_level?: number;
  resistance_level?: number;
}

export interface VolumeAnalysis {
  volume_ratio?: number;
  volume_status?: string;
  volume_meaning?: string;
}

export interface ChipStructure {
  profit_ratio?: number;
  avg_cost?: number;
  concentration?: number | string;
  chip_health?: string;
}

export interface DataPerspective {
  trend_status?: TrendStatus;
  price_position?: PricePosition;
  volume_analysis?: VolumeAnalysis;
  chip_structure?: ChipStructure;
}

export interface SniperPoints {
  ideal_buy?: string;
  secondary_buy?: string;
  stop_loss?: string;
  take_profit?: string;
  take_profit_2?: string;
}

export interface PositionStrategy {
  suggested_position?: string;
  entry_plan?: string;
  risk_control?: string;
}

export interface BattlePlan {
  sniper_points?: SniperPoints;
  position_strategy?: PositionStrategy;
  action_checklist?: string[];
}

export interface Intelligence {
  latest_news?: string;
  risk_alerts?: string[];
  positive_catalysts?: string[];
  earnings_outlook?: string;
  sentiment_summary?: string;
}

export interface Dashboard {
  core_conclusion?: CoreConclusion;
  data_perspective?: DataPerspective;
  intelligence?: Intelligence;
  battle_plan?: BattlePlan;
}

// ---------------------------------------------------------------------------
// Full LLM dashboard (stored in llm_dashboard)
// ---------------------------------------------------------------------------

export interface LLMDashboard {
  // Legacy flat fields (backward compat)
  key_points?: string[];
  risk_warnings?: string[];
  target_price?: number | null;
  stop_loss_price?: number | null;

  // DSA decision dashboard fields
  stock_name?: string;
  sentiment_score?: number;
  trend_prediction?: string;
  operation_advice?: string;
  decision_type?: string;
  confidence_level?: string;
  dashboard?: Dashboard;
  analysis_summary?: string;
  risk_warning?: string;
  buy_reason?: string;
  trend_analysis?: string;
  short_term_outlook?: string;
  medium_term_outlook?: string;
  technical_analysis?: string;
  fundamental_analysis?: string;
  news_summary?: string;
  data_sources?: string;
}

// ---------------------------------------------------------------------------
// Analysis result
// ---------------------------------------------------------------------------

export interface AnalysisResult {
  id: number | null;
  code: string;
  name: string;
  analysis_date: string;
  signal_score: number;
  buy_signal: string;
  signal_reasons: string[];
  risk_factors: string[];
  trend_status?: string;
  technical_detail?: Record<string, unknown>;
  model_score: number | null;
  model_percentile: number | null;
  llm_sentiment: number | null;
  llm_operation_advice: string;
  llm_confidence?: number | null;
  llm_summary: string;
  llm_dashboard: LLMDashboard;
  ohlcv?: OHLCVBar[];
  data_source?: string;
  processing_time_ms?: number;
}

/** 分析管线含 LLM，可能较慢 */
export async function triggerAnalysis(code: string): Promise<AnalysisResult> {
  const { data } = await api.post('/analysis/analyze', { code }, { timeout: 180000 });
  return data;
}

export interface OHLCVBar {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ProgressEvent {
  type: 'progress';
  step: number;
  total: number;
  message: string;
}

export interface OHLCVEvent {
  type: 'ohlcv';
  bars: OHLCVBar[];
}

export interface ResultEvent {
  type: 'result';
  data: AnalysisResult;
}

export interface ErrorEvent {
  type: 'error';
  message: string;
}

export type StreamEvent = ProgressEvent | OHLCVEvent | ResultEvent | ErrorEvent;

/**
 * SSE streaming analysis. Calls `onEvent` for each progress / result / error
 * event emitted by the backend. Returns the final AnalysisResult or throws.
 */
export async function triggerAnalysisStream(
  code: string,
  onEvent: (evt: StreamEvent) => void,
): Promise<AnalysisResult> {
  const token = localStorage.getItem('access_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const resp = await fetch('/api/v1/analysis/analyze-stream', {
    method: 'POST',
    headers,
    body: JSON.stringify({ code }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }

  const reader = resp.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let finalResult: AnalysisResult | null = null;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data: ')) continue;
      const json = trimmed.slice(6);
      try {
        const evt: StreamEvent = JSON.parse(json);
        onEvent(evt);
        if (evt.type === 'result') {
          finalResult = evt.data;
        } else if (evt.type === 'error') {
          throw new Error(evt.message);
        }
      } catch (e) {
        if (e instanceof Error && e.message !== json) throw e;
      }
    }
  }

  if (!finalResult) throw new Error('流结束但未收到结果');
  return finalResult;
}

export interface PaginatedHistory {
  total: number;
  items: AnalysisResult[];
}

export async function getAnalysisHistory(
  limit = 20,
  offset = 0
): Promise<PaginatedHistory> {
  const { data } = await api.get('/analysis/history', {
    params: { limit, offset },
  });
  return data;
}

export async function getAnalysisDetail(id: number): Promise<AnalysisResult> {
  const { data } = await api.get(`/analysis/${id}`);
  return data;
}

export async function deleteAnalysis(id: number): Promise<void> {
  await api.delete(`/analysis/${id}`);
}

export async function batchDeleteAnalysis(ids: number[]): Promise<{ deleted: number }> {
  const { data } = await api.post('/analysis/batch-delete', { ids });
  return data;
}
