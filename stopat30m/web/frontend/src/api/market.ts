import api from './client';

/* ---- Sector heatmap & indices ---- */

export interface SectorItem {
  name: string;
  code: string;
  change_pct: number;
  market_cap: number;
  turnover_rate: number;
  rising: number;
  falling: number;
  top_stock: string;
  top_stock_pct: number;
}

export interface IndexItem {
  code: string;
  name: string;
  current: number;
  change_pct: number;
  change_amount: number;
  volume: number;
  amount: number;
}

export async function getSectorHeatmap(): Promise<{ sectors: SectorItem[]; count: number; source?: string }> {
  const { data } = await api.get('/market/heatmap');
  return data;
}

export async function getMainIndices(): Promise<{ indices: IndexItem[]; source?: string }> {
  const { data } = await api.get('/market/indices');
  return data;
}

/* ---- Market review ---- */

export interface ReviewStep {
  message: string;
  done: boolean;
  duration_ms: number;
}

export interface ReviewJobStatus {
  status: 'idle' | 'running' | 'done' | 'failed';
  elapsed_sec: number;
  steps: ReviewStep[];
  report: string;
  error: string;
}

export interface TriggerResponse extends ReviewJobStatus {
  triggered: boolean;
  message: string;
}

export async function triggerMarketReview(): Promise<TriggerResponse> {
  const { data } = await api.post<TriggerResponse>('/analysis/market-review');
  return data;
}

export async function getMarketReviewStatus(): Promise<ReviewJobStatus> {
  const { data } = await api.get<ReviewJobStatus>('/analysis/market-review/status');
  return data;
}

/**
 * Subscribe to market review SSE progress stream.
 * Calls `onUpdate` each time a step completes or status changes.
 * Returns a cleanup function to abort the connection.
 */
export function subscribeMarketReviewStream(
  onUpdate: (status: ReviewJobStatus) => void,
): () => void {
  const ctrl = new AbortController();

  (async () => {
    try {
      const resp = await fetch('/api/v1/analysis/market-review/stream', {
        signal: ctrl.signal,
      });
      if (!resp.ok) return;

      const reader = resp.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed.startsWith('data: ')) continue;
          try {
            const payload: ReviewJobStatus = JSON.parse(trimmed.slice(6));
            onUpdate(payload);
          } catch {
            // ignore malformed lines
          }
        }
      }
    } catch {
      // aborted or network error
    }
  })();

  return () => ctrl.abort();
}
