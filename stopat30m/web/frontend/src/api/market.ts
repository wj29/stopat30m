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

export async function getMarketReviewStatus(signal?: AbortSignal): Promise<ReviewJobStatus> {
  const { data } = await api.get<ReviewJobStatus>('/analysis/market-review/status', { signal });
  return data;
}

/**
 * Subscribe to market review SSE progress stream.
 * Calls `onUpdate` each time a step completes or status changes.
 * Reports connection/authentication errors separately from the background job.
 * Returns a cleanup function to abort the connection.
 */
export function subscribeMarketReviewStream(
  onUpdate: (status: ReviewJobStatus) => void,
  onError?: (error: Error) => void,
): () => void {
  const ctrl = new AbortController();

  (async () => {
    try {
      const token = localStorage.getItem('access_token');
      if (!token) throw new Error('登录凭证缺失，请重新登录后读取复盘状态。');
      const resp = await fetch('/api/v1/analysis/market-review/stream', {
        signal: ctrl.signal,
        headers: { Authorization: `Bearer ${token}`, Accept: 'text/event-stream' },
      });
      if (!resp.ok) {
        if (resp.status === 401) throw new Error('登录已失效，请重新登录后读取复盘状态。');
        if (resp.status === 403) throw new Error('当前账号无权连接复盘进度。');
        throw new Error(`复盘进度连接失败（HTTP ${resp.status}），请重新读取状态。`);
      }
      if (!resp.body || !resp.headers.get('content-type')?.includes('text/event-stream')) throw new Error('服务器未返回有效的复盘进度流，请重新读取状态。');

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let terminal = false;

      function consume(line: string) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) return;
        let value: unknown;
        try { value = JSON.parse(trimmed.slice(5).trim()); }
        catch { throw new Error('复盘进度数据无法解析，请重新读取状态。'); }
        if (!isReviewJobStatus(value)) throw new Error('复盘进度数据格式无效，请重新读取状态。');
        if (ctrl.signal.aborted) return;
        terminal = value.status !== 'running';
        onUpdate(value);
      }
      try {
        while (!ctrl.signal.aborted) {
          const { done, value } = await reader.read();
          if (done) {
            buffer += decoder.decode();
            if (buffer.trim()) consume(buffer);
            break;
          }
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';
          for (const line of lines) { consume(line); if (terminal) break; }
          if (terminal) break;
        }
        if (!terminal && !ctrl.signal.aborted) throw new Error('实时进度连接已中断，尚未确认任务结果，请重新读取状态。');
      } finally {
        await reader.cancel().catch(() => { /* Cleanup after a server close or caller abort. */ });
        reader.releaseLock();
      }
    } catch (error) {
      if (!ctrl.signal.aborted) onError?.(error instanceof Error ? error : new Error('复盘进度连接失败，请重新读取状态。'));
    }
  })();

  return () => ctrl.abort();
}

function isReviewJobStatus(value: unknown): value is ReviewJobStatus {
  if (value == null || typeof value !== 'object') return false;
  const job = value as Record<string, unknown>;
  return ['idle', 'running', 'done', 'failed'].includes(String(job.status))
    && typeof job.elapsed_sec === 'number' && Number.isFinite(job.elapsed_sec)
    && typeof job.report === 'string' && typeof job.error === 'string'
    && Array.isArray(job.steps) && job.steps.every(step => step != null && typeof step === 'object'
      && typeof step.message === 'string' && typeof step.done === 'boolean'
      && typeof step.duration_ms === 'number' && Number.isFinite(step.duration_ms));
}
