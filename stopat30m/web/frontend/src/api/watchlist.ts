import api from './client';

export interface LatestAnalysis {
  id: number;
  analysis_date: string;
  signal_score: number | null;
  buy_signal: string;
  model_score: number | null;
  model_percentile: number | null;
  llm_operation_advice: string;
  llm_sentiment: number | null;
}

export interface WatchlistItem {
  id: number;
  code: string;
  name: string;
  note: string;
  sort_order: number;
  created_at: string;
  latest_analysis?: LatestAnalysis;
}


export async function getWatchlist(): Promise<{ items: WatchlistItem[]; count: number }> {
  const { data } = await api.get('/watchlist');
  return data;
}

export async function addStock(code: string, name?: string, note?: string): Promise<WatchlistItem> {
  const { data } = await api.post('/watchlist', { code, name, note });
  return data;
}

export async function addBatch(codes: string[]): Promise<{ added: string[]; skipped: string[] }> {
  const { data } = await api.post('/watchlist/batch', { codes });
  return data;
}

export async function removeStock(id: number): Promise<void> {
  await api.delete(`/watchlist/${id}`);
}

export async function batchRemove(ids: number[]): Promise<{ deleted: number }> {
  const { data } = await api.post('/watchlist/batch-delete', { ids });
  return data;
}

export async function updateItem(id: number, updates: { name?: string; note?: string; sort_order?: number }): Promise<WatchlistItem> {
  const { data } = await api.put(`/watchlist/${id}`, updates);
  return data;
}

export async function analyzeSingle(itemId: number): Promise<{ submitted: number; duplicates: number; code: string }> {
  const { data } = await api.post(`/watchlist/analyze/${itemId}`);
  return data;
}

export async function analyzeAll(): Promise<{ submitted: number; duplicates: number; codes: string[] }> {
  const { data } = await api.post('/watchlist/analyze-all');
  return data;
}
