import api from './client';

export interface SignalRow {
  id: number;
  signal_date: string;
  instrument: string;
  score: number;
  signal: string;
  weight: number;
  method: string;
  batch_id?: string;
}

export async function getLatestSignals(limit = 30): Promise<SignalRow[]> {
  const { data } = await api.get('/signals/latest', { params: { limit } });
  return data;
}

export async function getSignalHistory(limit = 100, offset = 0, instrument?: string): Promise<SignalRow[]> {
  const { data } = await api.get('/signals/history', {
    params: { limit, offset, instrument: instrument || undefined },
  });
  return data;
}
