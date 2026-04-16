import api from './client';

export interface PositionRow {
  instrument: string;
  code: string;
  name: string;
  quantity: number;
  avg_cost: number;
  current_price: number;
  market_value: number;
  cost: number;
  pnl: number;
  pnl_pct: number;
}

export interface PositionsResponse {
  positions: PositionRow[];
  total_value: number;
  total_cost: number;
  total_pnl?: number;
}

export interface TradeRow {
  id: number;
  trade_date: string;
  instrument: string;
  direction: string;
  quantity: number;
  price: number;
  amount: number;
  commission: number | null;
  note: string | null;
  source: string | null;
}

export async function getPositions(): Promise<PositionsResponse> {
  const { data } = await api.get('/trading/positions');
  return data;
}

export async function submitManualTrade(payload: {
  instrument: string;
  direction: 'BUY' | 'SELL';
  quantity: number;
  price: number;
  note?: string;
}): Promise<{ id: number; instrument: string; direction: string; quantity: number; price: number }> {
  const { data } = await api.post('/trading/trade', payload);
  return data;
}

export async function getTrades(limit = 50, offset = 0, instrument?: string): Promise<TradeRow[]> {
  const { data } = await api.get('/trading/trades', {
    params: { limit, offset, instrument: instrument || undefined },
  });
  return data;
}
