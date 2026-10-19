import api from './client';

export interface NavQuote {
  price: number;
  source: string;
  as_of: string;
  fetched_at: string;
  status: string;
  is_realtime: true;
  name: string;
}

export interface AccountNavSnapshot {
  id: number;
  date: string;
  recorded_at: string;
  revision: number;
  ledger_version: string;
  cash: number;
  market_value: number;
  equity: number;
  net_deposits: number;
  realized_pnl: number;
  unrealized_pnl: number;
  total_fees: number;
  total_pnl: number;
  valuation_complete: true;
  kind: 'saved_observation';
  facts: {
    ledger: { version: string; execution_count: number; cash_flow_count: number; last_event_date: string; facts: Record<string, unknown> };
    quotes: Record<string, NavQuote>;
    positions: Record<string, unknown>[];
    valuation: Record<string, unknown>;
    limitations: string[];
  };
}

export interface AccountNavHistory {
  items: AccountNavSnapshot[];
  total: number;
  limit: number;
  offset: number;
  limitations: string[];
}

export interface Reconciliation {
  as_of: string;
  statement_date: string;
  ledger_version: string;
  matched: boolean;
  cash: { ledger: number; statement: number; difference: number };
  positions: { instrument: string; ledger_quantity: number; statement_quantity: number; difference: number }[];
  warnings: string[];
}

export async function getAccountNav(params: { limit?: number; offset?: number } = {}, signal?: AbortSignal): Promise<AccountNavHistory> {
  return (await api.get('/trading/nav', { params, signal })).data;
}

export async function saveAccountNav(idempotencyKey: string): Promise<{ snapshot: AccountNavSnapshot; replayed: boolean }> {
  return (await api.post('/trading/nav', { idempotency_key: idempotencyKey })).data;
}

export async function reconcileAccount(params: { csv_text: string; broker_cash: number; statement_date?: string }): Promise<Reconciliation> {
  return (await api.post('/trading/reconcile', params)).data;
}
