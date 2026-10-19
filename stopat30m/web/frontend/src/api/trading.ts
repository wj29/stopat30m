import api from './client';

export interface AccountSummary {
  id: number;
  name: string;
  currency: string;
  timezone: string;
  opened_date: string;
  initial_capital: number;
  net_deposits: number;
  cash: number;
  market_value: number | null;
  equity: number | null;
  total_cost: number;
  realized_pnl: number;
  unrealized_pnl: number | null;
  total_pnl: number | null;
  total_fees: number;
  valuation_complete: boolean;
  valuation_warnings?: string[];
  priced_positions: number;
  total_positions: number;
  as_of: string;
}
export interface AccountResponse {
  initialized: boolean;
  account: AccountSummary | null;
  legacy_trade_count: number;
  legacy_unassigned_count: number;
}
export interface PositionRow {
  instrument: string;
  code: string;
  name: string;
  quantity: number;
  available_quantity: number;
  frozen_quantity: number;
  avg_cost: number;
  current_price: number | null;
  market_value: number | null;
  cost: number;
  pnl: number | null;
  pnl_pct: number | null;
  realized_pnl: number;
  price_status: string;
  price_source: string | null;
  price_as_of: string | null;
  price_fetched_at?: string | null;
  last_known_price?: number | null;
}
export interface PositionsResponse {
  positions: PositionRow[];
  total_value: number | null;
  total_cost: number;
  total_pnl: number | null;
  valuation_complete: boolean;
  as_of?: string;
  legacy_trade_count: number;
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
  stamp_tax: number | null;
  transfer_fee: number | null;
  total_fee: number | null;
  net_amount: number | null;
  note: string | null;
  source: string | null;
  created_at?: string;
}
export interface CashFlowRow {
  id: number;
  direction: 'INITIAL' | 'DEPOSIT' | 'WITHDRAW';
  amount: number;
  effective_date: string;
  cash_after: number;
  note: string | null;
  created_at?: string;
}
export interface PageResult<T> { items: T[]; total: number; limit: number; offset: number }
export interface ManualTradePayload {
  instrument: string;
  direction: 'BUY' | 'SELL';
  quantity: number;
  price: number;
  trade_date: string;
  note?: string;
  commission?: number;
  stamp_tax?: number;
  transfer_fee?: number;
  idempotency_key: string;
}
export async function getAccount(): Promise<AccountResponse> {
  return (await api.get('/trading/account')).data;
}
export async function initializeAccount(payload: { initial_capital: number; name: string; opened_date: string; idempotency_key: string }): Promise<AccountResponse> {
  return (await api.post('/trading/account/initialize', payload)).data;
}
export async function getPositions(): Promise<PositionsResponse> {
  return (await api.get('/trading/positions')).data;
}
export async function submitManualTrade(payload: ManualTradePayload): Promise<{ id: number; replayed: boolean }> {
  return (await api.post('/trading/trade', payload)).data;
}
export async function getTrades(limit = 20, offset = 0, instrument?: string): Promise<PageResult<TradeRow>> {
  return (await api.get('/trading/trades', { params: { limit, offset, instrument: instrument || undefined } })).data;
}
export async function getLegacyTrades(limit = 20, offset = 0, includeUnassigned = false): Promise<PageResult<TradeRow>> {
  return (await api.get('/trading/legacy-trades', { params: { limit, offset, include_unassigned: includeUnassigned } })).data;
}
export async function getCashFlows(limit = 20, offset = 0): Promise<PageResult<CashFlowRow>> {
  return (await api.get('/trading/cash-flows', { params: { limit, offset } })).data;
}
export async function createCashFlow(payload: { direction: 'DEPOSIT' | 'WITHDRAW'; amount: number; effective_date: string; note?: string; idempotency_key: string }): Promise<{ id: number; replayed: boolean }> {
  return (await api.post('/trading/cash-flows', payload)).data;
}
