import axios from 'axios';

export function money(value: number | null | undefined, signed = false): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return new Intl.NumberFormat('zh-CN', { style: 'currency', currency: 'CNY', currencyDisplay: 'narrowSymbol', signDisplay: signed ? 'exceptZero' : 'auto', minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value === 0 ? 0 : value);
}

export function number(value: number | null | undefined, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return value.toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

// Returns in API contracts are fractions unless the field explicitly ends in pnl_pct.
export function percent(value: number | null | undefined, signed = false): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${signed && value > 0 ? '+' : ''}${(value * 100).toFixed(2)}%`;
}

export function pnlColor(value: number | null | undefined): string {
  if (value == null || value === 0 || !Number.isFinite(value)) return 'text-gray-900';
  return value > 0 ? 'text-red-600' : 'text-green-600';
}

export function dateTime(value: string | null | undefined): string {
  if (!value) return '—';
  // Existing API timestamps without an offset are UTC, as are datetime.utcnow records.
  const normalized = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(value) && !/(Z|[+-]\d{2}:?\d{2})$/.test(value) ? `${value}Z` : value;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).format(date);
}

export function localDateTime(): string {
  const parts = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date());
  return parts.replace(' ', 'T');
}

export function errorMessage(error: unknown, fallback = '请求失败，请重试'): string {
  if (axios.isAxiosError(error)) {
    const detail: unknown = error.response?.data?.detail;
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) return detail.map((item: { loc?: string[]; msg?: string }) => `${item.loc?.slice(1).join('.') || '参数'}：${item.msg || '无效'}`).join('；');
    if (error.code === 'ECONNABORTED') return '请求超时，请稍后重试';
  }
  return error instanceof Error ? error.message : fallback;
}
