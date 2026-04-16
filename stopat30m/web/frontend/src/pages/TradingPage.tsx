import { useCallback, useEffect, useState } from 'react';
import { getPositions, getTrades, submitManualTrade, type PositionRow, type TradeRow } from '../api/trading';

export default function TradingPage() {
  const [positions, setPositions] = useState<PositionRow[]>([]);
  const [totals, setTotals] = useState<{ total_value: number; total_cost: number; total_pnl?: number } | null>(
    null
  );
  const [trades, setTrades] = useState<TradeRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  const [inst, setInst] = useState('');
  const [direction, setDirection] = useState<'BUY' | 'SELL'>('BUY');
  const [qty, setQty] = useState('');
  const [price, setPrice] = useState('');
  const [note, setNote] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const refresh = useCallback(async () => {
    setErr(null);
    try {
      const [p, t] = await Promise.all([getPositions(), getTrades(80, 0)]);
      setPositions(p.positions);
      setTotals({
        total_value: p.total_value,
        total_cost: p.total_cost,
        total_pnl: p.total_pnl,
      });
      setTrades(t);
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : '加载失败');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  async function onSubmitTrade(e: React.FormEvent) {
    e.preventDefault();
    const q = parseInt(qty, 10);
    const px = parseFloat(price);
    if (!inst.trim() || !Number.isFinite(q) || q <= 0 || !Number.isFinite(px) || px <= 0) return;
    setSubmitting(true);
    setErr(null);
    try {
      await submitManualTrade({
        instrument: inst.trim(),
        direction,
        quantity: q,
        price: px,
        note: note.trim() || undefined,
      });
      setQty('');
      setPrice('');
      setNote('');
      await refresh();
    } catch (e: unknown) {
      const msg =
        e && typeof e === 'object' && 'response' in e
          ? String((e as { response?: { data?: { detail?: string } } }).response?.data?.detail)
          : e instanceof Error
            ? e.message
            : '提交失败';
      setErr(msg || '提交失败');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div>
      <h1 className="mb-2 text-2xl font-bold text-gray-900">交易中心</h1>
      <p className="mb-6 text-sm text-gray-500">
        持仓与估值来自 SQLite 成交记录；行情为实时拉取。调仓计划请使用 CLI（API 调仓端点尚未开放）。
      </p>

      {err && (
        <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
          {err}
        </div>
      )}

      <div className="mb-8 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-lg font-semibold text-gray-900">当前持仓</h2>
          {totals && (
            <div className="text-sm text-gray-600">
              总市值 <span className="font-semibold text-gray-900">{totals.total_value}</span>
              {' · '}
              成本 {totals.total_cost}
              {totals.total_pnl != null && (
                <>
                  {' · '}
                  浮动盈亏{' '}
                  <span className={totals.total_pnl >= 0 ? 'text-red-600' : 'text-green-600'}>
                    {totals.total_pnl}
                  </span>
                </>
              )}
            </div>
          )}
          <button
            type="button"
            className="text-sm text-blue-600 hover:underline"
            onClick={() => refresh()}
            disabled={loading}
          >
            刷新
          </button>
        </div>
        {loading ? (
          <p className="text-sm text-gray-500">加载中…</p>
        ) : positions.length === 0 ? (
          <p className="text-sm text-gray-500">暂无持仓，可在下方录入成交或导入数据。</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-xs text-gray-500">
                  <th className="py-2 pr-4">代码</th>
                  <th className="py-2 pr-4">名称</th>
                  <th className="py-2 pr-4">数量</th>
                  <th className="py-2 pr-4">均价</th>
                  <th className="py-2 pr-4">现价</th>
                  <th className="py-2 pr-4">市值</th>
                  <th className="py-2">盈亏%</th>
                </tr>
              </thead>
              <tbody>
                {positions.map((r) => (
                  <tr key={r.instrument} className="border-b border-gray-100">
                    <td className="py-2 pr-4 font-mono">{r.code}</td>
                    <td className="py-2 pr-4">{r.name || '—'}</td>
                    <td className="py-2 pr-4">{r.quantity}</td>
                    <td className="py-2 pr-4">{r.avg_cost}</td>
                    <td className="py-2 pr-4">{r.current_price}</td>
                    <td className="py-2 pr-4">{r.market_value}</td>
                    <td
                      className={`py-2 ${r.pnl_pct >= 0 ? 'text-red-600' : 'text-green-600'}`}
                    >
                      {r.pnl_pct}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="mb-8 grid gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
          <h2 className="mb-4 text-lg font-semibold text-gray-900">手动成交</h2>
          <form onSubmit={onSubmitTrade} className="space-y-3">
            <div>
              <label className="block text-xs font-medium text-gray-500">标的代码</label>
              <input
                className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                value={inst}
                onChange={(e) => setInst(e.target.value)}
                placeholder="600519 或 SH600519"
                required
              />
            </div>
            <div className="flex gap-3">
              <div className="flex-1">
                <label className="block text-xs font-medium text-gray-500">方向</label>
                <select
                  className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  value={direction}
                  onChange={(e) => setDirection(e.target.value as 'BUY' | 'SELL')}
                >
                  <option value="BUY">买入</option>
                  <option value="SELL">卖出</option>
                </select>
              </div>
              <div className="flex-1">
                <label className="block text-xs font-medium text-gray-500">数量（股）</label>
                <input
                  type="number"
                  min={1}
                  className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                  value={qty}
                  onChange={(e) => setQty(e.target.value)}
                  required
                />
              </div>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500">成交价</label>
              <input
                type="number"
                step="0.001"
                min={0}
                className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                value={price}
                onChange={(e) => setPrice(e.target.value)}
                required
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500">备注（可选）</label>
              <input
                className="mt-1 w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                value={note}
                onChange={(e) => setNote(e.target.value)}
              />
            </div>
            <button
              type="submit"
              disabled={submitting}
              className="w-full rounded-lg bg-blue-600 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {submitting ? '提交中…' : '记一笔'}
            </button>
          </form>
        </div>

        <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
          <h2 className="mb-4 text-lg font-semibold text-gray-900">交易记录（只读）</h2>
          {trades.length === 0 ? (
            <p className="text-sm text-gray-500">暂无记录</p>
          ) : (
            <div className="max-h-80 overflow-y-auto">
              <table className="min-w-full text-left text-sm">
                <thead>
                  <tr className="sticky top-0 border-b border-gray-200 bg-white text-xs text-gray-500">
                    <th className="py-2 pr-2">时间</th>
                    <th className="py-2 pr-2">标的</th>
                    <th className="py-2 pr-2">方向</th>
                    <th className="py-2 pr-2">量</th>
                    <th className="py-2 pr-2">价</th>
                  </tr>
                </thead>
                <tbody>
                  {trades.map((t) => (
                    <tr key={t.id} className="border-b border-gray-50">
                      <td className="py-1.5 pr-2 text-xs text-gray-600">
                        {t.trade_date?.slice(0, 19).replace('T', ' ') || '—'}
                      </td>
                      <td className="py-1.5 pr-2 font-mono text-xs">{t.instrument}</td>
                      <td className="py-1.5 pr-2">{t.direction}</td>
                      <td className="py-1.5 pr-2">{t.quantity}</td>
                      <td className="py-1.5 pr-2">{t.price}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
