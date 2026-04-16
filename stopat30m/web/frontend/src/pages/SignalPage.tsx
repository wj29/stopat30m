import { useCallback, useEffect, useState } from 'react';
import { getLatestSignals, getSignalHistory, type SignalRow } from '../api/signals';

export default function SignalPage() {
  const [latest, setLatest] = useState<SignalRow[]>([]);
  const [history, setHistory] = useState<SignalRow[]>([]);
  const [filterDraft, setFilterDraft] = useState('');
  const [filterApplied, setFilterApplied] = useState('');
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setErr(null);
    setLoading(true);
    try {
      const [l, h] = await Promise.all([
        getLatestSignals(40),
        getSignalHistory(200, 0, filterApplied.trim() || undefined),
      ]);
      setLatest(l);
      setHistory(h);
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : '加载失败');
    } finally {
      setLoading(false);
    }
  }, [filterApplied]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return (
    <div>
      <h1 className="mb-2 text-2xl font-bold text-gray-900">信号中心</h1>
      <p className="mb-6 text-sm text-gray-500">
        展示已写入数据库的信号批次。新信号需通过 CLI / 定时任务生成后同步入库。
      </p>

      {err && (
        <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
          {err}
        </div>
      )}

      <div className="mb-6 flex flex-wrap items-end gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-500">按标的筛选（可选）</label>
          <input
            className="mt-1 w-56 rounded-lg border border-gray-300 px-3 py-2 text-sm"
            value={filterDraft}
            onChange={(e) => setFilterDraft(e.target.value)}
            placeholder="如 SH600519"
          />
        </div>
        <button
          type="button"
          className="rounded-lg bg-gray-100 px-4 py-2 text-sm font-medium text-gray-800 hover:bg-gray-200"
          onClick={() => {
            setFilterApplied(filterDraft.trim());
          }}
          disabled={loading}
        >
          应用筛选
        </button>
        <button
          type="button"
          className="rounded-lg border border-gray-300 px-4 py-2 text-sm font-medium text-gray-800 hover:bg-gray-50"
          onClick={() => refresh()}
          disabled={loading}
        >
          刷新全部
        </button>
      </div>

      <div className="mb-8 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="mb-4 text-lg font-semibold text-gray-900">最新信号</h2>
        {loading ? (
          <p className="text-sm text-gray-500">加载中…</p>
        ) : latest.length === 0 ? (
          <p className="text-sm text-gray-500">暂无数据。请先生成信号并写入 signal_history。</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-xs text-gray-500">
                  <th className="py-2 pr-4">日期</th>
                  <th className="py-2 pr-4">标的</th>
                  <th className="py-2 pr-4">分数</th>
                  <th className="py-2 pr-4">信号</th>
                  <th className="py-2 pr-4">权重</th>
                  <th className="py-2">方法</th>
                </tr>
              </thead>
              <tbody>
                {latest.map((r) => (
                  <tr key={r.id} className="border-b border-gray-100">
                    <td className="py-2 pr-4">{r.signal_date}</td>
                    <td className="py-2 pr-4 font-mono">{r.instrument}</td>
                    <td className="py-2 pr-4">{r.score?.toFixed?.(4) ?? r.score}</td>
                    <td className="py-2 pr-4">{r.signal}</td>
                    <td className="py-2 pr-4">{r.weight}</td>
                    <td className="py-2">{r.method}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="mb-4 text-lg font-semibold text-gray-900">历史信号</h2>
        {!loading && history.length === 0 ? (
          <p className="text-sm text-gray-500">无历史记录</p>
        ) : !loading ? (
          <div className="max-h-96 overflow-y-auto overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead>
                <tr className="sticky top-0 border-b border-gray-200 bg-white text-xs text-gray-500">
                  <th className="py-2 pr-3">日期</th>
                  <th className="py-2 pr-3">标的</th>
                  <th className="py-2 pr-3">分数</th>
                  <th className="py-2 pr-3">信号</th>
                  <th className="py-2 pr-3">batch</th>
                </tr>
              </thead>
              <tbody>
                {history.map((r) => (
                  <tr key={r.id} className="border-b border-gray-50">
                    <td className="py-1.5 pr-3">{r.signal_date}</td>
                    <td className="py-1.5 pr-3 font-mono text-xs">{r.instrument}</td>
                    <td className="py-1.5 pr-3">{typeof r.score === 'number' ? r.score.toFixed(4) : r.score}</td>
                    <td className="py-1.5 pr-3">{r.signal}</td>
                    <td className="py-1.5 pr-3 text-xs text-gray-500">{r.batch_id || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </div>
    </div>
  );
}
