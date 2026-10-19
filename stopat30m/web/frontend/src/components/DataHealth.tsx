import { Link } from 'react-router-dom';
import type { DataStatus } from '../api/system';
import { dateTime, number } from '../utils/format';

const labels: Record<DataStatus['status'], string> = { missing: '尚未准备数据', degraded: '数据存在缺口', stale: '数据需要更新', available: '数据可用' };
export default function DataHealth({ data, compact = false }: { data: DataStatus; compact?: boolean }) {
  return <section className="account-panel">
    <div className="mb-4 flex flex-wrap items-center justify-between gap-2"><h2 className="font-semibold text-gray-900">历史数据检查</h2><span className={`rounded-full px-2.5 py-1 text-xs ${data.status === 'available' ? 'bg-blue-50 text-blue-700' : 'bg-amber-50 text-amber-800'}`}>{labels[data.status] || '状态未知'}</span></div>
    <dl className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-3 xl:grid-cols-5">
      <div><dt className="text-xs text-gray-400">记录的可信水位</dt><dd className="mt-1 font-semibold">{data.trusted_until || '—'}</dd></div>
      <div><dt className="text-xs text-gray-400">最近证券数据日</dt><dd className="mt-1 font-semibold">{data.last_data_date || '—'}</dd></div>
      <div><dt className="text-xs text-gray-400">数据覆盖率</dt><dd className="mt-1 font-semibold">{data.coverage_pct == null ? '—' : `${number(data.coverage_pct)}%`}</dd></div>
      <div><dt className="text-xs text-gray-400">缺失 / 落后股票</dt><dd className="mt-1 font-semibold">{data.missing_stock_count ?? '—'} / {data.lagging_stock_count ?? '—'}</dd></div>
      <div><dt className="text-xs text-gray-400">检查时间</dt><dd className="mt-1 text-xs">{dateTime(data.checked_at)}</dd></div>
    </dl>
    {!data.freshness_confirmed && <p className="mt-4 text-xs leading-5 text-gray-500">尚未与官方交易日历核对。可用表示本地数据具备基础条件，不代表行情已经更新到最近交易日。</p>}
    {(data.warnings || []).length > 0 && <ul className="mt-3 space-y-1 text-xs leading-5 text-amber-700">{data.warnings.slice(0, compact ? 2 : undefined).map(warning => <li key={warning}>• {warning}</li>)}</ul>}
    {compact && <Link className="mt-4 inline-block text-xs font-medium text-blue-600" to="/settings">检查数据并更新 →</Link>}
  </section>;
}
