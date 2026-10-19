import { useEffect, useRef, useState, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { getAccount, type AccountResponse } from '../api/trading';
import { getDataStatus, type DataStatus } from '../api/system';
import AccountOverview from '../components/AccountOverview';
import DataHealth from '../components/DataHealth';
import { dateTime, errorMessage } from '../utils/format';
import { Table, Tag, Spin } from '@arco-design/web-react';
import type { ColumnProps } from '@arco-design/web-react/es/Table';
import { getSectorHeatmap, getMainIndices, type SectorItem, type IndexItem } from '../api/market';

function fmtPct(v: number) {
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
}

function IndexCard({ idx }: { idx: IndexItem }) {
  const up = idx.change_pct >= 0;
  return (
    <div className={`rounded-xl border p-4 ${up ? 'border-red-100 bg-red-50/50' : 'border-green-100 bg-green-50/50'}`}>
      <p className="text-xs font-medium text-gray-500">{idx.name}</p>
      <p className={`mt-1 text-2xl font-bold tabular-nums ${up ? 'text-red-600' : 'text-green-600'}`}>
        {idx.current.toFixed(2)}
      </p>
      <p className={`mt-0.5 text-sm tabular-nums font-semibold ${up ? 'text-red-500' : 'text-green-500'}`}>
        {up ? '+' : ''}{idx.change_amount.toFixed(2)}{' '}{fmtPct(idx.change_pct)}
      </p>
      <p className="mt-1 text-xs text-gray-400">成交 {idx.amount > 0 ? (idx.amount / 1e8).toFixed(0) : '—'} 亿</p>
    </div>
  );
}

const risingCols: ColumnProps<SectorItem>[] = [
  {
    title: '板块',
    dataIndex: 'name',
    width: 120,
    render: (v) => <span className="font-medium text-gray-900">{v}</span>,
  },
  {
    title: '涨跌幅',
    dataIndex: 'change_pct',
    width: 90,
    align: 'right',
    render: (v: number) => (
      <Tag color="red" size="small">{fmtPct(v)}</Tag>
    ),
  },
  {
    title: '涨/跌',
    width: 70,
    align: 'center',
    render: (_: unknown, r: SectorItem) => (
      <span className="text-xs tabular-nums">
        <span className="text-red-500">{r.rising}</span>
        <span className="text-gray-300"> / </span>
        <span className="text-green-500">{r.falling}</span>
      </span>
    ),
  },
  {
    title: '领涨股',
    dataIndex: 'top_stock',
    width: 110,
    render: (_: unknown, r: SectorItem) =>
      r.top_stock ? (
        <span className="text-xs">
          {r.top_stock} <span className="text-red-400">{fmtPct(r.top_stock_pct)}</span>
        </span>
      ) : '—',
  },
];

const fallingCols: ColumnProps<SectorItem>[] = [
  {
    title: '板块',
    dataIndex: 'name',
    width: 120,
    render: (v) => <span className="font-medium text-gray-900">{v}</span>,
  },
  {
    title: '涨跌幅',
    dataIndex: 'change_pct',
    width: 90,
    align: 'right',
    render: (v: number) => (
      <Tag color="green" size="small">{fmtPct(v)}</Tag>
    ),
  },
  {
    title: '涨/跌',
    width: 70,
    align: 'center',
    render: (_: unknown, r: SectorItem) => (
      <span className="text-xs tabular-nums">
        <span className="text-red-500">{r.rising}</span>
        <span className="text-gray-300"> / </span>
        <span className="text-green-500">{r.falling}</span>
      </span>
    ),
  },
  {
    title: '领涨股',
    dataIndex: 'top_stock',
    width: 110,
    render: (_: unknown, r: SectorItem) =>
      r.top_stock ? (
        <span className="text-xs">
          {r.top_stock} <span className="text-red-400">{fmtPct(r.top_stock_pct)}</span>
        </span>
      ) : '—',
  },
];

export default function HomePage() {
  const [sectors, setSectors] = useState<SectorItem[]>([]);
  const [indices, setIndices] = useState<IndexItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [dataSource, setDataSource] = useState('');
  const [marketFetchedAt, setMarketFetchedAt] = useState<string | null>(null);
  const [marketErrors, setMarketErrors] = useState<string[]>([]);
  const [account, setAccount] = useState<AccountResponse | null>(null);
  const [dataStatus, setDataStatus] = useState<DataStatus | null>(null);
  const [workbenchErrors, setWorkbenchErrors] = useState<string[]>([]);
  const [workbenchLoading, setWorkbenchLoading] = useState(true);
  const marketRequest = useRef(0);
  const workbenchRequest = useRef(0);

  const fetchData = useCallback(async () => {
    const id = ++marketRequest.current;
    setLoading(true);
    const [heatRes, idxRes] = await Promise.allSettled([getSectorHeatmap(), getMainIndices()]);
    if (id !== marketRequest.current) return;
    const errors: string[] = [];
    if (heatRes.status === 'fulfilled') setSectors(heatRes.value.sectors);
    else errors.push(`板块行情：${errorMessage(heatRes.reason)}`);
    if (idxRes.status === 'fulfilled') setIndices(idxRes.value.indices);
    else errors.push(`指数行情：${errorMessage(idxRes.reason)}`);
    setDataSource((idxRes.status === 'fulfilled' && idxRes.value.source) || (heatRes.status === 'fulfilled' && heatRes.value.source) || '');
    if (heatRes.status === 'fulfilled' || idxRes.status === 'fulfilled') setMarketFetchedAt(new Date().toISOString());
    setMarketErrors(errors);
    setLoading(false);
  }, []);
  const fetchWorkbench = useCallback(async () => {
    const id = ++workbenchRequest.current;
    setWorkbenchLoading(true);
    const [a, d] = await Promise.allSettled([getAccount(), getDataStatus()]);
    if (id !== workbenchRequest.current) return;
    const errors: string[] = [];
    if (a.status === 'fulfilled') setAccount(a.value); else errors.push(`账户：${errorMessage(a.reason)}`);
    if (d.status === 'fulfilled') setDataStatus(d.value); else errors.push(`数据检查：${errorMessage(d.reason)}`);
    setWorkbenchErrors(errors);
    setWorkbenchLoading(false);
  }, []);
  useEffect(() => {
    // Both callbacks load external data and manage their own pending state.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void fetchData(); void fetchWorkbench();
  }, [fetchData, fetchWorkbench]);

  const sorted = [...sectors].sort((a, b) => b.change_pct - a.change_pct);
  const rising = sorted.filter((s) => s.change_pct >= 0);
  const falling = [...sorted.filter((s) => s.change_pct < 0)].reverse();

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div><h1 className="text-2xl font-bold text-gray-900">我的工作台</h1><p className="mt-2 text-sm text-gray-500">先核对账户与数据，再进行研究和回测。</p></div>
        <button className="secondary-button" disabled={workbenchLoading} onClick={() => void fetchWorkbench()}>{workbenchLoading ? '检查中…' : '刷新工作台'}</button>
      </div>
      {workbenchErrors.length > 0 && <div role="alert" className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800">{workbenchErrors.map(error => <p key={error}>{error}</p>)}</div>}
      {account?.account && <AccountOverview account={account.account} compact />}
      {account && !account.initialized && <div className="account-panel"><h2 className="font-semibold text-gray-900">建立你的个人账户</h2><p className="mb-4 mt-2 text-sm text-gray-500">设置开户本金，按时间顺序录入实际成交与资金变动，形成可核对的现金和持仓账本。</p><Link className="primary-button inline-block" to="/trading">建立账户 →</Link></div>}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">{[{ to: '/trading', label: '核对持仓与账本', desc: '现金、持仓与净盈亏' }, { to: '/watchlist', label: '查看自选股', desc: '追踪研究标的' }, { to: '/analysis', label: '开始个股分析', desc: '结合数据与历史记录' }, { to: '/backtest', label: '验证交易策略', desc: '核对假设与回测结果' }].map(item => <Link key={item.to} to={item.to} className="account-panel transition-colors hover:border-blue-300"><p className="text-sm font-semibold text-gray-800">{item.label} →</p><p className="mt-1 text-xs text-gray-400">{item.desc}</p></Link>)}</div>
      {dataStatus && <DataHealth data={dataStatus} compact />}
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-gray-200 pt-6">
        <div className="flex items-baseline gap-3">
          <h2 className="text-lg font-semibold text-gray-900">市场概览</h2>
          {dataSource && (
            <span className="text-xs text-gray-400">数据来源：{dataSource}</span>
          )}
        </div>
        <button
          type="button"
          onClick={fetchData}
          disabled={loading}
          className="rounded-lg border border-gray-300 px-4 py-1.5 text-sm text-gray-600 hover:bg-gray-50 disabled:opacity-50"
        >
          {loading ? '加载中…' : '刷新'}
        </button>
      </div>

      <div className="!mt-2 text-xs text-gray-400">行情获取 {dateTime(marketFetchedAt)} · 获取时间不等于交易所行情时间；未提供行情时间的数据仅供参考。</div>
      {marketErrors.length > 0 && <div role="alert" className="rounded-lg bg-amber-50 p-3 text-xs text-amber-800">{marketErrors.map(error => <p key={error}>{error}</p>)}<p>部分行情未更新；已显示的数据可能来自上次成功请求。</p></div>}
      {/* Market context */}
      <div className="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {indices.length > 0
          ? indices.map((idx) => <IndexCard key={idx.code} idx={idx} />)
          : Array.from({ length: 6 }, (_, i) => (
              <div key={i} className="rounded-xl border border-gray-100 bg-gray-50 p-4">
                <p className="text-xs text-gray-300">{loading ? '加载中…' : '暂无数据'}</p>
              </div>
            ))
        }
      </div>

      {loading && sectors.length === 0 ? (
        <div className="flex items-center justify-center rounded-xl border border-gray-200 bg-white py-24">
          <Spin size={28} tip="加载板块数据…" />
        </div>
      ) : !loading && sectors.length === 0 ? (
        <div className="flex flex-col items-center justify-center rounded-xl border border-gray-200 bg-white py-20">
          <p className="text-sm text-gray-500">板块数据获取失败</p>
          <p className="mt-1 text-xs text-gray-400">可能是网络波动，请稍后重试</p>
          <button
            type="button"
            onClick={fetchData}
            className="mt-4 rounded-lg border border-gray-300 px-4 py-1.5 text-sm text-gray-600 hover:bg-gray-50"
          >
            重新加载
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {/* Rising sectors */}
          <div className="rounded-xl border border-gray-200 bg-white shadow-sm">
            <div className="border-b border-gray-100 px-5 py-3">
              <h2 className="text-sm font-semibold text-red-600">
                领涨板块
                <span className="ml-2 text-xs font-normal text-gray-400">{rising.length} 个</span>
              </h2>
            </div>
            <Table
              rowKey="code"
              columns={risingCols}
              data={rising}
              size="small"
              stripe
              pagination={false}
              scroll={{ y: 420 }}
              noDataElement={<p className="py-8 text-xs text-gray-400">暂无上涨板块</p>}
            />
          </div>

          {/* Falling sectors */}
          <div className="rounded-xl border border-gray-200 bg-white shadow-sm">
            <div className="border-b border-gray-100 px-5 py-3">
              <h2 className="text-sm font-semibold text-green-600">
                领跌板块
                <span className="ml-2 text-xs font-normal text-gray-400">{falling.length} 个</span>
              </h2>
            </div>
            <Table
              rowKey="code"
              columns={fallingCols}
              data={falling}
              size="small"
              stripe
              pagination={false}
              scroll={{ y: 420 }}
              noDataElement={<p className="py-8 text-xs text-gray-400">暂无下跌板块</p>}
            />
          </div>
        </div>
      )}
    </div>
  );
}
