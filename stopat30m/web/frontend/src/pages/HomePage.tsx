import { useEffect, useState, useCallback } from 'react';
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
        {up ? '+' : ''}{idx.change_amount.toFixed(2)}　{fmtPct(idx.change_pct)}
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

const CACHE_KEY_SECTORS = 'home_sectors';
const CACHE_KEY_INDICES = 'home_indices';
const CACHE_KEY_TS = 'home_cache_ts';
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

function readCache(): { sectors: SectorItem[]; indices: IndexItem[] } | null {
  try {
    const ts = Number(sessionStorage.getItem(CACHE_KEY_TS) || '0');
    if (Date.now() - ts > CACHE_TTL_MS) return null;
    const sectors = JSON.parse(sessionStorage.getItem(CACHE_KEY_SECTORS) || 'null');
    const indices = JSON.parse(sessionStorage.getItem(CACHE_KEY_INDICES) || 'null');
    if (Array.isArray(sectors) && Array.isArray(indices)) return { sectors, indices };
  } catch { /* corrupted cache */ }
  return null;
}

function writeCache(sectors: SectorItem[], indices: IndexItem[]) {
  try {
    sessionStorage.setItem(CACHE_KEY_SECTORS, JSON.stringify(sectors));
    sessionStorage.setItem(CACHE_KEY_INDICES, JSON.stringify(indices));
    sessionStorage.setItem(CACHE_KEY_TS, String(Date.now()));
  } catch { /* quota exceeded — ignore */ }
}

export default function HomePage() {
  const cached = readCache();
  const [sectors, setSectors] = useState<SectorItem[]>(cached?.sectors ?? []);
  const [indices, setIndices] = useState<IndexItem[]>(cached?.indices ?? []);
  const [loading, setLoading] = useState(!cached);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [heatRes, idxRes] = await Promise.allSettled([getSectorHeatmap(), getMainIndices()]);
      const newSectors = heatRes.status === 'fulfilled' ? heatRes.value.sectors : sectors;
      const newIndices = idxRes.status === 'fulfilled' ? idxRes.value.indices : indices;
      setSectors(newSectors);
      setIndices(newIndices);
      writeCache(newSectors, newIndices);
    } finally {
      setLoading(false);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!cached) fetchData();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const sorted = [...sectors].sort((a, b) => b.change_pct - a.change_pct);
  const rising = sorted.filter((s) => s.change_pct >= 0);
  const falling = [...sorted.filter((s) => s.change_pct < 0)].reverse();

  return (
    <div>
      <div className="mb-6 flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">市场概览</h1>
        <button
          type="button"
          onClick={fetchData}
          disabled={loading}
          className="rounded-lg border border-gray-300 px-4 py-1.5 text-sm text-gray-600 hover:bg-gray-50 disabled:opacity-50"
        >
          {loading ? '加载中…' : '刷新'}
        </button>
      </div>

      {/* Index cards — always render the grid, show placeholder when empty */}
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
