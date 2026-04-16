import { useEffect, useState } from 'react';
import {
  adminBatchDeleteAnalysis,
  adminBatchDeleteChatSessions,
  adminBatchDeleteMarketReviews,
  adminClearLogs,
  adminGetLogs,
  adminListAnalysisHistory,
  adminListChatSessions,
  adminListMarketReviews,
  type AdminAnalysisRecord,
  type AdminChatSession,
  type AdminLogs,
  type AdminMarketReview,
} from '../api/auth';
import { useAuth } from '../contexts/AuthContext';

type TabKey = 'chat' | 'analysis' | 'review' | 'logs';

export default function RecordManagePage() {
  const { isAdmin } = useAuth();
  const [tab, setTab] = useState<TabKey>('chat');

  if (!isAdmin) {
    return <p className="text-sm text-gray-500">仅管理员可访问</p>;
  }

  const TABS: { key: TabKey; label: string }[] = [
    { key: 'chat', label: '对话记录' },
    { key: 'analysis', label: '分析记录' },
    { key: 'review', label: '复盘记录' },
    { key: 'logs', label: '系统日志' },
  ];

  return (
    <div>
      <h1 className="mb-2 text-2xl font-bold text-gray-900">数据管理</h1>
      <p className="mb-6 text-sm text-gray-500">管理所有用户的对话、分析、复盘记录与系统日志</p>

      <div className="mb-6 flex gap-1 rounded-lg bg-gray-100 p-1">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`flex-1 rounded-md px-3 py-2 text-sm font-medium transition-colors ${
              tab === t.key ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'chat' && <ChatSessionsPanel />}
      {tab === 'analysis' && <AnalysisPanel />}
      {tab === 'review' && <MarketReviewPanel />}
      {tab === 'logs' && <LogsPanel />}
    </div>
  );
}


// ---------------------------------------------------------------------------
// Chat Sessions Panel
// ---------------------------------------------------------------------------

function ChatSessionsPanel() {
  const [items, setItems] = useState<AdminChatSession[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 30;

  const load = () => {
    setLoading(true);
    adminListChatSessions(PAGE_SIZE, page * PAGE_SIZE)
      .then((d) => { setItems(d.items); setTotal(d.total); })
      .catch(() => {})
      .finally(() => setLoading(false));
  };
  useEffect(load, [page]);

  const toggleSelect = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === items.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(items.map((i) => i.id)));
    }
  };

  const handleDelete = async () => {
    if (selected.size === 0) return;
    if (!confirm(`确认删除 ${selected.size} 条对话记录？`)) return;
    await adminBatchDeleteChatSessions(Array.from(selected));
    setSelected(new Set());
    load();
  };

  return (
    <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">
          对话记录 <span className="text-sm font-normal text-gray-400">({total})</span>
        </h2>
        {selected.size > 0 && (
          <button onClick={handleDelete} className="rounded-lg bg-red-600 px-3 py-1.5 text-xs text-white hover:bg-red-700">
            删除选中 ({selected.size})
          </button>
        )}
      </div>

      {loading ? <p className="text-sm text-gray-500">加载中...</p> : items.length === 0 ? (
        <p className="text-sm text-gray-500">暂无记录</p>
      ) : (
        <>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-gray-500">
                <th className="pb-2 pr-2"><input type="checkbox" checked={selected.size === items.length && items.length > 0} onChange={toggleAll} /></th>
                <th className="pb-2">ID</th>
                <th className="pb-2">用户</th>
                <th className="pb-2">标题</th>
                <th className="pb-2">股票</th>
                <th className="pb-2">更新时间</th>
              </tr>
            </thead>
            <tbody>
              {items.map((s) => (
                <tr key={s.id} className="border-b border-gray-100 hover:bg-gray-50">
                  <td className="py-2 pr-2"><input type="checkbox" checked={selected.has(s.id)} onChange={() => toggleSelect(s.id)} /></td>
                  <td className="py-2 text-gray-400">{s.id}</td>
                  <td className="py-2">{s.username}</td>
                  <td className="py-2 max-w-[200px] truncate">{s.title}</td>
                  <td className="py-2 font-mono text-xs">{s.stock_code || '—'}</td>
                  <td className="py-2 text-gray-500">{s.updated_at?.slice(0, 16).replace('T', ' ')}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <Pagination total={total} pageSize={PAGE_SIZE} page={page} onChange={setPage} />
        </>
      )}
    </section>
  );
}


// ---------------------------------------------------------------------------
// Analysis Panel
// ---------------------------------------------------------------------------

function AnalysisPanel() {
  const [items, setItems] = useState<AdminAnalysisRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 30;

  const load = () => {
    setLoading(true);
    adminListAnalysisHistory(PAGE_SIZE, page * PAGE_SIZE)
      .then((d) => { setItems(d.items); setTotal(d.total); })
      .catch(() => {})
      .finally(() => setLoading(false));
  };
  useEffect(load, [page]);

  const toggleSelect = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === items.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(items.map((i) => i.id)));
    }
  };

  const handleDelete = async () => {
    if (selected.size === 0) return;
    if (!confirm(`确认删除 ${selected.size} 条分析记录？`)) return;
    await adminBatchDeleteAnalysis(Array.from(selected));
    setSelected(new Set());
    load();
  };

  return (
    <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">
          分析记录 <span className="text-sm font-normal text-gray-400">({total})</span>
        </h2>
        {selected.size > 0 && (
          <button onClick={handleDelete} className="rounded-lg bg-red-600 px-3 py-1.5 text-xs text-white hover:bg-red-700">
            删除选中 ({selected.size})
          </button>
        )}
      </div>

      {loading ? <p className="text-sm text-gray-500">加载中...</p> : items.length === 0 ? (
        <p className="text-sm text-gray-500">暂无记录</p>
      ) : (
        <>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-gray-500">
                <th className="pb-2 pr-2"><input type="checkbox" checked={selected.size === items.length && items.length > 0} onChange={toggleAll} /></th>
                <th className="pb-2">ID</th>
                <th className="pb-2">股票</th>
                <th className="pb-2">名称</th>
                <th className="pb-2">用户</th>
                <th className="pb-2">信号分</th>
                <th className="pb-2">LLM建议</th>
                <th className="pb-2">分析时间</th>
              </tr>
            </thead>
            <tbody>
              {items.map((r) => (
                <tr key={r.id} className="border-b border-gray-100 hover:bg-gray-50">
                  <td className="py-2 pr-2"><input type="checkbox" checked={selected.has(r.id)} onChange={() => toggleSelect(r.id)} /></td>
                  <td className="py-2 text-gray-400">{r.id}</td>
                  <td className="py-2 font-mono text-xs">{r.code}</td>
                  <td className="py-2">{r.name}</td>
                  <td className="py-2">{r.username || '—'}</td>
                  <td className="py-2">{r.signal_score?.toFixed(0)}</td>
                  <td className="py-2">
                    <span className={`rounded px-1.5 py-0.5 text-xs font-medium ${
                      r.llm_operation_advice?.includes('买') ? 'bg-red-50 text-red-600'
                        : r.llm_operation_advice?.includes('卖') ? 'bg-green-50 text-green-600'
                        : 'bg-gray-50 text-gray-600'
                    }`}>
                      {r.llm_operation_advice || '—'}
                    </span>
                  </td>
                  <td className="py-2 text-gray-500">{r.analysis_date?.slice(0, 16).replace('T', ' ')}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <Pagination total={total} pageSize={PAGE_SIZE} page={page} onChange={setPage} />
        </>
      )}
    </section>
  );
}


// ---------------------------------------------------------------------------
// Market Review Panel
// ---------------------------------------------------------------------------

function MarketReviewPanel() {
  const [items, setItems] = useState<AdminMarketReview[]>([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const load = () => {
    setLoading(true);
    adminListMarketReviews()
      .then(setItems)
      .catch(() => {})
      .finally(() => setLoading(false));
  };
  useEffect(load, []);

  const toggleSelect = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(name) ? next.delete(name) : next.add(name);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === items.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(items.map((i) => i.filename)));
    }
  };

  const handleDelete = async () => {
    if (selected.size === 0) return;
    if (!confirm(`确认删除 ${selected.size} 个复盘报告？`)) return;
    await adminBatchDeleteMarketReviews(Array.from(selected));
    setSelected(new Set());
    load();
  };

  return (
    <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">
          复盘记录 <span className="text-sm font-normal text-gray-400">({items.length})</span>
        </h2>
        {selected.size > 0 && (
          <button onClick={handleDelete} className="rounded-lg bg-red-600 px-3 py-1.5 text-xs text-white hover:bg-red-700">
            删除选中 ({selected.size})
          </button>
        )}
      </div>

      {loading ? <p className="text-sm text-gray-500">加载中...</p> : items.length === 0 ? (
        <p className="text-sm text-gray-500">暂无复盘报告</p>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-gray-500">
              <th className="pb-2 pr-2"><input type="checkbox" checked={selected.size === items.length && items.length > 0} onChange={toggleAll} /></th>
              <th className="pb-2">文件名</th>
              <th className="pb-2">大小</th>
              <th className="pb-2">生成时间</th>
            </tr>
          </thead>
          <tbody>
            {items.map((r) => (
              <tr key={r.filename} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="py-2 pr-2"><input type="checkbox" checked={selected.has(r.filename)} onChange={() => toggleSelect(r.filename)} /></td>
                <td className="py-2 font-mono text-xs">{r.filename}</td>
                <td className="py-2 text-gray-500">{(r.size_bytes / 1024).toFixed(1)} KB</td>
                <td className="py-2 text-gray-500">{r.created_at?.slice(0, 16).replace('T', ' ')}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}


// ---------------------------------------------------------------------------
// Logs Panel
// ---------------------------------------------------------------------------

function LogsPanel() {
  const [logs, setLogs] = useState<AdminLogs | null>(null);
  const [loading, setLoading] = useState(true);
  const [clearing, setClearing] = useState(false);

  const load = () => {
    setLoading(true);
    adminGetLogs(300)
      .then(setLogs)
      .catch(() => {})
      .finally(() => setLoading(false));
  };
  useEffect(load, []);

  const handleClear = async () => {
    if (!confirm('确认清空日志文件？')) return;
    setClearing(true);
    try {
      await adminClearLogs();
      load();
    } finally {
      setClearing(false);
    }
  };

  return (
    <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">
          系统日志
          {logs && (
            <span className="ml-2 text-sm font-normal text-gray-400">
              ({(logs.size_bytes / 1024).toFixed(0)} KB)
            </span>
          )}
        </h2>
        <div className="flex gap-2">
          <button onClick={load} disabled={loading} className="rounded-lg bg-gray-100 px-3 py-1.5 text-xs text-gray-700 hover:bg-gray-200">
            刷新
          </button>
          <button onClick={handleClear} disabled={clearing} className="rounded-lg bg-red-600 px-3 py-1.5 text-xs text-white hover:bg-red-700 disabled:opacity-50">
            {clearing ? '清空中...' : '清空日志'}
          </button>
        </div>
      </div>

      {loading ? <p className="text-sm text-gray-500">加载中...</p> : !logs ? (
        <p className="text-sm text-gray-500">无法读取日志</p>
      ) : (
        <div className="max-h-[500px] overflow-auto rounded-lg bg-gray-900 p-4">
          <pre className="whitespace-pre-wrap break-all font-mono text-xs leading-relaxed text-green-400">
            {logs.content || '（日志为空）'}
          </pre>
        </div>
      )}
    </section>
  );
}


// ---------------------------------------------------------------------------
// Shared Pagination
// ---------------------------------------------------------------------------

function Pagination({ total, pageSize, page, onChange }: { total: number; pageSize: number; page: number; onChange: (p: number) => void }) {
  const totalPages = Math.ceil(total / pageSize);
  if (totalPages <= 1) return null;

  return (
    <div className="mt-4 flex items-center justify-between text-sm text-gray-500">
      <span>共 {total} 条 · 第 {page + 1}/{totalPages} 页</span>
      <div className="flex gap-1">
        <button onClick={() => onChange(Math.max(0, page - 1))} disabled={page === 0} className="rounded bg-gray-100 px-2 py-1 text-xs disabled:opacity-40">上一页</button>
        <button onClick={() => onChange(Math.min(totalPages - 1, page + 1))} disabled={page >= totalPages - 1} className="rounded bg-gray-100 px-2 py-1 text-xs disabled:opacity-40">下一页</button>
      </div>
    </div>
  );
}
