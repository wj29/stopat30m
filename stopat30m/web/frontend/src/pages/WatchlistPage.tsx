import { useCallback, useEffect, useState } from 'react';
import { Table, Button, Input, Space, Tag, Modal, Message, Spin } from '@arco-design/web-react';
import type { ColumnProps } from '@arco-design/web-react/es/Table';
import {
  getWatchlist,
  addStock,
  addBatch,
  removeStock,
  batchRemove,
  analyzeSingle,
  analyzeAll,
  type WatchlistItem,
} from '../api/watchlist';
import { getAnalysisDetail, type AnalysisResult, type LLMDashboard, type Dashboard } from '../api/analysis';
import TaskQueuePanel from '../components/TaskQueuePanel';

/* ========== Helpers ========== */

function fmtPct(n: number | null | undefined) {
  if (n == null || Number.isNaN(n)) return '—';
  return `${(n * 100).toFixed(1)}%`;
}

function signalColor(signal?: string): string {
  if (!signal) return 'gray';
  if (signal.includes('买入')) return 'green';
  if (signal.includes('卖出')) return 'red';
  if (signal.includes('警告')) return 'orangered';
  return 'gold';
}

function adviceColor(advice?: string): string {
  if (!advice) return 'gray';
  if (advice === '强烈买入') return 'green';
  if (advice === '买入') return 'cyan';
  if (advice === '强烈卖出') return 'red';
  if (advice === '卖出') return 'orangered';
  return 'gray';
}

/* ========== Detail modal sub-components (same as AnalysisPage) ========== */

function CoreConclusionCard({ dashboard }: { dashboard: Dashboard }) {
  const core = dashboard.core_conclusion;
  if (!core) return null;
  return (
    <div className="rounded-xl border border-gray-200 bg-gradient-to-br from-gray-50 to-white p-5 shadow-sm">
      {core.one_sentence && (
        <p className="text-base font-semibold text-gray-900">{core.one_sentence}</p>
      )}
      <div className="mt-2 flex flex-wrap gap-2 text-xs">
        {core.signal_type && <Tag color="arcoblue" size="small">{core.signal_type}</Tag>}
        {core.time_sensitivity && <Tag size="small">{core.time_sensitivity}</Tag>}
      </div>
      {core.position_advice && (
        <div className="mt-3 grid gap-2 sm:grid-cols-2 text-sm">
          {core.position_advice.no_position && (
            <div className="rounded-lg bg-emerald-50 p-2">
              <span className="font-medium text-emerald-700">空仓建议：</span>{core.position_advice.no_position}
            </div>
          )}
          {core.position_advice.has_position && (
            <div className="rounded-lg bg-amber-50 p-2">
              <span className="font-medium text-amber-700">持仓建议：</span>{core.position_advice.has_position}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function DashboardDisplay({ llm }: { llm: LLMDashboard }) {
  const dash: Dashboard | undefined = llm.dashboard;
  const hasDashboard = dash && (dash.core_conclusion || dash.data_perspective || dash.battle_plan || dash.intelligence);
  return (
    <div className="space-y-4">
      {dash?.core_conclusion && <CoreConclusionCard dashboard={dash} />}
      {llm.analysis_summary && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">综合分析</p>
          <p className="whitespace-pre-wrap text-sm leading-relaxed text-gray-800">{llm.analysis_summary}</p>
        </div>
      )}
      {(llm.technical_analysis || llm.fundamental_analysis) && (
        <div className="grid gap-3 sm:grid-cols-2">
          {llm.technical_analysis && (
            <div className="rounded-lg border border-gray-200 bg-white p-4">
              <p className="mb-1 text-xs font-medium text-gray-500">技术面分析</p>
              <p className="text-sm text-gray-800">{llm.technical_analysis}</p>
            </div>
          )}
          {llm.fundamental_analysis && (
            <div className="rounded-lg border border-gray-200 bg-white p-4">
              <p className="mb-1 text-xs font-medium text-gray-500">基本面分析</p>
              <p className="text-sm text-gray-800">{llm.fundamental_analysis}</p>
            </div>
          )}
        </div>
      )}
      {llm.buy_reason && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">操作理由</p>
          <p className="text-sm text-gray-800">{llm.buy_reason}</p>
        </div>
      )}
      {!hasDashboard && (
        <>
          {llm.key_points && llm.key_points.length > 0 && (
            <div>
              <p className="text-xs font-medium text-gray-500">关键要点</p>
              <ul className="mt-1 list-inside list-disc text-sm text-gray-700">
                {llm.key_points.map((s, i) => <li key={i}>{s}</li>)}
              </ul>
            </div>
          )}
          {llm.risk_warnings && llm.risk_warnings.length > 0 && (
            <div>
              <p className="text-xs font-medium text-gray-500">风险警告</p>
              <ul className="mt-1 list-inside list-disc text-sm text-amber-800">
                {llm.risk_warnings.map((s, i) => <li key={i}>{s}</li>)}
              </ul>
            </div>
          )}
        </>
      )}
    </div>
  );
}

/* ========== Main page ========== */

export default function WatchlistPage() {
  const [items, setItems] = useState<WatchlistItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [addInput, setAddInput] = useState('');
  const [adding, setAdding] = useState(false);
  const [analyzing, setAnalyzing] = useState(false);
  const [analyzingIds, setAnalyzingIds] = useState<Set<number>>(new Set());
  const [delVisible, setDelVisible] = useState(false);
  const [delLoading, setDelLoading] = useState(false);

  // Detail modal
  const [detailRecord, setDetailRecord] = useState<AnalysisResult | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await getWatchlist();
      setItems(res.items);
    } catch { /* empty state */ }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { load(); }, [load]);

  const refreshWatchlist = useCallback(() => {
    getWatchlist()
      .then((res) => setItems(res.items))
      .catch(() => {});
    setAnalyzingIds(new Set());
  }, []);

  /* ---- Actions ---- */

  async function handleAdd() {
    const raw = addInput.trim();
    if (!raw) return;
    setAdding(true);
    try {
      const codes = raw.split(/[,，\s]+/).filter(Boolean);
      if (codes.length === 1) {
        await addStock(codes[0]);
      } else {
        const res = await addBatch(codes);
        if (res.skipped.length > 0) Message.warning(`${res.skipped.length} 个已存在，已跳过`);
      }
      setAddInput('');
      load();
      Message.success('添加成功');
    } catch (e: unknown) {
      const msg = e && typeof e === 'object' && 'response' in e
        ? String((e as { response?: { data?: { detail?: string } } }).response?.data?.detail || '添加失败')
        : '添加失败';
      Message.error(msg);
    } finally { setAdding(false); }
  }

  async function handleAnalyzeAll() {
    setAnalyzing(true);
    try {
      const res = await analyzeAll();
      Message.success(`已提交 ${res.submitted} 个分析任务${res.duplicates > 0 ? `，${res.duplicates} 个重复跳过` : ''}`);
      setAnalyzingIds(new Set(items.map((it) => it.id)));
    } catch { Message.error('提交失败'); }
    finally { setAnalyzing(false); }
  }

  async function handleAnalyzeSingle(item: WatchlistItem) {
    setAnalyzingIds((prev) => new Set(prev).add(item.id));
    try {
      const res = await analyzeSingle(item.id);
      if (res.duplicates > 0) {
        Message.warning(`${item.code} 正在分析中`);
      } else {
        Message.success(`${item.code} 已提交分析`);
      }
    } catch {
      Message.error('提交失败');
      setAnalyzingIds((prev) => { const s = new Set(prev); s.delete(item.id); return s; });
    }
  }

  async function openDetail(item: WatchlistItem) {
    const a = item.latest_analysis;
    if (!a?.id) {
      Message.info('暂无分析记录，请先分析');
      return;
    }
    setDetailLoading(true);
    try {
      const detail = await getAnalysisDetail(a.id);
      setDetailRecord(detail);
    } catch {
      Message.error('获取详情失败');
    } finally { setDetailLoading(false); }
  }

  async function handleDeleteSingle(id: number) {
    try {
      await removeStock(id);
      load();
      Message.success('已移除');
    } catch { Message.error('移除失败'); }
  }

  async function handleBatchDelete() {
    setDelLoading(true);
    try {
      await batchRemove(Array.from(selected));
      setSelected(new Set());
      load();
      Message.success('批量移除成功');
      setDelVisible(false);
    } catch { Message.error('移除失败'); }
    finally { setDelLoading(false); }
  }

  /* ---- Table columns ---- */

  const columns: ColumnProps<WatchlistItem>[] = [
    {
      title: '代码 / 名称',
      dataIndex: 'code',
      width: 150,
      render: (_, r) => (
        <span>
          <span className="font-mono font-medium text-gray-900">{r.code}</span>
          {r.name && <span className="ml-1.5 text-gray-500">{r.name}</span>}
        </span>
      ),
    },
    {
      title: '技术信号',
      width: 130,
      render: (_, r) => {
        const a = r.latest_analysis;
        if (!a) return <span className="text-xs text-gray-300">未分析</span>;
        return (
          <span className="inline-flex items-center gap-1.5">
            {a.buy_signal
              ? <Tag color={signalColor(a.buy_signal)} size="small">{a.buy_signal}</Tag>
              : '—'}
            <span className="tabular-nums text-gray-400">{a.signal_score ?? ''}</span>
          </span>
        );
      },
    },
    {
      title: '模型分位',
      width: 80,
      align: 'right',
      render: (_, r) => {
        const a = r.latest_analysis;
        return <span className="tabular-nums">{a?.model_percentile != null ? fmtPct(a.model_percentile) : '—'}</span>;
      },
    },
    {
      title: 'LLM 建议',
      width: 100,
      render: (_, r) => {
        const advice = r.latest_analysis?.llm_operation_advice;
        if (!advice) return '—';
        return <Tag color={adviceColor(advice)} size="small">{advice}</Tag>;
      },
    },
    {
      title: '分析时间',
      width: 140,
      render: (_, r) => {
        const a = r.latest_analysis;
        if (!a?.analysis_date) return <span className="text-xs text-gray-300">—</span>;
        return (
          <span className="tabular-nums text-xs text-gray-400">
            {a.analysis_date.slice(0, 16).replace('T', ' ')}
          </span>
        );
      },
    },
    {
      title: '操作',
      width: 180,
      align: 'center',
      render: (_, r) => (
        <Space size="mini">
          <Button
            type="text"
            size="mini"
            loading={analyzingIds.has(r.id)}
            onClick={() => handleAnalyzeSingle(r)}
          >
            分析
          </Button>
          <Button type="text" size="mini" onClick={() => openDetail(r)}>
            详情
          </Button>
          <Button type="text" status="danger" size="mini" onClick={() => handleDeleteSingle(r.id)}>
            移除
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">自选股</h1>
          <p className="mt-1 text-sm text-gray-500">管理关注股票，一键批量分析</p>
        </div>
        <Space size="medium">
          <Button
            type="primary"
            onClick={handleAnalyzeAll}
            loading={analyzing}
            disabled={items.length === 0}
          >
            分析全部
          </Button>
          <Button onClick={load}>刷新</Button>
        </Space>
      </div>

      {/* Add stock + schedule config */}
      <div className="mb-6 flex flex-wrap items-center gap-4">
        <div className="flex items-center gap-2">
          <Input
            value={addInput}
            onChange={setAddInput}
            onPressEnter={handleAdd}
            placeholder="股票代码，多个用逗号分隔"
            style={{ width: 280 }}
            allowClear
          />
          <Button type="primary" onClick={handleAdd} loading={adding} disabled={!addInput.trim()}>
            添加
          </Button>
        </div>

      </div>

      {/* Task queue panel — shared with AnalysisPage, shows live task progress */}
      <TaskQueuePanel onTaskCompleted={refreshWatchlist} hideSubmit />

      {/* Batch delete */}
      {selected.size > 0 && (
        <div className="mb-3">
          <Button type="primary" status="danger" size="small" onClick={() => setDelVisible(true)}>
            移除选中 ({selected.size})
          </Button>
        </div>
      )}
      <Modal
        visible={delVisible}
        onCancel={() => setDelVisible(false)}
        title="批量移除"
        confirmLoading={delLoading}
        okButtonProps={{ status: 'danger' }}
        okText="确认移除"
        onOk={handleBatchDelete}
        autoFocus={false}
        style={{ width: 400 }}
      >
        <p>确定要从自选股中移除选中的 <strong>{selected.size}</strong> 只股票吗？</p>
      </Modal>

      {/* Detail modal */}
      <Modal
        visible={detailRecord != null || detailLoading}
        onCancel={() => { setDetailRecord(null); setDetailLoading(false); }}
        title={
          detailRecord
            ? `${detailRecord.name || detailRecord.code} (${detailRecord.code})`
            : '分析详情'
        }
        footer={null}
        style={{ width: 780, top: 40 }}
        autoFocus={false}
        unmountOnExit
      >
        {detailLoading ? (
          <div className="flex items-center justify-center py-16"><Spin size={28} /></div>
        ) : detailRecord ? (
          <div className="space-y-4">
            <div className="flex items-center gap-2 text-xs text-gray-400">
              <span>{detailRecord.analysis_date?.slice(0, 16).replace('T', ' ') || '—'}</span>
              {detailRecord.processing_time_ms != null && (
                <span>· {detailRecord.processing_time_ms}ms</span>
              )}
            </div>
            <div className="grid gap-4 sm:grid-cols-4">
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">技术</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">{detailRecord.signal_score}</p>
                <p className="text-sm text-gray-600">{detailRecord.buy_signal}</p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">模型</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">
                  {detailRecord.model_score != null ? detailRecord.model_score.toFixed(4) : '—'}
                </p>
                <p className="text-sm text-gray-600">
                  分位 {detailRecord.model_percentile != null ? fmtPct(detailRecord.model_percentile) : '—'}
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">LLM</p>
                <p className="mt-1 text-lg font-semibold text-gray-900">
                  {(detailRecord.llm_dashboard as LLMDashboard | undefined)?.operation_advice || detailRecord.llm_operation_advice || '—'}
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">情绪</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">
                  {detailRecord.llm_sentiment != null ? Math.round((detailRecord.llm_sentiment + 1) * 50) : '—'}
                </p>
              </div>
            </div>
            {detailRecord.llm_dashboard && Object.keys(detailRecord.llm_dashboard).length > 0 && (
              <DashboardDisplay llm={detailRecord.llm_dashboard as LLMDashboard} />
            )}
            {detailRecord.llm_summary && !(detailRecord.llm_dashboard as LLMDashboard | undefined)?.dashboard && (
              <div className="rounded-lg border border-gray-200 bg-gray-50 p-4">
                <p className="mb-1 text-xs font-medium text-gray-500">LLM 分析摘要</p>
                <p className="whitespace-pre-wrap text-sm leading-relaxed text-gray-800">{detailRecord.llm_summary}</p>
              </div>
            )}
          </div>
        ) : null}
      </Modal>

      {/* Table */}
      {loading ? (
        <div className="flex items-center justify-center rounded-xl border border-gray-200 bg-white py-24">
          <Spin size={28} />
        </div>
      ) : (
        <Table
          rowKey="id"
          columns={columns}
          data={items}
          size="small"
          stripe
          pagination={false}
          rowSelection={{
            type: 'checkbox',
            selectedRowKeys: Array.from(selected),
            onChange: (keys) => setSelected(new Set(keys.map(Number))),
            checkAll: true,
          }}
          noDataElement={
            <div className="py-12 text-center">
              <p className="text-sm text-gray-500">还没有自选股</p>
              <p className="mt-1 text-xs text-gray-400">在上方输入股票代码添加</p>
            </div>
          }
        />
      )}
    </div>
  );
}
