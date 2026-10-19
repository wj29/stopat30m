import { useCallback, useEffect, useId, useRef, useState, type ReactElement, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import ResearchWorkspace from '../components/ResearchWorkspace';
import { dateTime, errorMessage, money, number, percent, pnlColor } from '../utils/format';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  BarChart, Bar,
} from 'recharts';
import {
  listRuns, getRunDetail, getRunCharts, listModels, listPredictions,
  submitBacktest, getTaskStatus, getActiveTask, getBacktestDefaults,
  type BacktestRunSummary, type BacktestRunDetail, type AssetItem,
  type BacktestRunParams, type BacktestTaskStatus, type BacktestDefaults,
} from '../api/backtest';

type Tab = 'research' | 'launch' | 'history';
type Kind = 'backtest' | 'signal' | 'account';
type Source = 'model' | 'pred';

const KIND_LABELS: Record<Kind, string> = {
  backtest: '快速回测',
  signal: '信号回测',
  account: '账户回测',
};

const KIND_DESC: Record<Kind, string> = {
  backtest: '简化组合回测：Top-K 等权、净值曲线、基准对比。快速验证模型方向。',
  signal: '信号质量诊断：IC/RankIC、分位组收益、换手率、覆盖度。定位模型问题。',
  account: '按现金、持仓、成交费用与交易约束进行历史仿真，观察策略在给定假设下的表现。',
};

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function BacktestPage() {
  const [searchParams] = useSearchParams();
  const [tab, setTab] = useState<Tab>(() => {
    const mode = searchParams.get('mode');
    return mode === 'launch' || mode === 'history' ? mode : 'research';
  });
  const [runs, setRuns] = useState<BacktestRunSummary[]>([]);
  const [filterKind, setFilterKind] = useState('');
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const [detail, setDetail] = useState<BacktestRunDetail | null>(null);
  const [charts, setCharts] = useState<Record<string, unknown> | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const detailRequest = useRef(0);
  const runsRequest = useRef(0);

  const refreshRuns = useCallback(async () => {
    const requestId = ++runsRequest.current;
    setLoading(true);
    setErr(null);
    try {
      const r = await listRuns(filterKind || undefined, 50);
      if (requestId === runsRequest.current) setRuns(r);
    } catch (e: unknown) {
      if (requestId === runsRequest.current) setErr(errorMessage(e, '加载失败'));
    } finally {
      if (requestId === runsRequest.current) setLoading(false);
    }
  }, [filterKind]);

  useEffect(() => {
    ++detailRequest.current;
    setDetailLoading(false);
    setDetail(null);
    setCharts(null);
    refreshRuns();
  }, [refreshRuns]);

  const loadDetail = useCallback(async (id: number) => {
    const requestId = ++detailRequest.current;
    setDetailLoading(true);
    setDetail(null);
    setCharts(null);
    setErr(null);
    try {
      const [d, c] = await Promise.all([getRunDetail(id), getRunCharts(id)]);
      if (requestId === detailRequest.current) { setDetail(d); setCharts(c); }
    } catch (error) { if (requestId === detailRequest.current) setErr(errorMessage(error, '无法加载详情')); }
    finally { if (requestId === detailRequest.current) setDetailLoading(false); }
  }, []);
  function toggleDetail(id: number) {
    if (detail?.id === id) { ++detailRequest.current; setDetail(null); setCharts(null); return; }
    void loadDetail(id);
  }
  const onRunComplete = useCallback((runId: number | null) => {
    void refreshRuns();
    setTab('history');
    if (runId != null) void loadDetail(runId);
  }, [refreshRuns, loadDetail]);

  return (
    <div className="mx-auto max-w-6xl">
      <h1 className="mb-1 text-2xl font-bold text-gray-900">回测中心</h1>
      <p className="mb-6 text-sm text-gray-500">
        使用锁定协议和真实历史数据核对策略，也可查看已有模型回测。研究结果取决于数据、费用和执行假设。
      </p>

      {err && tab !== 'research' && (
        <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
          {err}
        </div>
      )}

      {/* Tabs */}
      <div className="mb-6 flex gap-1 rounded-lg bg-gray-100 p-1">
        {(['research', 'launch', 'history'] as Tab[]).map((t) => (
          <button
            key={t}
            className={`flex-1 rounded-md px-4 py-2 text-sm font-medium transition-colors ${
              tab === t ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'
            }`}
            onClick={() => { if (t === 'launch') setFilterKind(''); setTab(t); }}
          >
            {t === 'research' ? '样本外研究' : t === 'launch' ? '模型回测' : '回测历史'}
          </button>
        ))}
      </div>

      {tab === 'research' && <ResearchWorkspace />}

      {tab === 'launch' && <LaunchPanel onComplete={onRunComplete} />}

      {tab === 'history' && (
        <>
          <HistoryPanel
            runs={runs}
            filterKind={filterKind}
            setFilterKind={setFilterKind}
            loading={loading}
            onRefresh={refreshRuns}
            onSelect={toggleDetail}
            selectedId={detail?.id}
          />
          {(detailLoading || detail) && (
            <DetailPanel detail={detail} charts={charts} loading={detailLoading} />
          )}
        </>
      )}
    </div>
  );
}


// ---------------------------------------------------------------------------
// Launch panel
// ---------------------------------------------------------------------------

function LaunchPanel({ onComplete }: { onComplete: (runId: number | null) => void }) {
  const [kind, setKind] = useState<Kind>('backtest');
  const [source, setSource] = useState<Source>('pred');
  const [models, setModels] = useState<AssetItem[]>([]);
  const [preds, setPreds] = useState<AssetItem[]>([]);
  const [selectedModel, setSelectedModel] = useState('');
  const [selectedPred, setSelectedPred] = useState('');
  const [tag, setTag] = useState('');

  // Common params
  const [topK, setTopK] = useState('10');
  const [rebalFreq, setRebalFreq] = useState('5');
  const [benchmark, setBenchmark] = useState('SH000300');

  // Backtest-specific
  const [dealPrice, setDealPrice] = useState('open');

  // Signal-specific
  const [method, setMethod] = useState('top_k');
  const [groupCount, setGroupCount] = useState('10');

  // Account-specific
  const [execPrice, setExecPrice] = useState('open');
  const [orderType, setOrderType] = useState('market');
  const [slippage, setSlippage] = useState('0');
  const [partialFill, setPartialFill] = useState(false);
  const [participationRate, setParticipationRate] = useState('10');
  const [initialCapital, setInitialCapital] = useState('1000000');
  const [cashReserve, setCashReserve] = useState('2');
  const [riskManager, setRiskManager] = useState(true);

  // Task state
  const [task, setTask] = useState<BacktestTaskStatus | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const submitLock = useRef(false);
  const [defaults, setDefaults] = useState<BacktestDefaults | null>(null);
  const [assetLoading, setAssetLoading] = useState(true);
  const [assetError, setAssetError] = useState('');
  const [pollError, setPollError] = useState('');
  const [formError, setFormError] = useState('');

  const loadAssets = useCallback(async () => {
    setAssetLoading(true); setAssetError('');
    const [m, p, d] = await Promise.allSettled([listModels(), listPredictions(), getBacktestDefaults()]);
    const errors: string[] = [];
    if (m.status === 'fulfilled') { setModels(m.value); setSelectedModel(current => current || m.value[0]?.path || ''); }
    else errors.push(`模型：${errorMessage(m.reason)}`);
    if (p.status === 'fulfilled') { setPreds(p.value); setSelectedPred(current => current || p.value[0]?.path || ''); }
    else errors.push(`预测缓存：${errorMessage(p.reason)}`);
    if (d.status === 'fulfilled') setDefaults(d.value); else errors.push(`默认参数：${errorMessage(d.reason)}`);
    setAssetError(errors.join('；')); setAssetLoading(false);
  }, []);
  useEffect(() => { void loadAssets(); }, [loadAssets]);
  useEffect(() => {
    if (!defaults) return;
    const d = defaults[kind];
    setTopK(String(d.top_k ?? 10)); setRebalFreq(String(d.rebalance_freq ?? 5));
    setBenchmark(d.benchmark || 'SH000300'); setMethod('top_k');
    setDealPrice(d.deal_price || 'open'); setGroupCount(String(d.group_count ?? 10));
    setExecPrice(d.execution_price || 'open'); setOrderType(d.order_type || 'market');
    setSlippage(String(d.slippage_bps ?? 0)); setPartialFill(d.allow_partial_fill ?? false);
    setParticipationRate(String((d.participation_rate ?? 0.1) * 100));
    setInitialCapital(String(d.initial_capital ?? 1000000)); setCashReserve(String((d.cash_reserve_pct ?? 0.02) * 100));
    setRiskManager(d.enable_risk_manager ?? true);
  }, [defaults, kind]);
  useEffect(() => {
    let active = true;
    getActiveTask().then(value => { if (active && value) setTask(value); }).catch(error => { if (active) setPollError(`任务恢复检查失败：${errorMessage(error)}`); });
    return () => { active = false; };
  }, []);
  const isRunning = task?.status === 'running' || task?.status === 'pending';
  const taskId = task?.task_id;
  useEffect(() => {
    if (!isRunning || !taskId) return;
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const status = await getTaskStatus(taskId);
        if (!active) return;
        setTask(status); setPollError('');
        if (status.status === 'completed') { onComplete(status.run_id); return; }
        if (status.status === 'failed') return;
      } catch (error) {
        if (!active) return;
        if (typeof error === 'object' && error != null && 'response' in error && (error as { response?: { status?: number } }).response?.status === 404) {
          setTask({ task_id: taskId, status: 'failed', kind: task?.kind || kind, progress: '', run_id: null, error: '任务已不可用，服务可能已重启。请先检查回测历史，再决定是否重新运行。' }); return;
        }
        setPollError(`状态连接中断，将自动重试：${errorMessage(error)}`);
      }
      if (active) timer = setTimeout(poll, 2500);
    };
    timer = setTimeout(poll, 1200);
    return () => { active = false; clearTimeout(timer); };
  }, [isRunning, taskId, onComplete, kind, task?.kind]);

  async function handleSubmit() {
    if (submitLock.current || isRunning) return;
    const integer = (value: string, min: number, max: number) => value !== '' && Number.isInteger(Number(value)) && Number(value) >= min && Number(value) <= max;
    if (!integer(topK, 1, 1000) || !integer(rebalFreq, 1, 252)) { setFormError('Top K 需为 1–1000 的整数，换仓间隔需为 1–252 个交易日'); return; }
    if (kind === 'signal' && !integer(groupCount, 2, 100)) { setFormError('分位组数需为 2–100 的整数'); return; }
    if (kind === 'account' && (!Number.isFinite(Number(initialCapital)) || Number(initialCapital) <= 0 || Number(initialCapital) > 1e12 || slippage === '' || !Number.isFinite(Number(slippage)) || Number(slippage) < 0 || Number(slippage) > 1000 || participationRate === '' || !Number.isFinite(Number(participationRate)) || Number(participationRate) <= 0 || Number(participationRate) > 100 || cashReserve === '' || !Number.isFinite(Number(cashReserve)) || Number(cashReserve) < 0 || Number(cashReserve) >= 100)) { setFormError('请核对账户参数：资金 > 0，滑点 ≥ 0，参与率 0–100%，现金预留 0–100%（不含100%）'); return; }
    setFormError(''); setPollError('');
    submitLock.current = true;
    setSubmitting(true);
    setTask(null);
    try {
      const params: BacktestRunParams = { kind, tag: tag.trim() || undefined };
      if (source === 'model') params.model_path = selectedModel;
      else params.pred_path = selectedPred;

      if (topK) params.top_k = Number(topK);
      if (rebalFreq) params.rebalance_freq = Number(rebalFreq);
      if (benchmark) params.benchmark = benchmark.trim();

      if (kind === 'backtest') {
        params.deal_price = dealPrice;
      }
      if (kind === 'signal') {
        params.method = method;
        if (groupCount) params.group_count = Number(groupCount);
      }
      if (kind === 'account') {
        params.method = method;
        params.execution_price = execPrice;
        params.order_type = orderType;
        if (slippage) params.slippage_bps = parseFloat(slippage);
        params.allow_partial_fill = partialFill;
        if (participationRate) params.participation_rate = Number(participationRate) / 100;
        if (initialCapital) params.initial_capital = parseFloat(initialCapital);
        if (cashReserve) params.cash_reserve_pct = Number(cashReserve) / 100;
        params.enable_risk_manager = riskManager;
      }

      const { task_id } = await submitBacktest(params);
      setTask({ task_id, status: 'running', kind, progress: '已提交', run_id: null, error: null });

    } catch (e: unknown) {
      const msg = errorMessage(e, '提交失败');
      setTask({
        task_id: '', status: 'failed', kind, progress: '',
        run_id: null, error: msg,
      });
    } finally {
      submitLock.current = false;
      setSubmitting(false);
    }
  }

  const noSource = source === 'model' ? !selectedModel : !selectedPred;

  return (
    <fieldset disabled={submitting || isRunning || assetLoading} className="space-y-6">
      {assetError && <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-800" role="alert">{assetError}<button onClick={() => void loadAssets()} className="ml-3 underline">重新加载</button></div>}
      {assetLoading && <p className="text-sm text-gray-500">正在读取可用模型、预测缓存与默认参数…</p>}
      {formError && <p className="rounded-lg bg-red-50 p-3 text-sm text-red-700" role="alert">{formError}</p>}
      {pollError && <p className="rounded-lg bg-amber-50 p-3 text-sm text-amber-800" role="alert">{pollError}</p>}
      {(defaults?.limitations || []).length > 0 && <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-xs leading-5 text-amber-900"><p className="font-semibold">回测边界</p>{defaults?.limitations.map(item => <p key={item}>• {item}</p>)}</div>}
      {/* Type selector */}
      <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="mb-4 text-lg font-semibold text-gray-900">选择回测类型</h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          {(['backtest', 'signal', 'account'] as Kind[]).map((k) => (
            <button
              key={k}
              className={`rounded-lg border-2 p-4 text-left transition-colors ${
                kind === k
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 bg-white hover:border-gray-300'
              }`}
              onClick={() => setKind(k)}
            >
              <div className="mb-1 text-sm font-semibold text-gray-900">{KIND_LABELS[k]}</div>
              <div className="text-xs text-gray-500">{KIND_DESC[k]}</div>
            </button>
          ))}
        </div>
      </div>

      {/* Data source + params */}
      <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="mb-4 text-lg font-semibold text-gray-900">数据源与参数</h2>

        {/* Source toggle */}
        <div className="mb-4">
          <label className="mb-1 block text-xs font-medium text-gray-500">数据来源</label>
          <div className="flex gap-2">
            <button
              className={`rounded-md px-3 py-1.5 text-sm font-medium ${
                source === 'pred' ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600'
              }`}
              onClick={() => setSource('pred')}
            >
              预测缓存（推荐）
            </button>
            <button
              className={`rounded-md px-3 py-1.5 text-sm font-medium ${
                source === 'model' ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600'
              }`}
              onClick={() => setSource('model')}
            >
              模型文件
            </button>
          </div>
        </div>

        {source === 'pred' ? (
          <FormSelect
            label="预测缓存" value={selectedPred} onChange={setSelectedPred}
            options={preds.map((p) => ({ value: p.path, label: `${p.name} · ${p.size_mb} MB · ${dateTime(p.modified)}` }))}
            empty="无可用的预测缓存，请先运行 CLI: python main.py cache-predictions"
          />
        ) : (
          <FormSelect
            label="模型文件" value={selectedModel} onChange={setSelectedModel}
            options={models.map((m) => ({ value: m.path, label: `${m.name} · ${m.size_mb} MB · ${dateTime(m.modified)}` }))}
            empty="无可用的模型，请先运行 CLI: python main.py train"
          />
        )}

        <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
          <FormInput label="Top K" value={topK} onChange={setTopK} placeholder="10" type="number" />
          <FormInput label="换仓间隔（交易日）" value={rebalFreq} onChange={setRebalFreq} placeholder="5" type="number" />
          <FormInput label="标签" value={tag} onChange={setTag} placeholder="可选" />
          <FormInput label="基准" value={benchmark} onChange={setBenchmark} placeholder="SH000300" />
        </div>

        {/* Backtest-specific */}
        {kind === 'backtest' && (
          <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
            <FormSelect
              label="成交价格" value={dealPrice} onChange={setDealPrice}
              options={[{ value: 'open', label: '开盘价 (推荐)' }, { value: 'close', label: '收盘价（当日时点）' }]}
            />
          </div>
        )}

        {/* Signal-specific */}
        {kind === 'signal' && (
          <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
            <FormSelect
              label="信号构建方式" value={method} onChange={setMethod}
              options={[
                { value: 'top_k', label: 'Top K' },

              ]}
            />
            <FormInput label="分位组数" value={groupCount} onChange={setGroupCount} placeholder="10" type="number" />
          </div>
        )}

        {/* Account-specific */}
        {kind === 'account' && (
          <>
            <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
              <FormSelect
                label="信号方式" value={method} onChange={setMethod}
                options={[{ value: 'top_k', label: 'Top K（等权）' }]}
              />
              <FormSelect
                label="成交价格" value={execPrice} onChange={setExecPrice}
                options={[{ value: 'open', label: '开盘价' }, { value: 'close', label: '收盘价' }]}
              />
              <FormSelect
                label="订单类型" value={orderType} onChange={setOrderType}
                options={[{ value: 'market', label: '市价单' }, { value: 'limit', label: '限价单' }]}
              />
              <FormInput label="滑点(bps)" value={slippage} onChange={setSlippage} placeholder="5" type="number" />
            </div>
            <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
              <FormInput label="初始资金" value={initialCapital} onChange={setInitialCapital} placeholder="1000000" type="number" />
              <FormInput label="现金预留（%）" value={cashReserve} onChange={setCashReserve} placeholder="2" type="number" />
              <FormInput label="参与率上限（%）" value={participationRate} onChange={setParticipationRate} placeholder="10" type="number" />
              <div className="space-y-2">
                <FormCheck label="允许部分成交" checked={partialFill} onChange={setPartialFill} />
                <FormCheck label="启用风控" checked={riskManager} onChange={setRiskManager} />
              </div>
            </div>
          </>
        )}

        {/* Submit */}
        <div className="mt-6 flex items-center gap-4">
          <button
            className="rounded-lg bg-blue-600 px-6 py-2.5 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
            onClick={handleSubmit}
            disabled={submitting || isRunning || noSource || assetLoading || !defaults}
          >
            {isRunning ? '运行中…' : `启动${KIND_LABELS[kind]}`}
          </button>

          {task && (
            <TaskBadge task={task} />
          )}
        </div>
      </div>
    </fieldset>
  );
}


// ---------------------------------------------------------------------------
// History panel
// ---------------------------------------------------------------------------

function HistoryPanel({
  runs, filterKind, setFilterKind, loading, onRefresh, onSelect, selectedId,
}: {
  runs: BacktestRunSummary[];
  filterKind: string;
  setFilterKind: (v: string) => void;
  loading: boolean;
  onRefresh: () => void;
  onSelect: (id: number) => void;
  selectedId?: number;
}) {
  const filtered = runs.filter((r) => r.kind !== 'train');

  return (
    <div className="mb-6 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="mb-4 flex flex-wrap items-end gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-500">类型筛选</label>
          <select
            className="mt-1 rounded-lg border border-gray-300 px-3 py-2 text-sm"
            value={filterKind}
            onChange={(e) => setFilterKind(e.target.value)}
          >
            <option value="">全部</option>
            <option value="backtest">快速回测</option>
            <option value="signal">信号回测</option>
            <option value="account">账户回测</option>
          </select>
        </div>
        <button
          className="rounded-lg bg-gray-100 px-4 py-2 text-sm font-medium text-gray-800 hover:bg-gray-200"
          onClick={onRefresh}
          disabled={loading}
        >
          刷新
        </button>
      </div>

      <p className="mb-3 text-xs text-gray-400">显示最近 50 次记录 · 收益率按小数收益换算为百分比，胜率指盈利日占比。</p>
      {loading ? (
        <p className="text-sm text-gray-500">加载中…</p>
      ) : filtered.length === 0 ? (
        <p className="text-sm text-gray-500">暂无回测记录。请在上方「新建回测」启动，或通过 CLI 运行。</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="border-b border-gray-200 text-xs text-gray-500">
                <th className="py-2 pr-3">时间</th>
                <th className="py-2 pr-3">类型</th>
                <th className="py-2 pr-3">标签</th>
                <th className="py-2 pr-3">年化</th>
                <th className="py-2 pr-3">Sharpe</th>
                <th className="py-2 pr-3">最大回撤</th>
                <th className="py-2 pr-3">盈利日占比</th>
                <th className="py-2 pr-3">IC</th>
                <th className="py-2">操作</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r) => (
                <tr
                  key={r.id}
                  className={`border-b border-gray-100 cursor-pointer transition-colors ${
                    selectedId === r.id ? 'bg-blue-50' : 'hover:bg-gray-50'
                  }`}
                  onClick={() => onSelect(r.id)}
                >
                  <td className="py-2 pr-3 text-xs text-gray-600">
                    {dateTime(r.created_at)}
                  </td>
                  <td className="py-2 pr-3">
                    <span className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${
                      r.kind === 'signal' ? 'bg-purple-100 text-purple-700' :
                      r.kind === 'account' ? 'bg-green-100 text-green-700' :
                      'bg-gray-100 text-gray-700'
                    }`}>
                      {KIND_LABELS[r.kind as Kind] ?? r.kind}
                    </span>
                  </td>
                  <td className="py-2 pr-3 text-xs">{r.tag || '—'}</td>
                  <td className="py-2 pr-3">{fmtPct(r.annual_return)}</td>
                  <td className="py-2 pr-3">{fmtNum(r.sharpe, 3)}</td>
                  <td className="py-2 pr-3">{fmtPct(r.max_drawdown)}</td>
                  <td className="py-2 pr-3">{fmtPct(r.win_rate)}</td>
                  <td className="py-2 pr-3">{fmtNum(r.ic_mean, 4)}</td>
                  <td className="py-2">
                    <button className="text-blue-600 hover:underline" onClick={(e) => { e.stopPropagation(); onSelect(r.id); }}>
                      详情
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}


// ---------------------------------------------------------------------------
// Detail panel with charts
// ---------------------------------------------------------------------------

function DetailPanel({
  detail, charts, loading,
}: {
  detail: BacktestRunDetail | null;
  charts: Record<string, unknown> | null;
  loading: boolean;
}) {
  if (loading) {
    return <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm"><p className="text-sm text-gray-500">加载中…</p></div>;
  }
  if (!detail) return null;

  const report = detail.report as Record<string, number>;
  const kind = detail.kind;

  return (
    <div className="space-y-6">
      {/* Metrics cards */}
      <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="mb-4 text-lg font-semibold text-gray-900">
          {KIND_LABELS[kind as Kind] ?? kind} — {detail.tag || detail.created_at?.slice(0, 10)}
        </h2>
        <MetricsGrid report={report} kind={kind} />
      </div>

      {/* Charts */}
      {charts && <ChartSection charts={charts} kind={kind} report={detail.report} />}

      {/* Config */}
      <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        <h2 className="mb-4 text-sm font-semibold text-gray-700">本次回测口径</h2>
        <dl className="grid grid-cols-2 gap-4 text-sm sm:grid-cols-3">
          {Object.entries({ '数据区间': `${detail.config.test_start || '未记录'} 至 ${detail.config.test_end || '未记录'}`, '基准': detail.config.benchmark || '未记录', 'Top K / 换仓间隔': `${detail.config.top_k ?? '—'} / ${detail.config.rebalance_freq ?? '—'} 个交易日`, '成交价格': detail.config.execution_price || detail.config.deal_price || '未记录', '初始资金': typeof detail.config.initial_capital === 'number' ? money(detail.config.initial_capital) : '归一化组合', '滑点': detail.config.slippage_bps != null ? `${detail.config.slippage_bps} bps` : '按交易成本模型' }).map(([label, value]) => <div key={label}><dt className="text-xs text-gray-400">{label}</dt><dd className="mt-1 break-words">{String(value)}</dd></div>)}
        </dl>
        {detail.report.benchmark_available === false && <p className="mt-4 rounded-lg bg-amber-50 p-3 text-xs text-amber-800">本次缺少完整基准数据，不显示超额收益；图中也不将缺失基准补为 0。</p>}
        {Array.isArray(detail.config.limitations) && <ul className="mt-4 space-y-1 text-xs leading-5 text-amber-800">{detail.config.limitations.map((item, index) => <li key={index}>• {String(item)}</li>)}</ul>}
        <details className="mt-4"><summary className="cursor-pointer text-xs text-gray-500">查看完整配置与数据来源</summary><pre className="mt-3 max-h-64 overflow-auto rounded-lg bg-gray-50 p-4 text-xs text-gray-700">{JSON.stringify(detail.config, null, 2)}</pre></details>
      </div>
    </div>
  );
}


function MetricsGrid({ report, kind }: { report: Record<string, number>; kind: string }) {
  const cards: { label: string; value: string; color?: string }[] = [
    { label: '累计收益', value: fmtPct(report.total_return), color: numColor(report.total_return) },
    { label: '年化收益', value: fmtPct(report.annual_return), color: numColor(report.annual_return) },
    { label: 'Sharpe', value: fmtNum(report.sharpe, 2) },
    { label: '最大回撤', value: fmtPct(report.max_drawdown), color: 'text-amber-700' },
    { label: '盈利日占比', value: fmtPct(report.win_rate) },
  ];
  if (report.observation_days != null) cards.push({ label: '观测交易日', value: number(report.observation_days, 0) });
  if (report.total_trades != null) cards.push({ label: '成交笔数', value: number(report.total_trades, 0) });

  if (kind === 'signal') {
    const icKey = Object.keys(report).find((k) => k.startsWith('ic_') && k.endsWith('_mean'));
    const ricKey = Object.keys(report).find((k) => k.startsWith('rank_ic_') && k.endsWith('_mean'));
    if (icKey) cards.push({ label: 'IC Mean', value: fmtNum(report[icKey], 4) });
    if (ricKey) cards.push({ label: 'RankIC Mean', value: fmtNum(report[ricKey], 4) });
    if (report.avg_turnover != null) cards.push({ label: '平均换手率', value: fmtPct(report.avg_turnover) });
    if (report.top_bottom_spread != null) cards.push({ label: '头尾价差', value: fmtPct(report.top_bottom_spread) });
  }

  if (kind === 'account') {
    if (report.ending_equity != null) cards.push({ label: '期末权益', value: `¥${fmtMoney(report.ending_equity)}` });
    if (report.total_fees != null) cards.push({ label: '累计费用', value: `¥${fmtMoney(report.total_fees)}` });
    if (report.rejected_orders != null) cards.push({ label: '被拒订单', value: String(report.rejected_orders) });
    if (report.cash_utilization != null) cards.push({ label: '资金利用率', value: fmtPct(report.cash_utilization) });
  }

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {cards.map((c, i) => (
        <div key={i} className="rounded-lg bg-gray-50 px-4 py-3">
          <div className="text-xs text-gray-500">{c.label}</div>
          <div className={`text-lg font-semibold ${c.color ?? 'text-gray-900'}`}>{c.value}</div>
        </div>
      ))}
    </div>
  );
}


function ChartSection({ charts, kind, report }: { charts: Record<string, unknown>; kind: string; report: Record<string, unknown> }) {
  const sections: ReactElement[] = [];

  // Equity / NAV curve
  const equityData = kind === 'account'
    ? (charts.nav as Record<string, unknown>[] | undefined)
    : kind === 'signal'
      ? (charts.topk_returns as Record<string, unknown>[] | undefined)
      : (charts.returns as Record<string, unknown>[] | undefined);

  if (equityData && equityData.length > 0) {
    const dateKey = 'date' in equityData[0] ? 'date' : Object.keys(equityData[0])[0];
    const cumPortKey = kind === 'account'
      ? 'equity_cumulative'
      : 'portfolio_cumulative';
    const cumBenchKey = 'benchmark_cumulative';

    const cleaned = equityData
      .filter((d) => d[cumPortKey] != null && Number.isFinite(Number(d[cumPortKey])))
      .map((d) => ({
        date: String(d[dateKey] ?? '').slice(0, 10),
        portfolio: Number(d[cumPortKey]),
        benchmark: d[cumBenchKey] != null && Number.isFinite(Number(d[cumBenchKey])) ? Number(d[cumBenchKey]) : undefined,
      }));

    if (cleaned.length > 0) {
      sections.push(
        <ChartCard key="equity" title="净值曲线（起点 = 1）">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={cleaned}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
              <Tooltip formatter={value => typeof value === 'number' ? number(value, 4) : '—'} />
              <Legend />
              <Line type="monotone" dataKey="portfolio" name="组合" stroke="#2563eb" dot={false} strokeWidth={2} />
              <Line type="monotone" dataKey="benchmark" name="基准" stroke="#9ca3af" dot={false} strokeWidth={1.5} strokeDasharray="5 5" />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      );
    }
  }

  // Signal backtest: IC time series
  if (kind === 'signal' && charts.daily_ic) {
    const icData = (charts.daily_ic as Record<string, unknown>[]).map((d) => ({
      date: String(d.date ?? ''),
      ic_5d: d.ic_5d != null ? Number(d.ic_5d) : undefined,
      ic_1d: d.ic_1d != null ? Number(d.ic_1d) : undefined,
      ic_10d: d.ic_10d != null ? Number(d.ic_10d) : undefined,
    }));

    if (icData.length > 0) {
      sections.push(
        <ChartCard key="ic" title="IC 时间序列">
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={icData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="ic_1d" name="IC 1D" stroke="#93c5fd" dot={false} strokeWidth={1} />
              <Line type="monotone" dataKey="ic_5d" name="IC 5D" stroke="#2563eb" dot={false} strokeWidth={2} />
              <Line type="monotone" dataKey="ic_10d" name="IC 10D" stroke="#1e40af" dot={false} strokeWidth={1} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      );
    }
  }

  // Signal backtest: bucket returns
  if (kind === 'signal' && charts.bucket_returns) {
    const raw = charts.bucket_returns as Record<string, unknown>[];
    const grouped: Record<number, number[]> = {};
    for (const row of raw) {
      if (row.bucket == null || row.mean_return == null) continue;
      const bucket = Number(row.bucket);
      const meanReturn = Number(row.mean_return);
      if (!Number.isInteger(bucket) || bucket < 1 || !Number.isFinite(meanReturn)) continue;
      if (!grouped[bucket]) grouped[bucket] = [];
      grouped[bucket].push(meanReturn);
    }
    const bucketData = Object.entries(grouped)
      .sort(([a], [b]) => Number(a) - Number(b))
      .map(([bucket, vals]) => ({
        bucket: `Q${bucket}`,
        avg_return: vals.reduce((a, b) => a + b, 0) / vals.length,
      }));

    if (bucketData.length > 1) {
      sections.push(
        <ChartCard key="buckets" title="分位组平均收益">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={bucketData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="bucket" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${(v * 100).toFixed(1)}%`} />
              <Tooltip formatter={value => typeof value === 'number' ? percent(value) : '—'} />
              <Bar dataKey="avg_return" name="平均收益" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      );
    }
  }

  // Signal backtest: turnover
  if (kind === 'signal' && charts.turnover) {
    const turnData = (charts.turnover as Record<string, unknown>[]).map((d) => ({
      date: String(d.date ?? ''),
      turnover: d.turnover != null && Number.isFinite(Number(d.turnover)) ? Number(d.turnover) : null,
    }));

    if (turnData.length > 0) {
      sections.push(
        <ChartCard key="turnover" title="换手率">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={turnData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`} />
              <Tooltip formatter={value => typeof value === 'number' ? percent(value) : '—'} />
              <Bar dataKey="turnover" name="换手率" fill="#8b5cf6" radius={[2, 2, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      );
    }
  }

  // Account backtest: order summary
  if (kind === 'account') {
    const orders = charts.orders as Record<string, unknown>[] | undefined;
    const fills = charts.fills as Record<string, unknown>[] | undefined;
    const riskEvents = charts.risk_events as Record<string, unknown>[] | undefined;
    const reportedCount = (key: string) => typeof report[key] === 'number' && Number.isInteger(report[key]) && report[key] >= 0 ? report[key] as number : undefined;
    const orderCount = reportedCount('order_count');
    const fillCount = reportedCount('total_trades') ?? reportedCount('fill_count');

    sections.push(
      <ChartCard key="orders" title="交易概况">
        <div className="grid grid-cols-3 gap-4 text-center">
          <div>
            <div className="text-2xl font-bold text-gray-900">{number(orderCount, 0)}</div>
            <div className="text-xs text-gray-500">下单数</div>
          </div>
          <div>
            <div className="text-2xl font-bold text-gray-900">{number(fillCount, 0)}</div>
            <div className="text-xs text-gray-500">成交数</div>
          </div>
          <div>
            <div className="text-2xl font-bold text-amber-700">{number(riskEvents?.length, 0)}</div>
            <div className="text-xs text-gray-500">已载入风险记录</div>
          </div>
        </div>
        <p className="mt-4 text-xs text-gray-400">订单与成交总数以本次报告为准。明细已载入订单 {number(orders?.length, 0)} 条、成交 {number(fills?.length, 0)} 条；每类明细最多载入 2,000 条。</p>
      </ChartCard>
    );
  }

  if (sections.length === 0) return null;

  return <div className="space-y-4">{sections}</div>;
}


function ChartCard({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <h3 className="mb-4 text-sm font-semibold text-gray-700">{title}</h3>
      {children}
    </div>
  );
}


// ---------------------------------------------------------------------------
// Form components
// ---------------------------------------------------------------------------

function FormInput({
  label, value, onChange, placeholder, type = 'text',
}: {
  label: string; value: string; onChange: (v: string) => void;
  placeholder?: string; type?: string;
}) {
  const id = useId();
  return (
    <div>
      <label htmlFor={id} className="mb-1 block text-xs font-medium text-gray-500">{label}</label>
      <input
        id={id}
        type={type}
        step={type === 'number' ? 'any' : undefined}
        className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-400 focus:outline-none"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
      />
    </div>
  );
}

function FormSelect({
  label, value, onChange, options, empty,
}: {
  label: string; value: string; onChange: (v: string) => void;
  options: { value: string; label: string }[]; empty?: string;
}) {
  const id = useId();
  if (options.length === 0 && empty) {
    return (
      <div>
        <label className="mb-1 block text-xs font-medium text-gray-500">{label}</label>
        <p className="text-xs text-amber-600">{empty}</p>
      </div>
    );
  }
  return (
    <div>
      <label htmlFor={id} className="mb-1 block text-xs font-medium text-gray-500">{label}</label>
      <select
        id={id}
        className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-400 focus:outline-none"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
    </div>
  );
}

function FormCheck({
  label, checked, onChange,
}: {
  label: string; checked: boolean; onChange: (v: boolean) => void;
}) {
  return (
    <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
      <input
        type="checkbox"
        className="rounded border-gray-300"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
      />
      {label}
    </label>
  );
}

function TaskBadge({ task }: { task: BacktestTaskStatus }) {
  if (task.status === 'running' || task.status === 'pending') {
    return (
      <span className="flex items-center gap-2 text-sm text-blue-600">
        <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-blue-500" />
        {task.progress || '运行中…'}
      </span>
    );
  }
  if (task.status === 'completed') {
    return <span className="text-sm font-medium text-green-600">回测完成</span>;
  }
  return <span className="text-sm text-red-600">失败: {task.error || '原因未提供，请检查服务日志'}</span>;
}


// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function fmtPct(v: number | null | undefined): string {
  return percent(v);
}

function fmtNum(v: number | null | undefined, digits: number): string {
  return number(v, digits);
}

function fmtMoney(v: number): string {
  return number(v, 2);
}

function numColor(v: number | null | undefined): string {
  if (v == null) return 'text-gray-900';
  return pnlColor(v);
}
