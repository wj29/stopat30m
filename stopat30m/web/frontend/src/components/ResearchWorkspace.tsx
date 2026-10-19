import { useEffect, useRef, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import {
  getResearchRun, getResearchTable, listResearchJobs, listResearchProtocols,
  listResearchRuns, listResearchSnapshots, startResearchRun,
  type ResearchJob, type ResearchProtocol, type ResearchRunDetail,
  type ResearchRunSummary, type ResearchSnapshot, type ResearchTable, type ResearchTableName,
} from '../api/research';
import { useAuth } from '../contexts/AuthContext';
import { dateTime, errorMessage, number, percent, pnlColor } from '../utils/format';

const inputClass = 'w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm disabled:bg-gray-50 disabled:text-gray-400';
const buttonClass = 'rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50';
const strategies = [{ key: 'momentum', label: '固定动量', color: '#dc2626' }, { key: 'equal_weight', label: '等权再平衡', color: '#2563eb' }, { key: 'buy_hold', label: '买入持有', color: '#d97706' }, { key: 'benchmark', label: '价格指数', color: '#64748b' }];
const splits = [{ key: 'train', label: '开发段' }, { key: 'validation', label: '验证段' }, { key: 'final_oos', label: '最终历史样本外' }];
const statusLabels: Record<ResearchJob['status'], string> = { running: '运行中', completed: '已完成', failed: '失败', interrupted: '已中断', invalid: '记录损坏' };

function record(value: unknown): Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
function numeric(value: unknown): number | null { return typeof value === 'number' && Number.isFinite(value) ? value : null; }
function text(value: unknown): string { return value == null ? '—' : typeof value === 'string' ? value : typeof value === 'object' ? JSON.stringify(value) : String(value); }
function strings(value: unknown): string[] { return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []; }

export default function ResearchWorkspace() {
  const { isAdmin } = useAuth();
  return isAdmin ? <AdminResearchWorkspace /> : <div className="account-panel text-sm text-gray-600">样本外研究的数据下载与运行仅供个人管理员使用。你仍可使用模型回测和回测历史。</div>;
}

function AdminResearchWorkspace() {
  const [protocols, setProtocols] = useState<ResearchProtocol[]>([]);
  const [snapshots, setSnapshots] = useState<ResearchSnapshot[]>([]);
  const [jobs, setJobs] = useState<ResearchJob[]>([]);
  const [runs, setRuns] = useState<ResearchRunSummary[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  const [protocolId, setProtocolId] = useState('eastmoney_etf');
  const [snapshotId, setSnapshotId] = useState('');
  const [selectedRun, setSelectedRun] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [actionError, setActionError] = useState('');
  const [pollError, setPollError] = useState('');
  const submitLock = useRef(false);
  const mounted = useRef(true);

  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);

  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    async function load() {
      const results = await Promise.allSettled([
        listResearchProtocols(controller.signal), listResearchSnapshots(controller.signal),
        listResearchJobs(controller.signal), listResearchRuns(controller.signal),
      ]);
      if (!active) return;
      const [p, s, j, r] = results;
      const nextErrors: Record<string, string> = {};
      if (p.status === 'fulfilled') setProtocols(p.value); else nextErrors.protocols = errorMessage(p.reason, '协议加载失败');
      if (s.status === 'fulfilled') setSnapshots(s.value); else nextErrors.snapshots = errorMessage(s.reason, '快照加载失败');
      if (j.status === 'fulfilled') { setJobs(j.value); setPollError(''); } else nextErrors.jobs = errorMessage(j.reason, '任务状态加载失败');
      if (r.status === 'fulfilled') {
        setRuns(r.value);
        setSelectedRun(current => current || r.value.find(item => item.validation_status !== 'invalid')?.id || '');
      } else nextErrors.runs = errorMessage(r.reason, '结果列表加载失败');
      setErrors(nextErrors);
      setLoading(false);
    }
    void load();
    return () => { active = false; controller.abort(); };
  }, [revision]);

  // Server-persisted jobs are reloaded on mount; no browser-only task id is needed.
  const runningIds = jobs.filter(job => job.status === 'running').map(job => job.id).sort().join(',');
  useEffect(() => {
    if (!runningIds) return;
    const ids = new Set(runningIds.split(','));
    const controller = new AbortController();
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    let failures = 0;
    async function poll() {
      try {
        const latest = await listResearchJobs(controller.signal);
        if (!active) return;
        const finished = latest.filter(job => ids.has(job.id) && ['completed', 'failed', 'interrupted'].includes(job.status));
        if (finished.length) {
          const [s, r] = await Promise.allSettled([listResearchSnapshots(controller.signal), listResearchRuns(controller.signal)]);
          if (!active) return;
          if (s.status === 'fulfilled') setSnapshots(s.value);
          if (r.status === 'fulfilled') setRuns(r.value);
          setErrors(current => {
            const next = { ...current };
            if (s.status === 'fulfilled') delete next.snapshots; else next.snapshots = errorMessage(s.reason, '快照刷新失败');
            if (r.status === 'fulfilled') delete next.runs; else next.runs = errorMessage(r.reason, '结果刷新失败');
            delete next.jobs;
            return next;
          });
          const completed = finished.find(job => job.kind === 'run' && job.status === 'completed' && job.run_id);
          if (completed?.run_id) setSelectedRun(completed.run_id);
        }
        setJobs(latest);
        setPollError('');
        failures = 0;
      } catch (error) {
        if (!active) return;
        failures += 1;
        setPollError(`${errorMessage(error, '任务状态获取失败')}；稍后继续查询，也可手动刷新。`);
      }
      if (active) timer = setTimeout(() => { void poll(); }, Math.min(15000, 2500 * (failures + 1)));
    }
    timer = setTimeout(() => { void poll(); }, 1500);
    return () => { active = false; clearTimeout(timer); controller.abort(); };
  }, [runningIds]);

  const protocol = protocols.find(item => item.id === protocolId) || protocols[0];
  const compatible = snapshots.filter(item => item.validation_status !== 'invalid' && protocol && item.compatible_protocols.includes(protocol.id));
  const snapshot = compatible.find(item => item.id === snapshotId) || compatible[0];
  const selectedSummary = runs.find(item => item.id === selectedRun);
  const busy = Boolean(runningIds) || submitting;

  function refresh() { setLoading(true); setRevision(value => value + 1); }
  async function launch() {
    if (submitLock.current || busy || !protocol || !snapshot || errors.jobs) return;
    submitLock.current = true;
    setSubmitting(true);
    setActionError('');
    try {
      const job = await startResearchRun({ protocol_id: protocol.id, snapshot_id: snapshot.id });
      if (!mounted.current) return;
      setJobs(current => [job, ...current.filter(item => item.id !== job.id)].slice(0, 20));
      if (job.status === 'completed' && job.run_id) { setSelectedRun(job.run_id); refresh(); }
    } catch (error) {
      if (!mounted.current) return;
      setActionError(errorMessage(error, '研究启动失败'));
      // A lost POST response may still have started the job. Recheck before enabling retry.
      try { const latest = await listResearchJobs(); if (mounted.current) setJobs(latest); }
      catch { if (mounted.current) setErrors(current => ({ ...current, jobs: '无法确认任务状态，请刷新后再启动，避免重复运行。' })); }
    } finally {
      submitLock.current = false;
      if (mounted.current) setSubmitting(false);
    }
  }

  return <div className="space-y-5">
    <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-sm leading-6 text-amber-900">
      当前是固定规则的历史样本外研究。规则和固定 ETF 池在 2026 年锁定后回看历史，仍存在回看选池限制；未训练模型、未寻优，也未验证 20% 年化目标。复权分数敞口用于研究收益，不能当作真实整数股成交或账户资产。
    </div>
    <div className="flex items-center justify-between gap-3"><p className="text-sm text-gray-500">协议、数据与结果均读取服务器真实文件；刷新可恢复任务状态。</p><button onClick={refresh} disabled={loading} className={buttonClass}>{loading ? '加载中…' : '刷新研究状态'}</button></div>
    {Object.entries(errors).map(([key, value]) => <Alert key={key}>{({ protocols: '协议', snapshots: '快照', jobs: '任务', runs: '结果' } as Record<string, string>)[key]}：{value}</Alert>)}
    {actionError && <Alert>{actionError}</Alert>}
    {pollError && <Alert>{pollError}</Alert>}

    <section className="account-panel space-y-4" aria-labelledby="research-launch-heading">
      <h2 id="research-launch-heading" className="font-semibold text-gray-900">锁定协议与数据</h2>
      <div className="grid gap-4 md:grid-cols-2">
        <label className="space-y-1.5 text-sm text-gray-600"><span className="block">研究协议</span><select className={inputClass} value={protocol?.id || ''} disabled={!protocols.length || submitting} onChange={event => { setProtocolId(event.target.value); setSnapshotId(''); setActionError(''); }}>
          {!protocols.length && <option value="">{loading ? '加载协议…' : '没有可用的锁定协议'}</option>}
          {protocols.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}
        </select></label>
        <label className="space-y-1.5 text-sm text-gray-600"><span className="block">通过校验且匹配协议的真实快照</span><select className={inputClass} value={snapshot?.id || ''} disabled={!compatible.length || submitting} onChange={event => setSnapshotId(event.target.value)}>
          {!compatible.length && <option value="">{loading ? '加载快照…' : '尚无完整匹配数据'}</option>}
          {compatible.map(item => <option key={item.id} value={item.id}>{dateTime(item.created_at)} · {item.source} · {item.session_count} 交易日</option>)}
        </select></label>
      </div>
      {protocol && <ProtocolInfo protocol={protocol} />}
      {snapshot && <div className="break-all rounded-lg bg-gray-50 p-3 text-xs leading-6 text-gray-600">快照 {snapshot.id} · {snapshot.start_date} 至 {snapshot.end_date} · {snapshot.source} / {snapshot.adjustment} · {snapshot.instruments.join('、')}<details><summary className="cursor-pointer text-blue-700">逐证券覆盖与校验记录</summary><Json value={snapshot.coverage} /></details></div>}
      {snapshots.filter(item => item.validation_status === 'invalid').map(item => <Alert key={item.id}>快照 {item.id} 校验失败：{item.validation_error || '文件不完整或校验和不匹配'}。不会用于运行。</Alert>)}
      {!snapshot && !loading && <p className="text-sm text-gray-600">当前协议没有完整、匹配且通过校验的数据。<Link to="/settings" className="ml-1 font-medium text-blue-700 underline">到数据与设置下载或恢复</Link>。已有不匹配的快照不会自动拼接或替换来源。</p>}
      <div className="flex flex-wrap items-center gap-3"><button type="button" className="rounded-lg bg-blue-600 px-5 py-2.5 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-gray-300" disabled={loading || busy || !protocol || !snapshot || Boolean(errors.jobs)} onClick={() => { void launch(); }}>{submitting ? '正在提交…' : runningIds ? '已有研究任务运行中' : '按锁定协议运行'}</button><span className="text-xs text-gray-500">只读取锁定参数。成本敏感性协议是独立诊断，不能替换主结果。</span></div>
    </section>

    <section className="account-panel" aria-labelledby="research-jobs-heading"><h2 id="research-jobs-heading" className="mb-3 font-semibold text-gray-900">最近任务</h2>
      {!jobs.length ? <p className="text-sm text-gray-500">{loading ? '读取任务…' : '暂无研究下载或运行任务。'}</p> : <div className="space-y-3">{jobs.slice(0, 5).map(job => <div key={job.id} className="rounded-lg border border-gray-200 p-3 text-sm"><div className="flex flex-wrap items-center justify-between gap-2"><p className="font-medium text-gray-800">{job.kind === 'download' ? '数据下载' : job.kind === 'run' ? '样本外研究' : '未知任务'} · {statusLabels[job.status]}</p><span className="text-xs text-gray-400">{dateTime(job.started_at)}</span></div><p className="mt-1 break-words text-xs text-gray-500">{job.protocol_id} · {job.progress || '等待状态更新'}</p>{job.error && <p className="mt-2 whitespace-pre-wrap break-words text-xs text-red-700">{job.error}</p>}{job.status === 'interrupted' && <p className="mt-1 text-xs text-amber-800">服务重启中断了此任务，未生成成功结果。下载恢复请到数据与设置操作。</p>}{job.run_id && job.status === 'completed' && <button className="mt-2 text-xs font-medium text-blue-700 underline" onClick={() => setSelectedRun(job.run_id!)}>查看本次结果</button>}</div>)}</div>}
    </section>

    <section className="account-panel" aria-labelledby="research-runs-heading"><h2 id="research-runs-heading" className="mb-3 font-semibold text-gray-900">研究结果 <span className="text-xs font-normal text-gray-400">包含 CLI 与网页生成的有效运行</span></h2>
      {!runs.length ? <p className="py-4 text-sm text-gray-500">{loading ? '读取结果…' : '尚无真实研究结果。取得完整快照并成功运行后，才会显示净值和指标。'}</p> : <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr className="border-b border-gray-200 text-xs text-gray-500"><th className="py-2 pr-4">生成时间 / 协议</th><th className="pr-4">来源</th><th className="pr-4 text-right">动量年化</th><th className="pr-4 text-right">最大回撤</th><th className="text-right">结果</th></tr></thead><tbody>{runs.map(run => { const metric = run.metrics.momentum || {}; const invalid = run.validation_status === 'invalid'; return <tr key={run.id} className={`border-b border-gray-100 ${selectedRun === run.id ? 'bg-blue-50/60' : ''}`}><td className="max-w-72 py-3 pr-4"><p>{dateTime(run.generated_at)}</p><p className="mt-1 break-all text-xs text-gray-400">{protocols.find(item => item.id === run.protocol_id)?.label || run.protocol_id || 'CLI 独立协议'} · {run.id}</p>{invalid && <p className="mt-1 text-xs text-red-700">校验失败：{run.validation_error || '产物不完整'}</p>}</td><td className="pr-4 text-xs">{run.source || '—'}</td><td className={`pr-4 text-right tabular-nums ${pnlColor(numeric(metric.annual_return))}`}>{percent(numeric(metric.annual_return))}</td><td className="pr-4 text-right tabular-nums text-green-700">{percent(numeric(metric.max_drawdown))}</td><td className="text-right"><button disabled={invalid} className={`${buttonClass} whitespace-nowrap`} onClick={() => setSelectedRun(run.id)}>{invalid ? '不可用' : selectedRun === run.id ? '已选择' : '查看结果'}</button></td></tr>; })}</tbody></table></div>}
    </section>
    {selectedRun && (selectedSummary?.validation_status === 'invalid' ? <Alert>所选运行校验失败，已停止展示其结果：{selectedSummary.validation_error || '产物不完整或校验和不匹配'}</Alert> : <ResearchResult key={`${selectedRun}-${revision}`} runId={selectedRun} />)}
  </div>;
}

function ProtocolInfo({ protocol }: { protocol: ResearchProtocol }) {
  const config = protocol.config;
  return <div className="space-y-3 text-xs leading-6 text-gray-600"><div className="grid gap-x-6 md:grid-cols-2"><p>行情：{protocol.source} · 日历：{protocol.calendar_source}</p><p>数据请求：{protocol.request.start_date} 至 {protocol.request.end_date} · {protocol.request.adjustment}</p><p>请求证券（含基准）：{protocol.request.instruments.join('、')} · 池类型 {protocol.universe_kind}</p><p>信号：前日收盘 {text(config.lookback_sessions)} 日动量 · Top {text(config.top_k)} · 每 {text(config.rebalance_sessions)} 个交易日调仓</p><p>开发：{text(config.train_start)} 至 {text(config.train_end)}</p><p>验证：{text(config.validation_start)} 至 {text(config.validation_end)}</p><p>最终样本外：{text(config.oos_start)} 至 {text(config.oos_end)}</p><p>买 / 卖费用：{text(config.buy_fee_bps)} / {text(config.sell_fee_bps)} bp · 单边滑点：{text(config.slippage_bps)} bp</p></div><p className="break-all font-mono text-[10px] text-gray-400">协议 SHA-256：{protocol.protocol_sha256}</p>{protocol.limitations.length > 0 && <ul className="list-disc space-y-1 pl-4 text-amber-800">{protocol.limitations.map((item, index) => <li key={index}>{item}</li>)}</ul>}</div>;
}

function ResearchResult({ runId }: { runId: string }) {
  const [detail, setDetail] = useState<ResearchRunDetail | null>(null);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    getResearchRun(runId, controller.signal).then(value => { if (active) { setDetail(value); setError(''); } }).catch(reason => { if (active) setError(errorMessage(reason, '研究结果加载失败')); });
    return () => { active = false; controller.abort(); };
  }, [runId, revision]);
  if (error) return <div className="account-panel"><Alert>{error}</Alert><button className={`${buttonClass} mt-3`} onClick={() => { setError(''); setRevision(value => value + 1); }}>重试加载结果</button></div>;
  if (!detail) return <div className="account-panel text-sm text-gray-500">正在校验并读取研究结果…</div>;
  const bySplit = record(detail.report.splits);
  const final = record(record(bySplit.final_oos).momentum);
  return <section className="space-y-5" aria-label="研究结果详情"><div className="account-panel"><h2 className="font-semibold text-gray-900">最终历史样本外结果</h2><p className="mt-1 break-all text-xs text-gray-400">{runId} · 历史回看研究 · 分数敞口净值</p><div className="mt-4 grid grid-cols-2 gap-4 md:grid-cols-4">{[{ label: '动量累计净收益', value: percent(numeric(final.total_return)), raw: numeric(final.total_return) }, { label: '动量几何年化', value: percent(numeric(final.annual_return)), raw: numeric(final.annual_return) }, { label: '动量最大回撤', value: percent(numeric(final.max_drawdown)), raw: numeric(final.max_drawdown) }, { label: '动量 Sharpe', value: number(numeric(final.sharpe)), raw: null }].map(item => <div key={item.label}><p className="text-xs text-gray-500">{item.label}</p><p className={`mt-1 text-xl font-semibold tabular-nums ${pnlColor(item.raw)}`}>{item.value}</p></div>)}</div></div>
    <ResearchCharts detail={detail} />
    <section className="account-panel"><h3 className="mb-3 font-semibold text-gray-900">分段诊断与成本后指标</h3><p className="mb-3 text-xs text-gray-500">开发、验证段分别从净值 1 开始；最终样本外跨观察块连续持仓。价格指数不含分红且不扣成本。</p><div className="overflow-x-auto"><table className="w-full whitespace-nowrap text-right text-xs"><thead className="border-b border-gray-200 text-gray-500"><tr><th className="py-2 pr-4 text-left">区间 / 组合</th><th className="px-3">累计净收益</th><th className="px-3">几何年化</th><th className="px-3">最大回撤</th><th className="px-3">Sharpe</th><th className="px-3">观察日</th><th className="px-3">成交数</th><th className="px-3">费用 / 滑点</th><th className="px-3">日均单边换手</th></tr></thead><tbody>{splits.flatMap(split => strategies.slice(0, 3).map(strategy => { const metric = record(record(bySplit[split.key])[strategy.key]); return <tr key={`${split.key}-${strategy.key}`} className="border-b border-gray-100"><td className="py-3 pr-4 text-left"><p className="font-medium text-gray-700">{split.label} · {strategy.label}</p><p className="mt-1 text-[10px] text-gray-400">{text(metric.start)} 至 {text(metric.end)}</p></td><td className={`px-3 tabular-nums ${pnlColor(numeric(metric.total_return))}`}>{percent(numeric(metric.total_return))}</td><td className={`px-3 tabular-nums ${pnlColor(numeric(metric.annual_return))}`}>{percent(numeric(metric.annual_return))}</td><td className="px-3 tabular-nums text-green-700">{percent(numeric(metric.max_drawdown))}</td><td className="px-3 tabular-nums">{number(numeric(metric.sharpe))}</td><td className="px-3">{number(numeric(metric.observation_days), 0)}</td><td className="px-3">{number(numeric(metric.total_trades), 0)}</td><td className="px-3">{number(numeric(metric.fees_in_initial_nav_units), 5)} / {number(numeric(metric.slippage_in_initial_nav_units), 5)}</td><td className="px-3">{percent(numeric(metric.mean_daily_one_way_turnover))}</td></tr>; }))}</tbody></table></div><p className="mt-3 text-xs text-gray-400">费用、滑点为相对各段初始净值 1 的损失累计值，并非人民币金额。未定义或缺失指标显示 —。</p></section>
    <ResearchLedger runId={runId} />
    <section className="account-panel space-y-3"><h3 className="font-semibold text-gray-900">研究限制与来源清单</h3>{strings(detail.report.limitations).length > 0 && <ul className="list-disc space-y-1 pl-4 text-xs leading-6 text-amber-800">{strings(detail.report.limitations).map((item, index) => <li key={index}>{item}</li>)}</ul>}<details><summary className="cursor-pointer text-sm text-blue-700">运行 manifest · 数据 / 协议 / 代码校验和与依赖版本</summary><Json value={detail.manifest} /></details><details><summary className="cursor-pointer text-sm text-blue-700">完整锁定参数</summary><Json value={detail.config} /></details><details><summary className="cursor-pointer text-sm text-blue-700">完整报告与历史股票池审计</summary><Json value={detail.report} /></details></section>
  </section>;
}

function ResearchCharts({ detail }: { detail: ResearchRunDetail }) {
  if (!detail.chart.length) return <div className="account-panel text-sm text-gray-500">结果没有可绘制的真实净值序列。</div>;
  return <div className="grid min-w-0 gap-5"><div className="account-panel min-w-0"><h3 className="mb-1 font-semibold text-gray-900">最终样本外净值</h3><p className="mb-4 text-xs text-gray-500">初始净值为 1，组合扣除交易费用和滑点；价格指数仅作不扣成本的参考。</p><div className="h-72 min-w-0"><ResponsiveContainer width="100%" height="100%"><LineChart data={detail.chart}><CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" /><XAxis dataKey="date" minTickGap={40} tickFormatter={value => String(value).slice(2)} tick={{ fontSize: 11 }} /><YAxis domain={['auto', 'auto']} tick={{ fontSize: 11 }} width={50} tickFormatter={value => number(numeric(value), 2)} /><Tooltip formatter={value => number(numeric(value), 4)} /><Legend wrapperStyle={{ fontSize: 12 }} />{strategies.map(item => <Line key={item.key} dataKey={item.key} name={item.label} stroke={item.color} strokeWidth={item.key === 'momentum' ? 2 : 1.5} dot={false} isAnimationActive={false} connectNulls={false} />)}</LineChart></ResponsiveContainer></div></div><div className="account-panel min-w-0"><h3 className="mb-4 font-semibold text-gray-900">相对历史高点的回撤</h3><div className="h-56 min-w-0"><ResponsiveContainer width="100%" height="100%"><LineChart data={detail.chart}><CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" /><XAxis dataKey="date" minTickGap={40} tickFormatter={value => String(value).slice(2)} tick={{ fontSize: 11 }} /><YAxis tickFormatter={value => percent(numeric(value))} tick={{ fontSize: 11 }} width={65} /><Tooltip formatter={value => percent(numeric(value))} /><Legend wrapperStyle={{ fontSize: 12 }} />{strategies.map(item => <Line key={item.key} dataKey={`${item.key}_drawdown`} name={item.label} stroke={item.color} strokeWidth={item.key === 'momentum' ? 2 : 1.5} dot={false} isAnimationActive={false} connectNulls={false} />)}</LineChart></ResponsiveContainer></div></div></div>;
}

interface Column { key: string; label: string; format?: 'percent' | 'number' | 'boolean' }
const columns: Record<ResearchTableName, Column[]> = {
  folds: [{ key: 'fold', label: '观察块' }, { key: 'start', label: '开始' }, { key: 'end', label: '结束' }, { key: 'observation_days', label: '交易日' }, { key: 'start_nav', label: '块前净值', format: 'number' }, { key: 'end_nav', label: '块后净值', format: 'number' }, { key: 'total_return', label: '块内净收益', format: 'percent' }, { key: 'annual_return', label: '几何年化', format: 'percent' }, { key: 'max_drawdown', label: '块内回撤', format: 'percent' }, { key: 'sharpe', label: 'Sharpe', format: 'number' }, { key: 'total_trades', label: '成交数' }, { key: 'portfolio_reset', label: '重置持仓', format: 'boolean' }],
  trades: [{ key: 'date', label: '执行日' }, { key: 'signal_date', label: '信号日' }, { key: 'instrument', label: '证券' }, { key: 'direction', label: '方向' }, { key: 'adjusted_open', label: '复权开盘价', format: 'number' }, { key: 'notional', label: '成交敞口', format: 'number' }, { key: 'fee_cost', label: '费用', format: 'number' }, { key: 'slippage_cost', label: '滑点', format: 'number' }, { key: 'virtual_unit_change', label: '虚拟单位变化', format: 'number' }],
  positions: [{ key: 'date', label: '日期' }, { key: 'instrument', label: '证券' }, { key: 'virtual_units', label: '虚拟复权单位', format: 'number' }, { key: 'market_value', label: '收盘敞口价值', format: 'number' }, { key: 'weight', label: '持仓权重', format: 'percent' }],
  signals: [{ key: 'date', label: '执行日' }, { key: 'signal_date', label: '信号日' }, { key: 'instrument', label: '证券' }, { key: 'lookback_start', label: '回看起点' }, { key: 'score', label: '动量收益', format: 'percent' }, { key: 'selected', label: '入选', format: 'boolean' }, { key: 'reason', label: '筛选原因' }],
};
const tableLabels: Record<ResearchTableName, string> = { folds: '样本外观察块', trades: '成交敞口', positions: '持仓敞口', signals: '信号记录' };
function ResearchLedger({ runId }: { runId: string }) {
  const [name, setName] = useState<ResearchTableName>('folds');
  const [split, setSplit] = useState('final_oos');
  const [strategy, setStrategy] = useState('momentum');
  const [offset, setOffset] = useState(0);
  return <section className="account-panel"><div className="flex flex-wrap items-center justify-between gap-3"><h3 className="font-semibold text-gray-900">观察块与研究账本</h3><div className="flex flex-wrap gap-2"><label className="sr-only" htmlFor="research-ledger-split">研究区间</label><select id="research-ledger-split" className={inputClass + ' !w-auto'} disabled={name === 'folds'} value={split} onChange={event => { setSplit(event.target.value); setOffset(0); }}>{splits.map(item => <option key={item.key} value={item.key}>{item.label}</option>)}</select><label className="sr-only" htmlFor="research-ledger-strategy">组合</label><select id="research-ledger-strategy" className={inputClass + ' !w-auto'} value={strategy} onChange={event => { setStrategy(event.target.value); setOffset(0); }}>{strategies.slice(0, 3).map(item => <option key={item.key} value={item.key}>{item.label}</option>)}</select></div></div><div className="mt-4 flex flex-wrap gap-2">{(Object.keys(tableLabels) as ResearchTableName[]).map(key => <button key={key} className={`rounded-lg px-3 py-2 text-xs font-medium ${key === name ? 'bg-blue-50 text-blue-700' : 'bg-gray-50 text-gray-500'}`} onClick={() => { setName(key); setOffset(0); }}>{tableLabels[key]}</button>)}</div><p className="my-3 text-xs leading-5 text-gray-500">{name === 'folds' ? '观察块仅划分最终样本外；持仓、现金与费用连续，不按块重置。块内回撤与全区间回撤口径不同。' : '敞口、费用与滑点均以该段初始净值 1 为单位，虚拟复权单位不是实际股数。空记录保持为空。'}</p><TableRows key={`${runId}-${name}-${split}-${strategy}-${offset}`} runId={runId} name={name} split={name === 'folds' ? 'final_oos' : split} strategy={strategy} offset={offset} onOffset={setOffset} /></section>;
}

function TableRows({ runId, name, split, strategy, offset, onOffset }: { runId: string; name: ResearchTableName; split: string; strategy: string; offset: number; onOffset: (value: number) => void }) {
  const [table, setTable] = useState<ResearchTable | null>(null);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    getResearchTable(runId, name, { split, strategy, offset, limit: 50 }, controller.signal).then(value => { if (active) { setTable(value); setError(''); } }).catch(reason => { if (active) setError(errorMessage(reason, '账本加载失败')); });
    return () => { active = false; controller.abort(); };
  }, [runId, name, split, strategy, offset, revision]);
  if (error) return <div><Alert>{error}</Alert><button onClick={() => { setError(''); setRevision(value => value + 1); }} className={`${buttonClass} mt-3`}>重试账本</button></div>;
  if (!table) return <p className="py-5 text-sm text-gray-500">读取研究账本…</p>;
  return <><div className="overflow-x-auto"><table className="w-full whitespace-nowrap text-left text-xs"><thead className="border-b border-gray-200 text-gray-500"><tr>{columns[name].map(column => <th key={column.key} className="px-3 py-2">{column.label}</th>)}</tr></thead><tbody>{table.rows.map((row, index) => <tr key={offset + index} className="border-b border-gray-100">{columns[name].map(column => <td key={column.key} className={`px-3 py-3 tabular-nums ${column.format === 'percent' ? pnlColor(numeric(row[column.key])) : 'text-gray-700'}`}>{cellValue(row[column.key], column)}</td>)}</tr>)}</tbody></table>{!table.rows.length && <p className="py-6 text-center text-sm text-gray-500">此区间和组合暂无{tableLabels[name]}。</p>}</div><div className="mt-4 flex items-center justify-between gap-3 text-xs text-gray-500"><span>共 {number(table.total, 0)} 条{table.total > 0 ? ` · 当前 ${table.offset + 1}–${table.offset + table.rows.length}` : ''}</span><div className="flex gap-2"><button className={buttonClass} disabled={offset === 0} onClick={() => onOffset(Math.max(0, offset - table.limit))}>上一页</button><button className={buttonClass} disabled={offset + table.rows.length >= table.total} onClick={() => onOffset(offset + table.limit)}>下一页</button></div></div></>;
}

function cellValue(value: unknown, column: Column): string {
  if (column.format === 'percent') return percent(numeric(value));
  if (column.format === 'number') return number(numeric(value), 6);
  if (column.format === 'boolean') return typeof value === 'boolean' ? value ? '是' : '否' : '—';
  if (column.key === 'direction') return ({ BUY: '买入', SELL: '卖出' } as Record<string, string>)[text(value)] || text(value);
  if (column.key === 'reason') return ({ eligible: '满足条件', missing_history: '历史缺价', known_suspended: '已知停牌', nonpositive_momentum: '动量非正' } as Record<string, string>)[text(value)] || text(value);
  return text(value);
}
function Alert({ children }: { children: ReactNode }) { return <div role="alert" className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm leading-6 text-red-800">{children}</div>; }
function Json({ value }: { value: unknown }) { return <pre className="mt-3 max-h-96 overflow-auto whitespace-pre-wrap break-all rounded-lg bg-gray-50 p-3 font-mono text-[11px] leading-5 text-gray-600">{JSON.stringify(value, null, 2)}</pre>; }
