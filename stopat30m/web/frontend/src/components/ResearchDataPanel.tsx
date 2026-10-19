import axios from 'axios';
import { useCallback, useEffect, useRef, useState, type FormEvent } from 'react';
import { Link } from 'react-router-dom';
import {
  getResearchJob, listResearchJobs, listResearchProtocols, listResearchSnapshots,
  startResearchDownload, type ResearchJob, type ResearchSnapshot,
  type ResearchProtocol,
} from '../api/research';
import { dateTime, errorMessage } from '../utils/format';
import { getSystemSettings } from '../api/settings';

const DEFAULT_PROTOCOL = 'eastmoney_etf';
const statusLabels: Record<ResearchJob['status'], string> = {
  running: '进行中', completed: '任务完成', failed: '失败', interrupted: '已中断', invalid: '记录损坏',
};

function object(value: unknown): Record<string, unknown> {
  return value != null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function valueLabel(value: unknown): string {
  return typeof value === 'string' || typeof value === 'number' ? String(value) : '—';
}

function coverageRows(snapshot: ResearchSnapshot) {
  return (snapshot.instruments || []).map(instrument => {
    const entry = object(snapshot.coverage?.[instrument]);
    const missing = typeof entry.missing_session_count === 'number' && Number.isInteger(entry.missing_session_count) && entry.missing_session_count >= 0
      ? entry.missing_session_count : null;
    const missingPrices = typeof entry.missing_price_count === 'number' && Number.isInteger(entry.missing_price_count) && entry.missing_price_count >= 0
      ? entry.missing_price_count : null;
    return { instrument, first: valueLabel(entry.first_date), last: valueLabel(entry.last_date), missing, missingPrices };
  });
}

function sourceLabel(source: string) {
  return source === 'eastmoney' ? '东方财富' : source || '来源未知';
}

export default function ResearchDataPanel({
  modelDataBusy = false, onBusyChange,
}: { modelDataBusy?: boolean; onBusyChange?: (busy: boolean) => void }) {
  const [protocols, setProtocols] = useState<ResearchProtocol[]>([]);
  const [snapshots, setSnapshots] = useState<ResearchSnapshot[]>([]);
  const [jobs, setJobs] = useState<ResearchJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [jobsKnown, setJobsKnown] = useState(false);
  const [catalogErrors, setCatalogErrors] = useState<Record<string, string>>({});
  const [actionError, setActionError] = useState('');
  const [pollError, setPollError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [refresh, setRefresh] = useState(false);
  const [minInterval, setMinInterval] = useState(3);
  const [maxAttempts, setMaxAttempts] = useState(4);
  const [requestTimeout, setRequestTimeout] = useState(15);
  const [preferencesLoading, setPreferencesLoading] = useState(true);
  const [preferencesError, setPreferencesError] = useState('');
  const preferencesInitialized = useRef(false);
  const preferencesEdited = useRef(false);
  const submitLock = useRef(false);
  const mounted = useRef(false);
  const catalogSequence = useRef(0);

  const protocol = protocols.find(item => item.id === DEFAULT_PROTOCOL);
  const activeJob = jobs.find(item => item.status === 'running');
  const activeId = activeJob?.id;
  const busy = loading || preferencesLoading || submitting || Boolean(activeId) || !jobsKnown;
  const compatibleSnapshots = snapshots.filter(snapshot => snapshot.validation_status !== 'invalid'
    && snapshot.compatible_protocols?.includes(DEFAULT_PROTOCOL));
  const otherSnapshots = snapshots.filter(snapshot => snapshot.validation_status !== 'invalid'
    && !snapshot.compatible_protocols?.includes(DEFAULT_PROTOCOL));
  const latest = compatibleSnapshots[0];

  const loadCatalog = useCallback(async (signal?: AbortSignal) => {
    const sequence = ++catalogSequence.current;
    const [p, s, j] = await Promise.allSettled([
      listResearchProtocols(signal), listResearchSnapshots(signal), listResearchJobs(signal),
    ]);
    if (signal?.aborted || !mounted.current || sequence !== catalogSequence.current) return;
    const errors: Record<string, string> = {};
    if (p.status === 'fulfilled') setProtocols(p.value);
    else errors.protocols = `协议读取失败：${errorMessage(p.reason)}`;
    if (s.status === 'fulfilled') setSnapshots([...s.value].sort((a, b) => (b.created_at || '').localeCompare(a.created_at || '')));
    else errors.snapshots = `快照目录读取失败：${errorMessage(s.reason)}。已显示的记录可能是上次结果。`;
    if (j.status === 'fulfilled') { setJobs(j.value); setJobsKnown(true); }
    else { errors.jobs = `任务状态读取失败：${errorMessage(j.reason)}。核对状态前暂停提交，避免重复任务。`; setJobsKnown(false); }
    setCatalogErrors(errors);
    setLoading(false);
  }, []);

  useEffect(() => {
    mounted.current = true;
    const controller = new AbortController();
    void loadCatalog(controller.signal);
    return () => { mounted.current = false; controller.abort(); };
  }, [loadCatalog]);

  useEffect(() => {
    if (preferencesInitialized.current) return;
    const controller = new AbortController();
    getSystemSettings(controller.signal).then(settings => {
      if (controller.signal.aborted || !mounted.current) return;
      preferencesInitialized.current = true;
      if (!preferencesEdited.current) {
        const fields = settings.groups.flatMap(group => group.fields);
        const value = (key: string, fallback: number, min: number, max: number, integer = false) => {
          const candidate = fields.find(field => field.key === key)?.value;
          return typeof candidate === 'number' && Number.isFinite(candidate) && candidate >= min && candidate <= max && (!integer || Number.isInteger(candidate)) ? candidate : fallback;
        };
        setMinInterval(value('research.min_interval', 3, 2, 60));
        setMaxAttempts(value('research.max_attempts', 4, 1, 6, true));
        setRequestTimeout(value('research.request_timeout', 15, 1, 60));
      }
      setPreferencesLoading(false);
    }).catch(error => {
      if (controller.signal.aborted || !mounted.current) return;
      preferencesInitialized.current = true;
      setPreferencesError(`持久下载偏好读取失败：${errorMessage(error)}。当前保留已知默认参数（间隔 3 秒、最多 4 次、超时 15 秒），可手动调整。`);
      setPreferencesLoading(false);
    });
    return () => controller.abort();
  }, []);

  useEffect(() => { onBusyChange?.(busy); }, [busy, onBusyChange]);

  useEffect(() => {
    if (!activeId) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const current = await getResearchJob(activeId, controller.signal);
        if (controller.signal.aborted) return;
        setPollError('');
        if (current.status !== 'running') {
          // Keep the active job until its catalog refresh finishes: clearing it
          // first would clean up this effect and abort its own refresh request.
          await loadCatalog(controller.signal);
          if (!controller.signal.aborted) setJobs(previous => previous.map(item => item.id === current.id ? current : item));
          return;
        }
        setJobs(previous => previous.map(item => item.id === current.id ? current : item));
      } catch (error) {
        if (controller.signal.aborted) return;
        setPollError(`暂时无法读取任务进度：${errorMessage(error)}。将继续检查，请勿重复提交。`);
      }
      if (!controller.signal.aborted) timer = setTimeout(poll, 4000);
    };
    timer = setTimeout(poll, 2500);
    return () => { controller.abort(); clearTimeout(timer); };
  }, [activeId, loadCatalog]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (submitLock.current || busy || modelDataBusy || !protocol) return;
    if (!Number.isFinite(minInterval) || minInterval < 2 || minInterval > 60
      || !Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 6
      || !Number.isFinite(requestTimeout) || requestTimeout < 1 || requestTimeout > 60) {
      setActionError('请检查请求间隔、重试次数与超时范围。'); return;
    }
    submitLock.current = true;
    setSubmitting(true);
    setActionError('');
    try {
      const job = await startResearchDownload({ protocol_id: DEFAULT_PROTOCOL, refresh,
        min_interval: minInterval, max_attempts: maxAttempts, request_timeout: requestTimeout });
      if (!mounted.current) return;
      setJobs(previous => [job, ...previous.filter(item => item.id !== job.id)].slice(0, 20));
      setJobsKnown(true);
      if (job.status !== 'running') await loadCatalog();
    } catch (error) {
      if (!mounted.current) return;
      setActionError(axios.isAxiosError(error) && error.response?.status === 409
        ? `后台已有任务或数据目录正在使用：${errorMessage(error)}。正在核对任务，请等待完成。`
        : `${errorMessage(error)}。正在重新核对后台状态；请求失败不代表数据已下载。`);
      await loadCatalog();
    } finally { submitLock.current = false; if (mounted.current) setSubmitting(false); }
  }

  async function reload() { setLoading(true); await loadCatalog(); }

  const parameters = protocol?.config || {};
  return <section className="account-panel space-y-5" aria-label="可信历史研究数据">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div><h2 className="font-semibold text-gray-900">历史研究数据</h2>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-gray-500">默认使用东方财富同源未复权与后复权日线，配合 BaoStock 交易日历。先锁定协议，再下载、校验和运行历史研究。</p>
      </div>
      <button type="button" className="rounded-lg border border-gray-200 px-3 py-2 text-xs text-gray-600 disabled:opacity-50" disabled={loading || submitting} onClick={() => void reload()}>{loading ? '核对中…' : '刷新状态'}</button>
    </div>

    {Object.entries(catalogErrors).map(([key, message]) => <p key={key} role="alert" className="rounded-lg bg-amber-50 px-4 py-3 text-sm text-amber-800">{message}</p>)}
    {preferencesError && <p role="alert" className="rounded-lg bg-amber-50 px-4 py-3 text-sm text-amber-800">{preferencesError}</p>}

    {protocol ? <div className="rounded-xl border border-blue-100 bg-blue-50/50 p-4">
      <p className="text-sm font-medium text-gray-900">{protocol.label} · 已锁定协议</p>
      <dl className="mt-4 grid gap-4 text-sm sm:grid-cols-2">
        <div><dt className="text-xs text-gray-500">固定标的（含基准）</dt><dd className="mt-1 break-words font-mono text-xs">{protocol.request.instruments.join(' · ')}</dd></div>
        <div><dt className="text-xs text-gray-500">请求历史区间</dt><dd className="mt-1">{protocol.request.start_date} → {protocol.request.end_date}</dd></div>
        <div><dt className="text-xs text-gray-500">价格 / 交易日历来源</dt><dd className="mt-1">{sourceLabel(protocol.source)} · raw + {protocol.request.adjustment}<span className="mt-1 block break-all text-xs text-gray-500">{protocol.calendar_source}</span></dd></div>
        <div><dt className="text-xs text-gray-500">固定研究参数</dt><dd className="mt-1">观察 {valueLabel(parameters.lookback_sessions)} 个交易日 · 每 {valueLabel(parameters.rebalance_sessions)} 日调仓 · 选 {valueLabel(parameters.top_k)} 只</dd></div>
      </dl>
      <p className="mt-3 text-xs leading-5 text-gray-500">历史样本外：{valueLabel(parameters.oos_start)} → {valueLabel(parameters.oos_end)}。固定池按锁定协议声明，历史回看仍有选池限制，不能当成历史当时已预先选池；后复权用于连续收益研究，不用于真实整数股账户成交。</p>
      <details className="mt-3 text-xs text-gray-500"><summary className="cursor-pointer">查看锁定参数与研究限制</summary>
        <p className="mt-3 break-all font-mono">协议 SHA-256：{protocol.protocol_sha256}</p>
        <pre className="mt-2 overflow-x-auto rounded-lg bg-white p-3">{JSON.stringify(parameters, null, 2)}</pre>
        <ul className="mt-2 space-y-1">{protocol.limitations.map((item, index) => <li key={index}>• {item}</li>)}</ul>
      </details>
    </div> : !loading && <p className="rounded-lg bg-gray-50 px-4 py-3 text-sm text-gray-600">尚未取得东方财富锁定协议，暂不能下载。请刷新状态或检查后台协议配置。</p>}

    <form onSubmit={submit}>
      <fieldset disabled={busy || modelDataBusy || !protocol} className="space-y-4 disabled:opacity-60">
        <div className="grid gap-4 sm:grid-cols-3">
          <label><span className="field-label">串行请求间隔（秒）</span><input className="field-input" type="number" min="2" max="60" step="any" required value={minInterval} onChange={event => { preferencesEdited.current = true; setMinInterval(Number(event.target.value)); }} /></label>
          <label><span className="field-label">每次请求最多尝试（次）</span><input className="field-input" type="number" min="1" max="6" step="1" required value={maxAttempts} onChange={event => { preferencesEdited.current = true; setMaxAttempts(Number(event.target.value)); }} /></label>
          <label><span className="field-label">单次请求超时（秒）</span><input className="field-input" type="number" min="1" max="60" step="any" required value={requestTimeout} onChange={event => { preferencesEdited.current = true; setRequestTimeout(Number(event.target.value)); }} /></label>
        </div>
        <label className="flex items-start gap-2 text-sm text-gray-700"><input type="checkbox" className="mt-1" checked={refresh} onChange={event => setRefresh(event.target.checked)} /><span>全部重新下载，创建新采集批次<span className="mt-1 block text-xs leading-5 text-gray-500">默认校验并复用已有快照，或在同一上海日期内恢复未完成下载。跨日的未完成批次需要勾选；新批次重新获取 raw / adjusted，旧快照保留。</span></span></label>
        <p className="text-xs text-gray-500">{preferencesLoading ? '正在读取持久下载偏好…' : '本次参数可临时调整；持久默认值在“系统配置”保存，刷新页面后载入。'}</p>
        <div className="flex flex-wrap items-center gap-3"><button type="submit" className="primary-button">{submitting ? '提交中…' : activeJob ? '后台研究任务进行中…' : loading || preferencesLoading || !jobsKnown ? '先核对后台状态' : refresh ? '重新下载新批次' : '下载或恢复研究数据'}</button><span className="text-xs text-gray-500">403 或数据异常会停止；429 按服务端要求冷却，不绕过限制。</span></div>
      </fieldset>
    </form>
    {modelDataBusy && <p className="text-sm text-amber-700">旧模型数据任务尚未确认或正在运行，请展开下方“模型数据维护”核对任务状态，避免同时启动两套数据下载。</p>}
    {actionError && <p role="alert" className="whitespace-pre-wrap break-words rounded-lg bg-red-50 p-3 text-sm text-red-700">{actionError}</p>}
    {pollError && <p role="alert" className="text-sm text-amber-700">{pollError}</p>}

    <div className="border-t border-gray-100 pt-5">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2"><h3 className="text-sm font-semibold text-gray-900">已校验的真实数据快照</h3><Link className="text-sm font-medium text-blue-600" to="/backtest?mode=research">查看历史研究 →</Link></div>
      {latest ? <SnapshotCard snapshot={latest} /> : !loading && !catalogErrors.snapshots
        ? <div className="rounded-xl border border-dashed border-gray-300 bg-gray-50 p-5 text-sm leading-6 text-gray-600"><p className="font-medium text-gray-800">尚无与当前协议匹配的有效真实快照</p><p className="mt-1">完成下载并通过校验后，才会显示来源、实际数据日期和覆盖缺口。目前无法运行这份协议的真实历史研究，也没有收益结果。</p></div>
        : <p className="text-sm text-gray-500">{loading ? '正在核对快照…' : '快照目录暂不可确认，请重试。'}</p>}
      {snapshots.some(item => item.validation_status === 'invalid') && <div className="mt-3 space-y-2">{snapshots.filter(item => item.validation_status === 'invalid').map(snapshot => <div key={snapshot.id} className="rounded-lg border border-red-100 bg-red-50 p-3 text-sm text-red-700"><p className="font-medium">快照校验失败 · 不可用于研究</p><p className="mt-1 break-all font-mono text-xs">{snapshot.id}</p><p className="mt-2 break-words">{snapshot.validation_error || '文件或元数据校验未通过。'}</p></div>)}</div>}
      {compatibleSnapshots.length > 1 && <details className="mt-4 text-sm text-gray-600"><summary className="cursor-pointer">其他已校验快照（{compatibleSnapshots.length - 1}）</summary><div className="mt-3 space-y-3">{compatibleSnapshots.slice(1).map(snapshot => <SnapshotCard key={snapshot.id} snapshot={snapshot} />)}</div></details>}
      {otherSnapshots.length > 0 && <details className="mt-4 text-sm text-gray-600"><summary className="cursor-pointer">已校验但不满足当前协议的快照（{otherSnapshots.length}）</summary><p className="mt-2 text-xs leading-5 text-amber-700">这些文件通过基础校验，但来源、区间或必要价格完整性不符合当前锁定协议，不能用于该研究。可展开核对日期和缺口。</p><div className="mt-3 space-y-3">{otherSnapshots.map(snapshot => <SnapshotCard key={snapshot.id} snapshot={snapshot} />)}</div></details>}
      <p className="mt-3 text-xs leading-5 text-gray-500">快照创建时间是采集记录，不是实时行情发生时间。缓存命中不会更新源日期；校验通过也不证明公司行为现金账本完整或达到任何收益目标。</p>
    </div>

    <div className="border-t border-gray-100 pt-5"><h3 className="mb-3 text-sm font-semibold text-gray-900">最近任务</h3>
      {jobs.length ? <div className="max-h-80 space-y-3 overflow-y-auto pr-1">{jobs.map(job => <div key={job.id} role={job.status === 'running' ? 'status' : undefined} className={`rounded-lg border p-3 text-sm ${job.status === 'failed' || job.status === 'interrupted' || job.status === 'invalid' ? 'border-red-100 bg-red-50/50' : job.status === 'running' ? 'border-blue-100 bg-blue-50/50' : 'border-gray-100 bg-gray-50'}`}>
        <div className="flex flex-wrap justify-between gap-2"><p className="font-medium text-gray-800">{job.kind === 'download' ? '研究数据准备' : job.kind === 'run' ? '历史研究运行' : '任务记录'} · {statusLabels[job.status]}</p><span className="text-xs text-gray-500">{dateTime(job.started_at)}</span></div>
        <p className="mt-1 whitespace-pre-wrap break-words text-gray-600">{job.progress || (job.status === 'running' ? '等待后台进度…' : '没有额外进度信息')}</p>
        {job.error && <p className="mt-2 whitespace-pre-wrap break-words text-red-700">{job.error}</p>}
        {job.status === 'completed' && <p className="mt-2 text-xs text-gray-500">任务已结束，可能复用了已有缓存；请以上方快照的实际日期与覆盖为准。</p>}
        <details className="mt-2 text-xs text-gray-500"><summary className="cursor-pointer">任务详情</summary><p className="mt-2 break-all">任务 {job.id} · 协议 {job.protocol_id}</p>{job.completed_at && <p className="mt-1">结束 {dateTime(job.completed_at)}</p>}{job.snapshot_id && <p className="mt-1 break-all">快照 {job.snapshot_id}</p>}{job.run_id && <p className="mt-1 break-all">研究结果 {job.run_id}</p>}</details>
      </div>)}</div> : <p className="text-sm text-gray-500">{loading ? '正在读取任务…' : catalogErrors.jobs ? '暂不能确认任务记录。' : '尚无研究任务记录。'}</p>}
      <p className="mt-3 text-xs text-gray-500">任务在后台执行。刷新页面后重新读取任务状态，并继续跟踪运行中的任务。</p>
    </div>
  </section>;
}

function SnapshotCard({ snapshot }: { snapshot: ResearchSnapshot }) {
  const rows = coverageRows(snapshot);
  const known = rows.length > 0 && rows.every(row => row.missing !== null && row.missingPrices !== null);
  const missing = rows.reduce((sum, row) => sum + (row.missing || 0), 0);
  const missingPrices = rows.reduce((sum, row) => sum + (row.missingPrices || 0), 0);
  const lastDates = rows.map(row => row.last).filter(day => /^\d{4}-\d{2}-\d{2}$/.test(day));
  const coveredThrough = lastDates.length === rows.length && lastDates.length ? [...lastDates].sort()[0] : '尚不能确认';
  return <div className="rounded-xl border border-gray-200 bg-white p-4">
    <div className="flex flex-wrap items-center justify-between gap-2"><p className="text-sm font-medium text-gray-900">{sourceLabel(snapshot.source)} · {snapshot.adjustment || '复权方式未知'} · {snapshot.session_count ?? '—'} 个交易日</p><span className={`rounded-full px-2 py-1 text-xs ${known && missing === 0 && missingPrices === 0 ? 'bg-blue-50 text-blue-700' : 'bg-amber-50 text-amber-800'}`}>{known ? missing || missingPrices ? `缺失记录 ${missing} · 缺价 ${missingPrices}` : '请求区间无缺失记录或价格' : '覆盖仍需核对'}</span></div>
    <dl className="mt-3 grid gap-3 text-sm sm:grid-cols-3"><div><dt className="text-xs text-gray-500">请求区间</dt><dd className="mt-1">{snapshot.start_date || '—'} → {snapshot.end_date || '—'}</dd></div><div><dt className="text-xs text-gray-500">实际末日（取标的最早值）</dt><dd className="mt-1">{coveredThrough}</dd></div><div><dt className="text-xs text-gray-500">快照创建时间</dt><dd className="mt-1">{dateTime(snapshot.created_at)}</dd></div></dl>
    {known && (missing > 0 || missingPrices > 0) && <p className="mt-3 text-xs leading-5 text-amber-700">当前区间存在缺口，不能称为完整历史数据。请核对各标的实际日期、上市区间及研究所需数据；不会填造缺失价格。</p>}
    <details className="mt-3 text-xs text-gray-500"><summary className="cursor-pointer">按标的核对数据与快照编号</summary><div className="mt-3 grid gap-2 sm:grid-cols-2">{rows.map(row => <p key={row.instrument} className="rounded-lg bg-gray-50 p-2"><strong>{row.instrument}</strong><span className="mt-1 block">{row.first} → {row.last} · 缺失记录 {row.missing ?? '未知'} · 缺价 {row.missingPrices ?? '未知'}</span></p>)}</div><p className="mt-3 break-all font-mono">{snapshot.id}</p></details>
  </div>;
}
