import { useCallback, useEffect, useRef, useState, type FormEvent } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { changePassword } from '../api/auth';
import { getConfigSummary, getDataStatus, getDataUpdate, startDataUpdate, listModels, type ConfigSummary, type DataStatus, type DataUpdateJob, type ModelFile } from '../api/system';
import { useAuth } from '../contexts/AuthContext';
import DataHealth from '../components/DataHealth';
import ResearchDataPanel from '../components/ResearchDataPanel';
import SystemSettingsPanel from '../components/SystemSettingsPanel';
import { dateTime, errorMessage, localDateTime } from '../utils/format';

type SettingsTab = 'configuration' | 'data' | 'models' | 'security';
const tabs: { key: SettingsTab; label: string }[] = [{ key: 'configuration', label: '系统配置' }, { key: 'data', label: '历史研究数据' }, { key: 'models', label: '量化模型维护' }, { key: 'security', label: '账户安全' }];

export default function SettingsPage() {
  const { user: currentUser, isAdmin } = useAuth();
  const [params] = useSearchParams();
  const [tab, setTab] = useState<SettingsTab>(() => tabs.find(item => item.key === params.get('section'))?.key || 'configuration');
  const [maintenanceVisited, setMaintenanceVisited] = useState(() => ['data', 'models'].includes(params.get('section') || ''));
  const [dataStatus, setDataStatus] = useState<DataStatus | null>(null);
  const [cfg, setCfg] = useState<ConfigSummary | null>(null);
  const [models, setModels] = useState<ModelFile[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [researchBusy, setResearchBusy] = useState(true);
  const [modelDataBusy, setModelDataBusy] = useState(true);
  const mounted = useRef(false);
  const reloadData = useCallback(() => { getDataStatus().then(value => { if (mounted.current) setDataStatus(value); }).catch(error => { if (mounted.current) setErr(errorMessage(error)); }); }, []);

  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    if (!isAdmin) return;
    let active = true;
    Promise.allSettled([getDataStatus(), getConfigSummary(), listModels()] as const).then(([d, c, m]) => {
      if (!active) return;
      const errors: string[] = [];
      if (d.status === 'fulfilled') setDataStatus(d.value); else errors.push(`数据：${errorMessage(d.reason)}`);
      if (c.status === 'fulfilled') setCfg(c.value); else errors.push(`配置摘要：${errorMessage(c.reason)}`);
      if (m.status === 'fulfilled') setModels(m.value); else errors.push(`模型：${errorMessage(m.reason)}`);
      setErr(errors.length ? errors.join('；') : null);
      setLoading(false);
    });
    return () => { active = false; };
  }, [isAdmin]);

  const activeTab = isAdmin ? tab : 'security';
  function selectTab(value: SettingsTab) { setTab(value); if (value === 'data' || value === 'models') setMaintenanceVisited(true); }
  return <div className="mx-auto max-w-6xl">
    <h1 className="mb-2 text-2xl font-bold text-gray-900">系统设置</h1>
    <p className="mb-6 text-sm text-gray-500">当前用户：<strong>{currentUser?.username}</strong>（{isAdmin ? '个人管理员' : '普通用户'}） · 配置、数据维护与账户安全</p>
    <div className="mb-6 flex flex-wrap gap-1 rounded-lg bg-gray-100 p-1" aria-label="设置分组">{tabs.filter(item => isAdmin || item.key === 'security').map(item => <button key={item.key} type="button" className={`flex-1 whitespace-nowrap rounded-md px-3 py-2 text-sm font-medium ${activeTab === item.key ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`} aria-pressed={activeTab === item.key} onClick={() => selectTab(item.key)}>{item.label}</button>)}</div>
    {!isAdmin && <p className="account-panel mb-5 text-sm text-gray-600">系统配置与数据维护仅供管理员使用。你可以在这里修改自己的登录密码。</p>}
    {isAdmin && <div hidden={activeTab !== 'configuration'}><SystemSettingsPanel /></div>}
    {isAdmin && maintenanceVisited && <>
      <div hidden={activeTab !== 'data'}><ResearchDataPanel modelDataBusy={modelDataBusy} onBusyChange={setResearchBusy} /></div>
      <div hidden={activeTab !== 'models'} className="space-y-5">
        <div className="account-panel"><h2 className="font-semibold text-gray-900">模型数据维护（Qlib）</h2><p className="mt-2 text-sm leading-6 text-gray-500">这里维护模型训练与因子使用的 Qlib 数据，和历史研究快照是两套数据。只做固定 ETF 历史研究时，使用“历史研究数据”即可；研究快照不会自动转换为训练数据。</p></div>
        {err && <p role="alert" className="rounded-lg bg-red-50 p-4 text-sm text-red-700">{err}</p>}
        {loading && <p className="text-sm text-gray-500">正在读取模型数据状态…</p>}
        {!loading && dataStatus && <DataHealth data={dataStatus} />}
        <DataUpdatePanel onComplete={reloadData} blocked={researchBusy} onBusyChange={setModelDataBusy} />
        {!loading && cfg && <section className="account-panel"><h3 className="mb-4 font-semibold text-gray-900">模型与信号摘要</h3><dl className="grid gap-4 text-sm sm:grid-cols-2"><div><dt className="text-xs text-gray-500">Qlib provider</dt><dd className="mt-1 break-all font-mono text-xs">{cfg.qlib_provider_uri || '—'}</dd></div><div><dt className="text-xs text-gray-500">股票池</dt><dd className="mt-1">{cfg.universe}</dd></div><div><dt className="text-xs text-gray-500">模型类型</dt><dd className="mt-1">{cfg.model_type}</dd></div><div><dt className="text-xs text-gray-500">信号方法</dt><dd className="mt-1">{cfg.signal_method} · Top {cfg.signal_top_k}</dd></div></dl><p className="mt-4 text-xs text-gray-500">参数修改使用“系统配置”，已有锁定协议不会跟随默认值改变。</p></section>}
        {models.length > 0 && <section className="account-panel"><h3 className="mb-3 font-semibold text-gray-900">已训练模型</h3><ul className="space-y-1 text-sm">{models.map(model => <li key={model.name} className="flex justify-between gap-4 border-b border-gray-100 py-2"><span className="break-all font-mono">{model.name}</span><span className="whitespace-nowrap text-gray-500">{model.size_mb} MB</span></li>)}</ul></section>}
      </div>
    </>}
    <div hidden={activeTab !== 'security'}><AccountSecurityPanel /></div>
  </div>;
}

function AccountSecurityPanel() {
  const { logout } = useAuth();
  const navigate = useNavigate();
  const [current, setCurrent] = useState('');
  const [password, setPassword] = useState('');
  const [confirmation, setConfirmation] = useState('');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState(false);
  const [saving, setSaving] = useState(false);
  const submitLock = useRef(false);
  const mounted = useRef(false);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (submitLock.current || success) return;
    if (password.length < 10 || password.length > 128) { setError('新密码须为 10–128 个字符。'); return; }
    if (password !== confirmation) { setError('两次输入的新密码不一致。'); return; }
    submitLock.current = true; setSaving(true); setError('');
    try {
      await changePassword(current, password);
      if (mounted.current) { setCurrent(''); setPassword(''); setConfirmation(''); setSuccess(true); }
    } catch (reason) { if (mounted.current) setError(errorMessage(reason, '密码修改失败')); }
    finally { submitLock.current = false; if (mounted.current) setSaving(false); }
  }
  return <section className="account-panel max-w-2xl"><h2 className="font-semibold text-gray-900">修改登录密码</h2><p className="mb-5 mt-2 text-sm text-gray-500">输入当前密码验证身份，新密码保存后请重新登录。</p>{success ? <div role="status" className="rounded-lg bg-blue-50 p-4 text-sm text-blue-800"><p>密码已修改，所有密码输入已清空。请使用新密码重新登录。</p><button type="button" className="mt-3 rounded-lg bg-blue-600 px-4 py-2 text-white" onClick={() => { logout(); navigate('/login', { replace: true }); }}>退出并重新登录</button></div> : <form onSubmit={submit}><fieldset disabled={saving} className="space-y-4"><label className="block"><span className="field-label">当前密码</span><input className="field-input" type="password" required autoComplete="current-password" value={current} onChange={event => setCurrent(event.target.value)} /></label><label className="block"><span className="field-label">新密码（10–128 个字符）</span><input className="field-input" type="password" required minLength={10} maxLength={128} autoComplete="new-password" value={password} onChange={event => setPassword(event.target.value)} /></label><label className="block"><span className="field-label">确认新密码</span><input className="field-input" type="password" required minLength={10} maxLength={128} autoComplete="new-password" value={confirmation} onChange={event => setConfirmation(event.target.value)} /></label>{error && <p role="alert" className="text-sm text-red-700">{error}</p>}<button type="submit" className="primary-button">{saving ? '保存中…' : '修改密码'}</button></fieldset></form>}</section>;
}

function DataUpdatePanel({ onComplete, blocked, onBusyChange }: {
  onComplete: () => void; blocked: boolean; onBusyChange: (busy: boolean) => void;
}) {
  const [job, setJob] = useState<DataUpdateJob | null>(null);
  const [source, setSource] = useState('baostock');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState(localDateTime().slice(0, 10));
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const submitLock = useRef(false);
  const [checking, setChecking] = useState(true);
  const [statusKnown, setStatusKnown] = useState(false);
  const mounted = useRef(false);
  const running = job?.status === 'running';
  useEffect(() => { onBusyChange(checking || submitting || running || !statusKnown); }, [checking, submitting, running, statusKnown, onBusyChange]);
  const checkStatus = useCallback(async (markChecking = true) => {
    if (markChecking && mounted.current) setChecking(true);
    try {
      const value = await getDataUpdate();
      if (!mounted.current) return;
      setJob(value); setStatusKnown(true); setError('');
    } catch (error) {
      if (!mounted.current) return;
      setStatusKnown(false);
      setError(`旧模型数据任务状态无法确认：${errorMessage(error)}。请先重新核对状态，避免重复下载。`);
    } finally { if (mounted.current) setChecking(false); }
  }, []);
  useEffect(() => {
    mounted.current = true;
    void checkStatus(false);
    return () => { mounted.current = false; };
  }, [checkStatus]);
  useEffect(() => {
    if (!running) return;
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const value = await getDataUpdate();
        if (!active) return;
        setJob(value);
        setStatusKnown(true);
        setError('');
        if (value?.status === 'completed') onComplete();
        if (value?.status !== 'running') return;
      } catch (e) { if (active) { setStatusKnown(false); setError(`更新状态暂不可用：${errorMessage(e)}，将继续检查。`); } }
      if (active) timer = setTimeout(poll, 4000);
    };
    timer = setTimeout(poll, 3000);
    return () => { active = false; clearTimeout(timer); };
  }, [running, onComplete]);
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (submitLock.current || running || blocked || checking || !statusKnown) return;
    if (startDate && endDate && startDate > endDate) { setError('开始日期不能晚于结束日期'); return; }
    submitLock.current = true;
    setSubmitting(true);
    setError('');
    try {
      const value = await startDataUpdate({ source, start_date: startDate || undefined, end_date: endDate || undefined });
      if (mounted.current) { setJob(value); setStatusKnown(true); }
    } catch (error) {
      if (!mounted.current) return;
      const message = errorMessage(error);
      setError(`${message}。正在核对后台任务，暂不重复提交。`);
      try {
        const value = await getDataUpdate();
        if (mounted.current) { setJob(value); setStatusKnown(true); }
      } catch (statusError) {
        if (mounted.current) { setStatusKnown(false); setError(`${message}；后台状态也无法确认：${errorMessage(statusError)}。请重新核对任务状态。`); }
      }
    } finally { submitLock.current = false; if (mounted.current) setSubmitting(false); }
  }
  return <section className="account-panel">
    <div className="flex flex-wrap items-center justify-between gap-2"><h2 className="font-semibold text-gray-900">更新旧版模型数据</h2><button type="button" className="rounded-lg border border-gray-200 px-3 py-2 text-xs text-gray-600 disabled:opacity-50" disabled={checking || submitting} onClick={() => void checkStatus()}>{checking ? '核对中…' : '重新核对任务状态'}</button></div>
    <p className="mb-4 mt-2 text-sm text-gray-500">仅供 Qlib 模型与旧版因子流程使用。任务完成不表示上方历史研究快照已准备好；仍需核对本地可信日期和数据缺口。</p>
    {blocked && <p className="mb-4 text-sm text-amber-700">研究任务正在运行或状态尚未核对，暂不同时启动模型数据下载。</p>}
    <form onSubmit={submit}><fieldset disabled={submitting || running || checking || blocked || !statusKnown} className="grid items-end gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <label><span className="field-label">数据来源</span><select className="field-input" value={source} onChange={event => setSource(event.target.value)}><option value="baostock">BaoStock</option><option value="akshare">AKShare</option><option value="efinance">efinance</option><option value="tushare">Tushare</option></select></label>
      <label><span className="field-label">开始日期（留空按已有配置）</span><input className="field-input" type="date" max={endDate || undefined} value={startDate} onChange={event => setStartDate(event.target.value)} /></label>
      <label><span className="field-label">请求截止日</span><input className="field-input" type="date" max={localDateTime().slice(0, 10)} value={endDate} onChange={event => setEndDate(event.target.value)} /></label>
      <button className="primary-button" type="submit">{checking ? '检查任务中…' : running ? '数据更新中…' : submitting ? '提交中…' : '开始更新'}</button>
    </fieldset></form>
    {error && <p role="alert" className="mt-4 text-sm text-red-700">{error}</p>}
    {job && <div role="status" className="mt-4 rounded-lg bg-gray-50 px-4 py-3 text-sm text-gray-600">
      <p className="font-medium">{({ running: '更新进行中', completed: '更新任务完成，请核对数据状态', failed: '更新失败', interrupted: '更新任务已中断' })[job.status]} · {job.source}</p>
      <p className="mt-1 text-xs text-gray-400">开始 {dateTime(job.started_at)}{job.completed_at && ` · 结束 ${dateTime(job.completed_at)}`}</p>
      {job.error && <p className="mt-2 break-words text-xs text-red-700">{job.error}</p>}
    </div>}
  </section>;
}
