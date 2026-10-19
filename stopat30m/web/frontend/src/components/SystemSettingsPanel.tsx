import axios from 'axios';
import { Link } from 'react-router-dom';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import { getSystemSettings, getSystemSettingsAudit, saveSystemSettings, type SystemSettingField, type SystemSettingsAudit, type SystemSettingsResponse, type SystemSettingValue } from '../api/settings';
import { dateTime, errorMessage } from '../utils/format';

type Drafts = Record<string, string | boolean>;
const inputClass = 'w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm disabled:bg-gray-100 disabled:text-gray-500';
const sourceLabels = { default: '默认值', local: '本地设置', environment: '环境变量' };

function inputValue(field: SystemSettingField, drafts: Drafts): string | boolean {
  if (Object.hasOwn(drafts, field.key)) return drafts[field.key];
  if (field.type === 'secret') return '';
  if (field.type === 'boolean') return field.value === true;
  return field.value == null ? '' : String(field.value);
}

function changes(settings: SystemSettingsResponse | null, drafts: Drafts, clears: string[]) {
  const values: Record<string, SystemSettingValue> = {};
  const errors: Record<string, string> = {};
  const clearSecrets: string[] = [];
  for (const field of settings?.groups.flatMap(group => group.fields) || []) {
    if (field.read_only || field.source === 'environment') continue;
    const raw = inputValue(field, drafts);
    if (field.type === 'secret') {
      if (clears.includes(field.key) && field.configured) clearSecrets.push(field.key);
      else if (typeof raw === 'string' && raw !== '') {
        if (!raw.trim()) errors[field.key] = '密钥不能只包含空白；留空保留现有值，清除请勾选下方选项';
        else values[field.key] = raw;
      }
      continue;
    }
    if (!Object.hasOwn(drafts, field.key)) continue;
    let value: SystemSettingValue = raw;
    if (field.type === 'integer' || field.type === 'number') {
      const candidate = typeof raw === 'string' && raw.trim() !== '' ? Number(raw) : NaN;
      if (!Number.isFinite(candidate) || (field.type === 'integer' && !Number.isInteger(candidate))) errors[field.key] = field.type === 'integer' ? '请输入有效整数' : '请输入有效数字';
      else if (field.min != null && candidate < field.min) errors[field.key] = `不得小于 ${field.min}`;
      else if (field.max != null && candidate > field.max) errors[field.key] = `不得大于 ${field.max}`;
      value = candidate;
    } else if (field.type === 'enum') {
      const option = field.options?.find(item => String(item.value) === String(raw));
      if (!option) errors[field.key] = '请选择支持的选项';
      value = option?.value ?? String(raw);
    }
    if (!errors[field.key] && value !== field.value) values[field.key] = value;
  }
  return { values, errors, clearSecrets, count: Object.keys(values).length + clearSecrets.length };
}

export default function SystemSettingsPanel() {
  const [settings, setSettings] = useState<SystemSettingsResponse | null>(null);
  const [drafts, setDrafts] = useState<Drafts>({});
  const [clearSecrets, setClearSecrets] = useState<string[]>([]);
  const [audit, setAudit] = useState<SystemSettingsAudit[]>([]);
  const [auditLoading, setAuditLoading] = useState(true);
  const [auditError, setAuditError] = useState('');
  const [loadError, setLoadError] = useState('');
  const [saveError, setSaveError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [conflict, setConflict] = useState(false);
  const [revision, setRevision] = useState(0);
  const [auditRevision, setAuditRevision] = useState(0);
  const mounted = useRef(false);
  const saveLock = useRef(false);

  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    getSystemSettings(controller.signal).then(value => {
      if (!active) return;
      setSettings(value); setDrafts({}); setClearSecrets([]); setLoadError(''); setConflict(false); setLoading(false);
    }).catch(reason => { if (active) { setLoadError(errorMessage(reason, '系统设置加载失败')); setLoading(false); } });
    return () => { active = false; controller.abort(); };
  }, [revision]);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    getSystemSettingsAudit(controller.signal).then(value => { if (active) { setAudit(value); setAuditError(''); setAuditLoading(false); } }).catch(reason => { if (active) { setAuditError(errorMessage(reason, '变更记录加载失败')); setAuditLoading(false); } });
    return () => { active = false; controller.abort(); };
  }, [auditRevision]);

  const pending = changes(settings, drafts, clearSecrets);
  const invalid = Object.keys(pending.errors).length > 0;
  function reload() {
    if (saveLock.current) return;
    setDrafts({}); setClearSecrets([]); setLoading(true); setLoadError(''); setSaveError(''); setSuccess('');
    setRevision(value => value + 1); setAuditLoading(true); setAuditRevision(value => value + 1);
  }
  function edit(key: string, value: string | boolean) { setDrafts(current => ({ ...current, [key]: value })); setSuccess(''); }
  function clear(key: string, enabled: boolean) {
    setClearSecrets(current => enabled ? [...current.filter(item => item !== key), key] : current.filter(item => item !== key));
    setDrafts(current => ({ ...current, [key]: '' })); setSuccess('');
  }
  async function save(event: FormEvent) {
    event.preventDefault();
    if (saveLock.current || !settings || loading || loadError || conflict || invalid || !pending.count) return;
    saveLock.current = true;
    setSaving(true); setSaveError(''); setSuccess('');
    try {
      const response = await saveSystemSettings({ revision: settings.revision, values: pending.values, clear_secrets: pending.clearSecrets });
      if (!mounted.current) return;
      setSettings(response); setDrafts({}); setClearSecrets([]); setConflict(false);
      setSuccess(`已保存 ${response.changed_keys?.length ?? pending.count} 项设置。下方显示服务器当前生效值；密钥输入已清空。`);
      setAuditLoading(true); setAuditRevision(value => value + 1);
    } catch (reason) {
      if (!mounted.current) return;
      const status = axios.isAxiosError(reason) ? reason.response?.status : undefined;
      const unknown = axios.isAxiosError(reason) && (!reason.response || (status != null && status >= 500));
      if (status === 409 || unknown) {
        setConflict(true);
        setSaveError(status === 409 ? `设置已在其他位置变更：${errorMessage(reason)}。请重新加载，核对最新值后再修改；本次不会覆盖。` : `无法确认保存结果：${errorMessage(reason)}。请重新加载服务器设置后再修改，避免覆盖已保存内容。`);
      } else setSaveError(errorMessage(reason, '设置保存失败'));
    } finally { saveLock.current = false; if (mounted.current) setSaving(false); }
  }

  return <div className="space-y-5">
    <Link to="/model-settings" className="flex items-center justify-between gap-4 rounded-xl border border-blue-100 bg-blue-50/60 px-5 py-4"><div><p className="text-sm font-semibold text-gray-900">AI 模型配置</p><p className="mt-1 text-xs text-gray-500">选择 OpenAI、Claude 等服务商，管理模型、地址和密钥。</p></div><span className="shrink-0 text-sm font-medium text-blue-700">前往配置 →</span></Link>
    <div className="account-panel"><div className="flex flex-wrap items-start justify-between gap-3"><div><h2 className="font-semibold text-gray-900">系统配置</h2><p className="mt-2 max-w-3xl text-sm leading-6 text-gray-500">这里只展示服务器支持修改的设置。保存到本机配置，仅提交发生变化的字段；锁定的研究协议与已有结果保持各自记录的参数。</p></div><button type="button" disabled={loading || saving} onClick={reload} className="rounded-lg border border-gray-300 px-3 py-2 text-xs text-gray-600 disabled:opacity-50">{loading ? '加载中…' : pending.count || invalid ? '重新加载并放弃未保存修改' : '重新加载设置'}</button></div>
      {loadError && <p role="alert" className="mt-4 rounded-lg bg-red-50 p-3 text-sm text-red-700">{loadError}。请重新加载后再编辑。</p>}
      {!settings && loading && <p role="status" className="mt-4 text-sm text-gray-500">正在读取真实配置…</p>}
      {settings?.restart_required.length ? <p className="mt-4 rounded-lg bg-amber-50 p-3 text-sm leading-6 text-amber-900">以下设置需要重启服务才能由长驻实例使用：<span className="break-words font-mono text-xs">{settings.restart_required.join('、')}</span></p> : null}
    </div>
    {settings && <form onSubmit={save} className="space-y-5"><fieldset disabled={loading || saving || Boolean(loadError) || conflict} className="space-y-5">
      <div className="account-panel sticky top-0 z-10 space-y-3 shadow-sm"><div className="flex flex-wrap items-center justify-between gap-3"><p className="text-sm text-gray-600">{invalid ? '请展开标记的分组，修正字段后保存。' : pending.count ? `${pending.count} 项待保存` : '没有未保存的变更'}</p><button type="submit" disabled={!pending.count || invalid} className="rounded-lg bg-blue-600 px-5 py-2.5 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-gray-300">{saving ? '保存中…' : '保存变更'}</button></div>
        {saveError && <p role="alert" className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm leading-6 text-red-800">{saveError}</p>}
        {success && <p role="status" className="rounded-lg border border-blue-200 bg-blue-50 p-3 text-sm leading-6 text-blue-800">{success}</p>}
      </div>
      {settings.groups.filter(group => group.id !== 'llm').map(group => {
        const errors = group.fields.filter(field => pending.errors[field.key]).length;
        const changed = group.fields.filter(field => Object.hasOwn(pending.values, field.key) || pending.clearSecrets.includes(field.key)).length;
        return <details key={group.id} className="account-panel group" open={group.id === 'research'}>
          <summary className="cursor-pointer list-none"><div className="flex flex-wrap items-center justify-between gap-3"><div className="flex flex-wrap items-center gap-2"><span className="font-semibold text-gray-900">{group.title}</span>{changed > 0 && <span className="rounded bg-blue-50 px-2 py-0.5 text-xs text-blue-700">{changed} 项待保存</span>}{errors > 0 && <span className="rounded bg-red-50 px-2 py-0.5 text-xs text-red-700">{errors} 项需修正</span>}</div><span className="text-xs text-blue-700"><span className="group-open:hidden">展开配置 ↓</span><span className="hidden group-open:inline">收起配置 ↑</span></span></div><p className="mt-2 text-sm leading-6 text-gray-500">{group.description}</p></summary>
          <div className="mt-5 grid gap-x-8 gap-y-6 border-t border-gray-100 pt-5 lg:grid-cols-2">{group.fields.map(field => <SettingField key={field.key} field={field} value={inputValue(field, drafts)} clearing={clearSecrets.includes(field.key)} error={pending.errors[field.key]} onChange={value => edit(field.key, value)} onClear={enabled => clear(field.key, enabled)} />)}</div>
          <details className="mt-5 border-t border-gray-100 pt-3 text-xs text-gray-400"><summary className="cursor-pointer">高级详情：字段标识与生效范围</summary><dl className="mt-3 space-y-2">{group.fields.map(field => <div key={field.key}><dt className="font-mono">{field.key}</dt><dd className="mt-0.5">{field.apply_scope || '以本组说明为准'}</dd></div>)}</dl></details>
        </details>;
      })}
    </fieldset></form>}
    <section className="account-panel"><div className="mb-4 flex items-center justify-between gap-3"><h3 className="font-semibold text-gray-900">最近配置变更</h3><button type="button" className="text-xs text-blue-700 underline" disabled={auditLoading} onClick={() => { setAuditLoading(true); setAuditRevision(value => value + 1); }}>{auditLoading ? '读取中…' : '刷新记录'}</button></div><p className="mb-3 text-xs text-gray-500">最近 50 次变更，仅记录字段名与操作者，不记录密钥内容。</p>
      {auditError && <p role="alert" className="mb-3 text-sm text-red-700">{auditError}</p>}
      {!audit.length ? <p className="text-sm text-gray-500">{auditLoading ? '正在读取变更记录…' : auditError ? '暂时无法读取变更记录。' : '暂无配置变更记录。'}</p> : <div className="overflow-x-auto"><table className="w-full text-left text-xs"><thead className="border-b border-gray-200 text-gray-500"><tr><th className="py-2 pr-4">时间（上海）</th><th className="pr-4">操作者</th><th className="pr-4">变更字段</th><th>版本</th></tr></thead><tbody>{audit.map(item => <tr key={item.id} className="border-b border-gray-100"><td className="whitespace-nowrap py-3 pr-4">{dateTime(item.at)}</td><td className="pr-4">{item.username || `用户 ${item.user_id}`}</td><td className="max-w-xl break-all py-3 pr-4 font-mono leading-5">{item.changed_keys.join('、')}{item.secret_keys.length > 0 && <span className="mt-1 block font-sans text-amber-700">含密钥变更：{item.secret_keys.join('、')}</span>}</td><td title={item.revision} className="font-mono text-gray-400">{item.revision.slice(0, 12)}</td></tr>)}</tbody></table></div>}
    </section>
  </div>;
}

function SettingField({ field, value, clearing, error, onChange, onClear }: { field: SystemSettingField; value: string | boolean; clearing: boolean; error?: string; onChange: (value: string | boolean) => void; onClear: (enabled: boolean) => void }) {
  const id = `system-setting-${field.key.replace(/[^a-zA-Z0-9-]/g, '-')}`;
  const readOnly = field.read_only || field.source === 'environment';
  return <div className="min-w-0"><div className="mb-2 flex flex-wrap items-center justify-between gap-2"><label htmlFor={id} title={field.key} className="text-sm font-medium text-gray-800">{field.label}{field.unit && <span className="ml-1 font-normal text-gray-400">（{field.unit}）</span>}</label><span className="rounded bg-gray-100 px-2 py-0.5 text-[10px] text-gray-500">{sourceLabels[field.source]}{readOnly ? ' · 只读' : ''}</span></div>
    {field.type === 'boolean' ? <label className="flex items-center gap-2 text-sm text-gray-600"><input id={id} type="checkbox" checked={value === true} disabled={readOnly} onChange={event => onChange(event.target.checked)} />{value === true ? '已启用' : '已关闭'}</label> : field.type === 'enum' ? <select id={id} value={String(value)} disabled={readOnly} onChange={event => onChange(event.target.value)} className={inputClass}>{!(field.options || []).some(option => String(option.value) === String(value)) && <option value={String(value)}>{String(value) || '未设置'}</option>}{field.options?.map(option => <option key={String(option.value)} value={String(option.value)}>{option.label}</option>)}</select> : <input id={id} type={field.type === 'secret' ? 'password' : field.type === 'integer' || field.type === 'number' ? 'number' : 'text'} value={String(value)} disabled={readOnly || clearing} min={field.min} max={field.max} step={field.type === 'integer' ? 1 : 'any'} autoComplete={field.type === 'secret' ? 'new-password' : 'off'} onChange={event => onChange(event.target.value)} className={inputClass} placeholder={field.type === 'secret' ? field.configured ? '已配置，留空保留现有值' : '未配置，输入新密钥' : undefined} aria-invalid={Boolean(error)} aria-describedby={field.description ? `${id}-description` : undefined} />}
    {field.description && <p id={`${id}-description`} className="mt-2 text-xs leading-5 text-gray-500">{field.description}</p>}
    {(field.type === 'number' || field.type === 'integer') && <p className="mt-1 text-xs text-gray-500">{field.min != null && `最小 ${field.min}`}{field.min != null && field.max != null && ' · '}{field.max != null && `最大 ${field.max}`}{field.unit === '比例' && ' · 以小数填写，0.10 表示 10%'}{field.unit === 'bp' && ' · 1 bp = 0.01%'}</p>}
    {readOnly && field.source === 'environment' && <p className="mt-1 text-xs text-amber-700">由环境变量管理，请在服务运行环境中修改。</p>}
    {field.type === 'secret' && <div className="mt-2 text-xs text-gray-500"><p>{field.configured ? '服务器已配置密钥，内容不回传。' : '服务器尚未配置此密钥。'}空输入不作修改。</p>{field.configured && !readOnly && <label className="mt-2 flex items-center gap-2 text-red-700"><input type="checkbox" checked={clearing} onChange={event => onClear(event.target.checked)} />保存时清除此密钥</label>}</div>}
    {error && <p role="alert" className="mt-2 text-xs text-red-700">{error}</p>}
  </div>;
}
