import axios from 'axios';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import {
  getLlmDiagnostics, getSystemSettings, getSystemSettingsAudit, saveSystemSettings,
  type LlmDiagnostics, type SystemSettingField, type SystemSettingsAudit,
  type SystemSettingsResponse, type SystemSettingValue,
} from '../api/settings';
import { dateTime, errorMessage } from '../utils/format';

type ProviderId = 'openai' | 'anthropic' | 'deepseek' | 'gemini' | 'ollama' | 'aihubmix';
type ApiType = 'chat_completions' | 'responses';
interface Provider {
  id: ProviderId; label: string; prefix: string; key?: string; base?: string; address: string;
}
interface ProviderDraft { model: string; secret: string; clearSecret: boolean; base: string; apiType: ApiType }
type ProviderDrafts = Record<ProviderId, ProviderDraft>;
type Fields = Record<string, SystemSettingField>;
const providers: Provider[] = [
  { id: 'openai', label: 'OpenAI / 兼容服务', prefix: 'openai/', key: 'llm.openai_api_key', base: 'llm.openai_base_url', address: '留空使用 OpenAI 官方地址；兼容服务填写其 API 基础地址。' },
  { id: 'anthropic', label: 'Claude（Anthropic）', prefix: 'anthropic/', key: 'llm.anthropic_api_key', base: 'llm.anthropic_base_url', address: '留空使用 Anthropic 官方地址；兼容地址可填根路径或 /v1，不要填写完整 /v1/messages。' },
  { id: 'deepseek', label: 'DeepSeek', prefix: 'deepseek/', key: 'llm.deepseek_api_key', base: 'llm.deepseek_base_url', address: '留空使用 DeepSeek 官方地址。' },
  { id: 'gemini', label: 'Gemini（Google）', prefix: 'gemini/', key: 'llm.gemini_api_key', address: '使用 Google Gemini 官方接口。' },
  { id: 'ollama', label: 'Ollama（本地）', prefix: 'ollama/', base: 'llm.ollama_api_base', address: '留空使用 http://localhost:11434；无需 API 密钥。' },
  { id: 'aihubmix', label: 'AIHubMix', prefix: 'openai/', key: 'llm.aihubmix_api_key', address: '固定使用 https://aihubmix.com/v1，与 OpenAI 凭据分开保存。' },
];
const advancedKeys = ['llm.temperature', 'llm.max_tokens', 'llm.timeout', 'llm.num_retries', 'llm.integrity_retries'];
const inputClass = 'w-full rounded-lg border border-gray-300 bg-white px-3 py-2.5 text-sm focus:border-blue-500 focus:outline-none disabled:bg-gray-100 disabled:text-gray-500';
const secondaryClass = 'rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-600 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50';
const sourceLabels = { default: '默认值', local: '本机保存', environment: '环境变量 · 只读' };
const emptyDraft = (): ProviderDraft => ({ model: '', secret: '', clearSecret: false, base: '', apiType: 'chat_completions' });
function fieldMap(settings: SystemSettingsResponse | null): Fields { return Object.fromEntries((settings?.groups.flatMap(group => group.fields) || []).filter(field => field.key.startsWith('llm.')).map(field => [field.key, field])); }
function fieldString(fields: Fields, key: string): string { return typeof fields[key]?.value === 'string' ? fields[key].value as string : ''; }
function locked(field?: SystemSettingField): boolean { return Boolean(field?.read_only || field?.source === 'environment'); }
function providerInfo(id: ProviderId | null | ''): Provider | undefined { return providers.find(provider => provider.id === id); }

function currentProvider(fields: Fields): ProviderId | null {
  const explicit = fieldString(fields, 'llm.provider');
  if (providerInfo(explicit as ProviderId)) return explicit as ProviderId;
  if (explicit && explicit !== 'auto') return null;
  const model = fieldString(fields, 'llm.model');
  if (!model) return null;
  if (model.startsWith('aihubmix/')) return 'aihubmix';
  for (const provider of providers.filter(item => item.id !== 'aihubmix' && item.id !== 'openai')) {
    if (model.startsWith(provider.prefix) || (provider.id === 'ollama' && model.startsWith('ollama_chat/'))) return provider.id;
  }
  if (!model.startsWith('openai/') && model.includes('/')) return null;
  const openaiKey = fields['llm.openai_api_key'];
  const openaiBase = fields['llm.openai_base_url'];
  const explicitOpenai = openaiKey?.configured || fieldString(fields, 'llm.openai_base_url').trim()
    || openaiKey?.source === 'environment' || openaiBase?.source === 'environment';
  return !explicitOpenai && fields['llm.aihubmix_api_key']?.configured ? 'aihubmix' : 'openai';
}

function bareModel(model: string, provider: Provider): string {
  if (provider.id === 'aihubmix' && model.startsWith('aihubmix/')) return model.slice('aihubmix/'.length);
  if (provider.id === 'ollama' && model.startsWith('ollama_chat/')) return model.slice('ollama_chat/'.length);
  return model.startsWith(provider.prefix) ? model.slice(provider.prefix.length).replace(/^responses\//, '') : model;
}
function canonicalModel(raw: string, provider: Provider, savedModel: string): { value: string; error: string } {
  const name = raw.trim();
  if (!name) return { value: '', error: '请填写此服务商的模型名称。' };
  if (name.startsWith('aihubmix/')) {
    if (provider.id !== 'aihubmix') return { value: '', error: '这是 AIHubMix 模型标识，请核对所选服务商。' };
    const bare = name.slice('aihubmix/'.length);
    return bare ? { value: `openai/${bare}`, error: '' } : { value: '', error: '模型名称不能为空。' };
  }
  const known = providers.find(item => item.id !== 'aihubmix' && name.startsWith(item.prefix));
  if (known && known.prefix !== provider.prefix) return { value: '', error: '模型带有其他服务商前缀，请填写所选服务商的模型名称。' };
  if (name.startsWith('ollama_chat/')) return provider.id === 'ollama' ? { value: name, error: '' } : { value: '', error: '这是 Ollama 模型标识，请核对所选服务商。' };
  const bare = name.startsWith(provider.prefix) ? name.slice(provider.prefix.length) : name;
  const clean = provider.prefix === 'openai/' ? bare.replace(/^responses\//, '') : bare;
  if (!clean) return { value: '', error: '模型名称不能为空。' };
  const prefix = provider.id === 'ollama' && savedModel.startsWith('ollama_chat/') ? 'ollama_chat/' : provider.prefix;
  return { value: `${prefix}${clean}`, error: '' };
}

function initialDrafts(fields: Fields): ProviderDrafts {
  const active = currentProvider(fields);
  return Object.fromEntries(providers.map(provider => [provider.id, {
    ...emptyDraft(), model: active === provider.id ? bareModel(fieldString(fields, 'llm.model'), provider) : '',
    base: provider.base ? fieldString(fields, provider.base) : '',
    apiType: ['openai', 'aihubmix'].includes(provider.id) && fieldString(fields, 'llm.api_type') === 'responses' ? 'responses' : 'chat_completions',
  }])) as ProviderDrafts;
}
function configurationLabel(provider: Provider, fields: Fields): string {
  if (provider.key) return fields[provider.key]?.configured ? '已配置' : '未配置';
  return fieldString(fields, provider.base || '').trim() ? '已配置'
    : currentProvider(fields) === 'ollama' && fieldString(fields, 'llm.model').trim() ? '已配置 · 默认本机地址' : '未配置 · 默认本机地址';
}
function connectionChanges(provider: Provider, draft: ProviderDraft, fields: Fields) {
  const values: Record<string, SystemSettingValue> = {};
  const clearSecrets: string[] = [];
  const errors: string[] = [];
  if (provider.key && fields[provider.key] && !locked(fields[provider.key])) {
    if (draft.clearSecret && fields[provider.key].configured) clearSecrets.push(provider.key);
    else if (draft.secret) {
      if (!draft.secret.trim()) errors.push('密钥不能只包含空白；留空保留，清除请勾选。');
      else values[provider.key] = draft.secret;
    }
  }
  if (provider.base && fields[provider.base] && !locked(fields[provider.base]) && draft.base !== fieldString(fields, provider.base)) values[provider.base] = draft.base.trim();
  return { values, clearSecrets, errors };
}

export default function ModelSettingsPanel() {
  const [settings, setSettings] = useState<SystemSettingsResponse | null>(null);
  const [selected, setSelected] = useState<ProviderId | ''>('');
  const [drafts, setDrafts] = useState<ProviderDrafts>(() => Object.fromEntries(providers.map(provider => [provider.id, emptyDraft()])) as ProviderDrafts);
  const [baseline, setBaseline] = useState<ProviderDrafts>(() => Object.fromEntries(providers.map(provider => [provider.id, emptyDraft()])) as ProviderDrafts);
  const [advanced, setAdvanced] = useState<Record<string, string>>({});
  const [enabled, setEnabled] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [loadError, setLoadError] = useState('');
  const [saveError, setSaveError] = useState('');
  const [notice, setNotice] = useState('');
  const [conflict, setConflict] = useState(false);
  const [revision, setRevision] = useState(0);
  const [audit, setAudit] = useState<SystemSettingsAudit[]>([]);
  const [auditLoading, setAuditLoading] = useState(true);
  const [auditError, setAuditError] = useState('');
  const [auditRevision, setAuditRevision] = useState(0);
  const [diagnostic, setDiagnostic] = useState<LlmDiagnostics | null>(null);
  const [diagnosticError, setDiagnosticError] = useState('');
  const [diagnosticBusy, setDiagnosticBusy] = useState(false);
  const mounted = useRef(false);
  const saveLock = useRef(false);
  const diagnosticLock = useRef(false);
  const diagnosticSequence = useRef(0);
  const diagnosticController = useRef<AbortController | null>(null);

  useEffect(() => { mounted.current = true; return () => { mounted.current = false; diagnosticController.current?.abort(); }; }, []);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    getSystemSettings(controller.signal).then(response => {
      if (!active) return;
      const next = fieldMap(response);
      const initial = initialDrafts(next);
      setSettings(response); setDrafts(initial); setBaseline(initial); setSelected(currentProvider(next) || '');
      setEnabled(next['llm.enabled']?.value === true); setAdvanced({});
      setLoading(false); setLoadError(''); setConflict(false);
    }).catch(error => { if (active) { setLoadError(errorMessage(error, '模型设置读取失败')); setLoading(false); } });
    return () => { active = false; controller.abort(); };
  }, [revision]);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    getSystemSettingsAudit(controller.signal).then(value => { if (active) { setAudit(value.filter(item => item.changed_keys.some(key => key.startsWith('llm.')))); setAuditError(''); setAuditLoading(false); } }).catch(error => { if (active) { setAuditError(errorMessage(error, '配置记录读取失败')); setAuditLoading(false); } });
    return () => { active = false; controller.abort(); };
  }, [auditRevision]);

  const fields = fieldMap(settings);
  const activeProvider = currentProvider(fields);
  const activeInfo = providerInfo(activeProvider);
  const provider = providerInfo(selected);
  const draft = selected ? drafts[selected] : emptyDraft();
  const dirtyProviders = providers.filter(item => JSON.stringify(drafts[item.id]) !== JSON.stringify(baseline[item.id]));
  const advancedDirty = advancedKeys.some(key => Object.hasOwn(advanced, key) && advanced[key] !== String(fields[key]?.value ?? ''));
  const dirty = dirtyProviders.length > 0 || advancedDirty || (settings !== null && enabled !== (fields['llm.enabled']?.value === true));
  const connection = provider ? connectionChanges(provider, draft, fields) : { values: {}, clearSecrets: [], errors: [] as string[] };
  const model = provider ? canonicalModel(draft.model, provider, fieldString(fields, 'llm.model')) : { value: '', error: '' };
  const activationValues: Record<string, SystemSettingValue> = { ...connection.values };
  const activationErrors = [...connection.errors];
  const routeLocks: string[] = [];
  if (provider) {
    if (model.error) activationErrors.push(model.error);
    const targets: Record<string, SystemSettingValue> = { 'llm.provider': provider.id, 'llm.model': model.value, 'llm.api_type': ['openai', 'aihubmix'].includes(provider.id) ? draft.apiType : 'chat_completions', 'llm.enabled': enabled };
    for (const [key, value] of Object.entries(targets)) {
      if (!fields[key]) { routeLocks.push(`服务器尚未提供 ${key}，请重新加载后再启用。`); continue; }
      if (locked(fields[key]) && fields[key].value !== value) routeLocks.push(`${fields[key].label}由环境变量锁定，不能在网页切换。`);
      else if (!locked(fields[key]) && fields[key].value !== value) activationValues[key] = value;
    }
  }
  const advancedErrors: Record<string, string> = {};
  for (const key of advancedKeys) {
    const field = fields[key];
    if (!field || locked(field) || !Object.hasOwn(advanced, key)) continue;
    const raw = advanced[key];
    const value = raw.trim() === '' ? NaN : Number(raw);
    if (!Number.isFinite(value) || (field.type === 'integer' && !Number.isInteger(value))) advancedErrors[key] = '请输入有效数字';
    else if ((field.min != null && value < field.min) || (field.max != null && value > field.max)) advancedErrors[key] = `范围 ${field.min ?? '不限'}–${field.max ?? '不限'}`;
    else if (value !== field.value) activationValues[key] = value;
  }
  const blocked = loading || saving || Boolean(loadError) || conflict;
  const canConnect = Object.keys(connection.values).length + connection.clearSecrets.length > 0 && !connection.errors.length;
  const selectedKey = provider?.key ? fields[provider.key] : undefined;
  const keyAvailable = !provider?.key || (locked(selectedKey) ? selectedKey?.configured === true : !draft.clearSecret && (Boolean(draft.secret.trim()) || selectedKey?.configured === true));
  const missingRequiredKey = enabled && !keyAvailable;
  const canActivate = Boolean(provider) && !model.error && !missingRequiredKey && !activationErrors.length && !routeLocks.length && !Object.keys(advancedErrors).length && Object.keys(activationValues).length + connection.clearSecrets.length > 0;

  function clearDiagnostic() {
    ++diagnosticSequence.current; diagnosticController.current?.abort(); diagnosticController.current = null; diagnosticLock.current = false;
    setDiagnostic(null); setDiagnosticError(''); setDiagnosticBusy(false);
  }
  function editDraft(value: Partial<ProviderDraft>) {
    if (!selected) return;
    clearDiagnostic(); setDrafts(current => ({ ...current, [selected]: { ...current[selected], ...value } })); setNotice('');
  }
  function reload() {
    if (saveLock.current) return;
    clearDiagnostic(); setLoading(true); setLoadError(''); setSaveError(''); setNotice('');
    setRevision(value => value + 1); setAuditLoading(true); setAuditRevision(value => value + 1);
  }
  async function save(mode: 'connection' | 'activate') {
    if (saveLock.current || blocked || !settings || !provider || (mode === 'connection' ? !canConnect : !canActivate)) return;
    saveLock.current = true; setSaving(true); setSaveError(''); setNotice(''); clearDiagnostic();
    try {
      const response = await saveSystemSettings({ revision: settings.revision, values: mode === 'connection' ? connection.values : activationValues, clear_secrets: connection.clearSecrets });
      if (!mounted.current) return;
      const next = fieldMap(response);
      setSettings(response); setConflict(false);
      if (mode === 'activate') {
        const saved = initialDrafts(next)[provider.id];
        setDrafts(current => ({ ...current, [provider.id]: saved })); setBaseline(current => ({ ...current, [provider.id]: saved }));
        setAdvanced({}); setEnabled(next['llm.enabled']?.value === true); setSelected(currentProvider(next) || '');
        setNotice(`已保存并${next['llm.enabled']?.value === true ? '启用' : '停用'} ${provider.label} 配置。是否能够生成内容仍需服务商实际支持。`);
      } else {
        setDrafts(current => ({ ...current, [provider.id]: { ...current[provider.id], secret: '', clearSecret: false, base: provider.base ? fieldString(next, provider.base) : '' } }));
        setBaseline(current => ({ ...current, [provider.id]: { ...current[provider.id], secret: '', clearSecret: false, base: provider.base ? fieldString(next, provider.base) : '' } }));
        setNotice(`已保存 ${provider.label} 的连接配置，当前模型选择未改变。密钥输入已清空。`);
      }
      setAuditLoading(true); setAuditRevision(value => value + 1);
    } catch (error) {
      if (!mounted.current) return;
      const status = axios.isAxiosError(error) ? error.response?.status : undefined;
      if (status === 409 || (axios.isAxiosError(error) && (!error.response || (status != null && status >= 500)))) {
        setConflict(true); setSaveError(`${errorMessage(error)}。请重新加载最新配置后再修改，本次草稿仍保留，不会强行覆盖。`);
      } else setSaveError(errorMessage(error, '模型配置保存失败，草稿已保留'));
    } finally { saveLock.current = false; if (mounted.current) setSaving(false); }
  }
  async function diagnose() {
    if (diagnosticLock.current || blocked || dirty || !['openai', 'aihubmix'].includes(activeProvider || '')) return;
    diagnosticLock.current = true;
    const sequence = ++diagnosticSequence.current;
    const controller = new AbortController(); diagnosticController.current = controller;
    setDiagnosticBusy(true); setDiagnosticError(''); setDiagnostic(null);
    const valid = () => mounted.current && !controller.signal.aborted && sequence === diagnosticSequence.current;
    try { const response = await getLlmDiagnostics(controller.signal); if (valid()) setDiagnostic(response); }
    catch (error) { if (valid()) setDiagnosticError(errorMessage(error, '模型列表检查失败')); }
    finally { if (valid()) { diagnosticLock.current = false; diagnosticController.current = null; setDiagnosticBusy(false); } }
  }
  function submit(event: FormEvent) { event.preventDefault(); void save('activate'); }

  return <div className="space-y-5">
    <div className="flex flex-wrap items-start justify-between gap-3"><div><h1 className="text-2xl font-bold text-gray-900">模型配置</h1><p className="mt-2 text-sm text-gray-500">选择服务商后配置自己的模型。切换下拉只改变编辑对象，保存后才生效。</p></div><button type="button" disabled={loading || saving} onClick={reload} className={secondaryClass}>{loading ? '读取中…' : dirty ? '重载并放弃草稿' : '重新加载'}</button></div>
    {loadError && <p role="alert" className="rounded-xl bg-red-50 p-4 text-sm text-red-700">{loadError}。请重新加载。</p>}
    {!settings && loading && <p role="status" className="account-panel text-sm text-gray-500">正在读取已保存的模型配置…</p>}
    {settings && <div className="grid items-start gap-5 xl:grid-cols-[minmax(0,1.6fr)_minmax(280px,1fr)]">
      <div className="space-y-5">
        <section className="account-panel"><label htmlFor="model-provider" className="mb-2 block text-sm font-semibold text-gray-900">选择要配置的服务商</label><select id="model-provider" className={inputClass} value={selected} disabled={blocked} onChange={event => { clearDiagnostic(); setSelected(event.target.value as ProviderId); setNotice(''); }}><option value="" disabled>请选择服务商</option>{providers.map(item => <option key={item.id} value={item.id}>{item.label} · {configurationLabel(item, fields)}{item.id === activeProvider ? fields['llm.enabled']?.value === true ? ' · 当前启用' : ' · 当前保存' : ''}</option>)}</select><p className="mt-2 text-xs text-gray-500">“已配置”表示保存了所需配置，不表示已验证密钥、额度或模型可用性。</p></section>
        {!activeProvider && <p className="rounded-xl bg-amber-50 p-4 text-sm leading-6 text-amber-900">当前模型标识无法识别到已支持的服务商。页面没有改写它；请选择服务商并手动填写模型，再明确保存。</p>}
        {provider && <form onSubmit={submit} className="account-panel space-y-5"><fieldset disabled={blocked} className="space-y-5">
          <div className="flex flex-wrap items-center justify-between gap-2"><h2 className="font-semibold text-gray-900">正在编辑：{provider.label}</h2><span className="rounded-full bg-gray-100 px-2.5 py-1 text-xs text-gray-500">已保存连接：{configurationLabel(provider, fields)}</span></div>
          <label className="flex items-center justify-between gap-3 rounded-lg bg-gray-50 p-3 text-sm"><span>保存后启用 AI 分析{locked(fields['llm.enabled']) && <span className="ml-2 text-xs text-gray-400">环境变量管理</span>}</span><input type="checkbox" checked={enabled} disabled={locked(fields['llm.enabled'])} onChange={event => { clearDiagnostic(); setEnabled(event.target.checked); setNotice(''); }} /></label>
          <div><label htmlFor="model-name" className="mb-2 block text-sm font-medium text-gray-700">模型名称</label><input id="model-name" className={inputClass} value={draft.model} disabled={locked(fields['llm.model'])} placeholder="填写此服务商提供的模型标识" autoComplete="off" onChange={event => editDraft({ model: event.target.value })} /><p className="mt-2 text-xs leading-5 text-gray-500">可填写不带服务商前缀的名称。保存时使用 <span className="font-mono">{model.value || `${provider.prefix}模型名称`}</span>{locked(fields['llm.model']) ? '；模型由环境变量锁定。' : '。'}</p>{model.error && draft.model && <p className="mt-1 text-xs text-red-700">{model.error}</p>}</div>
          {provider.key && fields[provider.key] && <ProviderSecret field={fields[provider.key]} value={draft.secret} clearing={draft.clearSecret} onChange={secret => editDraft({ secret })} onClear={clearSecret => editDraft({ clearSecret, secret: '' })} />}
          {provider.base && fields[provider.base] && <div><label htmlFor="model-base-url" className="mb-2 block text-sm font-medium text-gray-700">服务地址 <span className="text-xs font-normal text-gray-400">{sourceLabels[fields[provider.base].source]}</span></label><input id="model-base-url" className={inputClass} type="url" autoComplete="off" value={draft.base} disabled={locked(fields[provider.base])} placeholder="留空使用默认地址" onChange={event => editDraft({ base: event.target.value })} /><p className="mt-2 text-xs leading-5 text-gray-500">{provider.address}{provider.id === 'openai' && ' 只填基础地址，不带 /chat/completions 或 /responses。'}</p></div>}
          {(!provider.base || !fields[provider.base]) && <p className="rounded-lg bg-gray-50 p-3 text-xs leading-6 text-gray-500">{provider.address}{provider.base && !fields[provider.base] && ' 当前服务器未提供地址编辑字段。'}</p>}
          {['openai', 'aihubmix'].includes(provider.id) ? <div><label htmlFor="model-api-type" className="mb-2 block text-sm font-medium text-gray-700">接口协议</label><select id="model-api-type" className={inputClass} value={draft.apiType} disabled={locked(fields['llm.api_type'])} onChange={event => editDraft({ apiType: event.target.value as ApiType })}>{(fields['llm.api_type']?.options || [{ value: 'chat_completions', label: 'Chat Completions（/chat/completions）' }, { value: 'responses', label: 'Responses（/responses）' }]).map(option => <option key={String(option.value)} value={String(option.value)}>{option.label}</option>)}</select><p className="mt-2 text-xs text-gray-500">按服务商支持的接口选择，模型名和地址不会自动决定协议。</p></div> : null}
          <details className="border-t border-gray-100 pt-4"><summary className="cursor-pointer text-sm font-medium text-gray-600">高级参数</summary><div className="mt-4 grid gap-4 sm:grid-cols-2">{advancedKeys.filter(key => fields[key]).map(key => { const field = fields[key]; return <label key={key} className="min-w-0 text-sm"><span className="mb-1 block text-gray-600">{field.label}{field.unit && `（${field.unit}）`}</span><input className={inputClass} type="number" min={field.min} max={field.max} step={field.type === 'integer' ? 1 : 'any'} disabled={locked(field)} value={advanced[key] ?? String(field.value ?? '')} onChange={event => { clearDiagnostic(); setAdvanced(current => ({ ...current, [key]: event.target.value })); setNotice(''); }} /><span className="mt-1 block text-[10px] text-gray-400">{sourceLabels[field.source]}{field.min != null && ` · 最小 ${field.min}`}{field.max != null && ` · 最大 ${field.max}`}</span>{advancedErrors[key] && <span className="mt-1 block text-xs text-red-700">{advancedErrors[key]}</span>}</label>; })}</div></details>
          {routeLocks.length > 0 && <p role="status" className="rounded-lg bg-amber-50 p-3 text-xs leading-6 text-amber-900">{[...new Set(routeLocks)].join(' ')}仍可仅保存此服务商可修改的连接配置。</p>}
          {missingRequiredKey && <p className="rounded-lg bg-amber-50 p-3 text-xs leading-6 text-amber-900">启用此服务商前需配置 API 密钥。请填写密钥，或关闭 AI 后保存。</p>}
          {connection.errors.map(message => <p key={message} className="text-xs text-red-700">{message}</p>)}
          <div className="border-t border-gray-100 pt-4"><p className="mb-3 text-xs text-gray-500">{dirty ? '有未保存草稿。切换服务商会保留草稿，保存启用后以服务器结果为准。' : '切换服务商不会改变当前启用配置。'}{fieldString(fields, 'llm.provider') === 'auto' && provider.id === activeProvider && ' 保存并使用后，将明确记录此服务商。'}</p><div className="flex flex-wrap gap-3"><button type="submit" className="rounded-lg bg-blue-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-gray-300" disabled={!canActivate}>{saving ? '保存中…' : `保存并${enabled ? '使用' : '停用'} ${provider.label}`}</button><button type="button" className={secondaryClass} disabled={!canConnect} onClick={() => { void save('connection'); }}>仅保存连接配置</button></div>{!draft.model && <p className="mt-2 text-xs text-gray-400">填写模型名称后才能保存并使用；也可先单独保存密钥和地址。</p>}</div>
        </fieldset></form>}
        {saveError && <p role="alert" className="rounded-xl bg-red-50 p-4 text-sm leading-6 text-red-800">{saveError}</p>}
        {notice && <p role="status" className="rounded-xl bg-blue-50 p-4 text-sm leading-6 text-blue-800">{notice}</p>}
      </div>
      <aside className="space-y-5"><section className="account-panel space-y-4"><div className="flex items-center justify-between gap-3"><h2 className="font-semibold text-gray-900">当前已保存配置</h2><span className={`rounded-full px-2.5 py-1 text-xs ${fields['llm.enabled']?.value === true ? 'bg-blue-50 text-blue-700' : 'bg-gray-100 text-gray-500'}`}>{fields['llm.enabled']?.value === true ? 'AI 已启用' : 'AI 已停用'}</span></div><dl className="space-y-3 text-sm"><div><dt className="text-xs text-gray-400">服务商</dt><dd className="mt-1">{activeInfo?.label || '未识别，请人工核对'}</dd></div><div><dt className="text-xs text-gray-400">模型</dt><dd className="mt-1 break-all font-mono text-xs">{fieldString(fields, 'llm.model') || '未设置'}</dd></div><div><dt className="text-xs text-gray-400">协议</dt><dd className="mt-1">{['openai', 'aihubmix', 'deepseek'].includes(activeProvider || '') ? fieldString(fields, 'llm.api_type') === 'responses' ? 'Responses' : 'Chat Completions' : '服务商默认接口'}</dd></div>{activeInfo && <div><dt className="text-xs text-gray-400">连接配置</dt><dd className="mt-1">{configurationLabel(activeInfo, fields)}</dd></div>}</dl><p className="text-xs leading-6 text-gray-500">这里显示服务器已保存的配置。左侧草稿不会立即生效；已配置不代表已验证生成能力。</p>{settings.restart_required.some(key => key.startsWith('llm.')) && <p className="rounded-lg bg-amber-50 p-3 text-xs leading-6 text-amber-900">密钥或服务地址已变更，长驻实例需重启后使用新配置。</p>}{dirtyProviders.length > 0 && <p className="rounded-lg bg-gray-50 p-3 text-xs text-gray-500">保留草稿：{dirtyProviders.map(item => item.label).join('、')}</p>}</section>
        <section className="account-panel space-y-3"><h2 className="text-sm font-semibold text-gray-900">只读模型检查</h2><p className="text-xs leading-6 text-gray-500">仅检查当前已保存的 OpenAI 兼容配置，读取模型列表，不生成内容，也不会自动更换模型。</p><button type="button" className={`${secondaryClass} w-full`} disabled={blocked || dirty || diagnosticBusy || !['openai', 'aihubmix'].includes(activeProvider || '')} onClick={() => { void diagnose(); }}>{diagnosticBusy ? '正在读取模型列表…' : '检查已保存的模型配置'}</button>{dirty && <p className="text-xs text-amber-700">请先保存或重载未保存草稿。</p>}{activeProvider && !['openai', 'aihubmix'].includes(activeProvider) && <p className="text-xs text-gray-400">此检查暂不支持当前服务商，不代表其配置错误。</p>}{diagnosticError && <p role="alert" className="text-xs text-red-700">{diagnosticError}。可点击按钮重试。</p>}{diagnostic && <Diagnostic result={diagnostic} />}</section>
        <details className="account-panel text-xs text-gray-500"><summary className="cursor-pointer font-medium text-gray-700">最近模型配置变更</summary>{auditError && <p role="alert" className="mt-3 text-red-700">{auditError}</p>}{audit.length ? <div className="mt-3 max-h-80 space-y-3 overflow-auto">{audit.map(item => <div key={item.id} className="border-t border-gray-100 pt-3"><p>{dateTime(item.at)} · {item.username}</p><p className="mt-1 break-all font-mono leading-5">{item.changed_keys.filter(key => key.startsWith('llm.')).join('、')}</p>{item.secret_keys.length > 0 && <p className="mt-1">含密钥变更，内容不记录。</p>}</div>)}</div> : <p className="mt-3">{auditLoading ? '正在读取配置记录…' : auditError ? '变更记录暂不可确认。' : '暂无模型配置变更。'}</p>}</details>
      </aside>
    </div>}
  </div>;
}

function ProviderSecret({ field, value, clearing, onChange, onClear }: { field: SystemSettingField; value: string; clearing: boolean; onChange: (value: string) => void; onClear: (value: boolean) => void }) {
  return <div><label htmlFor="model-provider-secret" className="mb-2 block text-sm font-medium text-gray-700">API 密钥 <span className="text-xs font-normal text-gray-400">{sourceLabels[field.source]}</span></label><input id="model-provider-secret" className={inputClass} type="password" autoComplete="new-password" value={value} disabled={locked(field) || clearing} placeholder={field.configured ? '已配置，留空保留' : '未配置，输入你的密钥'} onChange={event => onChange(event.target.value)} /><p className="mt-2 text-xs text-gray-500">密钥不会从服务器回显，空输入保留现有值。</p>{field.configured && !locked(field) && <label className="mt-2 flex items-center gap-2 text-xs text-red-700"><input type="checkbox" checked={clearing} onChange={event => onClear(event.target.checked)} />保存时清除此服务商密钥</label>}</div>;
}

function Diagnostic({ result }: { result: LlmDiagnostics }) {
  return <div role="status" className={`rounded-lg p-3 text-xs leading-6 ${result.status === 'ok' ? 'bg-blue-50 text-blue-800' : 'bg-amber-50 text-amber-900'}`}><p className="whitespace-pre-wrap break-words">{result.message}</p><p className="mt-2 text-[10px]">{dateTime(result.checked_at)} · {result.api_type === 'responses' ? 'Responses' : 'Chat Completions'}</p><p className="break-all font-mono">{result.model}</p><p>{result.model_listed === true ? '当前模型在返回列表中。' : result.model_listed === false ? '当前模型不在返回列表中。' : '尚不能确认模型列表匹配情况。'}未进行生成测试。</p>{result.models.length > 0 && <details className="mt-2"><summary className="cursor-pointer">查看可复制的模型标识（{result.models.length}）</summary><textarea readOnly aria-label="返回的模型标识，只读可复制" className="mt-2 w-full rounded border border-gray-200 bg-white p-2 font-mono text-xs text-gray-700" rows={Math.min(6, Math.max(2, result.models.length))} value={result.models.join('\n')} onFocus={event => event.currentTarget.select()} /></details>}</div>;
}
