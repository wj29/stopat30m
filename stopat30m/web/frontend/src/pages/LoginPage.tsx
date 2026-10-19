import axios from 'axios';
import { useEffect, useRef, useState, type FormEvent } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { bootstrapAdmin, getBootstrapStatus, type BootstrapStatus } from '../api/auth';
import { useAuth } from '../contexts/AuthContext';
import { errorMessage } from '../utils/format';

const inputClass = 'w-full rounded-lg border border-gray-300 px-3 py-2.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500 disabled:bg-gray-50';

export default function LoginPage() {
  const { login, settings } = useAuth();
  const navigate = useNavigate();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirmation, setConfirmation] = useState('');
  const [bootstrap, setBootstrap] = useState<BootstrapStatus | null>(null);
  const [checking, setChecking] = useState(true);
  const [checkError, setCheckError] = useState('');
  const [revision, setRevision] = useState(0);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const submitLock = useRef(false);
  const mounted = useRef(false);

  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    getBootstrapStatus(controller.signal).then(value => {
      if (active) { setBootstrap(value); setCheckError(''); setChecking(false); }
    }).catch(reason => {
      if (active) { setBootstrap(null); setCheckError(errorMessage(reason, '无法确认首次设置状态')); setChecking(false); }
    });
    return () => { active = false; controller.abort(); };
  }, [revision]);

  const creating = Boolean(bootstrap?.required && bootstrap.allowed);
  function recheck() { setChecking(true); setCheckError(''); setError(''); setRevision(value => value + 1); }
  async function handleSubmit(event: FormEvent) {
    event.preventDefault();
    if (submitLock.current || checking || !bootstrap || (bootstrap.required && !bootstrap.allowed)) return;
    if (creating && (password.length < 10 || password.length > 128)) { setError('管理员密码须为 10–128 个字符。'); return; }
    if (creating && password !== confirmation) { setError('两次输入的密码不一致。'); return; }
    submitLock.current = true;
    setError('');
    setLoading(true);
    let created = false;
    try {
      const account = creating ? username.trim() : username;
      if (creating) {
        await bootstrapAdmin(account, password);
        created = true;
        if (!mounted.current) return;
        setUsername(account);
        setConfirmation('');
        setBootstrap({ required: false, allowed: false, reason: null });
      }
      await login(account, password);
      if (mounted.current) { setPassword(''); setConfirmation(''); navigate('/', { replace: true }); }
    } catch (reason) {
      if (!mounted.current) return;
      setError(created ? `管理员已创建，但登录未完成：${errorMessage(reason)}。请用刚创建的账号登录。` : errorMessage(reason, creating ? '首次设置失败' : '登录失败'));
      if (created) setPassword('');
      if (creating && !created && axios.isAxiosError(reason) && (!reason.response || reason.response.status === 409 || reason.response.status >= 500)) { setPassword(''); setConfirmation(''); setChecking(true); setRevision(value => value + 1); }
    } finally { submitLock.current = false; if (mounted.current) setLoading(false); }
  }

  return <div className="flex min-h-screen items-center justify-center bg-gray-50 px-4 py-8"><div className="w-full max-w-sm rounded-2xl border border-gray-200 bg-white p-8 shadow-lg">
    <h1 className="mb-1 text-2xl font-bold text-gray-900">stopat30m</h1>
    <p className="mb-6 text-sm text-gray-500">{creating ? '首次设置 · 创建个人管理员' : '个人账户与量化研究工作台'}</p>
    {checking && <p role="status" className="py-4 text-sm text-gray-500">正在检查系统初始化状态…</p>}
    {!checking && checkError && <div className="space-y-3"><p role="alert" className="rounded-lg bg-red-50 p-3 text-sm text-red-700">无法确认系统状态：{checkError}</p><button type="button" onClick={recheck} className="w-full rounded-lg border border-gray-300 px-4 py-2 text-sm text-gray-700">重新检查</button></div>}
    {!checking && bootstrap?.required && !bootstrap.allowed && <div className="space-y-3"><p role="alert" className="rounded-lg bg-amber-50 p-3 text-sm leading-6 text-amber-900">系统尚未创建管理员。请从服务器本机访问此网页完成首次设置。{bootstrap.reason && <span className="mt-2 block">{bootstrap.reason}</span>}</p><button type="button" onClick={recheck} className="w-full rounded-lg border border-gray-300 px-4 py-2 text-sm text-gray-700">已完成设置，重新检查</button></div>}
    {!checking && bootstrap && (!bootstrap.required || bootstrap.allowed) && <>
      {creating && <p className="mb-4 rounded-lg bg-blue-50 p-3 text-xs leading-6 text-blue-800">这是此系统的第一个账号，将拥有管理员权限。请设置自己的用户名和密码；创建后进入工作台配置数据与服务。</p>}
      <form onSubmit={handleSubmit} className="space-y-4"><fieldset disabled={loading} className="space-y-4">
        <div><label htmlFor="login-username" className="mb-1 block text-sm font-medium text-gray-700">{creating ? '管理员用户名' : '用户名'}</label><input id="login-username" type="text" value={username} onChange={event => setUsername(event.target.value)} className={inputClass} required minLength={creating ? 2 : undefined} maxLength={creating ? 50 : undefined} autoComplete="username" autoFocus /></div>
        <div><label htmlFor="login-password" className="mb-1 block text-sm font-medium text-gray-700">{creating ? '设置密码（10–128 个字符）' : '密码'}</label><input id="login-password" type="password" value={password} onChange={event => setPassword(event.target.value)} className={inputClass} required minLength={creating ? 10 : undefined} maxLength={creating ? 128 : undefined} autoComplete={creating ? 'new-password' : 'current-password'} /></div>
        {creating && <div><label htmlFor="login-confirmation" className="mb-1 block text-sm font-medium text-gray-700">再次输入密码</label><input id="login-confirmation" type="password" value={confirmation} onChange={event => setConfirmation(event.target.value)} className={inputClass} required minLength={10} maxLength={128} autoComplete="new-password" /></div>}
        {error && <p role="alert" className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{error}</p>}
        <button type="submit" className="w-full rounded-lg bg-blue-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">{loading ? creating ? '正在创建并登录…' : '登录中…' : creating ? '创建管理员并进入工作台' : '登录'}</button>
      </fieldset></form>
      {!creating && settings.registration_enabled && <p className="mt-4 text-center text-xs text-gray-500">没有账号？<Link to="/register" className="ml-1 text-blue-600 hover:underline">使用邀请码注册</Link></p>}
    </>}
  </div></div>;
}
