import { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { createBackup, downloadBackup, getOperations, listBackups, type DatabaseBackup, type OperationsStatus } from '../api/operations';
import { useAuth } from '../contexts/AuthContext';
import { dateTime, errorMessage, number } from '../utils/format';

export default function OperationsPage() {
  const { isAdmin } = useAuth();
  return isAdmin ? <AdminOperations /> : <div className="account-panel text-sm text-gray-600">运行检查与数据库备份仅向管理员开放。</div>;
}

function AdminOperations() {
  const [status, setStatus] = useState<OperationsStatus | null>(null);
  const [backups, setBackups] = useState<DatabaseBackup[]>([]);
  const [errors, setErrors] = useState<string[]>([]);
  const [backupError, setBackupError] = useState('');
  const [actionError, setActionError] = useState('');
  const [notice, setNotice] = useState('');
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [revision, setRevision] = useState(0);
  const lock = useRef(false);
  const mounted = useRef(false);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    const controller = new AbortController();
    Promise.allSettled([getOperations(controller.signal), listBackups(controller.signal)]).then(([s, b]) => {
      if (controller.signal.aborted) return;
      const messages: string[] = [];
      if (s.status === 'fulfilled') setStatus(s.value); else messages.push(`运行检查：${errorMessage(s.reason)}`);
      if (b.status === 'fulfilled') { setBackups(b.value); setBackupError(''); }
      else { const message = `备份列表：${errorMessage(b.reason)}`; messages.push(message); setBackupError(message); }
      setErrors(messages); setLoading(false);
    });
    return () => controller.abort();
  }, [revision]);
  async function action(id?: string) {
    if (lock.current) return;
    lock.current = true; setBusy(true); setActionError(''); setNotice('');
    try {
      if (id) await downloadBackup(id);
      else {
        const created = await createBackup();
        if (mounted.current) { setNotice(`备份已创建并校验：${created.id}`); setRevision(value => value + 1); }
      }
    } catch (error) { if (mounted.current) setActionError(errorMessage(error, '备份操作未完成，请核对列表后重试')); }
    finally { lock.current = false; if (mounted.current) setBusy(false); }
  }
  return <div className="mx-auto max-w-6xl space-y-6">
    <div className="flex flex-wrap items-center justify-between gap-3"><div><h1 className="text-2xl font-bold text-gray-900">运行与备份</h1><p className="mt-2 text-sm text-gray-500">核对个人工作台准备情况，保存可校验的本地数据库副本。</p></div><button className="secondary-button" disabled={loading || busy} onClick={() => { setLoading(true); setRevision(value => value + 1); }}>{loading ? '检查中…' : '重新检查'}</button></div>
    {errors.length > 0 && <div role="alert" className="rounded-lg bg-red-50 p-4 text-sm text-red-700">{errors.map(value => <p key={value}>{value}</p>)}<p>检查失败时，保留上次数据供参考，请重新核对。</p></div>}
    <section className="account-panel"><h2 className="font-semibold text-gray-900">工作台准备情况</h2>{status ? <><p className="mb-4 mt-2 text-xs text-gray-500">检查于 {dateTime(status.checked_at)} · 当前进程运行 {number(status.uptime_seconds / 60, 1)} 分钟。本页只检查本机状态，未验证外部行情或模型接口连通性。</p><div className="grid gap-3 md:grid-cols-2">{status.checks.map(check => <div key={check.id} className="rounded-xl border border-gray-200 p-4"><div className="flex items-center justify-between gap-3"><h3 className="text-sm font-medium text-gray-900">{check.label}</h3><span className={`rounded-full px-2 py-1 text-xs ${check.status === 'ok' ? 'bg-emerald-50 text-emerald-700' : 'bg-amber-50 text-amber-800'}`}>{check.status === 'ok' ? '已就绪' : '待完善'}</span></div><p className="my-3 text-sm leading-6 text-gray-600">{check.message}</p><Link className="text-xs text-blue-700" to={check.to}>前往查看 →</Link></div>)}</div></> : <p className="mt-4 text-sm text-gray-500">{loading ? '正在检查本机状态…' : '状态尚未确认。'}</p>}</section>
    <section className="account-panel space-y-4"><div className="flex flex-wrap items-center justify-between gap-3"><h2 className="font-semibold text-gray-900">数据库备份</h2><button className="primary-button" disabled={busy || loading} onClick={() => void action()}>{busy ? '正在处理…' : '创建一致性备份'}</button></div><p className="text-sm leading-6 text-gray-600">备份包含本地数据库中的用户、账本、估值、分析和聊天记录；配置文件、行情、模型及研究文件不随包导出。SQLite 备份接口会包含已提交的 WAL 数据，生成后校验文件哈希。</p><p className="text-xs leading-5 text-gray-500">下载的 ZIP 用于保存和离线恢复核对。当前不提供网页覆盖恢复，避免误换正在使用的账本。</p>
      {notice && <p role="status" className="rounded-lg bg-emerald-50 p-3 text-sm text-emerald-800">{notice}</p>}{actionError && <p role="alert" className="rounded-lg bg-red-50 p-3 text-sm text-red-700">{actionError}</p>}
      {backups.length ? <div className="overflow-x-auto"><table className="ledger-table"><thead><tr><th>创建时间</th><th>备份编号 / 校验</th><th>文件大小</th><th>操作</th></tr></thead><tbody>{backups.map(item => <tr key={item.id}><td>{dateTime(item.created_at)}</td><td><p className="break-all font-mono text-xs">{item.id}</p><p className={`mt-1 text-xs ${item.validation_status === 'valid' ? 'text-emerald-700' : 'text-red-700'}`}>{item.validation_status === 'valid' ? '校验通过' : item.validation_error || '校验失败'}</p>{item.archive_sha256 && <details className="mt-1 text-xs text-gray-500"><summary className="cursor-pointer">文件校验和</summary><p className="mt-2 break-all font-mono">{item.archive_sha256}</p></details>}</td><td>{item.archive_bytes == null ? '—' : `${number(item.archive_bytes / 1024, 1)} KB`}</td><td><button className="secondary-button" disabled={busy || item.validation_status !== 'valid'} onClick={() => void action(item.id)}>下载 ZIP</button></td></tr>)}</tbody></table></div> : <p className="rounded-lg bg-gray-50 p-5 text-sm text-gray-500">{loading ? '正在读取备份…' : backupError ? '暂不能确认备份列表，请重新检查。' : '尚无本地备份，首次创建后会显示时间与校验结果。'}</p>}
    </section>
  </div>;
}
