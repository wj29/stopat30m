import { useEffect, useState, type FormEvent } from 'react';
import {
  adminResetPassword,
  changePassword,
  createInvite,
  listInvites,
  listUsers,
  updateUser,
  type InviteCodeInfo,
  type UserInfo,
} from '../api/auth';
import { useAuth } from '../contexts/AuthContext';

export default function UsersPage() {
  const { isAdmin } = useAuth();

  return (
    <div>
      <h1 className="mb-2 text-2xl font-bold text-gray-900">用户管理</h1>
      <p className="mb-6 text-sm text-gray-500">修改密码{isAdmin ? '、管理用户账号与邀请码' : ''}</p>

      <div className="space-y-6">
        <ChangePasswordSection />

        {isAdmin && (
          <>
            <UserManagementSection />
            <InviteCodeSection />
          </>
        )}
      </div>
    </div>
  );
}


function ChangePasswordSection() {
  const [oldPw, setOldPw] = useState('');
  const [newPw, setNewPw] = useState('');
  const [confirmPw, setConfirmPw] = useState('');
  const [msg, setMsg] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setMsg('');
    setError('');
    if (newPw !== confirmPw) {
      setError('两次密码输入不一致');
      return;
    }
    setLoading(true);
    try {
      await changePassword(oldPw, newPw);
      setMsg('密码已更新');
      setOldPw('');
      setNewPw('');
      setConfirmPw('');
    } catch (err: any) {
      setError(err.response?.data?.detail || '修改失败');
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-gray-900">修改密码</h2>
      <form onSubmit={handleSubmit} className="max-w-sm space-y-3">
        <input
          type="password"
          placeholder="当前密码"
          value={oldPw}
          onChange={(e) => setOldPw(e.target.value)}
          className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
          required
        />
        <input
          type="password"
          placeholder="新密码（至少 6 位）"
          value={newPw}
          onChange={(e) => setNewPw(e.target.value)}
          className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
          required
          minLength={6}
        />
        <input
          type="password"
          placeholder="确认新密码"
          value={confirmPw}
          onChange={(e) => setConfirmPw(e.target.value)}
          className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
          required
        />
        {error && <p className="text-sm text-red-600">{error}</p>}
        {msg && <p className="text-sm text-green-600">{msg}</p>}
        <button
          type="submit"
          disabled={loading}
          className="rounded-lg bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? '提交中...' : '修改密码'}
        </button>
      </form>
    </section>
  );
}


function UserManagementSection() {
  const { user: currentUser } = useAuth();
  const [users, setUsers] = useState<UserInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [resetTarget, setResetTarget] = useState<UserInfo | null>(null);
  const [resetPw, setResetPw] = useState('');
  const [resetMsg, setResetMsg] = useState('');
  const [resetErr, setResetErr] = useState('');

  const load = () => {
    setLoading(true);
    listUsers()
      .then(setUsers)
      .catch(() => {})
      .finally(() => setLoading(false));
  };

  useEffect(load, []);

  const toggleActive = async (u: UserInfo) => {
    await updateUser(u.id, { is_active: !u.is_active });
    load();
  };

  const toggleRole = async (u: UserInfo) => {
    const newRole = u.role === 'admin' ? 'user' : 'admin';
    await updateUser(u.id, { role: newRole });
    load();
  };

  const handleResetPassword = async (e: FormEvent) => {
    e.preventDefault();
    if (!resetTarget) return;
    setResetMsg('');
    setResetErr('');
    try {
      await adminResetPassword(resetTarget.id, resetPw);
      setResetMsg(`已重置 ${resetTarget.username} 的密码`);
      setResetPw('');
      setResetTarget(null);
    } catch (err: any) {
      setResetErr(err.response?.data?.detail || '重置失败');
    }
  };

  return (
    <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-lg font-semibold text-gray-900">用户列表</h2>

      {resetMsg && (
        <div className="mb-3 rounded-lg border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">{resetMsg}</div>
      )}

      {resetTarget && (
        <form onSubmit={handleResetPassword} className="mb-4 flex items-end gap-2 rounded-lg border border-blue-200 bg-blue-50 p-3">
          <div className="flex-1">
            <label className="mb-1 block text-xs text-blue-700">
              重置 <strong>{resetTarget.username}</strong> 的密码
            </label>
            <input
              type="password"
              placeholder="新密码（至少 6 位）"
              value={resetPw}
              onChange={(e) => setResetPw(e.target.value)}
              className="w-full rounded border border-blue-300 px-2 py-1.5 text-sm"
              required
              minLength={6}
              autoFocus
            />
            {resetErr && <p className="mt-1 text-xs text-red-600">{resetErr}</p>}
          </div>
          <button type="submit" className="rounded bg-blue-600 px-3 py-1.5 text-xs text-white hover:bg-blue-700">确认</button>
          <button type="button" onClick={() => { setResetTarget(null); setResetPw(''); setResetErr(''); }} className="rounded bg-gray-200 px-3 py-1.5 text-xs text-gray-700 hover:bg-gray-300">取消</button>
        </form>
      )}

      {loading ? (
        <p className="text-sm text-gray-500">加载中...</p>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-gray-500">
              <th className="pb-2">ID</th>
              <th className="pb-2">用户名</th>
              <th className="pb-2">角色</th>
              <th className="pb-2">状态</th>
              <th className="pb-2">最后登录</th>
              <th className="pb-2">操作</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.id} className="border-b border-gray-100">
                <td className="py-2">{u.id}</td>
                <td className="py-2 font-medium">{u.username}</td>
                <td className="py-2">
                  <span className={`rounded px-2 py-0.5 text-xs font-medium ${u.role === 'admin' ? 'bg-purple-100 text-purple-700' : 'bg-gray-100 text-gray-600'}`}>
                    {u.role}
                  </span>
                </td>
                <td className="py-2">
                  <span className={u.is_active ? 'text-green-600' : 'text-red-600'}>
                    {u.is_active ? '正常' : '已禁用'}
                  </span>
                </td>
                <td className="py-2 text-gray-500">{u.last_login?.slice(0, 16).replace('T', ' ') || '—'}</td>
                <td className="space-x-2 py-2">
                  <button onClick={() => toggleRole(u)} className="text-xs text-blue-600 hover:underline">
                    {u.role === 'admin' ? '降为user' : '升为admin'}
                  </button>
                  <button onClick={() => toggleActive(u)} className="text-xs text-amber-600 hover:underline">
                    {u.is_active ? '禁用' : '启用'}
                  </button>
                  {u.id !== currentUser?.id && (
                    <button onClick={() => { setResetTarget(u); setResetPw(''); setResetMsg(''); setResetErr(''); }} className="text-xs text-purple-600 hover:underline">
                      重置密码
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}


function InviteCodeSection() {
  const [invites, setInvites] = useState<InviteCodeInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [newCode, setNewCode] = useState('');

  const load = () => {
    setLoading(true);
    listInvites()
      .then(setInvites)
      .catch(() => {})
      .finally(() => setLoading(false));
  };

  useEffect(load, []);

  const handleCreate = async () => {
    setCreating(true);
    try {
      const res = await createInvite(7);
      setNewCode(res.code);
      load();
    } catch {
      /* handled globally */
    } finally {
      setCreating(false);
    }
  };

  return (
    <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">邀请码</h2>
        <button
          onClick={handleCreate}
          disabled={creating}
          className="rounded-lg bg-blue-600 px-3 py-1.5 text-xs text-white hover:bg-blue-700 disabled:opacity-50"
        >
          {creating ? '生成中...' : '生成邀请码'}
        </button>
      </div>

      {newCode && (
        <div className="mb-4 rounded-lg border border-green-200 bg-green-50 px-4 py-3">
          <p className="text-sm text-green-800">
            新邀请码：<code className="rounded bg-green-100 px-2 py-0.5 font-mono font-bold">{newCode}</code>
          </p>
        </div>
      )}

      {loading ? (
        <p className="text-sm text-gray-500">加载中...</p>
      ) : invites.length === 0 ? (
        <p className="text-sm text-gray-500">暂无邀请码</p>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-gray-500">
              <th className="pb-2">邀请码</th>
              <th className="pb-2">过期时间</th>
              <th className="pb-2">状态</th>
            </tr>
          </thead>
          <tbody>
            {invites.map((inv) => (
              <tr key={inv.id} className="border-b border-gray-100">
                <td className="py-2 font-mono text-xs">{inv.code}</td>
                <td className="py-2 text-gray-500">{inv.expires_at.slice(0, 16).replace('T', ' ')}</td>
                <td className="py-2">
                  {inv.used_by ? (
                    <span className="text-gray-400">已使用</span>
                  ) : new Date(inv.expires_at) < new Date() ? (
                    <span className="text-red-500">已过期</span>
                  ) : (
                    <span className="text-green-600">可用</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
