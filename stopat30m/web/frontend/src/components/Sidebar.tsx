import { NavLink, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';

interface NavItem { to: string; label: string; adminOnly?: boolean; multiUserOnly?: boolean }
const groups: { label: string; items: NavItem[] }[] = [
  { label: '我的工作台', items: [{ to: '/', label: '账户概览' }, { to: '/trading', label: '持仓与账本' }, { to: '/watchlist', label: '自选股' }] },
  { label: '分析与研究', items: [{ to: '/analysis', label: '个股分析' }, { to: '/market-review', label: '大盘复盘' }, { to: '/signals', label: '信号中心' }, { to: '/backtest', label: '回测验证' }, { to: '/model-lab', label: '模型实验室' }] },
  { label: '管理', items: [{ to: '/users', label: '用户管理', adminOnly: true, multiUserOnly: true }, { to: '/records', label: '记录管理', adminOnly: true }, { to: '/model-settings', label: '模型配置', adminOnly: true }, { to: '/operations', label: '运行与备份', adminOnly: true }, { to: '/settings', label: '系统设置' }] },
];

export default function Sidebar({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { user, logout, isAdmin, settings } = useAuth();
  const navigate = useNavigate();
  return (
    <>
      {open && <button className="fixed inset-0 z-30 bg-gray-950/30 md:hidden" aria-label="关闭菜单" onClick={onClose} />}
      <aside className={`fixed inset-y-0 left-0 z-40 flex w-56 shrink-0 flex-col border-r border-gray-200 bg-white transition-transform md:static md:translate-x-0 ${open ? 'translate-x-0' : '-translate-x-full'}`}>
        <div className="flex h-16 items-center justify-between border-b border-gray-100 px-5">
          <div><p className="font-bold tracking-tight text-blue-700">stopat30m</p><p className="text-xs text-gray-400">个人交易工作台</p></div>
          <button className="px-2 py-1 text-gray-500 md:hidden" onClick={onClose} aria-label="关闭菜单">×</button>
        </div>
        <nav className="flex-1 space-y-5 overflow-y-auto p-3" aria-label="主导航">
          {groups.map(group => <div key={group.label}>
            <p className="mb-2 px-3 text-[11px] font-semibold tracking-wider text-gray-400">{group.label}</p>
            <div className="space-y-1">{group.items.filter(item => (!item.adminOnly || isAdmin) && (!item.multiUserOnly || !settings.single_user)).map(item =>
              <NavLink key={item.to} to={item.to} end={item.to === '/'} onClick={onClose} className={({ isActive }) => `block rounded-lg px-3 py-2 text-sm font-medium transition-colors ${isActive ? 'bg-blue-50 text-blue-700' : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'}`}>{item.label}</NavLink>
            )}</div>
          </div>)}
        </nav>
        <div className="border-t border-gray-100 p-4">
          <p className="truncate text-sm font-semibold text-gray-800">{user?.username || '我的账户'}</p>
          <p className="mt-1 text-xs text-gray-400">{settings.single_user ? '个人模式 · 上海时间' : isAdmin ? '管理员' : '普通用户'}</p>
          <button onClick={() => { logout(); navigate('/login', { replace: true }); }} className="mt-3 text-xs text-gray-500 hover:text-gray-900">退出登录</button>
        </div>
      </aside>
    </>
  );
}
