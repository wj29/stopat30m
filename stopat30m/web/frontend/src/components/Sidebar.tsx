import { NavLink, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';

interface NavItem {
  to: string;
  label: string;
  icon: string;
  adminOnly?: boolean;
}

const NAV_ITEMS: NavItem[] = [
  { to: '/', label: '首页', icon: '📊' },
  { to: '/chat', label: 'Agent 对话', icon: '💬' },
  { to: '/analysis', label: '个股分析', icon: '🔍' },
  { to: '/watchlist', label: '自选股', icon: '⭐' },
  { to: '/market-review', label: '大盘复盘', icon: '🏛️' },
  { to: '/trading', label: '交易中心', icon: '💹' },
  { to: '/signals', label: '信号中心', icon: '📡' },
  { to: '/backtest', label: '回测中心', icon: '📈' },
  { to: '/model-lab', label: '模型实验室', icon: '🧪' },
  { to: '/users', label: '用户管理', icon: '👥', adminOnly: true },
  { to: '/records', label: '数据管理', icon: '🗂️', adminOnly: true },
  { to: '/settings', label: '系统设置', icon: '⚙️' },
];

export default function Sidebar() {
  const { user, logout, isAdmin } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/login', { replace: true });
  };

  return (
    <aside className="flex h-screen w-56 flex-col border-r border-gray-200 bg-gray-50">
      <div className="flex h-14 items-center gap-2 border-b border-gray-200 px-4">
        <span className="text-xl font-bold text-blue-600">stopat30m</span>
      </div>
      <nav className="flex-1 space-y-0.5 overflow-y-auto p-2">
        {NAV_ITEMS.filter((item) => !item.adminOnly || isAdmin).map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === '/'}
            className={({ isActive }) =>
              `flex items-center gap-2.5 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors ${
                isActive
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
              }`
            }
          >
            <span className="text-base">{item.icon}</span>
            {item.label}
          </NavLink>
        ))}
      </nav>

      <div className="border-t border-gray-200 p-3">
        {user && (
          <div className="mb-2 flex items-center gap-2">
            <div className="flex h-7 w-7 items-center justify-center rounded-full bg-blue-100 text-xs font-bold text-blue-700">
              {user.username[0].toUpperCase()}
            </div>
            <div className="min-w-0 flex-1">
              <p className="truncate text-sm font-medium text-gray-800">{user.username}</p>
              <p className="text-xs text-gray-400">
                {isAdmin ? 'admin' : 'user'}
              </p>
            </div>
          </div>
        )}
        <button
          onClick={handleLogout}
          className="w-full rounded-lg px-3 py-1.5 text-left text-xs text-gray-500 hover:bg-gray-100 hover:text-gray-700"
        >
          退出登录
        </button>
      </div>
    </aside>
  );
}
