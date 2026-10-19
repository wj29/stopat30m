import ModelSettingsPanel from '../components/ModelSettingsPanel';
import { useAuth } from '../contexts/AuthContext';

export default function ModelSettingsPage() {
  const { isAdmin } = useAuth();
  return <div className="mx-auto max-w-6xl">
    {isAdmin ? <ModelSettingsPanel /> : <p className="account-panel text-sm text-gray-600">模型配置仅向管理员开放。</p>}
  </div>;
}
