import { useEffect, useState } from 'react';
import { getConfigSummary, getDataStatus, listModels, type ConfigSummary, type DataStatus, type ModelFile } from '../api/system';
import { useAuth } from '../contexts/AuthContext';

export default function SettingsPage() {
  const { user: currentUser, isAdmin } = useAuth();
  const [dataStatus, setDataStatus] = useState<DataStatus | null>(null);
  const [cfg, setCfg] = useState<ConfigSummary | null>(null);
  const [models, setModels] = useState<ModelFile[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setErr(null);
    setLoading(true);
    const promises: Promise<any>[] = [];
    if (isAdmin) {
      promises.push(
        getDataStatus().catch(() => null),
        getConfigSummary().catch(() => null),
        listModels().catch(() => []),
      );
    } else {
      promises.push(Promise.resolve(null), Promise.resolve(null), Promise.resolve([]));
    }
    Promise.all(promises)
      .then(([d, c, m]) => {
        setDataStatus(d);
        setCfg(c);
        setModels(m ?? []);
      })
      .catch((e: unknown) => setErr(e instanceof Error ? e.message : '加载失败'))
      .finally(() => setLoading(false));
  }, [isAdmin]);

  return (
    <div>
      <h1 className="mb-2 text-2xl font-bold text-gray-900">系统设置</h1>
      <p className="mb-6 text-sm text-gray-500">
        当前用户：<strong>{currentUser?.username}</strong>（{isAdmin ? '管理员' : '普通用户'}）
      </p>

      {err && (
        <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">{err}</div>
      )}

      <div className="space-y-6">
        {isAdmin && !loading && dataStatus && (
          <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
            <h2 className="mb-4 text-lg font-semibold text-gray-900">数据层</h2>
            <dl className="grid gap-2 text-sm sm:grid-cols-2">
              <div>
                <dt className="text-gray-500">数据目录</dt>
                <dd className="font-mono text-gray-900">{dataStatus.data_dir}</dd>
              </div>
              <div>
                <dt className="text-gray-500">Qlib 数据</dt>
                <dd className={dataStatus.data_available ? 'text-green-700' : 'text-amber-700'}>
                  {dataStatus.data_available ? '已就绪' : '未初始化'}
                </dd>
              </div>
              {dataStatus.trusted_until != null && dataStatus.trusted_until !== '' && (
                <div>
                  <dt className="text-gray-500">可信截止日</dt>
                  <dd>{dataStatus.trusted_until}</dd>
                </div>
              )}
              {dataStatus.stock_count != null && (
                <div>
                  <dt className="text-gray-500">元数据股票数</dt>
                  <dd>{dataStatus.stock_count}</dd>
                </div>
              )}
            </dl>
          </section>
        )}

        {isAdmin && !loading && cfg && (
          <>
            <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
              <h2 className="mb-4 text-lg font-semibold text-gray-900">模型与信号</h2>
              <dl className="grid gap-2 text-sm sm:grid-cols-2">
                <div>
                  <dt className="text-gray-500">Qlib provider</dt>
                  <dd className="break-all font-mono text-xs text-gray-800">{cfg.qlib_provider_uri || '—'}</dd>
                </div>
                <div>
                  <dt className="text-gray-500">股票池</dt>
                  <dd>{cfg.universe}</dd>
                </div>
                <div>
                  <dt className="text-gray-500">模型类型</dt>
                  <dd>{cfg.model_type}</dd>
                </div>
                <div>
                  <dt className="text-gray-500">信号方法</dt>
                  <dd>{cfg.signal_method} (top_k={cfg.signal_top_k})</dd>
                </div>
              </dl>
            </section>

            <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
              <h2 className="mb-4 text-lg font-semibold text-gray-900">LLM</h2>
              <div className="space-y-3 text-sm">
                <p>
                  <span className="text-gray-500">启用：</span>
                  {cfg.llm_enabled ? '是' : '否'}
                  {cfg.llm_model && <> · 模型 <span className="font-mono">{cfg.llm_model}</span></>}
                </p>
                <ul className="grid gap-1 sm:grid-cols-2">
                  {Object.entries(cfg.llm_keys_configured).map(([k, v]) => (
                    <li key={k} className="flex justify-between rounded bg-gray-50 px-3 py-1.5 font-mono text-xs">
                      <span>{k}</span>
                      <span className={v ? 'text-green-700' : 'text-gray-400'}>{v ? '已配' : '未配'}</span>
                    </li>
                  ))}
                </ul>
              </div>
            </section>

            {models.length > 0 && (
              <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
                <h2 className="mb-4 text-lg font-semibold text-gray-900">已训练模型</h2>
                <ul className="space-y-1 text-sm">
                  {models.map((m) => (
                    <li key={m.name} className="flex justify-between border-b border-gray-100 py-2">
                      <span className="font-mono">{m.name}</span>
                      <span className="text-gray-500">{m.size_mb} MB</span>
                    </li>
                  ))}
                </ul>
              </section>
            )}
          </>
        )}
      </div>
    </div>
  );
}
