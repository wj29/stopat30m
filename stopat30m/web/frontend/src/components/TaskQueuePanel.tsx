import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useState } from 'react';
import { listTasks, submitTasks, type TaskInfo, type TaskStats } from '../api/tasks';

function tasksHaveActive(tasks: TaskInfo[] | undefined): boolean {
  if (!tasks?.length) return false;
  return tasks.some((t) => t.status === 'pending' || t.status === 'running');
}

const TASKS_QUERY_KEY = ['tasks', 'list'] as const;

function statusBadgeClass(status: TaskInfo['status']): string {
  switch (status) {
    case 'pending':
      return 'bg-gray-100 text-gray-700 border-gray-200';
    case 'running':
      return 'bg-blue-100 text-blue-800 border-blue-200';
    case 'completed':
      return 'bg-emerald-100 text-emerald-800 border-emerald-200';
    case 'failed':
      return 'bg-red-100 text-red-800 border-red-200';
    case 'cancelled':
      return 'bg-amber-100 text-amber-800 border-amber-200';
    default:
      return 'bg-gray-100 text-gray-600 border-gray-200';
  }
}

function statusLabel(status: TaskInfo['status']): string {
  const map: Record<TaskInfo['status'], string> = {
    pending: '等待中',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
    cancelled: '已取消',
  };
  return map[status] ?? status;
}

function StatsBar({ stats }: { stats: TaskStats | undefined }) {
  if (!stats) return null;
  const items: { label: string; value: number; color: string }[] = [
    { label: '等待', value: stats.pending, color: 'text-gray-600' },
    { label: '运行', value: stats.running, color: 'text-blue-600' },
    { label: '完成', value: stats.completed, color: 'text-emerald-600' },
    { label: '失败', value: stats.failed, color: 'text-red-600' },
  ];
  return (
    <div className="flex flex-wrap items-center gap-4 rounded-lg border border-gray-100 bg-gray-50 px-4 py-2 text-sm">
      {items.map((it) => (
        <span key={it.label} className="tabular-nums">
          <span className="text-gray-500">{it.label}</span>{' '}
          <strong className={it.color}>{it.value}</strong>
        </span>
      ))}
      <span className="text-gray-400">|</span>
      <span className="tabular-nums text-gray-600">
        合计 <strong>{stats.total}</strong>
      </span>
    </div>
  );
}

interface TaskQueuePanelProps {
  onTaskCompleted?: () => void;
  hideSubmit?: boolean;
}

export default function TaskQueuePanel({ onTaskCompleted, hideSubmit }: TaskQueuePanelProps) {
  const queryClient = useQueryClient();
  const [input, setInput] = useState('');
  const [sseConnected, setSseConnected] = useState(false);

  const { data, isLoading, isError, error } = useQuery({
    queryKey: TASKS_QUERY_KEY,
    queryFn: () => listTasks(50),
    refetchInterval: (query) => (tasksHaveActive(query.state.data?.tasks) ? 3000 : false),
  });

  const invalidate = useCallback(() => {
    void queryClient.invalidateQueries({ queryKey: TASKS_QUERY_KEY });
  }, [queryClient]);

  useEffect(() => {
    const token = localStorage.getItem('access_token') || '';
    const url = `${window.location.origin}/api/v1/tasks/stream?token=${encodeURIComponent(token)}`;
    const es = new EventSource(url);

    es.onopen = () => setSseConnected(true);
    es.onerror = () => setSseConnected(false);

    es.onmessage = (evt) => {
      invalidate();
      try {
        const payload = JSON.parse(evt.data);
        if (payload?.type === 'task_completed' && onTaskCompleted) {
          onTaskCompleted();
        }
      } catch { /* ignore parse errors */ }
    };

    return () => {
      es.close();
      setSseConnected(false);
    };
  }, [invalidate, onTaskCompleted]);

  const mutation = useMutation({
    mutationFn: async (codes: string[]) => submitTasks(codes),
    onSuccess: () => {
      setInput('');
      invalidate();
    },
  });

  function parseCodes(raw: string): string[] {
    return raw
      .split(/[,，\s]+/)
      .map((s) => s.trim())
      .filter(Boolean);
  }

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    const codes = parseCodes(input);
    if (codes.length === 0 || mutation.isPending) return;
    mutation.mutate(codes);
  }

  const tasks = data?.tasks ?? [];
  const stats = data?.stats;

  return (
    <details className="group mb-8 rounded-xl border border-gray-200 bg-white shadow-sm open:shadow-md">
      <summary className="cursor-pointer list-none px-6 py-4 [&::-webkit-details-marker]:hidden">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <span className="text-lg font-semibold text-gray-900">分析任务队列</span>
            <span
              className={`rounded-full px-2 py-0.5 text-xs font-medium ${
                sseConnected ? 'bg-emerald-100 text-emerald-800' : 'bg-gray-100 text-gray-500'
              }`}
            >
              {sseConnected ? '实时推送已连接' : '实时推送未连接'}
            </span>
          </div>
          <span className="text-sm text-blue-600 group-open:hidden">展开</span>
          <span className="hidden text-sm text-blue-600 group-open:inline">收起</span>
        </div>
        <p className="mt-1 text-sm text-gray-500">批量提交代码排队分析；列表每 3 秒自动刷新（有进行中任务时）。</p>
      </summary>

      <div className="border-t border-gray-100 px-6 pb-6">
        <div className="mb-4 mt-4">
          <StatsBar stats={stats} />
        </div>

        {!hideSubmit && (
          <>
            <form onSubmit={onSubmit} className="mb-6 flex flex-wrap gap-3">
              <input
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="股票代码，逗号分隔，如 600519, 000001"
                className="min-w-[240px] flex-1 rounded-lg border border-gray-300 px-4 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <button
                type="submit"
                disabled={!parseCodes(input).length || mutation.isPending}
                className="rounded-lg bg-blue-600 px-5 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
              >
                {mutation.isPending ? '提交中…' : '加入队列'}
              </button>
            </form>

            {mutation.isError && (
              <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-2 text-sm text-red-800">
                {mutation.error instanceof Error ? mutation.error.message : '提交失败'}
              </div>
            )}

            {mutation.isSuccess && mutation.data && (
              <div className="mb-4 rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-2 text-sm text-emerald-900">
                已接受 {mutation.data.accepted.length} 个任务
                {mutation.data.duplicates.length > 0 && (
                  <span className="ml-2 text-amber-800">（{mutation.data.duplicates.length} 个重复已跳过）</span>
                )}
              </div>
            )}
          </>
        )}

        {isError && (
          <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-2 text-sm text-red-800">
            {error instanceof Error ? error.message : '加载任务列表失败'}
          </div>
        )}

        {isLoading && !data ? (
          <p className="text-sm text-gray-500">加载中…</p>
        ) : tasks.length === 0 ? (
          <p className="text-sm text-gray-500">暂无任务，提交股票代码开始排队分析。</p>
        ) : (
          <ul className="space-y-3">
            {tasks.map((t) => (
              <li
                key={t.task_id}
                className="rounded-lg border border-gray-100 bg-gray-50/80 p-4"
              >
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div>
                    <span className="font-mono text-sm font-semibold text-gray-900">{t.stock_code}</span>
                    {t.stock_name && <span className="ml-2 text-sm text-gray-600">{t.stock_name}</span>}
                  </div>
                  <span
                    className={`inline-block rounded-full border px-2.5 py-0.5 text-xs font-medium ${statusBadgeClass(t.status)}`}
                  >
                    {statusLabel(t.status)}
                  </span>
                </div>
                <p className="mt-1 font-mono text-xs text-gray-400">{t.task_id}</p>
                {t.progress_message && (
                  <p className="mt-2 text-xs text-gray-600">{t.progress_message}</p>
                )}
                {t.status === 'running' && (
                  <div className="mt-3">
                    <div className="mb-1 flex justify-between text-xs text-gray-500">
                      <span>进度</span>
                      <span className="tabular-nums">{t.progress ?? 0}%</span>
                    </div>
                    <div className="h-2 w-full overflow-hidden rounded-full bg-blue-100">
                      <div
                        className="h-full rounded-full bg-blue-600 transition-all duration-300"
                        style={{ width: `${Math.min(100, Math.max(0, t.progress ?? 0))}%` }}
                      />
                    </div>
                  </div>
                )}
                {t.status === 'failed' && t.error_message && (
                  <p className="mt-2 text-xs text-red-700">{t.error_message}</p>
                )}
                <p className="mt-2 text-xs text-gray-400">
                  创建 {t.created_at}
                  {t.completed_at ? ` · 完成 ${t.completed_at}` : ''}
                </p>
              </li>
            ))}
          </ul>
        )}
      </div>
    </details>
  );
}
