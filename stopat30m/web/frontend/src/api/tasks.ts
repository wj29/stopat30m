import api from './client';

export interface TaskInfo {
  task_id: string;
  stock_code: string;
  stock_name?: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  progress?: number;
  progress_message?: string;
  error_message?: string;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  result?: unknown;
}

export interface TaskStats {
  pending: number;
  running: number;
  completed: number;
  failed: number;
  total: number;
}

function normalizeStatus(raw: string): TaskInfo['status'] {
  if (raw === 'processing') return 'running';
  if (raw === 'pending' || raw === 'running' || raw === 'completed' || raw === 'failed' || raw === 'cancelled') {
    return raw;
  }
  return 'pending';
}

function normalizeTask(raw: Record<string, unknown>): TaskInfo {
  return {
    task_id: String(raw.task_id ?? ''),
    stock_code: String(raw.stock_code ?? ''),
    stock_name: raw.stock_name != null ? String(raw.stock_name) : undefined,
    status: normalizeStatus(String(raw.status ?? 'pending')),
    progress: typeof raw.progress === 'number' ? raw.progress : undefined,
    progress_message:
      (raw.progress_message as string | undefined) ||
      (raw.message as string | undefined) ||
      undefined,
    error_message: (raw.error_message as string | undefined) || (raw.error as string | undefined) || undefined,
    created_at: String(raw.created_at ?? ''),
    started_at: raw.started_at != null ? String(raw.started_at) : undefined,
    completed_at: raw.completed_at != null ? String(raw.completed_at) : undefined,
    result: raw.result,
  };
}

function normalizeStats(raw: Record<string, unknown>): TaskStats {
  const pending = Number(raw.pending ?? 0);
  const running = Number(raw.running ?? raw.processing ?? 0);
  return {
    pending,
    running,
    completed: Number(raw.completed ?? 0),
    failed: Number(raw.failed ?? 0),
    total: Number(raw.total ?? 0),
  };
}

export async function submitTasks(codes: string[]): Promise<{ accepted: TaskInfo[]; duplicates: unknown[] }> {
  const { data } = await api.post<{ accepted: Record<string, unknown>[]; duplicates: unknown[] }>('/tasks', {
    codes,
  });
  return {
    accepted: (data.accepted ?? []).map((t) => normalizeTask(t)),
    duplicates: data.duplicates ?? [],
  };
}

export async function listTasks(limit = 50): Promise<{ tasks: TaskInfo[]; stats: TaskStats }> {
  const { data } = await api.get<{ tasks: Record<string, unknown>[]; stats: Record<string, unknown> }>('/tasks', {
    params: { limit },
  });
  return {
    tasks: (data.tasks ?? []).map((t) => normalizeTask(t)),
    stats: normalizeStats(data.stats ?? {}),
  };
}

export async function getTaskStatus(taskId: string): Promise<TaskInfo> {
  const { data } = await api.get<Record<string, unknown>>(`/tasks/${encodeURIComponent(taskId)}`);
  return normalizeTask(data);
}
