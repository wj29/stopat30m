import api from './client';

export interface ReadinessCheck {
  id: string;
  label: string;
  status: 'ok' | 'pending';
  message: string;
  to: string;
}
export interface OperationsStatus {
  checked_at: string;
  uptime_seconds: number;
  checks: ReadinessCheck[];
  external_services_checked: boolean;
}
export interface DatabaseBackup {
  id: string;
  created_at: string;
  archive_bytes: number | null;
  validation_status: 'valid' | 'invalid';
  validation_error: string | null;
  database_sha256?: string;
  archive_sha256?: string;
}
export async function getOperations(signal?: AbortSignal): Promise<OperationsStatus> {
  return (await api.get('/system/operations', { signal })).data;
}
export async function listBackups(signal?: AbortSignal): Promise<DatabaseBackup[]> {
  return (await api.get('/system/backups', { signal })).data;
}
export async function createBackup(): Promise<DatabaseBackup> {
  return (await api.post('/system/backups')).data;
}
export async function downloadBackup(id: string): Promise<void> {
  const response = await api.get<Blob>(`/system/backups/${encodeURIComponent(id)}/download`, { responseType: 'blob' });
  const url = URL.createObjectURL(response.data);
  try {
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = `${id}.zip`;
    anchor.click();
  } finally { setTimeout(() => URL.revokeObjectURL(url), 1000); }
}
