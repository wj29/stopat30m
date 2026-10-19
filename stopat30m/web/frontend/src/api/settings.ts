import api from './client';

export type SystemSettingValue = string | number | boolean | null;
export interface SystemSettingField {
  key: string;
  label: string;
  type: 'boolean' | 'integer' | 'number' | 'string' | 'secret' | 'enum';
  value: SystemSettingValue;
  configured?: boolean;
  source: 'default' | 'local' | 'environment';
  read_only: boolean;
  min?: number;
  max?: number;
  options?: { value: string | number; label: string }[];
  description: string;
  unit?: string;
  apply_scope?: string;
}
export interface SystemSettingGroup {
  id: string;
  title: string;
  description: string;
  fields: SystemSettingField[];
}
export interface SystemSettingsResponse {
  revision: string;
  groups: SystemSettingGroup[];
  restart_required: string[];
  changed_keys?: string[];
}
export interface SystemSettingsAudit {
  id: number | string;
  at: string;
  user_id: number;
  username: string;
  changed_keys: string[];
  secret_keys: string[];
  revision: string;
}
export interface LlmDiagnostics {
  status: 'ok' | 'warning' | 'unavailable' | 'unsupported';
  message: string;
  checked_at: string;
  model: string;
  api_type: 'chat_completions' | 'responses';
  models: string[];
  model_listed: boolean | null;
  generation_tested: false;
}
export async function getSystemSettings(signal?: AbortSignal): Promise<SystemSettingsResponse> {
  return (await api.get('/system/settings', { signal })).data;
}
export async function saveSystemSettings(payload: { revision: string; values: Record<string, SystemSettingValue>; clear_secrets: string[] }): Promise<SystemSettingsResponse> {
  return (await api.put('/system/settings', payload)).data;
}
export async function getSystemSettingsAudit(signal?: AbortSignal): Promise<SystemSettingsAudit[]> {
  return (await api.get('/system/settings/audit', { signal })).data;
}
export async function getLlmDiagnostics(signal?: AbortSignal): Promise<LlmDiagnostics> {
  return (await api.get('/system/llm/diagnostics', { signal })).data;
}
