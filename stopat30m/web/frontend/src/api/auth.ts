import api from './client';

export interface TokenResponse {
  access_token: string;
  token_type: string;
  role: string;
  username: string;
}

export interface UserInfo {
  id: number;
  username: string;
  role: string;
  is_active: boolean;
  created_at: string;
  last_login: string | null;
}

export async function login(username: string, password: string): Promise<TokenResponse> {
  const { data } = await api.post<TokenResponse>('/auth/login', { username, password });
  return data;
}

export async function register(username: string, password: string, invite_code: string): Promise<TokenResponse> {
  const { data } = await api.post<TokenResponse>('/auth/register', { username, password, invite_code });
  return data;
}

export async function getMe(): Promise<UserInfo> {
  const { data } = await api.get<UserInfo>('/auth/me');
  return data;
}

export async function changePassword(old_password: string, new_password: string): Promise<void> {
  await api.post('/auth/change-password', { old_password, new_password });
}

// Admin APIs

export interface InviteCodeInfo {
  id: number;
  code: string;
  created_by: number;
  used_by: number | null;
  expires_at: string;
  used_at: string | null;
  created_at: string;
}

export async function createInvite(expire_days: number = 7): Promise<{ code: string; expires_at: string }> {
  const { data } = await api.post('/admin/invite', { expire_days });
  return data;
}

export async function listInvites(): Promise<InviteCodeInfo[]> {
  const { data } = await api.get<InviteCodeInfo[]>('/admin/invites');
  return data;
}

export async function listUsers(): Promise<UserInfo[]> {
  const { data } = await api.get<UserInfo[]>('/admin/users');
  return data;
}

export async function updateUser(userId: number, updates: { role?: string; is_active?: boolean }): Promise<void> {
  await api.put(`/admin/users/${userId}`, updates);
}

export async function adminResetPassword(userId: number, new_password: string): Promise<void> {
  await api.put(`/admin/users/${userId}/reset-password`, { new_password });
}

// Record management APIs

export interface AdminChatSession {
  id: number;
  user_id: number;
  username: string;
  title: string;
  stock_code: string | null;
  updated_at: string;
  created_at: string;
}

export interface AdminAnalysisRecord {
  id: number;
  code: string;
  name: string;
  user_id: number | null;
  username: string;
  analysis_date: string;
  signal_score: number;
  llm_operation_advice: string;
  data_source: string;
}

export interface AdminMarketReview {
  filename: string;
  size_bytes: number;
  created_at: string;
}

export interface AdminLogs {
  path: string;
  content: string;
  size_bytes: number;
  total_lines: number;
}

export async function adminListChatSessions(limit = 50, offset = 0): Promise<{ total: number; items: AdminChatSession[] }> {
  const { data } = await api.get('/admin/chat-sessions', { params: { limit, offset } });
  return data;
}

export async function adminBatchDeleteChatSessions(ids: number[]): Promise<{ deleted: number }> {
  const { data } = await api.post('/admin/chat-sessions/batch-delete', { ids });
  return data;
}

export async function adminListAnalysisHistory(limit = 50, offset = 0, code?: string): Promise<{ total: number; items: AdminAnalysisRecord[] }> {
  const { data } = await api.get('/admin/analysis-history', { params: { limit, offset, code: code || undefined } });
  return data;
}

export async function adminBatchDeleteAnalysis(ids: number[]): Promise<{ deleted: number }> {
  const { data } = await api.post('/admin/analysis-history/batch-delete', { ids });
  return data;
}

export async function adminListMarketReviews(): Promise<AdminMarketReview[]> {
  const { data } = await api.get('/admin/market-reviews');
  return data;
}

export async function adminBatchDeleteMarketReviews(filenames: string[]): Promise<{ deleted: number }> {
  const { data } = await api.post('/admin/market-reviews/batch-delete', { filenames });
  return data;
}

export async function adminGetLogs(lines = 200): Promise<AdminLogs> {
  const { data } = await api.get('/admin/logs', { params: { lines } });
  return data;
}

export async function adminClearLogs(): Promise<void> {
  await api.post('/admin/logs/clear');
}
