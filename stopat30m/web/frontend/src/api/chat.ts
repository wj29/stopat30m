import api from './client';

export interface ChatSession {
  id: number;
  title: string;
  stock_code: string | null;
  stock_name: string | null;
  created_at: string;
  updated_at: string;
}

export interface ChatMessage {
  id: number;
  role: 'user' | 'assistant';
  content: string;
  tool_calls: unknown[] | null;
  tokens_used: number;
  model_used: string;
  created_at: string;
}

export interface ChatSSEEvent {
  type: 'thinking' | 'progress' | 'answer_chunk' | 'answer_done' | 'error' | 'done';
  message?: string;
  content?: string;
  model?: string;
  tokens_used?: number;
  tool_calls_count?: number;
  agent?: string;
  tool?: string;
  step?: number;
  success?: boolean;
  duration?: number;
}

export async function listSessions(): Promise<ChatSession[]> {
  const { data } = await api.get('/chat/sessions');
  return data;
}

export async function createSession(params: {
  title?: string;
  stock_code?: string;
  stock_name?: string;
}): Promise<ChatSession> {
  const { data } = await api.post('/chat/sessions', params);
  return data;
}

export async function updateSession(
  id: number,
  params: { title?: string; stock_code?: string; stock_name?: string },
): Promise<ChatSession> {
  const { data } = await api.patch(`/chat/sessions/${id}`, params);
  return data;
}

export async function deleteSession(id: number): Promise<void> {
  await api.delete(`/chat/sessions/${id}`);
}

export async function listMessages(sessionId: number): Promise<ChatMessage[]> {
  const { data } = await api.get(`/chat/sessions/${sessionId}/messages`);
  return data;
}

export async function sendMessageSSE(
  sessionId: number,
  content: string,
  onEvent: (evt: ChatSSEEvent) => void,
  options?: { stock_code?: string; stock_name?: string },
): Promise<void> {
  const token = localStorage.getItem('access_token');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const resp = await fetch(`/api/v1/chat/sessions/${sessionId}/send`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ content, ...options }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }

  const reader = resp.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data: ')) continue;
      try {
        const evt: ChatSSEEvent = JSON.parse(trimmed.slice(6));
        onEvent(evt);
      } catch {
        // skip malformed SSE
      }
    }
  }
}
