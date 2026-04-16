import { useCallback, useEffect, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import {
  type ChatMessage,
  type ChatSession,
  type ChatSSEEvent,
  createSession,
  deleteSession,
  listMessages,
  listSessions,
  sendMessageSSE,
} from '../api/chat';

export default function ChatPage() {
  const [sessions, setSessions] = useState<ChatSession[]>([]);
  const [activeId, setActiveId] = useState<number | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [sending, setSending] = useState(false);
  const [streamText, setStreamText] = useState('');
  const [statusText, setStatusText] = useState('');
  const bottomRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = useCallback(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  useEffect(() => {
    listSessions().then(setSessions).catch(() => {});
  }, []);

  useEffect(() => {
    if (!activeId) {
      setMessages([]);
      return;
    }
    listMessages(activeId).then(setMessages).catch(() => {});
  }, [activeId]);

  useEffect(scrollToBottom, [messages, streamText, scrollToBottom]);

  const handleNewSession = async () => {
    const s = await createSession({ title: '新对话' });
    setSessions((prev) => [s, ...prev]);
    setActiveId(s.id);
  };

  const handleDeleteSession = async (id: number) => {
    await deleteSession(id);
    setSessions((prev) => prev.filter((s) => s.id !== id));
    if (activeId === id) {
      setActiveId(null);
      setMessages([]);
    }
  };

  const handleSend = async () => {
    const text = input.trim();
    if (!text || sending) return;
    if (!activeId) {
      const s = await createSession({ title: text.slice(0, 40) });
      setSessions((prev) => [s, ...prev]);
      setActiveId(s.id);
      await doSend(s.id, text);
    } else {
      await doSend(activeId, text);
    }
  };

  const doSend = async (sessionId: number, text: string) => {
    setInput('');
    setSending(true);
    setStreamText('');
    setStatusText('');

    const userMsg: ChatMessage = {
      id: Date.now(),
      role: 'user',
      content: text,
      tool_calls: null,
      tokens_used: 0,
      model_used: '',
      created_at: new Date().toISOString(),
    };
    setMessages((prev) => [...prev, userMsg]);

    try {
      let accumulated = '';
      let model = '';
      await sendMessageSSE(sessionId, text, (evt: ChatSSEEvent) => {
        switch (evt.type) {
          case 'thinking':
            setStatusText(evt.message || '正在思考...');
            break;
          case 'progress':
            setStatusText(evt.message || evt.tool || evt.agent || '');
            break;
          case 'answer_chunk':
            accumulated += evt.content || '';
            setStreamText(accumulated);
            break;
          case 'answer_done':
            model = evt.model || '';
            break;
          case 'error':
            accumulated = `错误: ${evt.message}`;
            setStreamText(accumulated);
            break;
        }
      });

      if (accumulated) {
        const assistantMsg: ChatMessage = {
          id: Date.now() + 1,
          role: 'assistant',
          content: accumulated,
          tool_calls: null,
          tokens_used: 0,
          model_used: model,
          created_at: new Date().toISOString(),
        };
        setMessages((prev) => [...prev, assistantMsg]);
      }
    } finally {
      setSending(false);
      setStreamText('');
      setStatusText('');
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const activeSession = sessions.find((s) => s.id === activeId);

  return (
    <div className="flex h-full -m-6">
      {/* Session list sidebar */}
      <div className="w-64 flex flex-col border-r border-gray-200 bg-white">
        <div className="flex items-center justify-between border-b border-gray-200 px-4 py-3">
          <h2 className="text-sm font-semibold text-gray-700">对话列表</h2>
          <button
            onClick={handleNewSession}
            className="rounded-md bg-blue-600 px-2.5 py-1 text-xs font-medium text-white hover:bg-blue-700"
          >
            新建
          </button>
        </div>
        <div className="flex-1 overflow-y-auto">
          {sessions.length === 0 && (
            <p className="p-4 text-xs text-gray-400">暂无对话</p>
          )}
          {sessions.map((s) => (
            <div
              key={s.id}
              onClick={() => setActiveId(s.id)}
              className={`group flex cursor-pointer items-center justify-between px-4 py-3 text-sm transition-colors ${
                s.id === activeId
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-gray-600 hover:bg-gray-50'
              }`}
            >
              <div className="min-w-0 flex-1">
                <p className="truncate font-medium">{s.title}</p>
                <p className="mt-0.5 text-xs text-gray-400">
                  {s.updated_at ? new Date(s.updated_at).toLocaleString('zh-CN') : ''}
                </p>
              </div>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  handleDeleteSession(s.id);
                }}
                className="ml-2 hidden rounded p-0.5 text-gray-400 hover:bg-red-50 hover:text-red-500 group-hover:block"
                title="删除"
              >
                <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* Chat area */}
      <div className="flex flex-1 flex-col">
        {/* Header */}
        <div className="flex items-center border-b border-gray-200 bg-white px-6 py-3">
          <h1 className="text-sm font-semibold text-gray-700">
            {activeSession ? activeSession.title : 'Agent 对话'}
          </h1>
          {activeSession?.stock_code && (
            <span className="ml-2 rounded bg-blue-50 px-2 py-0.5 text-xs text-blue-600">
              {activeSession.stock_code}
            </span>
          )}
        </div>

        {/* Messages */}
        <div className="flex-1 overflow-y-auto px-6 py-4">
          {!activeId && messages.length === 0 && (
            <div className="flex h-full flex-col items-center justify-center text-gray-400">
              <svg className="mb-3 h-12 w-12" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                  d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z"
                />
              </svg>
              <p className="text-sm">新建或选择一个对话开始</p>
              <p className="mt-1 text-xs">支持股票分析、行情查询、投资问答</p>
            </div>
          )}

          {messages.map((m) => (
            <div
              key={m.id}
              className={`mb-4 flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}
            >
              <div
                className={`max-w-[75%] rounded-2xl px-4 py-3 text-sm leading-relaxed ${
                  m.role === 'user'
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-800'
                }`}
              >
                {m.role === 'assistant' ? (
                  <div className="prose prose-sm max-w-none">
                    <ReactMarkdown>{m.content}</ReactMarkdown>
                  </div>
                ) : (
                  <span className="whitespace-pre-wrap">{m.content}</span>
                )}
              </div>
            </div>
          ))}

          {/* Streaming answer */}
          {sending && streamText && (
            <div className="mb-4 flex justify-start">
              <div className="max-w-[75%] rounded-2xl bg-gray-100 px-4 py-3 text-sm leading-relaxed text-gray-800">
                <div className="prose prose-sm max-w-none">
                  <ReactMarkdown>{streamText}</ReactMarkdown>
                </div>
              </div>
            </div>
          )}

          {/* Status indicator */}
          {sending && !streamText && (
            <div className="mb-4 flex justify-start">
              <div className="flex items-center gap-2 rounded-2xl bg-gray-50 px-4 py-3 text-sm text-gray-500">
                <svg className="h-4 w-4 animate-spin" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                <span>{statusText || '正在思考...'}</span>
              </div>
            </div>
          )}

          <div ref={bottomRef} />
        </div>

        {/* Input */}
        <div className="border-t border-gray-200 bg-white px-6 py-4">
          <div className="mx-auto flex max-w-3xl items-end gap-3">
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              disabled={sending}
              rows={1}
              placeholder="输入消息... (Enter 发送, Shift+Enter 换行)"
              className="flex-1 resize-none rounded-xl border border-gray-300 px-4 py-2.5 text-sm
                focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500
                disabled:bg-gray-50 disabled:text-gray-400"
              style={{ maxHeight: 120, minHeight: 40 }}
              onInput={(e) => {
                const t = e.target as HTMLTextAreaElement;
                t.style.height = 'auto';
                t.style.height = Math.min(t.scrollHeight, 120) + 'px';
              }}
            />
            <button
              onClick={handleSend}
              disabled={sending || !input.trim()}
              className="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-xl bg-blue-600 text-white
                transition-colors hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed"
            >
              <svg className="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
                <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" />
              </svg>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
