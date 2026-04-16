import { useCallback, useEffect, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import {
  triggerMarketReview,
  getMarketReviewStatus,
  subscribeMarketReviewStream,
  type ReviewJobStatus,
  type ReviewStep,
} from '../api/market';

function StepTimeline({ steps, running }: { steps: ReviewStep[]; running: boolean }) {
  return (
    <div className="space-y-1">
      {steps.map((s, i) => (
        <div key={i} className="flex items-center gap-2 text-sm">
          {s.done ? (
            <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-emerald-100 text-xs text-emerald-700">
              ✓
            </span>
          ) : (
            <span className="flex h-5 w-5 shrink-0 items-center justify-center">
              <span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
            </span>
          )}
          <span className={s.done ? 'text-gray-700' : 'font-medium text-blue-700'}>
            {s.message}
          </span>
          {s.done && s.duration_ms > 0 && (
            <span className="ml-auto tabular-nums text-xs text-gray-400">
              {(s.duration_ms / 1000).toFixed(1)}s
            </span>
          )}
        </div>
      ))}
      {running && steps.length > 0 && steps.every((s) => s.done) && (
        <div className="flex items-center gap-2 text-sm text-gray-400">
          <span className="flex h-5 w-5 shrink-0 items-center justify-center">
            <span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-gray-300 border-t-transparent" />
          </span>
          <span>处理中…</span>
        </div>
      )}
    </div>
  );
}

export default function MarketReviewPage() {
  const [job, setJob] = useState<ReviewJobStatus | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);

  const loadStatus = useCallback(async () => {
    try {
      const s = await getMarketReviewStatus();
      setJob(s);
      if (s.status === 'running') {
        startSSE();
      }
    } catch {
      // server not ready yet
    }
  }, []);

  useEffect(() => {
    loadStatus();
    return () => cleanupRef.current?.();
  }, [loadStatus]);

  function startSSE() {
    cleanupRef.current?.();
    cleanupRef.current = subscribeMarketReviewStream((status) => {
      setJob(status);
      if (status.status === 'done' || status.status === 'failed') {
        cleanupRef.current?.();
        cleanupRef.current = null;
      }
    });
  }

  async function onTrigger() {
    setTriggering(true);
    setError(null);
    try {
      const resp = await triggerMarketReview();
      setJob(resp);
      if (resp.triggered) {
        startSSE();
      } else {
        setError(resp.message);
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : '触发失败');
    } finally {
      setTriggering(false);
    }
  }

  const isRunning = job?.status === 'running';
  const isDone = job?.status === 'done';
  const isFailed = job?.status === 'failed';
  const hasReport = isDone && !!job.report;

  return (
    <div>
      <h1 className="mb-2 text-2xl font-bold text-gray-900">大盘复盘</h1>
      <p className="mb-6 text-sm text-gray-500">
        获取 A 股指数、板块数据 + 新闻搜索 + 大模型生成复盘报告。触发后可离开页面，回来查看结果。
      </p>

      <div className="mb-6 flex flex-wrap items-center gap-3">
        <button
          type="button"
          onClick={onTrigger}
          disabled={triggering || isRunning}
          className="rounded-lg bg-blue-600 px-6 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
        >
          {isRunning ? '正在执行中…' : triggering ? '请求中…' : '触发复盘'}
        </button>
        {isRunning && (
          <span className="text-sm text-blue-600">
            可离开此页面，任务会在后台继续运行
          </span>
        )}
        {isDone && job.elapsed_sec > 0 && (
          <span className="text-xs text-gray-400">
            耗时 {job.elapsed_sec.toFixed(1)}s
          </span>
        )}
      </div>

      {error && (
        <div className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
          {error}
        </div>
      )}

      {/* Progress steps */}
      {job && (isRunning || isDone || isFailed) && job.steps.length > 0 && (
        <div
          className={`mb-6 rounded-xl border p-5 shadow-sm ${
            isDone
              ? 'border-emerald-200 bg-emerald-50'
              : isFailed
                ? 'border-red-200 bg-red-50'
                : 'border-gray-200 bg-white'
          }`}
        >
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-900">
              {isDone ? '分析完成' : isFailed ? '分析失败' : '分析进度'}
            </h3>
            {job.elapsed_sec > 0 && (
              <span className="tabular-nums text-xs text-gray-400">
                {job.elapsed_sec.toFixed(1)}s
              </span>
            )}
          </div>
          <StepTimeline steps={job.steps} running={isRunning} />
          {isFailed && job.error && (
            <p className="mt-3 text-sm text-red-700">{job.error}</p>
          )}
        </div>
      )}

      {/* Report */}
      {hasReport && (
        <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
          <div className="prose prose-sm max-w-none text-gray-800">
            <ReactMarkdown>{job.report}</ReactMarkdown>
          </div>
        </div>
      )}

      {/* Empty state */}
      {(!job || job.status === 'idle') && (
        <div className="rounded-xl border border-dashed border-gray-200 bg-gray-50 p-8 text-center text-sm text-gray-500">
          <p>点击「触发复盘」开始大盘复盘分析。</p>
          <p className="mt-1">也可通过命令行执行：<code className="rounded bg-gray-100 px-1.5 py-0.5 font-mono text-xs">py main.py market-review</code></p>
        </div>
      )}
    </div>
  );
}
