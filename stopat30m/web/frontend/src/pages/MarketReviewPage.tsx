import { useEffect, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { triggerMarketReview, getMarketReviewStatus, subscribeMarketReviewStream, type ReviewJobStatus, type ReviewStep } from '../api/market';
import { errorMessage } from '../utils/format';

function StepTimeline({ steps, running, failed }: { steps: ReviewStep[]; running: boolean; failed: boolean }) {
  return <div className="space-y-1">{steps.map((step, index) => <div key={index} className="flex items-center gap-2 text-sm">
    {step.done ? <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-emerald-100 text-xs text-emerald-700">✓</span>
      : running ? <span className="flex h-5 w-5 shrink-0 items-center justify-center"><span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" /></span>
        : <span className={`flex h-5 w-5 shrink-0 items-center justify-center rounded-full text-xs ${failed ? 'bg-red-100 text-red-700' : 'bg-gray-100 text-gray-500'}`}>{failed ? '×' : '—'}</span>}
    <span className={step.done ? 'text-gray-700' : running ? 'font-medium text-blue-700' : failed ? 'text-red-700' : 'text-gray-500'}>{step.message}</span>
    {step.done && step.duration_ms > 0 && <span className="ml-auto tabular-nums text-xs text-gray-400">{(step.duration_ms / 1000).toFixed(1)}s</span>}
  </div>)}
    {running && steps.length > 0 && steps.every(step => step.done) && <div className="flex items-center gap-2 text-sm text-gray-400"><span className="flex h-5 w-5 shrink-0 items-center justify-center"><span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-gray-300 border-t-transparent" /></span><span>处理中…</span></div>}
  </div>;
}

export default function MarketReviewPage() {
  const [job, setJob] = useState<ReviewJobStatus | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [checking, setChecking] = useState(true);
  const [revision, setRevision] = useState(0);
  const [error, setError] = useState('');
  const [statusError, setStatusError] = useState('');
  const [connectionError, setConnectionError] = useState('');
  const mounted = useRef(false);
  const triggerLock = useRef(false);
  const isRunning = job?.status === 'running';
  const isDone = job?.status === 'done';
  const isFailed = job?.status === 'failed';

  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    getMarketReviewStatus(controller.signal).then(value => {
      if (active) { setJob(value); setStatusError(''); setConnectionError(''); setChecking(false); }
    }).catch(reason => { if (active) { setStatusError(errorMessage(reason, '复盘状态读取失败')); setChecking(false); } });
    return () => { active = false; controller.abort(); };
  }, [revision]);

  useEffect(() => {
    if (!isRunning || checking || triggering || statusError) return;
    let active = true;
    const stop = subscribeMarketReviewStream(value => {
      if (active) { setJob(value); setConnectionError(''); }
    }, reason => { if (active) setConnectionError(reason.message); });
    return () => { active = false; stop(); };
  }, [isRunning, checking, triggering, statusError]);

  function refreshStatus() {
    if (triggerLock.current) return;
    setChecking(true); setRevision(value => value + 1);
  }
  async function onTrigger() {
    if (triggerLock.current || checking || !job || isRunning || statusError) return;
    triggerLock.current = true; setTriggering(true); setError('');
    try {
      const response = await triggerMarketReview();
      if (!mounted.current) return;
      setJob(response); setStatusError(''); setConnectionError('');
      if (!response.triggered) setError(response.message);
      // A newly accepted background thread may not yet have published running.
      if (response.triggered && response.status === 'idle') { setChecking(true); setRevision(value => value + 1); }
    } catch (reason) {
      if (!mounted.current) return;
      setError(errorMessage(reason, '触发复盘失败'));
      // A lost response does not establish that the POST did not start a job.
      setChecking(true); setRevision(value => value + 1);
    } finally { triggerLock.current = false; if (mounted.current) setTriggering(false); }
  }

  return <div>
    <h1 className="mb-2 text-2xl font-bold text-gray-900">大盘复盘</h1>
    <p className="mb-6 text-sm text-gray-500">获取 A 股指数、板块数据、新闻与大模型复盘报告。触发后可离开页面，回来查看任务状态和结果。</p>
    <div className="mb-6 flex flex-wrap items-center gap-3">
      <button type="button" onClick={() => { void onTrigger(); }} disabled={triggering || checking || isRunning || !job || Boolean(statusError)} className="rounded-lg bg-blue-600 px-6 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">{triggering ? '请求中…' : isRunning ? '任务执行中…' : '触发复盘'}</button>
      <button type="button" onClick={refreshStatus} disabled={checking || triggering} className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm text-gray-600 disabled:opacity-50">{checking ? '读取状态中…' : connectionError ? '重新读取状态并连接' : '刷新状态'}</button>
      {isRunning && <span className="text-sm text-blue-600">{connectionError || statusError ? '保留最后状态，重新读取后确认当前进度' : '任务在后台执行，可离开此页面'}</span>}
      {isDone && job.elapsed_sec > 0 && <span className="text-xs text-gray-400">耗时 {job.elapsed_sec.toFixed(1)}s</span>}
    </div>
    {error && <p role="alert" className="mb-5 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">{error}</p>}
    {statusError && <p role="alert" className="mb-5 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">无法确认复盘状态：{statusError}。请点击“刷新状态”重试，核对前不会重复触发任务。</p>}
    {connectionError && <p role="alert" className="mb-5 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm leading-6 text-amber-900">实时进度连接失败：{connectionError} 这不代表后台任务失败。请点击“重新读取状态并连接”核对结果。</p>}
    {!job && checking && <p role="status" className="account-panel text-sm text-gray-500">正在读取复盘状态…</p>}

    {job && (isRunning || isDone || isFailed) && <section className={`mb-6 rounded-xl border p-5 shadow-sm ${isDone ? 'border-emerald-200 bg-emerald-50' : isFailed ? 'border-red-200 bg-red-50' : 'border-gray-200 bg-white'}`}>
      <div className="mb-3 flex items-center justify-between"><h2 className="text-sm font-semibold text-gray-900">{isDone ? '分析完成' : isFailed ? '分析失败' : '分析进度'}</h2>{job.elapsed_sec > 0 && <span className="tabular-nums text-xs text-gray-400">{job.elapsed_sec.toFixed(1)}s</span>}</div>
      {job.steps.length > 0 ? <StepTimeline steps={job.steps} running={isRunning && !checking && !connectionError && !statusError} failed={isFailed} /> : <p className="text-sm text-gray-500">{isFailed ? '任务在生成进度步骤前失败。' : isDone ? '任务未保存步骤记录。' : checking || connectionError || statusError ? '尚未确认步骤进度，请重新读取状态。' : '任务已启动，等待首个进度步骤…'}</p>}
      {isFailed && <p role="alert" className="mt-3 whitespace-pre-wrap break-words text-sm text-red-700">{job.error || '后台任务失败，未返回具体原因。请检查服务日志或系统配置后再重试。'}</p>}
    </section>}

    {isDone && job.report && <section className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm"><div className="prose prose-sm max-w-none text-gray-800"><ReactMarkdown>{job.report}</ReactMarkdown></div></section>}
    {isDone && !job.report && <p className="account-panel text-sm text-gray-500">任务已结束，但没有返回报告正文。请核对后台执行记录。</p>}
    {job?.status === 'idle' && !statusError && <div className="rounded-xl border border-dashed border-gray-200 bg-gray-50 p-8 text-center text-sm text-gray-500"><p>尚无运行中的复盘，点击“触发复盘”开始分析。</p></div>}
  </div>;
}
