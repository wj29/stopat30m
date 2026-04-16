import { useCallback, useEffect, useRef, useState } from 'react';
import {
  getAnalysisHistory,
  getAnalysisDetail,
  deleteAnalysis,
  batchDeleteAnalysis,
  triggerAnalysisStream,
  type AnalysisResult,
  type LLMDashboard,
  type Dashboard,
  type ProgressEvent,
  type OHLCVBar,
  type OHLCVEvent,
} from '../api/analysis';
import TaskQueuePanel from '../components/TaskQueuePanel';
import CandlestickChart from '../components/CandlestickChart';
import { Table, Modal, Button, Space, Tag, Spin, Message } from '@arco-design/web-react';
import type { ColumnProps } from '@arco-design/web-react/es/Table';

function fmtPct(n: number | null | undefined) {
  if (n == null || Number.isNaN(n)) return '—';
  return `${(n * 100).toFixed(1)}%`;
}

interface CompletedStep {
  step: number;
  message: string;
  durationMs: number;
}

interface ProgressState {
  total: number;
  completed: CompletedStep[];
  current: { step: number; message: string } | null;
  currentStartMs: number;
  startMs: number;
}

/* ========== Sub-components for the decision dashboard ========== */

function SignalBadge({ signal }: { signal?: string }) {
  if (!signal) return null;
  const color = signal.includes('买入')
    ? 'bg-emerald-100 text-emerald-800 border-emerald-300'
    : signal.includes('卖出')
      ? 'bg-red-100 text-red-800 border-red-300'
      : signal.includes('警告')
        ? 'bg-amber-100 text-amber-800 border-amber-300'
        : 'bg-yellow-100 text-yellow-800 border-yellow-300';
  return (
    <span className={`inline-block rounded-full border px-3 py-0.5 text-xs font-semibold ${color}`}>
      {signal}
    </span>
  );
}

function CoreConclusionCard({ dashboard }: { dashboard: Dashboard }) {
  const core = dashboard.core_conclusion;
  if (!core) return null;
  return (
    <div className="rounded-xl border border-indigo-200 bg-indigo-50 p-5">
      <div className="mb-2 flex flex-wrap items-center gap-3">
        <h3 className="text-sm font-semibold text-indigo-900">核心结论</h3>
        <SignalBadge signal={core.signal_type} />
        {core.time_sensitivity && (
          <span className="text-xs text-indigo-600">⏱ {core.time_sensitivity}</span>
        )}
      </div>
      {core.one_sentence && (
        <p className="mb-3 text-base font-medium text-indigo-900">{core.one_sentence}</p>
      )}
      {core.position_advice && (
        <div className="grid gap-3 sm:grid-cols-2">
          {core.position_advice.no_position && (
            <div className="rounded-lg bg-white/60 p-3">
              <p className="mb-1 text-xs font-medium text-indigo-600">空仓者</p>
              <p className="text-sm text-gray-800">{core.position_advice.no_position}</p>
            </div>
          )}
          {core.position_advice.has_position && (
            <div className="rounded-lg bg-white/60 p-3">
              <p className="mb-1 text-xs font-medium text-indigo-600">持仓者</p>
              <p className="text-sm text-gray-800">{core.position_advice.has_position}</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function DataPerspectiveGrid({ dashboard }: { dashboard: Dashboard }) {
  const dp = dashboard.data_perspective;
  if (!dp) return null;
  const ts = dp.trend_status;
  const pp = dp.price_position;
  const va = dp.volume_analysis;
  const cs = dp.chip_structure;

  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
      {ts && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">趋势</p>
          <p className="text-sm font-semibold">{ts.ma_alignment || '—'}</p>
          {ts.trend_score != null && (
            <p className="text-xs text-gray-500">强度 {ts.trend_score}/100</p>
          )}
        </div>
      )}
      {pp && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">价格位置</p>
          {pp.bias_ma5 != null && (
            <p className={`text-sm font-semibold ${Math.abs(pp.bias_ma5) > 5 ? 'text-red-600' : 'text-gray-900'}`}>
              乖离率(MA5) {pp.bias_ma5 > 0 ? '+' : ''}{pp.bias_ma5.toFixed(2)}%
              <span className="ml-1 text-xs font-normal">{pp.bias_status || ''}</span>
            </p>
          )}
          {pp.support_level != null && (
            <p className="text-xs text-gray-500">支撑 {pp.support_level} · 压力 {pp.resistance_level ?? '—'}</p>
          )}
        </div>
      )}
      {va && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">量能</p>
          <p className="text-sm font-semibold">{va.volume_status || '—'}</p>
          {va.volume_meaning && <p className="text-xs text-gray-500">{va.volume_meaning}</p>}
        </div>
      )}
      {cs && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">筹码</p>
          <p className="text-sm font-semibold">{cs.chip_health || '—'}</p>
          {cs.avg_cost != null && <p className="text-xs text-gray-500">平均成本 {cs.avg_cost}</p>}
        </div>
      )}
    </div>
  );
}

function BattlePlanSection({ dashboard }: { dashboard: Dashboard }) {
  const bp = dashboard.battle_plan;
  if (!bp) return null;
  const sp = bp.sniper_points;
  const ps = bp.position_strategy;

  return (
    <div className="rounded-xl border border-orange-200 bg-orange-50 p-5">
      <h3 className="mb-3 text-sm font-semibold text-orange-900">作战计划</h3>
      {sp && (
        <div className="mb-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
          {sp.ideal_buy && (
            <div className="rounded-lg bg-white p-3">
              <p className="text-xs text-emerald-600">理想入场</p>
              <p className="text-sm font-bold text-emerald-800">{sp.ideal_buy}</p>
            </div>
          )}
          {sp.secondary_buy && (
            <div className="rounded-lg bg-white p-3">
              <p className="text-xs text-blue-600">次优入场</p>
              <p className="text-sm font-bold text-blue-800">{sp.secondary_buy}</p>
            </div>
          )}
          {sp.stop_loss && (
            <div className="rounded-lg bg-white p-3">
              <p className="text-xs text-red-600">止损位</p>
              <p className="text-sm font-bold text-red-800">{sp.stop_loss}</p>
            </div>
          )}
          {sp.take_profit && (
            <div className="rounded-lg bg-white p-3">
              <p className="text-xs text-green-600">止盈位</p>
              <p className="text-sm font-bold text-green-800">{sp.take_profit}</p>
            </div>
          )}
          {sp.take_profit_2 && (
            <div className="rounded-lg bg-white p-3">
              <p className="text-xs text-green-600">第二止盈</p>
              <p className="text-sm font-bold text-green-800">{sp.take_profit_2}</p>
            </div>
          )}
        </div>
      )}
      {ps && (
        <div className="mb-3 text-sm text-gray-800">
          {ps.suggested_position && <p><strong>仓位：</strong>{ps.suggested_position}</p>}
          {ps.entry_plan && <p><strong>建仓：</strong>{ps.entry_plan}</p>}
          {ps.risk_control && <p><strong>风控：</strong>{ps.risk_control}</p>}
        </div>
      )}
      {bp.action_checklist && bp.action_checklist.length > 0 && (
        <div>
          <p className="mb-1 text-xs font-medium text-orange-700">检查清单</p>
          <ul className="space-y-0.5">
            {bp.action_checklist.map((item, i) => (
              <li key={i} className="text-sm text-gray-800">{item}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function IntelligenceSection({ dashboard }: { dashboard: Dashboard }) {
  const intel = dashboard.intelligence;
  if (!intel) return null;

  return (
    <div className="rounded-xl border border-blue-200 bg-blue-50 p-5">
      <h3 className="mb-3 text-sm font-semibold text-blue-900">情报</h3>
      {intel.latest_news && (
        <p className="mb-2 text-sm text-gray-800">{intel.latest_news}</p>
      )}
      <div className="grid gap-3 sm:grid-cols-2">
        {intel.risk_alerts && intel.risk_alerts.length > 0 && (
          <div>
            <p className="mb-1 text-xs font-medium text-red-700">风险警报</p>
            <ul className="list-inside list-disc text-sm text-red-800">
              {intel.risk_alerts.map((s, i) => <li key={i}>{s}</li>)}
            </ul>
          </div>
        )}
        {intel.positive_catalysts && intel.positive_catalysts.length > 0 && (
          <div>
            <p className="mb-1 text-xs font-medium text-emerald-700">利好催化</p>
            <ul className="list-inside list-disc text-sm text-emerald-800">
              {intel.positive_catalysts.map((s, i) => <li key={i}>{s}</li>)}
            </ul>
          </div>
        )}
      </div>
      {intel.earnings_outlook && (
        <p className="mt-2 text-sm text-gray-700"><strong>业绩预期：</strong>{intel.earnings_outlook}</p>
      )}
      {intel.sentiment_summary && (
        <p className="mt-1 text-sm text-gray-600">{intel.sentiment_summary}</p>
      )}
    </div>
  );
}

function OutlookSection({ llm }: { llm: LLMDashboard }) {
  if (!llm.short_term_outlook && !llm.medium_term_outlook && !llm.trend_analysis) return null;
  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {llm.trend_analysis && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">走势分析</p>
          <p className="text-sm text-gray-800">{llm.trend_analysis}</p>
        </div>
      )}
      {llm.short_term_outlook && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">短期展望 (1-3日)</p>
          <p className="text-sm text-gray-800">{llm.short_term_outlook}</p>
        </div>
      )}
      {llm.medium_term_outlook && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">中期展望 (1-2周)</p>
          <p className="text-sm text-gray-800">{llm.medium_term_outlook}</p>
        </div>
      )}
    </div>
  );
}

/* ========== Full dashboard display ========== */

function DashboardDisplay({ llm }: { llm: LLMDashboard }) {
  const dash: Dashboard | undefined = llm.dashboard;
  const hasDashboard = dash && (dash.core_conclusion || dash.data_perspective || dash.battle_plan || dash.intelligence);

  return (
    <div className="space-y-4">
      {/* Core conclusion */}
      {dash?.core_conclusion && <CoreConclusionCard dashboard={dash} />}

      {/* Summary */}
      {llm.analysis_summary && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">综合分析</p>
          <p className="whitespace-pre-wrap text-sm leading-relaxed text-gray-800">{llm.analysis_summary}</p>
        </div>
      )}

      {/* Data perspective grid */}
      {dash?.data_perspective && <DataPerspectiveGrid dashboard={dash} />}

      {/* Battle plan */}
      {dash?.battle_plan && <BattlePlanSection dashboard={dash} />}

      {/* Outlook */}
      <OutlookSection llm={llm} />

      {/* Intelligence */}
      {dash?.intelligence && <IntelligenceSection dashboard={dash} />}

      {/* Technical + fundamental analysis text */}
      {(llm.technical_analysis || llm.fundamental_analysis) && (
        <div className="grid gap-3 sm:grid-cols-2">
          {llm.technical_analysis && (
            <div className="rounded-lg border border-gray-200 bg-white p-4">
              <p className="mb-1 text-xs font-medium text-gray-500">技术面分析</p>
              <p className="text-sm text-gray-800">{llm.technical_analysis}</p>
            </div>
          )}
          {llm.fundamental_analysis && (
            <div className="rounded-lg border border-gray-200 bg-white p-4">
              <p className="mb-1 text-xs font-medium text-gray-500">基本面分析</p>
              <p className="text-sm text-gray-800">{llm.fundamental_analysis}</p>
            </div>
          )}
        </div>
      )}

      {/* Buy reason */}
      {llm.buy_reason && (
        <div className="rounded-lg border border-gray-200 bg-white p-4">
          <p className="mb-1 text-xs font-medium text-gray-500">操作理由</p>
          <p className="text-sm text-gray-800">{llm.buy_reason}</p>
        </div>
      )}

      {/* Fallback: legacy flat fields if no full dashboard */}
      {!hasDashboard && (
        <>
          {llm.key_points && llm.key_points.length > 0 && (
            <div>
              <p className="text-xs font-medium text-gray-500">关键要点</p>
              <ul className="mt-1 list-inside list-disc text-sm text-gray-700">
                {llm.key_points.map((s, i) => <li key={i}>{s}</li>)}
              </ul>
            </div>
          )}
          {llm.risk_warnings && llm.risk_warnings.length > 0 && (
            <div>
              <p className="text-xs font-medium text-gray-500">风险警告</p>
              <ul className="mt-1 list-inside list-disc text-sm text-amber-800">
                {llm.risk_warnings.map((s, i) => <li key={i}>{s}</li>)}
              </ul>
            </div>
          )}
          {(llm.target_price ?? 0) > 0 && (
            <span className="inline-block rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-1 text-sm">
              止盈价 <strong>{llm.target_price!.toFixed(2)}</strong>
            </span>
          )}
          {(llm.stop_loss_price ?? 0) > 0 && (
            <span className="ml-2 inline-block rounded-lg border border-red-200 bg-red-50 px-3 py-1 text-sm">
              止损价 <strong>{llm.stop_loss_price!.toFixed(2)}</strong>
            </span>
          )}
        </>
      )}
    </div>
  );
}

/* ========== Step-by-step progress log ========== */

function StepLog({ progress, elapsedSec }: { progress: ProgressState; elapsedSec: number }) {
  return (
    <div className="mb-6 rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-900">
          分析进度
          <span className="ml-2 text-xs font-normal text-gray-500">
            {progress.completed.length}/{progress.total}
          </span>
        </h3>
        <span className="tabular-nums text-xs text-gray-400">{elapsedSec}s</span>
      </div>
      <div className="space-y-1">
        {progress.completed.map((s) => (
          <div key={s.step} className="flex items-center gap-2 text-sm">
            <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-emerald-100 text-xs text-emerald-700">
              ✓
            </span>
            <span className="text-gray-700">
              [{s.step}/{progress.total}] {s.message}
            </span>
            <span className="ml-auto tabular-nums text-xs text-gray-400">
              {(s.durationMs / 1000).toFixed(1)}s
            </span>
          </div>
        ))}
        {progress.current && (
          <div className="flex items-center gap-2 text-sm">
            <span className="flex h-5 w-5 shrink-0 items-center justify-center">
              <span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
            </span>
            <span className="font-medium text-blue-700">
              [{progress.current.step}/{progress.total}] {progress.current.message}
            </span>
          </div>
        )}
        {Array.from(
          { length: progress.total - progress.completed.length - (progress.current ? 1 : 0) },
          (_, i) => {
            const step = progress.completed.length + (progress.current ? 1 : 0) + i + 1;
            return (
              <div key={step} className="flex items-center gap-2 text-sm text-gray-300">
                <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-gray-200 text-[10px]">
                  {step}
                </span>
                <span>等待中…</span>
              </div>
            );
          },
        )}
      </div>
      {/* Overall progress bar */}
      <div className="mt-3 h-1.5 w-full overflow-hidden rounded-full bg-gray-100">
        <div
          className="h-full rounded-full bg-blue-500 transition-all duration-500 ease-out"
          style={{
            width: `${(progress.completed.length / progress.total) * 100}%`,
          }}
        />
      </div>
    </div>
  );
}

/* ========== Main page ========== */

export default function AnalysisPage() {
  const [code, setCode] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<AnalysisResult | null>(null);
  const [history, setHistory] = useState<AnalysisResult[]>([]);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyPage, setHistoryPage] = useState(0);
  const PAGE_SIZE = 15;
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [batchDelVisible, setBatchDelVisible] = useState(false);
  const [batchDelLoading, setBatchDelLoading] = useState(false);
  const [singleDelId, setSingleDelId] = useState<number | null>(null);
  const [singleDelLoading, setSingleDelLoading] = useState(false);
  const [detailRecord, setDetailRecord] = useState<AnalysisResult | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [progress, setProgress] = useState<ProgressState | null>(null);
  const [ohlcvBars, setOhlcvBars] = useState<OHLCVBar[]>([]);
  const [elapsedSec, setElapsedSec] = useState(0);
  const timerRef = useRef(0);
  const stepStartRef = useRef(0);

  const loadHistory = useCallback((page?: number) => {
    const p = page ?? historyPage;
    getAnalysisHistory(PAGE_SIZE, p * PAGE_SIZE)
      .then((res) => { setHistory(res.items); setHistoryTotal(res.total); })
      .catch(() => { setHistory([]); setHistoryTotal(0); });
  }, [historyPage]);

  useEffect(() => {
    loadHistory();
  }, [loadHistory]);

  useEffect(() => {
    loadHistory(historyPage);
    setSelected(new Set());
  }, [historyPage]);

  async function openDetail(id: number) {
    setDetailLoading(true);
    try {
      const record = await getAnalysisDetail(id);
      setDetailRecord(record);
    } catch {
      setDetailRecord(null);
    } finally {
      setDetailLoading(false);
    }
  }

  function confirmDeleteSingle(id: number) {
    setSingleDelId(id);
  }

  function signalColor(signal?: string): string {
    if (!signal) return 'gray';
    if (signal.includes('买入')) return 'green';
    if (signal.includes('卖出')) return 'red';
    if (signal.includes('警告')) return 'orangered';
    return 'gold';
  }

  const historyColumns: ColumnProps<AnalysisResult>[] = [
    {
      title: '时间',
      dataIndex: 'analysis_date',
      width: 160,
      render: (_, r) => (
        <span className="tabular-nums text-gray-500 whitespace-nowrap">
          {r.analysis_date?.slice(0, 16).replace('T', ' ') || '—'}
        </span>
      ),
    },
    {
      title: '代码 / 名称',
      dataIndex: 'code',
      width: 150,
      render: (_, r) => (
        <span>
          <span className="font-mono text-gray-900">{r.code}</span>
          {r.name && <span className="ml-1 text-gray-500">{r.name}</span>}
        </span>
      ),
    },
    {
      title: '技术信号',
      dataIndex: 'signal_score',
      width: 120,
      render: (_, r) => {
        if (r.status === 'pending') {
          return (
            <span className="inline-flex items-center gap-1.5 text-blue-600">
              <span className="inline-block h-3 w-3 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
              <span className="text-xs">分析中…</span>
            </span>
          );
        }
        if (r.status === 'failed') {
          return <Tag color="red" size="small">失败</Tag>;
        }
        return (
          <span className="inline-flex items-center gap-1.5">
            {r.buy_signal
              ? <Tag color={signalColor(r.buy_signal)} size="small">{r.buy_signal}</Tag>
              : '—'}
            <span className="tabular-nums text-gray-400">{r.signal_score ?? ''}</span>
          </span>
        );
      },
    },
    {
      title: '模型分',
      dataIndex: 'model_score',
      width: 90,
      align: 'right' as const,
      render: (v) => <span className="tabular-nums">{v != null ? (v as number).toFixed(4) : '—'}</span>,
    },
    {
      title: '模型分位',
      dataIndex: 'model_percentile',
      width: 90,
      align: 'right' as const,
      render: (v) => <span className="tabular-nums">{v != null ? fmtPct(v as number) : '—'}</span>,
    },
    {
      title: '情绪',
      dataIndex: 'llm_sentiment',
      width: 60,
      align: 'right' as const,
      render: (v) => <span className="tabular-nums">{v != null ? Math.round(((v as number) + 1) * 50) : '—'}</span>,
    },
    {
      title: 'LLM 建议',
      dataIndex: 'llm_operation_advice',
      width: 90,
      render: (v) => {
        if (!v) return '—';
        const s = v as string;
        const color = s === '强烈买入' ? 'green' : s === '买入' ? 'cyan' : s === '强烈卖出' ? 'red' : s === '卖出' ? 'orangered' : 'gray';
        return <Tag color={color} size="small">{s}</Tag>;
      },
    },
    {
      title: '操作',
      width: 130,
      align: 'center' as const,
      render: (_, r) =>
        r.id != null ? (
          <Space size="small">
            {r.status !== 'pending' && (
              <Button type="text" size="mini" onClick={() => openDetail(r.id!)}>
                详情
              </Button>
            )}
            <Button type="text" status="danger" size="mini" onClick={() => confirmDeleteSingle(r.id!)}>
              删除
            </Button>
          </Space>
        ) : null,
    },
  ];

  useEffect(() => {
    return () => {
      if (timerRef.current) window.clearInterval(timerRef.current);
    };
  }, []);

  useEffect(() => {
    if (!loading) return;
    const id = window.setInterval(() => loadHistory(), 3000);
    loadHistory();
    return () => window.clearInterval(id);
  }, [loading, loadHistory]);

  async function onAnalyze() {
    const c = code.trim();
    if (!c) return;
    setLoading(true);
    setError(null);
    setResult(null);
    setOhlcvBars([]);
    const startMs = Date.now();
    stepStartRef.current = startMs;
    setProgress({ total: 9, completed: [], current: null, currentStartMs: startMs, startMs });
    setElapsedSec(0);

    timerRef.current = window.setInterval(() => {
      setElapsedSec(Math.round((Date.now() - startMs) / 1000));
    }, 500);

    try {
      const r = await triggerAnalysisStream(c, (evt) => {
        if (evt.type === 'progress') {
          const p = evt as ProgressEvent;
          const now = Date.now();
          setProgress((prev) => {
            if (!prev) return prev;
            const completed = [...prev.completed];
            if (prev.current && prev.current.step < p.step) {
              completed.push({
                step: prev.current.step,
                message: prev.current.message,
                durationMs: now - prev.currentStartMs,
              });
            }
            return {
              ...prev,
              completed,
              current: { step: p.step, message: p.message },
              currentStartMs: now,
            };
          });
        } else if (evt.type === 'ohlcv') {
          setOhlcvBars((evt as OHLCVEvent).bars);
        }
      });

      setProgress((prev) => {
        if (!prev || !prev.current) return prev;
        const now = Date.now();
        return {
          ...prev,
          completed: [
            ...prev.completed,
            {
              step: prev.current.step,
              message: prev.current.message,
              durationMs: now - prev.currentStartMs,
            },
          ],
          current: null,
        };
      });

      if (r.ohlcv?.length) setOhlcvBars(r.ohlcv);
      setResult(r);
      loadHistory();
    } catch (e: unknown) {
      const msg =
        e && typeof e === 'object' && 'response' in e
          ? String((e as { response?: { data?: { detail?: string } } }).response?.data?.detail)
          : e instanceof Error
            ? e.message
            : '分析失败';
      setError(msg || '分析失败');
    } finally {
      setLoading(false);
      if (timerRef.current) {
        window.clearInterval(timerRef.current);
        timerRef.current = 0;
      }
    }
  }

  const llm = result?.llm_dashboard as LLMDashboard | undefined;
  const hasDashboard = llm?.dashboard;

  return (
    <div>
      <h1 className="mb-2 text-2xl font-bold text-gray-900">个股分析</h1>
      <p className="mb-6 text-sm text-gray-500">
        技术打分 + 模型分位 + LLM 决策仪表盘；结果写入本地 SQLite 历史。
      </p>
      <div className="mb-6 flex flex-wrap gap-3">
        <input
          type="text"
          value={code}
          onChange={(e) => setCode(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && !loading && onAnalyze()}
          placeholder="股票代码，如 600519"
          className="w-64 rounded-lg border border-gray-300 px-4 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        />
        <button
          type="button"
          className="rounded-lg bg-blue-600 px-6 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          disabled={!code.trim() || loading}
          onClick={onAnalyze}
        >
          {loading ? '分析中…' : '开始分析'}
        </button>
      </div>

      {/* ---- Step-by-step progress log ---- */}
      {progress && loading && <StepLog progress={progress} elapsedSec={elapsedSec} />}

      {/* ---- K-line chart (shows during analysis once OHLCV arrives, or with result) ---- */}
      {ohlcvBars.length > 0 && (
        <div className="mb-6 rounded-xl border border-gray-200 bg-white p-4 shadow-sm">
          <div className="mb-2 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-900">
              日K线
              <span className="ml-2 text-xs font-normal text-gray-400">
                近 {ohlcvBars.length} 个交易日
              </span>
            </h3>
            {result?.data_source && (
              <span className="text-xs text-gray-400">数据源：{result.data_source}</span>
            )}
          </div>
          <CandlestickChart bars={ohlcvBars} height={340} />
        </div>
      )}

      {/* ---- Completed progress summary ---- */}
      {progress && !loading && progress.completed.length > 0 && (
        <div className="mb-6 rounded-xl border border-emerald-200 bg-emerald-50 p-4 shadow-sm">
          <details>
            <summary className="cursor-pointer text-sm font-medium text-emerald-800">
              分析完成 — {progress.completed.length} 步，耗时{' '}
              {((Date.now() - progress.startMs) / 1000).toFixed(1)}s
              <span className="ml-2 text-xs font-normal text-emerald-600">点击展开详情</span>
            </summary>
            <div className="mt-2 space-y-0.5">
              {progress.completed.map((s) => (
                <div key={s.step} className="flex items-center gap-2 text-sm text-emerald-700">
                  <span className="text-xs">✓</span>
                  <span>
                    [{s.step}/{progress.total}] {s.message}
                  </span>
                  <span className="ml-auto tabular-nums text-xs text-emerald-500">
                    {(s.durationMs / 1000).toFixed(1)}s
                  </span>
                </div>
              ))}
            </div>
          </details>
        </div>
      )}

      {error && (
        <div className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
          {error}
        </div>
      )}

      {result && (
        <div className="mb-8 space-y-4">
          {/* Header card — scores summary */}
          <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
            <div className="mb-4 flex flex-wrap items-baseline justify-between gap-2">
              <h2 className="text-lg font-semibold text-gray-900">
                {llm?.stock_name || result.name || result.code}{' '}
                <span className="text-base font-normal text-gray-500">({result.code})</span>
              </h2>
              <span className="text-xs text-gray-400">
                {result.analysis_date || '—'}
                {result.data_source ? ` · 数据源：${result.data_source}` : ''}
                {result.processing_time_ms != null ? ` · ${result.processing_time_ms} ms` : ''}
              </span>
            </div>
            <div className="grid gap-4 sm:grid-cols-4">
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">技术</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">{result.signal_score}</p>
                <p className="text-sm text-gray-600">{result.buy_signal}</p>
                {result.trend_status && (
                  <p className="mt-1 text-xs text-gray-500">趋势：{result.trend_status}</p>
                )}
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">模型</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">
                  {result.model_score != null ? result.model_score.toFixed(4) : '—'}
                </p>
                <p className="text-sm text-gray-600">
                  分位 {result.model_percentile != null ? fmtPct(result.model_percentile) : '—'}
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">LLM</p>
                <p className="mt-1 text-lg font-semibold text-gray-900">
                  {llm?.operation_advice || result.llm_operation_advice || '—'}
                </p>
                <p className="text-sm text-gray-600">
                  {llm?.confidence_level ? `置信：${llm.confidence_level}` : ''}
                  {llm?.trend_prediction ? ` · ${llm.trend_prediction}` : ''}
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">情绪</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">
                  {llm?.sentiment_score ?? (result.llm_sentiment != null ? Math.round((result.llm_sentiment + 1) * 50) : '—')}
                </p>
                <p className="text-sm text-gray-600">/100</p>
              </div>
            </div>

            {/* Signal reasons + risk factors (technical) */}
            {result.signal_reasons?.length ? (
              <div className="mt-4">
                <p className="text-xs font-medium text-gray-500">看多理由</p>
                <ul className="mt-1 list-inside list-disc text-sm text-gray-700">
                  {result.signal_reasons.map((s) => <li key={s}>{s}</li>)}
                </ul>
              </div>
            ) : null}
            {result.risk_factors?.length ? (
              <div className="mt-3">
                <p className="text-xs font-medium text-gray-500">风险</p>
                <ul className="mt-1 list-inside list-disc text-sm text-amber-800">
                  {result.risk_factors.map((s) => <li key={s}>{s}</li>)}
                </ul>
              </div>
            ) : null}
          </div>

          {/* Full decision dashboard */}
          {llm && (
            <div className="space-y-4">
              <DashboardDisplay llm={llm} />
            </div>
          )}

          {/* Fallback: plain LLM summary for old records without dashboard */}
          {!hasDashboard && result.llm_summary && (
            <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
              <p className="text-xs font-medium text-gray-500">LLM 分析摘要</p>
              <p className="mt-2 whitespace-pre-wrap text-sm leading-relaxed text-gray-800">
                {result.llm_summary}
              </p>
            </div>
          )}
        </div>
      )}

      <TaskQueuePanel onTaskCompleted={() => loadHistory()} />

      {/* ---- Detail modal (Arco) ---- */}
      <Modal
        visible={detailRecord != null || detailLoading}
        onCancel={() => { setDetailRecord(null); setDetailLoading(false); }}
        title={
          detailRecord
            ? `${detailRecord.name || detailRecord.code} (${detailRecord.code})`
            : '分析详情'
        }
        footer={null}
        style={{ width: 780, top: 40 }}
        autoFocus={false}
        unmountOnExit
      >
        {detailLoading ? (
          <div className="flex items-center justify-center py-16">
            <Spin size={28} />
          </div>
        ) : detailRecord ? (
          <div className="space-y-4">
            <div className="flex items-center gap-2 text-xs text-gray-400">
              <span>{detailRecord.analysis_date?.slice(0, 16).replace('T', ' ') || '—'}</span>
              {detailRecord.processing_time_ms != null && (
                <span>· {detailRecord.processing_time_ms}ms</span>
              )}
            </div>
            <div className="grid gap-4 sm:grid-cols-4">
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">技术</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">{detailRecord.signal_score}</p>
                <p className="text-sm text-gray-600">{detailRecord.buy_signal}</p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">模型</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">
                  {detailRecord.model_score != null ? detailRecord.model_score.toFixed(4) : '—'}
                </p>
                <p className="text-sm text-gray-600">
                  分位 {detailRecord.model_percentile != null ? fmtPct(detailRecord.model_percentile) : '—'}
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">LLM</p>
                <p className="mt-1 text-lg font-semibold text-gray-900">
                  {(detailRecord.llm_dashboard as LLMDashboard | undefined)?.operation_advice || detailRecord.llm_operation_advice || '—'}
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase text-gray-400">情绪</p>
                <p className="mt-1 text-2xl font-bold text-gray-900">
                  {detailRecord.llm_sentiment != null ? Math.round((detailRecord.llm_sentiment + 1) * 50) : '—'}
                </p>
              </div>
            </div>
            {detailRecord.llm_dashboard && Object.keys(detailRecord.llm_dashboard).length > 0 && (
              <DashboardDisplay llm={detailRecord.llm_dashboard as LLMDashboard} />
            )}
            {detailRecord.llm_summary && !(detailRecord.llm_dashboard as LLMDashboard | undefined)?.dashboard && (
              <div className="rounded-lg border border-gray-200 bg-gray-50 p-4">
                <p className="mb-1 text-xs font-medium text-gray-500">LLM 分析摘要</p>
                <p className="whitespace-pre-wrap text-sm leading-relaxed text-gray-800">{detailRecord.llm_summary}</p>
              </div>
            )}
          </div>
        ) : null}
      </Modal>

      {/* ---- Single delete confirm (Arco Modal) ---- */}
      <Modal
        visible={singleDelId != null}
        onCancel={() => setSingleDelId(null)}
        title="删除记录"
        confirmLoading={singleDelLoading}
        okButtonProps={{ status: 'danger' }}
        okText="确认删除"
        onOk={async () => {
          if (singleDelId == null) return;
          setSingleDelLoading(true);
          try {
            await deleteAnalysis(singleDelId);
            loadHistory();
            Message.success('删除成功');
            setSingleDelId(null);
          } catch {
            Message.error('删除失败');
          } finally {
            setSingleDelLoading(false);
          }
        }}
        autoFocus={false}
        style={{ width: 400 }}
      >
        <p>确定要删除这条分析记录吗？</p>
      </Modal>

      {/* ---- Batch delete confirm (Arco Modal) ---- */}
      <Modal
        visible={batchDelVisible}
        onCancel={() => setBatchDelVisible(false)}
        title="批量删除"
        confirmLoading={batchDelLoading}
        okButtonProps={{ status: 'danger' }}
        okText="确认删除"
        onOk={async () => {
          setBatchDelLoading(true);
          try {
            await batchDeleteAnalysis(Array.from(selected));
            setSelected(new Set());
            loadHistory();
            Message.success('删除成功');
            setBatchDelVisible(false);
          } catch {
            Message.error('删除失败');
          } finally {
            setBatchDelLoading(false);
          }
        }}
        autoFocus={false}
        style={{ width: 400 }}
      >
        <p>确定要删除选中的 <strong>{selected.size}</strong> 条记录吗？此操作不可恢复。</p>
      </Modal>

      {/* ---- History section (Arco Table) ---- */}
      <div className="rounded-xl border border-gray-200 bg-white shadow-sm">
        <div className="flex items-center justify-between border-b border-gray-100 px-6 py-4">
          <h2 className="text-lg font-semibold text-gray-900">
            分析记录
            {historyTotal > 0 && (
              <span className="ml-2 text-sm font-normal text-gray-400">共 {historyTotal} 条</span>
            )}
          </h2>
          <Space size="medium">
            {selected.size > 0 && (
              <Button
                type="primary"
                status="danger"
                size="small"
                onClick={() => setBatchDelVisible(true)}
              >
                删除选中 ({selected.size})
              </Button>
            )}
            <Button size="small" onClick={() => loadHistory()}>刷新</Button>
          </Space>
        </div>
        <Table
          rowKey="id"
          columns={historyColumns}
          data={history.filter((h) => h.id != null)}
          scroll={{ x: 960 }}
          pagination={{
            total: historyTotal,
            pageSize: PAGE_SIZE,
            current: historyPage + 1,
            onChange: (page) => {
              setHistoryPage(page - 1);
              setSelected(new Set());
            },
            showTotal: true,
            sizeCanChange: false,
          }}
          rowSelection={{
            type: 'checkbox',
            selectedRowKeys: Array.from(selected),
            onChange: (keys) => {
              setSelected(new Set(keys.map(Number)));
            },
            checkAll: true,
          }}
          size="small"
          stripe
          noDataElement={<p className="py-8 text-sm text-gray-500">暂无历史，请先执行一次分析。</p>}
        />
      </div>
    </div>
  );
}
