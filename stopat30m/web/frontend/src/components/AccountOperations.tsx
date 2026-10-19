import { useCallback, useEffect, useRef, useState, type ChangeEvent, type FormEvent } from 'react';
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { getAccountNav, reconcileAccount, saveAccountNav, type AccountNavSnapshot, type Reconciliation } from '../api/accountOperations';
import { useAuth } from '../contexts/AuthContext';
import { completeRequest, requestKey } from '../utils/idempotency';
import { dateTime, errorMessage, localDateTime, money, number, pnlColor } from '../utils/format';

export default function AccountOperations({ refreshToken }: { refreshToken?: number | string }) {
  const { user } = useAuth();
  const [items, setItems] = useState<AccountNavSnapshot[]>([]);
  const [total, setTotal] = useState(0);
  const [limitations, setLimitations] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [saving, setSaving] = useState(false);
  const [historyError, setHistoryError] = useState('');
  const [saveError, setSaveError] = useState('');
  const [receipt, setReceipt] = useState('');
  const mounted = useRef(false);
  const saveLock = useRef(false);
  const sequence = useRef(0);

  const load = useCallback(async (offset = 0, signal?: AbortSignal) => {
    const current = ++sequence.current;
    try {
      const history = await getAccountNav({ limit: 90, offset }, signal);
      if (!mounted.current || signal?.aborted || current !== sequence.current) return;
      setItems(previous => offset === 0 ? history.items : [...new Map([...previous, ...history.items].map(item => [item.id, item])).values()]);
      setTotal(history.total); setLimitations(history.limitations); setHistoryError('');
    } catch (error) {
      if (!mounted.current || signal?.aborted || current !== sequence.current) return;
      setHistoryError(`估值记录读取失败：${errorMessage(error)}。已有记录可能是上次读取的结果。`);
    } finally {
      if (mounted.current && !signal?.aborted && current === sequence.current) { setLoading(false); setLoadingMore(false); }
    }
  }, []);

  useEffect(() => {
    mounted.current = true;
    const controller = new AbortController();
    if (user) void load(0, controller.signal);
    return () => { mounted.current = false; controller.abort(); };
  }, [load, user, refreshToken]);

  async function save() {
    if (saveLock.current || !user) return;
    saveLock.current = true; setSaving(true); setSaveError(''); setReceipt('');
    const scope = `account-nav:${user.id}`;
    try {
      const result = await saveAccountNav(requestKey(scope, {}));
      completeRequest(scope);
      if (!mounted.current) return;
      setReceipt(result.replayed
        ? `已返回原请求回执：${dateTime(result.snapshot.recorded_at)}，版本 ${result.snapshot.revision}。这次重试没有重新取价；再次保存可创建新的当日估值。`
        : `已保存 ${result.snapshot.date} 的估值，保存时间 ${dateTime(result.snapshot.recorded_at)}，当日版本 ${result.snapshot.revision}。`);
      await load();
    } catch (error) {
      if (mounted.current) setSaveError(`${errorMessage(error)}。不会生成不完整快照；同一请求重试会保留原标识，避免重复保存。`);
    } finally { saveLock.current = false; if (mounted.current) setSaving(false); }
  }

  const ordered = [...items].sort((a, b) => a.date.localeCompare(b.date));
  return <div className="space-y-6">
    <section className="account-panel space-y-4" aria-label="账户估值记录">
      <div className="flex flex-wrap items-start justify-between gap-3"><div><h2 className="font-semibold text-gray-900">账户估值记录</h2><p className="mt-2 max-w-3xl text-sm leading-6 text-gray-500">主动保存当前上海日期的账户资产与报价来源。同日再次保存会更新该日记录；这是保存时点的估值，不是历史收盘净值。</p></div><div className="flex flex-wrap gap-2"><button className="rounded-lg border border-gray-200 px-3 py-2 text-sm text-gray-600 disabled:opacity-50" type="button" disabled={loading || saving || loadingMore} onClick={() => { setLoading(true); void load(); }}>{loading ? '读取中…' : '刷新记录'}</button><button className="primary-button" type="button" disabled={saving || !user} onClick={() => void save()}>{saving ? '取价与核对账本中…' : '保存当日估值'}</button></div></div>
      <p className="text-xs leading-5 text-gray-500">仅点“保存”时获取新报价。持仓缺价、行情过期、来源时刻未知或取价期间账本变化时会拒绝保存；无持仓时可保存现金估值。入出金不算利润，不计算收益率或年化。</p>
      {saveError && <p role="alert" className="whitespace-pre-wrap break-words rounded-lg bg-red-50 p-3 text-sm text-red-700">{saveError}</p>}
      {receipt && <p role="status" className="rounded-lg bg-blue-50 p-3 text-sm text-blue-700">{receipt}</p>}
      {historyError && <p role="alert" className="rounded-lg bg-amber-50 p-3 text-sm text-amber-800">{historyError}</p>}
      {ordered.length ? <>
        <div className="grid gap-5 xl:grid-cols-2"><ObservationChart items={ordered} field="equity" title="保存时点的总资产" color="#2563eb" /><ObservationChart items={ordered} field="total_pnl" title="累计损益（总资产 − 累计净投入）" color="#7c3aed" /></div>
        <p className="text-xs leading-5 text-gray-500">图中仅连接实际保存记录，未补齐缺失日期；跨日保存时点可能不同。累计损益包含账本已记录的费用，不等于排除现金流时点影响后的收益率。</p>
        <div className="overflow-x-auto rounded-lg border border-gray-100"><table className="w-full min-w-[700px] text-left text-sm"><thead className="bg-gray-50 text-xs text-gray-500"><tr><th className="px-3 py-3 font-medium">保存日期 / 时点</th><th className="px-3 py-3 text-right font-medium">现金</th><th className="px-3 py-3 text-right font-medium">总资产</th><th className="px-3 py-3 text-right font-medium">累计净投入</th><th className="px-3 py-3 text-right font-medium">累计损益</th><th className="px-3 py-3 font-medium">依据</th></tr></thead><tbody className="divide-y divide-gray-100">{[...ordered].reverse().map(item => <tr key={item.id}><td className="px-3 py-3"><p>{item.date}</p><p className="mt-1 whitespace-nowrap text-xs text-gray-400">{dateTime(item.recorded_at)} · 版本 {item.revision}</p></td><td className="whitespace-nowrap px-3 py-3 text-right">{money(item.cash)}</td><td className="whitespace-nowrap px-3 py-3 text-right font-medium">{money(item.equity)}</td><td className="whitespace-nowrap px-3 py-3 text-right">{money(item.net_deposits)}</td><td className={`whitespace-nowrap px-3 py-3 text-right ${pnlColor(item.total_pnl)}`}>{money(item.total_pnl, true)}</td><td className="px-3 py-3"><SnapshotFacts item={item} /></td></tr>)}</tbody></table></div>
        <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-gray-500"><p>已读取 {items.length} / {total} 条每日记录，按日期倒序展示。</p>{items.length < total && <button type="button" disabled={loadingMore || loading || saving} className="rounded-lg border border-gray-200 px-3 py-2 disabled:opacity-50" onClick={() => { setLoadingMore(true); void load(items.length); }}>{loadingMore ? '读取中…' : '读取更早记录'}</button>}</div>
      </> : !loading && !historyError ? <div className="rounded-xl border border-dashed border-gray-300 bg-gray-50 p-5 text-sm leading-6 text-gray-600"><p className="font-medium text-gray-800">尚无账户估值记录</p><p className="mt-1">可主动保存今天的实际估值。没有保存的历史日期保持为空，不生成模拟净值或收益曲线。</p></div> : <p className="text-sm text-gray-500">{loading ? '正在读取实际保存记录…' : '暂不能确认账户估值记录。'}</p>}
      {limitations.length > 0 && <details className="text-xs leading-5 text-gray-500"><summary className="cursor-pointer">估值记录的范围</summary><ul className="mt-2 space-y-1">{limitations.map(item => <li key={item}>• {item}</li>)}</ul></details>}
    </section>
    <StatementReconciliation />
  </div>;
}

function ObservationChart({ items, field, title, color }: { items: AccountNavSnapshot[]; field: 'equity' | 'total_pnl'; title: string; color: string }) {
  return <div className="rounded-xl border border-gray-100 p-3"><h3 className="mb-4 text-sm font-medium text-gray-800">{title}</h3><div className="h-56 w-full"><ResponsiveContainer width="100%" height="100%"><LineChart data={items} margin={{ top: 10, right: 15, bottom: 5, left: 10 }}><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" /><XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={40} /><YAxis width={65} tick={{ fontSize: 11 }} domain={['auto', 'auto']} tickFormatter={value => new Intl.NumberFormat('zh-CN', { notation: 'compact', maximumFractionDigits: 1 }).format(Number(value))} /><Tooltip formatter={value => [money(Number(value)), field === 'equity' ? '总资产' : '累计损益']} labelFormatter={label => `保存日期 ${String(label)}`} /><Line type="linear" dataKey={field} stroke={color} strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} connectNulls={false} /></LineChart></ResponsiveContainer></div></div>;
}

function SnapshotFacts({ item }: { item: AccountNavSnapshot }) {
  return <details className="min-w-36 max-w-md text-xs text-gray-500"><summary className="cursor-pointer text-blue-600">账本与报价来源</summary><div className="mt-3 space-y-2"><p className="break-all">账本版本：{item.ledger_version}</p><p>成交 {item.facts.ledger.execution_count} 笔 · 资金流 {item.facts.ledger.cash_flow_count} 笔</p><p>持仓市值 {money(item.market_value)} · 已实现 {money(item.realized_pnl, true)} · 未实现 {money(item.unrealized_pnl, true)} · 费用 {money(item.total_fees)}</p>{Object.entries(item.facts.quotes).length ? Object.entries(item.facts.quotes).map(([instrument, quote]) => <div key={instrument} className="rounded bg-gray-50 p-2"><p>{instrument} · {number(quote.price, 4)} 元 · {quote.source}</p><p className="mt-1">行情 {dateTime(quote.as_of)}</p><p>获取 {dateTime(quote.fetched_at)}</p></div>) : <p>保存时无持仓，按现金估值，未请求证券报价。</p>}</div></details>;
}

function StatementReconciliation() {
  const [csvText, setCsvText] = useState('');
  const [brokerCash, setBrokerCash] = useState('');
  const [statementDate, setStatementDate] = useState(localDateTime().slice(0, 10));
  const [result, setResult] = useState<Reconciliation | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [readingFile, setReadingFile] = useState(false);
  const [error, setError] = useState('');
  const submitLock = useRef(false);
  const mounted = useRef(false);
  const fileSequence = useRef(0);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);

  async function readFile(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    const current = ++fileSequence.current;
    setResult(null); setError(''); setCsvText(''); setReadingFile(false);
    if (!file) return;
    if (file.size > 100_000) { setError('文件不能超过100KB；仅接受标准两列CSV文本。'); return; }
    setReadingFile(true);
    try {
      const text = await file.text();
      if (mounted.current && current === fileSequence.current) setCsvText(text);
    } catch (failure) { if (mounted.current && current === fileSequence.current) setError(`文件读取失败：${errorMessage(failure)}`); }
    finally { if (mounted.current && current === fileSequence.current) setReadingFile(false); }
  }

  function downloadTemplate() {
    const url = URL.createObjectURL(new Blob(['\ufeffinstrument,quantity\n'], { type: 'text/csv;charset=utf-8;' }));
    const link = document.createElement('a');
    link.href = url; link.download = '持仓对账模板.csv'; link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (submitLock.current || readingFile) return;
    const cash = Number(brokerCash);
    if (!brokerCash.trim() || !Number.isFinite(cash) || cash < 0 || cash > 1e12) { setError('请输入券商账户的实际可用现金，范围为0至1万亿元。'); return; }
    if (!csvText.trim() || new TextEncoder().encode(csvText).length > 100_000) { setError('请粘贴或选择不超过100KB的标准持仓CSV。'); return; }
    submitLock.current = true; setSubmitting(true); setError(''); setResult(null);
    const submittedSequence = fileSequence.current;
    try {
      const report = await reconcileAccount({ csv_text: csvText, broker_cash: cash, statement_date: statementDate || undefined });
      if (mounted.current && submittedSequence === fileSequence.current) setResult(report);
    } catch (failure) { if (mounted.current && submittedSequence === fileSequence.current) setError(errorMessage(failure)); }
    finally { submitLock.current = false; if (mounted.current) setSubmitting(false); }
  }

  return <section className="account-panel" aria-label="持仓与现金只读对账"><details><summary className="cursor-pointer font-semibold text-gray-900">持仓与现金只读对账</summary><p className="mb-4 mt-3 text-sm leading-6 text-gray-500">比较券商持仓数量及独立输入的现金，不读取行情、不导入成交，也不修改账本。请把券商导出整理为标准模板；仅支持当前上海日期。</p>
    <div className="mb-4 flex flex-wrap items-center gap-3"><button type="button" className="rounded-lg border border-gray-200 px-3 py-2 text-xs text-gray-600" onClick={downloadTemplate}>下载空白CSV模板</button><span className="text-xs text-gray-500">表头 instrument,quantity；代码可带 SH/SZ/BJ 前后缀，数量为非负整数。最多2000行，重复代码归一后会拒绝。</span></div>
    <form onSubmit={submit}><fieldset disabled={submitting || readingFile} className="space-y-4 disabled:opacity-60"><label className="block"><span className="field-label">粘贴标准持仓CSV</span><textarea className="field-input min-h-32 font-mono text-xs" value={csvText} maxLength={100_000} required placeholder={'instrument,quantity\nSH600000,100'} onChange={event => { fileSequence.current += 1; setReadingFile(false); setCsvText(event.target.value); setResult(null); }} /></label><label className="block"><span className="field-label">或选择本机CSV / TXT文本文件（不上传原文件）</span><input className="text-sm text-gray-600" type="file" accept=".csv,.txt,text/csv,text/plain" onChange={event => void readFile(event)} /></label><div className="grid items-end gap-4 sm:grid-cols-3"><label><span className="field-label">券商现金（元）</span><input className="field-input" type="number" min="0" max="1000000000000" step="0.01" required value={brokerCash} onChange={event => { setBrokerCash(event.target.value); setResult(null); }} /></label><label><span className="field-label">券商数据日期（上海今日）</span><input className="field-input" type="date" min={localDateTime().slice(0, 10)} max={localDateTime().slice(0, 10)} value={statementDate} onChange={event => { setStatementDate(event.target.value); setResult(null); }} /></label><button className="primary-button" type="submit">{readingFile ? '读取文件中…' : submitting ? '核对中…' : '只读核对持仓与现金'}</button></div></fieldset></form>
    <p className="mt-3 text-xs leading-5 text-gray-500">文件只在浏览器读成文本，再将两列数据提交给本系统核对。无持仓时保留CSV表头即可。匹配只能说明数量和现金相同，不能证明价格、成本或公司行为正确。</p>
    {error && <p role="alert" className="mt-4 break-words rounded-lg bg-red-50 p-3 text-sm text-red-700">{error}</p>}
    {result && <div role="status" className="mt-5 space-y-4"><div className={`rounded-lg p-3 text-sm ${result.matched ? 'bg-blue-50 text-blue-800' : 'bg-amber-50 text-amber-800'}`}><p className="font-medium">{result.matched ? '本次现金与持仓数量一致' : '本次核对发现差异，请人工检查流水'}</p><p className="mt-1 text-xs">核对时间 {dateTime(result.as_of)} · 券商数据日期 {result.statement_date}</p><p className="mt-1 text-xs">此结果仅反映核对时的账本，不会随之后的成交或入出金自动更新。</p></div><dl className="grid gap-3 text-sm sm:grid-cols-3"><div><dt className="text-xs text-gray-500">本地现金</dt><dd className="mt-1">{money(result.cash.ledger)}</dd></div><div><dt className="text-xs text-gray-500">券商现金</dt><dd className="mt-1">{money(result.cash.statement)}</dd></div><div><dt className="text-xs text-gray-500">现金差异（券商 − 本地）</dt><dd className={`mt-1 font-medium ${result.cash.difference ? 'text-red-700' : 'text-gray-800'}`}>{money(result.cash.difference, true)}</dd></div></dl><div className="overflow-x-auto rounded-lg border border-gray-100"><table className="w-full min-w-[450px] text-left text-sm"><thead className="bg-gray-50 text-xs text-gray-500"><tr><th className="px-3 py-2 font-medium">证券</th><th className="px-3 py-2 text-right font-medium">本地数量</th><th className="px-3 py-2 text-right font-medium">券商数量</th><th className="px-3 py-2 text-right font-medium">差异（券商 − 本地）</th></tr></thead><tbody className="divide-y divide-gray-100">{result.positions.map(row => <tr key={row.instrument}><td className="px-3 py-3 font-mono text-xs">{row.instrument}</td><td className="px-3 py-3 text-right">{number(row.ledger_quantity, 0)}</td><td className="px-3 py-3 text-right">{number(row.statement_quantity, 0)}</td><td className={`px-3 py-3 text-right ${row.difference ? 'font-medium text-red-700' : 'text-gray-500'}`}>{row.difference > 0 ? '+' : ''}{number(row.difference, 0)}</td></tr>)}{!result.positions.length && <tr><td colSpan={4} className="px-3 py-4 text-center text-gray-500">两边均无持仓记录</td></tr>}</tbody></table></div><ul className="space-y-1 text-xs leading-5 text-gray-500">{result.warnings.map(warning => <li key={warning}>• {warning}</li>)}</ul><details className="text-xs text-gray-500"><summary className="cursor-pointer">本次依据的账本版本</summary><p className="mt-2 break-all font-mono">{result.ledger_version}</p></details></div>}
  </details></section>;
}
