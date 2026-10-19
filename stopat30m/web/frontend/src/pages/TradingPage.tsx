import { useCallback, useEffect, useRef, useState, type FormEvent, type ReactNode } from 'react';
import {
  getAccount, getPositions, getTrades, getLegacyTrades, getCashFlows,
  initializeAccount, createCashFlow, submitManualTrade,
  type AccountResponse, type PositionsResponse, type TradeRow, type CashFlowRow, type PageResult,
} from '../api/trading';
import AccountOverview from '../components/AccountOverview';
import AccountOperations from '../components/AccountOperations';
import { useAuth } from '../contexts/AuthContext';
import { dateTime, errorMessage, localDateTime, money, number, pnlColor } from '../utils/format';
import { completeRequest, requestKey } from '../utils/idempotency';

type LedgerTab = 'trades' | 'cash' | 'legacy';
const PAGE_SIZE = 20;
const today = () => localDateTime().slice(0, 10);

export default function TradingPage() {
  const { user, isAdmin } = useAuth();
  const [accountData, setAccountData] = useState<AccountResponse | null>(null);
  const [positions, setPositions] = useState<PositionsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadErrors, setLoadErrors] = useState<string[]>([]);
  const [actionError, setActionError] = useState('');
  const [success, setSuccess] = useState('');
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const refreshId = useRef(0);
  const [revision, setRevision] = useState(0);
  const [initialCapital, setInitialCapital] = useState('');
  const [accountName, setAccountName] = useState('我的账户');
  const [openedDate, setOpenedDate] = useState(today);
  const [inst, setInst] = useState('');
  const [direction, setDirection] = useState<'BUY' | 'SELL'>('BUY');
  const [quantity, setQuantity] = useState('');
  const [price, setPrice] = useState('');
  const [tradeDate, setTradeDate] = useState(today);
  const [note, setNote] = useState('');
  const [actualFees, setActualFees] = useState(false);
  const [commission, setCommission] = useState('');
  const [stampTax, setStampTax] = useState('');
  const [transferFee, setTransferFee] = useState('');
  const [cashDirection, setCashDirection] = useState<'DEPOSIT' | 'WITHDRAW'>('DEPOSIT');
  const [cashAmount, setCashAmount] = useState('');
  const [cashDate, setCashDate] = useState(today);
  const [cashNote, setCashNote] = useState('');
  const [tab, setTab] = useState<LedgerTab>('trades');
  const [includeUnassigned, setIncludeUnassigned] = useState(false);
  const [offset, setOffset] = useState(0);
  const [filterInput, setFilterInput] = useState('');
  const [instrumentFilter, setInstrumentFilter] = useState('');
  const [tradePage, setTradePage] = useState<PageResult<TradeRow> | null>(null);
  const [cashPage, setCashPage] = useState<PageResult<CashFlowRow> | null>(null);
  const [ledgerLoading, setLedgerLoading] = useState(false);
  const [ledgerError, setLedgerError] = useState('');
  const tradeFormRef = useRef<HTMLDivElement>(null);

  const refresh = useCallback(async () => {
    const id = ++refreshId.current;
    setLoading(true);
    const [a, p] = await Promise.allSettled([getAccount(), getPositions()]);
    if (id !== refreshId.current) return;
    const errors: string[] = [];
    if (a.status === 'fulfilled') setAccountData(a.value);
    else errors.push(`账户：${errorMessage(a.reason)}`);
    if (p.status === 'fulfilled') setPositions(p.value);
    else errors.push(`持仓：${errorMessage(p.reason)}`);
    setLoadErrors(errors);
    setLoading(false);
  }, []);

  useEffect(() => { void refresh(); }, [refresh]);

  useEffect(() => {
    let active = true;
    setLedgerLoading(true);
    setLedgerError('');
    setTradePage(null);
    setCashPage(null);
    const request = tab === 'cash'
      ? getCashFlows(PAGE_SIZE, offset).then(data => { if (active) setCashPage(data); })
      : (tab === 'legacy' ? getLegacyTrades(PAGE_SIZE, offset, includeUnassigned) : getTrades(PAGE_SIZE, offset, instrumentFilter)).then(data => { if (active) setTradePage(data); });
    request.catch(error => { if (active) setLedgerError(errorMessage(error)); }).finally(() => { if (active) setLedgerLoading(false); });
    return () => { active = false; };
  }, [tab, offset, instrumentFilter, revision, includeUnassigned]);

  async function mutate(scope: string, payload: Record<string, unknown>, action: (key: string) => Promise<unknown>, message: string, onSuccess?: () => void) {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    setActionError('');
    setSuccess('');
    const keyScope = `trading:${user?.id}:${scope}`;
    try {
      await action(requestKey(keyScope, payload));
      completeRequest(keyScope);
      setSuccess(message);
      onSuccess?.();
      setRevision(value => value + 1);
      await refresh();
    } catch (error) {
      setActionError(`${errorMessage(error)}。如果网络中断，请保留原内容重试，同一请求不会重复记账。`);
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  }

  function onInitialize(event: FormEvent) {
    event.preventDefault();
    const amount = Number(initialCapital);
    if (initialCapital === '' || !Number.isFinite(amount) || amount < 0) { setActionError('请输入大于或等于 0 的开户本金'); return; }
    const payload = { initial_capital: amount, name: accountName.trim() || '我的账户', opened_date: openedDate };
    void mutate('initialize', payload, key => initializeAccount({ ...payload, idempotency_key: key }), '账户已建立，可以按时间顺序录入资金和成交。');
  }

  function onTrade(event: FormEvent) {
    event.preventDefault();
    const qty = Number(quantity), px = Number(price);
    if (!inst.trim() || !Number.isInteger(qty) || qty <= 0 || !Number.isFinite(px) || px <= 0) { setActionError('请填写有效代码、正整数股数和大于 0 的成交价'); return; }
    const fees = actualFees ? { commission: Number(commission), stamp_tax: Number(stampTax), transfer_fee: Number(transferFee) } : {};
    if (actualFees && (commission === '' || stampTax === '' || transferFee === '' || Object.values(fees).some(value => !Number.isFinite(value) || value < 0))) { setActionError('请填写三项实际费用；不产生费用的项目填写 0'); return; }
    const payload = { instrument: inst.trim(), direction, quantity: qty, price: px, trade_date: tradeDate, note: note.trim() || undefined, ...fees };
    void mutate('trade', payload, key => submitManualTrade({ ...payload, idempotency_key: key }), '成交已记入账本，现金与持仓已重新计算。', () => { setQuantity(''); setPrice(''); setNote(''); setCommission(''); setStampTax(''); setTransferFee(''); setActualFees(false); });
  }

  function onCashFlow(event: FormEvent) {
    event.preventDefault();
    const amount = Number(cashAmount);
    if (!Number.isFinite(amount) || amount <= 0) { setActionError('资金金额必须大于 0'); return; }
    const payload = { direction: cashDirection, amount, effective_date: cashDate, note: cashNote.trim() || undefined };
    void mutate('cash', payload, key => createCashFlow({ ...payload, idempotency_key: key }), cashDirection === 'DEPOSIT' ? '入金已记入账本。' : '出金已记入账本。', () => { setCashAmount(''); setCashNote(''); });
  }

  const account = accountData?.account;
  const initialized = accountData?.initialized === true;
  const total = tab === 'cash' ? cashPage?.total ?? 0 : tradePage?.total ?? 0;
  const legacyCount = (accountData?.legacy_trade_count ?? 0) + (accountData?.legacy_unassigned_count ?? 0);

  return <div className="space-y-6">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div><h1 className="text-2xl font-bold text-gray-900">持仓与账本</h1><p className="mt-2 text-sm text-gray-500">记录自己的实际成交与资金变动，核对现金、持仓和盈亏。金额单位为人民币，日期按上海时间。</p></div>
      <button className="secondary-button" disabled={loading || busy} onClick={() => { void refresh(); setRevision(value => value + 1); }}>{loading ? '更新中…' : '刷新账户'}</button>
    </div>
    {loadErrors.length > 0 && <Notice tone="error">{loadErrors.map(error => <p key={error}>{error}</p>)}<p>刷新失败时保留上次成功的数据，请刷新后再核对。</p></Notice>}
    {actionError && <Notice tone="error">{actionError}</Notice>}
    {success && <Notice tone="success">{success}</Notice>}
    {loading && !accountData && <div className="account-panel text-sm text-gray-500">正在核对账户…</div>}
    {account && <AccountOverview account={account} />}
    {accountData && !initialized && <section className="account-panel border-blue-200">
      <h2 className="text-lg font-semibold text-gray-900">建立个人账户</h2>
      <p className="mb-4 mt-2 text-sm text-gray-500">以开户本金建立现金账本，再按实际发生日期依次记录资金和成交。本金不计为收益。</p>
      <form onSubmit={onInitialize}><fieldset disabled={busy} className="grid items-end gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Field label="账户名称"><input className="field-input" value={accountName} onChange={event => setAccountName(event.target.value)} maxLength={80} required /></Field>
        <Field label="开户本金（元）"><input className="field-input" type="number" min="0" step="0.01" value={initialCapital} onChange={event => setInitialCapital(event.target.value)} placeholder="例如 100000" required /></Field>
        <Field label="开户日期"><input className="field-input" type="date" max={today()} value={openedDate} onChange={event => setOpenedDate(event.target.value)} required /></Field>
        <button className="primary-button" type="submit">{busy ? '建立中…' : '建立账户'}</button>
      </fieldset></form>
    </section>}
    {legacyCount > 0 && <Notice tone="warning">有 {legacyCount} 条历史或未归属成交保留在「历史参考」中，它们没有计入新账户的现金、持仓与收益。请勿把历史流水重复计入新账本。</Notice>}

    <section className="account-panel">
      <div className="mb-4 flex flex-wrap justify-between gap-2"><h2 className="font-semibold text-gray-900">当前持仓</h2><span className="text-xs text-gray-400">可卖数量遵循 T+1 · 空缺报价不按 0 估值</span></div>
      {positions && positions.positions.length > 0 ? <div className="overflow-x-auto"><table className="ledger-table">
        <thead><tr>{['标的', '持有 / 可卖', '成本价', '现价', '持仓市值', '浮动盈亏', '盈亏率', '报价时间 / 来源', '操作'].map(label => <th key={label}>{label}</th>)}</tr></thead>
        <tbody>{positions.positions.map(row => <tr key={row.instrument}>
          <td><p className="font-medium text-gray-900">{row.name || row.code}</p><p className="mt-1 font-mono text-xs text-gray-400">{row.instrument}</p></td>
          <td>{number(row.quantity, 0)} / {number(row.available_quantity, 0)}{row.frozen_quantity > 0 && <p className="mt-1 text-xs text-amber-600">当日冻结 {row.frozen_quantity} 股</p>}</td>
          <td>{number(row.avg_cost, 4)}</td><td>{number(row.current_price, 4)}</td><td>{money(row.market_value)}</td>
          <td className={pnlColor(row.pnl)}>{money(row.pnl, true)}</td><td className={pnlColor(row.pnl_pct)}>{row.pnl_pct == null ? '—' : `${row.pnl_pct > 0 ? '+' : ''}${number(row.pnl_pct)}%`}</td>
          <td className="text-xs text-gray-500">{row.current_price == null ? <span className="text-amber-700">{row.price_status === 'stale' ? '报价已过期' : '暂无有效报价'}</span> : <><p>{dateTime(row.price_as_of)}</p><p>{row.price_source || '来源未标记'}{row.price_status === 'cached' ? ' · 缓存' : ''}</p></>}</td>
          <td><button className="text-blue-600 disabled:text-gray-300" disabled={busy || !initialized || row.available_quantity <= 0} onClick={() => { setInst(row.instrument); setDirection('SELL'); setQuantity(String(row.available_quantity)); setPrice(''); tradeFormRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }); }}>录入卖出</button></td>
        </tr>)}</tbody>
      </table></div> : <p className="py-8 text-center text-sm text-gray-400">{loading ? '持仓加载中…' : positions ? '暂无持仓。建立账户后，可录入已发生的成交。' : '持仓数据暂不可用，请刷新重试。'}</p>}
    </section>

    {initialized && <AccountOperations refreshToken={revision} />}

    {initialized && <div className="grid gap-6 xl:grid-cols-[1.5fr_1fr]">
      <section className="account-panel" ref={tradeFormRef}>
        <h2 className="font-semibold text-gray-900">录入实际成交</h2>
        <p className="mb-4 mt-2 text-xs leading-5 text-gray-500">用于补记券商已成交记录。按日期顺序追加；本页不会向券商下单。系统会校验现金、持仓及 T+1。</p>
        <form onSubmit={onTrade}><fieldset disabled={busy} className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <Field label="标的代码"><input className="field-input" value={inst} onChange={event => setInst(event.target.value)} placeholder="600519 / SH600519" required /></Field>
            <Field label="成交日期"><input className="field-input" type="date" min={account?.opened_date} max={today()} value={tradeDate} onChange={event => setTradeDate(event.target.value)} required /></Field>
            <Field label="方向"><select className="field-input" value={direction} onChange={event => setDirection(event.target.value as 'BUY' | 'SELL')}><option value="BUY">买入</option><option value="SELL">卖出</option></select></Field>
            <Field label="股数（正整数）"><input className="field-input" type="number" min="1" step="1" value={quantity} onChange={event => setQuantity(event.target.value)} required /></Field>
            <Field label="实际成交价（元 / 股）"><input className="field-input" type="number" min="0.0001" step="0.0001" value={price} onChange={event => setPrice(event.target.value)} required /></Field>
            <div className="rounded-lg bg-gray-50 px-3 py-2"><p className="text-xs text-gray-500">成交金额（不含费用）</p><p className="mt-1 font-semibold tabular-nums">{quantity && price ? money(Number(quantity) * Number(price)) : '—'}</p></div>
          </div>
          <label className="flex items-center gap-2 text-sm text-gray-600"><input type="checkbox" checked={actualFees} onChange={event => setActualFees(event.target.checked)} />按券商交割单填写实际费用</label>
          {actualFees ? <div className="grid grid-cols-3 gap-3">
            <Field label="佣金（元）"><input className="field-input" type="number" min="0" step="0.01" value={commission} onChange={event => setCommission(event.target.value)} required /></Field>
            <Field label="印花税（元）"><input className="field-input" type="number" min="0" step="0.01" value={stampTax} onChange={event => setStampTax(event.target.value)} required /></Field>
            <Field label="过户费（元）"><input className="field-input" type="number" min="0" step="0.01" value={transferFee} onChange={event => setTransferFee(event.target.value)} required /></Field>
          </div> : <p className="text-xs text-amber-700">费用按系统费率估算，实际盈亏以券商费用核对为准。</p>}
          <Field label="备注（可选）"><input className="field-input" value={note} onChange={event => setNote(event.target.value)} maxLength={500} /></Field>
          <button className="primary-button" type="submit">{busy ? '记账中…' : `记录${direction === 'BUY' ? '买入' : '卖出'}成交`}</button>
        </fieldset></form>
      </section>
      <section className="account-panel">
        <h2 className="font-semibold text-gray-900">资金变动</h2><p className="mb-4 mt-2 text-xs leading-5 text-gray-500">记录银行卡与证券账户之间的入金、出金。资金变动不会计入交易收益。</p>
        <form onSubmit={onCashFlow}><fieldset disabled={busy} className="space-y-4">
          <Field label="方向"><select className="field-input" value={cashDirection} onChange={event => setCashDirection(event.target.value as 'DEPOSIT' | 'WITHDRAW')}><option value="DEPOSIT">入金</option><option value="WITHDRAW">出金</option></select></Field>
          <Field label="金额（元）"><input className="field-input" type="number" min="0.01" step="0.01" value={cashAmount} onChange={event => setCashAmount(event.target.value)} required /></Field>
          <Field label="发生日期"><input className="field-input" type="date" min={account?.opened_date} max={today()} value={cashDate} onChange={event => setCashDate(event.target.value)} required /></Field>
          <Field label="备注（可选）"><input className="field-input" value={cashNote} onChange={event => setCashNote(event.target.value)} maxLength={500} /></Field>
          <button className="primary-button" type="submit">{busy ? '记账中…' : `记录${cashDirection === 'DEPOSIT' ? '入金' : '出金'}`}</button>
        </fieldset></form>
      </section>
    </div>}

    <section className="account-panel">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex gap-1 rounded-lg bg-gray-100 p-1">{([{ key: 'trades', label: '成交账本' }, { key: 'cash', label: '资金流水' }, { key: 'legacy', label: '历史参考' }] as const).map(item => <button key={item.key} onClick={() => { setTab(item.key); setOffset(0); }} className={`rounded px-3 py-1.5 text-sm ${tab === item.key ? 'bg-white font-semibold text-gray-900 shadow-sm' : 'text-gray-500'}`}>{item.label}</button>)}</div>
        {tab === 'trades' && <form className="flex gap-2" onSubmit={event => { event.preventDefault(); setInstrumentFilter(filterInput.trim()); setOffset(0); }}><input className="field-input max-w-44" value={filterInput} onChange={event => setFilterInput(event.target.value)} placeholder="按股票代码筛选" aria-label="按股票代码筛选成交" /><button className="secondary-button" type="submit">筛选</button></form>}
      </div>
      {tab === 'legacy' && <div className="mb-3 space-y-2 text-xs text-amber-700"><p>历史记录仅供核对，未计入当前个人账户。</p>{isAdmin && <label className="flex items-center gap-2"><input type="checkbox" checked={includeUnassigned} onChange={event => { setIncludeUnassigned(event.target.checked); setOffset(0); }} />同时查看尚未归属的历史记录</label>}</div>}
      {ledgerError ? <Notice tone="error">{ledgerError}<button className="ml-3 underline" onClick={() => setRevision(value => value + 1)}>重试</button></Notice> : ledgerLoading ? <p className="py-8 text-center text-sm text-gray-400">正在读取账本…</p> : total === 0 ? <p className="py-8 text-center text-sm text-gray-400">暂无{tab === 'cash' ? '资金流水' : '成交记录'}</p> : <div className="overflow-x-auto">
        {tab === 'cash' ? <table className="ledger-table"><thead><tr>{['发生日期', '方向', '金额', '变动后现金', '备注'].map(label => <th key={label}>{label}</th>)}</tr></thead><tbody>{cashPage?.items.map(row => <tr key={row.id}><td>{row.effective_date}</td><td>{({ INITIAL: '开户本金', DEPOSIT: '入金', WITHDRAW: '出金' })[row.direction] || row.direction}</td><td>{row.direction === 'WITHDRAW' ? '−' : '+'}{money(row.amount)}</td><td>{money(row.cash_after)}</td><td className="max-w-64 truncate" title={row.note || ''}>{row.note || '—'}</td></tr>)}</tbody></table> : <table className="ledger-table"><thead><tr>{['成交日期', '标的', '方向', '股数', '成交价', '成交额', '总费用', '现金变动', '备注'].map(label => <th key={label}>{label}</th>)}</tr></thead><tbody>{tradePage?.items.map(row => <tr key={row.id}><td>{row.trade_date?.slice(0, 10) || '—'}</td><td className="font-mono">{row.instrument}</td><td className={row.direction === 'BUY' ? 'text-red-600' : 'text-green-600'}>{row.direction === 'BUY' ? '买入' : row.direction === 'SELL' ? '卖出' : row.direction}</td><td>{number(row.quantity, 0)}</td><td>{number(row.price, 4)}</td><td>{money(row.amount)}</td><td title={`佣金 ${money(row.commission)} / 印花税 ${money(row.stamp_tax)} / 过户费 ${money(row.transfer_fee)}`}>{money(row.total_fee)}</td><td>{money(row.net_amount, true)}</td><td className="max-w-64 truncate" title={row.note || ''}>{row.note || '—'}</td></tr>)}</tbody></table>}
      </div>}
      <div className="mt-4 flex items-center justify-between gap-3 text-xs text-gray-500"><span>共 {total} 条{total > 0 && ` · ${offset + 1}–${Math.min(offset + PAGE_SIZE, total)}`}</span><div className="flex gap-2"><button className="secondary-button" disabled={ledgerLoading || offset === 0} onClick={() => setOffset(value => Math.max(0, value - PAGE_SIZE))}>上一页</button><button className="secondary-button" disabled={ledgerLoading || offset + PAGE_SIZE >= total} onClick={() => setOffset(value => value + PAGE_SIZE)}>下一页</button></div></div>
    </section>
  </div>;
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return <label className="block min-w-0"><span className="field-label">{label}</span>{children}</label>;
}
function Notice({ tone, children }: { tone: 'error' | 'warning' | 'success'; children: ReactNode }) {
  const colors = tone === 'error' ? 'border-red-200 bg-red-50 text-red-800' : tone === 'success' ? 'border-green-200 bg-green-50 text-green-800' : 'border-amber-200 bg-amber-50 text-amber-800';
  return <div className={`rounded-lg border px-4 py-3 text-sm leading-6 ${colors}`} role={tone === 'error' ? 'alert' : 'status'}>{children}</div>;
}
