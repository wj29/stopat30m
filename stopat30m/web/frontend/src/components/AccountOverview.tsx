import type { AccountSummary } from '../api/trading';
import { dateTime, money, pnlColor } from '../utils/format';

export default function AccountOverview({ account, compact = false }: { account: AccountSummary; compact?: boolean }) {
  const metrics = [
    { label: '总资产', value: account.equity, note: '可用现金 + 持仓市值' },
    { label: '可用现金', value: account.cash, note: '按资金与成交账本计算' },
    { label: '持仓市值', value: account.market_value, note: `${account.total_positions} 只持仓` },
    { label: '累计盈亏', value: account.total_pnl, note: '总资产 − 净投入，已扣费用', pnl: true },
    ...(!compact ? [
      { label: '已实现盈亏', value: account.realized_pnl, note: '已卖出部分的净盈亏', pnl: true },
      { label: '持仓浮动盈亏', value: account.unrealized_pnl, note: '市值 − 剩余持仓成本', pnl: true },
      { label: '累计费用', value: account.total_fees, note: '佣金、印花税与过户费' },
      { label: '持仓成本', value: account.total_cost, note: '包含买入费用' },
    ] : []),
  ];
  return <div>
    <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">{metrics.map(item => <div className="account-panel min-w-0" key={item.label}>
      <p className="text-xs text-gray-500">{item.label}</p>
      <p className={`mt-2 break-words text-xl font-semibold tabular-nums sm:text-2xl ${item.pnl ? pnlColor(item.value) : 'text-gray-900'}`}>{money(item.value, item.pnl)}</p>
      <p className="mt-1 text-[11px] text-gray-400">{item.note}</p>
    </div>)}</div>
    <div className="mt-3 flex flex-wrap justify-between gap-2 text-xs text-gray-400">
      <span>{account.name} · 人民币 · 开户 {account.opened_date} · 净投入 {money(account.net_deposits)}（含本金 {money(account.initial_capital)}）</span>
      <span>估值检查 {dateTime(account.as_of)}（上海时间）</span>
    </div>
    {!account.valuation_complete && <p className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800" role="status">{account.total_positions - account.priced_positions} 只持仓暂无有效报价，总市值、总资产与浮动盈亏暂不计算。现金和已实现盈亏仍可核对。</p>}
    {(account.valuation_warnings || []).length > 0 && <ul className="mt-3 space-y-1 text-xs leading-5 text-amber-700">{account.valuation_warnings?.map(warning => <li key={warning}>• {warning}</li>)}</ul>}
  </div>;
}
