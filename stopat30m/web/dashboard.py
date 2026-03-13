"""
Streamlit monitoring dashboard.

Provides real-time visibility into:
- Portfolio state and P&L
- Signal history
- Model evaluation metrics
- Risk monitor status

Run: streamlit run stopat30m/web/dashboard.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path when Streamlit runs this file directly
_project_root = str(Path(__file__).resolve().parents[2])
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="StopAt30M - AI量化监控",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

import numpy as np

OUTPUT_DIR = Path("./output")
SIGNAL_DIR = OUTPUT_DIR / "signals"
MODEL_DIR = OUTPUT_DIR / "models"


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def sidebar() -> dict:
    st.sidebar.title("StopAt30M")
    st.sidebar.markdown("AI量化交易监控系统")

    page = st.sidebar.radio(
        "导航",
        ["概览", "调仓操作", "交易记录", "持仓管理", "信号历史", "模型评估", "风控监控", "因子分析"],
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(f"更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if st.sidebar.button("刷新数据"):
        st.rerun()

    return {"page": page}


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def page_overview() -> None:
    st.title("系统概览")

    from stopat30m.trading.ledger import (
        compute_positions, load_portfolio_nav, load_trades,
    )
    from stopat30m.trading.rebalancer import (
        bare_code, fetch_spot_prices, fetch_stock_names, normalize_instrument,
    )

    trades = load_trades()
    positions = compute_positions(trades)

    # ---- Fetch prices for live valuation ----
    total_market = 0.0
    total_cost = 0.0
    total_unrealized = 0.0
    pos_rows: list[dict] = []

    if not positions.empty:
        instruments = [normalize_instrument(str(r)) for r in positions["instrument"]]
        cache_key = "overview_prices"
        names_key = "overview_names"
        if cache_key not in st.session_state:
            with st.spinner("获取行情..."):
                p = fetch_spot_prices(instruments)
                n = fetch_stock_names(instruments)
                if not p:
                    p = _qlib_fallback_prices(instruments)
                st.session_state[cache_key] = p
                st.session_state[names_key] = n
        prices = st.session_state[cache_key]
        stock_names = st.session_state.get(names_key, {})

        for _, row in positions.iterrows():
            inst = normalize_instrument(str(row["instrument"]))
            qty = int(row["quantity"])
            avg_cost = float(row["avg_cost"])
            cost = float(row["total_cost"])
            price = prices.get(inst, 0)
            mv = qty * price
            pnl = mv - cost
            total_market += mv
            total_cost += cost
            total_unrealized += pnl
            pos_rows.append({
                "代码": bare_code(inst),
                "名称": stock_names.get(inst, ""),
                "持仓": qty,
                "现价": round(price, 2) if price > 0 else "N/A",
                "市值": round(mv, 2),
                "盈亏": round(pnl, 2),
            })

    # ---- Top metrics (consistent with 持仓管理) ----
    capital = float(st.session_state.get("rebal_capital", total_market * 1.1)) if total_market > 0 else 0
    pnl_pct = f"{total_unrealized / total_cost:.2%}" if total_cost > 0 else "0%"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("持仓数量", len(positions))
    col2.metric("持仓市值", f"¥{total_market:,.0f}")
    col3.metric("总成本", f"¥{total_cost:,.0f}")
    col4.metric("浮动盈亏", f"¥{total_unrealized:,.0f}", pnl_pct)

    # ---- Positions summary ----
    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("当前持仓")
        if pos_rows:
            st.dataframe(pd.DataFrame(pos_rows), use_container_width=True, hide_index=True)
        else:
            st.info("当前无持仓")

    with col_right:
        st.subheader("最新信号")
        signals = _load_latest_signals()
        if signals is not None and not signals.empty:
            st.dataframe(signals, use_container_width=True, hide_index=True)
        else:
            st.info("暂无信号数据")

    # ---- NAV chart ----
    nav_df = load_portfolio_nav()
    if not nav_df.empty:
        st.markdown("---")
        st.subheader("净值走势")
        chart_df = nav_df.set_index("date")[["total_value"]]
        chart_df.columns = ["总资产"]
        st.line_chart(chart_df)

    if st.button("刷新行情", key="overview_refresh"):
        st.session_state.pop("overview_prices", None)
        st.session_state.pop("overview_names", None)
        st.rerun()


def page_rebalance() -> None:
    st.title("调仓操作")

    from stopat30m.trading.ledger import (
        add_trades_batch, compute_positions, load_trades,
        save_portfolio_snapshot,
    )
    from stopat30m.trading.rebalancer import (
        RebalancePlan, compute_rebalance_plan, fetch_spot_prices,
        fetch_stock_names, load_latest_signals, normalize_instrument,
    )

    # ---- Step 1: Load signals ----
    st.subheader("① 最新信号")
    signals = load_latest_signals()
    if signals is None or signals.empty:
        st.warning("暂无信号文件。请先运行 `py main.py signal --model-path output/models/model_lgbm.pkl`")
        return

    sig_display = signals.copy()
    sig_names = st.session_state.get("rebal_names", {})
    if sig_names and "instrument" in sig_display.columns:
        sig_display.insert(
            sig_display.columns.get_loc("instrument") + 1, "名称",
            sig_display["instrument"].map(lambda x: sig_names.get(normalize_instrument(str(x)), "")),
        )
    if "score" in sig_display.columns:
        sig_display["score"] = sig_display["score"].map(lambda x: f"{x:.6f}")
    if "weight" in sig_display.columns:
        sig_display["weight"] = sig_display["weight"].map(lambda x: f"{x:.2%}")
    st.dataframe(sig_display, use_container_width=True, hide_index=True)

    target_count = len(signals[signals.get("signal", signals.columns[0]) == "BUY"]) if "signal" in signals.columns else len(signals)
    st.caption(f"目标持仓 {target_count} 只股票")

    # ---- Step 2: Current positions ----
    st.markdown("---")
    st.subheader("② 当前持仓")
    trades = load_trades()
    positions = compute_positions(trades)

    if positions.empty:
        st.info("当前无持仓（首次调仓将全部买入）")
    else:
        st.dataframe(positions.rename(columns={
            "instrument": "代码", "quantity": "数量",
            "avg_cost": "成本价", "total_cost": "成本",
        }), use_container_width=True, hide_index=True)

    # ---- Step 3: Capital input ----
    st.markdown("---")
    st.subheader("③ 资金设置")

    col_cap, col_reserve = st.columns(2)
    with col_cap:
        total_capital = st.number_input(
            "总资金（元）", min_value=10000.0, step=10000.0,
            value=float(st.session_state.get("rebal_capital", 500000.0)),
            format="%.0f",
        )
        st.session_state["rebal_capital"] = total_capital
    with col_reserve:
        cash_reserve = st.slider("现金预留比例 (%)", 0, 50, 0, 1)
        cash_reserve = cash_reserve / 100.0

    # ---- Step 4: Fetch prices & compute plan ----
    if st.button("生成调仓计划", type="primary"):
        all_instruments = list(signals["instrument"].unique())
        if not positions.empty:
            all_instruments += list(positions["instrument"].unique())
        all_instruments = list({normalize_instrument(c) for c in all_instruments})

        with st.spinner("正在获取实时行情..."):
            try:
                prices = fetch_spot_prices(all_instruments)
                names = fetch_stock_names(all_instruments)
            except Exception as e:
                st.warning(f"AKShare 行情获取失败: {e}")
                prices = {}
                names = {}

        price_source = "实时行情"
        if not prices:
            with st.spinner("AKShare 不可用，使用本地 Qlib 最新收盘价..."):
                prices = _qlib_fallback_prices(all_instruments)
                price_source = "本地收盘价（非实时）"

        if not prices:
            st.error("无法获取价格数据（AKShare 网络不通，且本地 Qlib 数据无匹配）")
            return

        st.caption(f"价格来源: {price_source}")

        result = compute_rebalance_plan(
            signals=signals,
            positions=positions,
            total_capital=total_capital,
            prices=prices,
            cash_reserve_pct=cash_reserve,
        )
        st.session_state["rebal_result"] = result
        st.session_state["rebal_prices"] = prices
        st.session_state["rebal_names"] = names

    # ---- Step 5: Display plan ----
    result: RebalancePlan | None = st.session_state.get("rebal_result")
    if result is None:
        return

    prices = st.session_state.get("rebal_prices", {})
    stock_names = st.session_state.get("rebal_names", {})
    plan = result.trades
    cf = result.capital_flow

    # Warnings
    for w in result.warnings:
        st.warning(w)

    # Capital flow summary
    st.markdown("---")
    st.subheader("④ 资金流向")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("当前现金", f"¥{cf.get('available_cash', 0):,.0f}")
    c2.metric("卖出回笼", f"¥{cf.get('sell_proceeds', 0):,.0f}")
    c3.metric("买入支出", f"¥{cf.get('buy_cost', 0):,.0f}")
    c4.metric("调仓后现金", f"¥{cf.get('cash_after_rebalance', 0):,.0f}")

    st.caption(
        f"总资金 ¥{cf.get('total_capital', 0):,.0f} | "
        f"持仓市值 ¥{cf.get('holding_value', 0):,.0f} | "
        f"手续费合计 ¥{cf.get('sell_commission', 0) + cf.get('buy_commission', 0):,.0f} | "
        f"预留目标 ¥{cf.get('reserve_target', 0):,.0f}"
    )

    # Trade details
    st.markdown("---")
    st.subheader("⑤ 调仓明细")

    sells = result.sells
    buys = result.buys
    errors = result.errors

    col1, col2, col3 = st.columns(3)
    sell_total = sells["amount"].sum() if not sells.empty else 0
    buy_total = buys["amount"].sum() if not buys.empty else 0
    col1.metric("卖出", f"¥{sell_total:,.0f}", f"{len(sells)} 笔")
    col2.metric("买入", f"¥{buy_total:,.0f}", f"{len(buys)} 笔")
    if result.hold_unchanged:
        col3.metric("继续持有", f"{len(result.hold_unchanged)} 只")
    else:
        col3.metric("净资金流", f"¥{sell_total - buy_total:,.0f}")

    st.info(
        "**执行建议**: 先完成全部卖出，再执行买入（A股卖出资金 T+0 可用于买入）。\n\n"
        "**关于价格**: 上表价格为实时行情，仅用于计算目标仓位和手数。"
        "实际成交价以券商委托价为准（建议以次日开盘价/集合竞价委托）。"
        "回测已按开盘价模拟执行，回测收益更贴近真实可执行水平。"
    )

    if not sells.empty:
        st.markdown("**卖出（先执行）**")
        _show_plan_table(sells, stock_names)

    if result.hold_unchanged:
        with st.expander(f"继续持有 ({len(result.hold_unchanged)} 只，无需操作)"):
            labels = [f"{c}({stock_names.get(c, '')})" if stock_names.get(c) else c for c in result.hold_unchanged]
            st.write(", ".join(labels))

    if not buys.empty:
        st.markdown("**买入（后执行）**")
        _show_plan_table(buys, stock_names)

    if not errors.empty:
        with st.expander(f"无法操作 ({len(errors)} 只)"):
            st.dataframe(errors[["instrument", "reason"]], use_container_width=True, hide_index=True)

    # ---- Step 6: One-click record ----
    st.markdown("---")
    actionable = plan[plan["direction"].isin(["BUY", "SELL"])]
    if actionable.empty:
        st.success("持仓已与目标完全一致，无需调仓")
        return

    st.subheader("⑥ 录入交易")
    trade_date = st.date_input("交易日期", value=datetime.now())

    record_col1, record_col2 = st.columns(2)
    with record_col1:
        if st.button("一键录入全部交易", type="primary"):
            _record_and_snapshot(actionable, trade_date, total_capital, prices)
    with record_col2:
        sell_only = actionable[actionable["direction"] == "SELL"]
        if not sell_only.empty and not buys.empty:
            if st.button("仅录入卖出（稍后买入）"):
                _record_and_snapshot(sell_only, trade_date, total_capital, prices)


def _record_and_snapshot(
    trades_to_record: pd.DataFrame,
    trade_date,
    total_capital: float,
    prices: dict[str, float],
) -> None:
    """Record trades and save portfolio snapshot."""
    from stopat30m.trading.ledger import (
        add_trades_batch, compute_positions, save_portfolio_snapshot,
    )
    from stopat30m.trading.rebalancer import normalize_instrument

    add_trades_batch(
        trades_plan=trades_to_record,
        date=trade_date.strftime("%Y-%m-%d"),
        note="调仓",
    )
    new_positions = compute_positions()
    if not new_positions.empty:
        holding_value = sum(
            int(row["quantity"]) * prices.get(normalize_instrument(str(row["instrument"])), 0)
            for _, row in new_positions.iterrows()
        )
        cost = new_positions["total_cost"].sum()
    else:
        holding_value = 0
        cost = 0
    cash = total_capital - holding_value
    save_portfolio_snapshot(
        date=trade_date.strftime("%Y-%m-%d"),
        total_value=holding_value + cash,
        total_cost=cost,
        cash=cash,
        unrealized_pnl=holding_value - cost,
    )
    n = len(trades_to_record)
    directions = trades_to_record["direction"].value_counts().to_dict()
    desc = ", ".join(f"{d} {c}笔" for d, c in directions.items())
    st.success(f"已录入 {n} 笔交易（{desc}）")
    st.session_state.pop("rebal_result", None)
    st.rerun()


def _qlib_fallback_prices(instruments: list[str]) -> dict[str, float]:
    """Fetch latest close prices from local Qlib data as fallback."""
    import traceback

    from stopat30m.trading.rebalancer import normalize_instrument

    norm = [normalize_instrument(c) for c in instruments]

    try:
        from stopat30m.data.provider import init_qlib
        init_qlib()
    except Exception as e:
        st.warning(f"Qlib 初始化失败: {e}")
        return {}

    try:
        import qlib.data
        df = qlib.data.D.features(
            instruments=norm, fields=["$close"],
            start_time="2020-01-01", end_time="2099-12-31",
        )
    except Exception:
        st.warning(f"Qlib 查询失败:\n```\n{traceback.format_exc()}\n```")
        return {}

    if df is None or df.empty:
        st.warning(f"Qlib 返回空数据。查询的股票: {norm[:5]}")
        return {}

    latest = df.groupby(level="instrument").tail(1).reset_index()
    prices = {}
    for _, row in latest.iterrows():
        inst = str(row["instrument"]).upper()
        p = float(row["$close"])
        if p > 0:
            prices[inst] = p

    if not prices:
        st.warning(f"Qlib 有数据({len(df)}行)但未匹配到价格。index示例: {list(latest.index[:3])}")
    return prices


def _diagnose_spot_fetch(instruments: list[str]) -> None:
    """Run AKShare fetch with full diagnostics visible on the page."""
    import traceback

    st.markdown("**诊断信息：**")

    try:
        import akshare as ak
        st.write(f"AKShare 版本: {ak.__version__}")
    except Exception as e:
        st.error(f"无法导入 AKShare: {e}")
        return

    for fn_name in ["stock_zh_a_spot_em", "stock_zh_a_spot"]:
        st.markdown(f"--- 尝试 `{fn_name}` ---")
        try:
            fn = getattr(ak, fn_name, None)
            if fn is None:
                st.warning(f"`ak.{fn_name}` 不存在，跳过")
                continue
            df = fn()
            if df is None or df.empty:
                st.warning("返回空数据")
                continue
            st.success(f"成功: {len(df)} 行, 列: {list(df.columns)}")
            st.dataframe(df.head(3), use_container_width=True)

            from stopat30m.trading.rebalancer import bare_code
            sample_codes = [bare_code(c) for c in instruments[:3]]
            matched = df[df.iloc[:, 1].astype(str).isin(sample_codes)]
            st.write(f"匹配测试 {sample_codes}: 找到 {len(matched)} 行")
            if not matched.empty:
                st.dataframe(matched, use_container_width=True)
            return
        except Exception:
            st.error(f"`{fn_name}` 报错:\n```\n{traceback.format_exc()}\n```")


def _show_plan_table(df: pd.DataFrame, names: dict[str, str] | None = None) -> None:
    """Display a rebalance plan sub-table with formatted columns."""
    display = df[["instrument", "direction", "quantity", "price", "amount", "commission", "reason"]].copy()
    if names:
        display.insert(1, "名称", display["instrument"].map(lambda x: names.get(x, "")))
    display = display.rename(columns={
        "instrument": "代码", "direction": "方向", "quantity": "数量",
        "price": "价格", "amount": "金额", "commission": "手续费", "reason": "操作",
    })
    display["金额"] = display["金额"].map(lambda x: f"¥{x:,.0f}")
    display["手续费"] = display["手续费"].map(lambda x: f"¥{x:.2f}")
    st.dataframe(display, use_container_width=True, hide_index=True)


def page_trades() -> None:
    st.title("交易记录")

    from stopat30m.trading.ledger import (
        add_trade, compute_daily_pnl, delete_trade, load_trades,
    )

    # --- Entry form ---
    st.subheader("录入交易")
    with st.form("trade_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            trade_date = st.date_input("日期", value=datetime.now())
            instrument = st.text_input("股票代码", placeholder="例: 600519")
        with col2:
            direction = st.selectbox("方向", ["BUY", "SELL"])
            quantity = st.number_input("数量（股）", min_value=100, step=100, value=100)
        with col3:
            price = st.number_input("成交价", min_value=0.01, step=0.01, value=10.0, format="%.4f")
            commission = st.number_input("手续费", min_value=0.0, step=0.01, value=0.0, format="%.2f")
        note = st.text_input("备注", placeholder="选填")
        submitted = st.form_submit_button("提交")

    if submitted:
        if not instrument.strip():
            st.error("请输入股票代码")
        else:
            add_trade(
                date=trade_date.strftime("%Y-%m-%d"),
                instrument=instrument,
                direction=direction,
                quantity=quantity,
                price=price,
                commission=commission,
                note=note,
            )
            st.success(f"已录入: {direction} {quantity} 股 {instrument.strip().upper()} @ {price:.4f}")
            st.rerun()

    # --- Trade history ---
    st.markdown("---")
    st.subheader("交易历史")
    trades = load_trades()

    if trades.empty:
        st.info("暂无交易记录")
        return

    display_df = trades.copy()
    display_df = display_df.rename(columns={
        "id": "ID", "date": "日期", "instrument": "代码",
        "direction": "方向", "quantity": "数量", "price": "价格",
        "commission": "手续费", "note": "备注", "created_at": "录入时间",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # --- Delete ---
    with st.expander("删除记录"):
        del_id = st.number_input("输入要删除的交易 ID", min_value=1, step=1, value=1)
        if st.button("确认删除"):
            delete_trade(int(del_id))
            st.success(f"已删除交易 ID={del_id}")
            st.rerun()

    # --- Realized P&L ---
    pnl = compute_daily_pnl(trades)
    if not pnl.empty:
        st.markdown("---")
        st.subheader("已实现盈亏")
        cum_pnl = pnl.cumsum()
        st.line_chart(cum_pnl, y_label="累计盈亏 (¥)")
        st.metric("总已实现盈亏", f"¥{pnl.sum():,.2f}")


def page_positions() -> None:
    st.title("持仓管理")

    from stopat30m.trading.ledger import (
        compute_positions, load_portfolio_nav, load_trades,
        save_portfolio_snapshot,
    )
    from stopat30m.trading.rebalancer import (
        bare_code, fetch_spot_prices, fetch_stock_names, normalize_instrument,
    )

    trades = load_trades()
    positions = compute_positions(trades)

    if positions.empty:
        st.info("当前无持仓（请先在「交易记录」或「调仓操作」页面录入买卖）")
        _show_nav_chart()
        return

    # ---- Fetch live prices & names ----
    instruments = [normalize_instrument(str(r)) for r in positions["instrument"]]
    prices_cache_key = "pos_prices"
    names_cache_key = "pos_names"
    if st.button("刷新行情"):
        st.session_state.pop(prices_cache_key, None)
        st.session_state.pop(names_cache_key, None)

    if prices_cache_key not in st.session_state:
        with st.spinner("获取行情..."):
            p = fetch_spot_prices(instruments)
            n = fetch_stock_names(instruments)
            if not p:
                p = _qlib_fallback_prices(instruments)
                if p:
                    st.caption("价格来源: 本地收盘价（AKShare 不可用）")
            st.session_state[prices_cache_key] = p
            st.session_state[names_cache_key] = n
    prices = st.session_state[prices_cache_key]
    stock_names = st.session_state.get(names_cache_key, {})

    # ---- Build enriched positions table ----
    rows = []
    for _, row in positions.iterrows():
        inst = normalize_instrument(str(row["instrument"]))
        qty = int(row["quantity"])
        avg_cost = float(row["avg_cost"])
        total_cost = float(row["total_cost"])
        price = prices.get(inst, 0)
        market_value = qty * price
        unrealized = market_value - total_cost
        pnl_pct = unrealized / total_cost if total_cost > 0 else 0
        rows.append({
            "代码": bare_code(inst),
            "名称": stock_names.get(inst, ""),
            "持仓量": qty,
            "成本价": round(avg_cost, 4),
            "现价": round(price, 2) if price > 0 else "N/A",
            "市值": round(market_value, 2),
            "浮动盈亏": round(unrealized, 2),
            "盈亏比例": pnl_pct,
            "持仓成本": round(total_cost, 2),
        })

    display = pd.DataFrame(rows)
    total_market = sum(r["市值"] for r in rows if isinstance(r["市值"], (int, float)))
    total_cost = display["持仓成本"].sum()
    total_unrealized = sum(r["浮动盈亏"] for r in rows if isinstance(r["浮动盈亏"], (int, float)))

    # ---- Summary metrics ----
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("持仓数量", len(positions))
    col2.metric("总市值", f"¥{total_market:,.0f}")
    col3.metric("总成本", f"¥{total_cost:,.0f}")
    pnl_delta = f"{total_unrealized / total_cost:.2%}" if total_cost > 0 else "0%"
    col4.metric("浮动盈亏", f"¥{total_unrealized:,.0f}", pnl_delta)

    # ---- Styled table ----
    st.markdown("---")
    st.subheader("持仓明细")

    styled = display.copy()
    styled["盈亏比例"] = styled["盈亏比例"].map(lambda x: f"{x:+.2%}")
    styled["浮动盈亏"] = styled["浮动盈亏"].map(lambda x: f"¥{x:+,.0f}")
    styled["市值"] = styled["市值"].map(lambda x: f"¥{x:,.0f}" if isinstance(x, (int, float)) else x)
    styled["持仓成本"] = styled["持仓成本"].map(lambda x: f"¥{x:,.0f}")
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # ---- Allocation pie chart ----
    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("持仓占比")
        if total_market > 0:
            alloc = pd.DataFrame(rows)[["代码", "名称", "市值"]].copy()
            alloc = alloc[alloc["市值"] > 0]
            alloc["标签"] = alloc.apply(
                lambda r: r["名称"] if r["名称"] else r["代码"], axis=1,
            )
            alloc = alloc.set_index("标签")[["市值"]]
            st.bar_chart(alloc, y_label="市值 (¥)")

    with col_right:
        st.subheader("盈亏分布")
        pnl_df = pd.DataFrame(rows)[["代码", "名称", "浮动盈亏"]].copy()
        pnl_df = pnl_df[pnl_df["浮动盈亏"] != 0]
        pnl_df["标签"] = pnl_df.apply(
            lambda r: r["名称"] if r["名称"] else r["代码"], axis=1,
        )
        pnl_df = pnl_df.set_index("标签")[["浮动盈亏"]]
        if not pnl_df.empty:
            st.bar_chart(pnl_df, y_label="浮动盈亏 (¥)")

    # ---- Record snapshot ----
    st.markdown("---")
    capital_input = st.number_input(
        "总资金（用于计算现金和净值）", min_value=0.0,
        value=float(st.session_state.get("rebal_capital", total_market * 1.1)),
        step=10000.0, format="%.0f",
    )
    if st.button("记录今日净值快照"):
        cash = capital_input - total_market
        save_portfolio_snapshot(
            date=datetime.now().strftime("%Y-%m-%d"),
            total_value=capital_input,
            total_cost=total_cost,
            cash=cash,
            unrealized_pnl=total_unrealized,
        )
        st.success(f"已记录净值: 总资产 ¥{capital_input:,.0f}, 持仓 ¥{total_market:,.0f}, 现金 ¥{cash:,.0f}")

    # ---- NAV chart ----
    _show_nav_chart()


def _show_nav_chart() -> None:
    """Display portfolio NAV history chart."""
    from stopat30m.trading.ledger import load_portfolio_nav

    nav = load_portfolio_nav()
    if nav.empty or len(nav) < 2:
        return

    st.markdown("---")
    st.subheader("净值曲线")
    chart_data = nav.set_index("date")[["total_value", "total_cost"]].rename(
        columns={"total_value": "总资产", "total_cost": "总成本"},
    )
    st.line_chart(chart_data, y_label="金额 (¥)")


def page_signals() -> None:
    st.title("信号历史")

    signal_files = sorted(SIGNAL_DIR.glob("signal_*.csv"), reverse=True) if SIGNAL_DIR.exists() else []

    if not signal_files:
        st.info("暂无信号文件")
        return

    selected = st.selectbox("选择信号文件", [f.name for f in signal_files])
    if selected:
        path = SIGNAL_DIR / selected
        df = pd.read_csv(path)
        st.dataframe(df, use_container_width=True)

        st.subheader("信号分布")
        if "signal" in df.columns:
            signal_counts = df["signal"].value_counts()
            st.bar_chart(signal_counts)


def page_model_eval() -> None:
    st.title("模型评估")

    # --- IC Metrics (from training) ---
    metrics_file = OUTPUT_DIR / "metrics.json"
    if metrics_file.exists():
        with open(metrics_file) as f:
            metrics = json.load(f)

        st.subheader("预测能力 (IC)")
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        ic = metrics.get("IC_mean", 0)
        icir = metrics.get("ICIR", 0)
        ric = metrics.get("RankIC_mean", 0)
        ricir = metrics.get("RankICIR", 0)
        col1.metric("IC", f"{ic:.4f}")
        col2.metric("IC Std", f"{metrics.get('IC_std', 0):.4f}")
        col3.metric("ICIR", f"{icir:.4f}")
        col4.metric("Rank IC", f"{ric:.4f}")
        col5.metric("Rank IC Std", f"{metrics.get('RankIC_std', 0):.4f}")
        col6.metric("Rank ICIR", f"{ricir:.4f}")

        _show_ic_assessment(ic, icir, ric, ricir)
    else:
        st.info("暂无 IC 数据。请先运行 `py main.py train`。")

    # --- Backtest Results ---
    st.markdown("---")
    backtest_dir = OUTPUT_DIR / "backtest"
    report_file = backtest_dir / "report.json"
    returns_file = backtest_dir / "returns.csv"

    if not report_file.exists():
        st.info("暂无回测数据。请先运行 `py main.py backtest --model-path output/models/model_lgbm.pkl`。")
        return

    with open(report_file) as f:
        report = json.load(f)

    bt_config = report.get("config", {})

    st.subheader("回测绩效")
    st.caption(
        f"Top-{bt_config.get('top_k', '?')} 等权组合 | "
        f"调仓周期 {bt_config.get('rebalance_freq', '?')} 天 | "
        f"买入费率 {bt_config.get('buy_cost', 0):.2%} | "
        f"卖出费率 {bt_config.get('sell_cost', 0):.2%} | "
        f"基准 {bt_config.get('benchmark', '?')}"
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("年化收益", f"{report.get('annual_return', 0):.2%}")
    col2.metric("夏普比率", f"{report.get('sharpe', 0):.2f}")
    col3.metric("最大回撤", f"{report.get('max_drawdown', 0):.2%}")
    col4.metric("胜率", f"{report.get('win_rate', 0):.2%}")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Sortino", f"{report.get('sortino', 0):.2f}")
    col6.metric("Calmar", f"{report.get('calmar', 0):.2f}")
    col7.metric("盈亏比", f"{report.get('profit_loss_ratio', 0):.2f}")
    if "excess_return" in report:
        col8.metric("超额收益", f"{report['excess_return']:.2%}")
    else:
        col8.metric("交易天数", report.get("total_trades", 0))

    _show_backtest_assessment(report)

    # --- Equity Curve & Drawdown ---
    if returns_file.exists():
        returns_df = pd.read_csv(returns_file, index_col="date", parse_dates=True)

        st.markdown("---")
        st.subheader("净值曲线")

        if "portfolio_cumulative" in returns_df.columns:
            equity_df = returns_df[["portfolio_cumulative"]].rename(columns={"portfolio_cumulative": "策略"})
            if "benchmark_cumulative" in returns_df.columns:
                equity_df["基准"] = returns_df["benchmark_cumulative"]
            st.line_chart(equity_df)

        st.subheader("回撤曲线")
        if "portfolio_cumulative" in returns_df.columns:
            cum = returns_df["portfolio_cumulative"]
            drawdown = (cum - cum.cummax()) / cum.cummax()
            dd_df = pd.DataFrame({"回撤": drawdown})
            st.area_chart(dd_df)

        st.subheader("月度收益")
        if "portfolio" in returns_df.columns:
            monthly = returns_df["portfolio"].resample("ME").apply(
                lambda x: (1 + x).prod() - 1 if len(x) > 0 else 0
            )
            monthly_df = pd.DataFrame({
                "年份": monthly.index.year,
                "月份": monthly.index.month,
                "收益": monthly.values,
            })
            pivot = monthly_df.pivot(index="年份", columns="月份", values="收益")
            st.dataframe(
                pivot.style.format("{:.2%}").background_gradient(cmap="RdYlGn", axis=None),
                use_container_width=True,
            )

    # --- Actual vs Backtest comparison ---
    _show_actual_vs_backtest(returns_file)


def _rate(value: float, thresholds: list[tuple[float, str, str]]) -> tuple[str, str]:
    """Return (level_label, color) for a metric value given ascending thresholds."""
    for threshold, label, color in thresholds:
        if value < threshold:
            return label, color
    return thresholds[-1][1], thresholds[-1][2]


def _show_ic_assessment(ic: float, icir: float, ric: float, ricir: float) -> None:
    """Display an interpretive assessment of IC metrics."""
    rows = []

    level, _ = _rate(abs(ic), [
        (0.02, "弱", "red"), (0.03, "偏弱", "orange"),
        (0.05, "及格", "blue"), (0.08, "良好", "green"), (999, "优秀", "green"),
    ])
    rows.append(("IC (信息系数)", f"{ic:.4f}", level,
                 "预测值与实际收益的相关性。|IC| > 0.03 及格，> 0.05 良好"))

    level, _ = _rate(abs(icir), [
        (0.3, "不稳定", "red"), (0.5, "一般", "orange"),
        (1.0, "稳定", "blue"), (1.5, "很稳定", "green"), (999, "极稳定", "green"),
    ])
    rows.append(("ICIR (信息比率)", f"{icir:.4f}", level,
                 "IC 的稳定性 = IC均值/IC标准差。> 0.5 可用，> 1.0 优秀"))

    level, _ = _rate(abs(ric), [
        (0.03, "弱", "red"), (0.05, "偏弱", "orange"),
        (0.08, "及格", "blue"), (0.12, "良好", "green"), (999, "优秀", "green"),
    ])
    rows.append(("Rank IC (排序相关)", f"{ric:.4f}", level,
                 "预测排序与实际排序的相关性。比 IC 更鲁棒，> 0.05 及格，> 0.1 良好"))

    level, _ = _rate(abs(ricir), [
        (0.3, "不稳定", "red"), (0.5, "一般", "orange"),
        (1.0, "稳定", "blue"), (1.5, "很稳定", "green"), (999, "极稳定", "green"),
    ])
    rows.append(("Rank ICIR", f"{ricir:.4f}", level,
                 "Rank IC 的稳定性。> 0.5 可用，> 1.0 优秀"))

    with st.expander("指标解读", expanded=True):
        for name, value, level, desc in rows:
            st.markdown(f"**{name}** = {value} → **{level}**  \n{desc}")

        # Overall verdict
        if abs(ric) >= 0.05 and abs(ricir) >= 0.5:
            st.success("综合评价：模型具备有效预测能力，可用于实盘信号生成")
        elif abs(ric) >= 0.03:
            st.warning("综合评价：模型有一定预测能力但不够稳定，建议优化后再实盘使用")
        else:
            st.error("综合评价：模型预测能力较弱，不建议直接用于实盘")


def _show_backtest_assessment(report: dict) -> None:
    """Display an interpretive assessment of backtest performance metrics."""
    annual = report.get("annual_return", 0)
    sharpe = report.get("sharpe", 0)
    mdd = abs(report.get("max_drawdown", 0))
    sortino = report.get("sortino", 0)
    win_rate = report.get("win_rate", 0)
    calmar = report.get("calmar", 0)

    rows = []

    level, _ = _rate(annual, [
        (0, "亏损", "red"), (0.05, "微利", "orange"),
        (0.10, "一般", "blue"), (0.20, "良好", "green"), (999, "优秀", "green"),
    ])
    rows.append(("年化收益", f"{annual:.2%}", level,
                 "< 5% 不如货基，5-15% 一般，15-30% 良好，> 30% 优秀"))

    level, _ = _rate(sharpe, [
        (0, "负收益风险比", "red"), (0.5, "偏低", "orange"),
        (1.0, "及格", "blue"), (1.5, "良好", "green"), (999, "优秀", "green"),
    ])
    rows.append(("夏普比率", f"{sharpe:.2f}", level,
                 "每承担1单位风险获得的超额收益。< 0.5 差，0.5-1.0 一般，> 1.5 优秀"))

    level, _ = _rate(mdd, [
        (0.10, "优秀", "green"), (0.20, "可接受", "blue"),
        (0.30, "偏高", "orange"), (999, "危险", "red"),
    ])
    rows.append(("最大回撤", f"{mdd:.2%}", level,
                 "最大亏损幅度。< 10% 优秀，10-20% 可接受，> 30% 需警惕"))

    level, _ = _rate(win_rate, [
        (0.40, "偏低", "red"), (0.45, "一般", "orange"),
        (0.50, "及格", "blue"), (0.55, "良好", "green"), (999, "优秀", "green"),
    ])
    rows.append(("胜率", f"{win_rate:.2%}", level,
                 "盈利天数占比。量化策略 > 50% 即可（配合盈亏比）"))

    level, _ = _rate(calmar, [
        (0, "负值", "red"), (0.5, "偏低", "orange"),
        (1.0, "及格", "blue"), (2.0, "良好", "green"), (999, "优秀", "green"),
    ])
    rows.append(("Calmar (收益/回撤)", f"{calmar:.2f}", level,
                 "年化收益 / 最大回撤。> 1 表示收益能覆盖回撤，> 2 良好"))

    with st.expander("回测指标解读", expanded=True):
        for name, value, level, desc in rows:
            st.markdown(f"**{name}** = {value} → **{level}**  \n{desc}")

        # Overall
        if sharpe >= 1.0 and mdd <= 0.20 and annual > 0.10:
            st.success("综合评价：策略表现良好，风险可控，可考虑实盘验证")
        elif sharpe >= 0.5 and annual > 0:
            st.warning("综合评价：策略有正收益但风险收益比一般，建议继续优化")
        else:
            st.error("综合评价：策略表现不佳，不建议实盘使用")


def page_risk() -> None:
    st.title("风控监控")

    portfolio = _load_portfolio_state()
    risk = portfolio.get("risk_status", {})

    if risk.get("circuit_breaker"):
        st.error("熔断触发 - 所有交易已暂停")
    else:
        st.success("系统运行正常")

    col1, col2, col3 = st.columns(3)
    col1.metric("峰值权益", f"¥{risk.get('peak_equity', 0):,.0f}")
    col2.metric("今日盈亏", f"{risk.get('daily_pnl', 0):.2%}")
    col3.metric("拒绝订单数", risk.get("rejected_orders", 0))

    st.markdown("---")
    st.subheader("风控参数")

    try:
        from stopat30m.config import get
        risk_cfg = get("risk") or {}
        trading_cfg = get("trading") or {}
        params = {
            "最大回撤限制": f"{risk_cfg.get('max_drawdown', 0.15):.0%}",
            "日亏损限制": f"{risk_cfg.get('max_daily_loss', 0.03):.0%}",
            "单笔止损": f"{trading_cfg.get('stop_loss_pct', 0.08):.0%}",
            "单笔止盈": f"{trading_cfg.get('take_profit_pct', 0.15):.0%}",
            "最大集中度": f"{risk_cfg.get('max_concentration', 0.10):.0%}",
            "熔断阈值": f"{risk_cfg.get('circuit_breaker_loss', 0.08):.0%}",
        }
        st.json(params)
    except Exception:
        st.info("无法加载风控配置")


def page_factor_analysis() -> None:
    st.title("因子分析")

    try:
        from stopat30m.factors.expressions import get_factor_groups
        groups = get_factor_groups()

        st.subheader("因子库统计")
        total = sum(len(v) for v in groups.values())
        st.write(f"扩展因子总数: **{total}** (不含Alpha158基础158个)")

        for group_name, factors in groups.items():
            with st.expander(f"{group_name} ({len(factors)} 个因子)"):
                rows = [{"名称": name, "表达式": expr[:80] + ("..." if len(expr) > 80 else "")} for expr, name in factors]
                st.dataframe(pd.DataFrame(rows), use_container_width=True)

    except Exception as e:
        st.error(f"加载因子库失败: {e}")


def _show_actual_vs_backtest(bt_returns_file: Path) -> None:
    """Compare actual manual trades P&L against backtest equity curve."""
    from stopat30m.trading.ledger import compute_daily_pnl, load_trades

    trades = load_trades()
    actual_pnl = compute_daily_pnl(trades)

    if actual_pnl.empty:
        return

    st.markdown("---")
    st.subheader("实盘 vs 回测 对比")

    actual_cum = actual_pnl.cumsum()
    actual_cum.name = "实盘累计盈亏"

    if bt_returns_file.exists():
        bt_df = pd.read_csv(bt_returns_file, index_col="date", parse_dates=True)
        if "portfolio" in bt_df.columns:
            # Normalize backtest returns to same initial capital scale
            bt_cum_ret = (1 + bt_df["portfolio"]).cumprod()
            # Scale: assume initial capital = total buy volume from trades
            buy_trades = trades[trades["direction"] == "BUY"]
            if not buy_trades.empty:
                initial_capital = (buy_trades["quantity"].astype(float) * buy_trades["price"].astype(float)).sum()
            else:
                initial_capital = 100_000
            bt_pnl = (bt_cum_ret - 1) * initial_capital
            bt_pnl.name = "回测累计盈亏"

            compare_df = pd.DataFrame({
                "实盘": actual_cum,
                "回测": bt_pnl,
            })
            st.line_chart(compare_df)
            return

    st.line_chart(actual_cum)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _load_portfolio_state() -> dict:
    state_file = OUTPUT_DIR / "portfolio_state.json"
    if state_file.exists():
        with open(state_file) as f:
            return json.load(f)
    return {
        "equity": 1_000_000,
        "position_count": 0,
        "total_position_value": 0,
        "positions": {},
        "risk_status": {},
    }


def _load_latest_signals() -> pd.DataFrame | None:
    if not SIGNAL_DIR.exists():
        return None
    files = sorted(SIGNAL_DIR.glob("signal_*.csv"), reverse=True)
    if not files:
        return None
    return pd.read_csv(files[0])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    nav = sidebar()
    page = nav["page"]

    page_map = {
        "概览": page_overview,
        "调仓操作": page_rebalance,
        "交易记录": page_trades,
        "持仓管理": page_positions,
        "信号历史": page_signals,
        "模型评估": page_model_eval,
        "风控监控": page_risk,
        "因子分析": page_factor_analysis,
    }

    page_fn = page_map.get(page, page_overview)
    page_fn()


if __name__ == "__main__":
    main()
else:
    main()
