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
PAPER_DIR = OUTPUT_DIR / "paper"
BACKTESTS_DIR = OUTPUT_DIR / "backtests"


# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------

_CUSTOM_CSS = """
<style>
/* ---- Metric cards ---- */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(128,128,128,0.03) 0%, rgba(128,128,128,0.07) 100%);
    border: 1px solid rgba(128,128,128,0.12);
    border-radius: 10px;
    padding: 14px 18px 10px 18px;
    transition: border-color 0.2s;
}
[data-testid="stMetric"]:hover {
    border-color: rgba(99,102,241,0.4);
}

/* ---- Sidebar ---- */
section[data-testid="stSidebar"] > div:first-child { padding-top: 1rem; }

.sidebar-brand {
    text-align: center;
    padding: 0.4rem 0 0.6rem 0;
}
.sidebar-brand h2 {
    margin: 0;
    font-size: 1.35rem;
    letter-spacing: 0.04em;
    font-weight: 700;
}
.sidebar-brand .subtitle {
    font-size: 0.78rem;
    opacity: 0.55;
    margin-top: 2px;
}

.sidebar-section-label {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    opacity: 0.45;
    padding: 10px 0 4px 4px;
    font-weight: 600;
}

.sidebar-mode-badge {
    display: inline-block;
    font-size: 0.72rem;
    font-weight: 600;
    padding: 2px 10px;
    border-radius: 10px;
    margin-top: 4px;
}
.badge-paper {
    background: rgba(234,179,8,0.15);
    color: #b45309;
    border: 1px solid rgba(234,179,8,0.3);
}
.badge-live {
    background: rgba(34,197,94,0.15);
    color: #15803d;
    border: 1px solid rgba(34,197,94,0.3);
}

/* ---- Watermark box ---- */
.watermark-box {
    background: rgba(128,128,128,0.06);
    border-radius: 8px;
    padding: 10px 14px;
    font-size: 0.8em;
    line-height: 1.7;
}

/* ---- Page header status bar ---- */
.page-status-bar {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 16px;
    border-radius: 8px;
    font-size: 0.85rem;
    margin-bottom: 1rem;
}
.status-bar-paper {
    background: rgba(234,179,8,0.08);
    border: 1px solid rgba(234,179,8,0.2);
}
.status-bar-live {
    background: rgba(34,197,94,0.08);
    border: 1px solid rgba(34,197,94,0.2);
}

/* ---- Sidebar footer ---- */
.sidebar-footer {
    font-size: 0.72rem;
    opacity: 0.4;
    text-align: center;
    padding: 8px 0;
}
</style>
"""

st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Table styling helpers (A-share: red=profit, green=loss)
# ---------------------------------------------------------------------------

_COLOR_POS = "color: #cf222e"  # red – profit in A-share
_COLOR_NEG = "color: #1a7f37"  # green – loss in A-share
_COLOR_NEUTRAL = ""


def _pnl_text_color(val: object) -> str:
    """Return CSS text color for a P&L value (A-share convention)."""
    try:
        v = float(val)
    except (TypeError, ValueError):
        return _COLOR_NEUTRAL
    if v > 0:
        return _COLOR_POS
    if v < 0:
        return _COLOR_NEG
    return _COLOR_NEUTRAL


def _style_pnl(styler: "pd.io.formats.style.Styler", cols: list[str]) -> "pd.io.formats.style.Styler":
    """Apply A-share P&L coloring to specified columns, compatible with pandas >=2.0."""
    _map = getattr(styler, "map", None) or styler.applymap
    return _map(_pnl_text_color, subset=cols)


# ---------------------------------------------------------------------------
# Page registry: (key, icon, label)
# ---------------------------------------------------------------------------

_PORTFOLIO_PAGES = [
    ("overview",    "📊", "投资概览"),
    ("trading",     "💹", "交易中心"),
    ("trades",      "📝", "交易记录"),
]

_SYSTEM_PAGES = [
    ("signals",     "📡", "信号中心"),
    ("model_eval",  "🧪", "模型评估"),
    ("signal_bt",   "🧭", "信号回测"),
    ("account_bt",  "🏦", "账户回测"),
    ("risk",        "🛡️", "风控监控"),
    ("factors",     "🔬", "因子分析"),
]

_ALL_PAGES = _PORTFOLIO_PAGES + _SYSTEM_PAGES
_PAGE_LABELS = {key: f"{icon} {label}" for key, icon, label in _ALL_PAGES}
_LABEL_TO_KEY = {v: k for k, v in _PAGE_LABELS.items()}


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def sidebar() -> dict:
    # ---- Brand ----
    st.sidebar.markdown(
        "<div class='sidebar-brand'>"
        "<h2>StopAt30M</h2>"
        "<div class='subtitle'>AI 量化交易监控系统</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    # ---- Mode badge ----
    is_paper = _paper_mode()
    badge_cls = "badge-paper" if is_paper else "badge-live"
    badge_text = "模拟交易" if is_paper else "手动记账"
    st.sidebar.markdown(
        f"<div style='text-align:center'>"
        f"<span class='sidebar-mode-badge {badge_cls}'>{badge_text}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )

    st.sidebar.markdown("")

    # ---- Portfolio section ----
    st.sidebar.markdown(
        "<div class='sidebar-section-label'>我的投资组合</div>",
        unsafe_allow_html=True,
    )
    portfolio_labels = [_PAGE_LABELS[k] for k, _, _ in _PORTFOLIO_PAGES]
    p_idx = _get_section_index("_port_idx", 0)
    portfolio_choice = st.sidebar.radio(
        "投资组合", portfolio_labels,
        index=p_idx, key="nav_portfolio", label_visibility="collapsed",
        on_change=_on_portfolio_click,
    )

    # ---- System section ----
    st.sidebar.markdown(
        "<div class='sidebar-section-label'>系统分析</div>",
        unsafe_allow_html=True,
    )
    system_labels = [_PAGE_LABELS[k] for k, _, _ in _SYSTEM_PAGES]
    s_idx = _get_section_index("_sys_idx", 0)
    system_choice = st.sidebar.radio(
        "系统分析", system_labels,
        index=s_idx, key="nav_system", label_visibility="collapsed",
        on_change=_on_system_click,
    )

    # Resolve active page from session
    section = st.session_state.get("_active_section", "portfolio")
    if section == "portfolio":
        page_key = _LABEL_TO_KEY.get(portfolio_choice, "overview")
    else:
        page_key = _LABEL_TO_KEY.get(system_choice, "signals")

    st.sidebar.markdown("---")

    # ---- Data watermark ----
    _render_sidebar_watermark()

    # ---- Refresh + timestamp ----
    st.sidebar.markdown("")
    ts = datetime.now().strftime("%H:%M:%S")
    col_ts, col_btn = st.sidebar.columns([3, 2])
    col_ts.caption(f"🕐 {ts}")
    if col_btn.button("刷新", use_container_width=True):
        _invalidate_price_cache()
        st.rerun()

    # ---- Footer ----
    st.sidebar.markdown(
        "<div class='sidebar-footer'>StopAt30M v0.1</div>",
        unsafe_allow_html=True,
    )

    return {"page": page_key}


def _get_section_index(key: str, default: int) -> int:
    return st.session_state.get(key, default)


def _on_portfolio_click() -> None:
    st.session_state["_active_section"] = "portfolio"
    portfolio_labels = [_PAGE_LABELS[k] for k, _, _ in _PORTFOLIO_PAGES]
    chosen = st.session_state.get("nav_portfolio", portfolio_labels[0])
    st.session_state["_port_idx"] = portfolio_labels.index(chosen) if chosen in portfolio_labels else 0


def _on_system_click() -> None:
    st.session_state["_active_section"] = "system"
    system_labels = [_PAGE_LABELS[k] for k, _, _ in _SYSTEM_PAGES]
    chosen = st.session_state.get("nav_system", system_labels[0])
    st.session_state["_sys_idx"] = system_labels.index(chosen) if chosen in system_labels else 0


def _render_sidebar_watermark() -> None:
    """Show data watermark and active source count in sidebar."""
    try:
        from stopat30m.data.provider import get_data_dir
        meta_path = get_data_dir() / "data_meta.json"
        if not meta_path.exists():
            return

        meta = json.loads(meta_path.read_text())
        trusted = meta.get("trusted_until", "")
        stocks = meta.get("stocks", {})
        total = len(stocks)
        active = sum(1 for s in stocks.values() if s.get("status") != "delisted")
        delisted = total - active

        lines = []
        if trusted:
            today = datetime.now().strftime("%Y-%m-%d")
            lag = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(trusted, "%Y-%m-%d")).days
            color = "#cf222e" if lag > 3 else ("#e3b341" if lag > 0 else "#1a7f37")
            lines.append(f"可信水位线: <b style='color:{color}'>{trusted}</b>")
            if lag > 0:
                lines.append(f"落后: {lag} 天")
        if total:
            lines.append(f"股票: {active} 活跃 / {delisted} 退市")

        if lines:
            st.sidebar.markdown(
                f"<div class='watermark-box'>{'<br>'.join(lines)}</div>",
                unsafe_allow_html=True,
            )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Shared price/name cache (across all pages, per session)
# ---------------------------------------------------------------------------

def _get_prices_and_names(
    instruments: list[str],
) -> tuple[dict[str, float], dict[str, str]]:
    """Fetch prices and names for given instruments, using session-level cache.

    Only fetches instruments not already cached. Merges results into the
    shared cache so subsequent pages/calls don't re-fetch.
    """
    from stopat30m.trading.rebalancer import (
        fetch_spot_prices, fetch_stock_names, normalize_instrument,
    )

    if "shared_prices" not in st.session_state:
        st.session_state["shared_prices"] = {}
        st.session_state["shared_names"] = {}

    cached_prices = st.session_state["shared_prices"]
    cached_names = st.session_state["shared_names"]

    normed = [normalize_instrument(c) for c in instruments]
    missing = [inst for inst in normed if inst not in cached_prices]

    if missing:
        with st.spinner(f"获取 {len(missing)} 只股票行情..."):
            new_prices = fetch_spot_prices(missing)
            new_names = fetch_stock_names(missing)
            cached_prices.update(new_prices)
            cached_names.update(new_names)

    return cached_prices, cached_names


def _invalidate_price_cache() -> None:
    """Clear the shared price cache (e.g. on refresh button)."""
    st.session_state.pop("shared_prices", None)
    st.session_state.pop("shared_names", None)


def _get_paper_broker():
    """Load PaperBroker from disk (returns None if not initialized)."""
    from stopat30m.trading.broker.paper import PaperBroker

    broker = PaperBroker(state_dir=PAPER_DIR)
    return broker if broker.is_initialized else None


def _paper_mode() -> bool:
    """True if a paper trading account exists."""
    return (PAPER_DIR / "paper_state.json").exists()


def _page_header(title: str, subtitle: str = "") -> None:
    """Render a page title with optional contextual status bar."""
    st.title(title)
    is_paper = _paper_mode()
    cls = "status-bar-paper" if is_paper else "status-bar-live"
    mode_label = "📋 模拟交易模式" if is_paper else "📒 手动记账模式"
    parts = [f"<b>{mode_label}</b>"]
    if subtitle:
        parts.append(f"<span style='opacity:0.7'>| {subtitle}</span>")
    st.markdown(
        f"<div class='page-status-bar {cls}'>{'  '.join(parts)}</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def page_overview() -> None:
    from stopat30m.trading.rebalancer import bare_code, normalize_instrument

    broker = _get_paper_broker()
    use_paper = broker is not None
    _page_header("投资概览", "实时持仓与盈亏一览")

    # ---- Load positions from broker or legacy ledger ----
    total_market = 0.0
    total_cost = 0.0
    total_unrealized = 0.0
    total_cash = 0.0
    realized_pnl = 0.0
    pos_rows: list[dict] = []
    pos_count = 0

    if use_paper:
        acct = broker.get_account()
        positions = broker.get_positions()
        instruments = list(positions.keys())
        if instruments:
            prices, stock_names = _get_prices_and_names(instruments)
            broker.update_prices(prices)
        else:
            prices, stock_names = {}, {}

        for inst, pos in positions.items():
            price = prices.get(inst, pos.current_price)
            mv = pos.quantity * price
            pnl = mv - pos.total_cost
            total_market += mv
            total_cost += pos.total_cost
            total_unrealized += pnl
            pos_rows.append({
                "代码": bare_code(inst),
                "名称": stock_names.get(inst, ""),
                "持仓": pos.quantity,
                "现价": round(price, 2) if price > 0 else "N/A",
                "市值": round(mv, 2),
                "盈亏": round(pnl, 2),
            })
        total_cash = acct.cash
        realized_pnl = acct.realized_pnl
        pos_count = len(positions)
    else:
        from stopat30m.trading.ledger import compute_positions, load_trades

        trades = load_trades()
        ledger_positions = compute_positions(trades)
        pos_count = len(ledger_positions)

        if not ledger_positions.empty:
            instruments = [str(r) for r in ledger_positions["instrument"]]
            prices, stock_names = _get_prices_and_names(instruments)
            for _, row in ledger_positions.iterrows():
                inst = normalize_instrument(str(row["instrument"]))
                qty = int(row["quantity"])
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

    # ---- Top metrics ----
    pnl_pct = f"{total_unrealized / total_cost:.2%}" if total_cost > 0 else "0%"

    if use_paper:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("持仓数量", pos_count)
        c2.metric("总权益", f"¥{acct.equity:,.0f}")
        c3.metric("现金", f"¥{total_cash:,.0f}")
        c4.metric("浮动盈亏", f"¥{total_unrealized:,.0f}", pnl_pct, delta_color="inverse")
        c5.metric("已实现盈亏", f"¥{realized_pnl:,.0f}", delta_color="inverse")
    else:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("持仓数量", pos_count)
        col2.metric("持仓市值", f"¥{total_market:,.0f}")
        col3.metric("总成本", f"¥{total_cost:,.0f}")
        col4.metric("浮动盈亏", f"¥{total_unrealized:,.0f}", pnl_pct, delta_color="inverse")

    # ---- Positions summary ----
    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("当前持仓")
        if pos_rows:
            df_overview = pd.DataFrame(pos_rows)
            styled_overview = _style_pnl(df_overview.style, ["盈亏"]).format({
                "现价": lambda x: f"¥{x:.2f}" if isinstance(x, (int, float)) else x,
                "市值": "¥{:,.0f}",
                "盈亏": lambda x: f"¥{x:+,.0f}",
            })
            st.dataframe(styled_overview, use_container_width=True, hide_index=True)
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
    st.markdown("---")
    if use_paper:
        nav_list = broker.get_nav_history()
        if len(nav_list) >= 2:
            st.subheader("净值走势")
            nav_df = pd.DataFrame([n.to_dict() for n in nav_list])
            nav_df["date"] = pd.to_datetime(nav_df["date"])
            chart_df = nav_df.set_index("date")[["equity"]]
            chart_df.columns = ["总权益"]
            st.line_chart(chart_df)
    else:
        from stopat30m.trading.ledger import load_portfolio_nav
        nav_df = load_portfolio_nav()
        if not nav_df.empty:
            st.subheader("净值走势")
            chart_df = nav_df.set_index("date")[["total_value"]]
            chart_df.columns = ["总资产"]
            st.line_chart(chart_df)

    if st.button("刷新行情", key="overview_refresh"):
        _invalidate_price_cache()
        st.rerun()


def page_trading() -> None:
    """Unified trading center: positions + rebalance + manual trade in tabs."""
    from stopat30m.trading.rebalancer import bare_code, normalize_instrument

    broker = _get_paper_broker()
    use_paper = broker is not None
    _page_header("交易中心", "持仓查看、调仓执行与手动交易")

    tab_pos, tab_rebal, tab_manual = st.tabs(["📊 持仓总览", "⚖️ 智能调仓", "✏️ 手动交易"])

    with tab_pos:
        _tab_positions(broker, use_paper, bare_code, normalize_instrument)
    with tab_rebal:
        _tab_rebalance(broker, use_paper, normalize_instrument)
    with tab_manual:
        _tab_manual_trade(broker, use_paper)


# ---------------------------------------------------------------------------
# Quick sell from positions
# ---------------------------------------------------------------------------

def _quick_sell(sellable: list[dict], broker, use_paper: bool) -> None:
    st.markdown("---")
    with st.expander("⚡ 快捷卖出"):
        labels = [s["label"] for s in sellable]
        sel_idx = st.selectbox(
            "选择持仓", range(len(labels)),
            format_func=lambda i: f"{labels[i]}  (可卖 {sellable[i]['available']})",
            key="qs_select",
        )
        chosen = sellable[sel_idx]
        max_qty = chosen["available"]

        col1, col2 = st.columns(2)
        with col1:
            qty = st.number_input(
                "卖出数量（股）", min_value=100, max_value=max_qty,
                step=100, value=min(max_qty, 100), key="qs_qty",
            )
        with col2:
            if use_paper:
                otype = st.selectbox("委托类型", ["MARKET", "LIMIT"], key="qs_otype")
            else:
                otype = "LEDGER"
                sell_price = st.number_input(
                    "成交价", min_value=0.01, step=0.01,
                    value=round(chosen["price"], 2) if chosen["price"] > 0 else 10.0,
                    format="%.4f", key="qs_price",
                )

        limit_price = None
        if use_paper and otype == "LIMIT":
            limit_price = st.number_input(
                "限价", min_value=0.01, step=0.01,
                value=round(chosen["price"], 2) if chosen["price"] > 0 else 10.0,
                format="%.4f", key="qs_limit",
            )

        if st.button("确认卖出", type="primary", key="qs_submit"):
            if use_paper:
                _quick_sell_paper(broker, chosen["instrument"], qty, otype, limit_price)
            else:
                _quick_sell_ledger(chosen["instrument"], qty, sell_price)


def _quick_sell_paper(broker, instrument: str, qty: int, otype: str, limit_price: float | None) -> None:
    from stopat30m.trading.models import Order, OrderDirection, OrderStatus, OrderType
    from stopat30m.trading.rebalancer import fetch_spot_prices

    if otype == "MARKET":
        prices = fetch_spot_prices([instrument])
        if prices:
            broker.update_prices(prices)
        else:
            st.error("无法获取实时价格")
            return

    order = Order(
        instrument=instrument,
        direction=OrderDirection.SELL,
        order_type=OrderType(otype),
        quantity=qty,
        limit_price=limit_price,
    )
    result = broker.submit_order(order)
    if result.status == OrderStatus.FILLED:
        st.success(
            f"卖出成交: {result.instrument} {result.filled_quantity} 股 "
            f"@ ¥{result.avg_fill_price:.4f}, 费用 ¥{result.total_fee:.2f}"
        )
        st.rerun()
    else:
        st.error(f"卖出被拒绝: {result.reject_reason}")


def _quick_sell_ledger(instrument: str, qty: int, price: float) -> None:
    from stopat30m.trading.ledger import add_trade

    add_trade(
        date=datetime.now().strftime("%Y-%m-%d"),
        instrument=instrument, direction="SELL",
        quantity=qty, price=price,
        commission=0.0, note="快捷卖出",
    )
    st.success(f"已录入卖出: {instrument} {qty} 股 @ ¥{price:.4f}")
    st.rerun()


# ---------------------------------------------------------------------------
# Tab: 持仓总览
# ---------------------------------------------------------------------------

def _tab_positions(broker, use_paper: bool, bare_code, normalize_instrument) -> None:
    if st.button("刷新行情", key="pos_refresh"):
        _invalidate_price_cache()

    rows: list[dict] = []
    total_market = 0.0
    total_cost = 0.0
    total_unrealized = 0.0
    sellable: list[dict] = []

    if use_paper:
        bp = broker.get_positions()
        if not bp:
            st.info("当前无持仓")
            _show_nav_chart_paper(broker)
            return

        instruments = list(bp.keys())
        prices, stock_names = _get_prices_and_names(instruments)
        broker.update_prices(prices)

        for inst, pos in bp.items():
            price = prices.get(inst, pos.current_price)
            mv = pos.quantity * price
            unrealized = mv - pos.total_cost
            pnl_pct = unrealized / pos.total_cost if pos.total_cost > 0 else 0
            total_market += mv
            total_cost += pos.total_cost
            total_unrealized += unrealized
            name = stock_names.get(inst, "")
            rows.append({
                "代码": bare_code(inst), "名称": name,
                "持仓量": pos.quantity, "可卖": pos.available_quantity, "冻结": pos.frozen_quantity,
                "成本价": round(pos.avg_cost, 4),
                "现价": round(price, 2) if price > 0 else "N/A",
                "市值": round(mv, 2), "浮动盈亏": round(unrealized, 2),
                "盈亏比例": pnl_pct, "持仓成本": round(pos.total_cost, 2),
            })
            if pos.available_quantity > 0:
                label = f"{bare_code(inst)} {name}".strip() if name else bare_code(inst)
                sellable.append({
                    "label": label, "instrument": inst,
                    "available": pos.available_quantity, "price": price,
                })
        pos_count = len(bp)
    else:
        from stopat30m.trading.ledger import compute_positions, load_trades
        trades = load_trades()
        positions = compute_positions(trades)
        if positions.empty:
            st.info("当前无持仓")
            _show_nav_chart()
            return

        instruments = [str(r) for r in positions["instrument"]]
        prices, stock_names = _get_prices_and_names(instruments)
        for _, row in positions.iterrows():
            inst = normalize_instrument(str(row["instrument"]))
            qty = int(row["quantity"])
            avg_cost = float(row["avg_cost"])
            tc = float(row["total_cost"])
            price = prices.get(inst, 0)
            mv = qty * price
            unrealized = mv - tc
            pnl_pct = unrealized / tc if tc > 0 else 0
            total_market += mv
            total_cost += tc
            total_unrealized += unrealized
            name = stock_names.get(inst, "")
            rows.append({
                "代码": bare_code(inst), "名称": name,
                "持仓量": qty, "成本价": round(avg_cost, 4),
                "现价": round(price, 2) if price > 0 else "N/A",
                "市值": round(mv, 2), "浮动盈亏": round(unrealized, 2),
                "盈亏比例": pnl_pct, "持仓成本": round(tc, 2),
            })
            if qty > 0:
                label = f"{bare_code(inst)} {name}".strip() if name else bare_code(inst)
                sellable.append({
                    "label": label, "instrument": inst,
                    "available": qty, "price": price,
                })
        pos_count = len(positions)

    # Metrics
    if use_paper:
        acct = broker.get_account()
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("持仓数量", pos_count)
        c2.metric("总权益", f"¥{acct.equity:,.0f}")
        c3.metric("现金", f"¥{acct.cash:,.0f}")
        pnl_delta = f"{total_unrealized / total_cost:.2%}" if total_cost > 0 else "0%"
        c4.metric("浮动盈亏", f"¥{total_unrealized:,.0f}", pnl_delta, delta_color="inverse")
        c5.metric("已实现盈亏", f"¥{acct.realized_pnl:,.0f}", delta_color="inverse")
    else:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("持仓数量", pos_count)
        col2.metric("总市值", f"¥{total_market:,.0f}")
        col3.metric("总成本", f"¥{total_cost:,.0f}")
        pnl_delta = f"{total_unrealized / total_cost:.2%}" if total_cost > 0 else "0%"
        col4.metric("浮动盈亏", f"¥{total_unrealized:,.0f}", pnl_delta, delta_color="inverse")

    # Position table
    st.markdown("---")
    display = pd.DataFrame(rows)
    pnl_cols = ["浮动盈亏", "盈亏比例"]
    fmt_dict = {
        "成本价": "¥{:.4f}",
        "现价": lambda x: f"¥{x:.2f}" if isinstance(x, (int, float)) else x,
        "市值": "¥{:,.0f}", "浮动盈亏": lambda x: f"¥{x:+,.0f}",
        "盈亏比例": "{:+.2%}", "持仓成本": "¥{:,.0f}",
    }
    st.dataframe(
        _style_pnl(display.style, pnl_cols).format(fmt_dict),
        use_container_width=True, hide_index=True,
    )

    # Quick sell
    if sellable:
        _quick_sell(sellable, broker, use_paper)

    # Charts
    st.markdown("---")
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("持仓占比")
        if total_market > 0:
            alloc = pd.DataFrame(rows)[["代码", "名称", "市值"]].copy()
            alloc = alloc[alloc["市值"] > 0]
            alloc["标签"] = alloc.apply(lambda r: r["名称"] if r["名称"] else r["代码"], axis=1)
            st.bar_chart(alloc.set_index("标签")[["市值"]], y_label="市值 (¥)")
    with col_right:
        st.subheader("盈亏分布")
        pnl_df = pd.DataFrame(rows)[["代码", "名称", "浮动盈亏"]].copy()
        pnl_df = pnl_df[pnl_df["浮动盈亏"] != 0]
        pnl_df["标签"] = pnl_df.apply(lambda r: r["名称"] if r["名称"] else r["代码"], axis=1)
        if not pnl_df.empty:
            st.bar_chart(pnl_df.set_index("标签")[["浮动盈亏"]], y_label="浮动盈亏 (¥)")

    # Settlement
    st.markdown("---")
    if use_paper:
        if st.button("执行 T+1 日终结算 (settle)", type="primary", key="settle_btn"):
            today = datetime.now().strftime("%Y-%m-%d")
            snap = broker.settle_day(today)
            st.success(f"结算完成 {today}: 权益 ¥{snap.equity:,.0f}, 日收益 {snap.daily_return:+.2%}")
            st.rerun()
        _show_nav_chart_paper(broker)
    else:
        from stopat30m.trading.ledger import save_portfolio_snapshot
        capital_input = st.number_input(
            "总资金（用于计算现金和净值）", min_value=0.0,
            value=float(st.session_state.get("rebal_capital", total_market * 1.1)),
            step=10000.0, format="%.0f", key="pos_capital",
        )
        if st.button("记录今日净值快照", key="snapshot_btn"):
            cash = capital_input - total_market
            save_portfolio_snapshot(
                date=datetime.now().strftime("%Y-%m-%d"),
                total_value=capital_input, total_cost=total_cost,
                cash=cash, unrealized_pnl=total_unrealized,
            )
            st.success(f"已记录净值: 总资产 ¥{capital_input:,.0f}, 持仓 ¥{total_market:,.0f}, 现金 ¥{cash:,.0f}")
        _show_nav_chart()


# ---------------------------------------------------------------------------
# Tab: 智能调仓
# ---------------------------------------------------------------------------

def _tab_rebalance(broker, use_paper: bool, normalize_instrument) -> None:
    from stopat30m.trading.rebalancer import (
        RebalancePlan, compute_rebalance_plan, load_latest_signals,
    )

    signals = load_latest_signals()
    if signals is None or signals.empty:
        st.warning("暂无信号文件。请先运行 `py main.py signal --model-path output/models/model_lgbm.pkl`")
        return

    # Signal display
    st.subheader("目标信号")
    sig_display = signals.copy()
    sig_names = st.session_state.get("shared_names", {})
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

    # Current positions
    st.markdown("---")
    if use_paper:
        acct = broker.get_account()
        positions = _paper_positions_to_df(broker.get_positions())
        st.info(f"Paper 账户权益: ¥{acct.equity:,.0f} (现金 ¥{acct.cash:,.0f})")
        default_capital = acct.equity
    else:
        from stopat30m.trading.ledger import compute_positions, load_trades
        positions = compute_positions(load_trades())
        default_capital = float(st.session_state.get("rebal_capital", 500000.0))

    # Capital & reserve
    col_cap, col_reserve = st.columns(2)
    with col_cap:
        total_capital = st.number_input(
            "总资金（元）", min_value=10000.0, step=10000.0,
            value=float(st.session_state.get("rebal_capital", default_capital)),
            format="%.0f", key="rebal_capital_input",
        )
        st.session_state["rebal_capital"] = total_capital
    with col_reserve:
        cash_reserve = st.slider("现金预留比例 (%)", 0, 50, 0, 1, key="rebal_reserve") / 100.0

    # Compute plan
    if st.button("生成调仓计划", type="primary", key="gen_plan_btn"):
        all_instruments = list(signals["instrument"].unique())
        if use_paper:
            all_instruments += list(broker.get_positions().keys())
        elif not positions.empty:
            all_instruments += list(positions["instrument"].unique())
        all_instruments = list(set(all_instruments))

        _invalidate_price_cache()
        prices, names = _get_prices_and_names(all_instruments)
        if not prices:
            st.error("所有实时数据源均不可用，请检查网络后重试")
            return
        if use_paper:
            broker.update_prices(prices)

        result = compute_rebalance_plan(
            signals=signals, positions=positions,
            total_capital=total_capital, prices=prices,
            cash_reserve_pct=cash_reserve,
        )
        st.session_state["rebal_result"] = result

    # Display plan
    result: RebalancePlan | None = st.session_state.get("rebal_result")
    if result is None:
        return

    prices = st.session_state.get("shared_prices", {})
    stock_names = st.session_state.get("shared_names", {})
    plan = result.trades
    cf = result.capital_flow

    for w in result.warnings:
        st.warning(w)

    if use_paper and not result.sells.empty:
        bp = broker.get_positions()
        for _, row in result.sells.iterrows():
            inst = str(row["instrument"])
            pos = bp.get(inst)
            if pos and pos.frozen_quantity > 0:
                sell_qty = int(row["quantity"])
                if sell_qty > pos.available_quantity:
                    st.warning(
                        f"T+1 限制: {inst} 持仓 {pos.quantity}, "
                        f"冻结 {pos.frozen_quantity}, 可卖 {pos.available_quantity}, 计划卖 {sell_qty}"
                    )

    st.markdown("---")
    st.subheader("资金流向")
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

    st.markdown("---")
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

    if not sells.empty:
        st.markdown("**卖出（先执行）**")
        _show_plan_table(sells, stock_names)
    if result.hold_unchanged:
        with st.expander(f"继续持有 ({len(result.hold_unchanged)} 只)"):
            labels = [f"{c}({stock_names.get(c, '')})" if stock_names.get(c) else c for c in result.hold_unchanged]
            st.write(", ".join(labels))
    if not buys.empty:
        st.markdown("**买入（后执行）**")
        _show_plan_table(buys, stock_names)
    if not errors.empty:
        with st.expander(f"无法操作 ({len(errors)} 只)"):
            st.dataframe(errors[["instrument", "reason"]], use_container_width=True, hide_index=True)

    # Execute
    st.markdown("---")
    actionable = plan[plan["direction"].isin(["BUY", "SELL"])]
    if actionable.empty:
        st.success("持仓已与目标完全一致，无需调仓")
        return

    if use_paper:
        rc1, rc2 = st.columns(2)
        with rc1:
            if st.button("一键执行全部交易", type="primary", key="exec_all"):
                _execute_via_broker(broker, result, prices, sells_only=False)
        with rc2:
            if not sells.empty and not buys.empty:
                if st.button("仅执行卖出", key="exec_sell"):
                    _execute_via_broker(broker, result, prices, sells_only=True)
    else:
        trade_date = st.date_input("交易日期", value=datetime.now(), key="rebal_date")
        rc1, rc2 = st.columns(2)
        with rc1:
            if st.button("一键录入全部交易", type="primary", key="record_all"):
                _record_and_snapshot(actionable, trade_date, total_capital, prices)
        with rc2:
            if not sells.empty and not buys.empty:
                if st.button("仅录入卖出", key="record_sell"):
                    sell_only = actionable[actionable["direction"] == "SELL"]
                    _record_and_snapshot(sell_only, trade_date, total_capital, prices)


# ---------------------------------------------------------------------------
# Tab: 手动交易
# ---------------------------------------------------------------------------

def _tab_manual_trade(broker, use_paper: bool) -> None:
    if use_paper:
        _manual_trade_paper(broker)
    else:
        _manual_trade_ledger()


def _manual_trade_paper(broker) -> None:
    """Manual buy/sell via PaperBroker."""
    from stopat30m.trading.models import Order, OrderDirection, OrderStatus, OrderType
    from stopat30m.trading.rebalancer import fetch_spot_prices, normalize_instrument

    acct = broker.get_account()
    st.info(f"账户现金: ¥{acct.cash:,.0f}")

    with st.form("manual_paper_trade", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            instrument = st.text_input("股票代码", placeholder="例: 600519", key="mp_inst")
            direction = st.selectbox("方向", ["BUY", "SELL"], key="mp_dir")
        with col2:
            quantity = st.number_input("数量（股）", min_value=100, step=100, value=100, key="mp_qty")
            order_type = st.selectbox("委托类型", ["MARKET", "LIMIT"], key="mp_type")
        limit_price = None
        if order_type == "LIMIT":
            limit_price = st.number_input("限价", min_value=0.01, step=0.01, value=10.0, format="%.4f", key="mp_lp")
        submitted = st.form_submit_button("提交委托", type="primary")

    if submitted:
        if not instrument.strip():
            st.error("请输入股票代码")
            return

        inst = normalize_instrument(instrument.strip())

        if order_type == "MARKET":
            with st.spinner("获取实时价格..."):
                prices = fetch_spot_prices([inst])
            if prices:
                broker.update_prices(prices)
            else:
                st.error("无法获取实时价格")
                return

        order = Order(
            instrument=inst,
            direction=OrderDirection(direction),
            order_type=OrderType(order_type),
            quantity=quantity,
            limit_price=limit_price,
        )
        result = broker.submit_order(order)

        if result.status == OrderStatus.FILLED:
            st.success(
                f"成交: {result.direction.value} {result.instrument} "
                f"{result.filled_quantity} 股 @ ¥{result.avg_fill_price:.4f}, "
                f"费用 ¥{result.total_fee:.2f}"
            )
        else:
            st.error(f"委托被拒绝: {result.reject_reason}")


def _manual_trade_ledger() -> None:
    """Manual buy/sell via CSV ledger."""
    from stopat30m.trading.ledger import add_trade

    with st.form("manual_ledger_trade", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            trade_date = st.date_input("日期", value=datetime.now(), key="ml_date")
            instrument = st.text_input("股票代码", placeholder="例: 600519", key="ml_inst")
        with col2:
            direction = st.selectbox("方向", ["BUY", "SELL"], key="ml_dir")
            quantity = st.number_input("数量（股）", min_value=100, step=100, value=100, key="ml_qty")
        with col3:
            price = st.number_input("成交价", min_value=0.01, step=0.01, value=10.0, format="%.4f", key="ml_price")
            commission = st.number_input("手续费", min_value=0.0, step=0.01, value=0.0, format="%.2f", key="ml_comm")
        note = st.text_input("备注", placeholder="选填", key="ml_note")
        submitted = st.form_submit_button("录入交易", type="primary")

    if submitted:
        if not instrument.strip():
            st.error("请输入股票代码")
        else:
            add_trade(
                date=trade_date.strftime("%Y-%m-%d"),
                instrument=instrument, direction=direction,
                quantity=quantity, price=price,
                commission=commission, note=note,
            )
            st.success(f"已录入: {direction} {quantity} 股 {instrument.strip().upper()} @ {price:.4f}")
            st.rerun()


def _paper_positions_to_df(positions: dict) -> pd.DataFrame:
    """Convert PaperBroker positions dict to DataFrame matching rebalancer format."""
    if not positions:
        return pd.DataFrame(columns=["instrument", "quantity", "avg_cost", "total_cost"])
    rows = []
    for inst, pos in positions.items():
        rows.append({
            "instrument": inst,
            "quantity": pos.quantity,
            "avg_cost": pos.avg_cost,
            "total_cost": pos.total_cost,
        })
    return pd.DataFrame(rows)


def _execute_via_broker(broker, result, prices, sells_only: bool) -> None:
    """Execute rebalance plan through PaperBroker."""
    from stopat30m.trading.executor import execute_plan
    from stopat30m.trading.models import OrderStatus

    broker.update_prices(prices)
    report = execute_plan(result, broker, sells_only=sells_only)

    if report.total_rejected > 0:
        for o in report.rejected:
            st.warning(f"被拒绝: {o.instrument} {o.direction.value} {o.quantity} — {o.reject_reason}")

    if report.total_filled > 0:
        acct = broker.get_account()
        st.success(
            f"已执行 {report.total_filled} 笔交易 "
            f"({report.sell_count}卖/{report.buy_count}买), "
            f"手续费 ¥{report.total_commission + report.total_stamp_tax:,.2f}, "
            f"账户现金 ¥{acct.cash:,.0f}"
        )
    else:
        st.info("无交易被执行")

    st.session_state.pop("rebal_result", None)
    st.rerun()


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
    """Pure read-only trade history."""
    broker = _get_paper_broker()
    use_paper = broker is not None
    _page_header("交易记录", "全部成交与委托历史")

    if use_paper:
        _trades_history_paper(broker)
    else:
        _trades_history_ledger()


def _trades_history_paper(broker) -> None:
    fills = broker.get_fills()
    orders = broker.get_orders()

    acct = broker.get_account()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("总交易笔数", len(fills))
    c2.metric("已实现盈亏", f"¥{acct.realized_pnl:,.0f}", delta_color="inverse")
    c3.metric("累计佣金", f"¥{acct.total_commission:,.2f}")
    c4.metric("累计印花税", f"¥{acct.total_stamp_tax:,.2f}")

    st.markdown("---")
    st.subheader("成交记录")
    if not fills:
        st.info("暂无成交记录")
        return

    fill_rows = []
    for f in reversed(fills):
        fill_rows.append({
            "时间": f.timestamp, "代码": f.instrument, "方向": f.direction.value,
            "数量": f.quantity, "成交价": round(f.price, 4),
            "金额": round(f.quantity * f.price, 2),
            "佣金": round(f.commission, 2), "印花税": round(f.stamp_tax, 2),
            "过户费": round(f.transfer_fee, 2),
        })
    st.dataframe(pd.DataFrame(fill_rows), use_container_width=True, hide_index=True)

    from stopat30m.trading.models import OrderStatus
    rejected = [o for o in orders if o.status == OrderStatus.REJECTED]
    if rejected:
        with st.expander(f"被拒绝的委托 ({len(rejected)} 笔)"):
            rej_rows = [{
                "时间": o.created_at, "代码": o.instrument, "方向": o.direction.value,
                "数量": o.quantity, "原因": o.reject_reason,
            } for o in reversed(rejected)]
            st.dataframe(pd.DataFrame(rej_rows), use_container_width=True, hide_index=True)


def _trades_history_ledger() -> None:
    from stopat30m.trading.ledger import compute_daily_pnl, delete_trade, load_trades

    trades = load_trades()
    if trades.empty:
        st.info("暂无交易记录。请在「交易中心 → 手动交易」页面录入。")
        return

    display_df = trades.rename(columns={
        "id": "ID", "date": "日期", "instrument": "代码",
        "direction": "方向", "quantity": "数量", "price": "价格",
        "commission": "手续费", "note": "备注", "created_at": "录入时间",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    with st.expander("删除记录"):
        del_id = st.number_input("输入要删除的交易 ID", min_value=1, step=1, value=1, key="del_trade_id")
        if st.button("确认删除", key="del_trade_btn"):
            delete_trade(int(del_id))
            st.success(f"已删除交易 ID={del_id}")
            st.rerun()

    pnl = compute_daily_pnl(trades)
    if not pnl.empty:
        st.markdown("---")
        st.subheader("已实现盈亏")
        cum_pnl = pnl.cumsum()
        st.line_chart(cum_pnl, y_label="累计盈亏 (¥)")
        total_pnl = pnl.sum()
        st.metric(
            "总已实现盈亏", f"¥{total_pnl:,.2f}",
            delta=f"¥{total_pnl:+,.2f}" if total_pnl != 0 else None,
            delta_color="inverse",
        )


def _show_nav_chart_paper(broker) -> None:
    """Display NAV history from PaperBroker."""
    nav_list = broker.get_nav_history()
    if len(nav_list) < 2:
        return

    st.markdown("---")
    st.subheader("净值曲线")
    nav_df = pd.DataFrame([n.to_dict() for n in nav_list])
    nav_df["date"] = pd.to_datetime(nav_df["date"])
    chart_data = nav_df.set_index("date")[["equity", "cash"]].rename(
        columns={"equity": "总权益", "cash": "现金"},
    )
    st.line_chart(chart_data, y_label="金额 (¥)")


def _show_nav_chart() -> None:
    """Display portfolio NAV history chart (legacy ledger)."""
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
    st.title("📡 信号中心")

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


def _list_run_dirs(kind: str) -> list[Path]:
    root = BACKTESTS_DIR / kind
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir()], reverse=True)


def _select_run_dir(kind: str, title: str) -> Path | None:
    run_dirs = _list_run_dirs(kind)
    if not run_dirs:
        st.info(title)
        return None
    selected = st.selectbox(
        "选择回测运行",
        [p.name for p in run_dirs],
        key=f"run_select_{kind}",
    )
    return next((p for p in run_dirs if p.name == selected), None)


def _read_run_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def _read_run_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def page_signal_backtest() -> None:
    st.title("🧭 信号回测")
    run_dir = _select_run_dir("signal", "暂无信号回测数据。请先运行 `py main.py signal-backtest --model-path ...`。")
    if run_dir is None:
        return

    summary = _read_run_json(run_dir / "summary.json")
    config = _read_run_json(run_dir / "config.json")
    topk_report = _read_run_json(run_dir / "topk_report.json")
    daily_ic = _read_run_csv(run_dir / "daily_ic.csv")
    daily_rank_ic = _read_run_csv(run_dir / "daily_rank_ic.csv")
    horizon_stats = _read_run_csv(run_dir / "horizon_stats.csv")
    bucket_returns = _read_run_csv(run_dir / "bucket_returns.csv")
    topk_returns = _read_run_csv(run_dir / "topk_returns.csv")
    turnover = _read_run_csv(run_dir / "turnover.csv")
    coverage = _read_run_csv(run_dir / "coverage.csv")
    signal_history = _read_run_csv(run_dir / "signal_history.csv")

    eval_horizon = int(config.get("eval_horizon", 5))
    st.caption(
        f"方法 {config.get('method', '?')} | Top-{config.get('top_k', '?')} | "
        f"调仓频率 {config.get('rebalance_freq', '?')} 天 | "
        f"区间 {config.get('test_start', '?')} ~ {config.get('test_end', '?')}"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("平均候选数", f"{summary.get('avg_candidate_count', 0):,.1f}")
    c2.metric("平均换手", f"{summary.get('avg_turnover', 0):.2%}")
    c3.metric(f"{eval_horizon}D IC", f"{summary.get(f'ic_{eval_horizon}d_mean', 0):.4f}")
    c4.metric(f"{eval_horizon}D RankIC", f"{summary.get(f'rank_ic_{eval_horizon}d_mean', 0):.4f}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric(f"BUY {eval_horizon}D 收益", f"{summary.get(f'buy_mean_return_{eval_horizon}d', 0):.2%}")
    c6.metric(f"BUY {eval_horizon}D 胜率", f"{summary.get(f'buy_win_rate_{eval_horizon}d', 0):.2%}")
    c7.metric("分层 Top-Bottom", f"{summary.get('top_bottom_spread', 0):.2%}")
    c8.metric("Top-K 年化", f"{topk_report.get('annual_return', 0):.2%}")

    st.markdown("---")
    if not topk_returns.empty:
        chart = topk_returns.copy()
        chart["date"] = pd.to_datetime(chart["date"])
        chart = chart.set_index("date")
        line_cols = []
        if "portfolio_cumulative" in chart.columns:
            chart["策略组合"] = chart["portfolio_cumulative"]
            line_cols.append("策略组合")
        if "benchmark_cumulative" in chart.columns:
            chart["基准"] = chart["benchmark_cumulative"]
            line_cols.append("基准")
        if line_cols:
            st.subheader("Top-K 组合净值")
            st.line_chart(chart[line_cols])

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("IC 时序")
        if not daily_ic.empty:
            ic_cols = [c for c in daily_ic.columns if c.startswith("ic_")]
            ic_df = daily_ic.copy()
            ic_df["date"] = pd.to_datetime(ic_df["date"])
            st.line_chart(ic_df.set_index("date")[ic_cols])
    with col_right:
        st.subheader("RankIC 时序")
        if not daily_rank_ic.empty:
            ric_cols = [c for c in daily_rank_ic.columns if c.startswith("rank_ic_")]
            ric_df = daily_rank_ic.copy()
            ric_df["date"] = pd.to_datetime(ric_df["date"])
            st.line_chart(ric_df.set_index("date")[ric_cols])

    st.markdown("---")
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("换手与覆盖")
        if not turnover.empty:
            tdf = turnover.copy()
            tdf["date"] = pd.to_datetime(tdf["date"])
            tdf = tdf.set_index("date")
            st.line_chart(tdf[["turnover"]])
        if not coverage.empty:
            cdf = coverage.copy()
            cdf["date"] = pd.to_datetime(cdf["date"])
            st.line_chart(cdf.set_index("date")[["candidate_count"]])
    with col_right:
        st.subheader("分层收益")
        if not bucket_returns.empty:
            bucket_avg = bucket_returns.groupby("bucket")["mean_return"].mean().to_frame("平均收益")
            st.bar_chart(bucket_avg)

    st.markdown("---")
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("期限收益统计")
        if not horizon_stats.empty:
            hz = horizon_stats.groupby("horizon").agg(
                平均收益=("mean_return", "mean"),
                中位数=("median_return", "mean"),
                胜率=("win_rate", "mean"),
                期数=("date", "count"),
            )
            st.dataframe(
                hz.style.format({
                    "平均收益": "{:.2%}",
                    "中位数": "{:.2%}",
                    "胜率": "{:.2%}",
                }),
                use_container_width=True,
            )
    with col_right:
        st.subheader("配置信息")
        st.json(config)

    st.markdown("---")
    st.subheader("最近信号明细")
    if not signal_history.empty:
        display = signal_history.sort_values(["date", "score"], ascending=[False, False]).head(200)
        st.dataframe(display, use_container_width=True, hide_index=True)


def page_account_backtest() -> None:
    st.title("🏦 账户回测")
    run_dir = _select_run_dir("account", "暂无账户回测数据。请先运行 `py main.py account-backtest --model-path ...`。")
    if run_dir is None:
        return

    report = _read_run_json(run_dir / "report.json")
    config = _read_run_json(run_dir / "config.json")
    nav = _read_run_csv(run_dir / "nav.csv")
    orders = _read_run_csv(run_dir / "orders.csv")
    fills = _read_run_csv(run_dir / "fills.csv")
    positions = _read_run_csv(run_dir / "positions.csv")
    risk_events = _read_run_csv(run_dir / "risk_events.csv")
    turnover = _read_run_csv(run_dir / "turnover.csv")

    st.caption(
        f"执行价 {config.get('execution_price', '?')} | Top-{config.get('top_k', '?')} | "
        f"调仓频率 {config.get('rebalance_freq', '?')} 天 | "
        f"初始资金 ¥{config.get('initial_capital', 0):,.0f}"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("期末权益", f"¥{report.get('ending_equity', 0):,.0f}")
    c2.metric("年化收益", f"{report.get('annual_return', 0):.2%}")
    c3.metric("夏普比率", f"{report.get('sharpe', 0):.2f}")
    c4.metric("最大回撤", f"{report.get('max_drawdown', 0):.2%}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("总费用", f"¥{report.get('total_fees', 0):,.2f}")
    c6.metric("委托数", int(report.get("order_count", 0)))
    c7.metric("拒单数", int(report.get("rejected_orders", 0)))
    c8.metric("部分成交", int(report.get("partial_fills", 0)))

    c9, c10 = st.columns(2)
    c9.metric("现金利用率", f"{report.get('cash_utilization', 0):.2%}")
    c10.metric("日均成交额", f"¥{report.get('avg_daily_turnover', 0):,.0f}")

    st.markdown("---")
    if not nav.empty:
        nav["date"] = pd.to_datetime(nav["date"])
        nav = nav.set_index("date")
        st.subheader("账户净值")
        line_cols = []
        if "equity_cumulative" in nav.columns:
            line_cols.append("equity_cumulative")
        if "benchmark_cumulative" in nav.columns:
            line_cols.append("benchmark_cumulative")
        if line_cols:
            chart = nav[line_cols].rename(columns={
                "equity_cumulative": "账户",
                "benchmark_cumulative": "基准",
            })
            st.line_chart(chart)

        st.subheader("现金与市值")
        cash_mv = nav[["cash", "market_value"]].rename(columns={"cash": "现金", "market_value": "持仓市值"})
        st.area_chart(cash_mv)

        st.subheader("回撤")
        drawdown = nav["equity"] / nav["equity"].cummax() - 1
        st.area_chart(pd.DataFrame({"回撤": drawdown}))

    st.markdown("---")
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("每日成交")
        if not turnover.empty:
            tdf = turnover.copy()
            tdf["date"] = pd.to_datetime(tdf["date"])
            st.bar_chart(tdf.set_index("date")[["turnover"]])
    with col_right:
        st.subheader("持仓数")
        if not nav.empty:
            st.line_chart(nav[["positions_count"]].rename(columns={"positions_count": "持仓数"}))

    st.markdown("---")
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("订单明细")
        if not orders.empty:
            display = orders.sort_values(["date", "created_at"], ascending=[False, False]).head(200)
            st.dataframe(display, use_container_width=True, hide_index=True)
    with col_right:
        st.subheader("成交明细")
        if not fills.empty:
            display = fills.sort_values(["date", "timestamp"], ascending=[False, False]).head(200)
            st.dataframe(display, use_container_width=True, hide_index=True)

    if not risk_events.empty:
        st.markdown("---")
        st.subheader("事件与异常")
        st.dataframe(risk_events.sort_values("date", ascending=False), use_container_width=True, hide_index=True)

    if not positions.empty:
        st.markdown("---")
        st.subheader("最新持仓快照")
        latest_date = positions["date"].max()
        latest_positions = positions[positions["date"] == latest_date].sort_values("market_value", ascending=False)
        st.dataframe(latest_positions, use_container_width=True, hide_index=True)


def page_model_eval() -> None:
    st.title("🧪 模型评估")

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
    st.title("🛡️ 风控监控")

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
    st.title("🔬 因子分析")

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
        "overview": page_overview,
        "trading": page_trading,
        "trades": page_trades,
        "signals": page_signals,
        "model_eval": page_model_eval,
        "signal_bt": page_signal_backtest,
        "account_bt": page_account_backtest,
        "risk": page_risk,
        "factors": page_factor_analysis,
    }

    page_fn = page_map.get(page, page_overview)
    page_fn()


if __name__ == "__main__":
    main()
else:
    main()
