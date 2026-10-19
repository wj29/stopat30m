"""Read-only, after-cost summaries of committed isolated A-share paper runs."""
from __future__ import annotations

import csv
from datetime import datetime, time, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo


def _now():
    return datetime.now(timezone.utc)


def _number(value):
    if isinstance(value, bool):
        raise ValueError("Paper report amounts must be finite numbers")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("Paper report amounts must be finite numbers") from None
    if not result.is_finite():
        raise ValueError("Paper report amounts must be finite numbers")
    return result


def _amount(value):
    return str(_number(value).quantize(Decimal("0.01")))


def _time(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Paper report timestamps need a timezone")
    return result


def _completed_opens(state):
    """Only one committed receipt per session, never failed attempt history."""
    trusted = {(str(Path(row["path"]).resolve()), row["sha256"]) for row in state["receipts"]}
    sessions = set()
    result = []
    for key, saved in sorted(state["completed"].items()):
        if saved.get("phase") != "open" or saved.get("status") != "completed":
            continue
        session = saved["session"]
        if key != session + "_open" or session in sessions:
            raise ValueError("Completed paper opening identity is not unique/canonical")
        path = Path(saved["receipt_path"]).resolve()
        digest = saved["receipt_sha256"]
        raw = path.read_bytes()
        receipt = json.loads(raw)
        if ((str(path), digest) not in trusted or hashlib.sha256(raw).hexdigest() != digest
                or receipt != {field: value for field, value in saved.items()
                    if field not in {"receipt_path", "receipt_sha256"}}
                or receipt.get("session") != session or receipt.get("phase") != "open"
                or receipt.get("status") != "completed"):
            raise ValueError("Completed paper opening differs from its actual committed receipt")
        sessions.add(session)
        result.append(receipt)
    return result


def _daily_valuations(state):
    # Current runner already replaces its same-session NAV. Explicitly retain
    # that rule when reading any legacy fixture containing earlier partial rows.
    days = {}
    for row in state["valuations"]:
        session = row["session"]
        if datetime.fromisoformat(session).date().isoformat() != session:
            raise ValueError("Invalid saved paper valuation session")
        observed = _time(row["observed_at"])
        previous = days.get(session)
        if previous is None or observed >= _time(previous["observed_at"]):
            days[session] = row
    return [days[key] for key in sorted(days)]


def _build_report(paper_dir):
    from stopat30m.ashare_forward import ashare_forward_status
    status = ashare_forward_status(paper_dir)
    manifest, state = status["manifest"], status["state"]
    initial = _number(manifest["initial_capital_per_strategy"])
    if initial <= 0:
        raise ValueError("Paper initial capital must be positive")
    opens, valuations = _completed_opens(state), _daily_valuations(state)
    accounts, curves = {}, []
    for name in manifest["strategies"]:
        totals = {key: Decimal(0) for key in ("commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost")}
        trade_count, execution_sessions, latest_fill = 0, 0, None
        for receipt in opens:
            account = receipt["accounts"][name]
            trades = account["trades"]
            if trades:
                execution_sessions += 1
            for trade in trades:
                if (account["status"] != "completed" or trade.get("status") != "simulated_fill"
                        or trade.get("session") != receipt["session"] or trade.get("live_order") is not False):
                    raise ValueError("A committed paper fill has invalid execution provenance")
                values = {key: _number(trade[key]) for key in totals}
                if (any(value < 0 for value in values.values())
                        or values["total_fee"] != values["commission"] + values["transfer_fee"] + values["stamp_tax"]):
                    raise ValueError("Committed paper fill fee components are inconsistent")
                for key in totals:
                    totals[key] += values[key]
                trade_count += 1
                latest_fill = max(latest_fill or receipt["session"], receipt["session"])
        peak, max_drawdown, incomplete = initial, Decimal(0), 0
        last = None
        curves.append({"strategy": name, "session": None, "observed_at": manifest["created_at"],
            "basis": "initial_cash", "nav_lower": _amount(initial), "nav_estimate": _amount(initial),
            "nav_upper": _amount(initial), "valuation_complete": True, "net_pnl_lower": "0.00",
            "return_lower": 0.0, "drawdown_lower": 0.0})
        for row in valuations:
            nav = row["accounts"][name]
            lower, estimate, upper = (_number(nav[key]) for key in
                ("equity_lower_bound", "equity_with_claim_estimate", "equity_upper_bound"))
            complete = nav["valuation_complete"]
            if type(complete) is not bool or not 0 <= lower <= estimate <= upper:
                raise ValueError("Saved paper NAV bounds/completion status are invalid")
            incomplete += int(not complete)
            peak = max(peak, lower)
            drawdown = lower / peak - 1
            max_drawdown = min(max_drawdown, drawdown)
            last = {"session": row["session"], "observed_at": row["observed_at"],
                "basis": "saved_close_raw_price_valuation", "nav_lower": _amount(lower),
                "nav_estimate": _amount(estimate), "nav_upper": _amount(upper),
                "valuation_complete": complete, "net_pnl_lower": _amount(lower - initial),
                "return_lower": float(lower / initial - 1), "drawdown_lower": float(drawdown)}
            curves.append({"strategy": name, **last})
        fills_after_nav = latest_fill is not None and (last is None or latest_fill > last["session"])
        if trade_count == 0:
            performance = "no_fills"
        elif fills_after_nav:
            performance = "awaiting_close_after_new_fills"
        elif not last["valuation_complete"]:
            performance = "partial_valuation"
        else:
            performance = "observed_net_gain" if _number(last["net_pnl_lower"]) > 0 else "observed_net_loss" if _number(last["net_pnl_lower"]) < 0 else "flat_after_costs"
        baseline = {key: value for key, value in curves[-1].items() if key != "strategy"}
        accounts[name] = {"initial_capital": _amount(initial), "last_nav": last or baseline,
            "performance_status": performance, "has_saved_close": last is not None,
            "fills_after_last_nav": fills_after_nav,
            "net_pnl_lower": last["net_pnl_lower"] if last else "0.00" if trade_count == 0 else None,
            "net_return_lower": last["return_lower"] if last else 0.0 if trade_count == 0 else None,
            "max_drawdown_lower": float(max_drawdown), "incomplete_valuation_days": incomplete,
            "valuation_days": len(valuations), "trade_count": trade_count,
            "execution_sessions_with_fills": execution_sessions, "latest_fill_session": latest_fill,
            "fees": {key: _amount(value) for key, value in totals.items()},
            "annualized_return": None}
    signals = state["signals"]
    latest_signal = signals[max(signals)] if signals else None
    pending = []
    expired = []
    local = _now().astimezone(ZoneInfo("Asia/Shanghai"))
    today = local.date().isoformat()
    for signal in signals.values():
        execution = signal["execution_session"]
        if signal["rebalance"] and any(signal["allowed"].values()) and execution + "_open" not in state["completed"]:
            if execution < today or execution == today and local.time() > time(9, 45):
                expired.append(execution)
            else:
                pending.append(execution)
    next_execution = min(pending) if pending else manifest["first_possible_execution"] if not signals and manifest["first_possible_execution"] >= today else None
    if state.get("observation_gaps"):
        next_execution = None
    report = {"kind": "isolated_ashare_paper_summary", "paper_dir": status["path"],
        "created_at": manifest["created_at"], "model_sha256": status["model_sha256"],
        "accounts": accounts, "latest_signal": ({key: latest_signal.get(key) for key in
            ("signal_date", "execution_session", "created_at", "rebalance", "allowed", "warnings")} if latest_signal else None),
        "next_possible_execution_session": next_execution,
        "expired_unexecuted_signal_sessions": sorted(set(expired)),
        "observation_gaps": state.get("observation_gaps", {}),
        "new_orders_frozen_by_observation_gap": bool(state.get("observation_gaps")),
        "next_execution_is_conditional": True, "last_failure": state["last_failure"],
        "last_success": state["last_success"], "live_order_count": 0, "real_account_accessed": False,
        "limitations": [
            "收益取保存净值减初始本金；佣金、过户费、印花税和分红税预留已在账内，不再次扣减。",
            "滑点已体现在成交价格，单列仅供查看，不再次从收益扣除。",
            "回撤从初始现金起计算，使用每日最终保守估值；不完整估值可能含旧价格或未决权益，收益与回撤需结合完整性阅读。",
            "没有新收盘估值时，最近成交后的当前收益未知；没有成交的零收益不表示盈利。",
            "不提供年化收益；下一执行日期还取决于真实日历、执行窗口及有效报价，不承诺成交。",
        ]}
    return report, curves


def ashare_paper_summary(paper_dir):
    """Verify source state/receipts and return a concise summary without writes."""
    return _build_report(paper_dir)[0]


def _atomic_export(path, text):
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".paper-report-", delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(text.encode())
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def export_ashare_paper_report(paper_dir, output_dir):
    """Explicit export to a separate directory, never modify frozen paper files."""
    root, output = Path(paper_dir).resolve(), Path(output_dir).resolve()
    if output == root or root in output.parents:
        raise ValueError("Paper report export must use a directory outside the paper ledger")
    report, curves = _build_report(root)
    output.mkdir(parents=True, exist_ok=True)
    summary_path, curve_path = output / "paper-summary.json", output / "curves.csv"
    _atomic_export(summary_path, json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(curves[0]))
    writer.writeheader()
    writer.writerows(curves)
    _atomic_export(curve_path, buffer.getvalue())
    return {"summary_path": str(summary_path), "curves_path": str(curve_path)}
