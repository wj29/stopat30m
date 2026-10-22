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


DAILY_FIELDS = ("strategy", "session", "close_observed_at", "previous_close_session",
    "previous_nav_lower", "previous_nav_upper", "nav_lower", "nav_upper",
    "net_pnl_since_previous_close", "return_since_previous_close", "daily_net_return",
    "valuation_complete", "conservative_drawdown", "diagnostic_status", "trade_count",
    "rejected_order_count", "orders_coverage_complete", "notional_cny",
    "one_way_turnover_previous_close_nav", "minimum_commission_fill_count",
    "commission_cny", "transfer_fee_cny", "stamp_tax_cny", "total_fee_cny",
    "slippage_cost_cny", "fees_plus_slippage_cny", "dividend_gross_recognized_cny",
    "dividend_tax_recognized_cny", "dividend_tax_coverage_complete")


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


def _paper_status(paper_dir):
    kind = json.loads((Path(paper_dir) / "manifest.json").read_text()).get("kind")
    if kind == "isolated_ashare_low_turnover_forward":
        from stopat30m.ashare_low_turnover_forward import low_turnover_status
        return low_turnover_status(paper_dir)
    if kind == "isolated_ashare_integer_forward_paper":
        from stopat30m.ashare_forward import ashare_forward_status
        return ashare_forward_status(paper_dir)
    raise ValueError("Unsupported verified A-share paper manifest kind")


def _monitor_calendar(root, manifest):
    path = Path(root) / "calendar.json"
    if not path.is_file():
        return None  # Legacy reports remain readable; never invent weekdays.
    if (manifest.get("calendar_sha256") is None
            or hashlib.sha256(path.read_bytes()).hexdigest() != manifest["calendar_sha256"]):
        raise ValueError("Paper monitoring calendar differs from the frozen manifest")
    from stopat30m.data.research_calendar import Calendar
    return Calendar.load(path)


def _dividend_taxes(state, name):
    account = state.get("accounts", {}).get(name)
    if account is None or not isinstance(account.get("entitlements"), dict):
        return None, "account_entitlement_evidence_unavailable"
    by_day = {}
    # Each entitlement is retained after settlement, so its unique ledger id is
    # counted once at ex-date, never again from a payment/replay receipt.
    for event_id, event in account["entitlements"].items():
        if event.get("kind") != "cash_dividend":
            continue
        if (not event_id or not all(key in event for key in ("ex_date", "gross_cash", "tax", "net_cash"))
                or event.get("status") not in {"pending", "settled"}):
            return None, "cash_entitlement_recognition_evidence_incomplete"
        day = event["ex_date"]
        if datetime.fromisoformat(day).date().isoformat() != day:
            raise ValueError("Invalid saved dividend recognition date")
        gross, tax, net = (_number(event[key]) for key in ("gross_cash", "tax", "net_cash"))
        if not 0 <= tax <= gross or net != gross - tax:
            raise ValueError("Saved dividend entitlement tax amounts are inconsistent")
        totals = by_day.setdefault(day, {"gross": Decimal(0), "tax": Decimal(0)})
        totals["gross"] += gross
        totals["tax"] += tax
    return by_day, "complete_recorded_entitlement_inventory_not_independent_tax_validation"


def _stability_windows(rows, sessions):
    closes = {row["session"]: row for row in rows if row["nav_lower"] is not None}
    latest = max(closes) if closes else None
    result = {}
    for length in (20, 60):
        base = {"required_actual_sessions": length, "observed_complete_sessions": 0,
            "end_session": latest, "net_return_lower": None, "max_drawdown_conservative": None,
            "positive_session_fraction": None, "worst_session_return": None,
            "status": "insufficient_continuous_complete_observations"}
        if sessions is None:
            base["status"] = "verified_calendar_unavailable"
        elif latest is not None:
            location = sessions.index(latest)
            selected = sessions[max(0, location - length + 1):location + 1]
            complete = [closes.get(day) for day in selected]
            base["observed_complete_sessions"] = sum(row is not None and row["daily_net_return"] is not None
                for row in complete)
            if len(selected) == length and base["observed_complete_sessions"] == length:
                first = complete[0]
                start = _number(first["previous_nav_lower"])
                peak, drawdown = _number(first["previous_nav_upper"]), Decimal(0)
                for row in complete:
                    peak = max(peak, _number(row["nav_upper"]))
                    drawdown = min(drawdown, _number(row["nav_lower"]) / peak - 1)
                returns = [row["daily_net_return"] for row in complete]
                base.update(status="observed_complete_window", start_session=selected[0],
                    net_return_lower=float(_number(complete[-1]["nav_lower"]) / start - 1),
                    max_drawdown_conservative=float(drawdown),
                    positive_session_fraction=sum(value > 0 for value in returns) / length,
                    worst_session_return=min(returns))
        result[str(length)] = base
    return result


def _monitoring(status, opens, valuations, initial):
    manifest, state = status["manifest"], status["state"]
    calendar = _monitor_calendar(status["path"], manifest)
    sessions = calendar.sessions(calendar.coverage_start, calendar.coverage_end) if calendar else None
    positions = {day: number for number, day in enumerate(sessions or ())}
    local = _now().astimezone(ZoneInfo("Asia/Shanghai"))
    today = local.date().isoformat()
    initialized = _time(manifest["created_at"]).astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
    expected = None
    calendar_current = calendar is not None and calendar.coverage_start <= today <= calendar.coverage_end
    if calendar_current:
        completed = [day for day in sessions if initialized <= day < today
            or initialized <= day == today and local.time() >= time(15, 5)]
        expected = completed[-1] if completed else None
    by_open = {row["session"]: row for row in opens}
    by_close = {row["session"]: row for row in valuations}
    accounts, all_rows = {}, []
    minimum = manifest.get("execution_policy", {}).get("minimum_commission")
    minimum = _number(minimum) if minimum is not None else None
    for name in manifest["strategies"]:
        taxes, tax_status = _dividend_taxes(state, name)
        days = sorted(set(by_open) | set(by_close) | set(taxes or {}))
        if sessions is not None and any(day not in positions for day in days):
            raise ValueError("Saved paper observations fall outside actual exchange sessions")
        previous, previous_upper, previous_day, previous_complete = initial, initial, None, True
        peak, maximum, rows = initial, Decimal(0), []
        for day in days:
            account = by_open.get(day, {}).get("accounts", {}).get(name, {})
            trades = account.get("trades", [])
            totals = {key: sum((_number(trade[key]) for trade in trades), Decimal(0))
                for key in ("commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost")}
            amounts = [_number(trade["notional"]) for trade in trades if "notional" in trade]
            if any(value < 0 for value in amounts):
                raise ValueError("Negative committed paper trade notional")
            notional = sum(amounts, Decimal(0)) if len(amounts) == len(trades) else None
            orders_known = not account or "orders" in account
            rejected = sum(row.get("status") == "rejected" for row in account.get("orders", [])) if orders_known else None
            nav = by_close.get(day, {}).get("accounts", {}).get(name)
            row = dict.fromkeys(DAILY_FIELDS)
            row.update(strategy=name, session=day, previous_close_session=previous_day,
                previous_nav_lower=_amount(previous), previous_nav_upper=_amount(previous_upper),
                trade_count=len(trades), rejected_order_count=rejected, orders_coverage_complete=orders_known,
                notional_cny=_amount(notional) if notional is not None else None,
                one_way_turnover_previous_close_nav=float(notional / (2 * previous))
                    if notional is not None and previous > 0 else None,
                minimum_commission_fill_count=sum(_number(trade["commission"]) == minimum for trade in trades)
                    if minimum is not None else 0 if not trades else None,
                **{key + "_cny": _amount(value) for key, value in totals.items()},
                fees_plus_slippage_cny=_amount(totals["total_fee"] + totals["slippage_cost"]),
                dividend_tax_coverage_complete=taxes is not None,
                dividend_gross_recognized_cny=_amount(taxes.get(day, {}).get("gross", 0)) if taxes is not None else None,
                dividend_tax_recognized_cny=_amount(taxes.get(day, {}).get("tax", 0)) if taxes is not None else None)
            if nav is None:
                row["diagnostic_status"] = "awaiting_saved_close"
            else:
                lower, upper = (_number(nav[key]) for key in ("equity_lower_bound", "equity_upper_bound"))
                peak = max(peak, upper)
                drawdown = lower / peak - 1
                maximum = min(maximum, drawdown)
                complete = nav["valuation_complete"]
                interval = [value for value in sessions or () if (previous_day < value <= day
                    if previous_day is not None else initialized <= value <= day)]
                consecutive = sessions is not None and interval == [day]
                gap = any((previous_day < value <= day if previous_day is not None else initialized <= value <= day)
                    for value in state.get("observation_gaps", {}))
                usable = complete and previous_complete and consecutive and not gap and previous > 0
                row.update(close_observed_at=by_close[day]["observed_at"], nav_lower=_amount(lower), nav_upper=_amount(upper),
                    net_pnl_since_previous_close=_amount(lower - previous),
                    return_since_previous_close=float(lower / previous - 1) if previous > 0 else None,
                    daily_net_return=float(lower / previous - 1) if usable else None,
                    valuation_complete=complete, conservative_drawdown=float(drawdown),
                    diagnostic_status="complete_observed_close" if usable else "diagnostic_incomplete_or_noncontinuous_interval")
                previous, previous_upper, previous_day, previous_complete = lower, upper, day, complete
            rows.append(row)
        latest = rows[-1] if rows else None
        close_days = [row["session"] for row in rows if row["nav_lower"] is not None]
        latest_close = close_days[-1] if close_days else None
        reasons = []
        if not calendar_current:
            reasons.append("calendar_missing_or_current_date_outside_verified_coverage")
        if latest_close is None:
            reasons.append("no_saved_close_observation")
        elif expected is not None and latest_close < expected:
            reasons.append("latest_close_is_stale")
        if latest is not None and latest["nav_lower"] is None:
            reasons.append("new_activity_awaiting_saved_close")
        if any(not row["valuation_complete"] for row in rows if row["nav_lower"] is not None):
            reasons.append("partial_valuation_history")
        if state.get("observation_gaps"):
            from .ashare_low_turnover_recovery import pending_gaps as _pending_observation_gaps
            reasons.append("permanent_observation_gap" if _pending_observation_gaps(state)
                else "historical_observation_gap_retained_audited_future_recovery")
        if not any(row["trade_count"] for row in rows):
            reasons.append("no_committed_fills_not_profit_evidence")
        accounts[name] = {"status": "diagnostic" if reasons else "observed_after_cost_monitoring",
            "diagnostic_reasons": reasons, "latest_session": latest["session"] if latest else None,
            "latest_close_session": latest_close, "latest": latest,
            "max_drawdown_conservative": float(maximum),
            "stability": _stability_windows(rows, sessions), "dividend_tax_coverage": tax_status,
            "dividend_tax_recognized_cny": _amount(sum((row["tax"] for row in taxes.values()), Decimal(0)))
                if taxes is not None else None,
            "annualized_return": None, "performance_qualification_claimed": False}
        all_rows.extend(rows)
    return {"schema_version": 1, "generated_at": _now().isoformat(),
        "calendar_status": "verified_local_calendar" if calendar is not None else "unavailable_no_weekday_fallback",
        "expected_latest_close_session": expected, "accounts": accounts,
        "definitions": {"daily_net_return": "only adjacent complete actual-session closes; initial cash is the inception anchor",
            "conservative_drawdown": "lower equity / cumulative upper equity peak - 1, including initial capital",
            "turnover": "actual buy-plus-sell filled notional / 2 / preceding saved close lower NAV, or initial cash before the first close (not annualized)",
            "tax": "recognition on evidenced ex-date once per ledger entitlement; settlement is not charged twice",
            "stability": "20/60 actual-session complete observation windows; historical frequency is not a future profit probability",
            "net_returns": "recorded NAV already includes execution costs and tax; no subtraction or addback"}}, all_rows


def _build_report(paper_dir):
    status = _paper_status(paper_dir)
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
    from .ashare_low_turnover_recovery import pending_gaps as _pending_observation_gaps
    pending_gaps = _pending_observation_gaps(state)
    if pending_gaps:
        next_execution = None
    report = {"kind": "isolated_ashare_paper_summary", "paper_dir": status["path"],
        "created_at": manifest["created_at"], "model_sha256": status["model_sha256"],
        "accounts": accounts, "latest_signal": ({key: latest_signal.get(key) for key in
            ("signal_date", "execution_session", "created_at", "rebalance", "allowed", "warnings")} if latest_signal else None),
        "next_possible_execution_session": next_execution,
        "expired_unexecuted_signal_sessions": sorted(set(expired)),
        "observation_gaps": state.get("observation_gaps", {}),
        "new_orders_frozen_by_observation_gap": bool(pending_gaps),
        "unresolved_operational_gaps": pending_gaps,
        "next_execution_is_conditional": True, "last_failure": state["last_failure"],
        "last_success": state["last_success"], "live_order_count": 0, "real_account_accessed": False,
        "limitations": [
            "收益取保存净值减初始本金；佣金、过户费、印花税和分红税预留已在账内，不再次扣减。",
            "滑点已体现在成交价格，单列仅供查看，不再次从收益扣除。",
            "回撤从初始现金起计算，使用每日最终保守估值；不完整估值可能含旧价格或未决权益，收益与回撤需结合完整性阅读。",
            "没有新收盘估值时，最近成交后的当前收益未知；没有成交的零收益不表示盈利。",
            "不提供年化收益；下一执行日期还取决于真实日历、执行窗口及有效报价，不承诺成交。",
        ]}
    report["daily_monitoring"], report["daily_summary_rows"] = _monitoring(status, opens, valuations, initial)
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
    daily_path = output / "daily-summary.csv"
    _atomic_export(summary_path, json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(curves[0]))
    writer.writeheader()
    writer.writerows(curves)
    _atomic_export(curve_path, buffer.getvalue())
    daily = io.StringIO()
    writer = csv.DictWriter(daily, fieldnames=DAILY_FIELDS)
    writer.writeheader()
    writer.writerows(report["daily_summary_rows"])
    _atomic_export(daily_path, daily.getvalue())
    return {"summary_path": str(summary_path), "curves_path": str(curve_path), "daily_summary_path": str(daily_path)}
