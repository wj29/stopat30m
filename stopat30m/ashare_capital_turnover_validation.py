"""Offline capital-fraction turnover guards over the frozen original H5 ledger."""
from copy import deepcopy
from decimal import Decimal

from stopat30m import ashare_stability_validation as stability
from stopat30m.ashare_turnover_validation import (
    HoldingAgeTracker, build_turnover_plan, _exit_audit,
    MODELS, _engine, session_policy, daily_inputs, _source_day,
    identity_execution_inputs, relevant_events, record_unsubscribed_rights,
    reserve_disclosed_cash_rounding, CashDistributionRoundingFundingError,
    a2, _rank, valuation_validity, path_stability,
)

CAPITAL_ARMS = (("hold10_cap2", None), ("cap5pct", "0.05"), ("cap7p5pct", "0.075"), ("cap10pct", "0.10"))
HOLD_RULE = {"key": "hold10_cap2", "minimum_hold_sessions": 10, "max_replacements": 2}
POLICY = "a11-original-H5-top20-hold10-cap2-per-side-capital-guard-v1"

def simulate_capital_turnover(context, *, period, fraction):
    from stopat30m.ashare_turnover_validation import simulate_turnover
    from stopat30m.ashare_capital_turnover_execution import execute_capital_turnover_plan
    if fraction not in (None, "0.05", "0.075", "0.10") or fraction is not None and type(fraction) is not str:
        raise ValueError("Only predeclared capital fractions are allowed")
    if fraction is None:
        return simulate_turnover(context, period=period, variant=dict(HOLD_RULE))
    variant = dict(HOLD_RULE)
    if context["protocol"]["execution"] != stability.EXECUTION:
        raise ValueError("Turnover comparison must retain all frozen execution parameters")
    model = "LGB"
    if model not in MODELS or period not in context["protocol"]["periods"]:
        raise ValueError("Undeclared fixed union model or period")
    start, end = context["protocol"]["periods"][period]
    calendar, config = context["calendar"], context["protocol"]["execution"]
    days = [day for day in calendar if start <= day <= end]
    if not days or calendar.index(days[0]) < 1:
        raise ValueError("Actual comparison sessions and preceding signal are required")
    engine_calendar = calendar[max(0, calendar.index(days[0])-80):calendar.index(days[-1])+1]
    from stopat30m.data.a5_distribution_tax import reserve_additional_distribution_tax, AdditionalTaxFundingError
    initial = Decimal(config["initial_cash_cny"])
    ex, _ = _engine(days[0])
    state, previous = ex.PortfolioState.initial(session_policy(days[0], config)), initial
    symbols = sorted(set(context["raw"].instrument) | {row["to_instrument"] for row in context["actions"].actions})
    records = {name: [] for name in ("curves", "trades", "orders", "positions", "claims",
        "signals", "corporate_actions", "plans", "valuation_audit", "score_coverage")}
    records.update(holding_age=[], holding_origin_actions=[], turnover_plans=[], capital_turnover=[])
    tracker = HoldingAgeTracker(calendar)
    unpriced_rights, terminal_fresh, stopped = set(), False, None
    rounding_events = [event for event in context["events"] if "cash_payment_gross_rounding" in event]
    for number, day in enumerate(days):
        ex, adapter = _engine(day)
        policy = session_policy(day, config)
        signal_day = calendar[calendar.index(day)-1]
        quotes, metadata, closes, suspended = daily_inputs(_source_day(context, day), context["basic"], day, symbols)
        unpriced_rights.difference_update(closes)
        metadata, suspended = identity_execution_inputs(context.get("identity_events", ()),
                                                        day, metadata, suspended)
        suspended = sorted(set(suspended) | {inst for inst in symbols if context["actions"].suspended(inst, day)})
        events = relevant_events(state, context["events"], day, days[0])
        rights = record_unsubscribed_rights(state, context.get("rights_events", ()), day)
        records["corporate_actions"].extend(rights)
        unpriced_rights.update(row["instrument"] for row in rights if row["shares_before"] > 0 and row["instrument"] not in closes)
        for contract in [*context["tax_contracts"], *rounding_events]:
            if contract["record_date"] < days[0]:
                state.holdings_history.setdefault(contract["record_date"], {})
        try:
            candidate, rounding = reserve_disclosed_cash_rounding(state, day, rounding_events)
            candidate, extra_tax = reserve_additional_distribution_tax(candidate, day, context["tax_contracts"])
        except (AdditionalTaxFundingError, CashDistributionRoundingFundingError) as exc:
            stopped = {"session": day, "reason": str(exc), "kind": (
                "unfunded_full_distribution_tax_reserve" if isinstance(exc, AdditionalTaxFundingError)
                else "unfunded_disclosed_cash_rounding_reserve")}
            terminal_fresh = False
            break
        state = candidate
        records["corporate_actions"].extend(rounding)
        records["corporate_actions"].extend(extra_tax)
        before_actions = deepcopy(state)
        state, ledger = a2._apply_events(state, day, events, context["actions"], engine_calendar, quotes, policy, days[0])
        records["corporate_actions"].extend(ledger)
        tracker.after_corporate_actions(before_actions, state, ledger, day)
        if number % config["rebalance_sessions"] == 0:
            ranked, members, prices = _rank(context, model, signal_day)
            records["score_coverage"].append(context["score_coverage"][(model, signal_day)])
            if not ranked:
                raise ValueError("No eligible frozen prior-close scores")
            plan, turnover_audit = build_turnover_plan(state, signal_date=signal_day, session=day,
                ranked_instruments=ranked, raw_signal_prices=prices,
                boards={inst: a2.board(inst) for inst in ranked}, signal_equity=previous,
                calendar=calendar, holding_started=tracker.holding_started,
                minimum_hold_sessions=variant["minimum_hold_sessions"], max_replacements=variant["max_replacements"],
                base_policy=adapter.RankBufferPolicy(top_k=config["top_k"], keep_rank=config["keep_rank"],
                    require_signal_affordability=config["require_signal_affordability"]), execution_policy=policy)
            before_execution = deepcopy(state)
            result = execute_capital_turnover_plan(state, plan=plan, session=day, quotes=quotes,
                metadata=metadata, calendar=engine_calendar, policy=policy, fraction=fraction,
                initial_build=number == 0, verified_suspended=suspended)
            records["capital_turnover"].append({"session": day, **result.audit["capital_turnover"]})
            state = result.state
            tracker.after_trades(before_execution, state, result.trades, day)
            records["turnover_plans"].append(_exit_audit(plan, result, turnover_audit))
            records["trades"].extend(result.trades)
            records["orders"].extend(result.orders)
            records["plans"].append({"session": day, **result.audit})
            records["signals"].extend({"session": day, "signal_date": signal_day,
                "instrument": inst, "selected": inst in plan.selected} for inst in members)
        valued = ex.mark_to_close(state, session=day, prices=closes, calendar=engine_calendar,
                                  policy=policy, verified_suspended=suspended)
        state, equity = valued.state, valued.equity_lower_bound
        validity = valuation_validity(valued, observed_closes=closes, source_suspended=suspended,
                                     unobserved_rights_ex_prices=unpriced_rights)
        price_bearing = set(state.positions)
        price_bearing.update(row["instrument"] for row in valued.claims if row.get("kind") != "cash_dividend")
        delisted = sorted(event["instrument"] for event in context.get("identity_events", ())
            if event.get("kind") == "delisting" and event["effective_date"] <= day
            and event["instrument"] in price_bearing)
        validity["unpriced_delisted_claims"] = delisted
        validity["accounting_valid"] &= not delisted
        if state.cash < 0 or equity <= 0:
            raise ValueError("Invalid cash or NAV in the integer-share ledger")
        records["valuation_audit"].append({"session": day, **validity})
        records["curves"].append({"date": day, "equity_cny": str(equity), "cash_cny": str(state.cash),
            "equity_upper_bound_cny": str(valued.equity_upper_bound),
            "equity_with_claim_estimate_cny": str(valued.equity_with_claim_estimate),
            "valuation_complete": bool(valued.valuation_complete), "accounting_valid": bool(validity["accounting_valid"]),
            "nav": float(equity / initial), "net_return": float(equity / previous - 1)})
        records["positions"].extend(valued.positions)
        records["claims"].extend({"session": day, **row} for row in valued.claims)
        terminal_fresh = not delisted and all(inst in closes for inst in state.positions) and all(
            row.get("kind") == "cash_dividend" or row["instrument"] in closes for row in valued.claims)
        records["holding_age"].extend(tracker.close(state, day))
        previous = equity
    records["holding_origin_actions"] = tracker.audit
    metrics = path_stability(records["curves"], records["trades"], initial)
    metrics.update(terminal_prices_fresh=terminal_fresh,
        rejected_order_count=sum(row["status"] == "rejected" for row in records["orders"]),
        source_confirmed_carry_days=sum(bool(row["source_confirmed_carried_instruments"]) for row in records["valuation_audit"]))
    # Ex-date recognition and pay-date settlement contain the same tax. Count
    # the former once, including a still-unpaid entitlement, never both stages.
    recognized = [row for row in records["corporate_actions"]
        if row.get("kind") == "cash_dividend" and row.get("status") == "pending"]
    metrics["recognized_dividend_gross_cash_cny"] = str(sum((Decimal(str(row["gross_cash"])) for row in recognized), Decimal(0)))
    metrics["recognized_distribution_tax_reserve_cny"] = str(sum((Decimal(str(row["tax"])) for row in recognized), Decimal(0)))
    metrics["additional_distribution_tax_reserved_cny"] = str(sum((
        Decimal(str(row["additional_tax_reserved_cny"])) for row in records["corporate_actions"]
        if row.get("kind") == "additional_distribution_tax_reserve"), Decimal(0)))
    metrics["total_distribution_tax_reserved_cny"] = str(
        Decimal(metrics["recognized_distribution_tax_reserve_cny"]) +
        Decimal(metrics["additional_distribution_tax_reserved_cny"]))
    metrics["cash_distribution_rounding_reserved_cny"] = str(sum((
        Decimal(row["rounding_reserved_cny"]) for row in records["corporate_actions"]
        if row.get("kind") == "cash_distribution_rounding_reserve"), Decimal(0)))
    metrics["recognized_dividend_legacy_rounded_gross_cash_cny"] = metrics["recognized_dividend_gross_cash_cny"]
    metrics["recognized_dividend_gross_cash_cny"] = str(
        Decimal(metrics["recognized_dividend_legacy_rounded_gross_cash_cny"]) -
        Decimal(metrics["cash_distribution_rounding_reserved_cny"]))
    metrics["declared_end_reached"] = stopped is None
    metrics["planned_exit_attempt_securities"] = sum(row["exit_intent_count"] for row in records["turnover_plans"])
    metrics["actual_exit_fill_securities"] = sum(row["actual_exit_fill_count"] for row in records["turnover_plans"])
    metrics["actual_cleared_exit_securities"] = sum(row["actual_exit_cleared_count"] for row in records["turnover_plans"])
    metrics["rebalance_actual_overflow_days"] = sum(row["actual_overflow_after_execution"] > 0 for row in records["turnover_plans"])
    valid = metrics["accounting_invalid_days"] == 0 and terminal_fresh and stopped is None
    return {"period": period, "model": model, "variant": variant, "execution_id": "E2", "performance_valid": valid,
        "metrics_basis": "declared_accounting_NAV_not_actual_liquidation" if valid else "invalid_indicative_carried_ledger_do_not_use_for_selection",
        "metrics": metrics, "records": records, "final_state": state.to_dict(),
        "stopped": stopped, "actual_last_valued_session": records["curves"][-1]["date"] if records["curves"] else None}
