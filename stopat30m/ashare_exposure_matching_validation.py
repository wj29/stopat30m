"""A12 lagged capital-use control on a separate, original-rule ledger.

The two controls delegate their entire original A11 result unchanged. Only the
new matched account executes a separately charged daily cash-control stage.
No reference target from today, a future date, or a full-period mean is read.
"""
from collections.abc import Mapping
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

MATCHING_ARMS = ("hold10_cap2", "cap7p5pct", "matched_original")
HOLD_RULE = {"key": "hold10_cap2", "minimum_hold_sessions": 10, "max_replacements": 2}
POLICY = "a12-original-H5-lagged-cap75-security-exposure-cash-control-v1"


def _reference_target(reference_exposures, prior_session):
    record = reference_exposures.get(prior_session)
    if not isinstance(record, Mapping):
        return None, False, "missing_or_invalid_previous_reference_record"
    if record.get("date") != prior_session:
        return None, False, "previous_reference_date_mismatch"
    covered = record.get("coverage_complete") is True
    if not covered:
        return None, False, "previous_reference_coverage_incomplete"
    value = record.get("risky_nav_fraction")
    if isinstance(value, bool):
        return None, True, "invalid_previous_reference_exposure"
    try:
        target = Decimal(str(value))
    except (ValueError, TypeError, ArithmeticError):
        return None, True, "invalid_previous_reference_exposure"
    if not target.is_finite() or not 0 <= target <= 1:
        return None, True, "invalid_previous_reference_exposure"
    return str(target), True, "verified_previous_reference_exposure"


def simulate_exposure_matching(context, *, period, arm_key, reference_exposures):
    """Run only a declared arm; actual control and core fills share one ledger.

    Reference coverage concerns the saved *previous* session only. A missing
    target is explicit and is never carried, interpolated, clipped or inferred
    from current/future exposure. A compliant-NAV failure stops this new path;
    all other implementation exceptions propagate rather than becoming skips.
    """
    if type(arm_key) is not str or arm_key not in MATCHING_ARMS:
        raise ValueError("Only predeclared exposure-matching arms are allowed")
    if arm_key in ("hold10_cap2", "cap7p5pct"):
        from stopat30m.ashare_capital_turnover_validation import simulate_capital_turnover
        return simulate_capital_turnover(context, period=period,
            fraction=None if arm_key == "hold10_cap2" else "0.075")
    if not isinstance(reference_exposures, Mapping):
        raise ValueError("Explicit prior-session reference exposure records are required")
    from stopat30m.ashare_exposure_matching_execution import (
        execute_exposure_overlay, ExposureMatchingUnavailableNAVError,
    )
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
    records.update(holding_age=[], holding_origin_actions=[], turnover_plans=[], exposure_matching=[])
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
            result = adapter.execute_rank_buffer_plan(state, plan=plan, session=day, quotes=quotes,
                metadata=metadata, calendar=engine_calendar, policy=policy,
                verified_suspended=suspended)
            state = result.state
            tracker.after_trades(before_execution, state, result.trades, day)
            records["turnover_plans"].append(_exit_audit(plan, result, turnover_audit))
            records["trades"].extend({**row, "order_purpose": "core"} for row in result.trades)
            records["orders"].extend({**row, "order_purpose": "core"} for row in result.orders)
            records["plans"].append({"session": day, **result.audit})
            records["signals"].extend({"session": day, "signal_date": signal_day,
                "instrument": inst, "selected": inst in plan.selected} for inst in members)
        if number == 0:
            # The first build remains the original strategy. In particular,
            # neither an earlier-period target nor a future reference is used.
            records["exposure_matching"].append({"session": day,
                "source_previous_date": signal_day, "source_coverage": None,
                "source_reason": "first_scheduled_batch_no_overlay",
                "target": None, "skip_reason": "first_scheduled_batch_no_overlay",
                "initial_build_skipped": True})
        else:
            target, source_coverage, source_reason = _reference_target(reference_exposures, signal_day)
            before_overlay = deepcopy(state)
            try:
                overlay = execute_exposure_overlay(state, session=day, signal_date=signal_day,
                    quotes=quotes, metadata=metadata, calendar=engine_calendar,
                    policy=policy, target_fraction=target, verified_suspended=suspended)
            except ExposureMatchingUnavailableNAVError as exc:
                state = before_overlay
                records["exposure_matching"].append({"session": day,
                    "source_previous_date": signal_day, "source_coverage": source_coverage,
                    "source_reason": source_reason, "target": target,
                    "skip_reason": "unavailable_compliant_opening_NAV",
                    "initial_build_skipped": False, "stopped": True,
                    "exception_type": type(exc).__name__, "reason": str(exc)})
                stopped = {"session": day, "kind": "exposure_matching_unavailable_opening_NAV",
                           "reason": str(exc), "source_previous_date": signal_day}
                terminal_fresh = False
                break
            state = overlay.state
            tracker.after_trades(before_overlay, state, overlay.trades, day)
            records["trades"].extend({**row, "order_purpose": "cash_control"} for row in overlay.trades)
            records["orders"].extend({**row, "order_purpose": "cash_control"} for row in overlay.orders)
            records["exposure_matching"].append({**overlay.audit["exposure_matching"],
                "session": day, "source_previous_date": signal_day,
                "source_coverage": source_coverage, "source_reason": source_reason,
                "target": target, "initial_build_skipped": False})
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
