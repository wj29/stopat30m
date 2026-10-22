"""A16 frozen-score and simple-control adapters over the same integer ledger.

This module fits and predicts nothing. The original and regularized models
delegate to the unchanged A14 execution path. Simple controls retain the full
original inference index; genuinely absent backward endpoints stay unknown.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import hashlib
import re

import numpy as np
import pandas as pd

from stopat30m import ashare_model_debug_validation as debug
from stopat30m import ashare_stability_validation as stability
from stopat30m.ashare_turnover_policy import build_turnover_plan
from stopat30m.ashare_turnover_validation import (
    simulate_turnover, _variant, HoldingAgeTracker, _exit_audit,
    MODELS, _engine, session_policy, daily_inputs, _source_day,
    identity_execution_inputs, relevant_events, record_unsubscribed_rights,
    reserve_disclosed_cash_rounding, CashDistributionRoundingFundingError,
    a2, valuation_validity, path_stability,
)
from stopat30m.ashare_union_validation import _known_identity


HASH_SEEDS = (1103, 2207, 3301, 4409, 5519)
STRATEGIES = ("original", "regularized300", "MOM60", "REV20",
              *("HASH" + str(seed) for seed in HASH_SEEDS))
LOOKBACKS = {"MOM60": 60, "REV20": 20}


def load_baseline_context(training_declaration, family):
    """Retain every original source/model/period guard from A14."""
    return debug.load_debug_context(training_declaration, family)


def _score_index(context):
    scores = context.get("scores", {}).get("LGB")
    if (not isinstance(scores, pd.Series) or not isinstance(scores.index, pd.MultiIndex)
            or list(scores.index.names) != ["datetime", "instrument"]
            or scores.empty or not scores.index.is_unique):
        raise ValueError("The complete unique original LGB inference index is required")
    dates = scores.index.get_level_values("datetime")
    instruments = scores.index.get_level_values("instrument")
    if (not isinstance(dates, pd.DatetimeIndex) or dates.tz is not None
            or not dates.equals(dates.normalize())
            or any(not isinstance(inst, str) or not re.fullmatch(r"(?:SH|SZ)\d{6}", inst)
                   for inst in instruments)):
        raise ValueError("Original inference dates and security identities are required")
    calendar = context["calendar"]
    if not calendar or calendar != sorted(set(calendar)):
        raise ValueError("The ordered unique actual source calendar is required")
    locations = pd.Index(calendar).get_indexer(dates.strftime("%Y-%m-%d"))
    if (locations < 0).any():
        raise ValueError("Inference dates must belong to the actual source calendar")
    return scores, locations, instruments


def _prices(context):
    prices = context.get("normalized")
    if (not isinstance(prices, pd.DataFrame) or not prices.index.is_unique
            or not prices.columns.is_unique or list(prices.index) != list(context["calendar"])):
        raise ValueError("Exact source-calendar adjusted closes are required; no price filling")
    return prices


def _endpoint_missing(context, instrument, signal_day, lookback):
    prices = _prices(context)
    location = context["calendar"].index(signal_day)
    if location < lookback or instrument not in prices:
        return True
    values = (prices.at[signal_day, instrument],
              prices.at[context["calendar"][location-lookback], instrument])
    return any(not np.isfinite(value) or value <= 0 for value in values)


def _simple_scores(context, strategy):
    original, locations, instruments = _score_index(context)
    if strategy.startswith("HASH"):
        seed = int(strategy[4:])
        mapping = {inst: int(hashlib.sha256(
            f"a16-static-hash-v1|{seed}|{inst}".encode("utf-8")).hexdigest()[:13], 16) / 2**52
            for inst in instruments.unique()}
        values = np.asarray([mapping[inst] for inst in instruments], dtype=float)
    else:
        lookback = LOOKBACKS[strategy]
        prices = _prices(context)
        columns = prices.columns.get_indexer(instruments)
        observed = (locations >= lookback) & (columns >= 0)
        values = np.full(len(original), np.nan, dtype=float)
        positions = np.flatnonzero(observed)
        matrix = prices.to_numpy(dtype=float, copy=False)
        current = matrix[locations[positions], columns[positions]]
        previous = matrix[locations[positions]-lookback, columns[positions]]
        finite = np.isfinite(current) & (current > 0) & np.isfinite(previous) & (previous > 0)
        returns = current[finite] / previous[finite] - 1
        if not np.isfinite(returns).all():
            raise ValueError("Positive observed endpoints produced a nonfinite control score")
        values[positions[finite]] = returns if strategy == "MOM60" else -returns
    return pd.Series(values, index=original.index.copy(), name=original.name)


def prepare_baseline_context(context, *, strategy, period, prediction_package=None):
    """Build scores without labels, changing neither data eligibility nor policy."""
    if strategy not in STRATEGIES:
        raise ValueError("Only the nine predeclared A16 strategies are supported")
    candidate = "regularized300" if strategy == "regularized300" else "baseline"
    prepared = debug.prepare_debug_context(context, candidate=candidate, period=period,
                                            prediction_package=prediction_package)
    if strategy not in {"original", "regularized300"}:
        scores = _simple_scores(prepared, strategy)
        prepared = {**prepared, "scores": {"LGB": scores}, "score_coverage": {}}
        counts = scores.isna().groupby(level="datetime").sum()
        missing = {day.date().isoformat(): int(count) for day, count in counts.items() if count}
    else:
        missing = {}
    prepared["baseline_control"] = {"strategy": strategy,
        "family": prepared["model_debug"]["family"], "period": period,
        "fits": 0, "predictions_generated": 0,
        "score_index_basis": "unchanged_original_inference_index_no_future_label_filter",
        "lookback_actual_sessions": LOOKBACKS.get(strategy),
        "missing_endpoint_rows_by_signal_date": missing,
        "hash_basis": "a16-static-hash-v1|seed|instrument_first13hex_div_2pow52" if strategy.startswith("HASH") else None}
    return prepared


def baseline_signal_scores(context, strategy=None, *, period=None, prediction_package=None):
    """Return a copy of the prepared full inference index for signal diagnostics."""
    if strategy is not None:
        if period is None:
            raise ValueError("Preparing diagnostic scores requires an explicit family period")
        context = prepare_baseline_context(context, strategy=strategy, period=period,
                                           prediction_package=prediction_package)
    elif period is not None or prediction_package is not None or "baseline_control" not in context:
        raise ValueError("Provide a prepared baseline context or a strategy and period")
    return context["scores"]["LGB"].copy(deep=True)


def _baseline_rank(context, model, signal_day):
    """Keep original membership rules; disclose only genuine endpoint exceptions."""
    strategy = context.get("baseline_control", {}).get("strategy")
    if strategy not in STRATEGIES[2:] or model != "LGB":
        raise ValueError("The simple rank adapter requires an explicit predeclared control")
    members = context["pool"].members_on(signal_day)
    try:
        scores = context["scores"][model].xs(pd.Timestamp(signal_day), level="datetime")
    except KeyError:
        raise ValueError("Required frozen preceding-close scores are absent") from None
    day_raw = _source_day(context, signal_day)
    observed = day_raw.loc[day_raw.source_observed_market_quote].set_index("instrument")
    blocked = {event["instrument"] for event in context.get("identity_events", ())
        if event.get("kind") == "delisting" and _known_identity(event, signal_day, close=True, fact="termination_known_at")}
    eligible = [inst for inst in members if inst in observed.index and inst not in blocked
        and not context["actions"].excluded(inst, signal_day, close=True)
        and inst in scores and np.isfinite(scores.loc[inst])]
    required = {inst for inst in members if inst in observed.index
        and bool(observed.loc[inst, "source_complete_quote"]) and inst not in blocked
        and not context["actions"].excluded(inst, signal_day, close=True)}
    absent = sorted(inst for inst in required if inst not in scores or not np.isfinite(scores.loc[inst]))
    exceptions = []
    for inst in absent:
        lookback = LOOKBACKS.get(strategy)
        if inst not in scores or lookback is None or not _endpoint_missing(context, inst, signal_day, lookback):
            raise ValueError("Missing control score despite complete original inference identity and observed declared endpoints: " + inst)
        exceptions.append({"instrument": inst, "reason": f"unobserved_declared_{lookback}_session_endpoint"})
    exceptions.extend({"instrument": inst, "reason": "observed_partial_session_not_original_complete_model_quote"}
        for inst in members if inst in observed.index and not bool(observed.loc[inst, "source_complete_quote"])
        and (inst not in scores or not np.isfinite(scores.loc[inst])))
    coverage = {"signal_date": signal_day, "model": model, "strategy": strategy, "members": len(members),
        "required_complete_quote_members": len(required), "ranked_count": len(eligible),
        "missing_required_scores": absent, "explicit_exceptions": exceptions}
    context.setdefault("score_coverage", {})[(model, signal_day)] = coverage
    return sorted(eligible, key=lambda inst: (-float(scores.loc[inst]), inst)), members, observed.close.to_dict()


def simulate_baseline_strategy(context, *, strategy, period, prediction_package=None):
    """Run the same fixed20 hold10/cap2 execution; never refit or predict."""
    prepared = prepare_baseline_context(context, strategy=strategy, period=period,
                                        prediction_package=prediction_package)
    simulator = simulate_turnover if strategy in {"original", "regularized300"} else _simulate_simple_turnover
    return simulator(prepared, period=period, variant=dict(debug.HOLD_RULE))


def _simulate_simple_turnover(context, *, period, variant):
    variant = _variant(variant)
    if variant["key"] == "baseline":
        return stability.simulate_stability(context, period=period, model="LGB")
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
    records.update(holding_age=[], holding_origin_actions=[], turnover_plans=[])
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
            ranked, members, prices = _baseline_rank(context, model, signal_day)
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
                metadata=metadata, calendar=engine_calendar, policy=policy, verified_suspended=suspended)
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
