"""Fixed-LGB turnover comparison over the unchanged secondary ledger.

Only the prior-close plan changes. Baseline delegates to the frozen simulator;
the treatments retain its tax, price, execution and valuation operations.
"""
from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from stopat30m import ashare_stability_validation as stability
from stopat30m.ashare_turnover_policy import build_turnover_plan


FIXED_VARIANTS = (
    {"key": "baseline", "minimum_hold_sessions": 0, "max_replacements": None},
    {"key": "hold10_cap2", "minimum_hold_sessions": 10, "max_replacements": 2},
    {"key": "hold10_cap5", "minimum_hold_sessions": 10, "max_replacements": 5},
    {"key": "hold20_cap2", "minimum_hold_sessions": 20, "max_replacements": 2},
    {"key": "hold20_cap5", "minimum_hold_sessions": 20, "max_replacements": 5},
)
_VARIANT_TERMS = tuple(tuple(row.items()) for row in FIXED_VARIANTS)


def _variant(value):
    if not isinstance(value, Mapping) or set(value) != {"key", "minimum_hold_sessions", "max_replacements"}:
        raise ValueError("A complete predeclared turnover variant is required")
    if (type(value["minimum_hold_sessions"]) is not int
            or value["max_replacements"] is not None and type(value["max_replacements"]) is not int):
        raise ValueError("Turnover parameters must retain their declared integer types")
    for terms in _VARIANT_TERMS:
        if dict(terms) == dict(value):
            return dict(terms)
    raise ValueError("Undeclared turnover variant; no parameter search is permitted")


def _positive_positions(state):
    quantities = {}
    for inst, position in state.positions.items():
        quantity = position.quantity
        if type(quantity) is not int or quantity < 0:
            raise ValueError("Actual position quantities must be nonnegative integer shares")
        if quantity:
            quantities[inst] = quantity
    return quantities


def _pending_bonus(state):
    quantities = {}
    for row in state.entitlements.values():
        if row.get("kind") != "bonus_shares" or row.get("status") != "pending":
            continue
        quantity = Decimal(str(row["quantity"]))
        if not quantity.is_finite() or quantity < 0:
            raise ValueError("Pending bonus quantity must be finite and nonnegative")
        if quantity:
            inst = row["instrument"]
            quantities[inst] = quantities.get(inst, Decimal(0)) + quantity
    return quantities


def active_instruments(state):
    """Match the legacy planner, including positive exact pending bonus shares.

    A pending0.4 bonus keeps the original episode until settlement. A settled
    nontradable fractional claim never keeps that episode alive for a new BUY.
    """
    return set(_positive_positions(state)) | set(_pending_bonus(state))


class HoldingAgeTracker:
    """Actual-fill, security-episode origins; never infer from position marks.

    Confirmed exchanges into an existing successor inherit the earlier security
    episode, rather than implementing a per-lot minimum age. Record-date source
    origins remain available after a sale, for an evidenced later bonus listing.
    Fractional-claim provenance is audited without extending a tradable episode.
    """

    def __init__(self, calendar):
        self.calendar = tuple(calendar)
        if not self.calendar or list(self.calendar) != sorted(set(self.calendar)):
            raise ValueError("A full ordered actual exchange calendar is required")
        self.locations = {day: index for index, day in enumerate(self.calendar)}
        self.holding_started = {}
        self.record_origins = {}
        self.audit = []

    def _day(self, day):
        if day not in self.locations:
            raise ValueError("Holding clock requires an actual exchange session")

    def _origin(self, inst, day):
        origin = self.holding_started.get(inst)
        if origin not in self.locations or self.locations[origin] > self.locations[day]:
            raise ValueError("Unknown actual first acquisition origin for " + inst)
        return origin

    def _inherit(self, inst, origin, day, *, basis, source_instrument=None):
        self._day(origin)
        if self.locations[origin] > self.locations[day]:
            raise ValueError("Corporate acquisition origin cannot come from the future")
        previous = self.holding_started.get(inst)
        self.holding_started[inst] = min(previous, origin) if previous else origin
        self.audit.append({"session": day, "instrument": inst, "kind": "inherited_holding_origin",
            "origin": self.holding_started[inst], "source_origin": origin,
            "source_instrument": source_instrument or inst, "basis": basis})

    def _sync(self, state, day):
        self._day(day)
        active = active_instruments(state)
        for inst in sorted(active):
            self._origin(inst, day)
        for inst in sorted(set(self.holding_started)-active):
            self.audit.append({"session": day, "instrument": inst, "kind": "tradable_episode_closed",
                "origin": self.holding_started.pop(inst), "fractional_claims_do_not_bridge_reentry": True})
        return active

    def after_corporate_actions(self, before, after, ledger, day):
        self._day(day)
        self._sync(before, day)
        for row in ledger:
            if row.get("kind") not in {"bonus_shares", "share_exchange"}:
                continue
            if row.get("session") != day or not isinstance(row.get("evidence_sha256"), str) or len(row["evidence_sha256"]) != 64:
                raise ValueError("Corporate origin transfer requires the actual bound action ledger")
            if row["kind"] == "share_exchange":
                if Decimal(str(row["old_raw_shares"])) <= 0:
                    continue
                source, target = row["from"], row["to"]
                origin = self._origin(source, day)
                if Decimal(str(row["new_integer_shares"])) > 0:
                    self._inherit(target, origin, day, basis="confirmed_actual_share_exchange", source_instrument=source)
            else:
                if Decimal(str(row["record_date_shares"])) <= 0:
                    continue
                inst, record_date = row["instrument"], row["record_date"]
                origin = self.record_origins.get(record_date, {}).get(inst)
                if origin is None:
                    raise ValueError("Bonus source requires the actual finalized record-date holding origin")
                if inst in active_instruments(after):
                    self._inherit(inst, origin, day, basis="actual_bonus_record_date_origin")
            fraction = Decimal(str(row.get("unresolved_fractional_claim", "0")))
            if fraction:
                self.audit.append({"session": day, "instrument": row.get("to", row.get("instrument")),
                    "kind": "fractional_claim_origin_only", "origin": origin,
                    "quantity": str(fraction), "extends_tradable_episode": False})
        self._sync(after, day)

    def after_trades(self, before, after, trades, day):
        self._day(day)
        self._sync(before, day)
        quantities = _positive_positions(before)
        pending = set(_pending_bonus(before))
        for row in trades:
            if row.get("status") != "simulated_fill" or row.get("session") != day:
                raise ValueError("Holding origins require genuine same-session simulated fills")
            inst, side = row["instrument"], row["side"]
            quantity = Decimal(str(row["quantity"]))
            if not quantity.is_finite() or quantity <= 0 or quantity != quantity.to_integral_value():
                raise ValueError("Filled trade quantity must be positive integer shares")
            quantity = int(quantity)
            if side == "BUY":
                if not quantities.get(inst, 0) and inst not in pending:
                    self.holding_started[inst] = day
                    self.audit.append({"session": day, "instrument": inst, "kind": "actual_first_buy",
                        "origin": day, "quantity": quantity})
                else:
                    self._origin(inst, day)
                quantities[inst] = quantities.get(inst, 0) + quantity
            elif side == "SELL":
                self._origin(inst, day)
                if quantity > quantities.get(inst, 0):
                    raise ValueError("Actual exits exceed the held integer shares")
                quantities[inst] -= quantity
                if not quantities[inst] and inst not in pending:
                    self.audit.append({"session": day, "instrument": inst, "kind": "tradable_episode_closed",
                        "origin": self.holding_started.pop(inst), "fractional_claims_do_not_bridge_reentry": True})
            else:
                raise ValueError("Only actual BUY/SELL fills can alter a holding clock")
        if {inst: quantity for inst, quantity in quantities.items() if quantity} != _positive_positions(after):
            raise ValueError("Actual fills do not reconcile with the post-execution share ledger")
        self._sync(after, day)

    def close(self, state, day):
        active = self._sync(state, day)
        snapshot = {inst: self._origin(inst, day) for inst in _positive_positions(state)}
        if day in self.record_origins and self.record_origins[day] != snapshot:
            raise ValueError("Finalized record-date holding origins cannot be rewritten")
        self.record_origins[day] = snapshot
        pending = _pending_bonus(state)
        return [{"session": day, "instrument": inst, "holding_started": self._origin(inst, day),
            "holding_age_sessions": self.locations[day]-self.locations[self.holding_started[inst]],
            "actual_position_shares": _positive_positions(state).get(inst, 0),
            "pending_bonus_shares": str(pending.get(inst, Decimal(0))),
            "age_basis": "planned_execution_session_minus_actual_first_buy_session"} for inst in sorted(active)]


def _exit_audit(plan, result, audit):
    attempted = set(plan.exits)
    fills = [row for row in result.trades if row.get("status") == "simulated_fill"
        and row.get("side") == "SELL" and row["instrument"] in attempted]
    filled = {row["instrument"] for row in fills}
    active = active_instruments(result.state)
    return {**audit, "actual_exit_fill_securities": sorted(filled),
        "actual_exit_fill_count": len(filled), "actual_exit_fill_rows": len(fills),
        "actual_exit_cleared_securities": sorted(attempted-active),
        "actual_exit_cleared_count": len(attempted-active),
        "exit_intent_without_sell_fill": sorted(attempted-filled),
        "exit_intent_count": len(attempted), "actual_active_after_execution": sorted(active),
        "actual_active_count_after_execution": len(active),
        "actual_overflow_after_execution": max(0, len(active)-plan.policy.top_k),
        "rejected_exit_orders": sum(row.get("status") == "rejected" and row.get("side") in (None, "SELL")
            and row.get("instrument") in attempted for row in result.orders),
        "rejected_exit_order_basis": "exit-intent instrument with rejected SELL or side-absent precheck; original orders unchanged",
        "rejected_exit_substitutions": 0}


from copy import deepcopy
from stopat30m.ashare_stability_validation import (
    MODELS, _engine, session_policy, daily_inputs, _source_day,
    identity_execution_inputs, relevant_events, record_unsubscribed_rights,
    reserve_disclosed_cash_rounding, CashDistributionRoundingFundingError,
    a2, _rank, valuation_validity, path_stability,
)


def simulate_turnover(context, *, period, variant):
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

