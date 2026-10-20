"""Prior-rank minimum holding and exit-intent caps over the frozen rank plan.

Only the planner is extended. The historical/modern fee, fill, rejection and
portfolio engines stay unchanged, including their treatment of failed exits.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import date
import re

from stopat30m import ashare_optimization_execution as legacy

POLICY_VERSION = "fixed-prior-rank-execution-session-holding-age-exit-intent-cap-v1"
AGE_BASIS = "index(planned_execution_session)-index(actual_first_buy_session);buy_session_age0"


def _day(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("An exact actual exchange-session date is required")
    date.fromisoformat(value)
    return value


def _active(state):
    """Use precisely the original rank planner's incumbent exposure definition."""
    instruments = {inst for inst, position in state.positions.items() if position.quantity}
    instruments.update(row["instrument"] for row in state.entitlements.values()
        if row.get("kind") == "bonus_shares" and row.get("status") == "pending"
        and legacy.ex.decimal(row["quantity"]) > 0)
    return instruments


def build_turnover_plan(state, *, signal_date, session, ranked_instruments,
        raw_signal_prices, boards, signal_equity, calendar, holding_started,
        minimum_hold_sessions, max_replacements, execution_policy, base_policy):
    """Return ``(RankBufferPlan, audit)`` using frozen prior-close quantities.

    Age uses the known execution-session calendar index, without reading an
    execution quote. A cap counts securities selected for exit attempts, even
    if the original executor subsequently rejects or partly fills those exits.
    No successful-exit cap, second-pass replacement, rescaling of retained
    shares, or new execution-day eligibility rule is introduced.

    ``holding_started`` is owned by the simulation's actual-fill tracker. The
    planner never reconstructs origins from position marks or holding quantity.
    A partly sold name or pending bonus remains active until its actual exposure
    is cleared. Corporate-origin transfers must be explicitly resolved by the
    caller's independently declared tracker policy.
    """
    if (type(minimum_hold_sessions) is not int or minimum_hold_sessions < 0
            or max_replacements is not None
            and (type(max_replacements) is not int or max_replacements < 0)):
        raise ValueError("Minimum holding age and exit-intent cap must be nonnegative integers")
    is_baseline = minimum_hold_sessions == 0 and max_replacements is None
    _day(signal_date); _day(session)
    days = list(calendar)
    if not days or days != sorted(set(days)):
        raise ValueError("A unique ordered actual exchange-session calendar is required")
    for day in days:
        _day(day)
    locations = {day: index for index, day in enumerate(days)}
    if (signal_date not in locations or session not in locations
            or locations[session] != locations[signal_date] + 1):
        raise ValueError("The ranking signal must be the immediately preceding actual exchange session")
    if state.session is not None and state.session > session:
        raise ValueError("Portfolio state cannot come from a future execution session")
    if not is_baseline and not isinstance(holding_started, Mapping):
        raise ValueError("Explicit actual first-buy session origins are required")
    incumbents = _active(state)
    if not is_baseline:
        for row in state.entitlements.values():
            if row.get("kind") == "bonus_shares" and row.get("status") == "pending":
                quantity = legacy.ex.decimal(row["quantity"])
                if quantity < 0:
                    raise ValueError("Pending bonus delivery cannot have a negative quantity")
    ages, origins = {}, {}
    for inst in sorted(incumbents) if not is_baseline else ():
        if inst not in holding_started:
            raise ValueError("Missing actual first-buy session for active instrument: " + inst)
        origin = _day(holding_started[inst])
        if origin not in locations or locations[origin] > locations[session]:
            raise ValueError("Active holding origin is outside the actual calendar or after execution: " + inst)
        origins[inst] = origin
        ages[inst] = locations[session] - locations[origin]
    ranked = list(ranked_instruments)
    base = legacy.build_rank_buffer_plan(state, signal_date=signal_date,
        ranked_instruments=ranked, raw_signal_prices=raw_signal_prices, boards=boards,
        signal_equity=signal_equity, policy=base_policy, execution_policy=execution_policy)
    ranks = {inst: rank for rank, inst in enumerate(ranked, 1)}
    rank_key = lambda inst: (ranks.get(inst, len(ranked)+1), inst)
    # Missing prior ranks are worst; within missing ranks use code ascending.
    exit_key = lambda inst: (0 if inst not in ranks else 1, -ranks.get(inst, 0), inst)
    locked = {inst for inst in incumbents if ages[inst] < minimum_hold_sessions} if not is_baseline else set()
    exit_priority = sorted(set(base.exits)-locked, key=exit_key)
    allowed = exit_priority if max_replacements is None else exit_priority[:max_replacements]
    cap_retained = set(exit_priority)-set(allowed)
    if is_baseline:
        # Preserve the original plan itself, including ordering/filtered audit,
        # quantities, policy, minimum fees and exact Decimal string budgets.
        plan = base
    else:
        retained = tuple(sorted(incumbents-set(allowed), key=rank_key))
        entry_slots = max(0, base.policy.top_k-len(retained))
        # Restricting base exits only increases retained names. Consequently
        # the original entry list's prefix is the same ranked, affordability-
        # checked candidate list with precisely the original per-slot budget.
        entries = base.entries[:entry_slots]
        if entry_slots == 0:
            filtered = ()
        elif len(entries) == entry_slots:
            cutoff = ranks[entries[-1][0]]
            filtered = tuple(row for row in base.filtered if ranks[row[0]] <= cutoff)
        else:
            filtered = base.filtered
        plan = replace(base, selected=retained+tuple(row[0] for row in entries), retained=retained,
            exits=tuple(allowed), entries=entries, filtered=filtered)
        if (set(plan.retained) | set(plan.exits) != incumbents
                or set(plan.retained) & set(plan.exits)
                or set(plan.retained) & {row[0] for row in plan.entries}
                or len(plan.entries) > max(0, base.policy.top_k-len(plan.retained))
                or not locked.issubset(plan.retained)):
            raise ValueError("The constrained plan lost an incumbent exposure or locked slot")
    audit = {"policy_version": POLICY_VERSION, "signal_date": signal_date, "session": session,
        "minimum_hold_sessions": minimum_hold_sessions, "max_replacements": max_replacements,
        "calendar_age_basis": AGE_BASIS, "holding_started": origins, "holding_age_sessions": ages,
        "holding_clock_enforced": not is_baseline,
        "rank_and_budget_basis": "preceding_actual_close_only_no_execution_quotes",
        "active_instruments": sorted(incumbents), "active_count_before_execution": len(incumbents),
        "minimum_hold_locked": sorted(locked, key=rank_key), "base_desired_exits": list(base.exits),
        "base_retained": list(base.retained), "exit_priority_worst_first": exit_priority,
        "allowed_exits": list(plan.exits), "cap_forced_retained": sorted(cap_retained, key=rank_key),
        "retained": list(plan.retained), "entry_count": len(plan.entries),
        "target_top_k": base.policy.top_k, "keep_rank": base.policy.keep_rank,
        "entry_slots": max(0, base.policy.top_k-len(plan.retained)),
        "overflow_count": max(0, len(plan.retained)-base.policy.top_k),
        "budget_per_slot_cny": plan.budget_per_slot_cny,
        "initial_build": not incumbents, "unconstrained_baseline_exact": is_baseline,
        "exit_cap_counts": "frozen_exit_intent_securities_not_submitted_orders_or_fills",
        "failed_exit_can_leave_actual_exposure_above_target": True,
        "execution_and_failed_order_handling": "unchanged_original_rank_executor_no_replacement_or_new_slot_hard_cap",
        "actual_exposure_after_execution": "must_be_reported_by_simulation_from_actual_fills_including_pending_bonus"}
    return plan, audit
