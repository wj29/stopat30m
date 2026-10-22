"""Current close audit for an explicitly versioned same-account recovery.

This is the frozen A9 close operation with only the historical-gap gates routed
through the separately audited recovery contract. Native model/training helpers
and the original legacy module remain byte-identical; all prices, entitlements,
valuation and candidate guards use their original implementation.
"""
from decimal import Decimal
import re

from .ashare_forward import (
    _account_instruments,
    _action_book,
    _apply_actions,
    _candidate_targets,
    _close_price,
    _digest,
    _evidence,
    _security_metadata,
    _serial,
    _signal_bar_eligibility,
    _status,
    _verified_action_reference,
)
from .ashare_low_turnover_recovery import pending_gaps, previous_observed_close


def close_after_explicit_recovery(root, manifest, state, candidate, calendar, batch, clock):
    from stopat30m.backtest import ashare_execution as execution
    session = batch["session"]
    unresolved_gaps = pending_gaps(state, close_session=session)
    universe = batch.get("instruments", state["current_universe"])
    if (not isinstance(universe, list) or len(universe) != len(set(universe))
            or any(not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", key) for key in universe)):
        raise ValueError("Close batch needs an explicit current source universe")
    if sorted(universe) != state["current_universe"] and not batch.get("universe_evidence"):
        raise ValueError("A changed current source universe requires its own evidence")
    current_evidence = _evidence(batch.get("universe_evidence", manifest["universe_evidence"]))
    info, exclusions = _security_metadata(batch["metadata"], session, clock)
    prices, eligible = {}, []
    needed = set(universe) | set().union(*(_account_instruments(account) for account in state["accounts"].values()))
    for instrument in sorted(needed):
        price, reason = _close_price(batch["quotes"].get(instrument), session, clock)
        prices[instrument] = price
        if instrument in universe:
            if reason:
                exclusions[instrument] = reason
            elif instrument not in info:
                exclusions.setdefault(instrument, "missing_security_metadata")
            else:
                eligible.append(instrument)
    eligible, model_exclusions, snapshot_sha256 = _signal_bar_eligibility(candidate, batch, eligible, session)
    exclusions.update(model_exclusions)
    sessions = calendar.sessions(calendar.coverage_start, calendar.coverage_end)
    next_session = calendar.next_session(session)
    location = sessions.index(next_session) - sessions.index(manifest["first_possible_execution"])
    rebalance = location >= 0 and location % candidate.protocol["rebalance_sessions"] == 0
    reasons, valuations, allowed = [], {}, {}
    policy = execution.ExecutionPolicy(**manifest["execution_policy"])
    events = _action_book(state, batch, clock)
    action_records = {}
    reference_audit = {}
    for name in manifest["strategies"]:
        account = execution.PortfolioState.from_dict(state["accounts"][name])
        account, action_records[name] = _apply_actions(account, events, manifest=manifest,
            session=session, calendar=sessions, clock=clock)
        account_prices = dict(prices)
        unresolved = state["unresolved_actions"][name]
        previous = calendar.previous_session(session)
        for instrument, position in account.positions.items():
            quote = batch["quotes"].get(instrument, {})
            reference = quote.get("preclose")
            reference_verified = _verified_action_reference(account, instrument, session, reference, events)
            if reference_verified:
                unresolved.pop(instrument, None)
            comparison_mark = position.last_mark
            if (position.last_mark_date == previous and position.last_mark_basis == "live_observed_quote"
                    and state["engineering_recovery"]["activation"] is None
                    and previous in state.get("observation_gaps", {}) and not reference_verified):
                comparison_mark = previous_observed_close(state, batch, instrument, previous)
                reference_audit[instrument] = {"previous_session": previous,
                    "saved_mark": str(position.last_mark), "saved_mark_basis": position.last_mark_basis,
                    "verified_raw_close": str(comparison_mark),
                    "source_response_sha256": quote["source_response_sha256"],
                    "usage": "comparison_only_no_historical_valuation_or_posting"}
            if (position.last_mark_date == previous and comparison_mark is not None and reference is not None
                    and not reference_verified
                    and abs(execution.decimal(reference) - comparison_mark) >= Decimal("0.01")):
                unresolved.setdefault(instrument, {"detected_session": session,
                    "previous_raw_close": str(comparison_mark), "source_preclose": str(reference),
                    "reason": "verified_cash_or_share_entitlement_required"})
            if instrument in unresolved:
                account_prices[instrument] = None
                reasons.append(f"{name}:unresolved_corporate_action:{instrument}")
        suspended = [key for key in account.positions if _status(batch["quotes"].get(key, {}).get("tradestatus")) == "suspended"
            and key in info and info[key].status_session == session]
        result = execution.mark_to_close(account, session=session, prices=account_prices, calendar=sessions,
                                        policy=policy, verified_suspended=suspended)
        state["accounts"][name] = result.state.to_dict()
        allowed[name] = (result.valuation_complete and len(eligible) >= manifest["min_eligible"]
                         and not unresolved_gaps)
        valuations[name] = _serial({key: value for key, value in vars(result).items() if key != "state"})
        if not result.valuation_complete:
            reasons.append(f"{name}:partial_held_valuation_freezes_orders")
        if len(eligible) < manifest["min_eligible"]:
            reasons.append(f"{name}:insufficient_eligible_candidates")
        if unresolved_gaps:
            reasons.append(f"{name}:missing_session_observation_permanently_freezes_new_orders")
    targets, diagnostics = {name: {} for name in manifest["strategies"]}, {}
    if rebalance and any(allowed.values()):
        if not batch.get("snapshot_path"):
            raise ValueError("A verified current Qlib snapshot is required for close signal generation")
        targets, diagnostics = _candidate_targets(candidate, batch["snapshot_path"], session, eligible)
    previous_signal_day = calendar.previous_session(session)
    pending = state["signals"].get(previous_signal_day)
    if pending and pending["execution_session"] == session and session + "_open" not in state["completed"]:
        state["completed"][session + "_open"] = {"status": "expired_no_fill", "session": session,
            "reason": "Opening phase was missed; no retrospective fill", "recorded_at": clock.isoformat()}
        reasons.append("missed_open_expired_without_fill")
    signal = {"signal_date": session, "execution_session": next_session, "created_at": clock.isoformat(),
        "model_sha256": candidate.model_sha256, "model_manifest_sha256": candidate.manifest["manifest_sha256"],
        "snapshot_path": str(batch.get("snapshot_path")) if batch.get("snapshot_path") else None,
        "snapshot_manifest_sha256": snapshot_sha256,
        "universe": sorted(universe), "universe_evidence": current_evidence, "eligible": eligible,
        "exclusions": exclusions, "rebalance": rebalance, "allowed": allowed,
        "targets": targets, "diagnostics": diagnostics, "warnings": sorted(set(reasons))}
    signal["signal_sha256"] = _digest(signal)
    state["signals"][session] = signal
    state["current_universe"] = sorted(universe)
    state["valuations"] = [row for row in state["valuations"] if row["session"] != session]
    state["valuations"].append({"session": session, "observed_at": clock.isoformat(), "accounts": valuations})
    return {"status": "completed" if all(allowed.values()) else "partial", "phase": "close", "session": session,
            "execution_session": next_session, "rebalance": rebalance, "eligible_count": len(eligible),
            "exclusions": exclusions, "warnings": signal["warnings"], "signal_sha256": signal["signal_sha256"],
            "valuations": valuations, "corporate_actions": action_records,
            "recovery_previous_reference_audit": reference_audit}
