"""A2 raw CNY/integer-share comparison on a separately repaired Qlib vintage.

Training remains chronological; historical daily metadata and corporate actions
are separate evidence. Prior A1 fractional results are never overwritten.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from stopat30m.ashare_experiment import PackageHistoricalPool, VerifiedCorporateActions
from stopat30m.backtest import ashare_execution as ex
from stopat30m.data.ashare_actions import dividend_events, load_dividend_evidence, apply_action_overrides
from stopat30m.data.ashare_execution_data import historical_download_targets, load_execution_collection
from stopat30m.data.qlib_incremental import read_qlib_security
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot
from stopat30m.model.evaluator import compute_portfolio_metrics
from stopat30m.model.trainer import FixedQlibLGBSpec

POLICY = "a2-raw-integer-cny-fixed-alpha158-v1"


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n")


def _receipt(path):
    return {"path": str(Path(path).resolve()), "sha256": _sha(path)}


def board(instrument):
    if instrument.startswith("SH688"):
        return "star"
    if instrument.startswith("SZ30"):
        return "chinext"
    if instrument.startswith("SH60"):
        return "sse_main"
    if instrument.startswith(("SZ00", "SZ001", "SZ002", "SZ003")):
        return "szse_main"
    raise ValueError(f"Unsupported A2 stock board {instrument}")


def execution_policy(spec):
    return ex.ExecutionPolicy(initial_cash=spec["initial_cash_cny"], commission_rate=spec["commission_rate"],
        minimum_commission=spec["minimum_commission_cny_per_order"], transfer_rate=spec["transfer_rate"],
        sell_stamp_rate=spec["sell_stamp_rate"], slippage_bps=spec["slippage_bps"],
        dividend_tax_rate=spec["dividend_tax_reserve_rate"])


def validate_repaired_vintage(snapshot):
    declaration = json.loads((snapshot.dataset_dir/"incremental_manifest.json").read_text())
    securities = declaration.get("securities", {})
    selected = declaration.get("selection", {}).get("failed_instruments", [])
    if (declaration.get("status") != "whole_security_source_rebuild" or not declaration.get("base_unchanged")
            or len(selected) != 36 or set(selected) != set(securities)
            or any(row.get("parent_finite_sessions_now_unavailable_count") != 0
                   or row.get("last_complete_finite") != declaration.get("audited_common_finite_end")
                   for row in securities.values())):
        raise ValueError("All 36 whole-history repairs must pass their immutable source audit")
    return {"count": len(selected), "as_of": declaration["audited_common_finite_end"],
            "increment_manifest": _receipt(snapshot.dataset_dir/"incremental_manifest.json")}


def validate_training_protocol(origin, spec):
    protocol = json.loads((origin/"protocol.json").read_text())
    from dataclasses import asdict
    if (any(protocol["config"].get(key) != spec[key] for key in
            ("train_start", "train_end", "valid_start", "valid_end", "oos_start", "oos_end", "top_k", "rebalance_sessions"))
            or protocol.get("model_spec") != asdict(FixedQlibLGBSpec())
            or protocol.get("parameter_search") is not False or protocol.get("early_stopping") is not False
            or protocol.get("label") != "Ref($open,-6)/Ref($open,-1)-1"
            or protocol.get("feature_schema") != "qlib.contrib.data.handler.Alpha158 official 158"):
        raise ValueError("Repaired-data training differs from the declared fixed chronological comparison")


def audit_raw_prices(dataset, raw, symbols, start, end):
    """Float32 restoration tolerance, not an adjustable cross-source seam."""
    records = []
    for inst in symbols:
        source = raw.loc[raw.instrument.eq(inst)].set_index("date")
        stored = read_qlib_security(dataset, inst).loc[start:end]
        overlap = stored.index.intersection(source.index)
        actual = source.loc[overlap, ["open", "high", "low", "close"]].replace("", np.nan).astype(float)
        restored = stored.loc[overlap, ["open", "high", "low", "close"]].div(stored.loc[overlap, "factor"], axis=0)
        valid = actual.gt(0).all(axis=1) & np.isfinite(actual).all(axis=1) & np.isfinite(restored).all(axis=1)
        if not valid.any():
            raise ValueError(f"No actual raw/Qlib price overlap for {inst}")
        left, right = actual.loc[valid], restored.loc[valid]
        if not np.isclose(left, right, rtol=1e-6, atol=1e-6).all():
            raise ValueError(f"Restored Qlib OHLC differs from original raw execution evidence for {inst}")
        records.append({"instrument": inst, "overlap_sessions": int(valid.sum()),
            "max_relative_error": float(((left-right).abs()/left).max().max()),
            "max_absolute_error": float((left-right).abs().max().max())})
    return records


def audit_ex_references(raw, events, calendar, *, start, end):
    """Every raw reference adjustment must have a source corporate action.

    Roll theoretical ex references through suspensions, but never use them to
    execute a trade. Non-dividend rights issues remain unsupported and stop
    this accounting validation rather than creating a fictitious cash credit.
    """
    event_map = {}
    for event in events:
        event_map.setdefault((event["instrument"], event["ex_date"]), []).append(event)
    records, failures = [], []
    days = [day for day in calendar if start <= day <= end]
    for inst, group in raw.groupby("instrument"):
        rows = group.set_index("date")
        previous = None
        for day in [value for value in calendar if value <= end and value >= str(group.date.min())]:
            matching = event_map.get((inst, day), [])
            if previous is not None:
                for event in matching:
                    previous = (previous - Decimal(event.get("reference_cash_per_share", event["cash_per_share"]))) / (1 + Decimal(event.get("reference_bonus_ratio", event["bonus_ratio"])))
            if day not in rows.index:
                continue
            row = rows.loc[day]
            if not bool(row.source_complete_quote):
                continue
            actual = Decimal(row.preclose)
            if previous is not None and day in days:
                difference = abs(actual - ex.money(previous))
                if difference > Decimal("0.011"):
                    failures.append({"instrument": inst, "session": day, "actual_preclose": str(actual),
                                     "expected_reference": str(previous), "difference": str(difference),
                                     "event_ids": [event["event_id"] for event in matching]})
                elif matching or difference:
                    records.append({"instrument": inst, "session": day, "difference": str(difference),
                                    "event_ids": [event["event_id"] for event in matching]})
            previous = Decimal(row.close)
    return {"status": "passed" if not failures else "blocked_unexplained_ex_reference",
            "checked_stocks": int(raw.instrument.nunique()), "matched_adjustments": records,
            "unexplained": failures, "price_tick_tolerance_cny": "0.011"}


def _daily_inputs(raw, basic, day, symbols):
    quotes, metadata, closes = {}, {}, {}
    rows = raw.loc[raw.date.eq(day)].set_index("instrument")
    identities = basic.set_index("code")
    for inst in symbols:
        if inst not in rows.index:
            continue
        row = rows.loc[inst]
        complete = bool(row.source_complete_quote)
        def number(name):
            return Decimal(row[name]) if str(row[name]).strip() else None
        quotes[inst] = ex.OpeningQuote(session=day, open=number("open"), preclose=number("preclose"),
            tradestatus={"1": "trading", "0": "suspended"}.get(row.tradestatus, "unknown"), source="baostock.original_daily_execution",
            execution_price_basis="historical_observed_open")
        original = identities.loc[inst[:2].lower() + "." + inst[2:]]
        metadata[inst] = ex.SecurityInfo(instrument=inst, board=board(inst), listing_date=original.ipoDate,
            is_st=row.isST == "1", known_at=day + "T00:00:00+08:00",
            source="reconstructed_historical_daily_isST_and_static_IPO_identity", status_session=day,
            delisting_period=False)
        if complete:
            closes[inst] = number("close")
    return quotes, metadata, closes


def _targets(strategy, signal_day, location, normalized_close, pool, scores, actions, top_k):
    members = pool.members_on(signal_day)
    eligible = [inst for inst in members if not actions.excluded(inst, signal_day, close=True)
                and inst in normalized_close.columns and np.isfinite(normalized_close.at[signal_day, inst])
                and normalized_close.at[signal_day, inst] > 0]
    ranked = []
    if strategy == "equal_weight":
        selected = eligible
    elif strategy == "momentum":
        for inst in eligible:
            window = normalized_close.iloc[location-61:location][inst]
            if len(window) == 61 and window.notna().all() and window.gt(0).all():
                score = float(window.iloc[-1]/window.iloc[0]-1)
                if score > 0:
                    ranked.append((inst, score))
        selected = [inst for inst, _ in sorted(ranked, key=lambda item: (-item[1], item[0]))[:top_k]]
    else:
        try:
            today = scores.xs(pd.Timestamp(signal_day), level="datetime")
        except KeyError:
            raise ValueError("Required frozen prior-close model scores are unavailable") from None
        ranked = [(inst, float(today.loc[inst])) for inst in eligible
                  if inst in today.index and np.isfinite(today.loc[inst])]
        if not ranked:
            raise ValueError("No eligible prior-close model predictions")
        selected = [inst for inst, _ in sorted(ranked, key=lambda item: (-item[1], item[0]))[:top_k]]
    if not selected:
        return {}, members
    # Match the forward runner's finite precision equal weights; retain an
    # unallocated <1e-15 residual rather than producing a leverage rounding bit.
    weight = (Decimal(1) / len(selected)).quantize(Decimal("0.000000000000000001"), rounding=ROUND_FLOOR)
    return {inst: weight for inst in selected}, members


def _apply_events(state, day, dividends, actions, calendar, opening, policy, first_day):
    records = []
    for event in dividends:
        record_date = event["record_date"]
        if record_date < first_day:
            state.holdings_history.setdefault(record_date, {})
        terms = {"instrument": event["instrument"], "record_date": record_date, "session": day,
                 "known_at": event["known_at"], "evidence_sha256": event["source_response_sha256"],
                 "calendar": calendar}
        def save(result):
            nonlocal state
            state = result.state
            if not result.replayed:
                records.append(result.record)
        if day == event["ex_date"]:
            if Decimal(event["cash_per_share"]):
                save(ex.register_dividend_entitlement(state, event_id=event["event_id"]+":cash",
                    ex_date=event["ex_date"], pay_date=event["pay_date"], cash_per_share=event["cash_per_share"],
                    tax_rate=event.get("cash_dividend_tax_rate", policy.dividend_tax_rate), **terms))
            if Decimal(event["bonus_ratio"]):
                quote = opening.get(event["instrument"])
                save(ex.register_bonus_entitlement(state, event_id=event["event_id"]+":bonus",
                    ex_date=event["ex_date"], listing_date=event["listing_date"], ratio=event["bonus_ratio"],
                    price=quote.open if quote is not None and quote.open and quote.tradestatus == "trading" else None, **terms))
        if day == event["pay_date"] and Decimal(event["cash_per_share"]):
            save(ex.apply_cash_dividend(state, event_id=event["event_id"]+":cash", pay_date=event["pay_date"],
                cash_per_share=event["cash_per_share"], tax_rate=event.get("cash_dividend_tax_rate", policy.dividend_tax_rate), **terms))
        if day == event["listing_date"] and Decimal(event["bonus_ratio"]):
            quote = opening.get(event["instrument"])
            save(ex.apply_bonus_shares(state, event_id=event["event_id"]+":bonus", listing_date=event["listing_date"],
                ratio=event["bonus_ratio"], price=quote.open if quote is not None else None, **terms))
    for action in actions.conversions(day):
        if action["id"] in state.applied_actions:
            continue
        quote = opening.get(action["to_instrument"])
        if state.positions.get(action["from_instrument"]) and (quote is None or quote.tradestatus != "trading"):
            raise ValueError("Confirmed successor has no observed tradable opening price")
        result = ex.apply_share_exchange(state, action_id=action["id"], from_instrument=action["from_instrument"],
            to_instrument=action["to_instrument"], ratio=str(action["ratio"]), session=day,
            tradable_from=action["confirmation"]["tradable_from"], confirmed_known_at=action["confirmation"]["known_at"],
            evidence_sha256=actions.sha256, calendar=calendar, successor_price=quote.open if quote else None)
        state = result.state
        if not result.replayed:
            records.append(result.record)
    return state, records


def _simulate(strategy, raw, basic, normalized_close, scores, pool, calendar, spec, dividends, actions, policy):
    days = [day for day in calendar if spec["oos_start"] <= day <= spec["oos_end"]]
    if not days:
        raise ValueError("No actual A2 OOS sessions")
    symbols = sorted(set(raw.instrument) | {item["to_instrument"] for item in actions.actions})
    state, previous = ex.PortfolioState.initial(policy), policy.initial_cash
    curves, positions, claims, trades, orders, signals, ledger = [], [], [], [], [], [], []
    first_location = calendar.index(days[0])
    if first_location < 61:
        raise ValueError("A2 requires actual preceding sessions for momentum and signal timing")
    for sequence, day in enumerate(days):
        location = calendar.index(day); signal_day = calendar[location-1]
        opening, metadata, closes = _daily_inputs(raw, basic, day, symbols)
        suspended = [inst for inst in symbols if actions.suspended(inst, day)]
        state, records = _apply_events(state, day, dividends, actions, calendar, opening, policy, days[0])
        ledger.extend({**row, "strategy": strategy} for row in records)
        today_trades = []
        if sequence % spec["rebalance_sessions"] == 0:
            weights, members = _targets(strategy, signal_day, location, normalized_close, pool, scores, actions, spec["top_k"])
            result = ex.execute_target_weights(state, session=day, signal_date=signal_day, targets=weights,
                quotes=opening, metadata=metadata, calendar=calendar, policy=policy, verified_suspended=suspended)
            state, today_trades = result.state, result.trades
            trades.extend({**row, "strategy": strategy} for row in today_trades)
            orders.extend({**row, "strategy": strategy} for row in result.orders)
            signals.extend({"session": day, "signal_date": signal_day, "instrument": inst,
                "target_weight": str(weights.get(inst, 0)), "selected": inst in weights, "strategy": strategy}
                for inst in members)
        valuation = ex.mark_to_close(state, session=day, prices=closes, calendar=calendar,
                                     policy=policy, verified_suspended=suspended)
        state, equity = valuation.state, valuation.equity_lower_bound
        if equity <= 0 or state.cash < 0:
            raise ValueError("Invalid A2 raw cash/share NAV")
        curves.append({"date": day, "strategy": strategy, "equity_cny": str(equity),
            "equity_with_claim_estimate_cny": str(valuation.equity_with_claim_estimate),
            "equity_upper_bound_cny": str(valuation.equity_upper_bound), "cash_cny": str(state.cash),
            "nav": float(equity/policy.initial_cash), "net_return": float(equity/previous-1),
            "valuation_complete": valuation.valuation_complete,
            "commission_cny": str(sum((Decimal(row["commission"]) for row in today_trades), Decimal(0))),
            "transfer_fee_cny": str(sum((Decimal(row["transfer_fee"]) for row in today_trades), Decimal(0))),
            "stamp_tax_cny": str(sum((Decimal(row["stamp_tax"]) for row in today_trades), Decimal(0))),
            "slippage_cny": str(sum((Decimal(row["slippage_cost"]) for row in today_trades), Decimal(0)))})
        positions.extend({**row, "strategy": strategy} for row in valuation.positions)
        claims.extend({**row, "session": day, "strategy": strategy} for row in valuation.claims)
        previous = equity
    return {"curves": curves, "positions": positions, "claims": claims, "trades": trades,
            "orders": orders, "signals": signals, "corporate_actions": ledger,
            "final_state": state.to_dict()}


def run_integer_comparison(snapshot_path, execution_collection, dividends_collection, output_dir, *,
        spec_path, corporate_actions_path, training_origin, action_overrides_path=None):
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("A2 output exists; preserve the previous result")
    spec = json.loads(Path(spec_path).read_text())
    policy = execution_policy(spec)
    if spec.get("parameter_search") is not False or spec["rebalance_sessions"] != 5:
        raise ValueError("A2 strategy parameters must retain the declared fixed experiment")
    snapshot = load_qlib_snapshot(snapshot_path)
    repair_audit = validate_repaired_vintage(snapshot)
    raw, basic, execution_receipt = load_execution_collection(execution_collection)
    _, dividend_receipt = load_dividend_evidence(dividends_collection)
    events = dividend_events(dividends_collection, start=spec["oos_start"], end=spec["oos_end"])
    if action_overrides_path is not None:
        events = apply_action_overrides(events, action_overrides_path)
        events = [row for row in events if spec["oos_start"] <= row["ex_date"] <= spec["oos_end"]]
    calendar = (snapshot.dataset_dir/"calendars/day.txt").read_text().splitlines()
    membership = snapshot.dataset_dir/"instruments"/(spec["universe"]+".txt")
    first_day = next(day for day in calendar if day >= spec["oos_start"])
    preceding = calendar[calendar.index(first_day)-1]
    symbols = historical_download_targets(membership, preceding, spec["oos_end"])
    if not set(symbols).issubset(set(raw.instrument)) or set(dividend_receipt["instruments"]) != set(execution_receipt["request"]["instruments"]):
        raise ValueError("Raw execution/dividend evidence omits historical OOS members")
    source_audit = audit_raw_prices(snapshot.dataset_dir, raw, symbols, preceding, spec["oos_end"])
    actions = VerifiedCorporateActions(corporate_actions_path)
    reference_audit = audit_ex_references(raw, events, calendar, start=spec["oos_start"], end=spec["oos_end"])
    bound = {"spec": _receipt(spec_path), "snapshot_manifest": _receipt(snapshot.path/"manifest.json"),
        "execution_collection": _receipt(Path(execution_collection)/"execution_collection.json"),
        "dividend_collection": _receipt(Path(dividends_collection)/"collection.json"),
        "merger_actions": _receipt(corporate_actions_path),
        "exchange_rules": _receipt(Path(__file__).resolve().parents[1]/"docs/research/a2_execution_rules.json"),
        "implementation": _receipt(__file__), "engine": _receipt(ex.__file__)}
    if action_overrides_path is not None:
        bound["issuer_action_overrides"] = _receipt(action_overrides_path)
    for event in events:
        if event.get("override_evidence"):
            bound["issuer_evidence:"+event["instrument"]+":"+event["ex_date"]] = event["override_evidence"]
    output.mkdir(parents=True, exist_ok=False)
    _write(output/"protocol.json", {"policy": POLICY, "spec": spec, "execution_policy": policy.to_dict(),
        "repair_audit": repair_audit,
        "bound_inputs": bound, "membership_status": "package_declared_intervals_not_full_PIT",
        "metadata_assumption": "Source historical daily isST is assumed publicly observable at that session open; current static IPO identity is reconstructed, not independent announcement-time evidence.",
        "ordinary_public_holder_assumption": True, "first_results_not_used_to_tune_rules": True})
    pd.DataFrame(source_audit).to_csv(output/"raw_price_audit.csv", index=False)
    _write(output/"reference_price_audit.json", reference_audit)
    _write(output/"dividend_events.json", {"events": events, "source_generation": dividend_receipt["generation"]})
    report = {"policy": POLICY, "comparison_complete": False, "metrics": {}, "activation_passed": False,
        "status": "blocked", "execution_ready_for_broker": False, "spec": spec,
        "limitations": ["Reconstructed source constituents and daily ST metadata are not a full announcement-time PIT database.",
            "Daily opening bars cannot prove auction queue position, market impact, intraday halts or actual broker fills.",
            "Cash dividend tax uses a fixed conservative 20% reserve, not exact holding-period personal tax.",
            "Corporate-action fractional allocation remains a non-spendable claim; conservative NAV excludes uncertain tails.",
            "The 100,000 CNY paper balance does not establish the user's board-access permissions or suitability."]}
    try:
        if reference_audit["unexplained"]:
            raise ValueError(f"Unexplained original ex-reference changes: {len(reference_audit['unexplained'])}")
        origin = Path(training_origin)
        validate_training_protocol(origin, spec)
        training_manifest = json.loads((origin/"manifest.json").read_text())
        if training_manifest["snapshot_manifest_sha256"] != snapshot.manifest_sha256:
            raise ValueError("A2 predictions must be refit on the repaired snapshot, not relabelled old-vintage scores")
        for name, digest in training_manifest["files"].items():
            if Path(name).is_absolute() or ".." in Path(name).parts or _sha(origin/name) != digest:
                raise ValueError("Frozen repaired-data model/prediction bytes changed")
        predictions = pd.read_csv(origin/"predictions.csv", parse_dates=["datetime"], float_precision="round_trip")
        scores = predictions.set_index(["datetime", "instrument"])["score"]
        if scores.index.duplicated().any():
            raise ValueError("Duplicate frozen model predictions")
        pool = PackageHistoricalPool(membership, start=spec["train_start"], end=spec["oos_end"])
        closes = {inst: read_qlib_security(snapshot.dataset_dir, inst)["close"] for inst in symbols}
        normalized = pd.DataFrame(closes).reindex(calendar)
        results = {}
        for strategy in ("lightgbm", "momentum", "equal_weight"):
            results[strategy] = _simulate(strategy, raw, basic, normalized, scores, pool, calendar, spec, events, actions, policy)
        for name in ("curves", "positions", "claims", "trades", "orders", "signals", "corporate_actions"):
            pd.DataFrame([row for result in results.values() for row in result[name]]).to_csv(output/(name+".csv"), index=False)
        for strategy, result in results.items():
            frame = pd.DataFrame(result["curves"])
            metrics = compute_portfolio_metrics(frame.net_return)
            metrics.update({"final_equity_cny": result["curves"][-1]["equity_cny"],
                "trade_count": len(result["trades"]), "rejected_order_count": sum(row["status"]=="rejected" for row in result["orders"]),
                "incomplete_valuation_days": int((~frame.valuation_complete).sum()),
                "commission_cny": str(sum((Decimal(row["commission_cny"]) for row in result["curves"]), Decimal(0))),
                "transfer_fee_cny": str(sum((Decimal(row["transfer_fee_cny"]) for row in result["curves"]), Decimal(0))),
                "stamp_tax_cny": str(sum((Decimal(row["stamp_tax_cny"]) for row in result["curves"]), Decimal(0))),
                "slippage_cny": str(sum((Decimal(row["slippage_cny"]) for row in result["curves"]), Decimal(0)))})
            report["metrics"][strategy] = metrics
            _write(output/(strategy+"_final_state.json"), result["final_state"])
        model_metrics = report["metrics"]["lightgbm"]
        report.update(comparison_complete=True, status="completed_integer_share_research",
            activation_passed=bool(model_metrics["total_return"]>0 and abs(model_metrics["max_drawdown"]) <= float(spec["activation_gates"]["model_absolute_max_drawdown_at_most"])
                                   and model_metrics["incomplete_valuation_days"]==0))
        report["training_origin"] = _receipt(origin/"manifest.json")
    except (ValueError, KeyError, OSError) as exc:
        report.update(status="blocked_data_or_accounting_validation", error=str(exc), metrics={})
    actions.assert_unchanged()
    for receipt in bound.values():
        if _sha(receipt["path"]) != receipt["sha256"]:
            raise ValueError("A2 inputs or accounting implementation changed during comparison")
    _write(output/"report.json", report)
    manifest = {"schema_version": 1, "policy": POLICY, "comparison_complete": report["comparison_complete"],
        "activation_passed": report["activation_passed"], "bound_inputs": bound,
        "files": {str(path.relative_to(output)):_sha(path) for path in sorted(output.rglob("*")) if path.is_file()}}
    _write(output/"manifest.json", manifest)
    return {"output_dir": str(output), "report": report, "manifest": manifest}
