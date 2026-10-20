"""Independent2025/2026 validation with explicitly funded noncash tax reserves.

Completed primary protocols and implementations remain unchanged. This window
starts a new cash account and is not a fabricated2023--2026 continuous ledger.
"""
from __future__ import annotations
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import shutil
import numpy as np
import pandas as pd
from stopat30m import ashare_union_validation as primary
from stopat30m.ashare_union_validation import (
    MODELS, EXECUTION, VALIDITY as PRIMARY_VALIDITY, sha, _save, _proof,
    _assert_proofs, _read_daily, _engine, session_policy, execution_year_scores,
    identity_execution_inputs, _rank, _source_day, path_stability, score_quality,
    a2, daily_inputs, relevant_events, record_unsubscribed_rights,
    valuation_validity, VerifiedCorporateActions, audit_source_proofs,
    load_verified_historical_scope, read_qlib_security, FixedQlibLGBSpec,
    guard_qlib_output_path, load_qlib_snapshot,
)
from stopat30m.ashare_stability_research import (
    LABEL, _load_protocol, validate_market_audit, assert_market_originals_unchanged,
    implementation_sha256 as training_implementation_sha256,
)

VALIDITY = deepcopy(PRIMARY_VALIDITY)
VALIDITY["capital_tax_accounting"]["insufficient_cash_to_cover_tax"] = (
    "full_evidenced_extra_tax_reserved_from_record_date_holders_cash_on_ex_date_before_orders; "
    "insufficient_existing_cash_stops_and_invalidates_path_without_borrowing")
VALIDITY["stability_window"] = "separate2025_2026_cash_start_not_four_year_continuous_account"
VALIDITY["extra_tax_timing"] = "conservative_ex_date_cash_expense_not_exact_broker_withholding_date_no_refunds"
VALIDITY["issuer_cash_rounding"] = (
    "explicit_original_issuer_gross_cent_floor_only; reserve_legacy_half_up_gross_excess_before_orders; "
    "retain_original_conservative_tax_no_refund; other_dividends_retain_declared_legacy_cent_rounding")


class CashDistributionRoundingFundingError(ValueError):
    pass


def reserve_disclosed_cash_rounding(state, day, events):
    """Offset an explicitly evidenced gross-cent floor without rewriting claims.

    The ordinary engine's gross/withholding records stay intact. A separate
    expense removes its possible one-cent gross overcredit on the ex-date;
    retaining the original tax can only reduce net cash further. This is a
    conservative reserve, not a reconstruction of the broker's tax clock.
    """
    pending, seen = [], set()
    if state.session is not None and state.session > day:
        raise ValueError("Cannot reserve cash rounding before current state session")
    for event in events:
        if "cash_payment_gross_rounding" not in event:
            continue
        identity = "issuer_cash_rounding:" + event["event_id"]
        if identity in seen:
            raise ValueError("Duplicate issuer cash rounding event")
        seen.add(identity)
        prior = state.applied_actions.get(identity)
        if event["ex_date"] != day and prior is None:
            continue
        proof = event.get("component_resolution_document", {})
        if (event["cash_payment_gross_rounding"] != "ROUND_DOWN"
                or not proof.get("path") or len(proof.get("sha256", "")) != 64):
            raise ValueError("Cash rounding requires the exact bound original issuer resolution")
        _assert_proofs({"issuer_cash_rounding_resolution": proof})
        quantity = state.holdings_history.get(event["record_date"], {}).get(event["instrument"], 0)
        if event["record_date"] not in state.holdings_history:
            raise ValueError("Cash rounding requires finalized actual record holdings")
        cash = Decimal(event["cash_per_share"])
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0 or not cash.is_finite() or cash <= 0:
            raise ValueError("Invalid record quantity or issuer cash amount")
        terms = {key: event[key] for key in ("event_id", "instrument", "record_date", "ex_date", "pay_date",
            "known_at", "cash_per_share", "source_response_sha256", "cash_payment_gross_rounding")}
        payload = {"terms": terms, "issuer_resolution": proof, "record_date_shares": quantity}
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if prior is not None:
            if prior.get("sha256") != digest:
                raise ValueError("Previously reserved cash rounding terms or record holdings changed")
            continue
        known = pd.Timestamp(event["known_at"])
        if (not event["record_date"] < day <= event["pay_date"] or pd.isna(known) or known.tzinfo is None
                or known > pd.Timestamp(day + "T09:30:00+08:00")
                or day in state.holdings_history
                or any(p.bought_today and p.bought_date == day for p in state.positions.values())):
            raise ValueError("Issuer cash rounding must be known and reserved before ex-date orders")
        exact = quantity * cash
        legacy = exact.quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
        floor = exact.quantize(Decimal(".01"), rounding=ROUND_DOWN)
        pending.append((identity, digest, event, quantity, legacy, floor))
    if not pending:
        return state, []
    required = sum((legacy - floor for _, _, _, _, legacy, floor in pending), Decimal(0))
    if state.cash < required:
        raise CashDistributionRoundingFundingError("Insufficient existing cash for disclosed cash rounding reserve")
    result, journal = deepcopy(state), []
    for identity, digest, event, quantity, legacy, floor in pending:
        reserved, before = legacy - floor, result.cash
        result.cash -= reserved
        entry = {"kind": "cash_distribution_rounding_reserve", "event_id": event["event_id"],
            "instrument": event["instrument"], "record_date": event["record_date"], "ex_date": day,
            "session": day, "record_date_shares": quantity, "legacy_gross_cash_cny": str(legacy),
            "issuer_floored_gross_cash_cny": str(floor), "rounding_reserved_cny": str(reserved),
            "cash_before": str(before), "cash_change": str(-reserved), "cash_after": str(result.cash),
            "original_tax_reserve_reduced": False, "source_response_sha256": event["source_response_sha256"],
            "issuer_resolution": event["component_resolution_document"]}
        result.applied_actions[identity] = {"sha256": digest, "record": entry}
        journal.append(entry)
    return result, journal


def read_bound_execution_model_prices(source, instrument, bound):
    """Bind the exact buffers consumed for momentum/IC, then recheck at exit."""
    prices = read_qlib_security(source.dataset_dir, instrument, fields=("open", "close"))
    for field in ("open", "close"):
        name = f"features/{instrument.lower()}/{field}.day.bin"
        digest = source.manifest["files"][name]["sha256"]
        if prices.attrs.get("checksums", {}).get(field) != digest:
            raise ValueError("Consumed model price bytes differ from the frozen snapshot")
        bound["consumed_model_price:" + name] = {
            "path": str((source.dataset_dir / name).resolve()), "sha256": digest}
    return prices


def implementation_proofs():
    from stopat30m.backtest import ashare_execution, ashare_historical_execution
    from stopat30m import ashare_optimization_execution, ashare_historical_optimization_execution, ashare_stability_rank_execution
    from stopat30m.data import a5_capital_tax, a5_distribution_tax
    return {name: _proof(path) for name, path in {
        "union_validation": __file__, "modern_engine": ashare_execution.__file__,
        "historical_engine": ashare_historical_execution.__file__,
        "modern_rank_adapter": ashare_optimization_execution.__file__,
        "stability_rank_adapter": ashare_stability_rank_execution.__file__,
        "historical_rank_adapter": ashare_historical_optimization_execution.__file__,
        "corporate_actions": a2.__file__, "rights_and_validity": daily_inputs.__code__.co_filename,
        "event_selection": relevant_events.__code__.co_filename,
        "capital_tax_adapter": a5_capital_tax.__file__,
        "capital_tax_spec": a5_capital_tax.SPEC_PATH,
        "distribution_tax_adapter": a5_distribution_tax.__file__,
        "distribution_tax_spec": a5_distribution_tax.SPEC_PATH,
        "primary_validation_helpers": primary.__file__}.items()}


def _engine(day):
    engine, adapter = primary._engine(day)
    if day >= "2025-01-01":
        from stopat30m import ashare_stability_rank_execution
        adapter = ashare_stability_rank_execution
    return engine, adapter


def freeze_stability_protocol(path, snapshot_path, scope_directory, *, evaluation_end,
        execution_years=(2025, 2026), historical_rules_path=None, modern_rules_path=None,
        rule_original_evidence_path=None):
    """Declare parameters, chronology and exact source bytes before fitting/PnL."""
    path = guard_qlib_output_path(path)
    if path.exists():
        raise ValueError("Preserve the prior union research declaration")
    years = list(execution_years)
    if years != [2025, 2026]:
        raise ValueError("This separate stability generation requires2025/2026 execution years")
    if str(evaluation_end)[:4] != str(years[-1]):
        raise ValueError("Evaluation end must belong to the last declared execution year")
    source = load_qlib_snapshot(snapshot_path, verify=True)
    calendar = (source.dataset_dir / "calendars/day.txt").read_text().splitlines()
    if evaluation_end not in calendar:
        raise ValueError("Evaluation must end on an original actual exchange session")
    scope = load_verified_historical_scope(scope_directory, start=f"{years[0]-5}-01-01",
        end=evaluation_end, require_official=True)
    scope.assert_unchanged()
    from stopat30m.backtest.ashare_historical_execution import verify_historical_rules
    historical = Path(historical_rules_path or Path(__file__).parents[1] /
                      "docs/research/a4_historical_execution_rules.json")
    verify_historical_rules(historical)
    rules = {"historical": _proof(historical)}
    if years[-1] >= 2025:
        modern = Path(modern_rules_path or Path(__file__).parents[1] / "docs/research/a2_execution_rules.json")
        declaration = json.loads(modern.read_text())
        if declaration.get("rule_version") != "a2-raw-integer-2025-modern-rules-v1":
            raise ValueError("Modern execution requires its separate dated rule declaration")
        rules["modern"] = _proof(modern)
    if rule_original_evidence_path is not None:
        original = Path(rule_original_evidence_path).resolve()
        evidence = json.loads(original.read_bytes())
        if (evidence.get("kind") != "a5_secondary_original_rule_evidence"
                or evidence.get("status") != "selected_provisions_original_verified_not_complete_broker_execution_certification"):
            raise ValueError("Supplementary rule originals require their explicit reviewed source receipt")
        rules["supplementary_original_review"] = _proof(original)
        rules.update(audit_source_proofs(evidence, original.parent, prefix="supplementary_rule_original"))
    payload = {"schema_version": 1, "kind": "a5_union_stability_fixed_research_protocol",
        "declared_at": datetime.now(timezone.utc).isoformat(),
        "source_snapshot_manifest_sha256": source.manifest_sha256,
        "historical_scope_manifest_sha256": sha(Path(scope_directory) / "manifest.json"),
        "implementation_sha256": training_implementation_sha256(),
        "execution_implementations": implementation_proofs(), "execution_rules": rules,
        "models": list(MODELS), "execution": EXECUTION, "validity": VALIDITY,
        "periods": {**{str(year): [f"{year}-01-01", min(f"{year}-12-31", evaluation_end)] for year in years},
                    "continuous": [f"{years[0]}-01-01", evaluation_end]},
        "training": {"execution_years": years, "evaluation_end": evaluation_end,
            "lookback_calendar_years": 5, "purge_future_sessions": 6, "label": LABEL,
            "fixed_lightgbm_spec": asdict(FixedQlibLGBSpec()), "hyperparameter_search": False,
            "early_stopping": False,
            "membership_filter": "signal_date_only_no_future_membership_survival_condition"}}
    path.parent.mkdir(parents=True, exist_ok=True)
    _save(path, payload)
    return payload


def load_stability_context(snapshot_path, scope_directory, protocol_path, market_audit_path,
                       predictions_directory, merger_actions_path):
    source = load_qlib_snapshot(snapshot_path, verify=True)
    protocol, training = _load_protocol(protocol_path, source, scope_directory)
    if (protocol.get("models") != list(MODELS) or protocol.get("execution") != EXECUTION
            or protocol.get("validity") != VALIDITY):
        raise ValueError("Execution/validity differs from the fixed union declaration")
    if protocol.get("execution_implementations") != implementation_proofs():
        raise ValueError("Execution implementation changed since its prior declaration")
    _assert_proofs(protocol["execution_rules"])
    audit = json.loads(Path(market_audit_path).read_text())
    basic_path = audit["stock_basic_path"]
    calendar = (source.dataset_dir / "calendars/day.txt").read_text().splitlines()
    first = next(day for day in calendar if day >= f"{training['execution_years'][0]}-01-01")
    validate_market_audit(audit, source_sha=source.manifest_sha256,
        scope_sha=sha(Path(scope_directory) / "manifest.json"), stock_basic_sha=sha(basic_path),
        coverage_start=first, coverage_end=training["evaluation_end"])
    assert_market_originals_unchanged(audit, basic_path)
    bound = dict(audit["bound_inputs"])
    needed = {"daily_csv", "stock_basic", "dividend_events", "rights_events", "identity_events", "distribution_tax_contracts"}
    if not needed.issubset(bound):
        raise ValueError("Market audit lacks its canonical consumed execution inputs")
    raw, basic = _read_daily(bound["daily_csv"]["path"]), pd.read_csv(basic_path, dtype=str, keep_default_na=False)
    pred_dir = Path(predictions_directory).resolve()
    manifest = json.loads((pred_dir / "manifest.json").read_text())
    if (manifest.get("kind") != "a5_union_stability_fixed_predictions"
            or manifest.get("source_manifest_sha256") != source.manifest_sha256
            or manifest.get("inputs", {}).get("declaration", {}).get("sha256") != sha(protocol_path)
            or manifest.get("inputs", {}).get("market_audit", {}).get("sha256") != sha(market_audit_path)):
        raise ValueError("Model package differs from the declared, verified union source generation")
    for name, digest in manifest["files"].items():
        original = (pred_dir / name).resolve()
        if not original.is_relative_to(pred_dir) or sha(original) != digest:
            raise ValueError("Frozen model/prediction artifact bytes changed")
        bound["model_artifact:" + name] = _proof(original)
    scope = load_verified_historical_scope(scope_directory,
        start=calendar[calendar.index(first)-1], end=training["evaluation_end"], require_official=True)
    symbols = sorted(set(raw.instrument))
    closes, opens = {}, {}
    for inst in symbols:
        original = read_bound_execution_model_prices(source, inst, bound)
        closes[inst], opens[inst] = original["close"].copy(), original["open"].copy()
    normalized, normalized_open = pd.DataFrame(closes).reindex(calendar), pd.DataFrame(opens).reindex(calendar)
    momentum = normalized.div(normalized.shift(VALIDITY["momentum_lookback_actual_sessions"])).sub(1)
    momentum.index.name, momentum.columns.name = "datetime", "instrument"
    momentum.index = pd.to_datetime(momentum.index)
    momentum_scores = momentum.stack(future_stack=True).dropna().rename("score")
    actions = VerifiedCorporateActions(merger_actions_path)
    bound.update({"protocol": _proof(protocol_path), "market_audit": _proof(market_audit_path),
        "predictions_manifest": _proof(pred_dir / "manifest.json"), "merger_actions": _proof(merger_actions_path)})
    bound.update(audit_source_proofs(actions.payload, actions.path.parent, prefix="merger_original"))
    bound.update(protocol["execution_implementations"])
    bound.update({"rules:"+key: value for key, value in protocol["execution_rules"].items()})
    _assert_proofs(bound)
    return {"protocol": protocol, "calendar": calendar, "pool": scope, "raw": raw, "basic": basic,
        "normalized": normalized, "normalized_open": normalized_open, "actions": actions,
        "events": json.loads(Path(bound["dividend_events"]["path"]).read_text())["events"],
        "rights_events": json.loads(Path(bound["rights_events"]["path"]).read_text())["events"],
        "identity_events": json.loads(Path(bound["identity_events"]["path"]).read_text())["events"],
        "tax_contracts": json.loads(Path(bound["distribution_tax_contracts"]["path"]).read_text())["contracts"],
        "raw_day_locations": raw.groupby("date", sort=False).indices,
        "scores": {"MOM60": momentum_scores, "LGB": execution_year_scores(pd.read_csv(pred_dir / "predictions.csv"), calendar)},
        "market_audit": audit, "bound": bound}


def simulate_stability(context, *, period, model):
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
        state, ledger = a2._apply_events(state, day, events, context["actions"], engine_calendar, quotes, policy, days[0])
        records["corporate_actions"].extend(ledger)
        if number % config["rebalance_sessions"] == 0:
            ranked, members, prices = _rank(context, model, signal_day)
            records["score_coverage"].append(context["score_coverage"][(model, signal_day)])
            if not ranked:
                raise ValueError("No eligible frozen prior-close scores")
            plan = adapter.build_rank_buffer_plan(state, signal_date=signal_day, ranked_instruments=ranked,
                raw_signal_prices=prices, boards={inst: a2.board(inst) for inst in ranked}, signal_equity=previous,
                policy=adapter.RankBufferPolicy(top_k=config["top_k"], keep_rank=config["keep_rank"],
                    require_signal_affordability=config["require_signal_affordability"]), execution_policy=policy)
            result = adapter.execute_rank_buffer_plan(state, plan=plan, session=day, quotes=quotes,
                metadata=metadata, calendar=engine_calendar, policy=policy, verified_suspended=suspended)
            state = result.state
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
        previous = equity
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
    valid = metrics["accounting_invalid_days"] == 0 and terminal_fresh and stopped is None
    return {"period": period, "model": model, "execution_id": "E2", "performance_valid": valid,
        "metrics_basis": "declared_accounting_NAV_not_actual_liquidation" if valid else "invalid_indicative_carried_ledger_do_not_use_for_selection",
        "metrics": metrics, "records": records, "final_state": state.to_dict(),
        "stopped": stopped, "actual_last_valued_session": records["curves"][-1]["date"] if records["curves"] else None}


def run_stability_validation(snapshot_path, scope_directory, protocol_path, market_audit_path,
                         predictions_directory, merger_actions_path, output_dir, *, progress=None):
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Preserve prior union validation results")
    context = load_stability_context(snapshot_path, scope_directory, protocol_path, market_audit_path,
                                 predictions_directory, merger_actions_path)
    results = []
    with context["pool"].verified_batch():
        for period in context["protocol"]["periods"]:
            for model in MODELS:
                if progress:
                    progress({"phase": "costed_comparison", "period": period, "model": model})
                result = simulate_stability(context, period=period, model=model)
                result["score_quality"] = score_quality(context, model=model, period=period)
                results.append(result)
    _assert_proofs(context["bound"])
    context["pool"].assert_unchanged()
    context["actions"].assert_unchanged()
    output.mkdir(parents=True)
    summaries = []
    for result in results:
        directory = output / result["period"] / result["model"]
        directory.mkdir(parents=True)
        for name, rows in result["records"].items():
            if name in {"plans", "valuation_audit", "score_coverage"}:
                _save(directory / (name + ".json"), rows)
            else:
                pd.DataFrame(rows).to_csv(directory / (name + ".csv"), index=False)
        _save(directory / "final_state.json", result["final_state"])
        _save(directory / "score_quality.json", result["score_quality"])
        summary = {key: value for key, value in result.items() if key not in {"records", "final_state"}}
        _save(directory / "summary.json", summary)
        summaries.append(summary)
    bound = dict(context["bound"])
    for name, proof in list(bound.items()):
        if name in implementation_proofs():
            archived = output / "source" / Path(proof["path"]).name
            archived.parent.mkdir(exist_ok=True)
            shutil.copyfile(proof["path"], archived)
            bound[name] = {**proof, "path": str(archived), "original_path": proof["path"]}
    report = {"schema_version": 1, "kind": "a5_union_stability_costed_comparison",
        "completed_at": datetime.now(timezone.utc).isoformat(), "results": summaries,
        "all_performance_valid": all(row["performance_valid"] for row in summaries),
        "models": list(MODELS), "execution": EXECUTION, "validity": VALIDITY,
        "source_coverage": {key: context["market_audit"][key] for key in
            ("start", "end", "instrument_count", "raw_rows", "event_count")},
        "tax_classification_unresolved": context["market_audit"].get("tax_classification_unresolved", []),
        "all_corporate_tax_source_classifications_verified": context["market_audit"].get(
            "all_corporate_tax_source_classifications_verified", False),
        "bound_inputs": bound,
        "limitations": ["Retrospective historical reconstruction; no new future out-of-sample claim.",
            "Daily observed bars and adverse costs do not establish queue priority or actual broker fillability.",
            "Research assumes existing board access; initial research cash does not establish real account permissions.",
            "Distribution tax uses a conservative maximum reserve without holding-period refunds; explicitly unresolved capital sources remain separately disclosed.",
            "Annualized partial-year returns are not realized full-year returns.",
            "No strategy selection, scheduler activation or broker orders are performed."]}
    _save(output / "report.json", report)
    _save(output / "manifest.json", {"kind": report["kind"], "bound_inputs": bound,
        "files": {str(path.relative_to(output)): sha(path) for path in sorted(output.rglob("*")) if path.is_file()}})
    return report
