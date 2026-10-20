"""Evidence-gated fixed momentum/LightGBM comparison on dated index members.

This is a new research generation. It calls the separately reviewed 2023--2024
and modern raw-share ledgers without modifying either ledger or the live paper
account. A profitable path with an unpriced delisted claim is still invalid.
"""
from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from stopat30m import ashare_execution_experiment as a2
from stopat30m.ashare_experiment import VerifiedCorporateActions
from stopat30m.ashare_optimization import relevant_events
from stopat30m.ashare_union_research import (
    LABEL, _load_protocol, assert_market_originals_unchanged,
    implementation_sha256 as training_implementation_sha256, validate_market_audit,
)
from stopat30m.ashare_validation import (
    audit_source_proofs, conservative_path_metrics, daily_inputs,
    record_unsubscribed_rights, valuation_validity,
)
from stopat30m.data.ashare_scope import load_verified_historical_scope
from stopat30m.data.qlib_incremental import read_qlib_security
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot
from stopat30m.model.trainer import FixedQlibLGBSpec


MODELS = ("MOM60", "LGB")
EXECUTION = {"initial_cash_cny": "100000.00", "top_k": 20, "keep_rank": 40,
    "rebalance_sessions": 5, "require_signal_affordability": True,
    "commission_rate": "0.0003", "minimum_commission_cny": "5.00",
    "transfer_rate": "0.00001", "adverse_slippage_bps": "2",
    "cash_dividend_tax_reserve_rate": "0.20"}
VALIDITY = {"fresh_future_oos_claim": False, "historical_model_freeze_claim": False,
    "selection_performed": False, "active_paper_changed": False,
    "source_verification_required_before_fit_and_returns": True,
    "signal_membership_only_no_future_member_survival_filter": True,
    "held_out_of_index_assets_remain_in_account": True,
    "unpriced_delisted_claim_invalidates_performance": True,
    "source_confirmed_suspension_carry_is_not_fresh_liquidation_value": True,
    "unknown_entitlements_or_uncovered_tax_stop_before_returns": True,
    "rights_issues": "fixed_no_subscription_no_fictitious_shares_or_cash",
    "training_history": "Qlib_base_with_identity_and_feature_checks_not_independently_recollected_every_bar",
    "retraining": "annual_previous_five_calendar_years_six_session_label_purge",
    "momentum_lookback_actual_sessions": 60}
VALIDITY.update(momentum_formula="adjusted_close_t_divided_by_adjusted_close_t_minus_60_minus_one",
    momentum_nonpositive_scores="included_in_relative_rank_no_absolute_momentum_filter",
    lgb_score_coverage="every_observed_complete_signal_member_required_no_silent_missing_scores",
    partial_session_missing_score="explicit_disclosed_exception_when_original_quote_is_not_complete")
VALIDITY["account_market_access"] = "research_assumes_existing_main_board_chinext_and_star_access_not_inferred_from_initial_cash"
VALIDITY["score_quality"] = "daily_cross_section_IC_and_rank_IC_on_mature_observed_open_t_plus_1_to_t_plus_6_labels_minimum_20_names"
VALIDITY["delisting_trading_period"] = "explicit_prior_known_period_metadata_blocks_unsupported_orders_retains_physical_shares_and_observed_close_valuation"
VALIDITY["capital_tax_accounting"] = {
    "known_share_premium_exemption": "individual_issuer_source_evidence_required",
    "other_capital_or_unresolved_source": "reserve_20_percent_of_evidenced_full_nominal_face_plus_cash_and_stock_dividend_face",
    "unresolved_classifications_remain_disclosed": True,
    "holding_period_refund": False,
    "insufficient_cash_to_cover_tax": "block_before_training_and_returns"}


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n")


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha(path)}


def implementation_proofs():
    from stopat30m.backtest import ashare_execution, ashare_historical_execution
    from stopat30m import ashare_optimization_execution, ashare_historical_optimization_execution
    from stopat30m.data import a5_capital_tax
    return {name: _proof(path) for name, path in {
        "union_validation": __file__, "modern_engine": ashare_execution.__file__,
        "historical_engine": ashare_historical_execution.__file__,
        "modern_rank_adapter": ashare_optimization_execution.__file__,
        "historical_rank_adapter": ashare_historical_optimization_execution.__file__,
        "corporate_actions": a2.__file__, "rights_and_validity": daily_inputs.__code__.co_filename,
        "event_selection": relevant_events.__code__.co_filename,
        "capital_tax_adapter": a5_capital_tax.__file__,
        "capital_tax_spec": a5_capital_tax.SPEC_PATH}.items()}


def freeze_union_protocol(path, snapshot_path, scope_directory, *, evaluation_end,
        execution_years=(2023, 2024), historical_rules_path=None, modern_rules_path=None):
    """Declare parameters, chronology and exact source bytes before fitting/PnL."""
    path = guard_qlib_output_path(path)
    if path.exists():
        raise ValueError("Preserve the prior union research declaration")
    years = list(execution_years)
    if years not in ([2023, 2024], [2023, 2024, 2025, 2026]):
        raise ValueError("Only the declared two-year or four-year historical comparisons are supported")
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
    payload = {"schema_version": 1, "kind": "a5_union_fixed_research_protocol",
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


def _engine(day):
    if not "2023-01-01" <= day <= "2026-12-31":
        raise ValueError("Union comparison is restricted to the declared 2023--2026 rule window")
    if day < "2025-01-01":
        from stopat30m.backtest import ashare_historical_execution as ex
        from stopat30m import ashare_historical_optimization_execution as adapter
    else:
        from stopat30m.backtest import ashare_execution as ex
        from stopat30m import ashare_optimization_execution as adapter
    return ex, adapter


def session_policy(day, config=EXECUTION):
    ex, _ = _engine(day)
    policy = ex.ExecutionPolicy(initial_cash=config["initial_cash_cny"],
        commission_rate=config["commission_rate"], minimum_commission=config["minimum_commission_cny"],
        transfer_rate=config["transfer_rate"], slippage_bps=config["adverse_slippage_bps"],
        dividend_tax_rate=config["cash_dividend_tax_reserve_rate"])
    return ex.historical_policy(day, policy) if day < "2025-01-01" else policy


def execution_year_scores(predictions, calendar):
    """December's last close uses the model for its next actual execution year."""
    required = {"model", "execution_year", "datetime", "instrument", "score"}
    if not required.issubset(predictions) or set(predictions.model) != {"LGB"}:
        raise ValueError("Union predictions require a declared LGB annual-fold package")
    frame = predictions.copy()
    if frame.duplicated(["model", "execution_year", "datetime", "instrument"]).any():
        raise ValueError("Duplicate score in an execution-year model fold")
    next_year = {day: int(calendar[number+1][:4]) for number, day in enumerate(calendar[:-1])}
    dates = pd.to_datetime(frame.datetime).dt.strftime("%Y-%m-%d")
    frame = frame.loc[dates.map(next_year).eq(pd.to_numeric(frame.execution_year))].copy()
    frame["datetime"] = pd.to_datetime(frame.datetime)
    if frame.empty or frame.duplicated(["datetime", "instrument"]).any():
        raise ValueError("Annual-fold routing did not produce unique prior-close scores")
    values = pd.to_numeric(frame.score, errors="raise")
    if not np.isfinite(values).all():
        raise ValueError("Nonfinite frozen model predictions")
    return frame.assign(score=values).set_index(["datetime", "instrument"]).score.sort_index()


def _read_daily(path):
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    for name in ("source_complete_quote", "source_observed_market_quote"):
        if name not in frame or not frame[name].isin(["True", "False"]).all():
            raise ValueError("Audited daily quote flags must be explicit Boolean observations")
        frame[name] = frame[name].map({"True": True, "False": False})
    if frame.duplicated(["instrument", "date"]).any():
        raise ValueError("Duplicate canonical original market row")
    # Canonical A5 prices retain each original response SHA rather than a
    # redundant provider label. The reviewed ledger also needs that label;
    # derive it in memory without changing the audited CSV or any quote field.
    if "source" not in frame:
        if ("source_response_sha256" not in frame
                or not frame.source_response_sha256.str.fullmatch(r"[0-9a-f]{64}").all()):
            raise ValueError("Canonical source labels require bound original response SHA256")
        frame["source"] = "baostock.original_daily_execution"
        partial = frame.source_observed_market_quote & ~frame.source_complete_quote
        frame.loc[partial, "source"] += "+independently_verified_partial_session_observation"
    return frame


def _assert_proofs(proofs):
    for proof in proofs.values():
        path = Path(proof["path"])
        if not path.is_absolute() or sha(path) != proof["sha256"]:
            raise ValueError("Bound union research source bytes changed")


def load_union_context(snapshot_path, scope_directory, protocol_path, market_audit_path,
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
    needed = {"daily_csv", "stock_basic", "dividend_events", "rights_events", "identity_events"}
    if not needed.issubset(bound):
        raise ValueError("Market audit lacks its canonical consumed execution inputs")
    raw, basic = _read_daily(bound["daily_csv"]["path"]), pd.read_csv(basic_path, dtype=str, keep_default_na=False)
    pred_dir = Path(predictions_directory).resolve()
    manifest = json.loads((pred_dir / "manifest.json").read_text())
    if (manifest.get("kind") != "a5_union_fixed_predictions"
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
        original = read_qlib_security(source.dataset_dir, inst)
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
        "raw_day_locations": raw.groupby("date", sort=False).indices,
        "scores": {"MOM60": momentum_scores, "LGB": execution_year_scores(pd.read_csv(pred_dir / "predictions.csv"), calendar)},
        "market_audit": audit, "bound": bound}


def _known_identity(event, day, *, close=False, fact="known_at"):
    known = pd.Timestamp(event.get(fact, event["known_at"]))
    cutoff = pd.Timestamp(day).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15 if close else 9, minutes=0 if close else 30)
    if known.tzinfo is None:
        raise ValueError("Identity facts need an explicit timezone-aware publication cutoff")
    return known <= cutoff


def identity_execution_inputs(events, day, metadata, suspended):
    """Apply known finite halts and flag unsupported delisting-period orders.

    The reviewed ledgers refuse trading in delisting periods. Keep that rule
    explicit instead of accidentally sending ordinary-board metadata; actual
    observed closes still value physical shares until the terminal session.
    """
    metadata, suspended = dict(metadata), set(suspended)
    for event in events:
        if event.get("kind") != "delisting":
            continue
        inst, halt = event["instrument"], event.get("suspension_start")
        resumed = event.get("resumption_before_delisting") or event.get("suspension_end")
        if (halt and halt <= day and _known_identity(event, day, fact="suspension_known_at")
                and not (resumed and resumed <= day and _known_identity(event, day, fact="resumption_known_at"))):
            suspended.add(inst)
        period = event.get("delisting_period_start")
        last = event.get("last_exchange_trading_session")
        if (period and last and period <= day <= last
                and _known_identity(event, day, fact="delisting_period_known_at") and inst in metadata):
            metadata[inst] = replace(metadata[inst], delisting_period=True)
    return metadata, sorted(suspended)


def _rank(context, model, signal_day):
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
    if model == "LGB" and absent:
        raise ValueError("Missing required frozen LGB scores for complete prior-close members: " + ",".join(absent))
    if model == "MOM60":
        # The declared 60-session endpoint must actually be observed. A missing
        # finite momentum score has to match that reason, not a damaged package.
        prices = context.get("normalized")
        location = context["calendar"].index(signal_day)
        for inst in absent:
            missing_endpoint = prices is not None and (location < 60 or inst not in prices
                or not np.isfinite(prices.at[signal_day, inst]) or prices.at[signal_day, inst] <= 0
                or not np.isfinite(prices.at[context["calendar"][location-60], inst])
                or prices.at[context["calendar"][location-60], inst] <= 0)
            if not missing_endpoint:
                raise ValueError("Missing momentum score despite observed declared endpoints")
            exceptions.append({"instrument": inst, "reason": "unobserved_declared_60_session_endpoint"})
    exceptions.extend({"instrument": inst, "reason": "observed_partial_session_not_original_complete_model_quote"}
        for inst in members if inst in observed.index and not bool(observed.loc[inst, "source_complete_quote"])
        and (inst not in scores or not np.isfinite(scores.loc[inst])))
    coverage = {"signal_date": signal_day, "model": model, "members": len(members),
        "required_complete_quote_members": len(required), "ranked_count": len(eligible),
        "missing_required_scores": absent, "explicit_exceptions": exceptions}
    context.setdefault("score_coverage", {})[(model, signal_day)] = coverage
    return sorted(eligible, key=lambda inst: (-float(scores.loc[inst]), inst)), members, observed.close.to_dict()


def _source_day(context, day):
    """Index original rows once; never fill an absent stock/session."""
    if "raw_day_locations" not in context:
        context["raw_day_locations"] = context["raw"].groupby("date", sort=False).indices
    locations = context["raw_day_locations"].get(day, np.array([], dtype=np.intp))
    return context["raw"].iloc[locations]


def path_stability(curves, trades, initial_cash):
    """Include the cash start in drawdown; separate accounting attribution."""
    result = conservative_path_metrics(curves, initial_cash)
    values = np.asarray([float(row["equity_cny"]) for row in curves])
    dates = pd.to_datetime([row["date"] for row in curves])
    prior = np.r_[float(initial_cash), values[:-1]]
    returns = pd.Series(values / prior - 1, index=dates)
    months = (1 + returns).groupby(returns.index.to_period("M")).prod().sub(1)
    years = (1 + returns).groupby(returns.index.year).prod().sub(1)
    result.update(session_count=len(curves), final_equity_cny=curves[-1]["equity_cny"],
        annualized_return=float((values[-1] / float(initial_cash)) ** (252 / len(values)) - 1),
        annualized_volatility=float(returns.std(ddof=0) * np.sqrt(252)),
        positive_month_fraction=float((months > 0).mean()),
        worst_month_return=float(months.min()), best_month_return=float(months.max()),
        monthly_returns={str(key): float(value) for key, value in months.items()},
        calendar_year_returns={str(key): float(value) for key, value in years.items()},
        incomplete_valuation_days=sum(not row["valuation_complete"] for row in curves),
        accounting_invalid_days=sum(not row["accounting_valid"] for row in curves),
        mean_invested_fraction=float(np.mean([1 - float(row["cash_cny"]) / float(row["equity_cny"]) for row in curves])),
        trade_count=len(trades))
    for length in (63, 126, 252):
        if len(values) >= length:
            starts = np.r_[float(initial_cash), values][:-length]
            rolling = values[length-1:] / starts - 1
            result[f"rolling_{length}_sessions"] = {"observations": len(rolling),
                "minimum": float(rolling.min()), "median": float(np.median(rolling)),
                "positive_fraction": float((rolling > 0).mean())}
    for name in ("commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost", "notional"):
        result[name + "_cny"] = str(sum((Decimal(row[name]) for row in trades), Decimal(0)))
    fee = Decimal(result["total_fee_cny"]) + Decimal(result["slippage_cost_cny"])
    result["fees_plus_slippage_as_initial_cash_fraction"] = float(fee / Decimal(initial_cash))
    result["one_way_turnover_average_nav"] = float(Decimal(result["notional_cny"]) / Decimal(str(values.mean())) / 2)
    result["turnover_definition"] = "sum_buy_and_sell_actual_notional_divided_by_two_and_mean_daily_NAV"
    result["cost_attribution_is_counterfactual_rerun"] = False
    return result


def score_quality(context, *, model, period):
    """Diagnose predictive ranks separately from fees and market exposure.

    This does not change inference, orders or training rows. An immature label
    is excluded from this diagnostic only; future membership is never tested.
    """
    start, end = context["protocol"]["periods"][period]
    calendar, opening = context["calendar"], context["normalized_open"]
    last = max(number for number, day in enumerate(calendar) if day <= end)
    days = [(number-1, calendar[number-1]) for number, day in enumerate(calendar)
        if number > 0 and start <= day <= end]
    rows, excluded = [], {"immature_signal_days": 0, "missing_label_pairs": 0, "insufficient_cross_section_days": 0,
                           "constant_score_or_label_days": 0}
    for location, signal_day in days:
        if location+6 > last:
            excluded["immature_signal_days"] += 1
            continue
        try:
            scores = context["scores"][model].xs(pd.Timestamp(signal_day), level="datetime")
        except KeyError:
            raise ValueError("Quality diagnostic lacks a declared prior-close score date") from None
        names = [inst for inst in context["pool"].members_on(signal_day)
            if inst in scores and np.isfinite(scores.loc[inst]) and inst in opening]
        left, right = opening.loc[calendar[location+1], names], opening.loc[calendar[location+6], names]
        observed = np.isfinite(left) & left.gt(0) & np.isfinite(right) & right.gt(0)
        excluded["missing_label_pairs"] += int((~observed).sum())
        names = list(observed.index[observed])
        if len(names) < 20:
            excluded["insufficient_cross_section_days"] += 1
            continue
        labels = right.loc[names] / left.loc[names] - 1
        predicted = scores.loc[names].astype(float)
        if labels.nunique() < 2 or predicted.nunique() < 2:
            excluded["constant_score_or_label_days"] += 1
            continue
        ic, rank_ic = predicted.corr(labels), predicted.rank().corr(labels.rank())
        if not np.isfinite(ic) or not np.isfinite(rank_ic):
            raise ValueError("Nonfinite cross-sectional quality diagnostic")
        rows.append({"signal_date": signal_day, "label_end": calendar[location+6], "pairs": len(names),
                     "IC": float(ic), "rank_IC": float(rank_ic)})
    summary = {"label": LABEL, "minimum_cross_section": 20, "daily_observations": len(rows),
        "excluded": excluded, "future_membership_filter_used": False,
        "quality_diagnostic_does_not_filter_actual_orders": True}
    for field in ("IC", "rank_IC"):
        values = np.asarray([row[field] for row in rows])
        summary[field+"_mean"] = float(values.mean()) if len(values) else None
        summary[field+"_std"] = float(values.std(ddof=0)) if len(values) else None
        summary[field+"_positive_fraction"] = float((values > 0).mean()) if len(values) else None
    return {"summary": summary, "daily": rows}


def simulate_union(context, *, period, model):
    if model not in MODELS or period not in context["protocol"]["periods"]:
        raise ValueError("Undeclared fixed union model or period")
    start, end = context["protocol"]["periods"][period]
    calendar, config = context["calendar"], context["protocol"]["execution"]
    days = [day for day in calendar if start <= day <= end]
    if not days or calendar.index(days[0]) < 1:
        raise ValueError("Actual comparison sessions and preceding signal are required")
    engine_calendar = calendar[max(0, calendar.index(days[0])-80):calendar.index(days[-1])+1]
    initial = Decimal(config["initial_cash_cny"])
    ex, _ = _engine(days[0])
    state, previous = ex.PortfolioState.initial(session_policy(days[0], config)), initial
    symbols = sorted(set(context["raw"].instrument) | {row["to_instrument"] for row in context["actions"].actions})
    records = {name: [] for name in ("curves", "trades", "orders", "positions", "claims",
        "signals", "corporate_actions", "plans", "valuation_audit", "score_coverage")}
    unpriced_rights, terminal_fresh = set(), False
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
    valid = metrics["accounting_invalid_days"] == 0 and terminal_fresh
    return {"period": period, "model": model, "execution_id": "E2", "performance_valid": valid,
        "metrics_basis": "declared_accounting_NAV_not_actual_liquidation" if valid else "invalid_indicative_carried_ledger_do_not_use_for_selection",
        "metrics": metrics, "records": records, "final_state": state.to_dict()}


def run_union_validation(snapshot_path, scope_directory, protocol_path, market_audit_path,
                         predictions_directory, merger_actions_path, output_dir, *, progress=None):
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Preserve prior union validation results")
    context = load_union_context(snapshot_path, scope_directory, protocol_path, market_audit_path,
                                 predictions_directory, merger_actions_path)
    results = []
    with context["pool"].verified_batch():
        for period in context["protocol"]["periods"]:
            for model in MODELS:
                if progress:
                    progress({"phase": "costed_comparison", "period": period, "model": model})
                result = simulate_union(context, period=period, model=model)
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
    report = {"schema_version": 1, "kind": "a5_union_costed_comparison",
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
