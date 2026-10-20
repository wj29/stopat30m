"""Evidence-gated 2023 development / 2024 fee-inclusive A-share validation.

This module may recommend a declared candidate. It never changes a forward
period or a scheduler. Missing raw status, rights or historical rules stop the
experiment before performance is calculated; carried marks remain incomplete.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import shutil

import numpy as np
import pandas as pd

from stopat30m import ashare_execution_experiment as a2
from stopat30m.ashare_experiment import PackageHistoricalPool, VerifiedCorporateActions
from stopat30m.ashare_optimization import relevant_events, rolling_scores_for_calendar
from stopat30m.data.ashare_execution_data import load_execution_collection
from stopat30m.data.qlib_incremental import read_qlib_security
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot
from stopat30m.model.evaluator import compute_portfolio_metrics


SPEC_SHA256 = "fd9fe63fa9b963f108b96e8955da1675a582c0715f2323f364f793a9497b3b50"
MODELS = ("M0", "M1")
EXECUTIONS = ("E0", "E1", "E2")
PERIODS = {"development": ("2023-01-01", "2023-12-31"),
           "validation": ("2024-01-01", "2024-12-31")}
AUDIT_CHECKS = ("raw_ohlc", "raw_preclose", "daily_trading_status", "historical_is_st",
                "ipo_identity", "calendar_coverage", "dividend_events",
                "ex_reference_explained", "source_vintage")
VALIDITY_POLICY = {
    "schema_version": 1,
    "kind": "a4_validation_declaration",
    "a3_spec_sha256": SPEC_SHA256,
    "periods": {key: list(value) for key, value in PERIODS.items()},
    "independent_cash_start_per_candidate_per_year_cny": "100000.00",
    "candidate_count_per_period": 6,
    "rebalance_count_starts_at_each_period_first_actual_session": True,
    "prediction_fold": "next_actual_execution_session_year",
    "trading_decisions": "prior_close_only_no_future_availability_substitution",
    "prior_signal_planning_prices_require_complete_original_quote": True,
    "prior_signal_price_observation_exception": "evidence_bound_partial_day_positive_volume_OHLC_only",
    "partial_day_observation_does_not_change_original_tradestatus_or_opening_permission": True,
    "partial_day_actual_close_may_value_holdings_and_next_signal_planning": True,
    "costs": "original_a3_actual_assumptions_with_session_effective_historical_stamp",
    "stock_dividend_tax_requires_evidenced_effective_cash_reserve_coverage": True,
    "capital_reserve_tax_exemption_requires_explicit_issuer_evidence": True,
    "unknown_tax_status": "stop_before_performance",
    "wealth_bounds_are_under_declared_accounting_and_tax_reserve_policy": True,
    "missing_or_unknown_source_evidence": "stop_before_performance",
    "carried_suspension_marks_remain_valuation_incomplete": True,
    "unresolved_fractional_rights_remain_valuation_incomplete": True,
    "selection_requires_all_12_declared_runs_and_complete_global_source_audits": True,
    "baseline_requires_both_periods_accounting_valid_and_terminal_fresh": True,
    "each_candidate_requires_its_own_both_periods_accounting_valid_and_terminal_fresh": True,
    "ineligible_nonbaseline_candidate_does_not_block_other_valid_candidates": True,
    "verified_suspension_carry_is_disclosed_accounting_nav_not_liquidation_value": True,
    "terminal_price_bearing_holdings_and_claims_require_actual_fresh_close": True,
    "known_terms_and_auditable_bounded_fractional_rights_are_allowed": True,
    "unknown_or_unbounded_rights_block_selection": True,
    "rights_issues": "fixed_no_subscription_no_new_shares_no_cash_payment",
    "nontransferable_unsubscribed_rights_have_no_tradable_asset_value": True,
    "rights_issue_exchange_preclose_gap_requires_verified_issuer_terms": True,
    "unknown_or_transferable_rights_issue_mechanism": "stop_before_performance",
    "held_rights_issue_without_fresh_ex_price": "accounting_invalid_until_actual_fresh_close_no_synthetic_carry_adjustment",
    "selection_compares_candidate_lower_return_to_baseline_upper_return": True,
    "selection_drawdown_basis": "current_lower_nav_over_running_upper_nav_peak",
    "selection_period": "validation_2024_only",
    "selection_requires_positive_total_return": True,
    "selection_maximum_absolute_drawdown": "0.20",
    "selection_requires_strictly_beating_same_cost_same_period_M0_E0": True,
    "selection_ties": ["net_total_return_desc", "absolute_max_drawdown_asc",
                       "traded_notional_asc", "candidate_id_asc"],
    "selection_uses_exact_decimal_equity_not_rounded_display_metrics": True,
    "historical_freeze_claim": False,
    "daily_status_known_at_semantics": "assumed_daily_effective_cutoff_00_00_not_observed_publication_timestamp",
    "historical_status_publication_timestamp_known": False,
    "historical_status_is_announcement_time_PIT": False,
    "paper_or_scheduler_mutation": False,
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
        sort_keys=True, allow_nan=False) + "\n")


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha(path)}


def _bound_path(proof, base):
    if not isinstance(proof, dict) or not re.fullmatch(r"[0-9a-f]{64}", str(proof.get("sha256", ""))):
        raise ValueError("An input requires its original SHA-256 receipt")
    path = Path(proof["path"])
    if not path.is_absolute():
        path = base / path
    path = path.resolve()
    if sha(path) != proof["sha256"]:
        raise ValueError("Bound validation input checksum differs")
    return path


def audit_source_proofs(tree, base, *, prefix="market_audit_source"):
    """Verify every nested original-byte receipt, including issuer attachments."""
    found = {}
    def visit(node, parts):
        if isinstance(node, dict):
            if "path" in node:
                path = _bound_path(node, base)
                key = prefix + ":" + ":".join(parts)
                found[key] = {"path": str(path), "sha256": node["sha256"]}
            for key, value in node.items():
                if isinstance(value, (dict, list)):
                    visit(value, [*parts, str(key)])
        elif isinstance(node, list):
            for index, value in enumerate(node):
                visit(value, [*parts, str(index)])
    visit(tree, [])
    return found


def write_validation_declaration(path, *, spec_path, now=None):
    """Freeze all validity choices and implementations before calculating returns."""
    from stopat30m.backtest import ashare_historical_execution as historical
    from stopat30m import ashare_historical_optimization_execution as execution
    from stopat30m.data.ashare_history_audit import apply_intraday_observation_evidence
    path = guard_qlib_output_path(path)
    if path.exists():
        raise ValueError("Preserve the previously frozen validation declaration")
    if sha(spec_path) != SPEC_SHA256:
        raise ValueError("A3 candidate declaration changed")
    stamp = pd.Timestamp(now or datetime.now(timezone.utc))
    if stamp.tzinfo is None:
        raise ValueError("Validation declaration clock must include timezone")
    declaration = {**VALIDITY_POLICY, "declared_at": stamp.isoformat(),
        "bound_implementations": {"validation": _proof(__file__),
            "historical_engine": _proof(historical.__file__),
            "historical_execution_adapter": _proof(execution.__file__),
            "ranking_and_rights_helpers": _proof(relevant_events.__code__.co_filename),
            "execution_and_corporate_action_helpers": _proof(a2.__file__),
            "execution_collection_loader": _proof(load_execution_collection.__code__.co_filename),
            "intraday_observation_implementation": _proof(apply_intraday_observation_evidence.__code__.co_filename),
            "metric_display": _proof(compute_portfolio_metrics.__code__.co_filename)},
        "spec": _proof(spec_path)}
    path.parent.mkdir(parents=True, exist_ok=True)
    save(path, declaration)
    return declaration


def _validate_declaration(path):
    declaration = json.loads(Path(path).read_text())
    if any(declaration.get(key) != value for key, value in VALIDITY_POLICY.items()):
        raise ValueError("Validation validity/selection policy differs from its frozen declaration")
    if pd.Timestamp(declaration["declared_at"]).tzinfo is None:
        raise ValueError("Frozen validation declaration lacks a real timezone-aware clock")
    for proof in [declaration["spec"], *declaration["bound_implementations"].values()]:
        _bound_path(proof, Path(path).resolve().parent)
    return declaration


def validate_market_audit(audit, inputs):
    """Require actual independent audits bound to the exact consumed source bytes."""
    if (audit.get("kind") != "a4_market_evidence_audit" or audit.get("status") != "passed"
            or any(audit.get("checks", {}).get(check) is not True for check in AUDIT_CHECKS)):
        raise ValueError("Raw prices/status/dividends/historical reference audits are not complete")
    required = {key: proof["sha256"] for key, proof in inputs.items()
        if key in {"execution_collection", "daily_csv", "basic_csv", "dividend_events", "rights_events", "intraday_observations", "calendar", "historical_intervals"}}
    if (not required or any(audit.get("audited_input_sha256", {}).get(key) != digest
                            for key, digest in required.items())):
        raise ValueError("Market audit is not bound to the actual consumed source bytes")


def validate_daily_rows(raw, basic, calendar, pool, *, start, end,
                        intraday_observations=None, observation_evidence_base=None):
    """Independently check source status and coverage without manufacturing bars."""
    required = {"date", "instrument", "open", "high", "low", "close", "preclose", "volume", "amount",
                "tradestatus", "isST", "source_response_sha256"}
    if not required.issubset(raw) or not {"code", "ipoDate", "outDate"}.issubset(basic):
        raise ValueError("Original execution status/identity fields are absent")
    raw = raw.copy()
    if (raw.duplicated(["date", "instrument"]).any() or basic.code.duplicated().any()
            or not raw.tradestatus.isin(["0", "1"]).all() or not raw.isST.isin(["0", "1"]).all()
            or not raw.source_response_sha256.str.fullmatch(r"[0-9a-f]{64}").all()):
        raise ValueError("Duplicate or unknown original execution status/provenance")
    dates = pd.to_datetime(raw.date, errors="raise")
    if not dates.dt.strftime("%Y-%m-%d").eq(raw.date).all() or not raw.date.isin(calendar).all():
        raise ValueError("Raw execution rows are not actual source exchange sessions")
    symbols = sorted(pool.intervals)
    if set(raw.instrument) != set(symbols):
        raise ValueError("Execution stock coverage differs from historical overlapping members")
    numbers = raw[["open", "high", "low", "close", "preclose", "volume", "amount"]].replace("", np.nan).apply(pd.to_numeric, errors="raise")
    if np.isinf(numbers.to_numpy()).any() or numbers.lt(0).any().any():
        raise ValueError("Invalid original raw execution numeric values")
    complete = raw.tradestatus.eq("1") & numbers.notna().all(axis=1) & numbers.gt(0).all(axis=1)
    if (raw.tradestatus.eq("1") & ~complete).any():
        raise ValueError("A trading session lacks a complete observed raw quote")
    observed = complete.copy()
    observations = intraday_observations or {"events": []}
    identities = set()
    for event in observations["events"]:
        identity = (event["instrument"], event["date"])
        if (identity in identities or event.get("execution_remains_blocked") is not True or len(event.get("evidence", [])) < 2
                or observation_evidence_base is None):
            raise ValueError("Partial-day price observation needs unique original source-byte evidence")
        identities.add(identity)
        for proof in event["evidence"]:
            if not Path(proof["path"]).is_absolute():
                raise ValueError("Partial-day price evidence paths must be absolute")
            _bound_path(proof, Path(observation_evidence_base))
        rows = raw.index[raw.instrument.eq(identity[0]) & raw.date.eq(identity[1])]
        if (len(rows) != 1 or raw.loc[rows[0], "tradestatus"] != "0"
                or raw.loc[rows[0], "source_response_sha256"] != event["source_response_sha256"]
                or not numbers.loc[rows[0]].notna().all() or not numbers.loc[rows[0]].gt(0).all()):
            raise ValueError("Partial-day observation differs from the bound positive-volume original OHLC response")
        if (Decimal(str(raw.loc[rows[0], "open"])) != Decimal(str(event["source_observed_open"]))
                or Decimal(str(raw.loc[rows[0], "close"])) != Decimal(str(event["source_observed_close"]))):
            raise ValueError("Independent partial-day observed prices differ from the original source")
        observed.loc[rows[0]] = True
    if "source_observed_market_quote" in raw:
        if not pd.api.types.is_bool_dtype(raw.source_observed_market_quote) or not raw.source_observed_market_quote.equals(observed):
            raise ValueError("Observed-market flag is not justified by the bound partial-day source evidence")
    elif observations["events"]:
        raise ValueError("Verified partial-day source observations have not been annotated")
    quoted = numbers.loc[observed]
    if (quoted.high.lt(quoted[["open", "low", "close"]].max(axis=1)).any()
            or quoted.low.gt(quoted[["open", "high", "close"]].min(axis=1)).any()
            or not np.isclose(quoted.volume, np.rint(quoted.volume), rtol=0, atol=1e-6).all()
            or ((quoted.amount / quoted.volume < quoted.low - 1e-4)
                | (quoted.amount / quoted.volume > quoted.high + 1e-4)).any()):
        raise ValueError("Original raw OHLC/amount/share-volume units are inconsistent")
    identities = basic.set_index("code")
    for instrument in symbols:
        code = instrument[:2].lower() + "." + instrument[2:]
        if code not in identities.index:
            raise ValueError("IPO identity evidence omits a historical execution stock")
        identity = identities.loc[code]
        listed = str(identity.ipoDate)
        pd.Timestamp(listed)
        out = str(identity.outDate).strip()
        expected = {day for day in calendar if start <= day <= end and listed <= day and (not out or day < out)}
        actual = set(raw.loc[raw.instrument.eq(instrument) & raw.date.between(start, end), "date"])
        if expected != actual:
            raise ValueError("Raw execution coverage omits actual historical sessions; missing bars are not verified halts")
    raw["source_complete_quote"] = complete
    raw["source_observed_market_quote"] = observed
    if "source" not in raw:
        raw["source"] = "checksum_bound_original_daily_execution"
    return raw


def _load_predictions(root, spec, calendar):
    root = Path(root)
    manifest = json.loads((root/"manifest.json").read_text())
    reproduction = manifest.get("baseline_reproduction", {})
    if (manifest.get("declaration_sha256") != SPEC_SHA256
            or manifest.get("source_manifest_sha256") != spec["source_manifest_sha256"]
            or manifest.get("hyperparameter_search") is not False or manifest.get("early_stopping") is not False
            or reproduction.get("max_absolute_score_error") != 0
            or reproduction.get("original_rows") != reproduction.get("overlap_rows")):
        raise ValueError("Frozen model predictions do not reproduce the declared fixed native models")
    for name, digest in manifest["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or sha(root/relative) != digest:
            raise ValueError("Frozen prediction/model source bytes changed")
    frame = pd.read_csv(root/"predictions.csv", float_precision="round_trip")
    return {model: rolling_scores_for_calendar(frame, calendar, model=model) for model in MODELS}


def load_validation_context(bundle_path, declaration_path):
    """Load audited evidence first. No performance is generated on a partial bundle."""
    from stopat30m.backtest import ashare_historical_execution as ex
    declaration = _validate_declaration(declaration_path)
    bundle_path = Path(bundle_path).resolve()
    bundle = json.loads(bundle_path.read_text())
    if bundle.get("schema_version") != 1 or bundle.get("kind") != "a4_historical_validation_bundle":
        raise ValueError("Unsupported historical validation evidence bundle")
    inputs = bundle["inputs"]
    required = {"dividend_events", "market_audit", "predictions_manifest", "historical_rules", "calendar", "historical_intervals", "merger_actions"}
    if not required.issubset(inputs) or not ("execution_collection" in inputs or {"daily_csv", "basic_csv"}.issubset(inputs)):
        raise ValueError("Incomplete historical validation source bundle")
    paths = {key: _bound_path(proof, bundle_path.parent) for key, proof in inputs.items()}
    audit = json.loads(paths["market_audit"].read_text())
    validate_market_audit(audit, inputs)
    external_audit_inputs = audit_source_proofs(audit.get("bound_inputs", {}), paths["market_audit"].parent)
    if not external_audit_inputs:
        raise ValueError("Market audit requires bound original dividend/issuer/source evidence bytes")
    spec = json.loads(Path(declaration["spec"]["path"]).read_text())
    rules = ex.verify_historical_rules(paths["historical_rules"])
    rule_sources = audit_source_proofs(rules["bound_inputs"], paths["historical_rules"].parent, prefix="historical_rule_source")
    calendar = paths["calendar"].read_text().splitlines()
    if (len(calendar) < 100 or calendar != sorted(set(calendar))
            or any(pd.Timestamp(day).date().isoformat() != day for day in calendar)):
        raise ValueError("A unique original full historical exchange calendar is required")
    source_start = next((day for day in reversed(calendar) if day < "2023-01-01"), None)
    if source_start is None or not any(day.startswith("2024-12") for day in calendar):
        raise ValueError("Historical exchange calendar does not cover both declared periods and their prior signal")
    pool = PackageHistoricalPool(paths["historical_intervals"], start=source_start, end="2024-12-31")
    if "execution_collection" in paths:
        raw, basic, receipt = load_execution_collection(paths["execution_collection"].parent)
        if receipt["manifest_sha256"] != inputs["execution_collection"]["sha256"]:
            raise ValueError("Execution loader receipt differs from bound bundle")
    else:
        raw = pd.read_csv(paths["daily_csv"], dtype=str, keep_default_na=False)
        basic = pd.read_csv(paths["basic_csv"], dtype=str, keep_default_na=False)
    observations = None
    observation_sources = {}
    if "intraday_observations" in paths:
        from stopat30m.data.ashare_history_audit import apply_intraday_observation_evidence
        observations = json.loads(paths["intraday_observations"].read_text())
        observation_sources = audit_source_proofs(observations, paths["intraday_observations"].parent, prefix="intraday_observation_source")
        raw = apply_intraday_observation_evidence(raw, observations)
    raw = validate_daily_rows(raw, basic, calendar, pool, start=source_start, end="2024-12-31",
        intraday_observations=observations,
        observation_evidence_base=paths["intraday_observations"].parent if observations is not None else None)
    snapshot = load_qlib_snapshot(spec["source_snapshot"])
    if snapshot.manifest_sha256 != spec["source_manifest_sha256"]:
        raise ValueError("Historical validation must retain its frozen repaired feature data vintage")
    if sha(paths["historical_intervals"]) != sha(snapshot.dataset_dir/"instruments/csi300.txt") or sha(paths["calendar"]) != sha(snapshot.dataset_dir/"calendars/day.txt"):
        raise ValueError("Validation membership/calendar differ from the frozen model source vintage")
    normalized = pd.DataFrame({inst: read_qlib_security(snapshot.dataset_dir, inst)["close"] for inst in sorted(pool.intervals)}).reindex(calendar)
    events_payload = json.loads(paths["dividend_events"].read_text())
    events = events_payload["events"]
    rights = json.loads(paths["rights_events"].read_text())["events"] if "rights_events" in paths else []
    validate_rights_issue_terms(rights)
    actions = VerifiedCorporateActions(paths["merger_actions"])
    context = {"spec": spec, "calendar": calendar, "pool": pool, "raw": raw, "basic": basic,
        "normalized": normalized, "events": events, "rights_events": rights, "actions": actions, "scores": _load_predictions(paths["predictions_manifest"].parent, spec, calendar),
        "policy": ex.ExecutionPolicy(initial_cash=spec["initial_cash_cny"], commission_rate=spec["cost_policy"]["commission_rate"],
            minimum_commission=spec["cost_policy"]["minimum_commission_cny"], transfer_rate=spec["cost_policy"]["transfer_rate"],
            sell_stamp_rate=spec["cost_policy"]["sell_stamp_before_20230828"], slippage_bps=spec["cost_policy"]["adverse_slippage_bps"],
            dividend_tax_rate=spec["cost_policy"]["cash_and_evidenced_stock_dividend_tax_reserve_rate"]),
        "bound": {key: {"path": str(paths[key]), "sha256": inputs[key]["sha256"]} for key in inputs}}
    context["bound"].update(bundle=_proof(bundle_path), declaration=_proof(declaration_path),
        source_manifest=_proof(snapshot.path/"manifest.json"), **declaration["bound_implementations"])
    context["bound"].update(external_audit_inputs)
    context["bound"].update(rule_sources)
    context["bound"].update(observation_sources)
    return context


def validate_rights_issue_terms(events):
    """Freeze nonparticipation; verify the documented right is nontransferable."""
    identities = set()
    for row in events:
        known = pd.Timestamp(row["known_at"])
        ex_open = pd.Timestamp(row["ex_date"]).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=9, minutes=30)
        if (not row.get("event_id") or row["event_id"] in identities
                or not re.fullmatch(r"(?:SH|SZ)\d{6}", str(row.get("instrument", "")))
                or not re.fullmatch(r"[0-9a-f]{64}", str(row.get("source_response_sha256", "")))
                or row.get("transferable") is not False or known.tzinfo is None or known > ex_open):
            raise ValueError("Unknown, transferable or not opening-known rights issue mechanism")
        identities.add(row["event_id"])


def record_unsubscribed_rights(state, events, day):
    """Record the declared choice without changing physical shares or cash."""
    validate_rights_issue_terms(events)
    records = []
    for row in events:
        if row["ex_date"] != day:
            continue
        position = state.positions.get(row["instrument"])
        quantity = position.quantity if position is not None else 0
        records.append({"session": day, "event_id": row["event_id"], "instrument": row["instrument"],
            "action": "rights_issue_no_subscription", "known_at": row["known_at"],
            "source_response_sha256": row["source_response_sha256"], "shares_before": quantity,
            "shares_after": quantity, "cash_before": str(state.cash), "cash_after": str(state.cash),
            "subscription_cash_paid": "0.00", "transferable_right_asset_value": "0.00"})
    return records


def daily_inputs(raw, basic, day, symbols):
    """Preserve historical source provenance and daily ST/status for execution."""
    from stopat30m.backtest import ashare_historical_execution as ex
    rows = raw.loc[raw.date.eq(day)].set_index("instrument")
    identities = basic.set_index("code")
    quotes, metadata, closes, suspended = {}, {}, {}, []
    for inst in symbols:
        if inst not in rows.index:
            continue
        row = rows.loc[inst]
        def number(name):
            return Decimal(str(row[name])) if str(row[name]).strip() else None
        quotes[inst] = ex.OpeningQuote(session=day, open=number("open"), preclose=number("preclose"),
            tradestatus={"1": "trading", "0": "suspended"}[row.tradestatus],
            source=str(row.source) + ":" + str(row.source_response_sha256),
            execution_price_basis="historical_observed_open")
        identity = identities.loc[inst[:2].lower() + "." + inst[2:]]
        metadata[inst] = ex.SecurityInfo(instrument=inst, board=a2.board(inst), listing_date=identity.ipoDate,
            is_st=row.isST == "1", known_at=day + "T00:00:00+08:00",
            source="reconstructed_daily_isST_IPO_assumed_daily_effective_cutoff_publication_timestamp_unknown", status_session=day,
            delisting_period=False)
        if bool(row.get("source_observed_market_quote", row.source_complete_quote)):
            closes[inst] = number("close")
        if row.tradestatus == "0":
            suspended.append(inst)
    return quotes, metadata, closes, suspended


def valuation_validity(valuation, *, observed_closes, source_suspended, unobserved_rights_ex_prices=()):
    """Disclose source-confirmed carried marks without calling them fresh quotes.

    The engine's ``valuation_complete`` is deliberately untouched. A known halt
    may use a carried observed accounting mark; an unknown gap or a right with
    no auditable finite upper bound invalidates the whole research session.
    """
    suspended = set(source_suspended)
    carried, unknown, unbounded = [], [], []
    for row in valuation.positions:
        if row["valuation_source"] != "observed_raw_close":
            (carried if row["instrument"] in suspended else unknown).append(row["instrument"])
    for row in valuation.claims:
        if row.get("kind") != "cash_dividend" and row["instrument"] not in observed_closes:
            (carried if row["instrument"] in suspended else unknown).append(row["instrument"])
        try:
            if row.get("kind") == "cash_dividend":
                bound = Decimal(str(row["estimated_value"]))
            elif "upper_value_bound" in row:
                bound = Decimal(str(row["upper_value_bound"]))
            elif row.get("kind") == "bonus_shares" and "integer_lower_value" in row:
                bound = Decimal(str(row["estimated_value"]))
            else:
                raise ValueError("Unknown right kind/bound")
            if not bound.is_finite() or bound < 0:
                raise ValueError("Invalid right upper bound")
        except (KeyError, ValueError, ArithmeticError):
            unbounded.append(row.get("event_id", row.get("action_id", row["instrument"])))
    unpriced_rights = sorted(set(unobserved_rights_ex_prices))
    return {"accounting_valid": not unknown and not unbounded and not unpriced_rights,
        "source_confirmed_carried_instruments": sorted(set(carried)),
        "unknown_price_instruments": sorted(set(unknown)), "unbounded_rights": sorted(set(unbounded)),
        "unobserved_rights_ex_price_instruments": unpriced_rights,
        "engine_valuation_complete": bool(valuation.valuation_complete)}


def conservative_path_metrics(curves, initial_cash):
    """Exact bounded wealth and a drawdown conservative to all recorded bounds."""
    initial = Decimal(str(initial_cash))
    if not curves or initial <= 0:
        raise ValueError("A nonempty costed equity path and positive initial cash are required")
    peak, drawdown = initial, Decimal(0)
    for row in curves:
        lower, upper = Decimal(row["equity_cny"]), Decimal(row["equity_upper_bound_cny"])
        if not lower.is_finite() or not upper.is_finite() or lower <= 0 or upper < lower:
            raise ValueError("Invalid conservative NAV/upper bound")
        peak = max(peak, upper)
        drawdown = min(drawdown, lower / peak - 1)
    return {"total_return_lower_exact": str(Decimal(curves[-1]["equity_cny"]) / initial - 1),
        "total_return_upper_exact": str(Decimal(curves[-1]["equity_upper_bound_cny"]) / initial - 1),
        "max_drawdown_conservative_exact": str(drawdown)}


def simulate_validation(context, *, period, model, execution):
    """Run one declared, independently cash-started model/execution/year pair."""
    from stopat30m.backtest import ashare_historical_execution as ex
    from stopat30m.ashare_historical_optimization_execution import (
        RankBufferPolicy, build_rank_buffer_plan, execute_rank_buffer_plan,
    )
    if period not in PERIODS or model not in MODELS or execution not in EXECUTIONS:
        raise ValueError("Undeclared validation period/model/execution")
    start, end = PERIODS[period]
    calendar, spec = context["calendar"], context["spec"]
    days = [day for day in calendar if start <= day <= end]
    if not days or calendar.index(days[0]) < 1:
        raise ValueError("Actual historical validation sessions or their prior signal are missing")
    engine_calendar = calendar[max(0, calendar.index(days[0])-80):calendar.index(days[-1])+1]
    raw, basic, actions, scores = context["raw"], context["basic"], context["actions"], context["scores"][model]
    symbols = sorted(set(raw.instrument) | {row["to_instrument"] for row in actions.actions})
    signal_raw = raw.loc[raw.get("source_observed_market_quote", raw.source_complete_quote)].set_index(["date", "instrument"])["close"]
    state, previous = ex.PortfolioState.initial(context["policy"]), context["policy"].initial_cash
    rows = {name: [] for name in ("curves", "trades", "orders", "positions", "claims", "signals", "corporate_actions", "plans", "valuation_audit")}
    terminal_fresh = False
    unpriced_rights = set()
    for number, day in enumerate(days):
        policy = ex.historical_policy(day, context["policy"])
        location = calendar.index(day)
        signal_day = calendar[location-1]
        quotes, metadata, closes, suspended = daily_inputs(raw, basic, day, symbols)
        unpriced_rights.difference_update(closes)
        suspended = sorted(set(suspended) | {inst for inst in symbols if actions.suspended(inst, day)})
        events = relevant_events(state, context["events"], day, days[0])
        rights_records = record_unsubscribed_rights(state, context.get("rights_events", []), day)
        rows["corporate_actions"].extend(rights_records)
        unpriced_rights.update(row["instrument"] for row in rights_records if row["shares_before"] > 0 and row["instrument"] not in closes)
        # Event functions share the frozen raw-share algorithms and accept the
        # same serialized ledger; no prices or future rights are substituted.
        state, ledger = a2._apply_events(state, day, events, actions, engine_calendar, quotes, policy, days[0])
        rows["corporate_actions"].extend(ledger)
        if number % spec["rebalance_sessions"] == 0:
            if execution == "E0":
                weights, members = a2._targets("lightgbm", signal_day, location, context["normalized"], context["pool"], scores, actions, spec["top_k"])
                result = ex.execute_target_weights(state, session=day, signal_date=signal_day, targets=weights,
                    quotes=quotes, metadata=metadata, calendar=engine_calendar, policy=policy, verified_suspended=suspended)
                selected = set(weights)
            else:
                # Select by prior actual membership and source closes only.
                members = context["pool"].members_on(signal_day)
                source = scores.xs(pd.Timestamp(signal_day), level="datetime")
                eligible = [inst for inst in members if not actions.excluded(inst, signal_day, close=True)
                    and inst in context["normalized"] and np.isfinite(context["normalized"].at[signal_day, inst])
                    and context["normalized"].at[signal_day, inst] > 0 and inst in source and np.isfinite(source.loc[inst])]
                ranked = sorted(eligible, key=lambda inst: (-float(source.loc[inst]), inst))
                if not ranked:
                    raise ValueError("No finite eligible frozen prior-close model scores")
                prices = {inst: signal_raw.loc[(signal_day, inst)] for inst in ranked if (signal_day, inst) in signal_raw.index}
                plan = build_rank_buffer_plan(state, signal_date=signal_day, ranked_instruments=ranked,
                    raw_signal_prices=prices, boards={inst: a2.board(inst) for inst in ranked}, signal_equity=previous,
                    policy=RankBufferPolicy(top_k=spec["top_k"], keep_rank=spec["rank_exit_threshold"], require_signal_affordability=execution == "E2"), execution_policy=policy)
                result = execute_rank_buffer_plan(state, plan=plan, session=day, quotes=quotes, metadata=metadata,
                    calendar=engine_calendar, policy=policy, verified_suspended=suspended)
                rows["plans"].append({"session": day, **result.audit})
                selected = set(plan.selected)
            state = result.state
            rows["trades"].extend(result.trades)
            rows["orders"].extend(result.orders)
            rows["signals"].extend({"session": day, "signal_date": signal_day, "instrument": inst, "selected": inst in selected} for inst in members)
        valued = ex.mark_to_close(state, session=day, prices=closes, calendar=engine_calendar, policy=policy, verified_suspended=suspended)
        state, equity = valued.state, valued.equity_lower_bound
        validity = valuation_validity(valued, observed_closes=closes, source_suspended=suspended, unobserved_rights_ex_prices=unpriced_rights)
        rows["valuation_audit"].append({"session": day, **validity})
        rows["curves"].append({"date": day, "equity_cny": str(equity), "cash_cny": str(state.cash),
            "equity_with_claim_estimate_cny": str(valued.equity_with_claim_estimate), "equity_upper_bound_cny": str(valued.equity_upper_bound),
            "valuation_complete": valued.valuation_complete, "accounting_valid": validity["accounting_valid"],
            "nav": float(equity / policy.initial_cash), "net_return": float(equity / previous - 1)})
        rows["positions"].extend(valued.positions)
        rows["claims"].extend({"session": day, **row} for row in valued.claims)
        terminal_fresh = all(inst in closes for inst in state.positions) and all(
            row.get("kind") == "cash_dividend" or row["instrument"] in closes for row in valued.claims)
        previous = equity
    curves = pd.DataFrame(rows["curves"])
    metrics = compute_portfolio_metrics(curves.net_return)
    metrics.update(conservative_path_metrics(rows["curves"], context["policy"].initial_cash))
    metrics.update(final_equity_cny=rows["curves"][-1]["equity_cny"], trade_count=len(rows["trades"]),
        incomplete_valuation_days=int((~curves.valuation_complete).sum()), accounting_invalid_days=int((~curves.accounting_valid).sum()),
        source_confirmed_carry_days=sum(bool(row["source_confirmed_carried_instruments"]) for row in rows["valuation_audit"]),
        terminal_prices_fresh=terminal_fresh,
        mean_non_cash_equity_fraction=float(((curves.equity_cny.astype(float)-curves.cash_cny.astype(float)) / curves.equity_cny.astype(float)).mean()),
        rejected_order_count=sum(row["status"] == "rejected" for row in rows["orders"]))
    for key in ("commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost", "notional"):
        metrics[key + "_cny"] = str(sum((Decimal(row[key]) for row in rows["trades"]), Decimal(0)))
    return {"case_id": model + "_" + execution, "period": period, "model": model, "execution_id": execution,
        "metrics": metrics, "records": rows, "final_state": state.to_dict()}


def select_candidate(results, *, evidence_valid):
    """Apply the frozen 2024 gate using exact conservative bounded NAV."""
    expected = {(period, model + "_" + execution) for period in PERIODS for model in MODELS for execution in EXECUTIONS}
    identities = [(row["period"], row["case_id"]) for row in results]
    if not evidence_valid or len(identities) != len(set(identities)) or set(identities) != expected:
        return {"selection_status": "blocked_missing_audited_evidence_or_declared_runs", "selected_candidate": None}
    validity = {}
    for model in MODELS:
        for execution in EXECUTIONS:
            identity = model + "_" + execution
            reasons = []
            for row in results:
                if row["case_id"] != identity:
                    continue
                if row["metrics"]["accounting_invalid_days"]:
                    reasons.append(row["period"] + ":unknown_marks_or_unbounded_rights")
                if not row["metrics"]["terminal_prices_fresh"]:
                    reasons.append(row["period"] + ":stale_terminal_price")
            validity[identity] = {"eligible": not reasons, "reasons": reasons}
    if not validity["M0_E0"]["eligible"]:
        return {"selection_status": "blocked_baseline_unknown_marks_rights_or_stale_terminal_prices", "selected_candidate": None,
            "candidate_validity": validity}
    validation = [row for row in results if row["period"] == "validation"]
    baseline = next(row for row in validation if row["case_id"] == "M0_E0")
    required_return = Decimal(baseline["metrics"]["total_return_upper_exact"])
    candidates = []
    for row in validation:
        metric = row["metrics"]
        lower = Decimal(metric["total_return_lower_exact"])
        upper = Decimal(metric["total_return_upper_exact"])
        dd = Decimal(metric["max_drawdown_conservative_exact"])
        notional = Decimal(metric["notional_cny"])
        if not all(value.is_finite() for value in (lower, upper, dd, notional)) or upper < lower or dd > 0 or notional < 0:
            raise ValueError("Invalid exact selection metrics")
        if validity[row["case_id"]]["eligible"] and row["case_id"] != "M0_E0" and lower > 0 and lower > required_return and abs(dd) <= Decimal("0.20"):
            candidates.append(row)
    if not candidates:
        return {"selection_status": "validated_no_candidate_passed_keep_baseline", "selected_candidate": None, "candidate_validity": validity}
    selected = min(candidates, key=lambda row: (-Decimal(row["metrics"]["total_return_lower_exact"]),
        abs(Decimal(row["metrics"]["max_drawdown_conservative_exact"])), Decimal(row["metrics"]["notional_cny"]), row["case_id"]))
    return {"selection_status": "validated_candidate_recommended_pending_separate_forward_freeze", "selected_candidate": selected["case_id"],
        "candidate_validity": validity}


def run_validation(bundle_path, output_dir, *, declaration_path, progress=None):
    """Publish all twelve predeclared results; never mutate an active paper run."""
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Preserve the existing A4 validation artifact")
    context = load_validation_context(bundle_path, declaration_path)
    results = []
    for period in PERIODS:
        for model in MODELS:
            for execution in EXECUTIONS:
                if progress:
                    progress("Starting " + period + " " + model + "_" + execution)
                result = simulate_validation(context, period=period, model=model, execution=execution)
                results.append(result)
                if progress:
                    progress({"period": period, "case": result["case_id"], **result["metrics"]})
    for proof in context["bound"].values():
        _bound_path(proof, Path.cwd())
    context["actions"].assert_unchanged()
    # Recheck every supplier, model and rule source after consuming the bytes.
    load_validation_context(bundle_path, declaration_path)
    decision = select_candidate(results, evidence_valid=True)
    output.mkdir(parents=True)
    bound = dict(context["bound"])
    for name in ("validation", "historical_engine", "historical_execution_adapter", "ranking_and_rights_helpers",
                 "execution_and_corporate_action_helpers", "execution_collection_loader", "metric_display", "intraday_observation_implementation"):
        original = Path(bound[name]["path"])
        archived = output / "source" / original.name
        archived.parent.mkdir(exist_ok=True)
        shutil.copyfile(original, archived)
        if sha(archived) != bound[name]["sha256"]:
            raise ValueError("Validation implementation changed while archiving")
        bound[name] = {**bound[name], "path": str(archived), "original_path": str(original)}
    summaries = {}
    for result in results:
        case = output / result["period"] / result["case_id"]
        case.mkdir(parents=True)
        for name, records in result["records"].items():
            if name in {"plans", "valuation_audit"}:
                save(case / (name + ".json"), records)
            else:
                pd.DataFrame(records).to_csv(case / (name + ".csv"), index=False)
        save(case / "final_state.json", result["final_state"])
        summaries[result["period"] + "/" + result["case_id"]] = {key: value for key, value in result.items() if key not in {"records", "final_state"}}
    report = {"schema_version": 1, "kind": "a4_fee_inclusive_historical_validation", "completed_at": datetime.now(timezone.utc).isoformat(),
        "historical_freeze_claim": False, "history_role": "retrospective_reconstructed_development_and_validation",
        "fresh_oos_start": context["spec"]["fresh_oos_start"], "cases": summaries, **decision,
        "active_paper_changed": False, "bound_inputs": bound,
        "valuation_basis": "disclosed_source_confirmed_halt_carry_accounting_nav_with_bounded_rights_lower_upper",
        "status_time_basis": VALIDITY_POLICY["daily_status_known_at_semantics"],
        "limitations": ["Historical daily ST/status and membership are reconstructed, not announcement-time PIT.",
            "Metadata known_at at daily 00:00 is an assumed daily effective cutoff; actual publication timestamps are unknown.",
            "Source-confirmed suspended marks are accounting values, not newly executable prices."],
        "selection_uses_2025_2026_history_or_current_paper": False}
    save(output / "report.json", report)
    save(output / "manifest.json", {"bound_inputs": bound, "files": {str(path.relative_to(output)): sha(path) for path in sorted(output.rglob("*")) if path.is_file()}})
    return report
