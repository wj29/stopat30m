"""Read-only pending share-exchange rights reconciliation of sealed ledgers.

An issuer's exchange ratio establishes conditional economic rights, not receipt
of tradable successor stock. This sidecar never changes an execution account or
turns an invalid original backtest into verified performance.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse

import pandas as pd


KIND = "a7_verified_pending_share_exchange"
SHAREHOLDER_CLASS = "ordinary_public_non_locked_no_cash_option_exercise"
ALLOCATION = "account_aggregate_floor_or_ceil_largest_remainder"
FACTS = {"record_date", "ratio", "delisting", "allocation_policy", "pending_completion"}
PERIODS = ("2025", "2026", "continuous")
VARIANTS = ("baseline", "hold10_cap2", "hold10_cap5", "hold20_cap2", "hold20_cap5")


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": _sha(path)}


def _read(path):
    return json.loads(Path(path).read_bytes())


def _save(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
        sort_keys=True, allow_nan=False)+"\n")


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _decimal(value):
    if isinstance(value, bool):
        raise ValueError("A finite economic value is required")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("A finite economic value is required") from exc
    if not result.is_finite():
        raise ValueError("A finite economic value is required")
    return result


def _day(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("An exact ISO session date is required")
    return value


def _known(value, session):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Public knowledge time needs an explicit timezone")
    cutoff = pd.Timestamp(session).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15)
    if stamp > cutoff:
        raise ValueError("Future-public facts cannot value a historical pending right")


def _verify_proofs(proofs):
    for proof in proofs.values():
        if (not Path(proof["path"]).is_absolute()
                or not re.fullmatch(r"[0-9a-f]{64}", proof.get("sha256", ""))
                or _sha(proof["path"]) != proof["sha256"]):
            raise ValueError("Bound original evidence bytes changed")


def validate_contract(contract):
    if (contract.get("kind") != KIND or not contract.get("id")
            or contract.get("shareholder_class") != SHAREHOLDER_CLASS
            or contract.get("allocation_policy") != ALLOCATION
            or set(contract.get("facts_verified", [])) != FACTS
            or contract.get("completion_verified") is not False
            or contract.get("tradability_verified") is not False):
        raise ValueError("A reviewed, explicitly pending exchange contract is required")
    for key in ("from_instrument", "to_instrument"):
        if not re.fullmatch(r"(?:SH|SZ)\d{6}", contract.get(key, "")):
            raise ValueError("Raw listed security identities are required")
    if (contract["from_instrument"] == contract["to_instrument"]
            or _decimal(contract["ratio"]) <= 0
            or _day(contract["record_date"]) >= _day(contract["delisting_date"])):
        raise ValueError("Invalid exchange ratio or record/delisting order")
    evidence = contract.get("evidence")
    if not isinstance(evidence, dict) or not evidence:
        raise ValueError("Original reviewed public evidence is required")
    for item in evidence.values():
        host = urlparse(item.get("url", "")).hostname or ""
        if (not item.get("url", "").startswith("https://")
                or not (host == "sse.com.cn" or host.endswith(".sse.com.cn")
                    or host == "cninfo.com.cn" or host.endswith(".cninfo.com.cn")
                    or host == "cicc.com" or host.endswith(".cicc.com"))
                or item.get("original_pdf_visual_reviewed") is not True):
            raise ValueError("Each fact requires a visually reviewed official original")
    _verify_proofs(evidence)
    if "source_terms_proof" in contract:
        _verify_proofs({"reviewed_source_terms": contract["source_terms_proof"],
            "source_manifest": contract["source_manifest_proof"]})
        source = _read(contract["source_terms_proof"]["path"])
        _verify_manifest(Path(contract["source_manifest_proof"]["path"]).parent)
        event = source["event"]
        for key, original in (("from_instrument", "from_instrument"),
                ("to_instrument", "to_instrument"), ("ratio", "raw_share_exchange_ratio"),
                ("record_date", "exchange_entitlement_record_date"),
                ("delisting_date", "delisting_date")):
            if contract[key] != event[original]:
                raise ValueError("Pending contract differs from its reviewed original source terms")
        if (event.get("ordinary_original_shares_delivered_to_successor_is_not_yet_proven") is not True
                or event.get("record_date_cutoff") != "market_close"):
            raise ValueError("The source does not support this pending-right policy")
        earliest_public = min(pd.Timestamp(item["conservative_known_at"]) for item in source["issuer_originals"])
        if any(pd.Timestamp(contract[key]) < earliest_public for key in
                ("record_date_known_at", "delisting_known_at", "ratio_known_at")):
            raise ValueError("Contract knowledge cannot predate its bound public evidence")


def value_pending_share_exchange_overlay(*, session, base_curve, base_positions,
        final_state, contract, successor_quote):
    """Return conditional proxy marks, leaving every input ledger untouched.

    The quantity interval describes CSDC integer allocation, not a cash recovery
    guarantee. Underlying raw close is a declared look-through valuation proxy;
    it is never a quote for the unregistered right, nor permission to sell it.
    This terminal-only reconciliation cannot be used as an intraday executor.
    """
    _day(session)
    validate_contract(contract)
    if base_curve.get("date") != session:
        raise ValueError("The base valuation must be the declared terminal session")
    original = deepcopy(base_curve)
    audit = {"contract_id": contract["id"], "session": session,
        "base_valuation_sha256": _digest(base_curve), "contract_sha256": _digest(contract),
        "original_performance_flag_is_not_reclassified": True,
        "performance_valid": False, "settlement_assumed": False, "tradable": False,
        "economic_rights_accounting_bounds_valid": False,
        "unconditional_recovery_floor_cny": "0.00", "old_mark_removed_cny": "0.00",
        "cash_change_cny": "0.00", "order_or_trade_submitted": False,
        "new_successor_physical_shares": 0,
        "valuation_policy": "conditional_integer_allocation_at_observed_successor_raw_close_proxy",
        "proxy_mark_interval_is_not_a_guaranteed_cash_value_interval": True}
    if session < contract["delisting_date"]:
        return {"curve": original, "rights": [], "audit": {**audit,
            "status": "event_outside_terminal_window_original_ledger_unchanged"}}
    if session != contract["delisting_date"]:
        raise ValueError("Later sessions require a separate confirmed ongoing rights ledger")
    if final_state.get("session") != session:
        raise ValueError("Final state and original terminal valuation sessions must match")
    for key in ("record_date_known_at", "delisting_known_at", "ratio_known_at"):
        _known(contract[key], session)
    if any(row.get("contract_id") == contract["id"] for row in base_positions):
        raise ValueError("Do not apply a pending overlay to an already adjusted valuation")
    if base_curve.get("pending_exchange_overlay_applied"):
        raise ValueError("An economic right must never replace the old marked value twice")
    old, new = contract["from_instrument"], contract["to_instrument"]
    history = final_state.get("holdings_history", {})
    if contract["record_date"] not in history:
        raise ValueError("Actual finalized record-date holdings are required")
    record_quantity = history[contract["record_date"]].get(old, 0)
    final_quantity = final_state.get("positions", {}).get(old, {}).get("quantity", 0)
    if any(type(value) is not int or value < 0 for value in (record_quantity, final_quantity)):
        raise ValueError("Actual raw share quantities must be nonnegative integers")
    if record_quantity != final_quantity:
        raise ValueError("Unresolved post-record share changes require a separate allocation ledger")
    old_rows = [row for row in base_positions if row.get("instrument") == old]
    if not record_quantity:
        if old_rows:
            raise ValueError("Terminal position records conflict with finalized share ownership")
        return {"curve": original, "rights": [], "audit": {**audit,
            "status": "no_record_date_exposure_original_ledger_unchanged"}}
    if len(old_rows) != 1 or old_rows[0].get("session") != session:
        raise ValueError("A unique original terminal marked position is required")
    row = old_rows[0]
    if _decimal(row["quantity"]) != record_quantity:
        raise ValueError("Marked quantity differs from actual record-date shares")
    for key, holdings in history.items():
        if contract["record_date"] <= key <= session and holdings.get(old, 0) != record_quantity:
            raise ValueError("Post-record holdings changed before pending-right reconciliation")
    if (row.get("valuation_source") != "carried_observed_mark"
            or _day(row["price_date"]) >= contract["delisting_date"]):
        raise ValueError("A delisted security's apparent fresh quote cannot replace verified pending rights")
    old_value = _decimal(row["market_value"])
    if old_value != (record_quantity*_decimal(row["price"])).quantize(Decimal(".01")):
        raise ValueError("The removed old mark must reconcile with raw shares and original price")
    if _decimal(final_state["cash"]) != _decimal(base_curve["cash_cny"]):
        raise ValueError("Original cash ledger and base valuation do not reconcile")
    for item in final_state.get("claims", {}).values():
        if item.get("instrument") == old:
            raise ValueError("Overlapping unresolved fractional rights need a separate allocation policy")
    for item in final_state.get("entitlements", {}).values():
        if (item.get("instrument") == old and item.get("status") == "pending"
                and item.get("kind") != "cash_dividend"
                and _decimal(item.get("quantity", 0)) > 0):
            raise ValueError("Overlapping pending stock rights need a separate allocation policy")
    quote = successor_quote or {}
    if (quote.get("instrument") != new or quote.get("session") != session
            or quote.get("tradestatus") != "1"
            or not re.fullmatch(r"[0-9a-f]{64}", quote.get("source_sha256", ""))
            or _decimal(quote.get("close", 0)) <= 0):
        raise ValueError("An observed same-session successor raw closing price is required")
    price = _decimal(quote["close"])
    exact = record_quantity*_decimal(contract["ratio"])
    whole = int(exact.to_integral_value(rounding=ROUND_FLOOR))
    ceiling = int(exact.to_integral_value(rounding=ROUND_CEILING))
    lower = (whole*price).quantize(Decimal(".01"), rounding=ROUND_FLOOR)
    upper = (ceiling*price).quantize(Decimal(".01"), rounding=ROUND_CEILING)
    estimate = (exact*price).quantize(Decimal(".01"))
    for key in ("equity_cny", "equity_upper_bound_cny", "equity_with_claim_estimate_cny"):
        if old_value < 0 or old_value > _decimal(base_curve[key]):
            raise ValueError("Old marked exposure is outside original accounting NAV")
    marked = deepcopy(base_curve)
    marked.update(equity_cny=str(_decimal(base_curve["equity_cny"])-old_value+lower),
        equity_upper_bound_cny=str(_decimal(base_curve["equity_upper_bound_cny"])-old_value+upper),
        equity_with_claim_estimate_cny=str(_decimal(base_curve["equity_with_claim_estimate_cny"])-old_value+estimate),
        pending_exchange_overlay_applied=True, accounting_valid=False,
        performance_valid=False, economic_rights_accounting_bounds_valid=True)
    # Old nav/net_return fields would be misleading after a new conditional
    # mark; the caller recomputes the complete diagnostic curve explicitly.
    marked.pop("nav", None); marked.pop("net_return", None)
    right = {"contract_id": contract["id"], "session": session,
        "kind": "pending_share_exchange_economic_right", "source_instrument": old,
        "instrument": new, "record_date": contract["record_date"],
        "actual_record_date_raw_shares": record_quantity,
        "ratio": contract["ratio"], "exact_successor_quantity": str(exact),
        "quantity_lower": whole, "quantity_upper": ceiling,
        "conditional_proxy_mark_lower_cny": str(lower),
        "conditional_proxy_mark_upper_cny": str(upper),
        "conditional_proxy_mark_estimate_cny": str(estimate),
        "observed_underlying_close_cny": str(price), "underlying_quote": quote,
        "unconditional_recovery_floor_cny": "0.00", "actual_delivered_shares": None,
        "spendable": False, "tradable": False, "completion_verified": False,
        "integer_allocation_is_unresolved": whole != ceiling,
        "cash_dividend_receivables_and_existing_successor_shares_unchanged": True}
    return {"curve": marked, "rights": [right], "audit": {**audit,
        "status": "conditional_economic_right_mark_reconciled_original_performance_stays_invalid",
        "economic_rights_accounting_bounds_valid": True, "old_mark_removed_cny": str(old_value),
        "record_quantity": record_quantity, "quantity_lower": whole, "quantity_upper": ceiling,
        "valuation_identity": _digest({"contract": _digest(contract), "base": _digest(base_curve),
            "record_shares": record_quantity, "session": session, "quote": quote})}}


def _verify_manifest(root):
    root = Path(root).resolve()
    manifest = _read(root/"manifest.json")
    actual = {str(path.relative_to(root)) for path in root.rglob("*")
        if path.is_file() and path != root/"manifest.json"}
    if manifest.get("kind") == "a7_official_exchange_source_investigation_manifest":
        files = {}
        for name, proof in manifest["original_files"].items():
            if Path(proof["path"]).resolve() != (root/name).resolve():
                raise ValueError("Original source file proof points outside its declared name")
            files[name] = proof["sha256"]
    else:
        files = manifest["files"]
    if actual != set(files):
        raise ValueError("Original artifact inventory changed")
    for name, digest in files.items():
        path = (root/name).resolve()
        if not path.is_relative_to(root) or _sha(path) != digest:
            raise ValueError("Original sealed artifact changed: "+name)
    return manifest


def _verify_original_baselines(experiment, report):
    declaration = _read(experiment/"declaration.json")
    original = Path(declaration["baseline_directory"])
    _verify_manifest(original)
    for period in PERIODS:
        if report["baseline_full_reproduction"][period].get("passed") is not True:
            raise ValueError("All three original baseline reproductions are required")
        copy = experiment/"baseline"/period
        source = original/period/"LGB"
        summary = _read(copy/"summary.json")
        for key in ("variant", "variant_terms", "minimum_commission_fill_fraction", "score_quality_is_fixed_universe_diagnostic"):
            summary.pop(key)
        if summary != _read(source/"summary.json"):
            raise ValueError("The original baseline summary changed")
        for path in source.iterdir():
            if path.is_file() and path.name != "summary.json" and _sha(path) != _sha(copy/path.name):
                raise ValueError("An original baseline canonical record differs: "+path.name)


def _conditional_metrics(curves, initial, notional):
    lower = [_decimal(initial), *[_decimal(row["equity_cny"]) for row in curves]]
    upper = [_decimal(initial), *[_decimal(row["equity_upper_bound_cny"]) for row in curves]]
    if any(lo <= 0 or hi < lo for lo, hi in zip(lower, upper)):
        raise ValueError("Conditional economic NAV bounds are inconsistent")
    peak, dd = upper[0], Decimal(0)
    for lo, hi in zip(lower, upper):
        peak = max(peak, hi); dd = min(dd, lo/peak-1)
    return {"conditional_proxy_net_return_lower": str(lower[-1]/_decimal(initial)-1),
        "conditional_proxy_net_return_upper": str(upper[-1]/_decimal(initial)-1),
        "conditional_proxy_conservative_max_drawdown": str(dd),
        "conditional_proxy_turnover_lower": float(_decimal(notional)/2/(sum(upper[1:])/len(curves))),
        "conditional_proxy_turnover_upper": float(_decimal(notional)/2/(sum(lower[1:])/len(curves))),
        "performance_valid": False,
        "not_a_realized_return_or_liquidation_valuation": True}


def run_pending_exchange_audit(*, experiment_directory, contract_path, output_directory):
    implementation_bytes = Path(__file__).read_bytes()
    implementation_proof = {"path": str(Path(__file__).resolve()),
        "sha256": hashlib.sha256(implementation_bytes).hexdigest()}
    contract_path = Path(contract_path).resolve()
    contract_bytes = contract_path.read_bytes()
    contract_proof = {"path": str(contract_path), "sha256": hashlib.sha256(contract_bytes).hexdigest()}
    contract = json.loads(contract_bytes)
    from stopat30m.data.qlib_snapshot import guard_qlib_output_path
    from stopat30m.ashare_stability_model_reuse import verify_reused_stability_models
    output = guard_qlib_output_path(output_directory)
    if output.exists():
        raise ValueError("Preserve every prior pending-right reconciliation")
    experiment = Path(experiment_directory).resolve()
    _verify_manifest(experiment)
    report = _read(experiment/"report.json")
    if report.get("kind") != "a6_fixed_lgb_turnover_results":
        raise ValueError("A complete original five-variant turnover experiment is required")
    paths = {(r["variant"], r["period"]): r for r in report["results"]}
    if len(paths) != 15 or set(paths) != {(v, p) for v in VARIANTS for p in PERIODS}:
        raise ValueError("All15 original paths must be reconciled without selection")
    _verify_proofs(report["bound_inputs"])
    _verify_original_baselines(experiment, report)
    declaration = _read(experiment/"declaration.json")
    protocol = report["bound_inputs"]["protocol"]["path"]
    audit_path = report["bound_inputs"]["market_audit"]["path"]
    model_receipt = verify_reused_stability_models(declaration["predictions_directory"], protocol, audit_path)
    validate_contract(contract)
    market = _read(audit_path)
    csv_proof = market["bound_inputs"]["daily_csv"]
    _verify_proofs({"original_daily": csv_proof})
    raw = pd.read_csv(csv_proof["path"], dtype=str, keep_default_na=False)
    bound = {"source_experiment_report": _proof(experiment/"report.json"),
        "source_experiment_manifest": _proof(experiment/"manifest.json"),
        "contract": contract_proof, "sidecar_implementation": implementation_proof,
        "canonical_daily_csv": csv_proof, **{"evidence:"+key: value for key, value in contract["evidence"].items()}}
    output.mkdir(parents=True)
    archive = output/"source"
    archive.mkdir()
    (archive/"ashare_pending_exchange_audit.py").write_bytes(implementation_bytes)
    (archive/"pending_exchange_contract.json").write_bytes(contract_bytes)
    _verify_proofs(bound)
    _save(output/"declaration.json", {"kind": "a7_pending_exchange_sidecar_declaration",
        "declared_at": datetime.now(timezone.utc).isoformat(), "contract": contract,
        "bound_inputs": bound, "all_variants": list(VARIANTS), "all_periods": list(PERIODS),
        "only_terminal_nontradable_conditional_proxy_mark_changes": True,
        "original_performance_flags_remain_unmodified": True,
        "original_predictions_reused_without_fitting": True, "active_paper_or_broker_change": False})
    results = []
    for variant in VARIANTS:
        for period in PERIODS:
            summary = paths[variant, period]
            source = experiment/variant/period
            curves = pd.read_csv(source/"curves.csv", dtype=str, keep_default_na=False).to_dict("records")
            final_day = curves[-1]["date"]
            positions = pd.read_csv(source/"positions.csv", dtype=str, keep_default_na=False)
            terminal_positions = positions.loc[positions.session.eq(final_day)].to_dict("records")
            raw_quote = raw.loc[raw.instrument.eq(contract["to_instrument"]) & raw.date.eq(final_day)]
            quote = None
            if len(raw_quote) == 1:
                item = raw_quote.iloc[0]
                quote = {"instrument": contract["to_instrument"], "session": final_day,
                    "close": item["close"], "tradestatus": item["tradestatus"],
                    "source_sha256": item["source_response_sha256"]}
            overlay = value_pending_share_exchange_overlay(session=final_day,
                base_curve=curves[-1], base_positions=terminal_positions,
                final_state=_read(source/"final_state.json"), contract=contract, successor_quote=quote)
            conditional = deepcopy(curves)
            conditional[-1] = overlay["curve"]
            valuation = _read(source/"valuation_audit.json")
            bad = [row for row in valuation if row["accounting_valid"] is not True]
            affected = bool(overlay["rights"])
            if affected and (len(bad) != 1 or bad[0]["session"] != final_day
                    or bad[0]["unknown_price_instruments"] != [contract["from_instrument"]]
                    or bad[0]["unpriced_delisted_claims"] != [contract["from_instrument"]]
                    or bad[0]["unbounded_rights"] or bad[0]["unobserved_rights_ex_price_instruments"]):
                raise ValueError("The pending exchange does not explain every original valuation failure")
            directory = output/variant/period
            directory.mkdir(parents=True)
            pd.DataFrame(conditional).to_csv(directory/"conditional_proxy_curves.csv", index=False)
            _save(directory/"pending_rights.json", overlay["rights"])
            _save(directory/"reconciliation.json", overlay["audit"])
            metrics = _conditional_metrics(conditional, "100000.00", summary["metrics"]["notional_cny"])
            removed = _decimal(overlay["audit"]["old_mark_removed_cny"])
            row = {"variant": variant, "period": period,
                "original_performance_valid": summary["performance_valid"],
                "original_metrics": summary["metrics"], "pending_exchange_affected": affected,
                "economic_rights_accounting_bounds_valid": overlay["audit"]["economic_rights_accounting_bounds_valid"],
                "conditional_proxy_metrics": metrics if affected else None,
                "unconditional_zero_recovery_terminal_equity_cny": str(_decimal(curves[-1]["equity_cny"])-removed) if affected else None,
                "old_mark_removed_cny": str(removed), "rights": overlay["rights"],
                "original_trades_cash_and_physical_positions_changed": False,
                "strategy_selection_or_model_training_allowed_by_this_sidecar": False}
            _save(directory/"summary.json", row)
            results.append(row)
    _verify_manifest(experiment)
    _verify_proofs(report["bound_inputs"])
    _verify_proofs(bound)
    validate_contract(contract)
    after = verify_reused_stability_models(declaration["predictions_directory"], protocol, audit_path)
    payload = {"kind": "a7_pending_exchange_read_only_reconciliation", "completed_at": datetime.now(timezone.utc).isoformat(),
        "results": results, "bound_inputs": bound, "model_verification_before": model_receipt,
        "model_verification_after": after, "original_performance_valid_count": sum(r["original_performance_valid"] for r in results),
        "pending_rights_reconciled_path_count": sum(r["pending_exchange_affected"] for r in results),
        "new_verified_performance_claim": False, "strategies_promoted": [], "models_trained": [],
        "next_step": "obtain_confirmed_registration_allocation_listing_and_forward_observation",
        "limitations": ["Underlying price is a conditional economic look-through proxy, not a market quote or guaranteed cash recovery for an unregistered right.",
            "Largest remainder/random integer allocation is represented by floor/ceil, never invented actual delivered stock.",
            "This sidecar leaves original invalid flags and strategy qualification unchanged.",
            "No execution account, cash, orders, training, predictions or active paper settings are modified."]}
    _save(output/"report.json", payload)
    _save(output/"manifest.json", {"kind": payload["kind"], "bound_inputs": bound,
        "files": {str(path.relative_to(output)): _sha(path) for path in sorted(output.rglob("*")) if path.is_file()}})
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-directory", type=Path, required=True)
    parser.add_argument("--contract-path", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = vars(parser.parse_args())
    result = run_pending_exchange_audit(**args)
    print(json.dumps({"completed": True, "pending_right_paths": result["pending_rights_reconciled_path_count"],
        "original_valid_paths": result["original_performance_valid_count"], "new_verified_performance": False}))


if __name__ == "__main__":
    main()
