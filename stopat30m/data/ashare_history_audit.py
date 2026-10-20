"""Bind actual 2023--2024 market evidence before portfolio returns are run.

The audit retains missing/suspended observations. It does not infer historical
ST status, trade availability or an exchange ex-right reference from prices.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from stopat30m.ashare_execution_experiment import audit_ex_references, audit_raw_prices
from stopat30m.backtest.ashare_execution import money
from stopat30m.data.ashare_actions import (
    apply_action_overrides, dividend_events, load_dividend_evidence,
)
from stopat30m.data.ashare_execution_data import load_execution_collection
from stopat30m.data.qlib_incremental import read_qlib_security
from stopat30m.data.qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
                                   sort_keys=True, allow_nan=False) + "\n")


def apply_intraday_observation_evidence(raw, evidence):
    """Keep status=0 non-executable while retaining evidenced intraday prices."""
    result = raw.copy()
    numeric = result[["open", "high", "low", "close", "preclose", "volume", "amount"]].replace("", np.nan).apply(pd.to_numeric, errors="raise")
    result["source_observed_market_quote"] = result.tradestatus.eq("1") & numeric.notna().all(axis=1) & numeric.gt(0).all(axis=1)
    seen = set()
    for event in evidence.get("events", []):
        key = (event["instrument"], event["date"])
        if key in seen:
            raise ValueError("Duplicate partial-session observation evidence")
        seen.add(key)
        matching = result.instrument.eq(key[0]) & result.date.eq(key[1])
        if int(matching.sum()) != 1:
            raise ValueError("Partial-session evidence must match exactly one original row")
        row = result.loc[matching].iloc[0]
        proofs = event.get("evidence", [])
        if (row.tradestatus != "0" or row.source_response_sha256 != event["source_response_sha256"]
                or event.get("execution_remains_blocked") is not True or len(proofs) < 2):
            raise ValueError("Partial-session source identity/status/proof differs")
        for proof in proofs:
            if not Path(proof["path"]).is_absolute() or sha(proof["path"]) != proof["sha256"]:
                raise ValueError("Original partial-session evidence bytes changed")
        numbers = pd.to_numeric(row[["open", "high", "low", "close", "preclose", "volume", "amount"]], errors="raise")
        if not np.isfinite(numbers).all() or not numbers.gt(0).all():
            raise ValueError("Partial-session observation requires actual complete positive OHLCV")
        if (Decimal(str(row.open)) != Decimal(event["source_observed_open"])
                or Decimal(str(row.close)) != Decimal(event["source_observed_close"])):
            raise ValueError("Independently verified partial-session price differs")
        result.loc[matching, "source_observed_market_quote"] = True
    return result


def audit_daily_coverage(raw, basic, calendar, instruments, *, start, end):
    """Every listed security/session needs explicit source status, even in a halt."""
    days = [day for day in calendar if start <= day <= end]
    if not days or len(calendar) != len(set(calendar)) or calendar != sorted(calendar):
        raise ValueError("A unique, ordered actual exchange calendar is required")
    identities = basic.set_index("code")
    missing, invalid, suspended = [], [], []
    if raw.duplicated(["instrument", "date"]).any():
        raise ValueError("Duplicate original daily execution keys")
    for inst in instruments:
        identity = identities.loc[inst[:2].lower() + "." + inst[2:]]
        ipo, out = str(identity.ipoDate), str(identity.outDate).strip()
        if not ipo or ipo == "nan":
            raise ValueError("A historical security has no source IPO identity")
        pd.Timestamp(ipo)
        frame = raw.loc[raw.instrument.eq(inst)].set_index("date")
        expected = [day for day in days if day >= ipo and (not out or out == "nan" or day < out)]
        missing.extend({"instrument": inst, "session": day} for day in expected if day not in frame.index)
        for day in frame.index.intersection(expected):
            row = frame.loc[day]
            if row.tradestatus not in {"0", "1"} or row.isST not in {"0", "1"}:
                invalid.append({"instrument": inst, "session": day, "reason": "unknown_daily_status"})
            elif row.tradestatus == "0":
                fields = ["open", "high", "low", "close", "preclose", "volume", "amount"]
                if all(field in row for field in fields):
                    observed = pd.to_numeric(row[fields], errors="coerce")
                    if observed.gt(0).all() and not bool(row.get("source_observed_market_quote", False)):
                        invalid.append({"instrument": inst, "session": day,
                            "reason": "positive_volume_suspension_needs_independent_partial_session_evidence"})
                suspended.append({"instrument": inst, "session": day,
                    "source_response_sha256": row.source_response_sha256})
            elif not bool(row.source_complete_quote):
                invalid.append({"instrument": inst, "session": day, "reason": "incomplete_trading_quote"})
    return {"status": "passed" if not missing and not invalid else "blocked",
        "start": start, "end": end, "sessions": len(days), "instruments": len(instruments),
        "missing_listed_security_sessions": missing, "invalid_trading_sessions": invalid,
        "source_confirmed_suspensions": suspended,
        "historical_st_row_count": int(raw.loc[raw.date.between(start, end), "isST"].eq("1").sum())}


def audit_qlib_availability(dataset, raw, instruments, *, start, end):
    """A missing model bar must not silently stand in for a tradable source bar."""
    mismatches, classified = [], []
    for inst in instruments:
        stored = read_qlib_security(dataset, inst).loc[start:end]
        source = raw.loc[raw.instrument.eq(inst) & raw.date.between(start, end)].set_index("date")
        for day in source.index.intersection(stored.index):
            row = source.loc[day]
            prices = stored.loc[day, ["open", "high", "low", "close"]].to_numpy(dtype=float)
            finite = np.isfinite(prices) & (prices > 0)
            if row.tradestatus == "1" and bool(row.source_complete_quote) and not finite.all():
                mismatches.append({"instrument": inst, "session": day, "reason": "trading_source_missing_model_bar"})
            elif row.tradestatus == "0" and not finite.any():
                classified.append({"instrument": inst, "session": day, "basis": "source_confirmed_suspension"})
            elif finite.any() and not finite.all():
                mismatches.append({"instrument": inst, "session": day, "reason": "partial_model_price_bar"})
    return {"status": "passed" if not mismatches else "blocked", "mismatches": mismatches,
            "classified_missing_model_sessions": classified}


def audit_reference_prices(raw, events, calendar, *, start, end, rights=()):
    """Explain rights separately from dividends; never deliver unsubscribed shares.

    A separately observed exchange reference can disambiguate a supplier's
    published rights rounding. The real issuance ratio and theoretical result
    are retained, with the same CNY0.011 tolerance as the original A2 audit.
    """
    by_day, rights_by_day = {}, {}
    for event in events:
        by_day.setdefault((event["instrument"], event["ex_date"]), []).append(event)
    for event in rights:
        key = (event["instrument"], event["ex_date"])
        if key in rights_by_day:
            raise ValueError("Duplicate rights event cannot be silently combined")
        rights_by_day[key] = event
    records, failures, used_rights = [], [], set()
    for inst, group in raw.groupby("instrument"):
        source = group.set_index("date")
        previous = None
        for day in [value for value in calendar if str(group.date.min()) <= value <= end]:
            matching = by_day.get((inst, day), [])
            right = rights_by_day.get((inst, day))
            if right and matching:
                raise ValueError("Combined same-day rights and distributions need separately verified combined terms")
            if previous is not None:
                # Multiple same-date delivered entitlements remain distinct,
                # but their exchange adjustment uses the original share base.
                cash = sum((Decimal(event.get("reference_cash_per_share", event["cash_per_share"])) for event in matching), Decimal(0))
                bonus = sum((Decimal(event.get("reference_bonus_ratio", event["bonus_ratio"])) for event in matching), Decimal(0))
                previous = (previous - cash) / (1 + bonus)
                if right:
                    ratio = Decimal(right["actual_issued_ratio"])
                    subscription = Decimal(right["subscription_price"])
                    if not ratio.is_finite() or not subscription.is_finite() or ratio <= 0 or subscription <= 0:
                        raise ValueError("Rights formula requires evidenced actual positive terms")
                    previous = (previous + ratio * subscription) / (1 + ratio)
            if day not in source.index or not bool(source.loc[day].get("source_observed_market_quote", source.loc[day].source_complete_quote)):
                continue
            row = source.loc[day]
            actual = Decimal(row.preclose)
            if previous is not None and start <= day <= end:
                difference = abs(actual - money(previous))
                detail = {"instrument": inst, "session": day, "actual_preclose": str(actual),
                    "expected_reference": str(previous), "difference": str(difference),
                    "event_ids": [event["event_id"] for event in matching]}
                if right:
                    detail.update(rights_event_id=right["event_id"],
                        reference_basis="independent_published_reference_with_official_actual_issuance_terms",
                        actual_issuance_theoretical_rounded=str(money(previous)))
                    if (right.get("independent_reference_verified") is not True
                            or actual != Decimal(right["reference_preclose"])):
                        raise ValueError("Rights exchange reference lacks independent original quote verification")
                    used_rights.add((inst, day))
                if difference > Decimal("0.011"):
                    failures.append(detail)
                elif matching or right or difference:
                    records.append(detail)
            previous = Decimal(row.close)
    if used_rights != set(rights_by_day):
        raise ValueError("Every rights issue requires an actual observed audited ex-date reference")
    return {"status": "passed" if not failures else "blocked_unexplained_ex_reference",
        "checked_stocks": int(raw.instrument.nunique()), "matched_adjustments": records,
        "unexplained": failures, "price_tick_tolerance_cny": "0.011"}


def audit_stock_dividend_tax(events):
    """The engine consumes the effective cash rate, not the face-value note."""
    errors = []
    for row in events:
        if Decimal(str(row.get("stock_dividend_ratio", "0"))) <= 0:
            continue
        try:
            face = Decimal(str(row["taxable_stock_face_value_per_record_share"]))
            cash = Decimal(str(row["cash_per_share"]))
            rate = Decimal(str(row["cash_dividend_tax_rate"]))
            # Cross multiplication avoids rejecting an upward-rounded saved
            # 50-digit rate against a differently rounded 28-digit division.
            with localcontext() as context:
                context.prec = 128
                covers_tax = rate * cash >= Decimal("0.20") * (cash + face)
            if (not all(value.is_finite() for value in (face, cash, rate))
                    or face <= 0 or cash <= 0 or not Decimal(0) <= rate <= Decimal(1)
                    or not row.get("tax_evidence_sha256")
                    or not covers_tax):
                raise ValueError("Effective cash reserve does not cover evidenced stock dividend tax")
        except (KeyError, ValueError, ArithmeticError):
            errors.append(row["event_id"])
    return errors


def audit_capital_reserve_tax(events, evidence):
    """Share-premium or explicit issuer tax evidence is required for exemption."""
    entries = {(row["instrument"], row["ex_date"]): row for row in evidence.get("events", [])}
    missing = []
    for event in events:
        if Decimal(event.get("capital_reserve_ratio", "0")) <= 0:
            continue
        fact = entries.get((event["instrument"], event["ex_date"]))
        if not fact or fact.get("capital_reserve_tax_exempt") is not True or not fact.get("evidence"):
            missing.append(event["event_id"])
            continue
        proofs = fact["evidence"] if isinstance(fact["evidence"], list) else [fact["evidence"]]
        if any(not Path(proof["path"]).is_absolute() or sha(proof["path"]) != proof["sha256"] for proof in proofs):
            raise ValueError("Original capital-reserve exemption evidence bytes changed")
    return missing


def build_historical_market_audit(snapshot_path, execution_collection, dividends_collection,
        output_dir, *, action_overrides_path=None, rights_events_path=None,
        intraday_observations_path=None, capital_reserve_tax_evidence_path=None):
    """Write immutable source audits and delivered events, never portfolio results."""
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Preserve existing historical market audit")
    locked_paths = {"snapshot": Path(snapshot_path) / "manifest.json",
        "execution": Path(execution_collection) / "execution_collection.json",
        "dividends": Path(dividends_collection) / "collection.json"}
    if action_overrides_path:
        locked_paths["overrides"] = Path(action_overrides_path)
    if rights_events_path:
        locked_paths["rights"] = Path(rights_events_path)
    if intraday_observations_path:
        locked_paths["intraday_observations"] = Path(intraday_observations_path)
    if capital_reserve_tax_evidence_path:
        locked_paths["capital_reserve_tax_evidence"] = Path(capital_reserve_tax_evidence_path)
    locked_hashes = {key: sha(path) for key, path in locked_paths.items()}
    snapshot = load_qlib_snapshot(snapshot_path, verify=True)
    raw, basic, receipt = load_execution_collection(execution_collection)
    intraday = json.loads(Path(intraday_observations_path).read_text()) if intraday_observations_path else {"events": []}
    raw = apply_intraday_observation_evidence(raw, intraday)
    _, dividend_receipt = load_dividend_evidence(dividends_collection)
    calendar_path = snapshot.dataset_dir / "calendars/day.txt"
    intervals = snapshot.dataset_dir / "instruments/csi300.txt"
    calendar = calendar_path.read_text().splitlines()
    instruments = receipt["request"]["instruments"]
    if receipt["request"]["start_date"] != "2022-12-30" or receipt["request"]["end_date"] != "2024-12-31":
        raise ValueError("A4 needs the actual preceding close and complete two-year source window")
    if dividend_receipt["instruments"] != instruments or dividend_receipt["years"] != [2023, 2024]:
        raise ValueError("Dividend generation differs from historical execution selection")
    events = dividend_events(dividends_collection, start="2023-01-01", end="2024-12-31")
    if action_overrides_path is not None:
        events = apply_action_overrides(events, action_overrides_path)
    rights = json.loads(Path(rights_events_path).read_text())["events"] if rights_events_path else []
    coverage = audit_daily_coverage(raw, basic, calendar, instruments, start="2023-01-01", end="2024-12-31")
    availability = audit_qlib_availability(snapshot.dataset_dir, raw, instruments, start="2023-01-01", end="2024-12-31")
    prices = audit_raw_prices(snapshot.dataset_dir, raw, instruments, "2023-01-01", "2024-12-31")
    references = audit_reference_prices(raw, events, calendar, start="2023-01-01", end="2024-12-31", rights=rights)
    missing_stock_tax = audit_stock_dividend_tax(events)
    exemptions = json.loads(Path(capital_reserve_tax_evidence_path).read_text()) if capital_reserve_tax_evidence_path else {"events": []}
    missing_capital_tax = audit_capital_reserve_tax(events, exemptions)
    # Supplier objects and source frames were read earlier. Recheck original
    # responses as well as manifests so the receipt cannot bless later bytes.
    load_execution_collection(execution_collection)
    load_dividend_evidence(dividends_collection)
    load_qlib_snapshot(snapshot_path, verify=True)
    if any(sha(path) != locked_hashes[key] for key, path in locked_paths.items()):
        raise ValueError("Historical evidence changed while auditing; preserve a new generation")
    output.mkdir(parents=True)
    _write(output / "dividend_events.json", {"events": events,
        "source_collection_manifest_sha256": sha(Path(dividends_collection) / "collection.json")})
    for filename, payload in (("coverage.json", coverage), ("availability.json", availability),
                              ("raw_price_audit.json", prices), ("ex_reference_audit.json", references)):
        _write(output / filename, payload)
    bound_paths = {"execution_collection": Path(execution_collection) / "execution_collection.json",
        "dividend_events": output / "dividend_events.json", "calendar": calendar_path,
        "historical_intervals": intervals, "dividend_collection": Path(dividends_collection) / "collection.json",
        "snapshot_manifest": snapshot.path / "manifest.json", "implementation": Path(__file__)}
    for symbol, proof in receipt["responses"].items():
        original = Path(execution_collection) / proof["file"]
        if sha(original) != proof["sha256"]:
            raise ValueError("Original daily response changed after source audit")
        bound_paths["daily_original_" + symbol] = original
    for key, proof in dividend_receipt["responses"].items():
        original = Path(dividends_collection) / proof["file"]
        if sha(original) != proof["sha256"]:
            raise ValueError("Original dividend response changed after source audit")
        bound_paths["dividend_original_" + key] = original
    if action_overrides_path:
        bound_paths["action_overrides"] = Path(action_overrides_path)
        patches = json.loads(Path(action_overrides_path).read_text())["patches"]
        for number, patch in enumerate(patches):
            evidence = patch["evidence"]
            original = (Path(action_overrides_path).resolve().parent / evidence["path"]).resolve()
            if sha(original) != evidence["sha256"]:
                raise ValueError("Original issuer evidence changed")
            bound_paths["issuer_evidence_" + str(number)] = original
    if rights_events_path:
        bound_paths["rights_events"] = Path(rights_events_path)
        for number, right in enumerate(rights):
            proofs = right.get("reference_evidence", [])
            if len(proofs) < 2:
                raise ValueError("Rights reference needs independent original source evidence")
            for item, proof in enumerate(proofs):
                original = Path(proof["path"])
                if not original.is_absolute():
                    original = Path(rights_events_path).resolve().parent / original
                if sha(original) != proof["sha256"]:
                    raise ValueError("Original rights/reference source bytes changed")
                bound_paths[f"rights_evidence_{number}_{item}"] = original
    for key, source_path, payload in (("intraday_observations", intraday_observations_path, intraday),
                                     ("capital_reserve_tax_evidence", capital_reserve_tax_evidence_path, exemptions)):
        if not source_path:
            continue
        bound_paths[key] = Path(source_path)
        for number, fact in enumerate(payload["events"]):
            proofs = fact["evidence"] if isinstance(fact["evidence"], list) else [fact["evidence"]]
            for item, proof in enumerate(proofs):
                original = Path(proof["path"])
                if sha(original) != proof["sha256"]:
                    raise ValueError("Original issuer/partial-session evidence changed")
                bound_paths[f"{key}_original_{number}_{item}"] = original
    checks = {"raw_ohlc": bool(prices) and availability["status"] == "passed",
        "raw_preclose": references["status"] == "passed", "daily_trading_status": coverage["status"] == "passed",
        "historical_is_st": coverage["status"] == "passed", "ipo_identity": coverage["status"] == "passed",
        "calendar_coverage": coverage["status"] == "passed", "dividend_events": not missing_stock_tax and not missing_capital_tax,
        "ex_reference_explained": references["status"] == "passed", "source_vintage": True}
    audit = {"schema_version": 1, "kind": "a4_market_evidence_audit",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if all(checks.values()) else "blocked", "checks": checks,
        "audited_input_sha256": {key: sha(path) for key, path in bound_paths.items()},
        "bound_inputs": {key: {"path": str(path.resolve()), "sha256": sha(path)} for key, path in bound_paths.items()},
        "missing_stock_dividend_tax_evidence": missing_stock_tax,
        "missing_capital_reserve_tax_evidence": missing_capital_tax,
        "raw_rows": len(raw), "event_count": len(events), "unexplained_references": len(references["unexplained"]),
        "historical_membership_full_announcement_time_PIT": False,
        "source_snapshot_atomic_supplier_vintage_proven": False,
        "limitations": ["Historical status and memberships were reconstructed now, not frozen in 2023/2024.",
            "Source-confirmed suspended marks are carried accounting values, never executable new prices."]}
    _write(output / "market_audit.json", audit)
    _write(output / "manifest.json", {"files": {path.name: sha(path) for path in sorted(output.iterdir()) if path.is_file()},
        "status": audit["status"], "bound_inputs": audit["bound_inputs"]})
    return audit
