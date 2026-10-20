"""Source-bound full distribution tax costs for independently gated A5 research.

Cash covered by the existing dividend rate stays in the old entitlement ledger.
Only the overflow is a separate, irreversible research expense on the ex date.
It uses actual record-date holdings and existing cash, never newly delivered
shares, future dividend cash, a fabricated loan, or an assumed capital exemption.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP, localcontext
import hashlib
import json
from pathlib import Path
import re

import pandas as pd

from . import a5_capital_tax as capital


SPEC_PATH = Path(__file__).resolve().parents[2] / "docs/research/a5_distribution_tax_spec_v1.json"
PREFIX = "a5:additional_distribution_tax:"
CENT = Decimal(".01")
RATE = Decimal(".20")
EXEMPT = {"exempt_share_premium", "exempt_issuer_implemented_tax_schedule", "exempt_individually_evidenced"}
TERMS = ("instrument", "record_date", "ex_date", "cash_per_share", "stock_dividend_ratio", "capital_reserve_ratio")


class AdditionalTaxFundingError(ValueError):
    """The research path cannot fund a complete mandatory tax-cost reserve."""


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _day(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("An explicit ISO distribution date is required")
    return value


def _number(value, name):
    return capital._number(value, name)


def _bindings(value, out):
    """Bind every recursive path/SHA pair, including supporting PDF/page proofs."""
    if isinstance(value, dict):
        if "path" in value and "sha256" in value:
            path, digest = value["path"], value["sha256"]
            if (not isinstance(path, str) or not Path(path).is_absolute()
                    or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
                    or _sha(path) != digest):
                raise ValueError("Distribution-tax original evidence bytes changed")
            if path in out and out[path] != digest:
                raise ValueError("Conflicting source vintages at one distribution evidence path")
            out[path] = digest
        for child in value.values():
            _bindings(child, out)
    elif isinstance(value, list):
        for child in value:
            _bindings(child, out)


def _policy():
    spec = json.loads(SPEC_PATH.read_text())
    expected = {
        "kind": "a5_distribution_tax_reserve_spec", "version": 1,
        "base_tax_reserve_rate": "0.20", "effective_cash_rate_decimal_places": 50,
        "holding_period_tax_refund": False, "source_classification_unknown_is_exempt": False,
        "nominal_share_face_default": None, "existing_cash_rate_maximum": "1",
        "reserve_clock": "known_ex_date_before_orders_from_existing_spendable_cash",
        "quantity_basis": "finalized_actual_record_date_holdings_only",
        "additional_tax_rounding": "ceil_cent_of_exact_additional_and_at_least_ceil_cent_full_less_legacy_cash_tax",
        "legacy_cash_tax_rounding": "HALF_UP(quantity_times_actual_cash)_then_HALF_UP(gross_times_effective_cash_rate)",
        "cash_shortfall_policy": "invalidate_and_stop_no_borrowing_no_tax_clipping",
        "replay_policy": "once_per_event_exact_terms_and_record_snapshot_digest",
        "payment_or_listing_day_extra_tax": False,
        "research_conservative_expense_not_actual_broker_tax_clock": True,
        "noncash_delivery_terms_changed": False,
        "source_scope": "new_independently_gated_stability_research_only_no_primary_rewrite",
    }
    if spec != expected:
        raise ValueError("Distribution-tax policy differs from the supported frozen specification")
    return spec


def _match(fact, row):
    for key in TERMS:
        if key in {"instrument", "record_date", "ex_date"}:
            if fact.get(key) != row.get(key):
                raise ValueError("Distribution tax evidence differs from actual " + key)
        elif _number(fact.get(key), key) != _number(row.get(key), key):
            raise ValueError("Distribution tax evidence differs from delivered " + key)


def _index(evidence):
    if not isinstance(evidence, dict) or not isinstance(evidence.get("events", []), list):
        raise ValueError("Distribution evidence must contain an events list")
    result = {}
    for fact in evidence.get("events", []):
        key = (fact["instrument"], fact["ex_date"])
        if key in result:
            raise ValueError("Ambiguous duplicate distribution tax fact")
        capital._proofs(fact)
        result[key] = fact
    return result


def _resolved(row, capital_fact, stock_fact):
    """Revalidate both named and deliberately unresolved source classifications."""
    cash = _number(row.get("cash_per_share"), "cash_per_share")
    stock = _number(row.get("stock_dividend_ratio"), "stock_dividend_ratio")
    ratio = _number(row.get("capital_reserve_ratio"), "capital_reserve_ratio")
    existing = _number(row.get("cash_dividend_tax_rate", ".20"), "cash_dividend_tax_rate")
    if existing > 1:
        raise ValueError("Cash-covered tax rate cannot exceed one")
    classification, capital_face, stock_face = None, Decimal(0), Decimal(0)
    if ratio:
        if capital_fact is None:
            raise ValueError("Missing individual capital source/face evidence")
        capital._proofs(capital_fact)
        _match(capital_fact, row)
        classification = capital._classification(capital_fact)
        if classification not in EXEMPT:
            capital_face = capital._capital_face(capital_fact, classification, ratio)
    elif capital_fact is not None:
        raise ValueError("A zero capital transfer cannot consume a nonzero capital fact")
    if stock:
        if stock_fact is None:
            raise ValueError("Missing independently evidenced stock nominal face")
        capital._proofs(stock_fact)
        _match(stock_fact, row)
        if (stock_fact.get("status") not in {"verified", "verified_stock_tax_base_pending_separate_liability_accounting",
                "verified_source_requires_tax_accounting"}
                or stock_fact.get("issuer_actual_terms_verified") is not True
                or stock_fact.get("review_required", False) is not False):
            raise ValueError("Stock tax face is not independently verified")
        stock_face = _number(stock_fact.get("taxable_stock_face_value_per_record_share"), "taxable stock face")
        nominal = _number(stock_fact.get("share_face_value_cny"), "stock nominal face")
        if stock_face <= 0 or nominal <= 0 or stock_face != stock * nominal:
            raise ValueError("Stock face differs from its actual ratio and proven nominal value")
    elif stock_fact is not None:
        raise ValueError("A zero stock dividend cannot consume a positive stock face")
    full = max(RATE * (cash + capital_face + stock_face), cash * existing)
    cash_covered = min(cash, full)
    rate = existing
    if cash and cash * rate < cash_covered:
        rate = (cash_covered / cash).quantize(Decimal("1e-50"), rounding=ROUND_CEILING)
    if not 0 <= rate <= 1:
        raise ValueError("Cash-covered tax rate cannot cover the evidenced cost")
    additional = max(Decimal(0), full - cash * rate)
    return {"classification": classification,
            "capital_face_value_per_record_share": str(capital_face),
            "stock_face_value_per_record_share": str(stock_face),
            "full_tax_reserve_per_record_share": str(full),
            "cash_tax_reserve_per_record_share": str(cash * rate),
            "additional_tax_reserve_per_record_share": str(additional),
            "cash_dividend_tax_rate": str(rate)}


def prepare_distribution_tax(events, capital_evidence, stock_evidence):
    """Return copied annotated events and a strict source-bound reserve report.

    Evidence inputs contain ``events`` lists with reviewed original PDF proofs,
    explicit actual C/S/R/record/ex terms, and individual source/nominal facts.
    Unresolved capital classification remains disclosed even when its entire
    proven nominal face is reserved. Unsupported events are pending, never
    silently treated as exempt. Changed original bytes always raise.
    """
    spec = _policy()
    bindings = {str(path.resolve()): _sha(path) for path in (
        Path(__file__), SPEC_PATH, Path(capital.__file__), capital.SPEC_PATH)}
    implementation_bindings = bindings.copy()
    _bindings(capital_evidence, bindings)
    _bindings(stock_evidence, bindings)
    capitals, stocks = _index(capital_evidence), _index(stock_evidence)
    rows, contracts, pending, applied, unresolved = [], [], [], [], []
    seen, ids = set(), set()
    with localcontext() as context:
        context.prec = 128
        for original in events:
            row = deepcopy(original)
            key = (row["instrument"], row["ex_date"])
            if key in seen or row["event_id"] in ids:
                raise ValueError("Ambiguous duplicate distribution event identity")
            seen.add(key)
            ids.add(row["event_id"])
            label = {k: row[k] for k in ("event_id", "instrument", "ex_date")}
            try:
                stock = _number(row.get("stock_dividend_ratio"), "stock_dividend_ratio")
                ratio = _number(row.get("capital_reserve_ratio"), "capital_reserve_ratio")
                if not stock and not ratio:
                    rows.append(row)
                    continue
                record, ex = _day(row["record_date"]), _day(row["ex_date"])
                if not record < ex:
                    raise ValueError("Distribution record date must precede ex date")
                known = pd.Timestamp(row["known_at"])
                if pd.isna(known) or known.tzinfo is None:
                    raise ValueError("Distribution known_at must be timezone aware")
                if known > pd.Timestamp(ex + "T09:30:00+08:00"):
                    raise ValueError("Distribution terms were not known before ex-date execution")
                if not re.fullmatch(r"[a-f0-9]{64}", row.get("source_response_sha256", "")):
                    raise ValueError("Actual distribution source SHA is required")
                cf = capitals.get(key) if ratio else None
                sf = stocks.get(key) if stock else None
                details = _resolved(row, cf, sf)
                # Rates already sufficient are preserved byte-for-byte.
                if Decimal(details["cash_dividend_tax_rate"]) != _number(row.get("cash_dividend_tax_rate", ".20"), "rate"):
                    row["cash_dividend_tax_rate"] = details["cash_dividend_tax_rate"]
                if details["classification"] == "unresolved_conservative_upper_bound":
                    unresolved.append({**label, "reason": cf.get("basis", "Capital source classification unresolved")})
                applied.append({**label, **details})
                if Decimal(details["additional_tax_reserve_per_record_share"]) > 0:
                    local_bindings = implementation_bindings.copy()
                    _bindings(cf, local_bindings)
                    _bindings(sf, local_bindings)
                    payload = {"event_id": row["event_id"],
                        "source_event": {k: row[k] for k in (*TERMS, "known_at", "source_response_sha256")},
                        **{k: row[k] for k in ("instrument", "record_date", "ex_date", "known_at")},
                        **details, "capital_fact": deepcopy(cf), "stock_fact": deepcopy(sf),
                        "tax_policy": {"path": str(SPEC_PATH), "sha256": bindings[str(SPEC_PATH.resolve())]},
                        "bound_inputs": [{"path": p, "sha256": s} for p, s in sorted(local_bindings.items())]}
                    payload["source_event"]["cash_dividend_tax_rate"] = details["cash_dividend_tax_rate"]
                    payload["terms_sha256"] = _digest(payload)
                    contracts.append(payload)
            except (ValueError, KeyError, ArithmeticError) as exc:
                pending.append({**label, "reason": str(exc)})
                row = deepcopy(original)
            rows.append(row)
    for path, digest in bindings.items():
        if _sha(path) != digest:
            raise ValueError("Distribution-tax source changed during validation")
    return rows, {"kind": "a5_distribution_tax_accounting", "status": "blocked" if pending else "passed",
        "spec": spec, "pending": pending, "applied": applied, "contracts": contracts,
        "tax_classification_unresolved": unresolved, "all_source_classifications_verified": not pending and not unresolved,
        "bound_inputs": [{"path": p, "sha256": s} for p, s in sorted(bindings.items())],
        "checks": {"source_unchanged": True, "all_noncash_events_covered": not pending,
                   "no_delivered_entitlements_changed": True, "full_overflow_explicit": not pending},
        "research_conservative_expense_not_actual_broker_tax_clock": True}


def _validate_contract(contract):
    payload = {k: v for k, v in contract.items() if k != "terms_sha256"}
    if _digest(payload) != contract.get("terms_sha256"):
        raise ValueError("Distribution tax reserve contract terms changed")
    if contract.get("tax_policy") != {"path": str(SPEC_PATH), "sha256": _sha(SPEC_PATH)}:
        raise ValueError("Distribution tax reserve policy binding differs")
    bindings = {}
    _bindings(contract, bindings)
    row = contract["source_event"]
    for key in ("instrument", "record_date", "ex_date", "known_at"):
        if row.get(key) != contract.get(key):
            raise ValueError("Distribution tax contract identity/clock differs")
    with localcontext() as context:
        context.prec = 128
        resolved = _resolved(row, contract["capital_fact"], contract["stock_fact"])
    for key, value in resolved.items():
        if key == "classification":
            if contract.get(key) != value:
                raise ValueError("Distribution tax contract source classification differs")
        elif _number(contract.get(key), key) != Decimal(value):
            raise ValueError("Distribution tax contract reserve arithmetic differs")
    return row


def reserve_additional_distribution_tax(state, day, contracts, *, now=None):
    """Return ``(copied_state, journal)``; failures never mutate the input.

    Call once before the ordinary ex-date event hook and before any orders.
    A report's ``contracts`` list (or an exported ``{'contracts': list}``) is
    accepted. Actual broker withholding timing is intentionally not simulated.
    """
    _policy()
    day = _day(day)
    contracts = contracts.get("contracts", []) if isinstance(contracts, dict) else contracts
    if not isinstance(contracts, list):
        raise ValueError("Explicit distribution tax contracts list required")
    result, journal, seen = deepcopy(state), [], set()
    if result.session is not None and result.session > day:
        raise ValueError("Cannot reserve distribution tax before current state session")
    with localcontext() as context:
        context.prec = 128
        for contract in contracts:
            identity = PREFIX + contract["event_id"]
            if identity in seen:
                raise ValueError("Duplicate distribution tax reserve contract")
            seen.add(identity)
            prior = result.applied_actions.get(identity)
            if contract["ex_date"] != day and prior is None:
                continue
            row = _validate_contract(contract)
            record = _day(contract["record_date"])
            if record not in result.holdings_history:
                raise ValueError("Missing finalized actual record-date holdings for additional tax")
            quantity = result.holdings_history[record].get(contract["instrument"], 0)
            if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0:
                raise ValueError("Actual record-date holdings must be nonnegative integer shares")
            payload = {"terms_sha256": contract["terms_sha256"], "record_date_shares": quantity}
            digest = _digest(payload)
            if prior is not None:
                if prior.get("sha256") != digest:
                    raise ValueError("Already reserved distribution tax has changed terms or record holdings")
                continue
            cutoff = pd.Timestamp(now) if now is not None else pd.Timestamp(day + "T09:30:00+08:00")
            known = pd.Timestamp(contract["known_at"])
            if (pd.isna(known) or known.tzinfo is None or cutoff.tzinfo is None
                    or cutoff.date().isoformat() != day or known > cutoff):
                raise ValueError("Additional distribution tax terms were not known at execution")
            if day in result.holdings_history or any(p.bought_today and p.bought_date == day for p in result.positions.values()):
                raise ValueError("Additional tax must be reserved before finalized holdings and same-day buys")
            if not record < day:
                raise ValueError("Actual record snapshot must precede ex-date tax reserve")
            full = _number(contract["full_tax_reserve_per_record_share"], "full tax")
            extra = _number(contract["additional_tax_reserve_per_record_share"], "additional tax")
            if extra <= 0:
                raise ValueError("An additional tax contract must reserve a positive overflow")
            gross = (quantity * _number(row["cash_per_share"], "cash")).quantize(CENT, rounding=ROUND_HALF_UP)
            cash_tax = (gross * _number(row["cash_dividend_tax_rate"], "cash rate")).quantize(CENT, rounding=ROUND_HALF_UP)
            base_extra = (quantity * extra).quantize(CENT, rounding=ROUND_CEILING)
            total_floor = (quantity * full).quantize(CENT, rounding=ROUND_CEILING)
            reserved = max(base_extra, total_floor - cash_tax)
            cash_before = _number(result.cash, "portfolio cash")
            if reserved > cash_before:
                raise AdditionalTaxFundingError("Insufficient existing cash for complete additional distribution tax: "
                    + contract["event_id"] + " needs " + str(reserved) + ", available " + str(cash_before))
            result.cash = cash_before - reserved
            entry = {"kind": "additional_distribution_tax_reserve", "event_id": contract["event_id"],
                "instrument": contract["instrument"], "record_date": record, "ex_date": day,
                "session": day, "record_date_shares": quantity, "additional_tax_reserved_cny": str(reserved),
                "legacy_cash_tax_reserved_cny": str(cash_tax), "full_tax_reserve_floor_cny": str(total_floor),
                "rounding_coverage_topup_cny": str(reserved - base_extra), "cash_change": str(-reserved),
                "cash_before": str(cash_before), "cash_after": str(result.cash),
                "terms_sha256": contract["terms_sha256"], "tax_policy_sha256": contract["tax_policy"]["sha256"],
                "source_response_sha256": row["source_response_sha256"],
                "research_conservative_expense_not_actual_broker_tax_clock": True}
            result.applied_actions[identity] = {"sha256": digest, "record": entry}
            journal.append(entry)
    return result, journal
