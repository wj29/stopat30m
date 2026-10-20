"""Individually evidenced capital-transfer tax reserves for A5 research only.

The existing ledger consumes an effective rate on the cash dividend. This
adapter reserves the evidenced noncash tax from that cash, without changing
delivered shares or pretending that unresolved reserve sources are classified.
The fixed policy deliberately does not refund a long-holder tax discount.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, InvalidOperation, ROUND_CEILING, localcontext
import hashlib
import json
from pathlib import Path
import re


SPEC_PATH = Path(__file__).resolve().parents[2] / "docs/research/a5_capital_tax_accounting_spec_v3.json"
CLASSIFICATIONS = {"exempt_share_premium", "exempt_issuer_implemented_tax_schedule", "exempt_individually_evidenced", "taxable_other_capital_reserve",
                   "unresolved_conservative_upper_bound"}
ECONOMIC_FIELDS = ("cash_per_share", "stock_dividend_ratio", "capital_reserve_ratio")


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _number(value, name):
    try:
        if isinstance(value, bool):
            raise ValueError(name + " must be numeric")
        result = Decimal(str(value))
        if not result.is_finite() or result < 0:
            raise ValueError(name + " must be finite and nonnegative")
        return result
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(name + " must be explicit and numeric") from exc


def _proofs(fact):
    sources = fact.get("evidence", [])
    sources = sources if isinstance(sources, list) else [sources]
    if not sources:
        raise ValueError("Capital tax fact needs original evidence")
    primary = None
    for source in sources:
        path = Path(source["path"])
        if (not path.is_absolute() or not re.fullmatch(r"[0-9a-f]{64}", source.get("sha256", ""))
                or _sha(path) != source["sha256"]):
            raise ValueError("Original A5 capital tax evidence bytes changed")
        pages = source.get("pdf_pages_1based", fact.get("pdf_pages_1based", fact.get("pdf_page_1based")))
        pages = [pages] if isinstance(pages, int) else pages
        if (path.suffix.lower() == ".pdf" and source.get("url", "").startswith("https://")
                and isinstance(pages, list) and pages and all(type(p) is int and p > 0 for p in pages)):
            primary = primary or source
    if primary is None or fact.get("original_pdf_visual_reviewed") is not True:
        raise ValueError("A reviewed original issuer PDF and page reference are required")
    return sources, primary


def _classification(fact):
    value = fact.get("capital_reserve_tax_classification")
    if value is None:
        if fact.get("capital_reserve_tax_exempt") is True and fact.get("capital_reserve_taxable") is not True:
            if fact.get("basis_kind") == "issuer_complete_personal_short_holding_tax_schedule_arithmetic":
                value = "exempt_issuer_implemented_tax_schedule"
            elif "capital_premium_debit_for_transfer_cny" in fact:
                value = "exempt_share_premium"
            else:
                # Older individually reviewed statements and schedules do not
                # share one fact schema. Preserve their reviewed basis without
                # relabeling them as proof of a named reserve subaccount.
                value = "exempt_individually_evidenced"
        elif fact.get("capital_reserve_taxable") is True and fact.get("capital_reserve_tax_exempt") is False:
            value = "taxable_other_capital_reserve"
    if value not in CLASSIFICATIONS:
        raise ValueError("Capital source classification is not explicitly supported")
    if value in {"exempt_share_premium", "exempt_issuer_implemented_tax_schedule", "exempt_individually_evidenced"}:
        valid = (fact.get("capital_reserve_tax_exempt") is True
                 and fact.get("capital_reserve_taxable") is not True and fact.get("status") == "verified")
        if value == "exempt_issuer_implemented_tax_schedule":
            cash = _number(fact.get("cash_per_share"), "cash_per_share")
            quoted = _number(fact.get("issuer_short_holding_tax_per10"), "issuer_short_holding_tax_per10")
            # The issuer prints its total one-month personal tax at six places
            # per ten shares. A half-unit allowance covers display rounding,
            # not an unproved reserve exemption or a change to cash tax.
            valid = (valid and cash > 0
                     and _number(fact.get("stock_dividend_ratio"), "stock_dividend_ratio") == 0
                     and fact.get("cash_tax_exemption_inferred") is False
                     and fact.get("capital_exemption_inferred_from_issuer_total_tax_schedule") is True
                     and fact.get("underlying_reserve_subaccount_independently_classified") is False
                     and fact.get("basis_kind") == "issuer_complete_personal_short_holding_tax_schedule_arithmetic"
                     and abs(quoted - cash * 10 * Decimal("0.20")) <= Decimal("0.0000005"))
    elif value == "taxable_other_capital_reserve":
        valid = (fact.get("capital_reserve_taxable") is True and fact.get("capital_reserve_tax_exempt") is False
                 and fact.get("status") in {"verified", "verified_source_requires_tax_accounting"}
                 and fact.get("classification_verified") is not False)
    else:
        valid = (fact.get("classification_verified") is False
                 and fact.get("conservative_tax_reserve_upper_bound") is True
                 and fact.get("capital_reserve_tax_exempt") is False
                 and fact.get("capital_reserve_taxable") is False
                 and fact.get("status") == "verified_face_value_source_classification_unresolved")
    if not valid or fact.get("review_required") is not False:
        raise ValueError("Capital fact verification/classification flags disagree")
    return value


def _capital_face(fact, classification, ratio):
    key = ("taxable_capital_face_value_per_record_share" if classification == "taxable_other_capital_reserve"
           else "capital_reserve_face_value_per_record_share")
    face = _number(fact.get(key), key)
    nominal = _number(fact.get("share_face_value_cny"), "share_face_value_cny")
    if face <= 0 or nominal <= 0 or face != ratio * nominal:
        raise ValueError("Capital face does not match the evidenced ratio and share face value")
    # Full taxable-other-capital proofs must identify the actual source debit.
    # Unresolved sources are deliberately not made to claim a particular debit.
    if classification == "taxable_other_capital_reserve":
        if fact.get("source_account") != "other_capital_reserve":
            raise ValueError("Taxable other-capital source account is not evidenced")
        record = _number(fact.get("record_shares"), "record_shares")
        transferred = _number(fact.get("transferred_shares"), "transferred_shares")
        debit = _number(fact.get("other_capital_reserve_debit_for_transfer_cny"), "capital debit")
        if (record <= 0 or transferred <= 0 or record != record.to_integral_value()
                or transferred != transferred.to_integral_value()
                or debit != transferred * nominal or abs(record * ratio - transferred) >= 1):
            raise ValueError("Taxable capital debit, transfer and record-share reconciliation differs")
        if record * ratio != transferred and fact.get("per_record_share_entitlement_rounding_disclosed") is not True:
            raise ValueError("Aggregate transfer rounding must be explicitly disclosed")
    return face


def apply_a5_capital_tax(events, evidence):
    """Return annotated copies and a source-bound accounting report.

    ``evidence`` is the ``{'events': [...]}`` document returned by the A5 source
    loader. Missing/unsupported facts yield explicit pending rows and leave the
    affected event untouched. A changed original checksum always raises. All
    accounting coverage may pass while separately disclosed source
    classifications remain unresolved under the fixed cost upper bound.
    """
    spec = json.loads(SPEC_PATH.read_text())
    if (spec.get("kind") != "a5_capital_tax_accounting_spec" or spec.get("version") != 3
            or spec.get("base_tax_reserve_rate") != "0.20" or spec.get("rate_decimal_places") != 50
            or spec.get("holding_period_tax_refund") is not False):
        raise ValueError("A5 capital tax policy differs from the fixed supported specification")
    bindings = {"spec": {"path": str(SPEC_PATH), "sha256": _sha(SPEC_PATH)},
                "implementation": {"path": str(Path(__file__).resolve()), "sha256": _sha(__file__)}}
    entries = {}
    for number, fact in enumerate(evidence.get("events", [])):
        key = (fact["instrument"], fact["ex_date"])
        if key in entries:
            raise ValueError("Ambiguous duplicate A5 capital tax fact")
        originals, primary = _proofs(fact)
        entries[key] = (fact, primary)
        for i, proof in enumerate(originals):
            bindings[f"fact_{number}_source_{i}"] = {"path": proof["path"], "sha256": proof["sha256"]}
    result, pending, applied, covered, unresolved, patches, schedule_inferences, source_inferences = [], [], [], [], [], [], [], []
    identities = set()
    for original in events:
        row = deepcopy(original)
        identity = (row["instrument"], row["ex_date"])
        if identity in identities:
            raise ValueError("Ambiguous duplicate delivered corporate action")
        identities.add(identity)
        ratio = _number(row.get("capital_reserve_ratio", "0"), "capital_reserve_ratio")
        if ratio == 0:
            result.append(row)
            continue
        label = {"event_id": row["event_id"], "instrument": row["instrument"], "ex_date": row["ex_date"]}
        try:
            if identity not in entries:
                raise ValueError("Missing individual capital source/nominal face evidence")
            fact, primary = entries[identity]
            for field in ECONOMIC_FIELDS:
                if _number(fact.get(field), field) != _number(row.get(field), field):
                    raise ValueError("Capital tax fact differs from delivered " + field)
            classification = _classification(fact)
            if classification in {"exempt_share_premium", "exempt_issuer_implemented_tax_schedule", "exempt_individually_evidenced"}:
                if classification == "exempt_issuer_implemented_tax_schedule":
                    schedule_inferences.append({**label, "reserve_subaccount_identified": False,
                        "issuer_short_holding_tax_per10": fact["issuer_short_holding_tax_per10"],
                        "basis": fact.get("basis", "Individually reviewed issuer total tax schedule")})
                detail = {**label, "classification": classification, "annotation_applied": False,
                          "capital_tax_reserved_per_record_share": "0", "basis_kind": fact.get("basis_kind"),
                          "basis": fact.get("basis"), "inference_disclosed": fact.get("inference_disclosed"),
                          "accounting_reconciliation_inference": fact.get("accounting_reconciliation_inference") is True,
                          "reserve_subaccount_identified": classification == "exempt_share_premium"}
                if classification == "exempt_individually_evidenced" or detail["accounting_reconciliation_inference"]:
                    source_inferences.append(detail.copy())
                covered.append(row["event_id"])
                applied.append(detail)
                result.append(row)
                continue
            with localcontext() as context:
                context.prec = 128
                cash = _number(row["cash_per_share"], "cash_per_share")
                capital_face = _capital_face(fact, classification, ratio)
                stock_ratio = _number(row["stock_dividend_ratio"], "stock_dividend_ratio")
                stock_face = Decimal(0)
                if stock_ratio:
                    stock_face = _number(fact.get("taxable_stock_face_value_per_record_share"), "stock face")
                    event_face = _number(row.get("taxable_stock_face_value_per_record_share"), "event stock face")
                    stock_proof = row.get("tax_evidence_sha256")
                    sources = fact["evidence"] if isinstance(fact["evidence"], list) else [fact["evidence"]]
                    if (stock_face <= 0 or stock_face != event_face
                            or stock_proof not in {source["sha256"] for source in sources}):
                        raise ValueError("Stock dividend face must match its separately evidenced annotation")
                elif (_number(fact.get("taxable_stock_face_value_per_record_share", "0"), "stock face")
                      or _number(row.get("taxable_stock_face_value_per_record_share", "0"), "event stock face")):
                    raise ValueError("A zero stock dividend cannot have nonzero stock face")
                required = Decimal("0.20") * (cash + stock_face + capital_face)
                if cash <= 0 or required > cash:
                    raise ValueError("Cash dividend cannot cover the complete conservative tax reserve")
                computed = (required / cash).quantize(Decimal("1e-50"), rounding=ROUND_CEILING)
                existing = _number(row.get("cash_dividend_tax_rate", "0.20"), "existing cash rate")
                if existing > 1:
                    raise ValueError("Existing cash tax rate exceeds cash")
                rate = max(computed, existing)
                if rate > 1 or rate * cash < required:
                    raise ValueError("Rounded effective cash rate cannot cover tax")
                values = {"cash_dividend_tax_rate": str(rate)}
                patch = {**{key: row[key] for key in ("instrument", "ex_date")},
                         "operation": "annotate", "values": values,
                         "evidence": {key: primary[key] for key in ("path", "sha256", "url")}}
                row.update(values, tax_evidence_sha256=primary["sha256"], override_evidence=patch["evidence"])
                patches.append(patch)
                detail = {**label, "classification": classification, "annotation_applied": True,
                    "classification_verified": classification != "unresolved_conservative_upper_bound",
                    "capital_face_value_per_record_share": str(capital_face),
                    "stock_face_value_per_record_share": str(stock_face), "cash_per_record_share": str(cash),
                    "required_tax_per_record_share": str(required), "cash_dividend_tax_rate": str(rate),
                    "reserved_tax_per_record_share": str(rate * cash), "tax_evidence_sha256": primary["sha256"]}
                applied.append(detail)
                if classification == "unresolved_conservative_upper_bound":
                    unresolved.append({**label, "reason": fact.get("basis", "Capital source classification unresolved"),
                                       "capital_face_value_per_record_share": str(capital_face)})
                covered.append(row["event_id"])
        except (ValueError, KeyError, ArithmeticError) as exc:
            pending.append({**label, "reason": str(exc)})
            row = deepcopy(original)
        result.append(row)
    # Final byte verification precedes the caller publishing annotated events.
    for proof in bindings.values():
        if _sha(proof["path"]) != proof["sha256"]:
            raise ValueError("A5 capital tax source changed during accounting validation")
    return result, {"kind": "a5_capital_tax_accounting", "status": "passed" if not pending else "blocked",
        "spec": spec, "bound_inputs": bindings, "pending": pending, "applied": applied,
        "checks": {"original_evidence_unchanged": True, "all_capital_events_covered": not pending,
                   "no_economic_terms_changed": True, "no_unsupported_tax_cash_shortfall": not pending},
        "covered_event_ids": covered, "tax_classification_unresolved": unresolved,
        "capital_exemptions_inferred_from_issuer_tax_schedule": schedule_inferences,
        "capital_exemption_source_inferences": source_inferences,
        "all_reserve_subaccounts_identified": not schedule_inferences and not unresolved
            and not any(item["classification"] == "exempt_individually_evidenced" for item in applied),
        "all_source_classifications_verified": not pending and not unresolved,
        "annotate_overrides": {"patches": patches}, "economic_terms_changed": False,
        "research_cost_upper_bound_not_broker_withholding_simulation": True}
