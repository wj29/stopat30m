"""Independent 2025/2026 source gate; frozen primary modules stay unchanged.

The only non-identical provider duplicate collapse is an issuer-verified exact
original-response/row-index/full-row resolution. The old raw payloads remain
unchanged. This secondary gate does not compute returns or select a model.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, localcontext
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from .a5_market_audit import (
    A5MarketEvidence, _day, _proof, _save, _scope_batch, sha,
    _capture_runtime_proofs, _assert_bound_inputs_unchanged,
    audit_original_dividend_request, audit_dividend_source_correction,
    merge_execution_frames, merge_dividend_events, load_event_evidence,
    audit_member_identity, audit_raw_qlib_bridge, audit_official_membership_evidence,
)
from .a5_adjusted_prices import DEFAULT_SPEC, audit_temporal_adjustments, load_partial_session_model_policy
from .a5_source_checks import audit_daily_coverage, audit_qlib_availability, audit_reference_prices
from .ashare_actions import SOURCE, _amount, apply_action_overrides, load_dividend_evidence
from .ashare_execution_data import load_execution_collection
from .ashare_history_audit import apply_intraday_observation_evidence
from .ashare_scope import load_verified_historical_scope
from .qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot
from .qlib_incremental import read_qlib_security

LEGACY_ARCHIVE_MANIFEST_SHA256 = frozenset({
    "54d247f03815cb7f6d2b417f73ccda658288c77f87e66602550d4fec6eac55b9",
    "d2c94f9d3118796e89cbb95b933a37bd094294f2b535228c39739cc2209123be",
})


@dataclass
class A5StabilityMarketEvidence(A5MarketEvidence):
    normalization_receipts: list = field(default_factory=list)


def _canonical_row_sha(row):
    return hashlib.sha256(json.dumps(row, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def _bind_proof_tree(tree, proofs, prefix):
    if isinstance(tree, dict):
        if "path" in tree and "sha256" in tree:
            path = Path(tree["path"])
            if not path.is_absolute() or _proof(path)["sha256"] != tree["sha256"]:
                raise ValueError("Secondary original/visual evidence changed or is not absolute")
            if path.suffix.lower() == ".pdf" and not tree.get("url", "").startswith("https://"):
                raise ValueError("Secondary issuer PDF needs its original HTTPS identity")
            proofs[prefix] = _proof(path)
        for key, value in tree.items():
            _bind_proof_tree(value, proofs, prefix + ":" + str(key))
    elif isinstance(tree, list):
        for n, value in enumerate(tree):
            _bind_proof_tree(value, proofs, prefix + ":" + str(n))


def _resolution_facts(paths):
    facts, proofs = {}, {}
    for n, source in enumerate(paths):
        path = Path(source).resolve()
        document = json.loads(path.read_bytes())
        proofs[f"duplicate_resolution_{n}"] = _proof(path)
        if (document.get("kind") != "a5_issuer_verified_duplicate_distribution_resolution"
                or document.get("status") != "source_resolution_verified_not_full_market_gate"):
            raise ValueError("Unverified or wrong duplicate-distribution resolution kind")
        _bind_proof_tree(document, proofs, f"duplicate_resolution_{n}:original")
        for fact in document["events"]:
            if (fact.get("status") != "verified_single_issuer_distribution_duplicate_provider_annotations"
                    or fact.get("original_pdf_visual_reviewed") is not True
                    or fact.get("execution_entitlement_issuer_verified") is not True
                    or fact.get("collapse_exact_rows_only") is not True
                    or fact.get("economic_fields_are_equal_across_original_duplicates") is not True
                    or fact.get("economic_events_after_resolution") != 1):
                raise ValueError("Duplicate resolution lacks exact issuer/economic/visual verification")
            if not any(Path(p["path"]).suffix.lower() == ".pdf" for p in fact["evidence"]):
                raise ValueError("Duplicate resolution lacks the original issuer implementation")
            if not any(Path(p["path"]).suffix.lower() == ".png" for p in fact["evidence"]):
                raise ValueError("Duplicate resolution lacks a verified rendered page")
            key = (str(Path(fact["original_response"]["path"]).resolve()),
                   fact["original_response"]["sha256"], fact["instrument"], fact["ex_date"])
            if key in facts:
                raise ValueError("Duplicate or conflicting exact-source resolution documents")
            facts[key] = {**fact, "resolution_document": _proof(path)}
    return facts, proofs


def _economic_source_key(row):
    return (row["code"], row["dividRegistDate"], row["dividOperateDate"],
            row["dividPayDate"], row["dividStockMarketDate"],
            *[_amount(row[name]) for name in
              ("dividCashPsBeforeTax", "dividStocksPs", "dividReserveToStockPs")])


def _collapse_exact_resolutions(path, payload, instrument, facts):
    rows = [dict(zip(payload["fields"], row)) for row in payload["rows"]]
    removed, receipts = set(), []
    digest = sha(path)
    for (original_path, original_sha, symbol, ex_date), fact in facts.items():
        if original_path != str(path.resolve()):
            continue
        if original_sha != digest or symbol != instrument:
            raise ValueError("Registered duplicate source bytes/security changed")
        declarations = fact["original_duplicate_rows"]
        indices = [r["row_index_zero_based"] for r in declarations]
        if (len(indices) < 2 or len(indices) != len(set(indices))
                or any(type(i) is not int or not 0 <= i < len(rows) for i in indices)
                or set(indices) & removed):
            raise ValueError("Duplicate resolution row indices are invalid or overlap")
        for original in declarations:
            observed = rows[original["row_index_zero_based"]]
            if (observed != original["row"]
                    or _canonical_row_sha(observed) != original["canonical_row_json_sha256"]):
                raise ValueError("Exact duplicate full row/index/checksum differs")
        canonical_index = fact["canonical_source_row_index_zero_based"]
        if canonical_index not in indices or rows[canonical_index] != fact["canonical_source_row"]:
            raise ValueError("Canonical duplicate resolution is not an exact original source row")
        canonical = rows[canonical_index]
        key = _economic_source_key(canonical)
        if (canonical["dividOperateDate"] != ex_date
                or canonical["code"].replace(".", "").upper() != symbol
                or any(_economic_source_key(rows[i]) != key for i in indices)
                or {i for i, row in enumerate(rows) if _economic_source_key(row) == key} != set(indices)):
            raise ValueError("Resolution would collapse different or unregistered economic entitlements")
        if (canonical["dividRegistDate"] != fact["record_date"]
                or canonical["dividPayDate"] != fact["pay_date"]
                or _amount(canonical["dividCashPsBeforeTax"]) != Decimal(fact["cash_per_share"])
                or _amount(canonical["dividStocksPs"]) != Decimal(fact["stock_dividend_ratio"])
                or _amount(canonical["dividReserveToStockPs"]) != Decimal(fact["capital_reserve_ratio"])):
            raise ValueError("Issuer verified event terms differ from the exact selected source row")
        allowed_annotations = {"dividCashPsAfterTax", "dividCashStock"}
        if set(fact["ignored_conflicting_annotations"]) - allowed_annotations:
            raise ValueError("Resolution may not ignore unverified economic/source fields")
        for name in canonical:
            values = {rows[i][name] for i in indices}
            if len(values) <= 1 or name in allowed_annotations:
                continue
            if (name != "dividPlanDate" or not fact.get("non_conflicting_optional_metadata_union_allowed")
                    or len(values - {""}) != 1 or canonical[name] not in values - {""}):
                raise ValueError("Unregistered conflicting source metadata cannot be collapsed")
        removed.update(set(indices) - {canonical_index})
        receipts.append({"instrument": symbol, "ex_date": ex_date,
            "original_response": _proof(path), "original_row_indices_zero_based": indices,
            "canonical_source_row_index_zero_based": canonical_index,
            "canonical_original_row_sha256": _canonical_row_sha(canonical),
            "resolution_document": fact["resolution_document"], "raw_original_bytes_modified": False})
    indices = [n for n in range(len(rows)) if n not in removed]
    return [rows[n] for n in indices], receipts, indices


def normalize_stability_dividends(directory, *, start, end, resolution_paths=(),
                                  legacy_complete_row_basis=None):
    """An explicit in-memory view; never forge a changed collection/response."""
    root = Path(directory).resolve()
    frame, manifest = load_dividend_evidence(root)
    facts, proofs = _resolution_facts(resolution_paths)
    if legacy_complete_row_basis is not None and (
            legacy_complete_row_basis != _proof(root / "collection.json")
            or legacy_complete_row_basis["sha256"] not in LEGACY_ARCHIVE_MANIFEST_SHA256):
        raise ValueError("Legacy complete-disclosure reuse needs the exact frozen old archive proof")
    events, resolutions, provider_rows, legacy_views, unresolved = {}, [], 0, [], []
    for key, record in sorted(manifest["responses"].items()):
        path = root / record["file"]
        audit_original_dividend_request(path, key)
        payload = json.loads(path.read_bytes())
        rows, used, original_indices = _collapse_exact_resolutions(path, payload, key.split("_")[0], facts)
        provider_rows += len(payload["rows"])
        resolutions.extend(used)
        for row_number, row in enumerate(rows):
            if not start <= row["dividOperateDate"] <= end:
                continue
            if not row["dividPlanDate"] and legacy_complete_row_basis is not None:
                complete = [(n, value) for n, value in enumerate(rows)
                            if value["dividPlanDate"] and _economic_source_key(value) == _economic_source_key(row)]
                if not complete or len({_canonical_row_sha(value) for _, value in complete}) != 1:
                    raise ValueError("Legacy missing implementation date has no unique complete original disclosure row")
                canonical_index, canonical = complete[0]
                legacy_views.append({"original_response": _proof(path),
                    "incomplete_view_row_index_zero_based": original_indices[row_number],
                    "complete_view_row_index_zero_based": original_indices[canonical_index],
                    "incomplete_original_row": row, "complete_original_row": canonical,
                    "incomplete_original_row_sha256": _canonical_row_sha(row),
                    "complete_original_row_sha256": _canonical_row_sha(canonical),
                    "basis": "frozen_legacy_parser_same_economic_identity_complete_row_reuse",
                    "issuer_new_verification_claimed": False, "original_payload_modified": False})
                row = canonical
            cash, stocks, reserve = [_amount(row[name]) for name in
                ("dividCashPsBeforeTax", "dividStocksPs", "dividReserveToStockPs")]
            if not cash and not stocks and not reserve:
                continue
            record_date, ex_date, announcement = [_day(row[name]) for name in
                ("dividRegistDate", "dividOperateDate", "dividPlanDate")]
            if not announcement <= record_date < ex_date:
                raise ValueError("Secondary dividend announcement/record/ex chronology is invalid")
            reasons = []
            pay_date = _day(row["dividPayDate"]) if cash and row["dividPayDate"] else None
            listing = _day(row["dividStockMarketDate"]) if stocks + reserve and row["dividStockMarketDate"] else None
            if cash and pay_date is None:
                reasons.append("gross_cash_present_but_original_payment_date_missing")
            if stocks + reserve and listing is None:
                reasons.append("noncash_distribution_present_but_original_listing_date_missing")
            if pay_date is not None and pay_date < ex_date or listing is not None and listing < ex_date:
                raise ValueError("Secondary dividend cannot become usable before the ex-date")
            identity = {"instrument": key.split("_")[0], "record_date": record_date, "ex_date": ex_date,
                "pay_date": pay_date, "listing_date": listing, "cash_per_share": str(cash),
                "stock_dividend_ratio": str(stocks), "capital_reserve_ratio": str(reserve)}
            event_id = "dividend:" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
            event = {**identity, "event_id": event_id, "bonus_ratio": str(stocks + reserve),
                "known_at": announcement + "T23:59:59+08:00", "source": SOURCE,
                "source_response_sha256": record["sha256"], "generation": manifest["generation"],
                "empty_optional_components_as_zero": [name for name in
                    ("dividCashPsBeforeTax", "dividStocksPs", "dividReserveToStockPs") if not row[name].strip()],
                "after_tax_source_text": row.get("dividCashPsAfterTax", ""),
                "tax_policy": "fixed personal research reserve; distribution helper must verify all noncash components"}
            if reasons:
                event["source_normalization_pending"] = True
                unresolved.append({"instrument": identity["instrument"], "ex_date": ex_date,
                    "source_candidate_event_id": event_id, "reasons": reasons,
                    "original_response": _proof(path), "original_row_index_zero_based": original_indices[row_number],
                    "original_row": row, "original_row_sha256": _canonical_row_sha(row),
                    "unknown_dates_not_invented": True})
            if event_id in events and events[event_id] != event:
                raise ValueError("Unresolved conflicting duplicate distribution source rows")
            events[event_id] = event
    receipt = {"kind": "a5_stability_original_distribution_normalization_view",
        "status": "normalized_originals_not_a_full_market_gate", "original_manifest": _proof(root / "collection.json"),
        "source_generation_preserved": manifest["generation"], "source_provider_rows": provider_rows,
        "window_economic_events": len(events), "exact_issuer_resolutions": resolutions,
        "legacy_complete_disclosure_views": legacy_views,
        "unresolved_source_rows": unresolved,
        "source_bytes_modified": False, "new_supplier_generation_claimed": False, "bound_inputs": proofs}
    return sorted(events.values(), key=lambda e: (e["ex_date"], e["instrument"], e["event_id"])), receipt


def verify_stability_capture_receipts(paths, manifest_paths, legacy_archive_manifest_proofs=()):
    """Bind new server echoes; legacy archives retain their expressly weaker basis."""
    requested = {str(Path(p).resolve()): _proof(p) for p in manifest_paths}
    covered, proofs = set(), {}
    for n, source in enumerate(paths):
        path = Path(source).resolve(); document = json.loads(path.read_bytes())
        proofs[f"strict_capture_{n}"] = _proof(path)
        if (document.get("kind") != "a5_complete_new_generation_strict_capture_guard_receipt"
                or document.get("status") != "capture_identity_iteration_verified_not_a_market_audit_pass"):
            raise ValueError("Wrong secondary strict capture receipt kind/status")
        _bind_proof_tree(document, proofs, f"strict_capture_{n}:receipt")
        for m, part in enumerate(document["sources"]):
            mp = part["original_collection_manifest"]; name = str(Path(mp["path"]).resolve())
            if (part.get("kind") not in {"execution", "dividend"}
                    or (part["kind"] == "execution") != (Path(name).name == "execution_collection.json")):
                raise ValueError("Strict capture source kind differs from its original manifest")
            if name not in requested or requested[name] != mp or name in covered:
                raise ValueError("Strict capture receipt is changed, duplicated, or outside the source archives")
            manifest = json.loads(Path(name).read_bytes()); root = Path(name).parent
            if (manifest["status"] != "complete" or manifest["failures"]
                    or part["source_generation"] != manifest["generation"]
                    or part["as_of_date"] != manifest["as_of_date"]
                    or part["response_count"] != len(manifest["responses"])):
                raise ValueError("Strict capture generation/inventory differs")
            entries = {e["key"]: e for e in part["entries"]}
            if len(entries) != len(part["entries"]) or set(entries) != set(manifest["responses"]):
                raise ValueError("Strict capture receipt omits or duplicates an original response")
            for key, rec in manifest["responses"].items():
                entry = entries[key]; original = root / rec["file"]
                if entry["original_response"] != _proof(original) or entry["original_response"]["sha256"] != rec["sha256"]:
                    raise ValueError("Strict capture canonical response bytes differ")
                payload = json.loads(original.read_bytes()); gp = entry["strict_server_echo_guard"]
                if _proof(gp["path"]) != gp:
                    raise ValueError("Strict capture guard bytes changed")
                guard = json.loads(Path(gp["path"]).read_bytes())
                request = {"code": "", "code_name": ""} if key == "stock_basic" else payload["request"]
                if (payload["generation"] != manifest["generation"] or payload["fetched_at"] != rec["fetched_at"]
                        or entry["request"] != request or guard["request"] != request
                        or guard["status"] != "verified" or guard["server_error_code"] != "0"
                        or guard["rows_verified"] != len(payload["rows"]) or rec["rows"] != len(payload["rows"])
                        or guard.get("complete_row_iteration_observed") is not True
                        or not guard["started_at"] <= guard["finished_at"] <= payload["fetched_at"]):
                    raise ValueError("Strict capture request/year/complete-iteration/time proof differs")
                if part["kind"] == "execution" and key != "stock_basic":
                    if (guard.get("row_mode_anomalies_observed") or request["adjustflag"] != "3"
                            or set(guard["server_echo"]) != set(request)
                            or any(value != request[name] and not (name == "adjustflag" and value == "")
                                   for name, value in guard["server_echo"].items())):
                        raise ValueError("Strict execution capture original mode/echo differs")
                elif guard["server_echo"] != request:
                    raise ValueError("Strict dividend/identity capture server request echo differs")
                proofs[f"strict_capture_{n}:{m}:{key}:guard"] = gp
            covered.add(name)
    legacy = set()
    for n, proof in enumerate(legacy_archive_manifest_proofs):
        name = str(Path(proof["path"]).resolve())
        if (name not in requested or requested[name] != proof or name in covered or name in legacy
                or proof["sha256"] not in LEGACY_ARCHIVE_MANIFEST_SHA256):
            raise ValueError("Explicit legacy archive basis differs from a distinct original manifest")
        proofs[f"legacy_archive_{n}"] = proof; legacy.add(name)
    missing = sorted(set(requested) - covered - legacy)
    return {"kind": "a5_stability_capture_and_explicit_legacy_archive_identity_audit",
        "status": "passed" if not missing else "blocked", "strict_server_echo_manifests": sorted(covered),
        "explicit_legacy_archive_manifests": sorted(legacy), "missing_capture_bases": missing,
        "legacy_empty_response_server_echo_proven": False if legacy else None,
        "all_sources_single_atomic_supplier_snapshot_proven": False}, proofs


def load_stability_market_evidence(execution_collections, dividend_collections, *, scope,
        calendar, start, end, action_overrides_paths=(), archive_scope_view=False,
        dividend_source_correction_receipt_paths=(), duplicate_distribution_resolution_paths=(),
        strict_capture_receipt_paths=(), legacy_archive_manifest_proofs=(), component_cash_distribution_resolution_paths=()):
    with _scope_batch(scope):
        return _load_stability_market_evidence(execution_collections, dividend_collections,
            scope=scope, calendar=calendar, start=start, end=end,
            action_overrides_paths=action_overrides_paths, archive_scope_view=archive_scope_view,
            dividend_source_correction_receipt_paths=dividend_source_correction_receipt_paths,
            duplicate_distribution_resolution_paths=duplicate_distribution_resolution_paths,
            strict_capture_receipt_paths=strict_capture_receipt_paths,
            legacy_archive_manifest_proofs=legacy_archive_manifest_proofs,
            component_cash_distribution_resolution_paths=component_cash_distribution_resolution_paths)


def resolve_component_cash_distributions(events, paths, *, available_originals):
    """Consume exact issuer-explicit component totals, never sum arbitrary rows."""
    result, receipts, proofs, seen = list(events), [], {}, set()
    originals = {(p["path"], p["sha256"]) for p in available_originals}
    for n, source in enumerate(paths):
        path = Path(source).resolve(); document = json.loads(path.read_bytes())
        if (document.get("kind") != "a5_issuer_verified_component_cash_distribution_resolution"
                or document.get("status") != "source_component_aggregation_verified_not_full_market_gate"):
            raise ValueError("Wrong issuer component distribution resolution kind/status")
        proofs[f"component_cash_resolution_{n}"] = _proof(path)
        _bind_proof_tree(document, proofs, f"component_cash_resolution_{n}:original")
        for fact in document["events"]:
            key = fact["instrument"], fact["ex_date"]
            if (key in seen or fact.get("status") != "verified_issuer_explicit_combined_cash_distribution"
                    or fact.get("original_pdf_visual_reviewed") is not True
                    or fact.get("issuer_actual_terms_verified") is not True
                    or fact.get("issuer_explicitly_combines_components") is not True
                    or fact.get("economic_events_after_resolution") != 1
                    or fact.get("original_source_bytes_modified") is not False):
                raise ValueError("Component resolution lacks unique explicit issuer/visual verification")
            seen.add(key)
            original = fact["original_response"]
            if (original["path"], original["sha256"]) not in originals:
                raise ValueError("Component resolution original is outside the consumed raw archives")
            payload = json.loads(Path(original["path"]).read_bytes())
            rows = [dict(zip(payload["fields"], row)) for row in payload["rows"]]
            declared = fact["original_component_rows"]
            indices = [item["row_index_zero_based"] for item in declared]
            if len(indices) < 2 or len(indices) != len(set(indices)):
                raise ValueError("Component resolution needs distinct exact original rows")
            for item in declared:
                index = item["row_index_zero_based"]
                if (type(index) is not int or not 0 <= index < len(rows)
                        or rows[index] != item["row"] or _canonical_row_sha(rows[index]) != item["canonical_row_json_sha256"]):
                    raise ValueError("Component original row/index/SHA differs")
            expected_indices = {i for i, row in enumerate(rows)
                if row["code"].replace(".", "").upper() == key[0] and row["dividOperateDate"] == key[1]}
            if set(indices) != expected_indices:
                raise ValueError("Component resolution omits another original same-date row")
            component_rows = [rows[i] for i in indices]
            if any(_amount(row["dividStocksPs"]) or _amount(row["dividReserveToStockPs"]) for row in component_rows):
                raise ValueError("Cash component resolution cannot combine noncash distributions")
            cash_terms = sorted(_amount(row["dividCashPsBeforeTax"]) for row in component_rows)
            explicit_total = Decimal(fact["issuer_explicit_total_cash_per_share"])
            method = fact.get("component_resolution_method", "issuer_explicit_components_exact_total")
            if any(cash <= 0 for cash in cash_terms):
                raise ValueError("Original cash components must be positive")
            with localcontext() as context:
                context.prec = 128
                provider_total = sum(cash_terms, Decimal(0))
                if method == "issuer_explicit_components_exact_total":
                    if (fact.get("component_sum_matches_issuer_explicit_total") is not True
                            or provider_total != explicit_total
                            or sorted(Decimal(t["cash_per_share"]) for t in fact["issuer_component_terms"]) != cash_terms):
                        raise ValueError("Original cash components do not match the issuer explicit terms and total")
                elif method == "issuer_explicit_total_replaces_exact_bound_provider_rounded_components":
                    derivation = fact["issuer_total_derivation"]
                    original_cash, entitled, total = [Decimal(derivation[k]) for k in
                        ("original_cash_per_share", "entitled_shares", "total_shares")]
                    if (fact.get("component_sum_matches_issuer_explicit_total") is not False
                            or fact.get("provider_component_values_are_not_issuer_adjusted_components") is not True
                            or fact.get("issuer_adjusted_components_not_individually_published") is not True
                            or fact.get("issuer_component_terms")
                            or not all(x.is_finite() and x > 0 for x in (original_cash, entitled, total))
                            or entitled > total or entitled != entitled.to_integral_value() or total != total.to_integral_value()
                            or derivation.get("rounding") != "ROUND_DOWN" or derivation.get("per10_decimal_places") != 6
                            or provider_total != Decimal(fact["provider_component_sum_exact"])
                            or explicit_total - provider_total != Decimal(fact["issuer_total_minus_provider_component_sum_exact"])
                            or explicit_total == provider_total):
                        raise ValueError("Issuer total replacement lacks its exact source-specific derivation")
                    per10 = (10 * original_cash * entitled / total).quantize(Decimal(".000001"), rounding=ROUND_DOWN)
                    if per10 != Decimal(derivation["issuer_adjusted_cash_per10"]) or per10 / 10 != explicit_total:
                        raise ValueError("Issuer explicit per10 truncation formula differs from its combined total")
                else:
                    raise ValueError("Unsupported issuer component resolution method")
            canonical = dict(fact["canonical_issuer_event"])
            if (canonical["instrument"], canonical["ex_date"]) != key:
                raise ValueError("Issuer combined event identity differs")
            record, ex, pay = [_day(canonical[k]) for k in ("record_date", "ex_date", "pay_date")]
            known = pd.Timestamp(canonical["known_at"])
            if (not record < ex <= pay or known.tzinfo is None or pd.isna(known)
                    or known.date().isoformat() > record or canonical.get("listing_date") is not None
                    or Decimal(canonical["cash_per_share"]) != explicit_total
                    or any(Decimal(canonical[k]) != 0 for k in ("stock_dividend_ratio", "capital_reserve_ratio", "bonus_ratio"))
                    or Decimal(canonical["cash_dividend_tax_rate"]) != Decimal(".20")):
                raise ValueError("Issuer cash total/chronology/noncash/tax terms differ")
            if any((row["dividRegistDate"], row["dividOperateDate"], row["dividPayDate"]) != (record, ex, pay)
                   for row in component_rows):
                raise ValueError("Original component settlement clocks differ from the issuer combined event")
            pdfs = [p for p in fact["evidence"] if Path(p["path"]).suffix.lower() == ".pdf"]
            if (not any(p["sha256"] == canonical["source_response_sha256"] for p in pdfs)
                    or not any(Path(p["path"]).suffix.lower() == ".png" for p in fact["evidence"])):
                raise ValueError("Issuer combined event lacks its original PDF and rendered page")
            if fact.get("issuer_cash_payment_gross_rounding") is not None or canonical.get("cash_payment_gross_rounding") is not None:
                if (fact.get("issuer_cash_payment_gross_rounding") != "ROUND_DOWN"
                        or canonical.get("cash_payment_gross_rounding") != "ROUND_DOWN"
                        or not any(p["sha256"] == canonical["source_response_sha256"]
                                   and 3 in p.get("pdf_pages_1based", []) for p in pdfs)
                        or not any(Path(p["path"]).suffix.lower() == ".png" and p.get("pdf_page_1based") == 3
                                   and p.get("source_pdf_sha256") == canonical["source_response_sha256"] for p in fact["evidence"])):
                    raise ValueError("Issuer-specific gross cash truncation lacks matching reviewed original page3 evidence")
                canonical["cash_payment_gross_rounding_source_pdf_sha256"] = canonical["source_response_sha256"]
                canonical["cash_payment_gross_rounding_source_page_1based"] = 3
            matches = [e for e in result if (e["instrument"], e["ex_date"]) == key]
            if (len(matches) != len(indices) or any(e["source_response_sha256"] != original["sha256"] for e in matches)
                    or sorted(Decimal(e["cash_per_share"]) for e in matches) != cash_terms):
                raise ValueError("Canonical source events differ from the exact original component rows")
            canonical["event_id"] = "issuer_combined:" + _canonical_row_sha(canonical)
            canonical["component_resolution_document"] = _proof(path)
            canonical["original_component_source_response"] = original
            canonical["original_component_event_ids"] = [e["event_id"] for e in matches]
            result = [e for e in result if (e["instrument"], e["ex_date"]) != key] + [canonical]
            receipts.append({"instrument": key[0], "ex_date": key[1], "original_response": original,
                "original_component_rows": declared, "original_event_ids": [e["event_id"] for e in matches],
                "canonical_issuer_event_id": canonical["event_id"], "resolution_document": _proof(path),
                "issuer_explicit_total_cash_per_share": str(explicit_total), "provider_component_sum_exact": str(provider_total),
                "issuer_total_minus_provider_component_sum_exact": str(explicit_total - provider_total),
                "component_resolution_method": method, "issuer_cash_payment_gross_rounding": fact.get("issuer_cash_payment_gross_rounding"),
                "source_bytes_modified": False})
    return sorted(result, key=lambda e: (e["ex_date"], e["instrument"], e["event_id"])), receipts, proofs


def _close_normalization_pending(events, receipts, applied_patch_values, applied_patches=()):
    for receipt in receipts:
        still_pending, closed = [], []
        for item in receipt.get("unresolved_source_rows", []):
            matches = [e for e in events if e["instrument"] == item["instrument"]
                       and e["ex_date"] == item["ex_date"]]
            patched = any(key[:2] == (item["instrument"], item["ex_date"]) for key in applied_patch_values)
            # A source correction legitimately changes the canonical event's
            # source SHA to its issuer PDF. The immutable supplier original
            # must instead remain explicitly bound in the applied patch.
            resolution_proofs = []
            for patch in applied_patches:
                if (patch.get("instrument"), patch.get("ex_date")) != (item["instrument"], item["ex_date"]):
                    continue
                if (patch.get("operation") not in {"replace", "annotate"}
                        or patch.get("issuer_actual_terms_verified") is not True
                        or patch.get("original_pdf_visual_reviewed") is not True
                        or not set(patch.get("values", {})).intersection({"cash_per_share", "stock_dividend_ratio",
                            "capital_reserve_ratio", "record_date", "pay_date", "listing_date"})):
                    continue
                patch_proofs = {}
                _bind_proof_tree(patch, patch_proofs, "normalization_correction")
                original = item["original_response"]
                if (any(p == original for p in patch_proofs.values())
                        and any(Path(p["path"]).suffix.lower() == ".pdf" for p in patch_proofs.values())
                        and any(Path(p["path"]).suffix.lower() == ".png" for p in patch_proofs.values())):
                    resolution_proofs.extend(patch_proofs.values())
            if len(matches) != 1 or not patched or not resolution_proofs:
                still_pending.append(item); continue
            event = matches[0]
            cash, stock, capital = [Decimal(event[k]) for k in
                ("cash_per_share", "stock_dividend_ratio", "capital_reserve_ratio")]
            if (not (cash or stock or capital) or cash and event.get("pay_date") is None
                    or stock + capital and event.get("listing_date") is None):
                still_pending.append(item); continue
            for key in ["record_date", "ex_date", *(["pay_date"] if cash else []),
                        *(["listing_date"] if stock + capital else [])]:
                _day(event[key])
            if (event["record_date"] >= event["ex_date"]
                    or cash and event["pay_date"] < event["ex_date"]
                    or stock + capital and event["listing_date"] < event["ex_date"]):
                raise ValueError("Issuer override leaves invalid distribution chronology")
            event.pop("source_normalization_pending", None)
            closed.append({**item, "resolved_event_id": event["event_id"],
                "correction_original_and_visual_proofs": resolution_proofs,
                "resolution_basis": "bound_issuer_action_override_replaces_complete_required_economics_or_dates"})
        receipt["unresolved_source_rows"] = still_pending
        receipt["resolved_source_rows"] = closed


def _distribution_tax(events, capital_evidence, stock_evidence, *, source_preflight_only=False):
    from .a5_distribution_tax import prepare_distribution_tax
    groups = {}
    for event in events:
        groups.setdefault((event["instrument"], event["ex_date"]), []).append(event)
    ambiguous = {key: rows for key, rows in groups.items() if len(rows) > 1}
    if ambiguous and source_preflight_only:
        # Preserve every source event, while exposing unrelated price/source
        # failures before issuer resolution is available. These groups cannot
        # enter formal accounting and are never silently combined or removed.
        selected = [e for e in events if (e["instrument"], e["ex_date"]) not in ambiguous]
        prepared, report = prepare_distribution_tax(selected, capital_evidence, stock_evidence)
        by_id = {e["event_id"]: e for e in prepared}
        annotated = [by_id.get(e["event_id"], e) for e in events]
        report["status"] = "blocked"
        report["source_ambiguity_groups"] = [{"instrument": key[0], "ex_date": key[1],
            "original_events_preserved": rows, "combined_or_selected_event_claimed": False}
            for key, rows in sorted(ambiguous.items())]
        report["pending"].extend({"instrument": e["instrument"], "ex_date": e["ex_date"], "event_id": e["event_id"],
            "reason": "multiple_original_distribution_events_same_security_and_ex_date_need_issuer_resolution"}
            for rows in ambiguous.values() for e in rows)
        report["all_source_classifications_verified"] = False
        report["checks"]["all_noncash_events_covered"] = False
        report["checks"]["full_overflow_explicit"] = False
    else:
        annotated, report = prepare_distribution_tax(events, capital_evidence, stock_evidence)
    if set(e["event_id"] for e in annotated) != set(e["event_id"] for e in events):
        raise ValueError("Secondary distribution tax preparation changed economic event identity")
    return annotated, report


def apply_official_code_validity_view(raw, scope_path, calendar):
    """Keep current-code supplier aliases outside their original code clock out.

    This view neither creates the old security's prices nor changes source IPO
    dates. The original complete responses and excluded rows remain auditable.
    """
    root = Path(scope_path).resolve(); path = root / "events.json"
    document = json.loads(path.read_bytes())
    if document.get("kind") != "ashare_official_index_adjustment_events" or document.get("price_mapping_allowed") is not False:
        raise ValueError("Official code clock requires the unchanged no-price-mapping event ledger")
    proofs = {"official_code_events": _proof(path), "official_code_scope_manifest": _proof(root / "manifest.json")}
    intervals, facts = {}, []
    first_source_session = str(raw.date.min())
    observed_instruments = set(raw.instrument)
    for event in document["events"]:
        if event.get("event_type") != "same_issuer_security_code_change":
            continue
        if not {event["old_instrument"], event["new_instrument"]} & observed_instruments:
            continue
        effective = _day(event["effective_on"])
        if (event.get("price_mapping_allowed") is not False or effective not in calendar
                or event.get("first_effective_session") != effective
                or pd.Timestamp(event["known_at"]).tzinfo is None
                or pd.Timestamp(event["known_at"]) >= pd.Timestamp(max(effective, first_source_session) + "T09:30:00+08:00")):
            raise ValueError("Official code validity clock is incomplete or not known before execution")
        _bind_proof_tree(event["evidence"], proofs, "official_code:" + event["id"])
        old, new = event["old_instrument"], event["new_instrument"]
        for instrument, field_name in ((old, "valid_before"), (new, "valid_from")):
            interval = intervals.setdefault(instrument, {})
            if field_name in interval and interval[field_name] != effective:
                raise ValueError("Conflicting official security code validity clocks")
            interval[field_name] = effective
        facts.append(event)
    mask = pd.Series(True, index=raw.index)
    for instrument, limits in intervals.items():
        selected = raw.instrument.eq(instrument)
        if "valid_from" in limits:mask &= ~selected | raw.date.ge(limits["valid_from"])
        if "valid_before" in limits:mask &= ~selected | raw.date.lt(limits["valid_before"])
    excluded = raw.loc[~mask].to_dict("records")
    return raw.loc[mask].copy(), {"kind": "a5_stability_official_security_code_validity_consumption_view",
        "status": "passed", "validity_intervals": intervals, "official_events": facts,
        "original_raw_rows": len(raw), "consumed_raw_rows": int(mask.sum()), "excluded_alias_rows": excluded,
        "excluded_row_count": len(excluded), "original_registry_ipo_dates_modified": False,
        "original_source_bytes_modified": False, "old_code_price_mapping_allowed": False,
        "announcement_known_before_relevant_observed_interval_verified": True,
        "full_historical_announcement_time_PIT_claimed": False}, proofs


def audit_code_valid_daily_coverage(raw, basic, calendar, instruments, *, start, end, code_view):
    intervals = code_view["validity_intervals"]
    if not any(i in intervals for i in instruments):
        return audit_daily_coverage(raw, basic, calendar, instruments, start=start, end=end)
    result = audit_daily_coverage(raw.iloc[:0], basic, calendar, [], start=start, end=end)
    results = []
    groups = {instrument: frame for instrument, frame in raw.groupby("instrument", sort=False)}
    for instrument in instruments:
        limits = intervals.get(instrument, {})
        sessions = [day for day in calendar if day >= limits.get("valid_from", "0001-01-01")
                    and day < limits.get("valid_before", "9999-12-31")]
        if any(start <= day <= end for day in sessions):
            results.append(audit_daily_coverage(groups.get(instrument, raw.iloc[:0]), basic,
                sessions, [instrument], start=start, end=end))
    for field_name in ("missing_listed_security_sessions", "invalid_trading_sessions", "source_confirmed_suspensions"):
        result[field_name] = [item for report in results for item in report[field_name]]
    result.update(instruments=len(instruments), status="passed" if all(r["status"] == "passed" for r in results) else "blocked",
                  historical_st_row_count=int(raw.loc[raw.date.between(start, end), "isST"].eq("1").sum()),
                  expected_sessions_basis="actual_calendar_intersect_source_ipo_out_and_official_security_code_validity")
    return result


def audit_model_code_validity(dataset, instruments, *, start, end, code_view):
    failures, checked = [], []
    for instrument in instruments:
        limits = code_view["validity_intervals"].get(instrument)
        if not limits:continue
        frame = read_qlib_security(dataset, instrument).loc[start:end]
        invalid = ((frame.index < limits.get("valid_from", "0001-01-01"))
                   | (frame.index >= limits.get("valid_before", "9999-12-31")))
        outside = frame.loc[invalid]
        if outside.empty:continue
        finite = np.isfinite(outside.astype(float)).any(axis=1)
        failures.extend({"instrument": instrument, "session": day,
            "reason": "finite_model_fields_outside_official_original_code_validity_no_price_mapping"}
            for day in outside.index[finite])
        checked.append({"instrument": instrument, "outside_code_clock_sessions": len(outside),
                        "finite_outside_sessions": int(finite.sum()), **limits})
    return {"status": "passed" if not failures else "blocked", "start": start, "end": end,
        "failures": failures, "records": checked, "old_code_price_mapping_allowed": False}


def _stability_source_queue(raw, events, references, identity, temporal, tax, normalizations,
                            *, start, end, coverage=None, availability=None, bridge=None):
    by_id = {e["event_id"]: e for e in events}
    items = [{**p, "reasons": [p["reason"]], "event": by_id.get(p.get("event_id"))}
             for p in tax["pending"]]
    for normalization in normalizations:
        items.extend(normalization.get("unresolved_source_rows", []))
    items.extend({**p, "reasons": ["official_distribution_or_rights_reference_terms"]}
                 for p in references["unexplained"])
    items.extend({**p, "reasons": [p["reason"]]} for p in identity["failures"])
    items.extend({**p, "reasons": [p["reason"]]} for p in temporal["failures"])
    for p in (coverage or {}).get("missing_listed_security_sessions", []):
        items.append({**p, "reasons": ["listed_session_original_source_absent_not_assumed_suspended"]})
    for p in (coverage or {}).get("invalid_trading_sessions", []):
        items.append({**p, "reasons": [p["reason"]], "source_check": "daily_source_coverage"})
    for p in (availability or {}).get("mismatches", []):
        items.append({**p, "reasons": [p["reason"]], "source_check": "source_model_availability"})
    for p in (bridge or {}).get("failures", []):
        items.append({**p, "reasons": [p.get("reason", "original_raw_model_seven_field_mismatch")],
                      "source_check": "raw_model_seven_field_bridge"})
    selected = raw.loc[raw.date.between(start, end) & raw.tradestatus.eq("0")]
    for row in selected.to_dict("records"):
        numbers = pd.to_numeric(pd.Series({k: row[k] for k in
            ("open", "high", "low", "close", "volume", "amount")}), errors="coerce")
        if numbers.gt(0).all() and not row.get("source_observed_market_quote", False):
            items.append({"instrument": row["instrument"], "session": row["date"],
                "source_response_sha256": row["source_response_sha256"],
                "reasons": ["official_partial_session_suspension_and_independent_quote"]})
    return {"kind": "a5_stability_market_source_review_queue", "items": items,
        "item_count": len(items), "portfolio_returns_computed": False,
        "no_security_excluded_to_improve_performance": True,
        "tax_classification_unresolved_with_conservative_full_face_tax_covered": tax["tax_classification_unresolved"]}


def _load_stability_market_evidence(execution_collections, dividend_collections, *, scope,
                            calendar, start, end, action_overrides_paths=(), archive_scope_view=False,
                            dividend_source_correction_receipt_paths=(), duplicate_distribution_resolution_paths=(),
                            strict_capture_receipt_paths=(), legacy_archive_manifest_proofs=(),
                            component_cash_distribution_resolution_paths=()):
    start, end = _day(start), _day(end)
    if type(archive_scope_view) is not bool:
        raise ValueError("Archive scope view must be an explicit Boolean choice")
    if start > end or not execution_collections or not dividend_collections:
        raise ValueError("A5 requires complete original execution and dividend collections")
    days = [day for day in calendar if start <= day <= end]
    preceding = [day for day in calendar if day < start]
    if not days or not preceding or calendar != sorted(set(calendar)):
        raise ValueError("Actual ordered calendar with a preceding session is required")
    targets = scope.download_targets(start, end, sessions=calendar, scope="union")
    # Package source histories are retrospective effective membership, not a
    # historical announcement-time database. The scope implementation checks
    # both 300/1000 cardinalities and same-day cross-index disjointness.
    if scope.audit_sessions(days, scope="union")["status"] != "passed":
        raise ValueError("Historical union membership failed its daily audit")
    frames, basics, receipts, dividend_receipts, event_lists = [], [], [], [], []
    proofs, execution_targets, dividend_targets = {}, set(), set()
    normalizations = []
    view_receipt = {"kind": "a5_stability_existing_immutable_archive_scope_view", "applied": archive_scope_view,
        "source_vintage": "original_archive_generations_preserved_not_new_supplier_collections",
        "permitted_instruments": targets, "execution_inputs": [], "dividend_inputs": [], "action_override_inputs": [],
        "new_download_targets_changed_by_this_loader": False}
    excluded_instruments = set()
    for number, directory in enumerate(execution_collections):
        directory = Path(directory).resolve()
        frame, basic, receipt = load_execution_collection(directory)
        request = receipt["request"]
        if request["start_date"] > preceding[-1] or request["end_date"] < end:
            raise ValueError("Original execution collection lacks the preceding close or audit end")
        outside = sorted(set(request["instruments"]) - set(targets))
        if outside and not archive_scope_view:
            raise ValueError("Execution collection targets exceed the permitted historical union")
        selected = frame.loc[frame.instrument.isin(targets)].copy()
        execution_targets.update(set(request["instruments"]) & set(targets))
        excluded_instruments.update(outside)
        view_receipt["execution_inputs"].append({"archive": str(directory),
            "original_instrument_count": len(request["instruments"]), "excluded_input_instruments": outside,
            "original_record_count": len(frame), "scope_view_record_count": len(selected),
            "excluded_input_record_count": len(frame) - len(selected)})
        frames.append(selected); basics.append(basic); receipts.append(receipt)
        proofs[f"execution_collection_{number}"] = _proof(directory / "execution_collection.json")
        proofs[f"execution_selection_{number}"] = _proof(request["selection_evidence_path"])
        for symbol, response in receipt["responses"].items():
            original = directory / response["file"]
            if sha(original) != response["sha256"]:
                raise ValueError("Original execution response changed")
            proofs[f"execution_{number}_original_{symbol}"] = _proof(original)
    years = list(range(int(start[:4]), int(end[:4]) + 1))
    for number, directory in enumerate(dividend_collections):
        directory = Path(directory).resolve()
        _, receipt = load_dividend_evidence(directory)
        outside = sorted(set(receipt["instruments"]) - set(targets))
        if receipt["years"] != years or outside and not archive_scope_view:
            raise ValueError("Dividend collection years/targets differ from the historical union")
        dividend_targets.update(set(receipt["instruments"]) & set(targets))
        dividend_receipts.append(receipt)
        legacy_basis = next((proof for proof in legacy_archive_manifest_proofs
                             if proof == _proof(directory / "collection.json")), None)
        original_events, normalized = normalize_stability_dividends(directory, start=start, end=end,
            resolution_paths=duplicate_distribution_resolution_paths, legacy_complete_row_basis=legacy_basis)
        normalizations.append(normalized)
        proofs.update({f"normalization_{number}:" + key: value for key, value in normalized["bound_inputs"].items()})
        selected_events = [event for event in original_events if event["instrument"] in targets]
        excluded_instruments.update(outside)
        view_receipt["dividend_inputs"].append({"archive": str(directory),
            "original_instrument_count": len(receipt["instruments"]), "excluded_input_instruments": outside,
            "original_window_event_count": len(original_events), "scope_view_window_event_count": len(selected_events),
            "excluded_input_event_count": len(original_events) - len(selected_events)})
        event_lists.append(selected_events)
        proofs[f"dividend_collection_{number}"] = _proof(directory / "collection.json")
        proofs.update({f"dividend_correction_{number}:" + key: value for key, value in
            audit_dividend_source_correction(directory, dividend_source_correction_receipt_paths).items()})
        for key, response in receipt["responses"].items():
            original = directory / response["file"]
            if sha(original) != response["sha256"]:
                raise ValueError("Original dividend response changed")
            audit_original_dividend_request(original, key)
            proofs[f"dividend_{number}_original_{key}"] = _proof(original)
    capture_audit, capture_proofs = verify_stability_capture_receipts(strict_capture_receipt_paths,
        [Path(d) / "execution_collection.json" for d in execution_collections] +
        [Path(d) / "collection.json" for d in dividend_collections], legacy_archive_manifest_proofs)
    proofs.update(capture_proofs)
    view_receipt["capture_identity_audit"] = capture_audit
    if execution_targets != set(targets) or dividend_targets != set(targets):
        raise ValueError("Both market and dividend generations must cover every historical union target")
    raw, basic = merge_execution_frames(frames, basics)
    events = merge_dividend_events(event_lists)
    events, components, component_proofs = resolve_component_cash_distributions(events,
        component_cash_distribution_resolution_paths, available_originals=proofs.values())
    proofs.update(component_proofs)
    view_receipt["issuer_explicit_component_cash_resolutions"] = components
    view_receipt["consumed_pre_override_deduplicated_event_count"] = len(events)
    applied_patch_values, applied_patches = {}, []
    for number, path in enumerate(action_overrides_paths):
        path = Path(path).resolve()
        document = json.loads(path.read_text())
        proofs[f"action_overrides_{number}"] = _proof(path)
        selected_patches, excluded_patches = [], []
        for item, patch in enumerate(document["patches"]):
            original = (path.parent / patch["evidence"]["path"]).resolve()
            if sha(original) != patch["evidence"]["sha256"] or not patch["evidence"].get("url", "").startswith("https://"):
                raise ValueError("Original archived action correction evidence changed")
            proofs[f"action_override_{number}_original_{item}"] = _proof(original)
            for supporting_number, supporting in enumerate(patch.get("supporting_evidence", [])):
                supporting_path = Path(supporting["path"])
                if (not supporting_path.is_absolute() or sha(supporting_path) != supporting["sha256"]
                        or supporting_path.suffix.lower() == ".pdf" and not supporting.get("url", "").startswith("https://")):
                    raise ValueError("Original supporting action evidence changed or lacks source identity")
                proofs[f"action_override_{number}_supporting_{item}_{supporting_number}"] = _proof(supporting_path)
            if patch["instrument"] in targets and start <= patch["ex_date"] <= end:
                for field_name, field_value in patch["values"].items():
                    identity = (patch["instrument"], patch["ex_date"], field_name)
                    if identity in applied_patch_values:
                        prior = applied_patch_values[identity]
                        try:
                            same = Decimal(str(prior)) == Decimal(str(field_value))
                        except ArithmeticError:
                            same = prior == field_value
                        if not same:
                            raise ValueError("Conflicting independently evidenced action correction values")
                    applied_patch_values[identity] = field_value
                selected_patches.append({**patch, "evidence": {**patch["evidence"], "path": str(original)}})
                applied_patches.append(selected_patches[-1])
            else:
                excluded_patches.append(patch)
        if excluded_patches and not archive_scope_view:
            raise ValueError("Original action corrections exceed the permitted official scope/window")
        # A local consumption view of an immutable archive is explicit. The
        # original full patch document and all its source PDFs remain bound.
        # Absolute proof paths preserve the frozen correction helper's checks.
        with TemporaryDirectory(prefix="a5_action_scope_view_") as temporary:
            view_path = Path(temporary) / "corrections.json"
            _save(view_path, {**document, "patches": selected_patches})
            events = apply_action_overrides(events, view_path)
        view_receipt["action_override_inputs"].append({"original_document": _proof(path),
            "original_patch_count": len(document["patches"]), "consumed_patch_count": len(selected_patches),
            "excluded_patch_count": len(excluded_patches), "consumed_patches": selected_patches,
            "excluded_patches": excluded_patches,
            "selection_basis": "fixed_official_target_and_audit_date_window_not_performance"})
    if any(event["instrument"] not in execution_targets or not start <= event["ex_date"] <= end
           for event in events):
        raise ValueError("A5 action overrides added an out-of-scope/window entitlement")
    if len({event["event_id"] for event in events}) != len(events):
        raise ValueError("Duplicate post-correction economic event IDs")
    _close_normalization_pending(events, normalizations, applied_patch_values, applied_patches)
    scope.assert_unchanged()
    view_receipt.update(excluded_input_instruments=sorted(excluded_instruments),
        excluded_input_record_count=sum(row["excluded_input_record_count"] for row in view_receipt["execution_inputs"]),
        excluded_input_event_count=sum(row["excluded_input_event_count"] for row in view_receipt["dividend_inputs"]),
        consumed_deduplicated_record_count=len(raw), consumed_post_override_event_count=len(events),
        complete_official_target_coverage_verified=True)
    return A5StabilityMarketEvidence(raw, basic, events, targets, list(calendar), proofs, receipts, dividend_receipts, view_receipt, normalizations)


def build_a5_stability_market_audit(snapshot_path, scope_path, execution_collections, dividend_collections,
        output_dir, *, start, end, action_overrides_paths=(), rights_events_paths=(),
        intraday_observations_paths=(), capital_reserve_tax_evidence_paths=(), identity_events_paths=(),
        official_membership_evidence_path=None, historical_scope=None,
        adjustment_bridge_spec_path=DEFAULT_SPEC, archive_scope_view=False,
        dividend_source_correction_receipt_paths=(), partial_session_model_policy_path=None,
        stock_dividend_tax_evidence_paths=(), duplicate_distribution_resolution_paths=(),
        strict_capture_receipt_paths=(), legacy_archive_manifest_proofs=(),
        component_cash_distribution_resolution_paths=(), source_preflight_only=True, progress=None):
    """Bind a complete or explicitly blocked union market audit, never returns."""
    if (start, end) != ("2025-01-01", "2026-10-19") or archive_scope_view is not True:
        raise ValueError("Secondary gate requires the fixed25/26 window and explicit archive_scope_view=True")
    if type(source_preflight_only) is not bool:
        raise ValueError("Secondary preflight/formal choice must be explicit Boolean")
    output = guard_qlib_output_path(output_dir)
    if output.exists():
        raise ValueError("Preserve existing A5 audit generation")
    from .a5_capital_tax import SPEC_PATH as capital_spec_path
    from .a5_distribution_tax import SPEC_PATH as distribution_spec_path
    runtime_paths = {"implementation": __file__, "adjustment_bridge_spec": adjustment_bridge_spec_path,
                     "capital_accounting_spec": capital_spec_path,
                     "distribution_tax_spec": distribution_spec_path}
    for name in ("a5_market_audit.py", "a5_distribution_tax.py", "a5_adjusted_prices.py", "a5_capital_tax.py", "a5_source_checks.py", "ashare_history_audit.py",
                 "ashare_actions.py", "ashare_execution_data.py", "ashare_scope.py",
                 "qlib_incremental.py", "qlib_snapshot.py"):
        runtime_paths["dependency:" + name] = Path(__file__).parent / name
    runtime_proofs = _capture_runtime_proofs(runtime_paths)
    _, partial_proofs = load_partial_session_model_policy(partial_session_model_policy_path)
    runtime_proofs.update({"partial_policy:" + key: value for key, value in partial_proofs.items()})

    def emit(phase):
        if progress:
            progress({"phase": phase})
        _assert_bound_inputs_unchanged(runtime_proofs)

    emit("load_original_sources")
    start, end = _day(start), _day(end)
    snapshot = load_qlib_snapshot(snapshot_path, verify=True)
    calendar_path = snapshot.dataset_dir / "calendars/day.txt"
    calendar = calendar_path.read_text().splitlines()
    # A later official-history reconstruction has its own type/provenance;
    # never mislabel it as the older code-window-only derivation for loading.
    scope = (load_verified_historical_scope(scope_path, start=start, end=end, require_official=True)
             if historical_scope is None else historical_scope)
    scope_manifest = (Path(scope_path) / "manifest.json").resolve()
    if not any(Path(proof["path"]).resolve() == scope_manifest and proof["sha256"] == sha(scope_manifest)
               for proof in scope.source_receipts().values()):
        raise ValueError("Historical scope object is not bound to the declared manifest")
    evidence = load_stability_market_evidence(execution_collections, dividend_collections,
        scope=scope, calendar=calendar, start=start, end=end, action_overrides_paths=action_overrides_paths,
        archive_scope_view=archive_scope_view,
        dividend_source_correction_receipt_paths=dividend_source_correction_receipt_paths,
        duplicate_distribution_resolution_paths=duplicate_distribution_resolution_paths,
        strict_capture_receipt_paths=strict_capture_receipt_paths,
        legacy_archive_manifest_proofs=legacy_archive_manifest_proofs,
        component_cash_distribution_resolution_paths=component_cash_distribution_resolution_paths)
    proofs = dict(evidence.source_proofs)
    proofs.update(runtime_proofs)
    rights, rights_proofs = load_event_evidence(rights_events_paths, key_fields=("instrument", "ex_date"))
    intraday, intraday_proofs = load_event_evidence(intraday_observations_paths, key_fields=("instrument", "date"))
    capital, capital_proofs = load_event_evidence(capital_reserve_tax_evidence_paths, key_fields=("instrument", "ex_date"))
    identities, identity_proofs = load_event_evidence(identity_events_paths, key_fields=("instrument", "effective_date"))
    stock, stock_proofs = load_event_evidence(stock_dividend_tax_evidence_paths, key_fields=("instrument", "ex_date"))
    proofs.update({"stock_tax:" + key: value for key, value in stock_proofs.items()})
    for prefix, entries in (("rights", rights_proofs), ("intraday", intraday_proofs),
                            ("capital_tax", capital_proofs), ("identity", identity_proofs)):
        proofs.update({prefix + ":" + key: value for key, value in entries.items()})
    official_membership, official_proofs = audit_official_membership_evidence(
        official_membership_evidence_path, scope_path, start=start, end=end)
    proofs.update(official_proofs)
    evidence.events, distribution_accounting = _distribution_tax(evidence.events, capital, stock,
        source_preflight_only=source_preflight_only)
    for n, proof in enumerate(distribution_accounting["bound_inputs"]):
        if _proof(proof["path"]) != proof:
            raise ValueError("Secondary distribution tax original changed")
        proofs[f"distribution_accounting:{n}"] = proof
    raw = apply_intraday_observation_evidence(evidence.raw, intraday)
    raw, code_view, code_proofs = apply_official_code_validity_view(raw, scope_path, calendar)
    proofs.update(code_proofs)
    evidence.archive_scope_view_receipt["official_code_validity_consumption_view"] = code_view
    emit("daily_source_coverage")
    coverage = audit_code_valid_daily_coverage(raw, evidence.basic, calendar, evidence.instruments,
        start=start, end=end, code_view=code_view)
    emit("daily_membership_identity")
    membership = audit_member_identity(raw, evidence.basic, scope, calendar, start=start, end=end,
                                      identity_events=identities["events"])
    preceding_signal = max(day for day in calendar if day < start)
    emit("model_availability")
    availability = audit_qlib_availability(snapshot.dataset_dir, raw, evidence.instruments, start=preceding_signal, end=end)
    code_model = audit_model_code_validity(snapshot.dataset_dir, evidence.instruments,
        start=preceding_signal, end=end, code_view=code_view)
    emit("raw_model_seven_field_bridge")
    bridge = audit_raw_qlib_bridge(snapshot.dataset_dir, raw, evidence.instruments, start=preceding_signal, end=end,
                                 partial_session_model_policy_path=partial_session_model_policy_path)
    emit("normalized_temporal_source_bridge")
    temporal = audit_temporal_adjustments(snapshot.dataset_dir, raw, evidence.instruments,
        start=preceding_signal, end=end, spec_path=adjustment_bridge_spec_path,
        partial_session_model_policy_path=partial_session_model_policy_path)
    emit("issuer_exchange_reference_prices")
    references = audit_reference_prices(raw, evidence.events, calendar, start=start, end=end, rights=rights["events"])
    queue = _stability_source_queue(raw, evidence.events, references, membership, temporal,
        distribution_accounting, evidence.normalization_receipts, start=start, end=end,
        coverage=coverage, availability=availability, bridge=bridge)
    queue["items"].extend({**p, "reasons": [p["reason"]], "source_check": "official_model_code_validity"}
                          for p in code_model["failures"])
    queue["item_count"] = len(queue["items"])
    normalization_complete = not any(n["unresolved_source_rows"] for n in evidence.normalization_receipts)
    checks = {"scope_coverage": membership["status"] == "passed" and official_membership["status"] == "passed",
        "scope_listing_validity": membership["status"] == "passed" and code_model["status"] == "passed",
        "raw_ohlc": bridge["status"] == "passed" and availability["status"] == "passed" and temporal["status"] == "passed" and code_model["status"] == "passed",
        "raw_preclose": references["status"] == "passed", "daily_trading_status": coverage["status"] == "passed",
        "historical_is_st": coverage["status"] == "passed", "ipo_identity": membership["status"] == "passed",
        "calendar_coverage": coverage["status"] == "passed",
        "dividend_events": normalization_complete and distribution_accounting["status"] == "passed",
        "ex_reference_explained": references["status"] == "passed",
        "source_vintage": evidence.archive_scope_view_receipt["capture_identity_audit"]["status"] == "passed",
        "distribution_tax_accounting": normalization_complete and distribution_accounting["status"] == "passed",
        "identity_events": membership["status"] == "passed"}
    proofs.update({"snapshot_manifest": _proof(snapshot.path / "manifest.json"),
                   "calendar": _proof(calendar_path)})
    if proofs["adjustment_bridge_spec"] != temporal["source_rule"]:
        raise ValueError("Temporal source rule changed after runtime provenance capture")
    proofs.update({"scope:" + key: proof for key, proof in scope.source_receipts().items()})
    # Recheck every original, not just the top-level collector manifests.
    scope.assert_unchanged()
    load_qlib_snapshot(snapshot_path, verify=True)
    _assert_bound_inputs_unchanged(proofs)
    emit("write_canonical_audit_artifacts")
    output.mkdir(parents=True)
    payloads = {"coverage.json": coverage, "membership_audit.json": membership,
        "official_membership_audit.json": official_membership,
        "availability.json": availability, "raw_price_audit.json": bridge,
        "adjusted_price_temporal_audit.json": temporal,
        "ex_reference_audit.json": references, "source_review_queue.json": queue,
        "dividend_events.json": {"events": evidence.events}, "rights_events.json": rights,
        "intraday_observations.json": intraday, "identity_events.json": identities,
        "capital_reserve_tax_evidence.json": capital, "stock_dividend_tax_evidence.json": stock,
        "official_code_validity_view.json": code_view, "model_code_validity_audit.json": code_model,
        "distribution_tax_accounting.json": distribution_accounting,
        "distribution_tax_contracts.json": {"kind": "a5_stability_distribution_tax_contracts", "contracts": distribution_accounting["contracts"]},
        "distribution_normalization.json": {"receipts": evidence.normalization_receipts}}
    payloads["archive_scope_view.json"] = evidence.archive_scope_view_receipt
    for name, payload in payloads.items():
        _save(output / name, payload)
    # This is an explicitly derived identity table, never a new raw response.
    # Full original registry coverage is preserved for dated training windows;
    # actual buy/download eligibility still comes only from the bound scope.
    evidence.basic.sort_values("code").to_csv(output / "stock_basic.csv", index=False, lineterminator="\n")
    raw.sort_values(["instrument", "date"]).to_csv(output / "daily.csv", index=False, lineterminator="\n")
    proofs["stock_basic"] = _proof(output / "stock_basic.csv")
    proofs["daily_csv"] = _proof(output / "daily.csv")
    for name in ("dividend_events", "rights_events", "intraday_observations", "identity_events", "capital_reserve_tax_evidence", "stock_dividend_tax_evidence", "distribution_tax_accounting", "distribution_tax_contracts", "distribution_normalization", "archive_scope_view", "official_code_validity_view", "model_code_validity_audit"):
        proofs[name] = _proof(output / (name + ".json"))
    _assert_bound_inputs_unchanged(proofs)
    audit = {"schema_version": 1, "kind": "a5_stability_market_audit",
        "created_at": datetime.now(timezone.utc).isoformat(), "start": start, "end": end,
        "status": "passed" if all(checks.values()) and not source_preflight_only else "blocked",
        "source_preflight_only": source_preflight_only, "checks": checks,
        "raw_rows": len(raw), "instrument_count": len(evidence.instruments), "event_count": len(evidence.events),
        "unexplained_references": len(references["unexplained"]), "source_review_items": queue["item_count"],
        "unexplained_normalized_price_temporal_seams": len(temporal["failures"]),
        "missing_stock_dividend_tax_evidence": distribution_accounting["pending"],
        "partial_model_score_exceptions": bridge["partial_model_score_exceptions"],
        "certified_partial_source_chains": temporal.get("certified_partial_source_chains", []),
        "missing_capital_reserve_tax_evidence": [row["event_id"] for row in distribution_accounting["pending"]],
        "tax_classification_unresolved": distribution_accounting["tax_classification_unresolved"],
        "all_corporate_tax_source_classifications_verified": distribution_accounting["all_source_classifications_verified"],
        "unresolved_distribution_normalization_rows": sum(len(n["unresolved_source_rows"]) for n in evidence.normalization_receipts),
        "source_snapshot_manifest_sha256": proofs["snapshot_manifest"]["sha256"],
        "historical_scope_manifest_sha256": sha(Path(scope_path) / "manifest.json"),
        "stock_basic_path": proofs["stock_basic"]["path"], "stock_basic_sha256": proofs["stock_basic"]["sha256"],
        "daily_csv_path": proofs["daily_csv"]["path"], "daily_csv_sha256": proofs["daily_csv"]["sha256"],
        "archive_scope_view": evidence.archive_scope_view_receipt,
        "stock_basic_basis": "canonical_CSV_deduplicated_original_source_registry_identity_fields_agree_all_original_responses_bound",
        "bound_inputs": proofs, "audited_input_sha256": {key: proof["sha256"] for key, proof in proofs.items()},
        "historical_membership_full_announcement_time_PIT": False,
        "source_snapshot_atomic_supplier_vintage_proven": False,
        "portfolio_returns_computed": False, "outside_scope_new_download_targets": False,
        "limitations": ["Supplier history and effective memberships reconstructed now, not historical announcement-time PIT.",
                        "Explicit suspended source status is never inferred from absent prices.",
                        "Full noncash reserve accounting uses independently bound distribution tax contracts; incomplete source facts never authorize a formal pass.",
                        "The25/26 window is a historical stability recheck, not a new future out-of-sample claim.",
                        "Explicit legacy archives preserve their original complete-row consumption and unproven old empty-response server echoes."]}
    _save(output / "market_audit.json", audit)
    _save(output / "manifest.json", {"status": audit["status"], "bound_inputs": proofs,
        "files": {path.name: sha(path) for path in sorted(output.iterdir()) if path.is_file()}})
    return audit
