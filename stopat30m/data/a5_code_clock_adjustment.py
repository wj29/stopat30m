"""One explicitly evidenced current-code adjustment basis reconstruction.

This is a local derived price view, not repaired supplier HFQ and not a mapping
of an old security code's observations. Original responses and flags stay intact.
"""
from datetime import datetime
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import ashare_repair, a5_ipo_prefix
from .ashare_daily import FIELDS, SHANGHAI

SPEC_PATH = Path(__file__).resolve().parents[2] / "docs/research/a5_code_clock_forward_adjustment_spec_v1.json"
PRICE = ("open", "high", "low", "close")


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _bind(tree, bound):
    if isinstance(tree, dict):
        if "path" in tree and "sha256" in tree:
            if not Path(tree["path"]).is_absolute():
                raise ValueError("Current-code source proof must use an absolute path")
            actual = _proof(tree["path"])
            if actual["sha256"] != tree["sha256"]:
                raise ValueError("Code-boundary original or declared implementation changed")
            bound[actual["path"]] = actual
        for value in tree.values():
            _bind(value, bound)
    elif isinstance(tree, list):
        for value in tree:
            _bind(value, bound)


def _six_decimal(value):
    if not isinstance(value, str) or value.count(".") != 1 or len(value.split(".")[1]) != 6:
        raise ValueError("Official factor precision must be the original six decimal places")
    number = Decimal(value)
    if not number.is_finite() or number <= 0:
        raise ValueError("Official forward/backward factors must be positive finite values")
    return number


def _ratio_interval(back, fore):
    half = Decimal("0.0000005")
    if fore <= half or back <= half:
        raise ValueError("Official factor rounding interval is not strictly positive")
    return (back-half)/(fore+half), (back+half)/(fore-half)


def derive_code_clock_forward_adjusted(collection_dir, factor_evidence_dir, *, contract_path):
    """Recompute a single frozen source-derived basis and retain all originals."""
    root, factor_root = Path(collection_dir).resolve(), Path(factor_evidence_dir).resolve()
    contract_path = Path(contract_path).resolve()
    contract = json.loads(contract_path.read_bytes())
    spec = json.loads(SPEC_PATH.read_bytes())
    instrument = spec["allowed_instrument"]
    boundary = spec["first_legal_code_session"]
    if (contract.get("kind") != "a5_verified_current_code_forward_factor_derivation_contract"
            or contract.get("status") != "source_only_frozen_before_full_ohlc_derivation"
            or contract.get("instrument") != instrument or instrument != "SZ302132"
            or boundary != "2025-02-17" or contract.get("spec") != _proof(SPEC_PATH)
            or contract.get("implementation") != _proof(__file__)
            or contract.get("original_collection_manifest") != _proof(root / "collection.json")
            or contract.get("factor_evidence_manifest") != _proof(factor_root / "factor_evidence.json")):
        raise ValueError("Wrong or changed frozen current-code adjustment contract")
    bound = {}
    _bind(contract, bound)
    for path in (contract_path, __file__, SPEC_PATH, ashare_repair.__file__, a5_ipo_prefix.__file__):
        item = _proof(path); bound[item["path"]] = item
    events = json.loads(Path(contract["official_code_events"]["path"]).read_bytes())["events"]
    code_events = [e for e in events if e.get("event_type") == "same_issuer_security_code_change"
        and e.get("new_instrument") == instrument]
    if len(code_events) != 1:
        raise ValueError("One exact official current-code identity event is required")
    event = code_events[0]
    if (event.get("old_instrument") != "SZ300114" or event.get("same_legal_issuer") is not True
            or event.get("share_quantity_changes") is not False or event.get("price_mapping_allowed") is not False
            or event.get("effective_on") != boundary or event.get("first_effective_session") != boundary
            or event.get("effective_basis") != "at_session_open"
            or datetime.fromisoformat(event["known_at"]) >= datetime.fromisoformat(boundary+"T09:30:00+08:00")):
        raise ValueError("Official code clock cannot authorize prices before its legal session")
    _bind(event.get("evidence", []), bound)
    calendar = Path(contract["calendar"]["path"]).read_text().splitlines()
    if calendar != sorted(set(calendar)) or boundary not in calendar:
        raise ValueError("Current code requires an exact ordered real session calendar")
    originals, payloads = {}, {}
    manifest = json.loads((root / "collection.json").read_bytes())
    for mode in ("raw", "hfq"):
        frame, source = ashare_repair._original_mode(root, instrument, mode)
        path = root / source["response"]["file"]
        if _proof(path) != contract["original_"+mode+"_response"]:
            raise ValueError("Current-code original price response differs")
        payload = json.loads(path.read_bytes());payloads[mode] = payload
        guard = a5_ipo_prefix._guard(contract["price_guard_proofs"][mode]["path"], payload["request"], payload, daily=True)
        requested = "3" if mode == "raw" else "1"
        anomalies = [{"date": r.date, "requested_mode": requested, "returned_row_mode": r.adjustflag}
            for r in frame.itertuples() if r.adjustflag != requested]
        if guard.get("row_mode_anomalies_observed") != anomalies:
            raise ValueError("Full original response flags differ from their capture guard")
        originals[mode] = frame
    raw_all, hfq_all = originals["raw"], originals["hfq"]
    if (raw_all.date.tolist() != hfq_all.date.tolist() or not raw_all.adjustflag.eq("3").all()
            or not raw_all.date.isin(calendar).all()):
        raise ValueError("Original source mode/date inventory differs")
    for field in ("code", "volume", "amount", "tradestatus", "isST"):
        if not raw_all[field].equals(hfq_all[field]):
            raise ValueError("Original raw/HFQ non-price fields differ")
    raw = raw_all.loc[raw_all.date.ge(boundary)].reset_index(drop=True)
    original_hfq = hfq_all.loc[hfq_all.date.ge(boundary)].reset_index(drop=True)
    expected_days = [d for d in calendar if boundary <= d <= manifest["request_end"]]
    if (raw.date.tolist() != expected_days or len(raw) < 2 or not original_hfq.adjustflag.eq("1").all()):
        raise ValueError("Legal code history must contain every exact source session with requested modes")
    factor_manifest = json.loads((factor_root / "factor_evidence.json").read_bytes())
    if (factor_manifest.get("kind") != "baostock_official_factor_identity_evidence"
            or factor_manifest.get("source") != "baostock" or factor_manifest.get("status") != "complete"
            or factor_manifest.get("instrument") != instrument
            or factor_manifest.get("request_start") != manifest["request_start"]
            or factor_manifest.get("request_end") != manifest["request_end"]):
        raise ValueError("Independent official metadata request must identify the original source window")
    tables = {}
    for key in ("identity", "factors"):
        record = factor_manifest["responses"][key]; path = factor_root / record["file"]
        if Path(record["file"]).name != record["file"] or _proof(path)["sha256"] != record["sha256"]:
            raise ValueError("Official metadata response bytes differ")
        bound[str(path)] = _proof(path)
        payload = json.loads(path.read_bytes())
        captured = datetime.fromisoformat(payload["fetched_at"])
        request = {"code": "sz.302132"} if key == "identity" else {
            "code": "sz.302132", "start_date": manifest["request_start"], "end_date": manifest["request_end"]}
        if (payload["request"] != request or payload["generation"] != factor_manifest["generation"]
                or payload["fetched_at"] != record["fetched_at"] or len(payload["rows"]) != record["rows"]
                or captured.tzinfo is None
                or captured.astimezone(SHANGHAI).date().isoformat() != manifest["as_of_date"]
                or payload["query"] != ("query_stock_basic" if key == "identity" else "query_adjust_factor")):
            raise ValueError("Official metadata identity, time, generation or complete row count differs")
        fields = (["code", "code_name", "ipoDate", "outDate", "type", "status"] if key == "identity" else
                  ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"])
        if payload["fields"] != fields or any(len(row) != len(fields) for row in payload["rows"]):
            raise ValueError("Official metadata original schema differs")
        expected_request = {**request, "code_name": ""} if key == "identity" else request
        a5_ipo_prefix._guard(contract["factor_guard_proofs"][key]["path"], expected_request, payload, daily=False)
        tables[key] = pd.DataFrame(payload["rows"], columns=payload["fields"])
    method = factor_manifest["method_evidence"]
    method_path = Path(method["receipt_path"]).resolve()
    if _proof(method_path)["sha256"] != method["receipt_sha256"]:
        raise ValueError("Official factor method receipt changed")
    method_doc = json.loads(method_path.read_bytes())
    pdf = method_path.parent / method_doc["file"]
    if (method_doc["source_url"] != ashare_repair.FACTOR_DOCUMENT
            or _proof(pdf)["sha256"] != method_doc["sha256"] or not pdf.read_bytes().startswith(b"%PDF-")):
        raise ValueError("Official original factor method differs")
    for path in (method_path, pdf):
        item = _proof(path); bound[item["path"]] = item
    identity, factors = tables["identity"], tables["factors"]
    if (len(identity) != 1 or identity.code.iloc[0] != "sz.302132" or identity.type.iloc[0] != "1"
            or identity.ipoDate.iloc[0] != "2010-08-27" or identity.ipoDate.iloc[0] >= boundary):
        raise ValueError("A code change must not be relabelled as a new IPO")
    if (len(factors) < 2 or not factors.code.eq("sz.302132").all()
            or not factors.dividOperateDate.is_monotonic_increasing or factors.dividOperateDate.duplicated().any()
            or factors.dividOperateDate.iloc[:2].tolist() != raw.date.iloc[:2].tolist()
            or not factors.dividOperateDate.isin(expected_days).all()):
        raise ValueError("Official factors must explicitly cover the legal code boundary and following session")
    fore = [_six_decimal(v) for v in factors.foreAdjustFactor]
    back = [_six_decimal(v) for v in factors.backAdjustFactor]
    if (fore[0] != fore[1] or back[0] != 1 or back[1] <= 1 or fore[-1] != 1):
        raise ValueError("The exact source backward-basis reset with continuous forward basis is not established")
    with localcontext() as ctx:
        ctx.prec = 60
        intervals = [_ratio_interval(b, f) for b, f in zip(back[1:], fore[1:])]
        lower, upper = max(v[0] for v in intervals), min(v[1] for v in intervals)
        first_interval = _ratio_interval(back[0], fore[0])
        if lower > upper or not (first_interval[1] < lower or first_interval[0] > upper):
            raise ValueError("Post-boundary backward/forward factors lack one source-precision-consistent scale")
        adjusted = raw.copy()
        original_errors = {field: 0. for field in PRICE}
        original_outlier = None
        for index, row in raw.iterrows():
            factor_index = factors.dividOperateDate.searchsorted(row.date, side="right")-1
            if factor_index < 0:
                raise ValueError("No official forward factor coverage for a legal code session")
            forward = fore[factor_index]/fore[0]
            for field in PRICE:
                source = Decimal(str(row[field]));observed = Decimal(str(original_hfq.at[index, field]))
                if not source.is_finite() or source <= 0 or not observed.is_finite() or observed <= 0:
                    raise ValueError("Current code source OHLC must be complete, positive and finite")
                expected = source*back[factor_index]
                error = abs(expected-observed)
                if error > Decimal("0.00000001") + abs(expected)*Decimal("0.000001"):
                    raise ValueError("Original HFQ disagrees with its independently supplied backward factor")
                original_errors[field] = max(original_errors[field], float(error))
                adjusted.at[index, field] = float(source*forward)
            if index == 0:
                original_outlier = {"date": row.date, "original_backward_factor": str(back[0]),
                    "original_HFQ_equals_raw": all(Decimal(str(row[f])) == Decimal(str(original_hfq.at[index,f])) for f in PRICE)}
        if not original_outlier["original_HFQ_equals_raw"]:
            raise ValueError("The declared first legal-session source outlier is not exactly raw-valued")
    # Only derived OHLC values change. Supplier mode annotations and non-price
    # fields stay those of the original requested HFQ response.
    adjusted["adjustflag"] = original_hfq.adjustflag
    raw, adjusted = raw[FIELDS.split(",")+["instrument"]], adjusted[FIELDS.split(",")+["instrument"]]
    for field in ("date", "code", "volume", "amount", "adjustflag", "tradestatus", "isST", "instrument"):
        if not adjusted[field].equals(original_hfq[field]):
            raise ValueError("Derived adjustment changed a supplier non-price field or mode annotation")
    excluded = [{"row_index_zero_based": i, "date": row.date,
        "reason": "before_official_current_code_clock_not_a_price_mapping"}
        for i, row in raw_all.iterrows() if row.date < boundary]
    result = {"kind": "a5_current_code_official_forward_factor_derived_adjustment", "status": "passed",
        "instrument": instrument, "first_legal_code_session": boundary, "contract": _proof(contract_path),
        "method": "raw_OHLC_times_latest_official_foreAdjustFactor_divided_by_first_legal_session_foreAdjustFactor",
        "first_legal_forward_factor": str(fore[0]), "legal_rows": len(raw), "excluded_archived_alias_rows": excluded,
        "original_first_session_backward_basis_outlier": original_outlier,
        "post_boundary_backward_over_forward_common_scale_rounding_interval": {"lower": str(lower), "upper": str(upper)},
        "official_factor_original_precision": "six decimal places; half-last-digit intervals only for independent scale validation",
        "original_HFQ_against_independent_backward_factor_max_absolute_errors": original_errors,
        "raw_frame_csv_sha256": hashlib.sha256(raw.to_csv(index=False,float_format="%.17g").encode()).hexdigest(),
        "derived_frame_csv_sha256": hashlib.sha256(adjusted.to_csv(index=False,float_format="%.17g").encode()).hexdigest(),
        "original_source_bytes_modified": False, "original_mode_flags_modified": False,
        "old_code_price_mapping_allowed": False, "independent_HFQ_supplier_confirmation_claimed": False,
        "price_factor_or_temporal_tolerance_relaxed": False, "bound_inputs": list(bound.values())}
    for item in result["bound_inputs"]:
        if _proof(item["path"]) != item:
            raise ValueError("A declared source changed during current-code derivation")
    return raw, adjusted, result
