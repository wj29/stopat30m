"""Narrow, source-bound two-session IPO factor derivation for A5 only."""
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .ashare_daily import FIELDS, SHANGHAI
from .ashare_repair import _original_mode, FACTOR_DOCUMENT

SPEC_PATH = Path(__file__).resolve().parents[2] / "docs/research/a5_two_session_ipo_hfq_spec_v1.json"
OHLC = ("open", "high", "low", "close")


def _proof(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _bind(tree, proofs, prefix):
    if isinstance(tree, dict):
        if "path" in tree and "sha256" in tree:
            path = Path(tree["path"])
            if not path.is_absolute() or _proof(path)["sha256"] != tree["sha256"]:
                raise ValueError("IPO prefix original proof changed or is not absolute")
            proofs[prefix] = dict(tree)
        for key, value in tree.items():_bind(value, proofs, prefix + ":" + str(key))
    elif isinstance(tree, list):
        for n, value in enumerate(tree):_bind(value, proofs, prefix + ":" + str(n))


def _guard(path, request, payload, *, daily):
    value = json.loads(Path(path).read_bytes())
    if (value.get("status") != "verified" or value.get("server_error_code") != "0"
            or value.get("request") != request or value.get("rows_verified") != len(payload["rows"])
            or value.get("complete_row_iteration_observed") is not True
            or not value["started_at"] <= value["finished_at"] <= payload["fetched_at"]):
        raise ValueError("IPO prefix source capture lacks an exact complete query guard")
    echo = value["server_echo"]
    if set(echo) != set(request) or any(v != request[k] and not (daily and k == "adjustflag" and v == "") for k, v in echo.items()):
        raise ValueError("IPO prefix original server request echo differs")
    return value


def derive_two_session_ipo_hfq(original_collection, instrument, factor_evidence_dir, *, contract_path):
    """Retain original labels; independently derive prices under one frozen contract."""
    root, factors_root = Path(original_collection).resolve(), Path(factor_evidence_dir).resolve()
    contract_path = Path(contract_path).resolve(); contract = json.loads(contract_path.read_bytes())
    spec = json.loads(SPEC_PATH.read_bytes())
    if (contract.get("kind") != "a5_verified_two_session_ipo_factor_derivation_contract"
            or contract.get("status") != "source_only_frozen_before_full_ohlc_derivation"
            or instrument != spec["allowed_instrument"] or contract["instrument"] != instrument
            or contract["spec"] != _proof(SPEC_PATH) or contract["implementation"] != _proof(__file__)
            or contract["original_collection_manifest"] != _proof(root / "collection.json")
            or contract["factor_evidence_manifest"] != _proof(factors_root / "factor_evidence.json")):
        raise ValueError("Wrong or changed two-session IPO source contract")
    proofs = {"contract": _proof(contract_path), "implementation": _proof(__file__), "spec": _proof(SPEC_PATH),
        "original_mode_parser": _proof(Path(__file__).parent / "ashare_repair.py"),
        "original_daily_schema": _proof(Path(__file__).parent / "ashare_daily.py")}
    _bind(contract, proofs, "contract")
    raw, raw_proof = _original_mode(root, instrument, "raw")
    original, hfq_proof = _original_mode(root, instrument, "hfq")
    source_manifest = json.loads((root / "collection.json").read_bytes())
    payloads = {}
    for mode in ("raw", "hfq"):
        path = root / source_manifest["responses"][instrument + "_" + mode]["file"]
        if contract["original_" + mode + "_response"] != _proof(path):raise ValueError("IPO original response differs")
        payloads[mode] = json.loads(path.read_bytes())
        guard = _guard(contract["price_guard_proofs"][mode]["path"], payloads[mode]["request"], payloads[mode], daily=True)
        observed = [{"date": day, "requested_mode": "1", "returned_row_mode": "3"} for day in raw.date.iloc[:2]] if mode == "hfq" else []
        if guard.get("row_mode_anomalies_observed") != observed:raise ValueError("IPO original row flags differ from capture guard")
    if (raw.columns.tolist() != FIELDS.split(",") + ["instrument"] or original.columns.tolist() != raw.columns.tolist()
            or len(raw) < 3 or not raw.adjustflag.eq("3").all() or raw.date.tolist() != original.date.tolist()
            or original.adjustflag.iloc[:2].tolist() != ["3", "3"] or not original.adjustflag.iloc[2:].eq("1").all()):
        raise ValueError("Only the two first IPO-session mixed-mode prefix is allowed")
    for name in ("code", "volume", "amount", "tradestatus", "isST"):
        if not raw[name].equals(original[name]):raise ValueError("Original raw/HFQ nonprice attributes differ")
    receipt = json.loads((factors_root / "factor_evidence.json").read_bytes())
    if (receipt.get("kind") != "baostock_official_factor_identity_evidence" or receipt.get("status") != "complete"
            or receipt.get("source") != "baostock" or receipt.get("instrument") != instrument
            or (receipt["request_start"], receipt["request_end"]) != (source_manifest["request_start"], source_manifest["request_end"])):
        raise ValueError("IPO factor capture identity/request/status differs")
    tables = {}
    for key in ("identity", "factors"):
        record = receipt["responses"][key]; path = factors_root / record["file"]
        if Path(record["file"]).name != record["file"] or _proof(path)["sha256"] != record["sha256"]:
            raise ValueError("IPO factor response SHA differs")
        proofs["factor_original_" + key] = _proof(path)
        payload = json.loads(path.read_bytes())
        request = {"code": raw.code.iloc[0]} if key == "identity" else {"code": raw.code.iloc[0], "start_date": receipt["request_start"], "end_date": receipt["request_end"]}
        instant = datetime.fromisoformat(payload["fetched_at"])
        if (payload["request"] != request or payload["generation"] != receipt["generation"]
                or payload["fetched_at"] != record["fetched_at"] or len(payload["rows"]) != record["rows"]
                or instant.tzinfo is None or instant.astimezone(SHANGHAI).date().isoformat() != source_manifest["as_of_date"]
                or payload["query"] != ("query_stock_basic" if key == "identity" else "query_adjust_factor")):
            raise ValueError("IPO factor original payload identity/time differs")
        fields = (["code", "code_name", "ipoDate", "outDate", "type", "status"] if key == "identity" else
                  ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"])
        if payload["fields"] != fields or any(len(row) != len(fields) for row in payload["rows"]):
            raise ValueError("IPO factor original schema differs")
        expected = {**request, "code_name": ""} if key == "identity" else request
        _guard(contract["factor_guard_proofs"][key]["path"], expected, payload, daily=False)
        tables[key] = pd.DataFrame(payload["rows"], columns=payload["fields"])
    identity, factors = tables["identity"], tables["factors"]
    calendar = Path(contract["calendar"]["path"]).read_text().splitlines()
    prefix = [day for day in calendar if day >= raw.date.iloc[0]][:3]
    if (len(identity) != 1 or identity.code.iloc[0] != raw.code.iloc[0] or identity.type.iloc[0] != "1"
            or identity.ipoDate.iloc[0] != raw.date.iloc[0] or raw.date.iloc[:3].tolist() != prefix
            or contract["prefix_sessions"] != prefix[:2] or not raw.tradestatus.iloc[:3].eq("1").all()
            or factors.empty or not factors.code.eq(raw.code.iloc[0]).all()
            or not factors.dividOperateDate.is_monotonic_increasing or factors.dividOperateDate.duplicated().any()
            or factors.dividOperateDate.iloc[0] != raw.date.iloc[2]
            or Decimal(factors.backAdjustFactor.iloc[0]) != Decimal("1")):
        raise ValueError("Official IPO/first unit-factor/actual two-session prefix is not established")
    dates = pd.to_datetime(factors.dividOperateDate, errors="raise")
    values = pd.to_numeric(factors.backAdjustFactor, errors="raise").to_numpy(float)
    if (not np.isfinite(values).all() or (values <= 0).any() or dates.isna().any()
            or factors.dividOperateDate.lt(raw.date.iloc[2]).any() or factors.dividOperateDate.gt(receipt["request_end"]).any()):
        raise ValueError("Official backward factor coverage/values differ")
    source_factors = []
    for day in raw.date:
        position = factors.dividOperateDate.searchsorted(day, side="right") - 1
        if position < 0 and day not in prefix[:2]:raise ValueError("Unknown factor coverage before its first event")
        source_factors.append(1.0 if position < 0 else values[position])
    derived = original.copy(deep=True); errors = {}
    for name in OHLC:
        expected = pd.to_numeric(raw[name], errors="raise").to_numpy(float) * source_factors
        actual = pd.to_numeric(original[name], errors="raise").to_numpy(float)
        if (not np.isfinite(expected).all() or not np.isfinite(actual).all()
                or not np.isclose(expected, actual, rtol=spec["ohlc_validation_rtol"], atol=spec["ohlc_validation_atol"]).all()):
            raise ValueError("Full-history official-factor derivation differs from original HFQ OHLC")
        derived[name] = expected; errors[name] = float(np.max(np.abs(expected - actual)))
    method = receipt["method_evidence"]; method_path = Path(method["receipt_path"])
    if _proof(method_path)["sha256"] != method["receipt_sha256"]:raise ValueError("Official factor method receipt changed")
    method_doc = json.loads(method_path.read_bytes()); pdf = method_path.parent / method_doc["file"]
    if (_proof(pdf)["sha256"] != method_doc["sha256"] or method_doc["source_url"] != FACTOR_DOCUMENT
            or not pdf.read_bytes().startswith(b"%PDF-")):
        raise ValueError("Official factor original method document changed")
    proofs["method_receipt"] = _proof(method_path); proofs["method_pdf"] = {**_proof(pdf), "url": FACTOR_DOCUMENT}
    for key, proof in list(proofs.items()):
        if _proof(proof["path"])["sha256"] != proof["sha256"]:raise ValueError("IPO derivation input changed during validation")
    digest = lambda frame: hashlib.sha256(frame.to_csv(index=False,float_format="%.17g",lineterminator="\n").encode()).hexdigest()
    return raw, derived, {"kind": "a5_official_ipo_prefix_hfq_derivation", "status": "passed", "instrument": instrument,
        "original_raw_response": contract["original_raw_response"], "original_hfq_response": contract["original_hfq_response"],
        "raw_frame_csv_sha256": digest(raw), "hfq_frame_csv_sha256": digest(derived),
        "original_mode_anomalies": [{"row_index_zero_based": n, "date": raw.date.iloc[n], "adjustflag": "3"} for n in (0,1)],
        "method": "raw_OHLC_times_independent_official_backward_factors_with_exact_two_verified_IPO_unit_prefix_sessions",
        "max_absolute_ohlc_validation_errors": errors, "validation_rtol": spec["ohlc_validation_rtol"], "validation_atol": spec["ohlc_validation_atol"],
        "original_flags_modified": False, "source_rows_removed": False, "atomic_supplier_vintage_verified": False,
        "derived_frame_retains_original_source_mode_annotations": True, "bound_inputs": proofs,
        "original_raw_source": raw_proof, "original_hfq_source": hfq_proof}
