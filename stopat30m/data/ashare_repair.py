"""Auditable whole-security source composition, without modifying originals.

The only derived-price repair is raw × official backward factors, anchored by
verified IPO identity. A composite is a local evidence set, not an atomic vendor
generation and not a way to waive generic adjustment-mode validation.
"""
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import socket
import time
from uuid import uuid4

import numpy as np
import pandas as pd

from .ashare_daily import FIELDS, SHANGHAI, _rows, _save, _sha, load_collection_frames
from .qlib_snapshot import guard_qlib_output_path

FACTOR_DOCUMENT = "https://www.baostock.com/helpdocs/pdf/BaoStock%E5%A4%8D%E6%9D%83%E5%9B%A0%E5%AD%90%E7%AE%80%E4%BB%8B.pdf"


def collect_factor_evidence(output_dir, instrument, start_date, end_date, *, client=None,
                            min_interval=.6, timeout=8, method_evidence_path=None):
    """Capture two explicitly identified source responses; never rewrite old data."""
    import baostock
    root = guard_qlib_output_path(output_dir)
    if root.exists():
        raise ValueError("Factor evidence requires a new directory")
    if min_interval < .5 or not 0 < timeout <= 60:
        raise ValueError("Factor evidence requires bounded sequential requests")
    code = instrument[:2].lower() + "." + instrument[2:]
    start, end = pd.Timestamp(start_date).date().isoformat(), pd.Timestamp(end_date).date().isoformat()
    if instrument not in {code.replace(".", "").upper()} or start > end:
        raise ValueError("Invalid factor evidence identity/dates")
    root.mkdir(parents=True)
    source, generation, logged = client or baostock, uuid4().hex, False
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    receipt = {"kind": "baostock_official_factor_identity_evidence", "source": "baostock",
        "instrument": instrument, "generation": generation, "request_start": start, "request_end": end,
        "responses": {}, "status": "collecting", "atomic_supplier_vintage_verified": False,
        "method_source_url": FACTOR_DOCUMENT}
    if method_evidence_path is not None:
        method_path = Path(method_evidence_path).resolve()
        method_raw = method_path.read_bytes()
        method = json.loads(method_raw)
        filename = method["file"]
        if Path(filename).name != filename or method["source_url"] != FACTOR_DOCUMENT:
            raise ValueError("Official factor method evidence identity differs")
        document = (method_path.parent / filename).read_bytes()
        if not document.startswith(b"%PDF-") or _sha(document) != method["sha256"]:
            raise ValueError("Official factor method document checksum differs")
        receipt["method_evidence"] = {"receipt_path": str(method_path), "receipt_sha256": _sha(method_raw),
            "document_sha256": method["sha256"], "source_url": FACTOR_DOCUMENT}
    try:
        with redirect_stdout(io.StringIO()):
            login = source.login()
        if str(login.error_code) != "0":
            raise ValueError("BaoStock login failed")
        logged = True
        for name, request, call in (
            ("identity", {"code": code}, lambda: source.query_stock_basic(code=code)),
            ("factors", {"code": code, "start_date": start, "end_date": end},
             lambda: source.query_adjust_factor(code=code, start_date=start, end_date=end))):
            result = call()
            payload = {"query": "query_stock_basic" if name == "identity" else "query_adjust_factor",
                "request": request, "fields": list(result.fields), "rows": _rows(result),
                "generation": generation, "fetched_at": datetime.now(timezone.utc).isoformat()}
            digest = _save(root / (name + ".json"), payload)
            receipt["responses"][name] = {"file": name + ".json", "sha256": digest,
                "fetched_at": payload["fetched_at"], "rows": len(payload["rows"])}
            _save(root / "factor_evidence.json", receipt)
            if name == "identity":
                time.sleep(min_interval)
        receipt["status"] = "complete"
        _save(root / "factor_evidence.json", receipt)
    finally:
        socket.setdefaulttimeout(old_timeout)
        if logged:
            with redirect_stdout(io.StringIO()):
                source.logout()
    return root


def _original_mode(root, symbol, mode):
    """Validate request identity/bytes; returned flags are not silently corrected."""
    root = Path(root).resolve()
    manifest_bytes = (root / "collection.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest["source"] != "baostock" or symbol not in manifest["instruments"]:
        raise ValueError("Wrong original factor-repair collection identity")
    record = manifest["responses"][symbol + "_" + mode]
    filename = record["file"]
    if Path(filename).name != filename or (root / filename).is_symlink():
        raise ValueError("Unsafe original response path")
    raw = (root / filename).read_bytes()
    payload = json.loads(raw)
    flag = "3" if mode == "raw" else "1"
    request = {"code": symbol[:2].lower() + "." + symbol[2:], "fields": FIELDS,
        "start_date": manifest["request_start"], "end_date": manifest["request_end"],
        "frequency": "d", "adjustflag": flag}
    instant = datetime.fromisoformat(payload["fetched_at"])
    if (_sha(raw) != record["sha256"] or payload["request"] != request
            or payload["generation"] != manifest["generation"] or payload["fields"] != FIELDS.split(",")
            or record["fetched_at"] != payload["fetched_at"] or instant.tzinfo is None
            or instant.astimezone(SHANGHAI).date().isoformat() != manifest["as_of_date"]
            or record["rows"] != len(payload["rows"])):
        raise ValueError("Original factor-repair response identity/checksum/time differs")
    frame = pd.DataFrame(payload["rows"], columns=payload["fields"])
    if frame.empty or not frame.code.eq(request["code"]).all():
        raise ValueError("Missing/wrong original factor-repair instrument")
    dates = pd.to_datetime(frame.date, errors="raise")
    if (dates.duplicated().any() or not dates.is_monotonic_increasing
            or frame.date.lt(request["start_date"]).any() or frame.date.gt(request["end_date"]).any()):
        raise ValueError("Invalid factor-repair original source dates")
    frame["instrument"] = symbol
    return frame, {"collection_path": str(root), "collection_manifest_sha256": _sha(manifest_bytes),
        "generation": manifest["generation"], "response": record, "request": request}


def derive_ipo_hfq(collection_dir, instrument, factor_evidence_dir):
    """Compute the entire HFQ series, retaining and checking anomalous originals.

    The narrowly declared anomaly is a source flag3 on the verified IPO date
    only. No row is dropped or relabelled; derived prices have a separate method.
    Every original HFQ numeric OHLC must independently agree with the factor
    computation, including the IPO. This does not admit arbitrary mixed modes.
    """
    raw, raw_proof = _original_mode(collection_dir, instrument, "raw")
    original, adjusted_proof = _original_mode(collection_dir, instrument, "hfq")
    root = Path(factor_evidence_dir).resolve()
    receipt_bytes = (root / "factor_evidence.json").read_bytes()
    receipt = json.loads(receipt_bytes)
    if (receipt.get("kind") != "baostock_official_factor_identity_evidence"
            or receipt.get("status") != "complete" or receipt.get("source") != "baostock"
            or receipt.get("instrument") != instrument or receipt.get("method_source_url") != FACTOR_DOCUMENT
            or receipt.get("request_start") != raw_proof["request"]["start_date"]
            or receipt.get("request_end") != raw_proof["request"]["end_date"]
            or set(receipt["responses"]) != {"identity", "factors"}):
        raise ValueError("Factor/IPO evidence does not identify the full source request")
    tables = {}
    for key, record in receipt["responses"].items():
        if Path(record["file"]).name != record["file"] or (root / record["file"]).is_symlink():
            raise ValueError("Unsafe factor/IPO evidence path")
        value = (root / record["file"]).read_bytes()
        payload = json.loads(value)
        request = ({"code": raw.code.iloc[0]} if key == "identity" else
            {"code": raw.code.iloc[0], "start_date": receipt["request_start"], "end_date": receipt["request_end"]})
        instant = datetime.fromisoformat(payload["fetched_at"])
        if (_sha(value) != record["sha256"] or payload["request"] != request
                or payload.get("query") != ("query_stock_basic" if key == "identity" else "query_adjust_factor")
                or payload["generation"] != receipt["generation"] or instant.tzinfo is None
                or payload["fetched_at"] != record["fetched_at"] or len(payload["rows"]) != record["rows"]):
            raise ValueError("Factor/IPO original evidence integrity differs")
        tables[key] = pd.DataFrame(payload["rows"], columns=payload["fields"])
    identity, factors = tables["identity"], tables["factors"]
    if (len(identity) != 1 or identity.code.iloc[0] != raw.code.iloc[0] or identity.type.iloc[0] != "1"
            or identity.ipoDate.iloc[0] != raw.date.iloc[0]
            or not raw.adjustflag.eq("3").all() or raw.date.tolist() != original.date.tolist()):
        raise ValueError("Cannot anchor a complete raw history at a verified IPO")
    ipo = identity.ipoDate.iloc[0]
    pd.to_datetime(factors.dividOperateDate, errors="raise")
    if (len(raw) < 2 or factors.empty or not factors.code.eq(raw.code.iloc[0]).all()
            or not factors.dividOperateDate.is_monotonic_increasing or factors.dividOperateDate.duplicated().any()
            or factors.dividOperateDate.iloc[0] != raw.date.iloc[1]
            or float(factors.backAdjustFactor.iloc[0]) != 1.
            or not original.loc[original.date.eq(ipo), "adjustflag"].eq("3").all()
            or not original.loc[~original.date.eq(ipo), "adjustflag"].eq("1").all()):
        raise ValueError("The narrow verified IPO/unit-factor anomaly is not established")
    values = pd.to_numeric(factors.backAdjustFactor, errors="raise").to_numpy(float)
    if not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError("Invalid official backward adjustment factors")
    if factors.dividOperateDate.lt(ipo).any() or factors.dividOperateDate.gt(receipt["request_end"]).any():
        raise ValueError("Official factor event lies outside the verified full history")
    source_factor = []
    for day in raw.date:
        index = factors.dividOperateDate.searchsorted(day, side="right") - 1
        # Only the verified IPO precedes the first supplied unit-factor row.
        if index < 0 and day != ipo:
            raise ValueError("Unknown official factor coverage before the first event")
        source_factor.append(1. if index < 0 else values[index])
    adjusted = raw.copy()
    adjusted = adjusted.drop(columns="adjustflag")  # This is derived data, not a relabelled source response.
    errors = {}
    for field in ("code", "volume", "amount", "tradestatus", "isST"):
        if not raw[field].equals(original[field]):
            raise ValueError("Original raw/HFQ non-price source attributes differ")
    for field in ("open", "high", "low", "close"):
        expected = pd.to_numeric(raw[field], errors="raise").to_numpy(float) * source_factor
        observed = pd.to_numeric(original[field], errors="raise").to_numpy(float)
        if (not np.isfinite(expected).all() or not np.isfinite(observed).all()
                or not np.isclose(expected, observed, rtol=1e-6, atol=1e-8).all()):
            raise ValueError("Independent official-factor reconstruction differs from source HFQ OHLC")
        errors[field] = float(np.max(np.abs(expected - observed)))
        adjusted[field] = expected
    proof = {"kind": "derived_hfq_from_raw_and_official_factors", "instrument": instrument,
        "method": "raw_OHLC_times_latest_backAdjustFactor_on_or_before_session",
        "ipo_anchor": {"date": ipo, "backward_factor": 1., "source_url": FACTOR_DOCUMENT},
        "raw": raw_proof, "anomalous_original_hfq": adjusted_proof,
        "factor_evidence_path": str(root), "factor_evidence_manifest_sha256": _sha(receipt_bytes),
        "factor_responses": receipt["responses"], "factor_generation": receipt["generation"],
        "original_anomaly": {"date": ipo, "returned_flag": "3", "requested_flag": "1"},
        "rows": len(raw), "max_absolute_OHLC_validation_errors": errors,
        "validation_rtol": 1e-6, "validation_atol": 1e-8,
        "original_flags_modified": False, "source_rows_removed": False,
        "atomic_supplier_vintage_verified": False,
        "limitations": ["BaoStock backward adjustment uses its price-change convention; it is not a shareholder cash/dividend ledger.",
            "Original factor/price queries are independently timestamped; supplier atomic vintage is not proven."]}
    if "method_evidence" in receipt:
        method = receipt["method_evidence"]
        method_path = Path(method["receipt_path"])
        method_raw = method_path.read_bytes()
        document = json.loads(method_raw)
        filename = document["file"]
        if (Path(filename).name != filename or _sha(method_raw) != method["receipt_sha256"]
                or document["source_url"] != FACTOR_DOCUMENT
                or _sha((method_path.parent / filename).read_bytes()) != method["document_sha256"]):
            raise ValueError("Official factor method evidence changed")
        proof["method_evidence"] = method
    return raw, adjusted, proof


def compose_rebuild_sources(original_collection, *, replacements, factor_repairs, receipt_dir):
    """Compose validated whole histories, keeping per-response origins and times."""
    original_root = Path(original_collection).resolve()
    original_bytes = (original_root / "collection.json").read_bytes()
    original = json.loads(original_bytes)
    selected = set(original["instruments"])
    overrides = set(replacements) | set(factor_repairs)
    if set(replacements) & set(factor_repairs) or not overrides.issubset(selected):
        raise ValueError("Invalid whole-history repair selection")
    root = guard_qlib_output_path(receipt_dir)
    if root.exists():
        raise ValueError("A source composition receipt requires a new directory")
    raw_parts, adjusted_parts, proofs = [], [], {}
    clean = sorted(selected - overrides)
    if clean:
        raw, adjusted, _ = load_collection_frames(original_root, instruments=clean)
        raw_parts.append(raw); adjusted_parts.append(adjusted)
        for instrument in clean:
            proofs[instrument] = {"kind": "unmodified_original_pair", "collection_path": str(original_root),
                "collection_manifest_sha256": _sha(original_bytes), "generation": original["generation"],
                "responses": {mode: original["responses"][instrument + "_" + mode] for mode in ("raw", "hfq")}}
    for instrument, directory in sorted(replacements.items()):
        raw, adjusted, manifest = load_collection_frames(directory, instruments=[instrument])
        if (manifest["request_start"], manifest["request_end"]) != (original["request_start"], original["request_end"]):
            raise ValueError("Whole-security replacement source request interval differs")
        raw_parts.append(raw); adjusted_parts.append(adjusted)
        proofs[instrument] = {"kind": "new_generation_complete_pair", "collection_path": str(Path(directory).resolve()),
            "collection_manifest_sha256": _sha((Path(directory) / "collection.json").read_bytes()),
            "generation": manifest["generation"], "responses": manifest["responses"]}
    for instrument, directory in sorted(factor_repairs.items()):
        raw, adjusted, proof = derive_ipo_hfq(original_root, instrument, directory)
        raw_parts.append(raw); adjusted_parts.append(adjusted); proofs[instrument] = proof
    raw, adjusted = pd.concat(raw_parts, ignore_index=True), pd.concat(adjusted_parts, ignore_index=True)
    raw = raw.sort_values(["instrument", "date"]).reset_index(drop=True)
    adjusted = adjusted.sort_values(["instrument", "date"]).reset_index(drop=True)
    # Strip vendor mode labels from the derived table; the method is in proof.
    # Raw labels remain untouched, and all originals remain byte-identical.
    receipt = {"kind": "whole_history_source_composition", "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(), "source": "baostock",
        "local_composition_id": uuid4().hex, "supplier_generation_claimed": False,
        "atomic_supplier_vintage_verified": False, "original_collection": str(original_root),
        "original_collection_manifest_sha256": _sha(original_bytes),
        "request_start": original["request_start"], "request_end": original["request_end"],
        "instruments": sorted(selected), "securities": proofs,
        "raw_frame_sha256": _sha(raw.to_csv(index=False, float_format="%.17g").encode()),
        "adjusted_frame_sha256": _sha(adjusted.to_csv(index=False, float_format="%.17g").encode()),
        "source_volume_unit": "shares", "source_amount_unit": "CNY"}
    root.mkdir(parents=True)
    _save(root / "rebuild_composite.json", receipt)
    return raw, adjusted, {**receipt, "receipt_path": str(root / "rebuild_composite.json"),
        "receipt_sha256": _sha((root / "rebuild_composite.json").read_bytes())}
