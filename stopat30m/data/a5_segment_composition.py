"""Explicit local composition of immutable whole and segmented source captures.

This module checks capture identity/provenance, not prices or profitability. It
never changes a vendor request, source row, adjustment flag, or failed archive.
The ordinary whole-security mathematical validator must still check its output.
"""
from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
import hashlib
import io
import json
from pathlib import Path
import re
import tempfile
from uuid import uuid4

import pandas as pd

from . import ashare_daily

FIELDS = ashare_daily.FIELDS.split(",")
MODES = {"raw": "3", "hfq": "1"}
PRICE_FIELDS = {"open", "high", "low", "close", "adjustflag"}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _day(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Require an exact ISO source date")
    date.fromisoformat(value)
    return value


def _instant(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("Source capture time must include its timezone")
    return result


def _read(path, bound):
    path = Path(path).resolve()
    raw = path.read_bytes()
    proof = {"path": str(path), "sha256": _sha(raw)}
    old = bound.setdefault(str(path), proof)
    if old != proof:
        raise ValueError("Source changed during local composition")
    return raw, proof


def _bound_tree(value, bound, *, base_dir=None):
    """Verify explicit material proofs in this consumed receipt.

    Referenced receipts themselves are byte-bound. Their semantics are parsed
    explicitly at the relevant gate below; archived diagnostic reports do not
    turn their then-current implementation into this composition's live code.
    """
    if isinstance(value, dict):
        if "path" in value and "sha256" in value:
            path = Path(value["path"])
            if not path.is_absolute():
                if base_dir is None:
                    raise ValueError("Relative bound source path lacks its containing receipt")
                path = Path(base_dir) / path
            raw, actual = _read(path, bound)
            if actual["sha256"] != value["sha256"]:
                raise ValueError("Bound original source SHA differs")
        for child in value.values():
            _bound_tree(child, bound, base_dir=base_dir)
    elif isinstance(value, list):
        for child in value:
            _bound_tree(child, bound, base_dir=base_dir)


def _manifest(directory, stop_receipt_path, bound):
    root = Path(directory).resolve()
    raw, proof = _read(root / "collection.json", bound)
    manifest = json.loads(raw)
    symbols = manifest.get("instruments", [])
    if (manifest.get("source") != "baostock" or manifest.get("schema_version") != 1
            or not isinstance(manifest.get("generation"), str) or not manifest["generation"]
            or not symbols or symbols != sorted(set(symbols))
            or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", symbol) for symbol in symbols)):
        raise ValueError("Invalid original collection provider/generation/inventory")
    _day(manifest["request_start"]); _day(manifest["request_end"]); _day(manifest["as_of_date"])
    if manifest["request_start"] > manifest["request_end"]:
        raise ValueError("Original request dates are reversed")
    responses = manifest.get("responses", {})
    expected = {symbol + "_" + mode for symbol in symbols for mode in MODES}
    if not set(responses).issubset(expected):
        raise ValueError("Original response inventory has an undeclared source key")
    if stop_receipt_path is None:
        if manifest.get("status") != "complete" or manifest.get("failures") or set(responses) != expected:
            raise ValueError("Segment collection must be complete with its entire inventory")
        selected = symbols
        stop_proof = None
    else:
        stop_raw, stop_proof = _read(stop_receipt_path, bound)
        stop = json.loads(stop_raw)
        _bound_tree(stop, bound, base_dir=Path(stop_receipt_path).resolve().parent)
        snapshot = stop["original_manifest_snapshot"]
        snapshot_raw, _ = _read(snapshot["path"], bound)
        successes = stop.get("original_successful_responses", [])
        by_key = {row["key"]: row for row in successes}
        if (manifest.get("status") != "partial_not_publishable" or not manifest.get("failures")
                or stop.get("status") != "stopped_after3_consecutive_failures_no_automatic_reconnect"
                or stop.get("raw_originals_modified") is not False
                or stop.get("old_snapshots_modified") is not False
                or stop.get("source_generation") != manifest["generation"]
                or snapshot_raw != raw or snapshot["sha256"] != proof["sha256"]
                or len(by_key) != len(successes) or set(by_key) != set(responses)
                or stop.get("success_count") != len(responses)
                or stop.get("failures") != manifest.get("failures")):
            raise ValueError("Stopped archive must exactly bind its unchanged partial manifest and every success")
        for key, record in responses.items():
            entry = by_key[key]
            if (entry["record"] != record or entry["original_response"]["sha256"] != record["sha256"]
                    or Path(entry["original_response"]["path"]).resolve() != root / record["file"]):
                raise ValueError("Stopped archive response inventory differs from the original stop receipt")
        selected = sorted({key.rsplit("_", 1)[0] for key in responses})
        if set(responses) != {symbol + "_" + mode for symbol in selected for mode in MODES}:
            raise ValueError("A stopped archive has an orphan mode; cannot silently omit a successful response")
    return root, manifest, proof, selected, stop_proof


def _guard_index(directories):
    index, seen = {}, set()
    for directory in directories:
        for path in sorted(Path(directory).glob("*.json")):
            path = path.resolve()
            if path in seen:
                continue
            seen.add(path)
            raw = path.read_bytes()
            guard = json.loads(raw)
            if guard.get("kind") != "baostock_daily_server_echo_guard" or not isinstance(guard.get("request"), dict):
                continue
            index.setdefault(_sha(_json(guard["request"])), []).append((path.resolve(), guard, _sha(raw)))
    return index


def _guard(payload, index, bound, allowed_mode_anomalies=()):
    request, fetched = payload["request"], _instant(payload["fetched_at"])
    candidates = []
    for path, guard, digest in index.get(_sha(_json(request)), []):
        started = _instant(guard["started_at"])
        if started <= fetched:
            candidates.append((started, path, guard, digest))
    if not candidates:
        raise ValueError("Missing source request/terminal-iteration guard")
    latest = max(row[0] for row in candidates)
    chosen = [row for row in candidates if row[0] == latest]
    if len(chosen) != 1:
        raise ValueError("Ambiguous source guard identity")
    started, path, guard, digest = chosen[0]
    _, proof = _read(path, bound)
    if proof["sha256"] != digest:
        raise ValueError("Source guard changed during local composition")
    echo = guard.get("server_echo")
    if (guard.get("status") != "verified" or guard.get("server_error_code") != "0"
            or guard.get("complete_row_iteration_observed") is not True
            or type(guard.get("rows_verified")) is not int
            or guard.get("rows_verified") != len(payload["rows"])
            or guard.get("row_mode_flags_repaired") is not False
            or guard.get("row_mode_anomalies_observed") != list(allowed_mode_anomalies)
            or not isinstance(echo, dict)
            or any(echo.get(key) != value for key, value in request.items() if key != "adjustflag")
            or not started <= _instant(guard["finished_at"]) <= fetched):
        raise ValueError("Source identity/terminal/rows/time/mode guard is not verified")
    present = guard.get("mode_server_echo_present")
    if ((present is True and echo.get("adjustflag") != request["adjustflag"])
            or (present is False and echo.get("adjustflag") not in ("", None))
            or type(present) is not bool):
        raise ValueError("Server adjustment-mode echo claim differs from the captured original")
    if "rows_sha256" in guard and guard["rows_sha256"] != _sha(_json(payload["rows"])):
        raise ValueError("Guard source-row hash differs from original response")
    return {**proof, "mode_server_echo_present": present,
        "mode_validation_basis": ("server_echo_and_all_original_row_flags" if present
            else "original_request_and_all_original_row_flags;server_mode_echo_absent"),
        "row_hash_recorded_in_original_guard": "rows_sha256" in guard,
        "original_mode_anomalies_retained": list(allowed_mode_anomalies),
        "explicit_factor_derivation_or_official_code_exclusion_required": bool(allowed_mode_anomalies)}


def _code_valid(day, limits):
    return (("valid_from" not in limits or day >= limits["valid_from"])
        and ("valid_before" not in limits or day < limits["valid_before"]))


def _response(root, manifest, symbol, mode, guards, bound, calendar, *, allowed_prefix_anomalies=(),
        code_validity_limits=None):
    record = manifest["responses"][symbol + "_" + mode]
    filename = record["file"]
    if Path(filename).name != filename or (root / filename).is_symlink():
        raise ValueError("Unsafe original response file")
    raw, proof = _read(root / filename, bound)
    if proof["sha256"] != record["sha256"]:
        raise ValueError("Original response SHA differs")
    payload = json.loads(raw)
    request = {"code": symbol[:2].lower() + "." + symbol[2:], "fields": ashare_daily.FIELDS,
        "start_date": manifest["request_start"], "end_date": manifest["request_end"],
        "frequency": "d", "adjustflag": MODES[mode]}
    fetched = _instant(payload["fetched_at"])
    if (proof["sha256"] != record["sha256"] or payload.get("request") != request
            or payload.get("generation") != manifest["generation"] or payload.get("fields") != FIELDS
            or record.get("fetched_at") != payload["fetched_at"]
            or fetched.astimezone(ashare_daily.SHANGHAI).date().isoformat() != manifest["as_of_date"]
            or type(record.get("rows")) is not int or record["rows"] != len(payload["rows"])):
        raise ValueError("Original response SHA/request/fields/generation/rows/vintage differs")
    dates, values = [], []
    observed_anomalies = []
    for index, row in enumerate(payload["rows"]):
        if len(row) != len(FIELDS) or not all(isinstance(value, str) for value in row):
            raise ValueError("Original source row shape/types differ")
        value = dict(zip(FIELDS, row)); day = _day(value["date"])
        if value["adjustflag"] != MODES[mode]:
            observed_anomalies.append({"row_index_zero_based": index, "date": day, "adjustflag": value["adjustflag"]})
        if (value["code"] != request["code"]
                or not request["start_date"] <= day <= request["end_date"]
                or day not in calendar):
            raise ValueError(f"Original row identity/mode/calendar differs: {symbol}/{mode}/{day}")
        if value["tradestatus"] not in {"0", "1"} or value["isST"] not in {"0", "1"}:
            raise ValueError("Unknown original trading/ST status")
        dates.append(day); values.append(value)
    if dates != sorted(set(dates)):
        raise ValueError("Original source dates are duplicate or unsorted")
    material_anomalies = [row for row in observed_anomalies if _code_valid(row["date"], code_validity_limits or {})]
    if material_anomalies != list(allowed_prefix_anomalies):
        raise ValueError(f"Original row identity/mode/calendar differs: {symbol}/{mode}/"
            + str(material_anomalies[0]["date"] if material_anomalies else "unexpected derivation contract"))
    anomalies = [{"date": row["date"], "requested_mode": MODES[mode], "returned_row_mode": row["adjustflag"]}
        for row in observed_anomalies]
    guard = _guard(payload, guards, bound, anomalies)
    return values, {"original_response": proof, "request": request,
        "original_generation": manifest["generation"], "fetched_at": payload["fetched_at"],
        "source_as_of_date": manifest["as_of_date"], "original_rows": len(values),
        "fields": list(FIELDS), "terminal_capture_guard": guard,
        "original_rows_sha256": _sha(_json(payload["rows"])),
        "code_invalid_archival_mode_anomalies": [row for row in observed_anomalies if row not in material_anomalies]}


def _official_code_validity(path, expected, calendar, bound):
    raw, proof = _read(path, bound)
    document = json.loads(raw)
    manifest_raw, manifest_proof = _read(Path(path).resolve().parent / "manifest.json", bound)
    manifest = json.loads(manifest_raw)
    if (document.get("kind") != "ashare_official_index_adjustment_events"
            or document.get("price_mapping_allowed") is not False
            or manifest.get("kind") != "ashare_official_event_membership_reconstruction"
            or manifest.get("status") != "passed" or manifest.get("files", {}).get("events.json") != proof["sha256"]):
        raise ValueError("Official security-code validity requires its unchanged passed no-price-mapping ledger")
    intervals, facts = {}, []
    for event in document["events"]:
        if event.get("event_type") != "same_issuer_security_code_change":
            continue
        old, new = event["old_instrument"], event["new_instrument"]
        if not {old, new} & set(expected):
            continue
        effective = _day(event["effective_on"])
        known = _instant(event["known_at"])
        if (event.get("price_mapping_allowed") is not False or event.get("same_legal_issuer") is not True
                or event.get("share_quantity_changes") is not False or effective not in calendar
                or event.get("first_effective_session") != effective
                or known >= _instant(effective + "T09:30:00+08:00")):
            raise ValueError("Official code validity clock is incomplete or not opening-known")
        _bound_tree(event["evidence"], bound, base_dir=Path(path).resolve().parent)
        for symbol, key in ((old, "valid_before"), (new, "valid_from")):
            limits = intervals.setdefault(symbol, {})
            if key in limits and limits[key] != effective:
                raise ValueError("Conflicting official security-code validity clocks")
            limits[key] = effective
        facts.append(event)
    return intervals, {"kind": "a5_official_code_validity_source_consumption_view",
        "official_events": proof, "official_scope_manifest": manifest_proof,
        "validity_intervals": intervals, "facts": facts, "old_code_price_mapping_allowed": False,
        "original_registry_ipo_dates_modified": False, "excluded_dates_are_not_missing_or_suspended_sessions": True}


def _prefix_derivation(contract_path, symbol, root, bound):
    """Recompute the narrow declared method; never accept caller-supplied frames."""
    from . import a5_ipo_prefix

    raw, contract_proof = _read(contract_path, bound)
    contract = json.loads(raw)
    _bound_tree(contract, bound, base_dir=Path(contract_path).resolve().parent)
    for path in (a5_ipo_prefix.__file__, a5_ipo_prefix.SPEC_PATH):
        _read(path, bound)
    if (contract.get("instrument") != symbol
            or Path(contract["original_collection_manifest"]["path"]).resolve().parent != root):
        raise ValueError("IPO prefix contract does not identify this original whole-source archive")
    factor_root = Path(contract["factor_evidence_manifest"]["path"]).resolve().parent
    raw_frame, adjusted_frame, receipt = a5_ipo_prefix.derive_two_session_ipo_hfq(
        root, symbol, factor_root, contract_path=contract_path)
    digest = lambda frame: _sha(frame.to_csv(index=False, float_format="%.17g", lineterminator="\n").encode())
    if (receipt.get("kind") != "a5_official_ipo_prefix_hfq_derivation"
            or receipt.get("status") != "passed" or receipt.get("instrument") != symbol
            or receipt.get("original_flags_modified") is not False or receipt.get("source_rows_removed") is not False
            or receipt.get("derived_frame_retains_original_source_mode_annotations") is not True
            or receipt.get("raw_frame_csv_sha256") != digest(raw_frame)
            or receipt.get("hfq_frame_csv_sha256") != digest(adjusted_frame)
            or raw_frame.columns.tolist() != FIELDS + ["instrument"]
            or adjusted_frame.columns.tolist() != FIELDS + ["instrument"]):
        raise ValueError("Recomputed IPO prefix method receipt/frames differ")
    _bound_tree(receipt, bound, base_dir=Path(contract_path).resolve().parent)
    return raw_frame, adjusted_frame, {**receipt, "contract": contract_proof}


def _single_prefix_derivation(contract_path, symbol, root, bound):
    """Reuse the frozen one-IPO-session method and retain source annotations."""
    from . import ashare_repair, a5_ipo_prefix

    raw, contract_proof = _read(contract_path, bound)
    contract = json.loads(raw)
    _bound_tree(contract, bound, base_dir=Path(contract_path).resolve().parent)
    for path in (ashare_repair.__file__, a5_ipo_prefix.__file__):
        _read(path, bound)
    manifest_raw, manifest_proof = _read(root / "collection.json", bound)
    manifest = json.loads(manifest_raw)
    if (contract.get("kind") != "a5_verified_single_session_ipo_factor_derivation_contract"
            or contract.get("status") != "source_only_frozen_before_full_ohlc_derivation"
            or contract.get("instrument") != symbol
            or contract.get("original_collection_manifest") != manifest_proof
            or contract.get("implementation", {}).get("sha256") != bound[str(Path(ashare_repair.__file__).resolve())]["sha256"]):
        raise ValueError("Wrong or changed frozen single-session IPO derivation contract")
    factor_proof = contract["factor_evidence_manifest"]
    factor_root = Path(factor_proof["path"]).resolve().parent
    factor_raw, actual_factor = _read(factor_proof["path"], bound)
    if actual_factor != factor_proof:
        raise ValueError("Single-session IPO original factor capture differs")
    factor = json.loads(factor_raw)
    source_payloads = {}
    for mode in MODES:
        original_path = root / manifest["responses"][symbol + "_" + mode]["file"]
        payload_raw, original_proof = _read(original_path, bound)
        if contract["original_" + mode + "_response"] != original_proof:
            raise ValueError("Single-session IPO original price response differs")
        payload = json.loads(payload_raw)
        a5_ipo_prefix._guard(contract["price_guard_proofs"][mode]["path"], payload["request"], payload, daily=True)
        source_payloads[mode] = payload
    for key in ("identity", "factors"):
        record = factor["responses"][key]
        path = factor_root / record["file"]
        payload_raw, original_proof = _read(path, bound)
        if original_proof["sha256"] != record["sha256"]:
            raise ValueError("Single-session IPO original factor/identity response changed")
        payload = json.loads(payload_raw)
        guard_request = {**payload["request"], "code_name": ""} if key == "identity" else payload["request"]
        a5_ipo_prefix._guard(contract["factor_guard_proofs"][key]["path"], guard_request, payload, daily=False)
    raw_frame, adjusted_frame, original_method = ashare_repair.derive_ipo_hfq(root, symbol, factor_root)
    original_hfq = pd.DataFrame(source_payloads["hfq"]["rows"], columns=FIELDS)
    if (len(original_hfq) != len(adjusted_frame)
            or original_hfq.date.tolist() != adjusted_frame.date.tolist()):
        raise ValueError("Single-session IPO derived rows do not align to the original HFQ response")
    adjusted_frame = adjusted_frame.assign(adjustflag=original_hfq.adjustflag.to_numpy())[FIELDS + ["instrument"]]
    raw_frame = raw_frame[FIELDS + ["instrument"]]
    # The original method intentionally drops a mode label on derived data.
    # This view retains the original label as an annotation, never as a claim
    # that the supplier returned a clean adjusted flag on its IPO session.
    method = original_method.get("method_evidence")
    if method is None:
        raise ValueError("Single-session IPO derivation requires the original official factor method document")
    method_raw, method_proof = _read(method["receipt_path"], bound)
    method_document = json.loads(method_raw)
    _, document_proof = _read(Path(method["receipt_path"]).parent / method_document["file"], bound)
    if (method_proof["sha256"] != method["receipt_sha256"]
            or document_proof["sha256"] != method_document["sha256"]):
        raise ValueError("Single-session IPO official factor method original changed")
    return raw_frame, adjusted_frame, {"kind": "a5_frozen_single_session_official_ipo_factor_derivation",
        "status": "passed", "instrument": symbol, "contract": contract_proof,
        "original_raw_response": contract["original_raw_response"], "original_hfq_response": contract["original_hfq_response"],
        "original_mode_anomalies": [{"row_index_zero_based": 0, "date": raw_frame.date.iloc[0], "adjustflag": "3"}],
        "original_method": original_method, "original_flags_modified": False, "source_rows_removed": False,
        "derived_frame_retains_original_source_mode_annotations": True,
        "original_method_implementation": bound[str(Path(ashare_repair.__file__).resolve())],
        "capture_guard_validator_implementation": bound[str(Path(a5_ipo_prefix.__file__).resolve())]}


def _validate_declaration(declaration, segments, sources, expected, days, bound):
    """Check the actual pre-capture inventory, rather than only hashing a plan."""
    if (declaration.get("kind") != "fixed42_three_nonoverlapping_segment_hard30_capture_protocol"
            or declaration.get("financial_policy_relaxed") is not False
            or declaration.get("synthetic_vendor_whole_request_claimed") is not False
            or declaration.get("automatic_reconnect") is not False
            or declaration.get("stop_after_consecutive_failures") != 3
            or declaration.get("hard_timeout_seconds_per_sdk_call") != 30
            or declaration.get("actual_min_interval_seconds_including_lazy_pages") != 2):
        raise ValueError("The original strict segmented capture declaration is required")
    old = sorted(symbol for source in sources if source["kind"].startswith("all_complete")
        for symbol in source["selected_instruments"])
    new = segments[0][3]
    original_selection = json.loads(Path(declaration["original_full109_selection"]["path"]).read_text())
    if (declaration.get("preserved_old_completed_pair_instruments") != old
            or declaration.get("remaining_instruments") != new or sorted(old + new) != expected
            or original_selection.get("instruments") != expected
            or original_selection.get("selection_uses_returns_or_model_metrics") is not False
            or declaration.get("completed_old_collection_manifests") != [source["manifest"]
                for source in sources if source["kind"].startswith("all_complete")]
            or len(declaration.get("segments", [])) != 3):
        raise ValueError("Predeclared segmented/archived target inventory differs")
    raw, _ = _read(declaration["calendar"]["path"], bound)
    if raw.decode().splitlines() != days:
        raise ValueError("Predeclared calendar differs from the consumed actual sessions")
    created = _instant(declaration["created_at"])
    for declared, (root, manifest, _, _, _) in zip(declaration["segments"], segments):
        snapshot = json.loads(Path(declared["predeclared_empty_generation_manifest"]["path"]).read_text())
        frozen_keys = ("schema_version", "source", "generation", "as_of_date", "instruments",
            "request_start", "request_end", "target_selection")
        if (Path(declared["directory"]).resolve() != root
                or declared["start_date"] != manifest["request_start"]
                or declared["end_date"] != manifest["request_end"]
                or declared["source_generation"] != manifest["generation"]
                or declared["actual_calendar_sessions"] != sum(manifest["request_start"] <= day <= manifest["request_end"] for day in days)
                or any(snapshot.get(key) != manifest.get(key) for key in frozen_keys)
                or snapshot.get("status") != "collecting" or snapshot.get("responses") != {} or snapshot.get("failures") != {}):
            raise ValueError("Actual segment differs from its predeclared empty generation")
    return created


def _capture_links(path, declaration, protocol_proof, segments, bound):
    raw, _ = _read(path, bound)
    receipt = json.loads(raw)
    _bound_tree(receipt, bound, base_dir=Path(path).resolve().parent)
    entries = receipt.get("entries", [])
    indexed = {entry["original_response"]["path"]: entry for entry in entries}
    expected = {str(root / record["file"]) for root, manifest, _, _, _ in segments
        for record in manifest["responses"].values()}
    if (receipt.get("kind") != "fixed42_three_segment_original_capture"
            or receipt.get("status") != "source_capture_complete_pending_exact109_composition_and_strict_whole_audit"
            or receipt.get("batch_generation") != declaration["batch_generation"]
            or receipt.get("capture_protocol") != protocol_proof
            or receipt.get("instruments") != segments[0][3]
            or receipt.get("source_manifests") != [row[2] for row in segments]
            or receipt.get("original_source_bytes_modified") is not False
            or receipt.get("source_snapshot_atomic_supplier_vintage_proven") is not False
            or receipt.get("synthetic_vendor_whole_request_claimed") is not False
            or len(indexed) != len(entries) or set(indexed) != expected
            or receipt.get("responses") != len(entries)
            or receipt.get("rows_both_modes") != sum(entry["rows"] for entry in entries)
            or receipt.get("verified_empty_responses") != sum(entry["rows"] == 0 for entry in entries)):
        raise ValueError("Segment capture sidecar does not exactly bind all declared original responses")
    finished = _instant(receipt["finished_at"])
    names = {str(root): declared["name"] for declared, (root, _, _, _, _) in zip(declaration["segments"], segments)}
    for original_path, entry in indexed.items():
        root = str(Path(original_path).parent)
        if (entry.get("segment") != names[root] or entry.get("key") + ".json" != Path(original_path).name
                or entry.get("row_hash_algorithm") != "UTF8_JSON_ensure_ascii_false_compact_no_sort_arrays"
                or _instant(entry["fetched_at"]) > finished):
            raise ValueError("Segment sidecar source key/row-hash method/time differs")
    return indexed


def _outside_listing(symbol, start, end, basic):
    if basic is None:
        raise ValueError("An empty source segment requires independently bound IPO/outDate identity")
    code = symbol[:2].lower() + "." + symbol[2:]
    if code not in basic:
        raise ValueError("Empty source segment lacks the target's verified stock identity")
    row = basic[code]
    ipo, out = row["ipoDate"], row["outDate"]
    if ipo and _day(ipo) > end:
        return {"reason": "segment_entirely_before_verified_ipo", "stock_basic_row": row}
    if out and _day(out) < start:
        return {"reason": "segment_entirely_after_verified_outDate", "stock_basic_row": row}
    raise ValueError("An empty source segment overlaps the verified listing interval")


def compose_segmented_rebuild_sources(completed_collections, segment_collections, *, expected_instruments,
        calendar, parent_request_start, parent_request_end, guard_directories, receipt_dir,
        capture_protocol_path=None, capture_receipt_path=None, stock_basic_path=None, calendar_path=None,
        two_session_prefix_contract_paths=(), official_code_validity_events_path=None,
        single_session_prefix_contract_paths=()):
    """Return unmodified ``(raw, hfq, composition)`` from fully bound originals.

    ``completed_collections`` items contain directory and stop_receipt_path for
    unchanged, permanently partial archives. Every successful original pair is
    retained. ``segment_collections`` are the three genuinely separate vendor
    requests; they never become a fabricated whole-window vendor collection.
    """
    output = Path(receipt_dir).resolve()
    if output.exists():
        raise ValueError("Preserve the previous local source-composition generation")
    start, end = _day(parent_request_start), _day(parent_request_end)
    expected = sorted(set(expected_instruments))
    if (not expected or len(expected) != len(expected_instruments)
            or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", symbol) for symbol in expected)):
        raise ValueError("Require the exact unique predeclared target inventory, not only its count")
    days = list(calendar)
    if not days or days != sorted(set(days)) or days[0] != start or days[-1] != end:
        raise ValueError("A unique complete parent actual-session calendar is required")
    for day in days:
        _day(day)
    actual = set(days)
    bound = {}
    for path in (__file__, ashare_daily.__file__):
        _read(path, bound)
    if calendar_path is not None:
        raw, _ = _read(calendar_path, bound)
        if raw.decode().splitlines() != days:
            raise ValueError("Consumed actual calendar differs from its bound original bytes")
    basic = None
    if stock_basic_path is not None:
        raw, _ = _read(stock_basic_path, bound)
        table = pd.read_csv(io.StringIO(raw.decode()), dtype=str, keep_default_na=False)
        if not {"code", "ipoDate", "outDate"}.issubset(table) or table.code.duplicated().any():
            raise ValueError("Canonical stock identity registry is malformed or duplicate")
        basic = table.set_index("code", drop=False).to_dict("index")
    declaration, protocol_proof = None, None
    if capture_protocol_path is not None:
        raw, protocol_proof = _read(capture_protocol_path, bound)
        declaration = json.loads(raw)
        _bound_tree(declaration, bound, base_dir=Path(capture_protocol_path).resolve().parent)
    guards = _guard_index(guard_directories)
    code_intervals, code_view = (_official_code_validity(official_code_validity_events_path, expected, actual, bound)
        if official_code_validity_events_path is not None else ({}, None))
    sources, by_symbol, whole_symbols = [], {}, set()
    for item in completed_collections:
        root, manifest, manifest_proof, selected, stop = _manifest(item["directory"], item["stop_receipt_path"], bound)
        if manifest["request_start"] > start or manifest["request_end"] < end:
            raise ValueError("A reused whole-request archive does not span the complete parent window")
        source = {"kind": "all_complete_pairs_from_unchanged_stopped_archive",
            "manifest": manifest_proof, "stop_receipt": stop, "selected_instruments": selected,
            "excluded_unreceived_instruments": sorted(set(manifest["instruments"]) - set(selected)),
            "actual_vendor_request": [manifest["request_start"], manifest["request_end"]]}
        sources.append(source)
        for symbol in selected:
            if symbol in by_symbol:
                raise ValueError("A security appears in more than one original source group")
            by_symbol[symbol] = [(root, manifest, manifest_proof)]
            whole_symbols.add(symbol)
    segments = [_manifest(directory, None, bound) for directory in segment_collections]
    if len(segments) != 3:
        raise ValueError("Require the three separately declared source request segments")
    previous = None
    target_segments = None
    for root, manifest, manifest_proof, selected, _ in segments:
        left, right = manifest["request_start"], manifest["request_end"]
        if ((previous is None and left != start) or (previous is not None
                and date.fromisoformat(left) != date.fromisoformat(previous) + timedelta(days=1))
                or not left <= right <= end or not 0 < sum(left <= day <= right for day in days) < 1000):
            raise ValueError("Segments must partition the whole window in order, each below1000 actual sessions")
        if target_segments is None:
            target_segments = set(selected)
        elif set(selected) != target_segments:
            raise ValueError("Every request segment must contain the same exact target inventory")
        previous = right
        sources.append({"kind": "original_segment_request", "manifest": manifest_proof,
            "selected_instruments": selected, "actual_vendor_request": [left, right]})
        for symbol in selected:
            if symbol in whole_symbols:
                raise ValueError("Whole and segmented target inventories overlap")
            by_symbol.setdefault(symbol, []).append((root, manifest, manifest_proof))
    if previous != end or set(by_symbol) != set(expected):
        raise ValueError("Composed sources do not cover the exact full-window target inventory")
    if any(output.is_relative_to(root) for root in [*[row[0] for row in segments],
            *[Path(item["directory"]).resolve() for item in completed_collections],
            *[Path(path).resolve() for path in guard_directories]]):
        raise ValueError("Composition receipts must not alter an original source archive")
    declared_at = (_validate_declaration(declaration, segments, sources, expected, days, bound)
        if declaration is not None else None)
    if (declaration is None) != (capture_receipt_path is None):
        raise ValueError("The segmented source declaration and final capture sidecar must be supplied together")
    capture_links = (_capture_links(capture_receipt_path, declaration, protocol_proof, segments, bound)
        if declaration is not None else {})
    prefix_contracts, single_contracts, used_contracts = {}, {}, set()
    for path in two_session_prefix_contract_paths:
        raw, proof = _read(path, bound)
        contract = json.loads(raw)
        symbol = contract["instrument"]
        if symbol in prefix_contracts or symbol not in whole_symbols:
            raise ValueError("Each IPO prefix contract must identify one selected original whole-source security")
        prefix_contracts[symbol] = proof["path"]
    for path in single_session_prefix_contract_paths:
        raw, proof = _read(path, bound)
        contract = json.loads(raw)
        symbol = contract["instrument"]
        if symbol in prefix_contracts or symbol in single_contracts or symbol not in expected:
            raise ValueError("Each single-session IPO contract must identify one selected source security")
        single_contracts[symbol] = {"path": proof["path"],
            "root": Path(contract["original_collection_manifest"]["path"]).resolve().parent}
    frames, provenance, securities, excluded_alias = {mode: [] for mode in MODES}, [], {}, []
    for symbol in expected:
        modes, source_records = {mode: [] for mode in MODES}, []
        for root, manifest, manifest_proof in by_symbol[symbol]:
            pair, proofs = {}, {}
            derived = (_prefix_derivation(prefix_contracts[symbol], symbol, root, bound)
                if symbol in prefix_contracts else None)
            if symbol in single_contracts and root == single_contracts[symbol]["root"]:
                derived = _single_prefix_derivation(single_contracts[symbol]["path"], symbol, root, bound)
                used_contracts.add(symbol)
            for mode in MODES:
                allowed = derived[2]["original_mode_anomalies"] if derived is not None and mode == "hfq" else ()
                pair[mode], proofs[mode] = _response(root, manifest, symbol, mode, guards, bound, actual,
                    allowed_prefix_anomalies=allowed, code_validity_limits=code_intervals.get(symbol, {}))
                if declared_at is not None and symbol not in whole_symbols:
                    guard_proof = proofs[mode]["terminal_capture_guard"]
                    guard = json.loads(Path(guard_proof["path"]).read_text())
                    if _instant(guard["started_at"]) < declared_at:
                        raise ValueError("Segment capture began before its source declaration")
                    item = capture_links[proofs[mode]["original_response"]["path"]]
                    if (item["original_response"] != proofs[mode]["original_response"]
                            or item["original_guard"] != {key: guard_proof[key] for key in ("path", "sha256")}
                            or item["source_generation"] != proofs[mode]["original_generation"]
                            or item["actual_request"] != proofs[mode]["request"]
                            or item["rows"] != proofs[mode]["original_rows"]
                            or item["rows_json_sha256"] != proofs[mode]["original_rows_sha256"]
                            or item["fetched_at"] != proofs[mode]["fetched_at"]
                            or item["original_mode_anomalies_preserved"] != guard["row_mode_anomalies_observed"]):
                        raise ValueError("Segment original response and its capture sidecar link differ")
            if ([row["date"] for row in pair["raw"]] != [row["date"] for row in pair["hfq"]]
                    or any(any(raw[key] != hfq[key] for key in FIELDS if key not in PRICE_FIELDS)
                        for raw, hfq in zip(pair["raw"], pair["hfq"]))):
                raise ValueError("Original raw/HFQ dates or nonprice fields disagree")
            empty = None
            if not pair["raw"]:
                valid_days = [day for day in days if manifest["request_start"] <= day <= manifest["request_end"]
                    and _code_valid(day, code_intervals.get(symbol, {}))]
                empty = ({"reason": "code_invalid_archive_only", "official_code_limits": code_intervals[symbol]}
                    if not valid_days else _outside_listing(symbol, valid_days[0], valid_days[-1], basic))
            original_pair = {mode: list(pair[mode]) for mode in MODES}
            if derived is not None:
                derived_raw = derived[0].drop(columns="instrument").to_dict("records")
                derived_hfq = derived[1].drop(columns="instrument").to_dict("records")
                if (derived_raw != pair["raw"]
                        or len(derived_hfq) != len(pair["hfq"])
                        or any(any(row[key] != original[key] for key in FIELDS if key not in {"open", "high", "low", "close"})
                            for row, original in zip(derived_hfq, pair["hfq"]))
                        or derived[2]["original_raw_response"] != proofs["raw"]["original_response"]
                        or derived[2]["original_hfq_response"] != proofs["hfq"]["original_response"]):
                    raise ValueError("IPO prefix derived frame changes original raw or nonprice/mode/source identity")
                pair["hfq"] = derived_hfq
            for mode in MODES:
                offset = len(modes[mode])
                source_indices = [index for index, row in enumerate(pair[mode])
                    if _code_valid(row["date"], code_intervals.get(symbol, {}))]
                source_index_set = set(source_indices)
                for index, row in enumerate(original_pair[mode]):
                    if index not in source_index_set:
                        excluded_alias.append({"instrument": symbol, "mode": mode, "date": row["date"],
                            "reason": "code_invalid_archive_only", "official_code_limits": code_intervals[symbol],
                            "original_response": proofs[mode]["original_response"],
                            "original_response_row_index_zero_based": index,
                            "original_row_values": [row[field] for field in FIELDS],
                            "canonical_original_row_sha256": _sha(_json([row[field] for field in FIELDS])),
                            "original_terminal_capture_guard": proofs[mode]["terminal_capture_guard"],
                            "old_code_price_mapping_allowed": False})
                pair[mode] = [pair[mode][index] for index in source_indices]
                for index, row in enumerate(pair[mode]):
                    original_index = source_indices[index]
                    provenance.append([symbol, mode, row["date"], offset + index,
                        proofs[mode]["original_response"]["sha256"], original_index,
                        _sha(_json([original_pair[mode][original_index][field] for field in FIELDS])),
                        _sha(_json([row[field] for field in FIELDS])), derived is not None and mode == "hfq"])
                modes[mode].extend(pair[mode])
            source_records.append({"collection_manifest": manifest_proof, "modes": proofs,
                "empty_segment_identity_evidence": empty, "original_response_rows_changed": False,
                "consumed_raw_rows": len(pair["raw"]), "consumed_hfq_rows": len(pair["hfq"]),
                "consumption_kind": "code_invalid_archive_only" if not pair["raw"] and original_pair["raw"]
                    else "official_code_clock_intersect_original_source_rows",
                "explicit_factor_derivation": derived[2] if derived is not None else None})
        if not modes["raw"]:
            raise ValueError("The composed security has no actual source observations in the parent window")
        for mode in MODES:
            rows = modes[mode]
            dates = [row["date"] for row in rows]
            if dates != sorted(set(dates)):
                raise ValueError("Composed source dates duplicate or cross segment boundaries out of order")
            frames[mode].append(pd.DataFrame(rows).assign(instrument=symbol))
        securities[symbol] = {"kind": ("immutable_original_whole_request_pair_with_explicit_official_factor_derivation"
            if symbol in prefix_contracts else "immutable_original_whole_request_pair") if len(source_records) == 1
            else "explicit_three_original_request_segments_local_row_concatenation",
            "sources": source_records, "raw_rows": len(modes["raw"]), "hfq_rows": len(modes["hfq"]),
            "source_first_observed_date": modes["raw"][0]["date"], "source_last_observed_date": modes["raw"][-1]["date"],
            "new_vendor_whole_request_claimed": False, "original_row_values_unmodified": True}
    if used_contracts != set(single_contracts):
        raise ValueError("A declared single-session IPO contract was not consumed by its exact source segment")
    raw, hfq = (pd.concat(frames[mode], ignore_index=True) for mode in MODES)
    background = {}
    if declaration is not None:
        selection_proof = declaration["original_full109_selection"]
        selection = json.loads(Path(selection_proof["path"]).read_text())
        original_preflight = selection.get("original_market_preflight")
        background = {"selection_receipt": selection_proof,
            "selection_basis": selection.get("selection_basis"),
            "selection_uses_returns_or_model_metrics": selection.get("selection_uses_returns_or_model_metrics"),
            "diagnostic_citation_used_for_current_source_pass": False,
            "old_diagnostic_implementation_reexecuted_or_claimed_current": False}
        if original_preflight is not None:
            diagnostic_raw, diagnostic_proof = _read(original_preflight["path"], bound)
            if diagnostic_proof["sha256"] != original_preflight["sha256"]:
                raise ValueError("Background failed-source selection diagnostic bytes changed")
            diagnostic = json.loads(diagnostic_raw)
            background.update(original_blocked_preflight=diagnostic_proof,
                archived_diagnostic_status=diagnostic.get("status"))
    background_paths = {row["path"] for row in background.values()
        if isinstance(row, dict) and "path" in row and "sha256" in row}
    composition = {"kind": "a5_immutable_archives_and_original_segments_local_composition",
        "local_composition_id": uuid4().hex, "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "source_identity_and_composition_verified_math_and_market_gate_required",
        "source": "baostock", "request_start": start, "request_end": end, "instruments": expected,
        "requested_window_is_local_derived_view": True, "new_vendor_whole_request_claimed": False,
        "atomic_supplier_vintage_verified": False, "supplier_generation_claimed": False,
        "original_payloads_or_mode_flags_modified": False, "price_or_factor_interpolation": False,
        "explicit_official_factor_derived_hfq_instruments": sorted(set(prefix_contracts) | set(single_contracts)),
        "official_code_validity_consumption_view": code_view,
        "excluded_code_alias_row_count_both_modes": len(excluded_alias),
        "raw_rows": len(raw), "hfq_rows": len(hfq), "raw_frame_csv_sha256": _sha(raw.to_csv(index=False).encode()),
        "hfq_frame_csv_sha256": _sha(hfq.to_csv(index=False).encode()), "calendar_values_sha256": _sha(_json(days)),
        "sources": sources, "securities": securities, "bound_inputs": list(bound.values()),
        "background_selection_citation": background,
        "consumed_original_evidence": [proof for path, proof in bound.items() if path not in background_paths],
        "capture_protocol_bound": capture_protocol_path is not None,
        "row_provenance_columns": ["instrument", "mode", "date", "composed_security_row_index_zero_based",
            "original_response_sha256", "original_response_row_index_zero_based", "canonical_original_row_sha256",
            "canonical_consumed_row_sha256", "explicit_official_factor_derived_hfq_row"],
        "profitability_or_full_market_validated": False}
    for proof in bound.values():
        if _sha(Path(proof["path"]).read_bytes()) != proof["sha256"]:
            raise ValueError("Original source or implementation changed before local composition publication")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".a5-source-composition-", dir=output.parent) as temporary:
        stage = Path(temporary) / "receipt"
        stage.mkdir()
        archive = stage / "runtime_source"
        archive.mkdir()
        runtime = {}
        for path, proof in bound.items():
            original = Path(path)
            if original.suffix == ".py" and original.parent == Path(__file__).resolve().parent:
                copied = archive / original.name
                copied.write_bytes(original.read_bytes())
                if _sha(copied.read_bytes()) != proof["sha256"]:
                    raise ValueError("Runtime source archive differs from its entry-time implementation")
                runtime[original.name] = {"path": str(output / "runtime_source" / original.name),
                    "sha256": proof["sha256"], "original_path": path}
        composition["runtime_source_archive"] = runtime
        p = stage / "row_provenance.jsonl"
        with p.open("wb") as stream:
            for row in provenance:
                stream.write(_json(row) + b"\n")
        composition["row_provenance"] = {"path": str(output / p.name), "sha256": _sha(p.read_bytes()), "rows": len(provenance)}
        excluded_path = stage / "excluded_code_alias_original_rows.jsonl"
        with excluded_path.open("wb") as stream:
            for row in excluded_alias:
                stream.write(_json(row) + b"\n")
        composition["excluded_code_alias_original_rows"] = {"path": str(output / excluded_path.name),
            "sha256": _sha(excluded_path.read_bytes()), "rows": len(excluded_alias)}
        (stage / "composition.json").write_text(json.dumps(composition, ensure_ascii=False, sort_keys=True,
            indent=2, allow_nan=False) + "\n")
        for proof in bound.values():
            if _sha(Path(proof["path"]).read_bytes()) != proof["sha256"]:
                raise ValueError("Original source or implementation changed during receipt serialization")
        if output.exists():
            raise ValueError("Preserve the prior concurrently created source-composition receipt")
        stage.rename(output)
    return raw, hfq, composition
