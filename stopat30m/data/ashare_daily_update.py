"""One bounded close batch, strict Qlib seam, and an immutable daily version.

The current index response selects prospective targets only. Historical index
intervals stay unchanged. Daily bars never authorize an earlier opening fill.
"""
from contextlib import redirect_stdout
from datetime import datetime, time as wall_time, timezone
import fcntl
import io
import json
from pathlib import Path
import re
import socket
import time
from uuid import uuid4

import numpy as np
import pandas as pd

from .ashare_daily import SHANGHAI, _rows, _save, _sha
from .ashare_execution_data import EXECUTION_FIELDS, load_execution_collection
from .ashare_repair import FACTOR_DOCUMENT
from .qlib_incremental import BridgePolicy, CanonicalIncrement, publish_increment, audit_bridge
from .qlib_snapshot import guard_qlib_output_path, load_qlib_snapshot, prepare_qlib_directory, read_qlib_field
from .research_calendar import Calendar, iso_day

EXPECTED_INDEX_SIZE = 300

def _now():
    return datetime.now(timezone.utc)


def _client():
    import baostock
    return baostock


def _instant(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Daily refresh time must include a timezone")
    return value.astimezone(SHANGHAI)


def _board(instrument):
    if instrument.startswith("SH688"):
        return "star"
    # SZSE's 2024 code-range table assigns 300000..309799 to ordinary
    # ChiNext shares (309800..309999 are depositary receipts).
    if instrument.startswith("SZ") and instrument[2:].isdigit() and 300000 <= int(instrument[2:]) <= 309799:
        return "chinext"
    if instrument.startswith("SH60"):
        return "sse_main"
    if instrument.startswith(("SZ000", "SZ001", "SZ002", "SZ003")):
        return "szse_main"
    raise ValueError("Unsupported source A-share board identity")


def _symbols(payload, session):
    if payload.get("query") != "query_hs300_stocks" or payload["request"] != {"date": session}:
        raise ValueError("Current index evidence does not identify this exact session")
    frame = pd.DataFrame(payload["rows"], columns=payload["fields"])
    if len(frame) != EXPECTED_INDEX_SIZE or "code" not in frame or frame.code.duplicated().any():
        raise ValueError("Current index source is empty or duplicated")
    symbols = sorted(frame.code.str.replace(".", "", regex=False).str.upper())
    if len(set(symbols)) != len(symbols) or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", i) for i in symbols):
        raise ValueError("Invalid or duplicate canonical index identities")
    for symbol in symbols:
        _board(symbol)
    if ("date" in frame and not frame.date.eq(session).all()
            or "updateDate" in frame and not frame.updateDate.eq(session).all()):
        raise ValueError("Current index source returned a different session")
    return symbols


def _response(root, receipt, key):
    record = receipt["responses"][key]
    filename = record["file"]
    if Path(filename).name != filename or (root / filename).is_symlink():
        raise ValueError("Unsafe original daily refresh response path")
    raw = (root / filename).read_bytes()
    payload = json.loads(raw)
    instant = _instant(datetime.fromisoformat(payload["fetched_at"]))
    if (_sha(raw) != record["sha256"] or payload["generation"] != receipt["generation"]
            or record["fetched_at"] != payload["fetched_at"] or record["rows"] != len(payload["rows"])
            or instant.date().isoformat() != receipt["as_of_date"]
            or instant.time().replace(tzinfo=None) < wall_time(15, 5)
            or instant > _instant(_now())):
        raise ValueError("Original close response checksum/identity/retrieval time differs")
    return payload


def _capture(root, session, start, parent, calendar_sha, progress, extras):
    """Same-day originals resume without changing successful response times."""
    path = root / "collection.json"
    request = {"start_date": start, "end_date": session, "parent_manifest_sha256": parent.manifest_sha256,
               "calendar_sha256": calendar_sha, "extra_instruments": extras}
    today = _instant(_now()).date().isoformat()
    if path.exists():
        receipt = json.loads(path.read_bytes())
        if (receipt.get("kind") != "baostock_daily_close_originals" or receipt.get("source") != "baostock"
                or receipt["request"] != request or receipt["as_of_date"] != today):
            raise ValueError("A different request/day requires a new daily collection directory")
        for key in receipt["responses"]:
            _response(root, receipt, key)
        if receipt["status"] == "captured":
            return receipt
    else:
        receipt = {"kind": "baostock_daily_close_originals", "schema_version": 1,
            "source": "baostock", "generation": uuid4().hex, "as_of_date": today,
            "request": request, "responses": {}, "status": "collecting", "failures": {},
            "atomic_supplier_vintage_verified": False, "live_opening_quote": False}
        _save(path, receipt)
    source, previous, logged = _client(), 0., False
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(8)
    def wait():
        nonlocal previous
        pause = .6 - (time.monotonic() - previous)
        if pause > 0:
            time.sleep(pause)
        previous = time.monotonic()
    def get(key, query, arguments):
        if key in receipt["responses"]:
            return _response(root, receipt, key)
        wait()
        with redirect_stdout(io.StringIO()):
            result = getattr(source, query)(**arguments)
            rows = _rows(result)
        instant = _instant(_now())
        if instant.date().isoformat() != today or instant.time().replace(tzinfo=None) < wall_time(15, 5):
            raise ValueError("A daily close collection cannot cross the session retrieval boundary")
        payload = {"query": query, "request": arguments, "fields": list(result.fields), "rows": rows,
            "generation": receipt["generation"], "fetched_at": instant.astimezone(timezone.utc).isoformat()}
        filename = key + ".json"
        digest = _save(root / filename, payload)
        receipt["responses"][key] = {"file": filename, "sha256": digest,
            "fetched_at": payload["fetched_at"], "rows": len(rows)}
        receipt["failures"].pop(key, None)
        _save(path, receipt)
        return payload
    try:
        with redirect_stdout(io.StringIO()):
            result = source.login()
        if str(result.error_code) != "0":
            raise ValueError("BaoStock login failed")
        logged = True
        index = get("current_index", "query_hs300_stocks", {"date": session})
        symbols = sorted(set(_symbols(index, session)) | set(extras))
        get("stock_basic", "query_stock_basic", {})
        for n, symbol in enumerate(symbols, 1):
            for mode, flag in (("raw", "3"), ("hfq", "1")):
                key = symbol + "_" + mode
                arguments = {"code": symbol[:2].lower() + "." + symbol[2:], "fields": EXECUTION_FIELDS,
                    "start_date": start, "end_date": session, "frequency": "d", "adjustflag": flag}
                try:
                    get(key, "query_history_k_data_plus", arguments)
                except Exception as exc:
                    receipt["failures"][key] = {"type": type(exc).__name__, "error": str(exc)[:300]}
                    receipt["status"] = "failed_not_publishable"
                    _save(path, receipt)
                    raise
            hfq = _response(root, receipt, symbol + "_hfq")
            table = pd.DataFrame(hfq["rows"], columns=hfq["fields"])
            if not table.empty and "adjustflag" in table and not table.adjustflag.eq("1").all():
                # A separate original factor response authorizes computation,
                # never the relabelling of the anomalous original flags.
                get(symbol + "_factors", "query_adjust_factor", {"code": symbol[:2].lower() + "." + symbol[2:],
                    "start_date": start, "end_date": session})
            if progress:
                progress(f"{n}/{len(symbols)} {symbol}: source pair captured")
        receipt["status"] = "captured"
        receipt["instruments"] = symbols
        _save(path, receipt)
        return receipt
    finally:
        socket.setdefaulttimeout(old_timeout)
        if logged:
            try:
                with redirect_stdout(io.StringIO()):
                    source.logout()
            except Exception:
                pass


def _frames(root, receipt):
    session = receipt["request"]["end_date"]
    index_symbols = _symbols(_response(root, receipt, "current_index"), session)
    symbols = sorted(set(index_symbols) | set(receipt["request"]["extra_instruments"]))
    expected = {"current_index", "stock_basic", *[i + "_" + m for i in symbols for m in ("raw", "hfq")]}
    optional = {i + "_factors" for i in symbols}
    if (receipt.get("kind") != "baostock_daily_close_originals" or receipt.get("source") != "baostock"
            or receipt["status"] != "captured" or receipt["failures"]
            or not expected.issubset(receipt["responses"]) or not set(receipt["responses"]).issubset(expected | optional)):
        raise ValueError("Incomplete original daily close response inventory")
    basic_payload = _response(root, receipt, "stock_basic")
    if basic_payload["query"] != "query_stock_basic" or basic_payload["request"] != {}:
        raise ValueError("Wrong daily security identity query")
    basic = pd.DataFrame(basic_payload["rows"], columns=basic_payload["fields"])
    if not {"code", "ipoDate", "outDate", "type"}.issubset(basic) or basic.code.duplicated().any():
        raise ValueError("Invalid original security identity table")
    parts = {"raw": [], "hfq": []}
    excluded, derivations = {}, {}
    for symbol in symbols:
        pairs = {}
        for mode, flag in (("raw", "3"), ("hfq", "1")):
            payload = _response(root, receipt, symbol + "_" + mode)
            expected_request = {"code": symbol[:2].lower() + "." + symbol[2:], "fields": EXECUTION_FIELDS,
                "start_date": receipt["request"]["start_date"], "end_date": session,
                "frequency": "d", "adjustflag": flag}
            if (payload["query"] != "query_history_k_data_plus" or payload["request"] != expected_request
                    or payload["fields"] != EXECUTION_FIELDS.split(",")):
                raise ValueError("Wrong daily original source instrument/fields/request")
            frame = pd.DataFrame(payload["rows"], columns=payload["fields"])
            if not frame.empty:
                days = pd.to_datetime(frame.date, errors="raise")
                if (not frame.code.eq(expected_request["code"]).all()
                        or mode == "raw" and not frame.adjustflag.eq(flag).all()
                        or days.duplicated().any() or not days.is_monotonic_increasing
                        or frame.date.lt(expected_request["start_date"]).any() or frame.date.gt(session).any()
                        or not frame.tradestatus.isin(["0", "1"]).all() or not frame.isST.isin(["0", "1"]).all()):
                    raise ValueError("Daily source mode/date/status is invalid; no flag relabelling or seam waiver")
            frame["instrument"] = symbol
            pairs[mode] = frame
        if pairs["raw"].date.tolist() != pairs["hfq"].date.tolist():
            raise ValueError("Daily raw/HFQ dates do not align; wait for the source to complete both modes")
        if pairs["raw"].empty:
            excluded[symbol] = "source_returned_no_bars"
            continue
        for name in ("tradestatus", "isST", "volume", "amount"):
            if not pairs["raw"][name].equals(pairs["hfq"][name]):
                raise ValueError("Daily raw/HFQ non-price source fields differ")
        if not pairs["hfq"].adjustflag.eq("1").all():
            factor_payload = _response(root, receipt, symbol + "_factors")
            pairs["hfq"], derivations[symbol] = _derive_ipo_pair(pairs["raw"], pairs["hfq"], basic,
                factor_payload, receipt["request"], receipt["responses"][symbol + "_factors"])
        elif symbol + "_factors" in receipt["responses"]:
            raise ValueError("Unexpected factor repair for an already valid HFQ response")
        parts["raw"].append(pairs["raw"]); parts["hfq"].append(pairs["hfq"])
    if not parts["raw"]:
        raise ValueError("No source histories are available for a strict daily update")
    return (pd.concat(parts["raw"], ignore_index=True), pd.concat(parts["hfq"], ignore_index=True),
            basic, index_symbols, symbols, excluded, derivations)


def _derive_ipo_pair(raw, hfq, basic, payload, request, response_record):
    """Verified IPO factor=1 plus official cumulative factors; entire OHLC is computed."""
    identity = basic.loc[basic.code.eq(raw.code.iloc[0])]
    factors = pd.DataFrame(payload["rows"], columns=payload["fields"])
    expected_request = {"code": raw.code.iloc[0], "start_date": request["start_date"], "end_date": request["end_date"]}
    if (len(identity) != 1 or identity.type.iloc[0] != "1" or identity.ipoDate.iloc[0] != raw.date.iloc[0]
            or len(raw) < 2 or payload["query"] != "query_adjust_factor" or payload["request"] != expected_request
            or not {"code", "dividOperateDate", "backAdjustFactor"}.issubset(factors) or factors.empty):
        raise ValueError("IPO reconstruction lacks full raw history and official source identity/factors")
    ipo = identity.ipoDate.iloc[0]
    pd.to_datetime(factors.dividOperateDate, errors="raise")
    values = pd.to_numeric(factors.backAdjustFactor, errors="raise").to_numpy(float)
    if (not factors.code.eq(raw.code.iloc[0]).all() or factors.dividOperateDate.duplicated().any()
            or not factors.dividOperateDate.is_monotonic_increasing or not np.isfinite(values).all()
            or (values <= 0).any() or factors.dividOperateDate.iloc[0] != raw.date.iloc[1] or values[0] != 1.
            or factors.dividOperateDate.gt(request["end_date"]).any()
            or not hfq.loc[hfq.date.eq(ipo), "adjustflag"].eq("3").all()
            or not hfq.loc[~hfq.date.eq(ipo), "adjustflag"].eq("1").all()):
        raise ValueError("Daily source does not satisfy the narrowly verified IPO unit-factor case")
    indices = factors.dividOperateDate.searchsorted(raw.date, side="right") - 1
    factor = np.array([1. if n < 0 else values[n] for n in indices])
    if any(n < 0 and day != ipo for n, day in zip(indices, raw.date)):
        raise ValueError("Unknown factor coverage before the initial official event")
    result, errors = raw.drop(columns="adjustflag").copy(), {}
    for field in ("open", "high", "low", "close"):
        derived = pd.to_numeric(raw[field], errors="raise").to_numpy(float) * factor
        observed = pd.to_numeric(hfq[field], errors="raise").to_numpy(float)
        if not np.isfinite(derived).all() or not np.isclose(derived, observed, rtol=1e-6, atol=1e-8).all():
            raise ValueError("Official-factor-derived daily OHLC differs from the original HFQ values")
        result[field] = derived
        errors[field] = float(np.max(np.abs(derived - observed)))
    return result, {"kind": "raw_times_official_backward_factors", "ipo_date": ipo,
        "initial_factor": 1., "method_source_url": FACTOR_DOCUMENT, "factor_response": response_record,
        "max_absolute_errors": errors, "original_flags_modified": False, "source_rows_removed": False,
        "atomic_supplier_vintage_verified": False}


def _result(snapshot, frame, basic, symbols, session, source_evidence, universe_evidence, collection_path, excluded,
            current_symbols=None):
    calendar = (snapshot.dataset_dir / "calendars/day.txt").read_text().splitlines()
    if session not in calendar:
        raise ValueError("Verified snapshot has no requested session/factor")
    current_symbols = symbols if current_symbols is None else current_symbols
    quotes, metadata, eligible = {}, {}, []
    basic = basic.set_index("code")
    for inst in symbols:
        code = inst[:2].lower() + "." + inst[2:]
        if code not in basic.index or str(basic.at[code, "type"]) != "1":
            raise ValueError("Daily source identity is missing or is not an A-share stock")
        ipo, out = str(basic.at[code, "ipoDate"]), str(basic.at[code, "outDate"])
        iso_day(ipo)
        if out:
            iso_day(out)
        group = frame.loc[frame.instrument.eq(inst) & frame.date.eq(session)]
        row = group.iloc[0] if len(group) == 1 else None
        st = bool(row.isST == "1") if row is not None else None
        metadata[inst] = {"instrument": inst, "board": _board(inst), "listing_date": ipo,
            "ipo_date": ipo, "out_date": out or None, "is_st": st, "status_session": session,
            "source": "baostock.query_stock_basic+daily_isST",
            "known_at": max(datetime.fromisoformat(source_evidence["identity_fetched_at"]),
                datetime.fromisoformat(source_evidence["raw_responses"][inst]["fetched_at"])).astimezone(timezone.utc).isoformat(),
            "listing_known_at": source_evidence["identity_fetched_at"],
            "status_known_at": source_evidence["raw_responses"][inst]["fetched_at"], "announcement_pit_verified": False}
        if out and out <= session:
            excluded[inst] = "source_identity_outDate_on_or_before_session"
        if row is None:
            excluded.setdefault(inst, "source_missing_session")
        elif row.tradestatus == "0":
            excluded.setdefault(inst, "source_tradestatus_0")
        quote = {"instrument": inst, "date": session, "session": session, "source": "baostock.daily_raw",
            "tradestatus": str(row.tradestatus) if row is not None else "unknown", "is_st": st,
            "open": None, "close": None, "preclose": None, "raw_open": None, "raw_close": None,
            "raw_preclose": None, "factor": None, "live_opening_quote": False, "bar_complete": False,
            "fetched_at": source_evidence["raw_responses"][inst]["fetched_at"],
            "source_response_sha256": source_evidence["raw_responses"][inst]["sha256"]}
        if row is not None and inst not in excluded:
            numbers = pd.to_numeric(row[["open", "high", "low", "close", "preclose", "volume", "amount"]], errors="raise")
            if not np.isfinite(numbers.to_numpy(float)).all() or not numbers.gt(0).all():
                raise ValueError("Daily traded source has incomplete price/preclose/share-volume/amount")
            factor_path = snapshot.dataset_dir / "features" / inst.lower() / "factor.day.bin"
            factor = read_qlib_field(factor_path, calendar)[calendar.index(session)]
            if not np.isfinite(factor) or factor <= 0:
                raise ValueError("A complete daily source quotation has no verified Qlib factor")
            for field in ("open", "high", "low", "close"):
                qprice = read_qlib_field(snapshot.dataset_dir / "features" / inst.lower() /
                    (field + ".day.bin"), calendar)[calendar.index(session)]
                if not np.isclose(qprice / factor, float(numbers[field]), rtol=1e-6, atol=1e-7):
                    raise ValueError("Current raw OHLC differs from the verified Qlib price/factor")
            for name in ("open", "close", "preclose"):
                quote[name] = quote["raw_" + name] = float(numbers[name])
            quote["factor"] = float(factor)
            quote["bar_complete"] = True
            if inst in current_symbols:
                eligible.append(inst)
        quotes[inst] = quote
    return {"snapshot_path": str(snapshot.path), "snapshot_manifest_sha256": snapshot.manifest_sha256,
        "collection_path": str(collection_path), "daily_quotes": quotes, "metadata": metadata,
        "current_instruments": current_symbols, "current_index_instruments": current_symbols,
        "eligible_current_instruments": eligible,
        "extra_instruments": sorted(set(symbols)-set(current_symbols)), "excluded": dict(sorted(excluded.items())),
        "universe_evidence": universe_evidence, "source_evidence": source_evidence,
        "session": session, "signal_ready": len(eligible) >= 20, "live_opening_quote": False}


def load_ashare_refresh(result_path):
    """Validate a committed refresh before page/process restart reuse."""
    path = Path(result_path)
    value = json.loads(path.read_bytes())
    unsigned = {k: v for k, v in value.items() if k != "result_sha256"}
    if _sha(json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()) != value["result_sha256"]:
        raise ValueError("Daily refresh result checksum differs")
    snapshot = load_qlib_snapshot(value["snapshot_path"])
    if snapshot.manifest_sha256 != value["snapshot_manifest_sha256"]:
        raise ValueError("Committed daily snapshot differs from the receipt")
    if value.get("bootstrap"):
        for record in value["source_evidence"]["bootstrap_inputs"]:
            if _sha(Path(record["path"]).read_bytes()) != record["sha256"]:
                raise ValueError("Offline bootstrap source evidence checksum differs")
        load_execution_collection(value["collection_path"])
        return value
    root = Path(value["collection_path"])
    raw = (root / "collection.json").read_bytes()
    if _sha(raw) != value["source_evidence"]["collection_manifest_sha256"]:
        raise ValueError("Committed daily original collection differs")
    receipt = json.loads(raw)
    for key in receipt["responses"]:
        _response(root, receipt, key)
    return value


def refresh_ashare_day(parent_snapshot, session, output_root, calendar_path, *, now=None, progress=None,
                        extra_instruments=()):
    """Download all current targets sequentially, validate all seams, publish once.

    A mode mismatch or an inadequate/mismatching seam blocks the entire update;
    this function does not silently promote a partial data version. An explicit
    source halt/missing day remains excluded with no invented finite price.
    """
    instant, day = _instant(now or _now()), iso_day(session)
    extras = sorted(set(extra_instruments))
    if len(extras) > 1000 or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", i) for i in extras):
        raise ValueError("Extra held/claim targets must be bounded canonical A-share identifiers")
    for i in extras:
        _board(i)
    if instant.date().isoformat() != day or instant.time().replace(tzinfo=None) < wall_time(15, 5):
        raise ValueError("Refresh requires the requested real session after 15:05 Shanghai time")
    calendar = Calendar.load(calendar_path)
    if not calendar.is_session(day):
        raise ValueError("Daily refresh requires a verified exchange session")
    root = guard_qlib_output_path(output_root)
    parent = load_qlib_snapshot(getattr(parent_snapshot, "path", parent_snapshot))
    old_days = (parent.dataset_dir / "calendars/day.txt").read_text().splitlines()
    if day < old_days[-1]:
        raise ValueError("A prospective daily refresh cannot move its parent calendar backwards")
    start = max((pd.Timestamp(day) - pd.Timedelta(days=400)).date().isoformat(), old_days[0])
    flags, old_set = dict(calendar.days), set(old_days)
    for d, flag in calendar.days:
        if old_days[0] <= d <= old_days[-1] and bool(flag) != (d in old_set):
            raise ValueError("Daily real calendar contradicts the immutable parent's exchange sessions")
    rows = []
    for stamp in pd.date_range(start, day):
        d = stamp.date().isoformat()
        if d <= old_days[-1]:
            flag = d in old_set
        elif d in flags:
            flag = bool(flags[d])
        else:
            raise ValueError("A real calendar is missing for the daily update tail")
        rows.append({"date": d, "is_open": int(flag)})
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".ashare-daily-refresh.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another A-share daily refresh is running") from None
        work = root / day
        result_path = work / "refresh_result.json"
        if result_path.exists():
            result = load_ashare_refresh(result_path)
            if not set(extras).issubset(result["daily_quotes"]):
                raise ValueError("Existing daily close batch omits an additional held/claim target")
            if parent.manifest_sha256 not in {result["source_evidence"]["parent_manifest_sha256"], result["snapshot_manifest_sha256"]}:
                raise ValueError("An existing daily refresh belongs to another parent version")
            return result
        work.mkdir(exist_ok=True)
        collection = work / "originals"
        collection.mkdir(exist_ok=True)
        try:
            receipt = _capture(collection, day, start, parent, _sha(Path(calendar_path).read_bytes()), progress, extras)
            raw, adjusted, basic, current_symbols, symbols, excluded, derivations = _frames(collection, receipt)
            source = {"source": "baostock", "generation": receipt["generation"],
                "parent_manifest_sha256": parent.manifest_sha256,
                "collection_manifest_sha256": _sha((collection / "collection.json").read_bytes()),
                "identity_fetched_at": receipt["responses"]["stock_basic"]["fetched_at"],
                "raw_responses": {inst: receipt["responses"][inst + "_raw"] for inst in symbols},
                "responses": receipt["responses"], "derivations": derivations, "atomic_supplier_vintage_verified": False,
                "calendar_path": str(Path(calendar_path).resolve()), "calendar_sha256": _sha(Path(calendar_path).read_bytes())}
            universe = {"source": "baostock.query_hs300_stocks", "date": day,
                "file": str(collection / "current_index.json"), **receipt["responses"]["current_index"],
                "prospective_targets_only": True, "historical_membership_backfilled": False}
            # Fail insufficient or invalid active-day quotes before cloning.
            available = raw.loc[raw.date.eq(day) & raw.tradestatus.eq("1")]
            codes = basic.set_index("code")
            for i in symbols:
                code = i[:2].lower()+"."+i[2:]
                if code not in codes.index or str(codes.at[code, "type"]) != "1":
                    raise ValueError("Current download target lacks verified stock identity")
                iso_day(str(codes.at[code, "ipoDate"]))
                if str(codes.at[code, "outDate"]):
                    iso_day(str(codes.at[code, "outDate"]))
            count = 0
            for _, row in available.iterrows():
                out = str(codes.at[row.code, "outDate"])
                if out and out <= day:
                    continue
                numbers = pd.to_numeric(row[["open", "high", "low", "close", "preclose", "volume", "amount"]], errors="raise")
                if not np.isfinite(numbers.to_numpy(float)).all() or not numbers.gt(0).all():
                    raise ValueError("Daily traded quote has incomplete raw price/preclose/units")
                if row.instrument in current_symbols:
                    count += 1
            if count < 20:
                raise ValueError("Fewer than 20 source-available current stocks; a new signal is forbidden")
            batch = CanonicalIncrement.from_frames(raw, adjusted, source="baostock", generation=receipt["generation"],
                calendar_rows=pd.DataFrame(rows), calendar_source="verified_parent_sessions+"+calendar.source,
                calendar_source_url=calendar.source_url,
                requested_start=start, requested_end=day, evidence=source)
            dataset = work / "dataset"
            policy = BridgePolicy(base_raw_volume_multiplier=100)
            if not dataset.exists():
                publish_increment(parent.dataset_dir, batch, dataset, policy, pilot_only=False)
            else:
                expected = audit_bridge(parent.dataset_dir, batch, policy)
                staged = json.loads((dataset/"incremental_manifest.json").read_bytes())
                if (staged["source_evidence"] != source or staged["generation"] != receipt["generation"]
                        or staged["source_bars_sha256"] != expected["source_bars_sha256"]
                        or staged["policy"] != json.loads(json.dumps(expected["policy"]))):
                    raise ValueError("Existing staged daily dataset binds another source generation/request")
            snapshot = prepare_qlib_directory(dataset, work / "versions", source=parent.manifest["source"],
                parent_snapshot=parent, increment_manifest_path=dataset / "incremental_manifest.json")
            result = _result(snapshot, raw, basic, symbols, day, source, universe, collection, excluded,
                current_symbols=current_symbols)
            if not result["signal_ready"]:
                raise ValueError("Fewer than 20 fully verified current stocks; a new signal is forbidden")
            result["result_sha256"] = _sha(json.dumps(result, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False).encode())
            _save(result_path, result)
            return result
        except Exception as exc:
            _save(work / ("failure_" + uuid4().hex + ".json"), {"at": _now().isoformat(),
                "session": day, "error_type": type(exc).__name__, "error": str(exc)[:300],
                "published_result": False})
            raise


def bootstrap_ashare_day(snapshot_path, index_response_path, execution_collection_path,
                         calendar_path, *, session, output_dir=None, supplements=()):
    """Reuse complete close evidence offline; require a separately dated index response.

    The index payload must preserve query='query_hs300_stocks', request={date},
    original fields/rows and a timezone-aware fetched_at. It is never inferred
    from the package's prior membership or today's stock-basic status.
    """
    day = iso_day(session)
    calendar = Calendar.load(calendar_path)
    if not calendar.is_session(day):
        raise ValueError("Offline bootstrap requires a verified exchange session")
    snapshot = load_qlib_snapshot(snapshot_path)
    index_path = Path(index_response_path).resolve()
    index_raw = index_path.read_bytes()
    index = json.loads(index_raw)
    symbols = _symbols(index, day)
    index_instant = _instant(datetime.fromisoformat(index["fetched_at"]))
    if index_instant.date().isoformat() != day or index_instant > _instant(_now()):
        raise ValueError("Offline index evidence was not actually observed on the requested session")
    raw, basic, receipt = load_execution_collection(execution_collection_path)
    records = receipt["responses"]
    raw_records = {inst: records[inst] for inst in symbols if inst in records}
    supplemental_proofs, supplemental_inputs = [], []
    for directory in supplements:
        additional, _, proof = load_daily_supplement(directory)
        if proof["session"] != day or proof["index_response_sha256"] != _sha(index_raw):
            raise ValueError("Supplement refers to a different current universe/session")
        if set(additional.instrument) & set(raw_records):
            raise ValueError("Supplement may only add missing original execution securities")
        raw = pd.concat([raw, additional], ignore_index=True)
        for inst in proof["instruments"]:
            raw_records[inst] = {**proof["responses"][inst+"_raw"], "source_root": str(Path(directory).resolve())}
        supplemental_proofs.append({"path": str(Path(directory).resolve()), "generation": proof["generation"],
            "manifest_sha256": proof["manifest_sha256"], "atomic_supplier_vintage_verified": False})
        supplemental_inputs.append({"path": str(Path(directory).resolve()/"supplement.json"), "sha256": proof["manifest_sha256"]})
        for record in proof["responses"].values():
            supplemental_inputs.append({"path": str(Path(directory).resolve()/record["file"]), "sha256": record["sha256"]})
    if not set(symbols).issubset(raw_records):
        raise ValueError("Current index contains stocks missing from the completed execution collection/supplement")
    source = {"source": "baostock", "generation": receipt["generation"],
        "collection_manifest_sha256": receipt["manifest_sha256"],
        "parent_manifest_sha256": snapshot.manifest_sha256,
        "identity_fetched_at": records["stock_basic"]["fetched_at"],
        "raw_responses": raw_records, "supplement_generations": supplemental_proofs,
        "atomic_supplier_vintage_verified": False,
        "bootstrap_inputs": [{"path": str(index_path), "sha256": _sha(index_raw)},
            {"path": str(Path(receipt["collection_path"]) / "execution_collection.json"),
             "sha256": receipt["manifest_sha256"]},
            {"path": str(Path(calendar_path).resolve()), "sha256": _sha(Path(calendar_path).read_bytes())}, *supplemental_inputs]}
    for record in source["raw_responses"].values():
        instant = _instant(datetime.fromisoformat(record["fetched_at"]))
        if instant.date().isoformat() != day or instant.time().replace(tzinfo=None) < wall_time(15, 5):
            raise ValueError("Offline source close response was not observed after 15:05 of the requested day")
    universe = {"source": "baostock.query_hs300_stocks", "date": day,
        "file": str(index_path), "sha256": _sha(index_raw), "fetched_at": index["fetched_at"],
        "prospective_targets_only": True, "historical_membership_backfilled": False}
    result = _result(snapshot, raw, basic, symbols, day, source, universe, receipt["collection_path"], {})
    if not result["signal_ready"]:
        raise ValueError("Fewer than 20 verified source stocks; bootstrap signal is forbidden")
    result["bootstrap"] = True
    result["result_sha256"] = _sha(json.dumps(result, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode())
    if output_dir is not None:
        root = guard_qlib_output_path(output_dir)
        path = root / "refresh_result.json"
        if path.exists():
            existing = load_ashare_refresh(path)
            if (existing["session"] != result["session"]
                    or existing["snapshot_manifest_sha256"] != result["snapshot_manifest_sha256"]
                    or existing["source_evidence"]["bootstrap_inputs"] != result["source_evidence"]["bootstrap_inputs"]
                    or existing["universe_evidence"]["sha256"] != result["universe_evidence"]["sha256"]):
                raise ValueError("Offline bootstrap output already binds different source inputs")
            # The committed receipt stays immutable, including across additive
            # display-schema changes. Reading it never retimestamps the source.
            return existing
        root.mkdir(parents=True, exist_ok=True)
        _save(path, result)
    return result


def collect_current_supplement(output_dir, instruments, start_date, session, *, index_response_path):
    """Small original pair capture for new constituents absent from an old close batch."""
    day, start = iso_day(session), iso_day(start_date)
    instant = _instant(_now())
    if instant.date().isoformat() != day or instant.time().replace(tzinfo=None) < wall_time(15, 5):
        raise ValueError("Current supplement requires the real session after 15:05")
    root = guard_qlib_output_path(output_dir)
    if root.exists():
        raise ValueError("Keep original supplement bytes; use a new directory")
    index_path = Path(index_response_path).resolve()
    index_raw = index_path.read_bytes()
    selected = sorted(set(instruments))
    if not selected or not set(selected).issubset(_symbols(json.loads(index_raw), day)) or start > day:
        raise ValueError("Supplement targets must belong to the actual current index response")
    root.mkdir(parents=True)
    receipt = {"kind": "baostock_current_execution_supplement", "source": "baostock",
        "generation": uuid4().hex, "as_of_date": day, "session": day, "request_start": start,
        "instruments": selected, "index_response_path": str(index_path), "index_response_sha256": _sha(index_raw),
        "responses": {}, "atomic_supplier_vintage_verified": False, "status": "collecting"}
    source, old_timeout, logged, previous = _client(), socket.getdefaulttimeout(), False, 0.
    socket.setdefaulttimeout(8)
    try:
        with redirect_stdout(io.StringIO()):
            login = source.login()
        if str(login.error_code) != "0":
            raise ValueError("BaoStock login failed")
        logged = True
        for inst in selected:
            for mode, flag in (("raw", "3"), ("hfq", "1")):
                wait = .6-(time.monotonic()-previous)
                if wait > 0:
                    time.sleep(wait)
                previous = time.monotonic()
                request = {"code": inst[:2].lower()+"."+inst[2:], "fields": EXECUTION_FIELDS,
                    "start_date": start, "end_date": day, "frequency": "d", "adjustflag": flag}
                with redirect_stdout(io.StringIO()):
                    result = source.query_history_k_data_plus(**request)
                    rows = _rows(result)
                payload = {"query": "query_history_k_data_plus", "request": request,
                    "fields": list(result.fields), "rows": rows, "generation": receipt["generation"],
                    "fetched_at": _now().isoformat()}
                filename = inst+"_"+mode+".json"
                digest = _save(root/filename, payload)
                receipt["responses"][inst+"_"+mode] = {"file": filename, "sha256": digest,
                    "rows": len(rows), "fetched_at": payload["fetched_at"]}
                _save(root/"supplement.json", receipt)
        receipt["status"] = "captured"
        _save(root/"supplement.json", receipt)
        load_daily_supplement(root)
        return root
    finally:
        socket.setdefaulttimeout(old_timeout)
        if logged:
            try:
                with redirect_stdout(io.StringIO()):
                    source.logout()
            except Exception:
                pass


def load_daily_supplement(path):
    """Validate original supplemental raw/HFQ responses, retaining both modes."""
    root = Path(path).resolve()
    raw_receipt = (root/"supplement.json").read_bytes()
    receipt = json.loads(raw_receipt)
    if (receipt.get("kind") != "baostock_current_execution_supplement" or receipt.get("source") != "baostock"
            or receipt["status"] != "captured"
            or set(receipt["responses"]) != {i+"_"+m for i in receipt["instruments"] for m in ("raw", "hfq")}):
        raise ValueError("Incomplete or incorrectly identified current supplement")
    index_raw = Path(receipt["index_response_path"]).read_bytes()
    if (_sha(index_raw) != receipt["index_response_sha256"]
            or not set(receipt["instruments"]).issubset(_symbols(json.loads(index_raw), receipt["session"]))):
        raise ValueError("Supplement current-universe evidence differs")
    frames = []
    adjusted = []
    for inst in receipt["instruments"]:
        pairs = {}
        for mode, flag in (("raw", "3"), ("hfq", "1")):
            payload = _response(root, receipt, inst+"_"+mode)
            request = {"code": inst[:2].lower()+"."+inst[2:], "fields": EXECUTION_FIELDS,
                "start_date": receipt["request_start"], "end_date": receipt["session"], "frequency": "d", "adjustflag": flag}
            frame = pd.DataFrame(payload["rows"], columns=payload["fields"])
            if (payload["query"] != "query_history_k_data_plus" or payload["request"] != request
                    or payload["fields"] != EXECUTION_FIELDS.split(",") or frame.empty
                    or not frame.code.eq(request["code"]).all() or not frame.adjustflag.eq(flag).all()
                    or not frame.tradestatus.isin(["0", "1"]).all() or not frame.isST.isin(["0", "1"]).all()
                    or frame.date.duplicated().any() or not frame.date.is_monotonic_increasing
                    or frame.date.lt(request["start_date"]).any() or frame.date.gt(request["end_date"]).any()):
                raise ValueError("Supplement source request/mode/date/status differs")
            pd.to_datetime(frame.date, errors="raise")
            frame["instrument"] = inst
            pairs[mode] = frame
        if pairs["raw"].date.tolist() != pairs["hfq"].date.tolist():
            raise ValueError("Supplement raw/HFQ dates do not align")
        frames.append(pairs["raw"]); adjusted.append(pairs["hfq"])
    # Callers publishing the pair must still pass the standard strict seam.
    return pd.concat(frames, ignore_index=True), pd.concat(adjusted, ignore_index=True), {
        **receipt, "manifest_sha256": _sha(raw_receipt)}
