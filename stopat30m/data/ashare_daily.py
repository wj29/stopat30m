"""Bounded, resumable BaoStock raw/HFQ collections for a Qlib bridge.

An index snapshot can select download targets, never historical membership.
Original responses remain separate from the validated, publishable increment.
"""
from __future__ import annotations

from contextlib import redirect_stdout
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import socket
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

SHANGHAI = ZoneInfo("Asia/Shanghai")
FIELDS = "date,code,open,high,low,close,volume,amount,adjustflag,tradestatus,isST"


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _save(path, payload):
    raw = (json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(raw)
    temporary.replace(path)
    return _sha(raw)


def _rows(response):
    from baostock.common.contants import BAOSTOCK_PER_PAGE_COUNT

    rows = []
    while str(response.error_code) == "0":
        if not response.next():
            # BaoStock's SDK returns False on an empty/failed socket read
            # without changing error_code. A consumed full page still present
            # in the object proves it never received a terminal next page.
            page = getattr(response, "data", None)
            if (page is not None and len(page) == BAOSTOCK_PER_PAGE_COUNT
                    and getattr(response, "cur_row_num", 0) >= len(page)):
                raise ValueError("BaoStock pagination ended without a confirmed terminal page")
            break
        row = response.get_row_data()
        if len(row) != len(response.fields):
            raise ValueError("BaoStock response has a malformed row")
        rows.append(row)
    if str(response.error_code) != "0":
        raise ValueError(f"BaoStock query failed: {response.error_code}")
    return rows


def _index_selection_date(root, manifest):
    selection = manifest["target_selection"]
    if selection["kind"] == "explicit_download_targets":
        return None
    if selection["kind"] != "baostock_index_snapshot_for_download_targets_only":
        raise ValueError("Unknown original download target selection")
    name = selection["evidence_file"]
    if Path(name).name != name:
        raise ValueError("Invalid original download target evidence path")
    raw = (root / name).read_bytes()
    metadata = json.loads(raw)
    if _sha(raw) != selection["evidence_sha256"]:
        raise ValueError("Original download target evidence checksum differs")
    frame = pd.DataFrame(metadata["rows"], columns=metadata["fields"])
    symbols = sorted(set(frame.code.str.replace(".", "", regex=False).str.upper()))
    if symbols != manifest["instruments"]:
        raise ValueError("Original target evidence differs from the collection inventory")
    return metadata["requested_date"]


def collect_baostock_increment(output_dir, start_date, end_date, *, instruments=None,
                               index_date=None, min_interval=0.5, timeout=10, client=None,
                               progress=None):
    """Collect one original response generation; never publish partial data.

    Resume only the same request, same Shanghai date and validated original
    response bytes. Use a new directory on a later date. Calls are sequential;
    there is no automatic source switch or unbounded retry.
    """
    import fcntl
    import baostock as bs

    if min_interval < 0.5 or timeout <= 0 or timeout > 60:
        raise ValueError("Require a bounded timeout and at least 0.5 seconds between queries")
    start_date = pd.Timestamp(start_date).date().isoformat()
    end_date = pd.Timestamp(end_date).date().isoformat()
    if start_date > end_date:
        raise ValueError("Increment dates are reversed")
    from .qlib_snapshot import guard_qlib_output_path
    root = guard_qlib_output_path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".collection.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another A-share collection is already running") from None
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(timeout)
        source = client or bs
        logged_in = False
        previous = 0.0
        def wait():
            nonlocal previous
            remaining = min_interval - (time.monotonic() - previous)
            if remaining > 0:
                time.sleep(remaining)
            previous = time.monotonic()
        try:
            today = datetime.now(SHANGHAI).date().isoformat()
            path = root / "collection.json"
            if path.exists():
                cached = json.loads(path.read_text())
                if (cached["request_start"] != start_date or cached["request_end"] != end_date
                        or instruments is not None and sorted(set(instruments)) != cached["instruments"]):
                    raise ValueError("Resume request/pool differs; use a new collection directory")
                original_index_date = _index_selection_date(root, cached)
                if ((instruments is None) != (original_index_date is not None)
                        or instruments is None and original_index_date != (index_date or end_date)):
                    raise ValueError("Resume download-target selection/date differs; use a new collection directory")
                if cached["status"] == "complete":
                    load_collection_frames(root)
                    return root
                if cached["as_of_date"] != today:
                    raise ValueError("Use a new collection directory to refresh an incomplete older generation")
            with redirect_stdout(io.StringIO()):
                response = source.login()
            if str(response.error_code) != "0":
                raise ValueError("BaoStock login failed")
            logged_in = True
            if path.exists():
                manifest = json.loads(path.read_text())
                if (manifest["request_start"] != start_date or manifest["request_end"] != end_date
                        or manifest["as_of_date"] != today):
                    raise ValueError("Use a new collection directory for a different request/date")
                symbols = manifest["instruments"]
                if instruments is not None and sorted(set(instruments)) != symbols:
                    raise ValueError("Resume instrument pool differs from the original collection")
            else:
                selection = {"kind": "explicit_download_targets"}
                if instruments is None:
                    wait()
                    result = source.query_hs300_stocks(date=index_date or end_date)
                    rows = _rows(result)
                    if not rows:
                        raise ValueError("An empty current index query cannot define download targets")
                    metadata = {"fields": list(result.fields), "rows": rows,
                                "requested_date": index_date or end_date,
                                "fetched_at": datetime.now(timezone.utc).isoformat()}
                    _save(root / "download_targets.json", metadata)
                    frame = pd.DataFrame(rows, columns=result.fields)
                    instruments = [code.replace(".", "").upper() for code in frame.code]
                    selection = {"kind": "baostock_index_snapshot_for_download_targets_only",
                                 "historical_membership_proven": False,
                                 "evidence_file": "download_targets.json",
                                 "evidence_sha256": _sha((root / "download_targets.json").read_bytes())}
                symbols = sorted(set(instruments))
                if not symbols or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", code) for code in symbols):
                    raise ValueError("Require explicit canonical A-share download targets")
                manifest = {"schema_version": 1, "source": "baostock", "generation": uuid4().hex,
                            "request_start": start_date, "request_end": end_date,
                            "as_of_date": today, "instruments": symbols, "target_selection": selection,
                            "responses": {}, "failures": {}, "status": "collecting"}
                _save(path, manifest)
            consecutive_failures = 0
            for n, symbol in enumerate(symbols, 1):
                try:
                    for mode, flag in (("raw", "3"), ("hfq", "1")):
                        key = symbol + "_" + mode
                        file = root / (key + ".json")
                        request = {"code": symbol[:2].lower() + "." + symbol[2:], "fields": FIELDS,
                                   "start_date": start_date, "end_date": end_date,
                                   "frequency": "d", "adjustflag": flag}
                        if key in manifest["responses"]:
                            raw = file.read_bytes()
                            saved = json.loads(raw)
                            if (_sha(raw) != manifest["responses"][key]["sha256"]
                                    or saved["request"] != request or saved["generation"] != manifest["generation"]):
                                raise ValueError("Original response integrity/identity check failed")
                            continue
                        wait()
                        result = source.query_history_k_data_plus(**request)
                        payload = {"request": request, "fields": list(result.fields), "rows": _rows(result),
                                   "generation": manifest["generation"],
                                   "fetched_at": datetime.now(timezone.utc).isoformat()}
                        digest = _save(file, payload)
                        manifest["responses"][key] = {"file": file.name, "sha256": digest,
                            "fetched_at": payload["fetched_at"], "rows": len(payload["rows"])}
                        _save(path, manifest)
                    manifest["failures"].pop(symbol, None)
                    consecutive_failures = 0
                except Exception as exc:
                    consecutive_failures += 1
                    manifest["failures"][symbol] = {"error_type": type(exc).__name__, "error": str(exc)[:300]}
                    _save(path, manifest)
                if progress:
                    progress(f"{n}/{len(symbols)} {symbol}: {'failed' if symbol in manifest['failures'] else 'collected'}")
                if consecutive_failures >= 3:
                    manifest["stopped_after_consecutive_failures"] = consecutive_failures
                    break
            manifest["status"] = "complete" if not manifest["failures"] and len(manifest["responses"]) == 2 * len(symbols) else "partial_not_publishable"
            manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            _save(path, manifest)
            return root
        finally:
            socket.setdefaulttimeout(old_timeout)
            if logged_in:
                try:
                    with redirect_stdout(io.StringIO()):
                        source.logout()
                except Exception:
                    pass


def load_collection_frames(path, *, instruments=None):
    """Validate original responses and return unmodified raw/HFQ long frames."""
    root = Path(path)
    manifest = json.loads((root / "collection.json").read_text())
    expected_keys = {symbol + "_" + mode for symbol in manifest["instruments"] for mode in ("raw", "hfq")}
    if manifest.get("source") != "baostock":
        raise ValueError("Collection source is not the declared BaoStock provider")
    if manifest["status"] != "complete" or manifest["failures"]:
        raise ValueError("An incomplete original collection cannot become a Qlib increment")
    if set(manifest["responses"]) != expected_keys:
        raise ValueError("Original raw/HFQ response inventory differs")
    _index_selection_date(root, manifest)
    selected = manifest["instruments"] if instruments is None else sorted(set(instruments))
    if not selected or not set(selected).issubset(manifest["instruments"]):
        raise ValueError("Requested source subset is outside the original collection")
    frames = {"raw": [], "hfq": []}
    for symbol in selected:
        source_dates = {}
        for mode, flag in (("raw", "3"), ("hfq", "1")):
            key = symbol + "_" + mode
            record = manifest["responses"][key]
            if Path(record["file"]).name != record["file"]:
                raise ValueError("Invalid source response path")
            raw = (root / record["file"]).read_bytes()
            payload = json.loads(raw)
            expected = {"code": symbol[:2].lower() + "." + symbol[2:], "fields": FIELDS,
                        "start_date": manifest["request_start"], "end_date": manifest["request_end"],
                        "frequency": "d", "adjustflag": flag}
            fetched = datetime.fromisoformat(payload["fetched_at"])
            if (_sha(raw) != record["sha256"] or payload["request"] != expected
                    or payload["generation"] != manifest["generation"] or fetched.tzinfo is None
                    or fetched.astimezone(SHANGHAI).date().isoformat() != manifest["as_of_date"]
                    or payload["fields"] != FIELDS.split(",")):
                raise ValueError("Original response identity/checksum/retrieval date differs")
            frame = pd.DataFrame(payload["rows"], columns=payload["fields"])
            if frame.empty:
                raise ValueError(f"No original daily bars for {symbol}; not a zero-return security")
            frame["instrument"] = symbol
            if not frame.code.eq(expected["code"]).all() or not frame.adjustflag.eq(flag).all():
                raise ValueError("Wrong source instrument or adjustment mode")
            dates = pd.to_datetime(frame.date, errors="raise")
            if (dates.duplicated().any() or not dates.is_monotonic_increasing
                    or frame.date.lt(manifest["request_start"]).any()
                    or frame.date.gt(manifest["request_end"]).any()):
                raise ValueError("Original source daily dates are duplicate, unsorted or outside the request")
            source_dates[mode] = frame.date.tolist()
            frames[mode].append(frame)
        if source_dates["raw"] != source_dates["hfq"]:
            raise ValueError(f"Original raw/HFQ dates differ for {symbol}; possible incomplete source pagination")
    return pd.concat(frames["raw"], ignore_index=True), pd.concat(frames["hfq"], ignore_index=True), manifest
