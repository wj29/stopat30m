"""Original daily execution evidence, distinct from a live opening quote.

Daily preclose is the supplier's ex-right reference, never the prior raw close.
Today's stock-basic table is identity evidence, not historical ST membership.
"""
from contextlib import redirect_stdout
from datetime import datetime, timezone
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

from .ashare_daily import _rows, _save, _sha, SHANGHAI
from .qlib_snapshot import guard_qlib_output_path

EXECUTION_FIELDS = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,tradestatus,isST"


def historical_download_targets(interval_path, start_date, end_date):
    """Select actual overlapping package intervals; never backfill today's pool."""
    start, end = pd.Timestamp(start_date).date().isoformat(), pd.Timestamp(end_date).date().isoformat()
    symbols = set()
    for line in Path(interval_path).read_text().splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) != 3:
            raise ValueError("Invalid original historical membership interval")
        instrument, begin, finish = parts
        if not re.fullmatch(r"(?:SH|SZ)\d{6}", instrument):
            # Old aliases outside the requested period are preserved in the
            # source file; only overlapping executable-stock targets must parse.
            if begin <= end and finish >= start:
                raise ValueError("Historical target has an unsupported security identity")
            continue
        pd.Timestamp(begin); pd.Timestamp(finish)
        if begin > finish:
            raise ValueError("Reversed historical membership interval")
        if begin <= end and finish >= start:
            symbols.add(instrument)
    return sorted(symbols)


def collect_execution_data(output_dir, start_date, end_date, *, instruments,
                           selection_evidence_path, min_interval=.6, timeout=8,
                           client=None, progress=None):
    """Sequential, bounded, resumable original raw daily and security identity capture."""
    import baostock
    start, end = pd.Timestamp(start_date).date().isoformat(), pd.Timestamp(end_date).date().isoformat()
    symbols = sorted(set(instruments))
    if (start > end or not symbols or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", x) for x in symbols)
            or min_interval < .5 or not 0 < timeout <= 60):
        raise ValueError("Invalid bounded execution-evidence request")
    selected_path = Path(selection_evidence_path).resolve()
    selection_sha = _sha(selected_path.read_bytes())
    if historical_download_targets(selected_path, start, end) != symbols:
        raise ValueError("Execution targets differ from overlapping historical source intervals")
    root = guard_qlib_output_path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".collection.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another execution collection is running") from None
        source = client or baostock
        path = root / "execution_collection.json"
        today = datetime.now(SHANGHAI).date().isoformat()
        request = {"start_date": start, "end_date": end, "instruments": symbols,
                   "selection_evidence_path": str(selected_path), "selection_evidence_sha256": selection_sha}
        if path.exists():
            manifest = json.loads(path.read_bytes())
            if manifest["request"] != request:
                raise ValueError("Execution resume request/selection differs")
            if manifest["status"] == "complete":
                load_execution_collection(root)
                return root
            if manifest["as_of_date"] != today:
                raise ValueError("A later day requires a new execution collection generation")
        else:
            manifest = {"schema_version": 1, "kind": "baostock_daily_execution_evidence",
                "source": "baostock", "generation": uuid4().hex, "as_of_date": today,
                "request": request, "responses": {}, "failures": {}, "status": "collecting",
                "live_opening_quote": False, "atomic_supplier_vintage_verified": False,
                "units": {"price": "CNY", "volume": "shares", "amount": "CNY"},
                "limitations": ["Daily bars cannot prove a fresh 09:30-09:35 opening quote.",
                    "preclose is the source ex-right reference; it is not substituted with yesterday's close.",
                    "stock_basic is identity observed now, not announcement-time PIT evidence; isST is taken per daily row."]}
            _save(path, manifest)
        old_timeout, logged_in, previous = socket.getdefaulttimeout(), False, 0.
        socket.setdefaulttimeout(timeout)
        def wait():
            nonlocal previous
            pause = min_interval - (time.monotonic() - previous)
            if pause > 0:
                time.sleep(pause)
            previous = time.monotonic()
        try:
            with redirect_stdout(io.StringIO()):
                result = source.login()
            if str(result.error_code) != "0":
                raise ValueError("BaoStock login failed")
            logged_in = True
            consecutive = 0
            for index, symbol in enumerate(["stock_basic", *symbols]):
                try:
                    if symbol in manifest["responses"]:
                        record = manifest["responses"][symbol]
                        if _sha((root / record["file"]).read_bytes()) != record["sha256"]:
                            raise ValueError("Cached original execution response checksum differs")
                        continue
                    wait()
                    query = ({"code": symbol[:2].lower() + "." + symbol[2:], "fields": EXECUTION_FIELDS,
                        "start_date": start, "end_date": end, "frequency": "d", "adjustflag": "3"}
                        if symbol != "stock_basic" else {})
                    result = (source.query_stock_basic() if symbol == "stock_basic"
                              else source.query_history_k_data_plus(**query))
                    payload = {"request": query, "fields": list(result.fields), "rows": _rows(result),
                        "generation": manifest["generation"], "fetched_at": datetime.now(timezone.utc).isoformat()}
                    filename = symbol + ".json"
                    digest = _save(root / filename, payload)
                    manifest["responses"][symbol] = {"file": filename, "sha256": digest,
                        "fetched_at": payload["fetched_at"], "rows": len(payload["rows"])}
                    manifest["failures"].pop(symbol, None)
                    consecutive = 0
                except Exception as exc:
                    manifest["failures"][symbol] = {"error_type": type(exc).__name__, "error": str(exc)[:300]}
                    consecutive += 1
                _save(path, manifest)
                if progress:
                    progress(f"{index}/{len(symbols)} {symbol}: {'failed' if symbol in manifest['failures'] else 'collected'}")
                if consecutive >= 3:
                    break
            manifest["status"] = ("complete" if not manifest["failures"] and
                set(manifest["responses"]) == {"stock_basic", *symbols} else "partial_not_publishable")
            manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            _save(path, manifest)
            if manifest["status"] == "complete":
                try:
                    load_execution_collection(root)
                except Exception as exc:
                    manifest["status"] = "invalid_not_publishable"
                    manifest["validation_error"] = str(exc)[:300]
                    _save(path, manifest)
                    raise
            return root
        finally:
            socket.setdefaulttimeout(old_timeout)
            if logged_in:
                try:
                    with redirect_stdout(io.StringIO()):
                        source.logout()
                except Exception:
                    pass


def load_execution_collection(path):
    """Return (raw daily frame, original stock-basic frame, checksum-bound receipt)."""
    root = Path(path).resolve()
    raw_manifest = (root / "execution_collection.json").read_bytes()
    manifest = json.loads(raw_manifest)
    symbols = manifest["request"]["instruments"]
    if (manifest.get("kind") != "baostock_daily_execution_evidence" or manifest.get("source") != "baostock"
            or manifest["status"] != "complete" or manifest["failures"]
            or set(manifest["responses"]) != {"stock_basic", *symbols}):
        raise ValueError("Incomplete or incorrectly identified execution evidence")
    selection = Path(manifest["request"]["selection_evidence_path"])
    if _sha(selection.read_bytes()) != manifest["request"]["selection_evidence_sha256"]:
        raise ValueError("Execution pool selection evidence checksum differs")
    if historical_download_targets(selection, manifest["request"]["start_date"], manifest["request"]["end_date"]) != symbols:
        raise ValueError("Execution pool differs from original overlapping historical intervals")
    frames = []
    basic = None
    for symbol, record in manifest["responses"].items():
        filename = record["file"]
        if Path(filename).name != filename or (root / filename).is_symlink():
            raise ValueError("Unsafe original execution response path")
        raw = (root / filename).read_bytes()
        payload = json.loads(raw)
        fetched = datetime.fromisoformat(payload["fetched_at"])
        if (_sha(raw) != record["sha256"] or payload["generation"] != manifest["generation"]
                or payload["fetched_at"] != record["fetched_at"] or fetched.tzinfo is None
                or fetched.astimezone(SHANGHAI).date().isoformat() != manifest["as_of_date"]
                or len(payload["rows"]) != record["rows"]):
            raise ValueError("Original execution identity/checksum/time differs")
        frame = pd.DataFrame(payload["rows"], columns=payload["fields"])
        if symbol == "stock_basic":
            if payload["request"] != {} or not {"code", "ipoDate", "outDate", "type", "status"}.issubset(frame):
                raise ValueError("Invalid original security identity response")
            basic = frame
            continue
        expected = {"code": symbol[:2].lower() + "." + symbol[2:], "fields": EXECUTION_FIELDS,
            "start_date": manifest["request"]["start_date"], "end_date": manifest["request"]["end_date"],
            "frequency": "d", "adjustflag": "3"}
        if (payload["request"] != expected or payload["fields"] != EXECUTION_FIELDS.split(",")
                or frame.empty or not frame.code.eq(expected["code"]).all() or not frame.adjustflag.eq("3").all()):
            raise ValueError("Wrong original daily execution instrument/mode/fields")
        dates = pd.to_datetime(frame.date, errors="raise")
        if (dates.duplicated().any() or not dates.is_monotonic_increasing
                or frame.date.lt(expected["start_date"]).any() or frame.date.gt(expected["end_date"]).any()
                or not frame.tradestatus.isin(["0", "1"]).all() or not frame.isST.isin(["0", "1"]).all()):
            raise ValueError("Invalid original execution dates or daily status")
        frame["instrument"] = symbol
        numeric = ["open", "high", "low", "close", "preclose", "volume", "amount"]
        numbers = frame[numeric].replace("", np.nan).apply(pd.to_numeric, errors="raise")
        if np.isinf(numbers.to_numpy()).any() or numbers.lt(0).any().any():
            raise ValueError("Invalid original execution prices or units")
        volume = numbers.volume.dropna()
        if not np.isclose(volume, np.rint(volume), rtol=0, atol=1e-6).all():
            raise ValueError("Original execution volume is not integer shares")
        complete = frame.tradestatus.eq("1") & numbers.notna().all(axis=1) & numbers.gt(0).all(axis=1)
        quoted = numbers.loc[complete]
        if (quoted.high.lt(quoted[["open", "high", "low", "close"]].max(axis=1)).any()
                or quoted.low.gt(quoted[["open", "high", "low", "close"]].min(axis=1)).any()):
            raise ValueError("Original execution OHLC bounds are inconsistent")
        vwap = quoted.amount / quoted.volume
        if ((vwap < quoted.low - 1e-4) | (vwap > quoted.high + 1e-4)).any():
            raise ValueError("Original execution amount/share-volume units differ from raw OHLC")
        frame["source_fetched_at"] = payload["fetched_at"]
        frame["source_response_sha256"] = record["sha256"]
        frame["source_complete_quote"] = complete
        frames.append(frame)
    if basic is None or basic.code.duplicated().any():
        raise ValueError("Missing or duplicate security identity evidence")
    required = {x[:2].lower() + "." + x[2:] for x in symbols}
    if not required.issubset(set(basic.code)):
        raise ValueError("Original identity table omits a requested security")
    receipt = {**manifest, "collection_path": str(root), "manifest_sha256": _sha(raw_manifest)}
    return pd.concat(frames, ignore_index=True), basic, receipt
