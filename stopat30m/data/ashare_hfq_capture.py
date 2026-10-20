"""Capture HFQ originals while reusing separately audited raw originals.

This source-only capture does not combine vintages, adjust a price flag, audit a
Qlib seam or publish a dataset. Per-security windows are explicit so a bounded
overlap can be downloaded without repeating an existing raw-price collection.
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

import pandas as pd

from .ashare_daily import _rows, _save, _sha, SHANGHAI
from .ashare_execution_data import EXECUTION_FIELDS
from .qlib_snapshot import guard_qlib_output_path


def _requests(windows):
    if not windows:
        raise ValueError("HFQ capture requires explicit per-security windows")
    result = {}
    for symbol, window in sorted(windows.items()):
        if not re.fullmatch(r"(?:SH|SZ)\d{6}", symbol) or set(window) != {"start_date", "end_date"}:
            raise ValueError("Invalid HFQ-only request identity/window")
        start = pd.Timestamp(window["start_date"]).date().isoformat()
        end = pd.Timestamp(window["end_date"]).date().isoformat()
        if start != window["start_date"] or end != window["end_date"] or start > end:
            raise ValueError("HFQ-only capture dates must be ordered ISO dates")
        result[symbol] = {"code": symbol[:2].lower() + "." + symbol[2:],
            "fields": EXECUTION_FIELDS, "start_date": start, "end_date": end,
            "frequency": "d", "adjustflag": "1"}
    return result


def _original(root, manifest, symbol):
    record = manifest["responses"][symbol]
    filename = record["file"]
    path = root / filename
    if Path(filename).name != filename or path.is_symlink():
        raise ValueError("Unsafe original HFQ response path")
    raw = path.read_bytes()
    payload = json.loads(raw)
    instant = datetime.fromisoformat(payload["fetched_at"])
    if (payload.get("query") != "query_history_k_data_plus"
            or instant.tzinfo is None or instant.astimezone(SHANGHAI).date().isoformat() != manifest["as_of_date"]
            or instant > datetime.now(timezone.utc) or _sha(raw) != record["sha256"]
            or payload["generation"] != manifest["generation"]
            or payload["request"] != manifest["requests"][symbol]
            or payload["fields"] != EXECUTION_FIELDS.split(",")
            or payload["fetched_at"] != record["fetched_at"]
            or len(payload["rows"]) != record["rows"]):
        raise ValueError("Original HFQ bytes/request/schema/generation/time differs")
    frame = pd.DataFrame(payload["rows"], columns=payload["fields"])
    if frame.empty:
        raise ValueError("An empty HFQ response cannot complete an observed raw-trading target")
    request = payload["request"]
    if (not frame.code.eq(request["code"]).all() or not frame.adjustflag.eq("1").all()
            or frame.date.duplicated().any() or not frame.date.is_monotonic_increasing
            or frame.date.lt(request["start_date"]).any() or frame.date.gt(request["end_date"]).any()
            or not frame.tradestatus.isin(["0", "1"]).all() or not frame.isST.isin(["0", "1"]).all()):
        raise ValueError("Original HFQ row identity/mode/date/status differs; preserve and block")
    for day in frame.date:
        if pd.Timestamp(day).date().isoformat() != day:
            raise ValueError("Original HFQ row has a non-ISO date")
    frame["instrument"] = symbol
    return frame


def collect_hfq_evidence(output_dir, windows, *, selection_evidence_path,
                         client, min_interval=.6, timeout=10, progress=None):
    """Single serial generation, same-day SHA-verified resume, no raw requests.

    Use a validated, hard-deadline and network-paced client. Every successful
    payload is retained before row-mode validation; invalid source rows cannot
    be silently replaced during resume. Three consecutive failures stop work.
    """
    requests = _requests(windows)
    if min_interval < .6 or not 0 < timeout <= 60:
        raise ValueError("HFQ capture requires bounded timeout and >=.6s pacing")
    root = guard_qlib_output_path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    selection = Path(selection_evidence_path).resolve()
    binding = {"path": str(selection), "sha256": _sha(selection.read_bytes())}
    with (root / ".collection.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another HFQ-only collector owns this generation") from None
        path = root / "collection.json"
        today = datetime.now(SHANGHAI).date().isoformat()
        if path.exists():
            manifest = json.loads(path.read_bytes())
            if (manifest.get("kind") != "baostock_hfq_only_original_evidence"
                    or manifest["requests"] != requests or manifest["selection_evidence"] != binding):
                raise ValueError("HFQ resume request/selection differs")
            if manifest["status"] == "complete":
                load_hfq_evidence(root)
                return root
            if manifest["as_of_date"] != today:
                raise ValueError("Use a new HFQ generation on a later date")
        else:
            manifest = {"kind": "baostock_hfq_only_original_evidence", "schema_version": 1,
                "source": "baostock", "generation": uuid4().hex, "as_of_date": today,
                "requests": requests, "selection_evidence": binding, "responses": {},
                "failures": {}, "status": "collecting", "raw_prices_downloaded": False,
                "source_snapshot_atomic_supplier_vintage_proven": False,
                "market_audit_passed": False, "qlib_seam_verified": False,
                "min_interval_seconds": min_interval}
            _save(path, manifest)
        previous, logged_in = 0., False
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(timeout)
        try:
            with redirect_stdout(io.StringIO()):
                result = client.login()
            if str(result.error_code) != "0":
                raise ValueError("HFQ-only BaoStock login failed")
            logged_in = True
            consecutive = 0
            for n, (symbol, request) in enumerate(requests.items(), 1):
                try:
                    if symbol not in manifest["responses"]:
                        pause = min_interval - (time.monotonic() - previous)
                        if pause > 0:
                            time.sleep(pause)
                        previous = time.monotonic()
                        result = client.query_history_k_data_plus(**request)
                        payload = {"query": "query_history_k_data_plus", "request": request,
                            "fields": list(result.fields), "rows": _rows(result),
                            "generation": manifest["generation"],
                            "fetched_at": datetime.now(timezone.utc).isoformat()}
                        filename = symbol + "_hfq.json"
                        if (root / filename).exists():
                            raise ValueError("An unindexed original exists; preserve for explicit recovery")
                        digest = _save(root / filename, payload)
                        manifest["responses"][symbol] = {"file": filename, "sha256": digest,
                            "fetched_at": payload["fetched_at"], "rows": len(payload["rows"])}
                        _save(path, manifest)
                    _original(root, manifest, symbol)
                    manifest["failures"].pop(symbol, None)
                    consecutive = 0
                except Exception as exc:
                    consecutive += 1
                    manifest["failures"][symbol] = {"error_type": type(exc).__name__,
                        "error": str(exc)[:300]}
                _save(path, manifest)
                if progress:
                    progress(f"{n}/{len(requests)} {symbol}: " +
                        ("failed" if symbol in manifest["failures"] else "captured"))
                if consecutive >= 3:
                    manifest["stopped_after_consecutive_failures"] = consecutive
                    break
            manifest["status"] = ("complete" if not manifest["failures"] and
                set(manifest["responses"]) == set(requests) else "partial_not_publishable")
            manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            _save(path, manifest)
            return root
        finally:
            socket.setdefaulttimeout(old_timeout)
            if logged_in:
                try:
                    with redirect_stdout(io.StringIO()):
                        client.logout()
                except Exception:
                    pass


def load_hfq_evidence(directory):
    """Read only a complete original inventory; this is not a market audit."""
    root = Path(directory)
    manifest = json.loads((root / "collection.json").read_bytes())
    if (manifest.get("kind") != "baostock_hfq_only_original_evidence"
            or manifest.get("source") != "baostock" or manifest["status"] != "complete"
            or manifest["failures"] or set(manifest["requests"]) != set(manifest["responses"])):
        raise ValueError("Incomplete HFQ-only original inventory")
    windows = {symbol: {key: req[key] for key in ("start_date", "end_date")}
               for symbol, req in manifest["requests"].items()}
    if manifest["requests"] != _requests(windows):
        raise ValueError("HFQ-only original request modes/schema differ")
    selection = manifest["selection_evidence"]
    if _sha(Path(selection["path"]).read_bytes()) != selection["sha256"]:
        raise ValueError("HFQ-only selection receipt changed")
    return pd.concat([_original(root, manifest, symbol) for symbol in sorted(windows)],
                     ignore_index=True), manifest
