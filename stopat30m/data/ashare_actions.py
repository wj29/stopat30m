"""Original BaoStock dividend/bonus evidence and explicit raw-share events.

Query by actual ex-date year, not report year. This is a reconstructed source
history, not proof of a complete announcement-time corporate-action database.
"""
from __future__ import annotations

from contextlib import redirect_stdout
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
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

from .ashare_daily import _rows
from .qlib_snapshot import guard_qlib_output_path

SHANGHAI = ZoneInfo("Asia/Shanghai")
SOURCE = "baostock.query_dividend_data.operate"
REQUIRED = {"code", "dividPlanDate", "dividRegistDate", "dividOperateDate", "dividPayDate",
            "dividStockMarketDate", "dividCashPsBeforeTax", "dividStocksPs", "dividReserveToStockPs"}


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _save(path, value):
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(raw); temporary.replace(path)
    return _sha(raw)


def collect_dividend_evidence(output_dir, instruments, years, *, min_interval=.6, timeout=8,
                              client=None, progress=None):
    """One bounded serial generation; validated successes survive same-day resume."""
    import fcntl
    import baostock as bs

    symbols, periods = sorted(set(instruments)), sorted(set(years))
    if not symbols or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", inst) for inst in symbols):
        raise ValueError("Require explicit A-share dividend targets")
    if not periods or any(type(year) is not int or not 2000 <= year <= 2100 for year in periods):
        raise ValueError("Require explicit ex-date years")
    if min_interval < .5 or not 0 < timeout <= 60:
        raise ValueError("Dividend source requests must be paced and bounded")
    root = guard_qlib_output_path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".dividend.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another dividend collection is already active") from None
        manifest_path = root / "collection.json"
        today = datetime.now(SHANGHAI).date().isoformat()
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            if manifest.get("source") != SOURCE or manifest["instruments"] != symbols or manifest["years"] != periods:
                raise ValueError("Dividend resume request differs; use a new generation directory")
            if manifest["status"] == "complete":
                load_dividend_evidence(root)
                return root
            if manifest["as_of_date"] != today:
                raise ValueError("An incomplete older dividend generation cannot silently refresh")
        else:
            manifest = {"schema_version": 1, "source": SOURCE, "generation": uuid4().hex,
                "instruments": symbols, "years": periods, "year_type": "operate", "as_of_date": today,
                "responses": {}, "failures": {}, "status": "collecting"}
            _save(manifest_path, manifest)
        source, old_timeout = client or bs, socket.getdefaulttimeout()
        logged_in = False
        previous = None
        socket.setdefaulttimeout(timeout)
        try:
            with redirect_stdout(io.StringIO()):
                login = source.login()
            if str(login.error_code) != "0":
                raise ValueError("Dividend source login failed")
            logged_in = True
            consecutive, completed = 0, 0
            for inst in symbols:
                for year in periods:
                    key = f"{inst}_{year}"
                    request = {"code": inst[:2].lower() + "." + inst[2:], "year": str(year), "yearType": "operate"}
                    try:
                        if key in manifest["responses"]:
                            _load_response(root, manifest, key)
                        else:
                            if previous is not None:
                                time.sleep(max(0, min_interval - (time.monotonic() - previous)))
                            previous = time.monotonic()
                            response = source.query_dividend_data(**request)
                            rows = _rows(response)
                            if not REQUIRED.issubset(response.fields):
                                raise ValueError("Dividend response lacks required entitlement fields")
                            if getattr(response, "code", request["code"]) != request["code"]:
                                raise ValueError("Wrong dividend source security identity")
                            payload = {"request": request, "fields": list(response.fields), "rows": rows,
                                "generation": manifest["generation"], "fetched_at": datetime.now(timezone.utc).isoformat()}
                            file = root / (key + ".json")
                            digest = _save(file, payload)
                            manifest["responses"][key] = {"file": file.name, "sha256": digest,
                                "fetched_at": payload["fetched_at"], "rows": len(rows)}
                            _load_response(root, manifest, key)
                        manifest["failures"].pop(key, None); consecutive = 0
                    except Exception as exc:
                        consecutive += 1
                        manifest["failures"][key] = {"error_type": type(exc).__name__, "error": str(exc)[:250]}
                    completed += 1
                    _save(manifest_path, manifest)
                    if progress:
                        progress(f"Dividends {completed}/{len(symbols)*len(periods)} {key}: {'failed' if key in manifest['failures'] else 'received'}")
                    if consecutive >= 3:
                        break
                if consecutive >= 3:
                    break
            manifest["status"] = ("complete" if not manifest["failures"] and len(manifest["responses"]) == len(symbols)*len(periods)
                                  else "partial_not_publishable")
            manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            _save(manifest_path, manifest)
            return root
        finally:
            socket.setdefaulttimeout(old_timeout)
            if logged_in:
                try:
                    with redirect_stdout(io.StringIO()):
                        source.logout()
                except Exception:
                    pass


def _load_response(root, manifest, key):
    inst, year = key.split("_")
    record = manifest["responses"][key]
    if Path(record["file"]).name != record["file"]:
        raise ValueError("Unsafe dividend original path")
    raw = (root / record["file"]).read_bytes()
    value = json.loads(raw)
    request = {"code": inst[:2].lower() + "." + inst[2:], "year": year, "yearType": "operate"}
    fetched = datetime.fromisoformat(value["fetched_at"])
    if (_sha(raw) != record["sha256"] or value["request"] != request or value["generation"] != manifest["generation"]
            or fetched.tzinfo is None or fetched.astimezone(SHANGHAI).date().isoformat() != manifest["as_of_date"]
            or not REQUIRED.issubset(value["fields"])):
        raise ValueError("Dividend source identity, bytes or retrieval date differs")
    frame = pd.DataFrame(value["rows"], columns=value["fields"])
    if not frame.empty and not frame.code.eq(request["code"]).all():
        raise ValueError("Wrong dividend source row security")
    frame["instrument"] = inst
    frame["source_response_sha256"] = record["sha256"]
    return frame


def load_dividend_evidence(path):
    root = Path(path)
    manifest = json.loads((root / "collection.json").read_text())
    keys = {f"{inst}_{year}" for inst in manifest["instruments"] for year in manifest["years"]}
    if (manifest.get("source") != SOURCE or manifest.get("year_type") != "operate" or manifest["status"] != "complete"
            or manifest["failures"] or set(manifest["responses"]) != keys):
        raise ValueError("Incomplete or wrong dividend evidence generation")
    frames = [_load_response(root, manifest, key) for key in sorted(keys)]
    return pd.concat(frames, ignore_index=True), manifest


def _day(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("A required corporate-action date is unavailable")
    return stamp.date().isoformat()


def _amount(value):
    # Empty optional components are preserved in evidence. A nonzero action
    # still needs implemented dates and reference-price reconciliation.
    try:
        result = Decimal(str(value)) if str(value).strip() else Decimal(0)
    except InvalidOperation as exc:
        raise ValueError("Non-numeric corporate-action amount") from exc
    if not result.is_finite() or result < 0:
        raise ValueError("Negative/non-finite corporate-action amount")
    return result


def dividend_events(path, *, start, end):
    """Return evidenced implemented events, never draft plans or guessed dates."""
    frame, manifest = load_dividend_evidence(path)
    events = {}
    rows = frame.to_dict("records")
    # The supplier can repeat the same implemented economic event with a
    # second, less complete disclosure row. Resolve only identical terms;
    # never substitute a proposal date for a missing implementation date.
    identity_fields = ("instrument", "dividRegistDate", "dividOperateDate", "dividPayDate", "dividStockMarketDate",
                       "dividCashPsBeforeTax", "dividStocksPs", "dividReserveToStockPs")
    complete = {}
    for row in rows:
        key = tuple(row[field] for field in identity_fields)
        if str(row.get("dividPlanDate", "")).strip():
            prior = complete.setdefault(key, row)
            if prior["dividPlanDate"] != row["dividPlanDate"]:
                raise ValueError("Conflicting implementation announcement dates")
    for row in rows:
        if not str(row.get("dividPlanDate", "")).strip():
            row = complete.get(tuple(row[field] for field in identity_fields), row)
        if not str(row.get("dividOperateDate", "")).strip():
            continue
        ex_date = _day(row["dividOperateDate"])
        if not start <= ex_date <= end:
            continue
        cash, stocks, reserve = (_amount(row[name]) for name in
                                ("dividCashPsBeforeTax", "dividStocksPs", "dividReserveToStockPs"))
        if not cash and not stocks and not reserve:
            continue
        record_date, announcement = _day(row["dividRegistDate"]), _day(row["dividPlanDate"])
        if not announcement <= record_date < ex_date:
            raise ValueError("Dividend announcement/record/ex-date chronology is invalid")
        pay_date = _day(row["dividPayDate"]) if cash else None
        listing_date = _day(row["dividStockMarketDate"]) if stocks + reserve else None
        if pay_date is not None and pay_date < ex_date or listing_date is not None and listing_date < ex_date:
            raise ValueError("Cash/bonus shares cannot become usable before the ex-date")
        identity = {"instrument": row["instrument"], "record_date": record_date, "ex_date": ex_date,
                    "pay_date": pay_date, "listing_date": listing_date, "cash_per_share": str(cash),
                    "stock_dividend_ratio": str(stocks), "capital_reserve_ratio": str(reserve)}
        key = "dividend:" + _sha(json.dumps(identity, sort_keys=True).encode())
        event = {**identity, "event_id": key, "bonus_ratio": str(stocks + reserve),
            "known_at": announcement + "T23:59:59+08:00", "source": SOURCE,
            "source_response_sha256": row["source_response_sha256"], "generation": manifest["generation"],
            "empty_optional_components_as_zero": [name for name in ("dividCashPsBeforeTax", "dividStocksPs", "dividReserveToStockPs") if not str(row[name]).strip()],
            "after_tax_source_text": row.get("dividCashPsAfterTax", ""),
            "tax_policy": "fixed conservative cash-dividend reserve declared by experiment; not a broker holding-period tax statement"}
        if key in events and events[key] != event:
            raise ValueError("Conflicting duplicate corporate-action source rows")
        events[key] = event
    return sorted(events.values(), key=lambda item: (item["ex_date"], item["instrument"], item["event_id"]))


def apply_action_overrides(events, path):
    """Apply separately evidenced issuer corrections without changing originals."""
    root = Path(path).resolve()
    document = json.loads(root.read_text())
    result = [dict(row) for row in events]
    for patch in document["patches"]:
        proof = patch["evidence"]
        source = (root.parent / proof["path"]).resolve()
        digest = _sha(source.read_bytes())
        if digest != proof["sha256"] or not proof.get("url", "").startswith("https://"):
            raise ValueError("Issuer action override source bytes or URL differ")
        matches = [i for i, row in enumerate(result) if row["instrument"] == patch["instrument"] and row["ex_date"] == patch["ex_date"]]
        operation = patch["operation"]
        if operation not in {"add", "replace", "annotate"} or (operation == "add" and matches) or (operation != "add" and len(matches) != 1):
            raise ValueError("Ambiguous or invalid issuer action override")
        row = {} if operation == "add" else result[matches[0]].copy()
        values = patch["values"]
        if operation == "annotate" and set(values) - {"reference_cash_per_share", "reference_bonus_ratio", "cash_dividend_tax_rate", "taxable_stock_face_value_per_record_share"}:
            raise ValueError("A reference annotation cannot change delivered economic terms")
        row.update(values, instrument=patch["instrument"], ex_date=patch["ex_date"])
        cash, bonus = _amount(row["cash_per_share"]), _amount(row["bonus_ratio"])
        record = _day(row["record_date"])
        known = pd.Timestamp(row["known_at"])
        if known.tzinfo is None or known.date().isoformat() > record or not record < row["ex_date"]:
            raise ValueError("Issuer action knowledge/record/ex chronology differs")
        if cash and _day(row["pay_date"]) < row["ex_date"] or bonus and _day(row["listing_date"]) < row["ex_date"]:
            raise ValueError("Issuer settlement cannot predate the ex-date")
        if "reference_cash_per_share" in values or "reference_bonus_ratio" in values:
            row["reference_evidence_sha256"] = digest
            for field in ("reference_cash_per_share", "reference_bonus_ratio"):
                if field in row:
                    _amount(row[field])
        if "cash_dividend_tax_rate" in values:
            rate = _amount(values["cash_dividend_tax_rate"])
            if rate > 1 or not cash:
                raise ValueError("Cash cannot cover the evidenced non-cash dividend reserve")
            row["tax_evidence_sha256"] = digest
        row["override_evidence"] = {"path": str(source), "sha256": digest, "url": proof["url"]}
        if operation != "annotate":
            row["source_response_sha256"] = digest
            row["source"] = "issuer_implemented_action_separately_verified"
            row["event_id"] = "issuer:" + _sha(json.dumps({key:row.get(key) for key in
                ("instrument", "record_date", "ex_date", "pay_date", "listing_date", "cash_per_share", "bonus_ratio")}, sort_keys=True).encode())
        if operation == "add":
            result.append(row)
        else:
            result[matches[0]] = row
    return sorted(result, key=lambda row: (row["ex_date"], row["instrument"], row["event_id"]))
