"""Original Sina daily prices for independent cross-checks, not execution status.

Sina's compressed stream supplies an explicit ``prevclose`` on exceptional
reference-price dates. The generic AKShare daily adapter discards that field.
This reader preserves it, the decoder output, and the original response. A
previous observed close is only a separately labelled reference *candidate*;
it never becomes a verified exchange reference merely because a field is
omitted. Neither a missing bar nor zero volume proves a historical suspension.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import importlib.metadata
import json
from pathlib import Path
import re

from .qlib_snapshot import guard_qlib_output_path


SINA_ENDPOINT = "https://finance.sina.com.cn/realstock/company/{symbol}/hisdata_klc2/klc_kl.js"
DECODER_SHA256 = "39a599c94dde4df1c2eb0882d4bff9560160cfd52ead0797cebb04b3122c2f52"
DECODER_REFERENCE = "https://github.com/akfamily/akshare/blob/main/akshare/stock/cons.py"
ADAPTER_REFERENCE = "https://github.com/akfamily/akshare/blob/main/akshare/stock/stock_zh_a_sina.py"
PRICE_FIELDS = ("open", "high", "low", "close")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _json(raw):
    return (json.dumps(raw, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def _instrument(value):
    if not isinstance(value, str) or not re.fullmatch(r"(?:SH|SZ)\d{6}", value):
        raise ValueError("Expected an explicit SH/SZ security identity")
    return value


def _number(value, field, *, integer=False):
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Missing or invalid original Sina {field}")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid original Sina {field}") from exc
    if not result.is_finite() or result < 0 or (integer and result != result.to_integral_value()):
        raise ValueError(f"Invalid original Sina {field} units")
    return format(result, "f")


def decode_sina_response(raw, instrument):
    """Run only the checksum-pinned local decoder on a string, never remote JS."""
    instrument = _instrument(instrument)
    if not isinstance(raw, bytes) or len(raw) > 10 * 1024 * 1024:
        raise ValueError("Invalid original Sina response size/type")
    text = raw.decode("ascii")
    pattern = (r'\s*var\s+KLC_(?:K2|KL)_' + instrument.lower()
               + r'\s*=\s*"(?P<data>[A-Za-z0-9+/=]+)"\s*;\s*(?:/\*[^*]*(?:\*(?!/)[^*]*)*\*/\s*)?')
    matched = re.fullmatch(pattern, text)
    if matched is None:
        raise ValueError("Wrong security, malformed response, or unexpected executable Sina content")
    from akshare.stock.cons import hk_js_decode
    import py_mini_racer
    if _sha(hk_js_decode.encode()) != DECODER_SHA256:
        raise ValueError("Sina decoder changed; review its reference-price semantics before use")
    engine = py_mini_racer.MiniRacer()
    engine.eval(hk_js_decode)
    rows = engine.call("d", matched.group("data"))
    if not isinstance(rows, list) or not rows:
        raise ValueError("Sina decoder returned no original daily observations")
    return rows


def normalize_sina_rows(rows, instrument, *, start_date, end_date):
    """Keep price observations and unknown daily statuses in distinct fields."""
    instrument = _instrument(instrument)
    start = datetime.strptime(start_date, "%Y-%m-%d").date().isoformat()
    end = datetime.strptime(end_date, "%Y-%m-%d").date().isoformat()
    if start > end or not isinstance(rows, list) or not rows:
        raise ValueError("Invalid Sina date interval or original daily observations")
    result, previous_day, previous_close = [], None, None
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("date"), str):
            raise ValueError("Invalid original Sina daily row")
        day = datetime.fromisoformat(row["date"].replace("Z", "+00:00")).date().isoformat()
        if previous_day is not None and day <= previous_day:
            raise ValueError("Original Sina dates must be unique and increasing")
        values = {field: _number(row.get(field), field) for field in PRICE_FIELDS}
        values.update({field: _number(row.get(field), field, integer=True) for field in ("volume", "amount")})
        prices = {field: Decimal(values[field]) for field in PRICE_FIELDS}
        if prices["high"] < max(prices.values()) or prices["low"] > min(prices.values()):
            raise ValueError("Original Sina OHLC bounds differ")
        if prices["close"] <= 0:
            raise ValueError("A zero raw close cannot provide a reference-price candidate")
        explicit = _number(row["prevclose"], "prevclose") if "prevclose" in row else None
        if explicit is not None and Decimal(explicit) <= 0:
            raise ValueError("Explicit original Sina reference price must be positive")
        candidate = explicit if explicit is not None else previous_close
        complete = (all(value > 0 for value in prices.values())
                    and Decimal(values["volume"]) > 0 and Decimal(values["amount"]) > 0)
        if complete:
            vwap = Decimal(values["amount"]) / Decimal(values["volume"])
            tolerance = Decimal("0.5") / Decimal(values["volume"]) + Decimal("0.0001")
            if not prices["low"] - tolerance <= vwap <= prices["high"] + tolerance:
                raise ValueError("Original Sina share-volume/CNY amount does not match raw OHLC")
        observation = {"date": day, "instrument": instrument, **values,
            "explicit_prevclose": explicit, "previous_observed_close": previous_close,
            "previous_observed_session": previous_day, "reference_price_candidate": candidate,
            "reference_basis": ("explicit_source_prevclose" if explicit is not None else
                "previous_observed_raw_close_requires_crosscheck" if previous_close is not None else "unavailable"),
            "reference_verified": explicit is not None,
            "tradestatus": None, "isST": None, "source_complete_quote": complete,
            "execution_evidence_complete": False}
        for field in ("postVol", "postAmt"):
            if field in row:
                observation[field] = _number(row[field], field, integer=True)
        if start <= day <= end:
            result.append(observation)
        previous_day, previous_close = day, values["close"]
    if not result:
        raise ValueError("Sina response does not intersect the requested interval")
    return result


def collect_sina_crosscheck(instrument, output_dir, *, start_date, end_date, session=None,
                           timeout=20, decoder=None, now=None):
    """One public request, no automatic retry/fallback, immutable evidence output."""
    instrument = _instrument(instrument)
    datetime.strptime(start_date, "%Y-%m-%d"); datetime.strptime(end_date, "%Y-%m-%d")
    if start_date > end_date or not 1 <= timeout <= 60:
        raise ValueError("Invalid bounded Sina historical request")
    root = guard_qlib_output_path(output_dir)
    if root.exists() and any(root.iterdir()):
        raise ValueError("Use a new empty Sina evidence directory")
    stamp = now or datetime.now(timezone.utc)
    if stamp.tzinfo is None:
        raise ValueError("Sina observation time must be timezone aware")
    root.mkdir(parents=True, exist_ok=True)
    if session is None:
        import requests
        session = requests.Session()
        owned = True
    else:
        owned = False
    url = SINA_ENDPOINT.format(symbol=instrument.lower())
    request = {"instrument": instrument, "start_date": start_date, "end_date": end_date, "url": url}
    try:
        response = session.get(url, timeout=timeout, allow_redirects=False,
            headers={"Referer":f"https://finance.sina.com.cn/realstock/company/{instrument.lower()}/nc.shtml"})
        raw = response.content
        (root / "response.js").write_bytes(raw)
        if response.status_code != 200:
            raise ValueError(f"Sina public endpoint returned HTTP {response.status_code}; no retry")
        rows = (decoder or decode_sina_response)(raw, instrument)
        decoded = _json(rows)
        normalized = _json(normalize_sina_rows(rows, instrument, start_date=start_date, end_date=end_date))
        (root / "decoded.json").write_bytes(decoded)
        (root / "observations.json").write_bytes(normalized)
        receipt = {"schema_version":1, "kind":"sina_raw_daily_crosscheck_not_status_bundle",
            "source":"sina", "request":request, "fetched_at":stamp.isoformat(),
            "decoder":{"reference":DECODER_REFERENCE, "sha256":DECODER_SHA256,
                "akshare_version":importlib.metadata.version("akshare"), "test_decoder_injected":decoder is not None},
            "units":{"price":"CNY", "volume":"shares", "amount":"CNY_rounded_to_integer"},
            "files":{name:_sha(data) for name,data in [("response.js",raw),("decoded.json",decoded),("observations.json",normalized)]},
            "execution_evidence_complete":False, "historical_ST_available":False,
            "historical_suspension_available":False, "live_opening_quote":False,
            "limitations":["Missing bars and volume do not establish daily trading status or suspension.",
                "Absent prevclose leaves an independently labelled previous-observed-close candidate; crosscheck required.",
                "Current names and current quotes do not establish historical ST state.",
                "Sina amount is rounded to integer CNY; original decimal amounts cannot be recovered."]}
        (root / "receipt.json").write_bytes(_json(receipt))
        return receipt
    except Exception as exc:
        (root / "failure.json").write_bytes(_json({"request":request, "error_type":type(exc).__name__,
            "error":str(exc)[:400], "fetched_at":stamp.isoformat(), "execution_evidence_complete":False}))
        raise
    finally:
        if owned:
            session.close()


def load_sina_crosscheck(path):
    """Verify immutable files and regenerate observations from saved decoder rows."""
    root = Path(path)
    receipt = json.loads((root / "receipt.json").read_bytes())
    if (receipt.get("kind") != "sina_raw_daily_crosscheck_not_status_bundle"
            or receipt.get("execution_evidence_complete") is not False
            or receipt.get("historical_ST_available") is not False
            or receipt.get("historical_suspension_available") is not False
            or receipt.get("live_opening_quote") is not False
            or set(receipt.get("files", {})) != {"response.js", "decoded.json", "observations.json"}):
        raise ValueError("Invalid Sina crosscheck identity or unsupported execution claims")
    for name,digest in receipt["files"].items():
        file = root / name
        if file.is_symlink() or _sha(file.read_bytes()) != digest:
            raise ValueError("Sina original evidence checksum differs")
    request = receipt["request"]
    _instrument(request["instrument"])
    if request["url"] != SINA_ENDPOINT.format(symbol=request["instrument"].lower()):
        raise ValueError("Sina source URL or security differs")
    rows = json.loads((root / "decoded.json").read_bytes())
    observations = normalize_sina_rows(rows, request["instrument"],
        start_date=request["start_date"], end_date=request["end_date"])
    if _json(observations) != (root / "observations.json").read_bytes():
        raise ValueError("Sina decoded observation semantics differ")
    if not receipt["decoder"]["test_decoder_injected"]:
        if _json(decode_sina_response((root / "response.js").read_bytes(), request["instrument"])) != (root / "decoded.json").read_bytes():
            raise ValueError("Sina original bytes and saved decoder rows differ")
    return observations, receipt
