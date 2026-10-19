"""Explicit Tencent observations for prospective A-share paper executions.

Observed latest quotes are never relabelled as a historical opening fill.
Original response bytes are saved separately from the parsed observations.
This adapter does not submit orders or silently switch data sources.
"""
from __future__ import annotations

from datetime import datetime, time, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import time as pacing
from zoneinfo import ZoneInfo

from .qlib_snapshot import guard_qlib_output_path

SHANGHAI = ZoneInfo("Asia/Shanghai")
ENDPOINT = "https://qt.gtimg.cn/q="
SOURCE = "tencent.public_level1_observation"


def _instant(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("A timezone-aware observation time is required")
    return value.astimezone(SHANGHAI)


def in_continuous_session(now):
    clock = _instant(now).time().replace(tzinfo=None)
    return time(9, 30) <= clock <= time(11, 30) or time(13) <= clock < time(14, 57)


def _number(value, *, positive=True):
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Invalid numeric quote field") from exc
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        raise ValueError("Invalid price or size in the quote")
    return number


def parse_tencent_observations(raw, instruments, *, now, require_live=True):
    """Validate code, source time and actually observed prices; preserve rejects.

    Tencent fields used here are name/code/latest/previous-close/open/volume,
    first bid/ask and source timestamp. A positive day volume is evidence of
    trading today, not proof that a future order will fill or that depth lasts.
    """
    instant = _instant(now)
    if require_live and not in_continuous_session(instant):
        raise ValueError("Prospective execution requires an active continuous trading session")
    requested = sorted(set(instruments))
    if not requested or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", item) for item in requested):
        raise ValueError("Require explicit canonical A-share quote instruments")
    try:
        body = raw.decode("gb18030")
    except UnicodeDecodeError as exc:
        raise ValueError("Quote response is not valid provider text") from exc
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    found, rejected = {}, {}
    for line in lines:
        match = re.fullmatch(r'v_((?:sh|sz)\d{6})="([^"\r\n]*)";?', line)
        if not match:
            raise ValueError("Unexpected quote response envelope")
        inst, values = match[1].upper(), match[2].split("~")
        if inst not in requested or inst in found or inst in rejected:
            raise ValueError("Unexpected or duplicate source quote instrument")
        try:
            if len(values) <= 30 or values[2] != inst[2:] or not values[1].strip():
                raise ValueError("Missing or wrong source security identity")
            if not re.fullmatch(r"\d{14}", values[30]):
                raise ValueError("Source timestamp must be an explicit fourteen-digit observation")
            asof = datetime.strptime(values[30], "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI)
            age = (instant - asof).total_seconds()
            if asof.date() != instant.date() or not 0 <= age <= 120:
                raise ValueError("Source quote is stale, future-dated or from another session")
            latest, previous, opening = (_number(values[index]) for index in (3, 4, 5))
            volume = _number(values[6])
            bid, bid_size, ask, ask_size = (_number(values[index], positive=False) for index in (9, 10, 19, 20))
            if bid and ask and bid > ask:
                raise ValueError("Crossed source order book")
            if len(values[1]) > 64:
                raise ValueError("Unexpected security name length")
            found[inst] = {
                "instrument": inst, "session": asof.date().isoformat(),
                "name": values[1], "is_st": "ST" in values[1].upper(),
                "open": str(opening), "preclose": str(previous),
                "execution_price": str(latest), "execution_price_basis": "live_observed_quote",
                "asof": asof.isoformat(), "age_seconds": age,
                "source": SOURCE, "observed_volume_hands": str(volume),
                "bid": str(bid), "bid_size_hands": str(bid_size),
                "ask": str(ask), "ask_size_hands": str(ask_size),
                "tradestatus": "1", "status_basis": "fresh_today_quote_with_positive_observed_volume",
                "guaranteed_fill": False, "broker_order": False,
                "live_execution_eligible": bool(require_live and in_continuous_session(instant)),
            }
        except (ValueError, IndexError) as exc:
            rejected[inst] = str(exc)
    for missing in set(requested) - set(found) - set(rejected):
        rejected[missing] = "Source did not return the requested security"
    return {"source": SOURCE, "observed_at": instant.isoformat(), "instruments": requested,
            "quotes": found, "rejected": rejected, "complete": not rejected,
            "execution_price_basis": "live_observed_quote", "broker_orders_submitted": False,
            "limitations": ["Public level-one observations do not establish queue position or a guaranteed fill.",
                            "A stale or zero quote cannot establish a halt or permit a synthetic opening fill."]}


def fetch_tencent_execution_quotes(instruments, output_dir, *, now=None, session=None,
                                   timeout=8, min_interval=.6, require_live=True):
    """One bounded batch, no retries; the caller controls daily attempt limits."""
    instant = _instant(now or datetime.now(timezone.utc))
    if require_live and not in_continuous_session(instant):
        raise ValueError("Skip quote IO outside the prospective execution window")
    symbols = sorted(set(instruments))
    if not symbols or any(not re.fullmatch(r"(?:SH|SZ)\d{6}", item) for item in symbols):
        raise ValueError("Invalid A-share quote selection")
    if not .5 <= min_interval <= 60 or not 0 < timeout <= 30:
        raise ValueError("Require bounded requests and source spacing")
    root = guard_qlib_output_path(output_dir)
    if root.exists():
        raise ValueError("Quote observation directory exists; keep prior original bytes")
    import requests
    client = session or requests.Session()
    root.mkdir(parents=True, exist_ok=False)
    records, observations = [], {"quotes": {}, "rejected": {}}
    previous = None
    try:
        for batch_number, offset in enumerate(range(0, len(symbols), 50)):
            group = symbols[offset:offset + 50]
            if previous is not None:
                pacing.sleep(max(0, min_interval - (pacing.monotonic() - previous)))
            url = ENDPOINT + ",".join(item.lower() for item in group)
            previous = pacing.monotonic()
            response = client.get(url, timeout=timeout, allow_redirects=False)
            if response.status_code != 200:
                raise ValueError(f"Explicit quote source returned HTTP {response.status_code}; no fallback")
            raw = response.content
            if not raw or len(raw) > 1024 * 1024:
                raise ValueError("Empty or excessive quote response")
            fetched = datetime.now(timezone.utc)
            # Injected now keeps offline fixtures reproducible. Real runs use
            # the receive time, including request latency, for freshness.
            observed_at = instant if now is not None else fetched
            path = root / f"response_{batch_number:03d}.txt"
            path.write_bytes(raw)
            parsed = parse_tencent_observations(raw, group, now=observed_at, require_live=require_live)
            records.append({"file": path.name, "sha256": hashlib.sha256(raw).hexdigest(),
                            "url": url, "fetched_at": fetched.isoformat(), "instruments": group,
                            "parsed_observed_at": _instant(observed_at).isoformat()})
            observations["quotes"].update(parsed["quotes"])
            observations["rejected"].update(parsed["rejected"])
        receipt = {"schema_version": 1, "source": SOURCE, "instruments": symbols,
                   "observed_at": instant.isoformat(), "require_live": require_live,
                   "responses": records, **observations,
                   "complete": not observations["rejected"], "broker_orders_submitted": False}
        (root / "receipt.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
        return receipt
    except Exception as exc:
        (root / "failure.json").write_text(json.dumps({"source": SOURCE, "error_type": type(exc).__name__,
            "error": str(exc)[:300], "responses": records, "broker_orders_submitted": False}, ensure_ascii=False, indent=2) + "\n")
        raise
    finally:
        if session is None:
            client.close()


def load_live_observations(path, *, now=None, audit_only=False):
    """Reparse bound originals; historical saved observations cannot authorize today's fills."""
    root = Path(path)
    receipt = json.loads((root / "receipt.json").read_text())
    if receipt.get("schema_version") != 1 or receipt.get("source") != SOURCE:
        raise ValueError("Wrong prospective quote receipt identity")
    quotes, rejected = {}, {}
    for record in receipt["responses"]:
        if Path(record["file"]).name != record["file"]:
            raise ValueError("Unsafe original quote path")
        raw = (root / record["file"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != record["sha256"]:
            raise ValueError("Original quote bytes changed")
        if record["url"] != ENDPOINT + ",".join(item.lower() for item in record["instruments"]):
            raise ValueError("Original quote request identity changed")
        observed = (datetime.fromisoformat(record["parsed_observed_at"]) if audit_only
                    else now or datetime.now(timezone.utc))
        parsed = parse_tencent_observations(raw, record["instruments"], now=observed,
                                             require_live=receipt["require_live"])
        if set(quotes) & set(parsed["quotes"]):
            raise ValueError("Duplicate prospective quote response")
        quotes.update(parsed["quotes"]); rejected.update(parsed["rejected"])
    if set(quotes) | set(rejected) != set(receipt["instruments"]):
        raise ValueError("Original quote inventory differs")
    if audit_only and (quotes != receipt["quotes"] or rejected != receipt["rejected"]):
        raise ValueError("Parsed observations differ from their original bytes")
    if audit_only:
        quotes = {key: {**value, "live_execution_eligible": False} for key, value in quotes.items()}
    return {**receipt, "quotes": quotes, "rejected": rejected, "complete": not rejected,
            "audit_only": audit_only}
