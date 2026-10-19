"""Append-only, prospective daily-bar research portfolios; never a broker.

Workflow: create -> today's close signal -> next session's completed-day
settlement -> that day's close signal. Every mutation requires the *current*
Shanghai session after 15:05. Historical/OOS bars may be used by the caller to
warm up its frozen target generator, but cannot become forward performance.

The trusted caller injects an aware clock, a source-backed Calendar, targets and
validated same-vintage bars. This is not external timestamp attestation. The
engine neither downloads data nor accesses the personal-account database.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")
STRATEGIES = ("momentum", "lightgbm")
_SHA = re.compile(r"^[0-9a-f]{64}$")
_INSTRUMENT = re.compile(r"^(SH|SZ|BJ)\d{6}$")
_POLICY = "forward-completed-bar-fractional-v1"
LIMITATIONS = [
    "Completed daily bars retrospectively simulate next-open research execution; these are not live fills or a claim that the opening price was executable.",
    "Fractional exposures use adjusted return ratios, not integer shares traded at adjusted prices; corporate-action cash/share books and historical adjustment vintages are not fully verified.",
    "No interest on cash; no volume impact, limit-order queue, minimum commission, lot-size or full T+1 execution model.",
    "Caller-supplied source hashes and clock must be independently validated by the adapter; local checksums are integrity evidence, not external provenance attestation.",
    "Missing sessions halt this forward period. Past/future performance is never backfilled, and historical OOS inputs are warmup only.",
]


class ForwardPaperError(ValueError):
    pass


@dataclass(frozen=True)
class ForwardPaperConfig:
    initial_capital: float = 100000.0
    buy_fee_bps: float = 3.0
    sell_fee_bps: float = 3.0
    slippage_bps: float = 2.0
    rebalance_sessions: int = 20
    close_delay_minutes: int = 5
    max_abs_adjusted_move: float = .4
    timezone: str = "Asia/Shanghai"

    def __post_init__(self):
        _number(self.initial_capital, "initial capital", positive=True)
        for key in ("buy_fee_bps", "sell_fee_bps", "slippage_bps"):
            value = _number(getattr(self, key), key)
            if not 0 <= value <= 1000:
                raise ForwardPaperError("Fees/slippage must be between 0 and 1000 bp.")
        if isinstance(self.rebalance_sessions, bool) or not isinstance(self.rebalance_sessions, int) or self.rebalance_sessions < 1:
            raise ForwardPaperError("Rebalance interval must be a positive integer.")
        if isinstance(self.close_delay_minutes, bool) or not isinstance(self.close_delay_minutes, int) or not 5 <= self.close_delay_minutes <= 240:
            raise ForwardPaperError("Completed-day processing must wait at least five minutes after close.")
        if not 0 < _number(self.max_abs_adjusted_move, "adjusted move limit") <= 1:
            raise ForwardPaperError("Adjusted move rejection limit must lie in (0, 1].")
        if self.timezone != "Asia/Shanghai":
            raise ForwardPaperError("Only the modern CN cash-market Shanghai session is supported.")


def _number(value, label, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or (positive and value <= 0):
        raise ForwardPaperError(f"Invalid {label}.")
    return float(value)


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError):
        raise ForwardPaperError("Evidence must be finite, JSON-serializable data.") from None


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _clone(value):
    return json.loads(_canonical(value))


def _day(value):
    if isinstance(value, datetime):
        raise ForwardPaperError("Use a YYYY-MM-DD session date, not a datetime.")
    try:
        parsed = date.fromisoformat(str(value))
    except ValueError:
        raise ForwardPaperError("Session dates must use YYYY-MM-DD.") from None
    if str(value) != parsed.isoformat():
        raise ForwardPaperError("Session dates must use YYYY-MM-DD.")
    return parsed.isoformat()


def _stamp(value, label):
    try:
        stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError
    except (ValueError, TypeError):
        raise ForwardPaperError(f"{label} must be a timezone-aware timestamp.") from None
    return stamp.astimezone(timezone.utc)


def _root(path):
    target = Path(path).expanduser()
    if target.is_symlink():
        raise ForwardPaperError("Paper directories must not be symlinks.")
    return target.resolve()


@contextmanager
def _lock(path):
    lock = path / ".write.lock"
    if lock.is_symlink():
        raise ForwardPaperError("Paper lock must not be a symlink.")
    descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ForwardPaperError("Another process is updating this forward paper.") from None
        yield
    finally:
        os.close(descriptor)


def _sealed(payload):
    result = _clone(payload)
    result["record_sha256"] = _digest(payload)
    return result


def _read(path):
    if path.is_symlink():
        raise ForwardPaperError("Paper records must not be symlinks.")
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ForwardPaperError(f"Paper record cannot be read: {path.name}.") from None
    if not isinstance(result, dict) or result.get("record_sha256") != _digest({key: value for key, value in result.items() if key != "record_sha256"}):
        raise ForwardPaperError(f"Paper checksum mismatch: {path.name}.")
    return result


def _append(path, payload):
    """Publish complete bytes without replacing an existing artifact."""
    if path.exists() or path.is_symlink():
        current = _read(path)
        if current != payload:
            raise ForwardPaperError(f"Immutable record conflicts with this request: {path.name}.")
        return current
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix=".pending-", dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            os.fchmod(handle.fileno(), 0o600)
            handle.write(_canonical(payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)  # Exclusive atomic publication, not os.replace.
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return payload


def create_forward_paper(path: str | Path, config: ForwardPaperConfig, *, protocol: dict,
                         model: dict, universe: Sequence[str], now: datetime) -> dict:
    """Freeze a separate paper period before recording its first close signal.

    ``protocol.protocol_sha256`` and ``model.model_sha256`` are required source
    references. ``model.trained_through`` must precede the creation date. The
    complete payloads, pool, fees, schedule and policy are additionally hashed.
    Calling again with the same binding returns the original creation record.
    """
    stamp = _stamp(now, "Clock")
    if not isinstance(config, ForwardPaperConfig):
        raise ForwardPaperError("An explicit ForwardPaperConfig is required.")
    if not isinstance(protocol, dict) or not _SHA.fullmatch(str(protocol.get("protocol_sha256", ""))):
        raise ForwardPaperError("A locked protocol SHA-256 reference is required.")
    if not isinstance(model, dict) or not _SHA.fullmatch(str(model.get("model_sha256", ""))):
        raise ForwardPaperError("A frozen model SHA-256 reference is required.")
    if _day(model.get("trained_through")) >= stamp.astimezone(SHANGHAI).date().isoformat():
        raise ForwardPaperError("Frozen model training must end before the forward period begins.")
    if isinstance(universe, str) or not universe or len(set(universe)) != len(universe) or any(not isinstance(item, str) or not _INSTRUMENT.fullmatch(item) for item in universe):
        raise ForwardPaperError("Declare a nonempty unique canonical forward universe.")
    interval = protocol.get("config", {}).get("rebalance_sessions", config.rebalance_sessions)
    if interval != config.rebalance_sessions:
        raise ForwardPaperError("Forward rebalance interval differs from the locked protocol.")
    binding = {"schema_version": 1, "policy_version": _POLICY, "kind": "prospective_bar_research",
               "config": asdict(config), "strategies": list(STRATEGIES), "universe": sorted(universe),
               "protocol": _clone(protocol), "model": _clone(model), "cash_return": 0,
               "execution_ready": False, "limitations": LIMITATIONS}
    target = _root(path)
    target.mkdir(parents=True, exist_ok=True)
    with _lock(target):
        manifest_path = target / "manifest.json"
        if manifest_path.exists():
            current = load_forward_paper(target)
            if any(current.get(key) != value for key, value in binding.items()):
                raise ForwardPaperError("This paper already freezes different rules/model/pool; use a new directory.")
            return current
        if any(item.name != ".write.lock" for item in target.iterdir()):
            raise ForwardPaperError("New paper directory contains unrelated files.")
        return _append(manifest_path, _sealed({**binding, "created_at": stamp.isoformat()}))


def load_forward_paper(path: str | Path) -> dict:
    manifest = _read(_root(path) / "manifest.json")
    if manifest.get("schema_version") != 1 or manifest.get("policy_version") != _POLICY or manifest.get("strategies") != list(STRATEGIES):
        raise ForwardPaperError("Unsupported forward paper manifest.")
    ForwardPaperConfig(**manifest["config"])
    return manifest


def _today(manifest, day, calendar, now):
    day, stamp = _day(day), _stamp(now, "Clock")
    local = stamp.astimezone(SHANGHAI)
    if day != local.date().isoformat():
        raise ForwardPaperError("Forward writes are only allowed for today's Shanghai session; no historical/future backfill.")
    if stamp < _stamp(manifest["created_at"], "Creation time"):
        raise ForwardPaperError("Clock precedes the frozen forward period.")
    if not calendar.is_session(day):
        raise ForwardPaperError("Today is not a verified exchange session.")
    close = datetime.combine(local.date(), time(15), SHANGHAI)
    delay = manifest["config"]["close_delay_minutes"]
    if local < close + timedelta(minutes=delay):
        raise ForwardPaperError("Wait for the completed session and the post-close publication delay.")
    calendar_payload = calendar.to_dict()
    if not calendar_payload.get("source") or not calendar_payload.get("source_url") or not _SHA.fullmatch(str(calendar_payload.get("sha256", ""))):
        raise ForwardPaperError("A source-backed complete exchange calendar is required.")
    if _stamp(calendar_payload["fetched_at"], "Calendar retrieval") > stamp:
        raise ForwardPaperError("Calendar evidence was retrieved after the injected clock.")
    return day, stamp, close, calendar_payload


def _verified_calendar(payload):
    from stopat30m.data.research_calendar import Calendar
    if not isinstance(payload, dict) or payload.get("schema_version") != 1 or payload.get("timezone") != "Asia/Shanghai" or payload.get("sha256") != _digest({key: value for key, value in payload.items() if key != "sha256"}):
        raise ForwardPaperError("Exchange calendar checksum/schema is invalid.")
    try:
        return Calendar.from_rows(payload["days"], payload["coverage_start"], payload["coverage_end"],
                                  source=payload["source"], source_url=payload["source_url"],
                                  fetched_at=payload["fetched_at"], market=payload["market"])
    except (KeyError, TypeError, ValueError):
        raise ForwardPaperError("Exchange calendar must contain complete, source-backed natural-day flags.") from None


def _evidence(value, manifest, day, stamp, close):
    if not isinstance(value, dict):
        raise ForwardPaperError("Source evidence is required.")
    result = _clone(value)
    if result.get("role") != "forward_completed_daily" or result.get("completed") is not True or result.get("session_date") != day:
        raise ForwardPaperError("Only today's completed forward bars qualify; historical OOS/warmup is not forward evidence.")
    if not isinstance(result.get("source"), str) or not result["source"].strip():
        raise ForwardPaperError("Price source identity is required.")
    for key in ("source_snapshot_sha256", "source_manifest_sha256", "model_sha256", "protocol_sha256"):
        if not _SHA.fullmatch(str(result.get(key, ""))):
            raise ForwardPaperError(f"Missing valid evidence hash: {key}.")
    if result["model_sha256"] != manifest["model"]["model_sha256"] or result["protocol_sha256"] != manifest["protocol"]["protocol_sha256"]:
        raise ForwardPaperError("Evidence differs from the frozen model/protocol.")
    fetched = _stamp(result.get("fetched_at"), "Source retrieval")
    if fetched < close.astimezone(timezone.utc) or fetched > stamp or fetched.astimezone(SHANGHAI).date().isoformat() != day:
        raise ForwardPaperError("Source retrieval must be today's post-close evidence, no stale/future data.")
    result["fetched_at"] = fetched.isoformat()
    return result


def _bars(value, manifest, day, previous):
    """Each instrument requires same-vintage current and previous daily marks.

    Mapping fields: session_date, previous_session (YYYY-MM-DD), raw_open,
    raw_close, adjusted_open, adjusted_close, previous_raw_close,
    previous_adjusted_close, complete=True; optional tradestatus. All six prices
    are positive. This adapter contract deliberately never accepts live quotes.
    """
    if not isinstance(value, Mapping) or set(value) != set(manifest["universe"]):
        raise ForwardPaperError("Completed bars must cover the entire frozen universe.")
    result = {}
    for instrument in manifest["universe"]:
        row = value[instrument]
        if not isinstance(row, Mapping) or row.get("session_date") != day or row.get("previous_session") != previous or row.get("complete") is not True:
            raise ForwardPaperError("Bars require today's complete session and the exact previous exchange session.")
        if str(row.get("tradestatus", "unknown")).lower() in {"0", "false", "suspended", "halted"}:
            raise ForwardPaperError("Known suspended bars do not support this opening-execution model.")
        result[instrument] = {"session_date": day, "previous_session": previous, "complete": True,
                              "tradestatus": str(row.get("tradestatus", "unknown"))}
        for key in ("raw_open", "raw_close", "adjusted_open", "adjusted_close", "previous_raw_close", "previous_adjusted_close"):
            result[instrument][key] = _number(row.get(key), f"{instrument} {key}", positive=True)
        row = result[instrument]
        limit = manifest["config"]["max_abs_adjusted_move"]
        returns = (row["adjusted_open"] / row["previous_adjusted_close"] - 1,
                   row["adjusted_close"] / row["adjusted_open"] - 1,
                   row["adjusted_close"] / row["previous_adjusted_close"] - 1)
        if any(abs(change) > limit for change in returns):
            raise ForwardPaperError("Suspicious adjusted-price discontinuity; verify company actions/source instead of repairing prices.")
    return result


def _targets(value, manifest):
    if not isinstance(value, Mapping) or set(value) != set(STRATEGIES):
        raise ForwardPaperError("Both frozen strategies must supply targets in the same run.")
    result = {}
    for strategy in STRATEGIES:
        weights = value[strategy]
        if not isinstance(weights, Mapping) or set(weights) - set(manifest["universe"]):
            raise ForwardPaperError("Target instruments differ from the frozen universe.")
        checked = {instrument: _number(weight, "target weight") for instrument, weight in weights.items()}
        if any(weight < 0 or weight > 1 for weight in checked.values()) or sum(checked.values()) > 1 + 1e-12:
            raise ForwardPaperError("Targets must be long-only, unlevered weights; unallocated weight stays in zero-yield cash.")
        result[strategy] = {key: checked[key] for key in sorted(checked) if checked[key] > 0}
    return result


def _records(root, folder):
    location = root / folder
    if location.is_symlink():
        raise ForwardPaperError("Artifact directories must not be symlinks.")
    result = []
    for path in sorted(location.glob("*.json")) if location.exists() else []:
        record = _read(path)
        if path.stem != _day(record.get("session_date")):
            raise ForwardPaperError("Forward artifact filename does not match its session.")
        result.append(record)
    return result


def _check_halted(root):
    if _records(root, "gaps"):
        raise ForwardPaperError("This forward period has a recorded session gap and is halted; do not backfill it.")


def _gap(root, manifest, day, stamp, reason):
    target = root / "gaps" / f"{day}.json"
    if not target.exists():
        _append(target, _sealed({"kind": "halted_gap", "session_date": day, "recorded_at": stamp.isoformat(),
                                "manifest_sha256": manifest["record_sha256"], "reason": reason,
                                "performance_recorded": False}))
    raise ForwardPaperError(f"Forward session gap: {reason}; this period is halted without backfilled performance.")


def _replay(path, input_hash):
    if not path.exists():
        return None
    current = _read(path)
    if current.get("input_sha256") != input_hash:
        raise ForwardPaperError("Today's immutable evidence differs; existing forward artifacts cannot be overwritten.")
    return {**current, "replayed": True}


def record_close_signal(path: str | Path, session_date: str, targets: dict, *, calendar,
                        evidence: dict, now: datetime, rebalance: bool | None = None) -> dict:
    """Freeze today's next-session targets; call only after today's settlement.

    The first signal starts a new all-cash period, without fabricating a prior
    NAV. Evidence must include ``bars`` using the documented _bars contract.
    ``rebalance`` may confirm, but never override, the frozen session cadence.
    On non-rebalance days targets are diagnostic only: existing exposures drift.
    Exact input retries return the original artifact with ``replayed=True``.
    """
    root = _root(path)
    with _lock(root):
        manifest = load_forward_paper(root)
        _status_unlocked(root, manifest)
        calendar = _verified_calendar(calendar.to_dict())
        day, stamp, close, calendar_payload = _today(manifest, session_date, calendar, now)
        _check_halted(root)
        previous, execution = calendar.previous_session(day), calendar.next_session(day)
        source = _evidence(evidence, manifest, day, stamp, close)
        bars = _bars(source.pop("bars", None), manifest, day, previous)
        checked_targets = _targets(targets, manifest)
        signals = _records(root, "signals")
        anchor = signals[0]["execution_session"] if signals else execution
        sessions = calendar.sessions(anchor, execution)
        if not sessions or sessions[-1] != execution:
            raise ForwardPaperError("Verified calendar does not cover the forward cadence.")
        should_rebalance = (len(sessions) - 1) % manifest["config"]["rebalance_sessions"] == 0
        if rebalance is not None and (not isinstance(rebalance, bool) or rebalance != should_rebalance):
            raise ForwardPaperError("Caller rebalance flag conflicts with the frozen session cadence.")
        inputs = {"session_date": day, "execution_session": execution, "targets": checked_targets,
                  "rebalance": should_rebalance, "source": source, "bars": bars,
                  "calendar": calendar_payload, "manifest_sha256": manifest["record_sha256"]}
        target = root / "signals" / f"{day}.json"
        replay = _replay(target, _digest(inputs))
        if replay:
            return replay
        if signals:
            if signals[-1]["session_date"] > day:
                raise ForwardPaperError("Cannot append signals behind existing future evidence.")
            if source["source"] != signals[0]["source"]["source"]:
                raise ForwardPaperError("Price source changed during this frozen forward period.")
            if not (root / "sessions" / f"{day}.json").exists():
                _gap(root, manifest, day, stamp, "today's completed settlement is missing before the next close signal")
            settlement = _read(root / "sessions" / f"{day}.json")
            if settlement["bars"] != bars:
                raise ForwardPaperError("Signal and settlement must use the same completed price bars.")
        payload = _sealed({"kind": "close_signal", **inputs, "input_sha256": _digest(inputs),
                           "recorded_at": stamp.isoformat(), "anchor_execution_session": anchor,
                           "target_role": "rebalance_weights" if should_rebalance else "diagnostic_only_hold_existing"})
        return {**_append(target, payload), "replayed": False}


def _rebalance(cash, old_values, weights, config, day, bars):
    equity = cash + sum(old_values.values())
    instruments = sorted(set(old_values) | set(weights))
    def deltas(post):
        return {instrument: weights.get(instrument, 0) * post - old_values.get(instrument, 0) for instrument in instruments}
    def cost(post):
        return sum(abs(delta) * ((config.buy_fee_bps if delta > 0 else config.sell_fee_bps) + config.slippage_bps) / 10000 for delta in deltas(post).values())
    low, high = 0.0, equity
    for _ in range(80):
        mid = low + (high - low) / 2
        if mid + cost(mid) > equity:
            high = mid
        else:
            low = mid
    post = low + (high - low) / 2
    trades, fees, slippage = [], 0.0, 0.0
    for instrument, delta in deltas(post).items():
        if abs(delta) <= equity * 1e-12:
            continue
        fee = abs(delta) * (config.buy_fee_bps if delta > 0 else config.sell_fee_bps) / 10000
        slip = abs(delta) * config.slippage_bps / 10000
        fees += fee
        slippage += slip
        trades.append({"instrument": instrument, "direction": "BUY" if delta > 0 else "SELL",
                       "notional": abs(delta), "fee": fee, "slippage": slip,
                       "execution_session": day, "raw_open_reference": bars[instrument]["raw_open"],
                       "price_model": "completed_bar_open_fractional_exposure", "live_fill": False})
    values = {instrument: weight * post for instrument, weight in weights.items() if weight > 0}
    result_cash = post - sum(values.values())
    if result_cash < -1e-7 or not math.isclose(equity - post, fees + slippage, rel_tol=1e-9, abs_tol=1e-7):
        raise ForwardPaperError("Forward cost/cash allocation did not reconcile.")
    return max(0.0, result_cash), values, trades, fees, slippage


def settle_completed_session(path: str | Path, session_date: str, bars: dict, *, calendar,
                             evidence: dict, now: datetime) -> dict:
    """After 15:05, simulate today's open from the *previous* frozen signal.

    ``bars[instrument]`` has session_date, previous_session, raw_open/raw_close,
    adjusted_open/adjusted_close, previous_raw_close/previous_adjusted_close,
    complete=True and optional tradestatus. Previous prices must be from the
    same source snapshot/vintage as current prices; previous_raw_close must
    match the immutable previous signal. No live quote or previous-day write is
    accepted. NAV and execution records are committed in one atomic day record.
    """
    root = _root(path)
    with _lock(root):
        manifest = load_forward_paper(root)
        _status_unlocked(root, manifest)
        calendar = _verified_calendar(calendar.to_dict())
        day, stamp, close, calendar_payload = _today(manifest, session_date, calendar, now)
        _check_halted(root)
        previous = calendar.previous_session(day)
        source = _evidence(evidence, manifest, day, stamp, close)
        if "bars" in source:
            raise ForwardPaperError("Settlement bars are a separate argument, not duplicated in evidence.")
        current_bars = _bars(bars, manifest, day, previous)
        inputs = {"session_date": day, "source": source, "bars": current_bars,
                  "calendar": calendar_payload, "manifest_sha256": manifest["record_sha256"]}
        target = root / "sessions" / f"{day}.json"
        replay = _replay(target, _digest(inputs))
        if replay:
            return replay
        signals = _records(root, "signals")
        signal_path = root / "signals" / f"{previous}.json"
        if not signals:
            raise ForwardPaperError("Create today's first close signal before any next-session settlement.")
        if not signal_path.exists():
            _gap(root, manifest, day, stamp, "the immediately previous session has no frozen close signal")
        signal = _read(signal_path)
        if signal["execution_session"] != day:
            raise ForwardPaperError("Calendar changed: frozen execution session does not match today.")
        if source["source"] != signal["source"]["source"]:
            raise ForwardPaperError("Settlement source differs from the frozen close evidence.")
        for instrument in manifest["universe"]:
            if not math.isclose(current_bars[instrument]["previous_raw_close"], signal["bars"][instrument]["raw_close"], rel_tol=1e-10, abs_tol=1e-9):
                raise ForwardPaperError("Previous raw close was revised or belongs to another source; no silent return rebasing.")
        history = _records(root, "sessions")
        if history and history[-1]["session_date"] != previous:
            _gap(root, manifest, day, stamp, "the immediately previous session has no saved closing NAV")
        if not history and signals[0]["execution_session"] != day:
            _gap(root, manifest, day, stamp, "the first frozen execution session was missed")
        config = ForwardPaperConfig(**manifest["config"])
        result = {}
        for strategy in STRATEGIES:
            old = history[-1]["portfolios"][strategy] if history else {"cash": config.initial_capital, "positions": {}, "equity": config.initial_capital, "total_fees": 0.0, "total_slippage": 0.0}
            opening = {instrument: value * current_bars[instrument]["adjusted_open"] / current_bars[instrument]["previous_adjusted_close"] for instrument, value in old["positions"].items()}
            pre_trade = old["cash"] + sum(opening.values())
            if signal["rebalance"]:
                cash, values, trades, fees, slippage = _rebalance(old["cash"], opening, signal["targets"][strategy], config, day, current_bars)
            else:
                cash, values, trades, fees, slippage = old["cash"], opening, [], 0.0, 0.0
            closing = {instrument: value * current_bars[instrument]["adjusted_close"] / current_bars[instrument]["adjusted_open"] for instrument, value in values.items()}
            equity = cash + sum(closing.values())
            if not math.isfinite(equity) or equity <= 0:
                raise ForwardPaperError("Invalid forward closing equity.")
            result[strategy] = {"cash": cash, "positions": closing, "weights": {instrument: value / equity for instrument, value in closing.items()},
                                "equity": equity, "nav": equity / config.initial_capital,
                                "daily_return": equity / old["equity"] - 1, "overnight_return": pre_trade / old["equity"] - 1,
                                "fee": fees, "slippage": slippage, "total_fees": old["total_fees"] + fees,
                                "total_slippage": old["total_slippage"] + slippage, "trades": trades,
                                "one_way_turnover": sum(trade["notional"] for trade in trades) / pre_trade / 2,
                                "cash_return": 0.0, "initial_capital": config.initial_capital}
        payload = _sealed({"kind": "completed_session", **inputs, "input_sha256": _digest(inputs),
                           "recorded_at": stamp.isoformat(), "signal_record_sha256": signal["record_sha256"],
                           "previous_session_record_sha256": history[-1]["record_sha256"] if history else None,
                           "signal_session": previous, "rebalance": signal["rebalance"],
                           "execution_model": "after-close acknowledgement of completed-bar open research exposure; not live trading",
                           "portfolios": result, "execution_ready": False})
        return {**_append(target, payload), "replayed": False}


def forward_paper_status(path: str | Path) -> dict:
    """Read/check the complete local chain without downloading or trading.

    Full latest signal/settlement records are returned for an idempotent daily
    CLI to skip already completed phases before acquiring new source data.
    No settlement means ``has_performance=False``: initial cash is not a
    fabricated performance observation. Returns are cost-net, not annualized.
    """
    root = _root(path)
    with _lock(root):
        manifest = load_forward_paper(root)
        return _status_unlocked(root, manifest)


def _status_unlocked(root, manifest):
    """Validate before both reads and writes; the caller already holds the lock."""
    signals, sessions, gaps = (_records(root, kind) for kind in ("signals", "sessions", "gaps"))
    by_day = {item.get("session_date"): item for item in signals}
    session_by_day = {item.get("session_date"): item for item in sessions}
    for kind, records in (("close_signal", signals), ("completed_session", sessions), ("halted_gap", gaps)):
        for item in records:
            if item.get("kind") != kind or item.get("manifest_sha256") != manifest["record_sha256"]:
                raise ForwardPaperError("Forward record has the wrong kind/manifest binding.")
            day = _day(item.get("session_date"))
            if day != _stamp(item.get("recorded_at"), "Record time").astimezone(SHANGHAI).date().isoformat():
                raise ForwardPaperError("Forward record is not a same-day observation.")
            if kind == "halted_gap":
                if item.get("performance_recorded") is not False:
                    raise ForwardPaperError("A session gap must not contain invented performance.")
                continue
            calendar = _verified_calendar(item.get("calendar"))
            _, stamp, close, _ = _today(manifest, day, calendar, _stamp(item["recorded_at"], "Record time"))
            _evidence(item.get("source"), manifest, day, stamp, close)
            _bars(item.get("bars"), manifest, day, calendar.previous_session(day))
            keys = ["session_date", "source", "bars", "calendar", "manifest_sha256"]
            if kind == "close_signal":
                keys += ["execution_session", "targets", "rebalance"]
                _targets(item.get("targets"), manifest)
                if item["execution_session"] != calendar.next_session(day):
                    raise ForwardPaperError("Signal execution session is not the exact next verified session.")
            if item.get("input_sha256") != _digest({key: item[key] for key in keys}):
                raise ForwardPaperError("Forward input binding checksum is invalid.")
    if sessions and not signals:
        raise ForwardPaperError("Settlement records have no frozen signals.")
    for index, signal in enumerate(signals):
        calendar = _verified_calendar(signal["calendar"])
        anchor = signals[0]["execution_session"]
        scheduled = (len(calendar.sessions(anchor, signal["execution_session"])) - 1) % manifest["config"]["rebalance_sessions"] == 0
        if signal.get("anchor_execution_session") != anchor or signal["rebalance"] is not scheduled:
            raise ForwardPaperError("Forward rebalance cadence/anchor is inconsistent.")
        if index and signal["session_date"] not in session_by_day:
            raise ForwardPaperError("A later signal is missing its preceding same-day settlement.")
        if signal["source"]["source"] != signals[0]["source"]["source"]:
            raise ForwardPaperError("Forward price source changed between signals.")
        if index and signal["bars"] != session_by_day[signal["session_date"]]["bars"]:
            raise ForwardPaperError("Same-day signal and settlement bars differ.")
    previous_record = None
    for session in sessions:
        calendar = _verified_calendar(session["calendar"])
        previous = calendar.previous_session(session["session_date"])
        signal = by_day.get(previous)
        if not signal or signal["execution_session"] != session["session_date"] or session.get("signal_record_sha256") != signal["record_sha256"]:
            raise ForwardPaperError("A settlement is missing its exact previous-session signal.")
        if session.get("signal_session") != previous or session.get("rebalance") is not signal["rebalance"]:
            raise ForwardPaperError("Settlement signal/session policy differs from the frozen decision.")
        if session["source"]["source"] != signal["source"]["source"]:
            raise ForwardPaperError("Settlement source differs from its signal source.")
        if previous_record:
            if previous_record["session_date"] != previous or session.get("previous_session_record_sha256") != previous_record["record_sha256"]:
                raise ForwardPaperError("Forward closing NAV chain has a missing/replaced session.")
        elif session.get("previous_session_record_sha256") is not None or session["session_date"] != signals[0]["execution_session"]:
            raise ForwardPaperError("Forward closing NAV chain has no valid initial execution anchor.")
        if set(session.get("portfolios", {})) != set(STRATEGIES):
            raise ForwardPaperError("Both strategy portfolios must be present.")
        for strategy in STRATEGIES:
            portfolio = session["portfolios"][strategy]
            equity = _number(portfolio.get("equity"), "saved equity", positive=True)
            cash = _number(portfolio.get("cash"), "saved cash")
            positions = portfolio.get("positions", {})
            if cash < 0 or set(positions) - set(manifest["universe"]) or any(_number(value, "position value") < 0 for value in positions.values()):
                raise ForwardPaperError("Saved fractional holdings/cash are invalid.")
            if not math.isclose(equity, cash + sum(positions.values()), rel_tol=1e-10, abs_tol=1e-7):
                raise ForwardPaperError("Saved forward equity does not reconcile to cash and positions.")
            old = previous_record["portfolios"][strategy] if previous_record else {"equity": manifest["config"]["initial_capital"], "total_fees": 0, "total_slippage": 0}
            checks = ((portfolio.get("nav"), equity / manifest["config"]["initial_capital"]),
                      (portfolio.get("daily_return"), equity / old["equity"] - 1),
                      (portfolio.get("total_fees"), old["total_fees"] + portfolio["fee"]),
                      (portfolio.get("total_slippage"), old["total_slippage"] + portfolio["slippage"]))
            if any(not math.isclose(_number(value, "saved accounting value"), expected, rel_tol=1e-10, abs_tol=1e-8) for value, expected in checks):
                raise ForwardPaperError("Saved forward returns/cumulative costs do not reconcile.")
        previous_record = session
    summary = {}
    initial = manifest["config"]["initial_capital"]
    for strategy in STRATEGIES:
        curve = [initial, *(item["portfolios"][strategy]["equity"] for item in sessions)]
        peak, max_drawdown = initial, 0.0
        for equity in curve:
            peak = max(peak, equity)
            max_drawdown = min(max_drawdown, equity / peak - 1)
        latest = sessions[-1]["portfolios"][strategy] if sessions else {"equity": initial, "cash": initial, "positions": {}, "nav": 1.0, "total_fees": 0.0, "total_slippage": 0.0}
        summary[strategy] = {**latest, "cumulative_net_return": latest["equity"] / initial - 1,
                             "max_drawdown": max_drawdown, "current_drawdown": latest["equity"] / peak - 1}
    return {"manifest": manifest, "latest_signal_date": signals[-1]["session_date"] if signals else None,
            "latest_settlement_date": sessions[-1]["session_date"] if sessions else None,
            "latest_signal": signals[-1] if signals else None, "latest_settlement": sessions[-1] if sessions else None,
            "has_performance": bool(sessions), "session_count": len(sessions), "portfolios": summary,
            "halted": bool(gaps), "gaps": gaps, "execution_ready": False}
