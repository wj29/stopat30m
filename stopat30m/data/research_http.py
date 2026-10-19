"""Polite, resumable public research downloads; no host/proxy fallbacks."""
from __future__ import annotations

import hashlib
import json
import math
import random as _random
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_HTTP_LOCK = threading.RLock()
_LAST_REQUEST: dict[tuple[str, int], float] = {}
_RETRYABLE = {408, 429, *range(500, 600)}
_ENDPOINT = "https://push2his.eastmoney.com/api/qt/stock/kline/get"


class ResearchDownloadError(ValueError):
    """A request failed; successful cached segments remain available for resume."""


def _encoded(value) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()


def _hash(value) -> str:
    return hashlib.sha256(_encoded(value)).hexdigest()


def _atomic(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_bytes(_encoded(value))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class ResearchHTTPTransport:
    """One serialized host, >=2 seconds between requests, bounded retry waits.

    A generation pins all raw/adjusted responses of one logical artifact
    download. Failed downloads resume the active generation. Explicit refresh
    starts a new one, including its raw prices, never reusing old QFQ/HFQ bars.
    The injected clock returns epoch seconds; injected sleep advances it in
    tests. A public response is cached only after its caller validates schema.
    """

    def __init__(self, session, *, cache_dir=None, clock=None, sleep=None, random=None,
                 min_interval: float = 2, max_attempts: int = 4, max_wait: float = 60):
        if (not math.isfinite(min_interval) or min_interval < 2 or not isinstance(max_attempts, int)
                or not 1 <= max_attempts <= 8 or not math.isfinite(max_wait) or not 0 < max_wait <= 60):
            raise ValueError("Require interval>=2s, 1–8 attempts and 0<max_wait<=60s.")
        self.session = session
        self.clock, self.sleep = clock or time.time, sleep or time.sleep
        self.random = random or _random.random
        self.min_interval, self.max_attempts, self.max_wait = min_interval, max_attempts, max_wait
        self.cache_dir = None if cache_dir is None else Path(cache_dir).expanduser().resolve()
        self.generation = None
        self._scope = None
        self._active_path = None
        self._last_request = None
        self._blocked_until = 0.0
        self._records = {}
        self._generation_date = None

    def configure_cache(self, path) -> None:
        if self.cache_dir is None:
            self.cache_dir = Path(path).expanduser().resolve()

    @contextmanager
    def download_lock(self):
        """Exclude another artifact downloader sharing this cache, without waiting."""
        if self.cache_dir is None:
            raise ResearchDownloadError("Configure persistent research cache before acquiring its download lock.")
        import fcntl
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        with (self.cache_dir / "download.lock").open("a+b") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ResearchDownloadError("Research download cache is busy in another process; retry after it finishes.") from exc
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _scope_path(self, request):
        if self.cache_dir is None:
            return None
        return self.cache_dir / "requests" / _hash(request) / "active.json"

    def _read_active(self, request):
        path = self._scope_path(request)
        if path is None or not path.exists():
            return None
        try:
            active = json.loads(path.read_bytes())
            if active.get("request") != request or active.get("request_sha256") != _hash(request):
                raise ResearchDownloadError("Download generation request identity mismatch.")
            if not re.fullmatch(r"[0-9a-f]{32}", str(active.get("generation", ""))):
                raise ResearchDownloadError("Invalid download generation identifier.")
            generation_path = self.cache_dir / "generations" / active["generation"] / "request.json"
            recorded = json.loads(generation_path.read_bytes())
            if recorded != {"request": request, "request_sha256": _hash(request), "generation": active["generation"]}:
                raise ResearchDownloadError("Download generation manifest does not match active request.")
            if active.get("status") not in {"pending", "complete"}:
                raise ResearchDownloadError("Invalid download generation status.")
            return active
        except (OSError, TypeError, ValueError) as exc:
            if isinstance(exc, ResearchDownloadError):
                raise
            raise ResearchDownloadError("Cannot validate active download generation.") from exc

    def has_pending(self, request) -> bool:
        active = self._read_active(request)
        return active is not None and active["status"] == "pending"

    def begin_request(self, request, *, refresh: bool = False) -> str:
        active = None if refresh else self._read_active(request)
        today = datetime.fromtimestamp(self.clock(), ZoneInfo("Asia/Shanghai")).date().isoformat()
        if active is not None and active["status"] == "pending" and active.get("as_of_date") != today:
            raise ResearchDownloadError("Incomplete adjusted-price generation is from another Shanghai date; "
                                        "explicit refresh is required to avoid mixing adjustment vintages.")
        if active is None or active["status"] == "complete":
            generation = uuid.uuid4().hex
            active = {"request": request, "request_sha256": _hash(request), "generation": generation,
                      "status": "pending", "created_at": datetime.fromtimestamp(self.clock(), timezone.utc).isoformat(),
                      "as_of_date": today, "resume_scope": "same_Shanghai_natural_day"}
            if self.cache_dir is not None:
                _atomic(self.cache_dir / "generations" / generation / "request.json",
                        {key: active[key] for key in ("request", "request_sha256", "generation")})
                _atomic(self._scope_path(request), active)
        self._scope, self.generation = request, active["generation"]
        self._active_path = self._scope_path(request)
        self._generation_date = active["as_of_date"]
        self._records = {}
        return self.generation

    def complete(self, artifact_id: str) -> None:
        if self._active_path is not None:
            active = self._read_active(self._scope)
            if active["generation"] != self.generation:
                raise ResearchDownloadError("Download generation changed during artifact construction.")
            _atomic(self._active_path, {**active, "status": "complete", "artifact_id": artifact_id})

    def _segment(self, params):
        identity = {"endpoint": _ENDPOINT, "params": params, "generation": self.generation}
        if self.cache_dir is None or self.generation is None:
            return identity, None
        return identity, self.cache_dir / "generations" / self.generation / "responses" / f"{_hash(identity)}.json"

    def _load(self, path, identity, validate):
        try:
            wrapper = json.loads(path.read_bytes())
            if wrapper.get("schema_version") != 1 or wrapper.get("request") != identity:
                raise ResearchDownloadError("Cached segment request identity mismatch.")
            if wrapper.get("payload_sha256") != _hash(wrapper.get("payload")):
                raise ResearchDownloadError("Cached segment checksum mismatch.")
            recorded = {key: wrapper.get(key) for key in ("request", "payload_sha256", "fetched_at")}
            if wrapper.get("record_sha256") != _hash(recorded):
                raise ResearchDownloadError("Cached segment metadata checksum mismatch.")
            validate(wrapper["payload"])
            self._record(identity, wrapper)
            return wrapper["payload"]
        except (OSError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, ResearchDownloadError):
                raise
            raise ResearchDownloadError("Cached research segment is malformed; refresh requires a new generation.") from exc

    def _record(self, identity, wrapper):
        fetched = wrapper.get("fetched_at")
        try:
            parsed = datetime.fromisoformat(fetched)
            if parsed.tzinfo is None or parsed.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat() != self._generation_date:
                raise ValueError("response timestamp differs from generation date")
            if parsed.timestamp() > self.clock() + 1:
                raise ValueError("response timestamp is in the future")
        except (TypeError, ValueError) as exc:
            raise ResearchDownloadError("Cached response has an invalid generation timestamp.") from exc
        self._records[_hash(identity)] = {"request_sha256": _hash(identity),
            "params": identity["params"], "payload_sha256": wrapper["payload_sha256"], "fetched_at": fetched}

    def provenance(self):
        records = sorted(self._records.values(), key=lambda record: record["request_sha256"])
        times = sorted(record["fetched_at"] for record in records)
        return {"generation": self.generation, "as_of_date": self._generation_date,
            "resume_scope": "same_Shanghai_natural_day", "atomic_supplier_vintage_verified": False,
            "first_response_at": times[0] if times else None, "last_response_at": times[-1] if times else None,
            "min_interval_seconds": self.min_interval, "max_attempts": self.max_attempts, "responses": records}

    def _block_path(self):
        return None if self.cache_dir is None else self.cache_dir / "blocked_until.json"

    def _check_blocked(self) -> None:
        path = self._block_path()
        if path is not None and path.exists():
            try:
                block = json.loads(path.read_bytes())
                if block["endpoint"] != _ENDPOINT:
                    raise ResearchDownloadError("Rate-limit state has a different endpoint identity.")
                deadline = float(block["blocked_until"])
                if not math.isfinite(deadline):
                    raise ValueError("non-finite rate-limit deadline")
                self._blocked_until = max(self._blocked_until, deadline)
            except (OSError, KeyError, TypeError, ValueError) as exc:
                raise ResearchDownloadError("Invalid persisted rate-limit state.") from exc
        if self._blocked_until > self.clock():
            try:
                resume = datetime.fromtimestamp(self._blocked_until, timezone.utc).isoformat()
            except (OverflowError, ValueError, OSError):
                resume = f"epoch_seconds:{self._blocked_until}"
            raise ResearchDownloadError(f"Eastmoney rate limit: blocked_until={resume}; resume later. "
                "Disable refresh for same-day resume; a crossed Shanghai date requires explicit refresh.")

    def _defer(self, seconds: float) -> None:
        self._save_block(seconds)
        self._check_blocked()

    def _save_block(self, seconds: float) -> None:
        self._blocked_until = self.clock() + seconds
        path = self._block_path()
        if path is not None:
            _atomic(path, {"endpoint": _ENDPOINT, "blocked_until": self._blocked_until})

    def _retry_after(self, response) -> float:
        value = str(getattr(response, "headers", {}).get("Retry-After", "")).strip()
        if not value:
            return 0
        try:
            seconds = float(value)
            return max(0, seconds) if math.isfinite(seconds) else 0
        except ValueError:
            try:
                parsed = parsedate_to_datetime(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return max(0, parsed.timestamp() - self.clock())
            except (ValueError, OverflowError, TypeError):
                return 0

    def _wait(self, delay: float) -> None:
        if delay > self.max_wait:
            self._defer(delay)
        if delay > 0:
            self.sleep(delay)

    def _last_persisted_request(self):
        path = None if self.cache_dir is None else self.cache_dir / "last_request.json"
        if path is None or not path.exists():
            return None
        try:
            state = json.loads(path.read_bytes())
            stamp = float(state["issued_at"])
            if state["endpoint"] != _ENDPOINT or not math.isfinite(stamp):
                raise ValueError("request timer identity or time invalid")
            return stamp
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise ResearchDownloadError("Invalid persisted request-spacing timer.") from exc

    def get(self, params: dict, *, timeout: float, headers: dict, validate):
        self._check_generation_date()
        identity, path = self._segment(params)
        if path is not None and path.exists():
            return self._load(path, identity, validate)
        # Hold one host lock through retries/waits to serialize every real call.
        # Clock identity isolates deterministic fake clocks from the real host.
        with _HTTP_LOCK:
            self._check_blocked()
            for attempt in range(self.max_attempts):
                rate_key = (_ENDPOINT, id(self.clock))
                previous = _LAST_REQUEST.get(rate_key, self._last_request)
                persisted = self._last_persisted_request()
                if persisted is not None:
                    previous = persisted if previous is None else max(previous, persisted)
                if previous is not None:
                    self._wait(max(0, previous + self.min_interval - self.clock()))
                self._check_generation_date()
                now = self.clock()
                _LAST_REQUEST[rate_key] = self._last_request = now
                if self.cache_dir is not None:
                    _atomic(self.cache_dir / "last_request.json", {"endpoint": _ENDPOINT, "issued_at": now})
                response = None
                try:
                    response = self.session.get(_ENDPOINT, params=params, timeout=timeout, headers=headers, allow_redirects=False)
                except Exception as exc:
                    # Restrict retries to actual transport failures. Programming
                    # errors and arbitrary fake-client exceptions must surface.
                    import requests
                    if not isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                                            ConnectionError, TimeoutError)):
                        raise ResearchDownloadError(f"Eastmoney request failed: {type(exc).__name__}") from exc
                    if attempt + 1 == self.max_attempts:
                        raise ResearchDownloadError(f"Eastmoney connection retries exhausted ({self.max_attempts} attempts).") from exc
                    self._wait(min(2 ** attempt * 2, 30) + self.random())
                    continue
                status = int(response.status_code)
                if status in _RETRYABLE:
                    retry_after = self._retry_after(response)
                    if retry_after:
                        self._save_block(retry_after)
                    if retry_after > self.max_wait:
                        self._defer(retry_after)
                    if attempt + 1 == self.max_attempts:
                        # Preserve even a short explicit rate-limit deadline so
                        # restarting the process cannot bypass Retry-After.
                        if retry_after:
                            self._defer(retry_after)
                        raise ResearchDownloadError(f"Eastmoney HTTP {status}: retries exhausted ({self.max_attempts} attempts).")
                    self._wait(max(retry_after, min(2 ** attempt * 2, 30) + self.random()))
                    continue
                if not 200 <= status < 300:
                    raise ResearchDownloadError(f"Eastmoney HTTP {status}: non-retryable response.")
                try:
                    payload = response.json()
                except (TypeError, ValueError) as exc:
                    raise ResearchDownloadError("Eastmoney returned invalid JSON/HTML; response was not cached or retried.") from exc
                validate(payload)
                wrapper = {"schema_version": 1, "request": identity, "payload": payload,
                    "payload_sha256": _hash(payload),
                    "fetched_at": datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()}
                wrapper["record_sha256"] = _hash({key: wrapper[key] for key in ("request", "payload_sha256", "fetched_at")})
                self._check_generation_date()
                if self.generation is not None:
                    self._record(identity, wrapper)
                if path is not None:
                    _atomic(path, wrapper)
                return payload
        raise ResearchDownloadError("Research request did not complete.")

    def _check_generation_date(self):
        if self.generation is not None:
            today = datetime.fromtimestamp(self.clock(), ZoneInfo("Asia/Shanghai")).date().isoformat()
            if today != self._generation_date:
                raise ResearchDownloadError("Download crossed the Shanghai date boundary; explicit refresh is required.")
