"""Source-backed CN market calendar with bounded coverage and offline caches.

A missing date is an error, never a weekday-based guess. Calendar rows describe
all natural days, including exchange holidays. The session schedule is for the
modern Shanghai/Shenzhen cash-equity auction, not futures or after-hours trading.
"""
from __future__ import annotations

import hashlib
import json
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo('Asia/Shanghai')
BAOSTOCK_CALENDAR_URL = 'https://www.baostock.com/api/'
SESSION_RULES_URL = 'https://www.sse.com.cn/lawandrules/sselawsrules2025/fund/trading/c/c_20260424_10817739.shtml'


class CalendarError(ValueError):
    pass


def iso_day(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise CalendarError('Timezone-aware datetime is required.')
        return value.astimezone(SHANGHAI).date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        parsed = date.fromisoformat(str(value))
    except ValueError:
        raise CalendarError(f'Invalid ISO calendar date: {value!r}') from None
    if str(value) != parsed.isoformat():
        raise CalendarError('Calendar dates must use YYYY-MM-DD.')
    return parsed.isoformat()


def natural_days(start: Any, end: Any) -> list[str]:
    first, last = date.fromisoformat(iso_day(start)), date.fromisoformat(iso_day(end))
    if last < first:
        raise CalendarError('Calendar end precedes start.')
    if (last - first).days > 36525:
        raise CalendarError('Calendar requests are limited to 100 years.')
    return [(first + timedelta(days=offset)).isoformat() for offset in range((last - first).days + 1)]


def _digest(payload: dict) -> str:
    data = {key: value for key, value in payload.items() if key != 'sha256'}
    return hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Calendar:
    coverage_start: str
    coverage_end: str
    days: tuple[tuple[str, bool], ...]
    source: str
    source_url: str
    fetched_at: str
    market: str = 'CN_A'

    @classmethod
    def from_rows(cls, rows: Iterable[dict], start: str, end: str, *, source: str,
                  source_url: str, fetched_at: str | None = None, market: str = 'CN_A') -> 'Calendar':
        expected = natural_days(start, end)
        if not source or not source_url:
            raise CalendarError('Calendar provenance is required.')
        if market != 'CN_A':
            raise CalendarError('Only the CN_A session schedule is supported.')
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        flags: dict[str, bool] = {}
        for row in rows:
            day = iso_day(row.get('date', row.get('calendar_date')))
            raw = row.get('is_trading_day', row.get('is_open'))
            if raw not in (0, 1, '0', '1', False, True):
                raise CalendarError(f'Invalid exchange day flag for {day}: {raw!r}')
            if day in flags:
                raise CalendarError(f'Duplicate exchange calendar date: {day}')
            flags[day] = raw in (1, '1', True)
        missing = sorted(set(expected) - flags.keys())
        extra = sorted(flags.keys() - set(expected))
        if missing or extra:
            raise CalendarError(f'Incomplete calendar coverage; missing={missing[:5]}, extra={extra[:5]}.')
        stamp = fetched_at or _utc_now()
        try:
            if datetime.fromisoformat(stamp).tzinfo is None:
                raise ValueError('naive retrieval timestamp')
        except (ValueError, TypeError):
            raise CalendarError('Calendar fetched_at must have a timezone.') from None
        return cls(expected[0], expected[-1], tuple((day, flags[day]) for day in expected),
                   source, source_url, stamp, market)

    @classmethod
    def from_sessions(cls, sessions: Iterable[Any], start: str, end: str, *, source: str,
                      source_url: str, fetched_at: str | None = None) -> 'Calendar':
        """Adapt a provider-certified complete session list, not observed bars.

        Never call this with a stock's trading dates: a suspension would then
        incorrectly become a market holiday. Use full provider daily flags when
        they are available.
        """
        raw = [iso_day(day) for day in sessions]
        if len(raw) != len(set(raw)):
            raise CalendarError('Duplicate provider sessions.')
        expected = natural_days(start, end)
        if set(raw) - set(expected):
            raise CalendarError('Provider sessions lie outside requested coverage.')
        selected = set(raw)
        return cls.from_rows([{'date': day, 'is_open': day in selected} for day in expected],
                             start, end, source=source, source_url=source_url, fetched_at=fetched_at)

    def _require_coverage(self, start: Any, end: Any | None = None) -> tuple[str, str]:
        first, last = iso_day(start), iso_day(end if end is not None else start)
        if first > last or first < self.coverage_start or last > self.coverage_end:
            raise CalendarError(f'Calendar coverage is {self.coverage_start}..{self.coverage_end}; requested {first}..{last}.')
        return first, last

    def is_session(self, day: Any) -> bool:
        selected, _ = self._require_coverage(day)
        return dict(self.days)[selected]

    def sessions(self, start: Any, end: Any) -> list[str]:
        first, last = self._require_coverage(start, end)
        return [day for day, flag in self.days if flag and first <= day <= last]

    def next_session(self, day: Any, *, include_current: bool = False) -> str:
        selected, _ = self._require_coverage(day)
        sessions = [day for day, flag in self.days if flag]
        index = bisect_left(sessions, selected) if include_current else bisect_right(sessions, selected)
        if index >= len(sessions):
            raise CalendarError('Next session is outside verified calendar coverage.')
        return sessions[index]

    def previous_session(self, day: Any, *, include_current: bool = False) -> str:
        selected, _ = self._require_coverage(day)
        sessions = [day for day, flag in self.days if flag]
        index = (bisect_right(sessions, selected) if include_current else bisect_left(sessions, selected)) - 1
        if index < 0:
            raise CalendarError('Previous session is outside verified calendar coverage.')
        return sessions[index]

    def session_status(self, when: datetime) -> dict:
        if not isinstance(when, datetime) or when.tzinfo is None:
            raise CalendarError('Session checks require a timezone-aware datetime.')
        local = when.astimezone(SHANGHAI)
        day = local.date().isoformat()
        opened = self.is_session(day)
        if day < '2018-08-20':
            raise CalendarError('Intraday schedule before 2018-08-20 is not certified; daily exchange flags remain usable.')
        phase = 'closed'
        clock = local.time()
        if opened:
            if time(9, 15) <= clock < time(9, 25):
                phase = 'opening_auction'
            elif time(9, 30) <= clock <= time(11, 30) or time(13) <= clock < time(14, 57):
                phase = 'continuous'
            elif time(14, 57) <= clock <= time(15):
                phase = 'closing_auction'
        return {'date': day, 'timezone': 'Asia/Shanghai', 'is_session': opened,
                'phase': phase, 'source': self.source, 'source_url': self.source_url,
                'schedule_scope': 'modern_cn_cash_equity_auction'}

    def require_session(self, when: datetime, *, phase: str = 'any') -> None:
        allowed = {'any': {'opening_auction', 'continuous', 'closing_auction'},
                   'continuous': {'continuous'}, 'auction': {'opening_auction', 'closing_auction'}}
        if phase not in allowed:
            raise CalendarError('phase must be any, continuous, or auction.')
        status = self.session_status(when)
        if status['phase'] not in allowed[phase]:
            raise CalendarError(f'Exchange is outside the requested {phase} session: {status["date"]}, {status["phase"]}.')

    def to_dict(self) -> dict:
        payload = {'schema_version': 1, 'market': self.market, 'timezone': 'Asia/Shanghai',
                   'coverage_start': self.coverage_start, 'coverage_end': self.coverage_end,
                   'source': self.source, 'source_url': self.source_url, 'fetched_at': self.fetched_at,
                   'days': [{'date': day, 'is_open': flag} for day, flag in self.days]}
        return {**payload, 'sha256': _digest(payload)}

    def audit(self) -> dict:
        payload = self.to_dict()
        return {key: value for key, value in payload.items() if key != 'days'} | {
            'status': 'verified_coverage', 'natural_days': len(self.days),
            'sessions': sum(flag for _, flag in self.days), 'weekday_fallback': False,
            'session_rules_url': SESSION_RULES_URL,
            'session_schedule_scope': 'CN cash equities from 2018-08-20; not funds, futures, or after-hours sessions',
        }

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + '.tmp')
        temp.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding='utf-8')
        temp.replace(path)
        return path

    @classmethod
    def load(cls, path: str | Path) -> 'Calendar':
        payload = json.loads(Path(path).read_text(encoding='utf-8'))
        if (payload.get('schema_version') != 1 or payload.get('timezone') != 'Asia/Shanghai'
                or payload.get('sha256') != _digest(payload)):
            raise CalendarError('Calendar schema/checksum validation failed.')
        return cls.from_rows(payload['days'], payload['coverage_start'], payload['coverage_end'],
                             source=payload['source'], source_url=payload['source_url'],
                             fetched_at=payload['fetched_at'], market=payload['market'])


def _baostock_calendar(start: str, end: str) -> list[dict]:
    import baostock as bs
    login = bs.login()
    if login.error_code != '0':
        raise CalendarError(f'BaoStock calendar login failed: {login.error_code}')
    try:
        response = bs.query_trade_dates(start_date=start, end_date=end)
        rows = []
        while response.error_code == '0' and response.next():
            rows.append(dict(zip(response.fields, response.get_row_data())))
        if response.error_code != '0':
            raise CalendarError(f'BaoStock calendar request failed: {response.error_code}')
        return rows
    finally:
        bs.logout()


def download_calendar(start: str, end: str, cache_path: str | Path, *, provider=None,
                      refresh: bool = False, offline: bool = False) -> Calendar:
    """Download actual daily flags once or load a fully verified offline cache."""
    start, end = iso_day(start), iso_day(end)
    path = Path(cache_path)
    if path.exists() and not refresh:
        cached = Calendar.load(path)
        cached._require_coverage(start, end)
        return cached
    if offline:
        raise CalendarError('Verified calendar cache is unavailable; offline mode cannot guess sessions.')
    rows = provider.fetch_calendar(start, end) if provider is not None else _baostock_calendar(start, end)
    calendar = Calendar.from_rows(rows, start, end, source='baostock.query_trade_dates',
                                  source_url=BAOSTOCK_CALENDAR_URL)
    calendar.save(path)
    return calendar


ResearchCalendar = Calendar
