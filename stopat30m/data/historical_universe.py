"""Auditable fixed research pools and source-backed membership intervals.

Today’s constituents are reference snapshots, never a historical index universe.
Only independently sourced effective intervals with verified coverage may pass a
point-in-time audit. A fixed ETF experiment is explicitly retrospective selection
when its pool was declared after the historical test began.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from .research_calendar import SHANGHAI, iso_day

BAOSTOCK_INDEX_URL = 'https://www.baostock.com/api/'
FIXED_LIMITATION = '固定研究池不是指数历史成分；池中证券由本次研究选定，不能据此声称消除了选池与幸存者偏差。'
SNAPSHOT_LIMITATION = '当前/指定日供应商成分快照不是完整有效区间；周度更新日期不等于指数调样生效日期，禁止向前或向后扩展为历史成员。'


class UniverseError(ValueError):
    pass


def normalize_member(value: str) -> str:
    raw = str(value).strip().upper()
    matched = re.fullmatch(r'(SH|SZ|BJ)\.?([0-9]{6})', raw)
    if matched:
        return matched.group(1) + matched.group(2)
    matched = re.fullmatch(r'([0-9]{6})\.(SH|SZ|BJ)', raw)
    if matched:
        return matched.group(2) + matched.group(1)
    if re.fullmatch(r'[0-9]{6}', raw):
        exchange = 'SH' if raw[0] in '569' else ('BJ' if raw[0] in '48' else 'SZ')
        return exchange + raw
    raise UniverseError(f'Invalid universe member code: {value!r}')


def _fingerprint(payload: dict) -> str:
    content = {key: value for key, value in payload.items() if key != 'sha256'}
    return hashlib.sha256(json.dumps(content, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def _known_time(value: str) -> datetime:
    """Date-only publications are conservatively available after that day.

    An unknown publication time is not assumed to precede the 15:00 signal.
    Effective dates and announcement availability remain separate facts.
    """
    if len(value) == 10:
        return datetime.combine(date.fromisoformat(iso_day(value)), time.max, SHANGHAI)
    try:
        parsed = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        raise UniverseError('known_at must be an ISO date or timezone-aware datetime.') from None
    if parsed.tzinfo is None:
        raise UniverseError('known_at datetime must include its source timezone.')
    return parsed.astimezone(SHANGHAI)


def _signal_cutoff(value: str | date | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise UniverseError('known_on datetime requires a timezone.')
        return value.astimezone(SHANGHAI)
    raw = str(value)
    if 'T' in raw or ' ' in raw:
        return _signal_cutoff(datetime.fromisoformat(raw))
    return datetime.combine(date.fromisoformat(iso_day(value)), time(15), SHANGHAI)


@dataclass(frozen=True)
class MembershipInterval:
    instrument: str
    effective_from: str
    effective_to: str | None
    known_at: str
    source: str
    source_url: str
    exit_known_at: str | None = None
    exit_source_url: str | None = None

    @classmethod
    def from_dict(cls, raw: dict) -> 'MembershipInterval':
        start = iso_day(raw['effective_from'])
        end = iso_day(raw['effective_to']) if raw.get('effective_to') else None
        if end is not None and end <= start:
            raise UniverseError('Membership effective_to is exclusive and must follow effective_from.')
        known = str(raw['known_at'])
        _known_time(known)
        if not raw.get('source') or not raw.get('source_url'):
            raise UniverseError('Each membership interval requires source and source_url evidence.')
        exit_known = str(raw['exit_known_at']) if raw.get('exit_known_at') is not None else None
        if exit_known is not None:
            _known_time(exit_known)
            if end is None:
                raise UniverseError('Exit announcement evidence requires an effective_to date.')
        return cls(normalize_member(raw['instrument']), start, end, known,
                   str(raw['source']), str(raw['source_url']), exit_known,
                   str(raw['exit_source_url']) if raw.get('exit_source_url') else None)

    def as_dict(self) -> dict:
        payload = asdict(self)
        # Preserve existing fixed/snapshot protocol payloads exactly. A missing
        # exit field is never synthesized from an effective end or entry notice.
        return {key: value for key, value in payload.items()
                if key not in {'exit_known_at', 'exit_source_url'} or value is not None}


@dataclass(frozen=True)
class HistoricalUniverse:
    name: str
    kind: str
    coverage_start: str
    coverage_end: str
    intervals: tuple[MembershipInterval, ...]
    selection_date: str | None = None
    expected_size: int | None = None
    coverage_evidence: tuple[dict, ...] = ()
    limitations: tuple[str, ...] = ()
    metadata: dict = field(default_factory=dict)

    @classmethod
    def fixed_pool(cls, name: str, instruments: list[str], coverage_start: str, coverage_end: str,
                   *, selection_date: str | None = None, source: str = 'user_declared_fixed_pool',
                   source_url: str = 'user:declared-fixed-pool') -> 'HistoricalUniverse':
        """Declare a fixed experiment pool, not historical index membership.

        selection_date defaults to today and is recorded honestly. Listing/bar
        availability must still be checked against the separate price artifact.
        """
        selected = iso_day(selection_date or datetime.now(SHANGHAI).date())
        members = [normalize_member(code) for code in instruments]
        if len(members) != len(set(members)):
            raise UniverseError('Fixed universe members must be unique.')
        end_exclusive = (date.fromisoformat(iso_day(coverage_end)) + timedelta(days=1)).isoformat()
        payload = {
            'name': name, 'kind': 'fixed', 'coverage_start': coverage_start, 'coverage_end': coverage_end,
            'selection_date': selected, 'expected_size': len(members),
            'limitations': [FIXED_LIMITATION],
            'intervals': [{'instrument': member, 'effective_from': coverage_start,
                           'effective_to': end_exclusive, 'known_at': selected,
                           'source': source, 'source_url': source_url} for member in sorted(members)],
        }
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: dict) -> 'HistoricalUniverse':
        start, end = iso_day(payload['coverage_start']), iso_day(payload['coverage_end'])
        if start > end:
            raise UniverseError('Universe coverage end precedes start.')
        kind = payload.get('kind')
        if kind not in {'fixed', 'index_pit', 'snapshot'}:
            raise UniverseError('Universe kind must be fixed, index_pit, or snapshot.')
        if not payload.get('name'):
            raise UniverseError('Universe name is required.')
        intervals = tuple(sorted((MembershipInterval.from_dict(item) for item in payload.get('intervals', [])),
                                 key=lambda item: (item.instrument, item.effective_from)))
        if not intervals:
            raise UniverseError('Universe cannot be empty.')
        previous: dict[str, MembershipInterval] = {}
        for interval in intervals:
            older = previous.get(interval.instrument)
            if older and (older.effective_to is None or interval.effective_from < older.effective_to):
                raise UniverseError(f'Overlapping membership intervals: {interval.instrument}')
            previous[interval.instrument] = interval
        selected = iso_day(payload['selection_date']) if payload.get('selection_date') else None
        if kind == 'fixed' and selected is None:
            raise UniverseError('A fixed pool requires its actual selection_date.')
        if kind == 'snapshot' and start != end:
            raise UniverseError('A snapshot can certify only its requested observation date.')
        expected = payload.get('expected_size')
        if expected is not None and (isinstance(expected, bool) or not isinstance(expected, int) or expected <= 0):
            raise UniverseError('expected_size must be a positive integer.')
        limits = list(payload.get('limitations', []))
        mandatory = FIXED_LIMITATION if kind == 'fixed' else (SNAPSHOT_LIMITATION if kind == 'snapshot' else None)
        if mandatory and mandatory not in limits:
            limits.append(mandatory)
        return cls(str(payload['name']), kind, start, end, intervals, selected, expected,
                   tuple(payload.get('coverage_evidence', [])), tuple(limits), dict(payload.get('metadata', {})))

    def _coverage(self, start: Any, end: Any | None = None) -> tuple[str, str]:
        first, last = iso_day(start), iso_day(end if end is not None else start)
        if first > last or first < self.coverage_start or last > self.coverage_end:
            raise UniverseError(f'Universe has verified coverage {self.coverage_start}..{self.coverage_end}; requested {first}..{last}.')
        return first, last

    def members_on(self, day: Any, known_on: Any | None = None) -> list[str]:
        selected, _ = self._coverage(day)
        cutoff = _signal_cutoff(known_on if known_on is not None else day)
        members = set()
        for interval in self.intervals:
            if interval.effective_from > selected:
                continue
            if self.kind == 'index_pit' and _known_time(interval.known_at) > cutoff:
                continue
            if interval.effective_to is not None and selected >= interval.effective_to:
                if self.kind == 'index_pit':
                    if interval.exit_known_at is None or not interval.exit_source_url:
                        raise UniverseError(f'Exit announcement evidence is missing for {interval.instrument}.')
                    if _known_time(interval.exit_known_at) > cutoff:
                        raise UniverseError(
                            f'Exit of {interval.instrument} is not known at the requested signal cutoff; '
                            'refusing to remove a member using future exit information.'
                        )
                continue
            members.add(interval.instrument)
        return sorted(members)

    def audit(self, start: str, end: str, require_pit: bool = False) -> dict:
        first, last = self._coverage(start, end)
        limitations = list(self.limitations)
        problems = []
        if self.kind == 'index_pit':
            if any(interval.effective_to is not None
                   and (interval.exit_known_at is None or not interval.exit_source_url)
                   for interval in self.intervals):
                problems.append('有限成员区间缺少独立的退池公告可知时间或来源证据。')
            if any(interval.source.startswith(('baostock.query_hs300_stocks', 'baostock.query_zz500_stocks',
                                               'baostock.query_sz50_stocks', 'legacy_current'))
                   or interval.source_url.startswith('legacy:') for interval in self.intervals):
                problems.append('周度/当前成分快照来源不能证明精确历史生效区间。')
            if self.expected_size is None:
                problems.append('缺少指数成员数量校验口径。')
            evidence_ranges = []
            for record in self.coverage_evidence:
                if not record.get('source_url') or record.get('method') not in {'official_change_log', 'verified_daily_membership'}:
                    continue
                left, right = iso_day(record['start']), iso_day(record['end'])
                evidence_ranges.append((left, right))
            cursor = first
            for left, right in sorted(evidence_ranges):
                if left <= cursor <= right:
                    cursor = (date.fromisoformat(right) + timedelta(days=1)).isoformat()
            if cursor <= last:
                problems.append('缺少完整历史基线/调样日志或逐日成员覆盖证据。')
            boundaries = {first, last}
            for interval in self.intervals:
                boundaries.update(day for day in (interval.effective_from, interval.effective_to) if day and first <= day <= last)
            for point in sorted(boundaries):
                try:
                    count = len(self.members_on(point, known_on=point))
                except UniverseError as exc:
                    problems.append(str(exc))
                    continue
                if not count or (self.expected_size is not None and count != self.expected_size):
                    problems.append(f'{point} 可知成员数 {count} 不符合 {self.expected_size or "非空"}。')
        retrospective = bool(self.kind == 'fixed' and self.selection_date and self.selection_date > first)
        if retrospective:
            limitations.append(f'研究池于 {self.selection_date} 选定，晚于历史研究起点；这是回看固定池研究，不是当时预先选池的验证。')
        pit = self.kind == 'index_pit' and not problems
        if require_pit and not pit:
            raise UniverseError('Point-in-time universe required: ' + '; '.join(problems + limitations or ['kind=' + self.kind]))
        return {'name': self.name, 'kind': self.kind, 'coverage_start': first, 'coverage_end': last,
                'selection_date': self.selection_date, 'retrospective_selection': retrospective,
                'point_in_time': pit, 'survivorship_bias_controlled': pit,
                'status': 'verified' if pit else ('limited' if self.kind == 'fixed' else 'blocked'),
                'limitations': limitations + problems, 'sha256': self.content_hash}

    def as_dict(self) -> dict:
        payload = {'schema_version': 1, 'name': self.name, 'kind': self.kind,
                   'coverage_start': self.coverage_start, 'coverage_end': self.coverage_end,
                   'selection_date': self.selection_date, 'expected_size': self.expected_size,
                   'intervals': [interval.as_dict() for interval in self.intervals],
                   'coverage_evidence': list(self.coverage_evidence),
                   'limitations': list(self.limitations), 'metadata': self.metadata}
        return {**payload, 'sha256': _fingerprint(payload)}

    @property
    def content_hash(self) -> str:
        return self.as_dict()['sha256']

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + '.tmp')
        temporary.write_text(json.dumps(self.as_dict(), ensure_ascii=False, indent=2), encoding='utf-8')
        temporary.replace(path)
        return path

    @classmethod
    def load(cls, path: str | Path) -> 'HistoricalUniverse':
        payload = json.loads(Path(path).read_text(encoding='utf-8'))
        if payload.get('schema_version') != 1 or payload.get('sha256') != _fingerprint(payload):
            raise UniverseError('Universe schema/checksum validation failed.')
        return cls.from_dict(payload)

    def write_qlib(self, path: str | Path, start: str, end: str) -> Path:
        """Only an audited index PIT universe can write a historical Qlib index."""
        self.audit(start, end, require_pit=True)
        rows = []
        for interval in self.intervals:
            left = max(start, interval.effective_from)
            right = min(end, ((date.fromisoformat(interval.effective_to) - timedelta(days=1)).isoformat()
                              if interval.effective_to else self.coverage_end))
            if left <= right:
                rows.append(f'{interval.instrument}\t{left}\t{right}')
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('\n'.join(rows) + '\n', encoding='utf-8')
        return path


def download_index_snapshot(index: str, as_of: str, cache_path: str | Path, *, client=None,
                            refresh: bool = False, offline: bool = False) -> HistoricalUniverse:
    """Keep free provider results as dated snapshots, never inferred intervals.

    BaoStock's updateDate is audited separately from the requested date. A
    response newer than the historical request is rejected as a current-data leak.
    Even older updateDate values do not establish exact effective membership.
    """
    day, path = iso_day(as_of), Path(cache_path)
    if path.exists() and not refresh:
        cached = HistoricalUniverse.load(path)
        if cached.kind != 'snapshot' or cached.name != index or cached.coverage_start != day:
            raise UniverseError('Snapshot cache identity does not match request.')
        return cached
    if offline:
        raise UniverseError('Verified snapshot cache is unavailable in offline mode.')
    methods = {'csi300': 'query_hs300_stocks', 'csi500': 'query_zz500_stocks', 'sz50': 'query_sz50_stocks'}
    if index not in methods:
        raise UniverseError('Supported free snapshots: csi300, csi500, sz50.')
    if client is None:
        import baostock as client
    login = client.login()
    if login.error_code != '0':
        raise UniverseError(f'BaoStock snapshot login failed: {login.error_code}')
    try:
        response = getattr(client, methods[index])(date=day)
        rows = []
        while response.error_code == '0' and response.next():
            rows.append(dict(zip(response.fields, response.get_row_data())))
        if response.error_code != '0':
            raise UniverseError(f'BaoStock snapshot request failed: {response.error_code}')
    finally:
        client.logout()
    if not rows:
        raise UniverseError('Historical snapshot returned no members; no fallback to today is allowed.')
    updates = {iso_day(row['updateDate']) for row in rows}
    if len(updates) != 1 or max(updates) > day:
        raise UniverseError(f'Provider snapshot updateDate {sorted(updates)} does not establish requested history {day}.')
    members = [normalize_member(row['code']) for row in rows]
    if len(members) != len(set(members)):
        raise UniverseError('Provider snapshot contains duplicate members.')
    stamp = datetime.now(timezone.utc).isoformat()
    following = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    universe = HistoricalUniverse.from_dict({
        'name': index, 'kind': 'snapshot', 'coverage_start': day, 'coverage_end': day,
        'expected_size': len(members),
        'metadata': {'requested_date': day, 'provider_update_date': min(updates), 'fetched_at': stamp,
                     'raw_rows_sha256': _fingerprint({'rows': rows}), 'weekly_snapshot_only': True},
        'intervals': [{'instrument': member, 'effective_from': day, 'effective_to': following,
                       'known_at': stamp, 'source': 'baostock.' + methods[index],
                       'source_url': BAOSTOCK_INDEX_URL} for member in sorted(members)],
    })
    universe.save(path)
    return universe
