"""Calendar tests use explicit exchange flags, including a weekday holiday."""
import json
import sys
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from stopat30m.data.research_calendar import Calendar, CalendarError, download_calendar


def calendar_rows():
    return [{'calendar_date': day, 'is_trading_day': flag} for day, flag in [
        ('2026-10-01', '0'), ('2026-10-02', '0'), ('2026-10-03', '0'),
        ('2026-10-04', '0'), ('2026-10-05', '0'), ('2026-10-06', '0'),
        ('2026-10-07', '0'), ('2026-10-08', '1'), ('2026-10-09', '1')]]


def make_calendar(rows=None):
    return Calendar.from_rows(calendar_rows() if rows is None else rows,
                              '2026-10-01', '2026-10-09', source='explicit_test_calendar',
                              source_url='https://example.test/exchange-calendar')


def test_exchange_holidays_are_not_weekday_guesses():
    calendar = make_calendar()
    assert calendar.is_session('2026-10-05') is False  # Monday holiday.
    assert calendar.sessions('2026-10-01', '2026-10-09') == ['2026-10-08', '2026-10-09']
    assert calendar.next_session('2026-10-02') == '2026-10-08'
    assert calendar.previous_session('2026-10-09') == '2026-10-08'
    with pytest.raises(CalendarError, match='coverage'):
        calendar.is_session('2026-10-12')
    with pytest.raises(CalendarError, match='outside'):
        calendar.next_session('2026-10-09')


@pytest.mark.parametrize('mutation', ['missing', 'duplicate', 'badflag', 'extra'])
def test_incomplete_or_invalid_calendar_cannot_become_cache(mutation):
    rows = calendar_rows()
    if mutation == 'missing': rows.pop(2)
    if mutation == 'duplicate': rows.append(rows[0])
    if mutation == 'badflag': rows[0]['is_trading_day'] = 'unknown'
    if mutation == 'extra': rows.append({'calendar_date': '2026-10-10', 'is_trading_day': '0'})
    with pytest.raises(CalendarError): make_calendar(rows)


def test_offline_cache_verifies_integrity_and_range(tmp_path):
    path = tmp_path / 'calendar.json'
    with pytest.raises(CalendarError, match='offline'):
        download_calendar('2026-10-01', '2026-10-09', path, offline=True)
    calls = []
    provider = SimpleNamespace(fetch_calendar=lambda start, end: calls.append((start, end)) or calendar_rows())
    result = download_calendar('2026-10-01', '2026-10-09', path, provider=provider)
    assert calls == [('2026-10-01', '2026-10-09')]
    assert download_calendar('2026-10-02', '2026-10-08', path, offline=True).audit()['sha256'] == result.audit()['sha256']
    payload = json.loads(path.read_text())
    payload['days'][0]['is_open'] = True
    path.write_text(json.dumps(payload))
    with pytest.raises(CalendarError, match='checksum'):
        Calendar.load(path)


def test_session_hours_timezone_and_lunch_are_explicit():
    calendar = make_calendar()
    zone = ZoneInfo('Asia/Shanghai')
    calendar.require_session(datetime(2026, 10, 8, 10, 0, tzinfo=zone), phase='continuous')
    calendar.require_session(datetime(2026, 10, 8, 14, 58, tzinfo=zone), phase='auction')
    assert calendar.session_status(datetime.fromisoformat('2026-10-08T02:00:00+00:00'))['phase'] == 'continuous'
    for moment in [datetime(2026, 10, 8, 12, 0, tzinfo=zone),
                   datetime(2026, 10, 5, 10, 0, tzinfo=zone),
                   datetime(2026, 10, 8, 15, 1, tzinfo=zone)]:
        with pytest.raises(CalendarError): calendar.require_session(moment)
    with pytest.raises(CalendarError, match='timezone-aware'):
        calendar.require_session(datetime(2026, 10, 8, 10, 0))


@pytest.mark.parametrize('fails_on_later_page', [False, True])
def test_baostock_iterator_pages_never_use_removed_dataframe_append(monkeypatch, tmp_path, fails_on_later_page):
    class PagedResult:
        fields = ['calendar_date', 'is_trading_day']
        error_code = '0'
        def __init__(self):
            self.pages = iter([[['2026-10-08', '1']], [['2026-10-09', '1']]])
            self.page = iter([])
            self.page_count = 0
        def get_data(self):
            raise AssertionError('get_data uses removed pandas.append on old SDKs')
        def next(self):
            self.row = next(self.page, None)
            if self.row is None:
                page = next(self.pages, None)
                if page is None: return False
                self.page_count += 1
                if fails_on_later_page and self.page_count == 2:
                    self.error_code = 'network_error'
                    return False
                self.page = iter(page)
                self.row = next(self.page)
            return True
        def get_row_data(self): return self.row
    result = PagedResult()
    client = SimpleNamespace(login=lambda: SimpleNamespace(error_code='0'), logout=lambda: None,
                             query_trade_dates=lambda **kwargs: result)
    monkeypatch.setitem(sys.modules, 'baostock', client)
    path = tmp_path / 'calendar.json'
    if fails_on_later_page:
        with pytest.raises(CalendarError, match='request failed'):
            download_calendar('2026-10-08', '2026-10-09', path)
        assert not path.exists()
    else:
        calendar = download_calendar('2026-10-08', '2026-10-09', path)
        assert result.page_count == 2
        assert calendar.sessions('2026-10-08', '2026-10-09') == ['2026-10-08', '2026-10-09']
