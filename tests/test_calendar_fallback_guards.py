"""Runtime calendars may never silently degrade to Monday-Friday guesses."""
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from stopat30m.data.fetcher import akshare as ak_module, baostock as bs_module
from stopat30m.data.fetcher.efinance import EfinanceFetcher
from stopat30m.data.research_calendar import Calendar
from stopat30m.data import sources
from stopat30m.trading import rules


def fail_calendar(*args):
    raise RuntimeError('offline provider')


@pytest.mark.parametrize('first', ['error', 'empty'])
def test_efinance_uses_an_actual_alternative_not_weekdays(monkeypatch, first):
    primary = fail_calendar if first == 'error' else lambda *args: []
    monkeypatch.setattr(ak_module, 'AKShareFetcher', lambda **kwargs: SimpleNamespace(fetch_trade_calendar=primary))
    monkeypatch.setattr(bs_module, 'BaoStockFetcher', lambda **kwargs: SimpleNamespace(
        fetch_trade_calendar=lambda *args: ['2026-10-08', '2026-10-09']))
    monkeypatch.setattr(pd, 'bdate_range', lambda *a, **k: pytest.fail('Must not synthesize an exchange calendar'))
    instance = object.__new__(EfinanceFetcher)
    assert instance.fetch_trade_calendar('2026-10-01', '2026-10-09') == ['2026-10-08', '2026-10-09']


def test_efinance_refuses_download_when_real_calendar_sources_fail(monkeypatch):
    for module, name in [(ak_module, 'AKShareFetcher'), (bs_module, 'BaoStockFetcher')]:
        monkeypatch.setattr(module, name, lambda **kwargs: SimpleNamespace(fetch_trade_calendar=fail_calendar))
    monkeypatch.setattr(pd, 'bdate_range', lambda *a, **k: pytest.fail('Must not synthesize an exchange calendar'))
    with pytest.raises(RuntimeError, match='refusing weekday approximation'):
        object.__new__(EfinanceFetcher).fetch_trade_calendar('2026-10-01', '2026-10-09')


def test_market_session_helpers_require_source_backed_calendar():
    local = ZoneInfo('Asia/Shanghai')
    holiday = datetime(2026, 10, 5, 10, 0, tzinfo=local)
    with pytest.raises(ValueError, match='verified exchange calendar'): rules.is_trading_day(holiday)
    with pytest.raises(ValueError, match='verified exchange calendar'): rules.is_trading_time(holiday)
    calendar = Calendar.from_rows([
        {'date': '2026-10-05', 'is_open': False}, {'date': '2026-10-06', 'is_open': True}],
        '2026-10-05', '2026-10-06', source='explicit_test_flags', source_url='https://example.test/calendar')
    assert not rules.is_trading_day(holiday, calendar=calendar)
    assert not rules.is_trading_time(holiday, calendar=calendar)
    assert rules.is_trading_time(datetime(2026, 10, 6, 10, 0, tzinfo=local), calendar=calendar)
    assert not rules.is_trading_time(datetime(2026, 10, 6, 12, 0, tzinfo=local), calendar=calendar)


def test_recent_bar_selection_does_not_invent_a_last_exchange_session(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 5, 12, 0, tzinfo=ZoneInfo('Asia/Shanghai'))
    frame = pd.DataFrame({'date': ['2026-09-30'], 'open': [10], 'close': [10],
                          'high': [10], 'low': [10], 'volume': [100]})
    monkeypatch.setattr(sources, 'datetime', Clock)
    monkeypatch.setattr(sources, 'resolve_daily_sources', lambda: ['offline_fixture'])
    monkeypatch.setattr(sources, '_try_load_fetcher', lambda key: SimpleNamespace(fetch_daily=lambda *a: frame))
    monkeypatch.setattr(sources, '_fetch_ohlcv_from_qlib', lambda *a: None)
    result, source = sources.fetch_daily_ohlcv('600000')
    assert source == 'offline_fixture'
    assert result.attrs['as_of'] == '2026-09-30'
    assert result.attrs['latest_session_verified'] is False
    assert result.attrs['warnings']
