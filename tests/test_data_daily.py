import pandas as pd
from stopat30m.data import sources


def frame(dates, closes):
    return pd.DataFrame({"date": dates, "open": closes, "high": closes, "low": closes, "close": closes, "volume": 100})


def test_daily_validator_drops_future_invalid_rows_and_sorts_deduplicates():
    raw = frame(["2026-01-06", "2026-01-05", "2026-01-05", "2099-01-01", "2026-01-04"], [10, 8, 9, 11, -1])
    result = sources._validate_daily_frame(raw, "2026-01-06")
    assert result["date"].tolist() == ["2026-01-05", "2026-01-06"]
    assert result["close"].tolist() == [9, 10]


def test_all_failed_sources_report_no_data_and_no_claimed_source(monkeypatch):
    monkeypatch.setattr(sources, "resolve_daily_sources", lambda: [])
    monkeypatch.setattr(sources, "_fetch_ohlcv_from_qlib", lambda *a: None)
    assert sources.fetch_daily_ohlcv("600000") == (None, "")


def test_older_first_source_does_not_hide_newer_alternative(monkeypatch):
    from types import SimpleNamespace
    today = pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y-%m-%d")
    providers = {"old": frame(["2020-01-01"], [10]), "new": frame([today], [11])}
    monkeypatch.setattr(sources, "resolve_daily_sources", lambda: ["old", "new"])
    monkeypatch.setattr(sources, "_try_load_fetcher", lambda key: SimpleNamespace(fetch_daily=lambda *a: providers[key]))
    result, source = sources.fetch_daily_ohlcv("600000")
    assert source == "new"
    assert result.attrs["source"] == "new"
    assert result.attrs["as_of"] == today
    assert result.attrs["latest_session_verified"] is False
