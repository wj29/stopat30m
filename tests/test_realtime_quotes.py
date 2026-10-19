"""All providers are mocked; no HTTP or local Qlib calls occur."""
import pytest
from stopat30m.data import realtime


@pytest.fixture(autouse=True)
def clean_quotes():
    realtime.invalidate_cache()
    yield
    realtime.invalidate_cache()


def test_refreshing_other_security_does_not_renew_old_quote(monkeypatch):
    clock = [1000.0]
    calls = []
    monkeypatch.setattr(realtime.time, "time", lambda: clock[0])
    def fetch(codes):
        calls.append(codes)
        return {code: (10.0, "test") for code in codes}
    monkeypatch.setattr(realtime, "_build_source_chain", lambda needed: [("test", fetch)])
    realtime.fetch_spot_quotes(["600000"], ttl=30)
    clock[0] += 25
    realtime.fetch_spot_quotes(["000001"], ttl=30)
    clock[0] += 10
    result = realtime.fetch_spot_quotes(["600000"], ttl=30)
    assert len(calls) == 3
    assert result["SH600000"]["status"] == "available"
    assert result["SH600000"]["as_of"] is None
    assert result["SH600000"]["is_realtime"] is False


def test_online_failure_does_not_expose_adjusted_qlib_close_as_quote(monkeypatch):
    monkeypatch.setattr(realtime, "_build_source_chain", lambda needed: [])
    monkeypatch.setattr(realtime, "fetch_prices_from_qlib", lambda *a: pytest.fail("Qlib is not an exchange quote"))
    result = realtime.fetch_spot_quotes(["600000"])
    assert result["SH600000"]["price"] is None
    assert result["SH600000"]["status"] == "unavailable"
    assert realtime.fetch_spot_prices(["600000"]) == {}


def test_stale_provider_timestamp_stays_stale_even_when_fetched_now(monkeypatch):
    monkeypatch.setattr(realtime, "_build_source_chain", lambda needed: [("Tencent", lambda codes: {"600000": (10, "test", "20200102150000")})])
    quote = realtime.fetch_spot_quotes(["600000"])["SH600000"]
    assert quote["status"] == "stale"
    assert quote["is_realtime"] is False
    assert quote["as_of"].startswith("2020-01-02")
    assert realtime.fetch_spot_quotes(["600000"])["SH600000"]["status"] == "stale"


def test_nonfinite_price_is_unavailable(monkeypatch):
    monkeypatch.setattr(realtime, "_build_source_chain", lambda needed: [("test", lambda codes: {"600000": (float("inf"), "")})])
    assert realtime.fetch_spot_prices(["600000"]) == {}


def test_quote_can_age_to_stale_inside_long_cache_ttl(monkeypatch):
    from datetime import datetime, timezone
    clock = [1000.0]
    monkeypatch.setattr(realtime.time, "time", lambda: clock[0])
    timestamp = datetime.fromtimestamp(900, timezone.utc).isoformat()
    monkeypatch.setattr(realtime, "_build_source_chain", lambda needed: [("Tencent", lambda codes: {"600000": (10, "test", timestamp)})])
    assert realtime.fetch_spot_quotes(["600000"], ttl=100)["SH600000"]["is_realtime"] is True
    clock[0] = 1030
    quote = realtime.fetch_spot_quotes(["600000"], ttl=100)["SH600000"]
    assert quote["status"] == "stale"
    assert quote["is_realtime"] is False


@pytest.mark.parametrize("quote_time, tradable", [(None, False), ("2020-01-02T15:00:00+08:00", False), ("fresh", True)])
def test_execution_mapping_excludes_reference_quotes_but_rich_api_preserves_them(monkeypatch, quote_time, tradable):
    from datetime import datetime, timezone
    timestamp = datetime.now(timezone.utc).isoformat() if quote_time == "fresh" else quote_time
    monkeypatch.setattr(realtime, "_build_source_chain", lambda needed: [("Tencent", lambda codes: {"600000": (10, "test", timestamp)})])
    rich = realtime.fetch_spot_quotes(["600000"])
    assert rich["SH600000"]["price"] == 10
    assert realtime.fetch_spot_prices(["600000"]) == ({"SH600000": 10} if tradable else {})
