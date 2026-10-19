"""Offline source, unit, precision, overlap and polite-cache Tencent checks."""
import copy
import json

import pandas as pd
import pytest

from stopat30m.data.research_prices import ResearchDataError, TencentResearchProvider, _pair, _price_quality, download_research_prices


class Clock:
    def __init__(self):
        self.now = 1792396800.0
    def __call__(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds


class Calendar:
    source, source_url = "baostock", "https://www.baostock.com/"
    calendar_source = "baostock.query_trade_dates"
    def instrument_info(self, instrument):
        return {"asset_type": "index" if instrument == "SH000300" else "ETF", "ipo_date": "2011-01-01"}
    def fetch_calendar(self, start, end):
        assert start == end == "2024-01-02"
        return pd.DataFrame({"date": ["2024-01-02"], "is_trading_day": [1]})


def row(day, *, adjusted=False):
    prices = ["20.000", "22.000", "24.000", "18.000"] if adjusted else ["10.000", "11.000", "12.000", "9.000"]
    return [day, *prices, "1000.00", {}, "1.00", "105.00", "", ""]


def payload(rows, mode="", symbol="sh510300"):
    return {"code": 0, "data": {symbol: {f"{mode}day" if mode else "day": rows, "version": "offline-fixture"}}}


class Response:
    def __init__(self, data, status=200):
        self.status_code, self.headers = status, {}
        self.text = data if isinstance(data, str) else "kline_fixture=" + json.dumps(data)


class Session:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def provider(responses, tmp_path=None, **kwargs):
    clock = Clock()
    session = Session(responses)
    result = TencentResearchProvider(session=session, calendar_provider=Calendar(), cache_dir=tmp_path,
        clock=clock, sleep=clock.sleep, random=lambda: 0, **kwargs)
    return result, session, clock


def test_explicit_same_source_raw_hfq_units_precision_and_pairing():
    p, session, _ = provider([Response(payload([row("2024-01-02")])),
        Response(payload([row("2024-01-02", adjusted=True)], "hfq"))])
    raw = p.fetch_raw("SH510300", "2024-01-01", "2024-01-05")
    adjusted = p.fetch_adjusted("SH510300", "2024-01-01", "2024-01-05", "hfq")
    assert raw.iloc[0]["volume"] == 100000 and raw.iloc[0]["amount"] == 1050000
    paired = _pair(raw, adjusted, "SH510300", "2024-01-01", "2024-01-05")
    assert paired.iloc[0]["adjustment_factor"] == 2
    assert p.instrument_info("SH510300")["ohlc_resolution_by_mode"] == {"day": .001, "hfqday": .001}
    assert all(call[0].startswith("https://proxy.finance.qq.com/") for call in session.calls)
    assert all(call[1]["allow_redirects"] is False for call in session.calls)


def test_two_year_windows_require_identical_full_overlap_and_crop_only_after_merge():
    p, session, _ = provider([Response(payload([row("2012-12-31"), row("2013-01-02"), row("2014-01-02")])),
        Response(payload([row("2014-01-02"), row("2015-01-05"), row("2016-12-30")]))])
    frame = p.fetch_raw("SH510300", "2013-01-01", "2016-12-31")
    assert frame["date"].tolist() == ["2013-01-02", "2014-01-02", "2015-01-05", "2016-12-30"]
    assert session.calls[0][1]["params"]["param"] == "sh510300,day,2013-01-01,2014-12-31,640,"
    changed = row("2014-01-02")
    changed[7] = "2.00"  # Even non-OHLC overlap changes must not be silently overwritten.
    bad, _, _ = provider([Response(payload([row("2014-01-02")])), Response(payload([changed]))])
    with pytest.raises(ResearchDataError, match="overlapping.*changed"):
        bad.fetch_raw("SH510300", "2013-01-01", "2016-12-31")


@pytest.mark.parametrize("problem", ["wrong-mode", "wrong-symbol", "bad-date", "duplicate", "nan", "unit", "html", "js"])
def test_semantically_invalid_response_is_never_cached_as_success(tmp_path, problem):
    data = payload([row("2024-01-02")])
    if problem == "wrong-mode":
        data = payload([row("2024-01-02")], "hfq")
    elif problem == "wrong-symbol":
        data = payload([row("2024-01-02")], symbol="sh510500")
    elif problem == "bad-date":
        data = payload([row("not-a-date")])
    elif problem == "duplicate":
        data = payload([row("2024-01-02"), row("2024-01-02")])
    elif problem == "nan":
        data["data"]["sh510300"]["day"][0][1] = "NaN"
    elif problem == "unit":
        data["data"]["sh510300"]["day"][0][8] = "0.01"
    elif problem == "html":
        data = "<html>private-fixture-body</html>"
    elif problem == "js":
        data = "kline_fixture=__import__('os').system('false')"
    p, _, _ = provider([Response(data)], tmp_path)
    p.begin_download(tmp_path, {"fixture": problem})
    with pytest.raises(ResearchDataError):
        p.fetch_raw("SH510300", "2024-01-01", "2024-01-05")
    assert not list(tmp_path.glob("generations/*/responses/*.json"))


def test_hfq_must_be_echoed_and_cannot_silently_fall_back_to_raw():
    p, _, _ = provider([Response(payload([row("2024-01-02")]))])
    with pytest.raises(ResearchDataError, match="explicit hfqday"):
        p.fetch_adjusted("SH510300", "2024-01-01", "2024-01-05", "hfq")


def test_resume_revalidates_cached_window_and_never_reissues_success(tmp_path):
    p, session, _ = provider([Response(payload([row("2024-01-02")]))], tmp_path)
    p.begin_download(tmp_path, {"fixture": "resume"})
    first = p.fetch_raw("SH510300", "2024-01-01", "2024-01-05")
    second = p.fetch_raw("SH510300", "2024-01-01", "2024-01-05")
    pd.testing.assert_frame_equal(first, second)
    assert len(session.calls) == 1


def test_403_stops_without_rotation_or_retry():
    p, session, _ = provider([Response({}, 403)], max_attempts=4)
    with pytest.raises(ResearchDataError, match="Tencent HTTP 403"):
        p.fetch_raw("SH510300", "2024-01-01", "2024-01-05")
    assert len(session.calls) == 1


def test_known_2015_etf_consolidation_is_visible_in_raw_and_removed_by_hfq():
    raw_rows = [["2015-04-10", "2.165", "2.243", "2.249", "2.151", "376902.83", {}, "0", "8314.72", "", ""],
                ["2015-04-15", "8.100", "7.818", "8.100", "7.790", "260645.05", {}, "0", "20686.94", "", ""]]
    hfq_rows = copy.deepcopy(raw_rows)
    hfq_rows[1][1:5] = ["2.271", "2.192", "2.271", "2.184"]
    p, _, _ = provider([Response(payload(raw_rows, symbol="sh510500")),
        Response(payload(hfq_rows, "hfq", symbol="sh510500"))])
    raw = p.fetch_raw("SH510500", "2015-04-10", "2015-04-17")
    adjusted = p.fetch_adjusted("SH510500", "2015-04-10", "2015-04-17", "hfq")
    pair = _pair(raw, adjusted, "SH510500", "2015-04-10", "2015-04-17")
    assert pair.iloc[1]["raw_close"] / pair.iloc[0]["raw_close"] > 3
    assert _price_quality(pair, "SH510500", {"asset_type": "ETF"})["status"] == "passed_rejection_checks"


def test_affine_gate_is_explicit_does_not_relax_other_sources_and_rejects_incoherence():
    raw = pd.DataFrame({"date": ["2014-02-25"], "open": [2.209], "close": [2.151],
        "high": [2.219], "low": [2.147], "volume": [100], "amount": [220]})
    adjusted = raw.copy()
    adjusted.loc[0, ["open", "close", "high", "low"]] = [.850, .828, .853, .827]
    raw.attrs["price_resolution"] = adjusted.attrs["price_resolution"] = .001
    with pytest.raises(ResearchDataError, match="coherent per-day factor"):
        _pair(raw, adjusted, "SH510300", "2014-02-25", "2014-02-25")
    raw.attrs["adjustment_transform"] = adjusted.attrs["adjustment_transform"] = "affine_cash_accumulation"
    result = _pair(raw, adjusted, "SH510300", "2014-02-25", "2014-02-25")
    assert result.iloc[0]["adjustment_factor"] == pytest.approx(.828 / 2.151)
    adjusted.loc[0, "high"] = .870
    with pytest.raises(ResearchDataError, match="positive affine"):
        _pair(raw, adjusted, "SH510300", "2014-02-25", "2014-02-25")


def test_snapshot_exposes_affine_cash_semantics_and_never_claims_total_return(tmp_path):
    p, _, _ = provider([Response(payload([row("2024-01-02")])),
        Response(payload([row("2024-01-02", adjusted=True)], "hfq"))])
    artifact = download_research_prices(["SH510300"], "2024-01-02", "2024-01-02", tmp_path, provider=p)
    semantics = artifact.manifest["price_semantics"]
    assert semantics["adjustment_transform"] == "affine_cash_accumulation"
    assert semantics["adjustment_factor"] == "observed_close_ratio_only_not_a_uniform_OHLC_multiplier"
    assert semantics["dividend_reinvestment_verified"] is False
    assert "not_dividend_reinvestment_total_return" in semantics["return_interpretation"]
    assert artifact.manifest["execution_ready"] is False
    assert any("Three-decimal" in limitation for limitation in artifact.manifest["limitations"])
