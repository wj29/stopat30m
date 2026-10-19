"""Source alignment, share units and immutable provenance; no network requests."""
import json

import numpy as np
import pandas as pd
import pytest

from stopat30m.data.research_prices import (
    BaoStockResearchProvider, EastmoneyResearchProvider, ResearchDataError, download_research_prices, load_research_prices,
)

START, END = "2026-01-01", "2026-01-06"
SESSIONS = ["2026-01-02", "2026-01-05", "2026-01-06"]


class FixtureProvider:
    source, version, source_url = "offline_fixture", "1", "fixture://unit-tests"
    volume_unit, amount_unit = "shares", "CNY"
    def __init__(self):
        self.calls = []
        self.raw = pd.DataFrame({"date": SESSIONS, "code": "sh.510300", "open": [10, 11, 12],
            "high": [11, 12, 13], "low": [9, 10, 11], "close": [10, 11, 12],
            "volume": [123, 234, 345], "amount": [1230, 2574, 4140], "tradestatus": "1", "isST": "0"})
        self.adjusted = self.raw.copy()
        for field in ["open", "high", "low", "close"]:
            self.adjusted[field] *= [1, 2, 2]
    def fetch_calendar(self, start, end):
        self.calls.append("calendar")
        dates = pd.date_range(start, end).strftime("%Y-%m-%d")
        return pd.DataFrame({"date": dates, "is_trading_day": [int(day in SESSIONS) for day in dates]})
    def fetch_raw(self, inst, start, end):
        self.calls.append("raw")
        return self.raw.copy()
    def fetch_adjusted(self, inst, start, end, adjustment):
        self.calls.append("adjusted")
        return self.adjusted.copy()


def download(tmp_path, provider=None, **kw):
    return download_research_prices(["SH510300"], START, END, tmp_path,
                                   provider=provider or FixtureProvider(), **kw)


def test_dual_prices_keep_raw_share_units_and_true_holiday_calendar(tmp_path):
    artifact = download(tmp_path)
    assert artifact.calendar.strftime("%Y-%m-%d").tolist() == SESSIONS
    assert pd.Timestamp("2026-01-01") not in artifact.calendar  # Weekday holiday is not inferred as open.
    assert artifact.raw_prices["raw_volume"].tolist() == [123, 234, 345]
    assert artifact.raw_prices["raw_close"].tolist() == [10, 11, 12]
    assert artifact.adjusted_close["SH510300"].tolist() == [10, 22, 24]
    assert artifact.adjusted_open["SH510300"].tolist() == [10, 22, 24]
    assert artifact.prices["adjustment_factor"].tolist() == [1, 2, 2]
    assert artifact.manifest["coverage"]["SH510300"]["factor_change_count"] == 1
    assert artifact.manifest["execution_ready"] is False
    assert artifact.manifest["corporate_actions_complete"] is False
    assert artifact.manifest["price_semantics"]["volume_unit"] == "shares"
    assert len(artifact.manifest_sha256) == 64


def test_cache_reuses_valid_snapshot_and_refresh_creates_new_immutable_version(tmp_path):
    provider = FixtureProvider()
    first = download(tmp_path, provider)
    cached = download(tmp_path, provider)
    assert len(provider.calls) == 3
    assert cached.path == first.path
    same_refresh = download(tmp_path, provider, refresh=True)
    assert same_refresh.manifest_sha256 == first.manifest_sha256
    provider.raw.loc[0, "amount"] += 1
    newer = download(tmp_path, provider, refresh=True)
    assert newer.path != first.path
    assert load_research_prices(first.path).raw_prices.iloc[0]["raw_amount"] == 1230
    assert load_research_prices(newer.path).raw_prices.iloc[0]["raw_amount"] == 1231


def test_corrupt_csv_and_relabelled_manifest_are_rejected(tmp_path):
    artifact = download(tmp_path / "one")
    prices = artifact.path / "prices.csv"
    prices.write_bytes(prices.read_bytes() + b"\n")
    with pytest.raises(ResearchDataError, match="checksum"):
        load_research_prices(artifact.path)
    artifact = download(tmp_path / "two")
    manifest = dict(artifact.manifest)
    manifest["price_semantics"]["volume_unit"] = "hands"
    (artifact.path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ResearchDataError, match="content address"):
        load_research_prices(artifact.path)


def test_equal_row_counts_are_not_enough_to_pair_raw_and_adjusted_dates(tmp_path):
    provider = FixtureProvider()
    provider.adjusted.loc[0, "date"] = "2026-01-01"
    with pytest.raises(ResearchDataError, match="dates do not align"):
        download(tmp_path, provider)


def test_missing_sessions_remain_nan_with_explicit_coverage(tmp_path):
    provider = FixtureProvider()
    provider.raw = provider.raw.iloc[[0, 2]]
    provider.adjusted = provider.adjusted.iloc[[0, 2]]
    artifact = download(tmp_path, provider)
    assert np.isnan(artifact.adjusted_close.loc["2026-01-05", "SH510300"])
    assert artifact.manifest["coverage"]["SH510300"]["missing_sessions"] == ["2026-01-05"]


@pytest.mark.parametrize("fault", ["active_nan", "fractional_volume", "incoherent_factor", "wrong_code", "duplicate_day"])
def test_invalid_provider_prices_fail_without_writing_a_snapshot(tmp_path, fault):
    provider = FixtureProvider()
    if fault == "active_nan":
        provider.raw.loc[0, "close"] = np.nan
    elif fault == "fractional_volume":
        provider.raw["volume"] = provider.raw["volume"].astype(float)
        provider.raw.loc[0, "volume"] = 1.5
    elif fault == "incoherent_factor":
        provider.adjusted.loc[1, "open"] = 21
    elif fault == "wrong_code":
        provider.raw["code"] = "sz.159915"
    elif fault == "duplicate_day":
        provider.raw.loc[1, "date"] = provider.raw.loc[0, "date"]
    with pytest.raises(ResearchDataError):
        download(tmp_path, provider)
    assert not (tmp_path / "snapshots").exists()


def test_known_suspension_does_not_turn_zero_prices_into_fills(tmp_path):
    provider = FixtureProvider()
    for frame in [provider.raw, provider.adjusted]:
        frame.loc[1, ["open", "high", "low", "close"]] = 0
        frame.loc[1, "tradestatus"] = "0"
        frame.loc[1, ["volume", "amount"]] = 0
    artifact = download(tmp_path, provider)
    assert np.isnan(artifact.adjusted_open.loc["2026-01-05", "SH510300"])
    assert artifact.manifest["coverage"]["SH510300"]["suspended_sessions"] == ["2026-01-05"]


def test_incomplete_calendar_or_unverified_units_are_not_guessed(tmp_path):
    provider = FixtureProvider()
    full = provider.fetch_calendar(START, END)
    provider.fetch_calendar = lambda *a: full.iloc[:-1]
    with pytest.raises(ResearchDataError, match="every requested natural day"):
        download(tmp_path, provider)
    provider = FixtureProvider()
    provider.volume_unit = "hands"
    with pytest.raises(ResearchDataError, match="shares"):
        download(tmp_path, provider)


class Response:
    error_code, error_msg = "0", "success"
    def __init__(self, frame=None):
        self.frame = frame
        self.fields = [] if frame is None else frame.columns.tolist()
        self._row = 0
    def next(self):
        return self.frame is not None and self._row < len(self.frame)
    def get_row_data(self):
        row = self.frame.iloc[self._row].tolist()
        self._row += 1
        return row
    def get_data(self):
        return self.frame.copy()


class BaoStockClientFixture:
    def __init__(self, kind="5"):
        self.kind, self.flags = kind, []
    def login(self):
        return Response()
    def logout(self):
        return Response()
    def query_stock_basic(self, code):
        return Response(pd.DataFrame({"code": [code], "type": [self.kind], "code_name": ["fixture"]}))
    def query_trade_dates(self, start_date, end_date):
        return Response(FixtureProvider().fetch_calendar(start_date, end_date).rename(columns={"date": "calendar_date"}))
    def query_history_k_data_plus(self, code, fields, **kw):
        self.flags.append(kw["adjustflag"])
        fixture = FixtureProvider()
        result = fixture.raw if kw["adjustflag"] == "3" else fixture.adjusted
        result = result.copy()
        if self.kind == "5" and kw["adjustflag"] != "3":
            for field in ["open", "high", "low", "close"]:
                result[field] = fixture.raw[field] * [1, 1.02, 1.02]
        result["code"], result["adjustflag"] = code, kw["adjustflag"]
        return Response(result[[field for field in fields.split(",")]])


def test_baostock_downloads_same_source_flag3_and_flag1_with_share_volume(tmp_path):
    client = BaoStockClientFixture()
    provider = BaoStockResearchProvider(client)
    artifact = download(tmp_path, provider)
    assert client.flags == ["3", "1"]
    assert artifact.manifest["instruments"]["SH510300"]["asset_type"] == "ETF"
    assert artifact.raw_prices["raw_volume"].tolist() == [123, 234, 345]


def test_index_identity_adjustment_is_explicit_and_not_a_fabricated_factor(tmp_path):
    client = BaoStockClientFixture(kind="2")
    artifact = download_research_prices(["SH000300"], START, END, tmp_path, provider=BaoStockResearchProvider(client))
    assert client.flags == ["3", "3"]
    assert artifact.manifest["instruments"]["SH000300"]["adjustment_applied"] == "none_index"
    assert artifact.prices["adjustment_factor"].tolist() == [1, 1, 1]


def test_baostock_pagination_uses_public_cursor_without_removed_pandas_append():
    class PagedResponse:
        fields = ["date", "is_trading_day"]
        error_code, error_msg = "0", "success"
        def __init__(self, fail_last=False):
            self.pages = [[["2026-01-01", "0"], ["2026-01-02", "1"]],
                          [["2026-01-03", "0"], ["2026-01-04", "0"]],
                          [["2026-01-05", "1"], ["2026-01-06", "1"]]]
            self.page, self.row = 0, 0
            self.fail_last = fail_last
        def get_data(self):
            raise AssertionError("get_data still uses removed DataFrame.append")
        def next(self):
            if self.row == len(self.pages[self.page]):
                self.page, self.row = self.page + 1, 0
            if self.fail_last and self.page == 2:
                self.error_code, self.error_msg = "10002007", "synthetic page failure"
                return False
            return self.page < len(self.pages)
        def get_row_data(self):
            result = self.pages[self.page][self.row]
            self.row += 1
            return result
    frame = BaoStockResearchProvider._frame(PagedResponse(), "calendar")
    assert len(frame) == 6
    assert frame.iloc[-1]["date"] == "2026-01-06"
    with pytest.raises(ResearchDataError, match="pagination failed"):
        BaoStockResearchProvider._frame(PagedResponse(fail_last=True), "calendar")


@pytest.mark.parametrize("fault", ["missing", "mixed"])
def test_baostock_adjustment_mode_must_be_echoed_for_every_bar(fault):
    client = BaoStockClientFixture()
    original = client.query_history_k_data_plus
    def invalid_response(*args, **kwargs):
        response = original(*args, **kwargs)
        frame = response.frame.copy()
        if fault == "missing":
            frame = frame.drop(columns="adjustflag")
        else:
            frame.loc[0, "adjustflag"] = "3"
        return Response(frame)
    client.query_history_k_data_plus = invalid_response
    with pytest.raises(ResearchDataError, match="unexpected adjustment mode"):
        BaoStockResearchProvider(client).fetch_adjusted("SH510300", START, END, "hfq")


class EastmoneyCalendarFixture(FixtureProvider):
    calendar_source = "baostock.query_trade_dates"
    calendar_source_url = "https://www.baostock.com/"
    def instrument_info(self, instrument):
        return {"asset_type": "index" if instrument == "SH000300" else "ETF", "name": "fixture"}


class JsonResponse:
    status_code, headers = 200, {}
    def __init__(self, payload):
        self.payload = payload
    def raise_for_status(self):
        return None
    def json(self):
        return self.payload


class EastmoneySessionFixture:
    def __init__(self, fault=None):
        self.calls, self.fault = [], fault
        self.now = 1767261600.0
    def clock(self):
        return self.now
    def sleep(self, delay):
        self.now += delay
    def get(self, url, **kwargs):
        params = kwargs["params"]
        self.calls.append(dict(params))
        # A 2-for-1 split: adjusted close is continuous across the action,
        # while raw prices and reported lots retain their original scale.
        raw = [10, 5, 5.1]
        adjusted = raw if params["fqt"] == "0" else [10, 10, 10.2]
        if self.fault == "missing_adjustment":
            adjusted = raw
        bars = []
        for i, day in enumerate(SESSIONS):
            price = adjusted[i]
            lots, amount = (i + 1) * 123, (i + 1) * 123 * 100 * raw[i]
            if self.fault == "wrong_volume_unit":
                lots *= 100
            bars.append(f"{day},{price},{price},{price},{price},{lots},{amount},0,0,0,0")
        payload = {"rc": 0, "data": {"code": params["secid"].split(".")[1], "market": int(params["secid"][0]),
            "name": "fixture", "decimal": 3, "klines": bars}}
        if self.fault == "wrong_market":
            payload["data"]["market"] = 0
        elif self.fault == "bad_schema":
            payload = []
        elif self.fault == "truncated":
            payload["data"]["truncated"] = True
        elif self.fault == "history_total":
            payload["data"]["dktotal"] = 9999
            payload["data"]["total"] = 9999
        return JsonResponse(payload)


def test_eastmoney_explicit_dual_prices_convert_lots_keep_calendar_provenance(tmp_path):
    session = EastmoneySessionFixture()
    provider = EastmoneyResearchProvider(session, EastmoneyCalendarFixture(), clock=session.clock, sleep=session.sleep)
    artifact = download(tmp_path, provider)
    assert [call["fqt"] for call in session.calls] == ["0", "2"]
    assert all(call["ut"] == "7eea3edcaed734bea9cbfc24409ed989" for call in session.calls)
    assert artifact.prices["raw_close"].tolist() == [10, 5, 5.1]
    assert artifact.prices["adjusted_close"].tolist() == [10, 10, 10.2]
    assert artifact.prices["raw_volume"].tolist() == [12300, 24600, 36900]
    assert artifact.manifest["provider"]["name"] == "eastmoney"
    assert artifact.manifest["provider"]["volume_provenance"]["multiplier_to_shares"] == 100
    assert artifact.manifest["calendar"]["source"] == "baostock.query_trade_dates"
    assert artifact.manifest["coverage"]["SH510300"]["unknown_status_count"] == 3
    assert artifact.manifest["coverage"]["SH510300"]["quality"]["max_absolute_adjusted_daily_return"] < 0.03


@pytest.mark.parametrize("fault, message", [("missing_adjustment", "discontinuity"),
    ("wrong_market", "wrong instrument"), ("wrong_volume_unit", "volume unit"), ("bad_schema", "no valid")])
def test_eastmoney_rejects_source_quality_schema_and_unit_errors(tmp_path, fault, message):
    session = EastmoneySessionFixture(fault)
    provider = EastmoneyResearchProvider(session, EastmoneyCalendarFixture(), clock=session.clock, sleep=session.sleep)
    with pytest.raises(ResearchDataError, match=message):
        download(tmp_path, provider)
    assert not (tmp_path / "snapshots").exists()


def test_eastmoney_qfq_is_explicit_and_never_substitutes_baostock_prices(tmp_path):
    session = EastmoneySessionFixture()
    provider = EastmoneyResearchProvider(session, EastmoneyCalendarFixture(), clock=session.clock, sleep=session.sleep)
    artifact = download(tmp_path, provider, adjustment="qfq")
    assert [call["fqt"] for call in session.calls] == ["0", "1"]
    assert artifact.manifest["price_semantics"]["adjusted"] == "qfq"


@pytest.mark.parametrize("incoherent", [False, True])
def test_declared_three_decimal_precision_uses_common_factor_interval(tmp_path, incoherent):
    provider = FixtureProvider()
    provider.raw = provider.raw.iloc[[0]].copy()
    provider.adjusted = provider.raw.copy()
    for field, raw, adjusted in zip(["open", "high", "low", "close"],
                                    [1.005, 1.055, .995, 1.025], [1.129, 1.185, 1.118, 1.152]):
        provider.raw[field] = raw
        provider.adjusted[field] = adjusted
    provider.raw.attrs["price_resolution"] = .001
    provider.adjusted.attrs["price_resolution"] = .001
    if incoherent:
        provider.adjusted["open"] = 1.135
        with pytest.raises(ResearchDataError, match="within declared precision"):
            download(tmp_path, provider)
    else:
        artifact = download(tmp_path, provider)
        assert artifact.prices.iloc[0]["adjustment_factor"] == pytest.approx(1.152 / 1.025)


def test_eastmoney_resume_reuses_raw_response_and_refresh_is_a_new_vintage(tmp_path):
    import requests
    session = EastmoneySessionFixture()
    original = session.get
    state = {"fail": True}
    def fail_adjusted(url, **kwargs):
        if kwargs["params"]["fqt"] == "2" and state["fail"]:
            session.calls.append(dict(kwargs["params"]))
            raise requests.ConnectionError("offline disconnect")
        return original(url, **kwargs)
    session.get = fail_adjusted
    provider = EastmoneyResearchProvider(session, EastmoneyCalendarFixture(),
        clock=session.clock, sleep=session.sleep, max_attempts=2)
    with pytest.raises(ResearchDataError, match="retries exhausted"):
        download(tmp_path, provider)
    assert [call["fqt"] for call in session.calls] == ["0", "2", "2"]
    state["fail"] = False
    artifact = download(tmp_path, provider)
    assert [call["fqt"] for call in session.calls] == ["0", "2", "2", "2"]
    original_generation = artifact.manifest["download_provenance"]["generation"]
    assert len(artifact.manifest["download_provenance"]["responses"]) == 2
    assert download(tmp_path, provider).path == artifact.path
    assert len(session.calls) == 4
    refreshed = download(tmp_path, provider, refresh=True)
    assert [call["fqt"] for call in session.calls][-2:] == ["0", "2"]
    assert refreshed.manifest["download_provenance"]["generation"] != original_generation
    assert load_research_prices(artifact.path).manifest_sha256 == artifact.manifest_sha256


def test_eastmoney_bad_single_response_is_not_cached_as_success(tmp_path):
    session = EastmoneySessionFixture("wrong_volume_unit")
    provider = EastmoneyResearchProvider(session, EastmoneyCalendarFixture(), clock=session.clock, sleep=session.sleep)
    with pytest.raises(ResearchDataError, match="volume unit"):
        download(tmp_path, provider)
    assert not list((tmp_path / "http").rglob("responses/*.json"))


def test_explicit_truncation_is_rejected_without_guessing_paging(tmp_path):
    session = EastmoneySessionFixture("truncated")
    provider = EastmoneyResearchProvider(session, EastmoneyCalendarFixture(), clock=session.clock, sleep=session.sleep)
    with pytest.raises(ResearchDataError, match="explicitly marked.*truncated"):
        download(tmp_path, provider)
    assert len(session.calls) == 1
    assert not list((tmp_path / "http").rglob("responses/*.json"))


def test_undocumented_total_counts_do_not_trigger_speculative_extra_requests(tmp_path):
    session = EastmoneySessionFixture("history_total")
    provider = EastmoneyResearchProvider(session, EastmoneyCalendarFixture(), clock=session.clock, sleep=session.sleep)
    download(tmp_path, provider)
    assert len(session.calls) == 2


def test_os_cache_lock_rejects_another_writer_and_releases_after_failure(tmp_path):
    from stopat30m.data.research_http import ResearchHTTPTransport, ResearchDownloadError
    first = ResearchHTTPTransport(EastmoneySessionFixture(), cache_dir=tmp_path)
    other = ResearchHTTPTransport(EastmoneySessionFixture(), cache_dir=tmp_path)
    with pytest.raises(RuntimeError, match="synthetic interruption"):
        with first.download_lock():
            with pytest.raises(ResearchDownloadError, match="busy"):
                with other.download_lock():
                    pytest.fail("second writer acquired the same OS cache lock")
            raise RuntimeError("synthetic interruption")
    with other.download_lock():
        pass
