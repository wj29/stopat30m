"""Offline adversarial checks for resumable public research downloads."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
import pandas as pd

from stopat30m.data.research_http import ResearchDownloadError, ResearchHTTPTransport
from stopat30m.data.research_prices import EastmoneyResearchProvider, ResearchDataError, download_research_prices


class Clock:
    # Retain injected clocks: transport separates independent offline hosts by
    # clock identity, which must not be recycled between test cases.
    instances = []

    def __init__(self):
        self.now = datetime(2026, 10, 19, 2, tzinfo=timezone.utc).timestamp()
        self.waits = []
        self.instances.append(self)

    def __call__(self):
        return self.now

    def sleep(self, delay):
        self.waits.append(delay)
        self.now += delay


class Response:
    def __init__(self, payload=None, *, status=200, headers=None, html=False):
        self.status_code, self.headers = status, headers or {}
        self.payload, self.html = payload, html

    def json(self):
        if self.html:
            raise ValueError("<html>rate limited</html>")
        return self.payload


class Session:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, endpoint, **kwargs):
        self.calls.append((endpoint, kwargs))
        assert self.responses, "unexpected network attempt in offline test"
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


SCOPE = {"provider": "eastmoney", "version": "fixture-v1", "request": {
    "instruments": ["SH510300"], "start_date": "2014-01-01", "end_date": "2025-12-31", "adjustment": "hfq"}}
RAW = {"secid": "1.510300", "beg": "20140101", "end": "20251231", "fqt": "0"}
ADJUSTED = {**RAW, "fqt": "2"}
PAYLOAD = {"code": "510300", "bars": [["2025-12-31", 10.0]]}


def valid(payload):
    if not isinstance(payload, dict) or payload.get("code") != "510300" or not payload.get("bars"):
        raise ResearchDownloadError("fixture schema/wrong instrument")


def transport(tmp_path, session, clock, **kwargs):
    return ResearchHTTPTransport(session, cache_dir=tmp_path, clock=clock,
                                 sleep=clock.sleep, random=lambda: 0, **kwargs)


def get(client, params=RAW):
    return client.get(params, timeout=20, headers={}, validate=valid)


def response_files(tmp_path):
    return sorted(tmp_path.glob("generations/*/responses/*.json"))


def test_resume_reuses_only_validated_response_and_preserves_original_download_time(tmp_path):
    clock = Clock()
    first = transport(tmp_path, Session(Response(PAYLOAD), Response(status=403)), clock)
    generation = first.begin_request(SCOPE)
    assert get(first) == PAYLOAD
    before = json.loads(response_files(tmp_path)[0].read_text())
    with pytest.raises(ResearchDownloadError, match="403"):
        get(first, ADJUSTED)

    clock.now += 30
    session = Session(Response(PAYLOAD))
    resumed = transport(tmp_path, session, clock)
    assert resumed.has_pending(SCOPE)
    assert resumed.begin_request(SCOPE) == generation
    assert get(resumed) == PAYLOAD
    assert len(session.calls) == 0
    assert json.loads(response_files(tmp_path)[0].read_text()) == before
    assert get(resumed, ADJUSTED) == PAYLOAD
    assert len(session.calls) == 1


def test_refresh_never_reuses_a_prior_generation_raw_or_adjusted_response(tmp_path):
    clock, session = Clock(), Session(Response(PAYLOAD), Response(PAYLOAD))
    first = transport(tmp_path, session, clock)
    old = first.begin_request(SCOPE)
    get(first)
    refreshed = transport(tmp_path, session, clock)
    new = refreshed.begin_request(SCOPE, refresh=True)
    assert new != old
    get(refreshed)
    assert len(session.calls) == 2
    assert len(response_files(tmp_path)) == 2
    assert {json.loads(path.read_text())["request"]["generation"] for path in response_files(tmp_path)} == {old, new}


@pytest.mark.parametrize("fault,message", [("payload", "checksum"), ("request", "identity")])
def test_corrupt_successful_response_stops_resume_without_network(tmp_path, fault, message):
    clock = Clock()
    client = transport(tmp_path, Session(Response(PAYLOAD)), clock)
    client.begin_request(SCOPE)
    get(client)
    path = response_files(tmp_path)[0]
    wrapper = json.loads(path.read_text())
    if fault == "payload":
        wrapper["payload"]["bars"][0][1] = 999
    else:
        wrapper["request"]["params"]["fqt"] = "2"
    path.write_text(json.dumps(wrapper))
    session = Session()
    resumed = transport(tmp_path, session, clock)
    resumed.begin_request(SCOPE)
    with pytest.raises(ResearchDownloadError, match=message):
        get(resumed)
    assert not session.calls


@pytest.mark.parametrize("response", [Response(html=True), Response({"code": "other", "bars": [1]})])
def test_html_and_wrong_instrument_json_are_not_retried_or_cached(tmp_path, response):
    clock, session = Clock(), Session(response)
    client = transport(tmp_path, session, clock)
    client.begin_request(SCOPE)
    with pytest.raises(ResearchDownloadError):
        get(client)
    assert len(session.calls) == 1
    assert not response_files(tmp_path)
    assert clock.waits == []


def test_http403_stops_immediately_while429_obeys_retry_after(tmp_path):
    clock, session = Clock(), Session(Response(status=403))
    client = transport(tmp_path / "forbidden", session, clock)
    client.begin_request(SCOPE)
    with pytest.raises(ResearchDownloadError, match="403"):
        get(client)
    assert len(session.calls) == 1
    assert clock.waits == []

    clock, session = Clock(), Session(Response(status=429, headers={"Retry-After": "7"}), Response(PAYLOAD))
    client = transport(tmp_path / "limited", session, clock)
    client.begin_request(SCOPE)
    assert get(client) == PAYLOAD
    assert len(session.calls) == 2
    assert clock.waits == [7]
    assert len(response_files(tmp_path / "limited")) == 1


def test_long_retry_after_survives_restart_and_refresh_without_waiting(tmp_path):
    clock, session = Clock(), Session(Response(status=429, headers={"Retry-After": "3600"}))
    client = transport(tmp_path, session, clock)
    client.begin_request(SCOPE)
    with pytest.raises(ResearchDownloadError, match="blocked_until"):
        get(client)
    assert len(session.calls) == 1
    assert clock.waits == []
    assert not response_files(tmp_path)

    session = Session()
    restarted = transport(tmp_path, session, clock)
    restarted.begin_request(SCOPE, refresh=True)
    with pytest.raises(ResearchDownloadError, match="blocked_until"):
        get(restarted)
    assert not session.calls
    clock.now += 3601
    after_cooldown = transport(tmp_path, Session(Response(PAYLOAD)), clock)
    after_cooldown.begin_request(SCOPE)
    assert get(after_cooldown) == PAYLOAD


def test_changed_logical_request_does_not_reuse_identical_http_parameters(tmp_path):
    clock, session = Clock(), Session(Response(PAYLOAD), Response(PAYLOAD))
    client = transport(tmp_path, session, clock)
    first = client.begin_request(SCOPE)
    get(client)
    changed_scope = {**SCOPE, "version": "fixture-v2"}
    second = client.begin_request(changed_scope)
    get(client)
    assert second != first
    assert len(session.calls) == 2


def test_each_attempt_on_one_host_has_the_minimum_interval(tmp_path):
    clock, session = Clock(), Session(Response(PAYLOAD), Response(PAYLOAD))
    client = transport(tmp_path, session, clock)
    client.begin_request(SCOPE)
    get(client)
    get(client, ADJUSTED)
    assert clock.waits == [2]
    assert len({call[0] for call in session.calls}) == 1
    assert all(call[1].get("allow_redirects") is False for call in session.calls)


def test_pending_generation_cannot_resume_across_shanghai_calendar_day(tmp_path):
    clock = Clock()
    first = transport(tmp_path, Session(Response(PAYLOAD)), clock)
    generation = first.begin_request(SCOPE)
    get(first)
    clock.now += 24 * 60 * 60
    restarted = transport(tmp_path, Session(), clock)
    with pytest.raises(ResearchDownloadError, match="refresh|Shanghai|day|date"):
        restarted.begin_request(SCOPE)
    assert restarted.begin_request(SCOPE, refresh=True) != generation


@pytest.mark.parametrize("timestamp", [None, "2026-10-19T10:00:00", "2026-10-19T03:00:00+00:00"])
def test_cached_response_requires_a_valid_original_generation_timestamp(tmp_path, timestamp):
    clock = Clock()
    first = transport(tmp_path, Session(Response(PAYLOAD)), clock)
    first.begin_request(SCOPE)
    get(first)
    path = response_files(tmp_path)[0]
    wrapper = json.loads(path.read_text())
    wrapper["fetched_at"] = timestamp
    path.write_text(json.dumps(wrapper))
    session = Session()
    resumed = transport(tmp_path, session, clock)
    resumed.begin_request(SCOPE)
    with pytest.raises(ResearchDownloadError, match="timestamp|checksum"):
        get(resumed)
    assert not session.calls


def test_request_finishing_after_shanghai_midnight_does_not_cache_a_mixed_day_response(tmp_path):
    clock = Clock()
    class CrossMidnightSession(Session):
        def get(self, endpoint, **kwargs):
            result = super().get(endpoint, **kwargs)
            clock.now += 24 * 60 * 60
            return result
    session = CrossMidnightSession(Response(PAYLOAD))
    client = transport(tmp_path, session, clock)
    client.begin_request(SCOPE)
    with pytest.raises(ResearchDownloadError, match="timestamp|date|day"):
        get(client)
    assert not response_files(tmp_path)


class ArtifactProvider:
    """Two complete synthetic sessions with distinct provenance, no network."""
    version, source_url = "1", "fixture://resume-provenance"
    volume_unit, amount_unit = "shares", "CNY"

    def __init__(self, source):
        self.source = source

    def fetch_calendar(self, start, end):
        return pd.DataFrame({"date": [start, end], "is_trading_day": [1, 1]})

    def fetch_raw(self, inst, start, end):
        return pd.DataFrame({"date": [start, end], "code": [inst, inst], "open": [10, 10],
                             "high": [10, 10], "low": [10, 10], "close": [10, 10],
                             "volume": [100, 100], "amount": [1000, 1000]})

    def fetch_adjusted(self, inst, start, end, adjustment):
        return self.fetch_raw(inst, start, end)

    def instrument_info(self, instrument):
        return {"asset_type": "ETF", "name": "fixture"}


def test_valid_artifact_cache_pointer_cannot_silently_change_source(tmp_path):
    args = (["SH510300"], "2020-01-02", "2020-01-03", tmp_path)
    first = download_research_prices(*args, provider=ArtifactProvider("fixture-source-A"))
    second = download_research_prices(*args, provider=ArtifactProvider("fixture-source-B"))
    assert first.path != second.path
    for index_path in (tmp_path / "requests").glob("*.json"):
        if json.loads(index_path.read_text())["artifact_id"] == first.manifest["artifact_id"]:
            index_path.write_text(json.dumps({"artifact_id": second.manifest["artifact_id"],
                                             "manifest_sha256": second.manifest_sha256}))
            break
    else:
        raise AssertionError("first request index was not recorded")
    with pytest.raises(ResearchDataError, match="provider|source|match|identity"):
        download_research_prices(*args, provider=ArtifactProvider("fixture-source-A"))


@pytest.mark.parametrize("fault", ["invalid_date", "nonfinite_close", "wrong_volume_unit", "truncated_bar"])
def test_invalid_single_bar_never_enters_successful_response_cache(tmp_path, fault):
    rows = [["2020-01-02", "10", "10", "10", "10", "1", "1000", "0", "0", "0", "0"],
            ["2020-01-03", "10", "10", "10", "10", "1", "1000", "0", "0", "0", "0"]]
    if fault == "invalid_date":
        rows[0][0] = "2020-01-32"
    elif fault == "nonfinite_close":
        rows[0][2] = "NaN"
    elif fault == "wrong_volume_unit":
        rows[0][5] = "100"
    else:
        rows[0].pop()
    payload = {"rc": 0, "data": {"code": "510300", "market": 1, "name": "fixture",
                                  "klines": [",".join(row) for row in rows]}}
    # A second response would be consumed only if invalid raw data escaped the
    # individual-response gate and reached subsequent adjusted-pair assembly.
    session = Session(Response(payload), Response(payload))
    clock = Clock()
    response_cache = tmp_path / "download-cache"
    provider = EastmoneyResearchProvider(session, ArtifactProvider("fixture-calendar"),
        cache_dir=response_cache, clock=clock, sleep=clock.sleep, random=lambda: 0)
    with pytest.raises((ResearchDataError, ValueError)):
        download_research_prices(["SH510300"], "2020-01-02", "2020-01-03", tmp_path, provider=provider)
    assert not response_files(response_cache)
    assert not (tmp_path / "snapshots").exists()
