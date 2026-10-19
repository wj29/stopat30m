"""Offline daily orchestration against real immutable price/paper artifacts.

Only target generation is a deterministic model stub. Synthetic exchange flags
and prices are test evidence, never real strategy results or weekday fallbacks.
"""
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import forward_runner as runner
from stopat30m.backtest.walk_forward import ResearchConfig
from stopat30m.data.research_calendar import Calendar
from stopat30m.data import research_prices as prices
from stopat30m.trading.forward_paper import forward_paper_status

POOL = ["SH510300", "SH510500"]
MODEL_SHA = "a" * 64
PROTOCOL_SHA = "b" * 64


def clock(day, at="15:20"):
    return datetime.fromisoformat(f"{day}T{at}:00+08:00")


def no_network(*_args, **_kwargs):
    pytest.fail("A supposedly offline path attempted an external download")


class FixtureProvider:
    source, version, source_url = "offline_forward_fixture", "1", "fixture:daily-prices"
    volume_unit, amount_unit = "shares", "CNY"

    def __init__(self, cached, env):
        self.cached, self.env = cached, env
        self.calendar_source, self.calendar_source_url = cached.calendar_source, cached.calendar_source_url
        self.closed = False

    def fetch_calendar(self, start, end):
        return self.cached.fetch_calendar(start, end)

    def instrument_info(self, instrument):
        return {"asset_type": "ETF", "name": instrument, "listing_date": "2013-01-01"}

    def fetch_raw(self, instrument, start, end):
        sessions = self.cached.calendar.sessions(start, end)
        rows = []
        for day in sessions:
            ordinal = (pd.Timestamp(day) - pd.Timestamp("2026-01-01")).days
            close = 10 + ordinal * (.001 if instrument == POOL[0] else .0005)
            rows.append({"date": day, "code": instrument[:2].lower() + "." + instrument[2:],
                "open": close, "high": close + .01, "low": close - .01, "close": close,
                "volume": 100000, "amount": 100000 * close, "tradestatus": "1", "isST": "0"})
        return pd.DataFrame(rows)

    def fetch_adjusted(self, instrument, start, end, adjustment):
        assert adjustment == "hfq"
        frame = self.fetch_raw(instrument, start, end)
        frame.loc[:, ["open", "high", "low", "close"]] *= 2
        return frame

    def close(self):
        self.closed = True


class FixtureModel:
    def __init__(self, path, env):
        self.path, self.env = Path(path), env
        if (self.path / "model.txt").read_text() != "immutable-test-model":
            raise ValueError("Fixture frozen model checksum mismatch")
        self.model_sha256 = MODEL_SHA
        self.manifest = deepcopy(env.model_manifest)

    def predict(self, artifact, *, signal_dates):
        self.env.predictions.append((artifact.manifest["artifact_id"], signal_dates[0]))
        if self.env.target_failures:
            self.env.target_failures -= 1
            raise ValueError("Synthetic target generation interruption")
        return pd.DataFrame({"signal_date": [signal_dates[0]] * 2, "instrument": POOL,
                             "score": [0.01, 0.02], "model_sha256": MODEL_SHA})

    def target_weights(self, predictions, signal_date, execution_date, eligible_instruments, cash_reserve_pct=0):
        assert eligible_instruments == POOL
        return {POOL[1]: 1 - cash_reserve_pct}, predictions.to_dict("records")


@pytest.fixture
def env(tmp_path, monkeypatch):
    state = SimpleNamespace(day="2026-10-19", at="15:20", downloads=[], providers=[],
        predictions=[], target_failures=0, download_failures=0, wrong_source=False)
    state.instant = lambda: clock(state.day, state.at)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = clock(state.day, "15:10")
            return instant.astimezone(tz or timezone.utc)

    monkeypatch.setattr(prices, "datetime", FrozenDatetime)
    monkeypatch.setattr(runner, "download_calendar", no_network)
    monkeypatch.setattr(runner, "download_research_prices", no_network)
    dates = pd.date_range("2026-01-01", "2027-01-08")
    state.calendar = Calendar.from_rows(
        [{"date": str(day.date()), "is_open": day.weekday() < 5} for day in dates],
        "2026-01-01", "2027-01-08", source="synthetic_explicit_flags", source_url="fixture:calendar",
        fetched_at="2026-01-01T00:00:00+08:00")
    calendar_path = tmp_path / "calendar.json"
    state.calendar.save(calendar_path)
    source_metadata = {instrument: {"asset_type": "ETF"} for instrument in POOL}
    cached = runner.CachedCalendarProvider(state.calendar, source_metadata)
    state.day = "2026-10-16"
    warmup = prices.download_research_prices(POOL, "2026-01-01", state.day, tmp_path / "warmup",
                                            provider=FixtureProvider(cached, state))
    state.day = "2026-10-19"
    state.model_manifest = {"training": {"declared_cutoff": "2025-12-31"},
        "protocol_sha256": "c" * 64, "manifest_sha256": "d" * 64,
        "source_snapshot": str(warmup.path), "source_manifest": deepcopy(warmup.manifest), "instruments": POOL}
    cfg = ResearchConfig(train_start="2014-01-01", train_end="2018-12-31",
        validation_start="2019-01-01", validation_end="2020-12-31", oos_start="2021-01-01", oos_end="2025-12-31")
    experiment = tmp_path / "experiment"
    model_path = experiment / "frozen_model"
    model_path.mkdir(parents=True)
    (model_path / "model.txt").write_text("immutable-test-model")
    (model_path / "protocol.json").write_text(json.dumps(
        {"base_protocol": {"protocol_sha256": PROTOCOL_SHA, "config": asdict(cfg)}}))
    output = experiment / "report.json"
    output.write_text("{\"synthetic\":true}")
    (experiment / "experiment_manifest.json").write_text(json.dumps(
        {"output_checksums": {"report.json": hashlib.sha256(output.read_bytes()).hexdigest()}}))
    monkeypatch.setattr(runner, "load_frozen_model", lambda path: FixtureModel(path, state))
    state.paper = tmp_path / "paper"
    state.initialize = lambda: runner.initialize_forward(experiment, state.paper, calendar_path,
                                                        now=clock("2026-10-19", "15:06"))
    state.initialize()

    def provider_factory(cached_calendar):
        provider = FixtureProvider(cached_calendar, state)
        if state.wrong_source:
            provider.source = "different_source_fixture"
        state.providers.append(provider)
        return provider

    def download(instruments, start, end, cache, **kwargs):
        state.downloads.append({"day": end, "start": start, **kwargs})
        if state.download_failures:
            state.download_failures -= 1
            raise ValueError("Synthetic public source unavailable")
        return prices.download_research_prices(instruments, start, end, cache, **kwargs)

    state.run = lambda: runner.run_daily(state.paper, clock=state.instant, download=download,
                                         provider_factory=provider_factory)
    return state


def test_initialization_and_first_close_signal_do_not_create_historical_performance(env):
    initial = forward_paper_status(env.paper)
    assert not initial["has_performance"] and initial["session_count"] == 0
    result = env.run()
    assert result["status"] == "complete" and result["paper"]["latest_signal_date"] == env.day
    assert not result["paper"]["has_performance"]
    assert result["paper"]["latest_signal"]["execution_session"] == "2026-10-20"
    assert len(env.downloads) == 1 and env.providers[0].closed
    assert not (env.paper / "sessions").exists()
    assert (env.paper / "frozen_model/model.txt").read_text() == "immutable-test-model"


def test_next_session_settles_both_frozen_strategies_then_records_its_next_signal(env):
    env.run(); env.day = "2026-10-20"
    result = env.run()["paper"]
    assert result["session_count"] == 1 and result["has_performance"]
    assert result["latest_settlement_date"] == result["latest_signal_date"] == env.day
    assert result["latest_signal"]["execution_session"] == "2026-10-21"
    assert result["latest_signal"]["rebalance"] is False
    assert result["portfolios"]["momentum"]["positions"].keys() == {POOL[0]}
    assert result["portfolios"]["lightgbm"]["positions"].keys() == {POOL[1]}
    for portfolio in result["portfolios"].values():
        assert portfolio["equity"] == pytest.approx(100000 / 1.0005)
        assert portfolio["total_fees"] > 0 and portfolio["total_slippage"] > 0
    assert len(env.downloads) == 2 and all(provider.closed for provider in env.providers)


def test_completed_day_repeats_skip_download_prediction_and_do_not_rewrite_records(env):
    env.run(); env.day = "2026-10-20"; env.run()
    before = {path: path.read_bytes() for path in env.paper.rglob("*.json")}
    calls, predictions = len(env.downloads), len(env.predictions)
    for _ in range(2):
        result = env.run()
        assert result["status"] == "already_complete"
    assert len(env.downloads) == calls and len(env.predictions) == predictions
    assert {path: path.read_bytes() for path in env.paper.rglob("*.json")} == before


def test_committed_settlement_resumes_exact_snapshot_without_network_or_double_fees(env):
    env.run(); env.day = "2026-10-20"; env.target_failures = 1
    with pytest.raises(ValueError, match="target generation interruption"):
        env.run()
    interrupted = forward_paper_status(env.paper)
    saved = interrupted["latest_settlement"]
    assert interrupted["latest_settlement_date"] == env.day
    assert interrupted["latest_signal_date"] == "2026-10-19"
    count = len(env.downloads)
    result = env.run()["paper"]
    assert len(env.downloads) == count
    assert result["latest_settlement"] == saved and result["session_count"] == 1
    assert result["latest_signal"]["source"]["source_snapshot_sha256"] == saved["source"]["source_snapshot_sha256"]
    receipts = [json.loads(path.read_text()) for path in sorted((env.paper / "runner").glob(f"{env.day}_attempt_*.json"))]
    assert [row["status"] for row in receipts] == ["failed", "complete"]


@pytest.mark.parametrize("day,at,status", [("2026-10-24", "15:20", "market_closed"),
    ("2026-10-19", "09:30", "waiting_for_close"), ("2026-10-19", "15:04", "waiting_for_close")])
def test_closed_market_and_preclose_checks_do_not_download_or_predict(env, day, at, status):
    env.day, env.at = day, at
    assert env.run()["status"] == status
    assert env.downloads == [] and env.predictions == [] and env.providers == []
    assert not (env.paper / "runner").exists()


def test_three_failed_attempts_are_preserved_and_fourth_attempt_stays_offline(env):
    env.download_failures = 10
    for attempt in range(3):
        with pytest.raises(ValueError, match="public source unavailable"):
            env.run()
        receipt = json.loads((env.paper / "runner" / f"{env.day}_attempt_{attempt + 1}.json").read_text())
        assert receipt["status"] == "failed" and receipt["historical_backfill_performed"] is False
    with pytest.raises(ValueError, match="attempt limit"):
        env.run()
    assert len(env.downloads) == 3 and all(provider.closed for provider in env.providers)
    assert not forward_paper_status(env.paper)["has_performance"]


def test_adapter_hash_tampering_is_rejected_before_download(env):
    path = env.paper / "adapter.json"
    adapter = json.loads(path.read_text()); adapter["source"] = "other-source"
    path.write_text(json.dumps(adapter))
    with pytest.raises(ValueError, match="integrity/binding"):
        env.run()
    assert not env.downloads


def test_model_hash_difference_is_rejected_before_download(env, monkeypatch):
    original = runner.load_frozen_model
    def changed_model(path):
        model = original(path); model.model_sha256 = "e" * 64
        return model
    monkeypatch.setattr(runner, "load_frozen_model", changed_model)
    with pytest.raises(ValueError, match="no longer matches"):
        env.run()
    assert not env.downloads and not (env.paper / "signals").exists()


def test_resumed_settlement_rejects_corrupt_snapshot_without_new_network(env):
    env.run(); env.day = "2026-10-20"; env.target_failures = 1
    with pytest.raises(ValueError, match="target generation interruption"):
        env.run()
    saved = forward_paper_status(env.paper)["latest_settlement"]
    path = env.paper / "data/snapshots" / saved["source"]["source_snapshot_sha256"] / "prices.csv"
    path.write_bytes(path.read_bytes() + b"\n")
    count = len(env.downloads)
    with pytest.raises(ValueError, match="checksum"):
        env.run()
    assert len(env.downloads) == count
    assert forward_paper_status(env.paper)["latest_signal_date"] == "2026-10-19"


def test_wrong_download_source_is_rejected_before_committing_the_next_nav(env):
    env.run(); env.day = "2026-10-20"; env.wrong_source = True
    with pytest.raises(ValueError, match="source|Source|provider|Provider"):
        env.run()
    status = forward_paper_status(env.paper)
    assert status["latest_settlement_date"] is None and status["latest_signal_date"] == "2026-10-19"


def test_download_receipt_hash_mismatch_is_rejected_before_committing_nav(env, monkeypatch):
    env.run(); env.day = "2026-10-20"
    original = prices.download_research_prices
    def wrong_receipt(*args, **kwargs):
        artifact = original(*args, **kwargs)
        return replace(artifact, manifest_sha256="e" * 64)
    monkeypatch.setattr(prices, "download_research_prices", wrong_receipt)
    with pytest.raises(ValueError, match="snapshot changed"):
        env.run()
    status = forward_paper_status(env.paper)
    assert status["latest_settlement_date"] is None and status["latest_signal_date"] == "2026-10-19"


def test_initialize_refuses_modified_experiment_before_new_paper_creation(env, tmp_path):
    experiment = env.paper.parent / "experiment"
    (experiment / "report.json").write_text("modified")
    with pytest.raises(ValueError, match="experiment checksum"):
        runner.initialize_forward(experiment, tmp_path / "other-paper", env.paper / "calendar.json",
                                  now=clock(env.day))
    assert not (tmp_path / "other-paper").exists()


def test_direct_forward_candidate_requires_explicit_role_and_keeps_period_separate(env, tmp_path):
    model = env.paper.parent / "experiment/frozen_model"
    target = tmp_path / "refitted-paper"
    with pytest.raises(ValueError, match="forward-only candidate"):
        runner.initialize_forward(None, target, env.paper / "calendar.json", model_artifact=model,
                                  now=clock(env.day))
    assert not target.exists()
    env.model_manifest["training_role"] = "forward_only_no_historical_oos"
    status = runner.initialize_forward(None, target, env.paper / "calendar.json", model_artifact=model,
                                      now=clock(env.day))
    assert status["session_count"] == 0 and status["latest_signal_date"] is None
    assert forward_paper_status(env.paper)["latest_signal_date"] is None


def test_conflicting_calendar_stops_before_creating_paper_period(env, tmp_path):
    rows = [{"date": day, "is_open": 1 - int(flag) if day == "2026-10-16" else flag}
            for day, flag in env.calendar.days]
    wrong = Calendar.from_rows(rows, env.calendar.coverage_start, env.calendar.coverage_end,
                               source="synthetic_conflict", source_url="fixture:conflict")
    path = tmp_path / "conflict.json"
    wrong.save(path)
    target = tmp_path / "conflicting-paper"
    with pytest.raises(ValueError, match="calendars disagree"):
        runner.initialize_forward(env.paper.parent / "experiment", target, path, now=clock(env.day))
    assert not target.exists()


def test_master_revision_failure_blocks_nav_and_signal_before_prediction(env, monkeypatch):
    from stopat30m.data import research_catalog
    env.run(); env.day = "2026-10-20"
    seed = prices.load_research_prices(env.model_manifest["source_snapshot"])
    path = env.paper / "adapter.json"
    adapter = json.loads(path.read_text())
    adapter["dataset_seed"] = {"path": str(seed.path), "manifest_sha256": seed.manifest_sha256}
    adapter["dataset_catalog"] = "data/catalog"
    adapter["sha256"] = runner._hash({key: value for key, value in adapter.items() if key != "sha256"})
    runner._write(path, adapter)
    def revised(*_args, **_kwargs):
        raise ValueError("Historical source revision requires an explicit rebuild")
    monkeypatch.setattr(research_catalog, "publish_daily_update", revised)
    before = len(env.predictions)
    with pytest.raises(ValueError, match="Historical source revision"):
        env.run()
    status = forward_paper_status(env.paper)
    assert status["latest_settlement_date"] is None and status["latest_signal_date"] == "2026-10-19"
    assert len(env.predictions) == before
