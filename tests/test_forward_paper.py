"""Synthetic forward-clock/accounting fixtures, never strategy performance."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta
import hashlib
import json
from threading import Event
from zoneinfo import ZoneInfo

import pytest

from stopat30m.data.research_calendar import Calendar
from stopat30m.trading import forward_paper as paper

A, B = "SH510300", "SH510500"
TZ = ZoneInfo("Asia/Shanghai")
MODEL = {"model_sha256": "a" * 64, "trained_through": "2025-12-31", "kind": "frozen_lightgbm_fixture"}
PROTOCOL = {"protocol_sha256": "b" * 64, "config": {"rebalance_sessions": 20}, "id": "offline_fixed_comparison"}
TARGET = {"momentum": {A: 1.0}, "lightgbm": {A: 1.0}}


def clock(day, at="15:20"):
    return datetime.fromisoformat(f"{day}T{at}:00+08:00")


@pytest.fixture
def calendar():
    start, end = date(2026, 10, 1), date(2027, 1, 5)
    days = [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
    # Explicit synthetic exchange flags; not a production weekday fallback.
    return Calendar.from_rows([{"date": day.isoformat(), "is_open": day.weekday() < 5} for day in days],
        str(start), str(end), source="synthetic_exchange_fixture", source_url="fixture:calendar",
        fetched_at="2026-10-01T00:00:00+08:00")


def freeze(tmp_path, config=None):
    config = config or paper.ForwardPaperConfig()
    protocol = deepcopy(PROTOCOL)
    protocol["config"]["rebalance_sessions"] = config.rebalance_sessions
    path = tmp_path / "paper"
    paper.create_forward_paper(path, config, protocol=protocol, model=MODEL, universe=[A, B], now=clock("2026-10-19", "15:06"))
    return path


def bars(day, calendar, *, previous=None, opening=None, closing=None, factor=2):
    previous, opening, closing = previous or {A: 10, B: 10}, opening or {A: 10, B: 10}, closing or {A: 10, B: 10}
    return {instrument: {"session_date": day, "previous_session": calendar.previous_session(day),
        "raw_open": opening[instrument], "raw_close": closing[instrument],
        "adjusted_open": opening[instrument] * factor, "adjusted_close": closing[instrument] * factor,
        "previous_raw_close": previous[instrument], "previous_adjusted_close": previous[instrument] * factor,
        "complete": True, "tradestatus": "1"} for instrument in (A, B)}


def evidence(day, rows=None):
    result = {"session_date": day, "source": "offline_same_source", "fetched_at": clock(day, "15:10").isoformat(),
        "source_snapshot_sha256": hashlib.sha256(f"{day}-snapshot".encode()).hexdigest(),
        "source_manifest_sha256": hashlib.sha256(f"{day}-manifest".encode()).hexdigest(),
        "model_sha256": MODEL["model_sha256"], "protocol_sha256": PROTOCOL["protocol_sha256"],
        "role": "forward_completed_daily", "completed": True}
    if rows is not None:
        result["bars"] = rows
    return result


def signal(path, day, calendar, targets=None, rows=None, **kwargs):
    return paper.record_close_signal(path, day, targets or TARGET, calendar=calendar,
        evidence=evidence(day, rows or bars(day, calendar)), now=clock(day), **kwargs)


def settle(path, day, calendar, rows=None, **kwargs):
    return paper.settle_completed_session(path, day, rows or bars(day, calendar), calendar=calendar,
        evidence=evidence(day), now=clock(day), **kwargs)


def files(path):
    return {str(item.relative_to(path)): item.read_bytes() for item in path.rglob("*") if item.is_file() and not item.name.startswith(".")}


def test_creation_freezes_both_strategies_model_pool_costs_and_cannot_be_overwritten(tmp_path):
    path = freeze(tmp_path)
    manifest = paper.load_forward_paper(path)
    assert manifest["strategies"] == ["momentum", "lightgbm"]
    assert manifest["execution_ready"] is False
    assert manifest["cash_return"] == 0
    assert paper.create_forward_paper(path, paper.ForwardPaperConfig(), protocol=PROTOCOL, model=MODEL,
        universe=[B, A], now=clock("2026-10-20")) == manifest
    for kwargs in ({"config": replace(paper.ForwardPaperConfig(), buy_fee_bps=10)}, {"model": {**MODEL, "model_sha256": "c" * 64}}, {"universe": [A]}):
        call = {"config": paper.ForwardPaperConfig(), "protocol": PROTOCOL, "model": MODEL, "universe": [A, B], **kwargs}
        with pytest.raises(paper.ForwardPaperError, match="different rules/model/pool"):
            paper.create_forward_paper(path, now=clock("2026-10-20"), **call)


def test_initial_signal_is_prospective_and_does_not_invent_a_nav(tmp_path, calendar):
    path = freeze(tmp_path)
    result = signal(path, "2026-10-19", calendar)
    status = paper.forward_paper_status(path)
    assert result["execution_session"] == "2026-10-20"
    assert result["rebalance"] is True
    assert status["latest_signal_date"] == "2026-10-19"
    assert status["latest_settlement_date"] is None and not status["has_performance"]
    assert status["portfolios"]["momentum"]["cash"] == 100000
    assert not (path / "sessions").exists()


def test_first_execution_costs_are_identical_and_included_in_net_nav_and_drawdown(tmp_path, calendar):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    result = settle(path, "2026-10-20", calendar)
    expected = 100000 / 1.0005
    for strategy in paper.STRATEGIES:
        portfolio = result["portfolios"][strategy]
        assert portfolio["equity"] == pytest.approx(expected)
        assert portfolio["nav"] == pytest.approx(expected / 100000)
        assert portfolio["fee"] == pytest.approx(expected * .0003)
        assert portfolio["slippage"] == pytest.approx(expected * .0002)
        assert portfolio["cash"] == 0
        assert portfolio["trades"][0]["live_fill"] is False
        assert "quantity" not in portfolio["trades"][0]
    assert result["portfolios"]["momentum"] == result["portfolios"]["lightgbm"]
    status = paper.forward_paper_status(path)
    assert status["portfolios"]["momentum"]["max_drawdown"] == pytest.approx(expected / 100000 - 1)
    assert status["session_count"] == 1 and status["has_performance"]
    assert "annual_return" not in status["portfolios"]["momentum"]


def test_cash_only_targets_have_zero_return_and_zero_cost(tmp_path, calendar):
    path = freeze(tmp_path)
    signal(path, "2026-10-19", calendar, targets={"momentum": {}, "lightgbm": {}})
    result = settle(path, "2026-10-20", calendar)
    for item in result["portfolios"].values():
        assert item["equity"] == item["cash"] == 100000
        assert item["daily_return"] == item["cash_return"] == item["fee"] == item["slippage"] == 0
        assert item["positions"] == {} and item["trades"] == []


def test_previous_holdings_earn_overnight_new_holdings_earn_intraday(tmp_path, calendar):
    config = paper.ForwardPaperConfig(rebalance_sessions=1, buy_fee_bps=0, sell_fee_bps=0, slippage_bps=0)
    path = freeze(tmp_path, config)
    signal(path, "2026-10-19", calendar); settle(path, "2026-10-20", calendar)
    signal(path, "2026-10-20", calendar, targets={"momentum": {B: 1}, "lightgbm": {B: 1}})
    day = bars("2026-10-21", calendar, opening={A: 11, B: 10}, closing={A: 11, B: 12})
    result = settle(path, "2026-10-21", calendar, day)
    portfolio = result["portfolios"]["momentum"]
    assert portfolio["overnight_return"] == pytest.approx(.1)
    assert portfolio["daily_return"] == pytest.approx(.32)
    assert portfolio["equity"] == pytest.approx(132000)
    assert [item["direction"] for item in portfolio["trades"]] == ["SELL", "BUY"]
    paper.forward_paper_status(path)


def test_switching_assets_charges_sell_and_buy_costs_without_borrowing(tmp_path, calendar):
    path = freeze(tmp_path, paper.ForwardPaperConfig(rebalance_sessions=1))
    signal(path, "2026-10-19", calendar)
    first = settle(path, "2026-10-20", calendar)["portfolios"]["momentum"]
    signal(path, "2026-10-20", calendar, targets={strategy: {B: 1} for strategy in paper.STRATEGIES})
    second = settle(path, "2026-10-21", calendar)["portfolios"]["momentum"]
    expected = first["equity"] * (1 - .0005) / (1 + .0005)
    assert second["equity"] == pytest.approx(expected)
    assert second["cash"] == 0
    assert [trade["direction"] for trade in second["trades"]] == ["SELL", "BUY"]
    assert second["fee"] == pytest.approx((first["equity"] + expected) * .0003)
    assert second["slippage"] == pytest.approx((first["equity"] + expected) * .0002)
    assert second["total_fees"] == pytest.approx(first["fee"] + second["fee"])
    paper.forward_paper_status(path)


def test_both_strategies_remain_independent_without_selecting_an_oos_winner(tmp_path, calendar):
    path = freeze(tmp_path, paper.ForwardPaperConfig(buy_fee_bps=0, sell_fee_bps=0, slippage_bps=0))
    signal(path, "2026-10-19", calendar, targets={"momentum": {A: 1}, "lightgbm": {B: 1}})
    result = settle(path, "2026-10-20", calendar, bars("2026-10-20", calendar, closing={A: 11, B: 9}))
    assert result["portfolios"]["momentum"]["equity"] == 110000
    assert result["portfolios"]["lightgbm"]["equity"] == 90000
    assert "winner" not in result and set(result["portfolios"]) == set(paper.STRATEGIES)


def test_non_rebalance_days_drift_without_rebalancing_until_session_20(tmp_path, calendar):
    path = freeze(tmp_path, paper.ForwardPaperConfig(buy_fee_bps=0, sell_fee_bps=0, slippage_bps=0))
    equal = {strategy: {A: .5, B: .5} for strategy in paper.STRATEGIES}
    changed = {strategy: {B: 1} for strategy in paper.STRATEGIES}
    signal(path, "2026-10-19", calendar, targets=equal)
    sessions = calendar.sessions("2026-10-20", "2026-11-17")
    previous_prices = {A: 10, B: 10}
    for index, day in enumerate(sessions):
        closes = {A: 11, B: 10}
        rows = bars(day, calendar, previous=previous_prices, opening=previous_prices, closing=closes)
        result = settle(path, day, calendar, rows)
        portfolio = result["portfolios"]["momentum"]
        if index == 0:
            assert portfolio["weights"][A] == pytest.approx(11 / 21)
        elif index < 20:
            assert portfolio["trades"] == []
            assert portfolio["weights"][A] == pytest.approx(11 / 21)
        else:
            assert result["rebalance"] and portfolio["positions"].keys() == {B}
            assert len(portfolio["trades"]) == 2
        decision = signal(path, day, calendar, targets=changed, rows=rows)
        assert decision["rebalance"] is ((index + 1) % 20 == 0)
        previous_prices = closes
    assert paper.forward_paper_status(path)["session_count"] == 21


def test_rebalance_override_is_rejected_and_next_session_respects_explicit_holidays(tmp_path, calendar):
    path = freeze(tmp_path)
    with pytest.raises(paper.ForwardPaperError, match="cadence"):
        signal(path, "2026-10-19", calendar, rebalance=False)
    payload = calendar.to_dict()
    for row in payload["days"]:
        if row["date"] == "2026-10-20":
            row["is_open"] = False
    holiday = Calendar.from_rows(payload["days"], payload["coverage_start"], payload["coverage_end"],
        source="synthetic_holiday", source_url="fixture:calendar", fetched_at=payload["fetched_at"])
    result = signal(path, "2026-10-19", holiday)
    assert result["execution_session"] == "2026-10-21"


def test_same_vintage_return_ratios_survive_adjustment_basis_rescaling(tmp_path, calendar):
    path = freeze(tmp_path, paper.ForwardPaperConfig(buy_fee_bps=0, sell_fee_bps=0, slippage_bps=0))
    signal(path, "2026-10-19", calendar); settle(path, "2026-10-20", calendar)
    signal(path, "2026-10-20", calendar)
    row = bars("2026-10-21", calendar, opening={A: 11, B: 10}, closing={A: 11, B: 10}, factor=10)
    result = settle(path, "2026-10-21", calendar, row)
    assert result["portfolios"]["momentum"]["equity"] == pytest.approx(110000)


def test_previous_raw_mark_revision_is_not_silently_rebased(tmp_path, calendar):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    row = bars("2026-10-20", calendar, previous={A: 9.9, B: 10})
    with pytest.raises(paper.ForwardPaperError, match="raw close was revised"):
        settle(path, "2026-10-20", calendar, row)
    assert not (path / "sessions").exists()


def test_exact_retries_replay_immutable_records_without_double_cost(tmp_path, calendar):
    path = freeze(tmp_path)
    first = signal(path, "2026-10-19", calendar)
    assert signal(path, "2026-10-19", calendar) == {**first, "replayed": True}
    saved = settle(path, "2026-10-20", calendar)
    before = files(path)
    assert settle(path, "2026-10-20", calendar) == {**saved, "replayed": True}
    assert files(path) == before
    assert len(list((path / "sessions").glob("*.json"))) == 1
    with pytest.raises(paper.ForwardPaperError, match="cannot be overwritten"):
        signal(path, "2026-10-19", calendar, targets={strategy: {B: 1} for strategy in paper.STRATEGIES})


def test_response_loss_after_atomic_publication_replays_original_receipt(tmp_path, calendar, monkeypatch):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    original = paper.os.link
    def publish_then_disconnect(source, target):
        original(source, target)
        raise OSError("synthetic response loss after atomic publication")
    monkeypatch.setattr(paper.os, "link", publish_then_disconnect)
    with pytest.raises(OSError, match="synthetic response loss"):
        settle(path, "2026-10-20", calendar)
    monkeypatch.setattr(paper.os, "link", original)
    replay = settle(path, "2026-10-20", calendar)
    assert replay["replayed"] and len(replay["portfolios"]["momentum"]["trades"]) == 1
    assert paper.forward_paper_status(path)["session_count"] == 1


def test_concurrent_writer_cannot_publish_another_daily_execution(tmp_path, calendar, monkeypatch):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    entered, release = Event(), Event()
    original = paper._append
    def held_append(target, payload):
        if target.parent.name == "sessions":
            entered.set()
            assert release.wait(3)
        return original(target, payload)
    monkeypatch.setattr(paper, "_append", held_append)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(settle, path, "2026-10-20", calendar)
        assert entered.wait(3)
        try:
            with pytest.raises(paper.ForwardPaperError, match="Another process"):
                settle(path, "2026-10-20", calendar)
        finally:
            release.set()
        assert future.result()["replayed"] is False
    assert paper.forward_paper_status(path)["session_count"] == 1


@pytest.mark.parametrize("when", ["14:59", "15:00", "15:04"])
def test_session_must_be_complete_with_five_minute_publication_delay(tmp_path, calendar, when):
    path = freeze(tmp_path)
    with pytest.raises(paper.ForwardPaperError, match="post-close publication delay|Clock precedes"):
        paper.record_close_signal(path, "2026-10-19", TARGET, calendar=calendar,
            evidence=evidence("2026-10-19", bars("2026-10-19", calendar)), now=clock("2026-10-19", when))


@pytest.mark.parametrize("session,now", [("2026-10-19", "2026-10-20"), ("2026-10-21", "2026-10-20")])
def test_past_and_future_forward_performance_cannot_be_written(tmp_path, calendar, session, now):
    path = freeze(tmp_path)
    with pytest.raises(paper.ForwardPaperError, match="no historical/future backfill"):
        paper.record_close_signal(path, session, TARGET, calendar=calendar,
            evidence=evidence(session, bars(session, calendar)), now=clock(now))


@pytest.mark.parametrize("update", [
    {"role": "historical_oos"}, {"role": "warmup"}, {"completed": False},
    {"session_date": "2025-12-31"}, {"source_snapshot_sha256": ""},
    {"model_sha256": "c" * 64}, {"protocol_sha256": "d" * 64},
    {"fetched_at": "2026-10-19T14:59:00+08:00"}, {"fetched_at": "2026-10-19T15:21:00+08:00"},
    {"fetched_at": "2026-10-19T15:10:00"},
])
def test_oos_warmup_stale_future_and_wrong_model_evidence_is_rejected(tmp_path, calendar, update):
    path = freeze(tmp_path)
    info = evidence("2026-10-19", bars("2026-10-19", calendar)); info.update(update)
    with pytest.raises(paper.ForwardPaperError):
        paper.record_close_signal(path, "2026-10-19", TARGET, calendar=calendar, evidence=info, now=clock("2026-10-19"))
    assert not (path / "signals").exists()


@pytest.mark.parametrize("field,value", [("raw_open", 0), ("adjusted_close", float("nan")),
    ("raw_close", -1), ("adjusted_open", float("inf")), ("complete", False),
    ("previous_session", "2026-10-15"), ("tradestatus", "0"), ("adjusted_close", 200)])
def test_missing_invalid_suspended_and_suspicious_bars_do_not_become_trades(tmp_path, calendar, field, value):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    row = bars("2026-10-20", calendar); row[A][field] = value
    before = files(path)
    with pytest.raises(paper.ForwardPaperError):
        settle(path, "2026-10-20", calendar, row)
    assert files(path) == before


@pytest.mark.parametrize("targets", [{"momentum": {A: 1}}, {"momentum": {A: -1}, "lightgbm": {}},
    {"momentum": {A: .8, B: .8}, "lightgbm": {}}, {"momentum": {"SH600000": 1}, "lightgbm": {}},
    {"momentum": {A: True}, "lightgbm": {}}])
def test_targets_cannot_drop_a_comparator_short_or_add_leverage(tmp_path, calendar, targets):
    path = freeze(tmp_path)
    with pytest.raises(paper.ForwardPaperError):
        signal(path, "2026-10-19", calendar, targets=targets)


def test_a_missed_session_is_an_immutable_gap_not_backfilled_performance(tmp_path, calendar):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    settle(path, "2026-10-20", calendar); signal(path, "2026-10-20", calendar)
    with pytest.raises(paper.ForwardPaperError, match="session gap"):
        settle(path, "2026-10-22", calendar)
    status = paper.forward_paper_status(path)
    assert status["halted"] and status["session_count"] == 1
    assert status["gaps"][0]["performance_recorded"] is False
    assert status["latest_settlement_date"] == "2026-10-20"
    assert not (path / "sessions" / "2026-10-21.json").exists()
    assert not (path / "sessions" / "2026-10-22.json").exists()
    with pytest.raises(paper.ForwardPaperError, match="halted"):
        signal(path, "2026-10-22", calendar)


def test_signal_requires_same_day_settlement_and_same_completed_bars(tmp_path, calendar):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    with pytest.raises(paper.ForwardPaperError, match="session gap"):
        signal(path, "2026-10-20", calendar)
    path = freeze(tmp_path / "other"); signal(path, "2026-10-19", calendar); settle(path, "2026-10-20", calendar)
    with pytest.raises(paper.ForwardPaperError, match="same completed price bars"):
        signal(path, "2026-10-20", calendar, rows=bars("2026-10-20", calendar, closing={A: 10.1, B: 10}))


def test_manifest_and_daily_record_tampering_are_rejected(tmp_path, calendar):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar); settle(path, "2026-10-20", calendar)
    target = path / "sessions" / "2026-10-20.json"
    row = json.loads(target.read_text()); row["portfolios"]["momentum"]["equity"] += 1
    target.write_text(json.dumps(row))
    with pytest.raises(paper.ForwardPaperError, match="checksum mismatch"):
        paper.forward_paper_status(path)
    target = path / "manifest.json"
    row = json.loads(target.read_text()); row["model"]["model_sha256"] = "f" * 64; target.write_text(json.dumps(row))
    with pytest.raises(paper.ForwardPaperError, match="checksum mismatch"):
        paper.load_forward_paper(path)


def test_deleted_middle_record_stops_writes_even_without_a_prior_status_call(tmp_path, calendar):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    for day in ("2026-10-20", "2026-10-21", "2026-10-22"):
        settle(path, day, calendar); signal(path, day, calendar)
    (path / "sessions" / "2026-10-21.json").unlink()
    before = files(path)
    with pytest.raises(paper.ForwardPaperError, match="missing"):
        settle(path, "2026-10-23", calendar)
    assert files(path) == before
    assert not (path / "sessions" / "2026-10-23.json").exists()


def test_exact_retry_still_checks_the_existing_history_for_corruption(tmp_path, calendar):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    settle(path, "2026-10-20", calendar)
    target = path / "sessions" / "2026-10-20.json"
    row = json.loads(target.read_text()); row["portfolios"]["momentum"]["cash"] += 1
    target.write_text(json.dumps(row))
    with pytest.raises(paper.ForwardPaperError, match="checksum mismatch"):
        settle(path, "2026-10-20", calendar)


def test_unknown_trade_status_is_disclosed_not_claimed_as_a_verified_fill(tmp_path, calendar):
    path = freeze(tmp_path); signal(path, "2026-10-19", calendar)
    row = bars("2026-10-20", calendar)
    for item in row.values():
        item.pop("tradestatus")
    result = settle(path, "2026-10-20", calendar, row)
    assert result["bars"][A]["tradestatus"] == "unknown"
    assert result["execution_ready"] is False
    assert "not live trading" in result["execution_model"]
