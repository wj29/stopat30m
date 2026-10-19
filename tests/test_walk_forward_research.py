"""Synthetic accounting/timing regressions, never evidence of strategy returns."""
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m.backtest.walk_forward import ResearchConfig, load_protocol, lock_protocol, run_walk_forward
from stopat30m.data.historical_universe import HistoricalUniverse

A, B, INDEX = "SH510300", "SH510500", "SH000300"


def sample(*, flat=False):
    calendar = pd.bdate_range("2019-11-01", periods=80)
    opens = pd.DataFrame({A: 10 * 1.002 ** np.arange(80), B: 10 * .999 ** np.arange(80), INDEX: 10 * 1.001 ** np.arange(80)}, index=calendar)
    if flat:
        opens.loc[:, :] = 10.0
    close = opens.copy()
    prices = close.rename_axis("date").reset_index().melt(id_vars="date", var_name="instrument", value_name="adjusted_close")
    prices["tradestatus"] = "1"
    artifact = SimpleNamespace(adjusted_open=opens, adjusted_close=close, calendar=calendar, prices=prices,
        manifest={"request": {"start_date": str(calendar[0].date()), "end_date": str(calendar[-1].date()), "adjustment": "hfq", "instruments": [A, B, INDEX]}, "provider": {"name": "synthetic_fixture"}}, path="synthetic-fixture")
    cfg = ResearchConfig(train_start=str(calendar[10].date()), train_end=str(calendar[19].date()),
        validation_start=str(calendar[20].date()), validation_end=str(calendar[29].date()),
        oos_start=str(calendar[30].date()), oos_end=str(calendar[49].date()),
        lookback_sessions=3, rebalance_sessions=2, fold_sessions=5)
    universe = HistoricalUniverse.fixed_pool("synthetic explicit research pool", [A, B], str(calendar[0].date()), str(calendar[-1].date()), selection_date="2026-10-19")
    return artifact, universe, cfg


def execute(tmp_path, artifact, universe, cfg, tag="run"):
    protocol = tmp_path / f"{tag}-protocol.json"
    lock_protocol(cfg, protocol, universe=universe)
    return run_walk_forward(artifact, universe, protocol, tmp_path / tag)


def final(result, strategy="momentum"):
    return result.returns[(result.returns.split == "final_oos") & (result.returns.strategy == strategy)].reset_index(drop=True)


def test_protocol_is_explicit_hash_locked_and_idempotent_without_price_access(tmp_path):
    artifact, universe, config = sample()
    path = tmp_path / "protocol.json"
    first = lock_protocol(config, path, universe=universe)
    assert first == lock_protocol(config, path, universe=universe)
    assert first["historical_oos_is_retrospective"] is True
    assert first["universe_sha256"]
    with pytest.raises(ValueError, match="cannot be overwritten"):
        lock_protocol(replace(config, lookback_sessions=4), path, universe=universe)
    first["config"]["lookback_sessions"] = 4
    path.write_text(json.dumps(first))
    with pytest.raises(ValueError, match="checksum mismatch"):
        load_protocol(path)


def test_overlapping_or_reversed_train_validation_oos_is_rejected():
    _, _, config = sample()
    for changes in ({"train_end": config.validation_start}, {"validation_end": config.oos_start}, {"oos_end": config.oos_start[:-2] + "01"}):
        with pytest.raises(ValueError):
            replace(config, **changes)


def test_universe_cannot_be_swapped_after_rules_are_locked(tmp_path):
    artifact, universe, config = sample()
    protocol = tmp_path / "locked.json"
    lock_protocol(config, protocol, universe=universe)
    changed = HistoricalUniverse.fixed_pool("changed pool", [A], universe.coverage_start, universe.coverage_end)
    with pytest.raises(ValueError, match="differs from the pool"):
        run_walk_forward(artifact, changed, protocol, tmp_path / "invalid")
    assert not (tmp_path / "invalid").exists()


def test_future_prices_do_not_change_development_or_same_day_signal(tmp_path):
    artifact, universe, config = sample()
    before = execute(tmp_path, artifact, universe, config, "before")
    day = pd.Timestamp(config.oos_start)
    artifact.adjusted_close.loc[day:, B] *= 7
    artifact.adjusted_open.loc[day:, B] *= 5
    after = execute(tmp_path, artifact, universe, config, "after")
    assert before.report["splits"]["train"] == after.report["splits"]["train"]
    assert before.report["splits"]["validation"] == after.report["splits"]["validation"]
    first_before = pd.read_csv(before.path / "signals.csv").query("split == 'final_oos'").iloc[:2]
    first_after = pd.read_csv(after.path / "signals.csv").query("split == 'final_oos'").iloc[:2]
    pd.testing.assert_frame_equal(first_before, first_after)
    assert set(first_before.signal_date) == {str(artifact.calendar[29].date())}
    assert before.manifest["deterministic_fingerprint"] != after.manifest["deterministic_fingerprint"]


def test_old_overnight_and_new_intraday_compound_without_lookahead(tmp_path):
    artifact, universe, config = sample(flat=True)
    days = artifact.calendar
    config = replace(config, lookback_sessions=1, rebalance_sessions=1, oos_end=str(days[31].date()), buy_fee_bps=0, sell_fee_bps=0, slippage_bps=0)
    artifact.adjusted_close.loc[days[29], A] = 20
    artifact.adjusted_close.loc[days[30], B] = 20
    artifact.adjusted_open.loc[days[31], A] = 11
    artifact.adjusted_close.loc[days[31], A] = 11
    artifact.adjusted_open.loc[days[31], B] = 20
    artifact.adjusted_close.loc[days[31], B] = 24
    result = execute(tmp_path, artifact, universe, config)
    out = final(result)
    assert out.nav.iloc[0] == pytest.approx(1)
    assert out.overnight_return.iloc[1] == pytest.approx(.1)
    assert out.intraday_return.iloc[1] == pytest.approx(.2)
    assert out.nav.iloc[1] == pytest.approx(1.32)
    trades = pd.read_csv(result.path / "trades.csv").query("split == 'final_oos' and strategy == 'momentum'")
    assert trades.iloc[0].instrument == A
    assert trades.iloc[-1].instrument == B


def test_first_day_fees_reduce_nav_and_are_in_drawdown_and_baselines(tmp_path):
    artifact, universe, config = sample(flat=True)
    config = replace(config, positive_momentum_only=False, rebalance_sessions=100)
    result = execute(tmp_path, artifact, universe, config)
    expected = 1 / (1 + .0005)  # 3bp commission + 2bp slippage, post-cost exposure.
    for strategy in ("momentum", "equal_weight", "buy_hold"):
        out = final(result, strategy)
        assert out.nav.iloc[0] == pytest.approx(expected)
        assert out.net_return.iloc[0] == pytest.approx(expected - 1)
        assert out.net_return.iloc[1:].to_numpy() == pytest.approx(np.zeros(len(out) - 1), abs=1e-12)
        report = result.report["splits"]["final_oos"][strategy]
        assert report["total_return"] == pytest.approx(expected - 1, abs=1e-6)
        assert report["max_drawdown"] == pytest.approx(expected - 1, abs=5e-5)
        assert out.fee_cost.sum() + out.slippage_cost.sum() == pytest.approx(1 - expected)
    assert result.report["execution_ready"] is False


def test_negative_or_zero_momentum_goes_to_cash_without_fake_training(tmp_path):
    artifact, universe, config = sample(flat=True)
    result = execute(tmp_path, artifact, universe, config)
    out = final(result)
    assert out.nav.to_numpy() == pytest.approx(np.ones(len(out)))
    assert out.fee_cost.sum() == 0
    assert result.report["splits"]["final_oos"]["momentum"]["total_trades"] == 0
    assert result.report["model_fit"] is False
    assert result.report["parameter_search"] is False
    assert result.report["universe_audit"]["retrospective_selection"] is True


def test_rolling_blocks_do_not_reset_portfolio_or_charge_new_entry_cost(tmp_path):
    artifact, universe, config = sample(flat=True)
    config = replace(config, positive_momentum_only=False, rebalance_sessions=100)
    result = execute(tmp_path, artifact, universe, config)
    folds = result.folds[result.folds.strategy == "momentum"]
    assert len(folds) == 4
    assert not folds.portfolio_reset.any()
    assert folds.total_trades.tolist() == [1, 0, 0, 0]
    assert folds.start_nav.iloc[1] == pytest.approx(folds.end_nav.iloc[0])
    assert folds.start_nav.iloc[1] != 1
    assert np.prod(1 + folds.total_return) - 1 == pytest.approx(result.report["splits"]["final_oos"]["momentum"]["total_return"], abs=1e-6)


@pytest.mark.parametrize("field,value", [("adjusted_open", np.nan), ("adjusted_close", np.nan), ("adjusted_close", 0.0), ("adjusted_open", -1.0), ("adjusted_close", np.inf)])
def test_missing_held_or_invalid_prices_are_never_forward_filled(tmp_path, field, value):
    artifact, universe, config = sample()
    getattr(artifact, field).loc[pd.Timestamp(config.oos_start), A] = value
    with pytest.raises(ValueError, match="price"):
        execute(tmp_path, artifact, universe, config)
    assert not (tmp_path / "run").exists()


def test_missing_comparator_and_unsorted_dates_are_rejected(tmp_path):
    artifact, universe, config = sample()
    artifact.adjusted_open = artifact.adjusted_open.drop(columns=INDEX)
    artifact.adjusted_close = artifact.adjusted_close.drop(columns=INDEX)
    with pytest.raises(ValueError, match="comparators"):
        execute(tmp_path, artifact, universe, config)
    artifact, universe, config = sample()
    artifact.adjusted_close = artifact.adjusted_close.iloc[::-1]
    with pytest.raises(ValueError, match="sorted and unique"):
        execute(tmp_path, artifact, universe, config, "unsorted")


def test_same_inputs_reproduce_all_report_and_csv_hashes_without_overwrite(tmp_path):
    artifact, universe, config = sample()
    protocol = tmp_path / "protocol.json"
    lock_protocol(config, protocol, universe=universe)
    a = run_walk_forward(artifact, universe, protocol, tmp_path / "a")
    b = run_walk_forward(artifact, universe, protocol, tmp_path / "b")
    assert a.manifest["run_id"] == b.manifest["run_id"]
    assert a.manifest["output_checksums"] == b.manifest["output_checksums"]
    assert a.report == b.report
    assert hashlib.sha256((a.path / "report.json").read_bytes()).hexdigest() == b.manifest["output_checksums"]["report.json"]
    with pytest.raises(FileExistsError):
        run_walk_forward(artifact, universe, protocol, a.path)


def test_external_model_uses_identical_cost_accounting_and_previous_session(tmp_path):
    artifact, universe, config = sample(flat=True)
    protocol = tmp_path / "model-base.json"
    lock_protocol(config, protocol, universe=universe)
    calls = []

    def target(signal_day, execution_day, members):
        assert artifact.calendar[artifact.calendar.get_loc(execution_day) - 1] == signal_day
        calls.append((signal_day, execution_day))
        return {A: 1.0}, [{"instrument": A, "score": 0.1, "selected": True}]

    result = run_walk_forward(artifact, universe, protocol, tmp_path / "comparison",
        additional_strategies={"lightgbm": target}, comparison_metadata={"training_end": config.train_end})
    model = final(result, "lightgbm")
    reference = final(result, "buy_hold")
    assert model.nav.tolist() == pytest.approx(reference.nav.tolist())
    assert model.fee_cost.sum() == pytest.approx(reference.fee_cost.sum())
    assert calls and result.report["training_metrics_are_in_sample"] is True
    assert result.report["model_fit"] is True
    assert result.report["comparison"]["training_end"] == config.train_end


@pytest.mark.parametrize("weights", [{INDEX: 1.0}, {A: -0.1}, {A: float('nan')}, {A: 1.1}, {A: .5, B: .5}])
def test_external_model_cannot_bypass_locked_universe_or_exposure_limits(tmp_path, weights):
    artifact, universe, config = sample()
    protocol = tmp_path / "base.json"
    lock_protocol(config, protocol, universe=universe)
    with pytest.raises(ValueError, match="External strategy"):
        run_walk_forward(artifact, universe, protocol, tmp_path / "invalid",
            additional_strategies={"lightgbm": lambda *args: (weights, [])})
    assert not (tmp_path / "invalid").exists()


def test_only_explicit_verified_halt_carries_valuation_and_defers_trade_without_filling_prices(tmp_path):
    artifact, universe, config = sample()
    day = artifact.calendar[artifact.calendar.get_loc(pd.Timestamp(config.oos_start)) + 2]
    artifact.adjusted_open.loc[day, A] = np.nan
    artifact.adjusted_close.loc[day, A] = np.nan
    protocol = tmp_path / "halt-base.json"
    lock_protocol(config, protocol, universe=universe)
    evidence = {(day, A): {"not_tradeable": True, "source_url": "https://issuer.example/suspension",
                           "known_at": "2019-01-01"}}
    result = run_walk_forward(artifact, universe, protocol, tmp_path / "halt-run", known_halts=evidence)
    rows = final(result)
    held = rows.loc[rows.date.eq(day)].iloc[0]
    assert held.carried_valuation_instruments == A
    assert held.rebalance_deferred_for_verified_halt
    assert held.fee_cost == 0 and held.slippage_cost == 0
    trades = pd.read_csv(result.path / "trades.csv", parse_dates=["date"])
    assert not ((trades.date == day) & (trades.instrument == A)).any()
    assert pd.isna(artifact.adjusted_open.loc[day, A]) and pd.isna(artifact.adjusted_close.loc[day, A])
    assert result.report["verified_halts"][0]["source_url"] == evidence[(day, A)]["source_url"]


def test_snapshot_membership_is_rejected_and_data_source_binding_is_checked(tmp_path):
    artifact, universe, config = sample()
    snapshot = SimpleNamespace(as_dict=lambda: {**universe.as_dict(), "kind": "snapshot"})
    with pytest.raises(ValueError, match="snapshot"):
        lock_protocol(config, tmp_path / "bad.json", universe=snapshot)
    payload = universe.as_dict()
    payload["metadata"] = {"research_data_request": {"source": "locked-provider", **artifact.manifest["request"]}}
    bound = HistoricalUniverse.from_dict(payload)
    lock_protocol(config, tmp_path / "bound.json", universe=bound)
    with pytest.raises(ValueError, match="provider differs"):
        run_walk_forward(artifact, bound, tmp_path / "bound.json", tmp_path / "wrong-source")


def test_known_suspension_is_not_treated_as_an_executable_exit(tmp_path):
    artifact, universe, config = sample()
    next_rebalance = artifact.calendar[32]
    mask = (artifact.prices.date == next_rebalance) & (artifact.prices.instrument == A)
    artifact.prices.loc[mask, "tradestatus"] = "0"
    with pytest.raises(ValueError, match="suspended"):
        execute(tmp_path, artifact, universe, config)


def test_source_backed_calendar_cannot_be_swapped_after_lock(tmp_path):
    artifact, universe, config = sample()
    payload = universe.as_dict()
    payload["metadata"] = {"research_data_request": {
        "source": artifact.manifest["provider"]["name"], **artifact.manifest["request"],
        "calendar_source": "baostock.query_trade_dates",
    }}
    bound = HistoricalUniverse.from_dict(payload)
    lock_protocol(config, tmp_path / "calendar-bound.json", universe=bound)
    artifact.manifest["calendar"] = {"source": "weekday_guess"}
    with pytest.raises(ValueError, match="Calendar provider"):
        run_walk_forward(artifact, bound, tmp_path / "calendar-bound.json", tmp_path / "wrong-calendar")
