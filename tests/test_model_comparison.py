"""Synthetic ML regressions, never evidence of strategy performance."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m.backtest import model_comparison as ml
from stopat30m.backtest import walk_forward as wf
from stopat30m.data.historical_universe import HistoricalUniverse

POOL = ["SH510300", "SH510500", "SZ159915"]
INDEX = "SH000300"


def fixture(start="2017-01-02", periods=440):
    days = pd.bdate_range(start, periods=periods)  # Synthetic test calendar only.
    t, bars = np.arange(periods), []
    for n, inst in enumerate([*POOL, INDEX]):
        opens = (10 + n * 7) * np.exp(np.cumsum(.0002 + .002 * np.sin(t / (8 + n) + n)))
        closes = opens * (1 + .001 * np.sin(t / 3 + n))
        for i, day in enumerate(days):
            bars.append({"date": day, "instrument": inst, "adjusted_open": opens[i],
                "adjusted_high": max(opens[i], closes[i]) * 1.002,
                "adjusted_low": min(opens[i], closes[i]) * .998, "adjusted_close": closes[i],
                "raw_volume": 1_000_000 * (1 + .2 * np.sin(i / 5 + n)), "tradestatus": "1"})
    prices = pd.DataFrame(bars)
    artifact = SimpleNamespace(calendar=days, prices=prices,
        adjusted_open=prices.pivot(index="date", columns="instrument", values="adjusted_open"),
        adjusted_close=prices.pivot(index="date", columns="instrument", values="adjusted_close"),
        manifest={"request": {"start_date": str(days[0].date()), "end_date": str(days[-1].date()),
                              "instruments": [*POOL, INDEX], "adjustment": "hfq"},
                  "provider": {"name": "synthetic_fixture"}}, path="synthetic-fixture")
    universe = HistoricalUniverse.fixed_pool("Synthetic fixed ETF experiment", POOL,
        str(days[0].date()), str(days[-1].date()), selection_date="2026-10-19")
    cfg = wf.ResearchConfig(train_start=str(days[80].date()), train_end=str(days[260].date()),
        validation_start=str(days[261].date()), validation_end=str(days[320].date()),
        oos_start=str(days[321].date()), oos_end=str(days[420].date())) if periods >= 421 else None
    return artifact, universe, cfg


def locked(tmp_path, universe, cfg):
    base, model = tmp_path / "base.json", tmp_path / "model-protocol.json"
    wf.lock_protocol(cfg, base, universe=universe)
    spec = ml.FrozenModelSpec(num_boost_round=8, min_data_in_leaf=20)
    ml.lock_model_protocol(base, model, spec=spec)
    return model, spec


@pytest.fixture
def fitted(tmp_path):
    artifact, universe, cfg = fixture()
    protocol, spec = locked(tmp_path, universe, cfg)
    model = ml.fit_frozen_model(artifact, universe, protocol, tmp_path / "frozen")
    return model, artifact, universe, cfg, protocol, spec


def test_protocol_is_separate_immutable_and_binds_features_labels_parameters(tmp_path):
    _, universe, cfg = fixture()
    base, path = tmp_path / "base.json", tmp_path / "model.json"
    wf.lock_protocol(cfg, base, universe=universe)
    before = base.read_bytes()
    first = ml.lock_model_protocol(base, path)
    assert first == ml.lock_model_protocol(base, path)
    assert first["feature_names"] == list(ml.FEATURE_NAMES)
    assert first["model_spec"]["label_horizon_sessions"] == 20
    assert first["train_params"]["num_threads"] == 1 and first["train_params"]["deterministic"]
    assert base.read_bytes() == before
    with pytest.raises(ValueError, match="already exists"):
        ml.lock_model_protocol(base, path, spec=replace(ml.FrozenModelSpec(), num_boost_round=101))
    first["model_spec"]["num_boost_round"] = 101
    path.write_text(json.dumps(first))
    with pytest.raises(ValueError, match="checksum"):
        ml._load_protocol(path)


def test_model_horizon_and_top1_must_match_shared_execution_protocol(tmp_path):
    _, universe, cfg = fixture()
    for tag, changes in [("horizon", {"rebalance_sessions": 10}), ("topk", {"top_k": 2})]:
        base = tmp_path / (tag + ".json")
        wf.lock_protocol(replace(cfg, **changes), base, universe=universe)
        with pytest.raises(ValueError, match="top1 and label horizon"):
            ml.lock_model_protocol(base, tmp_path / (tag + "-ml.json"))


def test_causal_features_ignore_future_bars_and_do_not_include_labels_or_identity():
    artifact, _, _ = fixture()
    original = ml.build_features(artifact, POOL)
    changed, shock = deepcopy(artifact), artifact.calendar[300]
    mask = changed.prices.date.ge(shock)
    changed.prices.loc[mask, ["adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close"]] *= 7
    changed.prices.loc[mask, "raw_volume"] *= 11
    after = ml.build_features(changed, POOL)
    pd.testing.assert_frame_equal(original.loc[original.signal_date.lt(shock)], after.loc[after.signal_date.lt(shock)])
    assert list(original.columns) == ["signal_date", "instrument", *ml.FEATURE_NAMES]
    assert original.loc[original.signal_date.lt(artifact.calendar[60]), list(ml.FEATURE_NAMES)].isna().all().all()


def test_missing_bar_invalidates_entire_trailing_window_without_fill():
    artifact, _, _ = fixture()
    missing = artifact.calendar[150]
    artifact.prices = artifact.prices.loc[~(artifact.prices.date.eq(missing) & artifact.prices.instrument.eq(POOL[0]))]
    features = ml.build_features(artifact, POOL)
    affected = features.loc[features.instrument.eq(POOL[0]) & features.signal_date.between(missing, artifact.calendar[210])]
    assert len(affected) == 61 and affected.loc[:, list(ml.FEATURE_NAMES)].isna().all().all()
    resumed = features.loc[features.instrument.eq(POOL[0]) & features.signal_date.eq(artifact.calendar[211])]
    assert np.isfinite(resumed.loc[:, list(ml.FEATURE_NAMES)].to_numpy()).all()


def test_label_uses_next_open_and_twenty_open_intervals_with_both_sides_costs():
    artifact, _, cfg = fixture()
    labels = ml._labels(artifact, POOL, ml.FrozenModelSpec(), cfg)
    day, entry, exit_ = artifact.calendar[[100, 101, 121]]
    row = labels.loc[labels.signal_date.eq(day) & labels.instrument.eq(POOL[0])].iloc[0]
    expected = artifact.adjusted_open.at[exit_, POOL[0]] * (1 - .0005) / (artifact.adjusted_open.at[entry, POOL[0]] * (1 + .0005)) - 1
    assert row.label_entry_date == entry and row.label_exit_date == exit_
    assert row.label == pytest.approx(expected)


def test_training_tail_purge_and_no_validation_or_oos_fitting(fitted):
    model, artifact, _, cfg, _, spec = fitted
    train = pd.read_csv(model.path / "training.csv", parse_dates=["signal_date", "label_entry_date", "label_exit_date"])
    assert train.label_exit_date.max() <= pd.Timestamp(cfg.train_end)
    assert train.signal_date.max() == artifact.calendar[260 - spec.label_horizon_sessions - 1]
    assert (train.label_entry_date > train.signal_date).all()
    meta = model.manifest["training"]
    assert meta["purged_or_missing_label_rows"] == 3 * 21
    assert meta["validation_used_for_fit"] is False and meta["final_oos_used_for_fit"] is False
    assert meta["early_stopping"] is False and model.booster.feature_name() == list(ml.FEATURE_NAMES)


def test_future_values_cannot_change_model_bytes_or_training_rows(fitted, tmp_path):
    model, artifact, universe, cfg, protocol, _ = fitted
    changed = deepcopy(artifact)
    future = changed.prices.date.gt(pd.Timestamp(cfg.train_end))
    changed.prices.loc[future, ["adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close"]] *= 5
    changed.prices.loc[future, "raw_volume"] *= 3
    other = ml.fit_frozen_model(changed, universe, protocol, tmp_path / "future-shocked")
    assert model.model_sha256 == other.model_sha256
    assert (model.path / "model.txt").read_bytes() == (other.path / "model.txt").read_bytes()
    assert (model.path / "training.csv").read_bytes() == (other.path / "training.csv").read_bytes()


def test_saved_native_model_and_predictions_reproduce_and_ignore_future_shocks(fitted):
    model, artifact, _, _, _, _ = fitted
    loaded, shock = ml.load_frozen_model(model.path), artifact.calendar[350]
    before, changed = loaded.predict(artifact), deepcopy(artifact)
    changed.prices.loc[changed.prices.date.ge(shock), ["adjusted_open", "adjusted_high", "adjusted_low", "adjusted_close"]] *= 4
    after = loaded.predict(changed)
    pd.testing.assert_frame_equal(before.loc[before.signal_date.lt(shock)], after.loc[after.signal_date.lt(shock)])
    saved = pd.read_csv(model.path / "predictions.csv", parse_dates=["signal_date"], float_precision="round_trip")
    np.testing.assert_allclose(saved.score, before.score, rtol=1e-14, equal_nan=True)
    assert before.loc[before["split"].eq("train"), "training_period_in_sample"].all()
    assert not before.loc[before["split"].eq("final_oos"), "training_period_in_sample"].any()


def test_future_snapshot_can_extend_beyond_original_dates_with_recent_warmup(fitted):
    model, _, _, _, _, _ = fitted
    future, _, _ = fixture("2026-01-02", periods=140)
    day = future.calendar[-1]
    scores_ = model.predict(future, signal_dates=[day])
    assert len(scores_) == 3 and scores_.signal_date.eq(day).all()
    assert np.isfinite(scores_.score).all() and scores_["split"].eq("outside_evaluation").all()
    assert set(model.target_weights(scores_, day, day + pd.Timedelta(days=1), POOL)[0]).issubset(POOL)


@pytest.mark.parametrize("field,value", [("provider", {"name": "different"}), ("request", {"adjustment": "qfq"})])
def test_prediction_rejects_silent_source_or_adjustment_switch(fitted, field, value):
    model, artifact, _, _, _, _ = fitted
    changed = deepcopy(artifact)
    changed.manifest[field] = value
    with pytest.raises(ValueError, match="differs"):
        model.predict(changed)


def scores(model, day, values):
    return pd.DataFrame({"signal_date": [day] * 3, "instrument": POOL, "score": values, "model_sha256": model.model_sha256})


def test_top1_positive_only_cash_and_stable_tie_break(fitted):
    model, artifact, _, _, _, _ = fitted
    signal, execution = artifact.calendar[320:322]
    weights, rows = model.target_weights(scores(model, signal, [.1, .1, -.2]), signal, execution, POOL, .02)
    assert weights == {POOL[0]: .98} and sum(row["selected"] for row in rows) == 1
    for values in [[0, 0, 0], [-.1, -.2, -.3]]:
        assert model.target_weights(scores(model, signal, values), signal, execution, POOL)[0] == {}


def test_missing_predictions_cannot_be_hidden_as_cash_and_wrong_model_is_rejected(fitted):
    model, artifact, _, _, _, _ = fitted
    signal, execution = artifact.calendar[320:322]
    frame = scores(model, signal, [.1, .2, np.nan])
    with pytest.raises(ValueError, match="Missing model features"):
        model.target_weights(frame, signal, execution, POOL)
    with pytest.raises(ValueError, match="Missing or duplicate"):
        model.target_weights(frame.iloc[:2], signal, execution, POOL)
    frame.score = .1
    frame.loc[0, "model_sha256"] = "not-frozen"
    with pytest.raises(ValueError, match="model identity"):
        model.target_weights(frame, signal, execution, POOL)
    with pytest.raises(ValueError, match="strictly precede"):
        model.target_weights(frame, execution, execution, POOL)


def test_callback_reuses_identical_cost_and_overnight_accounting(fitted):
    model, artifact, universe, cfg, _, _ = fitted
    days = artifact.calendar[321:342]
    predictions = pd.concat([scores(model, day, [.1, -.1, -.1]) for day in artifact.calendar[320:342]], ignore_index=True)
    callback = lambda signal, execution, members: model.target_weights(predictions, signal, execution, members)
    actual, _, trades, _ = wf._simulate(days, artifact.adjusted_open, artifact.adjusted_close, artifact.calendar,
        universe, cfg, "lightgbm", {}, target_callback=callback)
    reference, _, reference_trades, _ = wf._simulate(days, artifact.adjusted_open, artifact.adjusted_close,
        artifact.calendar, universe, cfg, "buy_hold", {})
    np.testing.assert_allclose(actual.nav, reference.nav, atol=1e-12)
    np.testing.assert_allclose(actual.net_return, reference.net_return, atol=1e-12)
    assert actual.iloc[0].fee_cost + actual.iloc[0].slippage_cost > 0
    assert len(trades) == len(reference_trades) == 1


def test_load_and_predict_recheck_model_file_feature_order_and_code(fitted, monkeypatch):
    model, artifact, _, _, _, _ = fitted
    with monkeypatch.context() as patch:
        patch.setattr(ml, "_code_hash", lambda: "0" * 64)
        with pytest.raises(ValueError, match="implementation changed"):
            ml.load_frozen_model(model.path)
    original = model.booster
    model.booster = SimpleNamespace(feature_name=lambda: list(reversed(ml.FEATURE_NAMES)))
    with pytest.raises(ValueError, match="feature order"):
        model.predict(artifact)
    model.booster = original
    file = model.path / "model.txt"
    file.write_bytes(file.read_bytes() + b"\nmodified\n")
    with pytest.raises(ValueError, match="checksum mismatch: model.txt"):
        model.predict(artifact)
    with pytest.raises(ValueError, match="checksum mismatch: model.txt"):
        ml.load_frozen_model(model.path)


def test_existing_model_output_never_overwrites_or_retrains(fitted):
    model, artifact, universe, _, protocol, _ = fitted
    checksum = hashlib.sha256((model.path / "manifest.json").read_bytes()).hexdigest()
    with pytest.raises(FileExistsError):
        ml.fit_frozen_model(artifact, universe, protocol, model.path)
    assert hashlib.sha256((model.path / "manifest.json").read_bytes()).hexdigest() == checksum


def test_frozen_feature_schema_manifest_and_exact_dependencies_are_enforced(fitted, monkeypatch):
    model, _, _, _, _, _ = fitted
    lgb = ml._lgbm()
    with monkeypatch.context() as patch:
        patch.setattr(lgb, "__version__", "4.7.99")
        with pytest.raises(ValueError, match="exact lightgbm version"):
            ml.load_frozen_model(model.path)
    path = model.path / "manifest.json"
    payload = json.loads(path.read_text())
    payload["feature_names"] = list(reversed(payload["feature_names"]))
    payload["manifest_sha256"] = ml._sha(ml._bytes({key: value for key, value in payload.items() if key != "manifest_sha256"}))
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="feature schema/order"):
        ml.load_frozen_model(model.path)


def test_predict_validates_new_source_file_checksums(fitted, tmp_path):
    model, artifact, _, _, _, _ = fitted
    changed = deepcopy(artifact)
    changed.path = tmp_path / "source"
    changed.path.mkdir()
    file = changed.path / "prices.csv"
    file.write_text("complete source fixture")
    changed.manifest["files"] = {"prices.csv": {"sha256": hashlib.sha256(file.read_bytes()).hexdigest()}}
    file.write_text("tampered source fixture")
    with pytest.raises(ValueError, match="source snapshot checksum"):
        model.predict(changed)


def test_known_suspended_entry_or_exit_and_missing_holding_mark_do_not_create_labels():
    artifact, _, cfg = fixture()
    day, entry, exit_ = artifact.calendar[[100, 101, 121]]
    for bad_day, field, value in [(entry, "tradestatus", "0"), (exit_, "tradestatus", "0"),
                                  (artifact.calendar[110], "adjusted_close", np.nan)]:
        changed = deepcopy(artifact)
        changed.prices.loc[changed.prices.date.eq(bad_day) & changed.prices.instrument.eq(POOL[0]), field] = value
        labels = ml._labels(changed, POOL, ml.FrozenModelSpec(), cfg)
        assert pd.isna(labels.loc[labels.signal_date.eq(day) & labels.instrument.eq(POOL[0]), "label"].iloc[0])
