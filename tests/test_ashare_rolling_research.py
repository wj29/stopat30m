"""Chronology and saved preprocessing for the one declared rolling candidate."""
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m.ashare_rolling_research import (
    annual_fold_plan, apply_frozen_processors, validate_training_rows,
)


def test_annual_folds_use_actual_prior_sessions_and_six_session_label_purge():
    # Explicit synthetic source sessions exercise arithmetic, not a real feed.
    days = pd.bdate_range("2017-01-01", "2026-10-16")
    days = days[~days.isin(pd.to_datetime(["2023-12-29", "2024-12-31"]))]
    folds = annual_fold_plan(days, [2023, 2024, 2025, 2026], evaluation_end="2026-10-16")
    assert [fold["train_start"] for fold in folds] == ["2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01"]
    assert folds[1]["data_cutoff"] == "2023-12-28"
    assert folds[2]["signal_start"] == "2024-12-30"
    assert folds[-1]["signal_end"] == "2026-10-16"
    for fold in folds:
        assert days.get_loc(pd.Timestamp(fold["data_cutoff"])) - days.get_loc(pd.Timestamp(fold["train_signal_end"])) == 6
    # The same final close intentionally appears in adjacent execution folds.
    assert folds[0]["signal_end"] == folds[1]["signal_start"]


def test_undeclared_folds_and_fake_cutoff_cannot_start_a_search():
    days = pd.bdate_range("2017-01-01", "2026-10-16")
    with pytest.raises(ValueError, match="four predeclared"):
        annual_fold_plan(days, [2024, 2025, 2026], evaluation_end="2026-10-16")
    with pytest.raises(ValueError, match="observed"):
        annual_fold_plan(days, [2023, 2024, 2025, 2026], evaluation_end="2026-10-17")


def test_saved_normalizer_clips_then_fills_without_learning_from_future():
    names = [f"x{i}" for i in range(158)]
    index = pd.MultiIndex.from_product([[pd.Timestamp("2023-01-03")], ["SH600000"]], names=["datetime", "instrument"])
    features = pd.DataFrame(np.ones((1, 158)) * 6, index=index, columns=names)
    features.iloc[0, 0], features.iloc[0, 1] = np.nan, -100
    seen = []
    class Booster:
        def predict(self, values, num_threads):
            seen.append(values.copy())
            return values.sum(axis=1).to_numpy()
    model = SimpleNamespace(feature_names=names, booster=Booster())
    processors = [{"class": "RobustZScoreNorm", "clip_outlier": True, "mean_train": [2.] * 158, "std_train": [2.] * 158}, {"class": "Fillna"}]
    original = apply_frozen_processors(features, model, processors)
    assert seen[-1].iloc[0, 0] == 0 and seen[-1].iloc[0, 1] == -3 and seen[-1].iloc[0, 2] == 2
    future = features.copy()
    future.index = pd.MultiIndex.from_product([[pd.Timestamp("2027-01-04")], ["SH600000"]], names=index.names)
    future *= 10000
    pd.testing.assert_series_equal(apply_frozen_processors(pd.concat([features, future]), model, processors).loc[features.index], original)
    with pytest.raises(ValueError, match="feature order"):
        apply_frozen_processors(features.iloc[:, ::-1], model, processors)


def test_immature_learning_rows_never_reach_fit():
    days = pd.bdate_range("2022-11-01", "2023-01-04")
    cutoff = pd.Timestamp("2022-12-30")
    mature = days[days.get_loc(cutoff) - 6]
    fold = dict(train_start="2018-01-01", data_cutoff="2022-12-30", train_signal_end=mature.date().isoformat())
    index = pd.MultiIndex.from_product([[mature], ["SH600000", "SH600001"]], names=["datetime", "instrument"])
    learning = pd.DataFrame({"label": [1., 2.]}, index=index)
    audit = validate_training_rows(learning, fold, days)
    assert audit["last_label_execution_end"] == "2022-12-30"
    learning.index = pd.MultiIndex.from_product([[days[days.get_loc(mature) + 1]], ["SH600000", "SH600001"]], names=index.names)
    with pytest.raises(ValueError, match="Immature"):
        validate_training_rows(learning, fold, days)


def test_saved_preprocessing_matches_official_float32_intermediate_rounding():
    RobustZScoreNorm = pytest.importorskip("qlib.data.dataset.processor").RobustZScoreNorm
    rng = np.random.default_rng(19)
    names = [f"x{i}" for i in range(158)]
    features = pd.DataFrame(rng.normal(size=(12, 158)).astype(np.float32), columns=names)
    mean = rng.normal(size=158).astype(np.float32)
    std = (rng.random(158) + .1).astype(np.float32)
    official = RobustZScoreNorm("2022-01-01", "2022-12-22", clip_outlier=True)
    official.cols, official.mean_train, official.std_train = names, mean, std
    expected = official(features.copy()).fillna(0)
    captured = []
    class Booster:
        def predict(self, values, num_threads):
            captured.append(values.copy())
            return np.zeros(len(values))
    model = SimpleNamespace(feature_names=names, booster=Booster())
    apply_frozen_processors(features, model, [{"class":"RobustZScoreNorm", "mean_train":mean.tolist(), "std_train":std.tolist(), "clip_outlier":True}, {"class":"Fillna"}])
    pd.testing.assert_frame_equal(captured[0], expected, check_exact=True)
    assert all(dtype == np.float32 for dtype in captured[0].dtypes)
