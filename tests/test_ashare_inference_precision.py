"""Saved train processors must reproduce native Qlib numerical execution.

The genuine Qlib processor is fitted once on a small fixture. Inference uses
its already fitted state and may never fit on the frame being predicted.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

native_processors = pytest.importorskip("qlib.data.dataset.processor", reason="Native numerical acceptance requires requirements-qlib.txt")
Fillna, RobustZScoreNorm = native_processors.Fillna, native_processors.RobustZScoreNorm

from stopat30m import ashare_forward as af
from stopat30m.ashare_rolling_research import apply_frozen_processors


class RecordingBooster:
    def predict(self, frame, **kwargs):
        self.last_frame = frame.copy(deep=True)
        return frame.iloc[:, 0].to_numpy().astype(float)


@pytest.fixture(params=[np.float32, np.float64], ids=["float32", "float64"])
def fitted_native_processor(request, monkeypatch):
    dtype = request.param
    rng = np.random.default_rng(81)
    days = pd.date_range("2023-01-03", periods=6)
    index = pd.MultiIndex.from_product([days, ["SH600000", "SH600001", "SH600002"]], names=["datetime", "instrument"])
    names = [f"feature{number}" for number in range(158)]
    train = pd.DataFrame(rng.normal(size=(len(index), 158)).astype(dtype), index=index, columns=names)
    normalizer = RobustZScoreNorm(str(days.min().date()), str(days.max().date()), fields_group="feature", clip_outlier=True)
    normalizer.fit(pd.concat({"feature": train}, axis=1))
    handler = SimpleNamespace(infer_processors=[normalizer, Fillna(fields_group="feature")])
    # JSON is the actual bundle representation: its lists have no numpy dtype.
    payload = json.loads(json.dumps(af._processor_payload(handler, names)))
    raw = pd.DataFrame(rng.normal(size=(len(index), 158)).astype(dtype), index=index, columns=names)
    raw.iloc[0, 0] = np.nan
    raw.iloc[1, 1] = 10000
    untouched = raw.copy(deep=True)
    native = Fillna(fields_group="feature")(normalizer(pd.concat({"feature": raw.copy(deep=True)}, axis=1)))["feature"]
    def no_fit(*args, **kwargs):
        pytest.fail("Prediction must apply already fitted train statistics; fit() was called")
    monkeypatch.setattr(RobustZScoreNorm, "fit", no_fit)
    booster = RecordingBooster()
    model = SimpleNamespace(feature_names=names, booster=booster)
    return raw, untouched, native, payload, model


def test_rolling_saved_processor_matches_native_values_dtype_and_order(fitted_native_processor):
    raw, untouched, native, payload, model = fitted_native_processor
    result = apply_frozen_processors(raw, model, payload["processors"])
    pd.testing.assert_frame_equal(model.booster.last_frame, native, check_exact=True)
    np.testing.assert_array_equal(result.to_numpy(), native.iloc[:, 0].to_numpy().astype(float))
    pd.testing.assert_frame_equal(raw, untouched, check_exact=True)


def test_forward_saved_processor_matches_native_values_dtype_and_order(fitted_native_processor, monkeypatch):
    raw, untouched, native, payload, model = fitted_native_processor
    manifest = {"feature_names": model.feature_names, "model_sha256": "synthetic-native-processor-fixture"}
    bundle = af.FrozenAshareCandidate(Path("unused-readonly-fixture"), manifest, {}, payload, model)
    monkeypatch.setattr(af, "load_ashare_forward_candidate", lambda path: bundle)
    scores = bundle.predict_features(raw)
    pd.testing.assert_frame_equal(model.booster.last_frame, native, check_exact=True)
    np.testing.assert_array_equal(scores.to_numpy(), native.iloc[:, 0].to_numpy().astype(float))
    pd.testing.assert_frame_equal(raw, untouched, check_exact=True)


def test_promoting_float32_json_statistics_changes_native_rounding(fitted_native_processor):
    raw, _, native, payload, _ = fitted_native_processor
    if raw.to_numpy().dtype != np.dtype("float32"):
        pytest.skip("This regression specifically demonstrates float32 promotion")
    state = payload["processors"][0]
    promoted = ((raw - np.asarray(state["mean_train"])) / np.asarray(state["std_train"])).clip(-3, 3).fillna(0)
    assert set(promoted.dtypes) == {np.dtype("float64")}
    assert set(native.dtypes) == {np.dtype("float32")}
    assert np.any(promoted.to_numpy() != native.to_numpy())
