"""Offline orchestration regressions; synthetic returns are not research results."""
from dataclasses import replace
from datetime import datetime
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import forward_runner, research_experiment
from stopat30m.backtest import model_comparison as ml
from stopat30m.backtest import walk_forward as wf
from stopat30m.data.historical_universe import HistoricalUniverse


POOL = ["SH510300", "SH510500"]


@pytest.fixture
def comparison_case(tmp_path, monkeypatch):
    # These business-day flags and deterministic marks are test fixtures only.
    days = pd.bdate_range("2024-01-02", periods=150)
    rows = []
    for instrument in [*POOL, "SH000300"]:
        for offset, day in enumerate(days):
            price = 10 * 1.001 ** offset
            rows.append({"date": day, "instrument": instrument,
                         "adjusted_open": price, "adjusted_close": price,
                         "tradestatus": "1"})
    frame = pd.DataFrame(rows)
    artifact = SimpleNamespace(
        calendar=days, prices=frame, path=tmp_path / "synthetic-prices", manifest_sha256=None,
        adjusted_open=frame.pivot(index="date", columns="instrument", values="adjusted_open"),
        adjusted_close=frame.pivot(index="date", columns="instrument", values="adjusted_close"),
        manifest={"artifact_id": "a" * 64, "provider": {"name": "synthetic_fixture"},
                  "request": {"start_date": str(days[0].date()), "end_date": str(days[-1].date()),
                              "instruments": [*POOL, "SH000300"], "adjustment": "hfq"}})
    universe = HistoricalUniverse.fixed_pool("Synthetic comparison fixture", POOL,
        str(days[0].date()), str(days[-1].date()), selection_date="2026-10-19")
    config = wf.ResearchConfig(train_start=str(days[80].date()), train_end=str(days[100].date()),
        validation_start=str(days[101].date()), validation_end=str(days[121].date()),
        oos_start=str(days[122].date()), oos_end=str(days[142].date()))
    base_path, model_path = tmp_path / "base.json", tmp_path / "model-protocol.json"
    base = wf.lock_protocol(config, base_path, universe=universe)
    ml.lock_model_protocol(base_path, model_path)
    calls = {"fit": 0, "predict": 0}

    class SyntheticModel:
        model_sha256 = "b" * 64

        def __init__(self, path, protocol):
            self.path = path
            self.manifest = {"base_protocol_sha256": protocol["base_protocol_sha256"],
                             "protocol_sha256": protocol["protocol_sha256"],
                             "source_manifest_sha256": None}

        def predict(self, source):
            assert source is artifact
            calls["predict"] += 1
            return pd.DataFrame([{"signal_date": day, "instrument": instrument,
                                  "score": .02 - number * .01, "model_sha256": self.model_sha256}
                                 for day in days for number, instrument in enumerate(POOL)])

        def target_weights(self, predictions, signal_day, execution_day, eligible, cash_reserve_pct=0):
            assert pd.Timestamp(signal_day) < pd.Timestamp(execution_day)
            current = predictions.loc[predictions.signal_date.eq(signal_day)
                                      & predictions.instrument.isin(eligible)]
            selected = current.sort_values(["score", "instrument"], ascending=[False, True]).iloc[0]
            return {selected.instrument: 1 - cash_reserve_pct}, current.to_dict("records")

    def fit(source, actual_universe, protocol_path, output):
        calls["fit"] += 1
        assert source is artifact and actual_universe is universe
        output.mkdir()
        (output / "model.txt").write_text("Synthetic frozen model; no SDK or network")
        return SyntheticModel(output, json.loads(protocol_path.read_text()))

    monkeypatch.setattr(ml, "fit_frozen_model", fit)
    return SimpleNamespace(artifact=artifact, universe=universe, cfg=config, base=base,
                           protocol=base_path, model_protocol=model_path,
                           calls=calls, output=tmp_path / "comparison")


def run_case(case, **kwargs):
    return research_experiment.run_comparison(case.artifact, case.universe,
        case.protocol, case.model_protocol, case.output, **kwargs)


def test_three_declared_cost_scenarios_fit_and_predict_once_and_charge_entry_once(comparison_case):
    case = comparison_case
    report = run_case(case)
    assert case.calls == {"fit": 1, "predict": 1}
    assert report["selection_performed"] is False
    assert report["cost_bps_per_side"] == [5, 10, 20]
    assert report["development_is_in_sample"] is True
    for side in research_experiment.COST_SCENARIOS:
        rows = pd.read_csv(case.output / f"cost_{int(side)}bps/returns.csv")
        for _, group in rows.groupby(["split", "strategy"], sort=False):
            first = group.iloc[0]
            expected_entry_nav = 1 / (1 + side / 10000)
            assert first.nav == pytest.approx(expected_entry_nav)
            assert first.net_return == pytest.approx(expected_entry_nav - 1)
            assert first.fee_cost + first.slippage_cost == pytest.approx(1 - expected_entry_nav)
            assert group.fee_cost.sum() + group.slippage_cost.sum() == pytest.approx(1 - expected_entry_nav)
            assert group.iloc[-1].nav == pytest.approx(1.001 ** (len(group) - 1) * expected_entry_nav)
        trades = pd.read_csv(case.output / f"cost_{int(side)}bps/trades.csv")
        assert (pd.to_datetime(trades.signal_date) < pd.to_datetime(trades.date)).all()
    manifest = json.loads((case.output / "experiment_manifest.json").read_text())
    for filename, expected in manifest["output_checksums"].items():
        assert hashlib.sha256((case.output / filename).read_bytes()).hexdigest() == expected


def test_model_bound_to_different_baseline_is_rejected_before_fit(comparison_case):
    case = comparison_case
    other_base = case.output.parent / "other-base.json"
    wf.lock_protocol(replace(case.cfg, buy_fee_bps=9), other_base, universe=case.universe)
    other_model = case.output.parent / "other-model.json"
    ml.lock_model_protocol(other_base, other_model)
    case.model_protocol = other_model
    with pytest.raises(ValueError, match="(?i)base|protocol|bound"):
        run_case(case)
    assert case.calls == {"fit": 0, "predict": 0}
    assert not case.output.exists()


def test_failed_model_fit_leaves_declared_plan_but_no_success_manifest(comparison_case, monkeypatch):
    def fail(*_args, **_kwargs):
        raise RuntimeError("Synthetic interrupted model fit")
    monkeypatch.setattr(ml, "fit_frozen_model", fail)
    case = comparison_case
    with pytest.raises(RuntimeError, match="interrupted"):
        run_case(case)
    assert (case.output / "comparison_plan.json").is_file()
    assert not (case.output / "experiment_manifest.json").exists()
    assert not (case.output / "comparison.json").exists()
    before = (case.output / "comparison_plan.json").read_bytes()
    with pytest.raises(FileExistsError):
        run_case(case)
    assert (case.output / "comparison_plan.json").read_bytes() == before


def retrieval_artifact():
    first, last = "2026-10-19T16:00:00+08:00", "2026-10-19T16:01:00+08:00"
    return SimpleNamespace(manifest={
        "provider": {"name": "tencent"}, "created_at": "2026-10-19T16:50:00+08:00",
        "download_provenance": {"generation": "c" * 32, "as_of_date": "2026-10-19",
            "first_response_at": first, "last_response_at": last,
            "responses": [{"fetched_at": first}, {"fetched_at": last}]}})


def completed_retrieval(artifact):
    return forward_runner._completed_retrieval(artifact, "2026-10-19",
                                               datetime.fromisoformat("2026-10-19T17:00:00+08:00"))


def test_completed_retrieval_uses_original_response_times_instead_of_new_snapshot_seal():
    artifact = retrieval_artifact()
    result = completed_retrieval(artifact)
    assert datetime.fromisoformat(result["fetched_at"]) == datetime.fromisoformat("2026-10-19T16:00:00+08:00")
    assert datetime.fromisoformat(result["first_response_at"]) == datetime.fromisoformat("2026-10-19T16:00:00+08:00")
    assert datetime.fromisoformat(result["last_response_at"]) == datetime.fromisoformat("2026-10-19T16:01:00+08:00")
    assert result["generation"] == "c" * 32


@pytest.mark.parametrize("case", ["intraday_cache", "future_response", "wrong_first", "wrong_last",
                                 "wrong_generation_day", "empty_responses", "missing_provenance"])
def test_real_source_requires_the_entire_original_collection_to_be_completed_postclose(case):
    artifact = retrieval_artifact()
    provenance = artifact.manifest["download_provenance"]
    if case == "intraday_cache":
        provenance["responses"][0]["fetched_at"] = "2026-10-19T14:59:00+08:00"
        provenance["first_response_at"] = provenance["responses"][0]["fetched_at"]
    elif case == "future_response":
        provenance["responses"][-1]["fetched_at"] = "2026-10-19T17:01:00+08:00"
        provenance["last_response_at"] = provenance["responses"][-1]["fetched_at"]
    elif case == "wrong_first":
        provenance["first_response_at"] = "2026-10-19T16:00:01+08:00"
    elif case == "wrong_last":
        provenance["last_response_at"] = "2026-10-19T16:01:01+08:00"
    elif case == "wrong_generation_day":
        provenance["as_of_date"] = "2026-10-16"
    elif case == "empty_responses":
        provenance["responses"] = []
    else:
        del artifact.manifest["download_provenance"]
    with pytest.raises(ValueError):
        completed_retrieval(artifact)
