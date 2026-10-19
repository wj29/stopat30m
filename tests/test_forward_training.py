"""Synthetic native-LightGBM forward refit tests; no performance claims/network."""
from dataclasses import replace
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m import forward_training as training
from stopat30m.backtest import model_comparison as ml
from stopat30m.backtest import walk_forward as wf
from stopat30m.data.historical_universe import HistoricalUniverse
from stopat30m.data.research_prices import download_research_prices

POOL = ["SH510300", "SH510500", "SZ159915"]
NOW = datetime(2026, 10, 20, 8, 0, tzinfo=timezone.utc)


class Provider:
    source, version, source_url = "synthetic_forward_refit", "1", "fixture:forward-refit"
    volume_unit, amount_unit = "shares", "CNY"
    calendar_source, calendar_source_url = "synthetic_explicit_flags", "fixture:calendar"

    def __init__(self, missing_last=False, future_shock=False):
        self.sessions = pd.bdate_range("2019-01-02", periods=440)  # Test flags only.
        self.missing_last, self.future_shock = missing_last, future_shock

    def instrument_info(self, instrument):
        return {"asset_type": "ETF"}

    def fetch_calendar(self, start, end):
        days = pd.date_range(start, end)
        return pd.DataFrame({"date": days.strftime("%Y-%m-%d"), "is_trading_day": days.isin(self.sessions).astype(int)})

    def fetch_raw(self, instrument, start, end):
        n = POOL.index(instrument)
        rows = []
        for i, day in enumerate(self.sessions):
            if not start <= str(day.date()) <= end or (self.missing_last and instrument == POOL[-1] and i == 439):
                continue
            opening = (10 + n * 5) * np.exp(.0004 * i + .01 * np.sin(i / (5 + n)))
            closing = opening * (1 + .002 * np.sin(i / 3))
            if self.future_shock and i == 439:
                opening *= 1.1; closing *= 1.1
            rows.append({"date": str(day.date()), "code": instrument[:2].lower() + "." + instrument[2:],
                "open": opening, "close": closing, "high": max(opening, closing) * 1.002,
                "low": min(opening, closing) * .998, "volume": 100000 + (i % 11) * 100,
                "amount": (100000 + (i % 11) * 100) * closing, "tradestatus": "1", "isST": "0"})
        return pd.DataFrame(rows)

    def fetch_adjusted(self, instrument, start, end, adjustment):
        frame = self.fetch_raw(instrument, start, end)
        frame.loc[:, ["open", "close", "high", "low"]] *= 2
        return frame


@pytest.fixture
def source(tmp_path):
    provider = Provider()
    dates = provider.sessions
    first, last = str(dates[0].date()), str(dates[-1].date())
    artifact = download_research_prices(POOL, first, last, tmp_path / "data", provider=provider)
    universe = HistoricalUniverse.fixed_pool("Original retrospective ETF pool", POOL, first,
        str(dates[300].date()), selection_date="2026-10-19")
    payload = universe.as_dict(); payload.pop("sha256")
    payload["metadata"] = {"research_data_request": {"source": provider.source,
        "start_date": first, "end_date": str(dates[300].date()), "adjustment": "hfq", "instruments": POOL}}
    universe = HistoricalUniverse.from_dict(payload)
    cfg = wf.ResearchConfig(train_start=str(dates[80].date()), train_end=str(dates[160].date()),
        validation_start=str(dates[161].date()), validation_end=str(dates[220].date()),
        oos_start=str(dates[221].date()), oos_end=str(dates[300].date()))
    base, reference = tmp_path / "base.json", tmp_path / "reference.json"
    wf.lock_protocol(cfg, base, universe=universe)
    ml.lock_model_protocol(base, reference, spec=ml.FrozenModelSpec(num_boost_round=8, min_data_in_leaf=20))
    return SimpleNamespace(artifact=artifact, universe=universe, reference=reference, cfg=cfg,
                           dates=dates, provider=provider, base=base)


@pytest.fixture
def candidate(source, tmp_path):
    return training.fit_forward_candidate(source.artifact, source.universe, source.reference,
                                           tmp_path / "candidate", now=NOW)


def test_latest_mature_refit_is_standard_native_bundle_and_cannot_claim_historical_oos(source, candidate):
    model = ml.load_frozen_model(candidate.path)
    manifest = model.manifest
    rows = pd.read_csv(model.path / "training.csv", parse_dates=["signal_date", "label_entry_date", "label_exit_date"])
    cutoff = source.dates[-1]
    assert manifest["training_role"] == "forward_only_no_historical_oos"
    assert manifest["training"]["declared_cutoff"] == str(cutoff.date())
    assert rows.label_exit_date.max() == cutoff
    assert rows.signal_date.max() == source.dates[-22]
    assert rows.signal_date.max() > pd.Timestamp(source.cfg.oos_end)
    assert (rows.label_entry_date > rows.signal_date).all() and (rows.label_exit_date <= cutoff).all()
    assert manifest["training"]["validation_used_for_fit"] is False
    assert manifest["training"]["final_oos_used_for_fit"] is False
    assert manifest["training"]["previous_historical_oos_reused_for_forward_fit"] is True
    assert manifest["training"]["purged_or_missing_label_rows"] == 3 * 21
    assert pd.read_csv(model.path / "predictions.csv").empty
    diagnostics = json.loads((model.path / "diagnostics.json").read_text())
    assert diagnostics["historical_oos_metrics_available"] is False
    assert diagnostics["final_oos"]["rows"] == diagnostics["validation"]["rows"] == 0
    assert "annual_return" not in json.dumps(diagnostics)
    assert model.booster.feature_name() == list(ml.FEATURE_NAMES)
    assert len(manifest["training_implementation_sha256"]) == 64
    assert manifest["training_input_manifest_sha256"] == source.artifact.manifest_sha256
    assert manifest["training_input_snapshot_sha256"] == source.artifact.manifest["artifact_id"]


def test_parameters_features_and_cost_rules_are_unchanged_and_future_slots_not_observed(source, candidate):
    reference = json.loads(source.reference.read_text())
    protocol = json.loads((candidate.path / "protocol.json").read_text())
    for key in ("model_spec", "train_params", "feature_version", "feature_names", "feature_implementation_sha256", "selection_rule", "label"):
        assert protocol[key] == reference[key]
    cfg = protocol["base_protocol"]["config"]
    assert cfg["validation_start"] == "2026-10-21" and cfg["oos_start"] == "2026-10-22"
    assert protocol["planned_future_not_observed"] is True
    assert protocol["compatibility_slots_are_exchange_sessions"] is False
    assert protocol["base_protocol"]["universe"]["selection_date"] == "2026-10-19"
    assert candidate.manifest["universe_audit"]["retrospective_selection"] is True
    for key in ("buy_fee_bps", "sell_fee_bps", "slippage_bps", "top_k", "rebalance_sessions"):
        assert cfg[key] == reference["base_protocol"]["config"][key]
    assert candidate.manifest["training"]["early_stopping"] is False
    assert candidate.manifest["training"]["automatic_parameter_search"] is False


def test_old_protocol_and_universe_are_preserved_and_new_artifact_never_overwrites(source, tmp_path):
    before = {path: path.read_bytes() for path in (source.base, source.reference)}
    old_universe = source.universe.as_dict()
    candidate = training.fit_forward_candidate(source.artifact, source.universe, source.reference,
                                                tmp_path / "candidate", now=NOW)
    saved = (candidate.path / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError, match="never overwrite"):
        training.fit_forward_candidate(source.artifact, source.universe, source.reference, candidate.path, now=NOW)
    assert (candidate.path / "manifest.json").read_bytes() == saved
    assert source.universe.as_dict() == old_universe
    assert {path: path.read_bytes() for path in before} == before
    new_pool = json.loads((candidate.path / "protocol.json").read_text())["base_protocol"]["universe"]
    assert new_pool["metadata"]["forward_training_origin_universe"] == old_universe
    assert new_pool["coverage_end"] > old_universe["coverage_end"]
    assert new_pool["metadata"]["research_data_request"]["end_date"] == str(source.dates[-1].date())


def test_identical_snapshot_and_parameters_reproduce_native_model_and_training_bytes(source, candidate, tmp_path):
    again = training.fit_forward_candidate(source.artifact.path, source.universe, source.reference,
                                            tmp_path / "repeated", now=NOW)
    assert again.model_sha256 == candidate.model_sha256
    for name in ("model.txt", "training.csv", "features.csv", "diagnostic_labels.csv"):
        assert (again.path / name).read_bytes() == (candidate.path / name).read_bytes()


def test_loaded_candidate_can_predict_and_select_future_targets_with_frozen_schema(source, candidate):
    scores = candidate.predict(source.artifact, signal_dates=[source.dates[-1]])
    assert set(scores.instrument) == set(POOL) and np.isfinite(scores.score).all()
    weights, rows = candidate.target_weights(scores, source.dates[-1], source.dates[-1] + pd.Timedelta(days=1), POOL)
    assert set(weights).issubset(POOL) and len(rows) == 3
    assert scores.training_period_in_sample.all()  # Historical scores stay explicitly in sample.


def test_common_cutoff_excludes_incomplete_tail_and_tail_shocks_cannot_leak_into_fit(source, tmp_path):
    outputs = []
    for tag, shock in (("normal", False), ("shocked", True)):
        provider = Provider(missing_last=True, future_shock=shock)
        artifact = download_research_prices(POOL, str(source.dates[0].date()), str(source.dates[-1].date()),
                                             tmp_path / tag, provider=provider)
        outputs.append(training.fit_forward_candidate(artifact, source.universe, source.reference,
                                                        tmp_path / (tag + "-model"), now=NOW))
    assert outputs[0].manifest["training"]["declared_cutoff"] == str(source.dates[-2].date())
    assert outputs[0].manifest["common_completed_coverage"]["excluded_calendar_sessions_after_cutoff"] == 1
    assert outputs[0].model_sha256 == outputs[1].model_sha256
    assert (outputs[0].path / "training.csv").read_bytes() == (outputs[1].path / "training.csv").read_bytes()


def test_partial_current_session_requires_postclose_source_and_publication_delay(source):
    artifact = SimpleNamespace(calendar=source.artifact.calendar, prices=source.artifact.prices)
    day = source.dates[-1].date().isoformat()
    for now, fetched in (("15:04", "15:00"), ("15:20", "14:59")):
        stamp = datetime.fromisoformat(f"{day}T{now}:00+08:00")
        source_time = datetime.fromisoformat(f"{day}T{fetched}:00+08:00")
        cutoff, _ = training._common_cutoff(artifact, POOL, stamp, source_time)
        assert cutoff == str(source.dates[-2].date())
    cutoff, _ = training._common_cutoff(artifact, POOL, datetime.fromisoformat(f"{day}T15:20:00+08:00"),
                                       datetime.fromisoformat(f"{day}T15:10:00+08:00"))
    assert cutoff == day


def test_corrupt_snapshot_or_changed_reference_fails_before_new_training(source, tmp_path):
    source.reference.write_text(source.reference.read_text().replace('"num_boost_round": 8', '"num_boost_round": 9'))
    with pytest.raises(ValueError, match="checksum"):
        training.fit_forward_candidate(source.artifact, source.universe, source.reference, tmp_path / "bad", now=NOW)
    assert not (tmp_path / "bad").exists()


def test_snapshot_checksum_is_verified_before_fitting(source, tmp_path):
    prices = source.artifact.path / "prices.csv"
    prices.write_bytes(prices.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="checksum"):
        training.fit_forward_candidate(source.artifact.path, source.universe, source.reference, tmp_path / "bad", now=NOW)
    assert not (tmp_path / "bad").exists()


@pytest.mark.parametrize("bad", ["different_source", "qfq"])
def test_unified_input_cannot_silently_switch_source_or_adjustment(source, tmp_path, bad):
    provider = Provider()
    if bad == "different_source":
        provider.source = bad
    artifact = download_research_prices(POOL, str(source.dates[0].date()), str(source.dates[-1].date()),
        tmp_path / "changed", provider=provider, adjustment="qfq" if bad == "qfq" else "hfq")
    with pytest.raises(ValueError, match="source/adjustment"):
        training.fit_forward_candidate(artifact, source.universe, source.reference, tmp_path / "bad", now=NOW)


def test_invalid_time_and_snapshot_receipt_are_rejected(source, tmp_path):
    with pytest.raises(ValueError, match="timezone-aware"):
        training.fit_forward_candidate(source.artifact, source.universe, source.reference, tmp_path / "bad", now=datetime(2026, 10, 20))
    wrong = replace(source.artifact, manifest_sha256="0" * 64)
    with pytest.raises(ValueError, match="verified immutable"):
        training.fit_forward_candidate(wrong, source.universe, source.reference, tmp_path / "bad", now=NOW)


def test_insufficient_mature_rows_do_not_create_a_model(source, tmp_path):
    with pytest.raises(ValueError, match="Insufficient mature"):
        training.fit_forward_candidate(source.artifact, source.universe, source.reference,
            tmp_path / "too-short", train_start=str(source.dates[-25].date()), now=NOW)
    assert not (tmp_path / "too-short").exists()


def test_refit_cannot_change_the_reference_pool(source, tmp_path):
    other = HistoricalUniverse.fixed_pool("Changed pool", POOL[:-1], source.universe.coverage_start,
        source.universe.coverage_end, selection_date="2026-10-19")
    with pytest.raises(ValueError, match="pool instruments differ"):
        training.fit_forward_candidate(source.artifact, other, source.reference, tmp_path / "other-pool", now=NOW)
    assert not (tmp_path / "other-pool").exists()
