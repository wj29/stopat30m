"""Split leakage/native training/accounting fixtures; never real performance."""
from dataclasses import replace
import hashlib
import json
import tarfile

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_experiment as experiment
from stopat30m.factors import handler as legacy
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm, load_fixed_qlib_model

A, B = "SH600000", "SZ000001"


def test_future_ref_label_horizon_is_derived_and_unknown_syntax_is_not_silently_unpurged():
    assert legacy.label_lookahead_sessions(["Ref($close,-5)/$close-1"]) == 5
    assert legacy.label_lookahead_sessions(["Ref($open,-6)/Ref($open,-1)-1"]) == 6
    assert legacy.label_lookahead_sessions(["$close/Ref($close,5)-1"]) == 0
    with pytest.raises(ValueError, match="explicit"):
        legacy.label_lookahead_sessions(["Ref(Mean($close,5),-5)"])


def test_label_purge_uses_real_sessions_and_preserves_latest_unlabelled_inference_dates():
    calendar = pd.bdate_range("2024-01-01", periods=45).delete(7)  # Explicit synthetic holiday.
    segments = {"train": (str(calendar[0].date()), str(calendar[19].date())),
        "valid": (str(calendar[20].date()), str(calendar[32].date())),
        "test": (str(calendar[33].date()), str(calendar[-1].date()))}
    actual, audit = legacy.purge_label_segments(segments, calendar, 5)
    assert actual["train"][1] == str(calendar[14].date())
    assert actual["valid"][1] == str(calendar[27].date())
    assert actual["test"] == segments["test"]
    assert audit["test"]["purged_label_sessions"] == 0
    full, _ = legacy.purge_label_segments(segments, calendar, 6, names=("train", "valid", "test"))
    for name, bounds in full.items():
        exit_index = calendar.get_loc(pd.Timestamp(bounds[1])) + 6
        assert calendar[exit_index] <= pd.Timestamp(segments[name][1])


def test_split_overlap_insufficient_history_and_understated_horizon_are_rejected(monkeypatch):
    calendar = pd.bdate_range("2024-01-01", periods=20)
    with pytest.raises(ValueError, match="overlap"):
        legacy.purge_label_segments({"train": ("2024-01-01", "2024-01-15"), "valid": ("2024-01-15", "2024-01-26")}, calendar, 1)
    with pytest.raises(ValueError, match="insufficient"):
        legacy.purge_label_segments({"train": ("2024-01-01", "2024-01-03")}, calendar, 5)
    monkeypatch.setattr(legacy, "get", lambda section, *args: {"label": "Ref($close,-5)/$close-1"} if section == "factors" else {})
    with pytest.raises(ValueError, match="understate"):
        legacy.AlphaExtendedHandler(groups=[], label_horizon_sessions=1)


class NativeDataset:
    def __init__(self, shock=1):
        self.calls = []
        rng = np.random.default_rng(72)
        index = pd.MultiIndex.from_product([pd.bdate_range("2024-01-01", periods=80), [A, B]], names=["datetime", "instrument"])
        names = [f"feature_{i}" for i in range(158)]
        self.train = pd.DataFrame(rng.normal(size=(len(index), 158)), index=index, columns=names)
        self.label = self.train.iloc[:, 0] * .02 + self.train.iloc[:, 1] * .01
        self.test = self.train.iloc[-20:].copy() * shock

    def prepare(self, segment, col_set=None, data_key=None):
        self.calls.append((segment, col_set, data_key))
        if segment == "train":
            return pd.concat({"feature": self.train, "label": self.label.to_frame("LABEL")}, axis=1)
        if segment == "test" and col_set == "feature":
            return self.test
        pytest.fail("Fixed native fitting must not request validation/test labels")


def test_fixed_native_training_never_reads_validation_or_oos_and_is_reproducible(tmp_path):
    datasets = [NativeDataset(1), NativeDataset(100)]
    models = [fit_fixed_qlib_lightgbm(ds, spec=FixedQlibLGBSpec(num_boost_round=8, min_data_in_leaf=10)) for ds in datasets]
    assert [ds.calls for ds in datasets] == [[("train", ["feature", "label"], "learn")]] * 2
    for model, name in zip(models, ("first", "second")):
        model.save_native(tmp_path / name, metadata={"snapshot": "synthetic-only"})
    assert (tmp_path / "first/model.txt").read_bytes() == (tmp_path / "second/model.txt").read_bytes()
    loaded = load_fixed_qlib_model(tmp_path / "first")
    pd.testing.assert_series_equal(loaded.predict(datasets[0]), models[0].predict(datasets[0]))
    assert loaded.training_rows == 160 and loaded.spec.params()["num_threads"] == 1
    with pytest.raises(FileExistsError):
        loaded.save_native(tmp_path / "first")
    (tmp_path / "first/model.txt").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        load_fixed_qlib_model(tmp_path / "first")


def test_native_schema_and_nonfinite_data_fail_before_fitting():
    ds = NativeDataset()
    ds.train = ds.train.iloc[:, :157]
    with pytest.raises(ValueError, match="schema/count"):
        fit_fixed_qlib_lightgbm(ds)
    ds = NativeDataset(); ds.train.iloc[0, 0] = np.inf
    with pytest.raises(ValueError, match="Infinite"):
        fit_fixed_qlib_lightgbm(ds)
    with pytest.raises(ValueError, match="regularization"):
        FixedQlibLGBSpec(learning_rate=float("nan"))


@pytest.fixture
def market(tmp_path):
    calendar = pd.bdate_range("2023-01-02", periods=120)
    prices = {field: pd.DataFrame(10.0 if field != "volume" else 100.0, index=calendar, columns=[A, B])
              for field in experiment.REQUIRED_FIELDS}
    prices["factor"][:] = 1
    path = tmp_path / "historical.txt"
    path.write_text(f"{A}\t2023-01-02\t2024-12-31\n{B}\t2023-01-02\t2024-12-31\n")
    pool = experiment.PackageHistoricalPool(path)
    cfg = experiment.AshareExperimentConfig(train_start=str(calendar[0].date()), train_end=str(calendar[20].date()),
        valid_start=str(calendar[21].date()), valid_end=str(calendar[50].date()),
        oos_start=str(calendar[61].date()), oos_end=str(calendar[70].date()), top_k=1)
    index = pd.MultiIndex.from_product([calendar, [A, B]], names=["datetime", "instrument"])
    scores = pd.Series([.02, .01] * len(calendar), index=index)
    return calendar, prices, pool, cfg, scores


def simulate(market, *, config=None, scores=None, strategy="lightgbm"):
    calendar, prices, pool, cfg, predictions = market
    cfg = config or cfg
    days = calendar[(calendar >= cfg.oos_start) & (calendar <= cfg.oos_end)]
    return experiment._simulate_ashare(days, calendar, prices, pool, cfg, strategy, scores if scores is not None else predictions)


def test_initial_execution_cost_and_non_rebalance_drift_are_not_daily_reset(market):
    calendar, prices, _, cfg, _ = market
    prices["close"].loc[calendar[62], A] = 11
    for field in ("open", "close", "high", "low", "vwap"):
        prices[field].loc[calendar[63]:, A] = 11
    curve, _, trades, _ = simulate(market)
    assert curve.iloc[0].nav == pytest.approx(1 / 1.00051)
    assert curve.loc[calendar[62], "nav"] == pytest.approx(1.1 / 1.00051)
    assert len(trades) == 1
    assert not curve.pending_unavailable_orders.any()
    assert trades.iloc[0].live_fill == False


def test_missing_new_target_stays_cash_and_is_executed_only_after_source_open_resumes(market):
    calendar, prices, _, _, _ = market
    for frame in prices.values(): frame.loc[calendar[61], A] = np.nan
    curve, positions, trades, _ = simulate(market)
    assert curve.iloc[0].nav == curve.iloc[0].cash == 1
    assert curve.iloc[0].pending_unavailable_orders
    assert trades.iloc[0].date == calendar[62]
    assert trades.iloc[0].signal_date == calendar[60]
    assert curve.iloc[1].nav == pytest.approx(1 / 1.00051)
    assert positions.loc[positions.date == calendar[62], "stale_sessions"].eq(0).all()


def test_whole_bar_missing_carries_only_held_valuation_with_stale_evidence(market):
    calendar, prices, _, _, _ = market
    for frame in prices.values(): frame.loc[calendar[62], A] = np.nan
    curve, positions, trades, _ = simulate(market)
    assert curve.loc[calendar[62], "nav"] == curve.loc[calendar[61], "nav"]
    held = positions.loc[(positions.date == calendar[62]) & (positions.instrument == A)].iloc[0]
    assert held.valuation_source == "carried_last_observed_close" and held.stale_sessions == 1
    assert not curve.loc[calendar[62], "valuation_complete"]
    assert trades.loc[trades.date == calendar[62]].empty
    assert curve.loc[calendar[63], "valuation_complete"]


@pytest.mark.parametrize("volume", [np.nan, 0.0])
def test_supplier_retained_factor_is_metadata_not_a_partial_tradable_quote(market, volume):
    calendar, prices, _, _, _ = market
    for field in ("open", "high", "low", "close", "vwap"):
        prices[field].loc[calendar[62], A] = np.nan
    prices["volume"].loc[calendar[62], A] = volume
    prices["factor"].loc[calendar[62], A] = 1.191578
    assert experiment._quote_state(prices, calendar[62], A) == "all_fields_unavailable"
    curve, positions, trades, _ = simulate(market)
    assert curve.loc[calendar[62], "nav"] == curve.loc[calendar[61], "nav"]
    assert not curve.loc[calendar[62], "valuation_complete"]
    assert trades.loc[trades.date == calendar[62]].empty
    assert positions.loc[positions.date == calendar[62], "valuation_source"].eq("carried_last_observed_close").all()


def test_suspended_exit_retains_position_and_defers_original_target_until_available(market):
    calendar, prices, _, _, original = market
    scores = original.copy()
    scores.loc[(calendar[65], A)], scores.loc[(calendar[65], B)] = -.01, .04
    for frame in prices.values(): frame.loc[calendar[66], A] = np.nan
    curve, positions, trades, _ = simulate(market, scores=scores)
    assert curve.loc[calendar[66], "pending_unavailable_orders"]
    assert positions.loc[positions.date == calendar[66], "instrument"].tolist() == [A]
    resumed = trades.loc[trades.date == calendar[67]]
    assert set(resumed.direction) == {"SELL", "BUY"}
    assert resumed.signal_date.eq(calendar[65]).all()
    assert positions.loc[positions.date == calendar[67], "instrument"].tolist() == [B]
    assert curve.loc[calendar[67], "fee_cost"] > 0 and not curve.loc[calendar[67], "pending_unavailable_orders"]


def test_partial_damage_and_prolonged_unknown_gaps_stop_instead_of_zeroing_holdings(market):
    calendar, prices, _, cfg, _ = market
    prices["close"].loc[calendar[62], A] = np.nan
    with pytest.raises(ValueError, match="Partial/corrupt held"):
        simulate(market)
    for frame in prices.values(): frame.loc[calendar[62]:calendar[83], A] = np.nan
    with pytest.raises(ValueError, match="20-session"):
        simulate(market, config=replace(cfg, oos_end=str(calendar[84].date())))


def action_book(tmp_path, calendar, *, confirmation_known_at=None):
    evidence = tmp_path / "issuer-announcement.txt"
    evidence.write_text("Explicit synthetic issuer evidence; not market data")
    day = lambda index: str(calendar[index].date())
    fact = lambda index: {"effective_date": day(index), "known_at": day(index - 1) + "T23:59:59+08:00", "evidence_ids": ["issuer"]}
    payload = {"schema_version": 1, "evidence": [{"id": "issuer", "path": evidence.name,
        "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(), "url": "https://example.com/synthetic-issuer"}],
        "actions": [{"id": "synthetic-share-exchange", "type": "share_exchange", "from_instrument": A,
            "to_instrument": B, "ratio": .62, "ratio_basis": "new_raw_shares_per_old_raw_share",
            "ratio_known_at": day(64) + "T23:59:59+08:00", "ratio_evidence_ids": ["issuer"],
            "suspension": fact(62), "delisting": fact(65),
            "scheduled": {"known_at": day(64) + "T23:59:59+08:00", "expected_tradable_from": day(87)},
            "confirmation": {"known_at": confirmation_known_at or day(89) + "T23:59:59+08:00",
                "tradable_from": day(91), "evidence_ids": ["issuer"]}}]}
    path = tmp_path / "actions.json"
    path.write_text(json.dumps(payload))
    return experiment.VerifiedCorporateActions(path)


def test_verified_conversion_uses_raw_share_ratio_and_two_factors_without_a_trade_or_refit(market, tmp_path):
    calendar, prices, pool, cfg, scores = market
    prices["factor"][A] = 2.0
    prices["factor"][B] = .5
    for field in ("open", "high", "low", "close", "vwap"):
        prices[field][B] = 5 / .62 * .5  # Same economic value after the raw share exchange.
        prices[field].loc[calendar[62]:, A] = np.nan
    prices["volume"].loc[calendar[62]:, A] = 0
    cfg = replace(cfg, oos_end=str(calendar[95].date()))
    book = action_book(tmp_path, calendar)
    curve, positions, trades, signals = experiment._simulate_ashare(calendar[61:96], calendar, prices, pool, cfg, "lightgbm", scores, book)
    ledger = curve.attrs["corporate_actions"]
    assert len(ledger) == 1 and ledger[0]["date"] == calendar[91]
    assert ledger[0]["new_virtual_units"] == pytest.approx(ledger[0]["old_virtual_units"] * 2 * .62 / .5)
    assert ledger[0]["old_raw_shares"] == pytest.approx(ledger[0]["old_virtual_units"] * 2)
    assert ledger[0]["fee_cost"] == ledger[0]["slippage_cost"] == 0
    assert curve.loc[calendar[90], "nav"] == pytest.approx(curve.loc[calendar[91], "nav"])
    before_admission = positions[(positions.date >= calendar[62]) & (positions.date < calendar[91])]
    assert before_admission.instrument.eq(A).all()
    assert before_admission.verified_suspended_entitlement.all()
    assert before_admission.stale_sessions.max() > 20
    assert trades[(trades.date >= calendar[62]) & (trades.date <= calendar[91])].empty
    assert signals[(signals.instrument == A) & (signals.signal_date >= calendar[65])].exclusion_reason.eq("verified_public_delisting").all()
    assert not signals[(signals.instrument == A) & (signals.signal_date >= calendar[65])].selected.any()
    assert curve.iloc[-1].nav == pytest.approx(curve.iloc[0].nav)


def test_scheduled_conversion_and_after_close_confirmation_cannot_be_previewed(market, tmp_path):
    calendar, prices, pool, cfg, scores = market
    for frame in prices.values():
        frame.loc[calendar[62]:, A] = np.nan
    book = action_book(tmp_path, calendar, confirmation_known_at=str(calendar[91].date()) + "T23:59:59+08:00")
    cfg = replace(cfg, oos_end=str(calendar[94].date()))
    curve, positions, _, _ = experiment._simulate_ashare(calendar[61:95], calendar, prices, pool, cfg, "lightgbm", scores, book)
    assert curve.attrs["corporate_actions"][0]["date"] == calendar[92]
    assert positions.loc[positions.date == calendar[91], "instrument"].tolist() == [A]
    assert positions.loc[positions.date == calendar[87], "instrument"].tolist() == [A]  # Scheduled date is not confirmation.
    assert not book.excluded(A, calendar[64], close=True)
    assert book.excluded(A, calendar[65], close=True)


def test_share_exchange_evidence_and_confirmation_quote_fail_closed(market, tmp_path):
    calendar, prices, pool, cfg, scores = market
    book = action_book(tmp_path, calendar)
    for frame in prices.values():
        frame.loc[calendar[62]:, A] = np.nan
        frame.loc[calendar[91], B] = np.nan
    cfg = replace(cfg, oos_end=str(calendar[94].date()))
    with pytest.raises(ValueError, match="admission has no usable"):
        experiment._simulate_ashare(calendar[61:95], calendar, prices, pool, cfg, "lightgbm", scores, book)
    (tmp_path / "issuer-announcement.txt").write_text("changed")
    with pytest.raises(ValueError, match="checksum"):
        book.assert_unchanged()
    with pytest.raises(ValueError, match="checksum"):
        experiment.VerifiedCorporateActions(book.path)


def test_future_suspension_evidence_does_not_waive_an_earlier_unknown_gap(market, tmp_path):
    calendar, prices, pool, cfg, scores = market
    book = action_book(tmp_path, calendar)
    book.payload["actions"][0]["suspension"]["known_at"] = str(calendar[90].date()) + "T23:59:59+08:00"
    book.path.write_text(json.dumps(book.payload))
    book = experiment.VerifiedCorporateActions(book.path)
    for frame in prices.values():
        frame.loc[calendar[62]:, A] = np.nan
    cfg = replace(cfg, oos_end=str(calendar[84].date()))
    with pytest.raises(ValueError, match="20-session"):
        experiment._simulate_ashare(calendar[61:85], calendar, prices, pool, cfg, "lightgbm", scores, book)


def test_observed_bar_cannot_override_a_verified_continuous_suspension(market, tmp_path):
    calendar, prices, pool, cfg, scores = market
    book = action_book(tmp_path, calendar)
    with pytest.raises(ValueError, match="conflicts with verified"):
        experiment._simulate_ashare(calendar[61:71], calendar, prices, pool, cfg, "lightgbm", scores, book)


def test_web_extract_hash_is_never_described_as_original_pdf_byte_verification(market, tmp_path):
    book = action_book(tmp_path, market[0])
    book.payload["evidence"][0].update(evidence_kind="official_pdf_text_extract_via_web", original_pdf_sha256=None, pages=[2, 3])
    book.path.write_text(json.dumps(book.payload))
    description = experiment.VerifiedCorporateActions(book.path).describe()
    assert "not original PDF bytes" in description["evidence_limitation"]
    assert description["evidence"][0]["original_pdf_sha256"] is None
    assert description["evidence"][0]["pages"] == [2, 3]


def test_historical_pool_uses_effective_ranges_without_backdating_current_members(tmp_path):
    path = tmp_path / "pool.txt"
    path.write_text(f"{A}\t2010-01-01\t2020-12-31\n{B}\t2021-01-01\t2026-12-31\n")
    pool = experiment.PackageHistoricalPool(path)
    assert pool.members_on("2019-01-01") == [A]
    assert pool.members_on("2025-01-01") == [B]
    assert pool.members_on("2021-01-01", known_on="2020-12-31") == [A]
    assert pool.qlib_intervals("2019-01-01", "2019-12-31").keys() == {A}


def test_expired_nonnumeric_package_alias_is_time_clipped_not_guessed_or_backdated(tmp_path):
    path = tmp_path / "pool.txt"
    path.write_text(f"SHT00018\t2005-04-08\t2006-12-31\n{A}\t2010-01-01\t2026-12-31\n")
    pool = experiment.PackageHistoricalPool(path, start="2012-01-01", end="2026-10-16")
    assert pool.members_on("2019-01-01") == [A]
    assert pool.clipped_non_numeric_intervals[0]["instrument"] == "SHT00018"
    with pytest.raises(ValueError, match="alias intersects"):
        experiment.PackageHistoricalPool(path, start="2005-01-01", end="2026-10-16")


def test_official_alpha158_purged_dataset_and_native_training_integrate(tmp_path):
    qlib = pytest.importorskip("qlib", reason="Root installs PyQlib; this test must run before actual acceptance")
    from qlib.contrib.data.handler import Alpha158
    from qlib.data.dataset import DatasetH
    from stopat30m.data.fetcher.qlib_dumper import QlibDumper
    dates = pd.bdate_range("2020-01-02", periods=170)
    root = tmp_path / "qlib"
    dumper = QlibDumper(root); dumper.set_calendar(dates.strftime("%Y-%m-%d").tolist())
    for n, instrument in enumerate([A, B]):
        p = 10 + np.arange(len(dates)) * .01 + .03 * np.sin(np.arange(len(dates)) / (n + 2))
        frame = pd.DataFrame({"date": dates.strftime("%Y-%m-%d"), "open": p, "close": p * 1.001,
            "high": p * 1.003, "low": p * .997, "volume": 10000 + np.arange(len(dates)) * 10,
            "change": .001, "factor": 1})
        dumper.dump_stock(instrument, frame)
        # The legacy dumper deliberately has no VWAP field: write the fixture's
        # observed synthetic VWAP explicitly, rather than claiming OHLC average.
        data = np.concatenate([[0], p * 1.0005]).astype("<f4")
        (root / "features" / instrument.lower() / "vwap.day.bin").write_bytes(data.tobytes())
    dumper.finalize()
    qlib.init(provider_uri=str(root), region="cn", kernels=1, expression_cache=None, dataset_cache=None)
    segments, _ = legacy.purge_label_segments({"train": (str(dates[65].date()), str(dates[120].date())),
        "valid": (str(dates[121].date()), str(dates[140].date())), "test": (str(dates[141].date()), str(dates[-1].date()))}, dates, 6, names=("train", "valid", "test"))
    handler = Alpha158(instruments=[A, B], start_time=str(dates[0].date()), end_time=str(dates[-1].date()),
        fit_start_time=segments["train"][0], fit_end_time=segments["train"][1],
        label=["Ref($open,-6)/Ref($open,-1)-1"])
    dataset = DatasetH(handler=handler, segments=segments)
    model = fit_fixed_qlib_lightgbm(dataset, spec=FixedQlibLGBSpec(num_boost_round=5, min_data_in_leaf=10))
    assert len(model.feature_names) == 158
    predictions = model.predict(dataset)
    assert len(predictions) > 0 and np.isfinite(predictions).all()
    sliced = model.predict(dataset, segment=slice(dates[140], dates[-1]))
    assert len(sliced) >= len(predictions)


def test_version_bound_official_qlib_experiment_runs_native_model_and_three_cost_matched_baselines(tmp_path, monkeypatch):
    pytest.importorskip("qlib")
    from stopat30m.data.fetcher.qlib_dumper import QlibDumper
    from stopat30m.data.qlib_snapshot import prepare_qlib_snapshot, load_qlib_snapshot
    dates = pd.bdate_range("2020-01-02", periods=330)
    base = tmp_path / "synthetic-source"
    dumper = QlibDumper(base); dumper.set_calendar(dates.strftime("%Y-%m-%d").tolist())
    for n, inst in enumerate((A, B)):
        p = 10 + .005 * np.arange(len(dates)) + .02 * np.sin(np.arange(len(dates)) / (4 + n))
        frame = pd.DataFrame({"date": dates.strftime("%Y-%m-%d"), "open": p, "close": p * 1.001,
            "high": p * 1.004, "low": p * .996, "volume": 10000 + np.arange(len(dates)),
            "change": .001, "factor": 1.0})
        dumper.dump_stock(inst, frame)
        (base / "features" / inst.lower() / "vwap.day.bin").write_bytes(np.concatenate([[0], p * 1.0005]).astype("<f4").tobytes())
    dumper.finalize()
    (base / "instruments/csi300.txt").write_text("".join(f"{inst}\t{dates[0].date()}\t{dates[-1].date()}\n" for inst in (A, B)))
    archive = tmp_path / "synthetic.tar.gz"
    with tarfile.open(archive, "w:gz") as stream:
        stream.add(base, arcname="cn_data")
    snapshot = prepare_qlib_snapshot(archive, tmp_path / "snapshots", source={"kind": "community", "fixture_only": True},
        expected_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), max_extract_bytes=10 * 2 ** 20)
    cfg = experiment.AshareExperimentConfig(train_start=str(dates[70].date()), train_end=str(dates[220].date()),
        valid_start=str(dates[221].date()), valid_end=str(dates[270].date()),
        oos_start=str(dates[271].date()), oos_end=str(dates[-1].date()), top_k=1)
    result = experiment.run_ashare_experiment(snapshot.dataset_dir, snapshot.path / "manifest.json",
        tmp_path / "experiment", config=cfg)
    report = result["report"]
    assert report["feature_count"] == 158 and report["training_rows"] > 200
    assert report["membership_status"] == "package_declared_membership_not_full_PIT"
    assert report["comparison_complete"]
    assert set(report["metrics"]) == {"lightgbm", "momentum", "equal_weight"}
    for metric in report["metrics"].values():
        assert metric["observation_days"] == len(dates[271:])
        assert metric["fees_in_initial_nav_units"] > 0 and metric["slippage_in_initial_nav_units"] > 0
    assert load_qlib_snapshot(snapshot.path).manifest_sha256 == snapshot.manifest_sha256
    model = load_fixed_qlib_model(result["model_path"])
    assert model.metadata["snapshot_manifest_sha256"] == snapshot.manifest_sha256
    with pytest.raises(FileExistsError):
        experiment.run_ashare_experiment(snapshot.dataset_dir, snapshot.path / "manifest.json",
            tmp_path / "experiment", config=cfg)
    before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in (tmp_path / "experiment").rglob("*") if path.is_file()}
    def no_refit(*args, **kwargs):
        pytest.fail("Quote-policy recovery must reuse the original native model and scores")
    monkeypatch.setattr(experiment, "fit_fixed_qlib_lightgbm", no_refit)
    recovered = experiment.resume_ashare_comparison(tmp_path / "experiment", tmp_path / "comparison-recovered")
    assert recovered["manifest"]["no_refit"]
    assert recovered["report"]["comparison_complete"]
    assert recovered["report"]["metrics"] == result["report"]["metrics"]
    assert {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in (tmp_path / "experiment").rglob("*") if path.is_file()} == before
