"""Dated union membership, label maturity and train-only native preprocessing."""
from dataclasses import asdict
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m.ashare_union_research import (
    MARKET_CHECKS, ObservedTrainingEligibility, _load_protocol, _train_dataset,
    assert_market_originals_unchanged, dated_membership_mask, load_eligibility,
    implementation_sha256, union_fold_plan, union_intervals, validate_market_audit,
)
from stopat30m.ashare_forward import LABEL, _processor_payload
from stopat30m.ashare_rolling_research import apply_frozen_processors
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm


def _index(days, symbols=("SH600000",)):
    return pd.MultiIndex.from_product([pd.to_datetime(days), symbols], names=["datetime", "instrument"])


def _eligibility(days, *, missing=(), ipo="2010-01-01", out=""):
    quotes = np.ones(len(days), bool)
    quotes[list(missing)] = False
    return ObservedTrainingEligibility(days, {"SH600000": quotes},
        {"SH600000": {"type": "1", "ipoDate": ipo, "outDate": out}})


def test_union_merges_cross_index_intervals_without_merging_stock_identities():
    stamp = pd.Timestamp
    scope = SimpleNamespace(_pools={
        "csi300": SimpleNamespace(intervals={"SH600000": [(stamp("2020-01-01"), stamp("2020-06-30"))]}),
        "csi1000": SimpleNamespace(intervals={"SH600000": [(stamp("2020-07-01"), stamp("2020-12-31"))],
            "SH600001": [(stamp("2020-02-01"), stamp("2020-12-31"))]})})
    merged = union_intervals(scope, "2020-03-01", "2020-10-01")
    assert merged["SH600000"] == [(stamp("2020-03-01"), stamp("2020-10-01"))]
    assert set(merged) == {"SH600000", "SH600001"}
    with pytest.raises(ValueError, match="Reversed"):
        union_intervals(scope, "2021-01-01", "2020-01-01")


def test_membership_filters_each_signal_date_without_current_constituent_backfill():
    days = pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-06"])
    index = _index(days, ("SH600000", "SH600001"))
    intervals = {"SH600000": [(days[0], days[1])], "SH600001": [(days[2], days[2])]}
    assert dated_membership_mask(index, intervals).tolist() == [True, False, True, False, False, True]
    with pytest.raises(ValueError, match="unique"):
        dated_membership_mask(index.append(index[:1]), intervals)


def test_label_does_not_condition_on_future_index_survival():
    days = pd.bdate_range("2023-01-02", periods=12)
    source = _eligibility(days)
    # The stock exits after signal day. Its real later prices still define label.
    intervals = {"SH600000": [(days[0], days[0])]}
    keep, audit = source.mask(_index(days[:2]), intervals, cutoff=str(days[9].date()), labels=[.1, .2])
    assert keep.tolist() == [True, False]
    assert audit["future_membership_conditioned"] is False
    assert audit["exclusions_in_application_order"]["outside_signal_membership"] == 1


def test_six_actual_sessions_are_purged_without_weekday_inference():
    days = pd.bdate_range("2023-01-02", periods=14).delete(3)
    source = _eligibility(days)
    intervals = {"SH600000": [(days[0], days[-1])]}
    keep, audit = source.mask(_index(days[:9]), intervals, cutoff=str(days[10].date()), labels=np.zeros(9))
    assert keep.tolist() == [True] * 5 + [False] * 4
    assert audit["exclusions_in_application_order"]["immature_label"] == 4


@pytest.mark.parametrize("endpoint", [1, 6])
def test_missing_label_endpoint_is_never_forward_filled(endpoint):
    days = pd.bdate_range("2023-01-02", periods=12)
    source = _eligibility(days, missing=[endpoint])
    intervals = {"SH600000": [(days[0], days[-1])]}
    keep, audit = source.mask(_index(days[:1]), intervals, cutoff=str(days[9].date()), labels=[.1])
    assert not keep[0]
    assert audit["exclusions_in_application_order"]["unobserved_or_illegal_label_endpoints"] == 1


def test_halt_between_label_endpoints_does_not_delete_observable_open_return():
    days = pd.bdate_range("2023-01-02", periods=12)
    source = _eligibility(days, missing=[3])
    intervals = {"SH600000": [(days[0], days[-1])]}
    keep, _ = source.mask(_index(days[:1]), intervals, cutoff=str(days[9].date()), labels=[.1])
    assert keep[0]


def test_inference_never_looks_at_future_quote_or_label_availability():
    days = pd.bdate_range("2023-01-02", periods=12)
    source = _eligibility(days, missing=[1, 6])
    intervals = {"SH600000": [(days[0], days[-1])]}
    keep, audit = source.mask(_index(days[[0, -1]]), intervals)
    assert keep.tolist() == [True, True]
    assert audit["label_endpoint_checks"] is False


def test_ipo_and_exclusive_delisting_boundaries_apply_to_signal_and_label():
    days = pd.bdate_range("2023-01-02", periods=12)
    intervals = {"SH600000": [(days[0], days[-1])]}
    source = _eligibility(days, ipo=str(days[1].date()), out=str(days[8].date()))
    keep, _ = source.mask(_index(days[[0, 1, 7, 8]]), intervals)
    assert keep.tolist() == [False, True, True, False]
    keep, _ = source.mask(_index(days[[1, 2]]), intervals, cutoff=str(days[10].date()), labels=[.1, .2])
    assert keep.tolist() == [True, False]  # t+6 for second signal is delisting date.


def test_unknown_identity_and_nonfinite_labels_are_explicitly_excluded():
    days = pd.bdate_range("2023-01-02", periods=12)
    intervals = {"SH600000": [(days[0], days[-1])], "SH600001": [(days[0], days[-1])]}
    source = _eligibility(days)
    keep, audit = source.mask(_index(days[:1], ("SH600000", "SH600001")), intervals,
        cutoff=str(days[9].date()), labels=[np.nan, .1])
    assert not keep.any()
    assert audit["exclusions_in_application_order"]["invalid_signal_identity"] == 1
    assert audit["exclusions_in_application_order"]["missing_label"] == 1


def test_native_training_processors_fit_only_supplied_purged_training_rows():
    pytest.importorskip("qlib")
    pytest.importorskip("lightgbm")
    rng = np.random.default_rng(20261020)
    days = pd.bdate_range("2022-01-03", periods=40)
    index = _index(days, tuple(f"SH60000{x}" for x in range(6)))
    names = [f"f{x}" for x in range(158)]
    features = pd.DataFrame(rng.normal(size=(len(index), 158)).astype(np.float32), index=index, columns=names)
    labels = pd.DataFrame(rng.normal(size=(len(index), 1)).astype(np.float32), index=index, columns=["LABEL0"])
    frame = pd.concat({"feature": features, "label": labels}, axis=1)
    fold = {"train_start": str(days[0].date()), "train_signal_end": str(days[-1].date())}
    dataset, handler = _train_dataset(frame, fold)
    model = fit_fixed_qlib_lightgbm(dataset, spec=FixedQlibLGBSpec())
    payload = _processor_payload(handler, model.feature_names)
    np.testing.assert_array_equal(payload["processors"][0]["mean_train"], features.median().to_numpy())
    learned = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
    expected = (labels.groupby("datetime").rank(pct=True) - .5) * 3.46
    pd.testing.assert_frame_equal(learned["label"], expected, check_dtype=False)
    future = features.iloc[:6].copy() * 100000
    future.index = _index(["2023-01-03"], tuple(f"SH60000{x}" for x in range(6)))
    original = apply_frozen_processors(features.iloc[:6], model, payload["processors"])
    combined = apply_frozen_processors(pd.concat([features.iloc[:6], future]), model, payload["processors"])
    pd.testing.assert_series_equal(combined.loc[original.index], original)
    assert model.training_rows == len(index)


def test_protocol_rejects_future_survival_filter_and_parameter_search(tmp_path):
    import hashlib
    import json
    scope = tmp_path / "scope"
    scope.mkdir()
    (scope / "manifest.json").write_text("{}\n")
    source = SimpleNamespace(manifest_sha256="a" * 64)
    protocol = {"kind": "a5_union_fixed_research_protocol", "source_snapshot_manifest_sha256": "a" * 64,
        "historical_scope_manifest_sha256": hashlib.sha256((scope / "manifest.json").read_bytes()).hexdigest(),
        "training": {"execution_years": [2023, 2024, 2025, 2026], "evaluation_end": "2026-10-16",
            "lookback_calendar_years": 5, "purge_future_sessions": 6, "label": LABEL,
            "fixed_lightgbm_spec": asdict(FixedQlibLGBSpec()), "hyperparameter_search": False,
            "early_stopping": False, "membership_filter": "signal_date_only_no_future_membership_survival_condition"}}
    path = tmp_path / "protocol.json"
    path.write_text(json.dumps(protocol))
    _load_protocol(path, source, scope)
    for field, value in (("membership_filter", "future_survival"), ("hyperparameter_search", True),
                         ("purge_future_sessions", 5), ("execution_years", [2024])):
        bad = json.loads(json.dumps(protocol))
        bad["training"][field] = value
        path.write_text(json.dumps(bad))
        with pytest.raises(ValueError, match="frozen fixed union"):
            _load_protocol(path, source, scope)


def test_market_gate_rejects_prior_pool_audit_unbound_sources_or_any_missing_check():
    import copy
    valid = {"kind": "a5_union_market_audit", "status": "passed",
        "start": "2023-01-03", "end": "2026-10-16",
        "source_snapshot_manifest_sha256": "a" * 64, "historical_scope_manifest_sha256": "b" * 64,
        "stock_basic_sha256": "c" * 64, "checks": {name: True for name in MARKET_CHECKS}}
    expected = {"source_sha": "a" * 64, "scope_sha": "b" * 64, "stock_basic_sha": "c" * 64,
        "coverage_start": "2023-01-03", "coverage_end": "2026-10-16"}
    validate_market_audit(valid, **expected)
    for field in ("kind", "source_snapshot_manifest_sha256", "historical_scope_manifest_sha256", "stock_basic_sha256"):
        bad = copy.deepcopy(valid)
        bad[field] = "previous_csi300_audit"
        with pytest.raises(ValueError, match="source-bound union"):
            validate_market_audit(bad, **expected)
    for check in MARKET_CHECKS:
        bad = copy.deepcopy(valid)
        bad["checks"][check] = False
        with pytest.raises(ValueError, match="source-bound union"):
            validate_market_audit(bad, **expected)
    bad = copy.deepcopy(valid)
    bad["checks"][MARKET_CHECKS[0]] = 1
    with pytest.raises(ValueError, match="source-bound union"):
        validate_market_audit(bad, **expected)
    for field, value in (("start", "2024-01-02"), ("end", "2024-12-31")):
        bad = copy.deepcopy(valid)
        bad[field] = value
        with pytest.raises(ValueError, match="source-bound union"):
            validate_market_audit(bad, **expected)
    first_two_years = copy.deepcopy(valid)
    first_two_years["end"] = "2024-12-31"
    validate_market_audit(first_two_years, **{**expected, "coverage_end": "2024-12-31"})


def test_canonical_audit_identity_csv_preserves_empty_delisting_and_rejects_duplicates(tmp_path):
    path = tmp_path / "stock_basic.csv"
    path.write_text("code,code_name,ipoDate,outDate,type,status\nsh.600000,测试,2010-01-01,,1,1\n")
    eligibility, used = load_eligibility(SimpleNamespace(), pd.bdate_range("2023-01-02", periods=8), [], path)
    assert eligibility.identities["SH600000"]["outDate"] == ""
    assert eligibility.identities["SH600000"]["type"] == "1"
    assert not used
    path.write_text(path.read_text() + "sh.600000,重复,2011-01-01,,1,1\n")
    with pytest.raises(ValueError, match="Duplicate"):
        load_eligibility(SimpleNamespace(), pd.bdate_range("2023-01-02", periods=8), [], path)


def test_market_source_hashes_are_rechecked_beyond_the_top_level_passed_receipt(tmp_path):
    import hashlib
    source = tmp_path / "raw_source.json"
    source.write_text("{}\n")
    basic = tmp_path / "stock_basic.csv"
    basic.write_text("code,ipoDate,outDate,type\n")
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    audit = {"stock_basic_path": str(basic.resolve()), "stock_basic_sha256": digest(basic), "bound_inputs": {
        "stock_basic": {"path": str(basic), "sha256": digest(basic)},
        "raw": {"path": str(source), "sha256": digest(source)}}}
    assert_market_originals_unchanged(audit, basic)
    source.write_text('{"changed": true}\n')
    with pytest.raises(ValueError, match="changed since"):
        assert_market_originals_unchanged(audit, basic)


def test_two_year_folds_need_only_their_own_declared_observed_years():
    days = pd.bdate_range("2017-01-02", "2024-12-31").delete(1200)
    folds = union_fold_plan(days, [2023, 2024], evaluation_end="2024-12-31")
    assert [row["train_start"] for row in folds] == ["2018-01-01", "2019-01-01"]
    for row in folds:
        assert days.get_loc(pd.Timestamp(row["data_cutoff"])) - days.get_loc(pd.Timestamp(row["train_signal_end"])) == 6
    with pytest.raises(ValueError, match="declared years"):
        union_fold_plan(days, [2023, 2024], evaluation_end="2023-12-29")
    with pytest.raises(ValueError, match="declared years"):
        union_fold_plan(days, [2024], evaluation_end="2024-12-31")


def test_live_official_replay_implementation_is_bound_in_addition_to_archived_source():
    import hashlib
    from pathlib import Path
    from stopat30m.data import official_index_membership
    assert implementation_sha256()["official_scope_replay"] == hashlib.sha256(
        Path(official_index_membership.__file__).read_bytes()).hexdigest()
