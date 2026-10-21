"""Synthetic source-family, annual-routing and unchanged real-ledger checks."""
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_model_debug_validation as debug
from stopat30m import ashare_model_debug_research as training
from stopat30m import ashare_turnover_validation as turnover
from test_ashare_turnover_validation import market


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return path


def context(tmp_path, *, family="stability"):
    value = market(changing=True)
    declaration = write(tmp_path/"training.json", {"synthetic": True})
    value["model_debug"] = {"family": family, "training_declaration": training._proof(declaration),
        "source_snapshot": str(tmp_path/"source"), "source_manifest_sha256": "a"*64,
        "scope_manifest_sha256": "b"*64}
    if family == "primary":
        value["protocol"]["periods"] = {"2023": ["2023-01-03", "2023-01-31"],
            "2024": ["2024-01-02", "2024-01-31"], "continuous": ["2023-01-03", "2024-01-31"]}
    return value


def annual_context(tmp_path):
    value = context(tmp_path, family="primary")
    value["calendar"] = ["2022-12-30", "2023-01-03", "2023-12-29", "2024-01-02", "2024-01-03"]
    index = pd.MultiIndex.from_product([pd.to_datetime(value["calendar"][:-1]), ["SH600000"]],
                                      names=["datetime", "instrument"])
    value["scores"] = {"LGB": pd.Series(range(len(index)), index=index, dtype=float)}
    return value


def fake_package(tmp_path, value, *, phase="retrospective", candidate="capacity300", name=None):
    path = tmp_path/(name or phase)
    years = training.PHASE_YEARS[phase]
    rows = []
    days = value["calendar"]
    next_year = {day: int(days[index+1][:4]) for index, day in enumerate(days[:-1])}
    for (date, inst), score in value["scores"]["LGB"].items():
        year = next_year.get(date.date().isoformat())
        if year in years:
            rows.append({"model": candidate, "execution_year": year, "datetime": date.date().isoformat(),
                         "instrument": inst, "score": float(score)+.25})
    # Retrospective package always contains both annual folds; the short
    # market fixture is used only for year2025 direct simulation below.
    models = list(training.CANDIDATES) if phase == "selection" else [candidate]
    manifest = {"phase": phase, "models": models, "execution_years": years,
                "inputs": {}, "source_families": {value["model_debug"]["family"]: {
                    "source_snapshot": value["model_debug"]["source_snapshot"],
                    "source_manifest_sha256": "a"*64, "scope_manifest_sha256": "b"*64}}}
    write(path/"manifest.json", manifest)
    pd.DataFrame(rows, columns=debug._REQUIRED_COLUMNS).to_csv(path/"predictions.csv", index=False)
    return path, manifest


def bind_verifier(monkeypatch, packages):
    calls = []
    def verify(path, declaration):
        calls.append((Path(path), Path(declaration)))
        return deepcopy(packages[Path(path)])
    monkeypatch.setattr(training, "verify_debug_model_package", verify)
    return calls


def test_baseline_delegates_entire_actual_fixed_hold10_cap2_ledger(tmp_path):
    value = context(tmp_path)
    before = deepcopy(value["scores"]["LGB"])
    expected = turnover.simulate_turnover(value, period="2025", variant=dict(debug.HOLD_RULE))
    actual = debug.simulate_debug_model(value, candidate="baseline", period="2025")
    assert actual == expected
    pd.testing.assert_series_equal(value["scores"]["LGB"], before)
    assert actual["variant"] == {"key": "hold10_cap2", "minimum_hold_sessions": 10, "max_replacements": 2}
    assert actual["records"]["turnover_plans"][-1]["exit_intent_count"] == 2


def test_baseline_isolates_only_runtime_coverage_not_scores_or_protocol(tmp_path):
    value = context(tmp_path)
    value["score_coverage"] = {("LGB", "old"): {"unsafe": True}}
    prepared = debug.prepare_debug_context(value, candidate="baseline", period="2025", prediction_package=None)
    assert prepared is not value
    assert prepared["score_coverage"] == {}
    assert value["score_coverage"]
    assert prepared["scores"] is value["scores"]
    assert prepared["protocol"] is value["protocol"]


@pytest.mark.parametrize("candidate,period,pkg", [("unknown", "2025", None),
    ("baseline", "2025", "new-model"), ("baseline", "2023", None)])
def test_undeclared_arm_or_family_period_cannot_reach_engine(tmp_path, candidate, period, pkg):
    with pytest.raises(ValueError):
        debug.simulate_debug_model(context(tmp_path), candidate=candidate, period=period, prediction_package=pkg)


def test_changed_declaration_is_detected_before_any_simulation(tmp_path):
    value = context(tmp_path)
    Path(value["model_debug"]["training_declaration"]["path"]).write_text("changed")
    with pytest.raises(ValueError, match="bound model-debug input changed"):
        debug.simulate_debug_model(value, candidate="baseline", period="2025")


def test_native_invalid_result_is_preserved_without_extra_qualification(tmp_path, monkeypatch):
    expected = {"performance_valid": False, "stopped": {"kind": "unknown_equity"}, "records": {}}
    monkeypatch.setattr(debug, "simulate_turnover", lambda *a, **k: expected)
    assert debug.simulate_debug_model(context(tmp_path), candidate="baseline", period="2025") is expected


def test_router_uses_next_execution_year_and_keeps_year_end_pair_separate(tmp_path):
    value = annual_context(tmp_path)
    table = pd.DataFrame([
        dict(model="capacity300", execution_year=2023, datetime="2022-12-30", instrument="SH600000", score=1.),
        dict(model="capacity300", execution_year=2023, datetime="2023-01-03", instrument="SH600000", score=2.),
        dict(model="capacity300", execution_year=2023, datetime="2023-12-29", instrument="SH600000", score=999.),
        dict(model="capacity300", execution_year=2024, datetime="2023-12-29", instrument="SH600000", score=3.),
        dict(model="capacity300", execution_year=2024, datetime="2024-01-02", instrument="SH600000", score=4.)])
    original = table.copy(deep=True)
    scores = debug._prediction_scores(table, candidate="capacity300", years=[2023, 2024],
                                      calendar=value["calendar"], expected=value["scores"]["LGB"])
    assert scores.tolist() == [1., 2., 3., 4.]
    pd.testing.assert_frame_equal(table, original)


@pytest.mark.parametrize("fault", ["duplicate", "missing", "extra", "nonfinite", "future_year", "outside_calendar",
                                   "invalid_symbol", "intraday", "tz", "bool_year", "outside_fold"])
def test_router_rejects_damaged_future_or_incomplete_candidate_rows(tmp_path, fault):
    value = annual_context(tmp_path)
    path, _ = fake_package(tmp_path, value, phase="selection")
    table = pd.read_csv(path/"predictions.csv")
    if fault == "duplicate": table = pd.concat([table, table.iloc[:1]])
    elif fault == "missing": table = table.iloc[:-1]
    elif fault == "extra": table = pd.concat([table, table.iloc[:1].assign(instrument="SH600001")])
    elif fault == "nonfinite": table.loc[0, "score"] = np.inf
    elif fault == "future_year": table.loc[0, "execution_year"] = 2024
    elif fault == "outside_calendar": table.loc[0, "datetime"] = "2023-01-04"
    elif fault == "invalid_symbol": table.loc[0, "instrument"] = "SH.600000"
    elif fault == "intraday": table.loc[0, "datetime"] = "2022-12-30T13:00:00"
    elif fault == "tz": table.loc[0, "datetime"] = "2022-12-30T00:00:00+08:00"
    elif fault == "bool_year": table.loc[0, "execution_year"] = True
    elif fault == "outside_fold": table.loc[0, "datetime"] = "2024-01-02"
    with pytest.raises((ValueError, TypeError)):
        debug._prediction_scores(table, candidate="capacity300", years=[2023],
                                 calendar=value["calendar"], expected=value["scores"]["LGB"])


def test_selection_candidate_isolated_scores_and_exact_original_execution(tmp_path, monkeypatch):
    value = annual_context(tmp_path)
    value["score_coverage"] = {"previous": True}
    path, manifest = fake_package(tmp_path, value, phase="selection")
    calls = bind_verifier(monkeypatch, {path: manifest})
    prepared = debug.prepare_debug_context(value, candidate="capacity300", period="2023", prediction_package=path)
    assert len(calls) == 1
    assert prepared["protocol"] == value["protocol"] and prepared["protocol"] is not value["protocol"]
    assert prepared["raw"] is value["raw"] and prepared["events"] is value["events"]
    assert prepared["tax_contracts"] is value["tax_contracts"]
    assert prepared["score_coverage"] == {} and value["score_coverage"] == {"previous": True}
    assert list(prepared["scores"]) == ["LGB"]
    assert prepared["scores"]["LGB"].tolist() == [.25, 1.25]
    assert value["scores"]["LGB"].tolist() == [0., 1., 2., 3.]


def test_candidate_actual_primary_ledger_keeps_holding_fees_and_effective_dividend_tax(tmp_path, monkeypatch):
    value = context(tmp_path)
    shift = lambda day: str(int(day[:4])-2)+day[4:]
    value["calendar"] = [shift(day) for day in value["calendar"]]
    value["raw"]["date"] = value["raw"]["date"].map(shift)
    scores = value["scores"]["LGB"]
    scores.index = pd.MultiIndex.from_arrays([
        pd.to_datetime([shift(day.date().isoformat()) for day in scores.index.get_level_values("datetime")]),
        scores.index.get_level_values("instrument")], names=["datetime", "instrument"])
    original_period = value["protocol"]["periods"]["2025"]
    period = [shift(day) for day in original_period]
    value["protocol"]["periods"] = {"2023": period, "2024": period, "continuous": period}
    value["model_debug"]["family"] = "primary"
    record = period[0]
    exday, pay = value["calendar"][value["calendar"].index(record)+1:value["calendar"].index(record)+3]
    inst = value["raw"].instrument.iloc[0]
    value["events"] = [{"event_id": "synthetic_primary_effective_tax", "instrument": inst,
        "record_date": record, "ex_date": exday, "pay_date": pay, "listing_date": exday,
        "cash_per_share": ".1", "bonus_ratio": "0", "cash_dividend_tax_rate": ".4",
        "known_at": "2022-12-20T23:59:59+08:00", "source_response_sha256": "c"*64}]
    path, manifest = fake_package(tmp_path, value, phase="selection")
    bind_verifier(monkeypatch, {path: manifest})
    expected = debug.simulate_debug_model(value, candidate="baseline", period="2023")
    actual = debug.simulate_debug_model(value, candidate="capacity300", period="2023", prediction_package=path)
    assert actual == expected
    held = actual["final_state"]["holdings_history"][record][inst]
    assert held > 0
    assert Decimal(actual["metrics"]["recognized_distribution_tax_reserve_cny"]) == held*Decimal(".1")*Decimal(".4")
    assert actual["metrics"]["additional_distribution_tax_reserved_cny"] == "0"
    assert all(Decimal(row["commission"]) >= 5 for row in actual["records"]["trades"])
    assert actual["records"]["turnover_plans"][-1]["exit_intent_count"] == 2


def test_continuous_primary_combines_same_selected_annual_models_without_phase_reset(tmp_path, monkeypatch):
    value = annual_context(tmp_path)
    first, one = fake_package(tmp_path, value, phase="selection")
    second, two = fake_package(tmp_path, value, phase="confirmation")
    decision = write(tmp_path/"decision.json", {"selected_candidate": "capacity300",
        "selection_model_manifest": training._proof(first/"manifest.json")})
    two["inputs"]["selection_decision"] = training._proof(decision)
    bind_verifier(monkeypatch, {first: one, second: two})
    before = deepcopy(value["protocol"])
    captured = {}
    monkeypatch.setattr(debug, "simulate_turnover", lambda prepared, **kwargs: captured.update(context=prepared, **kwargs) or captured)
    result = debug.simulate_debug_model(value, candidate="capacity300", period="continuous", prediction_package=[second, first])
    assert result["period"] == "continuous" and result["variant"] == debug.HOLD_RULE
    assert result["context"]["scores"]["LGB"].tolist() == [.25, 1.25, 2.25, 3.25]
    assert result["context"]["protocol"] == before == value["protocol"]


@pytest.mark.parametrize("fault", ["wrong_selection_manifest", "wrong_candidate", "missing_decision", "one_package",
                                   "duplicate_package", "wrong_period", "wrong_phase", "wrong_source"])
def test_primary_continuous_rejects_mixed_or_unbound_stages(tmp_path, monkeypatch, fault):
    value = annual_context(tmp_path)
    first, one = fake_package(tmp_path, value, phase="selection")
    second, two = fake_package(tmp_path, value, phase="confirmation")
    decision_data = {"selected_candidate": "capacity300", "selection_model_manifest": training._proof(first/"manifest.json")}
    if fault == "wrong_selection_manifest": decision_data["selection_model_manifest"] = training._proof(second/"manifest.json")
    if fault == "wrong_candidate": decision_data["selected_candidate"] = "regularized300"
    decision = write(tmp_path/"decision.json", decision_data)
    two["inputs"]["selection_decision"] = training._proof(decision)
    if fault == "missing_decision": two["inputs"] = {}
    if fault == "wrong_phase": two["phase"] = "retrospective"
    if fault == "wrong_source": two["source_families"]["primary"]["source_manifest_sha256"] = "c"*64
    bind_verifier(monkeypatch, {first: one, second: two})
    packages = first if fault == "one_package" else [first, first] if fault == "duplicate_package" else [first, second]
    with pytest.raises(ValueError):
        debug.prepare_debug_context(value, candidate="capacity300", period="2023" if fault == "wrong_period" else "continuous",
                                     prediction_package=packages)


def loader_fixture(tmp_path, monkeypatch, *, family):
    value = context(tmp_path, family=family)
    if family == "primary": value.pop("tax_contracts")
    protocol = write(tmp_path/"old_protocol.json", {"historical_scope_manifest_sha256": "b"*64})
    package = tmp_path/"old_package"
    manifest = write(package/"manifest.json", {"kind": "a5_union_fixed_predictions" if family == "primary" else "a5_union_stability_fixed_predictions",
        "source_snapshot": str(tmp_path/"source"), "source_manifest_sha256": "a"*64})
    audit = write(tmp_path/"audit.json", {})
    basic = write(tmp_path/"basic.json", {})
    merger = write(tmp_path/"merger.json", {})
    declaration = {"source_families": {family: {"model_manifest": training._proof(manifest),
        "protocol": training._proof(protocol), "market_audit": training._proof(audit), "stock_basic": training._proof(basic)}},
        "merger_actions": training._proof(merger)}
    path = write(tmp_path/"training_decl.json", declaration)
    monkeypatch.setattr(training, "_load_declaration", lambda p: (deepcopy(declaration), {}))
    monkeypatch.setattr(training, "_scope_directory", lambda *a: tmp_path/"scope")
    calls = []
    def load(*args): calls.append(args);return value
    monkeypatch.setattr(debug.primary if family == "primary" else debug.stability,
                        "load_union_context" if family == "primary" else "load_stability_context", load)
    return value, declaration, path, calls


@pytest.mark.parametrize("family", ["primary", "stability"])
def test_loader_delegates_all_six_original_bound_paths_and_preserves_tax_evidence(tmp_path, monkeypatch, family):
    value, decl, path, calls = loader_fixture(tmp_path, monkeypatch, family=family)
    original_events = value["events"]
    actual = debug.load_debug_context(path, family)
    assert len(calls) == 1 and calls[0] == (str(tmp_path/"source"), str(tmp_path/"scope"),
        str(tmp_path/"old_protocol.json"), str(tmp_path/"audit.json"), str(tmp_path/"old_package"), str(tmp_path/"merger.json"))
    assert actual["events"] is original_events
    assert actual["protocol"] is value["protocol"]
    assert actual["tax_contracts"] == []
    assert actual["model_debug"]["family"] == family
    if family == "primary":
        assert "original_effective_dividend_tax_rates_retained" in actual["model_debug"]["primary_additional_tax_contracts"]
        assert "tax_contracts" not in value


def test_missing_stability_contracts_cannot_be_defaulted_to_zero(tmp_path, monkeypatch):
    value, decl, path, _ = loader_fixture(tmp_path, monkeypatch, family="stability")
    value.pop("tax_contracts")
    with pytest.raises(ValueError, match="actual additional tax contracts"):
        debug.load_debug_context(path, "stability")


def test_source_map_mismatch_stops_before_old_loader(tmp_path, monkeypatch):
    value, decl, path, calls = loader_fixture(tmp_path, monkeypatch, family="primary")
    decl["sources"] = {"primary": {"snapshot": str(tmp_path/"other_source")}}
    with pytest.raises(ValueError, match="source map differs"):
        debug.load_debug_context(path, "primary")
    assert calls == []


def test_mapping_declaration_requires_exact_bound_disk_identity(tmp_path, monkeypatch):
    _, decl, path, _ = loader_fixture(tmp_path, monkeypatch, family="primary")
    complete = {**decl, "_self_proof": training._proof(path)}
    assert debug.load_debug_context(complete, "primary")["model_debug"]["family"] == "primary"
    complete["new_parameter"] = 1
    with pytest.raises(ValueError, match="In-memory debug declaration differs"):
        debug.load_debug_context(complete, "primary")


@pytest.mark.parametrize("family", ["future", None, "PRIMARY"])
def test_unknown_family_cannot_be_reinterpreted_as_old_loader(tmp_path, family):
    with pytest.raises(ValueError, match="separate primary and stability"):
        debug.load_debug_context(tmp_path/"missing.json", family)
