"""Stage isolation, fixed 2023 selection and exact all-record replay guards."""
from contextlib import nullcontext
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_model_debug_experiment as ex


def row(candidate, period="2023", *, net="0.10", dd="-0.10", fee="100", slip="50", valid=True):
    return {"candidate": candidate, "period": period, "performance_valid": valid,
        "metrics": {"total_return_lower_exact": net, "max_drawdown_conservative_exact": dd,
            "total_fee_cny": fee, "slippage_cost_cny": slip}}


def rows():
    return [row(key) for key in ("baseline", *ex.CANDIDATES)]


def test_equal_model_is_diagnostic_fallback_and_never_live():
    decision = ex.decide_debug_selection(rows())
    assert decision["selected_candidate"] == "capacity300"
    assert decision["primary_qualified"] is False
    assert decision["diagnostic_only"] is True
    assert decision["later_years_used_for_selection"] is False
    assert decision["live_model_selected"] is None


def test_eligible_return_and_drawdown_can_beat_a_higher_return_bad_risk_arm():
    data = rows()
    data[1]["metrics"].update(total_return_lower_exact="0.11")
    data[2]["metrics"].update(total_return_lower_exact="0.30", max_drawdown_conservative_exact="-0.20")
    data[3]["metrics"].update(total_return_lower_exact="0.12", max_drawdown_conservative_exact="-0.09")
    decision = ex.decide_debug_selection(data)
    assert decision["selected_candidate"] == "recent3_capacity300"
    assert decision["eligible_candidates"] == ["capacity300", "recent3_capacity300"]
    assert decision["primary_qualified"] is True


@pytest.mark.parametrize("tiebreak,expected", [("return", "regularized300"), ("drawdown", "recent3_capacity300"), ("friction", "regularized300"), ("lexical", "capacity300")])
def test_all_frozen_tiebreakers(tiebreak, expected):
    data = rows()
    if tiebreak == "return":
        data[2]["metrics"]["total_return_lower_exact"] = "0.10000000000000000000000001"
    elif tiebreak == "drawdown":
        data[3]["metrics"]["max_drawdown_conservative_exact"] = "-0.09"
    elif tiebreak == "friction":
        data[2]["metrics"]["total_fee_cny"] = "99.99"
    assert ex.decide_debug_selection(data)["selected_candidate"] == expected


def test_invalid_baseline_prevents_primary_eligibility_but_allows_diagnostic():
    data = rows(); data[0]["performance_valid"] = False
    data[1]["metrics"]["total_return_lower_exact"] = "0.20"
    decision = ex.decide_debug_selection(data)
    assert decision["selected_candidate"] == "capacity300"
    assert decision["primary_qualified"] is False
    assert decision["diagnostic_only"] is True


def test_invalid_high_return_cannot_win_selection():
    data = rows(); data[1]["performance_valid"] = False
    data[1]["metrics"]["total_return_lower_exact"] = "999"
    assert ex.decide_debug_selection(data)["selected_candidate"] == "recent3_capacity300"


def test_no_valid_candidate_stops_later_training():
    data = rows()
    for item in data[1:]:
        item["performance_valid"] = False
    result = ex.decide_debug_selection(data)
    assert result["selected_candidate"] is None
    assert result["confirmation_allowed"] is False


@pytest.mark.parametrize("fault", ["missing", "duplicate", "extra", "2024", "2025", "continuous", "unknown_candidate"])
def test_selection_rejects_any_coverage_or_later_year_contamination(fault):
    data = rows()
    if fault == "missing": data.pop()
    elif fault == "duplicate": data.append(data[0])
    elif fault == "extra": data.append(row("new_grid"))
    elif fault == "unknown_candidate": data[-1]["candidate"] = "H20"
    else: data[-1]["period"] = fault
    with pytest.raises(ValueError, match="predeclared"):
        ex.decide_debug_selection(data)


@pytest.mark.parametrize("value", [True, None, "0.1", "NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("field", ["total_return_lower_exact", "max_drawdown_conservative_exact"])
def test_nonfinite_or_missing_decision_metrics_rejected(field, value):
    data = rows(); data[1]["metrics"][field] = value
    if value == "0.1":
        # Numeric strings are the intended exact serialization.
        assert ex.decide_debug_selection(data)["selection_year"] == 2023
    else:
        with pytest.raises(ValueError): ex.decide_debug_selection(data)


@pytest.mark.parametrize("value", [1, 0, None, "True"])
def test_validity_is_not_truthiness(value):
    data = rows(); data[1]["performance_valid"] = value
    with pytest.raises(ValueError, match="Boolean"): ex.decide_debug_selection(data)


def confirmation_rows():
    return [row(k, p, net="0.11" if k != "baseline" else "0.10")
        for k in ("baseline", "capacity300") for p in ("2024", "continuous")]


def test_confirmation_does_not_reselect_or_promote():
    result = ex.decide_debug_confirmation(confirmation_rows(), {"selected_candidate": "capacity300", "primary_qualified": True})
    assert result["confirmation_pass"] is True
    assert result["no_reselection_from_2024"] is True
    assert result["live_model_selected"] is None


@pytest.mark.parametrize("fault", ["fallback2023", "equal2024", "worseDD2024", "invalid2024", "wrongcandidate", "missing"])
def test_confirmation_requires_each_declared_year_gate(fault):
    data = confirmation_rows(); decision = {"selected_candidate": "capacity300", "primary_qualified": True}
    target = next(r for r in data if r["candidate"] == "capacity300" and r["period"] == "2024")
    if fault == "fallback2023": decision["primary_qualified"] = False
    elif fault == "equal2024": target["metrics"]["total_return_lower_exact"] = "0.10"
    elif fault == "worseDD2024": target["metrics"]["max_drawdown_conservative_exact"] = "-0.11"
    elif fault == "invalid2024": target["performance_valid"] = False
    elif fault == "wrongcandidate": target["candidate"] = "regularized300"
    else: data.pop()
    if fault in ("wrongcandidate", "missing"):
        with pytest.raises(ValueError): ex.decide_debug_confirmation(data, decision)
    else: assert not ex.decide_debug_confirmation(data, decision)["confirmation_pass"]


def test_continuous_is_disclosed_without_adding_an_unfrozen_annual_gate():
    data = confirmation_rows(); data[-1]["performance_valid"] = False
    result = ex.decide_debug_confirmation(data, {"selected_candidate": "capacity300", "primary_qualified": True})
    assert result["confirmation_pass"] is True
    assert result["all_confirmation_and_continuous_accounts_financially_valid"] is False


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return path


def fake_family(tmp_path, family):
    scope = tmp_path / "scope"; scope.mkdir(exist_ok=True)
    scope_manifest = write(scope / "manifest.json", {"kind": "synthetic_scope"})
    snapshot = tmp_path / (family + "snapshot")
    source_file = snapshot / "dataset" / "features" / "synthetic.bin"
    source_file.parent.mkdir(parents=True); source_file.write_bytes(b"original observed feature")
    snapshot_manifest = write(snapshot / "manifest.json", {"kind": "synthetic_snapshot"})
    stock = write(tmp_path / (family + "stock.json"), {"identity": family})
    audit = write(tmp_path / (family + "audit.json"), {"bound_inputs": {"stock": ex.stability._proof(stock)}})
    protocol = write(tmp_path / (family + "protocol.json"), {
        "source_snapshot_manifest_sha256": ex.stability.sha(snapshot_manifest),
        "historical_scope_manifest_sha256": ex.stability.sha(scope_manifest)})
    package = tmp_path / (family + "models"); package.mkdir()
    write(package / "source_files.json", {"features/synthetic.bin": ex.stability.sha(source_file)})
    (package / "model.txt").write_text("original native bytes")
    write(package / "declaration.json", json.loads(protocol.read_text()))
    manifest = {"kind": ex.EXPECTED_MODEL_KIND[family], "source_snapshot": str(snapshot),
        "source_manifest_sha256": ex.stability.sha(snapshot_manifest),
        "inputs": {"declaration": ex.stability._proof(protocol), "stock_basic": ex.stability._proof(stock), "market_audit": ex.stability._proof(audit)},
        "scope_sources": {"scope_manifest": ex.stability._proof(scope_manifest)},
        "files": {p.name: ex.stability.sha(p) for p in package.iterdir()}}
    return write(package / "manifest.json", manifest)


def synthetic_freeze(tmp_path, monkeypatch):
    primary = fake_family(tmp_path, "primary"); stability = fake_family(tmp_path, "stability")
    paper = tmp_path / "active"; paper.mkdir()
    state = write(paper / "state.json", {"cash": "100000"})
    frozen = tmp_path / "frozen.py"; frozen.write_text("# original static implementation")
    write(paper / "manifest.json", {"state_observation": ex.stability._proof(state), "implementation": ex.stability._proof(frozen)})
    merger = write(tmp_path / "merger.json", {"events": []})
    monkeypatch.setattr(ex, "implementation_proofs", lambda: {"synthetic": ex.stability._proof(frozen)})
    monkeypatch.setattr(ex, "implementation_sha256", lambda: {"synthetic": ex.stability.sha(frozen)})
    design = Path(__file__).parents[1] / "docs/research/a14_model_debug_design_v1.json"
    output = tmp_path / "declaration.json"
    declaration = ex.freeze_debug_declaration(primary_manifest=primary, stability_manifest=stability,
        design_path=design, merger_actions=merger, active_paper=paper, output=output)
    return output, declaration, state


def test_freeze_keeps_separate_original_families_and_mutable_state_unbound(tmp_path, monkeypatch):
    path, declaration, state = synthetic_freeze(tmp_path, monkeypatch)
    assert set(declaration["source_families"]) == {"primary", "stability"}
    assert all(set(f) == {"model_manifest", "protocol", "market_audit", "stock_basic"} for f in declaration["source_families"].values())
    assert all(len(m) == 6 for m in declaration["sources"].values())
    assert not any(Path(p["path"]) == state for p in declaration["bound_inputs"].values())
    state.write_text('{"cash":"99999"}')
    ex.stability._assert_proofs(declaration["bound_inputs"])
    assert path.is_file()


@pytest.mark.parametrize("fault", ["swapfamilies", "snapshot", "sourcefile", "nativefile", "stockbasic"])
def test_family_guards_reject_source_replacements(tmp_path, fault):
    manifest_path = fake_family(tmp_path, "primary")
    manifest = json.loads(manifest_path.read_text())
    if fault == "swapfamilies": manifest["kind"] = ex.EXPECTED_MODEL_KIND["stability"]
    elif fault == "snapshot": manifest["source_manifest_sha256"] = "0" * 64
    elif fault == "sourcefile": (Path(manifest["source_snapshot"]) / "dataset/features/synthetic.bin").write_bytes(b"wrong")
    elif fault == "nativefile": (manifest_path.parent / "model.txt").write_text("wrong")
    else: Path(manifest["inputs"]["stock_basic"]["path"]).write_text("wrong")
    write(manifest_path, manifest)
    with pytest.raises(ValueError): ex._family_from_manifest(manifest_path, "primary")


def test_frozen_design_rejects_even_unrelated_mutation():
    design = json.loads((Path(__file__).parents[1] / "docs/research/a14_model_debug_design_v1.json").read_text())
    ex._assert_design(design)
    design["diagnostics"].append("post-result expansion")
    with pytest.raises(ValueError): ex._assert_design(design)


@pytest.mark.parametrize("relation", ["same", "child", "parent", "existing"])
def test_output_isolation_preserves_live_account_and_old_artifacts(tmp_path, relation):
    active = tmp_path / "active"; active.mkdir()
    output = {"same": active, "child": active / "new.json", "parent": tmp_path, "existing": tmp_path / "old"}[relation]
    if relation == "existing": output.mkdir()
    with pytest.raises(ValueError): ex._isolated_output(output, active)


def ledger(candidate, period):
    result = row(candidate, period, net="0.11" if candidate == "capacity300" else "0.10")
    result.pop("candidate")
    result.update(model="LGB", variant={"key": "hold10_cap2", "minimum_hold_sessions": 10, "max_replacements": 2},
        records={"curves": [{"date": period, "cash": "100000"}], "trades": [], "holding_age": []},
        final_state={"cash": "100000", "positions": {}}, score_quality={"summary": {}})
    return result


def setup_run(tmp_path, monkeypatch, phase):
    source = tmp_path / "source.py"; source.write_text("# immutable synthetic source")
    model = write(tmp_path / "models" / "manifest.json", {"kind": "synthetic_models"})
    decision = write(tmp_path / "selection.json", {"kind": ex.DECISION_KIND, "selected_candidate": "capacity300", "primary_qualified": True})
    protocol_file = write(tmp_path / "training.json", {"kind": ex.KIND})
    training = {"active_paper_directory": str(tmp_path / "active"), "design": {"execution": {"top_k": 20}}}
    declaration = {"phase": phase, "training_declaration": ex.stability._proof(protocol_file),
        "bound_inputs": {"new_model_manifest": ex.stability._proof(model), "selection_decision": ex.stability._proof(decision)},
        "expected_new_ledger_count": {"selection": 4, "confirmation": 4, "retrospective": 6}[phase],
        "models_directory": str(model.parent), "selection_models_directory": str(tmp_path / "oldmodels"),
        "selected_candidate": None if phase == "selection" else "capacity300", "baseline_directory": str(tmp_path / "A6")}
    execution = write(tmp_path / "execution.json", declaration)
    monkeypatch.setattr(ex, "_verify_execution", lambda _: (declaration, training))
    monkeypatch.setattr(ex, "implementation_proofs", lambda: {"synthetic": ex.stability._proof(source)})
    from stopat30m import ashare_model_debug_validation as val
    context = {"pool": SimpleNamespace(verified_batch=nullcontext, assert_unchanged=lambda: None),
        "actions": SimpleNamespace(assert_unchanged=lambda: None), "bound": {}}
    monkeypatch.setattr(val, "load_debug_context", lambda *_: context)
    monkeypatch.setattr(ex.stability, "score_quality", lambda *args, **kwargs: {"summary": {}})
    calls = []
    def simulate(_, *, candidate, period, prediction_package):
        calls.append((candidate, period, prediction_package)); return ledger(candidate, period)
    monkeypatch.setattr(val, "simulate_debug_model", simulate)
    monkeypatch.setattr(ex, "assert_debug_stability_baseline_reproduced", lambda *args: {"passed": True})
    return execution, calls


@pytest.mark.parametrize("phase,count", [("selection", 4), ("confirmation", 4), ("retrospective", 6)])
def test_exact_stage_budget_and_order_without_fit(tmp_path, monkeypatch, phase, count):
    execution, calls = setup_run(tmp_path, monkeypatch, phase)
    result = ex.run_debug_comparison(execution, tmp_path / "out")
    assert len(calls) == result["ledger_count"] == count
    assert result["new_native_fits_here"] == 0
    assert result["active_paper_changed"] is False
    assert result["automatic_model_promotion"] is False
    if phase == "selection":
        assert {period for _, period, _ in calls} == {"2023"}
        decision = json.loads((tmp_path / "out/selection_decision.json").read_text())
        assert decision["selected_candidate"] == "capacity300"
        assert decision["selection_report"] == ex.stability._proof(tmp_path / "out/report.json")
        assert decision["selection_model_manifest"] == result["model_manifest"]
    elif phase == "confirmation":
        assert calls[-1][1] == "continuous" and len(calls[-1][2]) == 2
        assert [c[0] for c in calls[:2]] == ["baseline"] * 2
    else:
        assert [c[:2] for c in calls[:3]] == [("baseline", p) for p in ("2025", "2026", "continuous")]


@pytest.mark.parametrize("bad_period", ["2025", "2026", "continuous"])
def test_any_old_baseline_mismatch_stops_all_retrospective_candidate_returns(tmp_path, monkeypatch, bad_period):
    execution, calls = setup_run(tmp_path, monkeypatch, "retrospective")
    def assert_old(result, _):
        if result["period"] == bad_period: raise ValueError("synthetic original record mismatch")
        return {"passed": True}
    monkeypatch.setattr(ex, "assert_debug_stability_baseline_reproduced", assert_old)
    with pytest.raises(ValueError, match="original record mismatch"):
        ex.run_debug_comparison(execution, tmp_path / "out")
    assert all(candidate == "baseline" for candidate, _, _ in calls)
    failure = json.loads((tmp_path / "out/failure.json").read_text())
    assert failure["no_qualified_model_comparison_claim"] is True


def test_invalid_path_is_preserved_without_repair_or_reselection(tmp_path, monkeypatch):
    execution, calls = setup_run(tmp_path, monkeypatch, "retrospective")
    from stopat30m import ashare_model_debug_validation as val
    def simulate(_, *, candidate, period, prediction_package):
        result = ledger(candidate, period)
        if candidate != "baseline" and period == "continuous":
            result.update(performance_valid=False, stopped={"reason": "unpriced_Cinda"})
        return result
    monkeypatch.setattr(val, "simulate_debug_model", simulate)
    result = ex.run_debug_comparison(execution, tmp_path / "out")
    assert result["all_performance_valid"] is False
    assert result["selected_candidate"] == "capacity300"
    saved = json.loads((tmp_path / "out/capacity300/continuous/summary.json").read_text())
    assert saved["stopped"] == {"reason": "unpriced_Cinda"}


def original_records(tmp_path):
    result = ledger("baseline", "2025")
    result.pop("candidate", None)
    for name, records in result["records"].items():
        if name in ex.JSON_RECORDS: write(tmp_path / (name + ".json"), records)
        else: pd.DataFrame(records).to_csv(tmp_path / (name + ".csv"), index=False)
    write(tmp_path / "final_state.json", result["final_state"])
    summary = {k: v for k, v in result.items() if k not in {"records", "final_state"}}
    summary.update(variant="hold10_cap2", variant_terms=deepcopy(result["variant"]), minimum_commission_fill_fraction=None, score_quality_is_fixed_universe_diagnostic=True)
    write(tmp_path / "summary.json", summary)
    return result


def test_exact_full_original_reproduction_including_invalid_path(tmp_path):
    result = original_records(tmp_path)
    assert ex.assert_debug_stability_baseline_reproduced(result, tmp_path)["passed"]
    result["performance_valid"] = False
    summary = json.loads((tmp_path / "summary.json").read_text()); summary["performance_valid"] = False; write(tmp_path / "summary.json", summary)
    assert ex.assert_debug_stability_baseline_reproduced(result, tmp_path)["passed"]


@pytest.mark.parametrize("fault", ["summary", "finalstate", "csv", "json", "missing_record", "extra_record", "variant", "missing_quality", "minimum_fraction", "quality_annotation"])
def test_each_canonical_record_or_summary_mismatch_rejected(tmp_path, fault):
    result = original_records(tmp_path)
    if fault == "summary": result["metrics"]["total_return_lower_exact"] = "9"
    elif fault == "finalstate": result["final_state"]["cash"] = "1"
    elif fault == "csv": result["records"]["curves"][0]["cash"] = "1"
    elif fault == "json": result["records"]["holding_age"].append({"changed": True})
    elif fault == "missing_record": result["records"].pop("trades")
    elif fault == "extra_record": result["records"]["injected"] = []
    elif fault == "variant": result["variant"]["minimum_hold_sessions"] = 20
    elif fault == "missing_quality": result.pop("score_quality")
    else:
        summary = json.loads((tmp_path / "summary.json").read_text())
        if fault == "minimum_fraction": summary["minimum_commission_fill_fraction"] = 0.5
        else: summary["score_quality_is_fixed_universe_diagnostic"] = False
        write(tmp_path / "summary.json", summary)
    with pytest.raises(ValueError): ex.assert_debug_stability_baseline_reproduced(result, tmp_path)


@pytest.mark.parametrize("fault", ["extra", "missing", "escaping", "absolute", "link"])
def test_package_proof_inventory_rejects_unbound_or_unsafe_files(tmp_path, fault):
    native = tmp_path / "native.txt"; native.write_text("fixed native model")
    manifest = {"files": {"native.txt": ex.stability.sha(native)}}
    write(tmp_path / "manifest.json", manifest)
    if fault == "extra": (tmp_path / "extra.txt").write_text("unbound")
    elif fault == "missing": manifest["files"]["absent.txt"] = "0" * 64
    elif fault == "escaping": manifest["files"]["../escape.txt"] = "0" * 64
    elif fault == "absolute": manifest["files"][str(native)] = ex.stability.sha(native)
    else: (tmp_path / "alias.txt").symlink_to(native)
    with pytest.raises(ValueError): ex._package_proofs(tmp_path, manifest, "synthetic")


def setup_freeze_stage(tmp_path, monkeypatch, phase):
    source = tmp_path / "frozen.py"; source.write_text("# frozen stage source")
    declaration_path = write(tmp_path / "training.json", {"kind": ex.KIND})
    model_path = write(tmp_path / "models/manifest.json", {"kind": "model_package"})
    old_model_path = write(tmp_path / "oldmodels/manifest.json", {"kind": "model_package"})
    decision_path = write(tmp_path / "selection.json", {"kind": ex.DECISION_KIND})
    decision = {"selected_candidate": "capacity300", "selection_model_manifest": ex.stability._proof(old_model_path)}
    protocol = {"active_paper_directory": str(tmp_path / "active"), "bound_inputs": {},
        "source_families": {"stability": {}}, "design": {"execution": {"top_k": 20}}}
    monkeypatch.setattr(ex, "load_debug_declaration", lambda _: protocol)
    monkeypatch.setattr(ex, "implementation_proofs", lambda: {"synthetic": ex.stability._proof(source)})
    monkeypatch.setattr(ex, "_verify_selection", lambda *_: decision)
    def models(directory, _, requested_phase):
        root = Path(directory)
        return {"phase": requested_phase, "models": list(ex.CANDIDATES) if requested_phase == "selection" else ["capacity300"],
            "inputs": {"selection_decision": ex.stability._proof(decision_path)}}, {"new_model:" + requested_phase + ":manifest": ex.stability._proof(root / "manifest.json")}
    monkeypatch.setattr(ex, "_new_model_proofs", models)
    return declaration_path, model_path.parent, old_model_path.parent, decision_path


def test_selection_stage_freeze_binds_models_before_returns_without_decision(tmp_path, monkeypatch):
    declaration, models, _, _ = setup_freeze_stage(tmp_path, monkeypatch, "selection")
    result = ex.freeze_debug_execution_declaration(declaration, models, tmp_path / "execution.json", phase="selection")
    assert result["expected_new_ledger_count"] == 4
    assert result["selected_candidate"] is None
    assert result["bound_inputs"]["new_model_manifest"] == ex.stability._proof(models / "manifest.json")


@pytest.mark.parametrize("fault", ["laterdecision", "earliermodels", "oldstabilityreference"])
def test_selection_stage_cannot_consume_later_phase_configuration(tmp_path, monkeypatch, fault):
    declaration, models, earlier, decision = setup_freeze_stage(tmp_path, monkeypatch, "selection")
    kwargs = {"laterdecision": {"selection_path": decision}, "earliermodels": {"selection_models_directory": earlier},
        "oldstabilityreference": {"baseline_directory": tmp_path / "old_A6"}}[fault]
    with pytest.raises(ValueError):
        ex.freeze_debug_execution_declaration(declaration, models, tmp_path / "execution.json", phase="selection", **kwargs)


def test_confirmation_freeze_requires_exact_earlier_selection_package(tmp_path, monkeypatch):
    declaration, models, earlier, decision = setup_freeze_stage(tmp_path, monkeypatch, "confirmation")
    result = ex.freeze_debug_execution_declaration(declaration, models, tmp_path / "execution.json", phase="confirmation",
        selection_path=decision, selection_models_directory=earlier)
    assert result["selected_candidate"] == "capacity300"
    assert result["expected_new_ledger_count"] == 4
    assert result["bound_inputs"]["selection_decision"] == ex.stability._proof(decision)
    wrong = tmp_path / "wrong"; write(wrong / "manifest.json", {"kind": "different-selection"})
    with pytest.raises(ValueError, match="original selected 2023"):
        ex.freeze_debug_execution_declaration(declaration, models, tmp_path / "wrong-execution.json", phase="confirmation",
            selection_path=decision, selection_models_directory=wrong)


@pytest.mark.parametrize("fault", ["missingdecision", "missingearlierpackage", "extraoldA6reference"])
def test_confirmation_freeze_missing_or_extra_stage_inputs_rejected(tmp_path, monkeypatch, fault):
    declaration, models, earlier, decision = setup_freeze_stage(tmp_path, monkeypatch, "confirmation")
    kwargs = {"selection_path": decision, "selection_models_directory": earlier}
    if fault == "missingdecision": kwargs.pop("selection_path")
    elif fault == "missingearlierpackage": kwargs.pop("selection_models_directory")
    else: kwargs["baseline_directory"] = tmp_path / "unexpectedA6"
    with pytest.raises(ValueError):
        ex.freeze_debug_execution_declaration(declaration, models, tmp_path / "execution.json", phase="confirmation", **kwargs)


def test_frozen_selection_recalculates_2023_decision_not_just_selected_string(tmp_path, monkeypatch):
    from stopat30m import ashare_model_debug_research as research
    declaration = write(tmp_path / "train.json", {"kind": ex.KIND})
    selection = ex.decide_debug_selection(rows())
    model = write(tmp_path / "models.json", {"kind": "synthetic"})
    report = write(tmp_path / "report.json", {"results": rows(), "selection": selection,
        "training_declaration": ex.stability._proof(declaration)})
    decision = write(tmp_path / "decision.json", {"kind": ex.DECISION_KIND, **selection,
        "selection_report": ex.stability._proof(report), "selection_model_manifest": ex.stability._proof(model)})
    monkeypatch.setattr(ex, "load_debug_declaration", lambda _: {})
    monkeypatch.setattr(research, "phase_plan", lambda *args, **kwargs: {})
    assert ex._verify_selection(decision, declaration)["selected_candidate"] == "capacity300"
    altered = json.loads(decision.read_text()); altered["primary_qualified"] = True; write(decision, altered)
    with pytest.raises(ValueError, match="2023-only selection decision"):
        ex._verify_selection(decision, declaration)
