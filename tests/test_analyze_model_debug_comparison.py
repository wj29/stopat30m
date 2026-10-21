"""Sealed synthetic model and ledger packages; no fitting or strategy replay."""
from copy import deepcopy
from decimal import Decimal
import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SPEC = importlib.util.spec_from_file_location("a14_debug_plots", Path(__file__).parents[1] / "scripts/analyze_model_debug_comparison.py")
m = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(m)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True); m.write(path, value); return path


def seal(root, kind, phase):
    save(root / "manifest.json", {"kind": kind, "phase": phase, "files": {
        str(p.relative_to(root)): m.sha(p) for p in sorted(root.rglob("*")) if p.is_file() and p != root / "manifest.json"}})


def model_package(root, phase, declaration, decision=None):
    years = {"selection": [2023], "confirmation": [2024], "retrospective": [2025, 2026]}[phase]
    keys = list(m.CANDIDATES) if phase == "selection" else ["capacity300"]
    records = []
    for year in years:
        for key in keys:
            folder = root / f"{key}_{year}"; (folder / "model").mkdir(parents=True)
            (folder / "model/model.txt").write_text("\n".join(f"Tree={i}" for i in range(300)) + "\n")
            record = {"candidate": key, "execution_year": year, "lookback_calendar_years": 3 if key.startswith("recent3") else 5,
                "actual_tree_count": 300, "spec": {"num_boost_round": 300}}
            for role, values in (("training", [.15, .25]), ("validation", [.02, .08])):
                stats = {"role": "training_in_sample" if role == "training" else phase + "_chronological_annual_prediction",
                    "session_count": 2, "known_rank_ic_days": 2, "row_count": 50,
                    "mean_daily_rank_ic": float(np.mean(values)), "independent_observations_claim": False,
                    "in_sample_optimism": role == "training"}
                save(folder / (role + "_diagnostics.json"), stats)
                pd.DataFrame({"date": [f"{year}-01-01", f"{year}-01-02"], "observations": [25, 25], "rank_ic": values}).to_csv(folder / (role + "_diagnostics_daily.csv"), index=False)
                record[role + "_diagnostics"] = stats
            pd.DataFrame({"feature": ["KLEN", "MIN60", "STD20"], "gain": [2.0, 6.0, 2.0], "split": [20, 60, 20]}).to_csv(folder / "feature_importance.csv", index=False)
            records.append(record)
    manifest = {"kind": m.MODEL_KIND, "phase": phase, "models": keys, "execution_years": years,
        "started_at": {"selection": "2026-10-21T10:00:00+00:00", "confirmation": "2026-10-21T11:00:00+00:00", "retrospective": "2026-10-21T12:00:00+00:00"}[phase],
        "completed_at": {"selection": "2026-10-21T10:10:00+00:00", "confirmation": "2026-10-21T11:10:00+00:00", "retrospective": "2026-10-21T12:10:00+00:00"}[phase],
        "inputs": {"declaration": m.proof(declaration)}, "native_fit_count": len(records), "folds": records,
        "source_families": {"stability" if phase == "retrospective" else "primary": {"snapshot": phase + "source"}}}
    if decision is not None: manifest["inputs"]["selection_decision"] = m.proof(decision)
    manifest["files"] = {str(p.relative_to(root)): m.sha(p) for p in sorted(root.rglob("*")) if p.is_file()}
    save(root / "manifest.json", manifest)


def ledger(root, phase, candidate, period, *, invalid=False):
    start, end = m.account_window(phase, period)
    first = {"2023": "2023-01-03", "2024": "2024-01-02", "2025": "2025-01-02", "2026": "2026-01-05", "continuous": "2023-01-03" if phase == "confirmation" else "2025-01-02"}[period]
    last = end
    final = {"baseline": Decimal(105000), "capacity300": Decimal(110000), "regularized300": Decimal(108000), "recent3_capacity300": Decimal(106000)}[candidate]
    lower = [Decimal(95000 if candidate == "baseline" else 96000), final]; upper = [lower[0] + 1000, final + 1000]
    peak = m.INITIAL; dd = []
    for lo, hi in zip(lower, upper): peak = max(peak, hi); dd.append(lo / peak - 1)
    curve = [{"date": date, "equity_cny": str(lo), "equity_upper_bound_cny": str(hi), "cash_cny": "1000",
        "nav": float(lo / m.INITIAL), "accounting_valid": not (invalid and i == 1), "valuation_complete": True}
        for i, (date, lo, hi) in enumerate(zip([first, last], lower, upper))]
    trade = {"notional": "5000", "commission": "5", "transfer_fee": ".05", "stamp_tax": "2.50", "total_fee": "7.55", "slippage_cost": "5"}
    metrics = {"session_count": 2, "final_equity_cny": str(final), "total_return_lower_exact": str(final / m.INITIAL - 1),
        "max_drawdown_conservative_exact": str(min([Decimal(0), *dd])), "accounting_invalid_days": int(invalid),
        "terminal_prices_fresh": not invalid, "declared_end_reached": True, "trade_count": 1,
        "one_way_turnover_average_nav": float(Decimal(5000) / Decimal(str(np.mean([float(v) for v in lower]))) / 2),
        "total_distribution_tax_reserved_cny": "2", "cash_distribution_rounding_reserved_cny": ".01",
        **{key + "_cny": value for key, value in trade.items()}}
    row = {"candidate": candidate, "period": period, "performance_valid": not invalid, "variant": deepcopy(m.HOLD_RULE),
        "metrics": metrics, "metrics_basis": "invalid_indicative_carried_ledger_do_not_use_for_selection" if invalid else "declared_accounting_NAV_not_actual_liquidation",
        "stopped": None, "actual_last_valued_session": last}
    folder = root / candidate / period; folder.mkdir(parents=True)
    pd.DataFrame(curve).to_csv(folder / "curves.csv", index=False); pd.DataFrame([trade]).to_csv(folder / "trades.csv", index=False)
    save(folder / "summary.json", row)
    return row


def synthetic_inputs(tmp_path):
    declaration = save(tmp_path / "training.json", {"kind": "a14_model_debug_training_protocol"})
    cost = {phase: tmp_path / (phase + "_cost") for phase in m.PHASES}
    models = {phase: tmp_path / (phase + "_models") for phase in m.PHASES}
    decision = cost["selection"] / "selection_decision.json"
    for phase in m.PHASES:
        model_package(models[phase], phase, declaration, decision if phase != "selection" else None)
        periods = {"selection": ("2023",), "confirmation": ("2024", "continuous"), "retrospective": ("2025", "2026", "continuous")}[phase]
        keys = ("baseline", *m.CANDIDATES) if phase == "selection" else ("baseline", "capacity300")
        rows = [ledger(cost[phase], phase, candidate, period,
            invalid=(phase == "retrospective" and candidate == "capacity300" and period == "continuous")) for candidate in keys for period in periods]
        selected = m.selected_from_2023(rows) if phase == "selection" else None
        report = {"kind": m.REPORT_KIND, "phase": phase, "results": rows, "ledger_count": len(rows),
            "model_manifest": m.proof(models[phase] / "manifest.json"), "training_declaration": m.proof(declaration),
            "active_paper_changed": False, "broker_order_sent": False, "automatic_model_promotion": False,
            "new_native_fits_here": 0, "selection": selected, "selected_candidate": "capacity300",
            "all_performance_valid": all(row["performance_valid"] for row in rows)}
        report_path = save(cost[phase] / "report.json", report)
        if phase == "selection":
            save(decision, {"kind": "a14_model_debug_selection_decision", "declared_at": "2026-10-21T10:11:00+00:00",
                "selection_report": m.proof(report_path), "selection_model_manifest": m.proof(models[phase] / "manifest.json"),
                "training_declaration": m.proof(declaration), **selected})
        seal(cost[phase], m.REPORT_KIND, phase)
    return {"selection_directory": cost["selection"], "confirmation_directory": cost["confirmation"],
        "retrospective_directory": cost["retrospective"], "selection_models": models["selection"],
        "confirmation_models": models["confirmation"], "retrospective_models": models["retrospective"]}


def update_model_manifest_and_cost_report(kwargs, phase):
    models = kwargs[phase + "_models"]; model = m.read(models / "manifest.json")
    model["files"] = {str(p.relative_to(models)): m.sha(p) for p in sorted(models.rglob("*")) if p.is_file() and p != models / "manifest.json"}
    save(models / "manifest.json", model)
    cost = kwargs[phase + "_directory"]; report = m.read(cost / "report.json"); report["model_manifest"] = m.proof(models / "manifest.json")
    save(cost / "report.json", report); seal(cost, m.REPORT_KIND, phase)


def test_all14_accounts_6fits_and_invalid_curve_retained(tmp_path):
    kwargs = synthetic_inputs(tmp_path); result = m.load_comparison(**kwargs)
    assert len(result["accounts"]) == 14 and len(result["fits"]) == 6
    assert result["decision"]["selected_candidate"] == "capacity300"
    invalid = [row for row in result["accounts"] if not row["valid"]]
    assert len(invalid) == 1 and len(invalid[0]["nav"]) == 2
    assert invalid[0]["period"] == "continuous"
    assert all(np.isclose(sum(v["gain_fraction"].values()), 1) for v in result["fits"])


def test_cash_start_and_historical_upper_peak_drawdown_kept(tmp_path):
    kwargs = synthetic_inputs(tmp_path); data = m.load_comparison(**kwargs)
    baseline = data["accounts"][0]
    assert baseline["dd"][0] == -.05
    assert np.isclose(baseline["dd"][-1], float(Decimal(105000) / Decimal(106000) - 1))
    assert baseline["costs"]["Embedded slippage"] == 5
    assert baseline["nav"][-1] == 1.05  # No fee/slippage addback.


@pytest.mark.parametrize("phase,count", [("selection", 4), ("confirmation", 4), ("retrospective", 6)])
def test_missing_or_duplicate_stage_account_rejected(tmp_path, phase, count):
    kwargs = synthetic_inputs(tmp_path); root = kwargs[phase + "_directory"]
    report = m.read(root / "report.json"); report["results"][-1] = report["results"][0]
    save(root / "report.json", report); seal(root, m.REPORT_KIND, phase)
    with pytest.raises(ValueError): m.load_comparison(**kwargs)


def test_decision_cannot_be_reselected_from_later_profit(tmp_path):
    kwargs = synthetic_inputs(tmp_path); root = kwargs["selection_directory"]
    decision = m.read(root / "selection_decision.json"); decision["selected_candidate"] = "regularized300"
    save(root / "selection_decision.json", decision); seal(root, m.REPORT_KIND, "selection")
    with pytest.raises(ValueError, match="Frozen 2023 decision"):
        m.load_comparison(**kwargs)


def test_wrong_source_family_rejected_even_with_resealed_model_and_report(tmp_path):
    kwargs = synthetic_inputs(tmp_path); root = kwargs["retrospective_models"]
    manifest = m.read(root / "manifest.json"); manifest["source_families"] = {"primary": {}}
    save(root / "manifest.json", manifest); update_model_manifest_and_cost_report(kwargs, "retrospective")
    with pytest.raises(ValueError, match="source families"):
        m.load_comparison(**kwargs)


def test_native_file_hash_change_rejected_before_plotting(tmp_path):
    kwargs = synthetic_inputs(tmp_path); native = next(kwargs["selection_models"].rglob("model.txt")); native.write_text("replaced")
    with pytest.raises(ValueError, match="changed"):
        m.load_comparison(**kwargs)


@pytest.mark.parametrize("fault", ["drawdown", "cost", "validity", "execution"])
def test_resealed_financial_or_policy_contradictions_rejected(tmp_path, fault):
    kwargs = synthetic_inputs(tmp_path); root = kwargs["confirmation_directory"]; report = m.read(root / "report.json"); row = report["results"][0]
    if fault == "drawdown": row["metrics"]["max_drawdown_conservative_exact"] = "0"
    elif fault == "cost": row["metrics"]["commission_cny"] = "0"
    elif fault == "validity": row["metrics"]["accounting_invalid_days"] = 1
    else: row["variant"]["max_replacements"] = 3
    save(root / row["candidate"] / row["period"] / "summary.json", row); save(root / "report.json", report); seal(root, m.REPORT_KIND, "confirmation")
    with pytest.raises(ValueError): m.load_comparison(**kwargs)


def test_native_tree_count_and_diagnostic_alignment_verified(tmp_path):
    kwargs = synthetic_inputs(tmp_path); root = kwargs["confirmation_models"]
    native = next(root.rglob("model.txt")); native.write_text("Tree=0\n")
    update_model_manifest_and_cost_report(kwargs, "confirmation")
    with pytest.raises(ValueError, match="native tree"):
        m.load_comparison(**kwargs)


def test_saved_daily_diagnostics_cannot_disagree_with_record(tmp_path):
    kwargs = synthetic_inputs(tmp_path); root = kwargs["confirmation_models"]
    daily = next(root.rglob("validation_diagnostics_daily.csv")); frame = pd.read_csv(daily); frame["rank_ic"] = .99; frame.to_csv(daily, index=False)
    update_model_manifest_and_cost_report(kwargs, "confirmation")
    with pytest.raises(ValueError, match="Fit daily rank IC"):
        m.load_comparison(**kwargs)


def test_input_inventory_cannot_hide_extra_artifact(tmp_path):
    kwargs = synthetic_inputs(tmp_path); (kwargs["retrospective_models"] / "unbound.csv").write_text("unexpected")
    with pytest.raises(ValueError, match="inventory"):
        m.load_comparison(**kwargs)


def test_render_preserves_all_sources_and_exports14paths(tmp_path):
    kwargs = synthetic_inputs(tmp_path); before = m.load_comparison(**kwargs)["input_sha256"]
    output = m.render_comparison(**kwargs, output_directory=tmp_path / "plots")
    manifest = m.read(output / "manifest.json")
    assert manifest["recorded_accounts"] == 14 and manifest["recorded_fits"] == 6
    assert manifest["invalid_paths_retained"] == ["retrospective/capacity300/continuous"]
    assert manifest["no_fits_predictions_or_strategy_replay"] is True
    assert all(m.sha(path) == digest for path, digest in before.items())
    assert len(list(output.glob("*.png"))) == len(list(output.glob("*.svg"))) == 4
    exported = pd.read_csv(output / "plotted_curves.csv")
    assert len(exported) == 28 and exported.account_valid.eq(False).sum() == 2
    assert len(pd.read_csv(output / "plotted_metrics.csv")) == 14


def test_source_change_during_render_fails_without_altering_inputs_itself(tmp_path, monkeypatch):
    kwargs = synthetic_inputs(tmp_path)
    monkeypatch.setattr(m, "_curve_figures", lambda *args: None)
    monkeypatch.setattr(m, "_metric_figure", lambda *args: None)
    def concurrent_change(*args):
        (kwargs["retrospective_directory"] / "report.json").write_text('{"concurrent_change":true}')
    monkeypatch.setattr(m, "_fit_figure", concurrent_change)
    with pytest.raises(ValueError, match="changed during rendering"):
        m.render_comparison(**kwargs, output_directory=tmp_path / "plots")
    assert (tmp_path / "plots/failure.json").is_file()


@pytest.mark.parametrize("target", ["forward", "existing", "inside_source", "parent_source"])
def test_output_cannot_touch_live_or_immutable_sources(tmp_path, target):
    kwargs = synthetic_inputs(tmp_path)
    output = {"forward": tmp_path / "forward/plot", "existing": tmp_path / "existing", "inside_source": kwargs["selection_models"] / "plots", "parent_source": tmp_path}[target]
    if target == "existing": output.mkdir()
    with pytest.raises(ValueError): m.render_comparison(**kwargs, output_directory=output)
